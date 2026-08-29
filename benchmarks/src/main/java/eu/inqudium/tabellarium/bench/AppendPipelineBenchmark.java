package eu.inqudium.tabellarium.bench;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.LoggingEvent;
import eu.inqudium.tabellarium.KafkaAppender;
import eu.inqudium.tabellarium.TopicMappingConfig;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Verifies findings 1 and 3 of PERF_ANALYSIS-2026-08-29T11-01-08 on the
 * full production caller path: {@code KafkaAppender.doAppend} -> encode
 * -> route -> enrich -> bounded-queue hand-off, with the per-class
 * worker draining into a {@link DiscardingProducer} (instant success
 * callbacks, so the breaker/metrics completion path runs).
 *
 * <p><b>Role after the instrument redesign:</b> an open-loop caller
 * saturates the single per-class worker, the queue fills, and
 * {@code offer} then rejects on its count check without touching the
 * put-lock - so this benchmark does NOT isolate finding 1's lock
 * (that is {@link HandoffBenchmark}) nor finding 3's delivered-path
 * envelope (that is {@link SenderPathBenchmark}). What it measures,
 * deliberately, is the caller-visible cost of {@code doAppend} in the
 * worst regime the design allows - sustained overload with shedding
 * engaged - across -t 1 / 8 / 32 (sample mode for p99), plus the
 * caller-side allocation per event via {@code -prof gc}. The teardown
 * prints the appender's own counters so the measured path mix
 * (delivered vs. diverted) is part of the evidence, not a hidden
 * variable.
 *
 * <p>A fresh {@link LoggingEvent} is built per invocation (as in
 * production - Logback creates one per log call); its construction and
 * deferred-processing freeze are therefore deliberately inside the
 * measured operation. Message content varies across 1024 pre-built
 * strings to defeat constant folding. The event escapes into the
 * dispatcher queue, so dead-code elimination cannot remove the work;
 * the Blackhole consume is belt and braces.
 *
 * <p>The internal producer-factory seam is reached via its mangled JVM
 * name ({@code setProducerFactory$tabellarium}) - Kotlin internal
 * members are public bytecode with a module suffix, callable from Java.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class AppendPipelineBenchmark {

    @Param({"false", "true"})
    public String metricsBound;

    private LoggerContext context;
    private KafkaAppender appender;
    private SimpleMeterRegistry registry;
    private Logger logger;
    private String[] messages;
    private Map<String, String> mdc;
    private int next;

    @Setup
    public void setUp() {
        context = new LoggerContext();
        appender = new KafkaAppender();
        appender.setContext(context);

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%msg");
        appender.setEncoder(encoder);

        appender.setComponent("bench-service");
        appender.setCmdbId("CMDB-BENCH");
        appender.setEnvironment("bench");
        appender.setKafkaProducerProperties("bootstrap.servers=bench:9092");
        TopicMappingConfig mapping = new TopicMappingConfig();
        mapping.setDefaultTopic("bench.topic");
        appender.setTopicMapping(mapping);
        // Production-default order of magnitude, enlarged: under open-loop
        // load the queue fills regardless (see the class KDoc), and the
        // teardown counters make the achieved path mix visible.
        appender.setSendQueueCapacity(1 << 16);
        appender.setProducerFactory$tabellarium(properties -> new DiscardingProducer());
        appender.start();
        if (!appender.isStarted()) {
            throw new IllegalStateException("appender failed to start; check status output");
        }
        if (Boolean.parseBoolean(metricsBound)) {
            registry = new SimpleMeterRegistry();
            appender.bindMeterRegistry(registry, Tags.empty());
        }

        logger = context.getLogger("bench.logger");
        messages = new String[1024];
        for (int i = 0; i < messages.length; i++) {
            messages[i] = "benchmark event number " + i + " with a payload-ish tail of ordinary log prose";
        }
        mdc = Map.of("traceId", "trace-0123456789abcdef0123456789abcdef");
    }

    @Benchmark
    public void appendOneEvent(Blackhole blackhole) {
        LoggingEvent event =
                new LoggingEvent("bench.fqcn", logger, Level.INFO, messages[nextIndex()], null, null);
        event.setMDCPropertyMap(mdc);
        appender.doAppend(event);
        blackhole.consume(event);
    }

    private int nextIndex() {
        // Racy on purpose under multi-threaded runs: an exact rotation is
        // irrelevant, avoiding a shared atomic on the measured path is not.
        int i = next + 1;
        next = i;
        return i & (messages.length - 1);
    }

    @TearDown
    public void tearDown() {
        // Path-mix evidence for finding 1: how many events were actually
        // delivered vs. diverted during this trial. Readable only on the
        // metrics-bound runs (the unbound variant uses the NO_OP hook by
        // design); the counters must be read BEFORE stop(), which unbinds
        // and deregisters the meters.
        if (registry != null) {
            double accepted = sum("kafka.appender.events.accepted");
            double dispatched = sum("kafka.appender.events.dispatched");
            double fallback = sum("kafka.appender.events.fallback");
            System.out.printf(
                    "[path-mix] metricsBound=%s accepted=%.0f dispatched=%.0f diverted=%.0f%n",
                    metricsBound, accepted, dispatched, fallback);
        } else {
            System.out.printf("[path-mix] metricsBound=%s counters unavailable (NO_OP by design)%n", metricsBound);
        }
        appender.stop();
        context.stop();
    }

    private double sum(String meterName) {
        return registry.find(meterName).counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count)
                .sum();
    }
}
