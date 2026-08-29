package eu.inqudium.tabellarium.bench;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.spi.ContextAwareBase;
import eu.inqudium.tabellarium.EnrichedRecord;
import eu.inqudium.tabellarium.MetricsBindings;
import eu.inqudium.tabellarium.MicrometerKafkaAppenderMetrics;
import eu.inqudium.tabellarium.ProducerPropertiesBuilder;
import eu.inqudium.tabellarium.ProducerRegistry;
import eu.inqudium.tabellarium.ResilientMessageSender;
import eu.inqudium.tabellarium.TopicClass;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Verifies finding 3 of PERF_ANALYSIS-2026-08-29T11-01-08: the
 * per-delivered-event observability envelope (capturing callback
 * lambda, {@code Duration} box, Resilience4j event objects via the
 * attached breaker consumers, Micrometer meter updates).
 *
 * <p><b>Why this instrument:</b> the full-pipeline benchmark saturates
 * its single worker under open load and then measures mostly the
 * overflow path, on which the worker-side envelope never runs. This
 * benchmark therefore calls {@code ResilientMessageSender.send}
 * directly - the exact delivered-path code, single-threaded, no queue -
 * against a {@link DiscardingProducer} whose instant success callback
 * exercises the breaker/metrics completion exactly as in production.
 * {@code metricsBound=true} attaches the production wiring
 * ({@code MetricsBindings.bind}, which also registers the breaker
 * event consumers of the finding); {@code false} leaves the NO_OP
 * hook. The primary result is the bound-vs-unbound delta of
 * {@code gc.alloc.rate.norm}; the time delta is secondary.
 *
 * <p>The library's internal building blocks are bytecode-public
 * (Kotlin {@code internal}); default parameters are spelled out because
 * Java sees only the full-arity signatures. Repository convention on
 * injected time sources is honored: the throttle gets an explicit
 * nanoTime source (never consulted in CLOSED state, so its boxing is
 * off the measured path).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class SenderPathBenchmark {

    @Param({"false", "true"})
    public String metricsBound;

    private ResilientMessageSender sender;
    private EnrichedRecord enrichment;
    private byte[] payload;
    private LoggingEvent[] events;
    private int next;

    @Setup
    public void setUp() {
        ProducerRegistry registry = ProducerRegistry.Companion.create(
                new ProducerPropertiesBuilder(Map.of("bootstrap.servers", "bench:9092"), null),
                Set.of(TopicClass.TECHNICAL),
                properties -> new DiscardingProducer(),
                Duration.ofSeconds(10));
        CircuitBreakerRegistry breakers = ResilientMessageSender.Companion.defaultCircuitBreakerRegistry();
        sender = new ResilientMessageSender(
                registry, breakers, null, Duration.ofMillis(5), System::nanoTime);

        if (Boolean.parseBoolean(metricsBound)) {
            ContextAwareBase status = new ContextAwareBase();
            status.setContext(new LoggerContext());
            MetricsBindings bindings = new MetricsBindings(status);
            MicrometerKafkaAppenderMetrics impl = bindings.bind(
                    new SimpleMeterRegistry(), Tags.empty(), "bench", breakers, registry);
            sender.setMetrics(impl);
        }

        enrichment = new EnrichedRecord(
                "trace-0123456789abcdef0123456789abcdef",
                Map.of(
                        "meta.component", "payment-service".getBytes(StandardCharsets.UTF_8),
                        "meta.cmdbId", "CMDB-12345".getBytes(StandardCharsets.UTF_8),
                        "meta.environment", "prod".getBytes(StandardCharsets.UTF_8),
                        "meta.agent.name", "logback-kafka-appender".getBytes(StandardCharsets.UTF_8),
                        "meta.agent.version", "1.0.0-SNAPSHOT".getBytes(StandardCharsets.UTF_8)));
        payload = new byte[1024];

        // Pre-built, pre-frozen events: the sender only carries the event
        // reference for the (never-taken) fallback path, so per-op event
        // construction would only add caller-path noise to a worker-path
        // measurement. 1024 distinct instances defeat constant folding.
        LoggerContext context = new LoggerContext();
        events = new LoggingEvent[1024];
        for (int i = 0; i < events.length; i++) {
            events[i] = new LoggingEvent(
                    "bench.fqcn", context.getLogger("bench"), Level.INFO, "sender event " + i, null, null);
            events[i].setMDCPropertyMap(Map.of());
        }
    }

    @Benchmark
    public void sendOneEvent() {
        LoggingEvent event = events[nextIndex()];
        sender.send(TopicClass.TECHNICAL, "bench.topic", payload, enrichment, event, () -> Boolean.TRUE);
    }

    private int nextIndex() {
        int i = next + 1;
        next = i;
        return i & (events.length - 1);
    }
}
