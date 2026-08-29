package eu.inqudium.tabellarium.bench;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Verifies finding 2 of PERF_ANALYSIS-2026-08-29T11-01-08: five
 * {@code RecordHeader} wrappers are allocated per event for five
 * constant headers ({@code ResilientMessageSender.buildRecord}).
 *
 * <p>Baseline replicates the production record-building shape (create
 * record, then five {@code headers().add(name, bytes)} calls); the
 * candidate passes one pre-built shared immutable header list to the
 * {@code ProducerRecord} constructor. The primary result is
 * {@code gc.alloc.rate.norm} (bytes/op) from {@code -prof gc}; the time
 * delta is secondary. Header names/values mirror the enricher's real
 * static header set in count and typical size.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class RecordHeadersBenchmark {

    private static final String TOPIC = "bench.topic";

    // Names and value sizes mirror MessageEnricher's static header set.
    private String[] headerNames;
    private byte[][] headerValues;
    private List<Header> sharedHeaders;
    private byte[] key;
    private byte[] payload;

    @Setup
    public void setUp() {
        headerNames = new String[] {
            "meta.component", "meta.cmdbId", "meta.environment", "meta.agent.name", "meta.agent.version",
        };
        headerValues = new byte[][] {
            "payment-service".getBytes(StandardCharsets.UTF_8),
            "CMDB-12345".getBytes(StandardCharsets.UTF_8),
            "prod".getBytes(StandardCharsets.UTF_8),
            "logback-kafka-appender".getBytes(StandardCharsets.UTF_8),
            "1.0.0-SNAPSHOT".getBytes(StandardCharsets.UTF_8),
        };
        List<Header> built = new java.util.ArrayList<>();
        for (int i = 0; i < headerNames.length; i++) {
            built.add(new RecordHeader(headerNames[i], headerValues[i]));
        }
        sharedHeaders = List.copyOf(built);
        key = "trace-0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        payload = new byte[1024];
    }

    @Benchmark
    public ProducerRecord<byte[], byte[]> baselinePerEventHeaders() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, null, null, key, payload);
        for (int i = 0; i < headerNames.length; i++) {
            record.headers().add(headerNames[i], headerValues[i]);
        }
        return record;
    }

    @Benchmark
    public ProducerRecord<byte[], byte[]> candidateSharedHeaders() {
        return new ProducerRecord<>(TOPIC, null, null, key, payload, sharedHeaders);
    }
}
