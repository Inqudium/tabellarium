package eu.inqudium.tabellarium.bench;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * Producer stand-in that completes every send instantly and retains
 * nothing. The benchmark measures the appender's own pipeline, not the
 * Kafka client; the callback IS invoked (with a pre-built success
 * metadata) so the breaker/metrics completion path runs exactly as in
 * production. Unlike {@code MockProducer}, no history list grows during
 * long measurement runs, so the producer contributes no allocation of
 * its own beyond the completed-future wrapper.
 */
final class DiscardingProducer implements Producer<byte[], byte[]> {

    private static final RecordMetadata METADATA =
            new RecordMetadata(new TopicPartition("bench.topic", 0), 0L, 0, 0L, 0, 0);
    private static final Future<RecordMetadata> COMPLETED = CompletableFuture.completedFuture(METADATA);

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        return COMPLETED;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
        if (callback != null) {
            callback.onCompletion(METADATA, null);
        }
        return COMPLETED;
    }

    @Override
    public void flush() {
        // nothing buffered
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return List.of();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Map.of();
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return Uuid.ZERO_UUID;
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        // no metrics
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        // no metrics
    }

    @Override
    public void initTransactions() {
        throw new UnsupportedOperationException("transactions are not part of the benchmarked path");
    }

    @Override
    public void beginTransaction() {
        throw new UnsupportedOperationException("transactions are not part of the benchmarked path");
    }

    @Override
    public void sendOffsetsToTransaction(
            Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
        throw new UnsupportedOperationException("transactions are not part of the benchmarked path");
    }

    @Override
    public void commitTransaction() {
        throw new UnsupportedOperationException("transactions are not part of the benchmarked path");
    }

    @Override
    public void abortTransaction() {
        throw new UnsupportedOperationException("transactions are not part of the benchmarked path");
    }

    @Override
    public void close() {
        // nothing to release
    }

    @Override
    public void close(Duration timeout) {
        // nothing to release
    }
}
