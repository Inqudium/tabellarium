package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
 * [Partitioner] that always targets partition 0.
 *
 * Kafka 4 removed the default partitioner and, with it, MockProducer's
 * serializer-only constructors. Test MockProducers must now supply a
 * partitioner explicitly; the appender builds records with a null partition,
 * so without one MockProducer would dereference an empty cluster. Partition
 * selection is irrelevant for these tests, so 0 is always returned.
 */
internal class FixedZeroPartitioner : Partitioner {
    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
    ): Int = 0

    override fun close() {}

    override fun configure(configs: Map<String, *>) {}
}
