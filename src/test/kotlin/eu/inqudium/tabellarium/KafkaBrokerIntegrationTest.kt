package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.slf4j.MarkerFactory
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.UUID

/**
 * Real-broker integration test for the central external system boundary.
 *
 * The offline suite proves the appender against `MockProducer`, which
 * verifies the producer API contract but neither the wire format nor the
 * broker's acceptance of the per-class producer configuration. This test
 * closes that gap: one successful TECHNICAL and one successful AUDIT
 * record - real serializers, real LZ4 compression, real headers and
 * partitioning key, and for AUDIT the mandatory `acks=all` plus
 * idempotence actually negotiated with a broker.
 *
 * Tagged `integration` and excluded from the default run (see the
 * `surefire.excludedGroups` property in the pom): it needs a Docker
 * daemon and a broker container, which the fast offline loop must not
 * depend on. Run deliberately with `mvn -Pintegration test`.
 */
@Tag("integration")
class KafkaBrokerIntegrationTest {
    /** Minimal encoder so the payload assertion is byte-exact. */
    private class PlainTextEncoder : EncoderBase<ILoggingEvent>() {
        override fun encode(event: ILoggingEvent): ByteArray = event.formattedMessage.toByteArray(Charsets.UTF_8)

        override fun headerBytes(): ByteArray = ByteArray(0)

        override fun footerBytes(): ByteArray = ByteArray(0)
    }

    @Test
    fun `should deliver a TECHNICAL and an AUDIT record through a real broker`() {
        KafkaContainer(KAFKA_IMAGE).use { kafka ->
            kafka.start()

            // Given: the production wiring - real KafkaProducer via the
            // default ProducerFactory, one TECHNICAL (default topic) and
            // one AUDIT class (marker mapping)
            val appender =
                KafkaAppender().apply {
                    context = LoggerContext()
                    encoder = PlainTextEncoder()
                    component = "integration-test"
                    cmdbId = "CMDB-IT"
                    environment = "it"
                    kafkaProducerProperties = "bootstrap.servers=${kafka.bootstrapServers}"
                    topicMapping =
                        TopicMappingConfig().apply {
                            defaultTopic = TECHNICAL_TOPIC
                            addMapping(
                                TopicMappingEntry().apply {
                                    marker = "SECURITY"
                                    topic = AUDIT_TOPIC
                                    topicClass = "AUDIT"
                                },
                            )
                        }
                }
            appender.start()
            assertThat(appender.isStarted).isTrue()

            // When: one marker-less event (TECHNICAL, keyed by MDC traceId)
            // and one SECURITY-marked event (AUDIT)
            appender.doAppend(
                newTestLoggingEvent(
                    message = "technical over the wire",
                    mdc = mapOf("traceId" to "trace-42"),
                ),
            )
            appender.doAppend(
                (newTestLoggingEvent(message = "audit over the wire") as LoggingEvent)
                    .apply { addMarker(MarkerFactory.getDetachedMarker("SECURITY")) },
            )
            // stop() drains the send queues through the open producers and
            // closes them with a flush - after this, both records are
            // either broker-acknowledged or the consumer below times out.
            appender.stop()

            // Then: both records are readable from the broker
            val records = consume(kafka.bootstrapServers, expectedCount = 2)
            assertThat(records).hasSize(2)

            val technical = records.single { it.topic() == TECHNICAL_TOPIC }
            assertThat(technical.value()).isEqualTo("technical over the wire".toByteArray(Charsets.UTF_8))
            assertThat(technical.key()).isEqualTo("trace-42".toByteArray(Charsets.UTF_8))

            val audit = records.single { it.topic() == AUDIT_TOPIC }
            assertThat(audit.value()).isEqualTo("audit over the wire".toByteArray(Charsets.UTF_8))

            // And: the enrichment headers survived the wire round-trip
            for (record in records) {
                val headers =
                    record.headers().toArray().associate { header ->
                        header.key() to String(header.value(), Charsets.UTF_8)
                    }
                assertThat(headers)
                    .containsEntry("meta.component", "integration-test")
                    .containsEntry("meta.cmdbId", "CMDB-IT")
                    .containsEntry("meta.environment", "it")
            }
        }
    }

    private fun consume(
        bootstrapServers: String,
        expectedCount: Int,
    ): List<ConsumerRecord<ByteArray, ByteArray>> {
        val properties =
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "it-${UUID.randomUUID()}",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            )
        KafkaConsumer(properties, ByteArrayDeserializer(), ByteArrayDeserializer()).use { consumer ->
            consumer.subscribe(listOf(TECHNICAL_TOPIC, AUDIT_TOPIC))
            val received = mutableListOf<ConsumerRecord<ByteArray, ByteArray>>()
            val deadline = System.nanoTime() + CONSUME_TIMEOUT.toNanos()
            while (received.size < expectedCount && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach { received += it }
            }
            return received
        }
    }

    private companion object {
        /**
         * Official Apache Kafka image, matching the major version of the
         * kafka-clients dependency. Pinned so the test does not shift
         * under a floating tag.
         */
        private const val KAFKA_IMAGE = "apache/kafka:4.0.0"

        private const val TECHNICAL_TOPIC = "it.technical.logs"
        private const val AUDIT_TOPIC = "it.audit.logs"

        /** Generous bound: a fresh broker may need a moment for topic auto-creation. */
        private val CONSUME_TIMEOUT: Duration = Duration.ofSeconds(30)
    }
}
