package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.MarkerFactory
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

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
 *
 * The class-level timeout overrides the suite's 2-minute default
 * (`junit-platform.properties`): each test starts the broker container
 * inside its method - with the image pull on a cold runner - and the
 * metrics test additionally waits for KafkaClientMetrics' 60-second
 * refresh, so the default bound could expire without any defect
 * (`DEFECT_ANALYSIS-2026-10-10T19-11-11`, finding 7). Five minutes still
 * fails a hung broker long before the CI job's 30-minute limit.
 */
@Tag("integration")
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBrokerIntegrationTest {
    /**
     * Every appender a test built; stopped after the test (stop() is
     * idempotent), so an assertion failing before a test's own stop()
     * does not leave real producers, workers and the process-wide
     * self-logging registrations behind for the rest of the JVM's suite
     * (`DEFECT_ANALYSIS-2026-10-10T17-20-10`, finding 5).
     */
    private val createdAppenders = mutableListOf<KafkaAppender>()

    @AfterEach
    fun stopAppenders() {
        createdAppenders.forEach { it.stop() }
        createdAppenders.clear()
    }

    @Nested
    inner class `Delivery through a real broker` {
        @Test
        fun `should deliver a TECHNICAL and an AUDIT record through a real broker`() {
            // What is to be tested? The central external system boundary
            //   with a real broker: whether the appender's production
            //   wiring - real KafkaProducer, forced ByteArraySerializers,
            //   LZ4 default compression, enrichment headers, MDC-derived
            //   partitioning key, and the AUDIT class's mandatory
            //   acks=all/idempotence configuration - produces records a
            //   broker actually accepts and a consumer can read back.
            // How will the test case be deemed successful and why? Successful
            //   if, after stop() drains and flushes, a fresh consumer reads
            //   exactly the TECHNICAL and the AUDIT record from their
            //   topics with byte-exact payloads, the traceId key, and the
            //   meta.* headers intact. Broker acceptance is the part no
            //   MockProducer test can prove.
            // Why is it important to test this test case? The offline suite
            //   verifies the producer API contract, not the wire: an
            //   API-compatible but broker-incompatible configuration (e.g.
            //   an idempotence setting the broker rejects) would otherwise
            //   surface first in a consuming service's production logs.

            KafkaContainer(KAFKA_IMAGE).use { kafka ->
                kafka.start()

                // Safety: pre-create the topics instead of relying on
                // auto-creation. The appender caps max.block.ms at 500 ms
                // (a production guarantee the test must not weaken), and on
                // a cold CI broker the auto-creation/metadata round trip
                // can exceed that - the first send then times out and the
                // record diverts (no fallback configured -> dropped), which
                // surfaced as a flaky empty consume in CI. Pre-creation
                // takes topic bootstrap off the capped path.
                createTopics(kafka.bootstrapServers)

                // Given: the production wiring - real KafkaProducer via the
                // default ProducerFactory, one TECHNICAL (default topic) and
                // one AUDIT class (marker mapping)
                val appender =
                    newBrokerAppender(
                        bootstrapServers = kafka.bootstrapServers,
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
                            },
                    )
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

        @Test
        fun `should mirror the producer's broker-dependent meters and remove them on stop`() {
            // What is to be tested? The dynamic path of the producer-metrics
            //   mirror against a real broker: the per-topic and per-node meters
            //   the Kafka client creates only once it has connected and sent,
            //   which Micrometer's KafkaClientMetrics picks up on its refresh
            //   schedule and the RenamingMeterMirror republishes under
            //   kafka.appender.producer.* - and their removal on stop().
            // How will the test case be deemed successful and why? Successful
            //   if, after one delivered TECHNICAL record, the operator's
            //   registry holds kafka.appender.producer.topic.* meters for the
            //   record's topic and kafka.appender.producer.node.* meters, all
            //   tagged with topic.class and appender, and no raw kafka.producer.*
            //   meter; and if after stop() no kafka.appender.* meter is left.
            //   The offline suite never connects (unreachable broker) and so
            //   only sees the meters the client registers at construction.
            // Why is it important to test this test case? The topic and node
            //   meters are the ones operators chart for per-topic throughput
            //   and per-broker latency; a gap in mirroring later additions, or
            //   an orphan left behind after stop, would show only under real
            //   conditions.

            KafkaContainer(KAFKA_IMAGE).use { kafka ->
                kafka.start()
                createTopics(kafka.bootstrapServers)

                // Given: a started appender against the broker, bound to a registry
                val registry = SimpleMeterRegistry()
                val appender =
                    newBrokerAppender(
                        bootstrapServers = kafka.bootstrapServers,
                        topicMapping = TopicMappingConfig().apply { defaultTopic = TECHNICAL_TOPIC },
                    )
                appender.start()
                assertThat(appender.isStarted).isTrue()
                appender.bindMeterRegistry(registry)

                // When: one record is delivered
                appender.doAppend(newTestLoggingEvent(message = "metrics over the wire"))

                // Then: the broker-dependent meters appear in the appender's
                //   namespace - KafkaClientMetrics adds them on its refresh
                //   schedule (60 s by default), hence the generous bound
                val topicMeters = { registry.meters.filter { it.id.name.startsWith("kafka.appender.producer.topic.") } }
                val nodeMeters = { registry.meters.filter { it.id.name.startsWith("kafka.appender.producer.node.") } }
                pollUntil(timeoutMs = METER_REFRESH_TIMEOUT.toMillis(), intervalMs = 250) {
                    topicMeters().any { it.id.getTag("topic") == TECHNICAL_TOPIC } && nodeMeters().isNotEmpty()
                }
                assertThat(topicMeters() + nodeMeters()).allSatisfy { meter ->
                    assertThat(meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS)).isEqualTo("technical")
                    assertThat(meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER)).isNotBlank()
                }
                assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.producer") }

                // And: stop() removes every appender meter, the dynamic ones included
                appender.stop()
                assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.appender.") }
            }
        }
    }

    /** The production wiring against the container: real producers via the default factory. */
    private fun newBrokerAppender(
        bootstrapServers: String,
        topicMapping: TopicMappingConfig,
    ): KafkaAppender =
        newTestAppender(
            component = "integration-test",
            cmdbId = "CMDB-IT",
            environment = "it",
            kafkaProducerProperties = "bootstrap.servers=$bootstrapServers",
            topicMapping = topicMapping,
            producerFactory = ProducerFactory.default(),
        ).also { createdAppenders += it }

    private fun createTopics(bootstrapServers: String) {
        Admin.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)).use { admin ->
            admin
                .createTopics(
                    listOf(
                        NewTopic(TECHNICAL_TOPIC, 1, 1.toShort()),
                        NewTopic(AUDIT_TOPIC, 1, 1.toShort()),
                    ),
                ).all()
                .get(30, TimeUnit.SECONDS)
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

        /**
         * KafkaClientMetrics checks for new client metrics every 60 s
         * (first check after one interval); bound with headroom.
         */
        private val METER_REFRESH_TIMEOUT: Duration = Duration.ofSeconds(90)
    }
}
