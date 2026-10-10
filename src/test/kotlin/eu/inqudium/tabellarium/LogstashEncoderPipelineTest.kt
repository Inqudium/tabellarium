package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.LoggingEvent
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import net.logstash.logback.encoder.LogstashEncoder
import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.event.KeyValuePair

/**
 * Default-run coverage of the optional `logstash-logback-encoder` - and
 * with it the Jackson 3 version the pom pins - inside the appender's
 * pipeline. The README recommends [LogstashEncoder], Dependabot moves
 * the encoder and the Jackson pin, and an encoder/Jackson incompatibility
 * would otherwise surface only at a consumer's runtime as an
 * `encoder.error` diversion on every event. The external-contract
 * characterization in [LogstashHttpMethodKeyValueIngestTest] is excluded
 * from the default run and from CI, so it cannot be this safety net
 * (`DEFECT_ANALYSIS-2026-10-10T13-00-00`, finding 2).
 */
class LogstashEncoderPipelineTest {
    @Test
    fun `should deliver a LogstashEncoder JSON document as the record payload`() {
        // What is to be tested? Whether a KafkaAppender configured with a
        //   real LogstashEncoder - running on the pinned Jackson 3 - turns a
        //   log event into a record whose payload is the encoder's JSON
        //   document, MDC entry and SLF4J 2 key-value pair included.
        // How will the test case be deemed successful and why? Successful
        //   if exactly one record is delivered, its payload parses as JSON
        //   with the event's message, level, logger name, MDC value and
        //   key-value pair, and the metrics show one dispatched event and no
        //   fallback (in particular no encoder.error). A binary or
        //   behavioral break between the encoder and Jackson fails either
        //   the parse or the fallback assertion.
        // Why is it important to test this test case? The encoder and the
        //   Jackson pin are bumped by Dependabot on the strength of the
        //   default-run checks; without this test those checks could not
        //   detect that the recommended encoder no longer works.

        // Given: an appender with a real LogstashEncoder, bound to a registry
        val context = LoggerContext()
        val factory = RecordingProducerFactory()
        val registry = SimpleMeterRegistry()
        val appender =
            KafkaAppender().apply {
                this.context = context
                encoder = LogstashEncoder().apply { this.context = context }
                component = "logstash-test"
                cmdbId = "CMDB-LOGSTASH"
                environment = "test"
                kafkaProducerProperties = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092"
                topicMapping = TopicMappingConfig().apply { defaultTopic = "logstash.topic" }
                producerFactory = factory
            }
        appender.start()
        assertThat(appender.isStarted).isTrue()
        appender.bindMeterRegistry(registry)
        // Held before stop(), which removes the meters from the registry
        val fallbackCounters = registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK).counters()
        val dispatchedCounters = registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED).counters()
        assertThat(fallbackCounters).isNotEmpty

        // When: one event with an MDC entry and a key-value pair is logged
        val event =
            (
                newTestLoggingEvent(
                    message = "order shipped",
                    mdc = mapOf("traceId" to "trace-7"),
                    level = Level.WARN,
                    loggerName = "com.example.Shipping",
                ) as LoggingEvent
            ).apply { addKeyValuePair(KeyValuePair("order.id", "o-42")) }
        appender.doAppend(event)
        appender.stop()

        // Then: the payload is the encoder's JSON document
        val record =
            factory.createdProducers
                .single()
                .history()
                .single()
        val json = ObjectMapper().readTree(record.value())
        assertThat(json.get("message").asText()).isEqualTo("order shipped")
        assertThat(json.get("level").asText()).isEqualTo("WARN")
        assertThat(json.get("logger_name").asText()).isEqualTo("com.example.Shipping")
        assertThat(json.get("traceId").asText()).isEqualTo("trace-7")
        assertThat(json.get("order.id").asText()).isEqualTo("o-42")

        // And: dispatched, not diverted
        assertThat(fallbackCounters).allSatisfy { assertThat(it.count()).isZero() }
        assertThat(dispatchedCounters.sumOf { it.count() }).isEqualTo(1.0)
    }
}
