package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.utils.AppInfoParser
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class AppenderInfoTest {
    @Nested
    inner class `Fallback labels` {
        @Test
        fun `should report none for both fallback labels without a fallback appender`() {
            // What is to be tested? Whether an appender without a fallback
            //   publishes the literal `none` as fallback name and type.
            // How will the test case be deemed successful and why? Successful
            //   if both labels read `none` - an explicit value, so a query can
            //   select the drop-policy appenders with fallback="none".
            // Why is it important to test this test case? Without a fallback,
            //   events.fallback counts dropped events; the label is the one
            //   place a dashboard learns that, without the logback.xml.

            // When
            val info = AppenderInfo.of(fallbackAppender = null, debug = false, topics = emptyMap(), effectiveProperties = emptyMap())

            // Then
            assertThat(info.fallback).isEqualTo(AppenderInfo.NONE)
            assertThat(info.fallbackType).isEqualTo(AppenderInfo.NONE)
        }

        @Test
        fun `should report the fallback appender's name and simple class name`() {
            // What is to be tested? Whether a configured fallback is reported
            //   by its Logback name and its class's simple name.
            // How will the test case be deemed successful and why? Successful
            //   if the labels equal the appender's name and simple class name.
            // Why is it important to test this test case? These two labels are
            //   the reason the info gauge exists: they tell where diverted
            //   events end up.

            // Given
            val fallback = RecordingAppender().apply { name = "FALLBACK_FILE" }

            // When
            val info = AppenderInfo.of(fallback, debug = false, topics = emptyMap(), effectiveProperties = emptyMap())

            // Then
            assertThat(info.fallback).isEqualTo("FALLBACK_FILE")
            assertThat(info.fallbackType).isEqualTo(RecordingAppender::class.java.simpleName)
        }

        @Test
        fun `should report a nameless fallback appender as unnamed`() {
            // What is to be tested? Whether a fallback appender without a name
            //   is distinguishable from no fallback at all.
            // How will the test case be deemed successful and why? Successful
            //   if the name label reads `unnamed` (the same convention as the
            //   appender tag), not `none` and not blank.
            // Why is it important to test this test case? `none` would claim a
            //   drop policy for an appender that does deliver to a fallback.

            // Given
            val fallback = RecordingAppender()

            // When
            val info = AppenderInfo.of(fallback, debug = false, topics = emptyMap(), effectiveProperties = emptyMap())

            // Then
            assertThat(info.fallback).isEqualTo("unnamed")
        }
    }

    @Nested
    inner class `Versions` {
        @Test
        fun `should default to the library version and the Kafka client version`() {
            // What is to be tested? Whether the two version labels come from
            //   the build-filtered library version and the Kafka client on the
            //   classpath.
            // How will the test case be deemed successful and why? Successful
            //   if they equal MessageEnricher.AGENT_VERSION (the same source as
            //   the meta.agent.version header) and AppInfoParser.getVersion().
            // Why is it important to test this test case? The info series and
            //   the record header must name the same version; two sources
            //   would drift.

            // When
            val info = AppenderInfo.of(fallbackAppender = null, debug = true, topics = emptyMap(), effectiveProperties = emptyMap())

            // Then
            assertThat(info.version).isEqualTo(MessageEnricher.AGENT_VERSION).isNotEqualTo("unknown")
            assertThat(info.kafkaClientVersion).isEqualTo(AppInfoParser.getVersion())
            assertThat(info.debug).isTrue()
        }
    }

    @Nested
    inner class `Topic info` {
        @Test
        fun `should list every topic sorted with its class and that class's producer settings`() {
            // What is to be tested? Whether each topic is paired with its
            //   class's effective acks and linger.ms.
            // How will the test case be deemed successful and why? Successful
            //   if the topics come out sorted by name, each with the settings
            //   of its own class - not of another class.
            // Why is it important to test this test case? The labels explain
            //   the per-class latency floor of send.duration; settings filed
            //   under the wrong topic would mislead exactly that diagnosis.

            // Given
            val topics = mapOf("svc.logs" to TopicClass.TECHNICAL, "audit.security" to TopicClass.AUDIT)
            val properties =
                mapOf(
                    TopicClass.TECHNICAL to mapOf(ProducerConfig.ACKS_CONFIG to "1", ProducerConfig.LINGER_MS_CONFIG to "50"),
                    TopicClass.AUDIT to mapOf(ProducerConfig.ACKS_CONFIG to "all", ProducerConfig.LINGER_MS_CONFIG to "20"),
                )

            // When
            val info = AppenderInfo.of(fallbackAppender = null, debug = false, topics = topics, effectiveProperties = properties)

            // Then
            assertThat(info.topics.map { listOf(it.topic, it.topicClass, it.acks, it.lingerMs) })
                .containsExactly(
                    listOf("audit.security", TopicClass.AUDIT, "all", "20"),
                    listOf("svc.logs", TopicClass.TECHNICAL, "1", "50"),
                )
        }

        @Test
        fun `should fall back to the Kafka client default for a setting the builder left out`() {
            // What is to be tested? Whether a setting absent from the merged
            //   properties (acks under explicit idempotence) reports the value
            //   the producer actually runs with.
            // How will the test case be deemed successful and why? Successful
            //   if the labels equal the Kafka client's ConfigDef defaults, not
            //   a blank or `unknown`.
            // Why is it important to test this test case? A blank acks label
            //   would hide that an idempotent producer runs with acks=all.

            // Given
            val defaults = ProducerConfig.configDef().defaultValues()

            // When
            val info =
                AppenderInfo.of(
                    fallbackAppender = null,
                    debug = false,
                    topics = mapOf("svc.logs" to TopicClass.TECHNICAL),
                    effectiveProperties = mapOf(TopicClass.TECHNICAL to emptyMap()),
                )

            // Then
            val topic = info.topics.single()
            assertThat(topic.acks).isEqualTo(defaults.getValue(ProducerConfig.ACKS_CONFIG).toString())
            assertThat(topic.lingerMs).isEqualTo(defaults.getValue(ProducerConfig.LINGER_MS_CONFIG).toString())
        }
    }
}
