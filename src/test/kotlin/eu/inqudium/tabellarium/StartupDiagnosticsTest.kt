package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class StartupDiagnosticsTest {
    // -- Test fixtures --------------------------------------------------

    private val registries = mutableListOf<ProducerRegistry>()

    @AfterEach
    fun closeRegistries() {
        registries.forEach { runCatching { it.close() } }
    }

    private val baseProperties =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        )

    /** A registry over [RecordingProducerFactory] mocks - no network, closed after each test. */
    private fun newRegistry(
        classes: Set<TopicClass>,
        base: Map<String, String> = baseProperties,
    ): ProducerRegistry =
        ProducerRegistry
            .create(
                propertiesBuilder = ProducerPropertiesBuilder(base, defaultClientIdPrefix = "tabellarium-svc"),
                activeTopicClasses = classes,
                producerFactory = RecordingProducerFactory(),
            ).also { registries += it }

    @Nested
    inner class `Mandatory override warning` {
        @Test
        fun `should name the class, the property and both values`() {
            // What is to be tested? Whether the warning carries everything an
            //   operator needs to act on it: which class enforced what, and
            //   which of their values was overruled.
            // How will the test case be deemed successful and why? Successful
            //   if the text contains the class, the property key, the user's
            //   value and the enforced value - the four fields of the
            //   violation.
            // Why is it important to test this test case? The message is the
            //   only place the override becomes visible; a field missing here
            //   sends the operator into the configuration guide to guess.

            // Given
            val violation =
                MandatoryOverrideViolation(
                    topicClass = TopicClass.AUDIT,
                    propertyKey = ProducerConfig.ACKS_CONFIG,
                    userValue = "1",
                    enforcedValue = "all",
                )

            // When
            val message = StartupDiagnostics.mandatoryOverrideWarning(violation)

            // Then
            assertThat(message)
                .contains("AUDIT")
                .contains(ProducerConfig.ACKS_CONFIG)
                .contains("'1'")
                .contains("'all'")
        }
    }

    @Nested
    inner class `Cleartext transport warning` {
        @Test
        fun `should warn when a graded class has no security protocol configured`() {
            // What is to be tested? Whether an active AUDIT class with an
            //   unset security.protocol (Kafka's default is PLAINTEXT) yields
            //   the warning.
            // How will the test case be deemed successful and why? Successful
            //   if a message is returned that names the graded class and the
            //   security.protocol setting the operator has to change.
            // Why is it important to test this test case? Audit records that
            //   travel unencrypted are readable and tamperable by anyone on
            //   the network path; the start-up log is where the operator has
            //   to learn that, since the appender cannot enforce TLS itself.

            // Given
            val registry = newRegistry(setOf(TopicClass.AUDIT))

            // When
            val message = StartupDiagnostics.cleartextTransportWarning(registry)

            // Then
            assertThat(message)
                .isNotNull()
                .contains("cleartext transport")
                .contains("AUDIT")
                .contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
        }

        @Test
        fun `should warn when the protocol is spelled plaintext in any case`() {
            // What is to be tested? Whether an explicit "plaintext" (lower
            //   case) counts as cleartext.
            // How will the test case be deemed successful and why? Successful
            //   if the warning is returned although the operator did set the
            //   property - Kafka accepts the value case-insensitively, so the
            //   check has to as well.
            // Why is it important to test this test case? A case-sensitive
            //   comparison would let an explicit lower-case PLAINTEXT pass
            //   silently - exactly the configuration most likely to be a
            //   copy-paste from an example.

            // Given
            val registry =
                newRegistry(
                    setOf(TopicClass.FUNCTIONAL),
                    baseProperties + (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to " plaintext "),
                )

            // When / Then
            assertThat(StartupDiagnostics.cleartextTransportWarning(registry))
                .isNotNull()
                .contains("FUNCTIONAL")
        }

        @Test
        fun `should stay silent when the graded class is configured for SSL`() {
            // What is to be tested? Whether the warning is keyed on the
            //   effective security.protocol: the same AUDIT class with SSL
            //   configured must not warn.
            // How will the test case be deemed successful and why? Successful
            //   if null is returned - no message, nothing for the appender to
            //   report.
            // Why is it important to test this test case? A warning that fires
            //   regardless of the transport would be noise, and noise trains
            //   operators to ignore the one case that matters.

            // Given
            val registry =
                newRegistry(
                    setOf(TopicClass.AUDIT),
                    baseProperties + (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL"),
                )

            // When / Then
            assertThat(StartupDiagnostics.cleartextTransportWarning(registry)).isNull()
        }

        @Test
        fun `should stay silent when only ungraded classes run over cleartext`() {
            // What is to be tested? Whether the warning is scoped to classes
            //   with mandatory overrides: TECHNICAL and PERFORMANCE over
            //   cleartext are the operator's call.
            // How will the test case be deemed successful and why? Successful
            //   if null is returned for an ungraded-only registry without any
            //   security.protocol.
            // Why is it important to test this test case? The warning argues
            //   from the enforced durability of graded records; applying it to
            //   best-effort classes would contradict its own rationale.

            // Given
            val registry = newRegistry(setOf(TopicClass.TECHNICAL, TopicClass.PERFORMANCE))

            // When / Then
            assertThat(StartupDiagnostics.cleartextTransportWarning(registry)).isNull()
        }
    }

    @Nested
    inner class `Debug messages` {
        @Test
        fun `should open with the no-per-event-effect reminder and list the active classes`() {
            // What is to be tested? The fixed head of the diagnostics: the
            //   reminder that <debug> is start-up only, then the active
            //   classes.
            // How will the test case be deemed successful and why? Successful
            //   if the first message names <debug> and the second lists both
            //   active classes.
            // Why is it important to test this test case? Operators leave
            //   <debug>true</debug> in production configurations because they
            //   assume a per-event cost; the reminder is the documented
            //   counter-measure and has to be the first thing they read.

            // Given
            val registry = newRegistry(setOf(TopicClass.AUDIT, TopicClass.TECHNICAL))

            // When
            val messages = StartupDiagnostics.debugMessages(registry, fallbackAppender = null, kafkaProducerProperties = "")

            // Then
            assertThat(messages[0]).contains("<debug>").contains("no per-event effect")
            assertThat(messages[1]).contains("AUDIT").contains("TECHNICAL")
        }

        @Test
        fun `should describe the fallback as absent or by its class`() {
            // What is to be tested? The fallback line for both configurations.
            // How will the test case be deemed successful and why? Successful
            //   if the line says "none" without a fallback and names the
            //   appender class with one.
            // Why is it important to test this test case? "No fallback" means
            //   silent drop on delivery failure; the diagnostics are the place
            //   where an operator confirms that this is what they configured.

            // Given
            val registry = newRegistry(setOf(TopicClass.TECHNICAL))

            // When
            val without = StartupDiagnostics.debugMessages(registry, fallbackAppender = null, kafkaProducerProperties = "")
            val with =
                StartupDiagnostics.debugMessages(
                    registry,
                    fallbackAppender = ListAppender<ILoggingEvent>(),
                    kafkaProducerProperties = "",
                )

            // Then
            assertThat(without).anyMatch { it.startsWith("Fallback appender: none") }
            assertThat(with).anyMatch { it.startsWith("Fallback appender: configured (ListAppender)") }
        }

        @Test
        fun `should list generated settings per class and never repeat operator values`() {
            // What is to be tested? The per-class "generated settings" lines:
            //   they must show what the appender derived (client.id) and
            //   enforced (acks=all for AUDIT) and must omit every value the
            //   operator supplied themselves.
            // How will the test case be deemed successful and why? Successful
            //   if the AUDIT line contains the derived client.id and acks=all,
            //   and no message contains the operator's credential string.
            // Why is it important to test this test case? The diff against the
            //   operator's base properties is what keeps the status output
            //   credential-safe by construction (SECURITY.md); a regression
            //   here would print SASL secrets into the start-up log.

            // Given: an operator credential in the base properties
            val secret = "org.apache.kafka.common.security.plain.PlainLoginModule required password=\"s3cr3t\";"
            val text =
                """
                bootstrap.servers=localhost:9092
                sasl.jaas.config=$secret
                """.trimIndent()
            val registry = newRegistry(setOf(TopicClass.AUDIT), parseKafkaProducerProperties(text))

            // When
            val messages = StartupDiagnostics.debugMessages(registry, fallbackAppender = null, kafkaProducerProperties = text)

            // Then
            val auditLine = messages.single { it.startsWith("Generated producer settings [audit]") }
            assertThat(auditLine)
                .contains("${ProducerConfig.CLIENT_ID_CONFIG}=tabellarium-svc-audit")
                .contains("${ProducerConfig.ACKS_CONFIG}=all")
            assertThat(messages).noneMatch { it.contains("s3cr3t") }
        }
    }
}
