package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.encoder.EncoderBase
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class KafkaAppenderTest {
    // -- Test fixtures --------------------------------------------------

    private class TestEncoder : EncoderBase<ILoggingEvent>() {
        val encodedEvents = mutableListOf<ILoggingEvent>()

        override fun encode(event: ILoggingEvent): ByteArray {
            encodedEvents += event
            return event.formattedMessage.toByteArray(Charsets.UTF_8)
        }

        override fun headerBytes(): ByteArray = ByteArray(0)

        override fun footerBytes(): ByteArray = ByteArray(0)
    }

    private class ThrowingEncoder : EncoderBase<ILoggingEvent>() {
        override fun encode(event: ILoggingEvent): ByteArray = throw RuntimeException("simulated encoder failure")

        override fun headerBytes(): ByteArray = ByteArray(0)

        override fun footerBytes(): ByteArray = ByteArray(0)
    }

    /**
     * Stateless encoder for tests that append from many threads
     * concurrently: unlike [TestEncoder] it records nothing, so the
     * test harness itself introduces no unsynchronized shared state
     * (TestEncoder's recording list is a plain ArrayList).
     */
    private class StatelessEncoder : EncoderBase<ILoggingEvent>() {
        override fun encode(event: ILoggingEvent): ByteArray = event.formattedMessage.toByteArray(Charsets.UTF_8)

        override fun headerBytes(): ByteArray = ByteArray(0)

        override fun footerBytes(): ByteArray = ByteArray(0)
    }

    private class TestProducerFactory : ProducerFactory {
        val createdProducers = mutableListOf<MockProducer<ByteArray, ByteArray>>()
        val createdWithProperties = mutableListOf<Map<String, String>>()

        override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
            val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
            createdProducers += mock
            createdWithProperties += properties
            return mock
        }
    }

    private class RecordingAppender : AppenderBase<ILoggingEvent>() {
        // Synchronized: the asynchronous-dispatch test appends from the
        // dispatcher worker thread while the test thread polls.
        val events: MutableList<ILoggingEvent> = Collections.synchronizedList(mutableListOf())

        init {
            start()
        }

        override fun append(event: ILoggingEvent) {
            events += event
        }
    }

    private fun newAppender(
        encoder: Encoder<ILoggingEvent>? = TestEncoder(),
        component: String = "test-service",
        cmdbId: String = "CMDB-TEST",
        environment: String = "test",
        defaultTopic: String = "default.topic",
        debug: Boolean = false,
        producerFactory: ProducerFactory = TestProducerFactory(),
        fallback: Appender<ILoggingEvent>? = null,
        kafkaProducerProperties: String = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092",
    ): KafkaAppender =
        KafkaAppender().apply {
            this.context = LoggerContext()
            this.encoder = encoder
            this.component = component
            this.cmdbId = cmdbId
            this.environment = environment
            this.kafkaProducerProperties = kafkaProducerProperties
            this.topicMapping =
                TopicMappingConfig().apply {
                    this.defaultTopic = defaultTopic
                }
            this.debug = debug
            this.producerFactory = producerFactory
            // Synchronous fallback dispatch so tests can assert on the
            // RecordingAppender without polling. Production path is async;
            // see KafkaAppender KDoc on the synchronous-flag test hook.
            this.useSynchronousFallbackForTests = true
            // The fallback slot is filled via addAppender (the same path
            // Joran's AppenderRefAction takes for <appender-ref>).
            fallback?.let { addAppender(it) }
        }

    private fun KafkaAppender.statusMessages(): List<String> = context.statusManager.copyOfStatusList.map { it.message }

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Configuration validation` {
        @Test
        fun `should refuse to start when no encoder is configured`() {
            // Given
            val appender = newAppender(encoder = null)

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("No <encoder> configured") }
        }

        @Test
        fun `should refuse to start when component is blank`() {
            // Given
            val appender = newAppender(component = "  ")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("<component>") && it.contains("blank") }
        }

        @Test
        fun `should refuse to start when cmdbId is blank`() {
            // Given
            val appender = newAppender(cmdbId = "")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("<cmdbId>") && it.contains("blank") }
        }

        @Test
        fun `should refuse to start when environment is blank`() {
            // Given
            val appender = newAppender(environment = "  ")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("<environment>") && it.contains("blank") }
        }

        @Test
        fun `should refuse to start when default topic is blank`() {
            // What is to be tested? Whether a blank <defaultTopic> in
            //   <topicMapping> is caught during pipeline construction
            //   (via TopicRouter validation) rather than slipping through
            //   to runtime.
            // How will the test case be deemed successful and why? Successful
            //   if the appender stays unstarted and the status manager
            //   contains an error message referencing the blank default topic.
            //   This confirms that build-time exceptions from TopicRouter
            //   are caught and surfaced rather than silently swallowed.
            // Why is it important to test this test case? A blank default
            //   topic would otherwise cause every event in the hot path to
            //   fail Kafka's topic validation, after the appender had
            //   already reported successful start - a latent misconfiguration.

            // Given
            val appender = newAppender(defaultTopic = "")

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Failed to build") || it.contains("Default topic") }
        }
    }

    @Nested
    inner class `Pipeline construction` {
        @Test
        fun `should mark the appender as started after a successful start`() {
            // Given
            val appender = newAppender()

            // When
            appender.start()

            // Then
            assertThat(appender.isStarted).isTrue()
        }

        @Test
        fun `should start the encoder when starting the appender`() {
            // Given
            val encoder = TestEncoder()
            val appender = newAppender(encoder = encoder)

            // When
            appender.start()

            // Then
            assertThat(encoder.isStarted).isTrue()
        }

        @Test
        fun `should instantiate one producer for the active topic class`() {
            // What is to be tested? Whether the minimal configuration
            //   (only <defaultTopic>) results in a single producer instantiation
            //   for the fallback class.
            // How will the test case be deemed successful and why? Successful
            //   if exactly one MockProducer was created via the factory.
            //   This pins down the single-producer guarantee: existing
            //   deployments do not silently spin up four Kafka producers
            //   when the configuration only mentions one default topic.
            // Why is it important to test this test case? A regression where
            //   the appender always instantiated all four producer classes
            //   would quadruple network threads and buffer memory in every
            //   deployment that uses the minimal configuration - silent
            //   resource bloat that operators would not notice until it
            //   showed up in capacity planning.

            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory)

            // When
            appender.start()

            // Then
            assertThat(factory.createdProducers).hasSize(1)
        }

        @Test
        fun `should give the producer a client id derived from the component`() {
            // What is to be tested? Whether the appender wires a per-class
            //   client.id default of tabellarium-<component>-<topicclass>
            //   into the producer configuration.
            // How will the test case be deemed successful and why? Successful
            //   if the created producer's properties carry that client.id.
            //   This pins down broker-side attributability: connections,
            //   quotas, and kafka.producer metrics name the service instead
            //   of Kafka's generic producer-N.
            // Why is it important to test this test case? Without the
            //   default, every producer in the JVM shows up as producer-N on
            //   the broker, and operators cannot tell which service (or
            //   which topic class) a connection belongs to.

            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory, component = "checkout-service")

            // When
            appender.start()

            // Then
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-checkout-service-technical")
        }

        @Test
        fun `should sanitize the component for the client id`() {
            // What is to be tested? Whether characters that are unsafe in JMX
            //   object names and metric tags are replaced before the component
            //   becomes part of the client.id.
            // How will the test case be deemed successful and why? Successful
            //   if a component with spaces and special characters yields a
            //   client.id containing only [a-zA-Z0-9._-]. This pins down the
            //   sanitization contract of the derived id.
            // Why is it important to test this test case? An unsanitized
            //   client.id breaks JMX MBean registration in the Kafka client,
            //   which surfaces as confusing warnings at startup in every
            //   deployment whose component name contains a space.

            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory, component = "My Service (prod)")

            // When
            appender.start()

            // Then
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-My-Service--prod--technical")
        }

        @Test
        fun `should let an operator-supplied client id win`() {
            // Given: the operator pins client.id in kafkaProducerProperties
            val factory = TestProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.CLIENT_ID_CONFIG}=pinned-id
                        """.trimIndent(),
                )

            // When
            appender.start()

            // Then
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "pinned-id")
        }
    }

    @Nested
    inner class `Mandatory override warnings` {
        @Test
        fun `should emit a status warning when a user value conflicts with a mandatory override`() {
            // What is to be tested? Whether mandatory-override violations
            //   from ProducerPropertiesBuilder are surfaced to operators
            //   via Logback's status manager - the addWarn wiring in
            //   start(), not just the builder-level violation records.
            //   Activated through the real configuration surface: a
            //   <mapping> classifying a topic as AUDIT.
            // How will the test case be deemed successful and why? Successful
            //   if a warning naming the property key, the user value, and
            //   the enforced value reaches the status manager. The warning
            //   is the only mechanism by which operators learn that their
            //   configuration intent was overruled for compliance reasons.
            // Why is it important to test this test case? Silent enforcement
            //   would let an operator believe their acks=1 had taken effect
            //   for audit topics, when in fact acks=all was forced. Auditors
            //   would later find a discrepancy between the documented
            //   configuration and the actual broker behavior.

            // Given: an AUDIT mapping and a conflicting operator acks=1
            val factory = TestProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.ACKS_CONFIG}=1
                        """.trimIndent(),
                )
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then: the appender started and the violation was surfaced
            assertThat(appender.isStarted).isTrue()
            assertThat(appender.statusMessages())
                .anyMatch {
                    it.contains("Mandatory override applied") &&
                        it.contains(ProducerConfig.ACKS_CONFIG) &&
                        it.contains("'1'") &&
                        it.contains("'all'")
                }
        }

        @Test
        fun `should apply the mandatory overrides of a defaultTopicClass to the single producer`() {
            // What is to be tested? Whether <defaultTopicClass>AUDIT
            //   upgrades the default stream itself: one producer, AUDIT
            //   client id, mandatory overrides enforced, violation warned.
            // How will the test case be deemed successful and why? Successful
            //   if exactly one producer exists, its client.id carries the
            //   -audit suffix, acks was forced to all despite the operator's
            //   acks=1, and the override warning reached the status manager.
            // Why is it important to test this test case? This is the direct
            //   path for compliance-grading the default stream - previously
            //   only reachable via a synthetic marker mapping that left a
            //   dormant TECHNICAL producer running.

            // Given
            val factory = TestProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.ACKS_CONFIG}=1
                        """.trimIndent(),
                )
            appender.topicMapping.defaultTopicClass = "AUDIT"

            // When
            appender.start()

            // Then: single AUDIT producer with enforced overrides
            assertThat(appender.isStarted).isTrue()
            assertThat(factory.createdProducers).hasSize(1)
            assertThat(factory.createdWithProperties.single())
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "tabellarium-test-service-audit")
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Mandatory override applied") && it.contains("'all'") }
        }

        @Test
        fun `should instantiate one producer per class activated by mappings`() {
            // What is to be tested? Whether a <mapping> with a non-default
            //   topic class activates a second producer next to the
            //   TECHNICAL fallback producer - the multi-class model
            //   reached through the real configuration surface.
            // How will the test case be deemed successful and why? Successful
            //   if exactly two producers exist (AUDIT + TECHNICAL) and the
            //   AUDIT one carries the per-class client.id suffix. This
            //   pins the activation path end-to-end at the appender level.
            // Why is it important to test this test case? The per-class
            //   producer model was previously verifiable only by driving
            //   internals directly; with the surface shipped, the
            //   activation itself is part of the operator contract.

            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then: AUDIT + TECHNICAL fallback producers
            assertThat(factory.createdProducers).hasSize(2)
            assertThat(factory.createdWithProperties)
                .anyMatch { it[ProducerConfig.CLIENT_ID_CONFIG] == "tabellarium-test-service-audit" }
            assertThat(factory.createdWithProperties)
                .anyMatch { it[ProducerConfig.CLIENT_ID_CONFIG] == "tabellarium-test-service-technical" }
        }

        @Test
        fun `should emit no override warning with the minimal configuration`() {
            // Given: the minimal configuration (everything TECHNICAL, no
            //   mandates) - the baseline the previous test's setup departs from
            val appender = newAppender()

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("Mandatory override applied") }
        }
    }

    @Nested
    inner class `Transport security signalling` {
        @Test
        fun `should warn when a compliance-graded class ships over cleartext`() {
            // What is to be tested? Whether an active AUDIT class combined
            //   with an unset (i.e. PLAINTEXT) security.protocol produces a
            //   startup warning.
            // How will the test case be deemed successful and why? Successful
            //   if a status warning names the graded class and the
            //   security.protocol setting. The appender enforces durability
            //   for graded classes and warns when overruling the operator;
            //   staying silent about cleartext transport would be the one
            //   compliance dimension without a signal.
            // Why is it important to test this test case? Audit records that
            //   travel unencrypted are readable and tamperable by anyone on
            //   the network path - the operator has to learn that from the
            //   startup log, since the appender deliberately does not (and
            //   cannot) enforce TLS itself.

            // Given: an AUDIT mapping, no security.protocol configured
            val appender = newAppender()
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .anyMatch {
                    it.contains("cleartext transport") &&
                        it.contains("AUDIT") &&
                        it.contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
                }
        }

        @Test
        fun `should not warn when the graded class is configured for SSL`() {
            // Given: the same AUDIT mapping, but SSL configured
            val appender =
                newAppender(
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${CommonClientConfigs.SECURITY_PROTOCOL_CONFIG}=SSL
                        """.trimIndent(),
                )
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("cleartext transport") }
        }

        @Test
        fun `should not warn for the minimal configuration without graded classes`() {
            // What is to be tested? Whether the warning is scoped to classes
            //   that actually carry compliance mandates. TECHNICAL has none,
            //   so a plain default-topic deployment must stay quiet.
            // How will the test case be deemed successful and why? Successful
            //   if no cleartext warning appears for the minimal (TECHNICAL-
            //   only) configuration, which is the common case.
            // Why is it important to test this test case? A warning that
            //   fires for every deployment would be trained away within a
            //   week, and would be gone when it finally matters.

            // Given: minimal configuration, no security.protocol
            val appender = newAppender()

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("cleartext transport") }
        }
    }

    @Nested
    inner class `Pipeline failure reporting` {
        @Test
        fun `should withhold the cause and stack trace when debug is disabled`() {
            // What is to be tested? Whether a producer-construction failure
            //   reports only the exception type by default, keeping the
            //   Kafka-authored message and the stack trace behind <debug>.
            // How will the test case be deemed successful and why? Successful
            //   if the error names the exception class and points at the
            //   debug flag, while the exception's own message text does not
            //   appear. That text is built by the Kafka client from
            //   credential-bearing configuration and is not under this
            //   appender's control.
            // Why is it important to test this test case? SECURITY.md names
            //   credential leakage through status output as an in-scope
            //   concern for this appender; this is the one path where
            //   foreign text reaches the status manager.

            // Given: a factory that fails with a message standing in for
            //   configuration-derived text
            val failingFactory =
                ProducerFactory { _ -> throw IllegalStateException("secret-bearing-detail") }
            val appender = newAppender(producerFactory = failingFactory, debug = false)

            // When
            appender.start()

            // Then: refused to start, type reported, cause withheld
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Failed to build KafkaAppender pipeline") && it.contains("IllegalStateException") }
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("secret-bearing-detail") }
        }

        @Test
        fun `should include the cause when debug is enabled`() {
            // Given
            val failingFactory =
                ProducerFactory { _ -> throw IllegalStateException("diagnostic-detail") }
            val appender = newAppender(producerFactory = failingFactory, debug = true)

            // When
            appender.start()

            // Then: operators who opt in get the full cause
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("diagnostic-detail") }
        }
    }

    @Nested
    inner class `Debug diagnostics` {
        @Test
        fun `should emit the diagnostics note when debug is enabled`() {
            // What is to be tested? Whether enabling <debug>true</debug>
            //   surfaces the explicit note that informs operators the flag
            //   has no per-event effect.
            // How will the test case be deemed successful and why? Successful
            //   if a status message says the flag is startup-only and
            //   recommends removing it. This pins down the operator-facing
            //   guidance.
            // Why is it important to test this test case? Operators must
            //   know that the flag affects only startup diagnostics; without
            //   the note they would either keep the flag (no harm done, but
            //   configuration debt accumulates) or expect per-record debug
            //   output that never comes.

            // Given
            val appender = newAppender(debug = true)

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("no per-event effect") && it.contains("removing") }
        }

        @Test
        fun `should not emit the diagnostics note when debug is disabled`() {
            // Given
            val appender = newAppender(debug = false)

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("Debug mode enabled") }
        }

        @Test
        fun `should list the generated producer settings when debug is enabled`() {
            // What is to be tested? Whether debug mode surfaces the values
            //   the appender GENERATED on top of the operator's base
            //   configuration - the derived client.id and the class
            //   overrides that actually took effect - without repeating
            //   values the operator supplied themselves.
            // How will the test case be deemed successful and why? Successful
            //   if the generated-settings line names the derived client.id
            //   and the acks default, but neither the operator's own
            //   bootstrap.servers nor their explicitly set linger.ms
            //   (an operator value is not a generated value, even when a
            //   class default exists for the same key).
            // Why is it important to test this test case? Operators debug
            //   delivery issues by asking "what did the appender change?";
            //   repeating their own configuration would bury the answer -
            //   and could leak credentials into status output.

            // Given: operator sets bootstrap and overrides linger.ms
            val appender =
                newAppender(
                    debug = true,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.LINGER_MS_CONFIG}=10
                        """.trimIndent(),
                )

            // When
            appender.start()

            // Then
            val generatedLine =
                appender.statusMessages().single { it.startsWith("Generated producer settings [technical]") }
            assertThat(generatedLine)
                .contains("client.id=tabellarium-test-service-technical")
                .contains("acks=1")
                .doesNotContain("bootstrap.servers")
                .doesNotContain("linger.ms")
        }

        @Test
        fun `should never repeat operator-supplied credentials in the debug output`() {
            // What is to be tested? Whether the generated-settings output
            //   is credential-safe: values from <kafkaProducerProperties>
            //   (which routinely carries keystore passwords and JAAS
            //   configs) must not appear in any status message.
            // How will the test case be deemed successful and why? Successful
            //   if no status message contains the secret value. The
            //   diff-against-base construction guarantees this; the test
            //   pins it against a refactor that switches back to dumping
            //   the full effective configuration.
            // Why is it important to test this test case? SECURITY.md names
            //   credential leakage through status messages as an explicit
            //   concern for this appender.

            // Given
            val appender =
                newAppender(
                    debug = true,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ssl.keystore.password=extremely-secret-123
                        """.trimIndent(),
                )

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("extremely-secret-123") }
        }

        @Test
        fun `should not emit generated producer settings when debug is disabled`() {
            // Given
            val appender = newAppender(debug = false)

            // When
            appender.start()

            // Then
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("Generated producer settings") }
        }
    }

    @Nested
    inner class `Producer self-logging guard` {
        @Test
        fun `should ignore events from threads whose name contains a producer client id`() {
            // What is to be tested? Whether a log event originating from one
            //   of the appender's own Kafka producer threads (the client
            //   names them after the client.id) is dropped instead of being
            //   routed back into that producer.
            // How will the test case be deemed successful and why? Successful
            //   if neither the producer nor the fallback sees the event.
            //   This pins down the feedback-loop guard: producer-internal
            //   logging must never be shipped through the producer itself.
            // Why is it important to test this test case? Under broker
            //   trouble the Kafka client logs from its network thread on
            //   every failure; feeding those events back into the failing
            //   producer amplifies load and floods the fallback exactly when
            //   the system is least able to absorb it.

            // Given
            val encoder = TestEncoder()
            val factory = TestProducerFactory()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = encoder,
                    producerFactory = factory,
                    fallback = fallback,
                    component = "checkout-service",
                )
            appender.start()

            // When: an event from the producer's own network thread
            appender.doAppend(
                newTestLoggingEvent(
                    message = "Connection to node -1 could not be established",
                    threadName = "kafka-producer-network-thread | tabellarium-checkout-service-technical",
                ),
            )

            // Then: fully ignored - not encoded, not sent, not in fallback
            assertThat(encoder.encodedEvents).isEmpty()
            assertThat(factory.createdProducers[0].history()).isEmpty()
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should deliver events from ordinary threads`() {
            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent(threadName = "http-nio-8080-exec-3"))

            // Then
            assertThat(factory.createdProducers[0].history()).hasSize(1)
        }

        @Test
        fun `should deliver events from application threads whose name merely contains a client id`() {
            // What is to be tested? Whether the guard is anchored to
            //   Kafka's producer-network-thread naming scheme instead of
            //   a bare substring match. An operator may pin a short,
            //   generic client.id; application threads whose names happen
            //   to contain it must still be logged.
            // How will the test case be deemed successful and why? Successful
            //   if an event from a thread named after the (short) client.id
            //   plus a suffix is delivered to the producer. With the old
            //   substring match this event would have been silently
            //   swallowed - the worst failure mode for a logging component.
            // Why is it important to test this test case? Silent log loss
            //   from unrelated threads is nearly undiagnosable in
            //   production; the anchored match is the guarantee that the
            //   guard only ever suppresses the producer's own logging.

            // Given: operator pins the generic client.id "app"
            val factory = TestProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.CLIENT_ID_CONFIG}=app
                        """.trimIndent(),
                )
            appender.start()

            // When: an ordinary application thread containing "app" logs
            appender.doAppend(newTestLoggingEvent(threadName = "app-worker-1"))
            // And: the actual producer network thread logs
            appender.doAppend(
                newTestLoggingEvent(threadName = "kafka-producer-network-thread | app"),
            )

            // Then: the application thread's event was delivered; the
            //   producer thread's event was suppressed
            assertThat(factory.createdProducers[0].history()).hasSize(1)
        }

        @Test
        fun `should not drop events when the operator sets a blank client id`() {
            // What is to be tested? Whether a blank operator-supplied
            //   client.id is excluded from the guard set.
            // How will the test case be deemed successful and why? Successful
            //   if an ordinary event is still delivered. A blank id in the
            //   guard would be contained in every thread name and silently
            //   drop all logging.
            // Why is it important to test this test case? `client.id=` (empty
            //   value) is accepted by the properties parser; without the
            //   blank-filter this valid-if-unusual configuration would turn
            //   the appender into a black hole.

            // Given
            val factory = TestProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.CLIENT_ID_CONFIG}=
                        """.trimIndent(),
                )
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent(threadName = "main"))

            // Then
            assertThat(factory.createdProducers[0].history()).hasSize(1)
        }
    }

    @Nested
    inner class `Hot path` {
        @Test
        fun `should encode and send the event when appended`() {
            // Given
            val encoder = TestEncoder()
            val factory = TestProducerFactory()
            val appender = newAppender(encoder = encoder, producerFactory = factory)
            appender.start()

            // When
            val event = newTestLoggingEvent(message = "hello kafka")
            appender.doAppend(event)

            // Then
            assertThat(encoder.encodedEvents).hasSize(1)
            assertThat(factory.createdProducers[0].history()).hasSize(1)
            assertThat(factory.createdProducers[0].history()[0].topic())
                .isEqualTo("default.topic")
        }

        @Test
        fun `should not invoke the fallback when the send succeeds`() {
            // Given
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent())

            // Then
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should not run any append logic when the appender is not started`() {
            // What is to be tested? Whether the AppenderBase isStarted gate
            //   keeps the hot path from running before start() or after a
            //   failed start. This is the safety net for the lateinit
            //   pipeline fields.
            // How will the test case be deemed successful and why? Successful
            //   if appending without prior start() leaves the producer
            //   untouched. Logback's UnsynchronizedAppenderBase.doAppend
            //   short-circuits when isStarted is false; if a regression
            //   removed that gate, the lateinit fields would throw
            //   UninitializedPropertyAccessException.
            // Why is it important to test this test case? A regression in
            //   the lifecycle gate would manifest as confusing test
            //   failures in production, where Logback's autoconfiguration
            //   path occasionally invokes appenders before fully starting
            //   them.

            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory)
            // Note: start() not called

            // When
            appender.doAppend(newTestLoggingEvent())

            // Then: no producer was ever created (start never ran)
            assertThat(factory.createdProducers).isEmpty()
        }
    }

    @Nested
    inner class `Hot path failure handling` {
        @Test
        fun `should route to the fallback appender when the encoder throws`() {
            // Given
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = fallback,
                )
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent(message = "encoder will throw"))

            // Then
            assertThat(fallback.events).hasSize(1)
            assertThat(fallback.events[0].message).isEqualTo("encoder will throw")
        }

        @Test
        fun `should log only the first hot path error to prevent log storms`() {
            // What is to be tested? Whether repeated hot-path failures are
            //   summarized as a single status-manager error, rather than
            //   one error per failing event.
            // How will the test case be deemed successful and why? Successful
            //   if after 100 failing appends, exactly one "Hot path error"
            //   message exists in the status manager. This pins down the
            //   AtomicBoolean-guarded one-shot error path.
            // Why is it important to test this test case? Without the
            //   guard, a permanently broken encoder would generate one
            //   status error per log event - flooding any operator
            //   dashboard that surfaces Logback status as health signal,
            //   and in extreme cases consuming significant CPU just on
            //   the error-formatting path.

            // Given
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = fallback,
                )
            appender.start()

            // When
            repeat(100) {
                appender.doAppend(newTestLoggingEvent())
            }

            // Then: all 100 events reached the fallback
            assertThat(fallback.events).hasSize(100)
            // But only one error was logged
            assertThat(appender.statusMessages().filter { it.contains("Hot path error") })
                .hasSize(1)
        }

        @Test
        fun `should not throw when both the hot path and the fallback fail`() {
            // Given: a fallback that also throws
            val fallbackContext = LoggerContext()
            val throwingFallback =
                object : AppenderBase<ILoggingEvent>() {
                    init {
                        context = fallbackContext
                        start()
                    }

                    override fun append(event: ILoggingEvent): Unit = throw RuntimeException("fallback also broken")
                }
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = throwingFallback,
                )
            appender.start()

            // When / Then: must not throw out of append
            appender.doAppend(newTestLoggingEvent())
        }
    }

    @Nested
    inner class `Shutdown` {
        @Test
        fun `should close all producers when stopping`() {
            // Given
            val factory = TestProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.start()

            // When
            appender.stop()

            // Then
            assertThat(factory.createdProducers[0].closed()).isTrue()
        }

        @Test
        fun `should stop the encoder when stopping`() {
            // Given
            val encoder = TestEncoder()
            val appender = newAppender(encoder = encoder)
            appender.start()

            // When
            appender.stop()

            // Then
            assertThat(encoder.isStarted).isFalse()
        }

        @Test
        fun `should not throw when stopping an appender that never started`() {
            // What is to be tested? Whether stop() is safe to call on an
            //   appender whose start() either was not called or failed.
            //   The lateinit fields are uninitialized in that case.
            // How will the test case be deemed successful and why? Successful
            //   if stop() returns normally. The this::producerRegistry.isInitialized
            //   guard is the contract under test.
            // Why is it important to test this test case? Logback's context
            //   shutdown invokes stop() on all registered appenders. If our
            //   stop() threw UninitializedPropertyAccessException on a
            //   never-started appender, it would break the orderly shutdown
            //   of other appenders too.

            // Given: an appender that was never started (e.g. config invalid)
            val appender = newAppender(encoder = null)
            appender.start() // will refuse to start
            assertThat(appender.isStarted).isFalse()

            // When / Then: must not throw
            appender.stop()
        }

        @Test
        fun `should stop the attached fallback appender when stopping the KafkaAppender`() {
            // What is to be tested? Whether stop() releases the fallback
            //   appender's resources (file handles, worker threads). This
            //   guards against a resource leak: the AppenderAttachable
            //   contract requires detachAndStopAllAppenders to run on
            //   shutdown.
            // How will the test case be deemed successful and why? Successful
            //   if the fallback appender reports isStarted=false after the
            //   KafkaAppender's stop() returns. This pins the lifecycle
            //   propagation that operators need for clean Kubernetes
            //   shutdowns.
            // Why is it important to test this test case? A FileAppender
            //   left in started state holds an open file handle until the
            //   JVM exits, which on graceful pod shutdowns means the file
            //   is not flushed/closed and the last several seconds of
            //   logs may be lost.

            // Given
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.start()
            assertThat(fallback.isStarted).isTrue()

            // When
            appender.stop()

            // Then
            assertThat(fallback.isStarted).isFalse()
        }
    }

    @Nested
    inner class `Hot path concurrency` {
        /**
         * Factory wrapping each MockProducer in a synchronizing delegate:
         * MockProducer itself is not thread-safe (its history is a plain
         * list), so without the wrapper the test harness would race
         * internally and hide or fabricate failures. The appender-side
         * code under test still runs fully concurrently.
         */
        private inner class SynchronizedTestProducerFactory : ProducerFactory {
            val mock =
                MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())

            override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> =
                object : Producer<ByteArray, ByteArray> by mock {
                    override fun send(
                        record: ProducerRecord<ByteArray, ByteArray>,
                        callback: Callback?,
                    ): Future<RecordMetadata> = synchronized(mock) { mock.send(record, callback) }

                    override fun send(record: ProducerRecord<ByteArray, ByteArray>): Future<RecordMetadata> = synchronized(mock) { mock.send(record) }
                }

            fun historySize(): Int = synchronized(mock) { mock.history().size }
        }

        @Test
        fun `should deliver every event when many threads append concurrently`() {
            // What is to be tested? Whether the unsynchronized hot path
            //   (append -> route -> classify -> enrich -> send, plus the
            //   breaker/throttle gates) is actually safe under real
            //   contention - the central design claim behind extending
            //   UnsynchronizedAppenderBase.
            // How will the test case be deemed successful and why? Successful
            //   if N threads x M events all reach the producer with no
            //   exception escaping append and nothing diverted to the
            //   fallback. Races that corrupt shared state would surface
            //   as lost events, duplicate metric state, or exceptions.
            // Why is it important to test this test case? The suite's only
            //   true contention test used to cover HalfOpenThrottle alone;
            //   a regression introducing shared mutable per-event state
            //   into the hot path would have passed every other test and
            //   failed only in production under load.

            // Given: a stateless encoder - the recording TestEncoder's
            //   plain ArrayList would race under concurrent appends and
            //   sporadically divert an event to the fallback (a harness
            //   race, not an appender defect)
            val threadCount = 8
            val eventsPerThread = 250
            val factory = SynchronizedTestProducerFactory()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = StatelessEncoder(),
                    producerFactory = factory,
                    fallback = fallback,
                )
            appender.start()

            val startLatch = CountDownLatch(1)
            val doneLatch = CountDownLatch(threadCount)
            val failures = AtomicInteger(0)
            val executor = Executors.newFixedThreadPool(threadCount)
            try {
                // When: all threads hammer append simultaneously
                repeat(threadCount) { t ->
                    executor.submit {
                        try {
                            startLatch.await()
                            repeat(eventsPerThread) { i ->
                                appender.doAppend(
                                    newTestLoggingEvent(
                                        message = "t$t-e$i",
                                        threadName = "worker-$t",
                                    ),
                                )
                            }
                        } catch (_: Throwable) {
                            failures.incrementAndGet()
                        } finally {
                            doneLatch.countDown()
                        }
                    }
                }
                startLatch.countDown()
                assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue()
            } finally {
                executor.shutdownNow()
            }

            // Then: no thread failed, every event reached the producer,
            //   nothing was diverted
            assertThat(failures.get()).isZero()
            assertThat(factory.historySize()).isEqualTo(threadCount * eventsPerThread)
            assertThat(fallback.events).isEmpty()
        }
    }

    @Nested
    inner class `Asynchronous fallback dispatch` {
        @Test
        fun `should deliver fallback events through the real worker thread`() {
            // What is to be tested? The production-representative
            //   asynchronous appender -> dispatcher -> worker -> fallback
            //   chain, which every other appender-level test bypasses via
            //   the synchronous test hook.
            // How will the test case be deemed successful and why? Successful
            //   if events diverted by a failing encoder arrive at the
            //   fallback appender asynchronously (bounded polling) and a
            //   subsequent stop() completes cleanly, draining the queue.
            // Why is it important to test this test case? A regression in
            //   the dispatcher wiring (worker not started, stop-ordering
            //   closing the dispatcher before the registry) would be
            //   invisible to all synchronous-mode tests.

            // Given: async dispatcher (the production path)
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = fallback,
                )
            appender.useSynchronousFallbackForTests = false
            appender.start()

            // When: events fail in the hot path and divert to the fallback
            repeat(5) { appender.doAppend(newTestLoggingEvent(message = "async-$it")) }

            // Then: the worker thread delivers them asynchronously
            pollUntil { fallback.events.size == 5 }

            // And: stop() drains and shuts down cleanly, with no drop warning
            appender.stop()
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("dropped") }
        }
    }

    @Nested
    inner class `Metrics lifecycle` {
        @Test
        fun `should deregister its meters from the registry on stop`() {
            // What is to be tested? Whether stop() removes everything
            //   bindMeterRegistry registered - otherwise every Logback
            //   reconfiguration cycle leaks meters and leaves gauges
            //   reading the previous (closed) dispatcher's queue.
            // How will the test case be deemed successful and why? Successful
            //   if after stop() the registry no longer contains any
            //   kafka.appender.* meter. This pins the register/deregister
            //   symmetry of the metrics lifecycle.
            // Why is it important to test this test case? Meter leaks are
            //   invisible in tests and single-start deployments; they
            //   surface as slowly growing scrape payloads and misleading
            //   dashboards only after reload cycles in production.

            // Given: a bound appender with a fallback (so queue gauges exist)
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.start()
            appender.bindMeterRegistry(registry)
            assertThat(registry.meters)
                .anyMatch { it.id.name.startsWith("kafka.appender.") }

            // When
            appender.stop()

            // Then: all appender meters are gone
            assertThat(registry.meters)
                .noneMatch { it.id.name.startsWith("kafka.appender.") }
        }

        @Test
        fun `should not duplicate meters when bound twice`() {
            // Given
            val registry = SimpleMeterRegistry()
            val appender = newAppender()
            appender.start()

            // When: bound twice against the same registry
            appender.bindMeterRegistry(registry)
            val countAfterFirst = registry.meters.count { it.id.name.startsWith("kafka.appender.") }
            appender.bindMeterRegistry(registry)
            val countAfterSecond = registry.meters.count { it.id.name.startsWith("kafka.appender.") }

            // Then: the second bind replaced, not duplicated
            assertThat(countAfterSecond).isEqualTo(countAfterFirst)
            appender.stop()
        }
    }

    @Nested
    inner class `Appender-ref handling` {
        @Test
        fun `should accept the first appender added via addAppender as the fallback`() {
            // What is to be tested? Whether the AppenderAttachable contract
            //   is wired such that Joran's <appender-ref> mechanism reaches
            //   the fallback slot.
            // How will the test case be deemed successful and why? Successful
            //   if calling addAppender(...) stores the appender as the
            //   fallback and a subsequent failed send routes the event
            //   through it. This pins down the path for the
            //   standard <appender-ref ref="..."/> Logback syntax.
            // Why is it important to test this test case? Without this,
            //   users could write the standard Logback XML and find that
            //   <appender-ref> silently has no effect - the worst kind of
            //   misconfiguration because nothing breaks loudly.

            // Given
            val fallback = RecordingAppender()
            val appender = newAppender(encoder = ThrowingEncoder())
            appender.addAppender(fallback)
            appender.start()

            // When: the hot path fails
            appender.doAppend(newTestLoggingEvent(message = "fallback me"))

            // Then: the fallback received the event
            assertThat(fallback.events).hasSize(1)
            assertThat(appender.fallbackAppender).isSameAs(fallback)
        }

        @Test
        fun `should warn and ignore subsequent appenders when more than one is attached`() {
            // What is to be tested? Whether the single-slot semantics is
            //   enforced when multiple <appender-ref> elements appear in
            //   the configuration.
            // How will the test case be deemed successful and why? Successful
            //   if the first addAppender call sets the fallback, the
            //   second emits a status warning identifying the ignored
            //   appender by name, and the fallback slot still holds the
            //   first appender. This pins down both the policy decision
            //   ("first wins") and its operator-visible signal.
            // Why is it important to test this test case? A silent acceptance
            //   of multiple appender-refs would leave operators unaware that
            //   their configuration intent is being overruled - exactly the
            //   class of silent compliance gap this whole refactor targets.

            // Given
            val first = RecordingAppender().apply { name = "FIRST" }
            val second = RecordingAppender().apply { name = "SECOND" }
            val appender = newAppender()

            // When
            appender.addAppender(first)
            appender.addAppender(second)

            // Then: first appender wins, warning identifies the ignored one
            assertThat(appender.fallbackAppender).isSameAs(first)
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("single fallback") && it.contains("SECOND") }
        }

        @Test
        fun `should report the attached appender via iteratorForAppenders and getAppender`() {
            // Given
            val fallback = RecordingAppender().apply { name = "MY_FALLBACK" }
            val appender = newAppender()
            appender.addAppender(fallback)

            // When / Then
            assertThat(appender.iteratorForAppenders().asSequence().toList())
                .containsExactly(fallback)
            assertThat(appender.getAppender("MY_FALLBACK")).isSameAs(fallback)
            assertThat(appender.getAppender("UNKNOWN")).isNull()
            assertThat(appender.isAttached(fallback)).isTrue()
        }

        @Test
        fun `should detach the appender and free the slot when detachAppender is called`() {
            // Given
            val fallback = RecordingAppender().apply { name = "FALLBACK" }
            val appender = newAppender()
            appender.addAppender(fallback)

            // When: detach by instance
            val detached = appender.detachAppender(fallback)

            // Then: slot is empty, detach reported success, slot is reusable
            assertThat(detached).isTrue()
            assertThat(appender.fallbackAppender).isNull()

            // And: a new appender can now be attached
            val replacement = RecordingAppender().apply { name = "REPLACEMENT" }
            appender.addAppender(replacement)
            assertThat(appender.fallbackAppender).isSameAs(replacement)
        }
    }
}
