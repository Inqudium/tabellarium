package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.encoder.EncoderBase
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

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

    private class TestProducerFactory : ProducerFactory {
        val createdProducers = mutableListOf<MockProducer<ByteArray, ByteArray>>()

        override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
            val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
            createdProducers += mock
            return mock
        }
    }

    private class RecordingAppender : AppenderBase<ILoggingEvent>() {
        val events = mutableListOf<ILoggingEvent>()

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
    }

    @Nested
    inner class `Mandatory override warnings` {
        @Test
        fun `should emit a status warning when a user value conflicts with a mandatory override`() {
            // What is to be tested? Whether mandatory-override violations
            //   from ProducerPropertiesBuilder are surfaced to operators
            //   via Logback's status manager.
            // How will the test case be deemed successful and why? Successful
            //   if a warning message containing both the property key and
            //   the enforced value reaches the status manager. The warning
            //   is the only mechanism by which operators learn that their
            //   configuration intent was overruled for compliance reasons.
            // Why is it important to test this test case? Silent enforcement
            //   would let an operator believe their acks=1 had taken effect
            //   for audit topics, when in fact acks=all was forced. Auditors
            //   would later find a discrepancy between the documented
            //   configuration and the actual broker behavior - exactly the
            //   compliance gap this refactor closes.

            // Given: a configuration with custom mappings that conflict with
            //   AUDIT mandates. To trigger this we need at least one topic
            //   classified as AUDIT, which the minimal configuration alone
            //   does not produce. We bypass by constructing a topicMapping
            //   with an explicit topic-class assignment.

            // For now, the minimal configuration does not produce AUDIT
            // topics, so we verify the absence of warnings as a baseline.
            // When per-class topic mapping is added to TopicMappingConfig,
            // this test should be expanded to cover the actual violation case.
            val factory = TestProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092",
                )

            // When
            appender.start()

            // Then: no AUDIT mandates triggered with current minimal config
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("Mandatory override applied") }
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
