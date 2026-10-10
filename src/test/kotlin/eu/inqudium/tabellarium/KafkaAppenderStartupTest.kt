package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Start and restart semantics of [KafkaAppender] beyond plain
 * validation: how a failing pipeline construction is reported and
 * rolled back (encoder, producers, guard, fallback worker), the debug
 * diagnostics emitted at start, and the one-shot lifecycle (no restart
 * after stop, repeated start ignored).
 */
class KafkaAppenderStartupTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Pipeline failure reporting` {
        @Test
        fun `should release the encoder and rethrow when the producer factory fails with an Error`() {
            // What is to be tested? Whether start() keeps its rollback
            //   contract for an Error from the pipeline construction: the
            //   encoder start() started first must be released, and the
            //   Error must propagate unchanged instead of being reported
            //   as a configuration problem.
            // How will the test case be deemed successful and why? Successful
            //   if start() throws the very LinkageError, the appender is
            //   not started and the encoder is stopped. Before the fix only
            //   Exception was handled, so the encoder stayed started
            //   (DEFECT_ANALYSIS-2026-10-10T13-46-50, finding 4).
            // Why is it important to test this test case? An encoder can
            //   hold streams and buffers; leaking it during an already
            //   degraded startup violates the documented lifecycle.

            // Given
            val failure = LinkageError("synthetic linkage failure")
            val encoder = RecordingEncoder()
            val appender = newAppender(encoder = encoder, producerFactory = ProducerFactory { _ -> throw failure })

            // When / Then
            assertThatThrownBy { appender.start() }.isSameAs(failure)
            assertThat(appender.isStarted).isFalse()
            assertThat(encoder.isStarted).isFalse()
        }

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

            // Then: refused to start, class and cause type reported, cause withheld
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch {
                    it.contains("Failed to build KafkaAppender pipeline") &&
                        it.contains("Kafka producer for TECHNICAL could not be constructed") &&
                        it.contains("java.lang.IllegalStateException") &&
                        it.contains("<debug>true</debug>")
                }
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("secret-bearing-detail") }
        }

        @Test
        fun `should include the cause when debug is enabled`() {
            // What is to be tested? Whether <debug>true</debug> opts the operator
            //   into the full pipeline-build failure message - the exception text
            //   that the non-debug path deliberately withholds.
            // How will the test case be deemed successful and why? Successful
            //   if the status error contains the injected "diagnostic-detail"
            //   cause. Together with the previous test this pins the two-tier
            //   reporting: type only by default, message and stack trace behind
            //   the explicit flag.
            // Why is it important to test this test case? The withheld message
            //   is usually the only clue to a failing producer configuration;
            //   if the opt-in did not surface it, operators would have no way at
            //   all to diagnose a refused start.

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
            // What is to be tested? Whether the startup diagnostics block
            //   (including the "Debug mode enabled" note) is emitted only when
            //   <debug> is actually set - the default start is silent about it.
            // How will the test case be deemed successful and why? Successful
            //   if no "Debug mode enabled" status message exists after a
            //   default start. This pins the gate around emitDebugDiagnostics().
            // Why is it important to test this test case? The diagnostics list
            //   active classes, fallback wiring and generated producer settings;
            //   emitting them unconditionally would clutter every startup log
            //   and defeat the purpose of the opt-in.

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
            // What is to be tested? Whether the per-class "Generated producer
            //   settings" lines are part of the debug-only diagnostics and never
            //   appear on a default start.
            // How will the test case be deemed successful and why? Successful
            //   if no status message starts with "Generated producer settings"
            //   after a start with debug=false. This pins the gate for the one
            //   diagnostics line that enumerates effective producer properties.
            // Why is it important to test this test case? Those lines are
            //   credential-safe by construction, but they still describe the
            //   producer configuration in detail; they must stay behind the
            //   explicit opt-in rather than land in every deployment's status
            //   output.

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
    inner class `Startup failure rollback` {
        @Test
        fun `should not build the pipeline when the encoder fails to start`() {
            // What is to be tested? Whether encoder.start() runs before
            //   any pipeline resource is allocated, so a failing encoder
            //   aborts the startup with nothing to leak.
            // How will the test case be deemed successful and why? Successful
            //   if the appender refuses to start, no producer was ever
            //   created, and the status manager names the encoder failure.
            // Why is it important to test this test case? Before the fix,
            //   encoder.start() ran last - a throwing custom encoder left
            //   producers, breakers, and daemon workers behind that only
            //   an external stop() could ever reach.

            // Given: an encoder whose start() throws
            val factory = RecordingProducerFactory()
            val throwingStartEncoder =
                object : EncoderBase<ILoggingEvent>() {
                    override fun start(): Unit = throw IllegalStateException("simulated encoder start failure")

                    override fun encode(event: ILoggingEvent): ByteArray = ByteArray(0)

                    override fun headerBytes(): ByteArray = ByteArray(0)

                    override fun footerBytes(): ByteArray = ByteArray(0)
                }
            val appender = newAppender(encoder = throwingStartEncoder, producerFactory = factory)

            // When
            appender.start()

            // Then: refused to start, nothing allocated
            assertThat(appender.isStarted).isFalse()
            assertThat(factory.createdProducers).isEmpty()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Failed to start encoder") }
        }

        @Test
        fun `should close already-created producers when pipeline construction fails after them`() {
            // What is to be tested? The transactional startup: a failure
            //   AFTER producer creation (here: circuit-breaker wiring)
            //   must roll the created producers back instead of leaving
            //   them - network threads, buffers, MBeans - orphaned behind
            //   a never-started appender.
            // How will the test case be deemed successful and why? Successful
            //   if the appender refuses to start and every producer the
            //   factory created has been closed again.
            // Why is it important to test this test case? Before the fix,
            //   the catch path only reported the failure; partially built
            //   resources survived until some external caller happened to
            //   invoke stop() - which for a failed configuration nobody
            //   does.

            // Given: breaker wiring that fails after the registry exists
            val factory = RecordingProducerFactory()
            val failingBreakerRegistry =
                object : CircuitBreakerRegistry by ResilientMessageSender.defaultCircuitBreakerRegistry() {
                    override fun circuitBreaker(name: String): CircuitBreaker = throw IllegalStateException("simulated breaker wiring failure")
                }
            val appender = newAppender(producerFactory = factory)
            appender.circuitBreakerRegistry = failingBreakerRegistry

            // When
            appender.start()

            // Then: refused to start, created producers rolled back
            assertThat(appender.isStarted).isFalse()
            assertThat(factory.createdProducers).isNotEmpty
            assertThat(factory.createdProducers).allMatch { it.closed() }
        }

        @Test
        fun `should release the self-logging guard and the fallback worker when pipeline construction fails`() {
            // What is to be tested? The rest of the rollback in
            //   KafkaTransport.open: a failure after the guard and the
            //   fallback dispatcher exist (breaker wiring) must close those
            //   too, not only the producers.
            // How will the test case be deemed successful and why? Successful
            //   if, after the refused start, no fallback worker started by
            //   this start() is still alive, and the guard has left the
            //   process-wide registry: a second appender with the same
            //   component derives the same client id, and the start-up
            //   diagnostic finds it held by no other live instance. A leaked
            //   guard would count the id twice and be reported as shared.
            //   The producer-only assertions of the neighbouring tests stay
            //   green when either is dropped from the rollback
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 4).
            // Why is it important to test this test case? A leaked guard
            //   keeps its client ids in the process-wide registry, so a later
            //   appender's echo filter drops foreign events; a leaked worker
            //   is a thread per failed (re)configuration.

            // Given: a fallback appender and breaker wiring that fails after
            //   the guard and the fallback dispatcher exist
            val failingBreakerRegistry =
                object : CircuitBreakerRegistry by ResilientMessageSender.defaultCircuitBreakerRegistry() {
                    override fun circuitBreaker(name: String): CircuitBreaker = throw IllegalStateException("simulated breaker wiring failure")
                }
            val appender = newAppender(fallback = RecordingAppender())
            appender.circuitBreakerRegistry = failingBreakerRegistry
            val fallbackWorkersBefore = liveThreadsNamed(FallbackDispatcher.THREAD_NAME)

            // When
            appender.start()

            // Then: refused, no new fallback worker alive
            assertThat(appender.isStarted).isFalse()
            assertThat(liveThreadsNamed(FallbackDispatcher.THREAD_NAME) - fallbackWorkersBefore).isEmpty()

            // And: the guard left the registry - a successor with the same
            //   component holds the derived client id alone
            val successorFactory = RecordingProducerFactory()
            val successor = newAppender(producerFactory = successorFactory)
            successor.start()
            assertThat(successor.isStarted).isTrue()
            val derivedClientId = successorFactory.createdWithProperties.single().getValue(ProducerConfig.CLIENT_ID_CONFIG)
            assertThat(derivedClientId).isEqualTo("tabellarium-test-service-technical")
            assertThat(SelfLoggingGuard.clientIdsHeldByOtherInstances(setOf(derivedClientId))).isEmpty()
            assertThat(successor.statusMessages()).noneMatch { it.contains("is also used by another running KafkaAppender") }
        }

        private fun liveThreadsNamed(name: String): Set<Thread> = Thread.getAllStackTraces().keys.filterTo(HashSet()) { it.name == name && it.isAlive }
    }

    @Nested
    inner class `No restart` {
        @Test
        fun `should refuse to start again after stop`() {
            // What is to be tested? The lifecycle decision of ADR-0004: a
            //   stopped appender is not restarted - start() after stop()
            //   is refused with a named error, no new producers are
            //   created, and the appender stays stopped.
            // How will the test case be deemed successful and why? Successful
            //   if after stop() a second start() leaves isStarted false,
            //   the producer count unchanged, and a status error naming
            //   the ADR and the alternative (a new instance) in the
            //   status manager; an event appended afterwards goes nowhere.
            // Why is it important to test this test case? Logback replaces
            //   appender instances on reconfiguration instead of restarting
            //   them; a same-instance restart only ever comes from
            //   application code, and silently rebuilding half a pipeline
            //   for it (the pre-ADR behavior) lost the fallback path.

            // Given: a started-then-stopped appender
            val factory = RecordingProducerFactory()
            val fallback = RecordingAppender()
            val appender = newAppender(producerFactory = factory, fallback = fallback)
            appender.start()
            appender.stop()
            val producersAfterFirstLife = factory.createdProducers.size

            // When: started again
            appender.start()
            appender.doAppend(newTestLoggingEvent(message = "after refusal"))

            // Then: refused, nothing rebuilt, nothing delivered anywhere
            assertThat(appender.isStarted).isFalse()
            assertThat(factory.createdProducers).hasSize(producersAfterFirstLife)
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("cannot be started again") && it.contains("ADR-0004") }
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should stay startable after a stop that preceded any successful start`() {
            // What is to be tested? The boundary of ADR-0004: a stop() on an
            //   instance that never started successfully (a defensive
            //   shutdown hook, a test harness, a stop after a refused start)
            //   ends no first life and must not arm the no-restart latch.
            // How will the test case be deemed successful and why? Successful
            //   if start() after such a stop() succeeds, and no status error
            //   claims the appender "cannot be started again". Before the
            //   fix stop() latched unconditionally, so the instance was dead
            //   with a message describing a teardown that never happened
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 8).
            // Why is it important to test this test case? ADR-0004's
            //   rationale is the released resources of a first life; without
            //   one, refusing is a misleading error and an unusable instance
            //   in a corner the ADR does not cover.

            // Given: stopped before ever being started
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.stop()
            assertThat(appender.isStarted).isFalse()

            // When: started for the first time
            appender.start()

            // Then: a normal first life
            assertThat(appender.isStarted).isTrue()
            assertThat(factory.createdProducers).isNotEmpty
            assertThat(appender.statusMessages()).noneMatch { it.contains("cannot be started again") }
        }
    }

    @Nested
    inner class `Repeated start` {
        @Test
        fun `should ignore a repeated start and keep the existing pipeline`() {
            // What is to be tested? Whether start() is idempotent. A second
            //   start() used to rebuild the whole pipeline and overwrite
            //   the references to the running one - orphaning the previous
            //   producers (network threads, buffers, MBeans), which no
            //   later stop() could reach.
            // How will the test case be deemed successful and why? Successful
            //   if the second start() creates no additional producers and
            //   stop() afterwards closes every producer that was ever
            //   created - i.e. nothing is leaked.
            // Why is it important to test this test case? Logback or Spring
            //   lifecycle quirks can call start() more than once; each
            //   duplicate call would leak a full set of Kafka producers
            //   until process exit.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.start()
            val producersAfterFirstStart = factory.createdProducers.size

            // When: a second start on the already-started appender
            appender.start()

            // Then: no new pipeline was built, the duplicate is reported,
            //   and stop() closes every producer ever created
            assertThat(factory.createdProducers).hasSize(producersAfterFirstStart)
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("already started") }
            appender.stop()
            assertThat(factory.createdProducers).allSatisfy { producer ->
                assertThat(producer.closed()).isTrue()
            }
        }
    }
}
