package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * The stop sequence of [KafkaAppender]: producer and encoder release,
 * the shared-fallback ordering under a Logback context reset, teardown
 * diversions, and the status reporting for dead workers and producers
 * that fail to close.
 */
class KafkaAppenderShutdownTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Shutdown` {
        @Test
        fun `should close all producers when stopping`() {
            // What is to be tested? Whether stop() closes the producer registry
            //   and with it every Kafka producer the appender created.
            // How will the test case be deemed successful and why? Successful
            //   if the single MockProducer reports closed() after stop(). This
            //   pins the registry close step of the shutdown sequence.
            // Why is it important to test this test case? An unclosed producer
            //   keeps its network thread, buffers and MBeans alive; on every
            //   Logback reconfiguration the leaked producers would accumulate
            //   and the JVM would not exit cleanly.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.start()

            // When
            appender.stop()

            // Then
            assertThat(factory.createdProducers[0].closed()).isTrue()
        }

        @Test
        fun `should stop the encoder when stopping`() {
            // What is to be tested? Whether stop() also stops the encoder the
            //   appender started in start() - the lifecycle is symmetric.
            // How will the test case be deemed successful and why? Successful
            //   if the encoder reports isStarted == false after stop(). The
            //   encoder stop is the last step of the teardown, so it also shows
            //   the sequence ran to completion.
            // Why is it important to test this test case? Encoders may hold
            //   resources of their own (formatter pools, buffers); the appender
            //   owns the encoder it started, and skipping the stop would leak
            //   those across reconfiguration cycles.

            // Given
            val encoder = RecordingEncoder()
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
            //   appender whose start() either was not called or failed - the
            //   plan and transport fields are null in that case.
            // How will the test case be deemed successful and why? Successful
            //   if stop() returns normally: the null-transport branch of
            //   stop() (Logback state, fallback and encoder only; nothing to
            //   close) is the contract under test.
            // Why is it important to test this test case? Logback's context
            //   shutdown invokes stop() on all registered appenders. If our
            //   stop() threw on a never-started appender, it would break the
            //   orderly shutdown of other appenders too.

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

            // Then: stopped, and still inspectable through the slot
            assertThat(fallback.isStarted).isFalse()
            assertThat(appender.fallbackAppender).isSameAs(fallback)
        }

        @Test
        fun `should warn when a context reset stopped a shared fallback appender before the KafkaAppender`() {
            // What is to be tested? Whether a full-context reset that reaches a
            //   fallback appender shared with the root logger BEFORE the
            //   KafkaAppender - Logback stops each logger's appenders in attach
            //   order - is named in a status warning when the KafkaAppender
            //   then stops, instead of surfacing only as generic drops.
            // How will the test case be deemed successful and why? Successful
            //   if, after LoggerContext.reset() on a root logger with the
            //   fallback attached first, the KafkaAppender's status names the
            //   fallback as already stopped and points at the attach order.
            //   The reset runs the real Logback path, so the ordering
            //   assumption is checked against Logback itself
            //   (DEFECT_ANALYSIS-2026-10-10T17-20-10, finding 1).
            // Why is it important to test this test case? Every event the
            //   teardown diverts in that configuration is dropped although a
            //   fallback is configured - exactly at a shutdown during a broker
            //   outage, when the fallback is needed most.

            // Given: the fallback shared with root and attached ahead of the KafkaAppender
            val appender = newAppender(fallback = null)
            val context = appender.context as LoggerContext
            val sharedFallback = RecordingAppender(context).apply { name = "CONSOLE" }
            appender.addAppender(sharedFallback)
            appender.name = "KAFKA"
            appender.start()
            val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
            root.addAppender(sharedFallback)
            root.addAppender(appender)

            // When: the full-context shutdown path
            context.reset()

            // Then: the KafkaAppender stopped and named the cause
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Fallback appender 'CONSOLE'") && it.contains("already stopped") && it.contains("attach order") }
        }

        @Test
        fun `should not warn about the fallback when the KafkaAppender stops ahead of it`() {
            // What is to be tested? The counterpart: with the KafkaAppender
            //   attached before the shared fallback, the reset stops the
            //   KafkaAppender while its fallback still runs, so its teardown
            //   diversions can be written and no warning is due.
            // How will the test case be deemed successful and why? Successful
            //   if no "already stopped" status exists after the reset; the
            //   warning must stay specific to the hazardous order, or operators
            //   learn to ignore it.
            // Why is it important to test this test case? It is the documented
            //   remedy for the shared-fallback configuration.

            // Given: the KafkaAppender attached ahead of the shared fallback
            val appender = newAppender(fallback = null)
            val context = appender.context as LoggerContext
            val sharedFallback = RecordingAppender(context).apply { name = "CONSOLE" }
            appender.addAppender(sharedFallback)
            appender.start()
            val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
            root.addAppender(appender)
            root.addAppender(sharedFallback)

            // When
            context.reset()

            // Then
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages()).noneMatch { it.contains("already stopped") }
        }

        /**
         * A producer whose send parks interruptibly until [release] - or
         * until the dispatcher's phase-2 interrupt wakes it with an
         * InterruptedException, which the sender's error path routes to
         * the fallback with reason send.error. Models a send parked in
         * `max.block.ms` at shutdown. [sendEntered] tells the test when
         * the worker is inside the send.
         */
        private inner class InterruptiblyParkedProducerFactory : ProducerFactory {
            val release = CountDownLatch(1)
            val sendEntered = CountDownLatch(1)

            override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
                val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
                return object : Producer<ByteArray, ByteArray> by mock {
                    override fun send(
                        record: ProducerRecord<ByteArray, ByteArray>,
                        callback: Callback?,
                    ): Future<RecordMetadata> {
                        sendEntered.countDown()
                        release.await()
                        return synchronized(mock) { mock.send(record, callback) }
                    }
                }
            }
        }

        @Test
        fun `should drop a teardown diversion when a context reset stopped the shared fallback first`() {
            // What is to be tested? The consequence the stop-time warning
            //   describes, exercised through the real reset: with the shared
            //   fallback attached to root ahead of the KafkaAppender and one
            //   send parked when the reset arrives, the dispatcher's
            //   interrupt unblocks the send with an InterruptedException,
            //   the sender diverts the event (send.error), and the fallback
            //   dispatcher refuses the already-stopped appender - the event
            //   is counted as dropped, not written.
            // How will the test case be deemed successful and why? Successful
            //   if after LoggerContext.reset() the status carries the
            //   "Fallback dispatcher dropped 1 event(s)" warning and the
            //   shared recorder holds nothing. The warning pair of tests
            //   above proves only the diagnostic; this one proves the loss
            //   it warns about (DEFECT_ANALYSIS-2026-10-10T19-11-11,
            //   finding 11).
            // Why is it important to test this test case? This is the
            //   shutdown-during-outage case the fallback exists for; the
            //   guide's remedy rests on the claim that the order decides
            //   between drop and write.

            // Given: the hazardous order, and a send parked in the producer
            val factory = InterruptiblyParkedProducerFactory()
            val appender = newAppender(encoder = MessageBytesEncoder(), producerFactory = factory, fallback = null)
            val context = appender.context as LoggerContext
            val sharedFallback = RecordingAppender(context).apply { name = "CONSOLE" }
            appender.addAppender(sharedFallback)
            appender.name = "KAFKA"
            appender.start()
            val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
            root.addAppender(sharedFallback)
            root.addAppender(appender)
            try {
                appender.doAppend(newTestLoggingEvent(message = "parked at reset"))
                assertThat(factory.sendEntered.await(2, TimeUnit.SECONDS)).isTrue()

                // When: the full-context shutdown path
                context.reset()

                // Then: the diversion found the fallback stopped and was dropped
                assertThat(appender.isStarted).isFalse()
                assertThat(appender.statusMessages()).anyMatch { it.contains("Fallback dispatcher dropped 1 event(s)") }
                assertThat(sharedFallback.events).isEmpty()
            } finally {
                factory.release.countDown()
            }
        }

        @Test
        fun `should write a teardown diversion to the shared fallback when the KafkaAppender stops ahead of it`() {
            // What is to be tested? The remedied order with an event in
            //   flight: the reset stops the KafkaAppender while the shared
            //   fallback still runs, so the send.error diversion of the
            //   parked send is written to it before the KafkaAppender's own
            //   stop() stops the fallback.
            // How will the test case be deemed successful and why? Successful
            //   if after the reset the shared recorder holds exactly the
            //   parked event and no drop warning exists. Together with the
            //   previous test this pins that the attach order alone decides
            //   between write and drop (DEFECT_ANALYSIS-2026-10-10T19-11-11,
            //   finding 11).
            // Why is it important to test this test case? It is the
            //   documented remedy; a remedy that lost the event anyway
            //   would leave operators with no safe shared-fallback setup.

            // Given: the remedied order, and a send parked in the producer
            val factory = InterruptiblyParkedProducerFactory()
            val appender = newAppender(encoder = MessageBytesEncoder(), producerFactory = factory, fallback = null)
            val context = appender.context as LoggerContext
            val sharedFallback = RecordingAppender(context).apply { name = "CONSOLE" }
            appender.addAppender(sharedFallback)
            appender.start()
            val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
            root.addAppender(appender)
            root.addAppender(sharedFallback)
            try {
                appender.doAppend(newTestLoggingEvent(message = "parked at reset"))
                assertThat(factory.sendEntered.await(2, TimeUnit.SECONDS)).isTrue()

                // When
                context.reset()

                // Then: written, not dropped
                assertThat(appender.isStarted).isFalse()
                assertThat(sharedFallback.events.map { it.formattedMessage }).containsExactly("parked at reset")
                assertThat(appender.statusMessages()).noneMatch { it.contains("Fallback dispatcher dropped") }
            } finally {
                factory.release.countDown()
            }
        }

        @Test
        fun `should report a dead fallback worker through the status manager`() {
            // What is to be tested? The appender-level wiring of the fallback
            //   dispatcher's worker-death hook: an Error escaping the fallback
            //   appender's doAppend ends the worker, and the transport's hook
            //   must surface that as a status warning naming the cause - the
            //   dispatcher-level tests pin the hook itself, nothing pinned
            //   that the appender routes it to the status manager.
            // How will the test case be deemed successful and why? Successful
            //   if after one diverted event the status manager carries a
            //   warning containing "Fallback dispatcher worker died from" and
            //   the Error's class name. Mutation checked: a no-op hook body in
            //   KafkaTransport leaves the suite green without this test
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 5).
            // Why is it important to test this test case? A dead worker
            //   otherwise masquerades as a slow fallback: every further
            //   diversion is dropped and counted, and nothing says why.

            // Given: a fallback whose doAppend dies with an Error
            val dyingFallback =
                object : AppenderBase<ILoggingEvent>() {
                    init {
                        context = LoggerContext()
                        start()
                    }

                    override fun doAppend(eventObject: ILoggingEvent): Unit = throw AssertionError("simulated fallback death")

                    override fun append(event: ILoggingEvent) = error("unreachable")
                }
            val appender = newAppender(encoder = ThrowingEncoder(), fallback = dyingFallback)
            appender.start()

            // When: one event diverts to the fallback, whose worker dies
            appender.doAppend(newTestLoggingEvent())
            pollUntil { appender.statusMessages().any { it.contains("Fallback dispatcher worker died from") } }

            // Then: the cause is named
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Fallback dispatcher worker died from java.lang.AssertionError") }
            appender.stop()
        }

        @Test
        fun `should report a dead send worker through the status manager`() {
            // What is to be tested? The send-side counterpart: an Error
            //   escaping producer.send (the sender catches Exception only)
            //   ends the class's send worker, and the transport's hook must
            //   surface it as a status warning naming the class and the cause.
            // How will the test case be deemed successful and why? Successful
            //   if after one append the status manager carries a warning
            //   containing "Send dispatcher worker for TECHNICAL died from"
            //   and the Error's class name, and the event reached the fallback
            //   (the death accounting diverts it with reason send.error).
            //   Mutation checked: a no-op hook body stays green otherwise
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 5).
            // Why is it important to test this test case? Without the report
            //   every further TECHNICAL event silently diverts to the
            //   fallback while Kafka is healthy, and operators would suspect
            //   the broker.

            // Given: a producer whose send dies with an Error
            val dyingSends =
                RecordingProducerFactory(
                    wrap = { mock ->
                        object : Producer<ByteArray, ByteArray> by mock {
                            override fun send(
                                record: ProducerRecord<ByteArray, ByteArray>,
                                callback: Callback?,
                            ): Future<RecordMetadata> = throw AssertionError("simulated send worker death")
                        }
                    },
                )
            val fallback = RecordingAppender()
            val appender = newAppender(producerFactory = dyingSends, fallback = fallback)
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent(message = "worker dies on this"))
            pollUntil { appender.statusMessages().any { it.contains("Send dispatcher worker for TECHNICAL died from") } }

            // Then: the cause is named and the event was not lost
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Send dispatcher worker for TECHNICAL died from java.lang.AssertionError") }
            appender.stop()
            assertThat(fallback.events.map { it.formattedMessage }).containsExactly("worker dies on this")
        }

        @Test
        fun `should report a producer that fails to close through the status manager`() {
            // What is to be tested? The appender-level surfacing of a close
            //   failure: the registry aggregates per-producer close failures
            //   into one exception after every close was attempted, and the
            //   transport's close must turn that into a status warning
            //   instead of losing it - the registry test proves the throw,
            //   nothing pinned the report.
            // How will the test case be deemed successful and why? Successful
            //   if after stop() the status manager carries a warning
            //   containing "Error closing producer registry" and the
            //   aggregate's "failed to close cleanly" text, and stop()
            //   itself returns normally. Mutation checked: swallowing the
            //   exception silently in closeAll stays green otherwise
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 5).
            // Why is it important to test this test case? A producer that
            //   cannot close keeps its network thread and buffers; the
            //   warning is the only hint at shutdown that something stayed
            //   behind.

            // Given: a producer whose close throws
            val unclosableProducers =
                RecordingProducerFactory(
                    wrap = { mock ->
                        object : Producer<ByteArray, ByteArray> by mock {
                            override fun close(timeout: Duration): Unit = throw IllegalStateException("simulated close failure")
                        }
                    },
                )
            val appender = newAppender(producerFactory = unclosableProducers)
            appender.start()

            // When: stop() must still complete
            appender.stop()

            // Then: the failure is reported with the aggregate's summary
            assertThat(appender.isStarted).isFalse()
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Error closing producer registry") && it.contains("failed to close cleanly") }
        }

        @Test
        fun `should run the teardown and its drop warning only once when stopped twice`() {
            // What is to be tested? The appender-level stop guard: Logback
            //   may call stop() more than once during a context teardown,
            //   and the second call must not re-run the close sequence -
            //   the dispatcher-level idempotence is pinned elsewhere, but
            //   nothing pinned that the appender itself does not re-enter
            //   closeAll and re-emit the lifetime drop warning.
            // How will the test case be deemed successful and why? Successful
            //   if after two stop() calls on an appender that dropped one
            //   event exactly one "Fallback dispatcher dropped" status entry
            //   exists. Mutation checked: removing the early return behind
            //   stopExecuted re-runs closeAll and emits the warning twice
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 14).
            // Why is it important to test this test case? A duplicated
            //   drop report overstates the loss in shutdown forensics, the
            //   very number operators rely on.

            // Given: a fallback whose doAppend throws, so one diversion is dropped
            val throwingFallback =
                object : AppenderBase<ILoggingEvent>() {
                    init {
                        context = LoggerContext()
                        start()
                    }

                    override fun doAppend(eventObject: ILoggingEvent): Unit = throw RuntimeException("fallback also broken")

                    override fun append(event: ILoggingEvent) = error("unreachable")
                }
            val appender = newAppender(encoder = ThrowingEncoder(), fallback = throwingFallback)
            appender.start()
            appender.doAppend(newTestLoggingEvent())

            // When: stopped twice
            appender.stop()
            appender.stop()

            // Then: one drop warning, not two
            assertThat(appender.statusMessages().filter { it.contains("Fallback dispatcher dropped") }).hasSize(1)
        }
    }
}
