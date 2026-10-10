package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.util.LogbackMDCAdapter
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.encoder.EncoderBase
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.MockProducer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.Marker
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The hot path of [KafkaAppender]: encode-and-send of an appended
 * event, caller-data capture on the caller thread, and the failure
 * handling that routes to the fallback appender, accounts drops and
 * keeps the counters conserved when the encoder, the routing or the
 * fallback itself fails.
 */
class KafkaAppenderHotPathTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Hot path` {
        @Test
        fun `should encode and send the event when appended`() {
            // What is to be tested? The basic hot path: an appended event is
            //   encoded by the configured encoder, routed to the default topic
            //   and handed to the producer of the active class.
            // How will the test case be deemed successful and why? Successful
            //   if the encoder saw exactly one event and the MockProducer history
            //   holds exactly one record addressed to "default.topic". The
            //   draining stop() makes the asynchronous send observable.
            // Why is it important to test this test case? This is the contract
            //   every other hot-path test builds on; if encode-route-send broke,
            //   the appender would start cleanly and deliver nothing.

            // Given
            val encoder = RecordingEncoder()
            val factory = RecordingProducerFactory()
            val appender = newAppender(encoder = encoder, producerFactory = factory)
            appender.start()

            // When
            val event = newTestLoggingEvent(message = "hello kafka")
            appender.doAppend(event)

            // Then
            appender.stop()
            assertThat(encoder.encodedEvents).hasSize(1)
            assertThat(factory.createdProducers[0].history()).hasSize(1)
            assertThat(factory.createdProducers[0].history()[0].topic())
                .isEqualTo("default.topic")
        }

        @Test
        fun `should deliver an event whose MDC cannot be materialized instead of failing every event`() {
            // What is to be tested? The embedded-context path: a
            //   LoggerContext without a bound MDC adapter makes
            //   LoggingEvent.getMDCPropertyMap() throw. append() swallows
            //   that from prepareForDeferredProcessing, but the default
            //   partitioning-key extractor read the same getter one step
            //   later - and turned every event into a hot-path failure.
            // How will the test case be deemed successful and why? Successful
            //   if an event built on a bare LoggerContext (no MDC map set,
            //   unlike the shared test helper) reaches the producer with
            //   no partitioning key, nothing lands in the fallback, and no
            //   hot-path error is reported.
            // Why is it important to test this test case? Before the fix
            //   (docs/assessment/CODE_ANALYSIS-2026-09-07T19-09-00.R2.md,
            //   finding R2-1) such a
            //   setup started cleanly, reported one error, and shipped
            //   nothing to Kafka - every event diverted as encoder.error.

            // Given: an appender with a fallback recorder
            val factory = RecordingProducerFactory()
            val fallback = RecordingAppender()
            val appender = newAppender(producerFactory = factory, fallback = fallback)
            appender.start()
            // And: an event on a bare LoggerContext whose MDC is unreadable
            //   (the helper sets the map explicitly; here it deliberately
            //   stays unset, so materialization throws)
            val bareContext = LoggerContext()
            val event =
                LoggingEvent(
                    "fqcn.dummy",
                    bareContext.getLogger("embedded"),
                    Level.INFO,
                    "no mdc adapter here",
                    null,
                    null,
                )

            // When
            appender.doAppend(event)

            // Then: delivered to Kafka without a key, not diverted
            appender.stop()
            assertThat(factory.createdProducers[0].history()).hasSize(1)
            assertThat(factory.createdProducers[0].history()[0].key()).isNull()
            assertThat(fallback.events).isEmpty()
            assertThat(appender.statusMessages()).noneMatch { it.contains("Hot path error") }
        }

        @Test
        fun `should not invoke the fallback when the send succeeds`() {
            // What is to be tested? Whether a successful send leaves the
            //   fallback appender untouched - the fallback is an error path,
            //   not a second delivery channel.
            // How will the test case be deemed successful and why? Successful
            //   if the fallback recorder holds no events after the draining
            //   stop() - the drained pipeline delivered to Kafka only.
            // Why is it important to test this test case? A fallback that also
            //   received successful events would double-write every log line to
            //   the local file and make the fallback's metrics useless as an
            //   outage signal.

            // Given
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent())

            // Then: the drained pipeline delivered to Kafka, not the fallback
            appender.stop()
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should not run any append logic when the appender is not started`() {
            // What is to be tested? Whether an event handed to a not-started
            //   appender touches nothing observable: Logback's
            //   UnsynchronizedAppenderBase.doAppend gate short-circuits when
            //   isStarted is false, and behind it append() returns at the
            //   null plan/transport before any hook runs.
            // How will the test case be deemed successful and why? Successful
            //   if the encoder saw no event, the attached (started) fallback
            //   received nothing, and no hot-path error was reported - three
            //   observables of the gate itself. The former assertion (no
            //   producer created) held regardless of what doAppend did,
            //   because producers are created in start(), which the test
            //   never calls (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 12).
            // Why is it important to test this test case? Logback's
            //   autoconfiguration path can hand events to appenders before
            //   they are fully started; an event that slipped through would
            //   be encoded or diverted by a half-built pipeline.

            // Given: a not-started appender with a live fallback attached
            val encoder = RecordingEncoder()
            val fallback = RecordingAppender()
            val appender = newAppender(encoder = encoder, fallback = fallback)
            // Note: start() not called

            // When
            appender.doAppend(newTestLoggingEvent())

            // Then: nothing was encoded, diverted or reported
            assertThat(encoder.encodedEvents).isEmpty()
            assertThat(fallback.events).isEmpty()
            assertThat(appender.statusMessages()).noneMatch { it.contains("Hot path error") }
        }
    }

    @Nested
    inner class `Caller data` {
        @Test
        fun `should capture caller data on the caller thread before the hand-off when includeCallerData is set`() {
            // What is to be tested? The behavior behind the includeCallerData
            //   flag (its Joran binding is pinned elsewhere): with the flag
            //   set, append() materializes the event's caller data on the
            //   logging thread before the event crosses to a worker, so a
            //   fallback layout using %caller names the application's call
            //   site and not a dispatcher thread's stack.
            // How will the test case be deemed successful and why? Successful
            //   if the event that reached the fallback carries caller data
            //   whose first frame is this test class. Logback extracts caller
            //   data from the stack of the thread that first asks for it,
            //   relative to the logger's FQCN frame - on a worker thread (or
            //   the asserting test thread, lazily) that frame does not
            //   exist and the array is empty. Mutation checked: deleting the
            //   `if (includeCallerData) event.callerData` lines turns the
            //   assertion red (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 15).
            // Why is it important to test this test case? The deferred-
            //   processing contract README documents for fallback layouts
            //   with %caller rested on a binding test alone; a refactor that
            //   moved the read behind the hand-off would silently name the
            //   dispatcher thread as the caller of every diverted event.

            // Given: a real logger feeding a diverting appender with the flag set
            val loggerContext = LoggerContext().apply { mdcAdapter = LogbackMDCAdapter() }
            val fallback = RecordingAppender()
            val appender =
                newAppender(encoder = ThrowingEncoder(), fallback = fallback).apply {
                    includeCallerData = true
                }
            appender.start()
            loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(appender)

            // When: logged through the real logger on this thread, then drained
            loggerContext.getLogger("caller-data-test").info("diverted with caller data")
            appender.stop()

            // Then: the diverted event names this class as its caller
            val diverted = fallback.events.single()
            assertThat(diverted.callerData).isNotEmpty
            assertThat(diverted.callerData[0].className).isEqualTo(javaClass.name)
        }

        @Test
        fun `should not compute caller data on the caller thread when includeCallerData is off`() {
            // What is to be tested? The negative control of the flag: with
            //   includeCallerData false (the default), append() leaves the
            //   event's caller data uncomputed - the stack walk is the
            //   expensive part of the hot path and is paid only on opt-in.
            // How will the test case be deemed successful and why? Successful
            //   if the event that reached the fallback reports
            //   hasCallerData() == false after the draining stop(): nothing
            //   on the hot path or on the workers asked for it. The positive
            //   test above alone lets a regression that always walks the
            //   stack pass (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 19).
            // Why is it important to test this test case? The default is
            //   the configuration every deployment without %caller runs;
            //   an unconditional stack walk per event there is a silent
            //   hot-path regression no other test would notice.

            // Given: the same diverting pipeline with the flag off
            val loggerContext = LoggerContext().apply { mdcAdapter = LogbackMDCAdapter() }
            val fallback = RecordingAppender()
            val appender = newAppender(encoder = ThrowingEncoder(), fallback = fallback)
            assertThat(appender.includeCallerData).isFalse()
            appender.start()
            loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(appender)

            // When: logged through the real logger on this thread, then drained
            loggerContext.getLogger("caller-data-test").info("diverted without caller data")
            appender.stop()

            // Then: nobody computed the caller data
            val diverted = fallback.events.single()
            assertThat(diverted.hasCallerData()).isFalse()
        }
    }

    @Nested
    inner class `Hot path failure handling` {
        @Test
        fun `should route to the fallback appender when the encoder throws`() {
            // What is to be tested? Whether a hot-path exception (here an
            //   encoder that always throws) diverts the original event to the
            //   configured fallback appender instead of losing it.
            // How will the test case be deemed successful and why? Successful
            //   if the fallback recorder holds exactly that event, identified by
            //   its message, after the draining stop(). This pins the catch
            //   branch of append(): count, then enqueue on the fallback
            //   dispatcher.
            // Why is it important to test this test case? Encoder bugs are the
            //   most likely hot-path failure; without the diversion each such
            //   event would vanish, and the local fallback file - the operator's
            //   last resort during an incident - would show nothing.

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
            appender.stop()
            assertThat(fallback.events).hasSize(1)
            assertThat(fallback.events[0].message).isEqualTo("encoder will throw")
        }

        @Test
        fun `should warn at start and count diversions as dropped when the fallback appender is not started`() {
            // What is to be tested? Whether a fallback appender that is
            //   attached but not started - its own start() failed, or a
            //   foreign owner stopped it - is detected at start() with a
            //   status warning, and whether each diversion to it is counted
            //   as a fallback-dispatcher drop instead of as delivered.
            // How will the test case be deemed successful and why? Successful
            //   if the start-up warning names the appender, the fallback
            //   recorded nothing (Logback's doAppend on a not-started
            //   appender returns without appending) and the
            //   kafka.appender.fallback.dropped counter reads 1 after the
            //   draining stop(). Before the fix doAppend's silent return
            //   passed as a delivery: no counter moved
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 10).
            // Why is it important to test this test case? A FileAppender
            //   whose path is unwritable stays attached in exactly this
            //   state; the fallback exists to make loss visible, and here
            //   the loss was invisible on the path built for visibility.

            // Given: an attached but never-started fallback, a diverting hot path
            val notStartedFallback =
                ThreadSafeListAppender().apply {
                    name = "DEAD_FALLBACK"
                    context = LoggerContext()
                }
            val registry = SimpleMeterRegistry()
            val appender = newAppender(encoder = ThrowingEncoder(), fallback = notStartedFallback)

            // When: started, bound, and one event diverted
            appender.start()
            appender.bindMeterRegistry(registry)
            appender.doAppend(newTestLoggingEvent(message = "diverted to a dead fallback"))

            // Then: warned at start, the loss counted on the fallback worker
            //   (read before stop(), which unbinds and removes the meters),
            //   nothing appended
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Fallback appender 'DEAD_FALLBACK'") && it.contains("is not started") }
            val dropped = registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED)
            pollUntil { dropped.count() == 1.0 }
            appender.stop()
            assertThat(notStartedFallback.events).isEmpty()
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
            appender.stop()
            assertThat(fallback.events).hasSize(100)
            // But only one error was logged
            assertThat(appender.statusMessages().filter { it.contains("Hot path error") })
                .hasSize(1)
        }

        @Test
        fun `should account the event as dropped when both the hot path and the fallback fail`() {
            // What is to be tested? The double-failure path: the encoder
            //   throws (event diverts to the fallback) and the fallback
            //   appender's doAppend throws as well. Nothing may escape
            //   the caller, and the loss must still be accounted - the
            //   dispatcher counts a delivery that threw as dropped and
            //   the appender reports the count at stop().
            // How will the test case be deemed successful and why? Successful
            //   if doAppend returns normally, the first hot-path error is
            //   reported once, and after stop() the status manager carries
            //   the "dropped 1 event(s)" warning. That warning is the only
            //   trace of the loss without a metrics registry; a test that
            //   merely proved "does not throw" would let it go dark.
            // Why is it important to test this test case? A fallback that
            //   throws (full disk, closed stream) is exactly the moment
            //   operators must learn that the last-resort path lost data.

            // Given: a fallback whose doAppend throws (overridden directly,
            //   because AppenderBase.doAppend would swallow an exception
            //   from append() before the dispatcher could count it)
            val fallbackContext = LoggerContext()
            val throwingFallback =
                object : AppenderBase<ILoggingEvent>() {
                    init {
                        context = fallbackContext
                        start()
                    }

                    override fun doAppend(eventObject: ILoggingEvent): Unit = throw RuntimeException("fallback also broken")

                    override fun append(event: ILoggingEvent) = error("unreachable")
                }
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = throwingFallback,
                )
            appender.start()

            // When: must not throw out of append
            appender.doAppend(newTestLoggingEvent())

            // Then: reported once on the hot path, and the loss counted at stop
            appender.stop()
            assertThat(appender.statusMessages().filter { it.contains("Hot path error") }).hasSize(1)
            assertThat(appender.statusMessages())
                .anyMatch { it.contains("Fallback dispatcher dropped 1 event(s)") }
        }

        @Test
        fun `should keep accepted and fallback counters conserved when the encoder throws`() {
            // What is to be tested? The eventAccepted contract on the
            //   materialize failure path: an event whose routing resolved
            //   a class but whose encoding then fails is counted as
            //   accepted exactly once (at routing time, not again in the
            //   catch) and as one encoder.error fallback under that class,
            //   so that accepted = dispatched + fallback holds. The
            //   pre-routing branch (no class resolved yet) is the subject
            //   of the next test (DEFECT_ANALYSIS-2026-10-10T19-11-11,
            //   finding 1).
            // How will the test case be deemed successful and why? Successful
            //   if one encoder-failing append increments both the
            //   accepted counter and the fallback counter (reason
            //   encoder.error) exactly once, both tagged technical - the
            //   default topic's class, which routing resolved before the
            //   encoder threw.
            // Why is it important to test this test case? Before the fix,
            //   encoder failures produced fallbacks without accepted
            //   events - operational ratios showed more diversions than
            //   ingress, breaking conservation checks exactly when
            //   diagnosis mattered.

            // Given
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = fallback,
                )
            appender.start()
            appender.bindMeterRegistry(registry)

            // When
            appender.doAppend(newTestLoggingEvent(message = "encoder will throw"))

            // Then: the event reached the fallback (async worker; the
            // counters must be read before stop() unbinds the meters)
            pollUntil { fallback.events.size == 1 }
            val accepted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )
            assertThat(accepted.count()).isEqualTo(1.0)
            val diverted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                    MicrometerKafkaAppenderMetrics.TAG_REASON to "encoder.error",
                )
            assertThat(diverted.count()).isEqualTo(1.0)
            appender.stop()
        }

        @Test
        fun `should count an event as accepted under the technical default when routing itself fails`() {
            // What is to be tested? The pre-routing branch of append()'s
            //   catch: when the failure hits before routing resolved a class
            //   - here a marker whose getName() throws, so the router never
            //   returns - the event was not yet counted as accepted, and the
            //   catch must record it under the TECHNICAL default together
            //   with the encoder.error fallback, so that
            //   accepted = dispatched + fallback holds on this path too.
            // How will the test case be deemed successful and why? Successful
            //   if one such append increments the accepted counter and the
            //   encoder.error fallback counter exactly once, both tagged
            //   technical, and the event reaches the fallback recorder.
            //   Mutation checked: deleting the late eventAccepted in the
            //   catch (the null-class branch) leaves accepted at zero and
            //   turns the test red; the encoder-throwing test above cannot
            //   see that, because routing has already counted the event
            //   there (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 1).
            // Why is it important to test this test case? A third-party
            //   marker is the one input the router cannot validate at
            //   start-up; a failure there must not break the conservation
            //   invariant operators use to spot silent loss.

            // Given: a diverting fallback, a bound registry, and an event
            //   carrying a marker the router cannot read
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.start()
            appender.bindMeterRegistry(registry)
            val unreadableMarker =
                object : Marker {
                    override fun getName(): String = throw IllegalStateException("marker name unavailable")

                    override fun add(reference: Marker) = Unit

                    override fun remove(reference: Marker): Boolean = false

                    // Compatibility: SLF4J deprecates hasChildren in favor of
                    // hasReferences; marked so the override emits no warning.
                    @Deprecated("SLF4J deprecated Marker.hasChildren; use hasReferences", ReplaceWith("hasReferences()"))
                    override fun hasChildren(): Boolean = false

                    override fun hasReferences(): Boolean = false

                    override fun iterator(): MutableIterator<Marker> = mutableListOf<Marker>().iterator()

                    override fun contains(other: Marker): Boolean = false

                    override fun contains(name: String): Boolean = false
                }
            val event = (newTestLoggingEvent(message = "routing will throw") as LoggingEvent).apply { addMarker(unreadableMarker) }

            // When
            appender.doAppend(event)

            // Then: the event reached the fallback, and both counters moved
            //   once under the technical default (read before stop() unbinds)
            pollUntil { fallback.events.size == 1 }
            assertThat(fallback.events.single().message).isEqualTo("routing will throw")
            val accepted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )
            assertThat(accepted.count()).isEqualTo(1.0)
            val diverted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                    MicrometerKafkaAppenderMetrics.TAG_REASON to "encoder.error",
                )
            assertThat(diverted.count()).isEqualTo(1.0)
            appender.stop()
        }

        @Test
        fun `should let an Error escape doAppend and count the event as accepted but neither dispatched nor diverted`() {
            // What is to be tested? The documented Error contract of the hot
            //   path: an Error thrown on the caller's thread (a LinkageError
            //   from a mismatched optional encoder dependency, an OOM) is
            //   not caught by append() - it propagates to the logging call
            //   like in Logback's own doAppend - and the event is then
            //   counted as accepted without being dispatched or diverted.
            // How will the test case be deemed successful and why? Successful
            //   if doAppend throws the Error, the accepted counter reads 1
            //   and the dispatched counter, every fallback counter and the
            //   fallback recorder stay at zero. The catch in append() takes
            //   Exception only; widening it to Throwable would swallow the
            //   Error and divert the event, which turns the throw assertion
            //   red (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 19).
            // Why is it important to test this test case? The accepted
            //   counter's "every event entering append" contract and the
            //   Error pass-through are both documented; neither had a test,
            //   so a change to either would have passed silently.

            // Given: an encoder that fails with an Error, a fallback that
            //   must stay empty, a bound registry
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender()
            val erroringEncoder =
                object : EncoderBase<ILoggingEvent>() {
                    override fun encode(event: ILoggingEvent): ByteArray = throw LinkageError("simulated optional dependency mismatch")

                    override fun headerBytes(): ByteArray = ByteArray(0)

                    override fun footerBytes(): ByteArray = ByteArray(0)
                }
            val appender = newAppender(encoder = erroringEncoder, fallback = fallback)
            appender.start()
            appender.bindMeterRegistry(registry)

            // When: the Error escapes to the caller
            assertThatThrownBy { appender.doAppend(newTestLoggingEvent(message = "error on the hot path")) }
                .isInstanceOf(LinkageError::class.java)
                .hasMessage("simulated optional dependency mismatch")

            // Then: accepted once, nothing else moved (read before stop() unbinds)
            val technical = MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag
            assertThat(registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED, technical).count()).isEqualTo(1.0)
            assertThat(registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED, technical).count()).isEqualTo(0.0)
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK).counters())
                .allSatisfy { counter -> assertThat(counter.count()).`as`("%s", counter.id).isEqualTo(0.0) }
            appender.stop()
            assertThat(fallback.events).isEmpty()
        }
    }
}
