package eu.inqudium.tabellarium.sequencing

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.util.LogbackMDCAdapter
import ch.qos.logback.core.AppenderBase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class SequencingAsyncAppenderTest {
    // -- Test fixtures --------------------------------------------------

    private lateinit var loggerContext: LoggerContext

    @BeforeEach
    fun setUp() {
        loggerContext =
            LoggerContext().apply {
                // logback 1.5 reads the MDC adapter from the LoggerContext (LoggingEvent.getMDCPropertyMap),
                // not from the global MDC. A hand-built context has none, so the async preprocess path
                // (prepareForDeferredProcessing -> getMDCPropertyMap) NPEs. Give it a real adapter, exactly as
                // the SLF4J-bound context receives one at binding time.
                setMDCAdapter(LogbackMDCAdapter())
            }
    }

    @AfterEach
    fun tearDown() {
        loggerContext.stop()
    }

    private fun loggingEvent(message: String = "test message"): LoggingEvent {
        val logger = loggerContext.getLogger("test-logger")
        return LoggingEvent("fqcn.dummy", logger, Level.INFO, message, null, null)
    }

    /**
     * Downstream appender used inside AsyncAppender tests. Records every
     * event it receives (in the worker thread). Optionally blocks on a
     * latch to simulate a slow downstream and provoke queue back-pressure
     * without using Thread.sleep.
     */
    private class RecordingAppender(
        private val gate: CountDownLatch? = null,
    ) : AppenderBase<ILoggingEvent>() {
        val received: ConcurrentLinkedQueue<ILoggingEvent> = ConcurrentLinkedQueue()

        init {
            start()
        }

        override fun append(event: ILoggingEvent) {
            gate?.await() // block until the test releases us
            received += event
        }
    }

    /**
     * Extracts the KeyValuePair value matching [key] from the event,
     * or null if not present.
     */
    private fun ILoggingEvent.kvValue(key: String): Any? = this.keyValuePairs?.firstOrNull { it.key == key }?.value

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `KeyValuePair stamping` {
        @Test
        fun `should attach a Long sequence and String instance to the event's KeyValuePairs list`() {
            // What is to be tested? Whether preprocess writes typed
            //   values into the KeyValuePairs channel: a `Long` for
            //   the sequence, a `String` for the instance. Preserving
            //   the numeric type is what allows Elasticsearch to
            //   dynamically map the field as `long` — the entire
            //   reason for using KeyValuePairs instead of MDC.
            // How will the test case be deemed successful and why?
            //   Successful if the extracted values match the expected
            //   Kotlin types exactly. A regression that wrote the
            //   sequence as `String` would silently break the ES
            //   mapping without any test failure elsewhere; this test
            //   pins the type contract at the source.
            // Why is it important to test this test case? The value
            //   type flows through the entire pipeline: KeyValuePair
            //   → LogstashEncoder → JSON → Elasticsearch dynamic
            //   mapping. Any type erosion here would negate the whole
            //   point of the KeyValuePair channel choice.

            // Given
            val appender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    instanceId = "pod-alpha"
                    addAppender(RecordingAppender())
                    start()
                }
            val event = loggingEvent()

            // When
            appender.preprocess(event)

            // Then: the values carry the intended types
            val seqValue = event.kvValue("log_async_sequence")
            val instanceValue = event.kvValue("log_async_instance")
            assertThat(seqValue).isInstanceOf(Long::class.javaObjectType)
            assertThat(seqValue).isEqualTo(1L)
            assertThat(instanceValue).isInstanceOf(String::class.java)
            assertThat(instanceValue).isEqualTo("pod-alpha")

            appender.stop()
        }

        @Test
        fun `should assign strictly monotonic sequence numbers across preprocess calls`() {
            // Given
            val appender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    instanceId = "pod-alpha"
                    addAppender(RecordingAppender())
                    start()
                }
            val events = (1..3).map { loggingEvent("event-$it") }

            // When
            events.forEach { appender.preprocess(it) }

            // Then
            assertThat(events.map { it.kvValue("log_async_sequence") })
                .containsExactly(1L, 2L, 3L)

            appender.stop()
        }

        @Test
        fun `should generate a fresh UUID when no instance identifier is configured`() {
            // Given
            val appender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    addAppender(RecordingAppender())
                    start()
                }
            val event = loggingEvent()

            // When
            appender.preprocess(event)

            // Then
            val instance = event.kvValue("log_async_instance") as? String
            assertThat(instance).isNotBlank
            assertThat(instance).matches(
                Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").toPattern(),
            )

            appender.stop()
        }

        @Test
        fun `should not touch the event's MDC map when stamping via KeyValuePairs`() {
            // What is to be tested? Whether the stamping uses only the
            //   KeyValuePairs channel and does NOT leak into MDC. The
            //   whole reason for choosing KeyValuePairs over MDC is
            //   the namespace separation from application code.
            // How will the test case be deemed successful and why?
            //   Successful if the event's MDC map, after stamping,
            //   contains only what the application put there — nothing
            //   the sequencing wrapper added. A regression that
            //   accidentally wrote to MDC alongside KeyValuePairs
            //   would break the namespace guarantee.
            // Why is it important to test this test case? MDC pollution
            //   is invisible in production until unrelated code
            //   suddenly sees mysterious entries in
            //   `MDC.getCopyOfContextMap()`. Pinning this negative
            //   assertion prevents such regressions.

            // Given: an event with pre-existing MDC entries
            val event =
                loggingEvent().apply {
                    mdcPropertyMap = mapOf("request.id" to "req-abc")
                }
            val appender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    instanceId = "pod-alpha"
                    addAppender(RecordingAppender())
                    start()
                }

            // When
            appender.preprocess(event)

            // Then: MDC untouched, KeyValuePairs carry the stamp
            assertThat(event.mdcPropertyMap).containsExactlyEntriesOf(
                mapOf("request.id" to "req-abc"),
            )
            assertThat(event.mdcPropertyMap).doesNotContainKeys("log_async_sequence", "log_async_instance")
            assertThat(event.kvValue("log_async_sequence")).isEqualTo(1L)

            appender.stop()
        }
    }

    @Nested
    inner class `Async queue behavior` {
        @Test
        fun `should stamp every event before enqueueing so a discarded event is still numbered`() {
            // What is to be tested? That preprocess stamps every event BEFORE the
            //   AsyncAppender's enqueue/discard decision, so an event the full queue
            //   drops STILL carries its sequence number - the entire justification for
            //   the wrapper. If preprocess ran after the discard, discarded events would
            //   go unnumbered and the gap analysis in Elasticsearch would be blind.
            // How will the test case be deemed successful and why? Stamping mutates the
            //   event OBJECT in place (addKeyValuePair) on the CALLING thread, before the
            //   enqueue, so the references held here reflect it even for discarded events.
            //   Success = every one of the emitted events carries a strictly sequential
            //   number 1..N, and the downstream received strictly fewer than N (proving the
            //   queue really discarded). A CountDownLatch (not Thread.sleep) blocks the
            //   downstream so the queue fills and discards deterministically.
            // Why check the emitted OBJECTS rather than a gap in the SURVIVING sequences?
            //   A FIFO queue with tail-drop (neverBlock) always leaves a contiguous prefix
            //   of survivors, so a "middle gap" can only appear through a non-deterministic
            //   offer/take race - it is not a reliable signal. The stamp on every emitted
            //   object is.

            // Given: a downstream appender blocked on a latch
            val gate = CountDownLatch(1)
            val downstream = RecordingAppender(gate)
            val appender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    instanceId = "pod-alpha"
                    queueSize = 2
                    discardingThreshold = 0
                    isNeverBlock = true
                    addAppender(downstream)
                    start()
                }

            val emittedTotal = 50
            val emitted = (1..emittedTotal).map { loggingEvent("event-$it") }

            // When: fire more events than the queue can hold, the downstream blocked
            try {
                emitted.forEach { appender.doAppend(it) }
                gate.countDown()
            } finally {
                appender.stop() // drains queue and joins worker
            }

            // Then: every emitted event object - even the ones the full queue discarded -
            //   was stamped in preprocess, with strictly sequential numbers.
            val stamped = emitted.map { it.kvValue("log_async_sequence") as? Long }
            assertThat(stamped)
                .describedAs("every event, discarded or not, must be numbered before the enqueue/discard")
                .isEqualTo((1L..emittedTotal.toLong()).toList())

            // And: the queue really did discard - fewer events reached the downstream than were fired.
            assertThat(downstream.received.size)
                .describedAs("some events must have been discarded to make this a valid test")
                .isLessThan(emittedTotal)
        }
    }

    @Nested
    inner class `Concurrency` {
        @Test
        fun `should produce contiguous non-overlapping sequences under concurrent preprocess calls`() {
            // Given
            val appender =
                SequencingAsyncAppender().apply {
                    context = loggerContext
                    instanceId = "pod-alpha"
                    addAppender(RecordingAppender())
                    start()
                }
            val threadCount = 16
            val eventsPerThread = 100
            val totalEvents = threadCount * eventsPerThread
            val start = CountDownLatch(1)
            val done = CountDownLatch(threadCount)
            val pool = Executors.newFixedThreadPool(threadCount)
            val allEvents = ConcurrentLinkedQueue<LoggingEvent>()

            // When
            try {
                repeat(threadCount) {
                    pool.submit {
                        start.await()
                        repeat(eventsPerThread) {
                            val event = loggingEvent()
                            appender.preprocess(event)
                            allEvents += event
                        }
                        done.countDown()
                    }
                }
                start.countDown()
                assertThat(done.await(10, TimeUnit.SECONDS))
                    .describedAs("all threads completed within 10 seconds")
                    .isTrue
            } finally {
                pool.shutdownNow()
                appender.stop()
            }

            // Then
            val sequences = allEvents.mapNotNull { it.kvValue("log_async_sequence") as? Long }
            assertThat(sequences).hasSize(totalEvents)
            assertThat(sequences.toSortedSet())
                .describedAs("sequence values should form a dense set 1..N")
                .isEqualTo((1L..totalEvents.toLong()).toSortedSet())
        }
    }
}
