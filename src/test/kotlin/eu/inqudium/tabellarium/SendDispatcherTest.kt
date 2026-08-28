package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class SendDispatcherTest {
    // -- Test fixtures --------------------------------------------------

    private val testContext = LoggerContext()

    /** Fallback recorder; the list is synchronized because the fallback worker writes while the test polls. */
    private inner class RecordingAppender : AppenderBase<ILoggingEvent>() {
        val events: MutableList<ILoggingEvent> = Collections.synchronizedList(mutableListOf())

        init {
            context = testContext
            start()
        }

        override fun append(event: ILoggingEvent) {
            events += event
        }
    }

    /** Metrics recorder for the fallback reasons the dispatcher emits. */
    private class RecordingMetrics : KafkaAppenderMetrics by KafkaAppenderMetrics.NO_OP {
        val fallbackReasons: MutableList<KafkaAppenderMetrics.FallbackReason> =
            Collections.synchronizedList(mutableListOf())
        val registeredQueueClasses: MutableList<TopicClass> =
            Collections.synchronizedList(mutableListOf())

        override fun eventFallback(
            topicClass: TopicClass,
            reason: KafkaAppenderMetrics.FallbackReason,
        ) {
            fallbackReasons += reason
        }

        override fun registerSendQueueGauges(
            topicClass: TopicClass,
            queueSize: () -> Int,
            capacity: Int,
        ) {
            registeredQueueClasses += topicClass
        }
    }

    private fun pending(message: String) = newTestLoggingEvent(message = message)

    /**
     * Real (asynchronous) fallback dispatcher, as in production.
     * Assertions on the recorder poll with [pollUntil]; where a test
     * must additionally prove that NO further event arrives, it closes
     * the returned dispatcher first (close drains the queue, so
     * everything enqueued up to that point is delivered before the
     * assertion).
     */
    private fun newFallback(recorder: RecordingAppender) = FallbackDispatcher(recorder)

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Asynchronous dispatch` {
        @Test
        fun `should not block the calling thread while the send action is parked`() {
            // What is to be tested? The defining property of SendDispatcher
            //   and the heart of finding H-1: dispatch() must return
            //   immediately even while the send action is blocked (the
            //   real producer.send may park for up to max.block.ms when
            //   metadata or buffer space is missing).
            // How will the test case be deemed successful and why? Successful
            //   if dispatch of a second event completes in far less than
            //   the time the first event's send is parked. A latch pins
            //   the send action deterministically; 200 ms is a generous
            //   bound for an O(1) queue offer.
            // Why is it important to test this test case? This is the
            //   latency assertion the analysis (H-3) found missing: a
            //   regression that runs the send action on the caller again
            //   would make every logging thread stall for max.block.ms
            //   per event during a broker outage.

            // Given: a send action parked on a latch
            val release = CountDownLatch(1)
            val entered = CountDownLatch(1)
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = {
                        entered.countDown()
                        release.await()
                    },
                    fallbackDispatcher = null,
                )
            try {
                // When: the first dispatch parks the worker in the send
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("first"))
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()

                // And: a second dispatch while the send is parked
                val startNanos = System.nanoTime()
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("second"))
                val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000

                // Then: the caller returned immediately
                assertThat(elapsedMs).isLessThan(200)
            } finally {
                release.countDown()
                dispatcher.close()
            }
        }

        @Test
        fun `should deliver dispatched events to the send action in FIFO order`() {
            // Given
            val delivered = Collections.synchronizedList(mutableListOf<String>())
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = { delivered += it.topicName },
                    fallbackDispatcher = null,
                )
            try {
                // When
                (1..5).forEach { i ->
                    dispatcher.dispatch("topic-$i", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("e$i"))
                }

                // Then: all delivered, in order
                pollUntil { delivered.size == 5 }
                assertThat(delivered).containsExactly("topic-1", "topic-2", "topic-3", "topic-4", "topic-5")
            } finally {
                dispatcher.close()
            }
        }

        @Test
        fun `should mark the worker thread with the reentry guard`() {
            // What is to be tested? Whether the worker carries the
            //   appender's reentry-guard ThreadLocal. The Kafka client
            //   logs synchronously on the producer.send caller - which is
            //   the worker now - and append() must drop those events via
            //   the guard instead of feeding them back into the queue.
            // How will the test case be deemed successful and why? Successful
            //   if the guard reads true inside the send action.
            // Why is it important to test this test case? Without the
            //   marking, Kafka-DEBUG self-logging would re-enter the
            //   pipeline from the worker thread - no longer as unbounded
            //   recursion (H-2 fixed that), but as a feedback loop that
            //   amplifies during broker trouble.

            // Given
            val guard = ThreadLocal.withInitial { false }
            val guardSeenTrue = AtomicBoolean(false)
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = { guardSeenTrue.set(guard.get()) },
                    fallbackDispatcher = null,
                    reentryGuard = guard,
                )
            try {
                // When
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("probe"))

                // Then
                pollUntil { guardSeenTrue.get() }
            } finally {
                dispatcher.close()
            }
        }
    }

    @Nested
    inner class `Overflow policy` {
        @Test
        fun `should divert to the fallback with reason queue-full when the queue is full`() {
            // What is to be tested? The bounded-queue contract: a full
            //   queue never blocks the caller - the event diverts to the
            //   fallback and is counted under reason QUEUE_FULL.
            // How will the test case be deemed successful and why? Successful
            //   if, with the worker pinned in a send and capacity 1,
            //   surplus dispatches land in the fallback recorder and the
            //   metric carries QUEUE_FULL. The latch anchors the worker
            //   so the queue state is deterministic.
            // Why is it important to test this test case? Blocking here
            //   would resurrect H-1 through the back door; silent dropping
            //   would lose events without the operator's escape hatch.

            // Given: worker pinned, capacity 1
            val release = CountDownLatch(1)
            val entered = CountDownLatch(1)
            val recorder = RecordingAppender()
            val metrics = RecordingMetrics()
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = {
                        entered.countDown()
                        release.await()
                    },
                    fallbackDispatcher = newFallback(recorder),
                    queueCapacity = 1,
                )
            dispatcher.setMetrics(metrics)
            try {
                // When: first dispatch pins the worker, second fills the
                //   queue, third and fourth overflow
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("in-flight"))
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("queued"))
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("overflow-1"))
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("overflow-2"))

                // Then: the overflow events reached the fallback with the
                //   queue-full reason; nothing blocked
                pollUntil { recorder.events.size == 2 }
                assertThat(recorder.events.map { it.formattedMessage })
                    .containsExactly("overflow-1", "overflow-2")
                assertThat(metrics.fallbackReasons)
                    .containsExactly(
                        KafkaAppenderMetrics.FallbackReason.QUEUE_FULL,
                        KafkaAppenderMetrics.FallbackReason.QUEUE_FULL,
                    )
            } finally {
                release.countDown()
                dispatcher.close()
            }
        }
    }

    @Nested
    inner class `Shutdown` {
        @Test
        fun `should drain the queue by sending when closed gracefully`() {
            // What is to be tested? Whether close() lets the worker finish
            //   delivering what is already queued - the producers are
            //   still open at that point in the appender's stop sequence,
            //   so draining by SENDING is both possible and the loss-free
            //   choice.
            // How will the test case be deemed successful and why? Successful
            //   if all events dispatched before close() reach the send
            //   action and nothing lands in the fallback.
            // Why is it important to test this test case? A close that
            //   discards the queue would turn every ordinary shutdown into
            //   avoidable log loss.

            // Given
            val sent = AtomicInteger(0)
            val recorder = RecordingAppender()
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = { sent.incrementAndGet() },
                    fallbackDispatcher = newFallback(recorder),
                )

            // When
            (1..4).forEach { i ->
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("e$i"))
            }
            dispatcher.close()

            // Then
            assertThat(sent.get()).isEqualTo(4)
            assertThat(recorder.events).isEmpty()
        }

        @Test
        fun `should divert the in-flight and queued events to the fallback when the drain times out`() {
            // What is to be tested? The forced-shutdown accounting, the
            //   send-path analogue of finding M-3: the event the worker is
            //   stuck sending plus everything still queued must divert to
            //   the fallback exactly once, tagged SHUTDOWN.
            // How will the test case be deemed successful and why? Successful
            //   if, with the worker pinned uninterruptibly in the send
            //   action, close() diverts exactly the in-flight event plus
            //   the queued ones - deterministic because the pinned worker
            //   cannot take a second item.
            // Why is it important to test this test case? On a pod
            //   shutdown with a hanging broker connection, precisely these
            //   events would otherwise vanish without fallback or count.

            // Given: an uninterruptibly pinned send action, short budget
            val release = CountDownLatch(1)
            val entered = CountDownLatch(1)
            val sendReturned = AtomicBoolean(false)
            val recorder = RecordingAppender()
            val metrics = RecordingMetrics()
            val fallbackDispatcher = newFallback(recorder)
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = {
                        entered.countDown()
                        var wasInterrupted = false
                        while (release.count > 0) {
                            try {
                                release.await()
                            } catch (_: InterruptedException) {
                                wasInterrupted = true
                            }
                        }
                        if (wasInterrupted) {
                            Thread.currentThread().interrupt()
                        }
                        sendReturned.set(true)
                    },
                    fallbackDispatcher = fallbackDispatcher,
                    drainTimeoutMs = 100,
                )
            dispatcher.setMetrics(metrics)
            dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("in-flight"))
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()
            (1..3).forEach { i ->
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("queued-$i"))
            }

            // When
            dispatcher.close()

            // Then: in-flight + queued diverted exactly once each
            pollUntil { recorder.events.size == 4 }
            assertThat(recorder.events.map { it.formattedMessage })
                .containsExactlyInAnyOrder("in-flight", "queued-1", "queued-2", "queued-3")
            assertThat(metrics.fallbackReasons)
                .containsOnly(KafkaAppenderMetrics.FallbackReason.SHUTDOWN)
                .hasSize(4)

            // Cleanup: release the worker; the completed send must not
            // divert the in-flight event a second time (close() already
            // claimed it - the worker's compare-and-set fails). Closing
            // the fallback dispatcher drains it, so a duplicate would be
            // visible in the recorder by the time the assertion runs.
            release.countDown()
            pollUntil { sendReturned.get() }
            fallbackDispatcher.close()
            assertThat(recorder.events).hasSize(4)
        }

        @Test
        fun `should divert events dispatched after close to the fallback`() {
            // Given
            val recorder = RecordingAppender()
            val metrics = RecordingMetrics()
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = { },
                    fallbackDispatcher = newFallback(recorder),
                )
            dispatcher.setMetrics(metrics)
            dispatcher.close()

            // When
            dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("late"))

            // Then
            pollUntil { recorder.events.size == 1 }
            assertThat(recorder.events.map { it.formattedMessage }).containsExactly("late")
            assertThat(metrics.fallbackReasons)
                .containsExactly(KafkaAppenderMetrics.FallbackReason.SHUTDOWN)
        }
    }

    @Nested
    inner class `Worker death` {
        @Test
        fun `should report a worker death and account for the in-flight item`() {
            // What is to be tested? Whether a worker killed by an Error
            //   (which the delivery loop deliberately does not catch) is
            //   surfaced via the onWorkerDeath hook and whether the item
            //   it was carrying is diverted instead of vanishing.
            // How will the test case be deemed successful and why? Successful
            //   if the hook receives the Error and the in-flight event
            //   lands in the fallback exactly once.
            // Why is it important to test this test case? A silently dead
            //   worker degrades the class to permanent queue.full
            //   diversion that reads like a slow broker; the hook is what
            //   lets the appender tell operators the real cause.

            // Given: a send action that dies with an Error
            val death = AtomicReference<Throwable?>()
            val recorder = RecordingAppender()
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = { throw AssertionError("simulated worker death") },
                    fallbackDispatcher = newFallback(recorder),
                    onWorkerDeath = { death.set(it) },
                )
            try {
                // When
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("doomed"))

                // Then: death reported, item diverted exactly once
                pollUntil { death.get() != null }
                assertThat(death.get()).hasMessage("simulated worker death")
                pollUntil { recorder.events.size == 1 }
                assertThat(recorder.events[0].formattedMessage).isEqualTo("doomed")
            } finally {
                dispatcher.close()
            }
        }

        @Test
        fun `should divert queued and later work instead of stranding it after a worker death`() {
            // What is to be tested? Whether a worker death transitions the
            //   dispatcher out of the accepting state: events already
            //   queued behind the dying item must be diverted by the
            //   death handler, and a dispatch after the death must divert
            //   on the caller instead of filling a queue no worker will
            //   ever drain.
            // How will the test case be deemed successful and why? Successful
            //   if the in-flight, the queued, and the post-death event all
            //   reach the fallback, every one counted with reason
            //   send.error. The latch pins the queued event behind the
            //   in-flight one deterministically.
            // Why is it important to test this test case? Before the fix,
            //   running stayed true after a worker death - up to the full
            //   queue capacity could strand silently until shutdown, and
            //   the loss surfaced only once the queue filled as
            //   misleading queue.full diversions.

            // Given: a send action that parks, then dies with an Error
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val death = AtomicReference<Throwable?>()
            val recorder = RecordingAppender()
            val metrics = RecordingMetrics()
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = {
                        entered.countDown()
                        release.await()
                        throw AssertionError("simulated worker death")
                    },
                    fallbackDispatcher = newFallback(recorder),
                    onWorkerDeath = { death.set(it) },
                )
            dispatcher.setMetrics(metrics)
            try {
                // When: one item in flight, one queued behind it
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("in-flight"))
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("queued"))
                release.countDown()
                pollUntil { death.get() != null }

                // Then: the death handler diverted both the in-flight and
                // the queued item
                pollUntil { recorder.events.size == 2 }
                assertThat(recorder.events.map { it.formattedMessage })
                    .containsExactlyInAnyOrder("in-flight", "queued")

                // And: a dispatch after the death diverts on the caller
                dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("after-death"))
                pollUntil { recorder.events.size == 3 }
                assertThat(recorder.events.map { it.formattedMessage })
                    .containsExactlyInAnyOrder("in-flight", "queued", "after-death")
                assertThat(metrics.fallbackReasons)
                    .hasSize(3)
                    .containsOnly(KafkaAppenderMetrics.FallbackReason.SEND_ERROR)
            } finally {
                dispatcher.close()
            }
        }
    }

    @Nested
    inner class `Diversion claim` {
        @Test
        fun `should let the send action stand down when the shutdown divert already claimed the item`() {
            // What is to be tested? The exactly-once contract between a
            //   forced close() and the send action's own error routing:
            //   the PendingSend's claim is handed to the sender
            //   (ResilientMessageSender uses it before every fallback
            //   diversion), so whoever claims first diverts alone.
            // How will the test case be deemed successful and why? Successful
            //   if, after close() diverted the pinned in-flight item with
            //   reason shutdown, the send action's later claim attempt
            //   returns false - modelling the sender finding the
            //   diversion already taken.
            // Why is it important to test this test case? This is the
            //   dispatcher-level pin for the duplicate-delivery scenario:
            //   without the shared claim, the same event would reach the
            //   fallback twice on exactly this timeline.

            // Given: an uninterruptibly pinned send action that records
            //   its claim attempt once released
            val release = CountDownLatch(1)
            val entered = CountDownLatch(1)
            val lateClaim = AtomicReference<Boolean?>()
            val recorder = RecordingAppender()
            val fallbackDispatcher = newFallback(recorder)
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.TECHNICAL,
                    sendAction = { item ->
                        entered.countDown()
                        var wasInterrupted = false
                        while (release.count > 0) {
                            try {
                                release.await()
                            } catch (_: InterruptedException) {
                                wasInterrupted = true
                            }
                        }
                        if (wasInterrupted) {
                            Thread.currentThread().interrupt()
                        }
                        // The sender would now take its send-error path
                        // and ask for the claim first:
                        lateClaim.set(item.tryClaimDiversion())
                    },
                    fallbackDispatcher = fallbackDispatcher,
                    drainTimeoutMs = 100,
                )
            dispatcher.dispatch("t", ByteArray(0), EnrichedRecord(null, emptyMap()), pending("pinned"))
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()

            // When: forced shutdown claims and diverts, then the send unblocks
            dispatcher.close()
            pollUntil { recorder.events.size == 1 }
            assertThat(recorder.events.map { it.formattedMessage }).containsExactly("pinned")
            release.countDown()
            pollUntil { lateClaim.get() != null }

            // Then: the late claim lost - no second diversion possible.
            // Closing the fallback dispatcher drains it, so a duplicate
            // would be visible in the recorder by the assertion.
            assertThat(lateClaim.get()).isFalse()
            fallbackDispatcher.close()
            assertThat(recorder.events).hasSize(1)
        }
    }

    @Nested
    inner class `Metrics wiring` {
        @Test
        fun `should register the send queue gauges for its topic class`() {
            // Given
            val metrics = RecordingMetrics()
            val dispatcher =
                SendDispatcher(
                    topicClass = TopicClass.AUDIT,
                    sendAction = { },
                    fallbackDispatcher = null,
                )
            try {
                // When
                dispatcher.setMetrics(metrics)

                // Then
                assertThat(metrics.registeredQueueClasses).containsExactly(TopicClass.AUDIT)
            } finally {
                dispatcher.close()
            }
        }
    }
}
