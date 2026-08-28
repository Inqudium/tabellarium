package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class FallbackDispatcherTest {
    // -- Test fixtures --------------------------------------------------

    /**
     * LoggerContext shared by the test appenders. Without setting it on
     * AppenderBase instances, Logback emits "No context given for ..."
     * warnings to stderr on every doAppend.
     */
    private val testContext = LoggerContext()

    /** Appender that records the events it receives. */
    private inner class RecordingAppender : AppenderBase<ILoggingEvent>() {
        val events = mutableListOf<ILoggingEvent>()

        init {
            context = testContext
            start()
        }

        override fun append(event: ILoggingEvent) {
            synchronized(events) { events += event }
        }

        fun eventCount(): Int = synchronized(events) { events.size }
    }

    /**
     * Appender that blocks on each append until released. With
     * [interruptible] = false the block survives the worker interrupt
     * that FallbackDispatcher.close() sends - modelling a fallback
     * appender stuck in non-interruptible I/O, which is what pins the
     * in-flight event across a forced shutdown.
     */
    private inner class BlockingAppender(
        private val interruptible: Boolean = true,
    ) : AppenderBase<ILoggingEvent>() {
        private val release = CountDownLatch(1)
        val appendCount = AtomicInteger(0)

        /**
         * True while a thread is parked in [append] waiting on the
         * release latch. Used by tests to deterministically wait for
         * the dispatcher worker to enter the blocking call.
         */
        val inAppend = AtomicBoolean(false)

        init {
            context = testContext
            start()
        }

        override fun append(event: ILoggingEvent) {
            inAppend.set(true)
            try {
                if (interruptible) {
                    release.await()
                } else {
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
                }
                appendCount.incrementAndGet()
            } finally {
                inAppend.set(false)
            }
        }

        fun unblock() = release.countDown()
    }

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Asynchronous dispatch` {
        @Test
        fun `should not block the calling thread when the fallback appender is slow`() {
            // What is to be tested? The defining property of FallbackDispatcher:
            //   enqueue() returns immediately even if the fallback appender
            //   is stuck in doAppend(). This is the entire reason this class
            //   exists - the Kafka I/O thread must never be held hostage by
            //   a slow downstream appender.
            // How will the test case be deemed successful and why? Successful
            //   if enqueue completes in under 200 ms even when the blocking
            //   appender holds the worker thread indefinitely. 200 ms is a
            //   generous upper bound; the actual time should be sub-millisecond.
            // Why is it important to test this test case? Without this test,
            //   a regression that synchronously called doAppend from enqueue
            //   would not be caught - and that regression would re-introduce
            //   the very Kafka-I/O-thread blocking that motivated this class.

            // Given: a fallback that blocks forever in append()
            val blockingAppender = BlockingAppender()
            val dispatcher = FallbackDispatcher(blockingAppender)
            try {
                // When: enqueue an event, measure the time
                val event = newTestLoggingEvent(message = "blocked")
                val startNanos = System.nanoTime()
                val accepted = dispatcher.enqueue(event)
                val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000

                // Then: enqueue accepted the event and returned quickly
                assertThat(accepted).isTrue()
                assertThat(elapsedMs).isLessThan(200)
            } finally {
                blockingAppender.unblock()
                dispatcher.close()
            }
        }

        @Test
        fun `should deliver enqueued events to the fallback appender on the worker thread`() {
            // Given
            val recorder = RecordingAppender()
            val dispatcher = FallbackDispatcher(recorder)
            try {
                // When
                dispatcher.enqueue(newTestLoggingEvent(message = "one"))
                dispatcher.enqueue(newTestLoggingEvent(message = "two"))

                // Then: events arrive at the recorder asynchronously
                pollUntil { recorder.eventCount() == 2 }
                assertThat(recorder.events.map { it.formattedMessage })
                    .containsExactly("one", "two")
            } finally {
                dispatcher.close()
            }
        }
    }

    @Nested
    inner class `Drop policy` {
        @Test
        fun `should drop events and count them when the queue is full`() {
            // What is to be tested? Whether a full queue triggers the drop
            //   policy rather than blocking the caller or throwing.
            // How will the test case be deemed successful and why? Successful
            //   if enqueueing more events than the capacity returns false
            //   for the overflow and increments droppedEventCount. Pins
            //   down the bounded-queue contract.
            // Why is it important to test this test case? Unbounded growth
            //   would trade one OOM risk (Kafka I/O thread blocking) for
            //   another (heap exhaustion). The drop is the safe choice.

            // Given: a tiny-capacity dispatcher with a blocking appender
            //   so events accumulate in the queue rather than being drained
            val blockingAppender = BlockingAppender()
            val dispatcher =
                FallbackDispatcher(
                    fallbackAppender = blockingAppender,
                    queueCapacity = 2,
                )
            try {
                // When: enqueue 5 events
                val accepted =
                    (1..5).map {
                        dispatcher.enqueue(newTestLoggingEvent(message = "event-$it"))
                    }

                // Then: the first events fit, the last get dropped.
                //   The worker may also pull one event off the queue and
                //   block in doAppend, which frees a slot. So we expect
                //   *at least* 2 accepted and *at least* 1 dropped.
                assertThat(accepted.count { it }).isGreaterThanOrEqualTo(2)
                assertThat(accepted.count { !it }).isGreaterThanOrEqualTo(1)
                assertThat(dispatcher.droppedEventCount).isGreaterThanOrEqualTo(1)
            } finally {
                blockingAppender.unblock()
                dispatcher.close()
            }
        }
    }

    @Nested
    inner class `Shutdown` {
        @Test
        fun `should drain remaining events when closed gracefully`() {
            // Given
            val recorder = RecordingAppender()
            val dispatcher = FallbackDispatcher(recorder)

            // When: enqueue events, then close immediately
            dispatcher.enqueue(newTestLoggingEvent(message = "before-close-1"))
            dispatcher.enqueue(newTestLoggingEvent(message = "before-close-2"))
            dispatcher.close()

            // Then: events delivered before close returned
            assertThat(recorder.eventCount()).isEqualTo(2)
        }

        @Test
        fun `should mark events enqueued after close as dropped`() {
            // Given
            val recorder = RecordingAppender()
            val dispatcher = FallbackDispatcher(recorder)
            dispatcher.close()

            // When: try to enqueue after close
            val accepted = dispatcher.enqueue(newTestLoggingEvent())

            // Then: rejected and counted
            assertThat(accepted).isFalse()
            assertThat(dispatcher.droppedEventCount).isEqualTo(1)
        }

        @Test
        fun `should count the in-flight event and every queued event as dropped if shutdown times out`() {
            // What is to be tested? The exact loss accounting of a forced
            //   shutdown: the event the worker has already taken off the
            //   queue and is stuck delivering (the in-flight event) plus
            //   every event still queued must each be counted as dropped
            //   exactly once.
            // How will the test case be deemed successful and why? Successful
            //   if a dispatcher whose worker is pinned in doAppend (an
            //   uninterruptible block, surviving close()'s worker
            //   interrupt) reports exactly 5 dropped events after close():
            //   the in-flight trigger plus the 4 queued ones. The anchor
            //   via inAppend makes the count deterministic - the worker
            //   cannot take a second event while pinned. A >= 1 assertion
            //   would let the in-flight event silently fall out of the
            //   balance (only queue drops would satisfy it).
            // Why is it important to test this test case? A silent loss
            //   during shutdown would mean operators trust their fallback
            //   captures everything, while in fact a slow disk at shutdown
            //   time silently discards events - for audit or error logs
            //   the in-flight one is typically the very event that
            //   triggered the shutdown investigation.

            // Given: an uninterruptibly blocking appender, short shutdown timeout
            val blockingAppender = BlockingAppender(interruptible = false)
            val dispatcher =
                FallbackDispatcher(
                    fallbackAppender = blockingAppender,
                    shutdownTimeoutMs = 100,
                )

            // First enqueue a single event and wait for the worker to
            // pull it off the queue and enter doAppend. This anchors
            // the worker in a known blocked state before we add more
            // events to the queue.
            dispatcher.enqueue(newTestLoggingEvent(message = "trigger"))
            pollUntil { blockingAppender.inAppend.get() }

            // Now fill the queue with events that cannot drain.
            (1..4).forEach {
                dispatcher.enqueue(newTestLoggingEvent(message = "stuck-$it"))
            }

            // When: close (the worker is pinned on the trigger event and
            //   ignores the interrupt, so it cannot drain anything)
            dispatcher.close()

            // Then: exactly 5 events are accounted as dropped - the
            //   in-flight trigger plus the 4 that were still queued.
            assertThat(dispatcher.droppedEventCount).isEqualTo(5L)

            // Cleanup: unblock so the worker thread can exit - and verify
            // that the trigger event, although close() claimed it, is not
            // double-counted when the worker finally finishes its append.
            blockingAppender.unblock()
            pollUntil { blockingAppender.appendCount.get() == 1 }
            assertThat(dispatcher.droppedEventCount).isEqualTo(5L)
        }

        @Test
        fun `should count an event as dropped when the fallback appender throws`() {
            // What is to be tested? Whether an event whose doAppend throws
            //   is accounted as dropped instead of silently vanishing -
            //   the dispatcher swallows the exception (log-storm safety),
            //   but the loss itself must reach the operator's counter.
            // How will the test case be deemed successful and why? Successful
            //   if the dropped count reaches exactly 1 for one failed
            //   event and the dispatcher keeps working afterwards.
            // Why is it important to test this test case? Before this
            //   contract existed, a fallback appender that throws (full
            //   disk, closed stream) lost every event without any trace
            //   in droppedEventCount - the loss metric lied precisely in
            //   the scenario it exists for.

            // Given: an appender whose doAppend always throws. It overrides
            //   doAppend directly because AppenderBase.doAppend would
            //   swallow exceptions from append() before the dispatcher
            //   could see them.
            val throwingAppender =
                object : AppenderBase<ILoggingEvent>() {
                    override fun doAppend(eventObject: ILoggingEvent): Unit = throw RuntimeException("simulated fallback failure")

                    override fun append(event: ILoggingEvent) = error("unreachable")
                }
            val dispatcher = FallbackDispatcher(throwingAppender)
            try {
                // When
                dispatcher.enqueue(newTestLoggingEvent(message = "doomed"))

                // Then: the failed delivery is counted as a drop
                pollUntil { dispatcher.droppedEventCount == 1L }
            } finally {
                dispatcher.close()
            }
            assertThat(dispatcher.droppedEventCount).isEqualTo(1L)
        }

        @Test
        fun `should not change the dropped count when closed twice`() {
            // What is to be tested? Whether close() is idempotent: the
            //   appender's stop() may run more than once during Logback
            //   context teardown, and each additional close() used to
            //   re-count the still-queued events as dropped.
            // How will the test case be deemed successful and why? Successful
            //   if the droppedEventCount observed after the first close()
            //   is unchanged after a second close(). This pins the
            //   drain-and-clear accounting: events are counted exactly once.
            // Why is it important to test this test case? The dropped count
            //   feeds an operator warning and a loss metric; double
            //   counting turns the primary loss-diagnostics signal into a
            //   lie precisely during shutdown investigations.

            // Given: a dispatcher whose worker is anchored in a blocked
            //   doAppend so events remain queued at close time
            val blockingAppender = BlockingAppender()
            val dispatcher =
                FallbackDispatcher(
                    fallbackAppender = blockingAppender,
                    shutdownTimeoutMs = 100,
                )
            dispatcher.enqueue(newTestLoggingEvent(message = "trigger"))
            pollUntil { blockingAppender.inAppend.get() }
            (1..3).forEach { dispatcher.enqueue(newTestLoggingEvent(message = "stuck-$it")) }

            // When
            dispatcher.close()
            val afterFirstClose = dispatcher.droppedEventCount
            dispatcher.close()

            // Then
            assertThat(afterFirstClose).isGreaterThanOrEqualTo(1L)
            assertThat(dispatcher.droppedEventCount).isEqualTo(afterFirstClose)

            // Cleanup
            blockingAppender.unblock()
        }
    }

    @Nested
    inner class `Worker death` {
        @Test
        fun `should report a worker death and count the in-flight event as dropped`() {
            // What is to be tested? Whether a worker killed by an Error
            //   from doAppend (only Exceptions are handled in place) is
            //   surfaced via onWorkerDeath and the event it carried is
            //   counted as dropped.
            // How will the test case be deemed successful and why? Successful
            //   if the hook receives the Error and droppedEventCount
            //   reaches exactly 1.
            // Why is it important to test this test case? Without the
            //   hook, a dead fallback worker looks like a full queue -
            //   operators would tune queue sizes instead of finding the
            //   dead thread.

            // Given: an appender whose doAppend dies with an Error
            val death = AtomicReference<Throwable?>()
            val dyingAppender =
                object : AppenderBase<ILoggingEvent>() {
                    override fun doAppend(eventObject: ILoggingEvent): Unit = throw AssertionError("simulated fallback death")

                    override fun append(event: ILoggingEvent) = error("unreachable")
                }
            val dispatcher =
                FallbackDispatcher(
                    fallbackAppender = dyingAppender,
                    onWorkerDeath = { death.set(it) },
                )
            try {
                // When
                dispatcher.enqueue(newTestLoggingEvent(message = "doomed"))

                // Then
                pollUntil { death.get() != null }
                assertThat(death.get()).hasMessage("simulated fallback death")
                assertThat(dispatcher.droppedEventCount).isEqualTo(1L)

                // And: the dispatcher left the accepting state - a later
                // enqueue is rejected and counted instead of stranding in
                // a queue no worker will ever drain
                assertThat(dispatcher.enqueue(newTestLoggingEvent(message = "after-death"))).isFalse()
                assertThat(dispatcher.droppedEventCount).isEqualTo(2L)
            } finally {
                dispatcher.close()
            }
        }

        @Test
        fun `should count queued events as dropped when the worker dies`() {
            // What is to be tested? The queue accounting of a worker
            //   death: events queued behind the dying delivery must be
            //   counted as dropped by the death handler itself, not first
            //   at some later close().
            // How will the test case be deemed successful and why? Successful
            //   if after the death both the in-flight and the queued event
            //   are in droppedEventCount. The latch pins the queued event
            //   behind the in-flight one deterministically.
            // Why is it important to test this test case? Before the fix,
            //   queued events stayed uncounted (and new ones kept being
            //   accepted) until shutdown - in a long-lived process the
            //   loss stayed invisible to operators indefinitely.

            // Given: an appender that parks, then dies with an Error
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val death = AtomicReference<Throwable?>()
            val dyingAppender =
                object : AppenderBase<ILoggingEvent>() {
                    init {
                        context = testContext
                        start()
                    }

                    override fun append(event: ILoggingEvent) {
                        entered.countDown()
                        release.await()
                        throw AssertionError("simulated fallback death")
                    }
                }
            val dispatcher =
                FallbackDispatcher(
                    fallbackAppender = dyingAppender,
                    onWorkerDeath = { death.set(it) },
                )
            try {
                // When: one event in flight, one queued behind it
                dispatcher.enqueue(newTestLoggingEvent(message = "in-flight"))
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue()
                dispatcher.enqueue(newTestLoggingEvent(message = "queued"))
                release.countDown()
                pollUntil { death.get() != null }

                // Then: the death handler counted the in-flight and the
                // queued event
                assertThat(dispatcher.droppedEventCount).isEqualTo(2L)
            } finally {
                dispatcher.close()
            }
        }
    }
}
