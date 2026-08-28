package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class FallbackDispatcherTest {
    // -- Test fixtures --------------------------------------------------

    /**
     * LoggerContext shared by the test appenders. Without setting it on
     * AppenderBase instances, Logback emits "No context given for ..."
     * warnings to stderr on every doAppend.
     */
    private val testContext =
        ch.qos.logback.classic
            .LoggerContext()

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

    /** Appender that blocks on each append until released. */
    private inner class BlockingAppender : AppenderBase<ILoggingEvent>() {
        private val release = CountDownLatch(1)
        val appendCount = AtomicInteger(0)

        /**
         * True while a thread is parked in [append] waiting on the
         * release latch. Used by tests to deterministically wait for
         * the dispatcher worker to enter the blocking call.
         */
        val inAppend =
            java.util.concurrent.atomic
                .AtomicBoolean(false)

        init {
            context = testContext
            start()
        }

        override fun append(event: ILoggingEvent) {
            inAppend.set(true)
            try {
                release.await()
                appendCount.incrementAndGet()
            } finally {
                inAppend.set(false)
            }
        }

        fun unblock() = release.countDown()
    }

    /**
     * Polls [condition] until it returns true or the timeout elapses.
     * Cheaper than introducing the awaitility dependency for the few
     * places we need it.
     */
    private fun pollUntil(
        timeoutMs: Long = 2000,
        intervalMs: Long = 10,
        condition: () -> Boolean,
    ) {
        val deadline = System.nanoTime() + timeoutMs * 1_000_000
        while (System.nanoTime() < deadline) {
            if (condition()) return
            Thread.sleep(intervalMs)
        }
        throw AssertionError("Condition did not become true within ${timeoutMs}ms")
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
        fun `should count remaining queued events as dropped if shutdown times out`() {
            // What is to be tested? Whether shutdown with a hung worker
            //   correctly attributes the remaining queued events to the
            //   dropped count. This is the failure mode where close()
            //   itself returns within its timeout but cannot drain
            //   everything.
            // How will the test case be deemed successful and why? Successful
            //   if a dispatcher whose worker is blocked has at least one
            //   queued event counted as dropped after close(). The exact
            //   count varies with scheduling: the worker may have processed
            //   one event before blocking, may have started a graceful
            //   drain pass before the timeout, etc. The contract we pin
            //   down is "blocked appender + queued events ⇒ dropped count
            //   is positive", which is what operators need for diagnostics.
            // Why is it important to test this test case? A silent loss
            //   during shutdown would mean operators trust their fallback
            //   captures everything, while in fact a slow disk at shutdown
            //   time silently discards events. Counting them allows the
            //   appender's stop() to surface the loss as a warning.

            // Given: blocking appender, short shutdown timeout
            val blockingAppender = BlockingAppender()
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

            // When: close (the worker is stuck on the trigger event)
            dispatcher.close()

            // Then: at least one event was counted as dropped.
            //   The exact count depends on whether the worker managed
            //   to grab additional events during the graceful-drain
            //   window of close(), which is intrinsically racy. The
            //   guaranteed property is that the dispatcher does not
            //   silently swallow events when its worker is hung.
            assertThat(dispatcher.droppedEventCount).isGreaterThanOrEqualTo(1L)

            // Cleanup: unblock so the worker thread can exit
            blockingAppender.unblock()
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
    inner class `Synchronous test mode` {
        @Test
        fun `should invoke the fallback appender directly when synchronous is true`() {
            // Given
            val recorder = RecordingAppender()
            val dispatcher = FallbackDispatcher(recorder, synchronous = true)

            // When
            dispatcher.enqueue(newTestLoggingEvent(message = "sync"))

            // Then: no polling needed; the event is already there
            assertThat(recorder.eventCount()).isEqualTo(1)
            assertThat(recorder.events[0].formattedMessage).isEqualTo("sync")
        }

        @Test
        fun `should invoke the fallback appender on the caller's thread when synchronous is true`() {
            // What is to be tested? Whether the synchronous flag truly
            //   bypasses the worker thread: the appender's append()
            //   must run on the thread that called enqueue(), not on
            //   a separate worker thread.
            // How will the test case be deemed successful and why? Successful
            //   if the thread captured inside append() is identical to
            //   the test thread. This is a stronger invariant than
            //   "no thread named X exists" - that approach is polluted
            //   by other tests' workers when the suite runs together.
            // Why is it important to test this test case? A regression
            //   that started the worker thread anyway would re-introduce
            //   asynchronous delivery in tests that rely on synchronous
            //   semantics for their assertions.

            // Given: a synchronous dispatcher and an appender that
            //   records the calling thread
            val callerThread = AtomicReference<Thread?>()
            val captureAppender =
                object : AppenderBase<ILoggingEvent>() {
                    init {
                        context = testContext
                        start()
                    }

                    override fun append(event: ILoggingEvent) {
                        callerThread.set(Thread.currentThread())
                    }
                }
            val dispatcher = FallbackDispatcher(captureAppender, synchronous = true)
            try {
                // When
                dispatcher.enqueue(newTestLoggingEvent())

                // Then: append ran on the test thread
                assertThat(callerThread.get()).isEqualTo(Thread.currentThread())
            } finally {
                dispatcher.close()
            }
        }
    }
}
