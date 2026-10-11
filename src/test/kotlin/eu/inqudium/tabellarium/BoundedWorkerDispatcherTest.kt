package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class BoundedWorkerDispatcherTest {
    // -- Test fixtures --------------------------------------------------

    /**
     * Queue that parks the offer of [pausedItem] between the
     * dispatcher's `running` check and the actual insertion: it signals
     * [entered], waits for [resume], and only then inserts. Every other
     * operation behaves like a plain [LinkedBlockingQueue].
     */
    private class PausingQueue(
        capacity: Int,
        private val pausedItem: String,
        private val entered: CountDownLatch,
        private val resume: CountDownLatch,
    ) : LinkedBlockingQueue<String>(capacity) {
        override fun offer(e: String): Boolean {
            if (e == pausedItem) {
                entered.countDown()
                check(resume.await(5, TimeUnit.SECONDS)) { "the racing close/death never finished" }
            }
            return super.offer(e)
        }
    }

    /** Minimal concrete dispatcher that records what it delivers and rejects. */
    private class ProbeDispatcher(
        queue: PausingQueue,
        private val onDeliver: (String) -> Unit = {},
        onWorkerDeath: (Throwable) -> Unit = {},
        threadName: String = "probe-worker",
        drainTimeoutMs: Long = 1000,
    ) : BoundedWorkerDispatcher<String>(
            threadName = threadName,
            queueCapacity = 16,
            drainTimeoutMs = drainTimeoutMs,
            onWorkerDeath = onWorkerDeath,
            queueFactory = { queue },
        ) {
        val delivered: MutableList<String> = Collections.synchronizedList(mutableListOf())
        val rejections: MutableList<Pair<String, String>> = Collections.synchronizedList(mutableListOf())

        fun hand(item: String): Boolean = offer(item)

        fun depth(): Int = queueSize()

        override fun deliver(item: String) {
            onDeliver(item)
            delivered += item
        }

        override fun reject(
            item: String,
            rejection: Rejection,
        ) {
            rejections += item to rejection.name
        }
    }

    /** Every dispatcher a test created; closed after the test so no worker outlives it. */
    private val openDispatchers = mutableListOf<ProbeDispatcher>()

    @AfterEach
    fun closeDispatchers() {
        openDispatchers.forEach { it.close() }
    }

    /** Runs [offer] on a helper thread and returns its result holder and thread. */
    private fun offerOnHelper(offer: () -> Boolean): Pair<AtomicReference<Boolean>, Thread> {
        val result = AtomicReference<Boolean>()
        val thread = Thread({ result.set(offer()) }, "racing-caller").apply { start() }
        return result to thread
    }

    @Nested
    inner class `Offer racing the final drain` {
        @Test
        fun `should reject an item whose offer lands after close finished its final drain`() {
            // What is to be tested? Whether offer's re-check of `running` after a
            //   successful queue insertion reclaims an item that a close() could
            //   no longer see: the caller passed the first running check, close()
            //   then ran to completion (worker gone, queue drained), and only then
            //   did the insertion happen.
            // How will the test case be deemed successful and why? Successful if
            //   the late offer returns false, the item is rejected exactly once as
            //   NOT_ACCEPTING, nothing was delivered, and the queue is empty. The
            //   PausingQueue forces exactly this interleaving instead of hoping
            //   for it under contention; without the re-check the offer returns
            //   true and the item strands in a queue nothing drains
            //   (DEFECT_ANALYSIS-2026-10-10T15-52-19, finding 1).
            // Why is it important to test this test case? The re-check is the
            //   only code closing this check-then-act window; a regression would
            //   lose an event without any delivery, fallback or drop accounting.

            // Given: a dispatcher whose queue parks the offer of "late"
            val entered = CountDownLatch(1)
            val resume = CountDownLatch(1)
            val dispatcher =
                ProbeDispatcher(PausingQueue(16, "late", entered, resume)).also { openDispatchers += it }
            val (result, caller) = offerOnHelper { dispatcher.hand("late") }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            // When: close() completes its final drain while the offer is parked,
            // then the offer resumes
            dispatcher.close()
            resume.countDown()
            caller.join(5000)

            // Then: the item is reclaimed and accounted exactly once
            assertThat(caller.isAlive).isFalse()
            assertThat(result.get()).isFalse()
            assertThat(dispatcher.rejections).containsExactly("late" to "NOT_ACCEPTING")
            assertThat(dispatcher.delivered).isEmpty()
            assertThat(dispatcher.depth()).isZero()
        }

        @Test
        fun `should reject an item whose offer lands after the worker-death drain`() {
            // What is to be tested? The same window against the death handler:
            //   the caller passed the running check while the worker was still
            //   delivering, the worker then died and the handler drained the
            //   queue, and only then did the insertion happen.
            // How will the test case be deemed successful and why? Successful if
            //   the in-flight item is rejected as WORKER_DEATH, the late item
            //   exactly once as NOT_ACCEPTING, the late offer returns false and
            //   the queue is empty. The worker's delivery waits until the offer
            //   is parked and the offer resumes only after onWorkerDeath, which
            //   the handler calls after its final drain - so the interleaving is
            //   forced, not probabilistic.
            // Why is it important to test this test case? After a worker death
            //   nothing will ever drain the queue again; an item that slipped in
            //   would be lost silently, while the death handler is supposed to
            //   leave every item accounted for.

            // Given: a worker whose first delivery dies once the late offer is parked
            val entered = CountDownLatch(1)
            val deathHandled = CountDownLatch(1)
            val dispatcher =
                ProbeDispatcher(
                    PausingQueue(16, "late", entered, resume = deathHandled),
                    onDeliver = { item ->
                        if (item == "poison") {
                            check(entered.await(5, TimeUnit.SECONDS)) { "the late offer never parked" }
                            throw AssertionError("simulated worker death")
                        }
                    },
                    onWorkerDeath = { deathHandled.countDown() },
                ).also { openDispatchers += it }
            assertThat(dispatcher.hand("poison")).isTrue()

            // When: the late offer passes the running check while the worker is
            // still delivering, and resumes after the death handler's drain
            val (result, caller) = offerOnHelper { dispatcher.hand("late") }
            caller.join(5000)

            // Then: both items are accounted exactly once
            assertThat(caller.isAlive).isFalse()
            assertThat(deathHandled.count).isZero()
            assertThat(result.get()).isFalse()
            assertThat(dispatcher.rejections).containsExactlyInAnyOrder(
                "poison" to "WORKER_DEATH",
                "late" to "NOT_ACCEPTING",
            )
            assertThat(dispatcher.delivered).isEmpty()
            assertThat(dispatcher.depth()).isZero()
        }
    }

    @Nested
    inner class `Interrupts the worker does not own` {
        /** A queue that pauses nothing - the plain skeleton under test. */
        private fun plainQueue() = PausingQueue(16, "never", CountDownLatch(1), CountDownLatch(1))

        @Test
        fun `should keep delivering when a delivery leaves the interrupt flag set while no close is in progress`() {
            // What is to be tested? Whether the worker tells a spurious
            //   interrupt - a delivery that calls
            //   Thread.currentThread().interrupt() on its own, as a fallback
            //   appender or an interceptor might on an internal timeout -
            //   from the close signal, and keeps working instead of
            //   retiring with the dispatcher still accepting.
            // How will the test case be deemed successful and why? Successful
            //   if the item delivered after the one that set the flag is
            //   delivered too, no worker death was reported, nothing was
            //   rejected and a later offer is still accepted. A worker that
            //   exits on any interrupt flag leaves "second" in a queue
            //   nothing drains, with running still true: the dead worker
            //   masquerading as a slow consumer, until the queue fills and
            //   every event is rejected as full
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 26).
            // Why is it important to test this test case? Silent loss of the
            //   whole delivery capability with misattributed accounting is
            //   the failure the death hook exists to prevent; this is the one
            //   path around it.

            // Given: a worker whose first delivery sets the interrupt flag
            val deaths = Collections.synchronizedList(mutableListOf<Throwable>())
            val dispatcher =
                ProbeDispatcher(
                    plainQueue(),
                    onDeliver = { item -> if (item == "spurious") Thread.currentThread().interrupt() },
                    onWorkerDeath = { deaths += it },
                ).also { openDispatchers += it }

            // When: the flag-setting delivery, then an ordinary one
            assertThat(dispatcher.hand("spurious")).isTrue()
            pollUntil { dispatcher.delivered.contains("spurious") }
            assertThat(dispatcher.hand("second")).isTrue()

            // Then: the worker survived the spurious flag
            pollUntil { dispatcher.delivered.contains("second") }
            assertThat(dispatcher.delivered).containsExactly("spurious", "second")
            assertThat(dispatcher.rejections).isEmpty()
            assertThat(deaths).isEmpty()
            assertThat(dispatcher.hand("third")).isTrue()
            pollUntil { dispatcher.delivered.contains("third") }
        }

        @Test
        fun `should keep polling when an idle worker is interrupted from outside while no close is in progress`() {
            // What is to be tested? The same distinction on the other wait
            //   of the worker: an interrupt that wakes the queue poll while
            //   the dispatcher is still running (nothing in the library does
            //   this; a third party holding the thread reference could).
            // How will the test case be deemed successful and why? Successful
            //   if an item handed after the stray interrupt is still
            //   delivered and no death was reported. A worker that treats
            //   every InterruptedException from the poll as the close signal
            //   exits and strands the item.
            // Why is it important to test this test case? Same failure as the
            //   delivery case - a stranded queue behind an accepting
            //   dispatcher - reached through the poll instead of the delivery.

            // Given: the worker thread, learned from its first delivery
            val deaths = Collections.synchronizedList(mutableListOf<Throwable>())
            val workerThread = AtomicReference<Thread>()
            val dispatcher =
                ProbeDispatcher(
                    plainQueue(),
                    onDeliver = { workerThread.set(Thread.currentThread()) },
                    onWorkerDeath = { deaths += it },
                ).also { openDispatchers += it }
            assertThat(dispatcher.hand("first")).isTrue()
            pollUntil { dispatcher.delivered.contains("first") }

            // When: a stray interrupt hits the (now idle) worker, then an item follows
            workerThread.get().interrupt()
            assertThat(dispatcher.hand("second")).isTrue()

            // Then: the worker is still there to deliver it
            pollUntil { dispatcher.delivered.contains("second") }
            assertThat(dispatcher.rejections).isEmpty()
            assertThat(deaths).isEmpty()
        }

        @Test
        fun `should still end the drain on the close interrupt when the delivery leaves the flag set`() {
            // What is to be tested? The converse: the close() interrupt,
            //   observed after a delivery that re-set the flag (the
            //   well-behaved appender convention), must still end the
            //   worker - the spurious-interrupt tolerance is keyed on
            //   `running`, which close() clears first.
            // How will the test case be deemed successful and why? Successful
            //   if, with the first delivery parked interruptibly and a
            //   second item queued, close() with a short drain budget ends
            //   the worker within the grace: the parked delivery returns
            //   (interrupted, flag restored) and counts as delivered, the
            //   queued item is claimed by the close as SHUTDOWN_REMAINDER
            //   and never delivered, and the worker thread is gone
            //   afterwards. A worker that also cleared the close interrupt
            //   would drain the second item instead of exiting.
            // Why is it important to test this test case? The forced exit
            //   is what bounds a shutdown with a stuck fallback; the
            //   tolerance for stray interrupts must not weaken it.

            // Given: a delivery parked in an interruptible wait that restores the flag
            val parked = CountDownLatch(1)
            val workerThread = AtomicReference<Thread>()
            val dispatcher =
                ProbeDispatcher(
                    plainQueue(),
                    onDeliver = { item ->
                        if (item == "parked") {
                            workerThread.set(Thread.currentThread())
                            try {
                                CountDownLatch(1).apply { parked.countDown() }.await()
                            } catch (_: InterruptedException) {
                                Thread.currentThread().interrupt()
                            }
                        }
                    },
                    drainTimeoutMs = 50,
                ).also { openDispatchers += it }
            assertThat(dispatcher.hand("parked")).isTrue()
            assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue()
            assertThat(dispatcher.hand("queued")).isTrue()

            // When: closed with the worker parked - the budget expires, the interrupt follows
            dispatcher.close()

            // Then: the worker exited on the close interrupt; the queued item was claimed by the close
            workerThread.get().join(5_000)
            assertThat(workerThread.get().isAlive).isFalse()
            assertThat(dispatcher.delivered).containsExactly("parked")
            assertThat(dispatcher.rejections).containsExactly("queued" to "SHUTDOWN_REMAINDER")
        }
    }

    @Nested
    inner class `Drain budget validation` {
        @ParameterizedTest(name = "drainTimeoutMs = {0}")
        @ValueSource(longs = [0, -1])
        fun `should refuse a non-positive drain budget before starting a worker`(drainTimeoutMs: Long) {
            // What is to be tested? Whether the constructor rejects a drain
            //   budget that close() could not honor: Thread.join(0) waits
            //   forever and a negative timeout makes join throw before the
            //   remainder is accounted (DEFECT_ANALYSIS-2026-10-10T19-11-11,
            //   finding 9).
            // How will the test case be deemed successful and why? Successful
            //   if construction throws IllegalArgumentException naming the
            //   join(0) trap, and no worker thread with the dispatcher's name
            //   exists afterwards - the check must run before the thread
            //   starts, or a refused dispatcher would leak its worker.
            // Why is it important to test this test case? Production passes
            //   constants today; the guard is what turns a future caller's
            //   "0 means no drain" into a named error instead of a close()
            //   that never returns.

            // When / Then: refused by name
            assertThatThrownBy {
                ProbeDispatcher(
                    PausingQueue(16, "never", CountDownLatch(1), CountDownLatch(1)),
                    threadName = "refused-probe-worker",
                    drainTimeoutMs = drainTimeoutMs,
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("drainTimeoutMs must be positive")
                .hasMessageContaining("join(0)")
                .hasMessageContaining(drainTimeoutMs.toString())

            // And: no worker was started for the refused dispatcher
            assertThat(Thread.getAllStackTraces().keys.map { it.name }).doesNotContain("refused-probe-worker")
        }
    }
}
