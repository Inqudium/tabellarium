package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
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
    ) : BoundedWorkerDispatcher<String>(
            threadName = "probe-worker",
            queueCapacity = 16,
            drainTimeoutMs = 1000,
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
}
