package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.ThrowingSupplier
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Pins the shared parallel-close helper on its own: the budget is one
 * overall deadline (not one per task), a task that overruns it does not
 * hold the caller, and an interrupt of the caller ends the wait early
 * and is restored. Before this class the helper was covered only
 * through the registry and appender tests, which exercise the happy
 * path and the failure aggregation but not the deadline arithmetic.
 *
 * The blocking tasks park on latches the fixture releases in
 * `@AfterEach`, so no daemon closer thread outlives its test.
 */
class ParallelCloseTest {
    // -- Test fixtures --------------------------------------------------

    /** Latches every test parked a task on; released after the test. */
    private val gates = mutableListOf<CountDownLatch>()

    @AfterEach
    fun releaseGates() {
        gates.forEach { it.countDown() }
        gates.clear()
    }

    /** A task that blocks until its gate opens, remembering whether it ran. */
    private fun blockingTask(ran: AtomicBoolean): () -> Unit {
        val gate = CountDownLatch(1).also { gates += it }
        return {
            ran.set(true)
            gate.await()
        }
    }

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Budget semantics` {
        @Test
        fun `should return as soon as every task has finished`() {
            // What is to be tested? Whether closeInParallel runs every task
            //   and returns once all of them are done - without waiting out
            //   the budget when there is nothing left to wait for.
            // How will the test case be deemed successful and why? Successful
            //   if all tasks ran and the call returned well within the
            //   budget: the join loop must end on the closer threads, not
            //   on the deadline.
            // Why is it important to test this test case? The helper sits
            //   in every stop() of the appender; a helper that always
            //   waited out its budget would add seconds to every shutdown
            //   and would be invisible in the tests that only check the
            //   outcome.

            // Given: three tasks that finish immediately
            val ran = AtomicInteger(0)
            val tasks =
                (1..3).map {
                    "closer-$it" to {
                        ran.incrementAndGet()
                        Unit
                    }
                }

            // When
            val startNanos = System.nanoTime()
            closeInParallel(budgetMs = 5_000, tasks = tasks)
            val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000

            // Then: everything ran, and the call did not sit on the budget
            assertThat(ran.get()).isEqualTo(3)
            assertThat(elapsedMs).isLessThan(2_000)
        }

        @Test
        fun `should return after the budget when tasks overrun it`() {
            // What is to be tested? Whether tasks that never finish hold
            //   the caller only for the shared budget - the single overall
            //   deadline that keeps N stacked timeouts from overrunning a
            //   Kubernetes termination grace period.
            // How will the test case be deemed successful and why? Successful
            //   if the call returns after roughly the budget (not before,
            //   not much after) although two closers are still parked, and
            //   the quick task ran. Two parked closers, not one: the first
            //   join uses up the budget, so the second is reached with no
            //   remainder - the trap the code marks with CAUTION, because
            //   Thread.join(0) waits forever on a live thread and a
            //   negative timeout throws. The loop must stop before the
            //   remainder reaches zero; with one parked closer the guard
            //   would never be exercised.
            // Why is it important to test this test case? A hung producer
            //   close during a broker outage is exactly when this budget
            //   matters; a helper that waited forever would turn a
            //   bounded shutdown into a killed pod, and no other test
            //   parks a closer past the deadline.

            // Given: one task that finishes, two that park on their gates
            val quickRan = AtomicBoolean(false)
            val blockedRan1 = AtomicBoolean(false)
            val blockedRan2 = AtomicBoolean(false)
            val tasks =
                listOf(
                    "closer-quick" to { quickRan.set(true) },
                    "closer-blocked-1" to blockingTask(blockedRan1),
                    "closer-blocked-2" to blockingTask(blockedRan2),
                )

            // When: the call must come back on its own - a hang fails the
            //   test instead of the build
            val budgetMs = 300L
            val elapsedMs: Long =
                assertTimeoutPreemptively(
                    Duration.ofSeconds(5),
                    ThrowingSupplier {
                        val startNanos = System.nanoTime()
                        closeInParallel(budgetMs = budgetMs, tasks = tasks)
                        (System.nanoTime() - startNanos) / 1_000_000
                    },
                )

            // Then: every task started, the caller waited the budget and no more
            assertThat(quickRan.get()).isTrue()
            assertThat(blockedRan1.get()).isTrue()
            assertThat(blockedRan2.get()).isTrue()
            assertThat(elapsedMs).isGreaterThanOrEqualTo(budgetMs - 50)
            assertThat(elapsedMs).isLessThan(budgetMs + 1_500)
        }

        @Test
        fun `should return immediately for an empty task list`() {
            // What is to be tested? The degenerate input: nothing to close.
            // How will the test case be deemed successful and why? Successful
            //   if the call returns without waiting - the early return
            //   before any thread is started.
            // Why is it important to test this test case? A transport with
            //   no fallback and a registry rollback with no producers both
            //   reach the helper with an empty list; waiting the budget
            //   there would be a silent shutdown delay.

            // When
            val startNanos = System.nanoTime()
            closeInParallel(budgetMs = 5_000, tasks = emptyList())
            val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000

            // Then
            assertThat(elapsedMs).isLessThan(1_000)
        }
    }

    @Nested
    inner class `Interrupt handling` {
        @Test
        fun `should end the wait early and restore the interrupt when the caller is interrupted`() {
            // What is to be tested? Whether an interrupt of the thread that
            //   calls closeInParallel ends its wait before the budget and
            //   leaves the interrupt flag set for the caller's own
            //   shutdown logic.
            // How will the test case be deemed successful and why? Successful
            //   if the calling thread returns from the helper well before
            //   the budget after being interrupted, and observes its own
            //   interrupt flag as set right after the call. The helper
            //   catches InterruptedException from join and re-asserts the
            //   flag; swallowing it would hide the interrupt from the
            //   caller's remaining stop sequence.
            // Why is it important to test this test case? The appender's
            //   stop() may itself run on an interrupted thread (a
            //   container shutdown hook); a helper that ate the interrupt
            //   would let the following bounded waits run their full
            //   budgets after the caller had been told to hurry.

            // Given: a task that never finishes and a caller thread we can interrupt
            val blockedRan = AtomicBoolean(false)
            val tasks = listOf("closer-blocked" to blockingTask(blockedRan))
            val returned = CountDownLatch(1)
            val interruptedAfterReturn = AtomicBoolean(false)
            val caller =
                Thread {
                    closeInParallel(budgetMs = 10_000, tasks = tasks)
                    interruptedAfterReturn.set(Thread.currentThread().isInterrupted)
                    returned.countDown()
                }
            caller.isDaemon = true
            caller.start()
            pollUntil { blockedRan.get() }

            // When: the caller is interrupted while waiting on the closer
            caller.interrupt()

            // Then: it returns long before the budget, with the flag restored
            assertThat(returned.await(3, TimeUnit.SECONDS)).isTrue()
            assertThat(interruptedAfterReturn.get()).isTrue()
        }
    }
}
