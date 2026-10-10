package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray

class DeliveryOwnershipTest {
    // -- Test fixtures --------------------------------------------------

    /**
     * Races [threads] callers of [transition] on ONE fresh ownership per
     * round, released together by a barrier, and returns the number of
     * callers that won in each of [rounds] rounds. [prepare] puts the
     * ownership into the state the transition starts from.
     *
     * Rationale: the transitions are compare-and-set operations whose
     * whole point is atomicity under contention, and the sequential
     * tests above cannot tell a CAS from a check-then-set. The race is
     * probabilistic by nature; many short spin-released rounds make a
     * non-atomic rewrite lose reliably within well under a second
     * (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 15).
     */
    private fun winnersPerRound(
        threads: Int = Runtime.getRuntime().availableProcessors().coerceIn(2, 8),
        rounds: Int = ROUNDS,
        prepare: (DeliveryOwnership) -> Unit = {},
        transition: (DeliveryOwnership) -> Boolean,
    ): List<Int> {
        val ownerships = List(rounds) { DeliveryOwnership().also(prepare) }
        val wins = AtomicIntegerArray(rounds)
        // The racers spin on the round counter instead of parking on a
        //   barrier: an unpark staggers the wake-ups by microseconds, a
        //   spin releases every racer within nanoseconds of the flip - the
        //   width of the check-then-set window the test exists to hit.
        //   Bounded to the core count so the spinners never wait for a
        //   descheduled peer.
        val round = AtomicInteger(-1)
        val finished = AtomicInteger(0)
        val abort = AtomicBoolean(false)
        val pool = Executors.newFixedThreadPool(threads)
        try {
            val racers =
                (1..threads).map {
                    pool.submit {
                        for (r in 0 until rounds) {
                            while (round.get() < r) {
                                if (abort.get()) return@submit
                                Thread.onSpinWait()
                            }
                            if (transition(ownerships[r])) wins.incrementAndGet(r)
                            finished.incrementAndGet()
                        }
                    }
                }
            val deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            for (r in 0 until rounds) {
                finished.set(0)
                round.set(r)
                while (finished.get() < threads) {
                    check(System.nanoTime() < deadlineNanos) { "the racers of round $r never finished" }
                    Thread.onSpinWait()
                }
            }
            racers.forEach { it.get(5, TimeUnit.SECONDS) }
        } finally {
            abort.set(true)
            pool.shutdownNow()
        }
        return List(rounds) { wins.get(it) }
    }

    private companion object {
        const val ROUNDS: Int = 5_000
    }

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Ownership transitions` {
        @Test
        fun `should let exactly one diversion win from the pending state`() {
            // What is to be tested? The exactly-once half of the ownership: two
            //   parties diverting the same pending event (a forced close and the
            //   sender's breaker gate, say) must resolve to one winner.
            // How will the test case be deemed successful and why? Successful
            //   if the first tryDivert returns true and every later diversion
            //   attempt - plain or after-send - returns false, and a hand-off
            //   is refused too.
            // Why is it important to test this test case? Every fallback
            //   delivery and every fallback metric hangs on this transition
            //   being single-winner; a regression here duplicates events in the
            //   fallback on every forced shutdown.

            // Given
            val ownership = DeliveryOwnership()

            // When / Then
            assertThat(ownership.tryDivert()).isTrue()
            assertThat(ownership.tryDivert()).isFalse()
            assertThat(ownership.tryDivertAfterSend()).isFalse()
            assertThat(ownership.tryHandOff()).isFalse()
        }

        @Test
        fun `should refuse a plain diversion once the event was handed off but let the callback divert`() {
            // What is to be tested? The M-2 transition table
            //   (docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md): after
            //   the hand-off, the forced close's tryDivert must stand down while
            //   the callback's tryDivertAfterSend may still divert - exactly
            //   once.
            // How will the test case be deemed successful and why? Successful
            //   if tryHandOff succeeds once, tryDivert then fails, a second
            //   hand-off fails, tryDivertAfterSend succeeds once and fails the
            //   second time.
            // Why is it important to test this test case? This is the state
            //   that separates "on its way to Kafka" from "still divertible";
            //   without it a record producer.send had accepted would also reach
            //   the fallback as a shutdown remainder.

            // Given
            val ownership = DeliveryOwnership()

            // When: the producer accepted the record
            assertThat(ownership.tryHandOff()).isTrue()

            // Then: no plain diversion, no second hand-off, one callback diversion
            assertThat(ownership.tryDivert()).isFalse()
            assertThat(ownership.tryHandOff()).isFalse()
            assertThat(ownership.tryDivertAfterSend()).isTrue()
            assertThat(ownership.tryDivertAfterSend()).isFalse()
        }

        @Test
        fun `should let the callback divert a pending event when the client reported synchronously`() {
            // What is to be tested? The callback's transition from PENDING: the
            //   Kafka client reports ApiExceptions synchronously inside send,
            //   before the sender could mark the hand-off, and that callback
            //   must still be able to divert.
            // How will the test case be deemed successful and why? Successful
            //   if tryDivertAfterSend succeeds on a pending ownership and the
            //   hand-off is refused afterwards.
            // Why is it important to test this test case? Without the PENDING
            //   branch the synchronous error path - metadata timeout, buffer
            //   exhausted, record too large - would lose its fallback delivery.

            // Given
            val ownership = DeliveryOwnership()

            // When / Then
            assertThat(ownership.tryDivertAfterSend()).isTrue()
            assertThat(ownership.tryHandOff()).isFalse()
            assertThat(ownership.tryDivert()).isFalse()
        }
    }

    @Nested
    inner class `Contended transitions` {
        @Test
        fun `should let exactly one of many concurrent diversions win a pending event`() {
            // What is to be tested? The atomicity of tryDivert under real
            //   contention: several parties diverting the same pending event
            //   at the same instant (a forced close on the closer thread and
            //   a dispatcher rejection, say) must resolve to exactly one
            //   winner - in every round, not just in sequence.
            // How will the test case be deemed successful and why? Successful
            //   if every one of 5000 spin-released rounds of up to 8 callers
            //   counts exactly one true. A check-then-set in place of the
            //   compare-and-set lets two callers win in some rounds, which the
            //   all-rounds assertion turns red; the sequential tests above
            //   cannot tell the two apart
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 15).
            // Why is it important to test this test case? Two winners mean one
            //   event in the fallback twice and events.fallback counting it
            //   twice, exactly on the forced-shutdown timeline the ownership
            //   exists for.

            // When
            val winners = winnersPerRound { it.tryDivert() }

            // Then
            assertThat(winners).hasSize(ROUNDS).containsOnly(1)
        }

        @Test
        fun `should let exactly one of many concurrent hand-offs win a pending event`() {
            // What is to be tested? The atomicity of tryHandOff under
            //   contention with itself, the transition the sender records the
            //   moment producer.send returns.
            // How will the test case be deemed successful and why? Successful
            //   if every round of concurrent hand-offs counts exactly one
            //   winner; a non-atomic rewrite lets several succeed in some
            //   rounds (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 15).
            // Why is it important to test this test case? Two hand-offs
            //   counting mean events.dispatched over-counts the same record,
            //   breaking the accepted = dispatched + fallback balance.

            // When
            val winners = winnersPerRound { it.tryHandOff() }

            // Then
            assertThat(winners).hasSize(ROUNDS).containsOnly(1)
        }

        @Test
        fun `should let exactly one of many concurrent callback diversions win a handed-off event`() {
            // What is to be tested? The atomicity of tryDivertAfterSend from
            //   HANDED_OFF - the callback's transition - under contention with
            //   itself, and the same from PENDING (the synchronous report).
            // How will the test case be deemed successful and why? Successful
            //   if every round counts exactly one winner from either starting
            //   state; the two-CAS chain in tryDivertAfterSend must stay
            //   single-winner as a whole
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 15).
            // Why is it important to test this test case? A second callback
            //   diversion would deliver the same failed record to the fallback
            //   twice while the counter reports one.

            // When: from HANDED_OFF, then from PENDING
            val fromHandedOff = winnersPerRound(prepare = { check(it.tryHandOff()) }) { it.tryDivertAfterSend() }
            val fromPending = winnersPerRound { it.tryDivertAfterSend() }

            // Then
            assertThat(fromHandedOff).hasSize(ROUNDS).containsOnly(1)
            assertThat(fromPending).hasSize(ROUNDS).containsOnly(1)
        }
    }
}
