package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class DeliveryOwnershipTest {
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
