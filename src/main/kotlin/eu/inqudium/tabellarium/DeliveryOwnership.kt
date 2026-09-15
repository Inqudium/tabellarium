package eu.inqudium.tabellarium

import java.util.concurrent.atomic.AtomicInteger

/**
 * Who owns the outcome of one event's Kafka delivery, shared between
 * the [SendDispatcher] (overflow, shutdown, worker death) and the
 * [ResilientMessageSender] (throttle, breaker, `producer.send`, its
 * callback). Three states, every transition a compare-and-set, so
 * whoever wins accounts for the event exactly once and everyone else
 * stands down:
 *
 * ```
 * PENDING ──tryHandOff()──▶ HANDED_OFF ──tryDivertAfterSend()──▶ DIVERTED
 *    │                                                              ▲
 *    └──────────── tryDivert() / tryDivertAfterSend() ──────────────┘
 * ```
 *
 * - **PENDING**: nothing has happened to the event yet. It may be
 *   diverted by anyone: the dispatcher's rejections - including the
 *   forced close's claim of the in-flight item - and the sender's
 *   gates before `producer.send`.
 * - **HANDED_OFF**: `producer.send` returned without a synchronous
 *   failure; the record is in the client's buffer and the Kafka
 *   callback owns the outcome. From here only the callback may divert
 *   ([tryDivertAfterSend], on an asynchronous error). A forced close
 *   that reaches the in-flight item after this point stands down: the
 *   event is on its way to Kafka and must not reach the fallback as a
 *   `shutdown` remainder as well
 *   (`docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md`, M-2 -
 *   the two-state claim let the close divert a record that
 *   `producer.send` had already accepted).
 * - **DIVERTED**: routed to the fallback (or dropped, when none is
 *   configured) and counted as `events.fallback`. Terminal.
 *
 * The one race that no state can close: a forced close that claims
 * the item while the worker is *inside* `producer.send`, after the
 * client accepted the record but before the worker could mark the
 * hand-off. The record is then in the producer and in the fallback.
 * The state still keeps the accounting exclusive - [tryHandOff] fails
 * and the sender does not count the event as dispatched - and the
 * window is the width of `producer.send`'s return path, entered only
 * when the worker is still alive after the close budget and the
 * interrupt grace.
 *
 * Detached from [SendDispatcher.PendingSend] on purpose: the Kafka
 * callback retains this object until the client completes the record
 * (under a slow broker up to `delivery.timeout.ms`), and it must not
 * keep the serialized payload reachable for that long.
 */
internal class DeliveryOwnership {
    private val state = AtomicInteger(PENDING)

    /**
     * Diverts an event nothing has been done with yet: the dispatcher's
     * rejections and the sender's gates before `producer.send`. False
     * once the event was handed off or already diverted.
     */
    fun tryDivert(): Boolean = state.compareAndSet(PENDING, DIVERTED)

    /**
     * Records that `producer.send` accepted the record. False when a
     * forced close diverted the event in the meantime - the caller then
     * leaves the accounting to that diversion.
     */
    fun tryHandOff(): Boolean = state.compareAndSet(PENDING, HANDED_OFF)

    /**
     * The callback's diversion on an error the client reported: either
     * synchronously inside `producer.send` (the event is still PENDING)
     * or asynchronously after the hand-off. False when the event was
     * already diverted - by a forced close before the hand-off, or by
     * a second callback.
     */
    fun tryDivertAfterSend(): Boolean = state.compareAndSet(PENDING, DIVERTED) || state.compareAndSet(HANDED_OFF, DIVERTED)

    /**
     * Returns to PENDING. **Benchmark instrument only**: production
     * allocates one ownership per [SendDispatcher.PendingSend] and
     * never reuses it, and reusing one whose event is still in the
     * client would let two events share one outcome. `SenderPathBenchmark`
     * calls this on a pre-built ring so the measured send path carries
     * no per-event allocation that production puts on the caller path.
     */
    @JvmName("reset")
    internal fun reset() {
        state.set(PENDING)
    }

    private companion object {
        const val PENDING = 0
        const val HANDED_OFF = 1
        const val DIVERTED = 2
    }
}
