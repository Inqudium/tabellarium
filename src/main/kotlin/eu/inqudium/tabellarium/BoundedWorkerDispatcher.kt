package eu.inqudium.tabellarium

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * The one bounded-queue-plus-single-worker skeleton beneath both
 * asynchronous hand-offs of the pipeline: [SendDispatcher] (caller →
 * `producer.send`) and [FallbackDispatcher] (any thread → the fallback
 * appender). Everything the two have in common lives here exactly once
 * - the queue, the worker, the accepting state, the in-flight ownership
 * protocol, the death handler, the two-phase close, the reentry mark -
 * and the two differences are the two abstract methods: what
 * *delivering* an item means ([deliver]) and what happens to an item
 * that will never be delivered ([reject]).
 *
 * Rationale: the two dispatchers used to be near-copies whose KDoc
 * cross-referenced each other ("mirrors", "the canonical description
 * lives there") - and they diverged in exactly one close-budget detail
 * that the 2026-09-07 defect analysis had to repair. A prose promise
 * that two pieces of code stay identical does not survive change; one
 * class does (finding 1 of the 2026-09-07 architecture review).
 *
 * ## The protocol, once
 *
 * - **Hand-off** ([offer]) is O(1) and never blocks: a full queue or a
 *   dispatcher that no longer accepts rejects the item on the caller.
 *   The check-then-act window between the `running` test and the offer
 *   is closed by re-checking and reclaiming the item.
 * - **In-flight ownership**: the worker records the item it took off
 *   the queue in [inFlight]; whoever wins the compare-and-set accounts
 *   for it exactly once - the worker on delivery success or failure, a
 *   forced [close] when the worker did not finish in time. Without
 *   this, precisely the item in flight at a forced shutdown would
 *   vanish from every accounting.
 * - **Worker death** (an [Error] escaping [deliver] - [Exception]s are
 *   handled in place): leave the accepting state FIRST (with the worker
 *   gone, anything accepted would strand in a queue nothing drains),
 *   then reject the in-flight item and everything queued, then surface
 *   the death through the hook. Later offers are rejected on the
 *   caller. [workerDied] lets a subclass name the terminal cause.
 * - **Two-phase close**: the worker keeps DELIVERING for the whole
 *   drain budget; only then is it interrupted, with a bounded grace for
 *   a delivery parked in interruptible I/O. Invariant: the interrupt
 *   ends the drain (an interrupted worker delivers at most one more
 *   item), so it must never come before the budget has been used. An
 *   interrupt of the *closing* thread ends its waits early, never the
 *   accounting, and is restored before returning.
 * - **Reentry mark**: the worker sets the appender's [reentryGuard]
 *   once for its lifetime, so anything delivered work logs through
 *   SLF4J from this thread is dropped by [KafkaAppender.append] instead
 *   of looping back into a queue.
 *
 * Safety: the worker thread starts in the constructor and therefore
 * sees `this` before a subclass has initialized its own state - but it
 * touches subclass state only through [deliver]/[reject], which need an
 * item, and items can only arrive through [offer] after construction.
 *
 * @param threadName Name of the daemon worker thread.
 * @param queueCapacity Maximum queued items; the bound that keeps
 *                      memory finite and makes overflow a visible
 *                      rejection instead of growth.
 * @param drainTimeoutMs Time [close] lets the worker drain by
 *                       delivering before interrupting it.
 * @param reentryGuard The appender's per-thread reentry guard; null
 *                     disables the marking (tests).
 * @param onWorkerDeath Invoked after a worker death has been accounted
 *                      for, so the owner can report it - a dead worker
 *                      must not masquerade as a merely slow consumer.
 */
internal abstract class BoundedWorkerDispatcher<T : Any>(
    threadName: String,
    queueCapacity: Int,
    private val drainTimeoutMs: Long,
    private val reentryGuard: ThreadLocal<Boolean>?,
    private val onWorkerDeath: (Throwable) -> Unit,
) : AutoCloseable {
    /** Why an item will never be delivered; the subclass decides how to account for it. */
    protected enum class Rejection {
        /** The queue was full at hand-off time. */
        QUEUE_FULL,

        /** Offered after [close] or after a worker death; see [workerDied] for which. */
        NOT_ACCEPTING,

        /** In flight or queued when the worker died. */
        WORKER_DEATH,

        /** Still in flight or queued when the close budget expired. */
        SHUTDOWN_REMAINDER,

        /** [deliver] threw, and no forced close had claimed the item first. */
        DELIVERY_FAILED,
    }

    private val queue: LinkedBlockingQueue<T> = LinkedBlockingQueue(queueCapacity)

    /** The item the worker has taken off the queue but not yet finished delivering; see the class KDoc. */
    private val inFlight = AtomicReference<T>()

    @Volatile
    private var running = true

    /**
     * True once the worker died: the dispatcher has permanently lost its
     * only worker and can never deliver again. Lets a subclass tell a
     * post-death rejection from a post-close one.
     */
    @Volatile
    protected var workerDied: Boolean = false
        private set

    /**
     * Ensures the close sequence runs exactly once: a second [close]
     * (the appender's stop may be invoked repeatedly during context
     * teardown) must not re-account the remaining queue or re-join the
     * worker.
     */
    private val closeExecuted = AtomicBoolean(false)

    private val worker: Thread =
        Thread(::runWorker, threadName).apply {
            isDaemon = true
            setUncaughtExceptionHandler { _, throwable ->
                workerDied = true
                running = false
                inFlight.getAndSet(null)?.let { reject(it, Rejection.WORKER_DEATH) }
                while (true) {
                    val item = queue.poll() ?: break
                    reject(item, Rejection.WORKER_DEATH)
                }
                onWorkerDeath(throwable)
            }
            start()
        }

    /** Current queue depth, for the gauges a subclass registers. */
    protected fun queueSize(): Int = queue.size

    /**
     * Hands [item] to the worker. Returns `true` if it was queued,
     * `false` if it was rejected on the caller (full queue, or no
     * longer accepting) - in which case [reject] has already run.
     */
    protected fun offer(item: T): Boolean {
        if (!running) {
            reject(item, Rejection.NOT_ACCEPTING)
            return false
        }
        if (!queue.offer(item)) {
            reject(item, Rejection.QUEUE_FULL)
            return false
        }
        // Close the check-then-act window against close() and against
        // the death handler: if either finished its final drain between
        // the running check and the offer, the item would be neither
        // delivered nor accounted for. Re-check and reclaim.
        if (!running && queue.remove(item)) {
            reject(item, Rejection.NOT_ACCEPTING)
            return false
        }
        return true
    }

    override fun close() {
        if (!closeExecuted.compareAndSet(false, true)) {
            return
        }
        running = false
        // Phase 1: graceful drain. The worker's poll(100, MS) wakes up on
        // its next timeout, sees running=false, enters the drain loop and
        // keeps delivering until the queue is empty.
        // Invariant: the drain gets the whole budget before any interrupt
        // - the interrupt ends the drain (see the class KDoc).
        var interrupted = false
        try {
            worker.join(drainTimeoutMs)
        } catch (_: InterruptedException) {
            interrupted = true
        }
        if (worker.isAlive) {
            // Phase 2: forced exit. Interrupt to wake the worker from
            // poll() or from an interruptible delivery, and give the
            // interrupt a bounded grace to take effect.
            // Rationale: whatever the worker is still doing after that
            // (parked in non-interruptible I/O) is its own problem - it
            // is a daemon thread, so the JVM can still exit.
            worker.interrupt()
            // CAUTION: Thread.join(0) means "wait forever", not "do not
            // wait" - the grace is a positive constant. Skip the wait if
            // we were interrupted ourselves.
            if (!interrupted) {
                try {
                    worker.join(INTERRUPT_GRACE_MS)
                } catch (_: InterruptedException) {
                    interrupted = true
                }
            }
        }
        // Claim the in-flight item, then everything still queued.
        // Invariant: each item is accounted exactly once - if the
        // surviving worker still completes the delivery, its own CAS
        // fails and nothing is counted twice (the conservative direction).
        // Rationale: drain rather than read queue.size, so the items are
        // released and cannot be re-accounted by a later call.
        inFlight.getAndSet(null)?.let { reject(it, Rejection.SHUTDOWN_REMAINDER) }
        while (true) {
            val item = queue.poll() ?: break
            reject(item, Rejection.SHUTDOWN_REMAINDER)
        }
        if (interrupted) {
            Thread.currentThread().interrupt()
        }
    }

    private fun runWorker() {
        // Safety: set once for the worker's lifetime - it never
        // legitimately logs through the appender, so everything raised on
        // this thread is a loop and must be dropped.
        reentryGuard?.set(true)
        while (running) {
            val item =
                try {
                    queue.poll(100, TimeUnit.MILLISECONDS)
                } catch (_: InterruptedException) {
                    // Forced shutdown: exit immediately rather than pull
                    // another item that could then park in delivery as a
                    // ghost; close() accounts for what remains.
                    Thread.currentThread().interrupt()
                    return
                } ?: continue
            deliverGuarded(item)
            if (Thread.currentThread().isInterrupted) {
                return
            }
        }
        // Graceful drain: running=false, no interrupt. Keep delivering;
        // close() waits for this within its budget.
        while (true) {
            val item = queue.poll() ?: return
            deliverGuarded(item)
            if (Thread.currentThread().isInterrupted) {
                return
            }
        }
    }

    private fun deliverGuarded(item: T) {
        inFlight.set(item)
        try {
            deliver(item)
            inFlight.compareAndSet(item, null)
        } catch (e: Exception) {
            // Safety: a failing delivery must not kill the worker -
            // account for the item (unless a forced close already claimed
            // it) and keep going. An InterruptedException converted from
            // a blocking delivery is the shutdown signal - preserve it.
            if (inFlight.compareAndSet(item, null)) {
                reject(item, Rejection.DELIVERY_FAILED)
            }
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }

    /** Delivers one item on the worker thread. May block; may throw (the item is then rejected as [Rejection.DELIVERY_FAILED]). */
    protected abstract fun deliver(item: T)

    /**
     * Accounts for an item that will never be delivered. Called on the
     * caller's thread (hand-off rejections), the worker (delivery
     * failure, death) or the closing thread (shutdown remainder); must
     * not block and must tolerate being called from any of them.
     */
    protected abstract fun reject(
        item: T,
        rejection: Rejection,
    )

    companion object {
        /**
         * How long [close] waits after interrupting the worker for the
         * interrupt to take effect before accounting for the remainder
         * itself. Comes on top of the drain budget.
         */
        const val INTERRUPT_GRACE_MS: Long = 500
    }
}
