package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Decouples invocation of the fallback [Appender] from caller threads -
 * primarily the Kafka producer's I/O thread, which must never block on
 * downstream logging.
 *
 * ## Why this exists
 *
 * Kafka's `producer.send` callback executes on the
 * `kafka-producer-network-thread`. If [ResilientMessageSender] called
 * `fallbackAppender.doAppend(event)` directly from that callback, a
 * blocking fallback appender (typically `FileAppender` under slow disk
 * conditions) would block the Kafka I/O thread. Since the Kafka client
 * has a single I/O thread per producer, blocking it stalls all
 * subsequent in-flight callbacks, including the `producer.send` calls
 * that the application's hot path performs.
 *
 * This dispatcher inserts a single-consumer queue between the Kafka
 * callback (and the hot-path's synchronous fallback path) and the
 * actual fallback appender. The caller [enqueue]s in O(1) without
 * blocking; a dedicated worker thread drains the queue and calls
 * `doAppend`.
 *
 * ## Drop policy
 *
 * The queue is **bounded**. If a fallback appender is slow enough that
 * the queue fills up, [enqueue] returns `false` immediately rather
 * than blocking. The dropped event is counted in [droppedEventCount]
 * for operator visibility. Dropping is the correct choice here:
 *
 * - The events are already in the *fallback* path, meaning Kafka
 *   delivery is already failing. The system is in a degraded state.
 * - Blocking the caller would mean either back-pressuring the hot
 *   path (the original problem) or back-pressuring the Kafka I/O
 *   thread (the problem this dispatcher exists to solve).
 * - An unbounded queue would grow until OOM.
 *
 * ## Lifecycle
 *
 * - Construction starts the worker thread immediately. The thread is
 *   marked daemon so it does not prevent JVM shutdown.
 * - [close] signals the worker to stop, waits up to [shutdownTimeoutMs]
 *   milliseconds for it to drain remaining events, then returns. Events
 *   still in the queue after the timeout are dropped (counted in
 *   [droppedEventCount]).
 *
 * @param fallbackAppender The appender to which events are dispatched.
 * @param queueCapacity Maximum number of events in flight. Default 1024
 *                      is a balance between memory (each event holds
 *                      references to MDC, throwable, etc.) and
 *                      tolerance for brief fallback slowness.
 * @param shutdownTimeoutMs Time allowed in [close] for the worker to
 *                          drain. Default 5 seconds.
 * @param onWorkerDeath Invoked when the worker thread dies from a
 *                      [Throwable] the delivery loop does not handle
 *                      (an [Error] such as OOM - [Exception]s are
 *                      handled in place). The appender reports this to
 *                      the status manager so a dead worker does not
 *                      masquerade as a merely slow fallback appender.
 */
internal class FallbackDispatcher(
    private val fallbackAppender: Appender<ILoggingEvent>,
    private val queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    private val shutdownTimeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS,
    private val onWorkerDeath: (Throwable) -> Unit = {},
) : AutoCloseable {
    private val queue: LinkedBlockingQueue<ILoggingEvent> = LinkedBlockingQueue(queueCapacity)
    private val droppedCount = AtomicLong(0)

    /**
     * The event the worker has taken off the queue but not yet finished
     * delivering. Owned via compare-and-set: exactly one party accounts
     * for it. The worker clears it on delivery success (nothing counted)
     * or counts it as dropped when `doAppend` throws; a forced [close]
     * claims and counts it when the worker did not finish in time.
     * Without this, precisely the event in flight at a forced shutdown
     * would vanish from the loss accounting - neither delivered nor
     * counted as dropped.
     */
    private val inFlight = AtomicReference<ILoggingEvent>()

    /**
     * Pluggable metrics hook. Defaults to [KafkaAppenderMetrics.NO_OP];
     * the appender replaces it via [setMetrics] when a Micrometer
     * registry is bound. Volatile because the setter may be called
     * from a Spring bootstrap thread while the worker thread is
     * concurrently dropping queued events.
     */
    @Volatile
    private var metrics: KafkaAppenderMetrics = KafkaAppenderMetrics.NO_OP

    /**
     * Replaces the metrics implementation and re-registers the queue
     * gauges with the new one. Idempotent - multiple calls simply
     * replace the previous instance and re-register the gauges with
     * the latest one.
     */
    fun setMetrics(metrics: KafkaAppenderMetrics) {
        this.metrics = metrics
        // Re-register the gauges so they bind to the new
        // implementation (typically a Micrometer-backed one). The
        // queue size supplier is a method reference that always
        // reads the current queue state.
        metrics.registerFallbackQueueGauges(queueSize = queue::size, capacity = queueCapacity)
    }

    @Volatile
    private var running = true

    /**
     * Ensures the close sequence runs exactly once: a second [close]
     * (the appender's stop may be invoked repeatedly during context
     * teardown) must not re-count the remaining queue as dropped or
     * re-join the worker.
     */
    private val closeExecuted = AtomicBoolean(false)

    private val worker: Thread =
        Thread(::runWorker, "kafka-appender-fallback-dispatcher").apply {
            isDaemon = true
            // An Error escaping the delivery loop kills the worker.
            // Leave the accepting state FIRST - with the worker gone,
            // anything accepted would strand in a queue nothing ever
            // drains - then count the event it was carrying plus
            // everything queued as dropped, and surface the death -
            // see onWorkerDeath. Later enqueue calls see
            // running=false and count on the caller.
            setUncaughtExceptionHandler { _, throwable ->
                running = false
                val m = metrics
                inFlight.getAndSet(null)?.let {
                    droppedCount.incrementAndGet()
                    m.fallbackDispatcherDropped()
                }
                while (queue.poll() != null) {
                    droppedCount.incrementAndGet()
                    m.fallbackDispatcherDropped()
                }
                onWorkerDeath(throwable)
            }
            start()
        }

    /**
     * Number of events lost by this dispatcher: the queue was full when
     * [enqueue] was called, the fallback appender's `doAppend` threw,
     * the worker died with events queued or in flight, or [close] timed
     * out before the queue - including the one event the worker had in
     * flight - drained. Read from any thread.
     */
    val droppedEventCount: Long
        get() = droppedCount.get()

    /**
     * Hands [event] off to the worker thread for delivery to the
     * fallback appender. Returns immediately:
     *
     * - `true` if the event was queued.
     * - `false` if the queue was full (event dropped) or the dispatcher
     *   has been [close]d.
     */
    fun enqueue(event: ILoggingEvent): Boolean {
        if (!running) {
            droppedCount.incrementAndGet()
            metrics.fallbackDispatcherDropped()
            return false
        }
        val accepted = queue.offer(event)
        if (!accepted) {
            droppedCount.incrementAndGet()
            metrics.fallbackDispatcherDropped()
            return false
        }
        // Close the check-then-act window against close(): if the
        // dispatcher was closed between the running check above and the
        // offer, the event may have been added after close() finished its
        // final drain accounting - it would then be neither delivered nor
        // counted. Re-check and, if we can still pull our own event back
        // out, count it as dropped ourselves.
        if (!running && queue.remove(event)) {
            droppedCount.incrementAndGet()
            metrics.fallbackDispatcherDropped()
            return false
        }
        return true
    }

    override fun close() {
        if (!closeExecuted.compareAndSet(false, true)) {
            // Close already ran; nothing left to account for.
            return
        }
        running = false
        // Phase 1: graceful drain attempt.
        // The worker's poll(100, MS) wakes up on its next timeout and
        // sees running=false; it then enters the drain-on-close loop
        // and processes any remaining events without blocking calls.
        // We give it up to GRACEFUL_DRAIN_WAIT_MS for this - long enough
        // for the typical case (fast appender, small queue), short
        // enough that a hung appender does not stretch shutdown.
        //
        // An interrupt of the closing thread (e.g. an expiring container
        // shutdown budget) must not abort the teardown half-way: the
        // joins are wrapped, the forced cleanup and the loss accounting
        // below still run without further blocking waits, and the
        // interrupt flag is restored before returning.
        var interrupted = false
        val gracefulWait = GRACEFUL_DRAIN_WAIT_MS.coerceAtMost(shutdownTimeoutMs)
        try {
            worker.join(gracefulWait)
        } catch (_: InterruptedException) {
            interrupted = true
        }

        if (worker.isAlive) {
            // Phase 2: forced exit.
            // Worker did not finish draining in time. Interrupt to wake
            // it from poll(); whatever it is currently doing (blocked
            // in doAppend, processing an event) is its own problem now.
            worker.interrupt()
            val remainingTimeout = shutdownTimeoutMs - gracefulWait
            // CAUTION: Thread.join(0) means "wait forever", not "do not
            // wait" - a Java API trap. Only join if we actually have
            // remaining budget and were not interrupted ourselves. If we
            // don't, accept that the worker may outlive us; it is a
            // daemon thread, so the JVM can still exit.
            if (remainingTimeout > 0 && !interrupted) {
                try {
                    worker.join(remainingTimeout)
                } catch (_: InterruptedException) {
                    interrupted = true
                }
            }
        }
        val m = metrics
        // Claim the event the worker is still processing (or abandoned
        // mid-delivery): from this point on it counts as dropped exactly
        // once. Should the surviving worker still complete the delivery,
        // its own compare-and-set fails and nothing is double-counted -
        // the conservative direction for a loss metric.
        inFlight.getAndSet(null)?.let {
            droppedCount.incrementAndGet()
            m.fallbackDispatcherDropped()
        }
        // Any remaining queued events are dropped on shutdown. Drain and
        // count in one pass (rather than reading queue.size) so the events
        // are actually released and cannot be re-counted by a later call.
        while (queue.poll() != null) {
            droppedCount.incrementAndGet()
            m.fallbackDispatcherDropped()
        }
        if (interrupted) {
            Thread.currentThread().interrupt()
        }
    }

    private fun runWorker() {
        while (running) {
            val event =
                try {
                    queue.poll(100, TimeUnit.MILLISECONDS)
                } catch (_: InterruptedException) {
                    // Shutdown signal received. Exit immediately rather than
                    // pulling another event from the queue: an in-flight
                    // event that then blocks the worker in doAppend would
                    // become a "ghost" - counted neither as delivered nor
                    // as dropped. The close() method counts everything that
                    // remains in the queue as dropped, which is the correct
                    // semantics once we exit here.
                    Thread.currentThread().interrupt()
                    return
                } ?: continue

            deliver(event)
            if (Thread.currentThread().isInterrupted) {
                // Forced shutdown arrived while delivering. Stop here;
                // close() drains and counts whatever remains queued.
                return
            }
        }
        // running=false but no interrupt: graceful shutdown path.
        // Drain whatever remains in the queue using non-blocking poll;
        // anything still queued or in flight at close() time is counted
        // as dropped by close() itself.
        while (true) {
            val event = queue.poll() ?: return
            deliver(event)
            if (Thread.currentThread().isInterrupted) {
                return
            }
        }
    }

    /**
     * Delivers one event to the fallback appender, keeping the
     * [inFlight] ownership protocol: on success the slot is cleared
     * without counting; when `doAppend` throws, the event is lost and
     * counted as dropped - unless a forced [close] already claimed and
     * counted it, in which case the compare-and-set fails and the event
     * is not double-counted.
     */
    private fun deliver(event: ILoggingEvent) {
        inFlight.set(event)
        try {
            fallbackAppender.doAppend(event)
            inFlight.compareAndSet(event, null)
        } catch (e: Exception) {
            // If the fallback throws, the event is gone - surfacing the
            // exception itself would be log-storm-prone, and the appender
            // has no status manager from this internal class. Account for
            // the loss, then swallow.
            if (inFlight.compareAndSet(event, null)) {
                droppedCount.incrementAndGet()
                metrics.fallbackDispatcherDropped()
            }
            if (e is InterruptedException) {
                // Preserve the shutdown signal a blocking appender may
                // have converted into an exception.
                Thread.currentThread().interrupt()
            }
        }
    }

    companion object {
        /** Default queue capacity. Tuned for typical microservice log volumes. */
        const val DEFAULT_QUEUE_CAPACITY: Int = 1024

        /** Default time allowed in close() for the worker to drain, in milliseconds. */
        const val DEFAULT_SHUTDOWN_TIMEOUT_MS: Long = 5000

        /**
         * How long [close] waits for the worker to drain gracefully before
         * forcibly interrupting. Picked to be slightly longer than the
         * worker's poll() interval (100 ms) so the worker has time to
         * observe `running=false` and enter the drain loop.
         */
        private const val GRACEFUL_DRAIN_WAIT_MS: Long = 200
    }
}
