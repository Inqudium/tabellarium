package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

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
 * @param synchronous Test-only hook. When true, [enqueue] invokes
 *                    [fallbackAppender.doAppend] directly on the
 *                    caller's thread and no worker thread is started.
 *                    This bypasses the entire reason this class exists
 *                    (decoupling from caller threads) and must NEVER
 *                    be used in production. The flag exists solely so
 *                    unit tests can assert "the fallback received the
 *                    event" without polling or sleeping.
 */
internal class FallbackDispatcher(
    private val fallbackAppender: Appender<ILoggingEvent>,
    queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    private val shutdownTimeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS,
    private val synchronous: Boolean = false,
) : AutoCloseable {
    private val queueCapacity: Int = queueCapacity
    private val queue: LinkedBlockingQueue<ILoggingEvent> = LinkedBlockingQueue(queueCapacity)
    private val droppedCount = AtomicLong(0)

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

    private val worker: Thread? =
        if (synchronous) {
            null
        } else {
            Thread(::runWorker, "kafka-appender-fallback-dispatcher").apply {
                isDaemon = true
                start()
            }
        }

    /**
     * Number of events dropped because the queue was full when
     * [enqueue] was called, or because [close] timed out before the
     * queue drained. Read from any thread.
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
        if (synchronous) {
            // Test mode: bypass the queue and the worker thread entirely.
            try {
                fallbackAppender.doAppend(event)
            } catch (_: Exception) {
                // Same swallow policy as the worker.
            }
            return true
        }
        val accepted = queue.offer(event)
        if (!accepted) {
            droppedCount.incrementAndGet()
            metrics.fallbackDispatcherDropped()
        }
        return accepted
    }

    override fun close() {
        running = false
        if (synchronous) {
            // No worker thread to wait for.
            return
        }
        // Phase 1: graceful drain attempt.
        // The worker's poll(100, MS) wakes up on its next timeout and
        // sees running=false; it then enters the drain-on-close loop
        // and processes any remaining events without blocking calls.
        // We give it up to GRACEFUL_DRAIN_WAIT_MS for this - long enough
        // for the typical case (fast appender, small queue), short
        // enough that a hung appender does not stretch shutdown.
        val gracefulWait = GRACEFUL_DRAIN_WAIT_MS.coerceAtMost(shutdownTimeoutMs)
        worker!!.join(gracefulWait)

        if (worker.isAlive) {
            // Phase 2: forced exit.
            // Worker did not finish draining in time. Interrupt to wake
            // it from poll(); whatever it is currently doing (blocked
            // in doAppend, processing an event) is its own problem now.
            worker.interrupt()
            val remainingTimeout = shutdownTimeoutMs - gracefulWait
            // CAUTION: Thread.join(0) means "wait forever", not "do not
            // wait" - a Java API trap. Only join if we actually have
            // remaining budget. If we don't, accept that the worker may
            // outlive us; it is a daemon thread, so the JVM can still
            // exit.
            if (remainingTimeout > 0) {
                worker.join(remainingTimeout)
            }
        }
        // Any remaining queued events are dropped on shutdown.
        val shutdownDrops = queue.size.toLong()
        if (shutdownDrops > 0) {
            droppedCount.addAndGet(shutdownDrops)
            val m = metrics
            repeat(shutdownDrops.toInt().coerceAtMost(Int.MAX_VALUE)) {
                m.fallbackDispatcherDropped()
            }
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

            try {
                fallbackAppender.doAppend(event)
            } catch (_: Exception) {
                // If the fallback throws, there is nothing meaningful
                // left to do - surfacing this would itself be log-storm-
                // prone, and the appender has no status manager from
                // this internal class. Swallow.
            }
        }
        // running=false but no interrupt: graceful shutdown path.
        // Drain whatever remains in the queue using non-blocking poll;
        // anything still in flight at close() time is counted as
        // dropped by close() itself.
        while (true) {
            val event = queue.poll() ?: return
            try {
                fallbackAppender.doAppend(event)
            } catch (_: Exception) {
                // Same swallow policy.
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
