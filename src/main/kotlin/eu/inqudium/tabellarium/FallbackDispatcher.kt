package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import java.util.concurrent.atomic.AtomicLong

/**
 * Decouples invocation of the fallback [Appender] from caller threads -
 * primarily the Kafka producer's I/O thread, which must never block on
 * downstream logging. A [BoundedWorkerDispatcher] whose delivery is
 * `doAppend` and whose every rejection is a counted drop.
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
 * that the send workers perform.
 *
 * This dispatcher inserts a single-consumer queue between the Kafka
 * callback (and the hot-path's synchronous fallback path) and the
 * actual fallback appender. The caller [enqueue]s in O(1) without
 * blocking; the worker drains the queue and calls `doAppend`.
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
 * The same counting applies to every other way an event can miss the
 * appender: `doAppend` throwing (surfacing the exception itself would
 * be log-storm-prone, and this class has no status manager - so the
 * loss is counted, then swallowed), a worker death, and the remainder
 * of a [close] whose budget expired.
 *
 * ## Threading and self-logging
 *
 * The worker carries the appender's reentry guard: a fallback appender
 * that logs through SLF4J per delivered event would otherwise feed each
 * such log back through the root logger into [KafkaAppender.append],
 * on to Kafka and - while Kafka is down - back into this very queue, a
 * loop that saturates both queues for the duration of an outage.
 *
 * @param fallbackAppender The appender to which events are dispatched.
 * @param queueCapacity Passed through to [BoundedWorkerDispatcher];
 *                      the default of 1024 balances memory (each event
 *                      holds MDC, throwable, ...) against tolerance for
 *                      brief fallback slowness.
 * @param shutdownTimeoutMs Passed through to [BoundedWorkerDispatcher]
 *                          as its drain budget; here the drain means
 *                          delivering to the fallback appender.
 * @param reentryGuard Passed through to [BoundedWorkerDispatcher].
 * @param onWorkerDeath Invoked after a worker death was accounted for
 *                      (in-flight and queued events counted as
 *                      dropped); the appender reports it to the status
 *                      manager so a dead worker does not masquerade as
 *                      a merely slow fallback appender.
 */
internal class FallbackDispatcher(
    private val fallbackAppender: Appender<ILoggingEvent>,
    private val queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    shutdownTimeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS,
    reentryGuard: ThreadLocal<Boolean>? = null,
    onWorkerDeath: (Throwable) -> Unit = {},
) : BoundedWorkerDispatcher<ILoggingEvent>(
        threadName = "kafka-appender-fallback-dispatcher",
        queueCapacity = queueCapacity,
        drainTimeoutMs = shutdownTimeoutMs,
        reentryGuard = reentryGuard,
        onWorkerDeath = onWorkerDeath,
    ) {
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
        metrics.registerFallbackQueueGauges(queueSize = ::queueSize, capacity = queueCapacity)
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
    fun enqueue(event: ILoggingEvent): Boolean = offer(event)

    override fun deliver(item: ILoggingEvent) {
        fallbackAppender.doAppend(item)
    }

    override fun reject(
        item: ILoggingEvent,
        rejection: Rejection,
    ) {
        droppedCount.incrementAndGet()
        metrics.fallbackDispatcherDropped()
    }

    companion object {
        /** Default queue capacity. Tuned for typical microservice log volumes. */
        const val DEFAULT_QUEUE_CAPACITY: Int = 1024

        /** Default time allowed in close() for the worker to drain by delivering, in milliseconds. */
        const val DEFAULT_SHUTDOWN_TIMEOUT_MS: Long = 5000
    }
}
