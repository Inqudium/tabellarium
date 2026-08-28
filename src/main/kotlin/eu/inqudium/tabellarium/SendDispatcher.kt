package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Decouples `producer.send` from the logging caller's thread - the
 * asynchronous heart of the appender's "the sender is never made to
 * wait" promise.
 *
 * ## Why this exists
 *
 * `KafkaProducer.send` is allowed to block the calling thread for up
 * to `max.block.ms` while waiting for topic metadata or free buffer
 * space. The appender caps that value per topic class (500 ms; 200 ms
 * for PERFORMANCE), which bounds the wait but does not eliminate it:
 * during a broker outage, every logging thread could still stall for
 * the full cap per event until the circuit breaker opens. This
 * dispatcher removes the caller from the send path entirely:
 *
 * - The **caller** ([KafkaAppender.append]) does only CPU-bound work -
 *   routing, encoding, enrichment - and then [dispatch]es the finished
 *   record package into a bounded queue in O(1), never blocking.
 * - A dedicated **worker thread** drains the queue and performs the
 *   potentially-blocking [ResilientMessageSender.send] (throttle,
 *   breaker, `producer.send`).
 *
 * One dispatcher exists **per active topic class**, mirroring the
 * per-class isolation of producers and circuit breakers: an AUDIT
 * send stalled at its `max.block.ms` cap never delays TECHNICAL or
 * PERFORMANCE delivery. FIFO order per topic class is preserved by
 * the single worker.
 *
 * ## Overflow and shutdown policy
 *
 * The queue is **bounded**. When it is full, [dispatch] never blocks:
 * the event is diverted to the fallback dispatcher (when configured)
 * and counted as [KafkaAppenderMetrics.FallbackReason.QUEUE_FULL]. A
 * full queue means Kafka delivery is not keeping up - the fallback is
 * the designed escape hatch for exactly that state, and blocking the
 * caller would resurrect the problem this class exists to solve.
 *
 * On [close], the worker first drains the queue gracefully (producers
 * are still open - the appender closes send dispatchers before the
 * producer registry). If the drain does not finish within the budget,
 * the worker is interrupted (a send parked in `max.block.ms` unblocks
 * with an `InterruptException`, which the sender's error path routes
 * to the fallback) and everything still queued or in flight is
 * diverted to the fallback with
 * [KafkaAppenderMetrics.FallbackReason.SHUTDOWN] - accounted exactly
 * once via the same compare-and-set ownership protocol the
 * [FallbackDispatcher] uses for its in-flight event.
 *
 * ## Threading and self-logging
 *
 * The worker thread marks itself with the appender's [reentryGuard]
 * ThreadLocal for its entire lifetime: the Kafka client logs
 * synchronously on the `producer.send` caller - which is now this
 * worker - and those events must be dropped by [KafkaAppender.append]
 * instead of being fed back into the queue (a feedback loop that
 * amplifies exactly during broker trouble).
 *
 * The [ILoggingEvent] crosses to the worker thread only as the payload
 * for the *fallback* path - the same cross-thread exposure the
 * [FallbackDispatcher] already has today, since the Kafka callback
 * thread hands events to it as well. Encoding and enrichment already
 * happened on the original caller thread, so MDC and markers were read
 * in their native context.
 *
 * @param topicClass The topic class this dispatcher serves; used for
 *                   metrics tagging and the worker thread name.
 * @param sendAction The potentially-blocking delivery step, typically
 *                   `messageSender.send(topicClass, ...)`. Injected as
 *                   a function so the dispatcher can be tested with
 *                   latches instead of a full Kafka pipeline.
 * @param fallbackDispatcher Receives diverted events (queue overflow,
 *                           shutdown remainder). Null means "drop" -
 *                           the operator's explicit choice, consistent
 *                           with the rest of the pipeline.
 * @param reentryGuard The appender's per-thread reentry guard; the
 *                     worker sets it once at startup. Null disables
 *                     the marking (tests).
 * @param queueCapacity Maximum queued events. The default matches the
 *                      fallback dispatcher's: large enough to absorb
 *                      bursts, small enough to bound memory.
 * @param drainTimeoutMs Time allowed in [close] for the worker to
 *                       drain the queue by actually sending.
 * @param synchronous Test-only hook, same pattern as
 *                    [FallbackDispatcher]: [dispatch] runs [sendAction]
 *                    inline on the caller and no worker is started, so
 *                    existing tests can assert producer state right
 *                    after `doAppend`. Must NEVER be used in
 *                    production - it re-introduces caller blocking.
 */
internal class SendDispatcher(
    private val topicClass: TopicClass,
    private val sendAction: (PendingSend) -> Unit,
    private val fallbackDispatcher: FallbackDispatcher?,
    private val reentryGuard: ThreadLocal<Boolean>? = null,
    private val queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    private val drainTimeoutMs: Long = DEFAULT_DRAIN_TIMEOUT_MS,
    private val synchronous: Boolean = false,
) : AutoCloseable {
    /**
     * The unit of work handed from the caller to the worker: everything
     * the send needs, pre-computed on the caller's thread.
     */
    internal class PendingSend(
        val topicName: String,
        val payload: ByteArray,
        val enrichment: EnrichedRecord,
        val originalEvent: ILoggingEvent,
    )

    private val queue: LinkedBlockingQueue<PendingSend> = LinkedBlockingQueue(queueCapacity)

    /**
     * The item the worker has taken off the queue but not yet finished
     * sending. Same compare-and-set ownership protocol as
     * [FallbackDispatcher]: exactly one party accounts for it on a
     * forced shutdown.
     */
    private val inFlight = AtomicReference<PendingSend>()

    @Volatile
    private var metrics: KafkaAppenderMetrics = KafkaAppenderMetrics.NO_OP

    @Volatile
    private var running = true

    private val closeExecuted = AtomicBoolean(false)

    private val worker: Thread? =
        if (synchronous) {
            null
        } else {
            Thread(::runWorker, "kafka-appender-send-dispatcher-${topicClass.tag}").apply {
                isDaemon = true
                start()
            }
        }

    /**
     * Replaces the metrics implementation and registers the queue
     * gauges with it. Called by [KafkaAppender.bindMeterRegistry].
     */
    fun setMetrics(metrics: KafkaAppenderMetrics) {
        this.metrics = metrics
        metrics.registerSendQueueGauges(topicClass, queueSize = queue::size, capacity = queueCapacity)
    }

    /**
     * Hands the pre-encoded record package off for asynchronous
     * delivery. Returns immediately; on a full queue or after [close]
     * the event is diverted to the fallback instead of blocking.
     */
    fun dispatch(
        topicName: String,
        payload: ByteArray,
        enrichment: EnrichedRecord,
        originalEvent: ILoggingEvent,
    ) {
        if (synchronous) {
            // Test mode: the caller performs the send inline.
            sendAction(PendingSend(topicName, payload, enrichment, originalEvent))
            return
        }
        val item = PendingSend(topicName, payload, enrichment, originalEvent)
        if (!running) {
            divert(item, KafkaAppenderMetrics.FallbackReason.SHUTDOWN)
            return
        }
        if (!queue.offer(item)) {
            divert(item, KafkaAppenderMetrics.FallbackReason.QUEUE_FULL)
            return
        }
        // Close the check-then-act window against close(), same as
        // FallbackDispatcher.enqueue: if close() finished its final
        // drain between the running check and the offer, the item would
        // be neither sent nor diverted. Re-check and reclaim.
        if (!running && queue.remove(item)) {
            divert(item, KafkaAppenderMetrics.FallbackReason.SHUTDOWN)
        }
    }

    override fun close() {
        if (!closeExecuted.compareAndSet(false, true)) {
            return
        }
        running = false
        if (synchronous) {
            return
        }
        // Two-phase shutdown: the graceful drain (the worker keeps
        // SENDING - the producers are still open at this point) gets
        // the full budget; only then is the worker interrupted, with a
        // short bounded wait for the interrupt to take effect. The
        // interrupt handling mirrors the M-6 fix: an interrupted closer
        // still runs the forced cleanup and restores its flag.
        var interrupted = false
        val worker = checkNotNull(worker) { "a non-synchronous dispatcher always has a worker thread" }
        try {
            worker.join(drainTimeoutMs)
        } catch (_: InterruptedException) {
            interrupted = true
        }
        if (worker.isAlive) {
            // A send parked in max.block.ms unblocks with an
            // InterruptException; the sender's error path routes that
            // event to the fallback itself.
            worker.interrupt()
            if (!interrupted) {
                try {
                    worker.join(INTERRUPT_GRACE_MS)
                } catch (_: InterruptedException) {
                    interrupted = true
                }
            }
        }
        // Claim the in-flight item (exactly-once via CAS; if the worker
        // still completes the send, its own CAS fails and nothing is
        // diverted twice), then divert everything still queued.
        inFlight.getAndSet(null)?.let {
            divert(it, KafkaAppenderMetrics.FallbackReason.SHUTDOWN)
        }
        while (true) {
            val item = queue.poll() ?: break
            divert(item, KafkaAppenderMetrics.FallbackReason.SHUTDOWN)
        }
        if (interrupted) {
            Thread.currentThread().interrupt()
        }
    }

    private fun runWorker() {
        // Mark this thread for the appender's reentry guard: everything
        // the Kafka client logs synchronously from inside producer.send
        // now happens here, and append() must drop it. Set once - the
        // worker never legitimately logs through the appender.
        reentryGuard?.set(true)
        while (running) {
            val item =
                try {
                    queue.poll(100, TimeUnit.MILLISECONDS)
                } catch (_: InterruptedException) {
                    // Forced shutdown: exit immediately; close() diverts
                    // what remains.
                    Thread.currentThread().interrupt()
                    return
                } ?: continue
            deliver(item)
            if (Thread.currentThread().isInterrupted) {
                return
            }
        }
        // Graceful drain: running=false, no interrupt. Keep sending -
        // the producers are still open, close() waits for this.
        while (true) {
            val item = queue.poll() ?: return
            deliver(item)
            if (Thread.currentThread().isInterrupted) {
                return
            }
        }
    }

    private fun deliver(item: PendingSend) {
        inFlight.set(item)
        try {
            sendAction(item)
            inFlight.compareAndSet(item, null)
        } catch (e: Exception) {
            // Unexpected: ResilientMessageSender.send handles its own
            // error paths internally. Whatever slipped through must not
            // kill the worker - divert the event (unless close() already
            // claimed it) and keep going.
            if (inFlight.compareAndSet(item, null)) {
                divert(item, KafkaAppenderMetrics.FallbackReason.SEND_ERROR)
            }
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }

    private fun divert(
        item: PendingSend,
        reason: KafkaAppenderMetrics.FallbackReason,
    ) {
        metrics.eventFallback(topicClass, reason)
        fallbackDispatcher?.enqueue(item.originalEvent)
    }

    companion object {
        /** Default queue capacity per topic class. */
        const val DEFAULT_QUEUE_CAPACITY: Int = 1024

        /** Default time allowed in [close] for the worker to drain by sending, in milliseconds. */
        const val DEFAULT_DRAIN_TIMEOUT_MS: Long = 1000

        /**
         * How long [close] waits after interrupting the worker for the
         * interrupt to take effect (a parked send unblocks with an
         * InterruptException) before diverting the remainder itself.
         */
        private const val INTERRUPT_GRACE_MS: Long = 500
    }
}
