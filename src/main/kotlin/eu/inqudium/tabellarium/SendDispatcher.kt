package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Decouples `producer.send` from the logging caller's thread - the
 * asynchronous heart of the appender's "the sender is never made to
 * wait" promise. A [BoundedWorkerDispatcher] whose delivery is the
 * potentially-blocking send and whose rejections divert to the
 * fallback.
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
 * ## Diversion policy
 *
 * Every rejection of the skeleton becomes a fallback diversion with a
 * metric reason: a full queue is `queue.full` (Kafka delivery is not
 * keeping up - the fallback is the designed escape hatch for exactly
 * that state, and blocking the caller would resurrect the problem this
 * class exists to solve); the remainder of a [close] is `shutdown`
 * (the drain still sends - the appender closes send dispatchers before
 * the producer registry; a send parked in `max.block.ms` unblocks on
 * the interrupt with an `InterruptException`, which the sender's error
 * path routes itself); a worker death, a failed delivery, and a
 * dispatch after a death are `send.error`. Every diversion is
 * accounted exactly once via [PendingSend.claim], shared with the
 * sender's own diversion paths.
 *
 * ## Threading and self-logging
 *
 * The worker carries the appender's reentry guard: the Kafka client
 * logs synchronously on the `producer.send` caller - which is now this
 * worker - and those events must be dropped by [KafkaAppender.append]
 * instead of being fed back into the queue (a feedback loop that
 * amplifies exactly during broker trouble).
 *
 * The [ILoggingEvent] crosses to the worker thread only as the payload
 * for the *fallback* path - the same cross-thread exposure the
 * [FallbackDispatcher] already has, since the Kafka callback thread
 * hands events to it as well. Encoding and enrichment already happened
 * on the original caller thread, so MDC and markers were read in their
 * native context.
 *
 * @param topicClass The topic class this dispatcher serves; used for
 *                   metrics tagging and the worker thread name.
 * @param sendAction The potentially-blocking delivery step, typically
 *                   `messageSender.send(topicClass, ...)`. Injected as
 *                   a function so the dispatcher can be tested with
 *                   latches instead of a full Kafka pipeline.
 * @param fallbackDispatcher Receives diverted events. Null means
 *                           "drop" - the operator's explicit choice,
 *                           consistent with the rest of the pipeline.
 * @param reentryGuard Passed through to [BoundedWorkerDispatcher].
 * @param queueCapacity Passed through to [BoundedWorkerDispatcher];
 *                      the default matches the fallback dispatcher's.
 * @param drainTimeoutMs Passed through to [BoundedWorkerDispatcher];
 *                       here the drain means actually sending.
 * @param onWorkerDeath Invoked after a worker death was accounted for
 *                      (in-flight and queued work diverted with reason
 *                      `send.error`); the appender reports it to the
 *                      status manager so it does not masquerade as a
 *                      slow broker.
 */
internal class SendDispatcher(
    private val topicClass: TopicClass,
    private val sendAction: (PendingSend) -> Unit,
    private val fallbackDispatcher: FallbackDispatcher?,
    reentryGuard: SelfLoggingGuard? = null,
    private val queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    drainTimeoutMs: Long = DEFAULT_DRAIN_TIMEOUT_MS,
    onWorkerDeath: (Throwable) -> Unit = {},
) : BoundedWorkerDispatcher<SendDispatcher.PendingSend>(
        threadName = "kafka-appender-send-dispatcher-${topicClass.tag}",
        queueCapacity = queueCapacity,
        drainTimeoutMs = drainTimeoutMs,
        reentryGuard = reentryGuard,
        onWorkerDeath = onWorkerDeath,
    ) {
    /**
     * The unit of work handed from the caller to the worker: everything
     * the send needs, pre-computed on the caller's thread.
     */
    internal class PendingSend(
        val topicName: String,
        val payload: ByteArray,
        val enrichment: EnrichedRecord,
        val originalEvent: ILoggingEvent,
    ) {
        /**
         * Exactly-once guard for the fallback diversion of this item,
         * shared between the dispatcher (overflow, shutdown, worker
         * death) and [ResilientMessageSender]'s own diversion paths
         * (throttle, open breaker, send failure): whoever wins the
         * compare-and-set diverts; everyone else stands down. Without
         * this, a forced shutdown could route the in-flight event to
         * the fallback twice - once as `shutdown` by close(), once as
         * `send.error` by the sender when the parked send later
         * unblocks with an exception.
         *
         * Held as a detached object so the sender can hand exactly this
         * claim to the Kafka callback without keeping the whole
         * [PendingSend] - and with it the payload copy - reachable for
         * as long as the client buffers the record.
         */
        val claim: DiversionClaim = DiversionClaim()

        fun tryClaimDiversion(): Boolean = claim.tryClaim()
    }

    /**
     * The compare-and-set behind [PendingSend.tryClaimDiversion], on its
     * own so that a reference to it retains nothing but one boolean.
     * Safety: this is what the send callback captures; the callback
     * lives until the Kafka client completes the record, which under a
     * slow broker can be `delivery.timeout.ms` - retaining the
     * [PendingSend] there would double the per-record heap footprint.
     */
    internal class DiversionClaim {
        private val diverted = AtomicBoolean(false)

        fun tryClaim(): Boolean = diverted.compareAndSet(false, true)
    }

    @Volatile
    private var metrics: KafkaAppenderMetrics = KafkaAppenderMetrics.NO_OP

    /**
     * Replaces the metrics implementation and registers the queue
     * gauges with it. Called by [KafkaAppender.bindMeterRegistry].
     */
    fun setMetrics(metrics: KafkaAppenderMetrics) {
        this.metrics = metrics
        metrics.registerSendQueueGauges(topicClass, queueSize = ::queueSize, capacity = queueCapacity)
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
        offer(PendingSend(topicName, payload, enrichment, originalEvent))
    }

    override fun deliver(item: PendingSend) {
        // ResilientMessageSender.send handles its own error paths; an
        // exception here is unexpected and becomes a send.error divert.
        sendAction(item)
    }

    override fun reject(
        item: PendingSend,
        rejection: Rejection,
    ) {
        val reason =
            when (rejection) {
                Rejection.QUEUE_FULL -> {
                    KafkaAppenderMetrics.FallbackReason.QUEUE_FULL
                }

                Rejection.SHUTDOWN_REMAINDER -> {
                    KafkaAppenderMetrics.FallbackReason.SHUTDOWN
                }

                Rejection.WORKER_DEATH, Rejection.DELIVERY_FAILED -> {
                    KafkaAppenderMetrics.FallbackReason.SEND_ERROR
                }

                // A dispatch after the worker died lost its delivery
                // capability to an error; after a regular close it is a
                // shutdown - operators see the real cause either way.
                Rejection.NOT_ACCEPTING -> {
                    if (workerDied) {
                        KafkaAppenderMetrics.FallbackReason.SEND_ERROR
                    } else {
                        KafkaAppenderMetrics.FallbackReason.SHUTDOWN
                    }
                }
            }
        // Exactly-once across ALL diversion paths, the sender's
        // included - see PendingSend.claim.
        if (!item.tryClaimDiversion()) {
            return
        }
        metrics.eventFallback(topicClass, reason)
        fallbackDispatcher?.enqueue(item.originalEvent)
    }

    companion object {
        /** Default queue capacity per topic class. */
        const val DEFAULT_QUEUE_CAPACITY: Int = 1024

        /** Default time allowed in [close] for the worker to drain by sending, in milliseconds. */
        const val DEFAULT_DRAIN_TIMEOUT_MS: Long = 1000
    }
}
