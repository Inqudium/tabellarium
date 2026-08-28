package eu.inqudium.tabellarium

import eu.inqudium.tabellarium.KafkaAppenderMetrics.Companion.NO_OP
import java.time.Duration

/**
 * Pluggable instrumentation hook for the [KafkaAppender] pipeline.
 *
 * The appender invokes the methods on this interface at well-defined
 * points in the hot path. The default implementation [NO_OP] does
 * nothing and allocates nothing, so the appender can run without
 * any Micrometer (or other) integration on the classpath.
 *
 * ## Wiring up Micrometer
 *
 * Production callers typically wire an instance backed by Micrometer
 * via [KafkaAppender.bindMeterRegistry]. Doing so is **optional**:
 * if `bindMeterRegistry` is never called, the appender stays on the
 * [NO_OP] implementation and emits no metrics.
 *
 * ## Implementation contract
 *
 * Implementations must:
 *
 * - **Never block.** These methods run on the application's calling
 *   threads (hot path) or on the Kafka producer's I/O thread
 *   (callback path). Any blocking would re-introduce the very
 *   thread-blocking problems the appender exists to solve.
 * - **Not throw.** A misbehaving metric must not corrupt the logging
 *   pipeline. Implementations should catch and swallow any internal
 *   exceptions (e.g. registry full, tag cardinality limit exceeded).
 *   The [NO_OP] is, trivially, exception-safe.
 * - **Be thread-safe.** Several appender hot-path entries may run
 *   concurrently.
 */
interface KafkaAppenderMetrics {
    /**
     * Recorded once per event that enters [KafkaAppender.append].
     * Counted regardless of what happens downstream.
     */
    fun eventAccepted(topicClass: TopicClass)

    /**
     * Recorded once per event that was successfully handed to
     * [org.apache.kafka.clients.producer.Producer.send]. Note: this
     * does NOT mean the record reached the broker - the Kafka callback
     * outcome is captured by [sendCompleted].
     */
    fun eventDispatched(topicClass: TopicClass)

    /**
     * Recorded once per event that was **diverted from Kafka delivery**.
     * The event is handed to the fallback appender when one is
     * configured; without a fallback it is dropped - the counter
     * increments either way, so it reads as "did not reach Kafka",
     * not as "was delivered to the fallback". The [reason] indicates
     * which gate produced the diversion.
     */
    fun eventFallback(
        topicClass: TopicClass,
        reason: FallbackReason,
    )

    /**
     * Recorded once per `producer.send(...)` callback (success or
     * error). The [duration] is the wall-clock time from send-start
     * to callback. Errors are split out by [outcome] so operators
     * can distinguish "Kafka is slow" from "Kafka rejects records".
     */
    fun sendCompleted(
        topicClass: TopicClass,
        outcome: SendOutcome,
        duration: Duration,
    )

    /**
     * Recorded by the [FallbackDispatcher] every time an event is
     * dropped because either the dispatcher's queue was full at
     * [FallbackDispatcher.enqueue] time, or the worker did not finish
     * draining within the shutdown timeout.
     */
    fun fallbackDispatcherDropped()

    /**
     * Called once at [KafkaAppender.start] to register gauges that
     * report the dispatcher's current queue depth and capacity. The
     * [queueSize] supplier is read on each metric scrape; it must be
     * cheap and non-blocking. The [capacity] is the fixed maximum.
     *
     * Implementations that bind to Micrometer typically register
     * `Gauge.builder("kafka.appender.fallback.queue.size", queueSize)`
     * once and reuse the registration. The [NO_OP] silently ignores.
     */
    fun registerFallbackQueueGauges(
        queueSize: () -> Int,
        capacity: Int,
    )

    /**
     * Reasons a single event was diverted from Kafka delivery (to the
     * fallback appender when configured, otherwise dropped).
     */
    enum class FallbackReason(
        /** Lowercase, dot-free tag value suitable for Prometheus etc. */
        val tag: String,
    ) {
        /** Circuit breaker is OPEN or HALF_OPEN with no permission left. */
        BREAKER_OPEN("breaker.open"),

        /** Half-open throttle gate; probe gap not yet elapsed. */
        THROTTLE("throttle"),

        /** producer.send() either threw synchronously or its callback reported an error. */
        SEND_ERROR("send.error"),

        /** Hot-path exception before send was even attempted (encoder, routing, OOM). */
        ENCODER_ERROR("encoder.error"),
    }

    /** Outcome of a single producer.send() callback. */
    enum class SendOutcome(
        val tag: String,
    ) {
        /** Callback reported no exception. */
        SUCCESS("success"),

        /** Callback reported an exception, or producer.send() threw synchronously. */
        ERROR("error"),
    }

    companion object {
        /**
         * Singleton no-op implementation. Used by [KafkaAppender] until
         * an operator calls [KafkaAppender.bindMeterRegistry] (or
         * equivalent setter). Allocates nothing on any call.
         */
        val NO_OP: KafkaAppenderMetrics = NoOpKafkaAppenderMetrics
    }
}

/**
 * Singleton no-op implementation. Lives at file level so the
 * `KafkaAppenderMetrics.NO_OP` reference always points to the same
 * instance - no allocation on `Companion.NO_OP` access.
 */
private object NoOpKafkaAppenderMetrics : KafkaAppenderMetrics {
    override fun eventAccepted(topicClass: TopicClass) = Unit

    override fun eventDispatched(topicClass: TopicClass) = Unit

    override fun eventFallback(
        topicClass: TopicClass,
        reason: KafkaAppenderMetrics.FallbackReason,
    ) = Unit

    override fun sendCompleted(
        topicClass: TopicClass,
        outcome: KafkaAppenderMetrics.SendOutcome,
        duration: Duration,
    ) = Unit

    override fun fallbackDispatcherDropped() = Unit

    override fun registerFallbackQueueGauges(
        queueSize: () -> Int,
        capacity: Int,
    ) = Unit
}
