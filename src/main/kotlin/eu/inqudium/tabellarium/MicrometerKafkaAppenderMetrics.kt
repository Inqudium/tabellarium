package eu.inqudium.tabellarium

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Micrometer-backed [KafkaAppenderMetrics] implementation.
 *
 * ## Metric inventory
 *
 * All metrics carry an `appender` tag derived from the
 * [KafkaAppender]'s Logback name (or `"unnamed"` if not set). This
 * disambiguates the rare case of multiple appender instances binding
 * to the same registry.
 *
 * | Metric                              | Type    | Tags (in addition to `appender`)  |
 * |-------------------------------------|---------|-----------------------------------|
 * | `kafka.appender.events.accepted`    | Counter | `topic.class`                     |
 * | `kafka.appender.events.dispatched`  | Counter | `topic.class`                     |
 * | `kafka.appender.events.fallback`    | Counter | `topic.class`, `reason`           |
 * | `kafka.appender.send.duration`      | Timer   | `topic.class`, `outcome`          |
 * | `kafka.appender.fallback.dropped`   | Counter | (only the common `appender` tag)  |
 * | `kafka.appender.fallback.queue.size`     | Gauge   | (only the common `appender` tag)  |
 * | `kafka.appender.fallback.queue.capacity` | Gauge   | (only the common `appender` tag)  |
 * | `kafka.appender.send.queue.size`         | Gauge   | `topic.class`                     |
 * | `kafka.appender.send.queue.capacity`     | Gauge   | `topic.class`                     |
 *
 * The canonical, operator-facing inventory (including the tag value
 * sets) is the metrics overview under `docs/metrics/`; this table
 * mirrors it for implementation readers and must be updated together
 * with it.
 *
 * Cardinality budget, derived from the enum sizes ([TopicClass]: 4
 * values, [KafkaAppenderMetrics.FallbackReason]: 6,
 * [KafkaAppenderMetrics.SendOutcome]: 2), with `appender` typically a
 * single value per application:
 *
 * - `events.accepted`/`events.dispatched`: 4 each → 8 series
 * - `events.fallback`: 4 × 6 = 24 series
 * - `send.duration`: 4 × 2 = 8 series
 * - `fallback.*`: 3 series
 * - `send.queue.*`: 4 × 2 = 8 series
 *
 * Total: 51 series per appender instance. At ~100 microservices in a
 * Prometheus this is ~5 100 series - well within Prometheus' default
 * cardinality budget.
 *
 * ## Pre-resolution
 *
 * Counters and timers are resolved (i.e. looked up or created in the
 * registry) once in the constructor, not on every hot-path call.
 * `MeterRegistry.counter(name, tags)` does an internal map lookup that
 * is fast but not free; pre-resolving saves it on the hot path.
 *
 * The pre-resolved tables are indexed by enum, so the hot path is an
 * `EnumMap.get()` (array indexing, O(1) without hashing).
 *
 * ## Exception safety
 *
 * All increments/timer-records are wrapped to swallow any exception
 * that might escape from a misbehaving registry implementation. A
 * single metrics bug must not corrupt the logging pipeline.
 *
 * @param registry The Micrometer registry to publish to.
 * @param commonTags Tags that are attached to every metric this
 *                   instance publishes. Use sparingly - every tag
 *                   multiplies the cardinality. Typical use: a
 *                   `service` tag if your registry does not already
 *                   carry one.
 */
internal class MicrometerKafkaAppenderMetrics(
    private val registry: MeterRegistry,
    commonTags: Iterable<Tag> = Tags.empty(),
    appenderName: String? = null,
) : KafkaAppenderMetrics {
    /**
     * The full set of tags applied to every metric this instance
     * publishes. Combines the user-supplied [commonTags] constructor
     * parameter with the [appenderName]-derived `appender` tag, which
     * disambiguates the (rare) case of two or more [KafkaAppender]
     * instances binding to the same registry.
     *
     * The property is named [fullTags] rather than `commonTags` to
     * avoid a Kotlin shadowing pitfall: a property with the same name
     * as a non-`val` constructor parameter compiles, but in other
     * property initializers in the same class body the unqualified
     * identifier resolves to the constructor parameter - silently
     * dropping the `appender` tag from any metric that uses the
     * shorter form. Renaming the property makes the resolution
     * unambiguous.
     *
     * The `appender` tag value is the Logback appender name, or
     * `"unnamed"` if the operator never set one. Cardinality cost is
     * +1 series per appender instance, which is the natural
     * dimensionality of the metric: an operator with two appenders
     * wants their fallback queues distinguishable, an operator with
     * one appender pays for a single tag value across all series.
     */
    private val fullTags: Tags =
        Tags
            .of(commonTags)
            .and(TAG_APPENDER, appenderName?.takeIf { it.isNotBlank() } ?: "unnamed")

    /**
     * Every meter this instance registered, so [deregisterFrom] can
     * remove them from the registry when the appender stops or
     * re-binds. Guarded by its own monitor: registration happens on
     * the bind thread, deregistration possibly on a different
     * shutdown thread.
     */
    private val registeredMeters = mutableListOf<Meter>()

    private fun <M : Meter> track(meter: M): M {
        synchronized(registeredMeters) { registeredMeters += meter }
        return meter
    }

    private val accepted: Map<TopicClass, Counter> =
        TopicClass.entries.associateWith { tc ->
            track(
                Counter
                    .builder(METRIC_EVENTS_ACCEPTED)
                    .tags(tagsWith(TAG_TOPIC_CLASS, tc.tag))
                    .description("Events handed to KafkaAppender.append by Logback")
                    .register(registry),
            )
        }

    private val dispatched: Map<TopicClass, Counter> =
        TopicClass.entries.associateWith { tc ->
            track(
                Counter
                    .builder(METRIC_EVENTS_DISPATCHED)
                    .tags(tagsWith(TAG_TOPIC_CLASS, tc.tag))
                    .description("Events accepted by producer.send (callback outcome not yet known)")
                    .register(registry),
            )
        }

    /**
     * Counters indexed by (topicClass, reason). Pre-resolved to avoid
     * a registry lookup per fallback event. Nested map: outer key
     * topic class, inner key reason.
     */
    private val fallback: Map<TopicClass, Map<KafkaAppenderMetrics.FallbackReason, Counter>> =
        TopicClass.entries.associateWith { tc ->
            KafkaAppenderMetrics.FallbackReason.entries.associateWith { reason ->
                track(
                    Counter
                        .builder(METRIC_EVENTS_FALLBACK)
                        .tags(
                            tagsWith(TAG_TOPIC_CLASS, tc.tag)
                                .and(TAG_REASON, reason.tag),
                        ).description(
                            "Events diverted from Kafka delivery - handed to the fallback " +
                                "appender when one is configured, otherwise dropped",
                        ).register(registry),
                )
            }
        }

    private val sendTimers: Map<TopicClass, Map<KafkaAppenderMetrics.SendOutcome, Timer>> =
        TopicClass.entries.associateWith { tc ->
            KafkaAppenderMetrics.SendOutcome.entries.associateWith { outcome ->
                track(
                    Timer
                        .builder(METRIC_SEND_DURATION)
                        .tags(
                            tagsWith(TAG_TOPIC_CLASS, tc.tag)
                                .and(TAG_OUTCOME, outcome.tag),
                        ).description("Wall-clock duration of producer.send from invocation to callback")
                        .register(registry),
                )
            }
        }

    private val fallbackDropped: Counter =
        track(
            Counter
                .builder(METRIC_FALLBACK_DROPPED)
                .tags(fullTags)
                .description("Events dropped by the FallbackDispatcher (queue full or shutdown timeout)")
                .register(registry),
        )

    /**
     * Holder for the queue-size supplier. Replaced atomically by
     * [registerFallbackQueueGauges]; the gauge always reads the
     * current supplier. A null supplier (initial state) reports 0.
     */
    private val queueSizeSupplier: AtomicReference<(() -> Int)?> = AtomicReference(null)

    init {
        // Register the size gauge once at construction. The actual
        // supplier is plugged in later via registerFallbackQueueGauges.
        // Micrometer keeps the FIRST registration for a given name+tags
        // combination - which is exactly why deregisterFrom() must run
        // on stop/rebind, so a fresh instance's gauge is not silently
        // shadowed by a stale one.
        track(
            Gauge
                .builder(METRIC_FALLBACK_QUEUE_SIZE) {
                    queueSizeSupplier.get()?.invoke()?.toDouble() ?: 0.0
                }.tags(fullTags)
                .description("Current number of events waiting in the FallbackDispatcher queue")
                .register(registry),
        )
    }

    /**
     * Removes every meter this instance registered from [registry].
     * Called by [KafkaAppender.stop] (and before a repeated bind) so
     * that reconfiguration cycles do not accumulate meters or leave
     * gauges reading a closed dispatcher's queue. Safe to call more
     * than once. Note: when two instances share identical name+tags
     * (two unnamed appenders on one registry), Micrometer hands both
     * the same meter object - deregistering one then removes the
     * shared meter; the `appender` tag exists to avoid that overlap.
     */
    internal fun deregisterFrom(registry: MeterRegistry) {
        val toRemove = synchronized(registeredMeters) { registeredMeters.toList().also { registeredMeters.clear() } }
        toRemove.forEach { meter ->
            safe { registry.remove(meter) }
        }
    }

    override fun eventAccepted(topicClass: TopicClass) {
        safe { accepted.getValue(topicClass).increment() }
    }

    override fun eventDispatched(topicClass: TopicClass) {
        safe { dispatched.getValue(topicClass).increment() }
    }

    override fun eventFallback(
        topicClass: TopicClass,
        reason: KafkaAppenderMetrics.FallbackReason,
    ) {
        safe { fallback.getValue(topicClass).getValue(reason).increment() }
    }

    override fun sendCompleted(
        topicClass: TopicClass,
        outcome: KafkaAppenderMetrics.SendOutcome,
        duration: Duration,
    ) {
        safe {
            sendTimers
                .getValue(topicClass)
                .getValue(outcome)
                .record(duration.toNanos(), TimeUnit.NANOSECONDS)
        }
    }

    override fun fallbackDispatcherDropped() {
        safe { fallbackDropped.increment() }
    }

    override fun registerFallbackQueueGauges(
        queueSize: () -> Int,
        capacity: Int,
    ) {
        safe {
            // Plug in the live supplier so the size gauge starts
            // reporting real numbers instead of zero.
            queueSizeSupplier.set(queueSize)
            // The capacity is fixed for the dispatcher's lifetime,
            // so it can be a constant-valued gauge.
            track(
                Gauge
                    .builder(METRIC_FALLBACK_QUEUE_CAPACITY) { capacity.toDouble() }
                    .tags(fullTags)
                    .description("Maximum number of events the FallbackDispatcher queue can hold")
                    .register(registry),
            )
        }
    }

    override fun registerSendQueueGauges(
        topicClass: TopicClass,
        queueSize: () -> Int,
        capacity: Int,
    ) {
        safe {
            // Unlike the fallback queue gauge (pre-registered with a
            // supplier holder), the send dispatchers exist before the
            // bind, so both gauges can be registered directly here.
            track(
                Gauge
                    .builder(METRIC_SEND_QUEUE_SIZE) { queueSize().toDouble() }
                    .tags(tagsWith(TAG_TOPIC_CLASS, topicClass.tag))
                    .description("Current number of events waiting in the SendDispatcher queue")
                    .register(registry),
            )
            track(
                Gauge
                    .builder(METRIC_SEND_QUEUE_CAPACITY) { capacity.toDouble() }
                    .tags(tagsWith(TAG_TOPIC_CLASS, topicClass.tag))
                    .description("Maximum number of events the SendDispatcher queue can hold")
                    .register(registry),
            )
        }
    }

    /**
     * Runs [block] and silently swallows any exception. A metrics
     * failure must never corrupt the logging pipeline.
     */
    private inline fun safe(block: () -> Unit) {
        try {
            block()
        } catch (_: Exception) {
            // Intentionally swallow.
        }
    }

    private fun tagsWith(
        key: String,
        value: String,
    ): Tags = fullTags.and(key, value)

    companion object {
        const val METRIC_EVENTS_ACCEPTED: String = "kafka.appender.events.accepted"
        const val METRIC_EVENTS_DISPATCHED: String = "kafka.appender.events.dispatched"
        const val METRIC_EVENTS_FALLBACK: String = "kafka.appender.events.fallback"
        const val METRIC_SEND_DURATION: String = "kafka.appender.send.duration"
        const val METRIC_FALLBACK_DROPPED: String = "kafka.appender.fallback.dropped"
        const val METRIC_FALLBACK_QUEUE_SIZE: String = "kafka.appender.fallback.queue.size"
        const val METRIC_FALLBACK_QUEUE_CAPACITY: String = "kafka.appender.fallback.queue.capacity"
        const val METRIC_SEND_QUEUE_SIZE: String = "kafka.appender.send.queue.size"
        const val METRIC_SEND_QUEUE_CAPACITY: String = "kafka.appender.send.queue.capacity"

        const val TAG_TOPIC_CLASS: String = "topic.class"
        const val TAG_REASON: String = "reason"
        const val TAG_OUTCOME: String = "outcome"
        const val TAG_APPENDER: String = "appender"
    }
}
