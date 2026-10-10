package eu.inqudium.tabellarium

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Micrometer-backed [KafkaAppenderMetrics] implementation.
 *
 * ## Metric inventory
 *
 * All metrics carry an `appender` tag derived from the
 * [KafkaAppender]'s Logback name (or `"unnamed"` if not set). This
 * disambiguates the rare case of multiple appender instances binding
 * to the same registry; two instances with the same name would get
 * the same meter objects, so [MetricsBindings] refuses the second
 * binding instead.
 *
 * The inventory - names, tags, cardinality - is canonical in
 * `docs/metrics/metrics-guide.md`, and `DocumentationContractTest`
 * keeps that document in step with the `METRIC_*`/`TAG_*` constants
 * and the enum sizes here. Invariant: one pre-resolved meter per enum
 * combination (`topic.class` × `reason` for fallbacks, `topic.class` ×
 * `outcome` for send timers, `topic.class` for the rest - the event-size
 * summary included -, none for the
 * fallback-dispatcher meters), so the series count per appender is a
 * function of the enum sizes alone - minus the two fallback queue
 * gauges, which exist only when a fallback appender is configured.
 * The info gauges are the exception: `kafka.appender.info` is one
 * series, `kafka.appender.topic.info` one per configured topic.
 *
 * ## Info gauges
 *
 * With an [AppenderInfo], two constant-1 gauges carry the appender's
 * configuration as labels (the Prometheus `*_info` convention): the
 * value says nothing, the labels are joined onto the other series with
 * `* on(appender) group_left(...)`. The value stays 1 so that join
 * leaves the joined series' values untouched. The labels are fixed for
 * the appender's lifetime, so a series never changes identity while it
 * is live; a reconfiguration replaces it with a new one.
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
 * @param info Configuration metadata for the info gauges; `null`
 *             registers none.
 * @param registeredMeters Receives every meter as it is registered;
 *             handed in by [register] so the meters of a construction
 *             that aborts halfway stay reachable for its rollback, and
 *             read by [deregisterFrom] when the appender stops or
 *             re-binds. Safety: guarded by its own monitor - registration
 *             happens on the bind thread, deregistration possibly on a
 *             different shutdown thread.
 * @param warn Operator-facing sink for a queue-gauge registration
 *             that failed (the Logback status manager via
 *             [MetricsBindings]); defaults to silence.
 */
internal class MicrometerKafkaAppenderMetrics(
    private val registry: MeterRegistry,
    commonTags: Iterable<Tag> = Tags.empty(),
    appenderName: String? = null,
    info: AppenderInfo? = null,
    private val registeredMeters: MutableList<Meter> = mutableListOf(),
    private val warn: (String, Throwable) -> Unit = { _, _ -> },
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

    /**
     * Payload size per class. Without a `MeterFilter` enabling
     * percentiles a record is a count, a sum and a max update; it runs
     * on the send worker (see [KafkaAppenderMetrics.eventSize]).
     */
    private val eventSizes: Map<TopicClass, DistributionSummary> =
        TopicClass.entries.associateWith { tc ->
            track(
                DistributionSummary
                    .builder(METRIC_EVENT_SIZE)
                    .baseUnit("bytes")
                    .tags(tagsWith(TAG_TOPIC_CLASS, tc.tag))
                    .description("Encoded payload size of the events the send worker takes up (key and headers excluded)")
                    .register(registry),
            )
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
     * Holder for the fallback queue-size supplier. Replaced atomically
     * by every [registerFallbackQueueGauges] call; the gauge, registered
     * once on the first call, always reads the current supplier.
     * Micrometer keeps the FIRST registration for a given name+tags
     * combination, so a repeated call cannot swap the gauge's lambda -
     * it swaps the holder instead. (Also why [deregisterFrom] must run
     * on stop/rebind: a fresh instance's gauge must not be silently
     * shadowed by a stale one.)
     */
    private val queueSizeSupplier: AtomicReference<(() -> Int)?> = AtomicReference(null)

    /**
     * Whether the two fallback queue gauges exist. Both are registered
     * together, and only when a [FallbackDispatcher] is wired to this
     * instance: without a fallback appender there is no queue, and a
     * constant-zero size series next to a missing capacity series would
     * read as a healthy queue where events are dropped by policy
     * (`CODE_ANALYSIS-2026-09-15T21-09-11`, finding 9).
     */
    private val fallbackGaugesRegistered = AtomicBoolean(false)

    init {
        if (info != null) {
            // As one transaction with a warning, like the queue gauge
            // pairs: a half-registered info set would silently drop
            // series from every join on its labels
            // (`DEFECT_ANALYSIS-2026-10-10T17-20-10`, finding 3).
            registerAllOrNone("info", "configuration labels will be missing from joins") {
                registerInfoGauges(info)
            }
        }
    }

    /**
     * Removes every meter this instance registered from [registry].
     * Called by [KafkaAppender.stop] (and before a repeated bind) so
     * that reconfiguration cycles do not accumulate meters or leave
     * gauges reading a closed dispatcher's queue. Safe to call more
     * than once. Removes only meters this instance registered - and
     * since [MetricsBindings] refuses a second binding with the same
     * name+tags on one registry, no other live instance shares them.
     */
    internal fun deregisterFrom(registry: MeterRegistry) {
        val toRemove = synchronized(registeredMeters) { registeredMeters.toList().also { registeredMeters.clear() } }
        toRemove.forEach { meter ->
            safe { registry.remove(meter) }
        }
    }

    private fun <M : Meter> track(meter: M): M {
        synchronized(registeredMeters) { registeredMeters += meter }
        return meter
    }

    private fun registerInfoGauges(info: AppenderInfo) {
        track(
            Gauge
                .builder(METRIC_INFO) { 1.0 }
                .tags(
                    fullTags
                        .and(TAG_FALLBACK, info.fallback)
                        .and(TAG_FALLBACK_TYPE, info.fallbackType)
                        .and(TAG_APPENDER_VERSION, info.version)
                        .and(TAG_KAFKA_CLIENT_VERSION, info.kafkaClientVersion)
                        .and(TAG_DEBUG, info.debug.toString()),
                ).description("Configuration of the KafkaAppender instance as labels; the value is always 1")
                .register(registry),
        )
        info.topics.forEach { topic ->
            track(
                Gauge
                    .builder(METRIC_TOPIC_INFO) { 1.0 }
                    .tags(
                        tagsWith(TAG_TOPIC_CLASS, topic.topicClass.tag)
                            .and(TAG_TOPIC, topic.topic)
                            .and(TAG_ACKS, topic.acks)
                            .and(TAG_LINGER_MS, topic.lingerMs),
                    ).description("A configured topic with its topic class and producer settings as labels; the value is always 1")
                    .register(registry),
            )
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

    override fun eventSize(
        topicClass: TopicClass,
        bytes: Int,
    ) {
        safe { eventSizes.getValue(topicClass).record(bytes.toDouble()) }
    }

    override fun fallbackDispatcherDropped() {
        safe { fallbackDropped.increment() }
    }

    override fun registerFallbackQueueGauges(
        queueSize: () -> Int,
        capacity: Int,
    ) {
        safe {
            // The live supplier first, so the gauge never reads a stale
            // dispatcher's queue between a repeated call and its return.
            queueSizeSupplier.set(queueSize)
            if (!fallbackGaugesRegistered.compareAndSet(false, true)) return@safe
            val registered =
                registerAllOrNone("fallback queue", QUEUE_UNOBSERVED) {
                    track(
                        Gauge
                            .builder(METRIC_FALLBACK_QUEUE_SIZE) {
                                queueSizeSupplier.get()?.invoke()?.toDouble() ?: 0.0
                            }.tags(fullTags)
                            .description("Current number of events waiting in the FallbackDispatcher queue")
                            .register(registry),
                    )
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
            // Marked complete only once both exist, so a repeated call
            // retries a failed pair instead of returning on the flag.
            if (!registered) fallbackGaugesRegistered.set(false)
        }
    }

    override fun registerSendQueueGauges(
        topicClass: TopicClass,
        queueSize: () -> Int,
        capacity: Int,
    ) {
        safe {
            // One dispatcher per class exists for the appender's lifetime
            // and calls this once per bind, so both gauges are registered
            // directly (a repeated bind deregisters first).
            registerAllOrNone("send queue (topic.class=${topicClass.tag})", QUEUE_UNOBSERVED) {
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
    }

    /**
     * Registers a group of gauges - a size/capacity pair, the info set -
     * as one transaction: if [block] throws, the gauges it already
     * registered are removed again and [warn] reports the failure with
     * its [consequence], so the registry never shows a size series
     * without its capacity (or a partial info set) and the operator
     * learns what is unobserved - a failure the non-throwing hook
     * contract would otherwise hide
     * (`DEFECT_ANALYSIS-2026-10-10T15-52-19`, finding 2).
     *
     * @return `true` if every gauge of the group was registered.
     */
    private inline fun registerAllOrNone(
        what: String,
        consequence: String,
        block: () -> Unit,
    ): Boolean {
        val before = synchronized(registeredMeters) { registeredMeters.size }
        try {
            block()
            return true
        } catch (e: Exception) {
            val partial = synchronized(registeredMeters) { registeredMeters.drop(before) }
            partial.forEach { meter ->
                try {
                    registry.remove(meter)
                    synchronized(registeredMeters) { registeredMeters.remove(meter) }
                } catch (rollbackFailure: Exception) {
                    // Stays tracked, so deregisterFrom still tries again.
                    e.addSuppressed(rollbackFailure)
                }
            }
            warn("Failed to register the $what gauges ($consequence): ${e.message}", e)
            return false
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
        /**
         * Constructs an instance as one transaction: the base meters are
         * registered one by one in the property initializers, so a
         * registry (or a meter filter) that throws halfway would leave
         * the meters registered so far orphaned - unreachable for
         * [deregisterFrom], and the first of them, `events.accepted`,
         * would make [MetricsBindings]' identity probe mistake the
         * aborted attempt for a live second appender and refuse every
         * retry (`DEFECT_ANALYSIS-2026-10-10T13-46-50`, finding 2). On
         * any failure the meters registered so far are removed again
         * before the failure is rethrown.
         */
        fun register(
            registry: MeterRegistry,
            commonTags: Iterable<Tag>,
            appenderName: String?,
            info: AppenderInfo?,
            warn: (String, Throwable) -> Unit = { _, _ -> },
        ): MicrometerKafkaAppenderMetrics {
            val registered = mutableListOf<Meter>()
            try {
                return MicrometerKafkaAppenderMetrics(registry, commonTags, appenderName, info, registered, warn)
            } catch (e: Throwable) {
                registered.forEach { meter ->
                    try {
                        registry.remove(meter)
                    } catch (rollbackFailure: Throwable) {
                        e.addSuppressed(rollbackFailure)
                    }
                }
                throw e
            }
        }

        /** The [registerAllOrNone] consequence of a failed queue gauge pair. */
        private const val QUEUE_UNOBSERVED: String = "queue saturation will be unobserved"

        const val METRIC_EVENTS_ACCEPTED: String = "kafka.appender.events.accepted"
        const val METRIC_EVENTS_DISPATCHED: String = "kafka.appender.events.dispatched"
        const val METRIC_EVENTS_FALLBACK: String = "kafka.appender.events.fallback"
        const val METRIC_SEND_DURATION: String = "kafka.appender.send.duration"
        const val METRIC_EVENT_SIZE: String = "kafka.appender.event.size"
        const val METRIC_FALLBACK_DROPPED: String = "kafka.appender.fallback.dropped"
        const val METRIC_FALLBACK_QUEUE_SIZE: String = "kafka.appender.fallback.queue.size"
        const val METRIC_FALLBACK_QUEUE_CAPACITY: String = "kafka.appender.fallback.queue.capacity"
        const val METRIC_SEND_QUEUE_SIZE: String = "kafka.appender.send.queue.size"
        const val METRIC_SEND_QUEUE_CAPACITY: String = "kafka.appender.send.queue.capacity"
        const val METRIC_INFO: String = "kafka.appender.info"
        const val METRIC_TOPIC_INFO: String = "kafka.appender.topic.info"

        const val TAG_TOPIC_CLASS: String = "topic.class"
        const val TAG_REASON: String = "reason"
        const val TAG_OUTCOME: String = "outcome"
        const val TAG_APPENDER: String = "appender"

        // Info-gauge labels. Rationale: `appender.version` rather than `version` -
        // a `version` common tag (the application's version) is common
        // and would be overridden on the info series, breaking joins.
        const val TAG_FALLBACK: String = "fallback"
        const val TAG_FALLBACK_TYPE: String = "fallback.type"
        const val TAG_APPENDER_VERSION: String = "appender.version"
        const val TAG_KAFKA_CLIENT_VERSION: String = "kafka.client.version"
        const val TAG_DEBUG: String = "debug"
        const val TAG_TOPIC: String = "topic"
        const val TAG_ACKS: String = "acks"
        const val TAG_LINGER_MS: String = "linger.ms"
    }
}
