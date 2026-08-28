package eu.inqudium.tabellarium

import ch.qos.logback.core.spi.ContextAware
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import java.util.IdentityHashMap

/**
 * Owns the Micrometer side of a [KafkaAppender]'s lifecycle: binding
 * (appender meters, per-producer Kafka client metrics, Resilience4j
 * circuit-breaker metrics) and the symmetric teardown. Extracted from
 * the appender so the composition root keeps a single responsibility
 * and the bind/unbind pairing lives in one place.
 *
 * ## Circuit-breaker metrics: own binder, `appender`-tagged
 *
 * The circuit-breaker meters are registered by this class itself
 * rather than by `resilience4j-micrometer`'s
 * `TaggedCircuitBreakerMetrics`. The official binder derives the meter
 * ID from the metric name plus the breaker name alone - and tabellarium
 * breaker names are per topic class (`kafka-appender-audit`, ...), so
 * two KafkaAppender instances bound to the same MeterRegistry would
 * collide: state gauges keep reporting whichever instance registered
 * first, counters mix both. The own binder mirrors the official
 * binder's metric names and tags 1:1 and adds the same `appender` tag
 * the appender's own meters carry ([MicrometerKafkaAppenderMetrics]),
 * making every meter ID appender-unique. It needs only
 * `resilience4j-circuitbreaker` (a required dependency) plus
 * `micrometer-core`, so the previously optional
 * `resilience4j-micrometer` bridge is no longer used.
 *
 * ## Lazy class-loading pattern for the optional Kafka binder
 *
 * The Kafka producer-metrics integration starts with a [Class.forName]
 * **probe** that succeeds only when the binder class is on the
 * classpath. If the probe throws [ClassNotFoundException], the typed
 * `doBind…` method is never entered and the JVM never has to resolve
 * the symbols it references - so the appender works without the
 * Micrometer Kafka binder in the dependency tree. The `doBind…` method
 * uses the binder class **directly** (no reflection): Kotlin compiles
 * `private fun` to a regular private JVM method whose referenced types
 * are resolved on first invocation, which the probe gates.
 *
 * ## Teardown
 *
 * [unbind] reverses everything a bind registered: it closes the
 * per-producer `KafkaClientMetrics` binders (they are [AutoCloseable]
 * and remove their meters on close), removes exactly the
 * circuit-breaker meters this instance's bind registered (tracked as
 * meter objects, identity-based - so neither an operator's unrelated
 * breakers nor another KafkaAppender's breaker meters on a shared
 * registry are ever touched), and deregisters the appender's own
 * meters. Without this, every Logback reconfiguration cycle would leak
 * meters and leave gauges reporting a closed dispatcher's queue.
 * The call-event consumers attached to the breakers cannot be
 * deregistered (Resilience4j offers no removal API); they write
 * through per-breaker [CallMeterHolder]s whose meter references
 * [unbind] clears, so events after teardown are discarded and a
 * re-bind swaps in fresh meters without stacking consumers.
 *
 * @param status Sink for operator-facing warnings/infos (the owning
 *               appender; Logback status manager).
 */
internal class MetricsBindings(
    private val status: ContextAware,
) {
    private var boundMetrics: MicrometerKafkaAppenderMetrics? = null
    private var boundRegistry: MeterRegistry? = null

    /**
     * Exactly the circuit-breaker meters THIS instance registered
     * (identity-compared: [Meter] does not override equals), so
     * [unbind] can remove precisely these - and never a meter that
     * another KafkaAppender instance registered on a shared registry.
     */
    private val boundResilience4jMeters = mutableListOf<Meter>()

    /**
     * Mutable sinks for the event-driven call meters, one per breaker,
     * attached exactly once for the lifetime of this instance: the
     * Resilience4j event publisher offers no consumer deregistration,
     * so the consumers stay attached and write through these holders.
     * [unbind] clears the meter references (events are then discarded);
     * a re-bind installs fresh meters without stacking a second set of
     * consumers.
     */
    private val callMeterHolders = IdentityHashMap<CircuitBreaker, CallMeterHolder>()

    private class CallMeterHolder {
        @Volatile
        var successfulCalls: Timer? = null

        @Volatile
        var failedCalls: Timer? = null

        @Volatile
        var ignoredCalls: Timer? = null

        @Volatile
        var notPermittedCalls: Counter? = null

        fun clear() {
            successfulCalls = null
            failedCalls = null
            ignoredCalls = null
            notPermittedCalls = null
        }
    }

    private val producerMetricBindings = mutableListOf<AutoCloseable>()

    /**
     * Binds everything to [registry] and returns the appender-metrics
     * implementation the caller should install on its hot path. A
     * previous bind is torn down first so a repeated bind replaces
     * instead of duplicating.
     */
    fun bind(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderName: String?,
        circuitBreakerRegistry: CircuitBreakerRegistry,
        producerRegistry: ProducerRegistry,
    ): MicrometerKafkaAppenderMetrics {
        unbind()
        val impl = MicrometerKafkaAppenderMetrics(registry, commonTags, appenderName = appenderName)
        boundMetrics = impl
        boundRegistry = registry
        // Same derivation as MicrometerKafkaAppenderMetrics: every meter
        // of this appender carries the identical appender tag value.
        val appenderTag = appenderName?.takeIf { it.isNotBlank() } ?: "unnamed"
        warnOnBreakerMeterCollision(registry, producerRegistry, appenderTag)
        bindResilience4jMetrics(registry, commonTags, appenderTag, circuitBreakerRegistry)
        bindKafkaProducerMetrics(registry, commonTags, appenderTag, producerRegistry)
        return impl
    }

    /**
     * Reverses everything [bind] registered. No-op when nothing is
     * bound; safe to call more than once.
     */
    fun unbind() {
        val registry = boundRegistry ?: return
        producerMetricBindings.forEach { binding ->
            try {
                binding.close()
            } catch (e: Exception) {
                status.addWarn("Error closing Kafka producer metric binding: ${e.message}", e)
            }
        }
        producerMetricBindings.clear()
        try {
            boundResilience4jMeters.forEach { registry.remove(it) }
        } catch (e: Exception) {
            status.addWarn("Error removing Resilience4j meters: ${e.message}", e)
        }
        boundResilience4jMeters.clear()
        // Stop the still-attached event consumers from recording into
        // the removed meters; see callMeterHolders.
        callMeterHolders.values.forEach { it.clear() }
        try {
            boundMetrics?.deregisterFrom(registry)
        } catch (e: Exception) {
            status.addWarn("Error deregistering appender meters: ${e.message}", e)
        }
        boundMetrics = null
        boundRegistry = null
    }

    /**
     * The `appender` tag makes the circuit-breaker meter IDs unique per
     * appender instance - unless two appenders share the same (or no)
     * name and bind to the same MeterRegistry, in which case the IDs
     * collide after all: state gauges then keep reporting whichever
     * instance registered first, and counters mix both. The binding
     * itself stays best-effort - but the operator gets told that the
     * breaker metrics are not trustworthy in this setup.
     */
    private fun warnOnBreakerMeterCollision(
        registry: MeterRegistry,
        producerRegistry: ProducerRegistry,
        appenderTag: String,
    ) {
        val breakerNames =
            producerRegistry.activeTopicClasses
                .map { ResilientMessageSender.circuitBreakerName(it) }
                .toSet()
        val colliding =
            registry.meters
                .filter { meter ->
                    meter.id.name.startsWith("resilience4j.circuitbreaker") &&
                        meter.id.getTag("name") in breakerNames &&
                        meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER) == appenderTag
                }.mapNotNull { it.id.getTag("name") }
                .toSortedSet()
        if (colliding.isEmpty()) return
        status.addWarn(
            "MeterRegistry already contains circuit-breaker meters for ${colliding.joinToString()} " +
                "with the same appender tag '$appenderTag' - most likely from another " +
                "KafkaAppender instance with the same (or no) name bound to the same registry. " +
                "The colliding breaker gauges/counters will not reflect this appender's state; " +
                "give each KafkaAppender a distinct name for trustworthy per-appender breaker metrics.",
        )
    }

    /**
     * Best-effort binding of the circuit-breaker metrics. Mirrors the
     * metric names, tags and semantics of `resilience4j-micrometer`'s
     * `TaggedCircuitBreakerMetrics` 1:1 and adds the `appender` tag
     * (plus the operator's common tags) - see the class KDoc for why
     * the official binder is not used. Failures are reported via the
     * status manager; every meter registered before a failure is
     * tracked and torn down by [unbind].
     */
    private fun bindResilience4jMetrics(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderTag: String,
        circuitBreakerRegistry: CircuitBreakerRegistry,
    ) {
        try {
            for (breaker in circuitBreakerRegistry.allCircuitBreakers) {
                bindBreakerMeters(registry, commonTags, appenderTag, breaker)
            }
        } catch (e: Exception) {
            status.addInfo(
                "Failed to bind Resilience4j metrics to MeterRegistry " +
                    "(circuit-breaker state metrics will be unavailable): ${e.message}",
            )
        }
    }

    private fun bindBreakerMeters(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderTag: String,
        breaker: CircuitBreaker,
    ) {
        val tags =
            Tags
                .of(commonTags)
                .and("name", breaker.name)
                .and(MicrometerKafkaAppenderMetrics.TAG_APPENDER, appenderTag)

        // One 0/1 gauge per possible state, exactly like the official
        // binder - dashboards select the active state via `== 1`.
        for (state in CircuitBreaker.State.entries) {
            boundResilience4jMeters +=
                Gauge
                    .builder("resilience4j.circuitbreaker.state", breaker) { b ->
                        if (b.state == state) 1.0 else 0.0
                    }.description("The states of the circuit breaker")
                    .tags(tags)
                    .tag("state", state.name.lowercase())
                    .register(registry)
        }
        boundResilience4jMeters +=
            Gauge
                .builder("resilience4j.circuitbreaker.buffered.calls", breaker) {
                    it.metrics.numberOfSuccessfulCalls.toDouble()
                }.description("The number of buffered successful calls stored in the ring buffer")
                .tags(tags)
                .tag("kind", "successful")
                .register(registry)
        boundResilience4jMeters +=
            Gauge
                .builder("resilience4j.circuitbreaker.buffered.calls", breaker) {
                    it.metrics.numberOfFailedCalls.toDouble()
                }.description("The number of buffered failed calls stored in the ring buffer")
                .tags(tags)
                .tag("kind", "failed")
                .register(registry)
        boundResilience4jMeters +=
            Gauge
                .builder("resilience4j.circuitbreaker.slow.calls", breaker) {
                    it.metrics.numberOfSlowSuccessfulCalls.toDouble()
                }.description("The number of slow successful calls which were slower than a certain threshold")
                .tags(tags)
                .tag("kind", "successful")
                .register(registry)
        boundResilience4jMeters +=
            Gauge
                .builder("resilience4j.circuitbreaker.slow.calls", breaker) {
                    it.metrics.numberOfSlowFailedCalls.toDouble()
                }.description("The number of slow failed calls which were slower than a certain threshold")
                .tags(tags)
                .tag("kind", "failed")
                .register(registry)
        boundResilience4jMeters +=
            Gauge
                .builder("resilience4j.circuitbreaker.failure.rate", breaker) {
                    it.metrics.failureRate.toDouble()
                }.description("The failure rate of the circuit breaker")
                .tags(tags)
                .register(registry)
        boundResilience4jMeters +=
            Gauge
                .builder("resilience4j.circuitbreaker.slow.call.rate", breaker) {
                    it.metrics.slowCallRate.toDouble()
                }.description("The slow call rate of the circuit breaker")
                .tags(tags)
                .register(registry)

        // Event-driven call meters. The consumers are attached exactly
        // once per breaker (no deregistration API exists) and write
        // through the holder; see callMeterHolders.
        val holder =
            callMeterHolders.getOrPut(breaker) {
                CallMeterHolder().also { h ->
                    breaker.eventPublisher.onSuccess { event ->
                        h.successfulCalls?.record(event.elapsedDuration)
                    }
                    breaker.eventPublisher.onError { event ->
                        h.failedCalls?.record(event.elapsedDuration)
                    }
                    breaker.eventPublisher.onIgnoredError { event ->
                        h.ignoredCalls?.record(event.elapsedDuration)
                    }
                    breaker.eventPublisher.onCallNotPermitted { _ ->
                        h.notPermittedCalls?.increment()
                    }
                }
            }
        holder.successfulCalls =
            Timer
                .builder("resilience4j.circuitbreaker.calls")
                .description("Total number of successful calls")
                .tags(tags)
                .tag("kind", "successful")
                .register(registry)
                .also { boundResilience4jMeters += it }
        holder.failedCalls =
            Timer
                .builder("resilience4j.circuitbreaker.calls")
                .description("Total number of failed calls")
                .tags(tags)
                .tag("kind", "failed")
                .register(registry)
                .also { boundResilience4jMeters += it }
        holder.ignoredCalls =
            Timer
                .builder("resilience4j.circuitbreaker.calls")
                .description("Total number of calls which failed but the exception was ignored")
                .tags(tags)
                .tag("kind", "ignored")
                .register(registry)
                .also { boundResilience4jMeters += it }
        holder.notPermittedCalls =
            Counter
                .builder("resilience4j.circuitbreaker.not.permitted.calls")
                .description("Total number of not permitted calls")
                .tags(tags)
                .tag("kind", "not_permitted")
                .register(registry)
                .also { boundResilience4jMeters += it }
    }

    /**
     * Best-effort binding of Kafka producer-internal metrics. Requires
     * the Micrometer Kafka binder on the classpath; silently skipped
     * if absent.
     */
    private fun bindKafkaProducerMetrics(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderTag: String,
        producerRegistry: ProducerRegistry,
    ) {
        try {
            Class.forName("io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics")
        } catch (_: ClassNotFoundException) {
            return
        }
        try {
            doBindKafkaProducerMetrics(registry, commonTags, appenderTag, producerRegistry)
        } catch (e: Exception) {
            status.addInfo(
                "Failed to bind Kafka producer metrics to MeterRegistry " +
                    "(producer-internal metrics will be unavailable): ${e.message}",
            )
        }
    }

    private fun doBindKafkaProducerMetrics(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderTag: String,
        producerRegistry: ProducerRegistry,
    ) {
        for (topicClass in producerRegistry.activeTopicClasses) {
            val producer = producerRegistry.producerFor(topicClass)
            // The appender tag keeps producer meters from colliding even
            // when an operator gives two appenders the same client.id.
            val tagsForClass =
                Tags
                    .of(commonTags)
                    .and(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS, topicClass.tag)
                    .and(MicrometerKafkaAppenderMetrics.TAG_APPENDER, appenderTag)
            val binding = KafkaClientMetrics(producer, tagsForClass)
            binding.bindTo(registry)
            producerMetricBindings += binding
        }
    }
}
