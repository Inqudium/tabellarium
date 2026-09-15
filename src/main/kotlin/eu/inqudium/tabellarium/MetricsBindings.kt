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
 * `TaggedCircuitBreakerMetrics`. Tabellarium breaker names are per
 * topic class (`kafka-appender-audit`, ...), so meter IDs need the
 * extra `appender` tag to stay unique when two KafkaAppender instances
 * bind to the same MeterRegistry. The official binder could carry that
 * tag too (it propagates tags set on the `CircuitBreaker` at creation),
 * so uniqueness alone would not justify an own binder. What the
 * official binder does not offer is the lifecycle this appender needs:
 * a symmetric [unbind] on Logback reconfiguration or [KafkaAppender]
 * stop. `TaggedCircuitBreakerMetrics` removes its meters only when a
 * breaker is deleted from a `CircuitBreakerRegistry` it observes; it
 * has no teardown API of its own, so every reconfiguration cycle would
 * leak the previous appender instance's meters into the shared
 * MeterRegistry. The own binder mirrors the official binder's metric
 * names and tags 1:1 (existing dashboards keep working), tracks every
 * meter it registers, and removes exactly those in [unbind]. It also
 * needs only `resilience4j-circuitbreaker` (a required dependency)
 * plus `micrometer-core`, so the previously optional
 * `resilience4j-micrometer` bridge is no longer used. The price is
 * deliberate: resilience4j upgrades must be checked against this
 * mirror (meter names, states, tags, event semantics) -
 * `CircuitBreakerMetricsMirrorTest` does the name/type/tag part on
 * every build against the official binder on the test classpath.
 *
 * ## One binding per meter identity
 *
 * Every meter of an appender carries the `appender` tag (its Logback
 * name, or `unnamed`) plus the operator's common tags. Two instances
 * with the same name bound to one registry would therefore ask
 * Micrometer for identical IDs, and Micrometer hands both the same
 * meter objects: their counters would mix, and the first [unbind]
 * would remove series the other instance still publishes through -
 * dashboards would show an outage as "no data"
 * (`DEFECT_ANALYSIS-2026-09-15T22-05-50`, L-2). [bind] therefore
 * checks the registry for a live binding with the same identity first
 * and refuses the second one entirely - no appender, breaker or
 * producer meters, a status warning that names the tag, [isBound]
 * stays false - so the first instance's series remain whole and
 * trustworthy. A per-instance discriminator tag was considered and
 * rejected: it would change the documented inventory and cardinality
 * for every deployment to serve a misconfiguration whose fix is a
 * distinct appender name.
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

    /** True between a successful [bind] and the next [unbind]. */
    val isBound: Boolean
        get() = boundRegistry != null

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
     * instead of duplicating. Returns [KafkaAppenderMetrics.NO_OP] and
     * binds nothing when [registry] already holds a live binding with
     * the same meter identity (see the class KDoc).
     */
    fun bind(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderName: String?,
        circuitBreakerRegistry: CircuitBreakerRegistry,
        producerRegistry: ProducerRegistry,
    ): KafkaAppenderMetrics {
        unbind()
        // Same derivation as MicrometerKafkaAppenderMetrics: every meter
        // of this appender carries the identical appender tag value.
        val appenderTag = appenderName?.takeIf { it.isNotBlank() } ?: "unnamed"
        if (isIdentityBound(registry, commonTags, appenderTag)) {
            status.addWarn(
                "MeterRegistry already contains the meters of a KafkaAppender tagged appender='$appenderTag' " +
                    "with the same common tags - another KafkaAppender instance with the same (or no) name is " +
                    "bound to this registry. Metrics binding refused: Micrometer would hand both instances the " +
                    "same meters, mixing their counts and removing the shared series when either stops. " +
                    "Give each KafkaAppender a distinct name and bind again.",
            )
            return KafkaAppenderMetrics.NO_OP
        }
        val impl = MicrometerKafkaAppenderMetrics(registry, commonTags, appenderName = appenderName)
        boundMetrics = impl
        boundRegistry = registry
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
     * Whether [registry] already holds a live binding with this
     * appender's meter identity: the `appender` tag plus the common
     * tags. Probed on the accepted-events counter, which every binding
     * registers first and removes on unbind, so a hit is a binding that
     * is still in place - not a leftover. [MeterRegistry.find] matches
     * meters that carry at least the given tags, so the counter's own
     * `topic.class` tag does not hide it.
     */
    private fun isIdentityBound(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderTag: String,
    ): Boolean =
        registry
            .find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED)
            .tags(Tags.of(commonTags).and(MicrometerKafkaAppenderMetrics.TAG_APPENDER, appenderTag))
            .meters()
            .isNotEmpty()

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
            // WARN like the teardown failures: an operator filtering the
            // status output for warnings must learn that the breaker
            // meters are missing.
            status.addWarn(
                "Failed to bind Resilience4j metrics to MeterRegistry " +
                    "(circuit-breaker state metrics will be unavailable): ${e.message}",
                e,
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
            registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.state", "state", state.name.lowercase(), "The states of the circuit breaker") {
                if (it.state == state) 1.0 else 0.0
            }
        }
        // The buffered/slow-call and rate gauges of the official binder,
        // as a table: name, kind tag (null = untagged), description,
        // value - so the mirror the class KDoc commits to checking on
        // every Resilience4j upgrade is one list, not a page of chains.
        registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.buffered.calls", "kind", "successful", "The number of buffered successful calls stored in the ring buffer") {
            it.metrics.numberOfSuccessfulCalls.toDouble()
        }
        registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.buffered.calls", "kind", "failed", "The number of buffered failed calls stored in the ring buffer") {
            it.metrics.numberOfFailedCalls.toDouble()
        }
        registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.slow.calls", "kind", "successful", "The number of slow successful calls which were slower than a certain threshold") {
            it.metrics.numberOfSlowSuccessfulCalls.toDouble()
        }
        registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.slow.calls", "kind", "failed", "The number of slow failed calls which were slower than a certain threshold") {
            it.metrics.numberOfSlowFailedCalls.toDouble()
        }
        registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.failure.rate", null, null, "The failure rate of the circuit breaker") {
            it.metrics.failureRate.toDouble()
        }
        registerBreakerGauge(registry, breaker, tags, "resilience4j.circuitbreaker.slow.call.rate", null, null, "The slow call rate of the circuit breaker") {
            it.metrics.slowCallRate.toDouble()
        }

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
        holder.successfulCalls = registerCallTimer(registry, tags, "successful", "Total number of successful calls")
        holder.failedCalls = registerCallTimer(registry, tags, "failed", "Total number of failed calls")
        holder.ignoredCalls = registerCallTimer(registry, tags, "ignored", "Total number of calls which failed but the exception was ignored")
        holder.notPermittedCalls =
            Counter
                .builder("resilience4j.circuitbreaker.not.permitted.calls")
                .description("Total number of not permitted calls")
                .tags(tags)
                .tag("kind", "not_permitted")
                .register(registry)
                .also { boundResilience4jMeters += it }
    }

    /** Registers one breaker gauge (optionally with one extra tag) and tracks it for [unbind]. */
    private fun registerBreakerGauge(
        registry: MeterRegistry,
        breaker: CircuitBreaker,
        tags: Tags,
        name: String,
        tagKey: String?,
        tagValue: String?,
        description: String,
        value: (CircuitBreaker) -> Double,
    ) {
        val builder = Gauge.builder(name, breaker) { value(it) }.description(description).tags(tags)
        if (tagKey != null && tagValue != null) {
            builder.tag(tagKey, tagValue)
        }
        boundResilience4jMeters += builder.register(registry)
    }

    /** Registers one `resilience4j.circuitbreaker.calls` timer of the given kind and tracks it for [unbind]. */
    private fun registerCallTimer(
        registry: MeterRegistry,
        tags: Tags,
        kind: String,
        description: String,
    ): Timer =
        Timer
            .builder("resilience4j.circuitbreaker.calls")
            .description(description)
            .tags(tags)
            .tag("kind", kind)
            .register(registry)
            .also { boundResilience4jMeters += it }

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
            status.addWarn(
                "Failed to bind Kafka producer metrics to MeterRegistry " +
                    "(producer-internal metrics will be unavailable): ${e.message}",
                e,
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
