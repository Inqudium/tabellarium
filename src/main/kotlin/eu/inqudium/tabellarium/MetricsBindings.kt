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
 * MeterRegistry. The own binder mirrors the official binder's meters,
 * types and tags 1:1 but publishes them under the appender's namespace
 * (`kafka.appender.circuitbreaker.*` instead of
 * `resilience4j.circuitbreaker.*`, see "One namespace" below), tracks
 * every meter it registers, and removes exactly those in [unbind]. It also
 * needs only `resilience4j-circuitbreaker` (a required dependency)
 * plus `micrometer-core`, so the previously optional
 * `resilience4j-micrometer` bridge is no longer used. The price is
 * deliberate: resilience4j upgrades must be checked against this
 * mirror (meter names, states, tags, event semantics) -
 * `CircuitBreakerMetricsMirrorTest` does the name/type/tag part on
 * every build against the official binder on the test classpath.
 *
 * ## One namespace: `kafka.appender.*`
 *
 * Every meter a bind registers starts with `kafka.appender.`, so one
 * prefix selects, filters or switches off all of them, and none of them
 * mixes with the application's own breakers or Kafka clients - a
 * product decision, recorded with its costs (a rename for dashboards on
 * the standard names, a bridge to check on Micrometer upgrades) in
 * ADR-0005. The
 * breaker meters get their names directly from the own binder. The Kafka
 * client meters come from Micrometer's `KafkaClientMetrics`, whose names
 * (`kafka.producer.*`) are fixed; the binders register into the private
 * source registry of a [RenamingMeterMirror], which publishes each meter
 * in the operator's registry as `kafka.appender.X` for `kafka.X` (see
 * there why a mirror and not a rename `MeterFilter`). [unbind] closes the
 * binders, then the mirror, which removes what is left.
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

    /** True while bound to exactly [registry] (identity). */
    fun isBoundTo(registry: MeterRegistry): Boolean = boundRegistry === registry

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
     * The mirror the Kafka client binders of the current bind register
     * into (see "One namespace" in the class KDoc); closed in [unbind]
     * after the binders.
     */
    private var producerMetricsMirror: RenamingMeterMirror? = null

    /**
     * Binds everything to [registry] and returns the appender-metrics
     * implementation the caller should install on its hot path. A
     * repeated bind replaces the previous one instead of duplicating it.
     * Returns `null` and binds nothing when [registry] already holds a
     * live binding with the same meter identity (see the class KDoc).
     *
     * Order: the previous binding is torn down only once the new one is
     * known to take effect - a refused or throwing bind to *another*
     * registry leaves a working binding in place instead of turning
     * monitoring dark (`DEFECT_ANALYSIS-2026-10-10T15-19-31`, finding 6).
     * A re-bind to the *same* registry must unbind first: the new meters
     * would carry the old ones' IDs, and Micrometer would hand back the
     * very meter objects the later unbind removes; a failure there
     * leaves the appender unbound ([isBound] false).
     *
     * @param info Configuration metadata for the info gauges; `null`
     *             (the benchmarks' sender-only binding) registers none.
     */
    @JvmOverloads
    fun bind(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        appenderName: String?,
        circuitBreakerRegistry: CircuitBreakerRegistry,
        producerRegistry: ProducerRegistry,
        info: AppenderInfo? = null,
    ): KafkaAppenderMetrics? {
        if (registry === boundRegistry) {
            unbind()
        }
        // Invariant: every meter of this appender carries the identical
        // appender tag value - same derivation as
        // MicrometerKafkaAppenderMetrics.
        val appenderTag = appenderName?.takeIf { it.isNotBlank() } ?: "unnamed"
        if (isIdentityBound(registry, commonTags, appenderTag)) {
            status.addWarn(
                "MeterRegistry already contains the meters of a KafkaAppender tagged appender='$appenderTag' " +
                    "with the same common tags - another KafkaAppender instance with the same (or no) name is " +
                    "bound to this registry. Metrics binding refused: Micrometer would hand both instances the " +
                    "same meters, mixing their counts and removing the shared series when either stops. " +
                    "Give each KafkaAppender a distinct name and bind again.",
            )
            return null
        }
        // Safety: transactional - a failure halfway removes the meters
        // registered so far, so nothing is left for the identity probe
        // to mistake for a live appender on the retry.
        val impl =
            MicrometerKafkaAppenderMetrics.register(registry, commonTags, appenderName, info) { message, cause ->
                status.addWarn(message, cause)
            }
        // Invariant: the new binding stands; only now replace the
        // previous one.
        unbind()
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
        producerMetricsMirror?.close()
        producerMetricsMirror = null
        // Safety: per meter - one failing removal must not skip the
        // rest, which would stay registered with nothing left to remove
        // them.
        boundResilience4jMeters.forEach { meter ->
            try {
                registry.remove(meter)
            } catch (e: Exception) {
                status.addWarn("Error removing Resilience4j meter '${meter.id.name}': ${e.message}", e)
            }
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
     * meters, tags and semantics of `resilience4j-micrometer`'s
     * `TaggedCircuitBreakerMetrics` 1:1 under the [CIRCUIT_BREAKER_PREFIX]
     * namespace and adds the `appender` tag (plus the operator's common
     * tags) - see the class KDoc for why the official binder is not
     * used. Failures are reported via the status manager; every meter
     * registered before a failure is tracked and torn down by [unbind].
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
            // Rationale: WARN like the teardown failures - an operator
            // filtering the status output for warnings must learn that the
            // breaker meters are missing.
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
            registerBreakerGauge(
                registry = registry,
                breaker = breaker,
                tags = tags,
                gauge =
                    BreakerGauge(
                        name = "$CIRCUIT_BREAKER_PREFIX.state",
                        tag = Tag.of("state", state.name.lowercase()),
                        description = "The states of the circuit breaker",
                    ) { if (it.state == state) 1.0 else 0.0 },
            )
        }
        for (gauge in BREAKER_GAUGES) {
            registerBreakerGauge(registry = registry, breaker = breaker, tags = tags, gauge = gauge)
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
                .builder("$CIRCUIT_BREAKER_PREFIX.not.permitted.calls")
                .description("Total number of not permitted calls")
                .tags(tags)
                .tag("kind", "not_permitted")
                .register(registry)
                .also { boundResilience4jMeters += it }
    }

    /** Registers one breaker gauge (plus its optional extra tag) and tracks it for [unbind]. */
    private fun registerBreakerGauge(
        registry: MeterRegistry,
        breaker: CircuitBreaker,
        tags: Tags,
        gauge: BreakerGauge,
    ) {
        val builder = Gauge.builder(gauge.name, breaker) { gauge.value(it) }.description(gauge.description).tags(tags)
        gauge.tag?.let { builder.tag(it.key, it.value) }
        boundResilience4jMeters += builder.register(registry)
    }

    /** Registers one `kafka.appender.circuitbreaker.calls` timer of the given kind and tracks it for [unbind]. */
    private fun registerCallTimer(
        registry: MeterRegistry,
        tags: Tags,
        kind: String,
        description: String,
    ): Timer =
        Timer
            .builder("$CIRCUIT_BREAKER_PREFIX.calls")
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
        val mirror =
            RenamingMeterMirror(registry, ::kafkaClientMeterName) { message, cause ->
                status.addWarn("Kafka producer metrics: $message", cause)
            }
        producerMetricsMirror = mirror
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
            binding.bindTo(mirror.source)
            producerMetricBindings += binding
        }
    }

    companion object {
        /** Name prefix of the circuit-breaker meters (official binder: `resilience4j.circuitbreaker`). */
        const val CIRCUIT_BREAKER_PREFIX: String = "kafka.appender.circuitbreaker"

        /**
         * The buffered/slow-call and rate gauges of the official binder,
         * one row each: name, kind tag (`null` = untagged), description,
         * value. Rationale: the mirror the class KDoc commits to checking
         * on every Resilience4j upgrade is this one list, not a page of
         * builder chains.
         */
        private val BREAKER_GAUGES: List<BreakerGauge> =
            listOf(
                BreakerGauge("$CIRCUIT_BREAKER_PREFIX.buffered.calls", Tag.of("kind", "successful"), "The number of buffered successful calls stored in the ring buffer") {
                    it.metrics.numberOfSuccessfulCalls.toDouble()
                },
                BreakerGauge("$CIRCUIT_BREAKER_PREFIX.buffered.calls", Tag.of("kind", "failed"), "The number of buffered failed calls stored in the ring buffer") {
                    it.metrics.numberOfFailedCalls.toDouble()
                },
                BreakerGauge("$CIRCUIT_BREAKER_PREFIX.slow.calls", Tag.of("kind", "successful"), "The number of slow successful calls which were slower than a certain threshold") {
                    it.metrics.numberOfSlowSuccessfulCalls.toDouble()
                },
                BreakerGauge("$CIRCUIT_BREAKER_PREFIX.slow.calls", Tag.of("kind", "failed"), "The number of slow failed calls which were slower than a certain threshold") {
                    it.metrics.numberOfSlowFailedCalls.toDouble()
                },
                BreakerGauge("$CIRCUIT_BREAKER_PREFIX.failure.rate", null, "The failure rate of the circuit breaker") {
                    it.metrics.failureRate.toDouble()
                },
                BreakerGauge("$CIRCUIT_BREAKER_PREFIX.slow.call.rate", null, "The slow call rate of the circuit breaker") {
                    it.metrics.slowCallRate.toDouble()
                },
            )

        /**
         * The appender-namespace name of a Kafka client binder meter. Every
         * meter of the binder lands under `kafka.appender.producer.`:
         * `kafka.producer.record.send.total` becomes
         * `kafka.appender.producer.record.send.total` (likewise the
         * `producer.node` and `producer.topic` groups), and a group outside
         * `kafka.producer` - the client's `kafka.app.info` - becomes
         * `kafka.appender.producer.app.info` rather than
         * `kafka.appender.app.info`, which would read like one of the
         * appender's own meters next to `kafka.appender.info`. `null` for
         * anything else (the binder's transient `delete.this` probe), which
         * is then not published.
         */
        internal fun kafkaClientMeterName(name: String): String? {
            if (!name.startsWith("kafka.") || name.startsWith("kafka.appender.")) return null
            val group = name.removePrefix("kafka.")
            return if (group.startsWith("producer.")) "kafka.appender.$group" else "kafka.appender.producer.$group"
        }
    }
}

/**
 * One row of the circuit-breaker gauge table: meter [name], an optional
 * extra [tag] (the official binder's `kind`/`state`), [description] and
 * the [value] read from the breaker. A plain class: it carries a
 * function, so data-class equality would be meaningless.
 */
private class BreakerGauge(
    val name: String,
    val tag: Tag?,
    val description: String,
    val value: (CircuitBreaker) -> Double,
)
