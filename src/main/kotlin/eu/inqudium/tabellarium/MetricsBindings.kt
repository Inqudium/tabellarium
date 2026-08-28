package eu.inqudium.tabellarium

import ch.qos.logback.core.spi.ContextAware
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import java.util.Collections
import java.util.IdentityHashMap

/**
 * Owns the Micrometer side of a [KafkaAppender]'s lifecycle: binding
 * (appender meters, per-producer Kafka client metrics, Resilience4j
 * circuit-breaker metrics) and the symmetric teardown. Extracted from
 * the appender so the composition root keeps a single responsibility
 * and the bind/unbind pairing lives in one place.
 *
 * ## Lazy class-loading pattern for the optional binders
 *
 * Both optional integrations start with a [Class.forName] **probe**
 * that succeeds only when the bridge class is on the classpath. If the
 * probe throws [ClassNotFoundException], the typed `doBind…` method is
 * never entered and the JVM never has to resolve the symbols it
 * references - so the appender works without `resilience4j-micrometer`
 * or the Micrometer Kafka binder in the dependency tree. The `doBind…`
 * methods use the bridge classes **directly** (no reflection): Kotlin
 * compiles `private fun` to a regular private JVM method whose
 * referenced types are resolved on first invocation, which the probe
 * gates.
 *
 * ## Teardown
 *
 * [unbind] reverses everything a bind registered: it closes the
 * per-producer `KafkaClientMetrics` binders (they are [AutoCloseable]
 * and remove their meters on close), removes exactly the
 * circuit-breaker meters this instance's bind added (the Resilience4j
 * binder offers no removal API, so the meters are recorded as a
 * registry diff around `bindTo` - identity-based, so neither an
 * operator's unrelated breakers nor another KafkaAppender's
 * identically-named breaker meters on a shared registry are ever
 * touched), and deregisters the appender's own meters. Without this,
 * every Logback reconfiguration cycle would leak meters and leave
 * gauges reporting a closed dispatcher's queue.
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
     * Exactly the meters the Resilience4j bind of THIS instance added to
     * the registry (identity-compared: [Meter] does not override
     * equals). Recorded as a before/after diff around `bindTo` so
     * [unbind] can remove precisely these - and never a meter that
     * another KafkaAppender instance registered under the same
     * (per-topic-class, appender-agnostic) breaker names.
     */
    private var boundResilience4jMeters: List<Meter> = emptyList()
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
        warnOnBreakerMeterCollision(registry, producerRegistry)
        bindResilience4jMetrics(registry, circuitBreakerRegistry)
        bindKafkaProducerMetrics(registry, commonTags, producerRegistry)
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
        boundResilience4jMeters = emptyList()
        try {
            boundMetrics?.deregisterFrom(registry)
        } catch (e: Exception) {
            status.addWarn("Error deregistering appender meters: ${e.message}", e)
        }
        boundMetrics = null
        boundRegistry = null
    }

    /**
     * The circuit-breaker names are derived from the topic class alone,
     * so two KafkaAppender instances bound to the same MeterRegistry
     * produce colliding Resilience4j meter IDs: state gauges then keep
     * reporting whichever breaker registered first, and counters mix
     * both instances. The binding itself stays best-effort - but the
     * operator gets told that the breaker metrics are not trustworthy
     * in this setup.
     */
    private fun warnOnBreakerMeterCollision(
        registry: MeterRegistry,
        producerRegistry: ProducerRegistry,
    ) {
        val breakerNames =
            producerRegistry.activeTopicClasses
                .map { ResilientMessageSender.circuitBreakerName(it) }
                .toSet()
        val colliding =
            registry.meters
                .filter { meter ->
                    meter.id.name.startsWith("resilience4j.circuitbreaker") &&
                        meter.id.getTag("name") in breakerNames
                }.mapNotNull { it.id.getTag("name") }
                .toSortedSet()
        if (colliding.isEmpty()) return
        status.addWarn(
            "MeterRegistry already contains circuit-breaker meters for ${colliding.joinToString()} - " +
                "most likely from another KafkaAppender instance bound to the same registry. " +
                "The colliding breaker gauges/counters will not reflect this appender's state; " +
                "bind each appender to its own registry (or distinct common tags) for " +
                "trustworthy per-appender breaker metrics.",
        )
    }

    /**
     * Best-effort binding of Resilience4j circuit-breaker metrics.
     * Requires `io.github.resilience4j:resilience4j-micrometer` on the
     * classpath; silently skipped if absent (expected when operators
     * opt out), reported via status manager if the call itself fails.
     */
    private fun bindResilience4jMetrics(
        registry: MeterRegistry,
        circuitBreakerRegistry: CircuitBreakerRegistry,
    ) {
        try {
            Class.forName("io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics")
        } catch (_: ClassNotFoundException) {
            return
        }
        try {
            doBindResilience4jMetrics(registry, circuitBreakerRegistry)
        } catch (e: Exception) {
            status.addInfo(
                "Failed to bind Resilience4j metrics to MeterRegistry " +
                    "(circuit-breaker state metrics will be unavailable): ${e.message}",
            )
        }
    }

    private fun doBindResilience4jMetrics(
        registry: MeterRegistry,
        circuitBreakerRegistry: CircuitBreakerRegistry,
    ) {
        // Diff the registry around bindTo to learn which meter objects
        // THIS bind added; see boundResilience4jMeters for why removal
        // must not go by name.
        val before = Collections.newSetFromMap(IdentityHashMap<Meter, Boolean>())
        before.addAll(registry.meters)
        TaggedCircuitBreakerMetrics
            .ofCircuitBreakerRegistry(circuitBreakerRegistry)
            .bindTo(registry)
        boundResilience4jMeters = registry.meters.filter { it !in before }
    }

    /**
     * Best-effort binding of Kafka producer-internal metrics. Requires
     * the Micrometer Kafka binder on the classpath; silently skipped
     * if absent.
     */
    private fun bindKafkaProducerMetrics(
        registry: MeterRegistry,
        commonTags: Iterable<Tag>,
        producerRegistry: ProducerRegistry,
    ) {
        try {
            Class.forName("io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics")
        } catch (_: ClassNotFoundException) {
            return
        }
        try {
            doBindKafkaProducerMetrics(registry, commonTags, producerRegistry)
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
        producerRegistry: ProducerRegistry,
    ) {
        for (topicClass in producerRegistry.activeTopicClasses) {
            val producer = producerRegistry.producerFor(topicClass)
            val tagsForClass =
                Tags
                    .of(commonTags)
                    .and(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS, topicClass.tag)
            val binding = KafkaClientMetrics(producer, tagsForClass)
            binding.bindTo(registry)
            producerMetricBindings += binding
        }
    }
}
