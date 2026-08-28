package eu.inqudium.tabellarium

import ch.qos.logback.core.spi.ContextAware
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags

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
 * and remove their meters on close), removes the circuit-breaker
 * meters for this appender's breaker names (the Resilience4j binder
 * offers no removal API, so the meters are found by name prefix plus
 * breaker-name tag - restricted to this appender's own breakers so an
 * operator's unrelated breakers on a shared registry are never
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
    private var boundBreakerNames: Set<String> = emptySet()
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
        boundBreakerNames =
            producerRegistry.activeTopicClasses
                .map { ResilientMessageSender.circuitBreakerName(it) }
                .toSet()
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
            removeResilience4jMeters(registry)
        } catch (e: Exception) {
            status.addWarn("Error removing Resilience4j meters: ${e.message}", e)
        }
        try {
            boundMetrics?.deregisterFrom(registry)
        } catch (e: Exception) {
            status.addWarn("Error deregistering appender meters: ${e.message}", e)
        }
        boundMetrics = null
        boundRegistry = null
        boundBreakerNames = emptySet()
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
        io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
            .ofCircuitBreakerRegistry(circuitBreakerRegistry)
            .bindTo(registry)
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
            val binding =
                io.micrometer.core.instrument.binder.kafka
                    .KafkaClientMetrics(producer, tagsForClass)
            binding.bindTo(registry)
            producerMetricBindings += binding
        }
    }

    private fun removeResilience4jMeters(registry: MeterRegistry) {
        registry.meters
            .filter { meter ->
                meter.id.name.startsWith("resilience4j.circuitbreaker") &&
                    meter.id.getTag("name") in boundBreakerNames
            }.forEach { registry.remove(it) }
    }
}
