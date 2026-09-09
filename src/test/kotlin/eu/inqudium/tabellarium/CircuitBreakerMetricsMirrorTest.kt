package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * Contract test for the appender's own circuit-breaker binder: it
 * promises to mirror `resilience4j-micrometer`'s
 * `TaggedCircuitBreakerMetrics` 1:1 in meter names, meter types and
 * tags (plus the `appender` tag), so existing dashboards keep working.
 * The official binder is on the test classpath only, as the reference;
 * the shipped code keeps not depending on it (see the rationale in
 * [MetricsBindings]).
 *
 * Compatibility: a Resilience4j upgrade that renames, adds or removes a
 * meter in the official binder fails this test instead of silently
 * drifting the mirror (the upgrade-tracking duty the binder's KDoc
 * states).
 */
class CircuitBreakerMetricsMirrorTest {
    // -- Test fixtures --------------------------------------------------

    private val createdAppenders = mutableListOf<KafkaAppender>()

    @AfterEach
    fun stopAppenders() {
        createdAppenders.forEach { it.stop() }
        createdAppenders.clear()
    }

    /** The meter identities of the breaker meters in [registry], without the tag only the mirror adds. */
    private fun breakerMeterIds(registry: MeterRegistry): Set<Triple<String, Meter.Type, Set<Pair<String, String>>>> =
        registry.meters
            .filter { it.id.name.startsWith("resilience4j.circuitbreaker") }
            .map { meter ->
                Triple(
                    meter.id.name,
                    meter.id.type,
                    meter.id.tags
                        .filter { it.key != MicrometerKafkaAppenderMetrics.TAG_APPENDER }
                        .map { it.key to it.value }
                        .toSet(),
                )
            }.toSet()

    // -- Tests ----------------------------------------------------------

    @Test
    fun `should mirror the meter ids of the official resilience4j-micrometer binder`() {
        // What is to be tested? Whether the appender's own breaker binder
        //   registers exactly the meters (names, types, tags) that
        //   resilience4j-micrometer's TaggedCircuitBreakerMetrics
        //   registers for the same breaker - the 1:1 mirror the binder
        //   promises so that dashboards built for the official binder
        //   keep working.
        // How will the test case be deemed successful and why? Successful
        //   if the set of (name, type, tags-without-appender) of the
        //   appender's breaker meters equals the official binder's set for
        //   the same CircuitBreakerRegistry. Both binders see the one
        //   breaker the minimal configuration activates
        //   (kafka-appender-technical), so the comparison covers the state
        //   gauges, the buffered/slow-call gauges, the rates, the call
        //   timers and the not-permitted counter.
        // Why is it important to test this test case? The mirror is
        //   hand-maintained; a Resilience4j upgrade that changes the
        //   official binder would otherwise drift the appender's meters
        //   without any build signal, and operators would notice only when
        //   a dashboard panel goes empty.

        // Given: one breaker registry shared by both binders, bound to the
        //   appender under test on one side and to the reference on the other
        val breakerRegistry = ResilientMessageSender.defaultCircuitBreakerRegistry()
        val appender =
            KafkaAppender().also { createdAppenders += it }.apply {
                context = LoggerContext()
                name = "KAFKA_MIRROR"
                encoder = MessageBytesEncoder()
                component = "mirror-test"
                cmdbId = "CMDB-TEST"
                environment = "test"
                kafkaProducerProperties = "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092"
                topicMapping = TopicMappingConfig().apply { defaultTopic = "default.topic" }
                producerFactory = RecordingProducerFactory()
                circuitBreakerRegistry = breakerRegistry
            }
        appender.start()
        assertThat(appender.isStarted).isTrue()
        val ownRegistry = SimpleMeterRegistry()
        val referenceRegistry = SimpleMeterRegistry()

        // When: both binders register against the same breakers
        appender.bindMeterRegistry(ownRegistry)
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(breakerRegistry).bindTo(referenceRegistry)

        // Then: identical meter identities once the appender tag is stripped
        val own = breakerMeterIds(ownRegistry)
        val reference = breakerMeterIds(referenceRegistry)
        assertThat(reference).isNotEmpty()
        assertThat(own).isEqualTo(reference)
        // ... and the appender's meters really carry the tag that was stripped
        assertThat(ownRegistry.meters.filter { it.id.name.startsWith("resilience4j.circuitbreaker") })
            .allSatisfy { assertThat(it.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER)).isEqualTo("KAFKA_MIRROR") }
    }
}
