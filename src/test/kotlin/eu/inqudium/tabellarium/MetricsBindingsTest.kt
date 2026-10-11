package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.spi.ContextAwareBase
import ch.qos.logback.core.status.Status
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.RecordTooLargeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.ToDoubleFunction

/**
 * Pins the metrics orchestrator on its own: what a bind registers,
 * what a refused or failing re-bind leaves in place, how the breaker
 * event consumers survive re-bind and unbind, and what the teardown
 * removes. Every behaviour here is observable on the class - which
 * registry holds which meters, which warning was issued - so none of
 * these proofs pays for an appender start and stop. The appender test
 * keeps what needs the appender: the hot path recording into bound
 * meters, the unbind in `stop()` after the dispatcher drain, and two
 * appenders on one registry
 * (`ARCHITECTURE_REVIEW-2026-10-10T22-15-57`, finding 3).
 *
 * The breaker outcomes are driven on the breaker directly
 * (`onSuccess`, `onError`, `tryAcquirePermission` on an open breaker):
 * the event publisher is what the binder's consumers listen to, and a
 * `send` would only add the asynchronous worker in front of it.
 */
class MetricsBindingsTest {
    // -- Test fixtures --------------------------------------------------

    /** The status sink of a binding: a fresh Logback context whose status manager records every entry. */
    private class RecordingStatus : ContextAwareBase() {
        init {
            context = LoggerContext()
        }

        val entries: List<Status>
            get() = context.statusManager.copyOfStatusList

        val messages: List<String>
            get() = entries.map { it.message }
    }

    /** Every producer registry a test built; closed after the test. */
    private val openRegistries = mutableListOf<ProducerRegistry>()

    /**
     * Every [Fixture] a test built; unbound after the test. Each bind
     * starts one `KafkaClientMetrics` refresh scheduler per active topic
     * class that only the binder close in `unbind()` stops - a fixture
     * left bound leaks that daemon thread and an open mirror for the
     * rest of the fork (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 3).
     */
    private val fixtures = mutableListOf<Fixture>()

    @AfterEach
    fun unbindAndCloseRegistries() {
        // The binders read the producers' metrics: unbind before the producers close.
        fixtures.forEach { it.bindings.unbind() }
        fixtures.clear()
        openRegistries.forEach { it.close() }
        openRegistries.clear()
    }

    /** The live threads of the Kafka client binder's refresh schedulers (`NamedThreadFactory("micrometer-kafka-metrics")`). */
    private fun kafkaMetricsSchedulerThreads(): Set<Thread> = Thread.getAllStackTraces().keys.filterTo(HashSet()) { it.isAlive && it.name.startsWith(KAFKA_METRICS_THREAD_PREFIX) }

    /** One [TopicClass.TECHNICAL] producer from [factory]; the default [RecordingProducerFactory] hands out `MockProducer`s, which expose no client metrics. */
    private fun producerRegistry(
        factory: ProducerFactory = RecordingProducerFactory(),
        bootstrapServers: String = "test:9092",
    ): ProducerRegistry =
        ProducerRegistry
            .create(
                propertiesBuilder =
                    ProducerPropertiesBuilder(
                        mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers),
                        defaultClientIdPrefix = "tabellarium-bindings-test",
                    ),
                activeTopicClasses = setOf(TopicClass.TECHNICAL),
                producerFactory = factory,
            ).also { openRegistries += it }

    /** A registry with one real `KafkaProducer` (no broker needed) - a `MockProducer` exposes no client metrics, so a rename that never fired would pass unnoticed. */
    private fun realProducers(): ProducerRegistry = producerRegistry(factory = ProducerFactory.default(), bootstrapServers = "localhost:1")

    /** The default breaker registry with the technical breaker already created - the one the sender would create on start. */
    private fun breakerRegistry(): CircuitBreakerRegistry =
        ResilientMessageSender.defaultCircuitBreakerRegistry().also {
            it.circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.TECHNICAL))
        }

    /** A [MetricsBindings] with its collaborators, bound through [bind] with the appender name `KAFKA` unless a test says otherwise. */
    private inner class Fixture(
        val producers: ProducerRegistry = producerRegistry(),
        val breakers: CircuitBreakerRegistry = breakerRegistry(),
    ) {
        val status = RecordingStatus()
        val bindings = MetricsBindings(status)

        init {
            fixtures += this
        }

        fun bind(
            registry: MeterRegistry,
            appenderName: String = "KAFKA",
            commonTags: Tags = Tags.empty(),
        ): KafkaAppenderMetrics? = bindings.bind(registry, commonTags, appenderName, breakers, producers)

        val technicalBreaker: CircuitBreaker
            get() = breakers.circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.TECHNICAL))
    }

    private fun MeterRegistry.appenderMeterIds(): List<Meter.Id> = meters.filter { it.id.name.startsWith("kafka.appender.") }.map { it.id }

    private fun MeterRegistry.breakerCallTimer(
        kind: String,
        appenderName: String = "KAFKA",
    ) = requireTimer(
        "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.calls",
        "kind" to kind,
        MicrometerKafkaAppenderMetrics.TAG_APPENDER to appenderName,
    )

    private fun MeterRegistry.notPermittedCounter(appenderName: String = "KAFKA") =
        requireCounter(
            "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.not.permitted.calls",
            MicrometerKafkaAppenderMetrics.TAG_APPENDER to appenderName,
        )

    // -- Tests ----------------------------------------------------------

    @Nested
    inner class `Binding` {
        @Test
        fun `should register the appender and breaker meters under kafka-appender tagged with the appender name`() {
            // What is to be tested? Whether one bind registers the appender's
            //   own meters and the breaker meters of every breaker in the
            //   registry, all under the kafka.appender. prefix and all tagged
            //   with the appender name - the tag that keeps two instances'
            //   meter ids apart on a shared registry.
            // How will the test case be deemed successful and why? Successful
            //   if bind returns an implementation, the binding reports itself
            //   bound to exactly this registry, the accepted-events counter
            //   is registered, and the breaker meters (state gauge, call
            //   timers, not-permitted counter, buffered calls, failure rate)
            //   exist for the technical breaker, each carrying appender=KAFKA
            //   and name=kafka-appender-technical. Meters, types and tags
            //   match TaggedCircuitBreakerMetrics with the prefix replaced
            //   (CircuitBreakerMetricsMirrorTest pins that equality).
            // Why is it important to test this test case? The breaker meters
            //   are the operator's primary broker-outage signal, and the
            //   appender tag is what finding M-5 of
            //   docs/assessment/CODE_ANALYSIS-2026-08-28T22-20-43.md found
            //   missing; both are pinned here without an appender.

            // Given
            val registry = SimpleMeterRegistry()
            val fixture = Fixture()

            // When
            val impl = fixture.bind(registry)

            // Then: bound, and the inventory is there
            assertThat(impl).isNotNull
            assertThat(fixture.bindings.isBound).isTrue()
            assertThat(fixture.bindings.isBoundTo(registry)).isTrue()
            registry.requireCounter(
                MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
            )
            val breakerMeters = registry.meters.filter { it.id.name.startsWith(MetricsBindings.CIRCUIT_BREAKER_PREFIX) }
            assertThat(breakerMeters).isNotEmpty
            assertThat(breakerMeters).allSatisfy { meter ->
                assertThat(meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER)).isEqualTo("KAFKA")
                assertThat(meter.id.getTag("name")).isEqualTo("kafka-appender-technical")
            }
            assertThat(breakerMeters.map { it.id.name }).contains(
                "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.state",
                "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.calls",
                "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.not.permitted.calls",
                "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.buffered.calls",
                "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.failure.rate",
            )
            assertThat(fixture.status.entries).noneMatch { it.level >= Status.WARN }
        }

        @Test
        fun `should refuse a second binding with the same meter identity and register nothing`() {
            // What is to be tested? The identity probe: a second binding
            //   whose appender tag and common tags equal those of a live
            //   binding on the same registry is refused as a whole, because
            //   Micrometer would hand both the same meter objects.
            // How will the test case be deemed successful and why? Successful
            //   if the second bind returns null, reports itself unbound,
            //   warns naming the tag, and the registry's meter set is
            //   unchanged; and if the first binding's later unbind leaves
            //   the registry empty - nothing of the refused one is left
            //   behind (docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md,
            //   L-2).
            // Why is it important to test this test case? Shared meters mix
            //   two instances' counts and let the first unbind remove the
            //   series the other still publishes through - an outage shown
            //   as "no data".

            // Given: a live binding named KAFKA
            val registry = SimpleMeterRegistry()
            val first = Fixture()
            first.bind(registry)
            val metersAfterFirst = registry.meters.map { it.id }

            // When: a second instance with the same name binds to the same registry
            val second = Fixture()
            val refused = second.bind(registry)

            // Then: refused, warned, nothing added
            assertThat(refused).isNull()
            assertThat(second.bindings.isBound).isFalse()
            assertThat(second.status.entries)
                .anyMatch { it.level == Status.WARN && it.message.contains("Metrics binding refused") && it.message.contains("appender='KAFKA'") }
            assertThat(registry.meters.map { it.id }).containsExactlyInAnyOrderElementsOf(metersAfterFirst)
            assertThat(first.bindings.isBound).isTrue()

            // And: the first binding owns everything that is registered
            first.bindings.unbind()
            assertThat(registry.meters).isEmpty()
        }

        @Test
        fun `should treat different common tags as a different meter identity`() {
            // What is to be tested? Whether the identity probe includes the
            //   common tags: two same-named instances whose operators tag
            //   them differently ask Micrometer for distinct ids and may both
            //   bind.
            // How will the test case be deemed successful and why? Successful
            //   if both binds succeed, the accepted-events counter exists
            //   once per tag value, and unbinding one leaves the other's
            //   counter registered.
            // Why is it important to test this test case? A probe on the
            //   appender tag alone would refuse a legitimate second instance
            //   that is distinguishable by its common tags - and the
            //   refusal message would send the operator to rename an
            //   appender that needs no renaming.

            // Given
            val registry = SimpleMeterRegistry()
            val blue = Fixture()
            val green = Fixture()

            // When
            val boundBlue = blue.bind(registry, commonTags = Tags.of("deployment", "blue"))
            val boundGreen = green.bind(registry, commonTags = Tags.of("deployment", "green"))

            // Then: both bound, both counters present
            assertThat(boundBlue).isNotNull
            assertThat(boundGreen).isNotNull
            registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED, "deployment" to "blue")
            registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED, "deployment" to "green")

            // When / Then: one unbind leaves the other intact
            blue.bindings.unbind()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED).tag("deployment", "blue").counters()).isEmpty()
            registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED, "deployment" to "green")
        }
    }

    @Nested
    inner class `Re-bind ordering` {
        @Test
        fun `should keep the binding to the first registry when a re-bind to another one is refused`() {
            // What is to be tested? A re-bind to a second registry that is
            //   refused (a same-named instance already publishes there) must
            //   leave the working binding to the first registry in place.
            // How will the test case be deemed successful and why? Successful
            //   if the refused call returns null, the binding still reports
            //   itself bound to the first registry, and the first registry
            //   still holds every meter. Before the fix the bind tore the
            //   first binding down before it knew the second would be
            //   refused, and the instance ended with no metrics at all
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 6).
            // Why is it important to test this test case? A refused call
            //   that silently turns working monitoring dark is worse than
            //   the refusal it reports.

            // Given: A bound to the first registry, a same-named B to the second
            val first = SimpleMeterRegistry()
            val second = SimpleMeterRegistry()
            val a = Fixture()
            val b = Fixture()
            assertThat(a.bind(first)).isNotNull
            assertThat(b.bind(second)).isNotNull
            val metersOnFirst = first.meters.map { it.id }

            // When: A is re-bound to the second registry
            val rebound = a.bind(second)

            // Then: refused, but the first binding is intact
            assertThat(rebound).isNull()
            assertThat(a.bindings.isBoundTo(first)).isTrue()
            assertThat(first.meters.map { it.id }).containsExactlyInAnyOrderElementsOf(metersOnFirst)
        }

        @Test
        fun `should keep the binding to the first registry when a re-bind to another one fails`() {
            // What is to be tested? A re-bind to a second registry whose
            //   registration throws must leave the binding to the first
            //   registry in place - the new binding is built before the old
            //   one is torn down.
            // How will the test case be deemed successful and why? Successful
            //   if the failure propagates to the caller, the binding still
            //   reports itself bound to the first registry, and the first
            //   registry's meters are all still there
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 6).
            // Why is it important to test this test case? A transient
            //   registry error on the new side must not cost the working old
            //   side.

            // Given
            val first = SimpleMeterRegistry()
            val counterRejectingRegistry =
                object : SimpleMeterRegistry() {
                    override fun newCounter(id: Meter.Id): Counter = throw IllegalStateException("simulated counter registration failure")
                }
            val fixture = Fixture()
            fixture.bind(first)
            val metersOnFirst = first.meters.map { it.id }

            // When / Then: the failure propagates
            assertThatThrownBy { fixture.bind(counterRejectingRegistry) }
                .hasMessageContaining("simulated counter registration failure")

            // And: the first binding is intact
            assertThat(fixture.bindings.isBoundTo(first)).isTrue()
            assertThat(first.meters.map { it.id }).containsExactlyInAnyOrderElementsOf(metersOnFirst)
        }

        @Test
        fun `should leave the instance unbound and the registry clean when a re-bind to the same registry fails`() {
            // What is to be tested? A re-bind to the SAME registry has to
            //   remove the old meters first (the new ones would share their
            //   ids and Micrometer would hand back the very objects the later
            //   unbind removes). When the registration then fails, the
            //   instance is unbound and the registry holds none of its meters.
            // How will the test case be deemed successful and why? Successful
            //   if the failure propagates, isBound is false afterwards and
            //   the registry holds no kafka.appender.* meter - neither the
            //   removed old ones nor a half-registered new set.
            // Why is it important to test this test case? A binding that
            //   reported itself unbound while meters stayed registered would
            //   leave orphans for the identity probe to mistake for a live
            //   instance on the retry (CHANGELOG, "Fixed": the leftover
            //   events.accepted counter refused every later bind).

            // Given: bound once; the registry rejects counters from now on
            val rejectCounters = AtomicBoolean(false)
            val registry =
                object : SimpleMeterRegistry() {
                    override fun newCounter(id: Meter.Id): Counter {
                        check(!rejectCounters.get()) { "simulated counter registration failure" }
                        return super.newCounter(id)
                    }
                }
            val fixture = Fixture()
            fixture.bind(registry)
            rejectCounters.set(true)

            // When
            assertThatThrownBy { fixture.bind(registry) }
                .hasMessageContaining("simulated counter registration failure")

            // Then: unbound, nothing left
            assertThat(fixture.bindings.isBound).isFalse()
            assertThat(registry.appenderMeterIds()).isEmpty()
        }

        @Test
        fun `should replace the binding on a re-bind to the same registry without duplicating or refusing`() {
            // What is to be tested? The documented re-bind semantics: a second
            //   bind to the registry already bound replaces the first instead
            //   of being refused by the identity probe (its own meters are
            //   removed before the probe runs) and instead of registering a
            //   second set.
            // How will the test case be deemed successful and why? Successful
            //   if the second bind returns an implementation, the binding
            //   is still bound to the registry, the meter id set is the same
            //   as after the first bind, and no refusal was warned.
            // Why is it important to test this test case? Spring's context
            //   refresh and manual re-wiring re-bind in production; a probe
            //   that saw its own meters would refuse every re-bind, and a
            //   missing unbind would double the inventory per cycle.

            // Given
            val registry = SimpleMeterRegistry()
            val fixture = Fixture()
            fixture.bind(registry)
            val metersAfterFirst = registry.meters.map { it.id }

            // When
            val rebound = fixture.bind(registry)

            // Then
            assertThat(rebound).isNotNull
            assertThat(fixture.bindings.isBoundTo(registry)).isTrue()
            assertThat(registry.meters.map { it.id }).containsExactlyInAnyOrderElementsOf(metersAfterFirst)
            assertThat(fixture.status.messages).noneMatch { it.contains("Metrics binding refused") }
        }
    }

    @Nested
    inner class `Breaker call meters` {
        @Test
        fun `should route each breaker outcome to the call meter of its kind and to no other`() {
            // What is to be tested? The hand-wired breaker event consumers:
            //   a success lands on the kind=successful timer, a counted error
            //   on kind=failed, an ignored exception (RecordTooLargeException
            //   per the default breaker configuration) on kind=ignored, and
            //   a permission refused by an open breaker on the not-permitted
            //   counter - each exactly once and on no other meter.
            // How will the test case be deemed successful and why? Successful
            //   if after one event of each kind every one of the four meters
            //   reads exactly 1. Each phase asserts all four, so a swapped
            //   pair of holders (failed/ignored) or a dropped consumer turns
            //   it red (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 4).
            // Why is it important to test this test case? "failed calls" is
            //   the series operators read during an outage and
            //   "not permitted" is how they size its loss; a mis-wired
            //   consumer would show a healthy breaker while Kafka is down.

            // Given: a bound instance and its technical breaker
            val registry = SimpleMeterRegistry()
            val fixture = Fixture()
            fixture.bind(registry)
            val breaker = fixture.technicalBreaker

            fun counts() =
                listOf(
                    registry.breakerCallTimer("successful").count(),
                    registry.breakerCallTimer("failed").count(),
                    registry.breakerCallTimer("ignored").count(),
                    registry.notPermittedCounter().count().toLong(),
                )
            assertThat(counts()).containsExactly(0L, 0L, 0L, 0L)

            // When / Then: one outcome per kind, each counted once in its own meter
            breaker.onSuccess(5, TimeUnit.MILLISECONDS)
            assertThat(counts()).containsExactly(1L, 0L, 0L, 0L)
            breaker.onError(5, TimeUnit.MILLISECONDS, IllegalStateException("simulated broker outage"))
            assertThat(counts()).containsExactly(1L, 1L, 0L, 0L)
            breaker.onError(5, TimeUnit.MILLISECONDS, RecordTooLargeException("simulated oversized record"))
            assertThat(counts()).containsExactly(1L, 1L, 1L, 0L)
            breaker.transitionToOpenState()
            assertThat(breaker.tryAcquirePermission()).isFalse()
            assertThat(counts()).containsExactly(1L, 1L, 1L, 1L)
        }

        @Test
        fun `should count a breaker outcome exactly once after a re-bind and nowhere after an unbind`() {
            // What is to be tested? The lifecycle of the breaker event
            //   consumers, which Resilience4j cannot deregister: a re-bind
            //   must reuse the per-breaker holder (fresh meters, no second
            //   set of consumers), and an unbind must clear the holder so
            //   later events record into nothing.
            // How will the test case be deemed successful and why? Successful
            //   if after a re-bind to the same registry one success counts
            //   exactly once on the new successful-calls timer, and if after
            //   the unbind a further success leaves that timer object
            //   (captured before the unbind) at its count and the registry
            //   without call timers. Attaching the consumers unconditionally
            //   makes the re-bound timer count 2; deleting the clear() in
            //   unbind makes the captured timer count 2
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 4).
            // Why is it important to test this test case? Stacked consumers
            //   would double every breaker series after each context
            //   refresh, and a missing clear would keep recording into
            //   removed meters.

            // Given: bound, with one counted success
            val registry = SimpleMeterRegistry()
            val fixture = Fixture()
            fixture.bind(registry)
            val breaker = fixture.technicalBreaker
            breaker.onSuccess(1, TimeUnit.MILLISECONDS)
            assertThat(registry.breakerCallTimer("successful").count()).isEqualTo(1L)

            // When: re-bound to the same registry, one more success
            assertThat(fixture.bind(registry)).isNotNull
            val rebound = registry.breakerCallTimer("successful")
            breaker.onSuccess(1, TimeUnit.MILLISECONDS)

            // Then: the new timer counts it exactly once
            assertThat(rebound.count()).isEqualTo(1L)

            // When: unbound, one more success
            fixture.bindings.unbind()
            breaker.onSuccess(1, TimeUnit.MILLISECONDS)

            // Then: recorded nowhere
            assertThat(rebound.count()).isEqualTo(1L)
            assertThat(registry.find("${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.calls").timers()).isEmpty()
        }
    }

    @Nested
    inner class `Failures` {
        @Test
        fun `should report a failing breaker metrics binding as a warning and stay bound`() {
            // What is to be tested? Whether a failure inside the breaker
            //   metrics binding (a registry that rejects gauges) is reported
            //   through the status sink at WARN and does not fail the bind:
            //   the appender's own meters stand, the breaker meters are
            //   reported missing.
            // How will the test case be deemed successful and why? Successful
            //   if bind returns an implementation, the binding is bound, and
            //   a WARN entry names the failed Resilience4j binding. Before
            //   the fix the report was INFO, invisible to an operator
            //   filtering the status output for warnings
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 11).
            // Why is it important to test this test case? Missing breaker
            //   meters are a silent observability gap: no state series, no
            //   alert, and the only trace of why is this one status entry.

            // Given: a registry whose gauge registration throws
            val gaugeRejectingRegistry =
                object : SimpleMeterRegistry() {
                    override fun <T : Any> newGauge(
                        id: Meter.Id,
                        obj: T?,
                        valueFunction: ToDoubleFunction<T>,
                    ): Gauge = throw IllegalStateException("simulated gauge registration failure")
                }
            val fixture = Fixture()

            // When
            val impl = fixture.bind(gaugeRejectingRegistry)

            // Then: bound, and the failure is a WARN
            assertThat(impl).isNotNull
            assertThat(fixture.bindings.isBound).isTrue()
            assertThat(fixture.status.entries)
                .anyMatch { it.level == Status.WARN && it.message.contains("Failed to bind Resilience4j metrics") }
        }

        @Test
        fun `should report a failed queue gauge registration through the binding's status sink as a warning`() {
            // What is to be tested? The warn sink bind hands to
            //   MicrometerKafkaAppenderMetrics.register: a queue gauge pair
            //   that a registry rejects must reach the appender's status
            //   manager as a WARN naming the gauges and their consequence -
            //   driven through the implementation the binding returned, not
            //   through a test-owned lambda.
            // How will the test case be deemed successful and why? Successful
            //   if, after registerFallbackQueueGauges on the returned
            //   implementation against a registry that rejects only the
            //   fallback-capacity gauge, the status holds a WARN entry with
            //   the "Failed to register the fallback queue gauges" message and
            //   the "queue saturation will be unobserved" consequence, with
            //   the registry's exception attached. Every other test captures
            //   the warn hook with its own lambda; dropping the lambda in bind
            //   (the default sink is silence) or downgrading it to addInfo
            //   stayed green (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 4).
            // Why is it important to test this test case? The warning is the
            //   one signal that the fallback queue is unobserved; with the
            //   wiring gone the failure it was added to make visible becomes
            //   invisible again.

            // Given: a registry that rejects the fallback-capacity gauge only
            val registry =
                object : SimpleMeterRegistry() {
                    override fun <T : Any> newGauge(
                        id: Meter.Id,
                        obj: T?,
                        valueFunction: ToDoubleFunction<T>,
                    ): Gauge {
                        check(id.name != MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY) { "simulated gauge registration failure" }
                        return super.newGauge(id, obj, valueFunction)
                    }
                }
            val fixture = Fixture()
            val impl = checkNotNull(fixture.bind(registry)) { "bind refused" }
            assertThat(fixture.status.entries).noneMatch { it.level >= Status.WARN }

            // When: the dispatcher wiring registers the pair through the returned implementation
            impl.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 1024)

            // Then: a WARN on the binding's status, cause attached, no half pair
            assertThat(fixture.status.entries)
                .filteredOn { it.message.contains("Failed to register the fallback queue gauges") }
                .singleElement()
                .satisfies({ entry ->
                    assertThat(entry.level).isEqualTo(Status.WARN)
                    assertThat(entry.message).contains("queue saturation will be unobserved")
                    assertThat(entry.throwable).hasMessage("simulated gauge registration failure")
                })
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE).gauge()).isNull()
        }

        @Test
        fun `should track the breaker meters registered before a failure so the unbind removes them`() {
            // What is to be tested? The KDoc promise of the breaker binding:
            //   "every meter registered before a failure is tracked and torn
            //   down by unbind". With the registry rejecting the
            //   not-permitted counter - the LAST meter of the breaker set,
            //   after ten state/rate gauges and three call timers - the
            //   failure is reported, the bind stands, and the unbind leaves
            //   the registry empty.
            // How will the test case be deemed successful and why? Successful
            //   if bind returns an implementation, a WARN names the failed
            //   Resilience4j binding, the registry holds breaker meters after
            //   the bind (so the failure struck mid-set), and after unbind()
            //   no meter is left. The sibling test above rejects the FIRST
            //   breaker gauge, so nothing was registered when the catch ran;
            //   collecting the breaker meters into a local list and tracking
            //   them only on normal completion stayed green there and orphans
            //   thirteen meters here (DEFECT_ANALYSIS-2026-10-11T16-25-57,
            //   finding 20).
            // Why is it important to test this test case? Orphaned breaker
            //   meters keep reporting a stopped appender's state, and an
            //   orphaned events.accepted-like series is exactly what the
            //   identity probe mistakes for a live instance.

            // Given: a registry that rejects the counters under the breaker prefix
            val registry =
                object : SimpleMeterRegistry() {
                    override fun newCounter(id: Meter.Id): Counter {
                        check(!id.name.startsWith(MetricsBindings.CIRCUIT_BREAKER_PREFIX)) { "simulated counter registration failure" }
                        return super.newCounter(id)
                    }
                }
            val fixture = Fixture()

            // When
            val impl = fixture.bind(registry)

            // Then: bound, warned, and the meters before the failure are there
            assertThat(impl).isNotNull
            assertThat(fixture.bindings.isBound).isTrue()
            assertThat(fixture.status.entries)
                .anyMatch { it.level == Status.WARN && it.message.contains("Failed to bind Resilience4j metrics") }
            val breakerMetersAfterBind = registry.meters.filter { it.id.name.startsWith(MetricsBindings.CIRCUIT_BREAKER_PREFIX) }
            assertThat(breakerMetersAfterBind).isNotEmpty
            assertThat(breakerMetersAfterBind).noneMatch { it.id.name.endsWith(".not.permitted.calls") }

            // When / Then: the unbind removes them with the rest
            fixture.bindings.unbind()
            assertThat(registry.meters).isEmpty()
        }

        @Test
        fun `should complete the unbind and allow a later bind when the registry refuses every removal`() {
            // What is to be tested? Whether an unbind against a registry whose
            //   `remove` throws for every meter - the mirrored Kafka client
            //   meters included - still runs to its end: the binders closed,
            //   the mirror closed, every failure reported, the instance
            //   unbound and able to bind a fresh registry afterwards.
            // How will the test case be deemed successful and why? Successful
            //   if unbind() does not throw, isBound is false afterwards, the
            //   removal failures of the breaker meters are WARNed, and a bind
            //   to a fresh registry succeeds. The mirror close used to be the
            //   one step of unbind() outside a try; it is safe only because
            //   the mirror swallows a throwing `target.remove` - with that
            //   swallow gone, an unguarded close would abort the unbind before
            //   the breaker removals and the deregistration, leave the
            //   registry reference set and make the identity probe refuse
            //   every later bind (DEFECT_ANALYSIS-2026-10-11T16-25-57,
            //   finding 27). Mutation checked: with the mirror's swallow
            //   deleted, the guard in unbind() is what keeps this test green.
            // Why is it important to test this test case? An unbind that
            //   leaves the appender permanently unbindable turns one hostile
            //   registry into a monitoring outage for the process lifetime.

            // Given: bound with real producers (so the mirror holds meters) to
            //   a registry that refuses every removal
            val hostile =
                object : SimpleMeterRegistry() {
                    override fun remove(mappedId: Meter.Id): Meter? = throw IllegalStateException("simulated removal failure")
                }
            val fixture = Fixture(producers = realProducers())
            assertThat(fixture.bind(hostile)).isNotNull
            assertThat(hostile.meters).anyMatch { it.id.name.startsWith("kafka.appender.producer.") }

            // When
            assertThatCode { fixture.bindings.unbind() }.doesNotThrowAnyException()

            // Then: unbound, failures reported, and a fresh registry binds
            assertThat(fixture.bindings.isBound).isFalse()
            assertThat(fixture.status.entries)
                .anyMatch { it.level == Status.WARN && it.message.contains("Error removing Resilience4j meter") }
            val fresh = SimpleMeterRegistry()
            assertThat(fixture.bind(fresh)).isNotNull
            assertThat(fixture.bindings.isBoundTo(fresh)).isTrue()
            fresh.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED, MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA")
        }

        @Test
        fun `should keep removing circuit-breaker meters after one removal fails`() {
            // What is to be tested? Whether the unbind removes the breaker
            //   meters one by one, so a registry that throws while removing
            //   one of them does not skip all later ones.
            // How will the test case be deemed successful and why? Successful
            //   if, after the unbind, exactly the one meter whose removal
            //   threw is still registered, every other meter is gone, and a
            //   WARN names the failed removal. A single try around the whole
            //   loop left every meter after the failing one orphaned
            //   (DEFECT_ANALYSIS-2026-10-10T13-46-50, finding 5).
            // Why is it important to test this test case? Orphaned breaker
            //   meters keep reporting a stopped appender's state and are
            //   unreachable for any later cleanup.

            // Given: a registry that fails to remove the first breaker meter asked for
            var failedRemoval: Meter.Id? = null
            val registry =
                object : SimpleMeterRegistry() {
                    override fun remove(meter: Meter): Meter? {
                        if (failedRemoval == null && meter.id.name.startsWith(MetricsBindings.CIRCUIT_BREAKER_PREFIX)) {
                            failedRemoval = meter.id
                            throw IllegalStateException("simulated removal failure")
                        }
                        return super.remove(meter)
                    }
                }
            val fixture = Fixture()
            fixture.bind(registry)
            assertThat(registry.meters.count { it.id.name.startsWith(MetricsBindings.CIRCUIT_BREAKER_PREFIX) }).isGreaterThan(1)

            // When
            fixture.bindings.unbind()

            // Then: only the failed one is left, and its failure is a WARN
            assertThat(registry.meters.map { it.id }).containsExactly(failedRemoval)
            assertThat(fixture.status.entries)
                .anyMatch { it.level == Status.WARN && it.message.contains("Error removing Resilience4j meter") }
        }
    }

    @Nested
    inner class `Unbind` {
        @Test
        fun `should remove every meter on unbind and tolerate a second unbind`() {
            // What is to be tested? The symmetric teardown: an unbind removes
            //   everything the bind registered and leaves the instance
            //   unbound; a second unbind (the appender's stop may run more
            //   than once during a context teardown) changes nothing and
            //   warns about nothing.
            // How will the test case be deemed successful and why? Successful
            //   if the registry is empty after the first unbind, isBound is
            //   false, and the second unbind neither throws nor adds a
            //   status entry.
            // Why is it important to test this test case? A leaked meter per
            //   Logback reconfiguration cycle is invisible in single-start
            //   deployments and shows up as a slowly growing scrape; a
            //   non-idempotent unbind would warn on every repeated stop.

            // Given
            val registry = SimpleMeterRegistry()
            val fixture = Fixture()
            fixture.bind(registry)
            assertThat(registry.meters).isNotEmpty

            // When
            fixture.bindings.unbind()

            // Then
            assertThat(registry.meters).isEmpty()
            assertThat(fixture.bindings.isBound).isFalse()
            val entriesAfterFirst = fixture.status.entries.size
            assertThatCode { fixture.bindings.unbind() }.doesNotThrowAnyException()
            assertThat(fixture.status.entries).hasSize(entriesAfterFirst)
        }
    }

    @Nested
    inner class `Kafka producer metrics` {
        @Test
        fun `should publish the Kafka client metrics under kafka-appender-producer and remove them on unbind`() {
            // What is to be tested? Whether the Kafka client binder's meters
            //   reach the operator's registry renamed into the appender's
            //   namespace (kafka.producer.* -> kafka.appender.producer.*,
            //   ADR-0005), tagged with topic.class and appender, and whether
            //   the unbind removes them without closing the operator's
            //   registry.
            // How will the test case be deemed successful and why? Successful
            //   if after the bind the registry holds kafka.appender.producer.*
            //   meters, every one carrying both tags, the client's app info
            //   under producer too, and not a single raw kafka.producer.*
            //   meter; and if after the unbind none of them is left, the
            //   registry is still open, and the binder's refresh scheduler
            //   thread (micrometer-kafka-metrics-N, started by the bind) has
            //   exited. The mirror's close removes the mirrored meters
            //   whether or not the binders were closed, so the thread is
            //   the one observable effect of the binder close loop
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 3).
            // Why is it important to test this test case? A rename that
            //   silently does nothing is indistinguishable from one that
            //   works until a dashboard keyed on the new names reads empty;
            //   the unbind must only remove the mirrored meters, never
            //   close the application's registry; and an unbind that skips
            //   the binder close leaks one scheduler per topic class on
            //   every Logback reconfiguration, polling a closed producer.

            // Given
            val registry = SimpleMeterRegistry()
            val fixture = Fixture(producers = realProducers())
            val schedulersBefore = kafkaMetricsSchedulerThreads()

            // When
            fixture.bind(registry)
            val schedulersStarted = kafkaMetricsSchedulerThreads() - schedulersBefore
            assertThat(schedulersStarted).`as`("refresh schedulers started by the bind").isNotEmpty

            // Then: renamed, tagged, no raw name left
            val producerMeters = registry.meters.filter { it.id.name.startsWith("kafka.appender.producer.") }
            assertThat(producerMeters).isNotEmpty
            assertThat(producerMeters).allSatisfy { meter ->
                assertThat(meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS)).isEqualTo(TopicClass.TECHNICAL.tag)
                assertThat(meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER)).isEqualTo("KAFKA")
            }
            assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.producer") }
            assertThat(registry.meters).anyMatch { it.id.name.startsWith("kafka.appender.producer.app.info") }
            assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.appender.app.") }

            // When / Then: the unbind removes them, leaves the registry open
            //   and stops the refresh schedulers (thread exit is asynchronous
            //   after shutdownNow, hence the bounded poll)
            fixture.bindings.unbind()
            assertThat(registry.appenderMeterIds()).isEmpty()
            assertThat(registry.isClosed).isFalse()
            pollUntil(timeoutMs = 5_000) { schedulersStarted.none { it.isAlive } }
        }

        @Test
        fun `should publish the renamed Kafka client metrics on a composite registry and leave it and its child open`() {
            // What is to be tested? Whether the rename also holds when the
            //   operator's registry is itself a composite - Spring Boot
            //   injects its auto-configured CompositeMeterRegistry - on the
            //   composite and on its child, and whether the unbind leaves
            //   both open.
            // How will the test case be deemed successful and why? Successful
            //   if the composite and its child both hold kafka.appender.producer.*
            //   and no kafka.producer.* meters after the bind, and after the
            //   unbind neither holds any appender meter and neither is closed.
            // Why is it important to test this test case? Composite
            //   registries are where rename filters have failed before: a
            //   filter on a private composite reaches the children but not
            //   the composite that in-process readers query. The mirror
            //   registers in the operator's composite like any binder, so
            //   both levels must show the new names.

            // Given
            val child = SimpleMeterRegistry()
            val composite = CompositeMeterRegistry().add(child)
            val fixture = Fixture(producers = realProducers())

            // When
            fixture.bind(composite)

            // Then
            for (registry in listOf(composite, child)) {
                assertThat(registry.meters).anyMatch { it.id.name.startsWith("kafka.appender.producer.") }
                assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.producer") }
            }
            fixture.bindings.unbind()
            assertThat(composite.appenderMeterIds()).isEmpty()
            assertThat(child.appenderMeterIds()).isEmpty()
            assertThat(composite.isClosed).isFalse()
            assertThat(child.isClosed).isFalse()
        }

        @Test
        fun `should let an operator filter on the renamed name switch the Kafka client metrics off`() {
            // What is to be tested? Whether the operator's own MeterFilter
            //   sees the renamed names, so the documented way to switch the
            //   client metrics off (deny kafka.appender.producer) works.
            // How will the test case be deemed successful and why? Successful
            //   if with that deny filter on the operator's registry no
            //   kafka.appender.producer.* meter is registered while the
            //   appender's own meters are.
            // Why is it important to test this test case? The rename happens
            //   in a private registry in front of the operator's; if the
            //   operator's filters saw the raw names instead, every
            //   documented filter on the new namespace would silently miss.

            // Given
            val registry = SimpleMeterRegistry()
            registry.config().meterFilter(MeterFilter.denyNameStartsWith("kafka.appender.producer"))
            val fixture = Fixture(producers = realProducers())

            // When
            fixture.bind(registry)

            // Then
            assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.appender.producer") }
            assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.producer") }
            assertThat(registry.meters).anyMatch { it.id.name == MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED }
        }
    }

    private companion object {
        /** Thread-name prefix of the scheduler `KafkaClientMetrics.bindTo` starts (`NamedThreadFactory`). */
        const val KAFKA_METRICS_THREAD_PREFIX = "micrometer-kafka-metrics"
    }
}
