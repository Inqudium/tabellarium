package eu.inqudium.tabellarium

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig
import io.micrometer.core.instrument.distribution.pause.PauseDetector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.data.Offset
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.ToDoubleFunction

class MicrometerKafkaAppenderMetricsTest {
    private fun newRegistry(): SimpleMeterRegistry = SimpleMeterRegistry()

    /** Every meter of this appender's inventory, by name. */
    private fun appenderMeters(registry: SimpleMeterRegistry): List<Meter> = registry.meters.filter { it.id.name.startsWith("kafka.appender.") }

    /** Calls every hook of the interface exactly once, so every meter the implementation can register exists. */
    private fun exerciseEveryHook(metrics: MicrometerKafkaAppenderMetrics) {
        metrics.eventAccepted(TopicClass.AUDIT)
        metrics.eventDispatched(TopicClass.AUDIT)
        metrics.eventFallback(TopicClass.AUDIT, KafkaAppenderMetrics.FallbackReason.QUEUE_FULL)
        metrics.sendCompleted(TopicClass.AUDIT, KafkaAppenderMetrics.SendOutcome.SUCCESS, Duration.ofMillis(1))
        metrics.eventSize(TopicClass.AUDIT, 512)
        metrics.fallbackDispatcherDropped()
        metrics.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 1024)
        TopicClass.entries.forEach { metrics.registerSendQueueGauges(it, queueSize = { 0 }, capacity = 1024) }
    }

    @Nested
    inner class `Counter increments` {
        @Test
        fun `should increment accepted counter for the matching topic class`() {
            // What is to be tested? Whether eventAccepted increments the counter series
            //   of the given topic class only, i.e. the pre-resolved EnumMap indexes the
            //   right meter per class.
            // How will the test case be deemed successful and why? Successful if two
            //   audit calls and one technical call leave audit=2.0 and technical=1.0
            //   under the topic.class tag - both series exist and neither absorbs the
            //   other's increments.
            // Why is it important to test this test case? The accepted counter is the
            //   left-hand side of accepted = dispatched + fallback that operators alert
            //   on per class; a mis-indexed map would file audit traffic under technical
            //   and break the invariant without any error.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            metrics.eventAccepted(TopicClass.AUDIT)
            metrics.eventAccepted(TopicClass.AUDIT)
            metrics.eventAccepted(TopicClass.TECHNICAL)

            // Then
            assertThat(
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    ).count(),
            ).isEqualTo(2.0)
            assertThat(
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    ).count(),
            ).isEqualTo(1.0)
        }

        @Test
        fun `should increment fallback counter with both topic class and reason tags`() {
            // What is to be tested? Whether the (topicClass, reason)
            //   pair is correctly attached as two separate tags, not
            //   merged into one. This is the cardinality contract:
            //   16 distinct series for fallback in a default deployment.
            // How will the test case be deemed successful and why? Successful
            //   if the registry contains exactly the series corresponding
            //   to the (audit, breaker.open) and (audit, throttle)
            //   combinations, with the right count, and the counts on
            //   the wrong combination remain at zero. This proves that
            //   the tags are independent, not collapsed.
            // Why is it important to test this test case? A regression
            //   that mistakenly used a single composite tag (e.g.
            //   "audit-breaker_open") would silently collapse the
            //   per-reason diagnostic capability - operators would see
            //   the right total count but no idea which gate fired.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            metrics.eventFallback(TopicClass.AUDIT, KafkaAppenderMetrics.FallbackReason.BREAKER_OPEN)
            metrics.eventFallback(TopicClass.AUDIT, KafkaAppenderMetrics.FallbackReason.BREAKER_OPEN)
            metrics.eventFallback(TopicClass.AUDIT, KafkaAppenderMetrics.FallbackReason.THROTTLE)

            // Then
            val breakerCount =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                        MicrometerKafkaAppenderMetrics.TAG_REASON to "breaker.open",
                    ).count()
            val throttleCount =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                        MicrometerKafkaAppenderMetrics.TAG_REASON to "throttle",
                    ).count()
            assertThat(breakerCount).isEqualTo(2.0)
            assertThat(throttleCount).isEqualTo(1.0)
        }

        @Test
        fun `should count fallback dispatcher drops on an untagged counter`() {
            // What is to be tested? Whether fallbackDispatcherDropped increments a
            //   single counter that carries no topic.class or reason tag - the drop
            //   happens inside the FallbackDispatcher, past the point where the class
            //   still matters.
            // How will the test case be deemed successful and why? Successful if five
            //   calls leave the counter, found by name alone, at exactly 5.0 - one
            //   series absorbing every call.
            // Why is it important to test this test case? This counter is the only
            //   signal that events were lost even from the fallback path; an alert keyed
            //   on the bare metric name must find one series, not a scatter of tagged
            //   ones that each stay below threshold.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            repeat(5) { metrics.fallbackDispatcherDropped() }

            // Then
            val droppedCount = registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED).count()
            assertThat(droppedCount).isEqualTo(5.0)
        }
    }

    @Nested
    inner class `Send-duration timer` {
        @Test
        fun `should record duration with the outcome tag`() {
            // What is to be tested? Whether sendCompleted records into the timer series
            //   keyed by (topic.class, outcome), separating successful sends from failed
            //   ones, and whether the passed Duration is recorded as-is.
            // How will the test case be deemed successful and why? Successful if the
            //   success timer counts 2 samples totaling 35 ms (15 + 20) and the error
            //   timer counts 1; the total-time check proves the nanosecond conversion in
            //   record() does not distort the value.
            // Why is it important to test this test case? Operators read this timer to
            //   tell "Kafka is slow" from "Kafka rejects records"; an outcome mix-up or a
            //   unit error (ms recorded as ns) would make the latency dashboard
            //   confidently wrong.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            metrics.sendCompleted(
                TopicClass.AUDIT,
                KafkaAppenderMetrics.SendOutcome.SUCCESS,
                Duration.ofMillis(15),
            )
            metrics.sendCompleted(
                TopicClass.AUDIT,
                KafkaAppenderMetrics.SendOutcome.SUCCESS,
                Duration.ofMillis(20),
            )
            metrics.sendCompleted(
                TopicClass.AUDIT,
                KafkaAppenderMetrics.SendOutcome.ERROR,
                Duration.ofMillis(500),
            )

            // Then
            val successTimer =
                registry.requireTimer(
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    MicrometerKafkaAppenderMetrics.TAG_OUTCOME to "success",
                )
            val errorTimer =
                registry.requireTimer(
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    MicrometerKafkaAppenderMetrics.TAG_OUTCOME to "error",
                )
            assertThat(successTimer.count()).isEqualTo(2L)
            assertThat(errorTimer.count()).isEqualTo(1L)
            // Total time for two 15+20=35 ms samples on the success timer
            assertThat(successTimer.totalTime(TimeUnit.MILLISECONDS))
                .isCloseTo(35.0, Offset.offset(0.001))
        }
    }

    @Nested
    inner class `Event-size summary` {
        @Test
        fun `should record count, total and max of the payload sizes per topic class in bytes`() {
            // What is to be tested? Whether eventSize feeds the size summary of
            //   the given class only, with the byte count as the recorded amount.
            // How will the test case be deemed successful and why? Successful if
            //   the audit summary reports count 2, total 1536 and max 1024, the
            //   technical summary count 1, and both carry the base unit bytes -
            //   the unit Prometheus appends to the name (_bytes).
            // Why is it important to test this test case? A mis-indexed class
            //   map would file audit sizes under technical, and a missing base
            //   unit would export an unlabelled number that dashboards read as
            //   events instead of bytes.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            metrics.eventSize(TopicClass.AUDIT, 512)
            metrics.eventSize(TopicClass.AUDIT, 1024)
            metrics.eventSize(TopicClass.TECHNICAL, 100)

            // Then
            val audit = registry.requireSummary(MicrometerKafkaAppenderMetrics.METRIC_EVENT_SIZE, MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit")
            assertThat(audit.count()).isEqualTo(2)
            assertThat(audit.totalAmount()).isEqualTo(1536.0)
            assertThat(audit.max()).isEqualTo(1024.0)
            assertThat(audit.id.baseUnit).isEqualTo("bytes")
            val technical = registry.requireSummary(MicrometerKafkaAppenderMetrics.METRIC_EVENT_SIZE, MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical")
            assertThat(technical.count()).isEqualTo(1)
        }
    }

    @Nested
    inner class `Queue gauges` {
        @Test
        fun `should expose live queue size via the supplier`() {
            // What is to be tested? Whether the size gauge actually
            //   reads its supplier on each scrape, instead of caching
            //   a value at registration time.
            // How will the test case be deemed successful and why? Successful
            //   if the supplier advances and the gauge reports the new
            //   value on a subsequent scrape. Pins the live-read
            //   behavior of Micrometer gauges, which is the contract
            //   the FallbackDispatcher relies on.
            // Why is it important to test this test case? A regression
            //   that cached the initial supplier value would freeze the
            //   queue-size dashboard at zero - looking healthy even
            //   while the queue is full.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)
            val backing = AtomicInteger(0)
            metrics.registerFallbackQueueGauges(queueSize = backing::get, capacity = 100)

            // When / Then: gauge follows the supplier
            val queueSize = registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
            assertThat(queueSize.value()).isEqualTo(0.0)

            backing.set(42)
            assertThat(queueSize.value()).isEqualTo(42.0)
        }

        @Test
        fun `should expose the fixed capacity as a constant gauge`() {
            // What is to be tested? Whether registerFallbackQueueGauges publishes the
            //   dispatcher's capacity as a gauge with the value handed in, alongside the
            //   size gauge it registers in the same call.
            // How will the test case be deemed successful and why? Successful if the
            //   fallback.queue.capacity gauge reads 2048.0 after registration; the
            //   capacity is fixed for the dispatcher's lifetime, so a constant gauge is
            //   the correct shape.
            // Why is it important to test this test case? Dashboards express queue fill
            //   as size / capacity; without the capacity series, or with a wrong one,
            //   the "queue nearly full" alert has no denominator.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            metrics.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 2048)

            // Then
            assertThat(registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY).value()).isEqualTo(2048.0)
        }

        @Test
        fun `should register neither fallback queue gauge until a fallback dispatcher is wired`() {
            // What is to be tested? Whether a fresh instance - the state of an
            //   appender without a fallback appender, whose transport never calls
            //   registerFallbackQueueGauges - registers neither fallback.queue.size
            //   nor fallback.queue.capacity, while the counters, timers and the
            //   send-queue gauges are registered as usual.
            // How will the test case be deemed successful and why? Successful if
            //   the registry holds no fallback.queue.* meter after construction and
            //   after the send-queue gauges were registered, but does hold the
            //   accepted counter and the send-queue gauges. Both fallback gauges are
            //   registered together in registerFallbackQueueGauges; previously the
            //   size gauge was pre-registered in the constructor and read a
            //   constant 0 without any fallback
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md, finding 9).
            // Why is it important to test this test case? A constant-zero queue
            //   size next to a missing capacity series reads as a healthy fallback
            //   queue in a deployment that drops by policy, and the documented
            //   saturation ratio divides by a series that does not exist; absent
            //   series say "no fallback" unambiguously.

            // Given: an instance that is never wired to a fallback dispatcher
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "no-fallback")

            // When: the hooks a fallback-less transport does call
            metrics.eventAccepted(TopicClass.TECHNICAL)
            metrics.registerSendQueueGauges(TopicClass.TECHNICAL, queueSize = { 3 }, capacity = 1024)

            // Then: no fallback queue gauge at all, everything else as usual
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE).gauge()).isNull()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY).gauge()).isNull()
            assertThat(
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    ).count(),
            ).isEqualTo(1.0)
            assertThat(
                registry
                    .requireGauge(
                        MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    ).value(),
            ).isEqualTo(3.0)
        }

        @Test
        fun `should follow the latest supplier and keep one gauge pair when the fallback gauges are registered twice`() {
            // What is to be tested? Whether a repeated registerFallbackQueueGauges on
            //   the same instance (the FallbackDispatcher calls it on every
            //   setMetrics) replaces the supplier the size gauge reads and does not
            //   register a second gauge pair.
            // How will the test case be deemed successful and why? Successful if the
            //   size gauge reads the second supplier's value after the second call
            //   and the registry holds exactly one size and one capacity gauge for
            //   the appender tag. Micrometer keeps the first registration per
            //   meter id, so a second registration with a new lambda would be
            //   silently ignored; the implementation therefore swaps a supplier
            //   holder instead - the contract "replace, do not duplicate" of the
            //   interface KDoc.
            // Why is it important to test this test case? A gauge frozen on the
            //   first supplier would keep reporting a stale dispatcher's queue
            //   after a re-bind; a duplicated pair would double the series and
            //   confuse every dashboard sum.

            // Given: an instance wired once
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "audit-appender")
            metrics.registerFallbackQueueGauges(queueSize = { 5 }, capacity = 100)

            // When: wired again with a different supplier
            metrics.registerFallbackQueueGauges(queueSize = { 9 }, capacity = 100)

            // Then: the gauge follows the latest supplier and no duplicate pair exists
            assertThat(registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE).value()).isEqualTo(9.0)
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE).gauges()).hasSize(1)
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY).gauges()).hasSize(1)
        }
    }

    @Nested
    inner class `Exception safety` {
        /**
         * Registry that constructs normally (the counters and timers
         * resolve) but whose meters throw on use and whose gauge
         * registration and removal throw - every registry-facing call
         * a hook makes fails.
         */
        private inner class HostileRegistry : SimpleMeterRegistry() {
            override fun newCounter(id: Meter.Id): Counter =
                object : Counter by super.newCounter(id) {
                    override fun increment(amount: Double): Unit = throw IllegalStateException("hostile counter")
                }

            override fun newTimer(
                id: Meter.Id,
                distributionStatisticConfig: DistributionStatisticConfig,
                pauseDetector: PauseDetector,
            ): Timer =
                object : Timer by super.newTimer(id, distributionStatisticConfig, pauseDetector) {
                    override fun record(
                        amount: Long,
                        unit: TimeUnit,
                    ): Unit = throw IllegalStateException("hostile timer")
                }

            override fun newDistributionSummary(
                id: Meter.Id,
                distributionStatisticConfig: DistributionStatisticConfig,
                scale: Double,
            ): DistributionSummary =
                object : DistributionSummary by super.newDistributionSummary(id, distributionStatisticConfig, scale) {
                    override fun record(amount: Double): Unit = throw IllegalStateException("hostile summary")
                }

            override fun <T : Any> newGauge(
                id: Meter.Id,
                obj: T?,
                valueFunction: ToDoubleFunction<T>,
            ): Gauge = throw IllegalStateException("hostile gauge")

            override fun remove(meter: Meter): Meter = throw IllegalStateException("hostile remove")
        }

        @Test
        fun `should return normally from every hook when the registry throws`() {
            // What is to be tested? Whether the `safe` wrapper holds for every
            //   hook of the interface - the counter increments, the timer
            //   record, the two gauge registrations and the deregistration -
            //   and for the info-gauge registration in the constructor, when
            //   the registry or its meters throw.
            // How will the test case be deemed successful and why? Successful
            //   if every hook returns without an exception against a registry
            //   whose meters throw on use and whose gauge registration and
            //   removal throw, and if the construction with an AppenderInfo
            //   (the bindMeterRegistry path) returns normally as well. The
            //   contract at KafkaAppenderMetrics ("a metrics
            //   failure must never corrupt the logging pipeline") is what the
            //   hot path relies on when it calls the hooks unguarded.
            // Why is it important to test this test case? A hook that let a
            //   registry exception escape would surface as the first (and then
            //   suppressed) hot-path error and divert every event to the
            //   fallback - a metrics bug turning into a logging outage - and
            //   nothing but this test would notice the wrapper going missing.

            // Given: an instance over a registry that fails every hook-time call
            val registry = HostileRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "hostile")

            // When / Then: the construction with info gauges returns normally -
            // a failing info registration must cost only the info gauges, not
            // the whole bind
            assertThatCode {
                MicrometerKafkaAppenderMetrics(
                    registry,
                    appenderName = "hostile-with-info",
                    info =
                        AppenderInfo(
                            fallback = "FALLBACK_FILE",
                            fallbackType = "RollingFileAppender",
                            debug = false,
                            topics = listOf(TopicInfo("audit.security", TopicClass.AUDIT, acks = "all", lingerMs = "50")),
                            version = "1.2.3",
                            kafkaClientVersion = "4.0.0",
                        ),
                )
            }.doesNotThrowAnyException()

            // When / Then: every hook returns normally
            assertThatCode {
                metrics.eventAccepted(TopicClass.AUDIT)
                metrics.eventDispatched(TopicClass.AUDIT)
                metrics.eventFallback(TopicClass.AUDIT, KafkaAppenderMetrics.FallbackReason.QUEUE_FULL)
                metrics.sendCompleted(TopicClass.AUDIT, KafkaAppenderMetrics.SendOutcome.SUCCESS, Duration.ofMillis(1))
                metrics.eventSize(TopicClass.AUDIT, 512)
                metrics.fallbackDispatcherDropped()
                metrics.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 1)
                metrics.registerSendQueueGauges(TopicClass.AUDIT, queueSize = { 0 }, capacity = 1)
                metrics.deregisterFrom(registry)
            }.doesNotThrowAnyException()
        }

        /**
         * Registry whose registration of the gauge named [failingName]
         * throws on exactly one attempt - the [failOnAttempt]-th one
         * (1-based) - and succeeds on every other: the selective,
         * transient failure a meter filter or custom registry can
         * produce. Attempts past the first are what let a test fail a
         * group in the MIDDLE, with more than one meter already
         * registered (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 2).
         * With [failingRemoval], the first `remove` of any meter throws
         * as well - the rollback that fails on top of the registration
         * that failed (finding 17).
         */
        private inner class FailOnceGaugeRegistry(
            private val failingName: String,
            private val failOnAttempt: Int = 1,
            private val failingRemoval: Boolean = false,
        ) : SimpleMeterRegistry() {
            val attempts = AtomicInteger()
            private val removalFailures = AtomicInteger()

            /** Every id removed from this registry, in order - pins the scope of a rollback or teardown. */
            val removals: MutableList<Meter.Id> = mutableListOf()

            override fun remove(mappedId: Meter.Id): Meter? {
                if (failingRemoval && removalFailures.getAndIncrement() == 0) {
                    throw IllegalStateException("transient removal failure")
                }
                removals += mappedId
                return super.remove(mappedId)
            }

            override fun <T : Any> newGauge(
                id: Meter.Id,
                obj: T?,
                valueFunction: ToDoubleFunction<T>,
            ): Gauge {
                if (id.name == failingName && attempts.incrementAndGet() == failOnAttempt) {
                    throw IllegalStateException("transient gauge failure")
                }
                return super.newGauge(id, obj, valueFunction)
            }
        }

        /** The ids currently registered in [registry]; the removal log must equal it after a teardown. */
        private fun idsIn(registry: SimpleMeterRegistry): List<Meter.Id> = registry.meters.map { it.id }

        /** The ids a plain instance named [appenderName] registers on construction, for comparison with an instance whose info set rolled back. */
        private fun idsWithoutInfo(appenderName: String): List<Meter.Id> = idsIn(newRegistry().also { MicrometerKafkaAppenderMetrics(it, appenderName = appenderName) })

        @Test
        fun `should roll back a half-registered fallback gauge pair, warn, and register both on the retry`() {
            // What is to be tested? Whether registerFallbackQueueGauges treats the
            //   size/capacity pair as one transaction: when the capacity gauge
            //   fails after the size gauge registered, the size gauge is removed
            //   again, the failure is reported through the warn sink, and the
            //   "registered" flag stays unset so a repeated call registers both.
            // How will the test case be deemed successful and why? Successful if
            //   after the failing call neither gauge exists and exactly one
            //   warning naming the fallback queue arrived, and if after the
            //   second call both gauges exist with the second supplier's value
            //   and the capacity. Previously the flag was set before either
            //   registration, so the retry returned on the flag and the pair
            //   stayed missing for the binding's lifetime without any status
            //   entry (DEFECT_ANALYSIS-2026-10-10T15-52-19, finding 2).
            // Why is it important to test this test case? The hostile-registry
            //   test above only proves that the hooks do not throw; it cannot
            //   see permanently missing saturation telemetry, which silences
            //   exactly the alert an operator relies on when the fallback
            //   queue fills up.

            // Given: an instance whose capacity-gauge registration fails once
            val registry = FailOnceGaugeRegistry(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY)
            val warnings = mutableListOf<String>()
            val metrics =
                MicrometerKafkaAppenderMetrics(registry, appenderName = "flaky", warn = { message, _ -> warnings += message })
            val baseline = registry.meters.map { it.id }

            // When: the first wiring hits the failure
            metrics.registerFallbackQueueGauges(queueSize = { 5 }, capacity = 100)

            // Then: no half pair is left behind, the rollback removed nothing
            //   but the pair (DEFECT_ANALYSIS-2026-10-10T17-20-10, finding 2),
            //   and the operator is told
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE).gauge()).isNull()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY).gauge()).isNull()
            assertThat(registry.meters.map { it.id }).containsExactlyInAnyOrderElementsOf(baseline)
            assertThat(warnings).singleElement().asString().contains("fallback queue")

            // When: wired again
            metrics.registerFallbackQueueGauges(queueSize = { 7 }, capacity = 100)

            // Then: both gauges exist and read the live values
            assertThat(registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE).value()).isEqualTo(7.0)
            assertThat(registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY).value()).isEqualTo(100.0)
            assertThat(warnings).hasSize(1)

            // And: the rolled-back first size gauge is no longer tracked -
            //   the teardown removes every surviving meter exactly once,
            //   the retried pair included (DEFECT_ANALYSIS-2026-10-10T19-11-11,
            //   finding 17)
            val survivors = idsIn(registry)
            registry.removals.clear()
            metrics.deregisterFrom(registry)
            assertThat(registry.removals).containsExactlyInAnyOrderElementsOf(survivors)
            assertThat(appenderMeters(registry)).isEmpty()
        }

        @Test
        fun `should roll back a half-registered send-queue gauge pair and warn`() {
            // What is to be tested? Whether registerSendQueueGauges applies the
            //   same pair transaction as the fallback gauges: a failing capacity
            //   gauge removes the already registered size gauge and is reported.
            // How will the test case be deemed successful and why? Successful if
            //   after the failing call no send-queue gauge exists for the class,
            //   one warning naming the topic class arrived, and the removed size
            //   gauge is no longer tracked (deregisterFrom leaves no meter of the
            //   pair behind either way).
            // Why is it important to test this test case? A size series without
            //   its capacity breaks the documented saturation ratio silently;
            //   the warning is the only signal that the queue is unobserved.

            // Given: an instance whose send-queue capacity registration fails once
            val registry = FailOnceGaugeRegistry(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY)
            val warnings = mutableListOf<String>()
            val metrics =
                MicrometerKafkaAppenderMetrics(registry, appenderName = "flaky", warn = { message, _ -> warnings += message })
            val baseline = registry.meters.map { it.id }

            // When
            metrics.registerSendQueueGauges(TopicClass.AUDIT, queueSize = { 3 }, capacity = 50)

            // Then: only the pair was rolled back, and the operator is told
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE).gauge()).isNull()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY).gauge()).isNull()
            assertThat(registry.meters.map { it.id }).containsExactlyInAnyOrderElementsOf(baseline)
            assertThat(warnings).singleElement().asString().contains("topic.class=audit")

            // And: the rolled-back size gauge is no longer tracked - the
            //   teardown removes exactly the surviving meters, nothing twice
            registry.removals.clear()
            metrics.deregisterFrom(registry)
            assertThat(registry.removals).containsExactlyInAnyOrderElementsOf(baseline)
            assertThat(appenderMeters(registry)).isEmpty()
        }

        @Test
        fun `should roll back a half-registered info gauge set and warn`() {
            // What is to be tested? Whether the info gauges are registered as one
            //   transaction like the queue gauge pairs: when a topic-info gauge
            //   fails after kafka.appender.info registered, the info gauges
            //   registered so far are removed again and the failure reaches the
            //   warn sink, while the rest of the instance is constructed normally.
            // How will the test case be deemed successful and why? Successful if
            //   no info gauge exists, exactly one warning names the info gauges,
            //   and every other meter is present. Before the fix the failure was
            //   swallowed silently and the appender series stayed without its
            //   topic series (DEFECT_ANALYSIS-2026-10-10T17-20-10, finding 3).
            // Why is it important to test this test case? Dashboards join the
            //   other series onto the info labels; a partial info set drops
            //   series from those joins without any signal to the operator.

            // Given: a registry whose first topic-info gauge registration fails
            val registry = FailOnceGaugeRegistry(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO)
            val warnings = mutableListOf<String>()
            val info =
                AppenderInfo(
                    fallback = AppenderInfo.NONE,
                    fallbackType = AppenderInfo.NONE,
                    debug = false,
                    topics = listOf(TopicInfo("svc.logs", TopicClass.TECHNICAL, acks = "1", lingerMs = "50")),
                )

            // When
            val metrics =
                MicrometerKafkaAppenderMetrics(registry, appenderName = "flaky", info = info, warn = { message, _ -> warnings += message })

            // Then: no info gauge is left, the operator is told, the rest stands
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_INFO).gauge()).isNull()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO).gauge()).isNull()
            assertThat(warnings).singleElement().asString().contains("info gauges")
            metrics.eventAccepted(TopicClass.TECHNICAL)
            assertThat(
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                    ).count(),
            ).isEqualTo(1.0)

            // And: the rolled-back info gauge is no longer tracked - the
            //   teardown removes exactly the surviving meters
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 17)
            val survivors = idsIn(registry)
            registry.removals.clear()
            metrics.deregisterFrom(registry)
            assertThat(registry.removals).containsExactlyInAnyOrderElementsOf(survivors)
            assertThat(appenderMeters(registry)).isEmpty()
        }

        @Test
        fun `should roll back every info gauge registered before a failure in the middle of the set`() {
            // What is to be tested? The scope of the all-or-none rollback when
            //   the group is more than one meter deep: with two configured
            //   topics and the SECOND topic-info gauge failing, both
            //   kafka.appender.info and the first topic.info were already
            //   registered, and both must go - not only the last one.
            // How will the test case be deemed successful and why? Successful
            //   if afterwards no info gauge of either name exists, the
            //   registry holds exactly the meters a plain instance without
            //   info registers (so nothing else was touched either), and one
            //   warning arrived. The existing rollback tests fail the second
            //   registration of their group, so their partial set is always
            //   exactly one meter and a rollback that removes only the last
            //   tracked meter passes them
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 2).
            // Why is it important to test this test case? A rollback that
            //   leaves the appender series and the first topic series behind
            //   is precisely the partial info set that drops the remaining
            //   topics from every join on the labels - the symptom the
            //   transaction was introduced to prevent.

            // Given: a registry whose second topic-info registration fails
            val registry = FailOnceGaugeRegistry(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO, failOnAttempt = 2)
            val warnings = mutableListOf<String>()
            val info =
                AppenderInfo(
                    fallback = AppenderInfo.NONE,
                    fallbackType = AppenderInfo.NONE,
                    debug = false,
                    topics =
                        listOf(
                            TopicInfo("audit.security", TopicClass.AUDIT, acks = "all", lingerMs = "50"),
                            TopicInfo("svc.logs", TopicClass.TECHNICAL, acks = "1", lingerMs = "50"),
                        ),
                )

            // When
            val metrics =
                MicrometerKafkaAppenderMetrics(registry, appenderName = "flaky", info = info, warn = { message, _ -> warnings += message })

            // Then: the failure hit the second topic, and the whole set is gone
            assertThat(registry.attempts.get()).isEqualTo(2)
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_INFO).gauges()).isEmpty()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO).gauges()).isEmpty()
            assertThat(idsIn(registry)).containsExactlyInAnyOrderElementsOf(idsWithoutInfo("flaky"))
            assertThat(warnings).singleElement().asString().contains("info gauges")

            // And: none of the three rolled-back gauges is tracked any more
            val survivors = idsIn(registry)
            registry.removals.clear()
            metrics.deregisterFrom(registry)
            assertThat(registry.removals).containsExactlyInAnyOrderElementsOf(survivors)
            assertThat(appenderMeters(registry)).isEmpty()
        }

        @Test
        fun `should attach a failing constructor rollback as suppressed and keep removing the rest`() {
            // What is to be tested? The failure of the failure path in the
            //   companion `register`: when the constructor throws halfway (a
            //   registry rejecting the third counter) and the rollback's
            //   `remove` of an already registered counter throws as well,
            //   the original failure must still be the one that propagates,
            //   with the removal failure attached as suppressed, and the
            //   rollback must go on to remove the other registered meter.
            // How will the test case be deemed successful and why? Successful
            //   if register throws the registration failure, that exception
            //   carries exactly one suppressed exception with the removal
            //   failure's message, and the registry holds exactly one meter
            //   afterwards - the one whose removal threw. The identical
            //   branch of registerAllOrNone is pinned by the test below; the
            //   companion's own try/catch around the rollback had no test, so
            //   deleting it (the removal failure then replaces the cause and
            //   aborts the rollback) stayed green
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 19).
            // Why is it important to test this test case? The operator reads
            //   the exception of a failed bind in the Spring binding's
            //   warning; a removal failure in its place hides why the
            //   registry rejected the meters, and an aborted rollback leaves
            //   the events.accepted counter behind for the identity probe to
            //   mistake for a live instance.

            // Given: a registry that rejects the third counter and fails the first removal
            val registry =
                object : SimpleMeterRegistry() {
                    private val counters = AtomicInteger()
                    private val removals = AtomicInteger()

                    override fun newCounter(id: Meter.Id): Counter {
                        if (counters.incrementAndGet() == 3) throw IllegalStateException("simulated counter registration failure")
                        return super.newCounter(id)
                    }

                    override fun remove(mappedId: Meter.Id): Meter? {
                        if (removals.getAndIncrement() == 0) throw IllegalStateException("transient removal failure")
                        return super.remove(mappedId)
                    }
                }

            // When / Then: the registration failure propagates, the removal failure travels with it
            assertThatThrownBy { MicrometerKafkaAppenderMetrics.register(registry, Tags.empty(), "flaky", info = null) }
                .hasMessage("simulated counter registration failure")
                .satisfies({ e -> assertThat(e.suppressed.map { it.message }).containsExactly("transient removal failure") })

            // And: the rollback continued past the failed removal
            assertThat(appenderMeters(registry)).hasSize(1)
        }

        @Test
        fun `should keep a meter whose rollback removal failed tracked and report the removal failure`() {
            // What is to be tested? The failure of the failure path: when a
            //   group registration throws and the rollback's `remove` of an
            //   already registered meter throws as well, the meter must stay
            //   tracked (so the final teardown still removes it) and the
            //   removal failure must travel as a suppressed exception of the
            //   warned cause instead of vanishing.
            // How will the test case be deemed successful and why? Successful
            //   if after the failing call the size gauge is still in the
            //   registry (the rollback could not remove it), the warned cause
            //   carries the removal failure as its suppressed exception, and
            //   deregisterFrom then removes the gauge so nothing is left. The
            //   hostile-registry test throws on every gauge registration, so
            //   its rollback never has a meter to remove and this branch was
            //   unreachable from the suite
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 17).
            // Why is it important to test this test case? A rollback that
            //   untracks a meter it failed to remove leaves a size series
            //   without its capacity in the operator's registry for good;
            //   and an operator reading the warning needs the removal failure
            //   attached to understand why the series is still there.

            // Given: the capacity registration and then the first removal fail
            val registry =
                FailOnceGaugeRegistry(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY, failingRemoval = true)
            val warnings = mutableListOf<Pair<String, Throwable>>()
            val metrics =
                MicrometerKafkaAppenderMetrics(registry, appenderName = "flaky", warn = { message, cause -> warnings += message to cause })

            // When
            metrics.registerSendQueueGauges(TopicClass.AUDIT, queueSize = { 3 }, capacity = 50)

            // Then: the size gauge survived the failed rollback, and the
            //   warning carries both failures
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE).gauge()).isNotNull()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY).gauge()).isNull()
            assertThat(warnings).singleElement().satisfies({ (message, cause) ->
                assertThat(message).contains("topic.class=audit")
                assertThat(cause).hasMessage("transient gauge failure")
                assertThat(cause.suppressed.single()).hasMessage("transient removal failure")
            })

            // And: still tracked - the teardown removes it with the rest
            metrics.deregisterFrom(registry)
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE).gauge()).isNull()
            assertThat(appenderMeters(registry)).isEmpty()
        }
    }

    @Nested
    inner class `Common tags` {
        @Test
        fun `should attach common tags to every metric`() {
            // What is to be tested? Whether the commonTags constructor parameter is
            //   merged into fullTags and therefore reaches both the per-class counters
            //   and the otherwise-untagged fallback.dropped counter.
            // How will the test case be deemed successful and why? Successful if
            //   counters looked up WITH service=payment-service (plus topic.class for
            //   accepted) exist and hold the increments; the lookup fails if the tag is
            //   missing on either metric.
            // Why is it important to test this test case? fullTags is named as it is to
            //   dodge a Kotlin shadowing pitfall that would silently drop shared tags
            //   from some meters (see its KDoc); this test pins the merge on a meter
            //   that uses fullTags directly and on one that goes through tagsWith().

            // Given: a metrics instance with a service-identifying common tag
            val registry = newRegistry()
            val commonTags = Tags.of("service", "payment-service")
            val metrics = MicrometerKafkaAppenderMetrics(registry, commonTags)

            // When
            metrics.eventAccepted(TopicClass.AUDIT)
            metrics.fallbackDispatcherDropped()

            // Then: counters carry the common tag in addition to the per-call tags
            val accepted =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        "service" to "payment-service",
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    ).count()
            assertThat(accepted).isEqualTo(1.0)

            val dropped =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED,
                        "service" to "payment-service",
                    ).count()
            assertThat(dropped).isEqualTo(1.0)
        }
    }

    @Nested
    inner class `Appender tag` {
        @Test
        fun `should attach the appender tag to every metric including the untagged ones`() {
            // What is to be tested? Whether the appender-name tag is
            //   applied to every metric - including the fallback queue
            //   gauges and the dropped counter, which have no other
            //   distinguishing dimensions.
            // How will the test case be deemed successful and why? Successful
            //   if both a per-class counter (accepted) and an
            //   otherwise-untagged counter (fallback.dropped) carry
            //   appender=audit-appender. Pins the contract that the
            //   tag is uniform across the metric inventory.
            // Why is it important to test this test case? The whole
            //   reason for the appender tag is to disambiguate the
            //   queue gauges in the rare multi-appender setup. If the
            //   tag were silently dropped from those very metrics,
            //   the feature would be useless precisely where it matters.

            // Given
            val registry = newRegistry()
            val metrics =
                MicrometerKafkaAppenderMetrics(
                    registry,
                    commonTags = Tags.empty(),
                    appenderName = "audit-appender",
                )

            // When
            metrics.eventAccepted(TopicClass.AUDIT)
            metrics.fallbackDispatcherDropped()
            metrics.registerFallbackQueueGauges(queueSize = { 7 }, capacity = 100)

            // Then: all three metric types carry the appender tag
            val accepted =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    ).count()
            assertThat(accepted).isEqualTo(1.0)

            val dropped =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED,
                        MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                    ).count()
            assertThat(dropped).isEqualTo(1.0)

            val queueSize =
                registry.requireGauge(
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                )
            assertThat(queueSize.value()).isEqualTo(7.0)

            val queueCapacity =
                registry.requireGauge(
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                )
            assertThat(queueCapacity.value()).isEqualTo(100.0)
        }

        @Test
        fun `should attach the appender tag and exactly the documented dimensions to every registered meter`() {
            // What is to be tested? Whether EVERY meter the implementation can
            //   register - the four counters, the timer, the two fallback queue
            //   gauges and the two send queue gauges - carries the appender tag
            //   and the common tag plus exactly its documented dimensions,
            //   checked over the registry's meters rather than over a
            //   hand-picked subset.
            // How will the test case be deemed successful and why? Successful
            //   if, after every hook was called once, the set of registered
            //   kafka.appender.* names equals the inventory and each meter's
            //   tag keys equal the expected keys for its name, with the
            //   appender tag's value the configured name and the common tag's
            //   value the configured one. The subset test above covers 4 of 9
            //   meters; registerSendQueueGauges was asserted against a real
            //   registry nowhere
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md, finding 5).
            //   The common tag is part of the full-inventory check because the
            //   `Common tags` test value-asserts it on two meters only: a
            //   send-queue gauge or a send timer built from
            //   Tags.of(TAG_APPENDER, ...) instead of tagsWith(...) keeps the
            //   appender tag and loses the common tag with the suite green
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 19).
            // Why is it important to test this test case? With the tag dropped
            //   from the send-queue gauges, Micrometer returns the first
            //   appender's gauge to a second appender bound to the same
            //   registry (registration is idempotent on the meter id), so one
            //   appender's queue depth is silently reported for both - at the
            //   gauge that shows an outage building up; and a meter without
            //   the common tag vanishes from every dashboard scoped by it.

            // Given: the documented tag keys per metric, plus the common tag
            val appender = MicrometerKafkaAppenderMetrics.TAG_APPENDER
            val topicClass = MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS
            val common = "service"
            val expectedTagKeys =
                mapOf(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED to setOf(common, appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED to setOf(common, appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK to
                        setOf(common, appender, topicClass, MicrometerKafkaAppenderMetrics.TAG_REASON),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION to
                        setOf(common, appender, topicClass, MicrometerKafkaAppenderMetrics.TAG_OUTCOME),
                    MicrometerKafkaAppenderMetrics.METRIC_EVENT_SIZE to setOf(common, appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED to setOf(common, appender),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE to setOf(common, appender),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY to setOf(common, appender),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE to setOf(common, appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY to setOf(common, appender, topicClass),
                )
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, Tags.of(common, "payment-service"), appenderName = "audit-appender")

            // When: every hook once
            exerciseEveryHook(metrics)

            // Then: the inventory is complete and every meter is tagged exactly as documented
            val meters = appenderMeters(registry)
            assertThat(meters.map { it.id.name }.toSet()).isEqualTo(expectedTagKeys.keys)
            assertThat(meters).allSatisfy { meter ->
                assertThat(
                    meter.id.tags
                        .map { it.key }
                        .toSet(),
                ).`as`("tag keys of %s", meter.id)
                    .isEqualTo(expectedTagKeys.getValue(meter.id.name))
                assertThat(meter.id.getTag(appender))
                    .`as`("appender tag of %s", meter.id)
                    .isEqualTo("audit-appender")
                assertThat(meter.id.getTag(common))
                    .`as`("common tag of %s", meter.id)
                    .isEqualTo("payment-service")
            }
        }

        @Test
        fun `should register exactly the series the enum sizes imply and reach every one of them`() {
            // What is to be tested? Whether the pre-resolved meter tables are
            //   complete: one series per topic class (accepted, dispatched,
            //   event.size, the send-queue gauges), per class and reason
            //   (events.fallback), per class and outcome (send.duration),
            //   and exactly one for the untagged fallback-dispatcher meters -
            //   and whether every one of those series is the one its hook
            //   increments.
            // How will the test case be deemed successful and why? Successful
            //   if, after every hook was called once per enum combination,
            //   the registry holds exactly the expected number of series per
            //   metric name and every per-class, per-reason and per-outcome
            //   series reads 1. The inventory test above pins the set of
            //   names and the tag keys only; a table built from a filtered
            //   enum would pass it, because every hook wraps its lookup in
            //   `safe {}` and a missing key is swallowed as a
            //   NoSuchElementException (DEFECT_ANALYSIS-2026-10-10T19-11-11,
            //   finding 3).
            // Why is it important to test this test case? The series count is
            //   the documented cardinality operators budget with, and a class
            //   or reason silently missing from the tables loses exactly
            //   that class's or reason's telemetry - at the counters the
            //   conservation alerts are built on.

            // Given: the enum sizes the tables are built from
            val classes = TopicClass.entries
            val reasons = KafkaAppenderMetrics.FallbackReason.entries
            val outcomes = KafkaAppenderMetrics.SendOutcome.entries
            val expectedSeries =
                mapOf(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED to classes.size,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED to classes.size,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK to classes.size * reasons.size,
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION to classes.size * outcomes.size,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENT_SIZE to classes.size,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED to 1,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE to 1,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY to 1,
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE to classes.size,
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY to classes.size,
                )
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "audit-appender")

            // When: every hook once per enum combination
            classes.forEach { tc ->
                metrics.eventAccepted(tc)
                metrics.eventDispatched(tc)
                metrics.eventSize(tc, 512)
                reasons.forEach { reason -> metrics.eventFallback(tc, reason) }
                outcomes.forEach { outcome -> metrics.sendCompleted(tc, outcome, Duration.ofMillis(1)) }
                metrics.registerSendQueueGauges(tc, queueSize = { 0 }, capacity = 1024)
            }
            metrics.fallbackDispatcherDropped()
            metrics.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 1024)

            // Then: exactly the expected number of series per name, and no other name
            val meters = appenderMeters(registry)
            assertThat(meters.groupingBy { it.id.name }.eachCount()).containsExactlyInAnyOrderEntriesOf(expectedSeries)

            // And: every series was reached by its hook
            val topicClass = MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS
            classes.forEach { tc ->
                assertThat(registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED, topicClass to tc.tag).count())
                    .`as`("accepted %s", tc.tag)
                    .isEqualTo(1.0)
                assertThat(registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED, topicClass to tc.tag).count())
                    .`as`("dispatched %s", tc.tag)
                    .isEqualTo(1.0)
                assertThat(registry.requireSummary(MicrometerKafkaAppenderMetrics.METRIC_EVENT_SIZE, topicClass to tc.tag).count())
                    .`as`("event.size %s", tc.tag)
                    .isEqualTo(1L)
                reasons.forEach { reason ->
                    assertThat(
                        registry
                            .requireCounter(
                                MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                                topicClass to tc.tag,
                                MicrometerKafkaAppenderMetrics.TAG_REASON to reason.tag,
                            ).count(),
                    ).`as`("fallback %s/%s", tc.tag, reason.tag).isEqualTo(1.0)
                }
                outcomes.forEach { outcome ->
                    assertThat(
                        registry
                            .requireTimer(
                                MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION,
                                topicClass to tc.tag,
                                MicrometerKafkaAppenderMetrics.TAG_OUTCOME to outcome.tag,
                            ).count(),
                    ).`as`("send.duration %s/%s", tc.tag, outcome.tag).isEqualTo(1L)
                }
            }
            assertThat(registry.requireCounter(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED).count()).isEqualTo(1.0)
        }

        @Test
        fun `should produce distinct gauge series for two appender instances sharing a registry`() {
            // What is to be tested? Whether two metrics instances with
            //   different appender names register distinct gauge series
            //   on the same MeterRegistry, rather than one silently
            //   overwriting the other.
            // How will the test case be deemed successful and why? Successful
            //   if each instance's queue-size gauge reads its own
            //   supplier independently. Without per-appender tagging,
            //   the second register() call would be idempotent on
            //   (name, tags) and return the first instance's gauge -
            //   a silent bug where the dashboard shows half the data.
            // Why is it important to test this test case? The
            //   multi-appender case is the only justification for the
            //   `appender` tag's existence in the metric model. A
            //   regression here would not surface in any single-
            //   appender test.

            // Given: two instances with different names but the same registry
            val registry = newRegistry()
            val firstInstance =
                MicrometerKafkaAppenderMetrics(
                    registry,
                    appenderName = "audit-appender",
                )
            val secondInstance =
                MicrometerKafkaAppenderMetrics(
                    registry,
                    appenderName = "technical-appender",
                )

            // When: each instance registers a gauge that reads a different supplier
            firstInstance.registerFallbackQueueGauges(queueSize = { 11 }, capacity = 100)
            secondInstance.registerFallbackQueueGauges(queueSize = { 22 }, capacity = 200)

            // Then: each instance has its own gauge series
            val first = registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE, MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender")
            val second = registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE, MicrometerKafkaAppenderMetrics.TAG_APPENDER to "technical-appender")
            assertThat(first.value()).isEqualTo(11.0)
            assertThat(second.value()).isEqualTo(22.0)
        }

        @Test
        fun `should use the literal unnamed tag value when no appender name is provided`() {
            // What is to be tested? Whether a null appender name maps to the literal
            //   "unnamed" tag value instead of a missing appender tag, so every metric
            //   carries the same set of tag keys regardless of configuration.
            // How will the test case be deemed successful and why? Successful if the
            //   accepted counter is found under appender=unnamed with count 1.0; a
            //   registration without the tag would not match this lookup.
            // Why is it important to test this test case? A tag that is sometimes
            //   present and sometimes absent splits one metric into two incompatible
            //   series shapes across deployments; the metrics guide documents
            //   `appender` on every metric with "unnamed" as the fallback value.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = null)

            // When
            metrics.eventAccepted(TopicClass.AUDIT)

            // Then: the tag is present with value "unnamed"
            val accepted =
                registry
                    .requireCounter(
                        MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                        MicrometerKafkaAppenderMetrics.TAG_APPENDER to "unnamed",
                    ).count()
            assertThat(accepted).isEqualTo(1.0)
        }

        @Test
        fun `should treat blank appender names as unnamed`() {
            // What is to be tested? Whether a whitespace-only appender name is
            //   normalized to "unnamed" like a null one, rather than producing an
            //   appender tag whose value is "   ".
            // How will the test case be deemed successful and why? Successful if the
            //   counter tagged appender=unnamed holds exactly the one increment after
            //   eventAccepted on an instance built with the name "   "; the
            //   takeIf(isNotBlank) guard is the single point that makes this true.
            // Why is it important to test this test case? Logback lets an XML
            //   name=" " through, and an all-whitespace label value is invisible in
            //   every dashboard and query UI; normalizing here keeps that
            //   misconfiguration from producing a series nobody can select.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "   ")

            // When
            metrics.eventAccepted(TopicClass.AUDIT)

            // Then
            val accepted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "unnamed",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                )
            assertThat(accepted.count()).isEqualTo(1.0)
        }
    }

    @Nested
    inner class `Info gauges` {
        private val info =
            AppenderInfo(
                fallback = "FALLBACK_FILE",
                fallbackType = "RollingFileAppender",
                debug = false,
                topics =
                    listOf(
                        TopicInfo("audit.security", TopicClass.AUDIT, acks = "all", lingerMs = "50"),
                        TopicInfo("svc.logs", TopicClass.TECHNICAL, acks = "1", lingerMs = "50"),
                    ),
                version = "1.2.3",
                kafkaClientVersion = "4.0.0",
            )

        @Test
        fun `should publish the appender info as a constant-1 gauge with every label`() {
            // What is to be tested? Whether kafka.appender.info carries the
            //   configuration labels plus the appender and common tags, with
            //   the value 1.
            // How will the test case be deemed successful and why? Successful
            //   if exactly one info gauge exists, reads 1.0 and carries exactly
            //   the documented label set with the given values.
            // Why is it important to test this test case? The value must stay
            //   1 for the group_left join to leave joined values untouched, and
            //   the common tags must be present for the join to match at all.

            // Given
            val registry = newRegistry()

            // When
            MicrometerKafkaAppenderMetrics(registry, Tags.of("service", "svc"), appenderName = "KAFKA", info = info)

            // Then
            val gauges = registry.find(MicrometerKafkaAppenderMetrics.METRIC_INFO).gauges()
            assertThat(gauges).hasSize(1)
            assertThat(gauges.single().value()).isEqualTo(1.0)
            assertThat(
                gauges
                    .single()
                    .id.tags
                    .associate { it.key to it.value },
            ).containsExactlyInAnyOrderEntriesOf(
                mapOf(
                    "service" to "svc",
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                    MicrometerKafkaAppenderMetrics.TAG_FALLBACK to "FALLBACK_FILE",
                    MicrometerKafkaAppenderMetrics.TAG_FALLBACK_TYPE to "RollingFileAppender",
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER_VERSION to "1.2.3",
                    MicrometerKafkaAppenderMetrics.TAG_KAFKA_CLIENT_VERSION to "4.0.0",
                    MicrometerKafkaAppenderMetrics.TAG_DEBUG to "false",
                ),
            )
        }

        @Test
        fun `should publish one constant-1 topic info gauge per configured topic`() {
            // What is to be tested? Whether kafka.appender.topic.info has one
            //   series per topic, labelled with topic, class and producer
            //   settings - plus the appender and common tags.
            // How will the test case be deemed successful and why? Successful
            //   if there are exactly two series, both reading 1.0, each with
            //   exactly its own topic's class and settings, the `appender` tag
            //   and the common tag. The tag sets are compared in full: building
            //   them from the topic labels alone (the Tags.of-instead-of-tagsWith
            //   shadowing trap the fullTags KDoc warns about) must fail here.
            // Why is it important to test this test case? One series per topic
            //   is the documented cardinality; the per-topic labels are what
            //   the join onto broker-side metrics matches on, and `appender`
            //   is the documented join key (`on (job, instance, appender,
            //   topic_class)`) - without it two appenders on one registry
            //   would share their topic-info series.

            // Given
            val registry = newRegistry()

            // When
            MicrometerKafkaAppenderMetrics(registry, Tags.of("service", "svc"), appenderName = "KAFKA", info = info)

            // Then
            val gauges = registry.find(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO).gauges()
            assertThat(gauges.map { it.value() }).containsExactly(1.0, 1.0)
            assertThat(
                gauges.map { gauge -> gauge.id.tags.associate { it.key to it.value } },
            ).containsExactlyInAnyOrder(
                mapOf(
                    "service" to "svc",
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC to "audit.security",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    MicrometerKafkaAppenderMetrics.TAG_ACKS to "all",
                    MicrometerKafkaAppenderMetrics.TAG_LINGER_MS to "50",
                ),
                mapOf(
                    "service" to "svc",
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC to "svc.logs",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    MicrometerKafkaAppenderMetrics.TAG_ACKS to "1",
                    MicrometerKafkaAppenderMetrics.TAG_LINGER_MS to "50",
                ),
            )
        }

        @Test
        fun `should register no info gauge without an AppenderInfo`() {
            // What is to be tested? Whether the info gauges are opt-in per
            //   instance (the sender-only binding of the benchmarks passes none).
            // How will the test case be deemed successful and why? Successful
            //   if neither info metric exists after constructing without info.
            // Why is it important to test this test case? An info series with
            //   placeholder labels would assert a configuration nobody made.

            // Given
            val registry = newRegistry()

            // When
            MicrometerKafkaAppenderMetrics(registry, appenderName = "KAFKA")

            // Then
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_INFO).gauges()).isEmpty()
            assertThat(registry.find(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO).gauges()).isEmpty()
        }

        @Test
        fun `should remove the info gauges on deregistration`() {
            // What is to be tested? Whether deregisterFrom removes the info
            //   gauges together with the other meters.
            // How will the test case be deemed successful and why? Successful
            //   if no kafka.appender.* meter remains.
            // Why is it important to test this test case? A stale info series
            //   after a reconfiguration would join the old fallback name onto
            //   the new instance's counters.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "KAFKA", info = info)

            // When
            metrics.deregisterFrom(registry)

            // Then
            assertThat(appenderMeters(registry)).isEmpty()
        }
    }
}
