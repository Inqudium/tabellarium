package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.status.Status
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The metrics lifecycle of a running [KafkaAppender]: meter
 * registration and removal around bind, re-bind and stop, the hot path
 * recording into the bound meters, and two appenders sharing one
 * registry.
 */
class KafkaAppenderMetricsLifecycleTest : KafkaAppenderTestBase() {
    /*
     * What needs the appender: the hot path recording into bound meters,
     * the unbind in stop() after the dispatcher drain, the not-started
     * refusal, and two appenders on one registry. The binding-level
     * behaviours (identity refusal, re-bind ordering, breaker consumer
     * lifecycle, partial removal, the producer-metrics mirror) are proven
     * on MetricsBindings directly in MetricsBindingsTest
     * (ARCHITECTURE_REVIEW-2026-10-10T22-15-57, finding 3).
     */
    @Nested
    inner class `Metrics lifecycle` {
        @Test
        fun `should deregister its meters from the registry on stop`() {
            // What is to be tested? Whether stop() removes everything
            //   bindMeterRegistry registered - otherwise every Logback
            //   reconfiguration cycle leaks meters and leaves gauges
            //   reading the previous (closed) dispatcher's queue.
            // How will the test case be deemed successful and why? Successful
            //   if after stop() the registry no longer contains any
            //   kafka.appender.* meter. This pins the register/deregister
            //   symmetry of the metrics lifecycle.
            // Why is it important to test this test case? Meter leaks are
            //   invisible in tests and single-start deployments; they
            //   surface as slowly growing scrape payloads and misleading
            //   dashboards only after reload cycles in production.

            // Given: a bound appender with a fallback (so queue gauges exist)
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.start()
            appender.bindMeterRegistry(registry)
            assertThat(registry.meters)
                .anyMatch { it.id.name.startsWith("kafka.appender.") }

            // When
            appender.stop()

            // Then: all appender meters are gone
            assertThat(registry.meters)
                .noneMatch { it.id.name.startsWith("kafka.appender.") }
        }

        @Test
        fun `should move every meter when rebound to a different registry`() {
            // What is to be tested? Whether a repeated bindMeterRegistry call
            //   (context refresh, manual re-wiring) tears the previous
            //   registration down before registering anew - observed on a
            //   SECOND registry, where a missing unbind leaves the first
            //   registry's meters behind.
            // How will the test case be deemed successful and why? Successful
            //   if after the rebind the first registry holds no kafka.appender.*
            //   meter (the breaker and producer meters live in that namespace
            //   too) while the second holds the full set, and the fallback
            //   queue gauge of the second reads the live dispatcher: with the
            //   fallback worker parked, the events diverted after the rebind
            //   show up in it. The former same-registry count comparison could
            //   not detect a missing unbind: Micrometer's register is
            //   idempotent on the meter id, so the count stayed equal either
            //   way (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 16).
            // Why is it important to test this test case? Meters left behind
            //   on a replaced registry keep reporting a pipeline that no
            //   longer publishes there - stale gauges and frozen counters on
            //   the old scrape endpoint, and a leak per context refresh.

            // Given: bound to a first registry; every send fails
            //   synchronously (diverting with send.error) and the fallback
            //   worker parks on its first event, so diverted events stay
            //   queued where the queue gauge can see them
            val first = SimpleMeterRegistry()
            val second = SimpleMeterRegistry()
            val release = CountDownLatch(1)
            val parkingFallback =
                object : AppenderBase<ILoggingEvent>() {
                    override fun append(eventObject: ILoggingEvent) {
                        release.await(10, TimeUnit.SECONDS)
                    }
                }.apply {
                    context = LoggerContext()
                    start()
                }
            val failingSends =
                RecordingProducerFactory(
                    wrap = { mock ->
                        object : Producer<ByteArray, ByteArray> by mock {
                            override fun send(
                                record: ProducerRecord<ByteArray, ByteArray>,
                                callback: Callback?,
                            ): Future<RecordMetadata> = throw IllegalStateException("simulated broker outage")
                        }
                    },
                )
            val appender = newAppender(fallback = parkingFallback, producerFactory = failingSends)
            appender.start()
            appender.bindMeterRegistry(first)
            val ownMeter: (Meter) -> Boolean = { it.id.name.startsWith("kafka.appender.") }
            val boundToFirst = first.meters.filter(ownMeter).map { it.id }
            assertThat(boundToFirst).isNotEmpty

            // When: rebound to a second registry, then three events are
            //   diverted - the fallback worker parks on the first, two stay
            //   queued
            appender.bindMeterRegistry(second)
            repeat(3) { appender.doAppend(newTestLoggingEvent()) }

            // Then: the first registry is empty of the appender's meters, the
            //   second holds the same set and its queue gauge is live
            assertThat(first.meters).noneMatch(ownMeter)
            assertThat(second.meters.filter(ownMeter).map { it.id })
                .containsExactlyInAnyOrderElementsOf(boundToFirst)
            val queueSize = second.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
            pollUntil { queueSize.value() == 2.0 }
            release.countDown()
            appender.stop()
        }

        @Test
        fun `should refuse a bind on an appender that is not started and register nothing`() {
            // What is to be tested? The not-started branch of the bind: a
            //   bindMeterRegistry call before start() and after stop()
            //   returns false, warns, and registers no meter.
            // How will the test case be deemed successful and why? Successful
            //   if both calls return false, the status names the ignored
            //   bind, and the registry stays empty - the boolean is what
            //   the Spring helper logs on, so a true here would print a
            //   success for nothing
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 5).
            // Why is it important to test this test case? Meters registered
            //   by a stopped appender have no later unbind to remove them.

            // Given
            val registry = SimpleMeterRegistry()
            val appender = newAppender()

            // When / Then: before start
            assertThat(appender.bindMeterRegistry(registry)).isFalse()
            assertThat(registry.meters).isEmpty()

            // When / Then: after stop
            appender.start()
            appender.stop()
            assertThat(appender.bindMeterRegistry(registry)).isFalse()
            assertThat(registry.meters).isEmpty()
            assertThat(appender.statusMessages())
                .filteredOn { it.contains("bindMeterRegistry called on a stopped/uninitialized appender") }
                .hasSize(2)
        }

        @Test
        fun `should stop recording into removed meters when a re-bind to the same registry fails`() {
            // What is to be tested? A re-bind to the SAME registry has to
            //   remove the old meters first (the new ones would share their
            //   IDs). When the registration then fails, the hot path must
            //   switch to no-op metrics instead of recording into the
            //   removed meter objects.
            // How will the test case be deemed successful and why? Successful
            //   if, after the failed re-bind, the appender is unbound and an
            //   appended event does not increment the old (removed) counter.
            //   Without the switch the hot path kept the old implementation
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 6).
            // Why is it important to test this test case? Counts into meters
            //   no registry exports are silent loss, and the appender would
            //   claim to be unbound while still recording.

            // Given: bound once; the registry rejects counters from now on
            val rejectCounters = AtomicBoolean(false)
            val registry =
                object : SimpleMeterRegistry() {
                    override fun newCounter(id: Meter.Id): Counter {
                        check(!rejectCounters.get()) { "simulated counter registration failure" }
                        return super.newCounter(id)
                    }
                }
            val appender = newAppender()
            appender.start()
            appender.bindMeterRegistry(registry)
            val oldAccepted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )
            rejectCounters.set(true)

            // When: re-bound to the same registry, which now fails
            assertThatThrownBy { appender.bindMeterRegistry(registry) }
                .hasMessageContaining("simulated counter registration failure")
            appender.doAppend(newTestLoggingEvent(message = "after the failed re-bind"))

            // Then: unbound, and the removed counter did not move
            assertThat(appender.isMeterRegistryBound).isFalse()
            assertThat(oldAccepted.count()).isEqualTo(0.0)
        }

        @Test
        fun `should keep recording into the first registry when a re-bind to another registry throws`() {
            // What is to be tested? The appender-level half of the re-bind
            //   ordering: when a bind to a SECOND registry throws, the
            //   binding to the first registry stays - and so do the hot
            //   path's and the transport's recorders, which installMetrics
            //   must not reset to NO_OP for a binding that is still intact.
            // How will the test case be deemed successful and why? Successful
            //   if the failure propagates, the appender still reports itself
            //   bound, and one appended event advances the first registry's
            //   accepted counter (recorded by the hot path) and dispatched
            //   counter (recorded by the send worker through the transport).
            //   MetricsBindingsTest proves isBoundTo(first) at binding level
            //   but cannot see the appender's metrics field; dropping the
            //   `if (!bindings.isBound)` guard in installMetrics installs
            //   NO_OP on every throwing bind and stayed green
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 2).
            // Why is it important to test this test case? isMeterRegistryBound
            //   true while the hot path records nothing is monitoring dark
            //   with a green suite - the symptom of
            //   DEFECT_ANALYSIS-2026-10-10T15-19-31, finding 6.

            // Given: bound to a first registry; a second registry that rejects counters
            val first = SimpleMeterRegistry()
            val counterRejectingRegistry =
                object : SimpleMeterRegistry() {
                    override fun newCounter(id: Meter.Id): Counter = throw IllegalStateException("simulated counter registration failure")
                }
            val appender = newAppender()
            appender.start()
            assertThat(appender.bindMeterRegistry(first)).isTrue()
            val accepted =
                first.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )
            val dispatched =
                first.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )

            // When: the re-bind to the second registry fails, then one event
            assertThatThrownBy { appender.bindMeterRegistry(counterRejectingRegistry) }
                .hasMessageContaining("simulated counter registration failure")
            appender.doAppend(newTestLoggingEvent(message = "after the failed re-bind"))

            // Then: still bound, and both recorders still feed the first registry
            assertThat(appender.isMeterRegistryBound).isTrue()
            assertThat(accepted.count()).isEqualTo(1.0)
            pollUntil { dispatched.count() == 1.0 }
        }

        @Test
        fun `should keep recording into the first registry when a re-bind to another registry is refused`() {
            // What is to be tested? The refused variant of the case above: a
            //   bind to a SECOND registry that already holds a same-named
            //   sibling's live binding is refused, and the appender keeps
            //   both its binding to the first registry and its recorders.
            // How will the test case be deemed successful and why? Successful
            //   if the refused bind returns false, the appender still reports
            //   itself bound, and one appended event advances the first
            //   registry's accepted and dispatched counters. The only
            //   appender-level refusal in this class refuses an appender that
            //   was never bound; installing NO_OP before the `return false` in
            //   installMetrics stayed green
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 2).
            // Why is it important to test this test case? Same as above: a
            //   refusal that silently turns working monitoring dark is worse
            //   than the refusal it reports.

            // Given: A bound to the first registry, a same-named B to the second
            val first = SimpleMeterRegistry()
            val second = SimpleMeterRegistry()
            val appenderA = newAppender()
            appenderA.name = "KAFKA"
            val appenderB = newAppender()
            appenderB.name = "KAFKA"
            appenderA.start()
            appenderB.start()
            assertThat(appenderA.bindMeterRegistry(first)).isTrue()
            assertThat(appenderB.bindMeterRegistry(second)).isTrue()
            val accepted =
                first.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )
            val dispatched =
                first.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )

            // When: A's re-bind to the second registry is refused, then one event
            assertThat(appenderA.bindMeterRegistry(second)).isFalse()
            assertThat(appenderA.statusMessages()).anyMatch { it.contains("Metrics binding refused") }
            appenderA.doAppend(newTestLoggingEvent(message = "after the refused re-bind"))

            // Then: still bound to the first registry, both recorders still feed it
            assertThat(appenderA.isMeterRegistryBound).isTrue()
            assertThat(accepted.count()).isEqualTo(1.0)
            pollUntil { dispatched.count() == 1.0 }
        }

        @Test
        fun `should record breaker call outcomes through the appender-tagged timers`() {
            // What is to be tested? Whether the event-driven call meters
            //   actually receive the breaker's outcomes - the own binder
            //   wires Resilience4j event consumers by hand, and a wiring
            //   mistake would leave permanently-zero timers that look
            //   healthy on a dashboard.
            // How will the test case be deemed successful and why? Successful
            //   if a successful send increments the kind=successful call
            //   timer of this appender's breaker.
            // Why is it important to test this test case? A silent
            //   zero-counting timer would be worse than an absent one:
            //   operators would read "no calls" during an incident and
            //   look elsewhere.

            // Given: a bound appender whose MockProducer auto-succeeds
            val registry = SimpleMeterRegistry()
            val appender = newAppender()
            appender.name = "KAFKA_A"
            appender.start()
            appender.bindMeterRegistry(registry)

            // When: one event flows through the breaker-guarded send
            appender.doAppend(newTestLoggingEvent(message = "counted"))

            // Then: the successful-calls timer of THIS appender counted it
            //   (the send - and with it the breaker outcome - completes on
            //   the dispatcher worker, hence the bounded poll)
            val timer =
                registry.requireTimer(
                    "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.calls",
                    "kind" to "successful",
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA_A",
                )
            pollUntil { timer.count() == 1L }
            appender.stop()
        }

        @Test
        fun `should count a send refused by an open breaker on the not-permitted counter`() {
            // What is to be tested? The not-permitted wiring: with the class's
            //   breaker OPEN, the sender is refused a permission and the
            //   breaker publishes a call-not-permitted event, which must
            //   increment kafka.appender.circuitbreaker.not.permitted.calls.
            // How will the test case be deemed successful and why? Successful
            //   if after one append against an open breaker the counter of
            //   this appender's breaker reads 1 and the event was diverted
            //   with reason breaker.open. Mutation checked: dropping the
            //   onCallNotPermitted consumer turns it red
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 4).
            // Why is it important to test this test case? The not-permitted
            //   count is how operators size the loss an open breaker causes;
            //   an unwired counter would read zero during the whole outage.

            // Given: a bound appender whose TECHNICAL breaker is forced open
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender()
            val appender = newAppender(fallback = fallback)
            appender.name = "KAFKA_O"
            appender.start()
            appender.bindMeterRegistry(registry)
            appender.circuitBreakerRegistry
                .circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.TECHNICAL))
                .transitionToOpenState()

            // When
            appender.doAppend(newTestLoggingEvent(message = "refused"))

            // Then: one not-permitted call, the event in the fallback
            val notPermitted =
                registry.requireCounter(
                    "${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.not.permitted.calls",
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA_O",
                )
            pollUntil { notPermitted.count() == 1.0 }
            appender.stop()
            assertThat(fallback.events.map { it.formattedMessage }).containsExactly("refused")
        }

        @Test
        fun `should keep two appenders' circuit-breaker meters apart on a shared registry`() {
            // What is to be tested? The multi-instance scenario of finding
            //   M-5 (same report as above): two KafkaAppenders bound to
            //   the same MeterRegistry
            //   must produce distinguishable breaker meters, and stopping
            //   one must not tear down the observability of the other.
            // How will the test case be deemed successful and why? Successful
            //   if both appenders' state gauges coexist under distinct
            //   appender tags, and after stopping the first appender its
            //   meters are gone while the second appender's remain.
            // Why is it important to test this test case? Before the
            //   appender tag, the IDs collided: gauges reported only one
            //   instance, and one appender's unbind removed the meters the
            //   other was still using - operators then made breaker
            //   decisions on missing or mixed data.

            // Given: two named appenders on one registry
            val registry = SimpleMeterRegistry()
            val appenderA = newAppender()
            appenderA.name = "KAFKA_A"
            val appenderB = newAppender()
            appenderB.name = "KAFKA_B"
            appenderA.start()
            appenderB.start()
            appenderA.bindMeterRegistry(registry)
            appenderB.bindMeterRegistry(registry)

            // Then: both instances' breaker meters coexist, distinct by tag
            val stateGauges =
                registry
                    .find("${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.state")
                    .gauges()
            assertThat(stateGauges.map { it.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER) }.toSet())
                .containsExactlyInAnyOrder("KAFKA_A", "KAFKA_B")
            // And: no collision warning was emitted for distinct names
            assertThat(appenderB.statusMessages())
                .noneMatch { it.contains("Metrics binding refused") }
            assertThat(appenderB.isMeterRegistryBound).isTrue()

            // When: the first appender stops
            appenderA.stop()

            // Then: its meters are gone, the second appender's remain
            val remaining =
                registry.meters.filter { it.id.name.startsWith("kafka.appender.circuitbreaker") }
            assertThat(remaining).isNotEmpty()
            assertThat(remaining).allSatisfy { meter ->
                assertThat(meter.id.getTag(MicrometerKafkaAppenderMetrics.TAG_APPENDER)).isEqualTo("KAFKA_B")
            }
            appenderB.stop()
            assertThat(registry.meters)
                .noneMatch { it.id.name.startsWith("kafka.appender.circuitbreaker") }
        }

        @Test
        fun `should refuse the second binding when two appenders share the same name on one registry`() {
            // What is to be tested? The residual collision the appender
            //   tag cannot resolve: two appenders with the SAME name (the
            //   tag value) on the same registry. Micrometer would hand
            //   both the same meter objects, so the second bind must be
            //   refused as a whole - appender, breaker and producer meters
            //   - with a status warning, and the first appender's series
            //   must survive the second appender's stop.
            // How will the test case be deemed successful and why? Successful
            //   if the second appender reports the refusal, is not bound,
            //   registered nothing (the meter count is unchanged and its
            //   events do not reach the shared counters), and after it
            //   stops the first appender's counters and gauges are still
            //   registered and still count. Before the fix both instances
            //   shared the meters, mixed their counts, and the first stop
            //   removed the series the survivor still published through
            //   (docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md, L-2).
            // Why is it important to test this test case? A dashboard that
            //   loses an appender's series when a same-named sibling stops
            //   shows an outage as "no data"; refusing loudly keeps the
            //   first instance trustworthy.

            // Given: two appenders with the same name on one registry
            val registry = SimpleMeterRegistry()
            val appenderA = newAppender()
            appenderA.name = "KAFKA"
            val appenderB = newAppender()
            appenderB.name = "KAFKA"
            appenderA.start()
            appenderB.start()
            assertThat(appenderA.bindMeterRegistry(registry)).isTrue()
            val metersAfterFirstBind = registry.meters.size

            // When
            val boundB = appenderB.bindMeterRegistry(registry)

            // Then: refused (also in the return value the Spring helper
            //   logs on), warned, nothing added
            assertThat(boundB).isFalse()
            assertThat(appenderB.statusMessages())
                .anyMatch { it.contains("Metrics binding refused") && it.contains("appender='KAFKA'") }
            assertThat(appenderB.isMeterRegistryBound).isFalse()
            assertThat(appenderA.isMeterRegistryBound).isTrue()
            assertThat(registry.meters).hasSize(metersAfterFirstBind)

            // And: B's events do not reach A's counters
            appenderB.doAppend(newTestLoggingEvent(message = "counted nowhere"))
            appenderA.doAppend(newTestLoggingEvent(message = "counted by A"))
            val accepted =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "KAFKA",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.TECHNICAL.tag,
                )
            assertThat(accepted.count()).isEqualTo(1.0)

            // When: the refused appender stops
            appenderB.stop()

            // Then: A's series are intact and still count
            assertThat(registry.meters).hasSize(metersAfterFirstBind)
            appenderA.doAppend(newTestLoggingEvent(message = "still counted by A"))
            assertThat(accepted.count()).isEqualTo(2.0)
            assertThat(registry.find("${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.state").gauges()).isNotEmpty()
            appenderA.stop()
            assertThat(registry.meters).isEmpty()
        }

        @Test
        fun `should bind and stop cleanly when a MeterFilter denies the info gauges`() {
            // What is to be tested? Whether the documented way to switch the
            //   info gauges off - a Micrometer MeterFilter.deny on their names -
            //   suppresses exactly them and leaves binding and stopping intact.
            // How will the test case be deemed successful and why? Successful
            //   if after the bind neither info metric is in the registry while
            //   the other kafka.appender.* meters are, no warning was reported,
            //   and stop() still leaves no kafka.appender.* meter behind.
            // Why is it important to test this test case? There is deliberately
            //   no appender switch for the info gauges; the MeterFilter is the
            //   documented replacement, so it must not break the lifecycle
            //   (the denied gauges come back as no-op meters that the
            //   deregistration also has to tolerate).

            // Given: a registry that denies both info metrics
            val registry = SimpleMeterRegistry()
            val infoNames =
                setOf(MicrometerKafkaAppenderMetrics.METRIC_INFO, MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO)
            registry.config().meterFilter(MeterFilter.deny { it.name in infoNames })
            val appender = newAppender(fallback = RecordingAppender())
            appender.start()
            val warningsBeforeBind = appender.statusEntries().count { it.level >= Status.WARN }

            // When
            appender.bindMeterRegistry(registry)

            // Then: only the info gauges are missing, and the bind added no warning
            assertThat(registry.meters).noneMatch { it.id.name in infoNames }
            assertThat(registry.meters).anyMatch { it.id.name == MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED }
            assertThat(appender.statusEntries().count { it.level >= Status.WARN }).isEqualTo(warningsBeforeBind)
            appender.stop()
            assertThat(registry.meters).noneMatch { it.id.name.startsWith("kafka.appender.") }
        }

        @Test
        fun `should publish the appender and topic info of the running configuration`() {
            // What is to be tested? Whether bindMeterRegistry publishes the
            //   info gauges with the labels of the started configuration:
            //   fallback name and type, debug flag, and one topic series per
            //   configured topic with its class's producer settings.
            // How will the test case be deemed successful and why? Successful
            //   if kafka.appender.info names the attached fallback and the
            //   debug flag, and kafka.appender.topic.info holds the default
            //   topic (TECHNICAL: acks=1, linger.ms=50) and the mapped audit
            //   topic (AUDIT: acks=all, linger.ms=50) - the TopicClass defaults.
            // Why is it important to test this test case? Only the appender
            //   knows the fallback slot, the plan and the producer properties
            //   at once; the unit tests of the parts cannot catch a wiring
            //   that passes the wrong source.

            // Given: a started appender with a named fallback and an audit mapping
            val registry = SimpleMeterRegistry()
            val fallback = RecordingAppender().apply { name = "FALLBACK_FILE" }
            val appender = newAppender(fallback = fallback, debug = true)
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )
            appender.start()

            // When
            appender.bindMeterRegistry(registry)

            // Then
            val info = registry.requireGauge(MicrometerKafkaAppenderMetrics.METRIC_INFO)
            assertThat(info.id.getTag(MicrometerKafkaAppenderMetrics.TAG_FALLBACK)).isEqualTo("FALLBACK_FILE")
            assertThat(info.id.getTag(MicrometerKafkaAppenderMetrics.TAG_FALLBACK_TYPE))
                .isEqualTo(RecordingAppender::class.java.simpleName)
            assertThat(info.id.getTag(MicrometerKafkaAppenderMetrics.TAG_DEBUG)).isEqualTo("true")
            assertThat(
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO).gauges().map { gauge ->
                    listOf(
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC,
                        MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS,
                        MicrometerKafkaAppenderMetrics.TAG_ACKS,
                        MicrometerKafkaAppenderMetrics.TAG_LINGER_MS,
                    ).map { gauge.id.getTag(it) }
                },
            ).containsExactlyInAnyOrder(
                listOf("default.topic", "technical", "1", "50"),
                listOf("audit.security", "audit", "all", "50"),
            )
        }
    }
}
