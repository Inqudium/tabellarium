package eu.inqudium.tabellarium

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Offset
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class MicrometerKafkaAppenderMetricsTest {
    private fun newRegistry(): SimpleMeterRegistry = SimpleMeterRegistry()

    private fun counter(
        registry: SimpleMeterRegistry,
        name: String,
        vararg tags: Pair<String, String>,
    ): Double {
        val tagList = tags.map { Tag.of(it.first, it.second) }
        return registry
            .find(name)
            .tags(tagList)
            .counter()
            ?.count()
            ?: error("counter $name with tags ${tags.toList()} not found in registry")
    }

    private fun timer(
        registry: SimpleMeterRegistry,
        name: String,
        vararg tags: Pair<String, String>,
    ): Timer {
        val tagList = tags.map { Tag.of(it.first, it.second) }
        return registry.find(name).tags(tagList).timer()
            ?: error("timer $name with tags ${tags.toList()} not found in registry")
    }

    private fun gauge(
        registry: SimpleMeterRegistry,
        name: String,
        vararg tags: Pair<String, String>,
    ): Gauge {
        val tagList = tags.map { Tag.of(it.first, it.second) }
        return registry.find(name).tags(tagList).gauge()
            ?: error("gauge $name with tags ${tags.toList()} not found in registry")
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
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                ),
            ).isEqualTo(2.0)
            assertThat(
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                ),
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
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    MicrometerKafkaAppenderMetrics.TAG_REASON to "breaker.open",
                )
            val throttleCount =
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    MicrometerKafkaAppenderMetrics.TAG_REASON to "throttle",
                )
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
            val droppedCount =
                registry
                    .find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED)
                    .counter()
                    ?.count()
                    ?: error("dropped counter not registered")
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
                timer(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                    MicrometerKafkaAppenderMetrics.TAG_OUTCOME to "success",
                )
            val errorTimer =
                timer(
                    registry,
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
            val queueSize = gauge(registry, MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
            assertThat(queueSize.value()).isEqualTo(0.0)

            backing.set(42)
            assertThat(queueSize.value()).isEqualTo(42.0)
        }

        @Test
        fun `should expose the fixed capacity as a constant gauge`() {
            // What is to be tested? Whether registerFallbackQueueGauges publishes the
            //   dispatcher's capacity as a gauge with the value handed in, alongside the
            //   size gauge that the constructor pre-registered.
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
            assertThat(gauge(registry, MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY).value()).isEqualTo(2048.0)
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
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    "service" to "payment-service",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                )
            assertThat(accepted).isEqualTo(1.0)

            val dropped =
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED,
                    "service" to "payment-service",
                )
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
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "audit",
                )
            assertThat(accepted).isEqualTo(1.0)

            val dropped =
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                )
            assertThat(dropped).isEqualTo(1.0)

            val queueSize =
                gauge(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                )
            assertThat(queueSize.value()).isEqualTo(7.0)

            val queueCapacity =
                gauge(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender",
                )
            assertThat(queueCapacity.value()).isEqualTo(100.0)
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
            val first = gauge(registry, MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE, MicrometerKafkaAppenderMetrics.TAG_APPENDER to "audit-appender")
            val second = gauge(registry, MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE, MicrometerKafkaAppenderMetrics.TAG_APPENDER to "technical-appender")
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
            //   series shapes across deployments; the metrics overview documents
            //   `appender` on every metric with "unnamed" as the fallback value.

            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = null)

            // When
            metrics.eventAccepted(TopicClass.AUDIT)

            // Then: the tag is present with value "unnamed"
            val accepted =
                counter(
                    registry,
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED,
                    MicrometerKafkaAppenderMetrics.TAG_APPENDER to "unnamed",
                )
            assertThat(accepted).isEqualTo(1.0)
        }

        @Test
        fun `should treat blank appender names as unnamed`() {
            // What is to be tested? Whether a whitespace-only appender name is
            //   normalized to "unnamed" like a null one, rather than producing an
            //   appender tag whose value is "   ".
            // How will the test case be deemed successful and why? Successful if a
            //   counter tagged appender=unnamed exists after eventAccepted on an
            //   instance built with the name "   "; the takeIf(isNotBlank) guard is the
            //   single point that makes this true.
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
                registry
                    .find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "unnamed")
                    .counter()
            assertThat(accepted).isNotNull
        }
    }
}
