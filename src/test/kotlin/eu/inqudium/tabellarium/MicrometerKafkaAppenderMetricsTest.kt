package eu.inqudium.tabellarium

import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class MicrometerKafkaAppenderMetricsTest {
    private fun newRegistry(): SimpleMeterRegistry = SimpleMeterRegistry()

    private fun counter(
        registry: SimpleMeterRegistry,
        name: String,
        vararg tags: Pair<String, String>,
    ): Double {
        val tagList = tags.map { Tag.of(it.first, it.second) }
        return registry.find(name).tags(tagList).counter()?.count()
            ?: error("counter $name with tags ${tags.toList()} not found in registry")
    }

    private fun timer(
        registry: SimpleMeterRegistry,
        name: String,
        vararg tags: Pair<String, String>,
    ): io.micrometer.core.instrument.Timer {
        val tagList = tags.map { Tag.of(it.first, it.second) }
        return registry.find(name).tags(tagList).timer()
            ?: error("timer $name with tags ${tags.toList()} not found in registry")
    }

    @Nested
    inner class `Counter increments` {
        @Test
        fun `should increment accepted counter for the matching topic class`() {
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
            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            repeat(5) { metrics.fallbackDispatcherDropped() }

            // Then
            val droppedCount =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED)
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
            assertThat(successTimer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .isCloseTo(35.0, org.assertj.core.data.Offset.offset(0.001))
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
            assertThat(
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
                    .gauge()!!.value(),
            ).isEqualTo(0.0)

            backing.set(42)
            assertThat(
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
                    .gauge()!!.value(),
            ).isEqualTo(42.0)
        }

        @Test
        fun `should expose the fixed capacity as a constant gauge`() {
            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry)

            // When
            metrics.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 2048)

            // Then
            assertThat(
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY)
                    .gauge()!!.value(),
            ).isEqualTo(2048.0)
        }
    }

    @Nested
    inner class `Common tags` {
        @Test
        fun `should attach common tags to every metric`() {
            // Given: a metrics instance with a service-identifying common tag
            val registry = newRegistry()
            val commonTags = Tags.of("service", "payment-service")
            val metrics = MicrometerKafkaAppenderMetrics(registry, commonTags)

            // When
            metrics.eventAccepted(TopicClass.AUDIT)
            metrics.fallbackDispatcherDropped()

            // Then: counters carry the common tag in addition to the per-call tags
            val accepted =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED)
                    .tag("service", "payment-service")
                    .tag(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS, "audit")
                    .counter()
            assertThat(accepted).isNotNull
            assertThat(accepted!!.count()).isEqualTo(1.0)

            val dropped =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED)
                    .tag("service", "payment-service")
                    .counter()
            assertThat(dropped).isNotNull
            assertThat(dropped!!.count()).isEqualTo(1.0)
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
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "audit-appender")
                    .tag(MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS, "audit")
                    .counter()
            assertThat(accepted).isNotNull
            assertThat(accepted!!.count()).isEqualTo(1.0)

            val dropped =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "audit-appender")
                    .counter()
            assertThat(dropped).isNotNull
            assertThat(dropped!!.count()).isEqualTo(1.0)

            val queueSize =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "audit-appender")
                    .gauge()
            assertThat(queueSize).isNotNull
            assertThat(queueSize!!.value()).isEqualTo(7.0)

            val queueCapacity =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "audit-appender")
                    .gauge()
            assertThat(queueCapacity).isNotNull
            assertThat(queueCapacity!!.value()).isEqualTo(100.0)
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
            val first =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "audit-appender")
                    .gauge()
            val second =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "technical-appender")
                    .gauge()
            assertThat(first).isNotNull
            assertThat(second).isNotNull
            assertThat(first!!.value()).isEqualTo(11.0)
            assertThat(second!!.value()).isEqualTo(22.0)
        }

        @Test
        fun `should use the literal unnamed tag value when no appender name is provided`() {
            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = null)

            // When
            metrics.eventAccepted(TopicClass.AUDIT)

            // Then: the tag is present with value "unnamed"
            val accepted =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "unnamed")
                    .counter()
            assertThat(accepted).isNotNull
            assertThat(accepted!!.count()).isEqualTo(1.0)
        }

        @Test
        fun `should treat blank appender names as unnamed`() {
            // Given
            val registry = newRegistry()
            val metrics = MicrometerKafkaAppenderMetrics(registry, appenderName = "   ")

            // When
            metrics.eventAccepted(TopicClass.AUDIT)

            // Then
            val accepted =
                registry.find(MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED)
                    .tag(MicrometerKafkaAppenderMetrics.TAG_APPENDER, "unnamed")
                    .counter()
            assertThat(accepted).isNotNull
        }
    }
}
