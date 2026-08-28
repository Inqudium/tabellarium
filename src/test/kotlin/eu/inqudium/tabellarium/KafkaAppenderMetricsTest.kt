package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration

class KafkaAppenderMetricsTest {
    @Nested
    inner class `No-op implementation` {
        @Test
        fun `should accept all hook calls without throwing or allocating`() {
            // What is to be tested? Whether the NO_OP singleton tolerates
            //   every hook call defined on the interface. This is the
            //   default state of the appender when no Micrometer registry
            //   is bound, so misbehavior here would crash production
            //   logging for users that opted out of metrics entirely.
            // How will the test case be deemed successful and why? Successful
            //   if every method on every enum combination runs without
            //   exception. We don't assert "no allocation" mechanically
            //   (impossible without JFR or similar), but the
            //   implementation is verified by inspection: NoOp methods
            //   have an empty body that returns Unit.
            // Why is it important to test this test case? The NoOp path
            //   runs on every hot-path event when metrics are not bound.
            //   A regression here (e.g. accidental allocation, accidental
            //   exception) would manifest as a CPU regression or a hot-
            //   path crash, both very expensive to diagnose in production.

            // Given
            val noOp = KafkaAppenderMetrics.NO_OP

            // When / Then: every combination must be silently accepted
            for (tc in TopicClass.entries) {
                noOp.eventAccepted(tc)
                noOp.eventDispatched(tc)
                for (reason in KafkaAppenderMetrics.FallbackReason.entries) {
                    noOp.eventFallback(tc, reason)
                }
                for (outcome in KafkaAppenderMetrics.SendOutcome.entries) {
                    noOp.sendCompleted(tc, outcome, Duration.ofMillis(7))
                }
            }
            noOp.fallbackDispatcherDropped()
            noOp.registerFallbackQueueGauges(queueSize = { 0 }, capacity = 100)
        }

        @Test
        fun `should always return the same NO_OP instance`() {
            // Then: companion-provided NO_OP must be a stable singleton
            //   so callers can hold a reference and compare by identity.
            val first = KafkaAppenderMetrics.NO_OP
            val second = KafkaAppenderMetrics.NO_OP
            assertThat(first).isSameAs(second)
        }
    }

    @Nested
    inner class `Enum tag values` {
        @Test
        fun `should have distinct lowercase dot-friendly tag values for FallbackReason`() {
            // Given / When
            val tags = KafkaAppenderMetrics.FallbackReason.entries.map { it.tag }

            // Then: tags are pairwise distinct, all lowercase, no spaces
            //   or backslashes that would confuse a Prometheus exporter
            assertThat(tags).doesNotHaveDuplicates()
            assertThat(tags).allSatisfy { tag ->
                assertThat(tag).isEqualTo(tag.lowercase())
                assertThat(tag).doesNotContain(" ")
                assertThat(tag).doesNotContain("\\")
            }
        }

        @Test
        fun `should have distinct lowercase tag values for SendOutcome`() {
            // Given / When
            val tags = KafkaAppenderMetrics.SendOutcome.entries.map { it.tag }

            // Then
            assertThat(tags).doesNotHaveDuplicates()
            assertThat(tags).allSatisfy { tag ->
                assertThat(tag).isEqualTo(tag.lowercase())
            }
        }

        @Test
        fun `should have distinct lowercase tag values for TopicClass`() {
            // What is to be tested? Whether each TopicClass exposes a
            //   stable, prometheus-safe tag value. This is the dimension
            //   along which the cardinality budget is calculated, so
            //   any duplicates would silently collapse series.
            // How will the test case be deemed successful and why? Successful
            //   if every TopicClass has a distinct, lowercase tag and the
            //   tags match the lowercase of the enum names. The latter
            //   pins the naming convention so future enum additions
            //   stay consistent.
            // Why is it important to test this test case? A duplicate
            //   tag value (e.g. two classes both mapping to "audit")
            //   would cause Micrometer to merge their metrics - a silent,
            //   nearly undetectable data-corruption bug in production
            //   dashboards.

            // Given / When
            val tags = TopicClass.entries.map { it.tag }
            val expected = TopicClass.entries.map { it.name.lowercase() }

            // Then
            assertThat(tags).doesNotHaveDuplicates()
            assertThat(tags).containsExactlyElementsOf(expected)
        }
    }
}
