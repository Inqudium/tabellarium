package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * Keeps the configuration guide's numbers in step with the code. The
 * guide's defaults quick reference is the canonical statement of the
 * "code" defaults (README and KDoc link there instead of restating
 * them); this test is what makes it canonical - a changed constant
 * fails the build until the table follows, the same anti-drift
 * principle the generated coverage and test-evidence pages apply
 * (docs/assessment/ARCHITECTURE_REVIEW-2026-09-07T20-23-00.md, finding 2).
 * The metric-inventory check does the same for the metrics overview
 * (docs/assessment/COMMENT_AUDIT-2026-09-07T21-20-00.md, finding 5).
 *
 * Runs from the module root (Surefire's working directory), where the
 * guide lives under `docs/`.
 */
class DocumentationContractTest {
    private val guide: List<String> =
        Files.readAllLines(Path.of("docs", "config", "kafka-appender-config-guide.md"))

    /** The "Default" cell of the defaults-table row whose "Setting" cell starts with [setting]. */
    private fun defaultOf(setting: String): String {
        val row =
            guide.singleOrNull { it.startsWith("| $setting") }
                ?: error("defaults table row '$setting' not found exactly once in the configuration guide")
        return row.split("|")[2].trim()
    }

    /** The "Default" cell of the circuit-breaker table row for [property]. */
    private fun breakerDefaultOf(property: String): String {
        val row =
            guide.singleOrNull { it.startsWith("| `$property`") }
                ?: error("circuit-breaker table row '$property' not found exactly once in the configuration guide")
        return row.split("|")[2].trim()
    }

    @Nested
    inner class `Defaults quick reference` {
        @Test
        fun `should state the dispatcher and producer budgets exactly as the constants define them`() {
            // What is to be tested? Whether the guide's defaults quick
            //   reference carries the queue capacities and the shutdown
            //   budgets that the code actually uses.
            // How will the test case be deemed successful and why? Successful
            //   if each "code" row's Default cell contains the value formatted
            //   from the corresponding constant - so a constant change without
            //   a table update fails the build.
            // Why is it important to test this test case? These numbers drifted
            //   three times on 2026-09-07 alone (the 200 ms drain window that
            //   fix 3 removed survived in the guide); operators size
            //   termination grace periods from this table.

            // Given / When: the guide as checked in; Then: every row matches its constant
            assertThat(defaultOf("Send dispatcher queue capacity (per class)"))
                .contains("`${SendDispatcher.DEFAULT_QUEUE_CAPACITY}`")
            assertThat(defaultOf("Send dispatcher drain on stop (parallel)"))
                .contains("`${SendDispatcher.DEFAULT_DRAIN_TIMEOUT_MS / 1000} s` drain")
            assertThat(defaultOf("Fallback dispatcher queue capacity"))
                .contains("`${FallbackDispatcher.DEFAULT_QUEUE_CAPACITY}`")
            assertThat(defaultOf("Fallback dispatcher shutdown timeout"))
                .contains("`${FallbackDispatcher.DEFAULT_SHUTDOWN_TIMEOUT_MS / 1000} s` drain")
                .contains("`${BoundedWorkerDispatcher.INTERRUPT_GRACE_MS / 1000.0} s` interrupt grace")
            assertThat(defaultOf("Producer close timeout"))
                .contains("`${ProducerRegistry.DEFAULT_CLOSE_TIMEOUT.toSeconds()} s`")
        }

        @Test
        fun `should state the circuit-breaker and throttle defaults exactly as the configuration defines them`() {
            // Given: the production breaker configuration
            val config = ResilientMessageSender.defaultCircuitBreakerConfig()

            // Then: both guide tables (the breaker section and the quick reference) match it
            assertThat(breakerDefaultOf("failureRateThreshold")).isEqualTo("`${config.failureRateThreshold.toInt()}%`")
            assertThat(breakerDefaultOf("slidingWindowSize")).isEqualTo("`${config.slidingWindowSize}` calls")
            assertThat(breakerDefaultOf("minimumNumberOfCalls")).isEqualTo("`${config.minimumNumberOfCalls}`")
            assertThat(breakerDefaultOf("waitDurationInOpenState"))
                .isEqualTo("`${config.waitIntervalFunctionInOpenState.apply(1) / 1000}s`")
            assertThat(breakerDefaultOf("permittedNumberOfCallsInHalfOpenState"))
                .isEqualTo("`${config.permittedNumberOfCallsInHalfOpenState}`")

            assertThat(defaultOf("Circuit breaker: failure-rate threshold")).isEqualTo("`${config.failureRateThreshold.toInt()}%`")
            assertThat(defaultOf("Circuit breaker: sliding window / min calls"))
                .isEqualTo("`${config.slidingWindowSize}` / `${config.minimumNumberOfCalls}`")
            assertThat(defaultOf("Circuit breaker: open-state wait"))
                .isEqualTo("`${config.waitIntervalFunctionInOpenState.apply(1) / 1000}s`")
            assertThat(defaultOf("Circuit breaker: half-open permitted calls"))
                .isEqualTo("`${config.permittedNumberOfCallsInHalfOpenState}`")
            assertThat(defaultOf("Half-open probe gap"))
                .isEqualTo("`${ResilientMessageSender.DEFAULT_HALF_OPEN_PROBE_GAP.toMillis()} ms`")
            assertThat(defaultOf("Partitioning key MDC source")).isEqualTo("`${MessageEnricher.TRACE_ID_MDC_KEY}`")
        }
    }

    @Nested
    inner class `Metrics overview` {
        private val overview: List<String> =
            Files.readAllLines(Path.of("docs", "metrics", "metrics-overview.md"))

        /** Metric name to its documented tag list, from the counters/timers/gauges tables. */
        private fun documentedTags(): Map<String, List<String>> =
            overview
                .filter { it.startsWith("| `kafka.appender.") }
                .associate { row ->
                    val cells = row.split("|").map { it.trim() }
                    cells[1].trim('`') to Regex("`([a-z.]+)`").findAll(cells[2]).map { it.groupValues[1] }.toList()
                }

        /** Series count of the cardinality-table row for [shortName] (the leading integer of the cell). */
        private fun documentedSeries(shortName: String): Int {
            val row =
                overview.singleOrNull { it.startsWith("| `$shortName`") }
                    ?: error("cardinality table row '$shortName' not found exactly once in the metrics overview")
            return Regex("\\d+").find(row.split("|")[2])?.value?.toInt() ?: error("no series count in row '$shortName'")
        }

        @Test
        fun `should list exactly the metrics the implementation registers with exactly their tags`() {
            // What is to be tested? Whether the operator-facing metrics
            //   overview names the same metrics, with the same tag keys,
            //   that MicrometerKafkaAppenderMetrics registers - the
            //   inventory the implementation KDoc no longer mirrors by
            //   hand but delegates to this check.
            // How will the test case be deemed successful and why? Successful
            //   if the set of `kafka.appender.*` rows equals the METRIC_*
            //   constants and every row's tag list equals the tags the
            //   implementation attaches (appender on all, plus the
            //   per-metric dimensions).
            // Why is it important to test this test case? The first
            //   comment audit's only Critical was this inventory being
            //   wrong in prose; a mirror kept by "update together" drifts,
            //   a mirror kept by a test cannot.

            // Given: the tags the implementation attaches, per metric
            val appender = MicrometerKafkaAppenderMetrics.TAG_APPENDER
            val topicClass = MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS
            val expected =
                mapOf(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_ACCEPTED to listOf(appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_DISPATCHED to listOf(appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK to
                        listOf(appender, topicClass, MicrometerKafkaAppenderMetrics.TAG_REASON),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION to
                        listOf(appender, topicClass, MicrometerKafkaAppenderMetrics.TAG_OUTCOME),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED to listOf(appender),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE to listOf(appender),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY to listOf(appender),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE to listOf(appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY to listOf(appender, topicClass),
                )

            // When / Then: the overview's inventory equals it, row by row
            assertThat(documentedTags()).containsExactlyInAnyOrderEntriesOf(expected)
        }

        @Test
        fun `should state the series counts that the enum sizes imply`() {
            // Given: the enum sizes the pre-resolved meter tables are built from
            val classes = TopicClass.entries.size
            val reasons = KafkaAppenderMetrics.FallbackReason.entries.size
            val outcomes = KafkaAppenderMetrics.SendOutcome.entries.size

            // When / Then: every cardinality row and the total follow from them
            assertThat(documentedSeries("events.accepted")).isEqualTo(classes)
            assertThat(documentedSeries("events.dispatched")).isEqualTo(classes)
            assertThat(documentedSeries("events.fallback")).isEqualTo(classes * reasons)
            assertThat(documentedSeries("send.duration")).isEqualTo(classes * outcomes)
            assertThat(documentedSeries("fallback.dropped")).isEqualTo(1)
            assertThat(documentedSeries("fallback.queue.size")).isEqualTo(1)
            assertThat(documentedSeries("fallback.queue.capacity")).isEqualTo(1)
            assertThat(documentedSeries("send.queue.size")).isEqualTo(classes)
            assertThat(documentedSeries("send.queue.capacity")).isEqualTo(classes)
            val total = classes * 2 + classes * reasons + classes * outcomes + 3 + classes * 2
            val totalRow = overview.single { it.startsWith("| **Total**") }
            assertThat(totalRow).contains("**$total**")
        }
    }
}
