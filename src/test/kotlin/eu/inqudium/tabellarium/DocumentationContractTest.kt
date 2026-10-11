package eu.inqudium.tabellarium

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerConfig
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
 * The metric-inventory check does the same for the metrics guide
 * (docs/assessment/COMMENT_AUDIT-2026-09-07T21-20-00.md, finding 5).
 *
 * Runs from the module root (Surefire's working directory), where the
 * guide lives under `docs/`.
 */
class DocumentationContractTest {
    private val guide: List<String> =
        Files.readAllLines(Path.of("docs", "config", "kafka-appender-config-guide.md"))

    /**
     * A millisecond constant as the guide writes seconds: `1000` is
     * `1`, `500` is `0.5`, `2500` is `2.5`. Rationale: an integer
     * division would still match a stale "2 s" for a constant changed to
     * 2500 ms (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 20).
     */
    private fun seconds(millis: Long): String = (millis / 1000.0).toString().removeSuffix(".0")

    /** The "Default" cell of the defaults-table row whose "Setting" cell starts with [setting]. */
    private fun defaultOf(setting: String): String = defaultCell(rowStart = "| $setting", table = "defaults table")

    /** The "Default" cell of the circuit-breaker table row for [property]. */
    private fun breakerDefaultOf(property: String): String = defaultCell(rowStart = "| `$property`", table = "circuit-breaker table")

    /** The "Default" (second) cell of the one guide row starting with [rowStart]; [table] names it in the failure message. */
    private fun defaultCell(
        rowStart: String,
        table: String,
    ): String {
        val row =
            guide.singleOrNull { it.startsWith(rowStart) }
                ?: error("$table row '$rowStart' not found exactly once in the configuration guide")
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
                .contains("`${seconds(SendDispatcher.DEFAULT_DRAIN_TIMEOUT_MS)} s` drain")
                .contains("`${seconds(BoundedWorkerDispatcher.INTERRUPT_GRACE_MS)} s` interrupt grace")
            assertThat(defaultOf("Fallback dispatcher queue capacity"))
                .contains("`${FallbackDispatcher.DEFAULT_QUEUE_CAPACITY}`")
            assertThat(defaultOf("Fallback dispatcher shutdown timeout"))
                .contains("`${seconds(FallbackDispatcher.DEFAULT_DRAIN_TIMEOUT_MS)} s` drain")
                .contains("`${seconds(BoundedWorkerDispatcher.INTERRUPT_GRACE_MS)} s` interrupt grace")
            assertThat(defaultOf("Producer close timeout"))
                .contains("`${ProducerRegistry.DEFAULT_CLOSE_TIMEOUT.toSeconds()} s`")
        }

        @Test
        fun `should state the circuit-breaker and throttle defaults exactly as the configuration defines them`() {
            // What is to be tested? Whether both places the guide states the breaker
            //   defaults - the circuit-breaker table and the defaults quick reference -
            //   plus the half-open probe gap and the MDC key carry the values that
            //   defaultCircuitBreakerConfig() and the constants actually use.
            // How will the test case be deemed successful and why? Successful if every
            //   Default cell equals the value formatted from the live configuration
            //   (threshold, window, min calls, open-state wait, half-open permits);
            //   equality rather than contains, so a stale extra number fails too.
            // Why is it important to test this test case? Operators size fallback
            //   capacity and alert thresholds from these numbers; a table that lags a
            //   tuning change would send them diagnosing behavior the code no longer
            //   has, and two tables can drift from each other as well as from the code.

            // Given: the production breaker configuration
            val config = ResilientMessageSender.defaultCircuitBreakerConfig()

            // Then: both guide tables (the breaker section and the quick reference) match it
            assertThat(breakerDefaultOf("failureRateThreshold")).isEqualTo("`${config.failureRateThreshold.toInt()}%`")
            assertThat(breakerDefaultOf("slidingWindowSize")).isEqualTo("`${config.slidingWindowSize}` calls")
            assertThat(breakerDefaultOf("minimumNumberOfCalls")).isEqualTo("`${config.minimumNumberOfCalls}`")
            assertThat(breakerDefaultOf("waitDurationInOpenState"))
                .isEqualTo("`${seconds(config.waitIntervalFunctionInOpenState.apply(1))}s`")
            assertThat(breakerDefaultOf("permittedNumberOfCallsInHalfOpenState"))
                .isEqualTo("`${config.permittedNumberOfCallsInHalfOpenState}`")

            assertThat(defaultOf("Circuit breaker: failure-rate threshold")).isEqualTo("`${config.failureRateThreshold.toInt()}%`")
            assertThat(defaultOf("Circuit breaker: sliding window / min calls"))
                .isEqualTo("`${config.slidingWindowSize}` / `${config.minimumNumberOfCalls}`")
            assertThat(defaultOf("Circuit breaker: open-state wait"))
                .isEqualTo("`${seconds(config.waitIntervalFunctionInOpenState.apply(1))}s`")
            assertThat(defaultOf("Circuit breaker: half-open permitted calls"))
                .isEqualTo("`${config.permittedNumberOfCallsInHalfOpenState}`")
            assertThat(defaultOf("Half-open probe gap"))
                .isEqualTo("`${ResilientMessageSender.DEFAULT_HALF_OPEN_PROBE_GAP.toMillis()} ms`")
            assertThat(defaultOf("Partitioning key MDC source")).isEqualTo("`${MessageEnricher.TRACE_ID_MDC_KEY}`")
        }

        @Test
        fun `should state the key bound, the class caps, the client id pattern and the fallback class exactly as the code defines them`() {
            // What is to be tested? Whether the remaining "code" rows of the
            //   quick reference - the partitioning-key bound, the per-class
            //   max.block.ms caps, the derived client.id pattern, the fallback
            //   class for unmapped topics - and the <sendQueueCapacity>
            //   default of the element table carry the values the code uses.
            // How will the test case be deemed successful and why? Successful
            //   if each cell equals the value rendered from the constant or
            //   enum (the caps grouped as the row writes them: the common cap
            //   first, every exception named), the fallback class equals a
            //   fresh TopicMappingConfig's default, and the client.id pattern,
            //   with <component> and <topicclass> substituted, equals the
            //   client.id a started appender actually hands its producer
            //   factory - that pins the literal prefix in the transport as
            //   well as the per-class suffix.
            // Why is it important to test this test case? These rows were the
            //   unpinned half of the "a changed constant fails mvn verify"
            //   promise: MAX_PARTITIONING_KEY_LENGTH = 64 or a changed cap
            //   left the table stale with the build green
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 11).

            // Given: the caps grouped as the row writes them
            val capsByValue = TopicClass.entries.groupBy { it.maxBlockMsCap }
            val commonCap = capsByValue.maxBy { it.value.size }.key
            val capCell =
                "`$commonCap ms`" +
                    capsByValue
                        .filterKeys { it != commonCap }
                        .entries
                        .joinToString("") { (cap, classes) -> "; `$cap ms` for ${classes.joinToString(", ") { "`${it.name}`" }}" }

            // And: the client.id a started appender hands its producer factory
            val factory = RecordingProducerFactory()
            val appender = newTestAppender(component = "doc-contract", producerFactory = factory)
            appender.start()
            try {
                check(appender.isStarted) { "the documentation-contract appender did not start" }
            } finally {
                appender.stop()
            }
            val actualClientId = factory.createdWithProperties.single().getValue(ProducerConfig.CLIENT_ID_CONFIG)

            // When / Then: every row matches its constant
            assertThat(defaultOf("Partitioning key maximum length"))
                .startsWith("`${MessageEnricher.MAX_PARTITIONING_KEY_LENGTH}` characters")
            assertThat(defaultOf("`max.block.ms` cap (per class)")).isEqualTo(capCell)
            assertThat(defaultOf("Fallback class for unmapped topics")).isEqualTo("`${TopicMappingConfig().defaultTopicClass}`")
            val documentedClientId = defaultOf("`client.id` (unless set by operator)").trim('`')
            assertThat(documentedClientId).contains("<component>").contains("<topicclass>")
            assertThat(documentedClientId.replace("<component>", "doc-contract").replace("<topicclass>", TopicClass.TECHNICAL.tag))
                .isEqualTo(actualClientId)
            val elementRow = guide.single { it.startsWith("| `<sendQueueCapacity>`") }
            assertThat(elementRow.split("|")[3]).contains("default `${SendDispatcher.DEFAULT_QUEUE_CAPACITY}`")
        }
    }

    @Nested
    inner class `Per-class tables` {
        private val metricsGuide: List<String> =
            Files.readAllLines(Path.of("docs", "metrics", "metrics-guide.md"))

        /** A table cell without its footnote marks and surrounding whitespace. */
        private fun plain(cell: String): String = cell.replace(Regex("[¹²³]"), "").trim()

        /** `v` as the tables write a value, or the em dash for an absent one. */
        private fun valueCell(value: String?): String = value?.let { "`$it`" } ?: "—"

        @Test
        fun `should list the mandatory overrides of every class exactly as TopicClass defines them`() {
            // What is to be tested? Whether the "Mandatory overrides" column of
            //   the guide's class table (A.3) names, per class, exactly the
            //   entries of TopicClass.mandatoryOverrides.
            // How will the test case be deemed successful and why? Successful
            //   if each class's cell equals the entries rendered as
            //   `key=value` in declaration order, or the literal *(none)* for
            //   a class without mandates - equality, so a dropped or added
            //   mandate fails.
            // Why is it important to test this test case? The mandates are
            //   the compliance promise of the class table; an operator reads
            //   acks=all for AUDIT here, not in the enum.

            // Given: the A.3 table rows
            val rows = subsection(guide, "### A.3 The four classes").filter { it.startsWith("| `") }

            // When / Then: one row per class, its last cell rendered from the enum
            assertThat(rows.map { plain(it.split("|")[1]).trim('`') })
                .containsExactlyElementsOf(TopicClass.entries.map { it.name })
            TopicClass.entries.forEach { topicClass ->
                val row = rows.single { it.startsWith("| `${topicClass.name}`") }
                val expected =
                    topicClass.mandatoryOverrides.entries
                        .joinToString(", ") { (key, value) -> "`$key=$value`" }
                        .ifEmpty { "*(none)*" }
                assertThat(plain(row.split("|")[5])).`as`(topicClass.name).isEqualTo(expected)
            }
        }

        @Test
        fun `should render the default-override table exactly from TopicClass`() {
            // What is to be tested? Whether the per-class default-override
            //   table (A.4) lists exactly the properties any class defaults
            //   or mandates, with each cell equal to that class's default or
            //   an em dash where the class has none.
            // How will the test case be deemed successful and why? Successful
            //   if the row set equals the union of the enum's default and
            //   mandatory keys and every cell equals the enum's value for
            //   (property, class) with footnote marks ignored - so changing
            //   PERFORMANCE linger.ms from 100 to 50, or dropping the AUDIT
            //   retries default, fails the build until the table follows.
            // Why is it important to test this test case? Operators size
            //   latency expectations and batch memory from this table; no
            //   test pinned any of the TopicClass tables before
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 11).

            // Given: the A.4 defaults table, header first
            val table =
                subsection(guide, "### A.4 Property overrides per class")
                    .dropWhile { !it.startsWith("| Property") }
                    .takeWhile { it.startsWith("|") }
            val classes =
                table
                    .first()
                    .split("|")
                    .drop(2)
                    .dropLast(1)
                    .map { TopicClass.valueOf(it.trim()) }
            val rows =
                table.filter { it.startsWith("| `") }.associate { row ->
                    val cells = row.split("|").drop(1).dropLast(1)
                    cells[0].trim().trim('`') to cells.drop(1).map(::plain)
                }

            // When: the expected row set is rendered from the enum
            val expectedKeys =
                TopicClass.entries.flatMap { it.defaultOverrides.keys + it.mandatoryOverrides.keys }.distinct()

            // Then: same columns, same rows, same cells
            assertThat(classes).containsExactlyElementsOf(TopicClass.entries)
            assertThat(rows.keys).containsExactlyInAnyOrderElementsOf(expectedKeys)
            rows.forEach { (property, cells) ->
                assertThat(cells).`as`(property).containsExactlyElementsOf(classes.map { valueCell(it.defaultOverrides[property]) })
            }
        }

        @Test
        fun `should state the max block default of every class as its cap`() {
            // What is to be tested? Whether the guide's sentence "The
            //   max.block.ms default of each class is also its cap" holds for
            //   every class of the enum.
            // How will the test case be deemed successful and why? Successful
            //   if each class's max.block.ms default equals its maxBlockMsCap;
            //   the code invariant itself (default at or below the cap) is
            //   pinned in TopicClassTest, this is the documentation's stronger
            //   claim.
            // Why is it important to test this test case? A class whose
            //   default drifted below its cap would make the sentence wrong,
            //   one above it would record a spurious override violation on
            //   every start.

            // Given / When / Then
            assertThat(guide).anyMatch { it.contains("default of each class is also its **cap**") }
            TopicClass.entries.forEach { topicClass ->
                assertThat(topicClass.defaultOverrides[ProducerConfig.MAX_BLOCK_MS_CONFIG])
                    .`as`(topicClass.name)
                    .isEqualTo(topicClass.maxBlockMsCap.toString())
            }
        }

        @Test
        fun `should render the metrics guide's per-class floor table exactly from TopicClass`() {
            // What is to be tested? Whether the "Each class has its own
            //   floor" table of the metrics guide states, per class tag, the
            //   linger.ms default, the effective acks (mandatory first, with
            //   "idempotent" where idempotence is mandated) and the
            //   max.block.ms default of the enum.
            // How will the test case be deemed successful and why? Successful
            //   if the row set equals the class tags and the three value
            //   cells of every row equal the values rendered from the enum.
            // Why is it important to test this test case? Operators read a
            //   p99 of ~100 ms on performance as "the configured batching
            //   delay" because this table says linger.ms=100; a changed
            //   default would turn that diagnosis wrong.

            // Given: the floor table
            val rows =
                metricsGuide
                    .dropWhile { !it.contains("Each class has its own floor") }
                    .dropWhile { !it.startsWith("| `") }
                    .takeWhile { it.startsWith("| `") }
                    .associate { row ->
                        val cells = row.split("|").drop(1).map { it.trim() }
                        cells[0].trim('`') to cells.subList(1, 4)
                    }

            // When / Then
            assertThat(rows.keys).containsExactlyInAnyOrderElementsOf(TopicClass.entries.map { it.tag })
            TopicClass.entries.forEach { topicClass ->
                val acks =
                    topicClass.mandatoryOverrides[ProducerConfig.ACKS_CONFIG]
                        ?: topicClass.defaultOverrides.getValue(ProducerConfig.ACKS_CONFIG)
                val idempotent = topicClass.mandatoryOverrides[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] == "true"
                assertThat(rows.getValue(topicClass.tag))
                    .`as`(topicClass.tag)
                    .containsExactly(
                        topicClass.defaultOverrides.getValue(ProducerConfig.LINGER_MS_CONFIG),
                        "`$acks`" + if (idempotent) ", idempotent" else "",
                        topicClass.defaultOverrides.getValue(ProducerConfig.MAX_BLOCK_MS_CONFIG),
                    )
            }
        }
    }

    @Nested
    inner class `Metrics guide` {
        private val overview: List<String> =
            Files.readAllLines(Path.of("docs", "metrics", "metrics-guide.md"))

        /**
         * Metric name to its documented tag list, from the tables of the
         * "Metric inventory" section - the appender's own meters, not the
         * circuit-breaker and producer bridges documented further down.
         */
        private fun documentedTags(): Map<String, List<String>> =
            section(overview, "## Metric inventory")
                .filter { it.startsWith("| `kafka.appender.") }
                .associate { row ->
                    val cells = row.split("|").map { it.trim() }
                    cells[1].trim('`') to Regex("`([a-z.]+)`").findAll(cells[2]).map { it.groupValues[1] }.toList()
                }

        /** Series count of the cardinality-table row for [shortName] (the leading integer of the cell). */
        private fun documentedSeries(shortName: String): Int {
            val row =
                overview.singleOrNull { it.startsWith("| `$shortName`") }
                    ?: error("cardinality table row '$shortName' not found exactly once in the metrics guide")
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
                    MicrometerKafkaAppenderMetrics.METRIC_EVENT_SIZE to listOf(appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_DROPPED to listOf(appender),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_SIZE to listOf(appender),
                    MicrometerKafkaAppenderMetrics.METRIC_FALLBACK_QUEUE_CAPACITY to listOf(appender),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_SIZE to listOf(appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_QUEUE_CAPACITY to listOf(appender, topicClass),
                    MicrometerKafkaAppenderMetrics.METRIC_INFO to
                        listOf(
                            appender,
                            MicrometerKafkaAppenderMetrics.TAG_FALLBACK,
                            MicrometerKafkaAppenderMetrics.TAG_FALLBACK_TYPE,
                            MicrometerKafkaAppenderMetrics.TAG_APPENDER_VERSION,
                            MicrometerKafkaAppenderMetrics.TAG_KAFKA_CLIENT_VERSION,
                            MicrometerKafkaAppenderMetrics.TAG_DEBUG,
                        ),
                    MicrometerKafkaAppenderMetrics.METRIC_TOPIC_INFO to
                        listOf(
                            appender,
                            topicClass,
                            MicrometerKafkaAppenderMetrics.TAG_TOPIC,
                            MicrometerKafkaAppenderMetrics.TAG_ACKS,
                            MicrometerKafkaAppenderMetrics.TAG_LINGER_MS,
                        ),
                )

            // When / Then: the overview's inventory equals it, row by row
            assertThat(documentedTags()).containsExactlyInAnyOrderEntriesOf(expected)
        }

        /**
         * The documented values of the tag named [tag], from the
         * `### \`tag\`` subsection of "Tag values": the first cell of each
         * table row, plus the "N possible values" count of the heading.
         */
        private fun documentedTagValues(tag: String): Pair<Int, Set<String>> {
            val heading = "### `$tag`"
            val subsection =
                section(overview, "## Tag values")
                    .dropWhile { !it.startsWith(heading) }
                    .also { require(it.isNotEmpty()) { "subsection '$heading' not found in the metrics guide" } }
            val count =
                Regex("(\\d+) possible values")
                    .find(subsection.first())
                    ?.groupValues
                    ?.get(1)
                    ?.toInt()
                    ?: error("no 'N possible values' count in heading '${subsection.first()}'")
            val values =
                subsection
                    .drop(1)
                    .takeWhile { !it.startsWith("### ") }
                    .filter { it.startsWith("| `") }
                    .map { it.split("|")[1].trim().trim('`') }
                    .toSet()
            return count to values
        }

        @Test
        fun `should list exactly the tag values the enums define`() {
            // What is to be tested? Whether the guide's "Tag values" tables for
            //   topic.class, reason and outcome enumerate exactly the tag
            //   strings of TopicClass, FallbackReason and SendOutcome, and
            //   whether each heading's "N possible values" is the enum size.
            // How will the test case be deemed successful and why? Successful
            //   if each documented value set equals the enum's tag set and
            //   each count equals the enum size. The inventory check pins
            //   names and tag keys only, so a renamed or added tag VALUE left
            //   the table stale with the build green
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 20).
            // Why is it important to test this test case? Alert rules and
            //   dashboard filters select on these literal values; a value the
            //   guide spells differently from the code selects nothing.

            // Given: the tag values the code emits
            val expected =
                mapOf(
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to TopicClass.entries.map { it.tag },
                    MicrometerKafkaAppenderMetrics.TAG_REASON to KafkaAppenderMetrics.FallbackReason.entries.map { it.tag },
                    MicrometerKafkaAppenderMetrics.TAG_OUTCOME to KafkaAppenderMetrics.SendOutcome.entries.map { it.tag },
                )

            // When / Then: every subsection lists exactly them, and counts them right
            expected.forEach { (tag, values) ->
                val (count, documented) = documentedTagValues(tag)
                assertThat(documented).`as`("values of %s", tag).containsExactlyInAnyOrderElementsOf(values)
                assertThat(count).`as`("count of %s", tag).isEqualTo(values.size)
            }
        }

        @Test
        fun `should state the series counts that the enum sizes imply`() {
            // What is to be tested? Whether the cardinality table's per-metric series
            //   counts and its total are the products of the enum sizes the pre-resolved
            //   meter tables are built from (classes, classes x reasons, classes x
            //   outcomes, 1 for the fallback-dispatcher meters and the appender info;
            //   the topic info scales with the configuration, not with an enum).
            // How will the test case be deemed successful and why? Successful if each
            //   row's leading integer equals the formula for that metric and the bold
            //   total equals their sum; adding an enum constant without updating the
            //   table fails the build.
            // Why is it important to test this test case? The series count is the
            //   number operators multiply by their appender count to budget registry
            //   cardinality; a stale figure understates the cost of every new
            //   FallbackReason.

            // Given: the enum sizes the pre-resolved meter tables are built from
            val classes = TopicClass.entries.size
            val reasons = KafkaAppenderMetrics.FallbackReason.entries.size
            val outcomes = KafkaAppenderMetrics.SendOutcome.entries.size

            // When / Then: every cardinality row and the total follow from them
            assertThat(documentedSeries("events.accepted")).isEqualTo(classes)
            assertThat(documentedSeries("events.dispatched")).isEqualTo(classes)
            assertThat(documentedSeries("events.fallback")).isEqualTo(classes * reasons)
            assertThat(documentedSeries("send.duration")).isEqualTo(classes * outcomes)
            assertThat(documentedSeries("event.size")).isEqualTo(classes)
            assertThat(documentedSeries("fallback.dropped")).isEqualTo(1)
            assertThat(documentedSeries("fallback.queue.size")).isEqualTo(1)
            assertThat(documentedSeries("fallback.queue.capacity")).isEqualTo(1)
            assertThat(documentedSeries("send.queue.size")).isEqualTo(classes)
            assertThat(documentedSeries("send.queue.capacity")).isEqualTo(classes)
            assertThat(documentedSeries("info")).isEqualTo(1)
            val total = classes * 2 + classes * reasons + classes * outcomes + classes + 3 + classes * 2 + 1
            val totalRow = overview.single { it.startsWith("| **Total**") }
            assertThat(totalRow).contains("**$total**")
        }

        @Test
        fun `should list exactly the circuit-breaker meters the binder registers with exactly their tags`() {
            // What is to be tested? Whether the guide's circuit-breaker table
            //   names the same kafka.appender.circuitbreaker.* meters, with
            //   the same type and tag keys, that a bound appender registers.
            // How will the test case be deemed successful and why? Successful
            //   if the table's rows equal the live breaker meters by name,
            //   their Type cell starts with the live meter type and their tag
            //   list equals the live tag keys - read from a real binding, so
            //   neither a renamed nor a re-tagged meter can drift unseen.
            // Why is it important to test this test case? Operators build
            //   breaker dashboards and alerts from this table; the
            //   2026-10-10 namespace rename showed how quickly prose about
            //   these names drifts (DEFECT_ANALYSIS-2026-10-10T13-00-00,
            //   finding 15).

            // Given: the breaker meters a bound appender registers
            val live = liveInventory().filterKeys { it.startsWith("${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.") }

            // When: the guide's breaker table is read
            val documented =
                section(overview, "## Circuit breaker metrics")
                    .filter { it.startsWith("| `kafka.appender.circuitbreaker.") }
                    .associate { row ->
                        val cells = row.split("|").map { it.trim() }
                        cells[1].trim('`') to (cells[3].substringBefore(':').substringBefore(' ') to tagsIn(cells[2]))
                    }

            // Then: both sides non-empty first - two empty sets would compare equal
            assertThat(live).isNotEmpty
            assertThat(documented).isNotEmpty
            assertThat(documented.keys).containsExactlyInAnyOrderElementsOf(live.keys)
            documented.forEach { (name, typeAndTags) ->
                assertThat(typeAndTags).`as`(name).isEqualTo(live.getValue(name))
            }
        }

        @Test
        fun `should name only producer meters the Kafka client binding actually publishes`() {
            // What is to be tested? Whether every example row of the guide's
            //   Kafka producer bridge table names a meter that the renaming
            //   mirror publishes for a real producer.
            // How will the test case be deemed successful and why? Successful
            //   if each listed kafka.appender.producer.* name is among the
            //   meters a bound appender with real producers (unreachable
            //   broker: the producer-level metrics exist from construction)
            //   registers. The table is a curated excerpt, so only its rows
            //   are checked, not completeness.
            // Why is it important to test this test case? A Kafka client
            //   upgrade that renames a metric, or a change of the mirror's
            //   rename rule, would leave the guide pointing operators at
            //   series that never appear.

            // Given: the producer meters a bound appender with real producers registers
            val live = liveInventory(realProducers = true).keys.filter { it.startsWith("kafka.appender.producer.") }
            assertThat(live).isNotEmpty

            // When: the guide's producer table is read
            val documented =
                section(overview, "## Kafka producer bridge")
                    .filter { it.startsWith("| `kafka.appender.producer.") }
                    .map { it.split("|")[1].trim().trim('`') }

            // Then
            assertThat(documented).isNotEmpty
            assertThat(live).containsAll(documented)
        }
    }

    @Nested
    inner class `Duplicated metric inventories` {
        /**
         * Name to (type, extra tags) from the rows of the first
         * `kafka.appender.*` table in [lines] after [heading] - the
         * README and configuration-guide copies of the inventory, which
         * leave out the `appender` tag every meter carries.
         */
        private fun inventoryTable(
            lines: List<String>,
            heading: String,
        ): Map<String, Pair<String, Set<String>>> =
            section(lines, heading)
                .dropWhile { !it.startsWith("| `kafka.appender.") }
                .takeWhile { it.startsWith("|") }
                .associate { row ->
                    val cells = row.split("|").map { it.trim() }
                    cells[1].trim('`') to (cells[2] to tagsIn(cells[3]))
                }

        /** The appender's own meters as the inventory tables state them: without the `appender` tag. */
        private fun liveOwnMeters(): Map<String, Pair<String, Set<String>>> =
            liveInventory()
                .filterKeys {
                    !it.startsWith("${MetricsBindings.CIRCUIT_BREAKER_PREFIX}.") &&
                        !it.startsWith("kafka.appender.producer.")
                }.mapValues { (_, typeAndTags) ->
                    typeAndTags.first to typeAndTags.second - MicrometerKafkaAppenderMetrics.TAG_APPENDER
                }

        @Test
        fun `should keep the README inventory equal to the registered meters`() {
            // What is to be tested? Whether the README's metric table lists
            //   exactly the appender's own meters with their type and their
            //   tags beyond `appender`.
            // How will the test case be deemed successful and why? Successful
            //   if the table equals the inventory read from a real binding,
            //   name by name, type by type and tag set by tag set.
            // Why is it important to test this test case? The README is the
            //   first page operators read; only the metrics guide's copy was
            //   checked, so this one could drift on every metric change
            //   (DEFECT_ANALYSIS-2026-10-10T13-00-00, finding 15).

            // Given: the README as checked in
            val readme = Files.readAllLines(Path.of("README.md"))

            // When / Then: its inventory equals the meters a real binding registers
            assertThat(inventoryTable(readme, "## Metrics")).containsExactlyInAnyOrderEntriesOf(liveOwnMeters())
        }

        @Test
        fun `should keep the configuration guide inventory equal to the registered meters`() {
            // What is to be tested? Whether the configuration guide's metric
            //   inventory lists exactly the appender's own meters with their
            //   type and their tags beyond `appender`.
            // How will the test case be deemed successful and why? Successful
            //   if the table equals the inventory read from a real binding.
            // Why is it important to test this test case? Same as for the
            //   README copy: an unchecked duplicate drifts.

            // Given / When / Then: the guide's inventory equals the meters a real binding registers
            assertThat(inventoryTable(guide, "### Metric inventory")).containsExactlyInAnyOrderEntriesOf(liveOwnMeters())
        }
    }

    private companion object {
        /** The backtick-quoted tag keys in a table cell. */
        fun tagsIn(cell: String): Set<String> = Regex("`([a-z._]+)`").findAll(cell).map { it.groupValues[1] }.toSet()

        /** The lines after the first line of [lines] starting with [heading], up to the next heading of any level. */
        fun subsection(
            lines: List<String>,
            heading: String,
        ): List<String> =
            lines
                .dropWhile { !it.startsWith(heading) }
                .also { require(it.isNotEmpty()) { "heading '$heading' not found" } }
                .drop(1)
                .takeWhile { !it.startsWith("#") }

        /** The lines after the first line of [lines] starting with [heading], up to the next `## ` heading. */
        fun section(
            lines: List<String>,
            heading: String,
        ): List<String> = lines.dropWhile { !it.startsWith(heading) }.drop(1).takeWhile { !it.startsWith("## ") }

        /** Documentation word for a Micrometer meter type. */
        fun typeWord(type: Meter.Type): String =
            when (type) {
                Meter.Type.COUNTER -> "Counter"
                Meter.Type.TIMER -> "Timer"
                Meter.Type.GAUGE -> "Gauge"
                Meter.Type.DISTRIBUTION_SUMMARY -> "Summary"
                else -> type.name
            }

        /**
         * Every `kafka.appender.*` meter a started, bound appender with a
         * fallback appender registers: name to (type, tag keys). With
         * [realProducers], real Kafka producers against an unreachable
         * broker, so the client binding has metrics to publish.
         */
        fun liveInventory(realProducers: Boolean = false): Map<String, Pair<String, Set<String>>> {
            val registry = SimpleMeterRegistry()
            val appender =
                newTestAppender(
                    component = "doc-contract",
                    cmdbId = "CMDB-DOC",
                    kafkaProducerProperties =
                        "${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=${if (realProducers) "localhost:1" else "test:9092"}",
                    topicMapping = TopicMappingConfig().apply { defaultTopic = "doc.topic" },
                    producerFactory = if (realProducers) ProducerFactory.default() else RecordingProducerFactory(),
                    fallback = RecordingAppender(),
                )
            appender.start()
            check(appender.isStarted) { "the documentation-contract appender did not start" }
            try {
                appender.bindMeterRegistry(registry)
                return registry.meters
                    .filter { it.id.name.startsWith("kafka.appender.") }
                    .groupBy { it.id.name }
                    .mapValues { (_, meters) ->
                        typeWord(meters.first().id.type) to meters.flatMapTo(HashSet()) { m -> m.id.tags.map { it.key } }
                    }
            } finally {
                appender.stop()
            }
        }
    }
}
