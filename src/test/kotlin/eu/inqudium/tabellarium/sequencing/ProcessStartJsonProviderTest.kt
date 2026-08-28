package eu.inqudium.tabellarium.sequencing

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.Context
import ch.qos.logback.core.ContextBase
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import eu.inqudium.tabellarium.sequencing.jackson3.ProcessStartJsonProvider
import eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import tools.jackson.databind.json.JsonMapper
import java.io.StringWriter
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class ProcessStartJsonProviderTest {
    /** Parses the emitted JSON; the assertions below are about JSON types, so a tree is needed. */
    private val mapper = ObjectMapper()

    /**
     * Writes it. The providers under test take the encoder's own generator, which since
     * logstash-logback-encoder 9.0 is a Jackson 3 one - unrelated to the Jackson 2 tree
     * model used for the assertions.
     */
    private val jackson3Mapper = JsonMapper.builder().build()

    /**
     * Fixed reference point: 2026-07-09T08:14:22.317Z. Chosen with a
     * non-zero millisecond component so that a formatter which drops
     * sub-second precision is detected.
     */
    private val startMillis: Long = Instant.parse("2026-07-09T08:14:22.317Z").toEpochMilli()

    private fun newContext(): Context = ContextBase()

    private fun eventAt(timestampMillis: Long): ILoggingEvent = LoggingEvent().apply { timeStamp = timestampMillis }

    /**
     * Runs the provider against one event and returns the parsed JSON
     * object. Parsing rather than string-comparing is the whole point:
     * the assertions below are about JSON *types*, and a string
     * comparison cannot distinguish `1234` from `"1234"`.
     */
    private fun encode(
        provider: ProcessStartJsonProvider,
        event: ILoggingEvent,
    ): JsonNode {
        val writer = StringWriter()
        jackson3Mapper.createGenerator(writer).use { generator ->
            generator.writeStartObject()
            provider.writeTo(generator, event)
            generator.writeEndObject()
        }
        return mapper.readTree(writer.toString())
    }

    private fun startedProvider(
        reference: Long = startMillis,
        configure: ProcessStartJsonProvider.() -> Unit = {},
    ): ProcessStartJsonProvider =
        ProcessStartJsonProvider { reference }.apply {
            context = newContext()
            configure()
            start()
        }

    @Nested
    @DisplayName("JSON types")
    inner class JsonTypes {
        /**
         * What is tested: that the uptime lands in the JSON document as an
         * unquoted number.
         *
         * Why this is the success criterion: Elasticsearch's dynamic
         * mapping infers `long` from a JSON number and `text`/`keyword`
         * from a JSON string. Only the former supports `date_histogram`,
         * `range` queries and the `derivative` pipeline aggregation.
         *
         * Why it matters: a string here produces no error anywhere. The
         * document indexes, the field appears in Kibana, and every
         * aggregation over it silently returns an empty result. The defect
         * would surface during a production deployment, which is the one
         * moment this provider exists to support.
         */
        @Test
        fun `should write the uptime as a native JSON number`() {
            // Given
            val provider = startedProvider()
            val event = eventAt(startMillis + 128_340L)

            // When
            val json = encode(provider, event)

            // Then
            assertThat(json.get("process_uptime_ms").isNumber).isTrue()
            assertThat(json.get("process_uptime_ms").asLong()).isEqualTo(128_340L)
        }

        /**
         * What is tested: that the start instant lands as a quoted string.
         *
         * Why this is the success criterion: where no index template
         * applies -- a catch-all data stream, for instance --
         * `date_detection` infers `date` from an ISO-8601 string, but
         * infers `long` from a number.
         *
         * Why it matters: this is the exact inverse of the rule for the
         * uptime field. The two fields must not be "harmonised". A start
         * instant stored as `long` renders in Kibana as roughly 1.78
         * trillion rather than as a date.
         */
        @Test
        fun `should write the start instant as a JSON string`() {
            // Given
            val provider = startedProvider()
            val event = eventAt(startMillis)

            // When
            val json = encode(provider, event)

            // Then
            assertThat(json.get("process_start").isTextual).isTrue()
            assertThat(json.get("process_start").asText()).isEqualTo("2026-07-09T08:14:22.317Z")
        }

        @Test
        fun `should format the start instant in UTC with millisecond precision`() {
            // Given
            val provider = startedProvider()

            // When
            val json = encode(provider, eventAt(startMillis))

            // Then
            assertThat(json.get("process_start").asText())
                .endsWith("Z")
                .isEqualTo(Instant.ofEpochMilli(startMillis).toString())
        }
    }

    @Nested
    @DisplayName("Uptime derivation")
    inner class UptimeDerivation {
        /**
         * What is tested: that the uptime is derived from the event's own
         * creation timestamp, not from a clock read at encode time.
         *
         * Why this is the success criterion: two events encoded by the same
         * provider instance, carrying timestamps 5000 ms apart, must yield
         * uptimes exactly 5000 ms apart -- regardless of how much wall-clock
         * time elapses between the two `writeTo` calls.
         *
         * Why it matters: the provider runs on the AsyncAppender's worker
         * thread. An event may sit in the queue for seconds before being
         * encoded, and the queue backs up hardest during a deployment. A
         * clock read in `writeTo` would inflate the uptime by exactly the
         * queue latency, corrupting the measurement precisely when it is
         * needed.
         */
        @Test
        fun `should derive the uptime from the event timestamp and not from encode time`() {
            // Given
            val provider = startedProvider()
            val early = eventAt(startMillis + 1_000L)
            val late = eventAt(startMillis + 6_000L)

            // When
            val earlyUptime = encode(provider, early).get("process_uptime_ms").asLong()
            val lateUptime = encode(provider, late).get("process_uptime_ms").asLong()

            // Then
            assertThat(earlyUptime).isEqualTo(1_000L)
            assertThat(lateUptime).isEqualTo(6_000L)
            assertThat(lateUptime - earlyUptime).isEqualTo(5_000L)
        }

        @Test
        fun `should write a zero uptime for an event created at the start instant`() {
            // Given
            val provider = startedProvider()

            // When
            val json = encode(provider, eventAt(startMillis))

            // Then
            assertThat(json.get("process_uptime_ms").asLong()).isZero()
        }

        /**
         * What is tested: that an event predating the resolved start instant
         * yields a negative uptime rather than a clamped zero.
         *
         * Why this is the success criterion: the situation is a
         * misconfiguration, and the contract is to surface it.
         *
         * Why it matters: a clamped zero is indistinguishable from a
         * correctly-measured event at t=0. A negative value in Kibana is
         * unmistakable. Silent correction of impossible input is how
         * measurement systems come to be trusted while being wrong.
         */
        @Test
        fun `should write a negative uptime rather than clamping when the event predates the start instant`() {
            // Given
            val provider = startedProvider()
            val impossible = eventAt(startMillis - 250L)

            // When
            val json = encode(provider, impossible)

            // Then
            assertThat(json.get("process_uptime_ms").asLong()).isEqualTo(-250L)
        }

        /**
         * What is tested: that uptime arithmetic does not overflow at the
         * 32-bit boundary.
         *
         * Why this is the success criterion: 2^31 ms is 24.8 days. A pod
         * that has not been redeployed for a month must still report a
         * positive, monotonically increasing uptime.
         *
         * Why it matters: an `Int` here would wrap to a negative value
         * after 24.8 days. Long-running pods are precisely the ones whose
         * uptime someone eventually inspects, so the defect would hide
         * until it mattered.
         */
        @Test
        fun `should not overflow for uptimes beyond the 32-bit millisecond boundary`() {
            // Given
            val thirtyDaysMillis = 30L * 24L * 60L * 60L * 1_000L // 2_592_000_000 > Int.MAX_VALUE
            val provider = startedProvider()
            val event = eventAt(startMillis + thirtyDaysMillis)

            // When
            val json = encode(provider, event)

            // Then
            assertThat(thirtyDaysMillis).isGreaterThan(Int.MAX_VALUE.toLong())
            assertThat(json.get("process_uptime_ms").asLong()).isEqualTo(thirtyDaysMillis)
        }
    }

    @Nested
    @DisplayName("The start-time supplier")
    inner class TheStartTimeSupplier {
        /**
         * What is tested: that the supplier is consulted exactly once,
         * during start(), and never per event.
         *
         * Why this is the success criterion: the invocation counter reads 1
         * after start() and still reads 1 after three encoded events.
         *
         * Why it matters: the supplier is the one place in this class that
         * touches ambient state. Invoking it per event would reintroduce
         * exactly the clock read that the whole design exists to avoid, and
         * would put a management-bean call on the encoder thread for every
         * log line -- roughly 58 times a second on the service this was
         * written for.
         */
        @Test
        fun `should invoke the supplier exactly once during start`() {
            // Given
            val invocations = AtomicInteger()
            val provider =
                ProcessStartJsonProvider {
                    invocations.incrementAndGet()
                    startMillis
                }.apply { context = newContext() }

            // When
            provider.start()
            val afterStart = invocations.get()
            repeat(3) { encode(provider, eventAt(startMillis + it)) }

            // Then
            assertThat(afterStart).isEqualTo(1)
            assertThat(invocations.get()).isEqualTo(1)
        }

        /**
         * What is tested: that the supplier is not invoked at construction
         * time.
         *
         * Why this is the success criterion: the counter reads 0 after the
         * constructor returns.
         *
         * Why it matters: Joran constructs the provider before setting its
         * context and its properties. A supplier invoked from the
         * constructor could neither report an error through the Logback
         * status manager nor depend on anything Joran configures. Lifecycle
         * work belongs in start().
         */
        @Test
        fun `should not invoke the supplier at construction time`() {
            // Given
            val invocations = AtomicInteger()

            // When
            ProcessStartJsonProvider {
                invocations.incrementAndGet()
                startMillis
            }

            // Then
            assertThat(invocations.get()).isZero()
        }

        /**
         * What is tested: that the no-argument constructor exists and
         * resolves the JVM start time by itself.
         *
         * Why this is the success criterion: the provider starts, and an
         * event timestamped far in the future yields a positive uptime. A
         * stricter assertion is not available without reading a clock,
         * which the test must not do.
         *
         * Why it matters: this is the production path. Joran can only
         * instantiate via the no-arg constructor, which Kotlin emits only
         * because every constructor parameter carries a default. Removing
         * that default would break XML configuration with no compile-time
         * signal. Every other test here injects a reference point, so
         * without this one both the default supplier and the generated
         * constructor would be untested.
         */
        @Test
        fun `should resolve the JVM start time through the generated no-arg constructor`() {
            // Given
            val provider = ProcessStartJsonProvider().apply { context = newContext() }

            // When
            provider.start()
            val json = encode(provider, eventAt(Instant.parse("2099-01-01T00:00:00Z").toEpochMilli()))

            // Then
            assertThat(provider.isStarted).isTrue()
            assertThat(json.get("process_start").isTextual).isTrue()
            assertThat(json.get("process_uptime_ms").asLong()).isPositive()
        }
    }

    @Nested
    @DisplayName("Configuration")
    inner class Configuration {
        @Test
        fun `should use the configured field names`() {
            // Given
            val provider =
                startedProvider {
                    startField = "svc.boot"
                    uptimeField = "svc.age.ms"
                }

            // When
            val json = encode(provider, eventAt(startMillis + 42L))

            // Then
            assertThat(json.get("svc.boot").isTextual).isTrue()
            assertThat(json.get("svc.age.ms").asLong()).isEqualTo(42L)
            assertThat(json.has("process_start")).isFalse()
            assertThat(json.has("process_uptime_ms")).isFalse()
        }

        @Test
        fun `should omit the start instant when includeStart is disabled`() {
            // Given
            val provider = startedProvider { includeStart = false }

            // When
            val json = encode(provider, eventAt(startMillis + 1L))

            // Then
            assertThat(json.has("process_start")).isFalse()
            assertThat(json.has("process_uptime_ms")).isTrue()
        }

        @Test
        fun `should omit the uptime when includeUptime is disabled`() {
            // Given
            val provider = startedProvider { includeUptime = false }

            // When
            val json = encode(provider, eventAt(startMillis + 1L))

            // Then
            assertThat(json.has("process_start")).isTrue()
            assertThat(json.has("process_uptime_ms")).isFalse()
        }

        /**
         * What is tested: that configuration set after start() has no effect
         * on the emitted document.
         *
         * Why this is the success criterion: the field name changed after
         * start() does not appear; the original one still does.
         *
         * Why it matters: start() copies the configuration into an immutable
         * snapshot, and writeTo reads only that snapshot. This is what makes
         * the cross-thread publication safe -- start() runs on Joran's
         * thread, writeTo on the encoder thread. If a late setter could
         * still influence the output, the mutable fields would be read from
         * the encoder thread and the snapshot would be pointless. The test
         * pins that property rather than trusting the reader of the code.
         */
        @Test
        fun `should freeze its configuration at start and ignore later mutation`() {
            // Given
            val provider = startedProvider()

            // When
            provider.startField = "changed.after.start"
            provider.includeUptime = false
            val json = encode(provider, eventAt(startMillis + 3L))

            // Then
            assertThat(json.has("changed.after.start")).isFalse()
            assertThat(json.get("process_start").isTextual).isTrue()
            assertThat(json.get("process_uptime_ms").asLong()).isEqualTo(3L)
        }

        /**
         * What is tested: that a non-positive start instant prevents the
         * provider from starting.
         *
         * Why this is the success criterion: `LifeCycle.isStarted` remains
         * false, and an error is recorded on the Logback context.
         *
         * Why it matters: a zero reference point would make every uptime
         * equal to the epoch-millis timestamp -- around 1.78 trillion, a
         * number so large it looks like a valid `long` and would be
         * silently indexed. Refusing to start converts a silent corruption
         * into a startup error.
         */
        @Test
        fun `should refuse to start when the supplier resolves a non-positive instant`() {
            // Given
            val context = newContext()
            val provider = ProcessStartJsonProvider { 0L }.apply { this.context = context }

            // When
            provider.start()

            // Then
            assertThat(provider.isStarted).isFalse()
            assertThat(context.statusManager.copyOfStatusList)
                .anySatisfy { status ->
                    assertThat(status.message).contains("must be a positive epoch-millisecond value")
                }
        }

        /**
         * What is tested: that a provider which failed to start contributes
         * no fields instead of throwing.
         *
         * Why this is the success criterion: the resulting JSON object is
         * empty, and no exception escapes writeTo.
         *
         * Why it matters: when start() rejects an invalid instant it returns
         * early, leaving the frozen snapshot null. Whether the encoder skips
         * unstarted providers is not something this class can rely on. If it
         * does not, an unguarded read would throw from inside the logging
         * path -- a logging subsystem that throws while logging turns a
         * misconfiguration into an application-wide outage. The error already
         * sits on the status manager; the correct behaviour here is silence.
         */
        @Test
        fun `should contribute no fields when the provider failed to start`() {
            // Given
            val provider = ProcessStartJsonProvider { -7L }.apply { context = newContext() }
            provider.start()

            // When
            val json = encode(provider, eventAt(startMillis + 500L))

            // Then
            assertThat(provider.isStarted).isFalse()
            assertThat(json.isEmpty).isTrue()
        }

        /**
         * What is tested: that writeTo on a provider which was never started
         * is also silent.
         *
         * Why this is the success criterion: same as above -- an empty JSON
         * object, no exception.
         *
         * Why it matters: this is the path taken when Joran constructs the
         * object but the encoder never propagates start(). It is a different
         * route to the same null snapshot, and the one an operator is more
         * likely to trigger.
         */
        @Test
        fun `should contribute no fields when the provider was never started`() {
            // Given
            val provider = ProcessStartJsonProvider { startMillis }.apply { context = newContext() }

            // When
            val json = encode(provider, eventAt(startMillis + 500L))

            // Then
            assertThat(provider.isStarted).isFalse()
            assertThat(json.isEmpty).isTrue()
        }

        /**
         * What is tested: that a repeated start() neither re-invokes the
         * supplier nor replaces the frozen snapshot.
         *
         * Why this is the success criterion: the supplier is called once, a
         * warning is recorded, and the configuration mutated between the two
         * start() calls is not picked up.
         *
         * Why it matters: Logback components can be started more than once
         * during a context reset, and Spring Boot initialises Logback twice
         * -- once early with defaults, then again once the Environment is
         * available. The reference point must not silently move on the
         * second pass.
         */
        @Test
        fun `should ignore a repeated start and keep the original snapshot`() {
            // Given
            val invocations = AtomicInteger()
            val context = newContext()
            val provider =
                ProcessStartJsonProvider {
                    invocations.incrementAndGet()
                    startMillis
                }.apply { this.context = context }
            provider.start()

            // When
            provider.startField = "second.start"
            provider.start()
            val json = encode(provider, eventAt(startMillis + 9L))

            // Then
            assertThat(invocations.get()).isEqualTo(1)
            assertThat(json.has("second.start")).isFalse()
            assertThat(json.get("process_start").isTextual).isTrue()
            assertThat(context.statusManager.copyOfStatusList)
                .anySatisfy { status ->
                    assertThat(status.message).contains("already started")
                }
        }
    }

    @Nested
    @DisplayName("Independence from SequencingJsonProvider")
    inner class IndependenceFromSequencingJsonProvider {
        /**
         * What is tested: that both providers can contribute to the same
         * JSON object without collision.
         *
         * Why this is the success criterion: the four fields appear side by
         * side, each with its expected JSON type, and the sequence still
         * increments across events.
         *
         * Why it matters: the two providers are intended to be registered
         * together on one encoder. Sharing a `JsonGenerator` is the whole
         * contract of the composite encoder, but nothing in the type system
         * enforces that neither provider writes a duplicate key or leaves
         * the generator in an unbalanced state.
         */
        @Test
        fun `should compose with the sequencing provider on a shared generator`() {
            // Given
            val context = newContext()
            val processStart =
                ProcessStartJsonProvider { startMillis }.apply {
                    this.context = context
                    start()
                }
            val sequencing =
                SequencingJsonProvider().apply {
                    this.context = context
                    instanceId = "test-instance"
                    start()
                }
            val event = eventAt(startMillis + 7L)

            // When
            val writer = StringWriter()
            jackson3Mapper.createGenerator(writer).use { generator ->
                generator.writeStartObject()
                processStart.writeTo(generator, event)
                sequencing.writeTo(generator, event)
                generator.writeEndObject()
            }
            val json = mapper.readTree(writer.toString())

            // Then
            assertThat(json.get("process_start").isTextual).isTrue()
            assertThat(json.get("process_uptime_ms").asLong()).isEqualTo(7L)
            assertThat(json.get("log_encoder_sequence").isNumber).isTrue()
            assertThat(json.get("log_encoder_sequence").asLong()).isEqualTo(1L)
            assertThat(json.get("log_encoder_instance").asText()).isEqualTo("test-instance")
        }

        @Test
        fun `should not consume a sequence number of its own`() {
            // Given
            val provider = startedProvider()

            // When
            val first = encode(provider, eventAt(startMillis + 1L))
            val second = encode(provider, eventAt(startMillis + 2L))

            // Then
            assertThat(first.fieldNames().asSequence().toList())
                .containsExactlyInAnyOrder("process_start", "process_uptime_ms")
            assertThat(second.get("process_start").asText())
                .isEqualTo(first.get("process_start").asText())
        }
    }
}
