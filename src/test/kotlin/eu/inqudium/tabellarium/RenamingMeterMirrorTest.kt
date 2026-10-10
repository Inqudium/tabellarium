package eu.inqudium.tabellarium

import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.ToDoubleFunction

class RenamingMeterMirrorTest {
    private val warnings = mutableListOf<String>()

    private fun mirrorInto(target: MeterRegistry): RenamingMeterMirror = RenamingMeterMirror(target, MetricsBindings::kafkaClientMeterName) { message, _ -> warnings += message }

    @Nested
    inner class `Publishing` {
        @Test
        fun `should publish a gauge and a function counter under the new name, reading through to the source`() {
            // What is to be tested? Whether the two meter types KafkaMetrics
            //   registers appear in the target renamed, with the source's tags,
            //   description and base unit, and report the source's live values.
            // How will the test case be deemed successful and why? Successful if
            //   the target holds kafka.appender.producer.* meters with the same
            //   tags and unit, and both values follow a change of the
            //   underlying state after registration.
            // Why is it important to test this test case? A mirror that copied
            //   a value once, or dropped tags or units, would export plausible
            //   but frozen or mislabelled series.

            // Given
            val target = SimpleMeterRegistry()
            val mirror = mirrorInto(target)
            val state = AtomicLong(1)

            // When
            Gauge
                .builder("kafka.producer.buffer.available.bytes", state) { it.get().toDouble() }
                .tags("topic.class", "audit", "appender", "KAFKA")
                .description("free buffer")
                .baseUnit("bytes")
                .register(mirror.source)
            FunctionCounter
                .builder("kafka.producer.record.send.total", state) { it.get() * 10.0 }
                .tags("topic.class", "audit", "appender", "KAFKA")
                .register(mirror.source)
            state.set(7)

            // Then
            val gauge = target.find("kafka.appender.producer.buffer.available.bytes").gauge()
            assertThat(gauge).isNotNull
            assertThat(gauge!!.value()).isEqualTo(7.0)
            assertThat(gauge.id.getTag("topic.class")).isEqualTo("audit")
            assertThat(gauge.id.getTag("appender")).isEqualTo("KAFKA")
            assertThat(gauge.id.baseUnit).isEqualTo("bytes")
            assertThat(gauge.id.description).isEqualTo("free buffer")
            assertThat(target.find("kafka.appender.producer.record.send.total").functionCounter()?.count()).isEqualTo(70.0)
            assertThat(target.meters).noneMatch { it.id.name.startsWith("kafka.producer") }
        }

        @Test
        fun `should keep the time unit of a time gauge`() {
            // What is to be tested? Whether a time gauge is mirrored as a time
            //   gauge with its base time unit, not as a plain gauge.
            // How will the test case be deemed successful and why? Successful if
            //   the mirrored meter is a TimeGauge reporting the source value in
            //   the source's unit.
            // Why is it important to test this test case? As a plain gauge the
            //   registry would export a raw number without unit conversion.

            // Given
            val target = SimpleMeterRegistry()
            val mirror = mirrorInto(target)

            // When
            TimeGauge.builder("kafka.producer.io.wait.time", this, TimeUnit.MILLISECONDS) { 250.0 }.register(mirror.source)

            // Then
            val mirrored = target.find("kafka.appender.producer.io.wait.time").timeGauge()
            assertThat(mirrored).isNotNull
            assertThat(mirrored!!.value(TimeUnit.MILLISECONDS)).isEqualTo(250.0)
        }

        @Test
        fun `should not publish a meter whose name the rename does not map`() {
            // What is to be tested? Whether meters outside the kafka.* namespace -
            //   KafkaMetrics registers and removes a delete.this probe on bind -
            //   stay in the private source.
            // How will the test case be deemed successful and why? Successful if
            //   the target holds no meter after such a registration.
            // Why is it important to test this test case? The probe would
            //   otherwise flash into the operator's registry on every bind.

            // Given
            val target = SimpleMeterRegistry()
            val mirror = mirrorInto(target)

            // When
            Meter.builder("delete.this", Meter.Type.OTHER, emptyList()).register(mirror.source)

            // Then
            assertThat(target.meters).isEmpty()
        }

        @Test
        fun `should be visible on a composite target and on its child`() {
            // What is to be tested? Whether a mirrored meter lands in a composite
            //   target itself, not only in the registries below it.
            // How will the test case be deemed successful and why? Successful if
            //   both the composite and its child hold the renamed meter.
            // Why is it important to test this test case? A rename filter on a
            //   private composite in front of the target fails exactly here:
            //   Micrometer forwards to the leaves of a nested composite, and the
            //   composite in-process readers query never sees the meter.

            // Given
            val child = SimpleMeterRegistry()
            val composite = CompositeMeterRegistry().add(child)
            val mirror = mirrorInto(composite)

            // When
            Gauge.builder("kafka.producer.waiting.threads") { 0.0 }.register(mirror.source)

            // Then
            assertThat(composite.find("kafka.appender.producer.waiting.threads").gauge()).isNotNull
            assertThat(child.find("kafka.appender.producer.waiting.threads").gauge()).isNotNull
        }
    }

    @Nested
    inner class `Lifecycle` {
        @Test
        fun `should remove the mirror when the source meter is removed`() {
            // What is to be tested? Whether a source meter the binder removes
            //   (KafkaMetrics does on refresh when a client metric disappears)
            //   takes its mirror with it.
            // How will the test case be deemed successful and why? Successful if
            //   the target no longer holds the renamed meter after the removal.
            // Why is it important to test this test case? A stale mirror would
            //   keep reading a meter that no longer exists.

            // Given
            val target = SimpleMeterRegistry()
            val mirror = mirrorInto(target)
            val gauge = Gauge.builder("kafka.producer.connection.count") { 1.0 }.register(mirror.source)

            // When
            mirror.source.remove(gauge)

            // Then
            assertThat(target.meters).isEmpty()
            assertThat(mirror.mirroredNames).isEmpty()
        }

        @Test
        fun `should remove every mirror on close, drop later additions and leave the target open`() {
            // What is to be tested? Whether close() removes all mirrors, a meter
            //   registered into the source afterwards (a late refresh of the
            //   binder) is not published, and the target registry stays open.
            // How will the test case be deemed successful and why? Successful if
            //   the target is empty after close and after the late registration,
            //   and target.isClosed is false.
            // Why is it important to test this test case? The binder refreshes on
            //   its own scheduler; a refresh racing the unbind must not leave
            //   meters behind, and the operator's registry is not ours to close.

            // Given
            val target = SimpleMeterRegistry()
            val mirror = mirrorInto(target)
            Gauge.builder("kafka.producer.a") { 1.0 }.register(mirror.source)
            Gauge.builder("kafka.producer.b") { 2.0 }.register(mirror.source)

            // When
            mirror.close()
            Gauge.builder("kafka.producer.late") { 3.0 }.register(mirror.source)

            // Then
            assertThat(target.meters).isEmpty()
            assertThat(target.isClosed).isFalse()
        }

        @Test
        fun `should swallow a failing registration and report only the first`() {
            // What is to be tested? Whether a target that rejects a registration
            //   neither breaks the source registration (the binder's thread) nor
            //   floods the status output.
            // How will the test case be deemed successful and why? Successful if
            //   two registrations against a closed-for-gauges target both return
            //   normally and exactly one warning is recorded.
            // Why is it important to test this test case? An exception escaping
            //   the listener would surface in KafkaMetrics' refresh and could stop
            //   it; a warning per meter would flood the Logback status.

            // Given: a target whose gauge registration throws
            val target =
                object : SimpleMeterRegistry() {
                    override fun <T : Any> newGauge(
                        id: Meter.Id,
                        obj: T?,
                        valueFunction: ToDoubleFunction<T>,
                    ): Gauge = throw IllegalStateException("rejected")
                }
            val mirror = mirrorInto(target)

            // When / Then
            assertThatCode {
                Gauge.builder("kafka.producer.x") { 1.0 }.register(mirror.source)
                Gauge.builder("kafka.producer.y") { 1.0 }.register(mirror.source)
            }.doesNotThrowAnyException()
            assertThat(warnings).hasSize(1)
        }
    }
}
