package eu.inqudium.tabellarium.sequencing.jackson3

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.sequencing.ProcessSequenceSupport
import net.logstash.logback.composite.AbstractJsonProvider
import tools.jackson.core.JsonGenerator

/**
 * `net.logstash.logback` JSON provider that enriches every logging event
 * with process-identity and event-ordering metadata in a single pass. It
 * writes up to four fields per event:
 *
 * - `process_start` -- the JVM start instant, as an ISO-8601 UTC string.
 * - `process_uptime_ms` -- the event's age relative to JVM start
 *   (`event.timeStamp - startMillis`), as a native JSON number.
 * - `log_encoder_sequence` -- a strictly monotonic per-JVM counter
 *   starting at 1, as a native JSON number. Written only when
 *   [includeSequence] is set.
 * - `log_encoder_instance` -- a per-JVM UUID, as a string. Written only
 *   when [includeSequence] is set.
 *
 * The field names are fixed; [includeSequence] is the only configurable
 * knob and merely suppresses the last two fields.
 *
 * ## Why the sequence and instance exist: loss detection
 *
 * The counter is strictly monotonic within a single JVM run and the UUID
 * identifies that run. Together they make otherwise-silent log loss
 * measurable: a downstream store can group by `log_encoder_instance` and
 * compare `max(sequence) - min(sequence) + 1` against the number of
 * events it actually received. Any positive difference is exactly the
 * count of events that passed through this provider but never arrived --
 * a gap left by a dropped async-queue entry, a broker outage, or an
 * ingest failure.
 *
 * ## Field types: string for the timestamp, numbers for the rest
 *
 * `process_start` is a string because an ISO-8601 timestamp is meant to
 * be read and range-queried as a date, not used in arithmetic. Uptime and
 * sequence are emitted as native JSON numbers so a schemaless store (for
 * example Elasticsearch with dynamic mapping) infers a numeric type on
 * first insert and the fields become aggregatable -- histograms over
 * uptime, gap arithmetic over the sequence -- with no index template to
 * maintain.
 *
 * ## Uptime is derived from the event, not from a clock
 *
 * `process_uptime_ms` is computed as `event.timeStamp` (the instant the
 * event was created) minus the captured JVM start; it never reads a fresh
 * clock. This keeps the value anchored to when the line was logged rather
 * than the possibly much later moment the encoder runs -- relevant when an
 * async appender defers encoding -- and avoids a second time lookup per
 * event.
 *
 * ## The sequence bypasses the MDC
 *
 * The counter is written straight to the JSON generator instead of being
 * routed through the MDC. MDC values are always `String`, which would
 * force the number to be serialized as text and parsed back downstream
 * (and would then need an explicit mapping to aggregate). The MDC is also
 * a process-global thread-local shared with application code, so a key
 * placed there could collide with an application key. Emitting the field
 * directly keeps it a native `Long` and free of shared-state side effects.
 *
 * ## Concurrency and lifecycle
 *
 * All state is captured in the constructor as `final` fields -- safely
 * published by the JMM -- so no [start] override or volatile snapshot is
 * needed. [ManagementFactory.getRuntimeMXBean] reports the JVM start time
 * regardless of when it is read, so capturing it here still yields the
 * earliest instant. The counter is an [AtomicLong]; there are no locks and
 * nothing blocks, so the provider is safe on the shared encoder thread and
 * on any caller thread it runs from.
 */
class ProcessSequenceJsonProvider(
    private val support: ProcessSequenceSupport = ProcessSequenceSupport(),
) : AbstractJsonProvider<ILoggingEvent>() {
    /**
     * Emit the sequence and instance fields. Disable to keep only the
     * process start and uptime. Volatile in [ProcessSequenceSupport]
     * because Joran sets it on the configuration thread while [writeTo]
     * reads it on the encoder thread.
     */
    var includeSequence: Boolean
        get() = support.includeSequence
        set(value) {
            support.includeSequence = value
        }

    override fun writeTo(
        generator: JsonGenerator,
        event: ILoggingEvent,
    ) {
        // Constant per JVM run; uptime is the event's own timestamp minus start, not a fresh clock read.
        generator.writeStringProperty(ProcessSequenceSupport.START_FIELD, support.startIso)
        generator.writeNumberProperty(ProcessSequenceSupport.UPTIME_FIELD, support.uptimeMillis(event.timeStamp))
        // nextSequence() advances the sequence once per emitted event; a gap downstream signals loss.
        if (support.includeSequence) {
            generator.writeNumberProperty(ProcessSequenceSupport.SEQUENCE_FIELD, support.nextSequence())
            generator.writeStringProperty(ProcessSequenceSupport.INSTANCE_FIELD, support.instance)
        }
    }
}
