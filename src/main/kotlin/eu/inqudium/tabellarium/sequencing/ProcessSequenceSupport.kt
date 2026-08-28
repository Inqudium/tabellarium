package eu.inqudium.tabellarium.sequencing

import java.lang.management.ManagementFactory
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * The Jackson-free state behind [ProcessSequenceJsonProvider] and its
 * `jackson3` twin. See [SequencingSupport] for why the providers are
 * split by Jackson generation and why this class is public.
 *
 * The four field names are fixed rather than configurable, exactly as
 * they are on the provider — [ProcessSequenceJsonProvider.includeSequence]
 * is the only knob, and it merely suppresses the last two fields.
 *
 * @param processStartMillisSupplier resolves the JVM start time in
 *   epoch milliseconds. Invoked exactly once, at construction.
 */
class ProcessSequenceSupport(
    processStartMillisSupplier: () -> Long = { ManagementFactory.getRuntimeMXBean().startTime },
) {
    /**
     * Emit the sequence and instance fields. Disable to keep only the
     * process start and uptime. Volatile because Joran sets it on the
     * configuration thread while the encoder thread reads it.
     */
    @Volatile
    var includeSequence: Boolean = true

    /** Source of the strictly monotonic per-event sequence number. */
    private val counter = AtomicLong(0)

    /** Identifies this JVM run so sequence ranges from different runs are not mixed. */
    @JvmField
    val instance: String = UUID.randomUUID().toString()

    /** JVM start captured once, in epoch milliseconds; drives [uptimeMillis]. */
    @JvmField
    val startMillis: Long = processStartMillisSupplier()

    /** The same instant in ISO-8601 UTC form, formatted once rather than per event. */
    @JvmField
    val startIso: String = Instant.ofEpochMilli(startMillis).toString()

    /** The next sequence number. Strictly monotonic per JVM run, starting at 1. */
    fun nextSequence(): Long = counter.incrementAndGet()

    /**
     * The event's age relative to JVM start. Derived from the event's
     * own timestamp — never from a fresh clock read, which would
     * measure encode time instead of log time.
     */
    fun uptimeMillis(eventTimeStamp: Long): Long = eventTimeStamp - startMillis

    companion object {
        const val START_FIELD: String = "process_start"
        const val UPTIME_FIELD: String = "process_uptime_ms"
        const val SEQUENCE_FIELD: String = "log_encoder_sequence"
        const val INSTANCE_FIELD: String = "log_encoder_instance"
    }
}
