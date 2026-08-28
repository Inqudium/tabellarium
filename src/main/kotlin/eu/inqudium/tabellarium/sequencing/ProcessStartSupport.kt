package eu.inqudium.tabellarium.sequencing

import java.lang.management.ManagementFactory
import java.time.Instant

/**
 * The Jackson-free state behind [ProcessStartJsonProvider] and its
 * `jackson3` twin. See [SequencingSupport] for why the providers are
 * split by Jackson generation and why this class is public.
 *
 * ## Configuration is frozen at start
 *
 * `freeze()` runs on Joran's configuration thread; the providers'
 * `writeTo` runs on the encoder thread — for an async appender, a
 * different thread entirely. Rather than rely on the happens-before
 * edge that the appender start-up sequence probably establishes, all
 * configuration is copied into an immutable [Frozen] snapshot,
 * published through a single volatile reference.
 *
 * Consequences: the mutable setters are never read from the encoder
 * thread; the snapshot's fields are final and therefore safely
 * published by the JMM; and a write performs exactly one volatile read.
 * A misconfigured provider leaves the reference null and simply
 * contributes nothing.
 *
 * @param processStartMillisSupplier resolves the JVM start time in
 *   epoch milliseconds. Invoked exactly once, from [freeze].
 */
class ProcessStartSupport(
    private val processStartMillisSupplier: () -> Long = { ManagementFactory.getRuntimeMXBean().startTime },
) {
    /** The JSON key for the JVM start instant, written as an ISO-8601 string. */
    var startField: String = DEFAULT_START_FIELD

    /** The JSON key for the per-event uptime in milliseconds, written as a native JSON number. */
    var uptimeField: String = DEFAULT_UPTIME_FIELD

    /** Emit [startField]. */
    var includeStart: Boolean = true

    /** Emit [uptimeField]. */
    var includeUptime: Boolean = true

    /**
     * The frozen configuration, or null while the provider is not
     * started. The only field read from the encoder thread, and the
     * only one that needs to be volatile: the snapshot it refers to is
     * immutable, so the JMM's final-field guarantees cover everything
     * reachable through it.
     */
    @Volatile
    var frozen: Frozen? = null
        private set

    /**
     * Resolves the start instant and publishes the [Frozen] snapshot,
     * or reports why it did not.
     *
     * The status text is returned rather than logged because this class
     * is not `ContextAware` — the calling provider owns the Logback
     * status manager and emits whichever of the three fields is set.
     */
    fun freeze(): FreezeResult {
        val startMillis = processStartMillisSupplier()

        if (startMillis <= 0L) {
            return FreezeResult(
                error =
                    "ProcessStartJsonProvider not started: the resolved process start must be a " +
                        "positive epoch-millisecond value, but was $startMillis",
            )
        }

        val warning =
            if (!includeStart && !includeUptime) {
                "ProcessStartJsonProvider started with both includeStart and includeUptime " +
                    "disabled; it will contribute no fields"
            } else {
                null
            }

        // Instant.toString() is always UTC, always Z-suffixed, and never exceeds
        // millisecond precision when built from ofEpochMilli -- which is what
        // Elasticsearch's default strict_date_optional_time expects. Formatted
        // once here, never per event: writeTo runs on the encoder thread, in the
        // critical path between the async queue and the appender's transport.
        val snapshot =
            Frozen(
                startMillis = startMillis,
                startIso = Instant.ofEpochMilli(startMillis).toString(),
                startField = startField,
                uptimeField = uptimeField,
                includeStart = includeStart,
                includeUptime = includeUptime,
            )
        frozen = snapshot

        return FreezeResult(
            frozen = snapshot,
            warning = warning,
            info =
                "ProcessStartJsonProvider started, processStart=${snapshot.startIso}, " +
                    "startField=$startField, uptimeField=$uptimeField",
        )
    }

    /**
     * Immutable snapshot of everything a write needs. All fields are
     * final, so publishing the instance through the volatile [frozen]
     * reference makes them visible to every reader without further
     * synchronisation.
     */
    class Frozen(
        @JvmField val startMillis: Long,
        @JvmField val startIso: String,
        @JvmField val startField: String,
        @JvmField val uptimeField: String,
        @JvmField val includeStart: Boolean,
        @JvmField val includeUptime: Boolean,
    )

    /**
     * Outcome of [freeze]: the snapshot when it succeeded, plus the
     * status texts the provider is to hand to Logback. At most one of
     * [error] and [info] is ever set.
     */
    class FreezeResult(
        @JvmField val frozen: Frozen? = null,
        @JvmField val error: String? = null,
        @JvmField val warning: String? = null,
        @JvmField val info: String? = null,
    )

    companion object {
        const val DEFAULT_START_FIELD: String = "process_start"
        const val DEFAULT_UPTIME_FIELD: String = "process_uptime_ms"
    }
}
