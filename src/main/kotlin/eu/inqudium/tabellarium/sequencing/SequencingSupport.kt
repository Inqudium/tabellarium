package eu.inqudium.tabellarium.sequencing

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * The Jackson-free state behind [SequencingJsonProvider] and its
 * `jackson3` twin.
 *
 * ## Why this class exists
 *
 * `net.logstash.logback.composite.JsonProvider.writeTo` takes the
 * encoder's `JsonGenerator`, and that parameter type changed with
 * logstash-logback-encoder 9.0 — from `com.fasterxml.jackson.core`
 * (Jackson 2) to `tools.jackson.core` (Jackson 3). The two types are
 * unrelated, so **one class file cannot implement both signatures**: a
 * class carrying an overload for the absent generation fails every
 * reflective lookup on itself with `NoClassDefFoundError`, which kills
 * Joran's property binding long before the first log event.
 *
 * The module therefore ships two provider classes — one per generation
 * — and keeps everything that is not a Jackson call in this class, so
 * the behaviour is defined once. See [JacksonGeneration] for how the
 * right one is chosen at runtime.
 *
 * ## Public because a second package needs it
 *
 * `internal` would name-mangle the accessors and make them unusable
 * from the Java sources under `src/main/jackson3`. Treat this class as
 * module-internal regardless: it carries no compatibility promise, and
 * nothing outside the two providers and [SequencingLogstashEncoder]
 * should touch it.
 *
 * ## Thread safety
 *
 * [nextSequence] is an [AtomicLong.incrementAndGet] — no locks, no
 * blocking. [resolvedInstance] is written once by [resolveInstance] on
 * the Joran configuration thread and only read afterwards.
 */
class SequencingSupport {
    /**
     * Overrides the auto-generated instance ID. Rarely needed — the
     * default of a fresh UUID per JVM start is what most deployments
     * want.
     */
    var instanceId: String? = null

    /**
     * The JSON key for the sequence number. Defaults to
     * `log_encoder_sequence` to distinguish it from the async-side
     * sequence written by [SequencingAsyncAppender].
     */
    var sequenceField: String = DEFAULT_SEQUENCE_FIELD

    /** The JSON key for the instance identifier. Defaults to `log_encoder_instance`. */
    var instanceField: String = DEFAULT_INSTANCE_FIELD

    private val counter: AtomicLong = AtomicLong(0)

    /**
     * The identifier resolved by [resolveInstance]. Reading it before
     * the provider has started is a programming error.
     */
    lateinit var resolvedInstance: String
        private set

    /**
     * Resolves [instanceId] — or a fresh UUID when it is unset or blank
     * — into [resolvedInstance] and returns it. Called from the
     * provider's `start()`.
     */
    fun resolveInstance(): String {
        resolvedInstance = instanceId?.takeIf { it.isNotBlank() } ?: UUID.randomUUID().toString()
        return resolvedInstance
    }

    /** The next sequence number. Strictly monotonic per JVM run, starting at 1. */
    fun nextSequence(): Long = counter.incrementAndGet()

    companion object {
        const val DEFAULT_SEQUENCE_FIELD: String = "log_encoder_sequence"
        const val DEFAULT_INSTANCE_FIELD: String = "log_encoder_instance"
    }
}
