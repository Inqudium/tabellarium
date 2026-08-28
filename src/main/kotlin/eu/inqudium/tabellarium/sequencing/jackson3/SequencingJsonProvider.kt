package eu.inqudium.tabellarium.sequencing.jackson3

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.sequencing.SequencingAsyncAppender
import eu.inqudium.tabellarium.sequencing.SequencingLogstashEncoder
import eu.inqudium.tabellarium.sequencing.SequencingSupport
import net.logstash.logback.composite.AbstractJsonProvider
import tools.jackson.core.JsonGenerator

/**
 * `net.logstash.logback` JSON provider that writes a strictly monotonic
 * per-JVM sequence number as a **native JSON number** (not a string)
 * and a per-JVM instance identifier as a string, directly to the
 * Jackson `JsonGenerator`.
 *
 * ## Why native JSON number matters
 *
 * MDC values in Logback are always `String`. If the sequence were
 * emitted through MDC, Elasticsearch's dynamic mapping would infer
 * `text`/`keyword` at first insert, and the Kibana loss-detection
 * arithmetic `max(seq) - min(seq) + 1 - count(*)` would fail — text
 * fields do not support numeric aggregation.
 *
 * This provider bypasses MDC by writing directly to the `JsonGenerator`
 * as a numeric field. Elasticsearch then infers `long` at first insert
 * without any index template.
 *
 * ## Independent counting
 *
 * This provider counts every event that passes through the encoder in
 * the target appender. When configured together with
 * [SequencingAsyncAppender], both count independently — they do not
 * cooperate or share state. This gives two Kibana queries:
 *
 * - `max(log_async_sequence) - min(log_async_sequence) + 1 - count(*)`
 *   = loss from AsyncAppender entry to Elasticsearch (includes async
 *   queue discards)
 * - `max(log_encoder_sequence) - min(log_encoder_sequence) + 1 - count(*)`
 *   = loss from encoder to Elasticsearch (excludes async queue discards,
 *   captures only Kafka / Logstash / Elasticsearch problems)
 * - The difference isolates the AsyncAppender queue's contribution.
 *
 * Both counters are **per JVM**, starting at 1 on every start. The
 * arithmetic above is therefore only meaningful when partitioned by
 * [instanceField]; run unpartitioned across a replica set it reports
 * losses that do not exist. This is what the instance identifier is
 * for.
 *
 * ## Composition with other providers
 *
 * This provider is independent of [ProcessStartJsonProvider]. Both may
 * be registered on the same encoder; they share no state and write
 * disjoint fields.
 *
 * ## Configuration with LoggingEventCompositeJsonEncoder
 *
 * ```xml
 * <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
 *   <providers>
 *     <timestamp/>
 *     <logLevel/>
 *     <message/>
 *     <mdc/>
 *     <provider class="eu.inqudium.tabellarium.sequencing.SequencingJsonProvider">
 *       <sequenceField>log_encoder_sequence</sequenceField>
 *       <instanceField>log_encoder_instance</instanceField>
 *     </provider>
 *   </providers>
 * </encoder>
 * ```
 *
 * ## Configuration with LogstashEncoder
 *
 * `LogstashEncoder` rejects the plural `<providers>` element — that is
 * a configuration error, and since logstash-logback-encoder 7.3 it
 * throws. Register this provider with the singular `<provider>`
 * element, which Joran binds to `LogstashEncoder.addProvider`:
 *
 * ```xml
 * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
 *   <provider class="eu.inqudium.tabellarium.sequencing.SequencingJsonProvider">
 *     <sequenceField>log_encoder_sequence</sequenceField>
 *   </provider>
 * </encoder>
 * ```
 *
 * The element is **repeatable**. `JsonProviders.addProvider` appends to
 * a list, and Joran treats an `addX` method as a collection property, so
 * several `<provider>` siblings register several providers. The plural
 * container is neither needed nor permitted here.
 *
 * ## Configuration with the LogstashEncoder shortcut
 *
 * Use [SequencingLogstashEncoder], which extends `LogstashEncoder` and
 * auto-registers this provider. Do **not** additionally declare a
 * `<provider class="...SequencingJsonProvider"/>` on that encoder: two
 * instances would write the same JSON key with two independent
 * counters, producing a duplicate field.
 *
 * ## Encoder version: this class is the Jackson 3 half
 *
 * `JsonProvider.writeTo` takes the encoder's own `JsonGenerator`, and
 * that type changed with logstash-logback-encoder 9.0 —
 * `com.fasterxml.jackson.core` (8.x) versus `tools.jackson.core` (9.x),
 * two unrelated types. One class cannot implement both signatures, so
 * this class serves **9.x** and its twin
 * `eu.inqudium.tabellarium.sequencing.SequencingJsonProvider`
 * serves 8.x. Both delegate to the same [SequencingSupport], so they
 * count and name fields identically.
 *
 * Which one to name in `logback.xml` follows the encoder on the
 * classpath. [SequencingLogstashEncoder] picks for you and needs no
 * such declaration.
 *
 * ## Thread safety
 *
 * [SequencingSupport.nextSequence] is an atomic increment, no locks or
 * blocking. The instance identifier is written once at [start] and
 * never mutated.
 *
 * @param support the shared, Jackson-free state. The default gives
 *   Joran the no-argument constructor it needs; the encoder passes its
 *   own instance so that its setters and the provider stay one object.
 */
open class SequencingJsonProvider(
    private val support: SequencingSupport = SequencingSupport(),
) : AbstractJsonProvider<ILoggingEvent>() {
    /**
     * Overrides the auto-generated instance ID. Rarely needed — the
     * default of a fresh UUID per JVM start is what most deployments
     * want.
     */
    var instanceId: String?
        get() = support.instanceId
        set(value) {
            support.instanceId = value
        }

    /**
     * The JSON key for the sequence number. Defaults to
     * `log_encoder_sequence` to distinguish it from the async-side
     * sequence written by [SequencingAsyncAppender].
     */
    var sequenceField: String
        get() = support.sequenceField
        set(value) {
            support.sequenceField = value
        }

    /**
     * The JSON key for the instance identifier. Defaults to
     * `log_encoder_instance`.
     */
    var instanceField: String
        get() = support.instanceField
        set(value) {
            support.instanceField = value
        }

    override fun start() {
        val resolvedInstance = support.resolveInstance()
        addInfo(
            "SequencingJsonProvider started, instance=$resolvedInstance, " +
                "sequenceField=$sequenceField, instanceField=$instanceField",
        )
        super.start()
    }

    /**
     * Writes the sequence number as a native JSON number and the
     * instance identifier as a JSON string. Called by the encoder for
     * every event that passes through.
     */
    override fun writeTo(
        generator: JsonGenerator,
        event: ILoggingEvent,
    ) {
        generator.writeNumberProperty(support.sequenceField, support.nextSequence())
        generator.writeStringProperty(support.instanceField, support.resolvedInstance)
    }

    companion object {
        const val DEFAULT_SEQUENCE_FIELD: String = SequencingSupport.DEFAULT_SEQUENCE_FIELD
        const val DEFAULT_INSTANCE_FIELD: String = SequencingSupport.DEFAULT_INSTANCE_FIELD
    }
}
