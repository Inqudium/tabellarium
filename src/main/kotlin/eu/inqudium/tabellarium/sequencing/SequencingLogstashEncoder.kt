package eu.inqudium.tabellarium.sequencing

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider
import net.logstash.logback.composite.JsonProvider
import net.logstash.logback.encoder.LogstashEncoder

/**
 * Drop-in replacement for [LogstashEncoder] that auto-registers a
 * [SequencingJsonProvider] in the provider chain. Purpose: let
 * operators enable encoder-side sequence stamping with a single
 * class-name change in `logback.xml`, without restructuring their
 * encoder configuration.
 *
 * ## Independence from SequencingAsyncAppender
 *
 * This encoder is independent of [SequencingAsyncAppender]. Both
 * classes maintain their own counters and write to different default
 * fields (`log.encoder.*` vs. `log.async.*`) so that Kibana can query
 * either or both without collision. If only this encoder is used and
 * no async appender wrapper, only the encoder-side sequence appears
 * in Elasticsearch.
 *
 * ## Usage
 *
 * Replace `net.logstash.logback.encoder.LogstashEncoder` with this
 * class. All other configuration options remain identical.
 *
 * ```xml
 * <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
 *   <encoder class="eu.inqudium.tabellarium.sequencing.SequencingLogstashEncoder">
 *     <sequenceField>log_encoder_sequence</sequenceField>
 *     <instanceField>log_encoder_instance</instanceField>
 *   </encoder>
 * </appender>
 * ```
 *
 * ## Why a subclass and not just documentation
 *
 * The subclass is a convenience, not a necessity. `LogstashEncoder`
 * exposes `addProvider(JsonProvider)`, and Joran binds the singular
 * `<provider class="...">` child element to it, so this provider can
 * equally be registered on a plain `LogstashEncoder`:
 *
 * ```xml
 * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
 *   <provider class="eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider"/>
 * </encoder>
 * ```
 *
 * What this subclass buys is a single class-name change in
 * `logback.xml`, typed setters (`sequenceField`, `instanceField`,
 * `instanceId`) lifted to the encoder level where an operator expects
 * to find them — and independence from the encoder generation: the
 * hand-registered form above has to name the `jackson3` half for
 * logstash-logback-encoder 9.x and the one in this package for 8.x,
 * while this encoder picks for itself.
 *
 * Note the plural: the `<providers>` element is **not** permitted on
 * `LogstashEncoder`. It is a configuration error, and since
 * logstash-logback-encoder 7.3 it throws rather than merely reporting a
 * status. Only the composite encoder accepts `<providers>`.
 *
 * ## Do not register the provider twice
 *
 * This encoder already holds a [SequencingJsonProvider]. Adding another
 * one via `<provider class="...SequencingJsonProvider"/>` yields two
 * independent counters writing the **same** JSON key, and therefore a
 * duplicate field in one JSON object. Jackson does not reject this;
 * Elasticsearch keeps the last occurrence.
 *
 * To combine encoder-side sequencing with other providers, add only the
 * others:
 *
 * ```xml
 * <encoder class="eu.inqudium.tabellarium.sequencing.SequencingLogstashEncoder">
 *   <sequenceField>log_encoder_sequence</sequenceField>
 *   <provider class="eu.inqudium.tabellarium.sequencing.jackson3.ProcessStartJsonProvider"/>
 * </encoder>
 * ```
 */
class SequencingLogstashEncoder : LogstashEncoder() {
    /**
     * The shared state. The encoder holds the state rather than the
     * provider, because which provider class is instantiated depends on
     * the encoder generation on the classpath — the properties below
     * must reach either one.
     */
    private val support: SequencingSupport = SequencingSupport()

    /**
     * Overrides the auto-generated instance ID on the underlying
     * provider. See [SequencingJsonProvider.instanceId] for semantics.
     */
    var instanceId: String?
        get() = support.instanceId
        set(value) {
            support.instanceId = value
        }

    /**
     * Overrides the JSON key for the sequence number. See
     * [SequencingJsonProvider.sequenceField].
     */
    var sequenceField: String
        get() = support.sequenceField
        set(value) {
            support.sequenceField = value
        }

    /**
     * Overrides the JSON key for the instance identifier. See
     * [SequencingJsonProvider.instanceField].
     */
    var instanceField: String
        get() = support.instanceField
        set(value) {
            support.instanceField = value
        }

    init {
        addProvider(newSequencingProvider())
    }

    /**
     * Picks the provider half that matches the encoder on the
     * classpath: the `jackson3` class this module builds against for
     * logstash-logback-encoder 9.x, its twin one package up for 8.x
     * (see [JacksonGeneration]).
     *
     * This is what makes the encoder shortcut version-agnostic — one
     * `<encoder class="…SequencingLogstashEncoder">` works on both,
     * while a hand-registered `<provider class="…"/>` has to name the
     * matching half itself.
     */
    private fun newSequencingProvider(): JsonProvider<ILoggingEvent> =
        if (JacksonGeneration.isJackson3) {
            SequencingJsonProvider(support)
        } else {
            JacksonGeneration.newJackson2Provider(
                JACKSON3_PROVIDER,
                support,
                SequencingSupport::class.java,
            )
        }

    private companion object {
        /**
         * The Jackson 3 half, which the module builds against; the
         * Jackson 2 twin is derived from this name and resolved by
         * string, so that under 9.x it is never loaded.
         */
        const val JACKSON3_PROVIDER = "eu.inqudium.tabellarium.sequencing.jackson3.SequencingJsonProvider"
    }
}
