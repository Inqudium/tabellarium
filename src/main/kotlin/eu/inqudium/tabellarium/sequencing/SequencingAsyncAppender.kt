package eu.inqudium.tabellarium.sequencing

import ch.qos.logback.classic.AsyncAppender
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import org.slf4j.event.KeyValuePair
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Drop-in replacement for Logback's [AsyncAppender] that stamps every
 * event with a strictly monotonic sequence number and a per-JVM
 * instance identifier **before** the queueing / discarding logic runs.
 *
 * ## Why this exists
 *
 * A plain `AsyncAppender` can silently drop events under load: with the
 * default `discardingThreshold` of `queueSize / 5`, events at level
 * `INFO`, `DEBUG` and `TRACE` are discarded when the queue is 80% full.
 * Downstream sequencing in the encoder cannot see those discards
 * because it runs after the queue.
 *
 * This wrapper hooks into [AsyncAppender.preprocess] — an explicit
 * extension point invoked in the caller's thread before enqueue and
 * before the discard check. It stamps every event that entered the
 * appender, so events that the queue subsequently discards still leave
 * a numbered gap that Kibana can detect.
 *
 * ## Channel: SLF4J KeyValuePairs, not MDC
 *
 * The stamp is attached to the event via [ILoggingEvent.getKeyValuePairs]
 * — the structured-attribute channel added in SLF4J 2.0 / Logback 1.3.
 * MDC is deliberately not used, because:
 *
 * - MDC is a global thread-local key-value store shared with application
 *   code; namespace collisions are always possible.
 * - MDC values are always `String`, forcing a numeric sequence to be
 *   serialized as text and parsed back later, which then requires an
 *   Elasticsearch mapping template to be aggregatable.
 * - KeyValuePairs carry typed `Object` values. A `Long` sequence
 *   remains a JSON number when serialized by `LogstashEncoder`, so
 *   Elasticsearch's dynamic mapping infers `long` at first insert —
 *   no index template required.
 *
 * The `LogstashEncoder` from `logstash-logback-encoder` writes every
 * KeyValuePair as a top-level JSON field by default, preserving the
 * value type. Nothing else needs to be configured on the encoder side
 * for the AsyncAppender's stamp to reach Elasticsearch correctly.
 *
 * ## Independent counting
 *
 * This wrapper counts events **entering the async appender**. A
 * separately-configured [SequencingJsonProvider] at the target encoder
 * counts events **passing the encoder**. The two counters are
 * independent — they do not cooperate or share state. The resulting
 * two Kibana queries answer different questions:
 *
 * - `max(log_async_sequence) - min(log_async_sequence) + 1 - count(*)`
 *   = total loss from AsyncAppender entry to Elasticsearch
 * - `max(log_encoder_sequence) - min(log_encoder_sequence) + 1 - count(*)`
 *   = loss from KafkaAppender encoder to Elasticsearch
 * - Difference = loss caused by the AsyncAppender queue itself
 *
 * ## Configuration
 *
 * ```xml
 * <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
 *   <encoder class="eu.inqudium.tabellarium.sequencing.SequencingLogstashEncoder">
 *     <sequenceField>log_encoder_sequence</sequenceField>
 *     <instanceField>log_encoder_instance</instanceField>
 *   </encoder>
 * </appender>
 *
 * <appender name="ASYNC" class="eu.inqudium.tabellarium.sequencing.SequencingAsyncAppender">
 *   <sequenceField>log_async_sequence</sequenceField>
 *   <instanceField>log_async_instance</instanceField>
 *   <queueSize>1024</queueSize>
 *   <appender-ref ref="KAFKA"/>
 * </appender>
 *
 * <root level="INFO">
 *   <appender-ref ref="ASYNC"/>
 * </root>
 * ```
 *
 * ## Ordering guarantee
 *
 * Sequence numbers are assigned strictly monotonically per JVM lifetime.
 * Because [preprocess] runs in the caller's thread and uses
 * [AtomicLong.incrementAndGet], no two events can ever share a number.
 * The order reflects event entry, not delivery order — arrival at
 * Elasticsearch can reorder, but the loss-detection arithmetic depends
 * only on the density of the sequence range, not on ordering.
 *
 * ## Thread safety
 *
 * The sequence counter is an [AtomicLong] and safe for concurrent use.
 * The instance identifier is written once at [start] and never mutated.
 * KeyValuePair addition to the event uses `addKeyValuePair`, which
 * synchronizes via the internal list; each event is handled from a
 * single thread at a time, so no cross-event interference is possible.
 */
class SequencingAsyncAppender : AsyncAppender() {
    /**
     * Overrides the auto-generated instance ID. Rarely needed — the
     * default of a fresh UUID per JVM start is what most deployments
     * want.
     */
    var instanceId: String? = null

    /**
     * The KeyValuePair key under which the sequence number is written.
     * Defaults to `log_async_sequence` to distinguish it from the
     * encoder-side sequence.
     */
    var sequenceField: String = "log_async_sequence"

    /**
     * The KeyValuePair key under which the instance identifier is
     * written. Defaults to `log_async_instance`.
     */
    var instanceField: String = "log_async_instance"

    private val counter: AtomicLong = AtomicLong(0)

    /**
     * Resolved at start() from [instanceId] if set, otherwise a fresh
     * UUID. Immutable after [start] returns.
     */
    private lateinit var resolvedInstance: String

    override fun start() {
        resolvedInstance = instanceId?.takeIf { it.isNotBlank() }
            ?: UUID.randomUUID().toString()
        addInfo(
            "SequencingAsyncAppender started, instance=$resolvedInstance, " +
                "sequenceField=$sequenceField, instanceField=$instanceField",
        )
        super.start()
    }

    /**
     * Called by [AsyncAppender.doAppend] in the caller's thread, before
     * the event is enqueued and before the discard check. This is the
     * earliest per-event extension point available on the appender.
     */
    public override fun preprocess(eventObject: ILoggingEvent) {
        super.preprocess(eventObject)
        stamp(eventObject)
    }

    /**
     * Attaches sequence and instance KeyValuePairs to the event. The
     * sequence value is a `Long` — `LogstashEncoder` will emit it as a
     * native JSON number, so Elasticsearch's dynamic mapping picks
     * `long` at first insert.
     */
    private fun stamp(event: ILoggingEvent) {
        val seq = counter.incrementAndGet()

        // Only ch.qos.logback.classic.spi.LoggingEvent exposes
        // addKeyValuePair. Cast defensively; if a custom event type
        // does not support mutation, warn once and skip stamping — the
        // downstream pipeline still runs, just without the stamp.
        val classic = event as? LoggingEvent
        if (classic == null) {
            addWarnOnce(
                "SequencingAsyncAppender cannot stamp event: expected " +
                    "ch.qos.logback.classic.spi.LoggingEvent, got ${event.javaClass.name}",
            )
            return
        }

        classic.addKeyValuePair(KeyValuePair(sequenceField, seq))
        classic.addKeyValuePair(KeyValuePair(instanceField, resolvedInstance))
    }

    private val warnOnceGuard = AtomicBoolean(false)

    private fun addWarnOnce(msg: String) {
        if (warnOnceGuard.compareAndSet(false, true)) {
            addWarn(msg)
        }
    }
}
