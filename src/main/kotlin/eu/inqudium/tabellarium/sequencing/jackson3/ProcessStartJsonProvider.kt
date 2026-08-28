package eu.inqudium.tabellarium.sequencing.jackson3

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.sequencing.ProcessStartSupport
import eu.inqudium.tabellarium.sequencing.SequencingLogstashEncoder
import net.logstash.logback.composite.AbstractJsonProvider
import tools.jackson.core.JsonGenerator

/**
 * `net.logstash.logback` JSON provider that writes the JVM start
 * instant and the per-event process uptime.
 *
 * The two fields exist to make a rolling deployment analysable. During
 * a rolling update, pods start at different wall-clock times, so a
 * `date_histogram` over `@timestamp` superimposes several offset
 * start-up transients into one broad hump. Bucketing on
 * [uptimeField] instead collapses those curves on top of one another,
 * which makes the decay constant directly readable.
 *
 * ## Opposite type rules
 *
 * The two fields deliberately use **different JSON types**, and this
 * is not cosmetic:
 *
 * - [startField] is written as an **ISO-8601 string** in UTC. Where no
 *   index template applies, Elasticsearch's `date_detection` infers
 *   `date` from an ISO string, but infers `long` from a number.
 * - [uptimeField] is written as a **native JSON number**. Dynamic
 *   mapping infers `long`, which supports `date_histogram`,
 *   `range` queries and the `derivative` pipeline aggregation. A
 *   string would be inferred as `text`/`keyword`, and every such
 *   aggregation would silently return nothing.
 *
 * Anyone who "harmonises" the two types breaks one of them. See the
 * class-level test for the assertions that guard this.
 *
 * ## Why uptime is derived from the event, not from the clock
 *
 * [ILoggingEvent.getTimeStamp] is the moment the event was **created**.
 * Reading a clock inside [writeTo] would instead measure the moment the
 * event was **encoded** — inflated by however long it sat in the
 * `AsyncAppender` queue. Under exactly the conditions this provider is
 * meant to illuminate, that error is largest.
 *
 * [writeTo] is therefore a pure function of the event and one frozen
 * constant. It reads no clock.
 *
 * ## A coordinate is not a duration
 *
 * The stronger form of the rule above — because "monotonic time for
 * durations, wall-clock time for timestamps" is a sound convention that
 * points the wrong way here.
 *
 * [uptimeField] **looks like a duration and is not one.** It is a
 * *coordinate*: an axis on which events are placed so they can be
 * compared with one another and with `@timestamp`. A coordinate must be
 * drawn from the same clock as whatever it is compared against.
 *
 * `@timestamp` is written by logstash-logback-encoder from
 * `event.getInstant()`, and `event.getTimeStamp()` is that same instant
 * truncated to milliseconds. So the identity
 *
 * ```
 * toEpochMilli(@timestamp) == process_start + process_uptime_ms
 * ```
 *
 * holds **by construction**, but only while the uptime comes from the
 * event. A monotonic source — `System.nanoTime()`,
 * `RuntimeMXBean.getUptime()`, an injected monotonic time source —
 * breaks it twice over: it is not the log time, because it is read at
 * encode time; and it is not on `@timestamp`'s axis, because it is
 * monotonic while [startField] is wall-clock.
 *
 * That identity is worth asserting in Elasticsearch after deployment,
 * because the defect is silent. An uptime read from a monotonic clock
 * still yields plausible, monotonically increasing numbers. Two symptoms
 * give it away: the difference is never negative, and it discriminates
 * *within* a single millisecond of `@timestamp` — which a value derived
 * from `getTimeStamp()` cannot do.
 *
 * ## Why the JVM start time and not some later instant
 *
 * `RuntimeMXBean.getStartTime()` is the earliest point available. Class
 * loading and framework initialisation therefore fall *inside* the
 * measured uptime — which is the point. On a WebFlux service with Kafka
 * consumers that is easily twenty seconds, and they are the twenty most
 * interesting seconds of a deployment.
 *
 * A start instant captured later, at an application's composition root,
 * would lose exactly that window. The constructor parameter is a test
 * seam, not an extension point.
 *
 * ## Configuration is frozen at start
 *
 * `start()` runs on Joran's configuration thread; `writeTo()` runs on
 * the encoder thread — for an async appender, a different thread
 * entirely. Rather than rely on the happens-before edge that the
 * appender start-up sequence probably establishes, all configuration is
 * copied into an immutable [ProcessStartSupport.Frozen] snapshot at
 * [start], published through a single volatile reference.
 *
 * Consequences: the mutable setters are never read from the encoder
 * thread; the snapshot's fields are final and therefore safely
 * published by the JMM; and [writeTo] performs exactly one volatile
 * read. A misconfigured provider leaves the reference null and simply
 * contributes nothing.
 *
 * ## Encoder version: this class is the Jackson 3 half
 *
 * `JsonProvider.writeTo` takes the encoder's own `JsonGenerator`, whose
 * type changed with logstash-logback-encoder 9.0 —
 * `com.fasterxml.jackson.core` (8.x) versus `tools.jackson.core` (9.x).
 * One class cannot implement both signatures, so this class serves
 * **9.x** and its twin one package up serves 8.x; both share the same
 * [ProcessStartSupport]. Name the one that matches the encoder on the
 * classpath.
 *
 * ## Independence from SequencingJsonProvider
 *
 * This provider is independent of [SequencingJsonProvider]. Both may be
 * registered on the same encoder; they share no state and write
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
 *     <provider class="eu.inqudium.tabellarium.sequencing.ProcessStartJsonProvider"/>
 *     <provider class="eu.inqudium.tabellarium.sequencing.SequencingJsonProvider"/>
 *   </providers>
 * </encoder>
 * ```
 *
 * ## Configuration alongside the LogstashEncoder shortcut
 *
 * `LogstashEncoder` rejects the `<providers>` element — that is a
 * configuration error, and since logstash-logback-encoder 7.3 it throws
 * rather than merely reporting a status. Additional providers are
 * registered with the singular `<provider class="...">` element, which
 * Joran binds to `LogstashEncoder.addProvider`.
 *
 * Because the method is named `addProvider`, Joran treats `provider` as
 * a **collection property**: the element may be repeated, exactly as
 * `<filter>` may be on an appender. `JsonProviders.addProvider` appends
 * to a list; it does not replace. Two providers therefore need two
 * sibling elements, not a plural container:
 *
 * ```xml
 * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
 *   <provider class="eu.inqudium.tabellarium.sequencing.ProcessStartJsonProvider">
 *     <startField>process_start</startField>
 *     <uptimeField>process_uptime_ms</uptimeField>
 *   </provider>
 *   <provider class="eu.inqudium.tabellarium.sequencing.SequencingJsonProvider">
 *     <sequenceField>log_encoder_sequence</sequenceField>
 *   </provider>
 * </encoder>
 * ```
 *
 * With [SequencingLogstashEncoder] only one element is needed, because
 * that encoder already registers a [SequencingJsonProvider] itself:
 *
 * ```xml
 * <encoder class="eu.inqudium.tabellarium.sequencing.SequencingLogstashEncoder">
 *   <sequenceField>log_encoder_sequence</sequenceField>
 *   <provider class="eu.inqudium.tabellarium.sequencing.ProcessStartJsonProvider">
 *     <startField>process_start</startField>
 *     <uptimeField>process_uptime_ms</uptimeField>
 *   </provider>
 * </encoder>
 * ```
 *
 * Additional providers run *after* the built-in ones. Field order in
 * JSON carries no meaning — unless two providers write the same key, in
 * which case the object holds it twice, Jackson raises nothing, and
 * Elasticsearch keeps the last occurrence.
 *
 * ## Mapping
 *
 * The field names are FLAT (underscore-separated), NOT dotted, so
 * Elasticsearch stores them as top-level keys rather than expanding a
 * `process.*` object - which is what avoids the mapping/nesting conflicts
 * a dotted `process.start` would create against other `process.*` users.
 *
 * ```json
 * "process_start":      { "type": "date" },
 * "process_uptime_ms":  { "type": "long" }
 * ```
 *
 * ## Finality
 *
 * The class is deliberately not `open`. Joran instantiates it by
 * reflection and configures it through setters; nothing in the encoder
 * chain requires a subclass. Were it open, [writeTo] would be
 * overridable — and [writeTo] reading a clock instead of the event
 * timestamp is exactly the defect this class exists to prevent. The
 * invariant is enforced by the type system rather than requested by the
 * documentation.
 *
 * @param support the shared, Jackson-free state, which also resolves
 *   the JVM start time. Because the parameter has a default, Kotlin
 *   also emits the public no-argument constructor that Joran requires.
 */
class ProcessStartJsonProvider(
    private val support: ProcessStartSupport = ProcessStartSupport(),
) : AbstractJsonProvider<ILoggingEvent>() {
    /**
     * Secondary constructor for the test seam: resolves the JVM start
     * time through [processStartMillisSupplier] instead of the
     * `RuntimeMXBean`.
     */
    constructor(processStartMillisSupplier: () -> Long) : this(ProcessStartSupport(processStartMillisSupplier))

    /**
     * The JSON key for the JVM start instant, written as an ISO-8601
     * string. Defaults to `process_start` - a FLAT field, deliberately
     * not the dotted ECS `process.start`, which Elasticsearch would
     * expand into a `process` object and so risk a mapping conflict.
     *
     * Read only during [start]. Mutating it afterwards has no effect.
     */
    var startField: String
        get() = support.startField
        set(value) {
            support.startField = value
        }

    /**
     * The JSON key for the per-event uptime in milliseconds, written as
     * a native JSON number. Defaults to `process_uptime_ms` (flat, like
     * [startField]).
     *
     * There is no ECS field for process uptime (ECS defines only
     * `process.start`), so this name is entirely ours; the flat form also
     * keeps it clear of any future ECS `process.*` object.
     *
     * Read only during [start]. Mutating it afterwards has no effect.
     */
    var uptimeField: String
        get() = support.uptimeField
        set(value) {
            support.uptimeField = value
        }

    /**
     * Emit [startField]. Disable when the start instant is already
     * contributed by another provider and only the uptime is wanted.
     */
    var includeStart: Boolean
        get() = support.includeStart
        set(value) {
            support.includeStart = value
        }

    /**
     * Emit [uptimeField]. Disable to store only the start instant and
     * derive uptime in Elasticsearch as a runtime field. That trades
     * index size for query-time computation.
     */
    var includeUptime: Boolean
        get() = support.includeUptime
        set(value) {
            support.includeUptime = value
        }

    override fun start() {
        if (isStarted) {
            addWarn("ProcessStartJsonProvider already started; ignoring repeated start()")
            return
        }

        val result = support.freeze()
        result.error?.let {
            addError(it)
            return
        }
        result.warning?.let { addWarn(it) }
        result.info?.let { addInfo(it) }
        super.start()
    }

    /**
     * Writes the start instant as a JSON string and the uptime as a
     * native JSON number.
     *
     * Contributes nothing when the provider is not started. That case is
     * reachable in two ways: [start] rejected the resolved start instant,
     * or the encoder never propagated `start()` at all. Neither may throw
     * from here — a logging subsystem that throws while logging turns a
     * misconfiguration into an application-wide outage. The diagnosis
     * already sits on the Logback status manager; the correct behaviour
     * in the logging path is silence.
     *
     * A negative uptime means the resolved start instant lies after the
     * event timestamp. The value is written as-is rather than clamped: a
     * visibly negative number in Kibana is a better outcome than a
     * silently plausible zero.
     */
    override fun writeTo(
        generator: JsonGenerator,
        event: ILoggingEvent,
    ) {
        val config = support.frozen ?: return

        if (config.includeStart) {
            generator.writeStringProperty(config.startField, config.startIso)
        }
        if (config.includeUptime) {
            generator.writeNumberProperty(config.uptimeField, event.timeStamp - config.startMillis)
        }
    }

    companion object {
        const val DEFAULT_START_FIELD: String = ProcessStartSupport.DEFAULT_START_FIELD
        const val DEFAULT_UPTIME_FIELD: String = ProcessStartSupport.DEFAULT_UPTIME_FIELD
    }
}
