package eu.inqudium.tabellarium

/**
 * Joran-populated holder for the `<topicMapping>` XML element.
 *
 * ## XML format
 *
 * ```xml
 * <topicMapping>
 *   <defaultTopic>my-application.logs</defaultTopic>
 *   <defaultTopicClass>FUNCTIONAL</defaultTopicClass>  <!-- optional -->
 *   <mapping>
 *     <marker>SECURITY</marker>
 *     <topic>audit.security</topic>
 *     <topicClass>AUDIT</topicClass>
 *   </mapping>
 *   <mapping>
 *     <marker>METRICS</marker>
 *     <topic>perf.metrics</topic>
 *     <topicClass>PERFORMANCE</topicClass>
 *   </mapping>
 * </topicMapping>
 * ```
 *
 * Joran maps the `<topicMapping>` element to a [TopicMappingConfig]
 * instance because the [KafkaAppender] exposes a setter of that type.
 * Inside it, `<defaultTopic>` is routed to the [defaultTopic] property,
 * and each `<mapping>` element is materialized as a [TopicMappingEntry]
 * via the collection-population setter [addMapping] (Joran picks up
 * `addXxx` methods taking a single bean-style parameter). The entry's
 * children are plain string properties, which is the most robust Joran
 * shape - no attribute or body-text special cases.
 *
 * Events whose markers match no `<mapping>` (and events without
 * markers) route to [defaultTopic], which is classified via the
 * table's fallback class - [TopicClass.TECHNICAL] unless the optional
 * `<defaultTopicClass>` says otherwise ([defaultTopicClass]).
 *
 * ## Validation
 *
 * All structural validation happens eagerly when the appender builds
 * its pipeline ([toTopicRouter] / [toTopicTable]), so misconfiguration
 * aborts `start()` with a named error instead of surfacing per event:
 *
 * - blank marker/topic names and Kafka-invalid topic names (via
 *   [TopicRouter]'s validation),
 * - an unknown `<topicClass>` or `<defaultTopicClass>` value (must be
 *   one of the [TopicClass] constants, case-insensitive),
 * - the same marker mapped twice,
 * - the same topic mapped to two different classes - including a
 *   `<mapping>` that names [defaultTopic] with a class conflicting
 *   with [defaultTopicClass].
 */
class TopicMappingConfig {
    /**
     * The default topic for events whose markers do not match any
     * explicit mapping. Set by Joran from the `<defaultTopic>` text
     * content; whitespace is trimmed automatically.
     */
    var defaultTopic: String = ""
        set(value) {
            field = value.trim()
        }

    /**
     * Name of the [TopicClass] governing [defaultTopic] - and thereby
     * every event whose markers match no `<mapping>`. Optional; the
     * default `TECHNICAL` is the deliberately neutral choice (no
     * compliance mandate, tolerable performance defaults). Set it to
     * `AUDIT`/`FUNCTIONAL`/`PERFORMANCE` (case-insensitive) when the
     * default stream itself carries that grade - the class's producer
     * tuning and mandatory overrides then apply to the default topic
     * without a synthetic marker mapping.
     */
    var defaultTopicClass: String = TopicClass.TECHNICAL.name
        set(value) {
            field = value.trim()
        }

    private val mutableMappings = mutableListOf<TopicMappingEntry>()

    /**
     * The `<mapping>` entries in configuration order. Exposed read-only
     * so tests (and diagnostics) can inspect what Joran bound.
     */
    val mappings: List<TopicMappingEntry>
        get() = mutableMappings.toList()

    /**
     * Called by Joran for every `<mapping>` element inside
     * `<topicMapping>`.
     */
    fun addMapping(entry: TopicMappingEntry) {
        mutableMappings += entry
    }

    /**
     * Builds the [TopicRouter] from the current configuration.
     *
     * @throws IllegalArgumentException when [defaultTopic] is blank or
     *                                  Kafka-invalid, when a mapping
     *                                  carries a blank marker/topic or
     *                                  a Kafka-invalid topic name, or
     *                                  when the same marker is mapped
     *                                  more than once.
     */
    fun toTopicRouter(): TopicRouter {
        val duplicateMarkers =
            mutableMappings
                .groupBy { it.marker }
                .filterValues { it.size > 1 }
                .keys
        require(duplicateMarkers.isEmpty()) {
            "Each marker may be mapped to exactly one topic; mapped more than once: " +
                duplicateMarkers.joinToString { "'$it'" }
        }
        return TopicRouter(
            defaultTopic = defaultTopic,
            markerMappings = mutableMappings.associate { it.marker to it.topic },
        )
    }

    /**
     * Builds the [TopicTable] from the current configuration. Topics
     * without an explicit `<mapping>` - including [defaultTopic] -
     * resolve to the fallback class configured via
     * `<defaultTopicClass>` ([TopicClass.TECHNICAL] by default).
     *
     * @throws IllegalArgumentException when a `<topicClass>` or
     *                                  `<defaultTopicClass>` value is
     *                                  not a [TopicClass] constant, or
     *                                  when the same topic is assigned
     *                                  two different classes (including
     *                                  a `<mapping>` naming
     *                                  [defaultTopic] with a class that
     *                                  conflicts with
     *                                  [defaultTopicClass]).
     */
    fun toTopicTable(): TopicTable {
        val fallbackClass = resolvedDefaultTopicClass()
        val byTopic = mutableMappings.groupBy({ it.topic }, { it.resolvedTopicClass() })
        val conflicting = byTopic.filterValues { it.toSet().size > 1 }
        require(conflicting.isEmpty()) {
            "Each topic must map to exactly one topic class; conflicting assignments: " +
                conflicting.entries.joinToString { (topic, classes) ->
                    "'$topic' -> ${classes.toSet().joinToString()}"
                }
        }
        // A <mapping> may name the default topic (e.g. to route a marker
        // there explicitly), but not with a class contradicting
        // <defaultTopicClass> - marker-less events and mapped events on
        // the same topic must never diverge in delivery guarantees.
        byTopic[defaultTopic]?.first()?.let { mappedClass ->
            require(mappedClass == fallbackClass) {
                "Topic '$defaultTopic' is the default topic (class $fallbackClass via " +
                    "<defaultTopicClass>) but a <mapping> assigns it $mappedClass; " +
                    "the two must agree"
            }
        }
        return TopicTable(
            topicsByName = byTopic.mapValues { (_, classes) -> classes.first() },
            fallbackClass = fallbackClass,
        )
    }

    private fun resolvedDefaultTopicClass(): TopicClass =
        TopicClass.entries.firstOrNull { it.name.equals(defaultTopicClass, ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "Unknown <defaultTopicClass> '$defaultTopicClass'; " +
                    "must be one of ${TopicClass.entries.joinToString()}",
            )
}

/**
 * One `<mapping>` element inside `<topicMapping>`: routes events that
 * carry [marker] to [topic], and classifies [topic] as [topicClass].
 *
 * A plain Joran bean: no-arg constructor, string setters, values
 * trimmed on assignment. Validation is centralized in
 * [TopicMappingConfig.toTopicRouter] / [TopicMappingConfig.toTopicTable]
 * so every error surfaces as a named startup failure.
 */
class TopicMappingEntry {
    /** SLF4J marker name that selects this mapping. Exact, case-sensitive match. */
    var marker: String = ""
        set(value) {
            field = value.trim()
        }

    /** Kafka topic events with [marker] are routed to. */
    var topic: String = ""
        set(value) {
            field = value.trim()
        }

    /**
     * Name of the [TopicClass] governing [topic]'s producer
     * configuration (`AUDIT`, `FUNCTIONAL`, `TECHNICAL`,
     * `PERFORMANCE`); case-insensitive.
     */
    var topicClass: String = ""
        set(value) {
            field = value.trim()
        }

    internal fun resolvedTopicClass(): TopicClass =
        TopicClass.entries.firstOrNull { it.name.equals(topicClass, ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "Unknown <topicClass> '$topicClass' for marker '$marker' (topic '$topic'); " +
                    "must be one of ${TopicClass.entries.joinToString()}",
            )
}
