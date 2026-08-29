package eu.inqudium.tabellarium

import org.slf4j.Marker

/**
 * Resolves the Kafka topic name for a list of SLF4J [Marker]s.
 *
 * The router is a **pure function**: its output depends only on its construction-time
 * configuration and the [Marker]s passed to [route]. It performs no I/O, holds no
 * mutable state, and is therefore safe to share across threads and trivial to test
 * in isolation.
 *
 * ## Resolution rules
 *
 * 1. If the marker list is empty, [defaultTopic] is returned.
 * 2. For each marker in iteration order:
 *    - If the marker's name is in [markerMappings], the associated topic is returned
 *      (**direct match**, takes precedence).
 *    - Otherwise, if the marker has hierarchical references
 *      (see [Marker.hasReferences]), each referenced marker is checked the same way.
 *      The first hierarchical match wins (**single-level resolution** - references of
 *      references are not followed, which prevents accidental cycles).
 * 3. If no marker matches directly or hierarchically, [defaultTopic] is returned.
 *
 * Marker names are matched by **exact string equality**: case-sensitive, no
 * whitespace trimming. The router assumes that its inputs have already been
 * normalized by the caller (typically a config-parsing builder that trims values
 * coming from XML).
 *
 * ## Input validation
 *
 * The constructor rejects any configuration that would lead to silent failures
 * at runtime:
 *
 * - The default topic must not be blank.
 * - All mapped marker names must not be blank.
 * - All mapped topic names must not be blank.
 * - All topic names must match the Kafka-permitted character set
 *   `[a-zA-Z0-9._-]+` (Kafka rejects spaces and other characters at the broker),
 *   must not be the reserved names `.` or `..`, and must not exceed
 *   Kafka's maximum topic-name length of 249 characters.
 *
 * Violations raise an [IllegalArgumentException] at construction time, never later.
 *
 * @param defaultTopic Topic to fall back to when no marker matches.
 * @param markerMappings Marker-name to topic-name map. Each marker name maps to
 *                       exactly one topic; if multiple categories share a marker,
 *                       the caller must resolve the collision before constructing
 *                       the router.
 *
 * @throws IllegalArgumentException if any input is blank or any topic name contains
 *                                  characters that Kafka does not permit.
 */
internal class TopicRouter(
    private val defaultTopic: String,
    private val markerMappings: Map<String, String>,
) {
    init {
        require(defaultTopic.isNotBlank()) {
            "Default topic must not be blank"
        }
        requireKafkaValidTopicName(defaultTopic) { "Default topic name" }
        markerMappings.forEach { (marker, topic) ->
            require(marker.isNotBlank()) {
                "Marker name must not be blank (mapped to topic '$topic')"
            }
            require(topic.isNotBlank()) {
                "Topic name for marker '$marker' must not be blank"
            }
            requireKafkaValidTopicName(topic) { "Topic name for marker '$marker'" }
        }
    }

    /**
     * Enforces Kafka's full topic-name rules, mirroring
     * `org.apache.kafka.common.internals.Topic.validate`: permitted
     * character set, the reserved names `.` and `..`, and the maximum
     * length of 249. Anything the broker would reject must fail HERE,
     * at construction - a name that passes startup but fails per send
     * would divert every event to the fallback while the breaker
     * (which deliberately ignores InvalidTopicException) reports a
     * healthy pipeline.
     */
    private inline fun requireKafkaValidTopicName(
        topic: String,
        what: () -> String,
    ) {
        require(topic != "." && topic != "..") {
            "${what()} must not be '.' or '..' (reserved by Kafka): '$topic'"
        }
        require(topic.length <= KAFKA_MAX_TOPIC_NAME_LENGTH) {
            "${what()} exceeds Kafka's maximum length of $KAFKA_MAX_TOPIC_NAME_LENGTH characters " +
                "(got ${topic.length}): '${topic.take(64)}…'"
        }
        require(topic.matches(KAFKA_TOPIC_PATTERN)) {
            "${what()} contains characters not permitted by Kafka: '$topic'"
        }
    }

    /**
     * Returns the Kafka topic name for the given list of markers.
     *
     * See the class documentation for the resolution algorithm.
     *
     * @param markers Markers attached to a log event. Order matters: the first
     *                marker that produces a match (direct or hierarchical) wins.
     * @return The resolved topic name, or [defaultTopic] if no marker matches.
     */
    fun route(markers: List<Marker>): String {
        if (markers.isEmpty()) return defaultTopic

        for (marker in markers) {
            val direct = markerMappings[marker.name]
            if (direct != null) return direct

            if (marker.hasReferences()) {
                val iterator = marker.iterator()
                while (iterator.hasNext()) {
                    val referenced = iterator.next()
                    val hierarchical = markerMappings[referenced.name]
                    if (hierarchical != null) return hierarchical
                }
            }
        }

        return defaultTopic
    }

    private companion object {
        // Kafka topic name validation: letters, digits, dot, underscore, hyphen.
        // See org.apache.kafka.common.internals.Topic.containsValidPattern.
        private val KAFKA_TOPIC_PATTERN = Regex("[a-zA-Z0-9._\\-]+")

        // See org.apache.kafka.common.internals.Topic.MAX_NAME_LENGTH.
        private const val KAFKA_MAX_TOPIC_NAME_LENGTH = 249
    }
}
