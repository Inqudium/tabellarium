package eu.inqudium.tabellarium

/**
 * Maps Kafka topic names to their [TopicClass], the bridge between
 * [TopicRouter]'s topic-name output and [ProducerRegistry]'s topic-class
 * input.
 *
 * The table is a **pure data structure**: immutable after construction,
 * holds no I/O, safe to share across threads.
 *
 * ## Resolution
 *
 * [classFor] looks up the class for a topic name. Unknown topics - those
 * not in the configured mapping - fall back to [fallbackClass]. This is
 * a deliberate safety net: if the [TopicRouter] returns a topic that the
 * operator forgot to classify in their `TopicMapping` configuration, the
 * appender still routes the record through *some* producer, rather than
 * crashing the log pipeline. The fallback default is [TopicClass.TECHNICAL]
 * because that class has no compliance mandates and tolerable performance
 * defaults - the most neutral choice when intent is unclear.
 *
 * ## Active classes
 *
 * [activeTopicClasses] is the set of classes the appender needs producers
 * for: every class that has at least one mapped topic, plus the
 * [fallbackClass]. The set is passed to [ProducerRegistry.create] so the
 * registry instantiates only the producers that are actually used -
 * configuring a deployment that only uses AUDIT and TECHNICAL topics does
 * not spin up dormant Functional/Performance producers and their I/O
 * threads.
 *
 * ## Input validation
 *
 * The constructor rejects blank topic names: a blank name would never
 * match a [TopicRouter] result (the router validates its own outputs)
 * and almost certainly indicates a configuration typo. The class also
 * captures a defensive copy of [topicsByName] so subsequent mutations
 * to the caller's map have no effect on the table.
 *
 * @param topicsByName Topic-name to topic-class assignments. May be empty,
 *                     in which case every topic resolves to [fallbackClass].
 * @param fallbackClass The class used when a topic is not in
 *                      [topicsByName]. Defaults to [TopicClass.TECHNICAL].
 *
 * @throws IllegalArgumentException if any key in [topicsByName] is blank.
 */
class TopicTable(
    topicsByName: Map<String, TopicClass>,
    val fallbackClass: TopicClass = TopicClass.TECHNICAL,
) {
    private val topicsToClass: Map<String, TopicClass>

    /**
     * The set of topic classes the appender needs producers for: every
     * class that has at least one mapped topic, plus [fallbackClass].
     */
    val activeTopicClasses: Set<TopicClass>

    init {
        require(topicsByName.keys.all { it.isNotBlank() }) {
            "Topic names in TopicTable must not be blank"
        }
        topicsToClass = java.util.Map.copyOf(topicsByName)
        activeTopicClasses = (topicsToClass.values + fallbackClass).toSet()
    }

    /**
     * Returns the class associated with [topic], or [fallbackClass] if the
     * topic was not explicitly mapped at construction time.
     */
    fun classFor(topic: String): TopicClass = topicsToClass[topic] ?: fallbackClass
}
