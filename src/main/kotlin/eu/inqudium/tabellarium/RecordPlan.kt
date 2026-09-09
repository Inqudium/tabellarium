package eu.inqudium.tabellarium

/**
 * How a log event becomes a Kafka record, fixed for the appender's
 * lifetime: which topic ([TopicRouter]), which topic class
 * ([TopicTable]), which key and headers ([MessageEnricher]).
 *
 * Derived from the routing and identity part of the configuration
 * alone. The three components are pure - they hold no producer, no
 * queue, no worker - so constructing a plan cannot leak anything and
 * there is nothing to close. [KafkaAppender.start] therefore builds the
 * plan before it opens the [KafkaTransport]: everything that can fail
 * for configuration reasons fails here, before the first resource
 * exists. The hot path in [KafkaAppender.append] reads the plan for the
 * CPU-bound steps and hands the result to the transport.
 */
internal class RecordPlan private constructor(
    val topicRouter: TopicRouter,
    val topicTable: TopicTable,
    val messageEnricher: MessageEnricher,
) {
    /**
     * The topic classes the mapping makes active - what the transport
     * has to open a producer, a breaker and a send queue for.
     */
    val activeTopicClasses: Set<TopicClass>
        get() = topicTable.activeTopicClasses

    companion object {
        /**
         * @throws IllegalArgumentException when the mapping is
         *         inconsistent (unknown class, marker mapped twice, one
         *         topic under two classes) or an identity field is
         *         blank; nothing has been created at that point.
         */
        fun from(
            topicMapping: TopicMappingConfig,
            component: String,
            cmdbId: String,
            environment: String,
        ): RecordPlan =
            RecordPlan(
                topicRouter = topicMapping.toTopicRouter(),
                topicTable = topicMapping.toTopicTable(),
                messageEnricher =
                    MessageEnricher(
                        component = component,
                        cmdbId = cmdbId,
                        environment = environment,
                    ),
            )
    }
}
