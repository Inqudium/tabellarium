package eu.inqudium.tabellarium

/**
 * The per-event functions of a [KafkaAppender] that never change while
 * it runs: marker routing ([TopicRouter]), topic classification
 * ([TopicTable]) and record enrichment ([MessageEnricher]).
 *
 * Derived from an [AppenderConfig] alone. The three components are pure
 * - they hold no producer, no queue, no worker - so constructing a plan
 * cannot leak anything and needs no rollback and no `close()`. That is
 * the boundary [AppenderPipeline.build] used to mark in prose ("from
 * here on real resources exist"); this type carries it instead. The hot
 * path in [KafkaAppender.append] reads the plan through the pipeline
 * and can tell from the type which components it may treat as
 * immutable.
 *
 * @throws IllegalArgumentException from [from] when the topic mapping
 *         is inconsistent (unknown class, marker mapped twice, one topic
 *         under two classes) or the identity fields are blank; nothing
 *         has been created at that point.
 */
internal class RoutingPlan private constructor(
    val topicRouter: TopicRouter,
    val topicTable: TopicTable,
    val messageEnricher: MessageEnricher,
) {
    companion object {
        fun from(config: AppenderConfig): RoutingPlan =
            RoutingPlan(
                topicRouter = config.topicMapping.toTopicRouter(),
                topicTable = config.topicMapping.toTopicTable(),
                messageEnricher =
                    MessageEnricher(
                        component = config.component,
                        cmdbId = config.cmdbId,
                        environment = config.environment,
                    ),
            )
    }
}
