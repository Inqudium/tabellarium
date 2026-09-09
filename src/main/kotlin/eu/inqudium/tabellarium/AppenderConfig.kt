package eu.inqudium.tabellarium

/**
 * The validated Joran surface of a [KafkaAppender] as one immutable
 * value: everything that is constant for the appender's lifetime and
 * for every event it handles.
 *
 * [KafkaAppender] validates its properties field by field (each failure
 * is reported to the status manager on its own) and then builds this
 * value once in `start()`. From here on the configuration travels as a
 * whole: [RoutingPlan.from] derives the pure per-event functions from
 * it, [AppenderPipeline.build] the stateful resources. Keeping it apart
 * from the collaborators the pipeline also needs (producer factory,
 * breaker registry, fallback appender, the appender's hooks) makes the
 * build signature say which inputs are values and which carry state.
 *
 * @param kafkaProducerProperties Raw text of `<kafkaProducerProperties>`.
 * @param topicMapping The `<topicMapping>` configuration.
 * @param component Service component identifier, non-blank.
 * @param cmdbId CMDB identifier of the deploying instance, non-blank.
 * @param environment Deployment environment, non-blank.
 * @param sendQueueCapacity Capacity of each per-class send queue, positive.
 */
internal data class AppenderConfig(
    val kafkaProducerProperties: String,
    val topicMapping: TopicMappingConfig,
    val component: String,
    val cmdbId: String,
    val environment: String,
    val sendQueueCapacity: Int,
)
