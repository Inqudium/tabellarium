package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.Encoder

/**
 * How a log event becomes a Kafka record, fixed for the appender's
 * lifetime. Two steps, because the appender books the accepted-event
 * metric between them:
 *
 * 1. [route]: which topic and which topic class, from the event's
 *    markers ([TopicRouter], [TopicTable]).
 * 2. [materialize]: the record for that route - the encoded payload
 *    ([Encoder]), the partitioning key and the headers
 *    ([MessageEnricher]).
 *
 * The plan holds no producer, no queue, no worker: every component is a
 * pure function of the event, so constructing a plan cannot leak
 * anything and there is nothing to close. [KafkaAppender.start]
 * therefore builds the plan before it opens the [KafkaTransport];
 * everything that can fail for configuration reasons fails here, before
 * the first resource exists. The encoder's lifecycle stays with the
 * appender (Joran sets it, `start()`/`stop()` drive it); the plan only
 * calls it.
 *
 * Both steps run on the logging thread and are the entire caller-side
 * cost of an append (see the README's "Which thread does what").
 */
internal class RecordPlan private constructor(
    private val topicRouter: TopicRouter,
    private val topicTable: TopicTable,
    private val messageEnricher: MessageEnricher,
    private val encoder: Encoder<ILoggingEvent>,
) {
    /**
     * The topic classes the mapping makes active - what the transport
     * has to open a producer, a breaker and a send queue for.
     */
    val activeTopicClasses: Set<TopicClass>
        get() = topicTable.activeTopicClasses

    /** Resolves the topic and the topic class from the event's markers. */
    fun route(event: ILoggingEvent): Route {
        val topicName = topicRouter.route(event.markerList ?: emptyList())
        return Route(topicName, topicTable.classFor(topicName))
    }

    /**
     * Produces the record for [route]: payload from the encoder, key and
     * headers from the enricher. Separate from [route] so a failure here
     * (an encoder bug, a throwing key extractor) can still be attributed
     * to the class the event was routed to.
     */
    fun materialize(
        route: Route,
        event: ILoggingEvent,
    ): Record =
        Record(
            route = route,
            payload = encoder.encode(event),
            enrichment = messageEnricher.enrich(event),
        )

    companion object {
        /**
         * @param encoder Started by the caller; the plan only calls it.
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
            encoder: Encoder<ILoggingEvent>,
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
                encoder = encoder,
            )
    }
}

/** Where an event goes: the resolved topic and its topic class. */
internal class Route(
    val topicName: String,
    val topicClass: TopicClass,
)

/**
 * A record ready for the transport: its [route], the encoded [payload]
 * and the [enrichment] (partitioning key and headers).
 */
internal class Record(
    val route: Route,
    val payload: ByteArray,
    val enrichment: EnrichedRecord,
)
