package eu.inqudium.tabellarium

import ch.qos.logback.core.Appender
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.utils.AppInfoParser

/**
 * The configuration metadata an appender publishes as info gauges
 * (`kafka.appender.info`, `kafka.appender.topic.info`): constant-1
 * series whose labels carry the facts, joined onto the appender's other
 * series with `group_left` (the Prometheus `*_info` convention).
 *
 * Every field is fixed for the lifetime of a started appender - the
 * fallback slot refuses changes while started, the topic mapping and
 * the producer properties are built once in `start()` - so the labels
 * never change under a live series. Only low-cardinality, credential-free
 * values belong here: the producer properties themselves are raw and may
 * hold secrets (see [ProducerRegistry.effectiveProperties]), so only the
 * named settings in [TopicInfo] are extracted.
 *
 * @param fallback Name of the fallback appender, [NONE] without one,
 *                 `unnamed` for one without a name.
 * @param fallbackType Simple class name of the fallback appender, [NONE]
 *                     without one.
 */
internal class AppenderInfo(
    val fallback: String,
    val fallbackType: String,
    val debug: Boolean,
    val topics: List<TopicInfo>,
    val version: String = MessageEnricher.AGENT_VERSION,
    val kafkaClientVersion: String = AppInfoParser.getVersion(),
) {
    companion object {
        /** Label value for an absent fallback appender. */
        const val NONE: String = "none"

        /**
         * Collects the info of a started appender: its fallback slot, its
         * debug flag, every topic the router can resolve to ([topics] -
         * mapped topics plus the default topic) and the per-class producer
         * settings the producers were created with ([effectiveProperties]).
         */
        fun of(
            fallbackAppender: Appender<*>?,
            debug: Boolean,
            topics: Map<String, TopicClass>,
            effectiveProperties: Map<TopicClass, Map<String, String>>,
        ): AppenderInfo =
            AppenderInfo(
                fallback = fallbackAppender?.let { it.name?.takeIf(String::isNotBlank) ?: "unnamed" } ?: NONE,
                fallbackType = fallbackAppender?.javaClass?.simpleName?.takeIf(String::isNotBlank) ?: NONE,
                debug = debug,
                topics =
                    topics.entries
                        .sortedBy { it.key }
                        .map { (topic, topicClass) ->
                            val properties = effectiveProperties[topicClass].orEmpty()
                            TopicInfo(
                                topic = topic,
                                topicClass = topicClass,
                                acks = effectiveSetting(properties, ProducerConfig.ACKS_CONFIG),
                                lingerMs = effectiveSetting(properties, ProducerConfig.LINGER_MS_CONFIG),
                            )
                        },
            )

        /**
         * The value the producer runs with: the merged property, or -
         * when the builder left the key out (e.g. `acks` under explicit
         * idempotence) - the Kafka client's own default.
         */
        private fun effectiveSetting(
            properties: Map<String, String>,
            key: String,
        ): String =
            properties[key]?.trim()?.takeIf(String::isNotEmpty)
                ?: ProducerConfig.configDef().defaultValues()[key]?.toString()
                ?: "unknown"
    }
}

/**
 * One configured topic with its class and the latency-relevant producer
 * settings of that class (see the metrics overview, "Each class has its
 * own floor").
 */
internal class TopicInfo(
    val topic: String,
    val topicClass: TopicClass,
    val acks: String,
    val lingerMs: String,
)
