package eu.inqudium.tabellarium

/**
 * Joran-populated holder for the legacy `<topicMapping>` XML element.
 *
 * ## Drop-in compatibility
 *
 * The current legacy XML format observed in production is minimal:
 *
 * ```xml
 * <topicMapping>
 *   <defaultTopic>ichp-de.customerproducts.out</defaultTopic>
 * </topicMapping>
 * ```
 *
 * Joran maps the `<topicMapping>` element to a [TopicMappingConfig]
 * instance because the [KafkaAppender] exposes a setter of that type.
 * Inside it, the `<defaultTopic>` sub-element is routed to the
 * [defaultTopic] property (via the generated `setDefaultTopic` setter).
 *
 * ## Future extension
 *
 * The [TopicRouter] already supports marker-to-topic mappings, and the
 * [TopicTable] already supports per-topic class assignments. When the
 * configuration grows to include those, the natural extension is a
 * nested element per [TopicClass], for example:
 *
 * ```xml
 * <topicMapping>
 *   <defaultTopic>default.topic</defaultTopic>
 *   <audit>
 *     <entry marker="SECURITY">audit.security</entry>
 *     <entry marker="MONEY">audit.transactions</entry>
 *   </audit>
 *   <technical>
 *     <entry marker="DEBUG">tech.debug</entry>
 *   </technical>
 * </topicMapping>
 * ```
 *
 * That would map to `addAudit(MarkerEntry)` / `addTechnical(MarkerEntry)`
 * setters on this class (Joran picks up `addXxx` methods that take a
 * single bean-style parameter as collection-population setters). The
 * `MarkerEntry` would be a tiny class with `setMarker(String)` and
 * `setTopic(String)` setters plus a `setValue(String)` for the text
 * content. Until that need arises this class deliberately stays
 * minimal - every Joran setter that exists must be tested, so adding
 * them speculatively is YAGNI.
 *
 * ## Defaults when only `<defaultTopic>` is configured
 *
 * - The [TopicRouter] returned from [toTopicRouter] has no marker
 *   mappings: every event resolves to [defaultTopic].
 * - The [TopicTable] returned from [toTopicTable] has no explicit
 *   topic-to-class assignments: every topic - including the default -
 *   resolves to [TopicClass.TECHNICAL] via the fallback. Only one
 *   producer (the TECHNICAL one) is instantiated, matching the
 *   semantics of the legacy single-producer appender.
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
     * Builds the [TopicRouter] from the current configuration.
     *
     * @throws IllegalArgumentException via [TopicRouter]'s own validation
     *                                  when [defaultTopic] is blank or
     *                                  contains characters Kafka does not
     *                                  permit.
     */
    fun toTopicRouter(): TopicRouter =
        TopicRouter(
            defaultTopic = defaultTopic,
            markerMappings = emptyMap(),
        )

    /**
     * Builds the [TopicTable] from the current configuration. With only
     * a `<defaultTopic>` configured, the table has no explicit
     * topic-to-class assignments and every topic resolves to the
     * fallback class.
     */
    fun toTopicTable(): TopicTable =
        TopicTable(
            topicsByName = emptyMap(),
            fallbackClass = TopicClass.TECHNICAL,
        )
}
