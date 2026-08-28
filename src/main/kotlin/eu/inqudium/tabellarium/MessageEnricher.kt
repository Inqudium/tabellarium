package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.MessageEnricher.Companion.DEFAULT_TRACE_ID_EXTRACTOR
import eu.inqudium.tabellarium.MessageEnricher.Companion.TRACE_ID_MDC_KEY

/**
 * Enriches logging events with static metadata and a per-event partitioning key.
 *
 * The enricher is a **pure function**: it never mutates the incoming logging event,
 * holds no per-call state, and returns the same immutable header map instance
 * across all calls.
 *
 * ## What gets enriched
 *
 * - **Static headers** - assembled once at construction time from [component],
 *   [cmdbId], and [environment], plus a fixed agent name and version. Values
 *   are pre-encoded as UTF-8 byte arrays so the hot path performs zero
 *   conversion. The same immutable map instance is reused for every event.
 * - **Partitioning key** - derived per event from the configured
 *   [partitioningKeyExtractor]. The default extractor reads the [TRACE_ID_MDC_KEY]
 *   entry from the event's MDC and returns it if non-blank; otherwise null.
 *
 * Callers attach the enrichment result to a Kafka `ProducerRecord`: the
 * partitioning key becomes the record key (UTF-8 encoded by the sender, since
 * it varies per event), the headers become record headers passed by reference
 * to Kafka's `Headers.add(String, byte[])`. Kafka does not defensive-copy the
 * header value arrays, so the byte arrays in [EnrichedRecord.headers] MUST
 * be treated as read-only by all consumers.
 *
 * ## Input validation
 *
 * The constructor rejects blank values for [component], [cmdbId], and
 * [environment]; an enricher with missing metadata should never produce records,
 * because the records could not be correlated back to the originating service.
 *
 * ## Custom extractors
 *
 * The default partitioning strategy is "use the MDC trace id", which fits the
 * Spring Boot + Sleuth/Micrometer Tracing setup. Callers that need a different
 * strategy (session id, user id, account id, etc.) pass a custom
 * [partitioningKeyExtractor]; the enricher applies the same blank-as-null
 * normalization to its output regardless of which extractor is configured.
 *
 * @param component The service component identifier (e.g. `spring.application.name`).
 * @param cmdbId The CMDB identifier of the deploying instance.
 * @param environment The deployment environment (e.g. `prod`, `staging`, `dev`).
 * @param partitioningKeyExtractor Function that returns a partitioning key for an
 *                                 event, or null/blank to omit the key. Defaults
 *                                 to [DEFAULT_TRACE_ID_EXTRACTOR].
 *
 * @throws IllegalArgumentException if any of [component], [cmdbId], or
 *                                  [environment] is blank.
 */
class MessageEnricher(
    component: String,
    cmdbId: String,
    environment: String,
    private val partitioningKeyExtractor: (ILoggingEvent) -> String? = DEFAULT_TRACE_ID_EXTRACTOR,
) {
    /**
     * Pre-encoded UTF-8 byte arrays of the static header values, keyed
     * by header name. Encoded once at construction time and shared
     * across all [enrich] calls so the hot path produces zero
     * allocations for the header set (one allocation for the
     * partitioning key remains, since that varies per event).
     *
     * The byte arrays are passed by reference to Kafka's
     * `ProducerRecord.headers()`, which does not defensive-copy them.
     * Callers must NOT mutate the arrays they receive in
     * [EnrichedRecord.headers]; doing so would corrupt subsequent
     * events.
     */
    private val staticHeaders: Map<String, ByteArray>

    init {
        require(component.isNotBlank()) { "Component must not be blank" }
        require(cmdbId.isNotBlank()) { "CMDB id must not be blank" }
        require(environment.isNotBlank()) { "Environment must not be blank" }

        // UTF-8 encode each header value ONCE here, not per event in the
        // hot path. Map.copyOf returns a guaranteed-immutable map: attempts
        // to modify it throw UnsupportedOperationException. The byte arrays
        // themselves remain mutable (Kotlin/Java has no built-in immutable
        // byte array), so callers must treat them as read-only by convention.
        staticHeaders =
            java.util.Map.copyOf(
                mapOf(
                    HEADER_COMPONENT to component.toByteArray(Charsets.UTF_8),
                    HEADER_CMDB_ID to cmdbId.toByteArray(Charsets.UTF_8),
                    HEADER_ENVIRONMENT to environment.toByteArray(Charsets.UTF_8),
                    HEADER_AGENT_NAME to AGENT_NAME.toByteArray(Charsets.UTF_8),
                    HEADER_AGENT_VERSION to AGENT_VERSION.toByteArray(Charsets.UTF_8),
                ),
            )
    }

    /**
     * Enriches the given event and returns the resulting [EnrichedRecord].
     *
     * The [EnrichedRecord.headers] is the shared immutable header map computed
     * at construction time. The [EnrichedRecord.partitioningKey] is non-null
     * only when the configured extractor returned a non-blank value.
     *
     * This method does not modify [event] in any way.
     */
    fun enrich(event: ILoggingEvent): EnrichedRecord {
        val key = partitioningKeyExtractor(event)?.takeIf { it.isNotBlank() }
        return EnrichedRecord(
            partitioningKey = key,
            headers = staticHeaders,
        )
    }

    companion object {
        /** Header key for the service component identifier. */
        const val HEADER_COMPONENT: String = "meta.component"

        /** Header key for the CMDB identifier. */
        const val HEADER_CMDB_ID: String = "meta.cmdbId"

        /** Header key for the deployment environment. */
        const val HEADER_ENVIRONMENT: String = "meta.environment"

        /** Header key for the agent (this library) name. */
        const val HEADER_AGENT_NAME: String = "meta.agent.name"

        /** Header key for the agent (this library) version. */
        const val HEADER_AGENT_VERSION: String = "meta.agent.version"

        /** Fixed value for the agent name. */
        const val AGENT_NAME: String = "logback-kafka-appender"

        /**
         * The library version, read once from a build-time-filtered
         * classpath resource so the header can never drift from the
         * actual artifact version (the pom's `revision`). Falls back to
         * `"unknown"` when the resource is missing (e.g. exotic
         * repackaging) - a visible signal rather than a stale lie.
         */
        val AGENT_VERSION: String = loadAgentVersion()

        private fun loadAgentVersion(): String =
            try {
                MessageEnricher::class.java
                    .getResourceAsStream("/tabellarium-version.properties")
                    ?.use { stream ->
                        java.util
                            .Properties()
                            .apply { load(stream) }
                            .getProperty("version")
                    }?.takeIf { it.isNotBlank() } ?: "unknown"
            } catch (_: Exception) {
                "unknown"
            }

        /** Default MDC key from which the trace id is read for partitioning. */
        const val TRACE_ID_MDC_KEY: String = "traceId"

        /**
         * Default partitioning key extractor: reads [TRACE_ID_MDC_KEY] from the
         * event's MDC map. Returns null if the MDC map is absent, the key is
         * absent, or the value is blank.
         */
        val DEFAULT_TRACE_ID_EXTRACTOR: (ILoggingEvent) -> String? = { event ->
            event.mdcPropertyMap?.get(TRACE_ID_MDC_KEY)?.takeIf { it.isNotBlank() }
        }
    }
}

/**
 * Result of enriching a logging event with Kafka-record metadata.
 *
 * Carries the per-event partitioning key and the static metadata headers
 * that should be attached to the resulting Kafka producer record by a
 * downstream sender.
 *
 * ## Header byte arrays
 *
 * [headers] holds pre-UTF-8-encoded byte arrays, ready to pass directly
 * to Kafka's [org.apache.kafka.common.header.Headers.add]. The encoding
 * is performed once by the [MessageEnricher] at construction time, not
 * per event - this avoids ~5 byte-array allocations per log event in
 * the hot path of a high-volume service.
 *
 * Kafka does not defensive-copy header value arrays; they are stored
 * by reference. Callers MUST therefore treat the byte arrays as
 * read-only. Mutating them would corrupt subsequent events that share
 * the same [MessageEnricher] instance and would also corrupt records
 * already accepted by Kafka but not yet serialized to the wire.
 *
 * The map itself is guaranteed immutable (built via
 * [java.util.Map.copyOf] in the enricher); attempts to put or remove
 * entries throw [UnsupportedOperationException].
 *
 * ## Partitioning key
 *
 * [partitioningKey] is a per-event String because it varies per event
 * (typically the MDC trace id). The sender UTF-8 encodes it on each
 * send - one allocation per event, unavoidable.
 *
 * ## Identity semantics
 *
 * Deliberately NOT a `data class`: the header values are byte arrays,
 * for which generated `equals`/`hashCode` would compare by reference -
 * two records with identical content would not be equal, and `copy()`
 * would silently share the mutable arrays. Instances compare by
 * identity; there is no use case for value equality on this type.
 *
 * ## Why [headers] is `internal`
 *
 * The shared byte arrays are safe only as long as nobody mutates
 * them. Keeping the map off the public API shrinks that read-only
 * contract from "every consumer of the library" to "code in this
 * module" - the only code that ever touches the arrays is the
 * enricher (writes once) and the sender (hands them to Kafka, which
 * does not mutate header values).
 *
 * @param partitioningKey The Kafka record key. Null means "no key": the
 *                        producer will then distribute records via its
 *                        configured partitioner (sticky-random by default).
 * @param headers Immutable map of header names to pre-encoded UTF-8
 *                value bytes. Same instance across all enrich calls of
 *                a given enricher. Byte arrays must NOT be mutated.
 */
class EnrichedRecord internal constructor(
    val partitioningKey: String?,
    internal val headers: Map<String, ByteArray>,
)
