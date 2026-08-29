package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.tabellarium.MessageEnricher.Companion.DEFAULT_TRACE_ID_EXTRACTOR
import eu.inqudium.tabellarium.MessageEnricher.Companion.TRACE_ID_MDC_KEY
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import java.util.Properties

/**
 * Enriches logging events with static metadata and a per-event partitioning key.
 *
 * The enricher is a **pure function**: it never mutates the incoming logging event,
 * holds no per-call state, and returns the same immutable header list instance
 * across all calls.
 *
 * ## What gets enriched
 *
 * - **Static headers** - assembled once at construction time from [component],
 *   [cmdbId], and [environment], plus a fixed agent name and version. Each
 *   header is pre-built as a complete [Header] wrapping the UTF-8-encoded
 *   value bytes, so the hot path performs zero conversion and zero wrapper
 *   allocation. The same immutable list instance is reused for every event.
 * - **Partitioning key** - derived per event from the configured
 *   [partitioningKeyExtractor]. The default extractor reads the [TRACE_ID_MDC_KEY]
 *   entry from the event's MDC and returns it if non-blank; otherwise null.
 *
 * Callers attach the enrichment result to a Kafka `ProducerRecord`: the
 * partitioning key becomes the record key (UTF-8 encoded by the sender, since
 * it varies per event), the header list is passed by reference to the record
 * constructor - read-only by convention; the full contract lives on
 * [EnrichedRecord.headers].
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
 * [partitioningKeyExtractor]; the enricher applies the same normalization
 * to its output regardless of which extractor is configured - blank and
 * over-long values (see [MAX_PARTITIONING_KEY_LENGTH]) both become "no
 * key".
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
internal class MessageEnricher(
    component: String,
    cmdbId: String,
    environment: String,
    private val partitioningKeyExtractor: (ILoggingEvent) -> String? = DEFAULT_TRACE_ID_EXTRACTOR,
) {
    /**
     * Pre-built [Header] instances for the static metadata, each
     * wrapping its UTF-8-encoded value bytes. Built once at
     * construction time and shared across all [enrich] calls so the
     * hot path produces zero allocations for the header set (one
     * allocation for the partitioning key remains, since that varies
     * per event). The wrappers and their arrays are shared and
     * read-only by convention - see [EnrichedRecord.headers].
     */
    private val staticHeaders: List<Header>

    init {
        require(component.isNotBlank()) { "Component must not be blank" }
        require(cmdbId.isNotBlank()) { "CMDB id must not be blank" }
        require(environment.isNotBlank()) { "Environment must not be blank" }

        // UTF-8 encode each header value and wrap it in its RecordHeader
        // ONCE here, not per event in the hot path. List.copyOf returns a
        // guaranteed-immutable list: attempts to modify it throw
        // UnsupportedOperationException. (The value arrays inside stay
        // mutable - see EnrichedRecord.headers for the read-only
        // convention.)
        staticHeaders =
            java.util.List.copyOf(
                listOf(
                    RecordHeader(HEADER_COMPONENT, component.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_CMDB_ID, cmdbId.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_ENVIRONMENT, environment.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_AGENT_NAME, AGENT_NAME.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_AGENT_VERSION, AGENT_VERSION.toByteArray(Charsets.UTF_8)),
                ),
            )
    }

    /**
     * Enriches the given event and returns the resulting [EnrichedRecord].
     *
     * The [EnrichedRecord.headers] is the shared immutable header list built
     * at construction time. The [EnrichedRecord.partitioningKey] is non-null
     * only when the configured extractor returned a non-blank value **of at
     * most [MAX_PARTITIONING_KEY_LENGTH] characters** - see
     * [MAX_PARTITIONING_KEY_LENGTH] for why an oversized key is treated as
     * absent rather than truncated.
     *
     * This method does not modify [event] in any way.
     */
    fun enrich(event: ILoggingEvent): EnrichedRecord {
        val key =
            partitioningKeyExtractor(event)
                ?.takeIf { it.isNotBlank() && it.length <= MAX_PARTITIONING_KEY_LENGTH }
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
                        Properties()
                            .apply { load(stream) }
                            .getProperty("version")
                    }?.takeIf { it.isNotBlank() } ?: "unknown"
            } catch (_: Exception) {
                "unknown"
            }

        /** Default MDC key from which the trace id is read for partitioning. */
        const val TRACE_ID_MDC_KEY: String = "traceId"

        /**
         * Upper bound on the partitioning key, in characters. A longer
         * value is treated as **absent** (no key), exactly like a blank
         * one.
         *
         * ## Why a bound exists
         *
         * The key is taken from the log event (by default the MDC trace
         * id) and becomes the Kafka record key verbatim. Applications
         * routinely bridge an inbound request header into the MDC, so
         * that value can be attacker-influenced. Without a bound, a
         * multi-hundred-kilobyte header inflates every record past
         * `max.request.size`; the resulting `RecordTooLargeException` is
         * deliberately ignored by the circuit breaker (it is a payload
         * problem, not a broker-health problem - see
         * [ResilientMessageSender]), so the breaker never opens and every
         * such event is routed to the fallback appender indefinitely.
         *
         * ## Why absent rather than truncated
         *
         * A truncated prefix would still be attacker-chosen, so it would
         * still steer the record onto a partition of their choosing -
         * truncation removes the inflation but keeps the steering. A
         * missing key hands partition selection back to the producer's
         * partitioner, which is the safe default. Note that a key within
         * the bound is passed through unchanged: partition selection is
         * key-driven by design, so an application that bridges
         * unvalidated inbound values into the MDC can still influence
         * distribution. Bounding the length is this component's part;
         * not trusting inbound headers is the application's.
         *
         * 128 characters is far above every established trace-id format
         * (W3C `traceparent` and B3 trace ids are 32 hex characters, a
         * UUID is 36) and above any plausible session/account key used
         * by a custom extractor.
         */
        const val MAX_PARTITIONING_KEY_LENGTH: Int = 128

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
 * ## Shared header instances
 *
 * This section is the canonical statement of the shared-header
 * read-only contract; the enricher's and sender's comments refer
 * here instead of repeating it.
 *
 * [headers] holds pre-built [Header] instances wrapping
 * pre-UTF-8-encoded value byte arrays, ready to pass directly to the
 * `ProducerRecord` constructor that accepts an `Iterable<Header>`.
 * Both the encoding and the wrappers are created once by the
 * [MessageEnricher] at construction time, not per event - this avoids
 * ~5 byte-array plus ~5 wrapper allocations per log event in the hot
 * path of a high-volume service.
 *
 * Kafka does not defensive-copy headers: the record stores the
 * [Header] references, and each wrapper stores its value array by
 * reference. Callers MUST therefore treat the wrappers and their byte
 * arrays as read-only. [RecordHeader] itself is immutable, but
 * mutating a value array would corrupt subsequent events that share
 * the same [MessageEnricher] instance and would also corrupt records
 * already accepted by Kafka but not yet serialized to the wire.
 *
 * The list itself is guaranteed immutable (built via
 * [java.util.List.copyOf] in the enricher); attempts to add or remove
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
 * Deliberately NOT a `data class`: instances compare by identity -
 * there is no use case for value equality on this type, and a
 * generated `copy()` would silently share the mutable value arrays
 * behind the headers.
 *
 * ## Why the type is `internal`
 *
 * The shared byte arrays are safe only as long as nobody mutates
 * them. Keeping the whole type (and with it [headers]) off the public
 * API shrinks that read-only contract from "every consumer of the
 * library" to "code in this module" - the only code that ever touches
 * the arrays is the enricher (writes once) and the sender (hands them
 * to Kafka, which does not mutate header values). See ADR-0002 for
 * the public-surface boundary.
 *
 * @param partitioningKey The Kafka record key. Null means "no key": the
 *                        producer will then distribute records via its
 *                        configured partitioner (sticky-random by default).
 * @param headers Immutable list of pre-built headers wrapping
 *                pre-encoded UTF-8 value bytes. Same instance across
 *                all enrich calls of a given enricher. The value byte
 *                arrays must NOT be mutated.
 */
internal class EnrichedRecord(
    val partitioningKey: String?,
    val headers: List<Header>,
)
