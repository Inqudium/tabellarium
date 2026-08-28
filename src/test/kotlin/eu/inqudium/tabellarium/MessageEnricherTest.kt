package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class MessageEnricherTest {
    private val component = "payment-service"
    private val cmdbId = "CMDB-12345"
    private val environment = "prod"

    private fun newEnricher(): MessageEnricher = MessageEnricher(component = component, cmdbId = cmdbId, environment = environment)

    private fun newEnricherWith(extractor: (ILoggingEvent) -> String?): MessageEnricher =
        MessageEnricher(
            component = component,
            cmdbId = cmdbId,
            environment = environment,
            partitioningKeyExtractor = extractor,
        )

    /**
     * Decodes the pre-encoded UTF-8 header byte arrays back into Strings
     * for human-readable assertions. The headers in [EnrichedRecord] are
     * byte arrays to avoid per-event allocation in the hot path; tests
     * still want to assert on their textual content.
     */
    private fun Map<String, ByteArray>.decoded(): Map<String, String> = mapValues { (_, v) -> String(v, Charsets.UTF_8) }

    @Nested
    inner class `Construction validation` {
        @Test
        fun `should reject construction when component is blank`() {
            // When / Then
            assertThatThrownBy {
                MessageEnricher(component = "  ", cmdbId = cmdbId, environment = environment)
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Component must not be blank")
        }

        @Test
        fun `should reject construction when cmdbId is blank`() {
            // When / Then
            assertThatThrownBy {
                MessageEnricher(component = component, cmdbId = "", environment = environment)
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("CMDB id must not be blank")
        }

        @Test
        fun `should reject construction when environment is blank`() {
            // When / Then
            assertThatThrownBy {
                MessageEnricher(component = component, cmdbId = cmdbId, environment = "")
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Environment must not be blank")
        }
    }

    @Nested
    inner class `Static headers` {
        @Test
        fun `should include component cmdbId and environment in the headers`() {
            // Given
            val enricher = newEnricher()
            val event = newTestLoggingEvent()

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.headers.decoded())
                .containsEntry(MessageEnricher.HEADER_COMPONENT, component)
                .containsEntry(MessageEnricher.HEADER_CMDB_ID, cmdbId)
                .containsEntry(MessageEnricher.HEADER_ENVIRONMENT, environment)
        }

        @Test
        fun `should include the agent name and version in the headers`() {
            // Given
            val enricher = newEnricher()
            val event = newTestLoggingEvent()

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.headers.decoded())
                .containsEntry(MessageEnricher.HEADER_AGENT_NAME, MessageEnricher.AGENT_NAME)
                .containsEntry(MessageEnricher.HEADER_AGENT_VERSION, MessageEnricher.AGENT_VERSION)
        }

        @Test
        fun `should load the agent version from the build-filtered resource`() {
            // What is to be tested? Whether AGENT_VERSION comes from the
            //   Maven-filtered tabellarium-version.properties resource
            //   rather than a hardcoded constant that would drift from
            //   the artifact version.
            // How will the test case be deemed successful and why? Successful
            //   if the loaded version is neither blank nor the "unknown"
            //   fallback and starts with a digit (a real version string).
            //   On the test classpath the filtered resource exists, so
            //   the fallback firing would mean the loading is broken.
            // Why is it important to test this test case? Downstream
            //   consumers filter records by meta.agent.version; a header
            //   that silently reports a stale hardcoded version after a
            //   release bump would corrupt that dimension.

            // When / Then
            assertThat(MessageEnricher.AGENT_VERSION)
                .isNotBlank()
                .isNotEqualTo("unknown")
                .matches("^\\d.*")
        }

        @Test
        fun `should expose exactly the five documented header keys`() {
            // What is to be tested? Whether the set of header keys is exactly the
            //   documented set (component, cmdbId, environment, agent name, agent
            //   version) - no more, no less.
            // How will the test case be deemed successful and why? Successful if
            //   the headers map has exactly five entries with exactly those keys.
            //   This pins down the header contract so that downstream consumers can
            //   rely on it.
            // Why is it important to test this test case? An accidentally added or
            //   renamed header would silently change the Kafka record schema and
            //   could break SIEM or audit consumers that filter by header.

            // Given
            val enricher = newEnricher()
            val event = newTestLoggingEvent()

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.headers).containsOnlyKeys(
                MessageEnricher.HEADER_COMPONENT,
                MessageEnricher.HEADER_CMDB_ID,
                MessageEnricher.HEADER_ENVIRONMENT,
                MessageEnricher.HEADER_AGENT_NAME,
                MessageEnricher.HEADER_AGENT_VERSION,
            )
        }

        @Test
        fun `should return the same headers map instance across multiple calls`() {
            // What is to be tested? Whether the static headers map is computed once
            //   and shared across all enrich calls, rather than recomputed per event.
            // How will the test case be deemed successful and why? Successful if two
            //   independent calls return the exact same map reference. This confirms
            //   the documented no-per-event-allocation contract.
            // Why is it important to test this test case? Recomputing the headers map
            //   on every log event would multiply allocations in the hot path; an
            //   explicit test pins the contract down so a later "small refactor"
            //   cannot accidentally regress it.

            // Given
            val enricher = newEnricher()

            // When
            val firstResult = enricher.enrich(newTestLoggingEvent())
            val secondResult = enricher.enrich(newTestLoggingEvent())

            // Then
            assertThat(firstResult.headers).isSameAs(secondResult.headers)
        }

        @Test
        fun `should return an immutable headers map`() {
            // Given
            val enricher = newEnricher()

            // When
            val result = enricher.enrich(newTestLoggingEvent())

            // Then: attempting to mutate the returned map throws
            @Suppress("UNCHECKED_CAST")
            assertThatThrownBy {
                (result.headers as MutableMap<String, ByteArray>)["intruder"] = ByteArray(0)
            }.isInstanceOf(UnsupportedOperationException::class.java)
        }
    }

    @Nested
    inner class `Default partitioning key extractor` {
        @Test
        fun `should derive the partitioning key from the traceId in the MDC`() {
            // Given
            val enricher = newEnricher()
            val event = newTestLoggingEvent(mdc = mapOf("traceId" to "trace-abc-123"))

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.partitioningKey).isEqualTo("trace-abc-123")
        }

        @Test
        fun `should return a null partitioning key when getMDCPropertyMap returns null`() {
            // What is to be tested? Whether the default extractor defensively handles
            //   ILoggingEvent implementations whose getMDCPropertyMap() returns null
            //   instead of an empty map. Logback's own LoggingEvent normalizes null
            //   to emptyMap, but third-party implementations or test fakes may not.
            // How will the test case be deemed successful and why? Successful if an
            //   event whose getMDCPropertyMap returns null produces a null
            //   partitioning key without throwing NPE. This pins the defensive
            //   null-safety in the extractor.
            // Why is it important to test this test case? The `?.` operator in the
            //   default extractor exists explicitly for this case; without a test,
            //   a later "simplification" could remove it and introduce a regression.

            // Given: a custom event whose getMDCPropertyMap returns null
            val context = LoggerContext()
            val logger = context.getLogger("test-logger")
            val event =
                object : LoggingEvent(
                    "fqcn.dummy",
                    logger,
                    Level.INFO,
                    "test message",
                    null,
                    null,
                ) {
                    override fun getMDCPropertyMap(): Map<String, String>? = null
                }

            // When
            val result = newEnricher().enrich(event)

            // Then
            assertThat(result.partitioningKey).isNull()
        }

        @Test
        fun `should return a null partitioning key when the MDC does not contain a traceId`() {
            // Given
            val enricher = newEnricher()
            val event = newTestLoggingEvent(mdc = mapOf("other-key" to "other-value"))

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.partitioningKey).isNull()
        }

        @Test
        fun `should treat an over-long traceId in the MDC as no key`() {
            // What is to be tested? Whether a trace id beyond
            //   MAX_PARTITIONING_KEY_LENGTH is treated as absent. The MDC is
            //   attacker-influenced whenever the application bridges an
            //   inbound header into it, and the value becomes the Kafka
            //   record key verbatim.
            // How will the test case be deemed successful and why? Successful
            //   if a key of MAX+1 characters produces a null partitioning
            //   key while one of exactly MAX passes through unchanged - the
            //   boundary is asserted from both sides.
            // Why is it important to test this test case? Without the bound,
            //   an oversized inbound header inflates every record past
            //   max.request.size; the resulting RecordTooLargeException is
            //   deliberately ignored by the circuit breaker, so the breaker
            //   never opens and every such event floods the fallback
            //   appender indefinitely.

            // Given
            val enricher = newEnricher()
            val atLimit = "a".repeat(MessageEnricher.MAX_PARTITIONING_KEY_LENGTH)
            val overLimit = "a".repeat(MessageEnricher.MAX_PARTITIONING_KEY_LENGTH + 1)

            // When / Then: at the limit the key is used
            assertThat(
                enricher.enrich(newTestLoggingEvent(mdc = mapOf("traceId" to atLimit))).partitioningKey,
            ).isEqualTo(atLimit)

            // And: one character beyond, it is dropped entirely (not truncated -
            //   a truncated prefix would still be attacker-chosen)
            assertThat(
                enricher.enrich(newTestLoggingEvent(mdc = mapOf("traceId" to overLimit))).partitioningKey,
            ).isNull()
        }

        @Test
        fun `should treat a blank traceId in the MDC as no key`() {
            // What is to be tested? Whether the default extractor distinguishes
            //   between a missing key and a present-but-blank key, and treats
            //   both the same way (no partitioning key).
            // How will the test case be deemed successful and why? Successful if a
            //   traceId entry of "   " produces a null partitioning key. This
            //   confirms that the extractor never emits a meaningless empty key.
            // Why is it important to test this test case? Sending records with
            //   empty-string keys to Kafka is legal but useless - they all hash
            //   to the same partition. Treating blank as null avoids the silent
            //   hot-partition pathology that would otherwise result.

            // Given
            val enricher = newEnricher()
            val event = newTestLoggingEvent(mdc = mapOf("traceId" to "   "))

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.partitioningKey).isNull()
        }
    }

    @Nested
    inner class `Custom partitioning key extractor` {
        @Test
        fun `should use the custom extractor when one is provided`() {
            // Given: an extractor that returns the formatted message
            val enricher = newEnricherWith { event -> event.formattedMessage }
            val event = newTestLoggingEvent(message = "specific-message")

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.partitioningKey).isEqualTo("specific-message")
        }

        @Test
        fun `should pass null returned by the custom extractor through unchanged`() {
            // Given
            val enricher = newEnricherWith { _ -> null }
            val event = newTestLoggingEvent()

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.partitioningKey).isNull()
        }

        @Test
        fun `should treat an over-long key returned by the custom extractor as no key`() {
            // What is to be tested? Whether the length bound is enforced
            //   centrally in enrich(), so it also covers custom extractors -
            //   not just the default MDC one.
            // How will the test case be deemed successful and why? Successful
            //   if an extractor returning MAX+1 characters yields a null
            //   partitioning key. The bound has to sit at the central
            //   normalization point; enforcing it only in the default
            //   extractor would leave the custom path unbounded.
            // Why is it important to test this test case? A custom extractor
            //   reading, say, a user-supplied header is exactly the case
            //   where the value is attacker-influenced.

            // Given
            val enricher = newEnricherWith { "x".repeat(MessageEnricher.MAX_PARTITIONING_KEY_LENGTH + 1) }

            // When
            val result = enricher.enrich(newTestLoggingEvent())

            // Then
            assertThat(result.partitioningKey).isNull()
        }

        @Test
        fun `should treat a blank key returned by the custom extractor as no key`() {
            // What is to be tested? Whether the blank-as-null normalization in
            //   enrich() applies to custom extractors as well, not only to the
            //   default one.
            // How will the test case be deemed successful and why? Successful if
            //   a custom extractor that returns "   " produces a null
            //   partitioning key. This confirms the contract is enforced
            //   centrally and not in the default extractor only.
            // Why is it important to test this test case? Otherwise a careless
            //   custom extractor could leak empty-string keys into Kafka,
            //   creating the same hot-partition pathology described above.

            // Given
            val enricher = newEnricherWith { _ -> "   " }
            val event = newTestLoggingEvent()

            // When
            val result = enricher.enrich(event)

            // Then
            assertThat(result.partitioningKey).isNull()
        }
    }

    @Nested
    inner class `Purity guarantees` {
        @Test
        fun `should not modify the MDC map of the incoming event`() {
            // What is to be tested? Whether the enricher leaves the input event's
            //   MDC map untouched.
            // How will the test case be deemed successful and why? Successful if
            //   the event's MDC map after enrich() contains exactly the same
            //   entries it had before. This is the enricher's purity contract.
            // Why is it important to test this test case? Mutation of the MDC map
            //   by one appender propagates to all other appenders sharing the
            //   logger context, causing cross-talk that is notoriously hard to
            //   debug. The purity guarantee is the entire reason this component
            //   exists.

            // Given
            val mdc = mapOf("traceId" to "trace-abc-123", "userId" to "u-42")
            val event = newTestLoggingEvent(mdc = mdc)

            // When
            newEnricher().enrich(event)

            // Then
            assertThat(event.mdcPropertyMap).containsExactlyInAnyOrderEntriesOf(mdc)
        }
    }
}
