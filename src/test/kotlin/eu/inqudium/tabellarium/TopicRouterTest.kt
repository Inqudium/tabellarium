package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.MarkerFactory

class TopicRouterTest {
    @Nested
    inner class `Default topic fallback` {
        @Test
        fun `should return the default topic when the marker list is empty`() {
            // What is to be tested? Rule 1 of the resolution algorithm: an event without any
            //   marker routes to the default topic, regardless of the configured mappings.
            // How will the test case be deemed successful and why? Successful if route(emptyList())
            //   returns "default-topic" although an AUDIT mapping is configured. This confirms that
            //   mappings are never applied without a matching marker.
            // Why is it important to test this test case? Marker-less events are the common case
            //   for ordinary application logging; if they did not land on the default topic, the
            //   bulk of every deployment's log stream would be misrouted.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )

            // When
            val result = router.route(emptyList())

            // Then
            assertThat(result).isEqualTo("default-topic")
        }

        @Test
        fun `should return the default topic when no marker name matches the configured mappings`() {
            // What is to be tested? Rule 3: a marker whose name is not a configured key routes
            //   to the default topic rather than to any mapped topic or to an error.
            // How will the test case be deemed successful and why? Successful if a detached
            //   UNKNOWN marker resolves to "default-topic" while only AUDIT is mapped. This
            //   confirms the default is a true catch-all for unrecognized markers.
            // Why is it important to test this test case? Applications attach markers the
            //   operator never configured (library markers, ad-hoc ones); the router must
            //   absorb them silently instead of dropping events or throwing in the hot path.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val unmatchedMarker = MarkerFactory.getDetachedMarker("UNKNOWN")

            // When
            val result = router.route(listOf(unmatchedMarker))

            // Then
            assertThat(result).isEqualTo("default-topic")
        }
    }

    @Nested
    inner class `Single marker direct match` {
        @Test
        fun `should return the mapped topic when a single marker matches by name`() {
            // What is to be tested? The basic positive case of rule 2: a marker whose name
            //   equals a configured key routes to that key's topic.
            // How will the test case be deemed successful and why? Successful if a detached
            //   AUDIT marker resolves to "audit-topic" and not to the default. This confirms
            //   the map lookup by marker name is wired to the returned topic.
            // Why is it important to test this test case? This is the entire reason the
            //   router exists - marker-driven topic separation; if it broke, every event
            //   would collapse onto the default topic and AUDIT streams would lose isolation.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val auditMarker = MarkerFactory.getDetachedMarker("AUDIT")

            // When
            val result = router.route(listOf(auditMarker))

            // Then
            assertThat(result).isEqualTo("audit-topic")
        }

        @Test
        fun `should distinguish between markers by exact case`() {
            // What is to be tested? Whether marker name matching is case-sensitive.
            // How will the test case be deemed successful and why? Successful if 'audit'
            //   (lowercase) does not match the configured 'AUDIT' (uppercase) and falls
            //   back to the default topic. This confirms strict case sensitivity.
            // Why is it important to test this test case? Case-insensitive matching
            //   would cause confusion and accidental fan-out between topics; an explicit
            //   test pins down the deliberate strict-case contract.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val lowercaseMarker = MarkerFactory.getDetachedMarker("audit")

            // When
            val result = router.route(listOf(lowercaseMarker))

            // Then
            assertThat(result).isEqualTo("default-topic")
        }

        @Test
        fun `should not trim whitespace from marker names when matching`() {
            // What is to be tested? Whether the router silently trims whitespace from
            //   marker names before comparing them to the configured map keys.
            // How will the test case be deemed successful and why? Successful if a
            //   marker named 'AUDIT ' (trailing space) does NOT match a configured key
            //   'AUDIT'. This confirms that input normalization is the caller's
            //   responsibility, not the router's.
            // Why is it important to test this test case? The router's contract states
            //   exact-string matching. Sneaking in defensive trimming would hide
            //   configuration bugs upstream (where they should be caught and rejected).

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val whitespaceMarker = MarkerFactory.getDetachedMarker("AUDIT ")

            // When
            val result = router.route(listOf(whitespaceMarker))

            // Then
            assertThat(result).isEqualTo("default-topic")
        }
    }

    @Nested
    inner class `Multiple markers` {
        @Test
        fun `should return the topic of the first marker that matches when several markers are present`() {
            // What is to be tested? The precedence rule for multi-marker events: the first
            //   marker in list order that matches wins, later matches are ignored.
            // How will the test case be deemed successful and why? Successful if [AUDIT,
            //   PERFORMANCE] - both mapped - resolves to "audit-topic". This confirms the
            //   documented "first match in iteration order" contract.
            // Why is it important to test this test case? An event can only go to one topic;
            //   without a stable order rule the winner would depend on implementation detail,
            //   making routing non-deterministic from the operator's point of view.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings =
                        mapOf(
                            "AUDIT" to "audit-topic",
                            "PERFORMANCE" to "performance-topic",
                        ),
                )
            val auditMarker = MarkerFactory.getDetachedMarker("AUDIT")
            val performanceMarker = MarkerFactory.getDetachedMarker("PERFORMANCE")

            // When
            val result = router.route(listOf(auditMarker, performanceMarker))

            // Then
            assertThat(result).isEqualTo("audit-topic")
        }

        @Test
        fun `should fall back to the default topic when none of the markers match`() {
            // What is to be tested? Rule 3 for the multi-marker case: when several markers are
            //   present and none of them is mapped, the default topic is returned.
            // How will the test case be deemed successful and why? Successful if [UNKNOWN_A,
            //   UNKNOWN_B] resolves to "default-topic". This confirms the loop over markers
            //   exhausts without a match and falls through to the default.
            // Why is it important to test this test case? The loop's fall-through is a separate
            //   code path from the empty-list early return; a bug there (e.g. returning the last
            //   marker's name) would only show up with several unmapped markers.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val firstUnknown = MarkerFactory.getDetachedMarker("UNKNOWN_A")
            val secondUnknown = MarkerFactory.getDetachedMarker("UNKNOWN_B")

            // When
            val result = router.route(listOf(firstUnknown, secondUnknown))

            // Then
            assertThat(result).isEqualTo("default-topic")
        }

        @Test
        fun `should skip earlier non-matching markers and return the topic of a later matching marker`() {
            // What is to be tested? Whether the router keeps scanning past a non-matching
            //   marker instead of falling back to the default at the first miss.
            // How will the test case be deemed successful and why? Successful if [UNKNOWN,
            //   AUDIT] resolves to "audit-topic". This confirms a miss only continues the
            //   loop and never short-circuits to the default topic.
            // Why is it important to test this test case? Events typically carry markers from
            //   several layers (framework plus application); an early bail-out would silently
            //   route AUDIT-marked events to the default topic whenever another marker came first.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val unknownMarker = MarkerFactory.getDetachedMarker("UNKNOWN")
            val auditMarker = MarkerFactory.getDetachedMarker("AUDIT")

            // When
            val result = router.route(listOf(unknownMarker, auditMarker))

            // Then
            assertThat(result).isEqualTo("audit-topic")
        }
    }

    @Nested
    inner class `Marker hierarchy` {
        @Test
        fun `should resolve via a referenced marker when the top-level marker has no direct mapping`() {
            // What is to be tested? Hierarchical resolution: a marker that is not mapped itself
            //   but references a mapped marker routes to the referenced marker's topic.
            // How will the test case be deemed successful and why? Successful if an unmapped
            //   TRANSACTION marker that contains AUDIT resolves to "audit-topic". This confirms
            //   the router iterates the marker's references and matches them by name.
            // Why is it important to test this test case? SLF4J marker hierarchies are how
            //   applications compose categories; without reference resolution every composite
            //   marker would need its own mapping and AUDIT events could escape the audit topic.

            // Given: a container marker that holds an AUDIT marker as a reference
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val container = MarkerFactory.getDetachedMarker("TRANSACTION")
            container.add(MarkerFactory.getDetachedMarker("AUDIT"))

            // When
            val result = router.route(listOf(container))

            // Then
            assertThat(result).isEqualTo("audit-topic")
        }

        @Test
        fun `should prefer a direct match over a hierarchical match when both are present`() {
            // What is to be tested? The resolution order when the top-level marker
            //   itself is directly mapped, but it also references another mapped marker.
            // How will the test case be deemed successful and why? Successful if the
            //   direct mapping wins over the hierarchical one; this confirms the
            //   documented "direct first, hierarchical second" rule.
            // Why is it important to test this test case? Without this guarantee, the
            //   routing would depend on the iterator order of the marker references,
            //   which is not stable across SLF4J versions.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings =
                        mapOf(
                            "TRANSACTION" to "transaction-topic",
                            "AUDIT" to "audit-topic",
                        ),
                )
            val container = MarkerFactory.getDetachedMarker("TRANSACTION")
            container.add(MarkerFactory.getDetachedMarker("AUDIT"))

            // When
            val result = router.route(listOf(container))

            // Then
            assertThat(result).isEqualTo("transaction-topic")
        }

        @Test
        fun `should not follow references of references when resolving the topic`() {
            // What is to be tested? Whether hierarchical resolution descends recursively
            //   through references of references, or stops at one level deep.
            // How will the test case be deemed successful and why? Successful if a
            //   transitively referenced marker (TOP -> MID -> AUDIT) does NOT resolve
            //   to the AUDIT topic. This confirms single-level resolution.
            // Why is it important to test this test case? Deep traversal would risk
            //   infinite recursion on cyclic marker references and add complexity for
            //   negligible practical benefit. The single-level contract must be pinned.

            // Given
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "audit-topic"),
                )
            val deeplyNested = MarkerFactory.getDetachedMarker("AUDIT")
            val midLevel = MarkerFactory.getDetachedMarker("MID")
            midLevel.add(deeplyNested)
            val topLevel = MarkerFactory.getDetachedMarker("TOP")
            topLevel.add(midLevel)

            // When
            val result = router.route(listOf(topLevel))

            // Then
            assertThat(result).isEqualTo("default-topic")
        }
    }

    @Nested
    inner class `Construction validation` {
        @Test
        fun `should reject construction when the default topic is blank`() {
            // What is to be tested? Whether a blank default topic is rejected at construction.
            // How will the test case be deemed successful and why? Successful if the constructor
            //   throws IllegalArgumentException with the message "Default topic must not be
            //   blank". This confirms validation is eager and the error names the offending field.
            // Why is it important to test this test case? The default topic is where every
            //   marker-less event goes; a blank name would fail at the broker on every send
            //   after a clean start() - the latent misconfiguration eager validation exists for.

            // When / Then
            assertThatThrownBy {
                TopicRouter(
                    defaultTopic = "  ",
                    markerMappings = emptyMap(),
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Default topic must not be blank")
        }

        @Test
        fun `should reject construction when the default topic contains characters not permitted by Kafka`() {
            // What is to be tested? Whether the default topic is checked against Kafka's
            //   permitted character set [a-zA-Z0-9._-] at construction.
            // How will the test case be deemed successful and why? Successful if a default
            //   topic containing spaces throws IllegalArgumentException mentioning "not
            //   permitted by Kafka". This confirms the broker's naming rule is enforced eagerly.
            // Why is it important to test this test case? The broker answers such a name with
            //   InvalidTopicException, which the circuit breaker deliberately ignores - the
            //   pipeline would report healthy while every marker-less event is lost.

            // When / Then
            assertThatThrownBy {
                TopicRouter(
                    defaultTopic = "default topic with space",
                    markerMappings = emptyMap(),
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("not permitted by Kafka")
        }

        @Test
        fun `should reject construction when a mapped marker name is blank`() {
            // What is to be tested? Whether a blank marker name in the mappings is rejected at
            //   construction.
            // How will the test case be deemed successful and why? Successful if a mapping with
            //   key "" throws IllegalArgumentException with "Marker name must not be blank".
            //   This confirms each mapping key is validated, not only the default topic.
            // Why is it important to test this test case? A blank key can never match a real
            //   marker, so the mapping would be dead configuration - almost certainly an empty
            //   <marker> element - and is better reported at start() than silently ignored.

            // When / Then
            assertThatThrownBy {
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("" to "some-topic"),
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Marker name must not be blank")
        }

        @Test
        fun `should reject construction when a mapped topic name is blank`() {
            // What is to be tested? Whether a blank topic name in the mappings is rejected at
            //   construction.
            // How will the test case be deemed successful and why? Successful if mapping AUDIT
            //   to "  " throws IllegalArgumentException containing "must not be blank". This
            //   confirms mapping values are validated like the default topic.
            // Why is it important to test this test case? A blank target would make every
            //   AUDIT-marked event fail at send time with a name the broker rejects, after the
            //   appender had already started - silent loss of the compliance-relevant stream.

            // When / Then
            assertThatThrownBy {
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "  "),
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("must not be blank")
        }

        @Test
        fun `should reject construction when a mapped topic name contains characters not permitted by Kafka`() {
            // What is to be tested? Whether mapped topic names, not only the default topic, are
            //   checked against Kafka's permitted character set at construction.
            // How will the test case be deemed successful and why? Successful if mapping AUDIT
            //   to "topic with space" throws IllegalArgumentException mentioning "not permitted
            //   by Kafka". This confirms requireKafkaValidTopicName runs for every mapping.
            // Why is it important to test this test case? A mapped topic the broker rejects
            //   would fail per send with InvalidTopicException, which the breaker ignores -
            //   the AUDIT stream would vanish while the pipeline reports healthy.

            // When / Then
            assertThatThrownBy {
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = mapOf("AUDIT" to "topic with space"),
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("not permitted by Kafka")
        }

        @Test
        fun `should reject construction when the default topic is a reserved Kafka name`() {
            // What is to be tested? Whether Kafka's reserved topic names
            //   "." and ".." are rejected at construction. They pass the
            //   character-set pattern but the broker refuses them - and
            //   the resulting InvalidTopicException is deliberately
            //   ignored by the circuit breaker, so a reserved name that
            //   survived startup would silently divert every event to
            //   the fallback while the pipeline reports healthy.
            // How will the test case be deemed successful and why? Successful
            //   if both "." and ".." throw IllegalArgumentException at
            //   construction. This closes the validate-eagerly contract.
            // Why is it important to test this test case? The failure mode
            //   is permanent silent log loss after a clean startup - the
            //   exact latent misconfiguration eager validation exists for.

            // When / Then
            listOf(".", "..").forEach { reserved ->
                assertThatThrownBy {
                    TopicRouter(defaultTopic = reserved, markerMappings = emptyMap())
                }.isInstanceOf(IllegalArgumentException::class.java)
                    .hasMessageContaining("reserved by Kafka")
            }
        }

        @Test
        fun `should reject construction when a topic name exceeds Kafka's maximum length`() {
            // What is to be tested? Whether Kafka's maximum topic-name length of 249 is
            //   enforced at construction, for the default topic and for mapped topics alike.
            // How will the test case be deemed successful and why? Successful if a 250-character
            //   name throws IllegalArgumentException mentioning "maximum length" in both
            //   positions. This confirms the length rule mirrors Topic.validate on the broker.
            // Why is it important to test this test case? An overlong name passes the character
            //   pattern but is refused by the broker with InvalidTopicException - ignored by the
            //   breaker - so it would survive start() and silently divert every affected event.

            // Given: 250 characters - one over Kafka's limit of 249
            val overlong = "a".repeat(250)

            // When / Then: rejected for the default topic
            assertThatThrownBy {
                TopicRouter(defaultTopic = overlong, markerMappings = emptyMap())
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("maximum length")

            // And: rejected for a mapped topic
            assertThatThrownBy {
                TopicRouter(defaultTopic = "default-topic", markerMappings = mapOf("AUDIT" to overlong))
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("maximum length")
        }

        @Test
        fun `should accept a topic name at exactly Kafka's maximum length`() {
            // What is to be tested? The boundary of the length rule: a name of exactly 249
            //   characters is still valid.
            // How will the test case be deemed successful and why? Successful if construction
            //   succeeds and route(emptyList()) returns the 249-character name unchanged. This
            //   confirms the check is "<= 249", not an off-by-one "< 249".
            // Why is it important to test this test case? An off-by-one would reject a name
            //   the broker accepts, failing start() for a legal configuration; the boundary
            //   value is the only input that distinguishes the two comparisons.

            // Given: exactly 249 characters - the boundary value
            val maxLength = "a".repeat(249)

            // When / Then: accepted
            val router = TopicRouter(defaultTopic = maxLength, markerMappings = emptyMap())
            assertThat(router.route(emptyList())).isEqualTo(maxLength)
        }

        @Test
        fun `should accept construction when the marker mappings are empty`() {
            // What is to be tested? Whether an empty marker map is a valid configuration.
            // How will the test case be deemed successful and why? Successful if no
            //   exception is thrown and the resulting router falls back to the default
            //   topic for every input. This confirms that an "everything to default"
            //   configuration is supported.
            // Why is it important to test this test case? Some deployments only need
            //   a single fall-through topic without any marker-based routing; rejecting
            //   that configuration would be over-strict.

            // Given / When
            val router =
                TopicRouter(
                    defaultTopic = "default-topic",
                    markerMappings = emptyMap(),
                )
            val anyMarker = MarkerFactory.getDetachedMarker("ANYTHING")

            // Then
            assertThat(router.route(listOf(anyMarker))).isEqualTo("default-topic")
            assertThat(router.route(emptyList())).isEqualTo("default-topic")
        }
    }
}
