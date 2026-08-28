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
