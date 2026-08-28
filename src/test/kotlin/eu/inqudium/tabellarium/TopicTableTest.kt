package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class TopicTableTest {
    @Nested
    inner class `Construction validation` {
        @Test
        fun `should reject construction when any topic name is blank`() {
            // When / Then
            assertThatThrownBy {
                TopicTable(mapOf("  " to TopicClass.AUDIT))
            }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("must not be blank")
        }

        @Test
        fun `should accept an empty topic mapping`() {
            // Given / When
            val table = TopicTable(emptyMap())

            // Then: every lookup returns the fallback
            assertThat(table.classFor("anything")).isEqualTo(TopicClass.TECHNICAL)
        }
    }

    @Nested
    inner class `Topic class lookup` {
        @Test
        fun `should return the configured class for a known topic`() {
            // Given
            val table =
                TopicTable(
                    mapOf(
                        "audit-events" to TopicClass.AUDIT,
                        "tech-events" to TopicClass.TECHNICAL,
                    ),
                )

            // When / Then
            assertThat(table.classFor("audit-events")).isEqualTo(TopicClass.AUDIT)
            assertThat(table.classFor("tech-events")).isEqualTo(TopicClass.TECHNICAL)
        }

        @Test
        fun `should return the explicit fallback class for an unknown topic`() {
            // Given
            val table =
                TopicTable(
                    topicsByName = mapOf("audit-events" to TopicClass.AUDIT),
                    fallbackClass = TopicClass.PERFORMANCE,
                )

            // When / Then
            assertThat(table.classFor("unmapped-topic")).isEqualTo(TopicClass.PERFORMANCE)
        }

        @Test
        fun `should default the fallback class to TECHNICAL when not configured`() {
            // Given
            val table = TopicTable(mapOf("audit-events" to TopicClass.AUDIT))

            // When / Then
            assertThat(table.classFor("unmapped-topic")).isEqualTo(TopicClass.TECHNICAL)
            assertThat(table.fallbackClass).isEqualTo(TopicClass.TECHNICAL)
        }
    }

    @Nested
    inner class `Active topic classes` {
        @Test
        fun `should include all classes that have at least one topic mapped`() {
            // Given
            val table =
                TopicTable(
                    mapOf(
                        "audit-events" to TopicClass.AUDIT,
                        "tech-events" to TopicClass.TECHNICAL,
                    ),
                )

            // When / Then
            assertThat(table.activeTopicClasses)
                .containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.TECHNICAL)
        }

        @Test
        fun `should always include the fallback class even when no topic maps to it`() {
            // What is to be tested? Whether the fallback class is always part of
            //   activeTopicClasses, even when no explicit topic was mapped to it.
            // How will the test case be deemed successful and why? Successful if
            //   activeTopicClasses contains the fallback class in a configuration
            //   where only an unrelated class is explicitly mapped. This pins
            //   down the contract that ProducerRegistry will always have a
            //   fallback producer available.
            // Why is it important to test this test case? Without the fallback
            //   class in activeTopicClasses, the ProducerRegistry would not
            //   instantiate a producer for it, and any lookup via classFor()
            //   for an unmapped topic would later resolve to a class with no
            //   producer - IllegalStateException at log time. The set must
            //   close over all classes that classFor() could ever return.

            // Given: only AUDIT topics; fallback is TECHNICAL
            val table =
                TopicTable(
                    topicsByName = mapOf("audit-events" to TopicClass.AUDIT),
                    fallbackClass = TopicClass.TECHNICAL,
                )

            // When / Then
            assertThat(table.activeTopicClasses).contains(TopicClass.TECHNICAL)
        }

        @Test
        fun `should not include classes that have no topic and are not the fallback`() {
            // Given
            val table =
                TopicTable(
                    topicsByName = mapOf("audit-events" to TopicClass.AUDIT),
                    fallbackClass = TopicClass.TECHNICAL,
                )

            // When / Then
            assertThat(table.activeTopicClasses)
                .doesNotContain(TopicClass.FUNCTIONAL, TopicClass.PERFORMANCE)
        }
    }

    @Nested
    inner class `Immutability` {
        @Test
        fun `should not be affected by subsequent mutations of the input map`() {
            // What is to be tested? Whether the table captures a defensive copy
            //   of the input map at construction time, so that the caller can
            //   safely mutate the original afterwards without affecting the
            //   table's behavior.
            // How will the test case be deemed successful and why? Successful if
            //   adding an entry to the original mutable map after construction
            //   does not change classFor() results. This confirms the Map.copyOf
            //   defensive-copy contract.
            // Why is it important to test this test case? Joran's configuration
            //   path typically populates a mutable map and passes it to the
            //   appender. If the table held a reference instead of a copy,
            //   later configuration changes (in some hot-reload scenarios)
            //   would silently retag topics.

            // Given: a mutable input map
            val mutableInput = mutableMapOf("audit-events" to TopicClass.AUDIT)
            val table = TopicTable(mutableInput)

            // When: the caller mutates the original map after construction.
            //   The values are chosen so that mutation would be observable -
            //   PERFORMANCE differs from both the configured class (AUDIT) and
            //   the fallback (TECHNICAL).
            mutableInput["audit-events"] = TopicClass.PERFORMANCE // retag
            mutableInput["tech-events"] = TopicClass.PERFORMANCE // new entry

            // Then: the table's lookups still reflect construction-time state.
            //   audit-events resolves to AUDIT (not PERFORMANCE) → retag ignored.
            //   tech-events resolves to TECHNICAL (the fallback, not PERFORMANCE)
            //   → new entry ignored.
            assertThat(table.classFor("audit-events")).isEqualTo(TopicClass.AUDIT)
            assertThat(table.classFor("tech-events")).isEqualTo(TopicClass.TECHNICAL)
        }
    }
}
