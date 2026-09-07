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
            // What is to be tested? Whether a blank topic name among the keys is rejected at
            //   construction.
            // How will the test case be deemed successful and why? Successful if a map with key
            //   "  " throws IllegalArgumentException containing "must not be blank". This confirms
            //   the table validates its keys eagerly instead of storing a dead entry.
            // Why is it important to test this test case? A blank key can never equal a
            //   TopicRouter result (the router rejects blank names itself), so it is almost
            //   certainly a configuration typo that should surface at start(), not stay hidden.

            // When / Then
            assertThatThrownBy {
                TopicTable(mapOf("  " to TopicClass.AUDIT))
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("must not be blank")
        }

        @Test
        fun `should accept an empty topic mapping`() {
            // What is to be tested? Whether a table without any explicit assignments is a valid
            //   configuration.
            // How will the test case be deemed successful and why? Successful if construction
            //   succeeds and classFor("anything") returns the TECHNICAL fallback. This confirms
            //   the empty map is not treated as an error and the fallback covers every lookup.
            // Why is it important to test this test case? A deployment with only a default topic
            //   builds exactly this table; rejecting it would make the minimal logback.xml fail.

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
            // What is to be tested? The basic lookup: a topic that was assigned a class at
            //   construction resolves to exactly that class.
            // How will the test case be deemed successful and why? Successful if "audit-events"
            //   resolves to AUDIT and "tech-events" to TECHNICAL. This confirms the assignments
            //   are stored per topic and not overwritten by each other or by the fallback.
            // Why is it important to test this test case? classFor() decides which producer - and
            //   thus which acks/idempotence overrides - a record gets; a wrong class would send
            //   AUDIT records through a producer without the mandated guarantees.

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
            // What is to be tested? Whether an unmapped topic resolves to the fallback class
            //   passed to the constructor, not to the built-in TECHNICAL default.
            // How will the test case be deemed successful and why? Successful if with
            //   fallbackClass = PERFORMANCE an unmapped topic resolves to PERFORMANCE. This
            //   confirms the constructor argument is the one consulted by classFor().
            // Why is it important to test this test case? <defaultTopicClass> is delivered through
            //   this parameter; if the lookup ignored it, an operator upgrading the default stream
            //   to AUDIT would silently keep TECHNICAL guarantees.

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
            // What is to be tested? The constructor's default for fallbackClass when the caller
            //   omits it.
            // How will the test case be deemed successful and why? Successful if fallbackClass
            //   is TECHNICAL and an unmapped topic resolves to TECHNICAL. This confirms the
            //   documented neutral default is what an unconfigured table applies.
            // Why is it important to test this test case? TECHNICAL carries no compliance
            //   mandate and tolerable producer defaults - the safest class when intent is
            //   unclear; a different default would impose overrides nobody configured.

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
            // What is to be tested? Whether activeTopicClasses collects the class of every
            //   mapped topic.
            // How will the test case be deemed successful and why? Successful if a table with an
            //   AUDIT and a TECHNICAL topic reports exactly {AUDIT, TECHNICAL}. This confirms the
            //   set is derived from the mapping values (here TECHNICAL is also the fallback).
            // Why is it important to test this test case? ProducerRegistry creates one producer
            //   per active class; a class missing here would leave its topics without a producer
            //   and fail at log time with an IllegalStateException.

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
            // What is to be tested? Whether activeTopicClasses excludes classes that neither have
            //   a mapped topic nor serve as the fallback.
            // How will the test case be deemed successful and why? Successful if a table with only
            //   an AUDIT topic and TECHNICAL fallback reports neither FUNCTIONAL nor PERFORMANCE.
            //   This confirms the set is minimal, not simply all TopicClass constants.
            // Why is it important to test this test case? Every active class costs a KafkaProducer
            //   with its own network thread and buffer memory; dormant producers for unused classes
            //   would waste resources in every deployment that uses only a subset.

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
