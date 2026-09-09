package eu.inqudium.tabellarium

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.slf4j.MarkerFactory

class RoutingPlanTest {
    // -- Test fixtures --------------------------------------------------

    private fun newConfig(mapping: TopicMappingConfig): AppenderConfig =
        AppenderConfig(
            kafkaProducerProperties = "bootstrap.servers=localhost:9092",
            topicMapping = mapping,
            component = "svc",
            cmdbId = "CMDB-1",
            environment = "test",
            sendQueueCapacity = 16,
        )

    private fun mapping(
        marker: String,
        topic: String,
        topicClass: String,
    ): TopicMappingEntry =
        TopicMappingEntry().apply {
            this.marker = marker
            this.topic = topic
            this.topicClass = topicClass
        }

    @Test
    fun `should derive routing, classification and enrichment from the config alone`() {
        // What is to be tested? Whether the plan is a complete function of
        //   the AppenderConfig: the router resolves a mapped marker, the
        //   table classifies the resulting topic, and the enricher carries
        //   the identity fields - without any producer, queue or worker.
        // How will the test case be deemed successful and why? Successful
        //   if the plan built from a config with one AUDIT mapping routes
        //   the marker to its topic, classifies that topic as AUDIT, lists
        //   AUDIT and the default TECHNICAL as active, and its enricher
        //   emits the component header.
        // Why is it important to test this test case? The plan is the
        //   per-event-invariant half of the pipeline; that it needs nothing
        //   but the config is what lets AppenderPipeline.build create it
        //   outside the resource transaction.

        // Given
        val config =
            newConfig(
                TopicMappingConfig().apply {
                    defaultTopic = "svc.logs"
                    addMapping(mapping("SECURITY", "audit.security", "AUDIT"))
                },
            )

        // When
        val plan = RoutingPlan.from(config)

        // Then
        val topic = plan.topicRouter.route(listOf(MarkerFactory.getMarker("SECURITY")))
        assertThat(topic).isEqualTo("audit.security")
        assertThat(plan.topicTable.classFor(topic)).isEqualTo(TopicClass.AUDIT)
        assertThat(plan.topicTable.activeTopicClasses).containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.TECHNICAL)
        assertThat(plan.messageEnricher.enrich(newTestLoggingEvent()).headers)
            .anyMatch { it.key() == MessageEnricher.HEADER_COMPONENT && String(it.value()) == "svc" }
    }

    @Test
    fun `should reject an inconsistent mapping before anything is created`() {
        // What is to be tested? Whether a topic mapped to two classes fails
        //   at plan construction with the mapping's own validation error.
        // How will the test case be deemed successful and why? Successful
        //   if RoutingPlan.from throws IllegalArgumentException naming the
        //   conflict - the same error start() reports - so the failure
        //   happens in the pure stage, where there is nothing to roll back.
        // Why is it important to test this test case? The plan's contract
        //   is "cannot leak"; a mapping error that only surfaced after
        //   producers exist would push it back into the transaction the
        //   type was introduced to leave.

        // Given: one topic, two classes
        val config =
            newConfig(
                TopicMappingConfig().apply {
                    defaultTopic = "svc.logs"
                    addMapping(mapping("A", "shared.topic", "AUDIT"))
                    addMapping(mapping("B", "shared.topic", "TECHNICAL"))
                },
            )

        // When / Then
        assertThatThrownBy { RoutingPlan.from(config) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("shared.topic")
    }
}
