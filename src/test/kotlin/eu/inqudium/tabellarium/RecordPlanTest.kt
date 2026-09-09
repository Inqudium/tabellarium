package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.Encoder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.MarkerFactory

class RecordPlanTest {
    // -- Test fixtures --------------------------------------------------

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

    /** Default topic `svc.logs` (TECHNICAL) plus one AUDIT mapping for the SECURITY marker. */
    private fun securityMapping(): TopicMappingConfig =
        TopicMappingConfig().apply {
            defaultTopic = "svc.logs"
            addMapping(mapping("SECURITY", "audit.security", "AUDIT"))
        }

    private fun planFor(
        mapping: TopicMappingConfig = securityMapping(),
        encoder: Encoder<ILoggingEvent> = MessageBytesEncoder(),
    ): RecordPlan =
        RecordPlan.from(
            topicMapping = mapping,
            component = "svc",
            cmdbId = "CMDB-1",
            environment = "test",
            encoder = encoder,
        )

    private fun eventWithMarker(
        marker: String?,
        message: String = "hello",
        mdc: Map<String, String> = emptyMap(),
    ): ILoggingEvent =
        (newTestLoggingEvent(message = message, mdc = mdc) as ch.qos.logback.classic.spi.LoggingEvent).apply {
            marker?.let { addMarker(MarkerFactory.getMarker(it)) }
        }

    @Nested
    inner class `Route` {
        @Test
        fun `should resolve a mapped marker to its topic and class`() {
            // What is to be tested? The first step of "event to record": an
            //   event carrying the SECURITY marker is routed to the audit
            //   topic and classified AUDIT.
            // How will the test case be deemed successful and why? Successful
            //   if route() returns the mapped topic and its class - the two
            //   values the transport needs to pick producer and queue.
            // Why is it important to test this test case? Routing decides
            //   which producer policy (acks, idempotence) a record gets; a
            //   wrong class here silently downgrades an audit record.

            // Given
            val plan = planFor()

            // When
            val route = plan.route(eventWithMarker("SECURITY"))

            // Then
            assertThat(route.topicName).isEqualTo("audit.security")
            assertThat(route.topicClass).isEqualTo(TopicClass.AUDIT)
        }

        @Test
        fun `should send marker-less events to the default topic with the default class`() {
            // What is to be tested? The fallback branch of routing: no marker
            //   means the default topic, classified via defaultTopicClass
            //   (TECHNICAL unless configured).
            // How will the test case be deemed successful and why? Successful
            //   if route() returns the default topic and TECHNICAL.
            // Why is it important to test this test case? Most events in a
            //   service carry no marker; this branch is the common case.

            // Given / When
            val route = planFor().route(eventWithMarker(marker = null))

            // Then
            assertThat(route.topicName).isEqualTo("svc.logs")
            assertThat(route.topicClass).isEqualTo(TopicClass.TECHNICAL)
        }

        @Test
        fun `should list the classes the mapping makes active`() {
            // What is to be tested? Whether the plan tells the transport which
            //   classes need a producer, a breaker and a queue.
            // How will the test case be deemed successful and why? Successful
            //   if the AUDIT mapping plus the TECHNICAL default yield exactly
            //   those two classes.
            // Why is it important to test this test case? The transport opens
            //   resources for exactly this set; a class missing here would
            //   fail the first dispatch to it, a surplus class would open an
            //   idle producer.

            assertThat(planFor().activeTopicClasses).containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.TECHNICAL)
        }
    }

    @Nested
    inner class `Materialize` {
        @Test
        fun `should produce payload, partitioning key and headers for the route`() {
            // What is to be tested? The second step of "event to record": the
            //   encoder's bytes become the payload, the MDC trace id the key,
            //   and the identity fields the headers, all attached to the
            //   route.
            // How will the test case be deemed successful and why? Successful
            //   if the record carries the route it was made for, the encoded
            //   message, the trace id as key and the component header.
            // Why is it important to test this test case? This is the whole
            //   claim of the class - "how a log event becomes a Kafka record"
            //   - as one observable function instead of three properties the
            //   appender has to wire itself.

            // Given
            val plan = planFor()
            val event = eventWithMarker("SECURITY", message = "audit me", mdc = mapOf("traceId" to "abc123"))
            val route = plan.route(event)

            // When
            val record = plan.materialize(route, event)

            // Then
            assertThat(record.route).isSameAs(route)
            assertThat(String(record.payload, Charsets.UTF_8)).isEqualTo("audit me")
            assertThat(record.enrichment.partitioningKey).isEqualTo("abc123")
            assertThat(record.enrichment.headers)
                .anyMatch { it.key() == MessageEnricher.HEADER_COMPONENT && String(it.value()) == "svc" }
        }

        @Test
        fun `should let an encoder failure surface after the route is known`() {
            // What is to be tested? The reason route() and materialize() are
            //   two steps: an encoder that throws fails materialize(), while
            //   route() for the same event has already succeeded.
            // How will the test case be deemed successful and why? Successful
            //   if route() returns AUDIT for the event and materialize() then
            //   throws the encoder's exception.
            // Why is it important to test this test case? The appender
            //   attributes a hot-path failure to the class the event was
            //   routed to; that is only possible because routing does not
            //   depend on encoding.

            // Given
            val plan = planFor(encoder = ThrowingEncoder())
            val event = eventWithMarker("SECURITY")

            // When
            val route = plan.route(event)

            // Then
            assertThat(route.topicClass).isEqualTo(TopicClass.AUDIT)
            assertThatThrownBy { plan.materialize(route, event) }
                .isInstanceOf(RuntimeException::class.java)
                .hasMessageContaining("simulated encoder failure")
        }
    }

    @Test
    fun `should reject an inconsistent mapping before anything is created`() {
        // What is to be tested? Whether a topic mapped to two classes fails
        //   at plan construction with the mapping's own validation error.
        // How will the test case be deemed successful and why? Successful
        //   if RecordPlan.from throws IllegalArgumentException naming the
        //   conflict - the same error start() reports - so the failure
        //   happens in the pure stage, where there is nothing to roll back.
        // Why is it important to test this test case? The plan's contract
        //   is "cannot leak"; a mapping error that only surfaced after
        //   producers exist would push it back into the transaction the
        //   type was introduced to leave.

        // Given: one topic, two classes
        val mapping =
            TopicMappingConfig().apply {
                defaultTopic = "svc.logs"
                addMapping(mapping("A", "shared.topic", "AUDIT"))
                addMapping(mapping("B", "shared.topic", "TECHNICAL"))
            }

        // When / Then
        assertThatThrownBy { planFor(mapping) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("shared.topic")
    }
}
