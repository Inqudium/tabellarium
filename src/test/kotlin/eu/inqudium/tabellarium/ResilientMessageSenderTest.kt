package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

class ResilientMessageSenderTest {
    private val baseProperties =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "broker:9092",
        )

    /**
     * Test factory that returns MockProducers with the given autoComplete mode.
     * autoComplete=true → the send callback fires immediately with success;
     * autoComplete=false → the test must call mockProducer.completeNext() or
     * mockProducer.errorNext(...) to trigger the callback.
     */
    private class TestFactory(
        private val autoComplete: Boolean = true,
    ) : ProducerFactory {
        val createdProducers = mutableListOf<MockProducer<ByteArray, ByteArray>>()

        override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
            val mock = MockProducer(autoComplete, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
            createdProducers += mock
            return mock
        }
    }

    /**
     * Test appender that records every event it receives. Started in its
     * init block because AppenderBase.doAppend() is a no-op for unstarted
     * appenders.
     */
    private class RecordingAppender : AppenderBase<ILoggingEvent>() {
        val events = mutableListOf<ILoggingEvent>()

        init {
            start()
        }

        override fun append(event: ILoggingEvent) {
            events += event
        }
    }

    /**
     * Capturing [KafkaAppenderMetrics] for tests: records every hook
     * call in a thread-safe list so assertions can inspect what the
     * sender actually invoked. Lives at the top level of the test
     * class (not inside an `@Nested inner class`) because Kotlin
     * disallows nested non-inner classes inside an inner class - and
     * the captured [Event] is a `data class`, which must be statically
     * nested.
     */
    private class CapturingMetrics : KafkaAppenderMetrics {
        data class Event(val kind: String, val topicClass: TopicClass?, val detail: String?)

        val events: MutableList<Event> = java.util.Collections.synchronizedList(mutableListOf())

        override fun eventAccepted(topicClass: TopicClass) {
            events.add(Event("accepted", topicClass, null))
        }

        override fun eventDispatched(topicClass: TopicClass) {
            events.add(Event("dispatched", topicClass, null))
        }

        override fun eventFallback(
            topicClass: TopicClass,
            reason: KafkaAppenderMetrics.FallbackReason,
        ) {
            events.add(Event("fallback", topicClass, reason.tag))
        }

        override fun sendCompleted(
            topicClass: TopicClass,
            outcome: KafkaAppenderMetrics.SendOutcome,
            duration: Duration,
        ) {
            events.add(Event("send.completed", topicClass, outcome.tag))
        }

        override fun fallbackDispatcherDropped() {
            events.add(Event("dispatcher.dropped", null, null))
        }

        override fun registerFallbackQueueGauges(
            queueSize: () -> Int,
            capacity: Int,
        ) = Unit

        fun kinds(): List<String> = synchronized(events) { events.map { it.kind } }
    }

    private fun newSender(
        autoComplete: Boolean = true,
        activeClasses: Set<TopicClass> = setOf(TopicClass.AUDIT),
        fallback: RecordingAppender? = RecordingAppender(),
        halfOpenProbeGap: Duration = ResilientMessageSender.DEFAULT_HALF_OPEN_PROBE_GAP,
        nanoTimeSource: () -> Long = System::nanoTime,
        cbRegistry: CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults(),
    ): SenderContext {
        val factory = TestFactory(autoComplete)
        val registry =
            ProducerRegistry.create(
                propertiesBuilder = ProducerPropertiesBuilder(baseProperties),
                activeTopicClasses = activeClasses,
                producerFactory = factory,
            )
        // Wrap the fallback appender in a synchronous dispatcher: the
        // synchronous flag bypasses the worker thread and the queue so
        // tests can assert "the fallback received the event" without
        // polling. See FallbackDispatcher KDoc.
        val dispatcher = fallback?.let { FallbackDispatcher(it, synchronous = true) }
        val sender =
            ResilientMessageSender(
                producerRegistry = registry,
                circuitBreakerRegistry = cbRegistry,
                fallbackDispatcher = dispatcher,
                halfOpenProbeGap = halfOpenProbeGap,
                nanoTimeSource = nanoTimeSource,
            )
        return SenderContext(sender, factory, cbRegistry, fallback, registry)
    }

    private data class SenderContext(
        val sender: ResilientMessageSender,
        val factory: TestFactory,
        val circuitBreakerRegistry: CircuitBreakerRegistry,
        val fallback: RecordingAppender?,
        val registry: ProducerRegistry,
    )

    private val basicEnrichment =
        EnrichedRecord(
            partitioningKey = "trace-abc-123",
            headers =
                mapOf(
                    "meta.component" to "payment-service".toByteArray(Charsets.UTF_8),
                    "meta.environment" to "prod".toByteArray(Charsets.UTF_8),
                ),
        )

    @Nested
    inner class `Successful send` {
        @Test
        fun `should send a record to the producer for the given topic class`() {
            // Given
            val ctx = newSender()

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload bytes".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then
            val sentRecords = ctx.factory.createdProducers[0].history()
            assertThat(sentRecords).hasSize(1)
            assertThat(sentRecords[0].topic()).isEqualTo("audit-events")
            assertThat(sentRecords[0].value()).isEqualTo("payload bytes".toByteArray())
        }

        @Test
        fun `should map the enrichment partitioning key to the record key as UTF-8 bytes`() {
            // Given
            val ctx = newSender()

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then
            val record = ctx.factory.createdProducers[0].history()[0]
            assertThat(record.key()).isEqualTo("trace-abc-123".toByteArray(Charsets.UTF_8))
        }

        @Test
        fun `should leave the record key null when the enrichment has no partitioning key`() {
            // Given
            val ctx = newSender()
            val noKeyEnrichment = basicEnrichment.copy(partitioningKey = null)

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = noKeyEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then
            val record = ctx.factory.createdProducers[0].history()[0]
            assertThat(record.key()).isNull()
        }

        @Test
        fun `should attach the enrichment headers to the record as UTF-8 bytes`() {
            // What is to be tested? Whether all entries from the enrichment's
            //   header map land on the Kafka record as proper headers with
            //   UTF-8-encoded values.
            // How will the test case be deemed successful and why? Successful if
            //   the record's headers reproduce the enrichment map exactly when
            //   each value is decoded as UTF-8. This pins down the contract that
            //   downstream consumers can rely on header names being plain strings
            //   and values being UTF-8 byte arrays.
            // Why is it important to test this test case? Header semantics are
            //   what tells downstream systems (SIEM, audit ingestion) which
            //   record came from which service in which environment. A
            //   regression here would silently corrupt downstream filtering.

            // Given
            val ctx = newSender()

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then
            val record = ctx.factory.createdProducers[0].history()[0]
            val actualHeaders =
                record.headers().toArray().associate { header ->
                    header.key() to String(header.value(), Charsets.UTF_8)
                }
            val expectedHeaders =
                basicEnrichment.headers.mapValues { (_, v) ->
                    String(v, Charsets.UTF_8)
                }
            assertThat(actualHeaders).containsExactlyInAnyOrderEntriesOf(expectedHeaders)
        }
    }

    @Nested
    inner class `Open circuit handling` {
        @Test
        fun `should route to the fallback appender when the circuit is open`() {
            // Given
            val ctx = newSender()
            val cbName = ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT)
            ctx.circuitBreakerRegistry.circuitBreaker(cbName).transitionToOpenState()

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(message = "lost event"),
            )

            // Then: producer was not touched; event reached the fallback
            assertThat(ctx.factory.createdProducers[0].history()).isEmpty()
            assertThat(ctx.fallback!!.events).hasSize(1)
            assertThat(ctx.fallback.events[0].message).isEqualTo("lost event")
        }

        @Test
        fun `should silently drop the event when the circuit is open and no fallback is configured`() {
            // Given: no fallback appender
            val ctx = newSender(fallback = null)
            val cbName = ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT)
            ctx.circuitBreakerRegistry.circuitBreaker(cbName).transitionToOpenState()

            // When / Then: must not throw
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // And: the producer was not touched
            assertThat(ctx.factory.createdProducers[0].history()).isEmpty()
        }
    }

    @Nested
    inner class `Asynchronous failure handling` {
        @Test
        fun `should route to the fallback appender when the producer callback reports an error`() {
            // What is to be tested? Whether an error reported via the Kafka send
            //   callback (the only mechanism for async delivery failures -
            //   leader-not-available, network drop, broker timeout, etc.) is
            //   correctly translated into a fallback-appender call.
            // How will the test case be deemed successful and why? Successful if
            //   the fallback receives the originalEvent only after the test
            //   explicitly triggers the error via MockProducer.errorNext().
            //   The deferred completion confirms that the sender does not
            //   block on the Future and depends entirely on the callback.
            // Why is it important to test this test case? Async-failure handling
            //   is the entire reason this sender exists; if a callback error
            //   went unhandled (audit finding F-002), the event would be lost
            //   without ever reaching the fallback - exactly the situation the
            //   refactor aims to eliminate.

            // Given: a non-auto-completing MockProducer so we control the callback
            val ctx = newSender(autoComplete = false)

            // When: send a record
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(message = "delivery will fail"),
            )

            // Then: producer received the record, fallback has not yet been called
            assertThat(ctx.factory.createdProducers[0].history()).hasSize(1)
            assertThat(ctx.fallback!!.events).isEmpty()

            // When: simulate an async failure from Kafka
            ctx.factory.createdProducers[0].errorNext(
                RuntimeException("leader not available"),
            )

            // Then: fallback received the original event
            assertThat(ctx.fallback.events).hasSize(1)
            assertThat(ctx.fallback.events[0].message).isEqualTo("delivery will fail")
        }
    }

    @Nested
    inner class `Synchronous failure handling` {
        @Test
        fun `should route to the fallback appender when the producer send throws synchronously`() {
            // Given: a closed MockProducer (which throws IllegalStateException
            //   on send - simulating buffer-full-after-max-block-ms or a
            //   prematurely closed producer)
            val ctx = newSender()
            ctx.factory.createdProducers[0].close()

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(message = "sync failure"),
            )

            // Then: fallback received the original event
            assertThat(ctx.fallback!!.events).hasSize(1)
            assertThat(ctx.fallback.events[0].message).isEqualTo("sync failure")
        }

        @Test
        fun `should silently drop the event when send throws and no fallback is configured`() {
            // Given
            val ctx = newSender(fallback = null)
            ctx.factory.createdProducers[0].close()

            // When / Then: must not throw
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )
        }
    }

    @Nested
    inner class `Topic-class isolation` {
        @Test
        fun `should use a topic-class-specific circuit breaker name`() {
            // Given
            assertThat(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))
                .isEqualTo("kafka-appender-audit")
            assertThat(ResilientMessageSender.circuitBreakerName(TopicClass.PERFORMANCE))
                .isEqualTo("kafka-appender-performance")
        }

        @Test
        fun `should not affect one topic class circuit breaker when another transitions open`() {
            // What is to be tested? Whether opening the AUDIT circuit leaves
            //   TECHNICAL sends still flowing through to the producer.
            // How will the test case be deemed successful and why? Successful if
            //   a TECHNICAL send reaches the TECHNICAL producer while the AUDIT
            //   breaker is open. This pins down the per-class isolation: a
            //   stuck audit broker does not throttle technical-log delivery.
            // Why is it important to test this test case? In production a
            //   single misbehaving topic must not cascade into a complete
            //   logging blackout. The isolation is the entire reason for
            //   having one breaker per class instead of one global breaker.

            // Given: a sender with two active classes; AUDIT breaker open
            val ctx =
                newSender(
                    activeClasses = setOf(TopicClass.AUDIT, TopicClass.TECHNICAL),
                )
            val auditCbName = ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT)
            ctx.circuitBreakerRegistry.circuitBreaker(auditCbName).transitionToOpenState()

            // When: send to TECHNICAL
            ctx.sender.send(
                topicClass = TopicClass.TECHNICAL,
                topicName = "technical-events",
                payload = "tech payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then: TECHNICAL producer received the record
            val technicalProducer =
                ctx.registry.producerFor(TopicClass.TECHNICAL)
                    as MockProducer<ByteArray, ByteArray>
            assertThat(technicalProducer.history()).hasSize(1)
            assertThat(technicalProducer.history()[0].topic()).isEqualTo("technical-events")

            // And: AUDIT fallback was not touched (we didn't send to AUDIT)
            assertThat(ctx.fallback!!.events).isEmpty()
        }
    }

    @Nested
    inner class `Half-open throttle` {
        @Test
        fun `should not throttle events when the breaker is CLOSED`() {
            // What is to be tested? Whether normal-traffic logging
            //   (breaker in CLOSED state) is unaffected by the
            //   half-open throttle. A regression here would mean the
            //   throttle silently rate-limits production logging.
            // How will the test case be deemed successful and why? Successful
            //   if a hundred rapid sends all reach the Kafka producer.
            //   The CLOSED-state pass-through is the most important
            //   invariant of HalfOpenThrottle and is asserted here
            //   end-to-end through the sender.
            // Why is it important to test this test case? An operator
            //   would never enable a half-open throttle if it could
            //   accidentally restrict normal traffic. The pass-through
            //   in CLOSED is what makes the throttle safe by default.

            // Given: a fresh sender (breaker in CLOSED state) and a
            //   non-advancing clock - so any throttle gating would
            //   manifest as denied sends
            val frozenClock = AtomicLong(0)
            val ctx = newSender(nanoTimeSource = frozenClock::get)

            // When: 100 rapid sends
            repeat(100) {
                ctx.sender.send(
                    topicClass = TopicClass.AUDIT,
                    topicName = "audit-events",
                    payload = "p".toByteArray(),
                    enrichment = basicEnrichment,
                    originalEvent = newTestLoggingEvent(),
                )
            }

            // Then: all 100 reached the producer; fallback is empty
            assertThat(ctx.factory.createdProducers[0].history()).hasSize(100)
            assertThat(ctx.fallback!!.events).isEmpty()
        }

        @Test
        fun `should route excess events to fallback in HALF_OPEN within the probe gap`() {
            // What is to be tested? Whether the throttle correctly
            //   limits probe admissions in HALF_OPEN state. When 5
            //   events arrive within the same gap window, only the
            //   first becomes a probe; the remaining 4 must be routed
            //   to the fallback without consuming Resilience4j
            //   permissions.
            // How will the test case be deemed successful and why? Successful
            //   if exactly 1 record reaches the producer and 4 reach
            //   the fallback. This pins the "one probe per gap" core
            //   behavior at the sender integration level (not just
            //   the throttle unit level).
            // Why is it important to test this test case? Without this
            //   integration test, a regression that disabled the
            //   throttle wiring in the sender would still pass the
            //   HalfOpenThrottleTest in isolation but cause the actual
            //   high-volume problem in production.

            // Given: HALF_OPEN breaker, frozen clock so the gap never elapses
            val frozenClock = AtomicLong(0)
            val ctx =
                newSender(
                    halfOpenProbeGap = Duration.ofMillis(5),
                    nanoTimeSource = frozenClock::get,
                )
            val breaker =
                ctx.circuitBreakerRegistry
                    .circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()

            // When: 5 events at the same instant
            repeat(5) {
                ctx.sender.send(
                    topicClass = TopicClass.AUDIT,
                    topicName = "audit-events",
                    payload = "p".toByteArray(),
                    enrichment = basicEnrichment,
                    originalEvent = newTestLoggingEvent(message = "evt-$it"),
                )
            }

            // Then: one probe reached the producer, four went to fallback
            assertThat(ctx.factory.createdProducers[0].history()).hasSize(1)
            assertThat(ctx.fallback!!.events).hasSize(4)
        }

        @Test
        fun `should admit a new probe after the gap has elapsed in HALF_OPEN`() {
            // What is to be tested? The complement of the previous test:
            //   once enough time has passed, the throttle allows the
            //   next probe through.
            // How will the test case be deemed successful and why? Successful
            //   if two events separated by exactly the gap both reach
            //   the producer. Pins the "spread probes over time" core
            //   property end-to-end.
            // Why is it important to test this test case? A regression
            //   that made the gap "lock once, deny forever" would still
            //   pass the previous test but break the actual goal -
            //   spreading probes, not blocking them outright.

            // Given: HALF_OPEN breaker, controlled clock
            val clock = AtomicLong(0)
            val gap = Duration.ofMillis(5)
            val ctx = newSender(halfOpenProbeGap = gap, nanoTimeSource = clock::get)
            val breaker =
                ctx.circuitBreakerRegistry
                    .circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()

            // When: first probe at t=0
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(message = "first"),
            )

            // And: clock advances by the gap, then a second probe
            clock.addAndGet(gap.toNanos())
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(message = "second"),
            )

            // Then: both probes reached the producer
            assertThat(ctx.factory.createdProducers[0].history()).hasSize(2)
            assertThat(ctx.fallback!!.events).isEmpty()
        }

        @Test
        fun `should disable throttling entirely when probe gap is zero`() {
            // Given: HALF_OPEN breaker, gap=0 (throttle disabled)
            val frozenClock = AtomicLong(0)
            val ctx =
                newSender(
                    halfOpenProbeGap = Duration.ZERO,
                    nanoTimeSource = frozenClock::get,
                )
            val breaker =
                ctx.circuitBreakerRegistry
                    .circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()

            // When: 5 events at the same instant
            repeat(5) {
                ctx.sender.send(
                    topicClass = TopicClass.AUDIT,
                    topicName = "audit-events",
                    payload = "p".toByteArray(),
                    enrichment = basicEnrichment,
                    originalEvent = newTestLoggingEvent(),
                )
            }

            // Then: all events that the breaker permits reach the
            //   producer (the breaker has permittedNumberOfCallsInHalfOpenState=10
            //   by Resilience4j default, so all 5 fit). The throttle
            //   does not add gating.
            assertThat(ctx.factory.createdProducers[0].history()).hasSize(5)
            assertThat(ctx.fallback!!.events).isEmpty()
        }
    }

    @Nested
    inner class `Circuit-breaker poisoning protection` {
        @Test
        fun `should not open the breaker on a RecordTooLargeException flood`() {
            // What is to be tested? Whether deterministic client-side
            //   exceptions (here: RecordTooLargeException) leave the
            //   breaker in CLOSED state regardless of how many times
            //   they occur. The breaker is an infrastructure-health
            //   signal, not a payload-validation filter.
            // How will the test case be deemed successful and why? Successful
            //   if 30 RecordTooLargeException callbacks in a row keep
            //   the breaker CLOSED. This is far more than the default
            //   minimumNumberOfCalls=10 and failure-rate=50% would
            //   normally tolerate, so without the ignoreExceptions
            //   wiring the breaker would have transitioned to OPEN
            //   somewhere around the 5th-10th event.
            // Why is it important to test this test case? An application
            //   bug that suddenly logs 2 MB stacktraces could otherwise
            //   silently freeze the entire logging pipeline of its
            //   service for 30 seconds. This is the exact protection
            //   the ignoreExceptions list provides; without a test that
            //   pins it down, a refactor of the config builder could
            //   easily drop the list and re-introduce the vulnerability.

            // Given: a sender using the production circuit-breaker config
            //   (the test default of CircuitBreakerRegistry.ofDefaults
            //   would NOT include our ignoreExceptions list)
            val productionCbRegistry = ResilientMessageSender.defaultCircuitBreakerRegistry()
            val ctx = newSender(autoComplete = false, cbRegistry = productionCbRegistry)
            val breaker =
                productionCbRegistry.circuitBreaker(
                    ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT),
                )

            // When: 30 records sent, all of which fail with RecordTooLargeException
            repeat(30) {
                ctx.sender.send(
                    topicClass = TopicClass.AUDIT,
                    topicName = "audit-events",
                    payload = "p".toByteArray(),
                    enrichment = basicEnrichment,
                    originalEvent = newTestLoggingEvent(message = "huge-$it"),
                )
                ctx.factory.createdProducers[0].errorNext(
                    org.apache.kafka.common.errors.RecordTooLargeException("payload exceeds max.request.size"),
                )
            }

            // Then: breaker still CLOSED, and every event went to fallback
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
            assertThat(ctx.fallback!!.events).hasSize(30)
        }

        @Test
        fun `should open the breaker on a TimeoutException flood`() {
            // What is to be tested? The complement of the previous test:
            //   transient infrastructure exceptions DO count toward the
            //   failure rate, as they should.
            // How will the test case be deemed successful and why? Successful
            //   if 20 TimeoutExceptions in a row open the breaker.
            //   This is the case the breaker exists for.
            // Why is it important to test this test case? Pins the
            //   complement of ignoreExceptions: anything not on the
            //   list must still be observed. A regression that added
            //   too many exceptions to ignoreExceptions would silently
            //   disable the breaker entirely.

            // Given
            val productionCbRegistry = ResilientMessageSender.defaultCircuitBreakerRegistry()
            val ctx = newSender(autoComplete = false, cbRegistry = productionCbRegistry)
            val breaker =
                productionCbRegistry.circuitBreaker(
                    ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT),
                )

            // When: 20 records fail with TimeoutException
            repeat(20) {
                ctx.sender.send(
                    topicClass = TopicClass.AUDIT,
                    topicName = "audit-events",
                    payload = "p".toByteArray(),
                    enrichment = basicEnrichment,
                    originalEvent = newTestLoggingEvent(message = "timeout-$it"),
                )
                ctx.factory.createdProducers[0].errorNext(
                    org.apache.kafka.common.errors.TimeoutException("ack not received in time"),
                )
            }

            // Then: breaker has transitioned to OPEN
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.OPEN)
        }

        @Test
        fun `should also ignore InvalidTopicException and SerializationException`() {
            // Given
            val productionCbRegistry = ResilientMessageSender.defaultCircuitBreakerRegistry()
            val ctx = newSender(autoComplete = false, cbRegistry = productionCbRegistry)
            val breaker =
                productionCbRegistry.circuitBreaker(
                    ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT),
                )

            // When: a mix of client-side exceptions, all ignored
            val ignoredExceptions =
                listOf<RuntimeException>(
                    org.apache.kafka.common.errors.RecordTooLargeException("too big"),
                    org.apache.kafka.common.errors.InvalidTopicException("bad name"),
                    org.apache.kafka.common.errors.SerializationException("encode failed"),
                    org.apache.kafka.common.errors.TopicAuthorizationException("denied"),
                )
            repeat(20) { i ->
                ctx.sender.send(
                    topicClass = TopicClass.AUDIT,
                    topicName = "audit-events",
                    payload = "p".toByteArray(),
                    enrichment = basicEnrichment,
                    originalEvent = newTestLoggingEvent(message = "evt-$i"),
                )
                ctx.factory.createdProducers[0].errorNext(ignoredExceptions[i % ignoredExceptions.size])
            }

            // Then: breaker remains CLOSED
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
        }
    }

    @Nested
    inner class `Metrics instrumentation` {
        @Test
        fun `should report a dispatched event with success outcome on a clean send`() {
            // Given
            val ctx = newSender()
            val metrics = CapturingMetrics()
            ctx.sender.setMetrics(metrics)

            // When: a single clean send (autoComplete=true triggers
            //   the callback synchronously with no exception)
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then: the sequence must include dispatched + send.completed(success)
            assertThat(metrics.kinds())
                .contains("dispatched", "send.completed")
            val success = metrics.events.single { it.kind == "send.completed" }
            assertThat(success.detail).isEqualTo("success")
            // And: no fallback was reported
            assertThat(metrics.kinds()).doesNotContain("fallback")
        }

        @Test
        fun `should report fallback with BREAKER_OPEN reason when the breaker is open`() {
            // What is to be tested? Whether the sender reports the
            //   correct fallback reason when the breaker denies the
            //   permission. This is the operator's primary signal for
            //   "Kafka is unreachable right now".
            // How will the test case be deemed successful and why? Successful
            //   if the captured event sequence shows exactly one
            //   fallback with reason "breaker.open" and zero dispatched.
            //   Pins the reason wiring against accidental swaps.
            // Why is it important to test this test case? The reason
            //   dimension is the entire point of the cardinality budget;
            //   if reason values are swapped the dashboard becomes
            //   misleading instead of empty (the worst kind of bug).

            // Given
            val ctx = newSender()
            val metrics = CapturingMetrics()
            ctx.sender.setMetrics(metrics)
            ctx.circuitBreakerRegistry
                .circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))
                .transitionToOpenState()

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then
            val fallbacks = metrics.events.filter { it.kind == "fallback" }
            assertThat(fallbacks).hasSize(1)
            assertThat(fallbacks[0].detail).isEqualTo("breaker.open")
            assertThat(metrics.kinds()).doesNotContain("dispatched")
        }

        @Test
        fun `should report fallback with THROTTLE reason when the half-open gap is not elapsed`() {
            // Given: HALF_OPEN breaker with a frozen clock
            val frozenClock = AtomicLong(0)
            val ctx =
                newSender(
                    halfOpenProbeGap = Duration.ofMillis(5),
                    nanoTimeSource = frozenClock::get,
                )
            val metrics = CapturingMetrics()
            ctx.sender.setMetrics(metrics)
            val breaker =
                ctx.circuitBreakerRegistry
                    .circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()

            // When: first send claims the probe slot, second is throttled
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then: the second event has a fallback with reason throttle
            val fallbacks = metrics.events.filter { it.kind == "fallback" }
            assertThat(fallbacks).hasSize(1)
            assertThat(fallbacks[0].detail).isEqualTo("throttle")
        }

        @Test
        fun `should report fallback with SEND_ERROR reason on async producer failure`() {
            // Given: a non-auto-complete producer so we control the callback
            val ctx = newSender(autoComplete = false)
            val metrics = CapturingMetrics()
            ctx.sender.setMetrics(metrics)

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )
            // The callback hasn't fired yet - only "dispatched" should be visible
            assertThat(metrics.kinds()).containsExactly("dispatched")

            // When: simulate an async error
            ctx.factory.createdProducers[0].errorNext(RuntimeException("leader gone"))

            // Then: send.completed with error outcome + fallback with reason send.error
            val sendDone = metrics.events.single { it.kind == "send.completed" }
            assertThat(sendDone.detail).isEqualTo("error")
            val fallbacks = metrics.events.filter { it.kind == "fallback" }
            assertThat(fallbacks).hasSize(1)
            assertThat(fallbacks[0].detail).isEqualTo("send.error")
        }
    }
}
