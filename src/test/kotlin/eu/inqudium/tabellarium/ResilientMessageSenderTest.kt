package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong

class ResilientMessageSenderTest {
    private val baseProperties =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "broker:9092",
        )

    /**
     * Models the Kafka client's ApiException path (kafka-clients 4.x,
     * `KafkaProducer.doSend`): metadata not available within
     * max.block.ms, buffer exhausted, record too large. The client
     * invokes the callback with the exception SYNCHRONOUSLY on the
     * calling thread and returns a failed future - it does not throw.
     * MockProducer has no mode for this, so the double implements it.
     */
    private class SynchronousCallbackErrorProducer(
        private val mock: MockProducer<ByteArray, ByteArray>,
        private val error: Exception,
    ) : Producer<ByteArray, ByteArray> by mock {
        override fun send(
            record: ProducerRecord<ByteArray, ByteArray>,
            callback: Callback?,
        ): Future<RecordMetadata> {
            callback?.onCompletion(null, error)
            return CompletableFuture<RecordMetadata>().apply { completeExceptionally(error) }
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
        data class Event(
            val kind: String,
            val topicClass: TopicClass?,
            val detail: String?,
        )

        val events: MutableList<Event> = Collections.synchronizedList(mutableListOf())

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

        override fun registerSendQueueGauges(
            topicClass: TopicClass,
            queueSize: () -> Int,
            capacity: Int,
        ) = Unit

        fun kinds(): List<String> = synchronized(events) { events.map { it.kind } }
    }

    /** Every context a test built; closed after the test so no dispatcher worker or producer outlives it. */
    private val openContexts = mutableListOf<SenderContext>()

    @AfterEach
    fun closeContexts() {
        openContexts.forEach { it.close() }
        openContexts.clear()
    }

    private fun newSender(
        autoComplete: Boolean = true,
        activeClasses: Set<TopicClass> = setOf(TopicClass.AUDIT),
        fallback: RecordingAppender? = RecordingAppender(),
        halfOpenProbeGap: Duration = ResilientMessageSender.DEFAULT_HALF_OPEN_PROBE_GAP,
        nanoTimeSource: () -> Long = System::nanoTime,
        cbRegistry: CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults(),
        wrapProducer: (MockProducer<ByteArray, ByteArray>) -> Producer<ByteArray, ByteArray> = { it },
    ): SenderContext {
        val factory = RecordingProducerFactory(autoComplete, wrapProducer)
        val registry =
            ProducerRegistry.create(
                propertiesBuilder = ProducerPropertiesBuilder(baseProperties),
                activeTopicClasses = activeClasses,
                producerFactory = factory,
            )
        // Real (asynchronous) fallback dispatcher - the same wiring the
        // appender uses in production. Assertions on the fallback
        // recorder poll with pollUntil, since delivery happens on the
        // dispatcher's worker thread.
        val dispatcher = fallback?.let { FallbackDispatcher(it) }
        val sender =
            ResilientMessageSender(
                producerRegistry = registry,
                circuitBreakerRegistry = cbRegistry,
                fallbackDispatcher = dispatcher,
                halfOpenProbeGap = halfOpenProbeGap,
                nanoTimeSource = nanoTimeSource,
            )
        return SenderContext(sender, factory, cbRegistry, fallback, registry, dispatcher).also { openContexts += it }
    }

    private data class SenderContext(
        val sender: ResilientMessageSender,
        val factory: RecordingProducerFactory,
        val circuitBreakerRegistry: CircuitBreakerRegistry,
        val fallback: RecordingAppender?,
        val registry: ProducerRegistry,
        val dispatcher: FallbackDispatcher?,
    ) : AutoCloseable {
        /** The fallback recorder of a context built with one; fails with a message otherwise. */
        val recorder: RecordingAppender
            get() = checkNotNull(fallback) { "this sender context was built without a fallback recorder" }

        override fun close() {
            // Order as in the appender: producers first (they can still
            // divert into the dispatcher), then the dispatcher drains.
            runCatching { registry.close() }
            dispatcher?.close()
            fallback?.stop()
        }
    }

    private val basicEnrichment =
        EnrichedRecord(
            partitioningKey = "trace-abc-123",
            headers =
                listOf(
                    RecordHeader("meta.component", "payment-service".toByteArray(Charsets.UTF_8)),
                    RecordHeader("meta.environment", "prod".toByteArray(Charsets.UTF_8)),
                ),
        )

    @Nested
    inner class `Successful send` {
        @Test
        fun `should send a record to the producer for the given topic class`() {
            // What is to be tested? The happy path of send(): a permitted event
            //   becomes exactly one ProducerRecord on the producer the registry
            //   holds for the topic class, addressed to the given topic name and
            //   carrying the encoded payload as its value.
            // How will the test case be deemed successful and why? Successful
            //   if the MockProducer's history holds exactly one record whose
            //   topic and value equal the arguments passed to send(). This pins
            //   the topicName -> record.topic and payload -> record.value mapping
            //   of buildRecord.
            // Why is it important to test this test case? Every other test in
            //   this class exercises a deviation from this path; if the plain
            //   send misrouted or duplicated records, nothing would reach the
            //   right Kafka topic even with a perfectly healthy cluster.

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
            // What is to be tested? Whether the enrichment's partitioningKey (a
            //   String, e.g. the trace id) becomes the record key as its UTF-8
            //   encoding - the key is what Kafka's default partitioner hashes,
            //   so it decides the partition.
            // How will the test case be deemed successful and why? Successful
            //   if the sent record's key is byte-equal to the UTF-8 bytes of
            //   "trace-abc-123". Pins the charset: a platform-default or UTF-16
            //   encoding would still be "a key" but hash differently.
            // Why is it important to test this test case? Events of one trace
            //   must land in one partition so consumers see them in order; a
            //   changed encoding would silently scatter a trace across
            //   partitions and break per-key ordering for downstream readers.

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
            // What is to be tested? The absent-key case: an enrichment without
            //   a partitioningKey must yield a record with a null key, not an
            //   empty byte array or a placeholder string.
            // How will the test case be deemed successful and why? Successful
            //   if the sent record's key() is null. A null key lets the Kafka
            //   partitioner spread the record (sticky/round-robin); an empty
            //   array would hash every keyless event to the same partition.
            // Why is it important to test this test case? Events without a trace
            //   context (startup logs, background jobs) are common; funnelling
            //   all of them into one partition would create a hot partition and
            //   an ordering guarantee nobody asked for.

            // Given
            val ctx = newSender()
            val noKeyEnrichment = EnrichedRecord(partitioningKey = null, headers = basicEnrichment.headers)

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
            //   header list land on the Kafka record as proper headers with
            //   UTF-8-encoded values.
            // How will the test case be deemed successful and why? Successful if
            //   the record's headers reproduce the enrichment list exactly when
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
                basicEnrichment.headers.associate { header ->
                    header.key() to String(header.value(), Charsets.UTF_8)
                }
            assertThat(actualHeaders).containsExactlyInAnyOrderEntriesOf(expectedHeaders)
        }
    }

    @Nested
    inner class `Open circuit handling` {
        @Test
        fun `should route to the fallback appender when the circuit is open`() {
            // What is to be tested? The OPEN-breaker path of send(): when
            //   tryAcquirePermission is denied, the producer is never called and
            //   the original ILoggingEvent goes to the fallback dispatcher
            //   instead.
            // How will the test case be deemed successful and why? Successful
            //   if the producer's history stays empty and the fallback recorder
            //   receives exactly the one event, identified by its message. Both
            //   halves matter: the event must not be sent AND must not be lost.
            // Why is it important to test this test case? During a Kafka outage
            //   the breaker is open for 30 s at a time; this path is what keeps
            //   the logs of those 30 s on disk instead of discarding them or
            //   hammering a dead broker.

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
            pollUntil { ctx.recorder.events.size == 1 }
            assertThat(ctx.recorder.events[0].message).isEqualTo("lost event")
        }

        @Test
        fun `should silently drop the event when the circuit is open and no fallback is configured`() {
            // What is to be tested? The operator's "no fallback" choice on the
            //   OPEN-breaker path: with a null fallback dispatcher the event is
            //   dropped, and send() neither throws nor touches the producer.
            // How will the test case be deemed successful and why? Successful
            //   if send() returns normally (an exception would fail the test
            //   right there) and the producer history is empty. Pins the
            //   null-safe sendToFallback plus the "denied -> return" flow.
            // Why is it important to test this test case? send() runs on the
            //   send-dispatcher worker; an NPE here would kill that worker and
            //   turn a deliberate best-effort configuration into a dead pipeline
            //   for the whole topic class.

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
            //   went unhandled, the event would be lost without ever reaching
            //   the fallback - exactly the situation this sender is meant to
            //   eliminate.

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
            assertThat(ctx.recorder.events).isEmpty()

            // When: simulate an async failure from Kafka
            ctx.factory.createdProducers[0].errorNext(
                RuntimeException("leader not available"),
            )

            // Then: fallback received the original event
            pollUntil { ctx.recorder.events.size == 1 }
            assertThat(ctx.recorder.events[0].message).isEqualTo("delivery will fail")
        }
    }

    @Nested
    inner class `Synchronous failure handling` {
        @Test
        fun `should route to the fallback appender when the producer send throws synchronously`() {
            // What is to be tested? The synchronous-throw path of send(): when
            //   producer.send itself throws (here a closed MockProducer raising
            //   IllegalStateException), the exception is caught and the original
            //   event is diverted to the fallback.
            // How will the test case be deemed successful and why? Successful
            //   if the fallback recorder receives exactly the one event with the
            //   message "sync failure" - which also proves the exception did not
            //   escape send() to the caller.
            // Why is it important to test this test case? A closed producer, an
            //   InterruptException from max.block.ms or a non-API KafkaException
            //   would otherwise propagate into the send worker or lose the
            //   event; this path keeps a thrown send on the same fallback
            //   contract as a callback error.

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
            pollUntil { ctx.recorder.events.size == 1 }
            assertThat(ctx.recorder.events[0].message).isEqualTo("sync failure")
        }

        @Test
        fun `should silently drop the event when send throws and no fallback is configured`() {
            // What is to be tested? The "no fallback" operator choice on
            //   the synchronous-throw path: the event is dropped without
            //   an exception escaping, but every accounting hook still
            //   fires exactly once - the drop is silent for the caller,
            //   not for the metrics.
            // How will the test case be deemed successful and why? Successful
            //   if send returns normally and the metrics show exactly one
            //   send.completed(error) and one fallback(send.error) with no
            //   dispatched - so a regression that swallowed the failure
            //   before the hooks, or counted it as dispatched, is caught.
            // Why is it important to test this test case? Without a
            //   fallback the metrics are the ONLY trace of the loss; a
            //   test that merely proves "does not throw" would let them
            //   silently go dark.

            // Given
            val ctx = newSender(fallback = null)
            val metrics = CapturingMetrics()
            ctx.sender.setMetrics(metrics)
            ctx.factory.createdProducers[0].close()

            // When: must not throw
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "payload".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(),
            )

            // Then: accounted exactly once as a send error, never as dispatched
            assertThat(metrics.kinds()).containsExactly("send.completed", "fallback")
            assertThat(metrics.events.single { it.kind == "send.completed" }.detail).isEqualTo("error")
            assertThat(metrics.events.single { it.kind == "fallback" }.detail).isEqualTo("send.error")
        }
    }

    @Nested
    inner class `Synchronous callback error handling` {
        @Test
        fun `should count a send the client failed through the synchronous callback as fallback only, never as dispatched`() {
            // What is to be tested? The accounting on the Kafka client's
            //   ApiException path: for a metadata timeout (max.block.ms
            //   elapsed - the standard broker-outage symptom), buffer
            //   exhaustion or an oversized record, kafka-clients 4.x
            //   invokes the callback with the exception synchronously
            //   on the calling thread and returns without throwing. The
            //   sender must not count such an event as dispatched on
            //   top of the fallback the callback already recorded.
            // How will the test case be deemed successful and why? Successful
            //   if the captured metrics hold exactly one
            //   send.completed(error) and one fallback(send.error) and
            //   NO dispatched, the breaker saw exactly one failure, and
            //   the event reached the fallback appender exactly once.
            //   MockProducer cannot model this path (it either throws or
            //   defers to errorNext), hence the dedicated producer double.
            // Why is it important to test this test case? Before the fix,
            //   exactly the events operators inspect during an outage
            //   (the ~10 until the breaker opens, plus every half-open
            //   probe) counted as both dispatched and fallback, breaking
            //   the conservation the dashboards are built on.

            // Given: a producer that fails every send through the
            //   synchronous callback
            val ctx =
                newSender(
                    cbRegistry = ResilientMessageSender.defaultCircuitBreakerRegistry(),
                    wrapProducer = { mock -> SynchronousCallbackErrorProducer(mock, TimeoutException("Topic audit-events not present in metadata after 500 ms.")) },
                )
            val metrics = CapturingMetrics()
            ctx.sender.setMetrics(metrics)
            val breaker =
                ctx.circuitBreakerRegistry.circuitBreaker(ResilientMessageSender.circuitBreakerName(TopicClass.AUDIT))

            // When
            ctx.sender.send(
                topicClass = TopicClass.AUDIT,
                topicName = "audit-events",
                payload = "p".toByteArray(),
                enrichment = basicEnrichment,
                originalEvent = newTestLoggingEvent(message = "metadata timeout"),
            )

            // Then: fallback only - accepted = dispatched + fallback holds
            assertThat(metrics.kinds()).containsExactly("send.completed", "fallback")
            assertThat(metrics.events.single { it.kind == "send.completed" }.detail).isEqualTo("error")
            assertThat(metrics.events.single { it.kind == "fallback" }.detail).isEqualTo("send.error")
            assertThat(breaker.metrics.numberOfFailedCalls).isEqualTo(1)
            pollUntil { ctx.recorder.events.size == 1 }
            assertThat(
                ctx.recorder
                    .events
                    .single()
                    .message,
            ).isEqualTo("metadata timeout")
        }

        @Test
        fun `should still count an asynchronously completed send as dispatched`() {
            // What is to be tested? The complement of the previous test: the
            //   errorReported check that suppresses "dispatched" for a
            //   synchronous callback failure must NOT suppress it for a send
            //   whose callback is still pending when producer.send returns.
            // How will the test case be deemed successful and why? Successful
            //   if the metrics show exactly "dispatched" right after send() and,
            //   once the deferred callback fails, "dispatched, send.completed,
            //   fallback" - the documented later outcome of an already-dispatched
            //   event.
            // Why is it important to test this test case? An over-eager fix for
            //   the synchronous case could zero the dispatched counter for every
            //   normal send; the accepted = dispatched + fallback balance on the
            //   dashboards would then break in the healthy state instead of the
            //   outage state.

            // Given: the regular deferred-callback producer - the
            //   complement of the previous test, pinning that the
            //   synchronous-failure detection does not suppress
            //   dispatched for a send whose outcome is still pending
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

            // Then: dispatched, outcome unknown so far
            assertThat(metrics.kinds()).containsExactly("dispatched")

            // And: a later asynchronous error adds the send.error fallback
            //   on top of the dispatched count - a later outcome of the
            //   same event, documented as such on eventAccepted
            ctx.factory.createdProducers[0].errorNext(RuntimeException("leader gone"))
            assertThat(metrics.kinds()).containsExactly("dispatched", "send.completed", "fallback")
        }
    }

    @Nested
    inner class `Topic-class isolation` {
        @Test
        fun `should use a topic-class-specific circuit breaker name`() {
            // What is to be tested? The naming scheme of circuitBreakerName:
            //   "kafka-appender-" plus the lowercase topic class, giving one
            //   distinct registry key per class.
            // How will the test case be deemed successful and why? Successful
            //   if AUDIT and PERFORMANCE map to "kafka-appender-audit" and
            //   "kafka-appender-performance". Two classes are enough to pin both
            //   the prefix and the lowercase derivation.
            // Why is it important to test this test case? The name is the
            //   registry key that gives each class its own breaker and the
            //   `name` tag of the Resilience4j metrics; a scheme change would
            //   silently merge the breakers (one class's outage trips all) and
            //   break every operator dashboard that filters on the tag.

            // When / Then: the name is derived from the class, lowercase
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
            assertThat(ctx.recorder.events).isEmpty()
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
            assertThat(ctx.recorder.events).isEmpty()
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
            pollUntil { ctx.recorder.events.size == 4 }
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
            assertThat(ctx.recorder.events).isEmpty()
        }

        @Test
        fun `should disable throttling entirely when probe gap is zero`() {
            // What is to be tested? Whether halfOpenProbeGap = Duration.ZERO
            //   reaches the per-class HalfOpenThrottle as its "disabled"
            //   sentinel, so that in HALF_OPEN every event goes straight to the
            //   breaker instead of one probe per gap.
            // How will the test case be deemed successful and why? Successful
            //   if 5 events at the same frozen instant all reach the producer
            //   and none the fallback. With a non-zero gap the same setup routes
            //   4 of 5 to the fallback (see the tests above), so the assertion
            //   isolates the zero-gap wiring.
            // Why is it important to test this test case? Duration.ZERO is the
            //   documented off-switch for the throttle; if the sender
            //   substituted a default or the throttle treated zero as "always
            //   too soon", an operator disabling the throttle would get either
            //   the throttle or a permanently gated breaker.

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
            assertThat(ctx.recorder.events).isEmpty()
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
                    RecordTooLargeException("payload exceeds max.request.size"),
                )
            }

            // Then: breaker still CLOSED, and every event went to fallback
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
            pollUntil { ctx.recorder.events.size == 30 }
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
                    TimeoutException("ack not received in time"),
                )
            }

            // Then: breaker has transitioned to OPEN
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.OPEN)
        }

        @Test
        fun `should also ignore InvalidTopicException and SerializationException`() {
            // What is to be tested? Whether the full ignoreExceptions list of
            //   defaultCircuitBreakerConfig - not just RecordTooLargeException -
            //   is wired: InvalidTopicException, SerializationException and
            //   TopicAuthorizationException must not count as failures.
            // How will the test case be deemed successful and why? Successful
            //   if 20 callback errors cycling through all four exception types
            //   leave the breaker CLOSED; with minimumNumberOfCalls=10 and a
            //   50 % threshold, a single non-ignored type in the mix would open
            //   it.
            // Why is it important to test this test case? Each of these is
            //   deterministic per record (bad topic name, encoder bug, ACL
            //   change) and unaffected by a breaker recovery; counting them
            //   would silence a healthy topic class for 30 s per trip - while
            //   the RecordTooLargeException test alone would still pass.

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
                    RecordTooLargeException("too big"),
                    InvalidTopicException("bad name"),
                    SerializationException("encode failed"),
                    TopicAuthorizationException("denied"),
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
            // What is to be tested? The metrics sequence for the happy path: a
            //   send accepted by the producer reports eventDispatched, and its
            //   callback reports sendCompleted with outcome SUCCESS - and no
            //   fallback is reported for it.
            // How will the test case be deemed successful and why? Successful
            //   if the captured kinds contain "dispatched" and one
            //   "send.completed" whose detail is "success", and no "fallback".
            //   The auto-completing MockProducer fires the callback
            //   synchronously, so the assertions can run inline.
            // Why is it important to test this test case? The dispatched counter
            //   and the success timer are the baseline every outage is measured
            //   against; a clean send that reported error, or a fallback, would
            //   make the dashboards show a permanent outage in a healthy system.

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
            // What is to be tested? The reason tag of the throttle path: an
            //   event denied by the HalfOpenThrottle (HALF_OPEN, gap not
            //   elapsed) must be reported as fallback with reason THROTTLE,
            //   distinct from BREAKER_OPEN.
            // How will the test case be deemed successful and why? Successful
            //   if of two sends at the same frozen instant exactly one fallback
            //   is reported and its detail is "throttle": the first send claims
            //   the probe slot, the second is gated.
            // Why is it important to test this test case? THROTTLE means "Kafka
            //   is recovering, probes are being spaced", BREAKER_OPEN means
            //   "Kafka is down" - conflating them would hide from operators
            //   whether an outage is ending or ongoing.

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
            // What is to be tested? The metrics sequence of an asynchronous
            //   delivery failure: dispatched at send time, then - when the
            //   callback reports the error - sendCompleted with outcome ERROR
            //   and a fallback with reason SEND_ERROR.
            // How will the test case be deemed successful and why? Successful
            //   if only "dispatched" is visible before errorNext and afterwards
            //   exactly one send.completed(error) and one fallback(send.error)
            //   exist. The deferred MockProducer makes the before/after split
            //   observable.
            // Why is it important to test this test case? Broker-side failures
            //   (leader gone, network drop) arrive only via the callback; if
            //   that path tagged them BREAKER_OPEN or reported no fallback at
            //   all, the dashboards would misattribute or hide the very failures
            //   the breaker reacts to.

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
