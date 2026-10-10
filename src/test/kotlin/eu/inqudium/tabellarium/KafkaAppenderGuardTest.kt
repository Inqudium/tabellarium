package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.Future

/**
 * The two guards on the hot path of [KafkaAppender]: the producer
 * self-logging guard that drops events logged by the Kafka client's
 * own threads, and the reentry guard that drops events logged
 * synchronously from inside append or producer send.
 */
class KafkaAppenderGuardTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Producer self-logging guard` {
        @Test
        fun `should ignore events from threads whose name contains a producer client id`() {
            // What is to be tested? Whether a log event originating from one
            //   of the appender's own Kafka producer threads (the client
            //   names them after the client.id) is dropped instead of being
            //   routed back into that producer.
            // How will the test case be deemed successful and why? Successful
            //   if neither the producer nor the fallback sees the event.
            //   This pins down the feedback-loop guard: producer-internal
            //   logging must never be shipped through the producer itself.
            // Why is it important to test this test case? Under broker
            //   trouble the Kafka client logs from its network thread on
            //   every failure; feeding those events back into the failing
            //   producer amplifies load and floods the fallback exactly when
            //   the system is least able to absorb it.

            // Given
            val encoder = RecordingEncoder()
            val factory = RecordingProducerFactory()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = encoder,
                    producerFactory = factory,
                    fallback = fallback,
                    component = "checkout-service",
                )
            appender.start()

            // When: an event from the producer's own network thread
            appender.doAppend(
                newTestLoggingEvent(
                    message = "Connection to node -1 could not be established",
                    threadName = "kafka-producer-network-thread | tabellarium-checkout-service-technical",
                ),
            )

            // Then: fully ignored - not encoded, not sent, not in fallback
            // (stop() drains the pipeline, so the emptiness is conclusive)
            appender.stop()
            assertThat(encoder.encodedEvents).isEmpty()
            assertThat(factory.createdProducers[0].history()).isEmpty()
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should deliver events from ordinary threads`() {
            // What is to be tested? Whether the self-logging guard is narrow
            //   enough: an event from a regular application thread (here a
            //   Tomcat worker) must pass through to the producer.
            // How will the test case be deemed successful and why? Successful
            //   if, after the draining stop(), the producer history holds
            //   exactly the one appended record. This is the positive control
            //   for the guard test above it.
            // Why is it important to test this test case? A guard that matched
            //   too broadly (or unconditionally) would silently discard
            //   application logging - the worst failure mode for a log appender,
            //   because nothing reports what was never sent.

            // Given
            val factory = RecordingProducerFactory()
            val appender = newAppender(producerFactory = factory)
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent(threadName = "http-nio-8080-exec-3"))

            // Then
            appender.stop()
            assertThat(factory.createdProducers[0].history()).hasSize(1)
        }

        @Test
        fun `should deliver events from application threads whose name merely contains a client id`() {
            // What is to be tested? Whether the guard is anchored to
            //   Kafka's producer-network-thread naming scheme instead of
            //   a bare substring match. An operator may pin a short,
            //   generic client.id; application threads whose names happen
            //   to contain it must still be logged.
            // How will the test case be deemed successful and why? Successful
            //   if an event from a thread named after the (short) client.id
            //   plus a suffix is delivered to the producer. With the old
            //   substring match this event would have been silently
            //   swallowed - the worst failure mode for a logging component.
            // Why is it important to test this test case? Silent log loss
            //   from unrelated threads is nearly undiagnosable in
            //   production; the anchored match is the guarantee that the
            //   guard only ever suppresses the producer's own logging.

            // Given: operator pins the generic client.id "app"
            val factory = RecordingProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.CLIENT_ID_CONFIG}=app
                        """.trimIndent(),
                )
            appender.start()

            // When: an ordinary application thread containing "app" logs
            appender.doAppend(newTestLoggingEvent(message = "from-app-thread", threadName = "app-worker-1"))
            // And: the actual producer network thread logs
            appender.doAppend(
                newTestLoggingEvent(
                    message = "from-producer-thread",
                    threadName = "kafka-producer-network-thread | app",
                ),
            )

            // Then: the application thread's event was delivered; the
            //   producer thread's event was suppressed (it never entered
            //   the pipeline, so the drained history is conclusive). The
            //   payload pins WHICH one survived - a count alone would also
            //   pass for an inverted guard.
            appender.stop()
            assertThat(factory.createdProducers[0].history())
                .singleElement()
                .extracting { String(it.value(), Charsets.UTF_8) }
                .isEqualTo("from-app-thread")
        }

        @Test
        fun `should not drop events when the operator sets a blank client id`() {
            // What is to be tested? Whether a blank operator-supplied
            //   client.id is excluded from the guard set.
            // How will the test case be deemed successful and why? Successful
            //   if an ordinary event is still delivered. A blank id in the
            //   guard would be contained in every thread name and silently
            //   drop all logging.
            // Why is it important to test this test case? `client.id=` (empty
            //   value) is accepted by the properties parser; without the
            //   blank-filter this valid-if-unusual configuration would turn
            //   the appender into a black hole.

            // Given
            val factory = RecordingProducerFactory()
            val appender =
                newAppender(
                    producerFactory = factory,
                    kafkaProducerProperties =
                        """
                        ${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}=test:9092
                        ${ProducerConfig.CLIENT_ID_CONFIG}=
                        """.trimIndent(),
                )
            appender.start()

            // When
            appender.doAppend(newTestLoggingEvent(threadName = "main"))

            // Then
            appender.stop()
            assertThat(factory.createdProducers[0].history()).hasSize(1)
        }

        @Test
        fun `should drop the producer logging of another appender instance in the same JVM`() {
            // What is to be tested? The cross-instance half of the guard:
            //   an event logged by the network thread of a SECOND
            //   appender's producer must not be shipped by the first, and
            //   vice versa - the guard consults a process-wide registry of
            //   every live instance's producer client ids, not only its
            //   own.
            // How will the test case be deemed successful and why? Successful
            //   if neither appender's producer receives the other's producer
            //   echo while both are started, an ordinary event still goes
            //   through, and after the second appender stopped its former
            //   producer thread name is no longer dropped by the first
            //   (the registry is left at stop). Before the fix each guard
            //   matched only its own client ids
            //   (docs/assessment/DEFECT_ANALYSIS-2026-09-15T22-05-50.md, M-3).
            // Why is it important to test this test case? Two appenders on
            //   the same logger formed a feedback loop across instances
            //   during broker trouble - each shipping the other's
            //   connection warnings through a producer whose own warnings
            //   the other shipped in turn - consuming queue and fallback
            //   capacity exactly when the degraded path must stay bounded.

            // Given: two started appenders with distinct derived client ids
            val factoryA = RecordingProducerFactory()
            val factoryB = RecordingProducerFactory()
            val appenderA = newAppender(producerFactory = factoryA, component = "service-a")
            val appenderB = newAppender(producerFactory = factoryB, component = "service-b")
            appenderA.start()
            appenderB.start()
            val producerThreadOfB = "kafka-producer-network-thread | tabellarium-service-b-technical"
            val producerThreadOfA = "kafka-producer-network-thread | tabellarium-service-a-technical"

            // When: each appender sees the other's producer echo, plus one ordinary event
            appenderA.doAppend(newTestLoggingEvent(message = "echo of B", threadName = producerThreadOfB))
            appenderB.doAppend(newTestLoggingEvent(message = "echo of A", threadName = producerThreadOfA))
            appenderA.doAppend(newTestLoggingEvent(message = "ordinary", threadName = "main"))

            // Then: only the ordinary event was shipped, by A
            appenderB.stop()
            // And: B's producer thread is no longer an echo once B left the registry
            appenderA.doAppend(newTestLoggingEvent(message = "after B stopped", threadName = producerThreadOfB))
            appenderA.stop()
            assertThat(factoryA.createdProducers[0].history().map { String(it.value(), Charsets.UTF_8) })
                .containsExactly("ordinary", "after B stopped")
            assertThat(factoryB.createdProducers[0].history()).isEmpty()
        }
    }

    @Nested
    inner class `Reentry guard` {
        @Test
        fun `should drop a reentrant event logged synchronously from inside producer send on the worker`() {
            // What is to be tested? Whether a log event created from inside
            //   producer.send is dropped instead of being fed back into the
            //   pipeline. The Kafka 4.x client logs ApiExceptions at DEBUG
            //   on the send-calling thread in its synchronous doSend failure
            //   path; since the asynchronous dispatch that thread is the
            //   send-dispatcher worker, so the event carries the worker's
            //   fixed thread name and the guard's NAME SET is what drops it.
            //   (The caller-thread half of the guard, the ThreadLocal
            //   bracket, is pinned by the next test.)
            // How will the test case be deemed successful and why? Successful
            //   if the producer receives exactly the application's own event
            //   and the reentrant "Kafka internal" event reaches neither the
            //   producer nor the fallback. Without the worker name in the
            //   guard's set, the echo would be dispatched as a second record.
            // Why is it important to test this test case? With
            //   org.apache.kafka at DEBUG - the very configuration an
            //   operator turns on to diagnose broker trouble - every
            //   synchronous send failure would otherwise feed itself, one
            //   extra record per failure, exactly during broker trouble.

            // Given: a producer whose send() first emits a log event on
            //   the calling thread (modelling Kafka's synchronous DEBUG
            //   logging), then delegates to a MockProducer
            val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
            var appenderRef: KafkaAppender? = null
            val selfLoggingFactory =
                ProducerFactory { _ ->
                    object : Producer<ByteArray, ByteArray> by mock {
                        override fun send(
                            record: ProducerRecord<ByteArray, ByteArray>,
                            callback: Callback?,
                        ): Future<RecordMetadata> {
                            checkNotNull(appenderRef).doAppend(
                                newTestLoggingEvent(
                                    message = "Kafka internal DEBUG on caller thread",
                                    loggerName = "org.apache.kafka.clients.producer.KafkaProducer",
                                    threadName = Thread.currentThread().name,
                                ),
                            )
                            return mock.send(record, callback)
                        }
                    }
                }
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = MessageBytesEncoder(),
                    producerFactory = selfLoggingFactory,
                    fallback = fallback,
                )
            appenderRef = appender
            appender.start()

            // When: the application logs one event
            appender.doAppend(newTestLoggingEvent(message = "app event", threadName = "http-nio-8080-exec-1"))

            // Then: only the application's event was sent; the reentrant
            //   Kafka-internal event was dropped entirely - no second send,
            //   nothing in the fallback. (The reentrant doAppend happens on
            //   the send worker, whose fixed thread name the guard
            //   recognizes.)
            appender.stop()
            assertThat(mock.history()).hasSize(1)
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should drop a reentrant event logged synchronously on the caller thread from inside append`() {
            // What is to be tested? Whether the appender brackets its hot
            //   path with the guard's enter()/exit() mark, so an event logged
            //   on the CALLER's own thread from inside append - by an encoder
            //   or a metrics hook that logs through SLF4J - is dropped. Such
            //   an event carries the application thread's name, so the name
            //   set cannot catch it; only the ThreadLocal mark can.
            // How will the test case be deemed successful and why? Successful
            //   if the producer receives exactly the application's event and
            //   the encoder's echo reaches neither the producer nor the
            //   fallback. The echoing encoder echoes only the application
            //   event, so a missing bracket does not overflow the stack but
            //   ships the echo as a second record - a deterministic red.
            //   Mutation checked: with enter()/exit() as no-ops in
            //   KafkaAppender.append the history holds two records.
            // Why is it important to test this test case? Before this test,
            //   no test called doAppend synchronously on the caller thread
            //   from inside append: the previous test's echo moved to the
            //   worker with the asynchronous dispatch, so a refactor dropping
            //   the bracket passed the suite and would ship a
            //   StackOverflowError on the application thread for any logging
            //   encoder (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 1).

            // Given: an encoder that logs an event on the caller thread while
            //   encoding the application's event
            var appenderRef: KafkaAppender? = null
            val echoingEncoder =
                object : MessageBytesEncoder() {
                    override fun encode(event: ILoggingEvent): ByteArray {
                        if (event.formattedMessage == "app event") {
                            checkNotNull(appenderRef).doAppend(
                                newTestLoggingEvent(
                                    message = "encoder echo on caller thread",
                                    threadName = Thread.currentThread().name,
                                ),
                            )
                        }
                        return super.encode(event)
                    }
                }
            val factory = RecordingProducerFactory()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = echoingEncoder,
                    producerFactory = factory,
                    fallback = fallback,
                )
            appenderRef = appender
            appender.start()

            // When: the application logs one event on this thread
            appender.doAppend(newTestLoggingEvent(message = "app event", threadName = Thread.currentThread().name))

            // Then: exactly the application's record was sent; the echo was
            //   dropped by the bracket - not sent, not diverted
            appender.stop()
            assertThat(factory.createdProducers.single().history()).hasSize(1)
            assertThat(
                String(
                    factory.createdProducers
                        .single()
                        .history()
                        .single()
                        .value(),
                    Charsets.UTF_8,
                ),
            ).isEqualTo("app event")
            assertThat(fallback.events).isEmpty()
        }

        @Test
        fun `should keep appending normally after a reentrant event was dropped`() {
            // What is to be tested? Whether the reentry guard is released
            //   after a top-level append in which a nested event WAS dropped
            //   - a guard that stuck in the "inside" state would silently
            //   drop every later event on that thread.
            // How will the test case be deemed successful and why? Successful
            //   if the first event's nested echo is dropped and the three
            //   following top-level events on the same thread all reach the
            //   producer. The former body appended only plain events, so no
            //   drop ever happened before the "after"
            //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
            //   finding 9).
            // Why is it important to test this test case? The guard is
            //   ThreadLocal state manipulated in a finally block; a
            //   regression here would turn one recursion incident into a
            //   permanently silenced application thread.

            // Given: an encoder that echoes once, on the caller thread, while
            //   encoding the first event
            var appenderRef: KafkaAppender? = null
            val echoingEncoder =
                object : MessageBytesEncoder() {
                    override fun encode(event: ILoggingEvent): ByteArray {
                        if (event.formattedMessage == "event-0") {
                            checkNotNull(appenderRef).doAppend(
                                newTestLoggingEvent(
                                    message = "nested echo",
                                    threadName = Thread.currentThread().name,
                                ),
                            )
                        }
                        return super.encode(event)
                    }
                }
            val factory = RecordingProducerFactory()
            val appender = newAppender(encoder = echoingEncoder, producerFactory = factory)
            appenderRef = appender
            appender.start()

            // When: the echoing event, then three plain ones on this thread
            repeat(4) { i ->
                appender.doAppend(newTestLoggingEvent(message = "event-$i", threadName = Thread.currentThread().name))
            }

            // Then: the echo was dropped, every top-level event was sent
            appender.stop()
            assertThat(factory.createdProducers[0].history().map { String(it.value(), Charsets.UTF_8) })
                .containsExactly("event-0", "event-1", "event-2", "event-3")
        }
    }
}
