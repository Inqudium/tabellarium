package eu.inqudium.tabellarium

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.LoggingEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
import org.slf4j.MarkerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * The asynchronous dispatch of [KafkaAppender]: doAppend returning while
 * a producer send is blocked, per-class isolation, overflow diversion,
 * exactly-once delivery of a pinned in-flight event after stop, the
 * concurrent hot path, and delivery through the real fallback worker.
 */
class KafkaAppenderDispatchTest : KafkaAppenderTestBase() {
    @Nested
    inner class `Asynchronous send dispatch` {
        /**
         * Factory whose producers park every send() on a latch until
         * released - modelling producer.send blocked in max.block.ms.
         * [blockedClasses] restricts the blocking to specific topic
         * classes (matched via the derived client.id); other classes
         * get plain auto-completing MockProducers.
         */
        private inner class BlockingProducerFactory(
            private val blockedClasses: Set<String> = setOf("audit", "functional", "technical", "performance"),
        ) : ProducerFactory {
            val release = CountDownLatch(1)
            val sendEntered = CountDownLatch(1)
            val producersByClientId = mutableMapOf<String, MockProducer<ByteArray, ByteArray>>()

            override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
                val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
                val clientId = properties[ProducerConfig.CLIENT_ID_CONFIG].orEmpty()
                producersByClientId[clientId] = mock
                val blocked = blockedClasses.any { clientId.endsWith("-$it") }
                // Always wrap: MockProducer is not thread-safe, and the
                // dispatcher worker sends while the test thread polls
                // history - same rationale as SynchronizingProducerFactory.
                return object : Producer<ByteArray, ByteArray> by mock {
                    override fun send(
                        record: ProducerRecord<ByteArray, ByteArray>,
                        callback: Callback?,
                    ): Future<RecordMetadata> {
                        if (blocked) {
                            sendEntered.countDown()
                            release.await()
                        }
                        return synchronized(mock) { mock.send(record, callback) }
                    }
                }
            }

            /** History size of the producer whose client.id ends in [clientIdSuffix], read under the send lock. */
            fun historySize(clientIdSuffix: String): Int {
                val mock =
                    producersByClientId.entries
                        .single { it.key.endsWith(clientIdSuffix) }
                        .value
                return synchronized(mock) { mock.history().size }
            }
        }

        @Test
        fun `should return from doAppend while producer send is blocked`() {
            // What is to be tested? The end-to-end never-block guarantee
            //   (finding H-1 in
            //   docs/assessment/CODE_ANALYSIS-2026-08-28T22-20-43.md):
            //   with the asynchronous dispatch, doAppend returns
            //   immediately even while producer.send is parked - the
            //   situation a broker outage creates for up to max.block.ms
            //   per send.
            // How will the test case be deemed successful and why? Successful
            //   if a doAppend issued on a helper thread while the dispatcher
            //   worker is provably inside the blocked send returns - observed
            //   through a latch with a generous timeout - while the send is
            //   still parked. The latch anchors the worker, so the assertion
            //   is deterministic, not timing-lucky; and a regression to a
            //   synchronous send parks the helper, not the test thread, so it
            //   fails as a bounded red instead of a Surefire hang (the former
            //   elapsed-time bound could only ever fail spuriously;
            //   docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 13).
            // Why is it important to test this test case? This is the
            //   missing latency assertion from finding H-3 (same report):
            //   every prior
            //   test used an auto-completing MockProducer that never
            //   blocks, so a synchronous send path stayed green.

            // Given: an appender with a producer that parks every send
            val factory = BlockingProducerFactory()
            val appender =
                newAppender(
                    encoder = MessageBytesEncoder(),
                    producerFactory = factory,
                )
            appender.start()
            // Both appends run on helper threads: a regression that sends
            //   on the caller would park the CALLER in release.await(), and
            //   on the test thread that would be a red only after the suite
            //   timeout instead of a bounded one here
            //   (DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 12).
            val firstAppendReturned = CountDownLatch(1)
            val secondAppendReturned = CountDownLatch(1)
            val firstCaller =
                Thread {
                    appender.doAppend(newTestLoggingEvent(message = "first"))
                    firstAppendReturned.countDown()
                }
            val secondCaller =
                Thread {
                    appender.doAppend(newTestLoggingEvent(message = "second"))
                    secondAppendReturned.countDown()
                }
            try {
                // When: the first event parks the worker in the send
                firstCaller.start()
                assertThat(factory.sendEntered.await(2, TimeUnit.SECONDS)).isTrue()
                assertThat(firstAppendReturned.await(2, TimeUnit.SECONDS)).isTrue()

                // And: a second append on a helper thread while the send is
                //   still parked (release has not been counted down)
                secondCaller.start()

                // Then: the caller returned although the send is parked
                //   (the latch is released only in the finally below, so
                //   the two awaits above are the whole proof)
                assertThat(secondAppendReturned.await(2, TimeUnit.SECONDS)).isTrue()
            } finally {
                factory.release.countDown()
                firstCaller.join(2000)
                secondCaller.join(2000)
                appender.stop()
            }
        }

        @Test
        fun `should keep delivering technical events while an audit send is blocked`() {
            // What is to be tested? The per-class isolation of the send
            //   dispatchers: a blocked AUDIT producer (stuck broker for
            //   that class) must not delay TECHNICAL delivery - the same
            //   isolation the per-class producers and breakers promise.
            // How will the test case be deemed successful and why? Successful
            //   if, while the AUDIT worker is provably parked in its send,
            //   TECHNICAL events still reach their producer.
            // Why is it important to test this test case? With a single
            //   shared worker, one stuck class would stall all logging
            //   until its breaker opens (~10 x max.block.ms); the
            //   per-class split is what bounds the blast radius.

            // Given: AUDIT blocked, TECHNICAL free
            val factory = BlockingProducerFactory(blockedClasses = setOf("audit"))
            val appender =
                newAppender(
                    encoder = MessageBytesEncoder(),
                    producerFactory = factory,
                )
            appender.topicMapping.addMapping(
                TopicMappingEntry().apply {
                    marker = "SECURITY"
                    topic = "audit.security"
                    topicClass = "AUDIT"
                },
            )
            appender.start()
            try {
                // When: an AUDIT event parks its worker
                appender.doAppend(
                    (newTestLoggingEvent(message = "audit-event") as LoggingEvent)
                        .apply { addMarker(MarkerFactory.getDetachedMarker("SECURITY")) },
                )
                assertThat(factory.sendEntered.await(2, TimeUnit.SECONDS)).isTrue()

                // And: TECHNICAL events keep flowing
                repeat(3) { i ->
                    appender.doAppend(newTestLoggingEvent(message = "technical-$i"))
                }

                // Then: the technical producer received them all
                pollUntil { factory.historySize("-technical") == 3 }
            } finally {
                factory.release.countDown()
                appender.stop()
            }
        }

        @Test
        fun `should divert overflowing events to the fallback instead of blocking the caller`() {
            // What is to be tested? The overflow policy end-to-end: a full
            //   send queue (worker parked, capacity exhausted) diverts
            //   further events to the fallback appender instead of
            //   blocking the logging thread.
            // How will the test case be deemed successful and why? Successful
            //   if with capacity 1 and a parked worker the surplus events
            //   arrive in the (synchronous) fallback recorder while
            //   doAppend keeps returning immediately.
            // Why is it important to test this test case? The bounded
            //   queue is what makes the never-blocking promise safe; the
            //   fallback diversion is what keeps it loss-visible.

            // Given
            val factory = BlockingProducerFactory()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = MessageBytesEncoder(),
                    producerFactory = factory,
                    fallback = fallback,
                )
            appender.sendQueueCapacity = 1
            appender.start()
            try {
                // When: first event parks the worker, second fills the
                //   queue, the rest overflow
                appender.doAppend(newTestLoggingEvent(message = "in-flight"))
                assertThat(factory.sendEntered.await(2, TimeUnit.SECONDS)).isTrue()
                appender.doAppend(newTestLoggingEvent(message = "queued"))
                appender.doAppend(newTestLoggingEvent(message = "overflow-1"))
                appender.doAppend(newTestLoggingEvent(message = "overflow-2"))

                // Then: the overflow events reached the fallback
                pollUntil { fallback.events.size == 2 }
                assertThat(fallback.events.map { it.formattedMessage })
                    .containsExactly("overflow-1", "overflow-2")
            } finally {
                factory.release.countDown()
                appender.stop()
            }
        }

        @Test
        fun `should deliver the in-flight event to the fallback exactly once when a pinned send fails after stop`() {
            // What is to be tested? The exactly-once diversion guard
            //   across dispatcher AND sender: stop() claims the event
            //   pinned in an uninterruptible producer.send and diverts it
            //   (reason shutdown); when the send later unblocks by
            //   THROWING, the sender's own send-error routing must stand
            //   down instead of delivering the same event a second time.
            // How will the test case be deemed successful and why? Successful
            //   if the fallback recorder holds exactly one copy of the
            //   pinned event - after stop() and still after the late
            //   failure. Without the shared claim, this scenario produced
            //   a duplicate fallback line and a double fallback count.
            // Why is it important to test this test case? Loss accounting
            //   is the operator's trust anchor during shutdown forensics;
            //   duplicates overstate the loss and pollute the fallback
            //   file with repeated (possibly audit-grade) records.

            // Given: a producer whose send blocks uninterruptibly, then
            //   throws once released - modelling a send parked past the
            //   interrupt grace that fails late. The double also captures
            //   the send worker thread, so the test can join it and thereby
            //   know the sender's error path has fully run.
            val release = CountDownLatch(1)
            val sendEntered = CountDownLatch(1)
            val sendWorker = AtomicReference<Thread?>()
            val throwingBlockedFactory =
                ProducerFactory { _ ->
                    val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
                    object : Producer<ByteArray, ByteArray> by mock {
                        override fun send(
                            record: ProducerRecord<ByteArray, ByteArray>,
                            callback: Callback?,
                        ): Future<RecordMetadata> {
                            sendWorker.set(Thread.currentThread())
                            sendEntered.countDown()
                            awaitUninterruptibly(release)
                            throw RuntimeException("late send failure")
                        }
                    }
                }
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = MessageBytesEncoder(),
                    producerFactory = throwingBlockedFactory,
                    fallback = fallback,
                )
            appender.start()
            // The metrics are the observable for the late path: by then
            // stop() has closed the fallback dispatcher and stopped the
            // recorder, so a duplicate could never reach the recorder
            // even with a broken claim. stop() deliberately leaves the
            // transport's recorders on the bound Micrometer implementation
            // (only the hot path's own reference is reset - see the
            // rationale at KafkaAppender.stop(),
            // DEFECT_ANALYSIS-2026-10-10T19-11-11, finding 10), so its meter
            // objects keep counting after stop() removed them from the
            // registry: the meters are captured before stop() and read
            // afterwards.
            val registry = SimpleMeterRegistry()
            appender.bindMeterRegistry(registry)
            val sendErrorFallbacks =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    MicrometerKafkaAppenderMetrics.TAG_REASON to "send.error",
                )
            val shutdownFallbacks =
                registry.requireCounter(
                    MicrometerKafkaAppenderMetrics.METRIC_EVENTS_FALLBACK,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    MicrometerKafkaAppenderMetrics.TAG_REASON to "shutdown",
                )
            val sendErrors =
                registry.requireTimer(
                    MicrometerKafkaAppenderMetrics.METRIC_SEND_DURATION,
                    MicrometerKafkaAppenderMetrics.TAG_TOPIC_CLASS to "technical",
                    MicrometerKafkaAppenderMetrics.TAG_OUTCOME to "error",
                )
            // Safety: the release lives in a finally - a red assertion
            //   before it would otherwise leave the send worker parked in
            //   awaitUninterruptibly for the rest of the fork
            //   (DEFECT_ANALYSIS-2026-10-11T16-25-57, finding 12).
            try {
                appender.doAppend(newTestLoggingEvent(message = "pinned"))
                assertThat(sendEntered.await(2, TimeUnit.SECONDS)).isTrue()

                // When: stop() claims and diverts the pinned event (reason
                //   shutdown), then the send unblocks and fails; joining the
                //   worker proves the sender's error routing has completed
                appender.stop()
                assertThat(fallback.events.map { it.formattedMessage }).containsExactly("pinned")
                assertThat(shutdownFallbacks.count()).isEqualTo(1.0)
                release.countDown()
                val worker = checkNotNull(sendWorker.get()) { "the producer double never captured the send worker" }
                worker.join(5000)
                assertThat(worker.isAlive).isFalse()

                // Then: the late failure was observed (send.duration error),
                //   but the sender's error routing found the diversion already
                //   claimed - no second fallback of any reason, and still
                //   exactly one copy in the recorder
                assertThat(sendErrors.count()).isEqualTo(1L)
                assertThat(sendErrorFallbacks.count()).isEqualTo(0.0)
                assertThat(shutdownFallbacks.count()).isEqualTo(1.0)
                assertThat(fallback.events.map { it.formattedMessage }).containsExactly("pinned")
            } finally {
                release.countDown()
            }
        }
    }

    @Nested
    inner class `Hot path concurrency` {
        /**
         * Factory wrapping each MockProducer in a synchronizing delegate:
         * MockProducer itself is not thread-safe (its history is a plain
         * list), so without the wrapper the test harness would race
         * internally and hide or fabricate failures. The appender-side
         * code under test still runs fully concurrently.
         */
        private inner class SynchronizingProducerFactory : ProducerFactory {
            val mock =
                MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())

            override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> =
                object : Producer<ByteArray, ByteArray> by mock {
                    override fun send(
                        record: ProducerRecord<ByteArray, ByteArray>,
                        callback: Callback?,
                    ): Future<RecordMetadata> = synchronized(mock) { mock.send(record, callback) }

                    override fun send(record: ProducerRecord<ByteArray, ByteArray>): Future<RecordMetadata> = synchronized(mock) { mock.send(record) }
                }

            fun historySize(): Int = synchronized(mock) { mock.history().size }

            fun sentMessages(): List<String> = synchronized(mock) { mock.history().map { String(it.value(), Charsets.UTF_8) } }
        }

        @Test
        fun `should deliver every event when many threads append concurrently`() {
            // What is to be tested? Whether the unsynchronized hot path
            //   (append -> route -> classify -> enrich -> dispatch, plus
            //   the breaker/throttle gates) is actually safe under real
            //   contention - the central design claim behind extending
            //   UnsynchronizedAppenderBase - including the hand-off:
            //   many threads race into dispatch() while a single worker
            //   drains, and the bounded queue offer plus the worker loop
            //   must neither lose nor duplicate events.
            // How will the test case be deemed successful and why? Successful
            //   if all N x M events eventually reach the producer with no
            //   exception escaping append and nothing diverted to the
            //   fallback. The queue capacity is raised above the total so
            //   no legitimate overflow occurs; races that corrupt shared
            //   state would surface as lost events, duplicates, or
            //   exceptions.
            // Why is it important to test this test case? A regression
            //   introducing shared mutable per-event state into the hot
            //   path or the queue hand-off would pass every sequential
            //   test and fail only in production under load.

            // Given: a stateless encoder - the recording TestEncoder's
            //   plain ArrayList would race under concurrent appends and
            //   sporadically divert an event to the fallback (a harness
            //   race, not an appender defect)
            val threadCount = 8
            val eventsPerThread = 250
            val factory = SynchronizingProducerFactory()
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = MessageBytesEncoder(),
                    producerFactory = factory,
                    fallback = fallback,
                )
            appender.sendQueueCapacity = threadCount * eventsPerThread + 1
            appender.start()

            val startLatch = CountDownLatch(1)
            val doneLatch = CountDownLatch(threadCount)
            val failures = AtomicInteger(0)
            // The first throwable, so a red run names its cause instead of a bare count
            val firstFailure = AtomicReference<Throwable?>()
            val executor = Executors.newFixedThreadPool(threadCount)
            try {
                // When: all threads hammer the asynchronous dispatch
                repeat(threadCount) { t ->
                    executor.submit {
                        try {
                            startLatch.await()
                            repeat(eventsPerThread) { i ->
                                appender.doAppend(
                                    newTestLoggingEvent(
                                        message = "t$t-e$i",
                                        threadName = "worker-$t",
                                    ),
                                )
                            }
                        } catch (t: Throwable) {
                            failures.incrementAndGet()
                            firstFailure.compareAndSet(null, t)
                        } finally {
                            doneLatch.countDown()
                        }
                    }
                }
                startLatch.countDown()
                assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue()

                // Then: the worker delivers everything, nothing diverted
                assertThat(firstFailure.get()).isNull()
                assertThat(failures.get()).isZero()
                pollUntil(timeoutMs = 10_000) { factory.historySize() == threadCount * eventsPerThread }
                assertThat(fallback.events).isEmpty()
                // And: every event exactly once - a count alone lets one loss
                //   and one duplicate cancel out
                //   (docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md,
                //   finding 10)
                val expected = (0 until threadCount).flatMap { t -> (0 until eventsPerThread).map { i -> "t$t-e$i" } }
                assertThat(factory.sentMessages()).containsExactlyInAnyOrderElementsOf(expected)
            } finally {
                executor.shutdownNow()
                appender.stop()
            }
        }
    }

    @Nested
    inner class `Asynchronous fallback dispatch` {
        @Test
        fun `should deliver fallback events through the real worker thread`() {
            // What is to be tested? The full asynchronous appender ->
            //   dispatcher -> worker -> fallback chain while the
            //   appender keeps running (no stop-drain shortcut).
            // How will the test case be deemed successful and why? Successful
            //   if events diverted by a failing encoder arrive at the
            //   fallback appender asynchronously (bounded polling) and a
            //   subsequent stop() completes cleanly, draining the queue.
            // Why is it important to test this test case? A regression in
            //   the dispatcher wiring (worker not started, stop-ordering
            //   closing the dispatcher before the registry) must surface
            //   in live delivery, not only after a drain.

            // Given
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = fallback,
                )
            appender.start()

            // When: events fail in the hot path and divert to the fallback
            repeat(5) { appender.doAppend(newTestLoggingEvent(message = "async-$it")) }

            // Then: the worker thread delivers them asynchronously
            pollUntil { fallback.events.size == 5 }

            // And: stop() drains and shuts down cleanly, with no drop warning
            appender.stop()
            assertThat(appender.statusMessages())
                .noneMatch { it.contains("dropped") }
        }

        @Test
        fun `should freeze the event state before it crosses to the fallback worker`() {
            // What is to be tested? The deferred-processing contract at
            //   the asynchronous hand-off: append() must materialize the
            //   event's lazy state (here: the formatted message computed
            //   from a mutable argument) on the caller's thread, so the
            //   fallback worker can never observe a value the caller
            //   mutated after append() returned.
            // How will the test case be deemed successful and why? Successful
            //   if the event delivered by the real fallback worker carries
            //   the argument's value from append time, although the
            //   argument was mutated right after doAppend returned.
            //   Without the freeze this assertion races - the worker
            //   formats the message later and can see the mutation.
            // Why is it important to test this test case? The fallback is
            //   the last-resort audit record, delivered exactly when Kafka
            //   is unavailable; late-mutated content there misleads
            //   incident diagnosis at the worst possible moment.

            // Given: an encoder that fails without touching the lazy
            // state itself
            val fallback = RecordingAppender()
            val appender =
                newAppender(
                    encoder = ThrowingEncoder(),
                    fallback = fallback,
                )
            appender.start()
            val mutable = StringBuilder("original")
            val event =
                LoggingEvent(
                    "fqcn.dummy",
                    LoggerContext().getLogger("deferred.test"),
                    Level.INFO,
                    "value: {}",
                    null,
                    arrayOf<Any>(mutable),
                ).apply { mdcPropertyMap = emptyMap() }

            // When: the argument mutates right after append returns
            appender.doAppend(event)
            mutable.setLength(0)
            mutable.append("mutated")

            // Then: the worker-delivered event carries the frozen value
            pollUntil { fallback.events.size == 1 }
            assertThat(fallback.events[0].formattedMessage).isEqualTo("value: original")
            appender.stop()
        }
    }
}
