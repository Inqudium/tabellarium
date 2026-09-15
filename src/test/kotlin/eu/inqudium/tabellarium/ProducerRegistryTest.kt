package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class ProducerRegistryTest {
    private val baseProperties =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "broker:9092",
        )

    private fun newBuilder(base: Map<String, String> = baseProperties) = ProducerPropertiesBuilder(base)

    /**
     * Test producer that throws on [close], used to verify the registry's
     * per-producer try/catch in [ProducerRegistry.close].
     */
    private class ThrowingOnCloseProducer :
        MockProducer<ByteArray, ByteArray>(
            true,
            FixedZeroPartitioner(),
            ByteArraySerializer(),
            ByteArraySerializer(),
        ) {
        override fun close(timeout: Duration): Unit = throw RuntimeException("simulated close failure")
    }

    /** Test producer that records the timeout its [close] was called with. */
    private class TimeoutRecordingProducer :
        MockProducer<ByteArray, ByteArray>(
            true,
            FixedZeroPartitioner(),
            ByteArraySerializer(),
            ByteArraySerializer(),
        ) {
        val closedWith = AtomicReference<Duration>()

        override fun close(timeout: Duration) {
            closedWith.set(timeout)
            super.close(timeout)
        }
    }

    /**
     * Test producer whose [close] parks uninterruptibly until [unblock]
     * - a client that never finishes flushing, the case the registry's
     * overall close budget exists for.
     */
    private class ParkedCloseProducer :
        MockProducer<ByteArray, ByteArray>(
            true,
            FixedZeroPartitioner(),
            ByteArraySerializer(),
            ByteArraySerializer(),
        ) {
        private val release = CountDownLatch(1)
        val entered = CountDownLatch(1)

        override fun close(timeout: Duration) {
            entered.countDown()
            var wasInterrupted = false
            while (release.count > 0) {
                try {
                    release.await()
                } catch (_: InterruptedException) {
                    wasInterrupted = true
                }
            }
            if (wasInterrupted) {
                Thread.currentThread().interrupt()
            }
        }

        fun unblock() = release.countDown()
    }

    @Nested
    inner class `Construction` {
        @Test
        fun `should reject construction when the active topic classes set is empty`() {
            // What is to be tested? Whether the registry refuses to exist without at least
            //   one active topic class.
            // How will the test case be deemed successful and why? Successful if create()
            //   throws an IllegalArgumentException with the "At least one active topic
            //   class" message before any producer is built.
            // Why is it important to test this test case? A registry with no producers
            //   would make every producerFor lookup fail at the first log event; failing at
            //   configuration time turns a runtime surprise into a clear startup error.

            // When / Then
            assertThatThrownBy {
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = emptySet(),
                    producerFactory = RecordingProducerFactory(),
                )
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("At least one active topic class")
        }

        @Test
        fun `should create exactly one producer per active topic class`() {
            // What is to be tested? Whether the registry drives the factory exactly once
            //   per active class and reports the same set back as activeTopicClasses.
            // How will the test case be deemed successful and why? Successful if two active
            //   classes yield exactly two factory calls and activeTopicClasses contains
            //   precisely AUDIT and TECHNICAL.
            // Why is it important to test this test case? Each KafkaProducer owns a network
            //   thread and buffer memory; creating extras (or producers for inactive
            //   classes) would leak resources, creating too few would fail sends.

            // Given
            val factory = RecordingProducerFactory()

            // When
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = setOf(TopicClass.AUDIT, TopicClass.TECHNICAL),
                    producerFactory = factory,
                )

            // Then
            assertThat(factory.createdProducers).hasSize(2)
            assertThat(registry.activeTopicClasses)
                .containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.TECHNICAL)
        }

        @Test
        fun `should exclude a blank client id from clientIds and expose the effective properties per class`() {
            // What is to be tested? The two registry-level views the transport
            //   and the diagnostics read: clientIds - the input of the
            //   self-logging guard - must leave out a blank operator client.id
            //   (a blank id would match every thread name), and
            //   effectiveProperties must hold, per class, exactly the map the
            //   factory was given.
            // How will the test case be deemed successful and why? Successful
            //   if a blank `client.id` in the base properties yields an empty
            //   clientIds set although both producers were created, and each
            //   class's effectiveProperties equals the properties recorded by
            //   the factory for that class. Neither accessor was asserted in
            //   this file before.
            // Why is it important to test this test case? A blank id reaching
            //   the guard would make it drop every event from every thread;
            //   effectiveProperties feeds the <debug> diagnostics and the
            //   cleartext-transport warning, which read it as the truth
            //   about the producers (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 26).

            // Given: an operator client.id that is blank
            val factory = RecordingProducerFactory()

            // When
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(baseProperties + (ProducerConfig.CLIENT_ID_CONFIG to "   ")),
                    activeTopicClasses = setOf(TopicClass.AUDIT, TopicClass.TECHNICAL),
                    producerFactory = factory,
                )

            // Then: the blank id is not a guard input; the effective map is
            //   what each producer was built from
            assertThat(factory.createdProducers).hasSize(2)
            assertThat(registry.clientIds).isEmpty()
            assertThat(registry.effectiveProperties.keys).containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.TECHNICAL)
            assertThat(registry.effectiveProperties.getValue(TopicClass.AUDIT))
                .isEqualTo(factory.createdWithProperties[0])
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "   ")
            assertThat(registry.effectiveProperties.getValue(TopicClass.TECHNICAL))
                .isEqualTo(factory.createdWithProperties[1])
            registry.close()
        }

        @Test
        fun `should pass the merged topic-class properties to the factory`() {
            // What is to be tested? Whether the registry actually applies the
            //   property merge for each topic class - specifically that mandatory
            //   overrides reach the factory, not the unmerged base.
            // How will the test case be deemed successful and why? Successful if
            //   the factory receives acks=all for AUDIT, even though the base
            //   sets acks=0. This confirms that the producer is built from the
            //   merged properties, not from the raw user input.
            // Why is it important to test this test case? The entire point of
            //   the registry is to enforce class-specific configurations. A
            //   regression that bypassed the builder would silently break
            //   compliance.

            // Given: base sets a value that AUDIT will override
            val factory = RecordingProducerFactory()
            val builder =
                newBuilder(
                    mapOf(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "broker:9092",
                        ProducerConfig.ACKS_CONFIG to "0",
                    ),
                )

            // When
            ProducerRegistry.create(
                propertiesBuilder = builder,
                activeTopicClasses = setOf(TopicClass.AUDIT),
                producerFactory = factory,
            )

            // Then: the factory received the enforced value
            assertThat(factory.createdWithProperties).hasSize(1)
            assertThat(factory.createdWithProperties[0])
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
        }

        @Test
        fun `should aggregate mandatory override violations across all active topic classes`() {
            // What is to be tested? Whether the violations of every active class are
            //   collected into the registry's single mandatoryOverrideViolations list.
            // How will the test case be deemed successful and why? Successful if acks=0
            //   against AUDIT, FUNCTIONAL and TECHNICAL yields exactly two violations,
            //   attributed to AUDIT and FUNCTIONAL - TECHNICAL has no mandate to violate.
            // Why is it important to test this test case? The appender emits this list as
            //   startup warnings; if only the last class's violations survived, an operator
            //   would learn about one overruled setting and miss the other.

            // Given: a base that conflicts with mandates of two classes
            val builder =
                newBuilder(
                    mapOf(ProducerConfig.ACKS_CONFIG to "0"),
                )

            // When: include three classes; AUDIT contributes one conflict (acks),
            //   FUNCTIONAL contributes one conflict (acks), TECHNICAL has no
            //   mandates so contributes none.
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = builder,
                    activeTopicClasses =
                        setOf(
                            TopicClass.AUDIT,
                            TopicClass.FUNCTIONAL,
                            TopicClass.TECHNICAL,
                        ),
                    producerFactory = RecordingProducerFactory(),
                )

            // Then
            assertThat(registry.mandatoryOverrideViolations).hasSize(2)
            assertThat(registry.mandatoryOverrideViolations)
                .extracting<TopicClass> { it.topicClass }
                .containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.FUNCTIONAL)
        }
    }

    @Nested
    inner class `Producer lookup` {
        @Test
        fun `should return the producer instance that was created for the given topic class`() {
            // What is to be tested? Whether producerFor hands back the very object the
            //   factory created for that class, not a wrapper or a copy.
            // How will the test case be deemed successful and why? Successful if the lookup
            //   result is the same instance (isSameAs) as the factory's first product.
            // Why is it important to test this test case? The dispatcher sends through this
            //   instance and close() closes the stored one; if they diverged, records would
            //   go to a producer nobody ever closes.

            // Given
            val factory = RecordingProducerFactory()
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = setOf(TopicClass.AUDIT),
                    producerFactory = factory,
                )

            // When
            val producer = registry.producerFor(TopicClass.AUDIT)

            // Then
            assertThat(producer).isSameAs(factory.createdProducers[0])
        }

        @Test
        fun `should throw when looking up a producer for a topic class that is not active`() {
            // What is to be tested? Whether a lookup for an inactive class fails loudly
            //   instead of returning null or a producer of another class.
            // How will the test case be deemed successful and why? Successful if
            //   producerFor(PERFORMANCE) on an AUDIT-only registry throws an
            //   IllegalStateException naming the requested class.
            // Why is it important to test this test case? The appender only routes to
            //   active classes; if this contract broke, a routing bug would surface as an
            //   NPE deep in the send path rather than as a named error at the lookup.

            // Given
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = setOf(TopicClass.AUDIT),
                    producerFactory = RecordingProducerFactory(),
                )

            // When / Then
            assertThatThrownBy { registry.producerFor(TopicClass.PERFORMANCE) }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("PERFORMANCE")
        }
    }

    @Nested
    inner class `Construction failure handling` {
        @Test
        fun `should close already-created producers when a later factory call throws`() {
            // What is to be tested? Whether the registry rolls back partial
            //   initialization: when the factory throws while creating one
            //   producer, the producers created before that point are closed
            //   to avoid leaking Kafka network threads.
            // How will the test case be deemed successful and why? Successful
            //   if the partially-created MockProducers report as closed after
            //   the exception propagates, AND the factory's exception reaches
            //   the caller as the cause of a ProducerConstructionException
            //   naming the class that failed - the type the appender withholds
            //   by default. This confirms the rollback path and the wrapping.
            // Why is it important to test this test case? Without rollback, a
            //   failed registry init would leak Kafka network threads,
            //   accumulating with every retry attempt at application startup.
            //   In Spring Boot's bootstrap loop that could be many retries.

            // Given: a factory that creates two producers, then throws on the third
            val createdMocks = mutableListOf<MockProducer<ByteArray, ByteArray>>()
            val failingFactory =
                ProducerFactory { _ ->
                    if (createdMocks.size >= 2) {
                        throw RuntimeException("simulated factory failure")
                    }
                    MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
                        .also { createdMocks += it }
                }

            // When / Then: the factory failure propagates, wrapped and attributed
            assertThatThrownBy {
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses =
                        setOf(
                            TopicClass.AUDIT,
                            TopicClass.FUNCTIONAL,
                            TopicClass.TECHNICAL,
                        ),
                    producerFactory = failingFactory,
                )
            }.isInstanceOf(ProducerConstructionException::class.java)
                .hasMessageContaining("TECHNICAL")
                .hasMessageContaining("simulated factory failure")
                .cause()
                .isInstanceOf(RuntimeException::class.java)
                .hasMessage("simulated factory failure")

            // And: the two already-created producers were closed by rollback
            assertThat(createdMocks).hasSize(2)
            assertThat(createdMocks).allSatisfy { producer ->
                assertThat(producer.closed()).isTrue()
            }
        }
    }

    @Nested
    inner class `Closing` {
        @Test
        fun `should close all producers when the registry is closed`() {
            // What is to be tested? Whether close() reaches every producer the registry
            //   owns, not only the first or the last.
            // How will the test case be deemed successful and why? Successful if all four
            //   MockProducers created for the full class set report closed() afterwards.
            // Why is it important to test this test case? Producers are closed in parallel
            //   on their own threads; a producer skipped here keeps its Kafka network
            //   thread alive past appender stop and blocks a clean JVM shutdown.

            // Given
            val factory = RecordingProducerFactory()
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = TopicClass.entries.toSet(),
                    producerFactory = factory,
                )

            // When
            registry.close()

            // Then: all four producers were closed
            assertThat(factory.createdProducers).hasSize(4)
            assertThat(factory.createdProducers).allSatisfy { producer ->
                assertThat(producer.closed()).isTrue()
            }
        }

        @Test
        fun `should pass the configured close timeout to every producer close`() {
            // What is to be tested? Whether the registry's closeTimeout - the
            //   value the "10 s fits a 30 s termination grace" argument rests
            //   on - actually reaches producer.close(Duration) for each
            //   producer, rather than a bare close() or some other budget.
            // How will the test case be deemed successful and why? Successful
            //   if every producer double recorded exactly the configured
            //   1234 ms as its close timeout. MockProducer.close(Duration)
            //   ignores its argument, so without the recording double a
            //   close() (0 ms) or close(Duration.ZERO) mutation stayed green.
            // Why is it important to test this test case? The close timeout
            //   is what gives the Kafka client's retry loop time to flush
            //   buffered records on shutdown; a close that silently passed
            //   zero would abort every in-flight record on each pod stop
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 26).

            // Given: recording producers and an unmistakable timeout
            val created = mutableListOf<TimeoutRecordingProducer>()
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = setOf(TopicClass.AUDIT, TopicClass.TECHNICAL),
                    producerFactory = { _ -> TimeoutRecordingProducer().also { created += it } },
                    closeTimeout = Duration.ofMillis(1234),
                )

            // When
            registry.close()

            // Then: each producer was closed with exactly that timeout
            assertThat(created).hasSize(2)
            assertThat(created).allSatisfy { producer ->
                assertThat(producer.closedWith.get()).isEqualTo(Duration.ofMillis(1234))
            }
        }

        @Test
        fun `should return from close within the overall budget when a producer close never finishes`() {
            // What is to be tested? The registry's overall close budget: with
            //   one producer parked in close() for good, registry.close() must
            //   still return once closeTimeout plus the join margin have
            //   passed, instead of waiting for the parked closer thread.
            // How will the test case be deemed successful and why? Successful
            //   if a close() run on a helper thread completes (observed via a
            //   latch, generously bounded) while the parked producer is
            //   provably inside close and is never released before the
            //   assertion, and the healthy producer was still closed. The
            //   elapsed bound is deliberately wide - 200 ms budget, 500 ms
            //   margin, seconds of slack - so it pins "bounded" rather than
            //   an exact figure; the exact deadline arithmetic lives in
            //   ParallelCloseTest.
            // Why is it important to test this test case? A registry close
            //   that joined a hung producer without a budget would hold the
            //   appender's stop() and with it the pod's termination grace
            //   hostage to one flaky broker connection; nothing in this file
            //   proved the budget reaches the join
            //   (docs/assessment/CODE_ANALYSIS-2026-09-15T21-09-11.md,
            //   finding 26).

            // Given: one producer that never finishes closing, one healthy
            val parked = ParkedCloseProducer()
            var parkedHandedOut = false
            val healthy = mutableListOf<MockProducer<ByteArray, ByteArray>>()
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = setOf(TopicClass.AUDIT, TopicClass.TECHNICAL),
                    producerFactory = { _ ->
                        if (!parkedHandedOut) {
                            parkedHandedOut = true
                            parked
                        } else {
                            MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
                                .also { healthy += it }
                        }
                    },
                    closeTimeout = Duration.ofMillis(200),
                )
            val closeReturned = CountDownLatch(1)
            val closer = Thread({ registry.close().also { closeReturned.countDown() } }, "test-registry-closer")
            try {
                // When: close runs while the parked producer stays parked
                val startNanos = System.nanoTime()
                closer.start()
                assertThat(parked.entered.await(2, TimeUnit.SECONDS)).isTrue()

                // Then: close returned within the budget plus a wide margin
                assertThat(closeReturned.await(5, TimeUnit.SECONDS)).isTrue()
                val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000
                assertThat(elapsedMs).isLessThan(200 + 500 + 3000)
                assertThat(healthy).singleElement().satisfies({ producer ->
                    assertThat(producer.closed()).isTrue()
                })
            } finally {
                parked.unblock()
                closer.join(2000)
            }
        }

        @Test
        fun `should close the remaining producers and rethrow an aggregate when one of them throws on close`() {
            // What is to be tested? Whether a single producer's close-failure
            //   prevents the registry from closing the others - and whether
            //   the failure is surfaced instead of swallowed.
            // How will the test case be deemed successful and why? Successful
            //   if all healthy producers report closed even after one of them
            //   threw during close, AND close() rethrows one aggregated
            //   exception carrying the original cause as suppressed. The
            //   caller (the appender's stop()) turns that into a status
            //   warning; a silently-swallowed close failure would leave
            //   operators without any diagnostic for leaked producers.
            // Why is it important to test this test case? On shutdown in a
            //   Kubernetes pod, the registry must do best-effort cleanup. A
            //   single misbehaving producer must not cascade into a complete
            //   leak of the others - but it must not vanish without a trace
            //   either.

            // Given: a factory that creates one throwing-on-close producer first,
            //   then two healthy ones. Uses an explicit flag rather than checking
            //   the healthyProducers list, because the throwing producer is not
            //   added to that list.
            val healthyProducers = mutableListOf<MockProducer<ByteArray, ByteArray>>()
            var throwingProducerCreated = false
            val throwingFactory =
                ProducerFactory { _ ->
                    if (!throwingProducerCreated) {
                        throwingProducerCreated = true
                        ThrowingOnCloseProducer()
                    } else {
                        MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
                            .also { healthyProducers += it }
                    }
                }
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses =
                        setOf(
                            TopicClass.AUDIT,
                            TopicClass.FUNCTIONAL,
                            TopicClass.TECHNICAL,
                        ),
                    producerFactory = throwingFactory,
                )

            // When: close the whole registry - the failure is aggregated
            //   and rethrown after every producer's close was attempted
            assertThatThrownBy { registry.close() }
                .hasMessageContaining("1 of 3")
                .hasMessageContaining("simulated close failure")
                .satisfies({ aggregate ->
                    assertThat(aggregate.suppressed)
                        .anySatisfy { cause ->
                            assertThat(cause).hasMessage("simulated close failure")
                        }
                })

            // Then: the two healthy producers were still closed
            assertThat(healthyProducers).hasSize(2)
            assertThat(healthyProducers).allSatisfy { producer ->
                assertThat(producer.closed()).isTrue()
            }
        }
    }
}
