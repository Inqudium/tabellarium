package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class ProducerRegistryTest {
    private val baseProperties =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "broker:9092",
        )

    private fun newBuilder(base: Map<String, String> = baseProperties) = ProducerPropertiesBuilder(base)

    /**
     * Test factory that records every invocation. Returns auto-completing
     * [MockProducer]s so send() calls (if any) succeed without configuring
     * a Cluster.
     */
    private class RecordingFactory : ProducerFactory {
        val createdProducers = mutableListOf<MockProducer<ByteArray, ByteArray>>()
        val receivedProperties = mutableListOf<Map<String, String>>()

        override fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray> {
            receivedProperties += properties
            val mock = MockProducer(true, FixedZeroPartitioner(), ByteArraySerializer(), ByteArraySerializer())
            createdProducers += mock
            return mock
        }
    }

    /**
     * Test producer that throws on [close], used to verify the registry's
     * per-producer try/catch in [ProducerRegistry.close].
     */
    private class ThrowingOnCloseProducer : MockProducer<ByteArray, ByteArray>(
        true,
        FixedZeroPartitioner(),
        ByteArraySerializer(),
        ByteArraySerializer(),
    ) {
        override fun close(timeout: java.time.Duration) {
            throw RuntimeException("simulated close failure")
        }
    }

    @Nested
    inner class `Construction` {
        @Test
        fun `should reject construction when the active topic classes set is empty`() {
            // When / Then
            assertThatThrownBy {
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = emptySet(),
                    producerFactory = RecordingFactory(),
                )
            }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("At least one active topic class")
        }

        @Test
        fun `should create exactly one producer per active topic class`() {
            // Given
            val factory = RecordingFactory()

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
            val factory = RecordingFactory()
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
            assertThat(factory.receivedProperties).hasSize(1)
            assertThat(factory.receivedProperties[0])
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
        }

        @Test
        fun `should aggregate mandatory override violations across all active topic classes`() {
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
                    producerFactory = RecordingFactory(),
                )

            // Then
            assertThat(registry.mandatoryOverrideViolations).hasSize(2)
            assertThat(registry.mandatoryOverrideViolations).extracting<TopicClass> { it.topicClass }
                .containsExactlyInAnyOrder(TopicClass.AUDIT, TopicClass.FUNCTIONAL)
        }
    }

    @Nested
    inner class `Producer lookup` {
        @Test
        fun `should return the producer instance that was created for the given topic class`() {
            // Given
            val factory = RecordingFactory()
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
            // Given
            val registry =
                ProducerRegistry.create(
                    propertiesBuilder = newBuilder(),
                    activeTopicClasses = setOf(TopicClass.AUDIT),
                    producerFactory = RecordingFactory(),
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
            //   the exception propagates, AND the original exception reaches
            //   the caller unchanged. This confirms the rollback path.
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

            // When / Then: exception propagates unchanged
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
            }
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
            // Given
            val factory = RecordingFactory()
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
        fun `should continue closing the remaining producers when one of them throws on close`() {
            // What is to be tested? Whether a single producer's close-failure
            //   prevents the registry from closing the others.
            // How will the test case be deemed successful and why? Successful
            //   if all healthy producers report closed even after one of them
            //   threw during close. This confirms the per-producer try/catch.
            // Why is it important to test this test case? On shutdown in a
            //   Kubernetes pod, the registry must do best-effort cleanup. A
            //   single misbehaving producer must not cascade into a complete
            //   leak of the others.

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

            // When: close the whole registry
            registry.close()

            // Then: the two healthy producers were still closed
            assertThat(healthyProducers).hasSize(2)
            assertThat(healthyProducers).allSatisfy { producer ->
                assertThat(producer.closed()).isTrue()
            }
        }
    }
}
