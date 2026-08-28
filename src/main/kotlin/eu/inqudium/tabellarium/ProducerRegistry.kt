package eu.inqudium.tabellarium

import eu.inqudium.tabellarium.ProducerFactory.Companion.default
import eu.inqudium.tabellarium.ProducerRegistry.Companion.create
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.time.Duration

/**
 * Holds one Kafka [Producer] per active [TopicClass], each configured with
 * the class-specific [ProducerPropertiesBuilder] output.
 *
 * The registry is the first side-effecting component in this module: it
 * triggers actual Kafka client construction (and the associated network-
 * thread startup) via [ProducerFactory]. Its lifecycle is:
 *
 * 1. **Initialization** in [create]: one producer per requested topic
 *    class is built. If any factory call throws, already-created
 *    producers are closed before the exception is rethrown - the
 *    registry is never partially initialized.
 * 2. **Operation**: [producerFor] returns the producer for a topic class.
 *    Looking up an inactive class is a programming error and throws.
 * 3. **Shutdown** via [close]: all producers are closed with the
 *    configured [closeTimeout]. Per-producer close failures are swallowed
 *    so they do not prevent the others from being shut down.
 *
 * [mandatoryOverrideViolations] aggregates the
 * [MandatoryOverrideViolation]s reported by [ProducerPropertiesBuilder]
 * across all active classes. The caller (typically the appender's
 * `start()` method) forwards these to Logback's status manager so
 * operators see them in the startup log.
 */
class ProducerRegistry private constructor(
    private val producersByClass: Map<TopicClass, Producer<ByteArray, ByteArray>>,
    private val closeTimeout: Duration,
    val mandatoryOverrideViolations: List<MandatoryOverrideViolation>,
) : AutoCloseable {
    /**
     * Topic classes for which a producer was successfully created.
     */
    val activeTopicClasses: Set<TopicClass>
        get() = producersByClass.keys

    /**
     * Returns the producer for the given [topicClass].
     *
     * @throws IllegalStateException if [topicClass] is not active in this registry.
     */
    fun producerFor(topicClass: TopicClass): Producer<ByteArray, ByteArray> =
        producersByClass[topicClass]
            ?: error("Topic class $topicClass is not active in this registry")

    /**
     * Closes all producers using the configured [closeTimeout].
     *
     * If a single producer fails to close cleanly, the registry still
     * attempts to close the remaining producers. This is the right
     * behavior for shutdown paths in Kubernetes: partial cleanup is
     * strictly better than no cleanup. The worst-case total close time
     * is `N * closeTimeout` where N is the number of active classes;
     * callers with strict grace-period budgets should pass a tighter
     * timeout via [create].
     */
    override fun close() {
        producersByClass.values.forEach { producer ->
            try {
                producer.close(closeTimeout)
            } catch (_: Exception) {
                // Swallow: closing one producer must not block closing the others.
            }
        }
    }

    companion object {
        /**
         * Default per-producer close timeout.
         *
         * Ten seconds is a deliberate compromise:
         *
         * - **Short enough** to fit comfortably in the standard Kubernetes
         *   default `terminationGracePeriodSeconds: 30`, even when multiple
         *   producers close sequentially (worst case: N classes × this
         *   timeout - for a typical deployment with 1-2 active classes,
         *   well under the grace period).
         * - **Long enough** to give the Kafka producer's internal retry
         *   loop time to flush partially-buffered records on flaky
         *   networks. The legacy 5-second value tended to drop records
         *   that would have been recoverable.
         *
         * ## Interaction with `delivery.timeout.ms`
         *
         * Even with a generous close timeout, records can still be lost if
         * the Kafka producer's `delivery.timeout.ms` (default 120 seconds)
         * is longer than the close timeout - a record in mid-retry at
         * close time will be aborted. Operators who need stronger
         * guarantees for AUDIT topics should set
         * `delivery.timeout.ms` to a value ≤ this close timeout via the
         * `<kafkaProducerProperties>` element, e.g.:
         *
         * ```
         * delivery.timeout.ms=10000
         * ```
         *
         * This caps the producer's retry window so close-time aborts do
         * not abandon records that "would have made it" eventually.
         */
        val DEFAULT_CLOSE_TIMEOUT: Duration = Duration.ofSeconds(10)

        /**
         * Timeout used when rolling back partially-initialized state after
         * a factory failure. Short on purpose: rollback runs on the
         * startup-failure path, where draining is pointless and quick
         * cleanup matters more than completeness.
         */
        private val ROLLBACK_CLOSE_TIMEOUT: Duration = Duration.ofMillis(500)

        /**
         * Creates a registry that holds one producer per
         * [activeTopicClasses]. Properties for each producer are derived
         * by [propertiesBuilder.buildFor].
         *
         * If [producerFactory] throws while creating any producer, all
         * already-created producers are closed (with a short rollback
         * timeout) before the exception is rethrown. The registry is
         * never partially initialized.
         *
         * @throws IllegalArgumentException if [activeTopicClasses] is empty.
         */
        fun create(
            propertiesBuilder: ProducerPropertiesBuilder,
            activeTopicClasses: Set<TopicClass>,
            producerFactory: ProducerFactory = default(),
            closeTimeout: Duration = DEFAULT_CLOSE_TIMEOUT,
        ): ProducerRegistry {
            require(activeTopicClasses.isNotEmpty()) {
                "At least one active topic class is required"
            }

            val createdProducers = LinkedHashMap<TopicClass, Producer<ByteArray, ByteArray>>()
            val allViolations = mutableListOf<MandatoryOverrideViolation>()

            try {
                activeTopicClasses.forEach { topicClass ->
                    val resolved = propertiesBuilder.buildFor(topicClass)
                    allViolations += resolved.mandatoryOverrideViolations
                    createdProducers[topicClass] = producerFactory.create(resolved.properties)
                }
            } catch (e: Exception) {
                // Roll back: close producers that were already created.
                // Suppress rollback failures so the original cause is preserved.
                createdProducers.values.forEach { producer ->
                    try {
                        producer.close(ROLLBACK_CLOSE_TIMEOUT)
                    } catch (_: Exception) {
                        // Swallow during rollback.
                    }
                }
                throw e
            }

            return ProducerRegistry(
                producersByClass = java.util.Map.copyOf(createdProducers),
                closeTimeout = closeTimeout,
                mandatoryOverrideViolations = allViolations.toList(),
            )
        }
    }
}

/**
 * Constructs a Kafka [Producer] from a property map.
 *
 * The default implementation ([default]) instantiates a real
 * [org.apache.kafka.clients.producer.KafkaProducer] and **forcibly sets** `ByteArraySerializer` for both key
 * and value, regardless of any serializer property the caller supplied.
 * This is the wire format produced by the appender; using a different
 * serializer would cause a `ClassCastException` in the Kafka sender
 * thread for every record (audit finding F-034). The override is silent -
 * if the appender's mandatory-override mechanism in
 * [ProducerPropertiesBuilder] is later extended to cover serializers as
 * well, the resulting violation will surface there instead.
 *
 * Tests typically inject a custom factory that returns a
 * `MockProducer<ByteArray, ByteArray>` and records the properties it
 * receives.
 */
fun interface ProducerFactory {
    /**
     * Creates a producer from the given properties.
     *
     * @param properties Final, merged producer properties. The factory must
     *                   not mutate this map.
     */
    fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray>

    companion object {
        /**
         * Returns the default factory: instantiates a real [org.apache.kafka.clients.producer.KafkaProducer]
         * with [org.apache.kafka.common.serialization.ByteArraySerializer] for both key and value, regardless of
         * any serializer properties in the input map.
         */
        fun default(): ProducerFactory =
            ProducerFactory { properties ->
                // Force the serializers - see KDoc above and audit finding F-034.
                val configs: Map<String, Any> =
                    properties +
                        mapOf(
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
                        )
                KafkaProducer(configs)
            }
    }
}
