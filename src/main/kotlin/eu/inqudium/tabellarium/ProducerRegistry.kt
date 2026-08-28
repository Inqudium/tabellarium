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
 * 3. **Shutdown** via [close]: all producers are closed in parallel
 *    within the configured [closeTimeout] budget. Per-producer close
 *    failures do not prevent the others from being shut down; they are
 *    aggregated and rethrown after every close was attempted.
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
    /**
     * The effective `client.id` values of all created producers, for the
     * appender's self-logging guard: the Kafka client names its internal
     * threads after the client.id, and log events from those threads must
     * not be routed back into the producer. Blank ids are excluded (a
     * blank id would match every thread name).
     */
    val clientIds: Set<String>,
    /**
     * The merged producer properties each producer was created with,
     * per class - the [ProducerPropertiesBuilder] output (base +
     * default + mandatory overrides; the serializers forced by
     * [ProducerFactory.default] apply on top and are not part of this
     * map). Kept RAW, including operator-supplied credentials:
     * consumers must never surface these values wholesale. The
     * appender's debug diagnostics, for example, only print the diff
     * against the operator's base properties (the generated settings),
     * which keeps credentials out by construction.
     */
    val effectiveProperties: Map<TopicClass, Map<String, String>>,
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
     * Closes all producers **in parallel**, bounded by the configured
     * [closeTimeout] as an overall budget rather than a per-producer one.
     *
     * Sequential closing would stack the timeouts (`N * closeTimeout` -
     * up to 40 seconds with four active classes), easily exceeding a
     * Kubernetes `terminationGracePeriodSeconds: 30` and getting the
     * later producers plus the fallback drain killed mid-flight. Each
     * producer therefore gets its own closer thread with the full
     * [closeTimeout]; this method waits for all of them within the same
     * budget (plus a small join margin).
     *
     * If any producer fails to close cleanly, the others are still
     * closed - partial cleanup is strictly better than no cleanup. The
     * failures are collected and, after every close was attempted,
     * rethrown as one aggregated exception (individual causes attached
     * as suppressed exceptions) so the caller's warn path can surface
     * them instead of losing them silently.
     *
     * An interrupt while waiting stops the wait early, restores the
     * interrupt flag, and leaves the daemon closer threads to finish on
     * their own.
     */
    override fun close() {
        val failures = java.util.concurrent.ConcurrentLinkedQueue<Pair<TopicClass, Exception>>()
        val closers =
            producersByClass.map { (topicClass, producer) ->
                Thread({
                    try {
                        producer.close(closeTimeout)
                    } catch (e: Exception) {
                        failures += topicClass to e
                    }
                }, "tabellarium-producer-close-${topicClass.name.lowercase()}").apply {
                    isDaemon = true
                    start()
                }
            }
        var interrupted = false
        val deadlineNanos = System.nanoTime() + closeTimeout.toNanos() + JOIN_MARGIN.toNanos()
        for (closer in closers) {
            val remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000
            if (remainingMs <= 0) break
            try {
                closer.join(remainingMs)
            } catch (_: InterruptedException) {
                interrupted = true
                break
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt()
        }
        if (failures.isNotEmpty()) {
            val summary =
                failures.joinToString(separator = "; ") { (topicClass, cause) ->
                    "$topicClass: ${cause.message ?: cause.javaClass.simpleName}"
                }
            val aggregate =
                RuntimeException(
                    "${failures.size} of ${producersByClass.size} Kafka producer(s) " +
                        "failed to close cleanly ($summary)",
                )
            failures.forEach { (_, cause) -> aggregate.addSuppressed(cause) }
            throw aggregate
        }
    }

    companion object {
        /**
         * Default per-producer close timeout.
         *
         * Ten seconds is a deliberate compromise:
         *
         * - **Short enough** to fit comfortably in the standard Kubernetes
         *   default `terminationGracePeriodSeconds: 30`: producers close
         *   in parallel, so this value is the overall budget regardless
         *   of how many topic classes are active.
         * - **Long enough** to give the Kafka producer's internal retry
         *   loop time to flush partially-buffered records on flaky
         *   networks. A shorter value (e.g. 5 seconds) tends to drop
         *   records that would have been recoverable.
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
         * Extra wait beyond [close]'s overall [closeTimeout] budget when
         * joining the parallel closer threads, absorbing thread startup
         * and scheduling jitter so a producer that used its full timeout
         * is not misreported as hung.
         */
        private val JOIN_MARGIN: Duration = Duration.ofMillis(500)

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
         * by [ProducerPropertiesBuilder.buildFor].
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
            val clientIds = mutableSetOf<String>()
            val effectiveProperties = LinkedHashMap<TopicClass, Map<String, String>>()

            try {
                activeTopicClasses.forEach { topicClass ->
                    val resolved = propertiesBuilder.buildFor(topicClass)
                    allViolations += resolved.mandatoryOverrideViolations
                    resolved.properties[ProducerConfig.CLIENT_ID_CONFIG]
                        ?.takeIf { it.isNotBlank() }
                        ?.let { clientIds += it }
                    effectiveProperties[topicClass] = resolved.properties
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
                clientIds = clientIds.toSet(),
                effectiveProperties = java.util.Map.copyOf(effectiveProperties),
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
 * thread for every record. The override is silent -
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
                // Force the serializers - see KDoc above.
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
