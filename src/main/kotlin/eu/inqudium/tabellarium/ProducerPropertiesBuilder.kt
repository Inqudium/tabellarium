package eu.inqudium.tabellarium

/**
 * Composes the Kafka producer configuration for a specific [TopicClass]
 * from caller-supplied base properties and the class-specific overrides
 * declared on [TopicClass].
 *
 * The builder is a **pure function**: it never mutates its [baseProperties]
 * input, holds no per-call state, and produces a deterministic result for
 * any given (baseProperties, topicClass) pair.
 *
 * ## Composition order
 *
 * For each [buildFor] call, three layers are merged in this order:
 *
 * 1. **Base properties** - the caller's configuration, typically parsed
 *    from `kafkaProducerProperties` in the legacy XML configuration.
 * 2. **Default overrides** - applied via `putIfAbsent`, so the caller's
 *    value wins where both are present.
 * 3. **Mandatory overrides** - applied unconditionally. When the caller's
 *    value differs from the enforced value, the conflict is recorded as
 *    a [MandatoryOverrideViolation] in the result; the enforced value
 *    still wins in the produced properties.
 *
 * The recorded violations are not warnings logged here - the builder has
 * no logging side effects. Callers (typically the appender's `start()`
 * method) are expected to forward them to Logback's status manager so
 * operators see them in the startup log.
 *
 * ## Why mandatory overrides
 *
 * Mandatory overrides exist because some classes of log carry
 * non-negotiable compliance requirements that the user must not be able
 * to weaken through configuration. The canonical example is `acks=all`
 * for audit topics in a regulated banking environment: a misconfigured
 * `acks=1` would silently allow audit-record loss on a Kafka leader
 * failover, which would be a compliance violation. The builder enforces
 * the safe value and surfaces the conflict for operator awareness.
 *
 * @param baseProperties User-supplied Kafka producer properties. The
 *                       builder copies these on construction; subsequent
 *                       mutations of the original map have no effect.
 */
class ProducerPropertiesBuilder(
    baseProperties: Map<String, String>,
) {
    private val baseProperties: Map<String, String> = java.util.Map.copyOf(baseProperties)

    /**
     * Builds the merged producer properties for the given [topicClass].
     *
     * Returns a [TopicClassProperties] that carries:
     * - the merged, immutable properties map, and
     * - the list of mandatory-override conflicts that were resolved in
     *   favor of the enforced value.
     */
    fun buildFor(topicClass: TopicClass): TopicClassProperties {
        val violations = mutableListOf<MandatoryOverrideViolation>()
        val merged = LinkedHashMap(baseProperties)

        // Default overrides: only when the caller did not set the property
        topicClass.defaultOverrides.forEach { (key, value) ->
            merged.putIfAbsent(key, value)
        }

        // Mandatory overrides: always applied; record conflicts
        topicClass.mandatoryOverrides.forEach { (key, enforcedValue) ->
            val currentValue = merged[key]
            if (currentValue != null && currentValue != enforcedValue) {
                violations +=
                    MandatoryOverrideViolation(
                        topicClass = topicClass,
                        propertyKey = key,
                        userValue = currentValue,
                        enforcedValue = enforcedValue,
                    )
            }
            merged[key] = enforcedValue
        }

        return TopicClassProperties(
            topicClass = topicClass,
            properties = java.util.Map.copyOf(merged),
            mandatoryOverrideViolations = violations.toList(),
        )
    }
}

/**
 * Result of [ProducerPropertiesBuilder.buildFor].
 *
 * @param topicClass The class for which the properties were built.
 * @param properties The merged, immutable properties map ready to be
 *                   passed to a Kafka `KafkaProducer` constructor.
 * @param mandatoryOverrideViolations Conflicts between caller-supplied
 *                                    values and mandatory overrides for
 *                                    this class. Empty when no conflicts
 *                                    occurred. The merged [properties]
 *                                    always carry the enforced values
 *                                    regardless of any conflict.
 */
data class TopicClassProperties(
    val topicClass: TopicClass,
    val properties: Map<String, String>,
    val mandatoryOverrideViolations: List<MandatoryOverrideViolation>,
)

/**
 * Records a conflict between a caller-supplied property value and the
 * enforced value of a mandatory override.
 *
 * The enforced value always wins in the produced properties map; this
 * record exists so that the appender can surface the conflict to the
 * Logback status manager at startup, alerting operators that their
 * configuration intent was overruled for compliance reasons.
 *
 * @param topicClass The class whose mandatory override produced the conflict.
 * @param propertyKey The producer-property key that was overridden.
 * @param userValue The caller-supplied value that was discarded.
 * @param enforcedValue The class-mandated value that was used instead.
 */
data class MandatoryOverrideViolation(
    val topicClass: TopicClass,
    val propertyKey: String,
    val userValue: String,
    val enforcedValue: String,
)
