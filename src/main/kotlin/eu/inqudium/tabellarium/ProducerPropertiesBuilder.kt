package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.ProducerConfig

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
 *    from the `<kafkaProducerProperties>` XML element.
 * 2. **Default overrides** - applied via `putIfAbsent`, so the caller's
 *    value wins where both are present. When [defaultClientIdPrefix] is
 *    set, a per-class `client.id` default of
 *    `<defaultClientIdPrefix>-<topicclass>` belongs to this layer: it
 *    gives each producer a distinct, attributable client id on the
 *    broker (metrics, quotas, logs) instead of Kafka's generic
 *    auto-generated `producer-N`, while an explicit operator-supplied
 *    `client.id` still wins.
 * 3. **Mandatory overrides** - applied unconditionally. When the caller's
 *    value differs from the enforced value, the conflict is recorded as
 *    a [MandatoryOverrideViolation] in the result; the enforced value
 *    still wins in the produced properties.
 * 4. **The `max.block.ms` cap** - a ceiling, not a fixed value: caller
 *    values at or below [TopicClass.maxBlockMsCap] are kept, higher or
 *    unparseable values are clamped to the ceiling and recorded as a
 *    [MandatoryOverrideViolation]. See [capMaxBlockMs] for why the
 *    bound is non-negotiable.
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
 * @param defaultClientIdPrefix Prefix for the per-class `client.id`
 *                              default (`<prefix>-<topicclass>`,
 *                              lowercase class name). Null disables the
 *                              default entirely: with no operator-set
 *                              `client.id`, Kafka then auto-generates
 *                              `producer-N` ids.
 */
internal class ProducerPropertiesBuilder(
    baseProperties: Map<String, String>,
    private val defaultClientIdPrefix: String? = null,
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

        // Per-class client.id default - same putIfAbsent semantics, so an
        // operator-supplied client.id (shared across classes, their choice)
        // is never overruled.
        defaultClientIdPrefix?.let { prefix ->
            merged.putIfAbsent(
                ProducerConfig.CLIENT_ID_CONFIG,
                "$prefix-${topicClass.name.lowercase()}",
            )
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

        capMaxBlockMs(merged, topicClass, violations)

        validateIdempotenceCompatibility(merged, topicClass)

        return TopicClassProperties(
            topicClass = topicClass,
            properties = java.util.Map.copyOf(merged),
            mandatoryOverrideViolations = violations.toList(),
        )
    }

    /**
     * Enforces the class-specific `max.block.ms` **cap** ([TopicClass.maxBlockMsCap]).
     *
     * `producer.send` may block up to `max.block.ms` waiting for topic
     * metadata or free buffer space. The send runs on the class's
     * dedicated `SendDispatcher` worker (never on the logging caller),
     * so the cap bounds the worker's worst-case stall per event - the
     * bound that keeps queue drain during an outage and the shutdown
     * drain budget predictable. Those bounds only hold if this value
     * cannot be raised through `<kafkaProducerProperties>`. Unlike a
     * mandatory override, the cap keeps operator values that *tighten*
     * the bound: a lower value wins, a higher (or unparseable) value is
     * clamped to the ceiling and recorded as a
     * [MandatoryOverrideViolation] so the overruled intent is visible
     * at startup.
     */
    private fun capMaxBlockMs(
        merged: LinkedHashMap<String, String>,
        topicClass: TopicClass,
        violations: MutableList<MandatoryOverrideViolation>,
    ) {
        val cap = topicClass.maxBlockMsCap
        // Non-null: every class carries a max.block.ms default override,
        // so the defaults layer has filled the key if the caller did not.
        val currentValue = merged.getValue(ProducerConfig.MAX_BLOCK_MS_CONFIG)
        val currentMs = currentValue.toLongOrNull()
        if (currentMs != null && currentMs in 0..cap) return
        if (currentValue != cap.toString()) {
            violations +=
                MandatoryOverrideViolation(
                    topicClass = topicClass,
                    propertyKey = ProducerConfig.MAX_BLOCK_MS_CONFIG,
                    userValue = currentValue,
                    enforcedValue = cap.toString(),
                )
        }
        merged[ProducerConfig.MAX_BLOCK_MS_CONFIG] = cap.toString()
    }

    /**
     * Rejects property combinations that the Kafka producer constructor
     * would refuse anyway - but with a clear, named message instead of
     * the generic construction failure the appender would otherwise
     * surface. Relevant when a class mandates `enable.idempotence=true`
     * (AUDIT) and the caller's tuning contradicts the idempotence
     * preconditions.
     */
    private fun validateIdempotenceCompatibility(
        merged: Map<String, String>,
        topicClass: TopicClass,
    ) {
        if (merged[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG]?.toBoolean() != true) return
        merged[ProducerConfig.RETRIES_CONFIG]?.toIntOrNull()?.let { retries ->
            require(retries > 0) {
                "retries=$retries is incompatible with enable.idempotence=true " +
                    "(required for $topicClass): the idempotent producer needs retries > 0"
            }
        }
        merged[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION]?.toIntOrNull()?.let { inFlight ->
            require(inFlight <= 5) {
                "max.in.flight.requests.per.connection=$inFlight is incompatible with " +
                    "enable.idempotence=true (required for $topicClass): the idempotent " +
                    "producer needs a value of at most 5"
            }
        }
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
internal data class TopicClassProperties(
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
internal data class MandatoryOverrideViolation(
    val topicClass: TopicClass,
    val propertyKey: String,
    val userValue: String,
    val enforcedValue: String,
)
