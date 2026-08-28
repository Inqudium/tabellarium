package eu.inqudium.tabellarium

import org.apache.kafka.clients.producer.ProducerConfig

/**
 * Classifies a log topic by its compliance and performance requirements,
 * which in turn determine the Kafka producer configuration used for sending
 * records to that topic.
 *
 * Each topic class carries two sets of producer-property overrides:
 *
 * - **[mandatoryOverrides]** are applied regardless of what the caller
 *   configured. They encode non-negotiable requirements (such as
 *   `acks=all` for audit topics in a regulated banking environment) and
 *   override any conflicting user-supplied value, recording the conflict
 *   as a [MandatoryOverrideViolation] so the appender can surface it to
 *   Logback's status manager at startup.
 *
 * - **[defaultOverrides]** are applied only when the caller did not set
 *   the property themselves. They encode reasonable defaults that the
 *   caller is free to override.
 *
 * The four classes correspond to the topic groups `audit`, `functional`,
 * `technical`, and `performance` in the `TopicMapping` configuration
 * structure. Their Kafka-side requirements differ substantially:
 *
 * | Class       | Durability | Reorder cost | Volume    |
 * | ----------- | ---------- | ------------ | --------- |
 * | AUDIT       | Maximum    | Critical     | Low       |
 * | FUNCTIONAL  | Maximum    | High         | Medium    |
 * | TECHNICAL   | Best-effort| Acceptable   | High      |
 * | PERFORMANCE | Best-effort| Tolerated    | Very high |
 *
 * For AUDIT and FUNCTIONAL the durability is enforced via mandatory
 * `acks=all`; for AUDIT, idempotence is additionally mandatory. The other
 * classes fall back to default values that the caller can tune.
 */
enum class TopicClass(
    internal val mandatoryOverrides: Map<String, String>,
    internal val defaultOverrides: Map<String, String>,
    /**
     * Lowercase, dot-free identifier suitable as a metric tag value
     * (e.g. Prometheus `topic_class="audit"`). Pre-computed so the
     * metrics hot path needs no per-event `name.lowercase()` call.
     */
    internal val tag: String,
    /**
     * Upper bound for `max.block.ms`, enforced as a **cap** by
     * [ProducerPropertiesBuilder]: an operator may configure a lower
     * value, but a higher (or unparseable) value is clamped to this
     * ceiling and recorded as a [MandatoryOverrideViolation].
     *
     * The cap exists because `producer.send` runs on the logging
     * caller's thread and may block up to `max.block.ms` waiting for
     * topic metadata or free buffer space. The appender's central
     * design promise - a bounded worst-case block per `send()` even
     * when the cluster is unreachable - only holds if this value
     * cannot be raised through configuration.
     */
    internal val maxBlockMsCap: Long,
) {
    /**
     * Audit and compliance logs. Non-negotiable durability and idempotence;
     * cannot be tuned for throughput at the expense of safety. Suited for
     * BaFin/MaRisk-relevant audit trails.
     */
    AUDIT(
        mandatoryOverrides =
            mapOf(
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
            ),
        defaultOverrides =
            mapOf(
                ProducerConfig.LINGER_MS_CONFIG to "50",
                ProducerConfig.MAX_BLOCK_MS_CONFIG to "500",
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
                ProducerConfig.RETRIES_CONFIG to "10",
            ),
        tag = "audit",
        maxBlockMsCap = 500,
    ),

    /**
     * Functional logs that are important for operational correctness but
     * not strictly compliance-bound. Durability is enforced via mandatory
     * `acks=all`; other properties are tunable.
     */
    FUNCTIONAL(
        mandatoryOverrides =
            mapOf(
                ProducerConfig.ACKS_CONFIG to "all",
            ),
        defaultOverrides =
            mapOf(
                ProducerConfig.LINGER_MS_CONFIG to "50",
                ProducerConfig.MAX_BLOCK_MS_CONFIG to "500",
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
            ),
        tag = "functional",
        maxBlockMsCap = 500,
    ),

    /**
     * Technical and debug logs. No durability mandate; the caller may tune
     * acks, batching and timeouts freely. Defaults aim for a balance of
     * throughput and reasonable latency.
     */
    TECHNICAL(
        mandatoryOverrides = emptyMap(),
        defaultOverrides =
            mapOf(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.LINGER_MS_CONFIG to "50",
                ProducerConfig.MAX_BLOCK_MS_CONFIG to "500",
                ProducerConfig.BATCH_SIZE_CONFIG to "32768",
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
            ),
        tag = "technical",
        maxBlockMsCap = 500,
    ),

    /**
     * High-volume performance and metric logs. Optimized for throughput;
     * the caller may tune everything, including acks. Default `acks=1` is
     * a balance, not a requirement.
     */
    PERFORMANCE(
        mandatoryOverrides = emptyMap(),
        defaultOverrides =
            mapOf(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.LINGER_MS_CONFIG to "100",
                ProducerConfig.MAX_BLOCK_MS_CONFIG to "200",
                ProducerConfig.BATCH_SIZE_CONFIG to "65536",
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
            ),
        tag = "performance",
        maxBlockMsCap = 200,
    ),
}
