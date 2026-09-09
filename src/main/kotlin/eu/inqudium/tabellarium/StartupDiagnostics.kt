package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import org.apache.kafka.clients.CommonClientConfigs

/**
 * The messages [KafkaAppender.start] reports to Logback's status manager
 * once the pipeline is built: the mandatory-override warnings, the
 * cleartext-transport warning for compliance-graded classes, and the
 * `<debug>` diagnostics.
 *
 * Pure functions from the built [ProducerRegistry] and the configuration
 * to text. The appender only decides the status level (`addWarn`,
 * `addInfo`); the wording and the conditions live here, testable without
 * a started appender, and the composition root keeps to wiring and
 * lifecycle.
 */
internal object StartupDiagnostics {
    /** Kafka's cleartext security protocol - also its default when unset. */
    const val CLEARTEXT_SECURITY_PROTOCOL: String = "PLAINTEXT"

    /** Warning for one [MandatoryOverrideViolation] the properties builder recorded. */
    fun mandatoryOverrideWarning(violation: MandatoryOverrideViolation): String =
        "Mandatory override applied for ${violation.topicClass}: " +
            "${violation.propertyKey} forced from '${violation.userValue}' to " +
            "'${violation.enforcedValue}'. This is a non-negotiable topic-class " +
            "requirement; see TopicClass.${violation.topicClass} for rationale."

    /**
     * Warning when at least one compliance-graded class (one with
     * mandatory overrides) runs over cleartext, i.e. its effective
     * `security.protocol` is unset or `PLAINTEXT`. Null when every
     * graded class is configured for an encrypted transport or no graded
     * class is active.
     */
    fun cleartextTransportWarning(registry: ProducerRegistry): String? {
        val gradedClasses =
            registry.activeTopicClasses
                .filter { it.mandatoryOverrides.isNotEmpty() }
                .filter { topicClass ->
                    val protocol =
                        registry.effectiveProperties
                            .getValue(topicClass)[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG]
                            ?.trim()
                    protocol == null || protocol.equals(CLEARTEXT_SECURITY_PROTOCOL, ignoreCase = true)
                }
        if (gradedClasses.isEmpty()) return null
        return "Compliance-graded topic class(es) ${gradedClasses.joinToString()} are configured " +
            "for cleartext transport (${CommonClientConfigs.SECURITY_PROTOCOL_CONFIG} is unset " +
            "or $CLEARTEXT_SECURITY_PROTOCOL). Their records are enforced to be durable " +
            "(acks/idempotence) but travel unencrypted and unauthenticated - anyone on the " +
            "network path can read or tamper with them. Configure SSL or SASL_SSL in " +
            "<kafkaProducerProperties> unless the transport is secured below the application."
    }

    /**
     * The `<debug>true</debug>` start-up diagnostics, in emission order:
     * the reminder that the flag has no per-event effect, the active
     * classes, the fallback configuration, and per active class the
     * producer settings the appender generated - i.e. every effective
     * property whose value differs from what the operator supplied in
     * [kafkaProducerProperties]. Safety: only generated values are
     * listed, never operator-supplied ones, so credentials from the
     * configuration cannot leak into the status log.
     */
    fun debugMessages(
        registry: ProducerRegistry,
        fallbackAppender: Appender<ILoggingEvent>?,
        kafkaProducerProperties: String,
    ): List<String> {
        val messages = mutableListOf<String>()
        messages +=
            "Debug mode enabled. Note: <debug> affects only startup " +
            "diagnostics and has no per-event effect. Consider removing " +
            "<debug>true</debug> from your logback configuration."
        messages += "Active topic classes: ${registry.activeTopicClasses.joinToString()}"
        messages +=
            "Fallback appender: " +
            (
                fallbackAppender?.let { "configured (${it.javaClass.simpleName})" }
                    ?: "none - events will be silently dropped on send failure"
            )
        val baseProperties = parseKafkaProducerProperties(kafkaProducerProperties)
        registry.activeTopicClasses.forEach { topicClass ->
            val generated =
                registry.effectiveProperties
                    .getValue(topicClass)
                    .filter { (key, value) -> baseProperties[key] != value }
                    .toSortedMap()
                    .entries
                    .joinToString(", ") { (key, value) -> "$key=$value" }
            messages += "Generated producer settings [${topicClass.tag}]: $generated"
        }
        return messages
    }
}
