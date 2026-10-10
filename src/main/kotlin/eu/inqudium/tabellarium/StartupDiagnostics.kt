package eu.inqudium.tabellarium

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import org.apache.kafka.clients.CommonClientConfigs

// The messages KafkaAppender.start reports to Logback's status manager
// once the transport is open: the mandatory-override warnings, the
// cleartext-transport warning for compliance-graded classes, the
// shared-client-id warning, and the `<debug>` diagnostics.
//
// Pure top-level functions from the built ProducerRegistry and the
// configuration to text, like the module's other stateless helpers. The
// appender only decides the status level (`addWarn`, `addInfo`); the
// wording and the conditions live here, testable without a started
// appender, and the composition root keeps to wiring and lifecycle.

/** Kafka's cleartext security protocol - also its default when unset. */
internal const val CLEARTEXT_SECURITY_PROTOCOL: String = "PLAINTEXT"

/** Warning for one [MandatoryOverrideViolation] the properties builder recorded. */
internal fun mandatoryOverrideWarning(violation: MandatoryOverrideViolation): String =
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
internal fun cleartextTransportWarning(registry: ProducerRegistry): String? {
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
 * Warning for client ids this appender shares with another live
 * instance (`DEFECT_ANALYSIS-2026-10-10T15-19-31`, finding 7): Kafka
 * keys its JMX MBeans by `client.id`, so the second producer's
 * `app-info` registration fails, either instance's close unregisters
 * the other's producer MBeans, and the broker attributes and throttles
 * both as one client. `null` when nothing is shared.
 */
internal fun sharedClientIdWarning(registry: ProducerRegistry): String? {
    val shared = SelfLoggingGuard.clientIdsHeldByOtherInstances(registry.clientIds)
    if (shared.isEmpty()) return null
    return "client.id ${shared.joinToString()} is also used by another running KafkaAppender " +
        "(same <component>, or the same explicit client.id): Kafka registers its JMX MBeans per " +
        "client.id, so the producers' JMX metrics collide and the first appender to stop removes " +
        "them for both, and the broker attributes and throttles both as one client. Give each " +
        "appender its own <component> or an explicit, distinct client.id in <kafkaProducerProperties>."
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
internal fun debugMessages(
    registry: ProducerRegistry,
    fallbackAppender: Appender<ILoggingEvent>?,
    kafkaProducerProperties: String,
): List<String> {
    val fallbackDescription =
        when {
            fallbackAppender == null -> "none - events will be silently dropped on send failure"
            fallbackAppender.isStarted -> "configured (${fallbackAppender.javaClass.simpleName})"
            else -> "configured (${fallbackAppender.javaClass.simpleName}, NOT STARTED)"
        }
    val baseProperties = parseKafkaProducerProperties(kafkaProducerProperties)
    return buildList {
        add(
            "Debug mode enabled. Note: <debug> affects only startup " +
                "diagnostics and has no per-event effect. Consider removing " +
                "<debug>true</debug> from your logback configuration.",
        )
        add("Active topic classes: ${registry.activeTopicClasses.joinToString()}")
        add("Fallback appender: $fallbackDescription")
        registry.activeTopicClasses.forEach { topicClass ->
            val generated =
                registry.effectiveProperties
                    .getValue(topicClass)
                    .filter { (key, value) -> baseProperties[key] != value }
                    .toSortedMap()
                    .entries
                    .joinToString(", ") { (key, value) -> "$key=$value" }
            add("Generated producer settings [${topicClass.tag}]: $generated")
        }
    }
}
