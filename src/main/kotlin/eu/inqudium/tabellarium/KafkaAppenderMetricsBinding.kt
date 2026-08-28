package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.spi.AppenderAttachable
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import org.slf4j.LoggerFactory
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import java.util.concurrent.ConcurrentHashMap

/**
 * Spring `@Configuration` class that binds every [KafkaAppender] in
 * the Logback `LoggerContext` to the application's [MeterRegistry]
 * once the Spring context is fully refreshed.
 *
 * ## Why this is not auto-configuration
 *
 * This class is deliberately not annotated with `@AutoConfiguration`
 * and not registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
 * Operators import it explicitly:
 *
 * ```kotlin
 * @Configuration
 * @Import(KafkaAppenderMetricsBinding::class)
 * class LoggingConfig
 * ```
 *
 * or expose it as a bean directly:
 *
 * ```kotlin
 * @Configuration
 * class LoggingConfig {
 *     @Bean
 *     fun kafkaAppenderMetricsBinding(registry: MeterRegistry) =
 *         KafkaAppenderMetricsBinding(registry, Tags.of("application", "payment-service"))
 * }
 * ```
 *
 * Explicit import keeps the dependency tree honest: the appender
 * library itself does not transitively pull Spring into projects
 * that do not want it (Spring is `<optional>true</optional>` in the
 * appender's pom). Operators who want Spring-side metrics binding
 * opt in with one line.
 *
 * ## Lifecycle
 *
 * The binding runs on [ContextRefreshedEvent], which fires after the
 * Spring context is fully wired and ready for use. This is the
 * earliest reliable point at which the [MeterRegistry] bean exists
 * and the Logback configuration is complete.
 *
 * Pre-Spring log events (Logback initialization, Spring bootstrap)
 * are not counted. Capturing them would require a static
 * [MeterRegistry] reference and would clash with applications that
 * have multiple Spring contexts in the same JVM.
 *
 * ## Discovery
 *
 * Walks every logger in the [LoggerContext], collects all attached
 * appenders, and recurses into nested appenders (the common case is
 * a `KafkaAppender` wrapped in an `AsyncAppender` - see
 * [the AsyncAppender discussion in the README](#) for why this is
 * generally not recommended). Each [KafkaAppender] is bound exactly
 * once, even if attached to multiple loggers.
 *
 * ## Idempotency
 *
 * Multiple [ContextRefreshedEvent] firings (which occur in some test
 * harnesses or context-reload scenarios) result in only one bind per
 * appender. The set of already-bound appenders is tracked by
 * reference identity.
 *
 * @param meterRegistry The application's Micrometer registry. Required.
 * @param commonTags Tags attached to every metric this binding
 *                   publishes. Pass [Tags.empty] if the registry's
 *                   own common tags already cover the dimensions
 *                   you need (typical Spring Boot Actuator setup).
 */
open class KafkaAppenderMetricsBinding(
    private val meterRegistry: MeterRegistry,
    private val commonTags: Iterable<Tag> = Tags.empty(),
) {
    private val log = LoggerFactory.getLogger(KafkaAppenderMetricsBinding::class.java)

    /**
     * Set of appenders already bound. Reference identity, not equality
     * - two different appender instances with the same name should be
     * bound separately. Concurrent-safe so the listener can run on
     * any thread Spring chooses for event dispatch.
     */
    private val bound: MutableSet<KafkaAppender> = ConcurrentHashMap.newKeySet()

    /**
     * Triggered when the Spring context finishes wiring. Walks the
     * Logback `LoggerContext`, discovers all [KafkaAppender] instances,
     * and binds each to the configured [meterRegistry].
     *
     * Open so application code can extend the class and override the
     * trigger if needed (e.g. listen to a different event type).
     */
    @EventListener(ContextRefreshedEvent::class)
    open fun bindAppenders() {
        val loggerContext = LoggerFactory.getILoggerFactory() as? LoggerContext
        if (loggerContext == null) {
            log.warn(
                "ILoggerFactory is not a Logback LoggerContext (got {}); " +
                    "cannot auto-bind KafkaAppender metrics. " +
                    "Bind manually via KafkaAppender.bindMeterRegistry.",
                LoggerFactory.getILoggerFactory().javaClass.name,
            )
            return
        }

        val appenders = collectKafkaAppenders(loggerContext)
        if (appenders.isEmpty()) {
            log.debug("No KafkaAppender found in LoggerContext; nothing to bind.")
            return
        }

        for (appender in appenders) {
            if (!bound.add(appender)) {
                // Already bound on a previous refresh.
                continue
            }
            try {
                appender.bindMeterRegistry(meterRegistry, commonTags)
                log.info(
                    "Bound KafkaAppender '{}' to MeterRegistry.",
                    appender.name ?: "<unnamed>",
                )
            } catch (e: Exception) {
                log.warn(
                    "Failed to bind KafkaAppender '{}' to MeterRegistry: {}",
                    appender.name ?: "<unnamed>",
                    e.message,
                )
            }
        }
    }

    /**
     * Walks every logger in the context and returns all attached
     * appenders that are of type [KafkaAppender], deduplicated by
     * reference identity.
     */
    private fun collectKafkaAppenders(loggerContext: LoggerContext): List<KafkaAppender> {
        val result = LinkedHashSet<KafkaAppender>()
        val visited =
            java.util.Collections.newSetFromMap(
                java.util.IdentityHashMap<Appender<ILoggingEvent>, Boolean>(),
            )
        for (logger in loggerContext.loggerList) {
            val iterator = logger.iteratorForAppenders()
            while (iterator.hasNext()) {
                visit(iterator.next(), result, visited)
            }
        }
        return result.toList()
    }

    /**
     * Recursively descends into appenders attached to other appenders.
     * Handles the common case of a `KafkaAppender` wrapped in an
     * `AsyncAppender`. The [visited] identity-set makes the walk safe
     * on cyclic appender attachments (a pathological but constructible
     * configuration) - without it, a cycle would overflow the stack
     * inside the ContextRefreshedEvent listener and abort application
     * startup.
     */
    private fun visit(
        appender: Appender<ILoggingEvent>,
        sink: MutableSet<KafkaAppender>,
        visited: MutableSet<Appender<ILoggingEvent>>,
    ) {
        if (!visited.add(appender)) {
            return
        }
        if (appender is KafkaAppender) {
            sink.add(appender)
            return
        }
        if (appender is AppenderAttachable<*>) {
            @Suppress("UNCHECKED_CAST")
            val attachable = appender as AppenderAttachable<ILoggingEvent>
            val iterator = attachable.iteratorForAppenders()
            while (iterator.hasNext()) {
                visit(iterator.next(), sink, visited)
            }
        }
    }
}
