package eu.inqudium.tabellarium

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.spi.AppenderAttachable
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import java.util.Collections
import java.util.IdentityHashMap

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
 * a `KafkaAppender` wrapped in an `AsyncAppender` - generally not
 * recommended; see the README section "Should I wrap this in a
 * Logback AsyncAppender?" for the loss-semantics rationale). Each
 * [KafkaAppender] is bound exactly once, even if attached to multiple
 * loggers.
 *
 * ## Idempotency
 *
 * Multiple [ContextRefreshedEvent] firings (which occur in some test
 * harnesses or context-reload scenarios) result in only one bind per
 * appender: an appender that already has a metrics binding is
 * skipped. The decision is made on the appender's own bound state,
 * not on instance identity - so a bind that failed is retried on the
 * next call and a manual bind is never duplicated.
 *
 * ## Context close
 *
 * On [ContextClosedEvent] of its own application context the binding
 * unbinds every appender that is bound to its [MeterRegistry] - the
 * registry is typically a bean of that context and is closed with it.
 * The appenders keep running; a later application context in the same
 * JVM (a restart that does not re-initialize Logback, Spring's test
 * context cache) then finds them unbound and binds them to its own
 * registry on refresh. Close events of child contexts, which Spring
 * also publishes to the parent's listeners, are ignored. Two contexts
 * that are open *at the same time* still share one appender: the first
 * to refresh binds it, the second skips it until the first closes.
 *
 * ## Logback reconfiguration
 *
 * A Logback reconfiguration (`<configuration scan="true">`, a
 * programmatic `LoggerContext.reset()` plus re-configuration) replaces
 * every appender instance: the old [KafkaAppender] is stopped and
 * removes its meters, the new one is unbound. Spring publishes no
 * further [ContextRefreshedEvent] in a running application, and
 * Logback fires no listener callback *after* the new configuration is
 * in place (`LoggerContextListener.onStart` runs only for the initial
 * start, `onReset` before the new appenders exist), so this class
 * cannot rebind automatically. Compatibility: after a reconfiguration
 * the metrics stay dark until [bindAppenders] is called again - it is
 * public and idempotent for exactly this purpose (e.g. from an
 * application-side `LoggerContextListener` that defers to the next
 * scheduler tick, or from an operations endpoint).
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
) : ApplicationContextAware {
    private val log = LoggerFactory.getLogger(KafkaAppenderMetricsBinding::class.java)

    /** The context this binding is a bean of; null outside Spring. */
    @Volatile
    private var applicationContext: ApplicationContext? = null

    override fun setApplicationContext(applicationContext: ApplicationContext) {
        this.applicationContext = applicationContext
    }

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
            if (!appender.isStarted) {
                // bindMeterRegistry on a stopped appender is a no-op; a
                // later call, by which time the appender may have
                // started, retries instead of skipping it forever.
                log.debug(
                    "KafkaAppender '{}' is not started; deferring metrics binding to a later refresh.",
                    appender.name ?: "<unnamed>",
                )
                continue
            }
            if (appender.isMeterRegistryBound) {
                // Already bound (by a previous call or manually); the
                // appender's own state is the source of truth.
                continue
            }
            try {
                if (appender.bindMeterRegistry(meterRegistry, commonTags)) {
                    log.info(
                        "Bound KafkaAppender '{}' to MeterRegistry.",
                        appender.name ?: "<unnamed>",
                    )
                } else {
                    // Refused (duplicate meter identity) or stopped in the
                    // meantime: the appender's status warning is the
                    // authoritative diagnostic - no success record here.
                    log.debug(
                        "KafkaAppender '{}' was not bound to MeterRegistry; see its Logback status warning.",
                        appender.name ?: "<unnamed>",
                    )
                }
            } catch (e: Exception) {
                // Rationale: a transient failure (registry not ready,
                // meter clash) leaves the appender unbound, so the next
                // call retries instead of leaving it dark forever.
                log.warn(
                    "Failed to bind KafkaAppender '{}' to MeterRegistry: {}",
                    appender.name ?: "<unnamed>",
                    e.message,
                )
            }
        }
    }

    /**
     * Triggered when an application context closes. Unbinds every
     * [KafkaAppender] bound to [meterRegistry] when the closing context
     * is this binding's own (see "Context close" in the class KDoc).
     */
    @EventListener(ContextClosedEvent::class)
    open fun unbindAppenders(event: ContextClosedEvent) {
        val own = applicationContext
        if (own != null && event.applicationContext !== own) {
            // A child context closing - its event is also published to
            // the parent's listeners; the parent's registry stays live.
            return
        }
        val loggerContext = LoggerFactory.getILoggerFactory() as? LoggerContext ?: return
        for (appender in collectKafkaAppenders(loggerContext)) {
            try {
                if (appender.unbindMeterRegistry(meterRegistry)) {
                    log.debug("Unbound KafkaAppender '{}' from the closing context's MeterRegistry.", appender.name ?: "<unnamed>")
                }
            } catch (e: Exception) {
                log.warn(
                    "Failed to unbind KafkaAppender '{}' from MeterRegistry: {}",
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
            Collections.newSetFromMap(
                IdentityHashMap<Appender<ILoggingEvent>, Boolean>(),
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
