@file:JvmName("KafkaAppenderMeterRegistry")

package eu.inqudium.tabellarium

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags

/**
 * Wires the appender to a Micrometer [MeterRegistry].
 *
 * After this call, the appender publishes counters, timers and
 * gauges for hot-path events; the [ResilientMessageSender] reports
 * dispatch outcomes; the [FallbackDispatcher] reports queue depth
 * and dropped events. See [MicrometerKafkaAppenderMetrics] for the
 * complete metric inventory.
 *
 * **Additional bindings:** the circuit-breaker state and
 * call-outcome metrics are bound by the appender's own binder
 * (the meters, types and tags of `resilience4j-micrometer`'s
 * `TaggedCircuitBreakerMetrics`, named `kafka.appender.circuitbreaker.*`
 * instead of `resilience4j.circuitbreaker.*`, with an additional
 * `appender` tag so multiple appender instances on one registry
 * never collide). If the Micrometer Kafka binder is on the
 * classpath, the underlying Kafka producers' internal metrics are
 * bound as well, published as `kafka.appender.producer.*` instead
 * of `kafka.producer.*` and carrying the same `appender` tag. Every
 * meter of a bind therefore lives under `kafka.appender.`. A binding
 * failing (missing classpath, registry error) is non-fatal and
 * reported via Logback's status manager.
 *
 * **When to call:** typically from a Spring `@PostConstruct` or
 * an `ApplicationReadyEvent` handler, after the application's
 * [MeterRegistry] bean is available.
 * Pre-Spring log events are not captured (they happen before
 * the registry exists), which is acceptable for almost all
 * monitoring needs.
 *
 * Calling this function on a stopped appender is a no-op. A bind to
 * a registry that already holds the meters of another appender
 * with the same name (the `appender` tag) and common tags is
 * refused with a status warning: the two would share meter objects
 * (see [MetricsBindings]); give each appender a distinct name. A
 * refused or failing bind leaves an existing binding to another
 * registry in place; a binding is replaced only by one that took
 * effect.
 *
 * **Why an extension, not a member:** `micrometer-core` is an optional
 * dependency, and Joran resolves the parameter types of every public
 * method of [KafkaAppender] while configuring it - a member with a
 * [MeterRegistry] parameter made Logback configuration fail without
 * Micrometer on the classpath (see [KafkaAppender.installMetrics]).
 * Kotlin callers keep writing `appender.bindMeterRegistry(registry)`;
 * Java callers use `KafkaAppenderMeterRegistry.bindMeterRegistry(appender, registry)`.
 *
 * @param registry The Micrometer registry to publish to.
 * @param commonTags Tags attached to every metric. Use sparingly.
 *                   The registry's own common tags are typically
 *                   enough; pass [Tags.empty]
 *                   for the no-extra-tags case.
 * @return `true` if the appender is bound to [registry] afterwards;
 *         `false` when it is not started or the bind was refused for a
 *         duplicate meter identity (both reported via Logback's status
 *         manager).
 */
@JvmOverloads
fun KafkaAppender.bindMeterRegistry(
    registry: MeterRegistry,
    commonTags: Iterable<Tag> = Tags.empty(),
): Boolean =
    installMetrics { bindings, transport, info ->
        bindings.bind(
            registry = registry,
            commonTags = commonTags,
            appenderName = name,
            circuitBreakerRegistry = transport.circuitBreakerRegistry,
            producerRegistry = transport.producerRegistry,
            info = info,
        )
    }

/**
 * Reverses a [bindMeterRegistry] binding to [registry] while the
 * appender keeps running: the hot path and the dispatchers switch
 * back to no-op metrics, then every meter of the binding is removed.
 * A binding to any other registry is left untouched, so an owner
 * of a registry can withdraw from it without knowing who else bound
 * the appender. Used by [KafkaAppenderMetricsBinding] when its
 * Spring context closes.
 *
 * @return `true` if a binding to [registry] was removed.
 */
internal fun KafkaAppender.unbindMeterRegistry(registry: MeterRegistry): Boolean = withdrawMetrics { it.isBoundTo(registry) }
