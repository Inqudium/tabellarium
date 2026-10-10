package eu.inqudium.tabellarium

import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Publishes the meters of a third-party binder under different names:
 * the binder registers into [source], a private registry nothing
 * exports, and for every meter added there whose name [rename] maps,
 * an equivalent reading meter is registered in [target] under the new
 * name - removed again when the source meter is removed or the mirror
 * is [closed][close].
 *
 * Used for Micrometer's `KafkaClientMetrics`, whose `kafka.producer.*`
 * names are fixed, to publish them as `kafka.appender.producer.*` - the
 * one-namespace decision of ADR-0005 (every meter the appender publishes
 * starts with `kafka.appender.`; see "One namespace" in
 * [MetricsBindings]).
 *
 * ## Why a mirror and not a rename `MeterFilter`
 *
 * A filter on [target] would stay there for the life of the operator's
 * registry (filters cannot be removed), would see every meter of the
 * application, and makes Micrometer warn when it is added to a registry
 * that already holds meters - at bind time always the case. A filter on
 * a private composite in front of [target] avoids that but fails when
 * [target] is itself a composite, as Spring Boot's is: Micrometer
 * forwards a composite's meters straight to the leaf registries below a
 * nested composite, so the operator's composite - the registry
 * in-process readers query - never holds them. The mirror registers in
 * [target] like any binder, so composites, the operator's filters and
 * common tags behave as for every other meter.
 *
 * ## What is mirrored
 *
 * Gauges (time gauges keeping their unit) and function counters - the
 * types `KafkaMetrics` registers - read through to the source meter.
 * Any other type is not mirrored but reported through [warn], once: it
 * would mean the binder changed, and a generic copy by measurements
 * that no binder version has ever needed is not kept for that case
 * (`ARCHITECTURE_REVIEW-2026-10-10T22-15-57`, finding 2). Names
 * [rename] returns `null` for are not mirrored (`KafkaMetrics` briefly
 * registers a `delete.this` probe on bind).
 *
 * ## Threads
 *
 * The binder adds and removes meters on its own refresh scheduler, so
 * the listeners run on that thread; the mirror map is concurrent, and
 * a meter added while or after [close] runs is removed again instead
 * of outliving the binding. A failing mirror registration must not
 * break the binder: it is swallowed and the first one reported through
 * [warn].
 */
internal class RenamingMeterMirror(
    private val target: MeterRegistry,
    private val rename: (String) -> String?,
    private val warn: (String, Throwable) -> Unit,
) : AutoCloseable {
    /** The registry the binder registers into; never exported. */
    val source: MeterRegistry = SimpleMeterRegistry()

    /** Mirror in [target] per source meter id. */
    private val mirrors = ConcurrentHashMap<Meter.Id, Meter>()
    private val closed = AtomicBoolean(false)
    private val failureReported = AtomicBoolean(false)

    init {
        source.config().onMeterAdded(::mirror)
        source.config().onMeterRemoved { meter -> mirrors.remove(meter.id)?.let(::removeFromTarget) }
    }

    /** Names of the meters currently mirrored into [target]. */
    val mirroredNames: Set<String>
        get() = mirrors.values.mapTo(HashSet()) { it.id.name }

    private fun mirror(meter: Meter) {
        if (closed.get()) return
        val name = rename(meter.id.name) ?: return
        try {
            val mirrored = register(meter, name)
            mirrors.put(meter.id, mirrored)?.let(::removeFromTarget)
            // Safety: lost the race with close() - it may already have
            // swept the map.
            if (closed.get()) mirrors.remove(meter.id)?.let(::removeFromTarget)
        } catch (e: Exception) {
            if (failureReported.compareAndSet(false, true)) {
                warn("Failed to publish meter '${meter.id.name}' as '$name' (further failures are not reported)", e)
            }
        }
    }

    private fun register(
        meter: Meter,
        name: String,
    ): Meter {
        val id = meter.id
        return when (meter) {
            is TimeGauge -> {
                TimeGauge
                    .builder(name, meter, meter.baseTimeUnit()) { it.value() }
                    .tags(id.tags)
                    .description(id.description)
                    .strongReference(true)
                    .register(target)
            }

            is Gauge -> {
                Gauge
                    .builder(name, meter) { it.value() }
                    .tags(id.tags)
                    .description(id.description)
                    .baseUnit(id.baseUnit)
                    .strongReference(true)
                    .register(target)
            }

            is FunctionCounter -> {
                FunctionCounter
                    .builder(name, meter) { it.count() }
                    .tags(id.tags)
                    .description(id.description)
                    .baseUnit(id.baseUnit)
                    .register(target)
            }

            else -> {
                // Rationale: reaches the warn path in mirror() - once, like
                // a failing registration - instead of a generic copy; see
                // "What is mirrored" in the class KDoc.
                throw IllegalStateException(
                    "meter type ${id.type} is not one KafkaClientMetrics registers (gauge, time gauge, function counter)",
                )
            }
        }
    }

    private fun removeFromTarget(meter: Meter) {
        try {
            target.remove(meter)
        } catch (_: Exception) {
            // A registry that fails removal leaves the series behind; nothing else to do.
        }
    }

    /**
     * Removes every mirror from [target] and closes [source]. The binder
     * must be closed first, so nothing is added any more; a late addition
     * is dropped anyway. Safe to call more than once.
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        mirrors.keys.toList().forEach { id -> mirrors.remove(id)?.let(::removeFromTarget) }
        source.close()
    }
}
