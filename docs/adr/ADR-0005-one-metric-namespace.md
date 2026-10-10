# ADR-0005: One metric namespace for every meter the appender publishes

**Status:** Accepted  
**Date:** 2026-10-11  
**Deciders:** Dirk Haase (maintainer)  
**Related:** ADR-0002 (the metrics binding is part of the operator
surface whose published names this ADR fixes)

## Context

A bound `KafkaAppender` publishes three groups of meters: its own
(`kafka.appender.events.*`, queues, sizes, info), the circuit-breaker
meters of its per-class breakers, and the Kafka client's
producer-internal meters. Until 2026-10-10 the last two carried the
names of the binders that produce them: `resilience4j.circuitbreaker.*`
from the appender's own mirror of `TaggedCircuitBreakerMetrics`, and
`kafka.producer.*` from Micrometer's `KafkaClientMetrics`. Both groups
were already distinguishable from the application's own breakers and
Kafka clients by the `appender` tag (and the producer meters by
`topic.class`), because tags are how Micrometer tells clients apart.

What the tags did not give an operator was one handle: with Spring
Boot's `management.metrics.enable.<prefix>` or a `MeterFilter` on a name
prefix, the appender's meters could not be selected or switched off as
one group, and the breaker and producer meters sat in the application's
`resilience4j.*` and `kafka.producer.*` namespaces next to meters the
appender has nothing to do with. The 2026-10-10 change moved both groups
under `kafka.appender.` (`kafka.appender.circuitbreaker.*`,
`kafka.appender.producer.*`).

The producer group needs a mechanism for that: `KafkaClientMetrics`'s
names are fixed. A rename `MeterFilter` on the operator's registry stays
for the registry's life (filters cannot be removed), sees every meter of
the application, and makes Micrometer warn when added to a registry that
already holds meters - at bind time always the case. A filter on a
private composite in front of the operator's registry fails when that
registry is itself a composite, as Spring Boot's is: Micrometer forwards
a nested composite's meters straight to the leaf registries, so the
operator's composite - the one in-process readers query - never holds
them. What remained is `RenamingMeterMirror`: the binder registers into
a private registry, and for every meter added there a read-through
counterpart is registered in the operator's registry under the new name,
removed again with the source meter or on unbind.

The fifth architecture review
(`docs/assessment/ARCHITECTURE_REVIEW-2026-10-10T22-15-57.md`,
finding 2) asked whether the goal is worth any mechanism: the rename is a
breaking change for every dashboard and alert on the standard names, the
Kafka client's JMX MBeans keep `kafka.producer:type=…` so a JMX exporter
and Micrometer report the same producer under two name families, and the
mirror is a bridge whose correctness depends on the binder's
registration behaviour on its own refresh scheduler. No operator had
asked for the single namespace; the change was still unreleased
(`1.1.3-SNAPSHOT`) and therefore cheap to decide either way.

## Decision

**Every meter the appender publishes starts with `kafka.appender.`.**
The single namespace is a product decision: the appender is one
component of the application, and its observability is selected,
filtered, documented and switched off as one group, by one prefix.

### The three groups

| Group | Names | Mechanism |
|---|---|---|
| Appender meters | `kafka.appender.events.*`, `kafka.appender.send.*`, `kafka.appender.fallback.*`, `kafka.appender.event.size`, `kafka.appender.info`, `kafka.appender.topic.info` | Registered directly by `MicrometerKafkaAppenderMetrics` |
| Circuit-breaker meters | `kafka.appender.circuitbreaker.*` (the meters, types and tags of `TaggedCircuitBreakerMetrics`, plus the `appender` tag) | The appender's own binder in `MetricsBindings` names them directly |
| Kafka producer meters | `kafka.appender.producer.*` for `kafka.producer.*`, `kafka.appender.producer.app.info.*` for the client's `kafka.app.info.*` | `RenamingMeterMirror`: the binder registers into a private registry, the mirror publishes read-through counterparts in the operator's registry |

### The mirror handles exactly what the binder registers

`RenamingMeterMirror` mirrors gauges, time gauges (keeping their unit)
and function counters - the meter types `KafkaClientMetrics` registers.
A meter of any other type is not mirrored and is reported once through
the binding's status warning. **Rationale:** a generic copy by
measurements, which the first version carried, served a case no binder
version has ever produced and had no test; a type change in the binder
is a Micrometer upgrade to look at, not something to paper over.

**Verification:** `RenamingMeterMirrorTest` pins the three types, the
refusal of a fourth, the composite target, the `delete.this` probe, the
removal on source removal and on close, and the close race;
`MetricsBindingsTest` pins the renamed names, tags and teardown against
real `KafkaProducer` instances.

**Revisit when:** a Micrometer upgrade changes `KafkaClientMetrics`'s
registration behaviour (the meter types, the refresh scheduler, the
`delete.this` probe) - the mirror's tests are the signal; or an operator
documents a need for the standard `kafka.producer.*` names in Micrometer
next to the JMX names.

## Consequences

**Positive:**

- One prefix selects, filters or switches off every meter of the
  appender: `management.metrics.enable.kafka.appender=false`, a single
  `MeterFilter.denyNameStartsWith("kafka.appender")`, one Prometheus
  relabel rule. The metrics guide documents the three switch-off levels
  on that prefix alone.
- The appender's breaker and producer meters never mix with the
  application's own `resilience4j.*` and `kafka.producer.*` series, so
  dashboards and alerts on the application's clients are unaffected by
  the appender's presence.
- Composite registries (Spring Boot's) see the new names on the
  composite itself, and the operator's own filters and common tags apply
  to the mirrored meters like to any other.

**Negative:**

- A **breaking rename** for every existing dashboard and alert on the
  standard `resilience4j.circuitbreaker.*` and `kafka.producer.*` names
  (Prometheus `kafka_producer_*` → `kafka_appender_producer_*`),
  announced in the CHANGELOG for the first release that carries it. The
  shipped Grafana dashboards use the new names.
- Two name families for one producer: Micrometer reports
  `kafka.appender.producer.*`, the Kafka client's JMX MBeans keep
  `kafka.producer:type=…`. A JMX exporter and Micrometer therefore name
  the same producer differently; the metrics guide says so.
- The mirror is a bridge to the binder's behaviour: every Micrometer
  upgrade must be checked against `RenamingMeterMirrorTest` and the
  real-producer tests in `MetricsBindingsTest`.
- The producer-metrics proofs need real `KafkaProducer` instances (a
  `MockProducer` exposes no client metrics).

**Neutral:**

- Reversal after a release is a second breaking rename; before the
  first release that carries the namespace it is a revert of the
  mechanism. This ADR records that the decision was taken with that cost
  known.
- The breaker meters need no mirror because the appender's own binder
  names them; the cost of the namespace is paid for the producer group
  only.
