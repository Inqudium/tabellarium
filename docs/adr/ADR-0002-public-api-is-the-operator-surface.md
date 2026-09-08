# ADR-0002: The public API is the operator surface

**Status:** Accepted  
**Date:** 2026-08-29  
**Deciders:** Dirk Haase (maintainer)  
**Related:** ADR-0004 (bounds the lifecycle contract of `KafkaAppender`, the
centre of this surface)

## Context

The third architecture review
(`docs/assessment/ARCHITECTURE_REVIEW-2026-08-29T02-16-27.md`,
finding 1) found the published API surface to be roughly twice the
composable surface. Eleven public top-level declarations
(`TopicRouter`, `TopicTable`, `ProducerRegistry`, `ProducerFactory`,
`ProducerPropertiesBuilder`, `TopicClassProperties`,
`MandatoryOverrideViolation`, `MessageEnricher`, `EnrichedRecord`,
`parseKafkaProducerProperties`, `KafkaAppenderMetrics`) were published
by Dokka as API although no public wiring path existed to compose or
inject them: the appender's substitution seams are `internal`, and
`ResilientMessageSender`, without which no pipeline can be assembled,
always was.

The README additionally promised constructor-injection substitutability
that the wiring never delivered. One concrete sharp edge:
`ProducerRegistry` carried the raw, credential-bearing effective
producer properties on a public type.

At `1.0.0-SNAPSHOT` this boundary was still cheap to decide; after a
first release it would have been a breaking change with a deprecation
cycle.

## Decision

**The supported public API of this library is the operator surface,
and nothing else.**

### The operator surface

| Public declaration | Why it is public |
|---|---|
| `KafkaAppender` | The Logback appender; Joran instantiates and configures it |
| `TopicMappingConfig`, `TopicMappingEntry` | Joran-bound holders of the `<topicMapping>` XML element |
| `TopicClass` | Names the four topic classes in configuration and documentation |
| `KafkaAppenderMetricsBinding` | The opt-in Spring helper that binds appenders to a `MeterRegistry` |

### Everything else is `internal`

The building blocks below `KafkaAppender` remain individually testable
(Kotlin `internal` is visible to the test compilation) and remain
substitutable through the existing `internal` seams. They are not a
consumer contract, carry no semver commitment, and do not appear in the
published API reference (Dokka documents public API only).

### Extension goes through the operator surface

Extension needs are met by widening the operator surface, not by
exposing internals. A future per-deployment partitioning-key override,
for example, becomes a `KafkaAppender` property (XML-bindable), not a
public `MessageEnricher` constructor.

## Consequences

**Positive:**

- Dokka's API reference shrinks to the supported surface; consumers
  cannot accidentally bind to implementation types. The compiler now
  enforces what the README previously only implied.
- The credential-bearing `ProducerRegistry.effectiveProperties` map is
  no longer reachable from outside the module.
- The README architecture and extension-points sections describe this
  boundary instead of the former substitutability claim.

**Negative:**

- A consumer who wants to compose the building blocks programmatically
  (an own producer factory, a custom enricher) has no supported path;
  the answer is a new XML-bindable property, which is slower than
  subclassing would have been.
- The test suite depends on `internal` visibility and is therefore
  coupled to the module's compilation unit; it cannot be moved into a
  separate module without re-opening seams.

**Neutral:**

- Reversal is deliberate: making a type public again is a conscious,
  documented API addition (follow-up ADR), never a side effect of a
  refactoring.
