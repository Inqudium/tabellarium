# ADR-0002: The public API is the operator surface

- **Status:** accepted
- **Date:** 2026-08-29
- **Context:** The third architecture review
  (`docs/assessment/ARCHITECTURE_REVIEW-2026-08-29T02-16-27.md`,
  finding 1) found the published API surface to be roughly twice the
  composable surface: eleven public top-level declarations
  (`TopicRouter`, `TopicTable`, `ProducerRegistry`, `ProducerFactory`,
  `ProducerPropertiesBuilder`, `TopicClassProperties`,
  `MandatoryOverrideViolation`, `MessageEnricher`, `EnrichedRecord`,
  `parseKafkaProducerProperties`, `KafkaAppenderMetrics`) were
  published by Dokka as API although no public wiring path existed to
  compose or inject them - the appender's substitution seams are
  `internal`, and `ResilientMessageSender`, without which no pipeline
  can be assembled, always was. The README additionally promised
  constructor-injection substitutability that the wiring never
  delivered. One concrete sharp edge: `ProducerRegistry` carried the
  raw, credential-bearing effective producer properties on a public
  type. At `1.0.0-SNAPSHOT` this boundary was still cheap to decide;
  after a first release it would have been a breaking change.

## Decision

**The supported public API of this library is the operator surface,
and nothing else:**

| Public declaration | Why it is public |
|---|---|
| `KafkaAppender` | The Logback appender; Joran instantiates and configures it |
| `TopicMappingConfig`, `TopicMappingEntry` | Joran-bound holders of the `<topicMapping>` XML element |
| `TopicClass` | Names the four topic classes in configuration and documentation |
| `KafkaAppenderMetricsBinding` | The opt-in Spring helper that binds appenders to a `MeterRegistry` |

Everything else in the module is `internal` implementation. The
building blocks below `KafkaAppender` remain individually testable
(Kotlin `internal` is visible to the test compilation) and remain
substitutable through the existing `internal` seams - but they are
not a consumer contract, carry no semver commitment, and do not
appear in the published API reference (Dokka documents public API
only).

Extension needs are met by widening the **operator surface**, not by
exposing internals: a future per-deployment partitioning-key override,
for example, becomes a `KafkaAppender` property (XML-bindable), not a
public `MessageEnricher` constructor.

## Consequences

- Dokka's API reference shrinks to the supported surface; consumers
  cannot accidentally bind to implementation types (the compiler
  enforces what the README previously only implied).
- The credential-bearing `ProducerRegistry.effectiveProperties` map is
  no longer reachable from outside the module.
- Reversal is deliberate: making a type public again is a conscious,
  documented API addition (follow-up ADR), never a side effect.
- The README architecture section and extension-points section
  describe this boundary instead of the former substitutability claim.
