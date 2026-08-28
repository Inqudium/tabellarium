# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial public release of the `KafkaAppender`: resilient Logback
  appender shipping structured log events to Apache Kafka.
- Per-topic-class circuit breakers (Resilience4j) with half-open
  throttling.
- Mandatory override policy for compliance-graded topic classes
  (AUDIT, FUNCTIONAL, TECHNICAL, PERFORMANCE).
- Asynchronous fallback appender dispatch via `FallbackDispatcher`.
- Message enrichment with `meta.*` headers and MDC-`traceId`
  partitioning key.
- Opt-in Micrometer metrics with Spring binding helper
  (`KafkaAppenderMetricsBinding`).
- Per-class `client.id` default (`tabellarium-<component>-<topicclass>`)
  so producers are attributable on the broker; an operator-supplied
  `client.id` wins.
- Self-logging guard: events from the appender's own Kafka producer
  network threads are ignored, preventing producer-log feedback loops.
- Marker-based topic routing and per-topic classification via
  `<mapping>` elements in `<topicMapping>` (marker → topic →
  topicClass), activating the four-class compliance model through
  configuration; duplicate markers, conflicting classes, and unknown
  class names are rejected at startup.
- Optional `<defaultTopicClass>` element: classifies the default topic
  (and any unmapped topic) directly, so the default stream can carry a
  compliance grade without a synthetic marker mapping; conflicts with
  a `<mapping>` naming the default topic are rejected at startup.
- `<debug>true</debug>` additionally emits the generated producer
  settings per active class (derived `client.id`, applied default and
  mandatory overrides) as a diff against the operator's base
  properties — operator-supplied values, credentials included, are
  never repeated.
- Joran round-trip tests: the declarative XML surface (including
  `<mapping>` and `<appender-ref>`) is exercised end-to-end through
  `JoranConfigurator`, offline against a real Kafka producer.
- Eager validation of idempotence-incompatible producer tuning
  (`retries=0`, `max.in.flight.requests.per.connection>5`) with a
  clear startup error naming the conflict.
- Full Kafka topic-name validation at startup: reserved names (`.`,
  `..`) and names over 249 characters are rejected eagerly.
- `meta.agent.version` header is derived from the build (Maven-filtered
  resource) instead of a hardcoded constant.

### Fixed

- Self-logging guard matches the exact producer network-thread naming
  scheme instead of a substring, so an operator-supplied short
  `client.id` can no longer silently swallow events from unrelated
  application threads.
- `stop()` deregisters all Micrometer meters, closes the per-producer
  `KafkaClientMetrics` binders, and removes the appender's
  circuit-breaker meters; repeated binds replace instead of duplicate.
- `KafkaAppender.stop()` and `FallbackDispatcher.close()` are
  idempotent; remaining queued events are counted as dropped exactly
  once and the queue is drained.
- `HalfOpenThrottle` anchors its "no probe yet" state to the actual
  monotonic clock, removing a theoretical permanent-denial mode on
  platforms with a deeply negative `nanoTime` origin.
- Appender discovery in `KafkaAppenderMetricsBinding` is cycle-safe.
- `kafka.appender.events.fallback` is documented as "diverted from
  Kafka" (counted also when no fallback is configured and the event is
  dropped).

### Security

- The partitioning key is bounded at 128 characters; a longer value is
  treated as absent. It originates in the MDC and can therefore be
  attacker-influenced, and an unbounded key inflated records past
  `max.request.size` — whose `RecordTooLargeException` the circuit
  breaker deliberately ignores, so such events flooded the fallback
  appender indefinitely instead of tripping the breaker.
- A startup warning is emitted when a compliance-graded topic class
  (`AUDIT`/`FUNCTIONAL`) is served by a producer configured for
  cleartext transport (`security.protocol` unset or `PLAINTEXT`).
- Pipeline-construction failures report the exception type only; the
  Kafka-authored message and stack trace — built from credential-bearing
  configuration — now require `<debug>true</debug>`.
- `jackson-databind` moved to `test` scope: the shipped code contains no
  Jackson reference, so consumers no longer inherit it on their runtime
  classpath.
- CI hardening: explicit least-privilege `permissions` on the CI
  workflow, and all GitHub Actions pinned to commit SHAs instead of
  mutable tags.
- CI now scans the resolved dependency graph against the OSV database
  (CycloneDX SBOM via `cyclonedx-maven-plugin` + OSV-Scanner) on every
  push and pull request and weekly, failing the build on any known
  advisory; the SBOM is retained as a build artifact.
- `lz4-java` pinned to 1.11.2, above the version `kafka-clients`
  resolves: versions up to 1.11.0 can crash the JVM through their JNI
  XXHash range handling (CVE-2026-59949). Not exploitable through this
  appender — the advisory excludes the attacker-controls-contents-only
  case, which is how Kafka's LZ4 codec uses it — but it shipped
  transitively to every consumer.

### Changed

- The Micrometer bind/unbind lifecycle moved from the appender into the
  internal `MetricsBindings` component (behavior unchanged).
- `EnrichedRecord.headers` is `internal`: the shared pre-encoded header
  arrays no longer appear on the public API, shrinking their read-only
  contract to module-internal code.
- Tests tagged `external-contract` are excluded from the default test
  run; run them with `mvn -Pexternal-contract test`.
- Configuration guide, metrics overview, and Grafana dashboards under
  `docs/config/` and `docs/metrics/`.

[Unreleased]: https://github.com/Inqudium/tabellarium/commits/main
