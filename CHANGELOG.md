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
- Configuration guide, metrics overview, and Grafana dashboards under
  `docs/config/` and `docs/metrics/`.

[Unreleased]: https://github.com/Inqudium/tabellarium/commits/main
