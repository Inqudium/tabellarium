![Tabellarium banner](assets/banner-light-docs.svg#only-light)
![Tabellarium banner](assets/banner-dark-docs.svg#only-dark)

# Tabellarium

Tabellarium is a resilient Logback appender that ships structured log
events to Apache Kafka. Named after the Roman letter-carrier, it never
blocks the sender: per-topic-class circuit breakers stop hammering a
broken route, mandatory overrides seal audit-grade delivery
(`acks=all`, idempotence), and a fallback appender catches what cannot
be shipped.

## Features

### Delivery

- **The sender is never made to wait.** The hot path takes no lock
  (`UnsynchronizedAppenderBase`, atomics only), so it neither pins
  carrier threads on virtual threads nor stalls a Reactor event loop.
  `max.block.ms` is capped at 500 ms per class, bounding the worst case
  even when the producer buffer is full.
- **Undeliverable events take the side road, not the ditch.** An
  optional fallback appender receives what Kafka refuses, fed through a
  bounded queue and its own worker thread — the Kafka I/O thread is
  never blocked by a slow file appender, and dropped events are counted
  rather than silently lost.

### Circuit breaking

- **A broken route is not hammered.** One Resilience4j circuit breaker
  per topic class, so a stuck audit broker never throttles technical
  logging. Deterministic payload errors (`RecordTooLargeException` and
  friends) are deliberately excluded from the failure rate — a buggy log
  statement must not silence a healthy pipeline.
- **Recovery probes are spread over time.** In half-open state a
  throttle admits one probe per interval instead of letting a
  high-volume logger burn every permitted call in microseconds.

### Compliance & routing

- **Compliance grades that configuration cannot weaken.** Topics are
  classified `AUDIT` / `FUNCTIONAL` / `TECHNICAL` / `PERFORMANCE`; the
  first two enforce `acks=all` (and idempotence for `AUDIT`) over any
  conflicting operator value, and every override is reported at startup
  rather than applied silently.
- **Marker-based routing.** `<mapping>` elements route by SLF4J marker
  to their own topic and class; one producer, breaker and `client.id`
  per active class, and none for dormant ones.

### Operations

- **Misconfiguration fails at startup, not per event.** Blank identity
  fields, invalid Kafka topic names, unknown topic classes, duplicate
  markers and idempotence-incompatible tuning all abort `start()` with a
  named error.
- **Metrics are opt-in and complete.** Counters, timers and queue gauges
  for a Micrometer registry, plus Grafana dashboards and a Spring
  binding helper — and nothing at all until you bind a registry.

### Footprint & security

- **A lean dependency tree.** Micrometer, Spring and the Logstash
  encoder are all `optional`; consumers who do not want them do not get
  them.
- **Security-conscious defaults.** Diagnostics never echo your producer
  configuration, compliance-graded topics warn when shipped over
  cleartext, the partitioning key is length-bounded, and the appender
  ignores its own producer's log output instead of feeding it back.

## Quick start

```xml
<appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
    <encoder class="net.logstash.logback.encoder.LogstashEncoder"/>
    <kafkaProducerProperties>
        bootstrap.servers=kafka.example.com:9092
    </kafkaProducerProperties>
    <topicMapping>
        <defaultTopic>my-application.logs</defaultTopic>
    </topicMapping>
    <environment>${STAGE}</environment>
    <component>${ARTIFACT_ID}</component>
    <cmdbId>MyApplication</cmdbId>
    <appender-ref ref="KAFKA_FALLBACK_FILE"/>
</appender>
```

## Documentation

- **[Configuration guide](config/kafka-appender-config-guide.md)** —
  every XML element, producer property composition, topic routing,
  resilience behavior, and startup validation.
- **[Metrics overview](metrics/metrics-overview.md)** — the full
  Micrometer metric inventory with tags and dashboard guidance.
- **[Example configuration](config/example-logback-spring.xml)** —
  a complete, annotated `logback-spring.xml`.
- **Grafana dashboards** —
  [appender dashboard](metrics/kafka-appender-dashboard.json) and
  [producer-internals dashboard](metrics/kafka-producer-internals-dashboard.json),
  ready for import.
- **[API reference](https://inqudium.github.io/tabellarium/api/)** —
  the KDoc of the public API, generated with Dokka.

## Project

- [README](https://github.com/Inqudium/tabellarium#readme) — the full
  project story, architecture, and design rationale.
- [Contributing](https://github.com/Inqudium/tabellarium/blob/main/CONTRIBUTING.md)
- [Changelog](https://github.com/Inqudium/tabellarium/blob/main/CHANGELOG.md)
- [License (Apache 2.0)](https://github.com/Inqudium/tabellarium/blob/main/LICENSE)
