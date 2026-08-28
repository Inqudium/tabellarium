![Tabellarium banner](assets/banner-light-docs.svg#only-light)
![Tabellarium banner](assets/banner-dark-docs.svg#only-dark)

# Tabellarium

Tabellarium is a resilient Logback appender that ships structured log
events to Apache Kafka. Named after the Roman letter-carrier, it never
blocks the sender: per-topic-class circuit breakers stop hammering a
broken route, mandatory overrides pin the strictest producer-side
delivery settings for audit-class topics (`acks=all`, idempotence),
and a fallback appender catches what cannot be shipped. Delivery is
best-effort transport with visible loss — see the
[delivery guarantees](https://github.com/Inqudium/tabellarium#delivery-guarantees)
for the exact scope.

## Features

### Delivery

- **The sender is never made to wait.** The hot path never blocks
  (`UnsynchronizedAppenderBase` - no synchronized `doAppend`, no waits,
  no I/O) and never calls `producer.send` itself: the caller enqueues
  into a bounded per-topic-class send queue in O(1), and a dedicated
  worker per class performs the send. `max.block.ms` is additionally capped per class
  (500 ms; 200 ms for `PERFORMANCE`), bounding each worker's worst
  case.
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

### Routing & service levels

- **Quality of service per log stream.** Each topic class carries its
  own producer tuning and its own circuit breaker: `AUDIT` buys
  producer-side durability (`acks=all`, idempotence, retries),
  `PERFORMANCE` buys throughput (larger batches, longer linger, tighter
  block budget), with `FUNCTIONAL` and `TECHNICAL` in between.
  Compliance-graded classes additionally enforce their producer settings
  over any conflicting operator value — and report every override at
  startup instead of applying it silently.
- **Marker-based routing.** `<mapping>` elements route by SLF4J marker
  to their own topic and class; one producer, breaker and `client.id`
  per active class, and none for dormant ones.

### Traceability

- **Every record says where it came from.** `meta.component`,
  `meta.cmdbId`, `meta.environment` and `meta.agent.*` ride on every
  record as headers, encoded once at startup rather than per event —
  so a consumer can filter by service, instance or stage without
  parsing the payload.
- **Trace affinity, attributable producers.** The record key is the
  MDC trace id, so the records of one trace share a partition and keep
  their relative order; each producer announces itself to the broker
  as `tabellarium-<component>-<class>`, so connections, quotas and
  `kafka.producer.*` metrics name the service and its service level
  instead of a generic `producer-N`.

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
- **[Test evidence](https://inqudium.github.io/tabellarium/tests/test-evidence/)** —
  the generated inventory of the test suite: every test sentence plus
  its rationale, grouped by component.
- **[Coverage report](https://inqudium.github.io/tabellarium/coverage/)** —
  the JaCoCo report of the run that built this site.
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
