![Tabellarium banner](assets/banner-light-docs.svg#only-light)
![Tabellarium banner](assets/banner-dark-docs.svg#only-dark)

# Tabellarium

Tabellarium is a resilient Logback appender that ships structured log
events to Apache Kafka. Named after the Roman letter-carrier, it never
blocks the sender: per-topic-class circuit breakers stop hammering a
broken route, mandatory overrides seal audit-grade delivery
(`acks=all`, idempotence), and a fallback appender catches what cannot
be shipped.

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
