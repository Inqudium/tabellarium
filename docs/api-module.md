# Module tabellarium

![Tabellarium — a resilient Logback appender for Apache Kafka](https://inqudium.github.io/tabellarium/assets/banner-dark.svg)

Tabellarium is a resilient Logback appender that ships structured log
events to Apache Kafka: per-topic-class circuit breakers, mandatory
producer-side delivery overrides for audit-class topics, and a fallback
appender for what cannot be shipped. Delivery is best-effort transport
with visible loss; the topic classes harden the producer policy, not
the end-to-end pipeline.

This is the API reference of the public surface. The
[configuration guide](https://inqudium.github.io/tabellarium/config/kafka-appender-config-guide/)
and the
[metrics overview](https://inqudium.github.io/tabellarium/metrics/metrics-overview/)
live on the [documentation site](https://inqudium.github.io/tabellarium/).
