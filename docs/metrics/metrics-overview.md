## Complete metrics overview (as of all patches applied)

All metrics carry the `appender` tag, reflecting the Logback appender name (`"unnamed"` if not set). Plus, optionally, the common tags the operator passed to the `KafkaAppenderMetricsBinding` constructor.

### Counters

| Metric                             | Tags                                | When it is incremented                                       |
| ---------------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| `kafka.appender.events.accepted`   | `appender`, `topic.class`           | Every event that enters `KafkaAppender.append()` (after routing to its topic class) |
| `kafka.appender.events.dispatched` | `appender`, `topic.class`           | Event was successfully handed to `producer.send()` (callback outcome still unknown) |
| `kafka.appender.events.fallback`   | `appender`, `topic.class`, `reason` | Event was routed past Kafka (to the fallback appender if configured, otherwise dropped) |
| `kafka.appender.fallback.dropped`  | `appender`                          | FallbackDispatcher had to drop (queue full or shutdown timeout) |

### Timers

| Metric                         | Tags                                 | What is measured                                       |
| ------------------------------ | ------------------------------------ | ------------------------------------------------------ |
| `kafka.appender.send.duration` | `appender`, `topic.class`, `outcome` | Wall clock from the `producer.send()` call to the callback |

### Gauges

| Metric                                   | Tags                      | What it shows                                                |
| ---------------------------------------- | ------------------------- | ------------------------------------------------------------ |
| `kafka.appender.fallback.queue.size`     | `appender`                | Current depth of the FallbackDispatcher queue (live per scrape) |
| `kafka.appender.fallback.queue.capacity` | `appender`                | Maximum depth of the queue (constant)                        |
| `kafka.appender.send.queue.size`         | `appender`, `topic.class` | Current depth of the class's SendDispatcher queue (live per scrape) |
| `kafka.appender.send.queue.capacity`     | `appender`, `topic.class` | Maximum depth of the SendDispatcher queue (constant)         |

## Tag values

### `appender` — one value per appender instance

| Value                 | Source                                                       |
| --------------------- | ------------------------------------------------------------ |
| Logback appender name | From the `<appender name="...">` attribute in the XML        |
| `unnamed`             | Fallback when no name is set (should not occur in production) |

### `topic.class` — 4 possible values

| Value         | When set                                                     |
| ------------- | ------------------------------------------------------------ |
| `audit`       | Events for `TopicClass.AUDIT`                                |
| `functional`  | Events for `TopicClass.FUNCTIONAL`                           |
| `technical`   | Events for `TopicClass.TECHNICAL` (default for unclassified events) |
| `performance` | Events for `TopicClass.PERFORMANCE`                          |

### `reason` — 6 possible values (only on `events.fallback`)

| Value           | Meaning                                                      |
| --------------- | ------------------------------------------------------------ |
| `breaker.open`  | Circuit breaker gave no permission (OPEN, or HALF_OPEN exhausted) |
| `throttle`      | Half-open throttle: probe gap not yet elapsed                |
| `send.error`    | `producer.send()` threw synchronously or the callback reported an exception |
| `encoder.error` | Hot-path exception before `send()` (encoder, routing, OOM)   |
| `queue.full`    | The class's SendDispatcher queue was full — Kafka delivery cannot keep up |
| `shutdown`      | Event was still in the SendDispatcher queue or in flight when the appender stopped |

### `outcome` — 2 possible values (only on `send.duration`)

| Value     | Meaning                                                      |
| --------- | ------------------------------------------------------------ |
| `success` | Callback reported `exception == null`                        |
| `error`   | Callback reported an exception or `producer.send()` threw synchronously |

## Common tags (configured by the operator)

Tags the operator passes to `KafkaAppenderMetricsBinding` via the constructor are attached to **every** metric:

```kotlin
KafkaAppenderMetricsBinding(
    registry,
    Tags.of(
        "application", "payment-service",
        "region", "eu-central-1",
        "environment", "prod",
    )
)
```

These then appear in addition to the per-metric tags on all counters, timers, and gauges listed above.

## Cardinality per appender instance

| Metric                    | Series count                          |
| ------------------------- | ------------------------------------- |
| `events.accepted`         | 4 (one per `topic.class`)             |
| `events.dispatched`       | 4                                     |
| `events.fallback`         | 24 (4 × 6 = `topic.class` × `reason`) |
| `send.duration`           | 8 (4 × 2 = `topic.class` × `outcome`) |
| `fallback.dropped`        | 1                                     |
| `fallback.queue.size`     | 1                                     |
| `fallback.queue.capacity` | 1                                     |
| `send.queue.size`         | 4 (one per active `topic.class`)      |
| `send.queue.capacity`     | 4                                     |
| **Total**                 | **51**                                |

Multiplied by the `appender` tag (1 value in the default case) and the common-tags cardinality (typically 1, since constant per service).

## Circuit breaker metrics

Registered by the appender's own binder (no `resilience4j-micrometer` needed; `micrometer-core` suffices). The metric names and tags match those of `TaggedCircuitBreakerMetrics` 1:1, extended by the `appender` tag and the common tags — so multiple KafkaAppender instances no longer collide on the same registry:

| Metric                                       | Tags                        | Type                                                         |
| -------------------------------------------- | --------------------------- | ------------------------------------------------------------ |
| `resilience4j.circuitbreaker.state`          | `appender`, `name`, `state` | Gauge: 1 if the breaker is in this state, otherwise 0        |
| `resilience4j.circuitbreaker.calls`          | `appender`, `name`, `kind`  | Timer per call outcome (`successful`, `failed`, `ignored`)   |
| `resilience4j.circuitbreaker.not.permitted.calls` | `appender`, `name`, `kind` | Counter (`kind=not_permitted`): calls rejected by the open breaker |
| `resilience4j.circuitbreaker.buffered.calls` | `appender`, `name`, `kind`  | Gauge: currently in the sliding window                       |
| `resilience4j.circuitbreaker.slow.calls`     | `appender`, `name`, `kind`  | Gauge: slow calls in the sliding window                      |
| `resilience4j.circuitbreaker.failure.rate`   | `appender`, `name`          | Gauge: current failure rate in percent                       |
| `resilience4j.circuitbreaker.slow.call.rate` | `appender`, `name`          | Gauge: current slow-call share in percent                    |

**`name` values** correspond to the active topic classes:

- `kafka-appender-audit`
- `kafka-appender-functional`
- `kafka-appender-technical`
- `kafka-appender-performance`

## Kafka producer bridge (when `KafkaClientMetrics` is on the classpath)

Activated automatically via reflection when `io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics` is available. One binding per active TopicClass:

| Metric prefix                           | Examples                                  |
| --------------------------------------- | ----------------------------------------- |
| `kafka.producer.record.send.total`      | Records the producer accepted             |
| `kafka.producer.record.error.total`     | Records that failed                       |
| `kafka.producer.record.size.avg`        | Average record size                       |
| `kafka.producer.batch.size.avg`         | Average batch size                        |
| `kafka.producer.request.latency.avg`    | Average request latency to brokers        |
| `kafka.producer.outgoing.byte.rate`     | Bytes/second to the cluster               |
| `kafka.producer.buffer.available.bytes` | Free buffer bytes                         |
| …                                       | (~40 producer-internal metrics in total)  |

All carry the `topic.class` tag to disambiguate between the per-class producers.

## Key Prometheus queries for a dashboard

```promql
# Throughput per topic class
rate(kafka_appender_events_accepted_total[1m])

# Loss rate broken down by reason
rate(kafka_appender_events_fallback_total[1m])

# p99 send latency
histogram_quantile(0.99,
    rate(kafka_appender_send_duration_seconds_bucket[5m]))

# Fallback queue saturation as a ratio
kafka_appender_fallback_queue_size
  / kafka_appender_fallback_queue_capacity

# Actual data loss (= dropped events)
rate(kafka_appender_fallback_dropped_total[5m])

# Circuit breaker state
resilience4j_circuitbreaker_state{name=~"kafka-appender-.*"}
```

## Key alert thresholds

| Condition                                                    | Meaning                                                      | Severity                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------- |
| `kafka_appender_fallback_dropped_total > 0`                  | Actual data loss, FallbackDispatcher queue overflowed        | Critical                        |
| `kafka_appender_fallback_queue_size / capacity > 0.8` for 5 min | Fallback appender is slower than the event rate              | Warning                         |
| `resilience4j_circuitbreaker_state{state="open"} == 1`       | Cluster loss for this topic class                            | Critical (for AUDIT/FUNCTIONAL) |
| `rate(kafka_appender_events_fallback{reason="send.error"}[1m]) > 0` for 10 min | Persistent send errors that were not filtered out as client errors | Warning                         |
| `rate(kafka_appender_events_fallback{reason="encoder.error"}[5m]) > 0` | Hot-path exceptions in the encoder/routing                   | Warning (code bug)              |

Ready-made Grafana dashboards built on these metrics live next to this page:
[`kafka-appender-dashboard.json`](./kafka-appender-dashboard.json) and
[`kafka-producer-internals-dashboard.json`](./kafka-producer-internals-dashboard.json).
