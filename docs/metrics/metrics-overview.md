# Metrics overview

All metrics carry the `appender` tag, reflecting the Logback appender name (`"unnamed"` if not set). Plus, optionally, the common tags the operator passed to the `KafkaAppenderMetricsBinding` constructor.

### Counters

| Metric                             | Tags                                | When it is incremented                                       |
| ---------------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| `kafka.appender.events.accepted`   | `appender`, `topic.class`           | Every event that enters `KafkaAppender.append()` (after routing to its topic class; a hot-path failure before routing counts under `technical`). Events dropped by the reentry and self-logging guards never enter the pipeline and are not counted |
| `kafka.appender.events.dispatched` | `appender`, `topic.class`           | Event was handed to `producer.send()` without a synchronous failure (callback outcome still unknown); an event the client rejected before `send()` returned - metadata timeout, buffer exhausted, record too large - counts as `events.fallback{reason="send.error"}` instead, never as both |
| `kafka.appender.events.fallback`   | `appender`, `topic.class`, `reason` | Event was routed past Kafka (to the fallback appender if configured, otherwise dropped) |
| `kafka.appender.fallback.dropped`  | `appender`                          | FallbackDispatcher had to drop: queue full, the fallback appender's `doAppend` threw, its worker died, or events remained at shutdown |

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
| `send.error`    | `producer.send()` threw synchronously, the callback reported an exception, or the class's SendDispatcher worker died |
| `encoder.error` | Hot-path exception before `send()` (encoder, routing, OOM)   |
| `queue.full`    | The class's SendDispatcher queue was full — Kafka delivery cannot keep up |
| `shutdown`      | Event was still in the SendDispatcher queue or in flight when the appender stopped, or was logged after it stopped |

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

## Reading `kafka.appender.send.duration`

**What the clock spans.** It starts immediately before `producer.send()`, after the breaker and the half-open throttle have granted permission and the record has been built, and stops in the producer callback (or in the catch block when `send()` throws synchronously). In between lie:

- the synchronous part of `send()`: waiting for metadata and buffer space, capped by `max.block.ms`, plus partitioning;
- the wait in the client's record accumulator until `linger.ms` expires or the batch reaches `batch.size`;
- the network round trip and broker processing, including replication to the in-sync replicas for `acks=all`;
- client-internal retries, up to `delivery.timeout.ms`.

**What it does not span.** The wait in the class's SendDispatcher queue between the logging thread and the send worker, encoding and enrichment, and every event the breaker or the throttle turned away (no `send()` call, no timer sample). It is therefore not an end-to-end "log call to ack" measure; `send.queue.size` covers the missing leg. With `outcome=error` the sample is the time to failure, which during an outage clusters at the `max.block.ms` cap or at `delivery.timeout.ms`, not at broker latency.

**It is not the application's logging delay.** The application thread runs only the synchronous part of `doAppend` (event construction, routing, encoding, enrichment, an O(1) `offer` into the class's queue; a full queue diverts to the fallback instead of waiting) and never touches the Kafka client. No metric times that path, deliberately - a timer sample per event would itself be hot-path cost; the JMH benchmark `AppendPipelineBenchmark` measures it. Measured `doAppend` cost per event (sample mode, raw output `benchmarks/results/2026-08-29/r6-pipeline-sample-t{1,8,32}.txt` of the bench report of 2026-08-29; open-loop saturation with shedding engaged, 12-core workstation on the powersave governor, JDK 26 - orders of magnitude, not percentages):

| Caller threads | p50     | p90     | p99     | p99.9   | Mean     |
|----------------|---------|---------|---------|---------|----------|
| 1              | 0.12 µs | 0.22 µs | 0.45 µs | 2.6 µs  | 0.37 µs  |
| 8              | 0.25 µs | 1.3 µs  | 34 µs   | 62 µs   | 2.4 µs   |
| 32             | 0.50 µs | 3.8 µs  | 352 µs  | 428 µs  | 26 µs    |

The tail growth at 32 threads comes from oversubscribing the cores and from the saturated regime, not from Kafka, which the caller never touches; encoding cost is inside these figures and grows with the event size. In production, `send.queue.size` is the early indicator that the worker falls behind and `events.fallback{reason="queue.full"}` marks the point where events start diverting; the caller stays fast throughout. End-to-end latency from log call to broker ack is caller path + queue wait + `send.duration`; the middle leg is what no metric covers. A consumer can reconstruct it because the appender sets no record timestamp: the client stamps `CreateTime` at the `send()` call, so `CreateTime` minus the event timestamp in the payload equals caller path plus queue wait.

**Histograms are opt-in.** The timer is registered without percentile histograms. The `_bucket` series the queries below use exist only once the operator enables them, for example with a `MeterFilter` that sets `percentileHistogram` for `kafka.appender.send.duration`; without it a Prometheus registry exports `_count`, `_sum` and `_max` only. Enabling histograms multiplies the timer's series count by the number of buckets.

**Each class has its own floor.** The producer defaults per class shape the distribution more than the broker does:

| Class         | `linger.ms` | `acks`            | `max.block.ms` | Typical p50 at low volume                  |
| ------------- | ----------- | ----------------- | -------------- | ------------------------------------------ |
| `audit`       | 50          | `all`, idempotent | 500            | ~50 ms + round trip + replication          |
| `functional`  | 50          | `all`             | 500            | ~50 ms + round trip + replication          |
| `technical`   | 50          | `1`               | 500            | ~50 ms + round trip                        |
| `performance` | 100         | `1`               | 200            | ~100 ms + round trip                       |

- **Linger dominates.** At low volume no batch fills before `linger.ms` expires, so every record waits the full window. A p99 near 100 ms on `performance` is the configured batching delay, not a slow broker.
- **Volume shortens it.** At high volume the batch fills first and the duration drops towards the round trip; `performance` needs more throughput to get there (64 KB batches) than `technical` (32 KB). A class's duration therefore depends on its log rate, not only on Kafka.
- **`acks=all` adds milliseconds.** `audit` and `functional` wait for replication: a few milliseconds on a healthy cluster, much more with lagging replicas. Against the 50 ms linger floor the difference is small in normal operation, and `audit` and `functional` are practically indistinguishable.
- **Errors separate by cap.** While the client still blocks synchronously in `send()`, `performance` errors cluster at up to 200 ms, the other classes at up to 500 ms.

**Consequence for dashboards and alerts.** Aggregate and alert **per `topic_class`**. A quantile over all classes mixes the 50 ms and 100 ms floors and moves whenever the class mix shifts, without any change in Kafka. An alert threshold for `performance` has to sit above the one for `audit`. The shipped dashboard already groups its latency panels by `topic_class`.

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

All carry the `topic.class` tag to disambiguate between the per-class producers, plus the `appender` tag and the common tags.

## Key Prometheus queries for a dashboard

```promql
# Throughput per topic class
rate(kafka_appender_events_accepted_total[1m])

# Loss rate broken down by reason
rate(kafka_appender_events_fallback_total[1m])

# p99 send latency, per topic class (never across classes - each class
# has its own linger.ms floor, see "Reading kafka.appender.send.duration")
histogram_quantile(0.99, sum by (topic_class, le) (
    rate(kafka_appender_send_duration_seconds_bucket[5m])))

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
| `kafka_appender_fallback_dropped_total > 0`                  | Actual data loss in the fallback path (queue overflow, failing fallback appender, dead worker, shutdown remainder) | Critical                        |
| `kafka_appender_fallback_queue_size / capacity > 0.8` for 5 min | Fallback appender is slower than the event rate              | Warning                         |
| `resilience4j_circuitbreaker_state{state="open"} == 1`       | Cluster loss for this topic class                            | Critical (for AUDIT/FUNCTIONAL) |
| `rate(kafka_appender_events_fallback{reason="send.error"}[1m]) > 0` for 10 min | Persistent send errors that were not filtered out as client errors | Warning                         |
| `rate(kafka_appender_events_fallback{reason="encoder.error"}[5m]) > 0` | Hot-path exceptions in the encoder/routing                   | Warning (code bug)              |

Ready-made Grafana dashboards built on these metrics live next to this page:
[`kafka-appender-dashboard.json`](./kafka-appender-dashboard.json) and
[`kafka-producer-internals-dashboard.json`](./kafka-producer-internals-dashboard.json).
