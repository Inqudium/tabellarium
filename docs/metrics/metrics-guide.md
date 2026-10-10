# Metrics guide

## When metrics are active

The appender emits no metrics by default. They appear once all of the
following hold:

1. **Micrometer is on the classpath.** `micrometer-core` is an optional
   dependency of tabellarium, so the application declares it (Spring Boot
   Actuator brings it along). Without it the appender runs normally and
   emits nothing.
2. **The appender is bound to a `MeterRegistry`.** This is opt-in and
   never automatic:
   - Spring: declare `KafkaAppenderMetricsBinding` as a bean or `@Import`
     it (needs `spring-context`, also optional). It binds every
     `KafkaAppender` of the Logback context on `ContextRefreshedEvent`.
     It is deliberately not Spring Boot auto-configuration.
   - Without Spring: call `KafkaAppender.bindMeterRegistry()` for each
     appender once the registry exists.
3. **The appender has started.** A bind on an appender that is not (or
   no longer) started is ignored with a status warning; the Spring binding
   retries it on the next refresh. An appender whose configuration is
   invalid never starts and therefore never has metrics.
4. **Its name is unique per registry.** A second appender with the same
   `<appender name="...">` (or a second unnamed one) on the same registry
   is refused entirely, with a status warning (see the `appender` tag).
5. **No filter switches them off.** A `MeterFilter` or
   `management.metrics.enable.*` can suppress single meters or all of
   them - see [Switching metrics off](#switching-metrics-off).

Exporting the registry - a Prometheus registry and its scrape endpoint,
an OTLP push, ... - is the application's part; tabellarium only registers
the meters.

**Not counted** are events logged before the bind (Logback initialization,
Spring bootstrap logging), and after a **Logback reconfiguration** the
replacement appenders stay unbound until the binding runs again
(`KafkaAppenderMetricsBinding.bindAppenders()`, public and idempotent;
see the README, "Wiring up: Spring applications").

**Additionally conditional:**

| Metrics                                   | Present only when                                                  |
| ----------------------------------------- | ------------------------------------------------------------------ |
| The two fallback queue gauges             | a fallback appender is configured (`<appender-ref>`)               |
| Kafka-producer bridge (`kafka.appender.producer.*`) | Micrometer's `KafkaClientMetrics` binder is on the classpath (part of `micrometer-core` 1.x); skipped silently otherwise |
| Percentile histograms (`_bucket` series) of the send timer and the size summary | a `MeterFilter` or `management.metrics.distribution.percentiles-histogram.*` enables them |

**Checking it.** With the Spring binding, the application log shows
`Bound KafkaAppender '<name>' to MeterRegistry.` per appender; a refused
or failed bind leaves a warning in the Logback status output. In the
exported metrics, a `kafka_appender_info` series for the appender is the
quickest sign that the bind took place.

## Metric inventory

All metrics carry the `appender` tag, reflecting the Logback appender name (`"unnamed"` if not set). Plus, optionally, the common tags the operator passed to the `KafkaAppenderMetricsBinding` constructor.

### Counters

| Metric                             | Tags                                | When it is incremented                                       |
| ---------------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| `kafka.appender.events.accepted`   | `appender`, `topic.class`           | Every event that enters `KafkaAppender.append()` (after routing to its topic class; a hot-path failure before routing counts under `technical`). Events dropped by the reentry and self-logging guards never enter the pipeline and are not counted |
| `kafka.appender.events.dispatched` | `appender`, `topic.class`           | Event was handed to `producer.send()` without a synchronous failure (callback outcome still unknown); an event the client rejected before `send()` returned - metadata timeout, buffer exhausted, record too large - counts as `events.fallback{reason="send.error"}` instead, never as both. Likewise never together with `reason="shutdown"`: an event the producer accepted is no longer divertible by a forced shutdown, and in the one residual race (the shutdown claim landing while the client was accepting the record) the event counts as the shutdown fallback only |
| `kafka.appender.events.fallback`   | `appender`, `topic.class`, `reason` | Event was routed past Kafka (to the fallback appender if configured, otherwise dropped) |
| `kafka.appender.fallback.dropped`  | `appender`                          | FallbackDispatcher had to drop: queue full, the fallback appender's `doAppend` threw, its worker died, or events remained at shutdown |

### Timers

| Metric                         | Tags                                 | What is measured                                       |
| ------------------------------ | ------------------------------------ | ------------------------------------------------------ |
| `kafka.appender.send.duration` | `appender`, `topic.class`, `outcome` | Wall clock from the `producer.send()` call to the callback |

### Distribution summaries

| Metric                      | Tags                      | What is measured                                             |
| --------------------------- | ------------------------- | ------------------------------------------------------------ |
| `kafka.appender.event.size` | `appender`, `topic.class` | Encoded payload size in bytes of every event the class's send worker takes up, before the breaker and throttle gates (key and headers excluded) |

### Gauges

| Metric                                   | Tags                      | What it shows                                                |
| ---------------------------------------- | ------------------------- | ------------------------------------------------------------ |
| `kafka.appender.fallback.queue.size`     | `appender`                | Current depth of the FallbackDispatcher queue (live per scrape) |
| `kafka.appender.fallback.queue.capacity` | `appender`                | Maximum depth of the queue (constant)                        |
| `kafka.appender.send.queue.size`         | `appender`, `topic.class` | Current depth of the class's SendDispatcher queue (live per scrape) |
| `kafka.appender.send.queue.capacity`     | `appender`, `topic.class` | Maximum depth of the SendDispatcher queue (constant)         |

The two `fallback.queue.*` gauges exist only when a fallback appender is
configured (`<appender-ref>`): without one there is no queue, and both
series are absent rather than reading a constant zero. An absent
`fallback.queue.size` therefore means "drop policy", never "empty queue".

### Info gauges

| Metric                      | Tags                                                                                    | What it shows                                                |
| --------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `kafka.appender.info`       | `appender`, `fallback`, `fallback.type`, `appender.version`, `kafka.client.version`, `debug` | Configuration of the appender instance; the value is always `1` |
| `kafka.appender.topic.info` | `appender`, `topic.class`, `topic`, `acks`, `linger.ms`                                 | One series per configured topic (mapped topics plus the default topic); the value is always `1` |

Info gauges follow the Prometheus `*_info` convention: the value carries
no information, the labels do. They are joined onto the other series
with `group_left`, and because the value is `1` the join leaves the
joined values untouched:

```promql
# Fallback rate per reason, labelled with where the events end up
sum by (job, instance, appender, reason) (rate(kafka_appender_events_fallback_total[5m]))
  * on (job, instance, appender) group_left (fallback, fallback_type) kafka_appender_info

# Send latency per class next to the settings that set its floor
histogram_quantile(0.99,
    sum by (job, instance, appender, topic_class, le) (rate(kafka_appender_send_duration_seconds_bucket[5m])))
  * on (job, instance, appender, topic_class) group_left (acks, linger_ms)
    max by (job, instance, appender, topic_class, acks, linger_ms) (kafka_appender_topic_info)
```

Join on the target labels (`job`, `instance`) together with `appender`:
appender names repeat across services (`KAFKA` everywhere), so
`on (appender)` alone matches the info series of other services and the
join fails with "many-to-many matching". The second query collapses the
topics with `max by` because `group_left` needs one info series per
class, and `acks`/`linger.ms` are the same for every topic of a class.

The labels are fixed for the lifetime of an appender instance: the
fallback slot refuses changes while the appender runs, and the topic
mapping and producer properties are built once at start. A Logback
reconfiguration replaces the instance, its info series end and new ones
begin. Keep that in mind for alerts that filter on a label value - a
renamed fallback appender is a new series.

| Label                  | Value                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `fallback`             | Name of the fallback appender (`<appender-ref ref="...">`); `none` without one, `unnamed` for one without a name. `fallback="none"` marks the appenders whose `events.fallback` are dropped events |
| `fallback.type`        | Simple class name of the fallback appender (e.g. `RollingFileAppender`); `none` without one |
| `appender.version`     | tabellarium version (the same value as the `meta.agent.version` record header). Not `version`, so that a `version` common tag for the application is not overridden on the info series |
| `kafka.client.version` | Version of the Kafka client on the classpath - often chosen by the application's dependency management, not by tabellarium |
| `debug`                | `true` or `false`, the `<debug>` setting                                                |
| `topic`                | Topic name as configured                                                                |
| `acks`, `linger.ms`    | The effective producer settings of the topic's class - the configured value, or the Kafka client default when the class leaves the key out (`acks` under explicit idempotence) |

The producer properties are deliberately not exported beyond these two
settings: they may hold credentials (`sasl.jaas.config`) and
infrastructure details that do not belong in a time-series database.

**Cost.** The info gauges never run on the logging path: they are
registered once by `bindMeterRegistry()` and read only when the registry
exports. Their cost is one series per appender plus one per configured
topic. Measured on 2026-10-10 with a `PrometheusMeterRegistry` (Micrometer
1.17.1), one appender with a fallback, all four topic classes and five
topics, real producers without a reachable broker:

| Measure                        | Info gauges on | Info gauges denied | Difference          |
| ------------------------------ | -------------- | ------------------ | ------------------- |
| Sample lines per scrape        | 505            | 499                | +6 (+1.2 %)         |
| Scrape size                    | 102,318 bytes  | 100,997 bytes      | +1,321 bytes (+1.3 %) |
| Scrape time (mean of 5,000)    | 546-607 µs     | 593-601 µs         | within run-to-run noise |
| Retained heap                  | -              | -                  | ~390 bytes per info gauge |

Of the 505 lines, 340 come from the Kafka-producer bridge, 88 from the
circuit-breaker binder and 67 from the appender's other meters. With a
reachable broker the producer adds per-node and per-topic series, so the
info gauges' share drops further.

**Switching them off.** Like every other meter, with
`management.metrics.enable.kafka.appender.info=false` and
`...topic.info=false` (Spring Boot), a `MeterFilter.deny`, or a Prometheus
drop rule - see [Switching metrics off](#switching-metrics-off). Without
them, the dashboard panels that join on them ("Fallback events by
destination", the configuration tables) stay empty.

## Tag values

### `appender` — one value per appender instance

| Value                 | Source                                                       |
| --------------------- | ------------------------------------------------------------ |
| Logback appender name | From the `<appender name="...">` attribute in the XML        |
| `unnamed`             | Fallback when no name is set (should not occur in production) |

The value is the meter identity of the instance, so it must be unique per registry. A second appender with the same name (or both unnamed) bound to the same registry would receive the very same meter objects; its binding is therefore refused with a Logback status warning, and it publishes nothing until it is given a distinct name.

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
| `shutdown`      | Event was still in the SendDispatcher queue, or in flight but not yet accepted by the producer, when the appender stopped, or was logged after it stopped |

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
| `event.size`              | 4 (one per `topic.class`)             |
| `fallback.dropped`        | 1                                     |
| `fallback.queue.size`     | 1 (only with a fallback appender)     |
| `fallback.queue.capacity` | 1 (only with a fallback appender)     |
| `send.queue.size`         | 4 (one per active `topic.class`)      |
| `send.queue.capacity`     | 4                                     |
| `info`                    | 1                                     |
| `topic.info`              | one per configured topic              |
| **Total**                 | **56** + one per configured topic (2 fewer without a fallback appender) |

Multiplied by the `appender` tag (1 value in the default case) and the common-tags cardinality (typically 1, since constant per service).

## Switching metrics off

Metrics can be switched off at three levels: not binding at all, filtering
individual meters in the application, or dropping series in Prometheus.
The appender has no setting of its own for any of them.

### Everything: do not bind

Without `bindMeterRegistry()` the appender runs on a no-op metrics hook:
no meters, no registry, and per event nothing but an empty call. This
also covers the circuit-breaker and Kafka-producer metrics, which are
registered only by the bind.

- **Spring:** do not declare (or `@Import`) `KafkaAppenderMetricsBinding`.
  A conditional bean switches it per environment:

  ```kotlin
  @Bean
  @ConditionalOnProperty("logging.kafka.metrics.enabled", matchIfMissing = true)
  fun kafkaAppenderMetricsBinding(registry: MeterRegistry) = KafkaAppenderMetricsBinding(registry)
  ```

  (`logging.kafka.metrics.enabled` is a property of your application, not
  one tabellarium reads.)
- **Without Spring:** do not call `KafkaAppender.bindMeterRegistry()`.

### Individual metrics: filter in the application

Every meter goes through the registry's `MeterFilter`s. A denied meter is
registered as a no-op: the appender keeps calling it, the call does
nothing, and binding, re-binding and `stop()` behave as before.

**Spring Boot** (`management.metrics.enable`, meter-name prefixes, the
longest match wins):

```properties
# all of the appender's own meters
management.metrics.enable.kafka.appender=false
# or single ones - the size summary, the two info gauges
management.metrics.enable.kafka.appender.event.size=false
management.metrics.enable.kafka.appender.info=false
management.metrics.enable.kafka.appender.topic.info=false
# the Kafka-producer bridge
management.metrics.enable.kafka.appender.producer=false
# the circuit-breaker metrics
management.metrics.enable.kafka.appender.circuitbreaker=false
```

All of the appender's meters, the circuit-breaker and producer metrics
included, live under `kafka.appender.`, so these prefixes never touch the
application's own breakers or Kafka clients.

**Plain Micrometer** (and Spring Boot, as a `MeterFilter` bean):

```kotlin
registry.config()
    // single meters or groups by name
    .meterFilter(MeterFilter.denyNameStartsWith("kafka.appender.event.size"))
    .meterFilter(MeterFilter.denyNameStartsWith("kafka.appender.circuitbreaker"))
    .meterFilter(MeterFilter.denyNameStartsWith("kafka.appender.producer"))
```

Configure the filters **before** the appender binds. Micrometer does not
apply a filter retroactively: meters registered before it was added stay
in the registry (and Micrometer logs a warning). Spring Boot applies its
filters when it creates the registry, so the binding on
`ContextRefreshedEvent` always sees them.

### In Prometheus: drop at scrape time

```yaml
scrape_configs:
  - job_name: payment-service
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: kafka_appender_event_size_bytes_.*|kafka_appender_(topic_)?info|kafka_appender_producer_.*
        action: drop
```

This saves storage in Prometheus only: the application still maintains
the meters and still serializes them on every scrape.

### What to keep in mind

- **The duplicate-name check depends on `kafka.appender.events.accepted`.**
  The bind looks for that counter to detect a second appender with the
  same name on the same registry (see the `appender` tag). If the counter
  is denied - for example by switching off the whole `kafka.appender`
  prefix - the check sees nothing, and two same-named appenders would
  share their circuit-breaker and producer meters. Keep appender names
  unique, or keep this one counter.
- **Invariants and alerts.** `events.accepted` = `events.dispatched` +
  `events.fallback` only holds if all three are kept; the alerts under
  "Key alert thresholds" need their metric.
- **Dashboards.** Panels on a denied metric stay empty. Without the info
  gauges these are "Fallback events by destination" and the configuration
  tables, without `event.size` the event-size panel; all other panels are
  unaffected.
- **Cost.** What switching off saves per metric: the info gauges cost
  nothing on the logging path (see "Info gauges"), `event.size` costs
  21-31 ns per event on the send worker (see "Reading
  `kafka.appender.event.size`"); all meters together, including the
  breaker metrics, cost about 120-150 ns per delivered event on the send
  worker (`SenderPathBenchmark`, 72-74 ns unbound vs. 192-223 ns bound on
  2026-10-10) and nothing on the logging caller beyond the counter
  increments of `append`.

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

## Reading `kafka.appender.event.size`

**What is counted.** The size of the encoded payload - what the encoder
produced for the event, without the partitioning key and the `meta.*`
headers - recorded once per event when the class's send worker takes it
up, whatever the breaker and the throttle then decide. A diverted event
is therefore measured too: the summary describes what the application
logs, not what Kafka accepted. Not measured are events that never reach
the worker: `queue.full`, `encoder.error` and `shutdown` remainders.

**Exported series.** A Prometheus registry exports
`kafka_appender_event_size_bytes_count`, `_sum` and `_max` per
`topic_class`; `_count` equals the events the worker took up, so its
rate trails `events.accepted` by exactly the three diversions above.
Percentiles are opt-in like the send-duration timer's, via a
`MeterFilter` that sets `percentileHistogram` for
`kafka.appender.event.size`.

```promql
# Average payload size per class
sum by (topic_class) (rate(kafka_appender_event_size_bytes_sum[5m]))
  / sum by (topic_class) (rate(kafka_appender_event_size_bytes_count[5m]))

# Largest payload in the max window, against max.request.size (default 1 MiB)
max by (topic_class) (kafka_appender_event_size_bytes_max) / 1048576
```

**Relation to the Kafka client's `record.size`.** `kafka.appender.producer.record.size.avg`
and `.max` (Kafka binder) measure the whole serialized record - payload
plus key and headers, before compression - only for records that
reached `producer.send`, averaged over the client's own sample window.
`event.size` measures the payload alone, includes diverted events and
follows the registry's windows. Close to `max.request.size`, alert on
the client's `record.size.max`: it is the value the client compares.

**Cost.** One `record()` per event, on the send worker, never on the
logging caller. Measured with `SenderPathBenchmark` on 2026-10-10
(`benchmarks/results/2026-10-10/`): +21 to +31 ns per event with metrics
bound (197/192 → 219/223 ns), unchanged without; the summary allocates
nothing (224 B/op in three of four bound runs, the fourth fork-bimodal as
documented for that benchmark). The run was single-threaded against an
instantly completing producer, so the figure is the bare worker-side
envelope; a real `producer.send` costs microseconds.

## Circuit breaker metrics

Registered by the appender's own binder (no `resilience4j-micrometer` needed; `micrometer-core` suffices). Meters, types and tags match those of `TaggedCircuitBreakerMetrics` 1:1, extended by the `appender` tag and the common tags; the names carry the appender's prefix `kafka.appender.circuitbreaker.` instead of `resilience4j.circuitbreaker.`, so they never mix with the application's own breakers and a dashboard built for the official binder needs only the prefix changed:

| Metric                                         | Tags                        | Type                                                         |
| ---------------------------------------------- | --------------------------- | ------------------------------------------------------------ |
| `kafka.appender.circuitbreaker.state`          | `appender`, `name`, `state` | Gauge: 1 if the breaker is in this state, otherwise 0        |
| `kafka.appender.circuitbreaker.calls`          | `appender`, `name`, `kind`  | Timer per call outcome (`successful`, `failed`, `ignored`)   |
| `kafka.appender.circuitbreaker.not.permitted.calls` | `appender`, `name`, `kind` | Counter (`kind=not_permitted`): calls rejected by the open breaker |
| `kafka.appender.circuitbreaker.buffered.calls` | `appender`, `name`, `kind`  | Gauge: currently in the sliding window                       |
| `kafka.appender.circuitbreaker.slow.calls`     | `appender`, `name`, `kind`  | Gauge: slow calls in the sliding window                      |
| `kafka.appender.circuitbreaker.failure.rate`   | `appender`, `name`          | Gauge: current failure rate in percent                       |
| `kafka.appender.circuitbreaker.slow.call.rate` | `appender`, `name`          | Gauge: current slow-call share in percent                    |

**`name` values** correspond to the active topic classes:

- `kafka-appender-audit`
- `kafka-appender-functional`
- `kafka-appender-technical`
- `kafka-appender-performance`

## Kafka producer bridge (when `KafkaClientMetrics` is on the classpath)

Activated automatically via reflection when `io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics` is available. One binding per active TopicClass. The binder's own names (`kafka.producer.*`, `kafka.producer.node.*`, `kafka.producer.topic.*`) are published under the appender's prefix - `kafka.X` becomes `kafka.appender.X` - so they never mix with the application's own Kafka clients:

| Metric                                           | Examples                                  |
| ------------------------------------------------ | ----------------------------------------- |
| `kafka.appender.producer.record.send.total`      | Records the producer accepted             |
| `kafka.appender.producer.record.error.total`     | Records that failed                       |
| `kafka.appender.producer.record.size.avg`        | Average record size                       |
| `kafka.appender.producer.batch.size.avg`         | Average batch size                        |
| `kafka.appender.producer.request.latency.avg`    | Average request latency to brokers        |
| `kafka.appender.producer.outgoing.byte.rate`     | Bytes/second to the cluster               |
| `kafka.appender.producer.buffer.available.bytes` | Free buffer bytes                         |
| …                                                | (~40 producer-internal metrics in total)  |

All carry the `topic.class` tag to disambiguate between the per-class producers, plus the `appender` tag, the binder's own tags (`client.id`, `kafka.version`, ...) and the common tags.

**How the renaming works.** The binder registers into a private registry
that nothing exports; for every meter it adds there, the appender
registers a reading counterpart under the new name in your registry, and
removes it when the binder removes the original or the appender stops.
The counterparts are ordinary meters of your registry, so composite
registries (Spring Boot's included), your `MeterFilter`s and common tags
apply to them as to any other meter - filter on the new names. No filter
is added to your registry.

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

# Fallback queue saturation as a ratio (both series exist only with a
# fallback appender configured; see "Gauges")
kafka_appender_fallback_queue_size
  / kafka_appender_fallback_queue_capacity

# Actual data loss (= dropped events)
rate(kafka_appender_fallback_dropped_total[5m])

# Circuit breaker state
kafka_appender_circuitbreaker_state
```

## Key alert thresholds

| Condition                                                    | Meaning                                                      | Severity                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------- |
| `kafka_appender_fallback_dropped_total > 0`                  | Actual data loss in the fallback path (queue overflow, failing fallback appender, dead worker, shutdown remainder) | Critical                        |
| `kafka_appender_fallback_queue_size / capacity > 0.8` for 5 min | Fallback appender is slower than the event rate              | Warning                         |
| `kafka_appender_circuitbreaker_state{state="open"} == 1`     | Cluster loss for this topic class                            | Critical (for AUDIT/FUNCTIONAL) |
| `rate(kafka_appender_events_fallback{reason="send.error"}[1m]) > 0` for 10 min | Persistent send errors that were not filtered out as client errors | Warning                         |
| `rate(kafka_appender_events_fallback{reason="encoder.error"}[5m]) > 0` | Hot-path exceptions in the encoder/routing                   | Warning (code bug)              |

Ready-made Grafana dashboards built on these metrics live next to this page:
[`kafka-appender-dashboard.json`](./kafka-appender-dashboard.json) and
[`kafka-producer-internals-dashboard.json`](./kafka-producer-internals-dashboard.json).

The appender dashboard plots `event.size` (average and max per class) in
its send-latency row. Both use the info gauges. The appender dashboard shows "Fallback events
by destination" in its fallback row - the fallback rate joined with
`kafka_appender_info`, where a destination of `none` (red) is dropped
events that the "Drop rate" panel does not count - and a "Configuration"
row with two tables: the appender configuration (`fallback="none"` and
`debug="true"` highlighted) and the configured topics with `acks` and
`linger.ms`. The tables count the scraped instances per configuration,
so instances that differ, e.g. during a rolling deployment, show up as
separate rows. The producer-internals dashboard has the topics table as
well, filtered by its `topic_class` variable.
