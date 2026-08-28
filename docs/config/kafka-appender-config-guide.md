# KafkaAppender Configuration Guide

Reference for configuring `eu.inqudium.tabellarium.KafkaAppender` — the
Logback appender that ships log events to Kafka through per-topic-class
circuit breakers, with an optional fallback appender for delivery failures.

A working example lives in
[`example-logback-spring.xml`](./example-logback-spring.xml); the emitted
metrics are catalogued in [`metrics-overview.md`](../metrics/metrics-overview.md).

- [1. Minimal configuration](#1-minimal-configuration)
- [2. XML element reference](#2-xml-element-reference)
- [3. Kafka producer properties](#3-kafka-producer-properties)
- [4. Producer property composition](#4-producer-property-composition)
- [5. Topic routing](#5-topic-routing)
- [6. Record metadata: headers and partitioning key](#6-record-metadata-headers-and-partitioning-key)
- [7. Resilience: circuit breaker, throttle, fallback](#7-resilience-circuit-breaker-throttle-fallback)
- [8. Producer lifecycle and shutdown](#8-producer-lifecycle-and-shutdown)
- [9. Metrics integration](#9-metrics-integration)
- [10. Startup validation and failure behavior](#10-startup-validation-and-failure-behavior)
- [11. Wrapping in an AsyncAppender](#11-wrapping-in-an-asyncappender)
- [12. Defaults quick reference](#12-defaults-quick-reference)
- [Appendix A: Topic classes (prepared, not yet active)](#appendix-a-topic-classes-prepared-not-yet-active)

---

## 1. Minimal configuration

The smallest configuration that starts successfully needs an encoder, the
Kafka bootstrap servers, a default topic, and the three identity fields:

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
</appender>
```

With only `<defaultTopic>` configured, every event is routed to that one
topic and classified as `TECHNICAL`, so exactly **one** Kafka producer is
instantiated. Without a fallback appender, events that cannot be delivered to
Kafka are silently dropped (see [§7](#7-resilience-circuit-breaker-throttle-fallback)).

---

## 2. XML element reference

All elements below are direct children of the
`<appender class="eu.inqudium.tabellarium.KafkaAppender">` element.
Joran binds each to a setter of the matching name on the appender.

| Element                     | Required | Type / binding                         | Notes |
| --------------------------- | :------: | -------------------------------------- | ----- |
| `<encoder>`                 |   yes    | `Encoder<ILoggingEvent>`               | Standard Logback encoder. `LogstashEncoder` recommended for JSON. Started by the appender at `start()`. |
| `<kafkaProducerProperties>` |   yes¹   | raw multi-line text                    | `.properties`-style Kafka producer config. See [§3](#3-kafka-producer-properties). |
| `<topicMapping>`            |   yes    | nested `TopicMappingConfig`            | Contains `<defaultTopic>`. See [§5](#5-topic-routing). |
| `<environment>`             |   yes    | `String` (trimmed, non-blank)          | Deployment environment (`prod`, `staging`, …). Emitted as the `meta.environment` header. |
| `<component>`               |   yes    | `String` (trimmed, non-blank)          | Service component id (e.g. `spring.application.name`). Emitted as the `meta.component` header. |
| `<cmdbId>`                  |   yes    | `String` (trimmed, non-blank)          | CMDB identifier of the deploying instance. Emitted as the `meta.cmdbId` header. |
| `<debug>`                   |    no    | `Boolean` (default `false`)            | Startup diagnostics only — **no per-event effect**. See below. |
| `<appender-ref ref="…"/>`   |    no    | fallback `Appender<ILoggingEvent>`     | Single fallback slot; first registration wins. See [§7](#7-resilience-circuit-breaker-throttle-fallback). |

¹ `<kafkaProducerProperties>` may technically be omitted, but a producer
without `bootstrap.servers` fails at Kafka client construction, which aborts
`start()`. In practice it is required.

`<environment>`, `<component>`, and `<cmdbId>` are trimmed on assignment and
validated as non-blank at `start()`. Blank values abort startup with an
`addError` (see [§10](#10-startup-validation-and-failure-behavior)).

### The `<debug>` flag

`<debug>true</debug>` affects only **startup diagnostics**: when enabled,
the appender emits the active topic classes, the fallback configuration,
and any mandatory-override conflicts to Logback's status manager. It has
no per-event effect. Leave it unset or `false` in new deployments.

---

## 3. Kafka producer properties

`<kafkaProducerProperties>` carries the raw Kafka producer configuration as
`.properties`-style text, one `key=value` per line. Helm/Spring placeholder
substitution happens **before** the appender parses the text.

```xml
<kafkaProducerProperties>
    bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:-kafka.example.com:9092}
    security.protocol=SSL
    ssl.keystore.location=/cert/identity.pkcs12
    ssl.keystore.password=${KAFKA_KEYSTORE_PASSWORD}
    ssl.keystore.type=PKCS12
    ssl.truststore.location=/configs/http-trust.jks
    ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD}
    ssl.truststore.type=JKS
</kafkaProducerProperties>
```

Parsing is delegated to `java.util.Properties.load`, so the full
`.properties` specification applies:

- **Leading whitespace** (XML indentation) and **blank lines** are ignored.
- Lines starting with `#` or `!` are **comments** and are skipped.
- The first `=` or `:` is the key/value separator; later occurrences belong
  to the value. This is required for SASL JAAS config such as
  `sasl.jaas.config=…required username="…" password="…";`.
- **Multi-line continuations** with a trailing backslash `\` are supported —
  the usual layout for long JAAS blocks.
- Unicode escapes (`\uXXXX`) and standard escapes (`\n`, `\t`, `\\`) are
  decoded. A malformed Unicode escape raises an `IllegalArgumentException`
  identifying the `<kafkaProducerProperties>` element.
- Empty values are accepted (`client.id=` yields the empty string).
- **Trailing whitespace** on values is trimmed (it is almost always an
  accidental XML-indentation artefact); keys cannot contain whitespace.

The returned map's iteration order is **unspecified** (`Properties` is backed
by a `Hashtable`); Kafka's `ProducerConfig` is order-insensitive, so this
does not matter.

### Serializers are forced

The key and value serializers are **always** set to `ByteArraySerializer`,
regardless of any `key.serializer` / `value.serializer` you supply. The
appender's wire format is bytes (the encoder's output); a different
serializer would raise a `ClassCastException` on every record in the Kafka
sender thread. Do not set the serializer properties — they are silently
overridden.

---

## 4. Producer property composition

Today the appender runs a **single Kafka producer**: every event routes to
`<defaultTopic>` and is handled by that one producer ([§5](#5-topic-routing)).
Its configuration is composed from your base properties plus a set of built-in
defaults.

> The appender is internally built around a **four-class model** (AUDIT,
> FUNCTIONAL, TECHNICAL, PERFORMANCE), each class with its own producer tuning
> and circuit breaker. That model is fully implemented but **not yet reachable
> through configuration** — today only the `TECHNICAL` class is ever active.
> This section describes the composition as it behaves today; see
> [Appendix A](#appendix-a-topic-classes-prepared-not-yet-active) for the full
> class model.

### Composition

The producer properties are assembled in two layers, then the serializers are
forced:

1. **Base properties** — everything from `<kafkaProducerProperties>`
   ([§3](#3-kafka-producer-properties)).
2. **Default overrides** — a set of built-in defaults applied with
   `putIfAbsent`: your value wins wherever you set the same key; a default
   only fills a gap you left.
3. **Forced serializers** — `key.serializer` and `value.serializer` are set
   to `ByteArraySerializer` unconditionally ([§3](#3-kafka-producer-properties)).

> A generic third merge layer — *mandatory* overrides that win even over an
> explicit value you set — exists in the appender, but it is **empty for the
> active `TECHNICAL` class**, so it changes nothing today. It only carries
> enforced values for the AUDIT/FUNCTIONAL classes of the prepared model; see
> [Appendix A](#appendix-a-topic-classes-prepared-not-yet-active).

### Default overrides

The built-in defaults filled in for the active `TECHNICAL` producer
(`putIfAbsent` — applied only when you did not set the property yourself):

| Property           | Default |
| ------------------ | ------- |
| `acks`             | `1`     |
| `linger.ms`        | `50`    |
| `max.block.ms`     | `500`   |
| `batch.size`       | `32768` |
| `compression.type` | `lz4`   |
| `client.id`        | `tabellarium-<component>-technical` |

The `client.id` default makes the producer attributable on the broker
(connection logs, quotas, `kafka.producer.*` metrics) instead of Kafka's
auto-generated `producer-N`. The `<component>` part is the appender's
`<component>` value with characters outside `[a-zA-Z0-9._-]` replaced by
`-` (they would break JMX registration). Under the prepared four-class
model each class gets its own suffix (`…-audit`, `…-functional`, …), so
producers never collide on JMX names. An explicit `client.id` in
`<kafkaProducerProperties>` wins — note that it is then shared by every
active class.

### Worked example

Given this configuration:

```xml
<kafkaProducerProperties>
    bootstrap.servers=kafka.example.com:9092
    linger.ms=10
    compression.type=zstd
</kafkaProducerProperties>
<topicMapping>
    <defaultTopic>my-application.logs</defaultTopic>
</topicMapping>
```

every event routes to `my-application.logs` and is handled by the single
producer. Its effective properties are your base properties, with the defaults
filling the gaps and the serializers forced:

| Property             | Source                         | Effective value          |
| -------------------- | ------------------------------ | ------------------------ |
| `bootstrap.servers`  | yours                          | `kafka.example.com:9092` |
| `linger.ms`          | **yours wins** over default 50 | `10`                     |
| `compression.type`   | **yours wins** over default lz4| `zstd`                   |
| `acks`               | default (you set nothing)      | `1`                      |
| `max.block.ms`       | default (you set nothing)      | `500`                    |
| `batch.size`         | default (you set nothing)      | `32768`                  |
| `client.id`          | default (you set nothing)      | `tabellarium-<component>-technical` |
| `key.serializer`     | forced                         | `ByteArraySerializer`    |
| `value.serializer`   | forced                         | `ByteArraySerializer`    |

Nothing you set is ever overruled today — the active `TECHNICAL` class has no
mandatory overrides. (Under the prepared `AUDIT` class the same `acks=1` would
be forced to `acks=all`; see
[Appendix A](#appendix-a-topic-classes-prepared-not-yet-active).)

---

## 5. Topic routing

Routing is configured through `<topicMapping>`, which today accepts a
**single** child element, `<defaultTopic>`:

```xml
<topicMapping>
    <defaultTopic>ichp-de.customerproducts.out</defaultTopic>
</topicMapping>
```

`<defaultTopic>` is trimmed on assignment and validated at `start()`: it must
be non-blank and match the Kafka-permitted topic-name character set
`[a-zA-Z0-9._-]+` (spaces and other characters are rejected). A violation
aborts `start()`.

**Every event resolves to `<defaultTopic>`.** There is no other routing lever
today:

- a `Marker` on a log statement has **no effect** — the appender's marker map
  is empty, so no marker can ever match;
- there is no way to route to a second topic, and no way to select a topic
  class — all events go to the default topic and are handled by the single
  (`TECHNICAL`) producer.

A marker-based, multi-topic, multi-class routing model is fully implemented in
the code but **not yet exposed through XML**. See
[Appendix A](#appendix-a-topic-classes-prepared-not-yet-active) for that model
and its planned configuration shape.

---

## 6. Record metadata: headers and partitioning key

Every Kafka record carries a fixed set of **headers**, encoded once as UTF-8
at startup and shared across all events (zero per-event allocation):

| Header               | Value source                          |
| -------------------- | ------------------------------------- |
| `meta.component`     | `<component>`                         |
| `meta.cmdbId`        | `<cmdbId>`                            |
| `meta.environment`   | `<environment>`                       |
| `meta.agent.name`    | fixed: `logback-kafka-appender`       |
| `meta.agent.version` | fixed: `1.0.0`                        |

The **partitioning key** (the Kafka record key) is derived per event. The
default extractor reads the MDC entry `traceId` and uses it if non-blank;
otherwise the key is `null` and Kafka distributes the record via its
configured partitioner (sticky-random by default). Using the trace id as the
key keeps all records of one trace on the same partition, preserving their
relative order. A custom extraction strategy (session id, account id, …) is
supported at the API level but is not currently exposed through XML.

---

## 7. Resilience: circuit breaker, throttle, fallback

Delivery is guarded by a Resilience4j circuit breaker per *active* topic
class, a half-open probe throttle, and an optional asynchronous fallback
appender. Classes fail **independently** — the breaker is per class by design,
so once the [prepared multi-class model](#appendix-a-topic-classes-prepared-not-yet-active)
is active a stuck audit broker will not throttle technical-log delivery.
(Today only the `TECHNICAL` class is active, so there is a single breaker.)

### Circuit breaker

One breaker is acquired per active class, named `kafka-appender-<class>`
(today just `kafka-appender-technical`; e.g. `kafka-appender-audit` once that
class is active). The default
configuration is tuned for logging traffic:

| Setting                              | Default          |
| ------------------------------------ | ---------------- |
| `failureRateThreshold`               | `50%`            |
| `slidingWindowSize`                  | `20` calls       |
| `minimumNumberOfCalls`               | `10`             |
| `waitDurationInOpenState`            | `30s`            |
| `permittedNumberOfCallsInHalfOpenState` | `10`          |

To override the configuration for a specific class, register a named config
on the `CircuitBreakerRegistry` under `kafka-appender-<class>` before the
appender builds its pipeline.

**Ignored exceptions.** The breaker is an *infrastructure-health* signal
("is Kafka reachable?"), not a payload validator. Deterministic,
payload-dependent errors do **not** count toward the failure rate:
`RecordTooLargeException`, `InvalidTopicException`, `SerializationException`,
`TopicAuthorizationException`. A buggy service suddenly logging 2 MB
stacktraces would otherwise open the breaker and silence *all* logs from that
service even though the cluster is healthy. The individual failed events
still go to the fallback, so no log is lost — only the breaker statistics are
spared. Transient infrastructure errors (`TimeoutException`,
`NetworkException`, `LeaderNotAvailableException`, `NotEnoughReplicasException`)
are **not** ignored — those are exactly what the breaker reacts to.

### Half-open probe throttle

In HALF_OPEN state, Resilience4j admits the probe calls back-to-back; at high
logging volume all probes fire within microseconds and then every further
event is routed to the fallback for the duration of the Kafka round-trip,
even after the cluster has recovered. The throttle spreads probes out to one
per **`halfOpenProbeGap`** (default **5 ms**), so the probes are dispatched
over `N × gap` of wall time. It is transparent in CLOSED and OPEN. Setting
the gap to zero disables it. This is a programmatic setting; it is not
currently exposed through XML.

### Fallback appender

Attach a fallback appender with `<appender-ref ref="…"/>`:

```xml
<appender name="KAFKA_FALLBACK_FILE" class="ch.qos.logback.core.FileAppender">
    <file>/var/log/myapp/kafka-fallback.log</file>
    <encoder>
        <pattern>%d{ISO8601} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
</appender>

<appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
    <!-- … -->
    <appender-ref ref="KAFKA_FALLBACK_FILE"/>
</appender>
```

An event is routed to the fallback when the breaker is open, the throttle
gate denies a probe, or a send fails (synchronously or via callback error).

- **Single slot, first wins.** Only one fallback appender is attached; a
  second `<appender-ref>` is ignored.
- **No fallback ⇒ silent drop.** Leaving it unset is the operator's explicit
  choice that best-effort delivery is acceptable. Configuring one is the way
  to say "loss is unacceptable here". **Strongly recommended in production.**
- **Asynchronous by design.** The fallback is fed through a bounded
  single-consumer queue (`FallbackDispatcher`, capacity **1024**) drained by
  a dedicated daemon thread. This is mandatory, not cosmetic: the Kafka
  `send` callback runs on the single `kafka-producer-network-thread`, and
  calling a potentially-blocking `FileAppender` directly from it would stall
  every in-flight callback. The dispatcher accepts events in O(1) and never
  blocks the caller.
- **Overflow ⇒ drop, counted.** If the fallback is slow enough to fill the
  queue, further events are dropped (never blocked) and counted in the
  `kafka.appender.fallback.dropped` metric. The system is already degraded
  (Kafka delivery is failing); an unbounded queue would grow to OOM.
- **Shutdown.** On `close()` the dispatcher gets up to **5 s** to drain
  (a ~200 ms graceful window, then an interrupt); events still queued after
  the timeout are dropped and counted.

---

## 8. Producer lifecycle and shutdown

Producers are created eagerly at `start()` (one per active class). If any
producer fails to construct, the already-created ones are closed and the
exception is rethrown — the registry is never partially initialized, and
`start()` reports the failure via `addError`.

At `stop()` every producer is closed with a **10 s** default timeout. A
per-producer close failure does not prevent the others from closing (partial
cleanup beats none). The worst-case total close time is `N × 10s` for `N`
active classes — for the typical 1–2 active classes this fits comfortably
inside the Kubernetes default `terminationGracePeriodSeconds: 30`.

### Interaction with `delivery.timeout.ms`

A record in mid-retry at close time is aborted. Kafka's default
`delivery.timeout.ms` is 120 s — longer than the 10 s close timeout — so on a
flaky network a record could be abandoned at shutdown. Operators who need
stronger guarantees (especially for AUDIT topics) should cap the retry window
below the close timeout via `<kafkaProducerProperties>`:

```
delivery.timeout.ms=10000
```

---

## 9. Metrics integration

Metrics are **optional** and off by default: without binding, the appender
uses a zero-allocation no-op and emits nothing. Binding is programmatic —
there is no XML for it.

### Direct binding

Call `bindMeterRegistry` once, after the appender has started (e.g. from a
Spring `@PostConstruct`):

```kotlin
kafkaAppender.bindMeterRegistry(meterRegistry, Tags.of("application", "payment-service"))
```

`commonTags` is optional (default `Tags.empty()`) — the registry's own common
tags are usually enough. Calling this on a stopped appender is a no-op with a
status-manager warning.

### Spring binding

`KafkaAppenderMetricsBinding` discovers every `KafkaAppender` in the
`LoggerContext` (recursing into `AsyncAppender` wrappers) and binds it on
`ContextRefreshedEvent`. It is **not** auto-configuration — import it
explicitly so the appender library never transitively pulls Spring into
projects that do not want it:

```kotlin
@Configuration
class LoggingConfig {
    @Bean
    fun kafkaAppenderMetricsBinding(registry: MeterRegistry) =
        KafkaAppenderMetricsBinding(registry, Tags.of("application", "payment-service"))
}
```

Binding is idempotent (each appender bound once by reference identity).
Pre-Spring log events are not counted.

### Metric inventory

All metrics carry an `appender` tag (the Logback appender name, or `unnamed`).
See [`metrics-overview.md`](../metrics/metrics-overview.md) for the full catalogue.

| Metric                                     | Type    | Extra tags                | Meaning |
| ------------------------------------------ | ------- | ------------------------- | ------- |
| `kafka.appender.events.accepted`           | Counter | `topic.class`             | Events entering `append()` (after class routing). |
| `kafka.appender.events.dispatched`         | Counter | `topic.class`             | Events handed to `producer.send()` (callback outcome not yet known). |
| `kafka.appender.events.fallback`           | Counter | `topic.class`, `reason`   | Events routed to the fallback instead of Kafka. |
| `kafka.appender.send.duration`             | Timer   | `topic.class`, `outcome`  | Wall-clock from `send()` invocation to callback. |
| `kafka.appender.fallback.dropped`          | Counter | —                         | Events dropped by the dispatcher (queue full / shutdown timeout). |
| `kafka.appender.fallback.queue.size`       | Gauge   | —                         | Current dispatcher queue depth (live per scrape). |
| `kafka.appender.fallback.queue.capacity`   | Gauge   | —                         | Fixed queue capacity. |

Tag values: `topic.class` ∈ {`audit`, `functional`, `technical`,
`performance`}; `reason` ∈ {`breaker.open`, `throttle`, `send.error`,
`encoder.error`}; `outcome` ∈ {`success`, `error`}. Worst case ≈ 35 series
per appender instance.

### Optional extra bindings

`bindMeterRegistry` additionally attempts two best-effort bindings, each
gated by a `Class.forName` probe so the appender works without the dependency
on the classpath:

- **Resilience4j circuit-breaker metrics** — needs
  `io.github.resilience4j:resilience4j-micrometer` (binds
  `TaggedCircuitBreakerMetrics` for the per-class breakers).
- **Kafka client metrics** — needs `micrometer-core`'s `KafkaClientMetrics`
  (binds each active producer's native client metrics).

Both are silently skipped when the class is absent.

---

## 10. Startup validation and failure behavior

`start()` validates the configuration and reports every problem via
`addError` to Logback's status manager; a failed validation leaves the
appender **stopped** rather than throwing. Checked conditions:

- `<encoder>` is present.
- `<component>`, `<cmdbId>`, `<environment>` are non-blank.
- Pipeline construction succeeds (`<kafkaProducerProperties>` parses,
  `<defaultTopic>` is a valid Kafka topic name, all producers construct).

Mandatory-override conflicts are logged as warnings but do **not** stop the
appender. In the hot path, an unexpected per-event failure (encoder bug, OOM)
is logged **once** (subsequent occurrences suppressed to prevent log storms)
and the event is routed to the fallback.

Inspect Logback's status output to confirm a clean start — for example add
`<statusListener class="ch.qos.logback.core.status.OnConsoleStatusListener"/>`
to your configuration, or enable `<debug>true</debug>` for the extra
startup diagnostics.

---

## 11. Wrapping in an AsyncAppender

In production, wrap the appender in a Logback `AsyncAppender` to decouple
application threads from Kafka producer back-pressure:

```xml
<appender name="ASYNC_KAFKA" class="ch.qos.logback.classic.AsyncAppender">
    <appender-ref ref="KAFKA"/>
    <queueSize>1024</queueSize>
    <discardingThreshold>0</discardingThreshold>
    <neverBlock>true</neverBlock>
</appender>

<root level="INFO">
    <appender-ref ref="ASYNC_KAFKA"/>
</root>
```

`AsyncAppender` is part of Logback core; this module deliberately does not
provide its own async layer so operators tune queue size and discard policy
to their environment. The metrics binding recurses through the
`AsyncAppender`, so the wrapped `KafkaAppender` is still discovered and bound.

---

## 12. Defaults quick reference

| Setting                                      | Default                          | Where |
| -------------------------------------------- | -------------------------------- | ----- |
| `<debug>`                                    | `false`                          | XML |
| Fallback (`<appender-ref>`)                  | none → silent drop               | XML |
| Forced serializers                           | `ByteArraySerializer` (key+value)| always |
| `client.id` (unless set by operator)         | `tabellarium-<component>-<topicclass>` | code |
| Fallback class for unmapped topics           | `TECHNICAL`                      | code |
| Partitioning key MDC source                  | `traceId`                        | code |
| Circuit breaker: failure-rate threshold      | `50%`                            | code |
| Circuit breaker: sliding window / min calls  | `20` / `10`                      | code |
| Circuit breaker: open-state wait             | `30s`                            | code |
| Circuit breaker: half-open permitted calls   | `10`                             | code |
| Half-open probe gap                          | `5 ms`                           | code |
| Fallback dispatcher queue capacity           | `1024`                           | code |
| Fallback dispatcher shutdown timeout         | `5 s`                            | code |
| Producer close timeout                       | `10 s`                           | code |
| Metrics                                      | off (no-op) until bound          | code |

"code" defaults are not exposed through the XML surface today; they are
listed so operators understand the runtime behavior.

---

## Appendix A: Topic classes (prepared, not yet active)

> **Status — prepared, not reachable through configuration.** Everything in
> this appendix describes machinery that is fully implemented and unit-tested
> in the appender but **inert today**. The XML binding (`TopicMappingConfig`)
> wires the routing with an empty marker map, an empty topic→class map, and a
> fixed `TECHNICAL` fallback, so at runtime only the `TECHNICAL` class is ever
> active (see [§4](#4-producer-property-composition) and
> [§5](#5-topic-routing)). **None of the XML shown in this appendix is
> accepted by the current version.** It documents the intended full model so
> it is understood ahead of the release that activates it.

### A.1 What a topic class is for

A **topic class** is the unit that decides *how carefully a given kind of log
is shipped*. It is not something you attach to a log statement; it is a
property of the **destination topic**, resolved at runtime. Each class buys
two things:

- a **producer configuration** tuned to that class's durability/throughput
  trade-off (the override tables in [A.4](#a4-property-overrides-per-class)),
  and
- its **own circuit breaker and producer**, so classes fail **independently**
  — a stuck audit broker never throttles technical-log delivery, and vice
  versa ([§7](#7-resilience-circuit-breaker-throttle-fallback)).

### A.2 Runtime flow (how a class will be selected)

Once activated, every log event will travel this path on the way to Kafka:

```
log.info(MARKER, "…")
   │
   ▼
encoder.encode(event)                        → payload bytes
   │
   ▼
TopicRouter.route(markers)                   → topic NAME      (e.g. "audit.security")
   │
   ▼
TopicTable.classFor(topicName)               → topic CLASS     (e.g. AUDIT)
   │
   ▼
producer[class].send(record)  via  circuitBreaker[class]
```

The class is never chosen directly. It is derived: **marker → topic name →
topic class → the producer and breaker for that class.** The producer for a
class is built once at startup by merging your base properties with that
class's overrides ([A.4](#a4-property-overrides-per-class)). Only classes that
some topic actually resolves to are *active* — the appender creates a
producer, breaker, and I/O thread only for those, never for dormant classes.
(Today no topic resolves to anything but `TECHNICAL`, which is why it is the
only active class.)

### A.3 The four classes

| Class         | Durability   | Reorder cost | Volume    | Mandatory overrides                    |
| ------------- | ------------ | ------------ | --------- | -------------------------------------- |
| `AUDIT`       | Maximum      | Critical     | Low       | `acks=all`, `enable.idempotence=true`  |
| `FUNCTIONAL`  | Maximum      | High         | Medium    | `acks=all`                             |
| `TECHNICAL`   | Best-effort  | Acceptable   | High      | *(none)*                               |
| `PERFORMANCE` | Best-effort  | Tolerated    | Very high | *(none)*                               |

Each class carries two sets of producer-property overrides:

- **Mandatory overrides** are applied unconditionally and **win over any
  conflicting value you set**. They encode non-negotiable compliance
  requirements (`acks=all` for audit trails in a regulated banking
  environment). A conflict is not silently ignored — it is recorded and
  logged to the status manager at startup ([A.4](#a4-property-overrides-per-class)).
- **Default overrides** are applied only when you did **not** set the property
  yourself (`putIfAbsent` semantics). They are reasonable defaults you remain
  free to tune.

### A.4 Property overrides per class

**Default overrides** (applied only when you did not set the property):

| Property             | AUDIT | FUNCTIONAL | TECHNICAL | PERFORMANCE |
| -------------------- | :---: | :--------: | :-------: | :---------: |
| `acks`               |   —¹  |     —¹     |    `1`    |     `1`     |
| `enable.idempotence` |   —¹  |      —     |     —     |      —      |
| `linger.ms`          |  `50` |    `50`    |   `50`    |    `100`    |
| `max.block.ms`       | `500` |   `500`    |   `500`   |    `200`    |
| `batch.size`         |   —   |      —     |  `32768`  |   `65536`   |
| `compression.type`   | `lz4` |   `lz4`    |   `lz4`   |    `lz4`    |
| `retries`            |  `10` |      —     |     —     |      —      |

¹ For AUDIT and FUNCTIONAL, `acks=all` (and, for AUDIT, `enable.idempotence=true`)
is a **mandatory** override, not a default — you cannot weaken it.

**Merge order** — for each active class the final producer properties are
composed in three layers:

1. **Base properties** — everything from `<kafkaProducerProperties>`.
2. **Default overrides** — applied via `putIfAbsent`; your value wins where
   both are present.
3. **Mandatory overrides** — applied unconditionally; the enforced value wins,
   and any conflict is recorded.

(Today only layers 1 and 2 have any effect, because the sole active class,
`TECHNICAL`, has no mandatory overrides — see [§4](#4-producer-property-composition).)

**Mandatory-override violations** — when a value you set conflicts with a
mandatory override, the appender logs a warning to Logback's status manager at
`start()`, for example:

```
Mandatory override applied for AUDIT: acks forced from '1' to 'all'.
This is a compliance requirement; see TopicClass.AUDIT for rationale.
```

The record is still delivered with the enforced value; the warning exists so
operators notice that their configuration intent was overruled.

### A.5 Marker-based routing (planned XML)

`TopicRouter` and `TopicTable` already support marker-to-topic mappings and
per-topic class assignments; the XML binding for them is intentionally not
implemented yet (YAGNI — every Joran setter that exists must be tested).

**Resolution rules** (`TopicRouter`, once its marker map is populated):

1. An event with **no markers** resolves to `<defaultTopic>`.
2. Otherwise each SLF4J marker is checked in order: a direct name match wins;
   failing that, the marker's single-level hierarchical references are checked
   (references of references are not followed, which prevents cycles). The
   first match wins.
3. No match → `<defaultTopic>`.

The resolved topic name is then mapped to a topic class by `TopicTable`;
unmapped topics fall back to `TECHNICAL` (the most neutral class: no
compliance mandate, tolerable performance defaults), so a routing typo never
crashes the log pipeline.

**Planned XML shape** — a nested element per class, each holding
`marker → topic` entries:

```xml
<topicMapping>
    <defaultTopic>default.topic</defaultTopic>
    <audit>
        <entry marker="SECURITY">audit.security</entry>
        <entry marker="MONEY">audit.transactions</entry>
    </audit>
    <technical>
        <entry marker="DEBUG">tech.debug</entry>
    </technical>
</topicMapping>
```

Until that lands, configuration is limited to a single default topic and the
single `TECHNICAL` producer.
