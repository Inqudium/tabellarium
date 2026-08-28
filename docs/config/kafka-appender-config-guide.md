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
- [Appendix A: The four-class topic model](#appendix-a-the-four-class-topic-model)

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
| `<encoder>`                 |   yes    | `Encoder<ILoggingEvent>`               | Standard Logback encoder. `LogstashEncoder` recommended — for parseability **and** for log-forging resistance, see below. Started by the appender at `start()`. |
| `<kafkaProducerProperties>` |   yes¹   | raw multi-line text                    | `.properties`-style Kafka producer config. See [§3](#3-kafka-producer-properties). |
| `<topicMapping>`            |   yes    | nested `TopicMappingConfig`            | Contains `<defaultTopic>`, an optional `<defaultTopicClass>`, and any number of `<mapping>` elements. See [§5](#5-topic-routing). |
| `<environment>`             |   yes    | `String` (trimmed, non-blank)          | Deployment environment (`prod`, `staging`, …). Emitted as the `meta.environment` header. |
| `<component>`               |   yes    | `String` (trimmed, non-blank)          | Service component id (e.g. `spring.application.name`). Emitted as the `meta.component` header. |
| `<cmdbId>`                  |   yes    | `String` (trimmed, non-blank)          | CMDB identifier of the deploying instance. Emitted as the `meta.cmdbId` header. |
| `<debug>`                   |    no    | `Boolean` (default `false`)            | Startup diagnostics only — **no per-event effect**. See below. |
| `<sendQueueCapacity>`       |    no    | `Int` (default `1024`, must be > 0)    | Capacity of each per-topic-class send queue — the bounded hand-off between logging threads and the worker that performs `producer.send`. Overflow diverts to the fallback (reason `queue.full`) instead of blocking. |
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
any mandatory-override conflicts, and — per active class — the
**generated producer settings**: the values the appender put on top of
your own configuration, i.e. the derived `client.id` and the class's
default/mandatory overrides that actually took effect, for example:

```
Generated producer settings [technical]: acks=1, batch.size=32768,
client.id=tabellarium-my-service-technical, compression.type=lz4,
linger.ms=50, max.block.ms=500
```

Your own values are deliberately **not repeated** (a value you set
yourself — including any credential — never appears in this output; the
line is a diff against your base properties). The flag additionally
unlocks the full cause and stack trace of a pipeline-construction failure,
which is withheld by default
([§10](#10-startup-validation-and-failure-behavior)). It has no per-event
effect. Leave it unset or `false` in new deployments, and enable it
temporarily when diagnosing a startup problem.

### Why the encoder choice is a security decision

The appender is encoder-agnostic: it ships the encoder's bytes verbatim as
the record value and neither inspects nor neutralizes them. That makes the
encoder the component which decides whether attacker-influenced message
text can break out of its field:

- With a **JSON encoder** (`LogstashEncoder`), message text, MDC values and
  stack traces are JSON-escaped. A newline or a fabricated `"level":"INFO"`
  inside a user-supplied string stays a *value* — it cannot become a
  separate record or a forged field.
- With a **line-oriented encoder** (`PatternLayoutEncoder`), a CRLF
  sequence in attacker-influenced text produces what looks downstream like
  additional log records (CWE-117, log forging), which poisons log
  analysis, dashboards and alerting rules built on that topic.

If a line-oriented encoder is unavoidable, neutralize the newlines at the
encoder (Logback's `replace(…)` conversion word) rather than relying on
callers to sanitize every log statement.

`LogstashEncoder` is a **recommendation, not a dependency**: the appender
accepts any `Encoder<ILoggingEvent>` and never inspects the bytes it
produces, and `logstash-logback-encoder` is declared `optional`, so it
only reaches your classpath if you ask for it. Any JSON-producing
encoder satisfies the property described above; switching to another one
is a change to the `<encoder>` element and nothing else.

### Placeholder resolution (where the values come from)

The appender performs **no placeholder substitution of its own** — Joran
hands it fully resolved strings. Every `${…}` in the examples of this guide
(`${STAGE}`, `${ARTIFACT_ID}`, keystore passwords in
`<kafkaProducerProperties>`) is resolved *before* the appender sees the
value, by one of four mechanisms:

1. **Logback's own variable substitution** (always active). While parsing
   the XML, Joran resolves `${NAME}` in this order: local
   `<property>`/`<variable>` definitions → Logback context properties →
   Java system properties (`-DSTAGE=prod`) → **OS environment variables**.
   `<environment>${STAGE}</environment>` therefore works out of the box
   when the container sets an env var `STAGE` (in Kubernetes, typically
   from the Deployment manifest). Defaults use the `:-` syntax:
   `${STAGE:-dev}`.

2. **Spring properties via `<springProperty>`** (only in
   `logback-spring.xml`). `spring.application.name` is *not* a system
   property — a literal `${spring.application.name}` would NOT be resolved
   by Logback alone. Bridge it from the Spring `Environment`:

   ```xml
   <springProperty scope="context" name="appName" source="spring.application.name"/>
   ...
   <component>${appName}</component>
   ```

   This pulls the value from `application.yml`/config server. (Recent
   Spring Boot versions also expose the application name as a predefined
   logging property, but `<springProperty>` is the robust,
   version-independent mechanism.)

3. **Deployment templating.** Helm/Kustomize render values into the file
   at deploy time (`{{ .Values.stage }}`), before Logback ever parses it.
   This is the usual channel for `<cmdbId>` and for credentials in
   `<kafkaProducerProperties>`.

4. **Build-time resource filtering.** Placeholders like `${ARTIFACT_ID}`
   can be filled by Maven resource filtering when the logback XML lives in
   `src/main/resources`. Note that under the Spring Boot parent, filtering
   uses `@…@` delimiters (`@project.artifactId@`), not `${…}` — the
   default delimiters are disabled precisely so Spring/Logback
   placeholders survive the build.

**Safety net and one caveat:** if substitution yields an empty string
(e.g. a forgotten env var resolving to nothing), the non-blank validation
of `<component>`/`<cmdbId>`/`<environment>` aborts startup with a named
error ([§10](#10-startup-validation-and-failure-behavior)). If Logback
cannot resolve a placeholder *at all*, however, the literal text
(`${STAGE}`) is kept as the value — that is not blank, passes validation,
and surfaces only later as an odd `meta.environment` header on every
record. When a header value looks like an unresolved placeholder in your
log sink, check the substitution chain above.

---

## 3. Kafka producer properties

`<kafkaProducerProperties>` carries the raw Kafka producer configuration as
`.properties`-style text, one `key=value` per line. Helm/Spring placeholder
substitution happens **before** the appender parses the text — see
[Placeholder resolution](#placeholder-resolution-where-the-values-come-from)
for the resolution chain.

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

The appender runs **one Kafka producer per active topic class**. With the
minimal configuration (only `<defaultTopic>`) that is a single producer for
the `TECHNICAL` class; every `<mapping>` that names another class activates
an additional producer ([§5](#5-topic-routing)). Each producer's
configuration is composed from your base properties plus that class's
built-in overrides.

> The **four-class model** (AUDIT, FUNCTIONAL, TECHNICAL, PERFORMANCE) gives
> each class its own producer tuning and circuit breaker, and is activated
> per class through `<mapping>` elements. This section describes the
> composition for the `TECHNICAL` class of the minimal configuration; see
> [Appendix A](#appendix-a-the-four-class-topic-model) for the full
> class model and all per-class values.

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
> explicit value you set — is **empty for the `TECHNICAL` class**, so it
> changes nothing in the minimal configuration. It carries the enforced
> values of the AUDIT/FUNCTIONAL classes once a `<mapping>` activates them;
> see [Appendix A](#appendix-a-the-four-class-topic-model).

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
`-` (they would break JMX registration). Each active class gets its own
suffix (`…-audit`, `…-functional`, …), so
producers never collide on JMX names. An explicit `client.id` in
`<kafkaProducerProperties>` wins — note that it is then shared by every
active class.

The client.id also powers the **self-logging guard**: the Kafka client
names its producer network thread
`kafka-producer-network-thread | <client.id>`, and the appender ignores
any log event whose thread name matches exactly that scheme for one of
its producers' client.ids — otherwise the producer's own logging would
be routed back through the producer, a feedback loop that amplifies
exactly during broker trouble. Ignored means ignored entirely: no
metrics, no fallback. The match is anchored to the full thread-naming
scheme, so an operator-supplied short `client.id` can never suppress
events from unrelated application threads whose names merely contain
it; a blank operator-supplied `client.id` is additionally excluded
from the guard.

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

Nothing you set is overruled for the `TECHNICAL` class — it has no mandatory
overrides. (For a topic mapped to `AUDIT`, the same `acks=1` is forced to
`acks=all`, with a startup warning; see
[Appendix A](#appendix-a-the-four-class-topic-model).)

---

## 5. Topic routing

Routing is configured through `<topicMapping>`: a mandatory
`<defaultTopic>`, an optional `<defaultTopicClass>`, plus any number of
`<mapping>` elements, each routing one SLF4J marker to a topic and
assigning that topic its
[topic class](#appendix-a-the-four-class-topic-model):

```xml
<topicMapping>
    <defaultTopic>ichp-de.customerproducts.out</defaultTopic>
    <defaultTopicClass>FUNCTIONAL</defaultTopicClass>  <!-- optional; default TECHNICAL -->
    <mapping>
        <marker>SECURITY</marker>
        <topic>audit.security</topic>
        <topicClass>AUDIT</topicClass>
    </mapping>
    <mapping>
        <marker>METRICS</marker>
        <topic>perf.metrics</topic>
        <topicClass>PERFORMANCE</topicClass>
    </mapping>
</topicMapping>
```

**Resolution rules** per event:

1. An event with **no markers** resolves to `<defaultTopic>`.
2. Otherwise each SLF4J marker is checked in order: a direct name match
   (exact, case-sensitive) wins; failing that, the marker's single-level
   hierarchical references are checked (references of references are not
   followed, which prevents cycles). The first match wins.
3. No match → `<defaultTopic>`.

The resolved topic is then classified: topics named in a `<mapping>` carry
their `<topicClass>`; every other topic — including `<defaultTopic>` — falls
back to the class named by `<defaultTopicClass>`, which defaults to
`TECHNICAL` (the most neutral class: no compliance mandate, tolerable
performance defaults), so a routing typo never crashes the log pipeline.
Set `<defaultTopicClass>` when the default stream itself carries a
compliance grade — e.g. `AUDIT` applies that class's producer tuning and
mandatory overrides (`acks=all`, idempotence) to the default topic without
any marker mapping, and no dormant `TECHNICAL` producer is created.

**Validation at `start()`** — all of the following abort startup with a
named error instead of surfacing per event:

- blank or Kafka-invalid topic names (character set `[a-zA-Z0-9._-]+`, not
  `.` or `..`, at most 249 characters) for `<defaultTopic>` and every
  `<mapping>`;
- a blank `<marker>`;
- a `<topicClass>` or `<defaultTopicClass>` that is not one of `AUDIT`,
  `FUNCTIONAL`, `TECHNICAL`, `PERFORMANCE` (case-insensitive);
- the same marker mapped twice;
- the same topic assigned two different classes (several markers may share
  one topic *with the same class*) — including a `<mapping>` that names
  `<defaultTopic>` with a class conflicting with `<defaultTopicClass>`.

All values are whitespace-trimmed on assignment. Multiple mappings to
distinct classes activate one producer (and circuit breaker) per class —
see [§4](#4-producer-property-composition) and
[Appendix A](#appendix-a-the-four-class-topic-model).

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
| `meta.agent.version` | the library version, read from a build-time-filtered resource (`unknown` if missing) |

The three `<…>` values are whatever placeholder resolution produced at
startup — see
[Placeholder resolution](#placeholder-resolution-where-the-values-come-from)
for where they come from and for the unresolved-placeholder caveat.

The **partitioning key** (the Kafka record key) is derived per event. The
default extractor reads the MDC entry `traceId` and uses it if non-blank
**and at most 128 characters long**; otherwise the key is `null` and Kafka
distributes the record via its configured partitioner (sticky-random by
default). Using the trace id as the key keeps all records of one trace on
the same partition, preserving their relative order. A custom extraction
strategy (session id, account id, …) is supported at the API level but is
not currently exposed through XML.

### Why the key is length-bounded

The key comes from the log event, and applications routinely bridge an
inbound request header into the MDC — so the value can be
attacker-influenced. Without a bound, an oversized header would inflate
every record past `max.request.size`; the resulting
`RecordTooLargeException` is deliberately **ignored** by the circuit
breaker (it is a payload problem, not a broker-health problem —
[§7](#7-resilience-circuit-breaker-throttle-fallback)), so the breaker
would never open and every such event would be routed to the fallback
appender indefinitely.

An over-long key is therefore dropped entirely rather than truncated: a
truncated prefix would still be attacker-chosen and would still steer the
record onto a partition of their choosing. 128 characters is far above
every established trace-id format (W3C `traceparent` and B3 trace ids are
32 hex characters, a UUID is 36). The bound is applied centrally, so it
covers custom extractors too.

Note what the bound does **not** do: partition selection is key-driven by
design, so an application that bridges unvalidated inbound values into the
MDC can still influence distribution within the bound. Bounding the length
is this appender's part; not trusting inbound headers is the
application's.

---

## 7. Resilience: circuit breaker, throttle, fallback

Delivery is guarded by a Resilience4j circuit breaker per *active* topic
class, a half-open probe throttle, and an optional asynchronous fallback
appender. Classes fail **independently** — the breaker is per class by
design, so a stuck audit broker does not throttle technical-log delivery.
(With the minimal configuration only the `TECHNICAL` class is active, so
there is a single breaker.)

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

At `stop()` the send dispatchers are closed first, **in parallel**
within one shared **~2 s** budget: each drains its queue by still
sending through the open producers; whatever cannot be sent in time
diverts to the fallback appender with metric reason `shutdown`. Then every producer is closed **in parallel**
within a **10 s** overall budget. A per-producer close failure does not
prevent the others from closing (partial cleanup beats none); failures
are aggregated and surfaced as a status warning. The parallel close
keeps the total well inside the Kubernetes default
`terminationGracePeriodSeconds: 30` regardless of how many classes are
active.

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
| `kafka.appender.events.fallback`           | Counter | `topic.class`, `reason`   | Events diverted from Kafka delivery (to the fallback if configured, otherwise dropped). |
| `kafka.appender.send.duration`             | Timer   | `topic.class`, `outcome`  | Wall-clock from `send()` invocation to callback. |
| `kafka.appender.fallback.dropped`          | Counter | —                         | Events dropped by the dispatcher (queue full / shutdown timeout). |
| `kafka.appender.fallback.queue.size`       | Gauge   | —                         | Current dispatcher queue depth (live per scrape). |
| `kafka.appender.fallback.queue.capacity`   | Gauge   | —                         | Fixed queue capacity. |
| `kafka.appender.send.queue.size`           | Gauge   | `topic.class`             | Current send-dispatcher queue depth for the class. |
| `kafka.appender.send.queue.capacity`       | Gauge   | `topic.class`             | Fixed send queue capacity. |

Tag values: `topic.class` ∈ {`audit`, `functional`, `technical`,
`performance`}; `reason` ∈ {`breaker.open`, `throttle`, `send.error`,
`encoder.error`, `queue.full`, `shutdown`}; `outcome` ∈ {`success`,
`error`}. Worst case ≈ 51 series per appender instance.

### Additional bindings

`bindMeterRegistry` additionally attempts two best-effort bindings:

- **Resilience4j circuit-breaker metrics** — registered by the appender's
  own binder (no `resilience4j-micrometer` needed); mirrors
  `TaggedCircuitBreakerMetrics`' metric names for the per-class breakers
  and adds the `appender` tag, so several appender instances on one
  registry never collide.
- **Kafka client metrics** — needs `micrometer-core`'s `KafkaClientMetrics`
  (binds each active producer's native client metrics, tagged with
  `topic.class` and `appender`); gated by a `Class.forName` probe and
  silently skipped when the class is absent.

---

## 10. Startup validation and failure behavior

`start()` validates the configuration and reports every problem via
`addError` to Logback's status manager; a failed validation leaves the
appender **stopped** rather than throwing. Checked conditions:

- `<encoder>` is present.
- `<component>`, `<cmdbId>`, `<environment>` are non-blank.
- Pipeline construction succeeds (`<kafkaProducerProperties>` parses,
  `<defaultTopic>` is a valid Kafka topic name, all producers construct).

Two conditions are reported as **warnings** and do *not* stop the appender:

- **Mandatory-override conflicts** — a value of yours was overruled for a
  compliance-graded class ([§4](#4-producer-property-composition)).
- **Cleartext transport for a graded class** — a topic classified `AUDIT`
  or `FUNCTIONAL` is served by a producer whose `security.protocol` is
  unset or `PLAINTEXT`. Such records are enforced to be durable but travel
  unencrypted and unauthenticated, so anyone on the network path can read
  or tamper with them. The appender does not enforce TLS (it has no way to
  supply certificates, and transport is the operator's decision) — it
  signals, so the gap is a choice rather than an oversight. Configure
  `security.protocol=SSL` or `SASL_SSL` in `<kafkaProducerProperties>`
  unless the transport is secured below the application. Classes without
  compliance mandates (`TECHNICAL`, `PERFORMANCE`) do not trigger this
  warning.

In the hot path, an unexpected per-event failure (encoder bug, OOM) is
logged **once** (subsequent occurrences suppressed to prevent log storms)
and the event is routed to the fallback.

**Pipeline-construction failures report the exception type only.** The
message and stack trace of a failing producer construction are authored by
the Kafka client from credential-bearing configuration, so by default they
are withheld from the status output; set `<debug>true</debug>` to include
the full cause when diagnosing a startup problem.

Inspect Logback's status output to confirm a clean start — for example add
`<statusListener class="ch.qos.logback.core.status.OnConsoleStatusListener"/>`
to your configuration, or enable `<debug>true</debug>` for the extra
startup diagnostics.

---

## 11. Wrapping in an AsyncAppender

**Not needed.** The appender ships its own asynchronous layer: each
topic class has a bounded send queue and a dedicated worker thread that
performs `producer.send`, so the logging caller only routes, encodes and
enqueues in O(1) — see `<sendQueueCapacity>` in [§2](#2-xml-element-reference)
and the discussion in the README. Attach the `KafkaAppender` directly:

```xml
<root level="INFO">
    <appender-ref ref="KAFKA"/>
</root>
```

Wrapping it in a `ch.qos.logback.classic.AsyncAppender` anyway adds an
extra thread, an extra queue and — with the default
`discardingThreshold` — silent INFO/DEBUG loss that bypasses the
fallback appender and its loss accounting. If you do wrap it (e.g. for
organisational conventions), the metrics binding still recurses through
the `AsyncAppender`, so the wrapped `KafkaAppender` is discovered and
bound.

---

## 12. Defaults quick reference

| Setting                                      | Default                          | Where |
| -------------------------------------------- | -------------------------------- | ----- |
| `<debug>`                                    | `false`                          | XML |
| Fallback (`<appender-ref>`)                  | none → silent drop               | XML |
| Forced serializers                           | `ByteArraySerializer` (key+value)| always |
| `client.id` (unless set by operator)         | `tabellarium-<component>-<topicclass>` | code |
| Fallback class for unmapped topics           | `TECHNICAL`                      | XML (`<defaultTopicClass>`) |
| Partitioning key MDC source                  | `traceId`                        | code |
| Circuit breaker: failure-rate threshold      | `50%`                            | code |
| Circuit breaker: sliding window / min calls  | `20` / `10`                      | code |
| Circuit breaker: open-state wait             | `30s`                            | code |
| Circuit breaker: half-open permitted calls   | `10`                             | code |
| Half-open probe gap                          | `5 ms`                           | code |
| Send dispatcher queue capacity (per class)   | `1024`                           | XML (`<sendQueueCapacity>`) |
| Send dispatcher drain on stop (parallel)     | `1 s` drain + margin, shared     | code |
| Fallback dispatcher queue capacity           | `1024`                           | code |
| Fallback dispatcher shutdown timeout         | `5 s`                            | code |
| Producer close timeout                       | `10 s`                           | code |
| Metrics                                      | off (no-op) until bound          | code |

"code" defaults are not exposed through the XML surface today; they are
listed so operators understand the runtime behavior.

---

## Appendix A: The four-class topic model

> **Status — active.** The four-class model is reachable through
> configuration: every `<mapping>` element in `<topicMapping>` routes a
> marker to a topic and assigns that topic a class
> ([§5](#5-topic-routing)). With no `<mapping>` elements, only the
> `TECHNICAL` fallback class is active and the appender runs a single
> producer.

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

### A.2 Runtime flow (how a class is selected)

Every log event travels this path on the way to Kafka:

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
(Without `<mapping>` elements everything resolves to the
`<defaultTopicClass>` — `TECHNICAL` unless configured otherwise — which is
then the only active class.)

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

(In the minimal configuration only layers 1 and 2 have any effect, because
the sole active class, `TECHNICAL`, has no mandatory overrides — see
[§4](#4-producer-property-composition).)

**Mandatory-override violations** — when a value you set conflicts with a
mandatory override, the appender logs a warning to Logback's status manager at
`start()`, for example:

```
Mandatory override applied for AUDIT: acks forced from '1' to 'all'.
This is a compliance requirement; see TopicClass.AUDIT for rationale.
```

The record is still delivered with the enforced value; the warning exists so
operators notice that their configuration intent was overruled.

### A.5 Marker-based routing (XML)

The routing and classification surface is documented in
[§5](#5-topic-routing): one `<mapping>` element per marker, carrying
`<marker>`, `<topic>`, and `<topicClass>` as plain nested elements. (This
flat shape was chosen over the originally sketched per-class nesting —
`<audit><entry marker="…">…</entry></audit>` — because it uses only Joran's
plainest binding mechanism: repeated `addXxx` collection setters with
simple string properties, no attribute or body-text special cases.)

A worked multi-class example:

```xml
<topicMapping>
    <defaultTopic>default.topic</defaultTopic>
    <mapping>
        <marker>SECURITY</marker>
        <topic>audit.security</topic>
        <topicClass>AUDIT</topicClass>
    </mapping>
    <mapping>
        <marker>MONEY</marker>
        <topic>audit.transactions</topic>
        <topicClass>AUDIT</topicClass>
    </mapping>
    <mapping>
        <marker>DEBUG</marker>
        <topic>tech.debug</topic>
        <topicClass>TECHNICAL</topicClass>
    </mapping>
</topicMapping>
```

This activates AUDIT (two topics) alongside the TECHNICAL fallback:
two producers, two breakers, and the AUDIT mandatory overrides of
[A.4](#a4-property-overrides-per-class) enforced for both audit topics.
