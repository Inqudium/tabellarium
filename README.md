<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/banner-light.svg">
  <img src="docs/assets/banner-light.svg" alt="Tabellarium — a resilient Logback appender for Apache Kafka">
</picture>

# Tabellarium

[![Maven Central](https://img.shields.io/maven-central/v/eu.inqudium/tabellarium)](https://central.sonatype.com/artifact/eu.inqudium/tabellarium)
[![CI](https://github.com/Inqudium/tabellarium/actions/workflows/ci.yml/badge.svg)](https://github.com/Inqudium/tabellarium/actions/workflows/ci.yml)
[![Coverage](https://inqudium.github.io/tabellarium/coverage/badge.svg)](https://inqudium.github.io/tabellarium/coverage/)
[![License](https://img.shields.io/github/license/Inqudium/tabellarium)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.4.10-7F52FF?logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Last commit](https://img.shields.io/github/last-commit/Inqudium/tabellarium)](https://github.com/Inqudium/tabellarium/commits/main)
[![Issues](https://img.shields.io/github/issues/Inqudium/tabellarium)](https://github.com/Inqudium/tabellarium/issues)
[![Docs](https://img.shields.io/badge/docs-inqudium.github.io-8E2C21)](https://inqudium.github.io/tabellarium/)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Inqudium/tabellarium/badge)](https://scorecard.dev/viewer/?uri=github.com/Inqudium/tabellarium)

Tabellarium is a resilient Logback appender that ships structured log events to
Apache Kafka. Named after the Roman letter-carrier, it never blocks the sender:
per-topic-class circuit breakers stop hammering a broken route, mandatory
overrides pin the strictest producer-side delivery settings for audit-class
topics (acks=all, idempotence), and a fallback appender catches what cannot
be shipped. Delivery is best-effort transport with visible loss - see
[Delivery guarantees](#delivery-guarantees) for the exact scope.

**Documentation:** [inqudium.github.io/tabellarium](https://inqudium.github.io/tabellarium/) —
configuration guide, metrics overview, and Grafana dashboards.

## Features

### Delivery

- **The sender is never made to wait.** The hot path never blocks
  (`UnsynchronizedAppenderBase` - no synchronized `doAppend`, no waits,
  no I/O) and never calls `producer.send` itself: the caller only
  routes, encodes and enriches, then hands the record to a bounded
  per-topic-class send queue in O(1).
  A dedicated worker per class performs the send, so a stalled broker
  neither pins carrier threads on virtual threads nor stalls a Reactor
  event loop - and a stuck `AUDIT` route never delays `TECHNICAL`
  delivery. `max.block.ms` is additionally capped per class (500 ms;
  200 ms for `PERFORMANCE`), bounding each worker's worst case.
- **Undeliverable events take the side road, not the ditch.** An optional
  fallback appender receives what Kafka refuses, fed through a bounded
  queue and its own worker thread — the Kafka I/O thread is never blocked
  by a slow file appender, and dropped events are counted rather than
  silently lost.

### Circuit breaking

- **A broken route is not hammered.** One Resilience4j circuit breaker per
  topic class, so a stuck audit broker never throttles technical logging.
  Deterministic payload errors (`RecordTooLargeException` and friends) are
  deliberately excluded from the failure rate — a buggy log statement must
  not silence a healthy pipeline.
- **Recovery probes are spread over time.** In half-open state a throttle
  admits one probe per interval instead of letting a high-volume logger
  burn every permitted call in microseconds.

### Routing & service levels

- **Quality of service per log stream.** Each topic class carries its own
  producer tuning and its own circuit breaker: `AUDIT` buys producer-side
  durability (`acks=all`, idempotence, retries), `PERFORMANCE` buys
  throughput (larger batches, longer linger, tighter block budget), with
  `FUNCTIONAL` and `TECHNICAL` in between. Compliance-graded classes
  additionally enforce their producer settings over any conflicting
  operator value — and report every override at startup instead of
  applying it silently.
- **Marker-based routing.** `<mapping>` elements route by SLF4J marker to
  their own topic and class; one producer, breaker and `client.id` per
  active class, and none for dormant ones.

### Traceability

- **Every record says where it came from.** `meta.component`,
  `meta.cmdbId`, `meta.environment` and `meta.agent.*` ride on every
  record as headers, encoded once at startup rather than per event — so
  a consumer can filter by service, instance or stage without parsing
  the payload.
- **Trace affinity, attributable producers.** The record key is the MDC
  trace id, so the records of one trace share a partition and keep their
  relative order within a topic (order across topics is
  [not a guarantee](#ordering-across-topic-classes)); each producer
  announces itself to the broker as
  `tabellarium-<component>-<class>`, so connections, quotas and
  `kafka.producer.*` metrics name the service and its service level
  instead of a generic `producer-N`.

### Operations

- **Misconfiguration fails at startup, not per event.** Blank identity
  fields, invalid Kafka topic names, unknown topic classes, duplicate
  markers and idempotence-incompatible tuning all abort `start()` with a
  named error.
- **Metrics are opt-in and complete.** Counters, timers and queue gauges
  for a Micrometer registry, plus Grafana dashboards and a Spring binding
  helper — and nothing at all until you bind a registry.

### Footprint & security

- **A lean dependency tree.** Micrometer, Spring and the Logstash encoder
  are all `optional`; consumers who do not want them do not get them.
- **Security-conscious defaults.** Diagnostics never echo your producer
  configuration, compliance-graded topics warn when shipped over cleartext,
  the partitioning key is length-bounded, and the appender ignores its own
  producer's log output instead of feeding it back.

## The name

*Tabellarium* is named after the **tabellarius**, the letter-carrier of the Roman world.
His load was the *tabella* — a wax tablet, a small written record of fixed form — and his
craft was not writing but *delivery*: taking the tablet off the sender's hands at the
door, and getting it to its destination even when the usual road was closed.

That is precisely this project's job, transposed to logging. Every log event is a
tabella — one encoded, structured record — and the appender is the carrier that accepts
it at the moment of logging and delivers it to Kafka. The craft lies in *how* it
carries: the **sender is never made to wait** (the hot path never blocks, and the
delivery outcome is reported asynchronously through the send callback), a **broken road
is not hammered** (a circuit breaker per topic class suspends dispatch while the route
is down), and an undeliverable tablet takes the **side road rather than the ditch** (the
fallback appender). Dispatches of rank travel under stricter carriage rules the sender
cannot waive: the mandatory overrides of the AUDIT class — `acks=all`, idempotence — are
the seal the carrier applies whatever the configuration asked for. The name deliberately
refers to the carrier, not the road: Kafka is route and destination, the broker
infrastructure someone else operates; Tabellarium is only ever the one carrying.

The form follows the naming of chemical elements. Real elements take their names from
places, figures and ideas — rhenium after the Rhine, promethium after a myth — and
*tabellarius* + the element suffix *-ium* yields a plausible entry in that series. This
places Tabellarium in the same fictional periodic table as **Inqudium** (the
`eu.inqudium` group it is published under) and **Limesium**: an element-style name for
one well-defined capability, here the element of reliable carriage. The two neighbours
even share a story — Limesium is the watchtower that records each crossing at the
service's own boundary; Tabellarium is the courier who carries the records away.

## Installation

Releases are published to
[Maven Central](https://central.sonatype.com/artifact/eu.inqudium/tabellarium)
(GPG-signed, with sources, javadoc, and a CycloneDX SBOM) — no
repository configuration needed:

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>tabellarium</artifactId>
    <version>1.1.0</version>
    <scope>runtime</scope>
</dependency>
```

Mirrors: the
[GitHub Packages Maven registry](https://github.com/Inqudium/tabellarium/packages)
(`https://maven.pkg.github.com/Inqudium/tabellarium`; needs a token
with `read:packages` even for public packages), and the jar plus SBOM
attached to each
[GitHub release](https://github.com/Inqudium/tabellarium/releases).

## Quick start

1. Declare the appender in your `logback-spring.xml`:

   ```xml
   <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
   ```

2. Fill in the required elements — see [Configuration](#configuration)
   and the complete example at
   [`docs/config/example-logback-spring.xml`](docs/config/example-logback-spring.xml).

3. Optionally add a fallback appender (recommended) — see
   [Resilience](#resilience) below.

4. Deploy.

## Configuration

A complete configuration example lives at
[`docs/config/example-logback-spring.xml`](docs/config/example-logback-spring.xml).
Reference for every supported element:

| Element                      | Required | Type    | Notes                                                                              |
|------------------------------|----------|---------|------------------------------------------------------------------------------------|
| `<encoder>`                  | Yes      | nested  | Any standard Logback encoder. `LogstashEncoder` recommended (and `optional`, so opt-in) — JSON escaping also prevents log forging via attacker-influenced message text; any JSON encoder does. |
| `<kafkaProducerProperties>`  | Yes      | text    | Multi-line `key=value` Kafka producer config. Comments with `#` supported.         |
| `<topicMapping>`             | Yes      | nested  | `<defaultTopic>` plus any number of `<mapping>` elements (marker → topic → topic class) — see [Topic routing](#topic-routing). |
| `<environment>`              | Yes      | string  | Deployment environment (e.g. `prod`, `staging`).                                   |
| `<component>`                | Yes      | string  | Service component identifier (typically `${spring.application.name}`).             |
| `<cmdbId>`                   | Yes      | string  | CMDB identifier of the deploying instance.                                         |
| `<debug>`                    | No       | boolean | Startup diagnostics only: logs active topic classes, fallback configuration, and the generated producer settings (derived `client.id`, applied class overrides) to Logback's status manager. No per-event effect. |
| `<sendQueueCapacity>`        | No       | int     | Capacity of each per-topic-class send queue (default 1024). Overflow diverts to the fallback (reason `queue.full`) instead of blocking. |
| `<includeCallerData>`        | No       | boolean | Captures caller data on the logging thread before the asynchronous hand-off (default false); only relevant when a fallback layout uses `%caller`. |
| `<appender-ref ref="..."/>`  | No       | ref     | Single fallback appender — see [Resilience](#resilience).                          |

A missing `<encoder>` or a blank `<environment>`, `<component>` or
`<cmdbId>` makes the appender refuse startup with an explicit `addError`
on Logback's status manager naming the element; a missing
`<defaultTopic>` or unusable producer properties fail the pipeline
construction the same way.

## Delivery guarantees

Tabellarium is a **best-effort transport with visible loss**, not a
durable audit store. The scope of every guarantee in this document:

- **What the topic classes guarantee:** the Kafka **producer policy** of
  a send that reaches the broker path. `AUDIT` enforces `acks=all` and
  idempotence, so a record the producer has accepted is not silently
  lost to a leader failover or duplicated by a retry.
- **What they do not guarantee:** end-to-end completeness. Ahead of the
  producer sit bounded in-memory queues drained by daemon workers. A
  full send queue, an open circuit breaker, a send failure, an expired
  shutdown budget, or a JVM crash loses events — counted and (except
  for a crash) routed to the optional fallback appender, but lost to
  Kafka nonetheless. The fallback path itself is again a bounded queue
  that drops (counted) on overflow, and is optional.
- **Consequence:** a deployment whose compliance requirement is "every
  audit event is durably recorded" needs a durable record ahead of or
  beside this pipeline (e.g. a transactional outbox in the emitting
  service). Use `AUDIT` to make the *transport* as safe as a logging
  pipeline can be — configure a fallback appender and alert on the
  `kafka.appender.events.fallback` / `kafka.appender.fallback.dropped`
  metrics to make the residual loss observable.

## Mandatory override policy

A single Kafka producer shared by every topic would mean an audit
topic and a debug topic share the same `acks` value — and if the
operator configured `acks=1` for throughput, audit records accepted by
the producer could silently be lost on a Kafka leader failover.

This module classifies every topic into one of four classes and
enforces per-class producer configuration:

| Class       | Producer durability | Mandatory overrides                              | Use case                          |
|-------------|---------------------|--------------------------------------------------|-----------------------------------|
| AUDIT       | Strictest           | `acks=all`, `enable.idempotence=true`            | Audit-relevant log streams (e.g. BaFin/MaRisk contexts) |
| FUNCTIONAL  | Strict              | `acks=all`                                       | Operationally important logs      |
| TECHNICAL   | Best-effort         | (none — operator-tunable)                        | Debug and diagnostic logs         |
| PERFORMANCE | Best-effort         | (none — operator-tunable)                        | High-volume metric logs           |

A **mandatory override** is non-negotiable: if the operator's
`<kafkaProducerProperties>` specifies `acks=1` for an audit topic, the
appender forces `acks=all` at startup and emits a status warning naming
the property, the operator-supplied value, and the enforced value.
Auditors and operators see the override in the Logback startup log:

```
WARN  Mandatory override applied for AUDIT: acks forced from '1' to 'all'.
      This is a non-negotiable topic-class requirement; see TopicClass.AUDIT for rationale.
```

With the minimal configuration (only `<defaultTopic>`) all topics are
treated as TECHNICAL — no mandatory overrides apply. The compliance
differentiation activates per class through
[`<mapping>` elements](#topic-routing).

## Topic routing

`<topicMapping>` routes events by SLF4J marker and classifies each
mapped topic:

```xml
<topicMapping>
  <defaultTopic>my-application.logs</defaultTopic>
  <mapping>
    <marker>SECURITY</marker>
    <topic>audit.security</topic>
    <topicClass>AUDIT</topicClass>
  </mapping>
</topicMapping>
```

Events whose markers match no `<mapping>` (including marker-less
events) go to `<defaultTopic>`, classified via the optional
`<defaultTopicClass>` (default: TECHNICAL) — set it to `AUDIT` etc.
when the default stream itself carries that compliance grade. Each
active class gets its own producer and circuit breaker with the
class's overrides. Misconfiguration — unknown class names, a marker
mapped twice, one topic with two classes, Kafka-invalid topic names —
aborts `start()` with a named error. Full resolution rules and
validation live in the
[configuration guide](docs/config/kafka-appender-config-guide.md).

### Why one topic cannot belong to two classes

Several markers of the same class may route to one topic. What
`start()` rejects is the same topic under two *different* classes,
including a `<mapping>` that names the `<defaultTopic>` with a class
other than `<defaultTopicClass>`. Allowing it would mean two producers
with two policies writing one topic, and every guarantee this module
makes is scoped to a class:

- **Delivery guarantees would diverge on one topic.** The `AUDIT`
  producer writes with `acks=all` and idempotence, the `TECHNICAL`
  producer with `acks=1`. A consumer would see one topic whose records
  are partly durable and partly best-effort, with nothing on the
  record to tell them apart. For an audit topic that is a compliance
  hole.
- **Two producers would write one topic.** Idempotence and ordering
  are per producer and partition. Records of the same trace key would
  arrive through two producers with different `linger.ms` and batch
  settings and could interleave on the partition. Trace affinity via
  the MDC key survives, the relative order between the two classes
  does not.
- **The breaker would split.** On a broken topic the breaker of one
  class opens after roughly ten failures, the other earlier or later
  depending on its volume. Part of the topic's events would divert to
  the fallback while the rest keeps hammering the broker, which
  defeats the breaker's purpose as a health signal for one route
  (see [Why one circuit breaker per topic class](#why-one-circuit-breaker-per-topic-class)).
- **Attribution would blur.** Two `client.id`s, two sets of producer
  metrics and two fallback counters would describe one topic. Broker
  quotas and dashboards could no longer be read per topic.
- **Mandatory overrides would become optional.** Mapping an audit
  topic a second time as `TECHNICAL` and setting the matching marker
  would be enough to bypass the enforced producer settings (see
  [Mandatory override policy](#mandatory-override-policy)).

The check is scoped to one appender instance. Two `KafkaAppender`
instances in the same `logback.xml` that configure the same topic
under different classes are not detected, because the instances do
not know about each other, and every effect above applies. Closing
that gap needs a process-wide registry consulted at start-up, which is
adjacent to the
[producer-registry consolidation](#producer-registry-consolidation)
listed under future work.

### Ordering across topic classes

A consequence of the class isolation: the relative order of events in
*different* classes is not preserved, and nothing downstream may rely
on it. Four independent mechanisms reorder across classes:

- **Separate queues and workers.** FIFO holds within one class's
  `SendDispatcher`. An `AUDIT` event logged before a `TECHNICAL` event
  may reach `producer.send` later if the `AUDIT` worker is sitting at
  its `max.block.ms` cap.
- **Different batching windows.** `PERFORMANCE` lingers 100 ms, the
  other classes 50 ms. Even with empty queues, records leave the
  process at a class-dependent cadence.
- **Independent breakers.** With one class's breaker open, its events
  go to the fallback appender while the other class keeps writing to
  Kafka. Order between the fallback file and Kafka is then lost, not
  merely shifted.
- **No record timestamp is set.** Kafka stamps the record with the
  time of the `send` call (or the broker append), not with the time of
  the log event. Sorting by Kafka timestamp sorts by delivery order.

This costs nothing that Kafka offered: Kafka orders records only
within a partition, different classes always mean different topics
(one topic under two classes is rejected, see above), and different
topics never share a partition. A consumer reading an audit topic and
a technical topic would have had no reliable order between them with
a single producer and a single queue either. What the isolation gives
up is the approximate wall-clock proximity that never guaranteed
anything.

What does hold:

- **Within one topic, per trace.** The MDC trace id keys all records
  of a trace onto one partition. `AUDIT` enforces idempotence, so a
  retry cannot reorder within the partition. `TECHNICAL` and
  `PERFORMANCE` default to `acks=1`, which makes the Kafka client
  silently disable idempotence; a retry with
  `max.in.flight.requests.per.connection > 1` can then reorder even
  within a partition, which is the "Reorder cost: acceptable /
  tolerated" cell in the class table.
- **Correlation across topics** goes through the event timestamp in
  the payload and the trace id, never through Kafka offsets or Kafka
  record timestamps.

### What the partitioning key does

The MDC entry `traceId` of the log event becomes the Kafka record key,
UTF-8 encoded. Kafka picks the partition from the hash of the key, and
everything the key does follows from that.

- **All records of one trace land on one partition** of a topic. Kafka
  orders records only within a partition, so a consumer sees the log
  lines of one request in the order the producer sent them. Without a
  key the sticky partitioner spreads records batch by batch over all
  partitions and the lines of one request scatter; a consumer would
  have to reassemble them by timestamp.
- **Locality for consumers.** Evaluating one trace means reading one
  partition, not all of them. Kafka Streams or a log indexer can group
  by key without parsing the payload.
- **Even distribution.** Trace ids are random hex strings, so their
  hashes spread evenly over the partitions. That is what keeps
  key-based partitioning free of hot spots.

Where the effect ends:

- **Within one topic only.** A trace whose events go partly to the
  audit topic and partly to the technical topic has no order across
  the two (see [Ordering across topic classes](#ordering-across-topic-classes)).
- **Events without a trace id have no key.** Start-up logs, scheduled
  jobs, background threads and reactive code without an MDC bridge
  yield a null `traceId`; those records use the sticky partitioner and
  land anywhere. In a Reactor service without a bridge from the
  Reactor `Context` into the MDC, that is practically every request
  log - see [MDC propagation](#mdc-propagation-for-the-partitioning-key)
  for the check.
- **The snapshot is consistent.** Logback freezes the MDC into the
  event at the `log.info` call, so the appender reads the logging
  thread's value at that moment, never another thread's.

Two risks the key brings:

- **A hot partition at low cardinality.** An application that writes a
  constant or low-variety value under `traceId` - a misconfigured
  bridge, a job name - sends every record to one partition. The topic
  is then effectively single-partitioned, whatever its partition
  count.
- **Partition steering by an attacker.** Applications commonly bridge
  an inbound request header into the MDC; whoever controls the header
  controls the key and thereby the partition. The appender bounds the
  key at 128 characters so an oversized header cannot push every
  record past `max.request.size`; it cannot prevent the steering
  itself. A value over the bound counts as "no key" rather than being
  truncated, because a truncated prefix would still be
  attacker-chosen.

One side effect on batching: with a key, every batch is bound to one
partition. With many partitions and low volume a batch fills more
slowly and `linger.ms` dominates the send duration more; the sticky
partitioner without a key fills batches faster. For the per-class
latency floors described under
[Metrics](#reading-the-send-duration-timer), the key makes the
high-volume shortening a little harder to reach.

The MDC key name is not configurable today; see
[Custom partitioning key](#custom-partitioning-key).

## Resilience

Three resilience mechanisms run independently per topic class:

1. **Per-class circuit breaker.** A Resilience4j `CircuitBreaker` is
   instantiated per active topic class. A stuck audit-topic broker does
   not throttle technical-log delivery, and vice versa. The
   thresholds are tuned for logging volume and fixed in code; their
   canonical values live in the configuration guide's
   [defaults quick reference](docs/config/kafka-appender-config-guide.md#12-defaults-quick-reference),
   which a test keeps in step with the constants. Why they are not
   configurable - and what else is deliberately fixed - is explained in
   the guide's [section on fixed behavior](docs/config/kafka-appender-config-guide.md#13-what-is-deliberately-not-configurable).

2. **Asynchronous delivery with callback-driven outcome tracking.**
   Kafka's `producer.send` is invoked with a callback that feeds the
   circuit breaker (`onSuccess` / `onError`). The `Future` returned by
   `send` is deliberately not retained — the callback is the single
   source of truth for delivery outcome, so delivery failures are
   never invisible.

3. **Fallback appender.** Whenever an event cannot reach Kafka — an
   open breaker, a throttled probe, a failed send (synchronous or via
   the callback), a full send queue, a hot-path error, or the remainder
   at shutdown — the original `ILoggingEvent` is routed to the
   configured fallback appender, tagged with the reason in the metrics.
   Standard Logback `<appender-ref>` syntax is supported:

   ```xml
   <appender name="KAFKA_FALLBACK_FILE" class="ch.qos.logback.core.FileAppender">
     <file>/var/log/myapp/kafka-fallback.log</file>
     <encoder>...</encoder>
   </appender>

   <appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
     ...
     <appender-ref ref="KAFKA_FALLBACK_FILE"/>
   </appender>
   ```

   When no fallback is configured, records are silently dropped on
   failure. This is the deliberate operator choice: configuring a
   fallback says "loss is unacceptable here"; leaving it out says
   "best-effort is fine".

In addition, a **self-logging guard** keeps the appender out of feedback
loops: log events originating from the appender's own Kafka producer
threads (recognizable because the Kafka client names them after the
producer's `client.id`) are ignored entirely — the producer's internal
logging is never shipped through the producer itself.

### Why one circuit breaker per topic class

Separate breakers only pay off if one topic class can be unhealthy
while another is fine. In Kafka that is the normal shape of an
outage, not the exception:

- **Partition leaders live on different brokers.** When a broker
  dies, only the partitions it led are affected. An audit topic whose
  leader sat on that broker times out until the controller elects a
  new leader; a technical topic led by a surviving broker keeps
  flowing.
- **`acks=all` meets `min.insync.replicas`.** `AUDIT` and `FUNCTIONAL`
  enforce `acks=all`. Once a partition has too few in-sync replicas
  the broker rejects exactly that topic's sends with
  `NotEnoughReplicasException`, while topics with `acks=1` or healthy
  replica sets are untouched. This is the most common way a single
  class turns red.
- **Broker-side limits are per topic or per client.** Quotas are keyed
  by `client.id` (one per class, see
  [Traceability](#traceability)), `max.message.bytes` is a topic
  setting, and a full log directory hits only the partitions stored
  there.
- **Each class has its own producer.** Its own buffer, its own
  `max.block.ms` cap and its own I/O thread. A full buffer or a
  stalled metadata fetch in one producer is local to that class.
  With a single shared breaker, a failure burst from the noisy
  `PERFORMANCE` class would open the breaker for the quiet `AUDIT`
  class and divert compliance-relevant events to the fallback although
  their route is healthy.

Where the assumption does not hold - a cluster-wide outage, a network
partition, an authentication failure at connection level - all
classes fail together and every breaker opens independently after
roughly ten failures. That costs nothing beyond redundancy: the split
helps in a partial outage and is neutral in a total one.

Two design details protect the isolation:

- **Deterministic payload errors are excluded from the failure
  rate.** `RecordTooLargeException`, `InvalidTopicException`,
  `SerializationException` and `TopicAuthorizationException` are
  ignored by the breaker (they still reach the fallback). Otherwise
  one oversized log statement or one misnamed topic could open the
  breaker of its whole class and pull every healthy topic of that
  class into the fallback with it.
- **The granularity is the class, not the topic.** Several `<mapping>`
  topics of the same class share one producer and one breaker. A
  breaker per topic would be cosmetic without a producer per topic:
  the topics would still share buffer, `max.block.ms` budget and I/O
  thread, and a stall in one would hold the others regardless of what
  the breaker reports. Per-class producers are the granularity at
  which failure isolation is actually enforceable, so that is where
  the breakers sit.

## Should I wrap this in a Logback `AsyncAppender`?

**Short answer: no.** A common pattern with Kafka appenders is to
wrap them in `ch.qos.logback.classic.AsyncAppender` to keep
application threads from blocking on Kafka I/O. This module makes
that pattern unnecessary and, with default `AsyncAppender` settings,
counterproductive.

### Why it is not needed

`AsyncAppender` exists to absorb caller-thread blocking. This module
eliminates caller-thread blocking through four layered defenses:

1. **`producer.send` never runs on the caller.** Each topic class has
   its own bounded send queue and worker thread (`SendDispatcher`);
   the logging thread only routes, encodes, enriches and enqueues in
   O(1). A full queue diverts to the fallback (metric reason
   `queue.full`) instead of blocking.
2. **`max.block.ms` is capped at 500 ms** per topic class (200 ms for
   `PERFORMANCE`). The cap is enforced: a lower operator value wins, a
   higher one is clamped with a startup warning. It bounds how long a
   send *worker* can be held per event.
3. **The circuit breaker trips after ~10 failures** (50% failure rate
   in a 20-call sliding window). Once open, subsequent events are
   routed to the fallback in O(1).
4. **The fallback uses an asynchronous `FallbackDispatcher`** — a
   bounded queue with its own daemon worker, so the Kafka I/O thread
   and the send workers are never held hostage by a slow fallback
   appender (e.g. a `FileAppender` on saturated disk).

Worst case for a service whose Kafka cluster has just gone down:
caller threads keep logging in microseconds; each class's send worker
absorbs at most `max.block.ms` per event until its breaker opens
(~10 × 500 ms = 5 s, on the worker, not on your request threads),
and queue overflow flows to the fallback, counted.

### Which thread does what

The encoder runs on the application's logging thread, the one that
calls `log.info(...)`. `append` performs, in this order, on that
thread:

1. `prepareForDeferredProcessing` freezes MDC, thread name and the
   formatted message into the event.
2. `callerData`, only if `<includeCallerData>` is set.
3. `encoder.encode(event)` produces the JSON payload as a byte array.
4. Marker routing, class lookup, and the key extraction from the MDC.
5. The O(1) hand-off of payload, key and headers into the class queue.

Only then does the thread change. The `SendDispatcher` worker of the
class takes the finished package from the queue and calls
`producer.send`; it encodes nothing, it receives bytes. The Kafka
client's I/O thread takes over from there and runs the send callback.

Two consequences follow from encoding on the caller:

- **Encoding scales with the caller threads.** Twenty logging threads
  encode in parallel. A single worker per class would otherwise have
  to encode every event of its class alone and would become the
  bottleneck at high volume - which is exactly what happens behind an
  `AsyncAppender` (see below).
- **Encoding is what the caller pays.** The `doAppend` figures under
  [Reading the send-duration timer](#reading-the-send-duration-timer)
  include the encoder. For a small event that is a fraction of a
  microsecond; an event with a long stack trace costs the caller
  correspondingly more, because the encoder writes out the throwable
  proxy.

A third thread can run an encoder: the fallback appender's own. A
`FileAppender` configured as fallback encodes the original event
again with its own encoder, on the `FallbackDispatcher` worker - not
on the caller and not on the Kafka I/O thread. The Kafka payload is
not reused for this, because the fallback receives the `ILoggingEvent`,
not the bytes. This is also why `<includeCallerData>` exists: a
fallback layout with `%caller` would otherwise walk the wrong stack on
the fallback worker.

### Why wrapping in `AsyncAppender` now hurts

Wrapping in `AsyncAppender` with its default settings introduces
**worse** loss semantics than this module's built-in mechanisms:

- **`neverBlock=false` (the default) is a trap.** When the
  `AsyncAppender` queue (`queueSize=256` by default) fills up,
  `doAppend` blocks the caller — defeating the entire point of using
  the wrapper. The 500 ms `max.block.ms` protection is bypassed
  because the wait happens in the queue offer, not in `producer.send`.
- **`discardingThreshold=queueSize/5` (the default) silently drops
  INFO/DEBUG/TRACE** once the queue is 80% full. These dropped events
  do not reach the fallback appender; they vanish.
- **`AsyncAppender` adds an extra thread, an extra queue, and extra
  indirection** between the application and the appender for no
  benefit this module does not already provide.

### Recommendation

Use the `KafkaAppender` directly:

```xml
<appender name="KAFKA" class="eu.inqudium.tabellarium.KafkaAppender">
    ...
    <appender-ref ref="KAFKA_FALLBACK_FILE"/>
</appender>

<root level="INFO">
    <appender-ref ref="KAFKA"/>
</root>
```

If an existing `logback-spring.xml` wraps the Kafka appender in an
`AsyncAppender`, drop the wrapper:

```diff
- <appender name="ASYNC_KAFKA" class="ch.qos.logback.classic.AsyncAppender">
-   <appender-ref ref="KAFKA"/>
- </appender>
- <root level="INFO">
-   <appender-ref ref="ASYNC_KAFKA"/>
- </root>
+ <root level="INFO">
+   <appender-ref ref="KAFKA"/>
+ </root>
```

### When `AsyncAppender` is still justified

The one cost that remains on the logging thread is the synchronous
part of `append`: freezing the event's lazy state, JSON encoding,
routing, key extraction and the O(1) hand-off into the class queue.
It is measured, not estimated (see
[Reading the send-duration timer](#reading-the-send-duration-timer)
for the table and the regime): a p50 of 0.12 µs and a p99 of 0.45 µs
on one thread, and a p99 of 352 µs at 32 threads under open-loop
saturation with shedding engaged. A sub-millisecond latency budget is
therefore met without a wrapper. Two cases remain:

- **Budgets in the tens of microseconds** on the request path itself,
  where even the encoding of a small event counts.
- **Large events on a latency-critical path.** Encoding cost grows
  with the event size; a long stack trace costs the caller far more
  than a one-line message, and an `AsyncAppender` moves that off the
  request thread.

Beyond those, an organisational convention may simply require the
wrapper. In every such case use these non-default settings:

```xml
<appender name="ASYNC_KAFKA" class="ch.qos.logback.classic.AsyncAppender">
    <appender-ref ref="KAFKA"/>
    <queueSize>2048</queueSize>
    <discardingThreshold>0</discardingThreshold>
    <neverBlock>true</neverBlock>
    <includeCallerData>false</includeCallerData>
</appender>
```

Know what the wrapper saves and what it costs:

- **It saves the encoding, little else.** Logback's `AsyncAppender`
  calls `prepareForDeferredProcessing` on the caller, exactly as this
  appender does, so freezing the MDC and the formatted message stays
  on the logging thread either way. The hand-off into the class queue
  is replaced by the hand-off into the wrapper's `ArrayBlockingQueue`,
  another lock. What actually moves is encoding, routing and key
  extraction.
- **It caps throughput at one encoder thread.** Attached directly,
  encoding runs in parallel on the caller threads. Behind the wrapper
  every event is encoded by the single `AsyncAppender` worker. At high
  volume that worker becomes the bottleneck, its queue fills, and
  `neverBlock` discards.
- **Its loss is invisible to every metric.** An event the wrapper
  discards never reaches this appender: it appears neither in
  `events.fallback` nor in `fallback.dropped`, does not reach the
  fallback appender, and the `AsyncAppender` keeps no counter of its
  own. The loss signals and alerts documented under
  [Metrics](#metrics) are blind to it.
- **It defeats the per-class isolation upstream.** One queue sits in
  front of all topic classes. A burst of `PERFORMANCE` logging fills
  the wrapper's queue, and `neverBlock` discards `AUDIT` events before
  they ever reach their own class queue, producer and breaker.
- **Caller data must be captured by the wrapper.** This appender's
  own `<includeCallerData>` captures the logging site on the thread
  that calls it, which behind the wrapper is the `AsyncAppender`
  worker. Set the flag on the `AsyncAppender` instead if a fallback
  layout consumes `%caller`, and leave it off otherwise: the stack walk
  is the most expensive step of the whole path.

## Reactive applications

This module is safe to use in Reactor-Netty / WebFlux services and
in code that runs on JDK virtual threads. The largest reactive
hazard — `synchronized` blocks in the appender hot path, which cause
carrier-thread pinning on virtual threads and Reactor-Netty
event-loop stalls — does not arise: the appender extends
`UnsynchronizedAppenderBase`. There are no locks in the hot path;
only atomics, volatiles and a per-thread reentry flag.

Two reactive-specific concerns remain that are worth tuning per
service.

### `max.block.ms` bounds the send worker, not the event loop

`KafkaProducer.send()` is asynchronous per the Kafka spec, but it
can synchronously block for up to `max.block.ms` when the producer
buffer is full, metadata is stale, or buffer allocation contends
with other producers. That block lands on the topic class's
dedicated send worker — the event-loop (or virtual) thread that
called `log.info()` only encodes and enqueues, and returns in
microseconds regardless of this setting.

`max.block.ms` therefore no longer needs reactive-specific tuning
for latency. What it still governs is **queue drain speed during
broker trouble**: with the 500 ms class default, a stalled cluster
lets each worker absorb at most ~10 × 500 ms before its breaker
opens, during which the bounded send queue may fill and overflow to
the fallback (reason `queue.full`). A service that prefers to shed
to the fallback faster can lower the value in
`<kafkaProducerProperties>`:

```xml
<kafkaProducerProperties>
    bootstrap.servers=...
    max.block.ms=50
</kafkaProducerProperties>
```

Trade-off: with 50 ms the worker gives up earlier under transient
buffer pressure (not a cluster outage, just a brief backlog) and
those events divert to the fallback. This is the intended use of
the fallback. The circuit breaker still trips after roughly 10
failures, after which all further events route to the fallback in
O(1) regardless of `max.block.ms`.

Everything else (`acks`, `enable.idempotence`, `linger.ms`) is
decided by topic class, for servlet and reactive services alike.

### Heap while a broker is slow

A broker that is reachable but slow to acknowledge is the one
condition the circuit breaker cannot see early: every `send()`
succeeds, and the failures arrive through the callbacks only after
`delivery.timeout.ms` (Kafka default: two minutes). Until then the
client buffers up to `buffer.memory` bytes of serialized records, and
every buffered record keeps its original log event (MDC map,
arguments, throwable proxy) reachable for the fallback path. The
retained heap is therefore bounded by `buffer.memory` measured in
**event** size, not in payload size — exception-heavy logging at high
volume can pin a multiple of `buffer.memory`. Size `buffer.memory`
and `delivery.timeout.ms` per class in `<kafkaProducerProperties>`
with that in mind; the appender itself retains nothing else per
buffered record.

### BlockHound

Services that run BlockHound (`io.projectreactor.tools:blockhound`)
in their integration tests will see the appender's internal
operations flagged as blocking — most notably the
`LinkedBlockingQueue.offer()` of the per-class send queue (every
event) and of the fallback dispatcher. These are not true blocks in
the harmful sense (the offer is non-blocking, a full queue rejects
instead of waiting; `KafkaProducer.send()` itself runs on the
appender's own worker threads, never on the caller), but BlockHound's
heuristics don't know that.

Add an allow-list entry in the test setup:

```kotlin
BlockHound.builder()
    .allowBlockingCallsInside(
        "ch.qos.logback.classic.Logger", "callAppenders"
    )
    .allowBlockingCallsInside(
        "eu.inqudium.tabellarium.KafkaAppender", "append"
    )
    .install()
```

This declares that logging calls are an accepted block point in the
service's contract — which they have to be, regardless of which
appender is used.

### MDC propagation for the partitioning key

The default partitioning-key extractor reads `traceId` from the MDC
at the moment of the `log.info(...)` call (what the key does and where
its effect ends is described under
[What the partitioning key does](#what-the-partitioning-key-does)). Logback freezes the MDC
into the `ILoggingEvent` at that moment, so the appender always sees
a consistent snapshot — there is no risk of reading "the wrong
thread's MDC" inside the appender.

The key is **length-bounded at 128 characters**; a longer value is
treated as no key at all. Applications commonly bridge an inbound
request header into the MDC, so the value can be attacker-influenced,
and an unbounded key would inflate every record past
`max.request.size` — the resulting `RecordTooLargeException` is
deliberately ignored by the circuit breaker, so those events would
flood the fallback appender indefinitely. Note that the bound limits
record size, not distribution control: an application that bridges
unvalidated inbound values into the MDC can still influence which
partition its records land on.

The reactive concern is upstream: in Reactor code, the trace context
typically lives in the Reactor `Context`, not in the MDC of the
event-loop thread. If the service does not bridge the Reactor
`Context` into the MDC, `MDC.get("traceId")` returns null at the
`log.info()` call, and the appender consequently routes records
without a partitioning key (round-robin distribution by Kafka's
default partitioner, instead of partition-locality per trace).

Verify with a quick check in any reactive handler:

```kotlin
@GetMapping("/test")
fun test(): Mono<String> = Mono.fromCallable {
    val traceId = MDC.get("traceId")
    log.info("traceId in MDC: {}", traceId)
    "ok"
}
```

If `traceId` is null in the log line, the MDC bridge is missing.
Common bridges:

- Micrometer Tracing with `reactor.MicrometerTracingObservationHandler`
- Reactor Core 3.5+ with `ContextSnapshotFactory` (`Hooks.enableAutomaticContextPropagation()`)
- Spring Boot 3.2+ with `spring.reactor.context-propagation=auto`

Fixing this is a service-side concern, not an appender concern.
A service without trace-id-in-MDC still functions correctly; it
just loses partition-locality for related events.

## Metrics

The appender publishes hot-path counters, send-duration timers and
queue-depth gauges to a [Micrometer](https://micrometer.io/)
`MeterRegistry`. Metrics are **opt-in**: the appender runs without
Micrometer on the classpath and emits no metrics until
`bindMeterRegistry()` is called.

### Metric inventory

Every metric below additionally carries the tag `appender`, whose
value is the Logback appender name from `<appender name="...">`
(`unnamed` if none is set). It keeps two appender instances bound to
the same registry apart: without it their meter IDs would be
identical, Micrometer would hand both the same meter, and stopping one
appender would deregister the other's meters as well. One appender
means one tag value, so the cardinality budget below already includes
it. The tags listed per metric are the ones that vary within an
appender instance.

| Metric                              | Type    | Tags                              | Meaning                                                       |
|-------------------------------------|---------|-----------------------------------|---------------------------------------------------------------|
| `kafka.appender.events.accepted`    | Counter | `topic.class`                     | Events entering `KafkaAppender.append`                        |
| `kafka.appender.events.dispatched`  | Counter | `topic.class`                     | Events handed to `producer.send` without a synchronous failure (callback outcome unknown) |
| `kafka.appender.events.fallback`    | Counter | `topic.class`, `reason`           | Events diverted from Kafka (to the fallback if configured, otherwise dropped) |
| `kafka.appender.send.duration`      | Timer   | `topic.class`, `outcome`          | Wall-clock send duration from invocation to callback          |
| `kafka.appender.fallback.dropped`   | Counter | —                                 | Events lost by the fallback dispatcher (queue full, `doAppend` threw, worker died, shutdown remainder) |
| `kafka.appender.fallback.queue.size`     | Gauge   | —                                 | Current depth of the fallback dispatcher queue                |
| `kafka.appender.fallback.queue.capacity` | Gauge   | —                                 | Maximum depth of the fallback dispatcher queue                |
| `kafka.appender.send.queue.size`         | Gauge   | `topic.class`                     | Current depth of the class's send dispatcher queue            |
| `kafka.appender.send.queue.capacity`     | Gauge   | `topic.class`                     | Maximum depth of the class's send dispatcher queue            |

`reason` values: `breaker.open`, `throttle`, `send.error`,
`encoder.error`, `queue.full`, `shutdown`.
`outcome` values: `success`, `error`.
`topic.class` values: `audit`, `functional`, `technical`,
`performance`.

Cardinality budget per appender instance: ~51 time series.
At 100 microservices in a shared Prometheus this is ~5 100 series —
well within the default cardinality budget.

### Reading the send-duration timer

`kafka.appender.send.duration` starts immediately before
`producer.send` and stops in the producer callback. It spans the
client's synchronous wait for metadata and buffer space (capped by
`max.block.ms`), the wait in the record accumulator until `linger.ms`
expires or the batch fills, the broker round trip including
replication for `acks=all`, and client-internal retries. It does not
span the wait in the class's send queue, encoding and enrichment, or
events the breaker or throttle turned away; `send.queue.size` covers
the missing leg. Percentile histograms are opt-in via a Micrometer
`MeterFilter`; without one the timer exports count, sum and max only.

Because each class has its own producer defaults, each class has its
own latency floor: `PERFORMANCE` lingers 100 ms, the other classes
50 ms, and `AUDIT` and `FUNCTIONAL` add replication time for
`acks=all`. At low volume every record waits the full linger window,
so a p99 near 100 ms on `PERFORMANCE` is the configured batching
delay, not a slow broker; at high volume batches fill first and the
duration drops towards the round trip. Aggregate and alert per
`topic_class`: a quantile over all classes mixes the floors and moves
with the class mix. The per-class table and the query patterns live in
the [metrics overview](docs/metrics/metrics-overview.md#reading-kafkaappendersendduration).

The timer therefore says nothing about the delay the **application**
experiences when it logs. That delay is the synchronous part of
`doAppend`: Logback builds the event, the appender routes, encodes,
enriches and offers the package to the class's queue in O(1); a full
queue diverts to the fallback instead of waiting. No metric times this
path, deliberately: a timer sample per event would itself be hot-path
cost. It is measured by the JMH benchmark `AppendPipelineBenchmark`
([benchmarks/README.md](benchmarks/README.md)). Measured `doAppend`
cost per event, sample mode, from the raw output of the
[bench report of 2026-08-29](docs/assessment/BENCH_REPORT-2026-08-29T11-38-12.md)
(`benchmarks/results/2026-08-29/r6-pipeline-sample-t{1,8,32}.txt`):

| Caller threads | p50     | p90     | p99     | p99.9   | Mean     |
|----------------|---------|---------|---------|---------|----------|
| 1              | 0.12 µs | 0.22 µs | 0.45 µs | 2.6 µs  | 0.37 µs  |
| 8              | 0.25 µs | 1.3 µs  | 34 µs   | 62 µs   | 2.4 µs   |
| 32             | 0.50 µs | 3.8 µs  | 352 µs  | 428 µs  | 26 µs    |

The regime is deliberately the worst the design allows: open-loop
callers saturate the single per-class worker, the queue fills and
shedding to the fallback is engaged, on a 12-core workstation with the
CPU governor on powersave and JDK 26 against the Java 21 target. Read
the figures as orders of magnitude: the tail growth at 32 threads
comes from oversubscribing the cores and from the saturated regime,
not from Kafka, which the caller never touches. Encoding cost is
inside these figures and grows with the event size; a long stack trace
costs the caller more than a one-line message. In production,
`send.queue.size` is the
early indicator that the worker falls behind, and
`events.fallback{reason="queue.full"}` marks the point where the
caller starts losing events to the fallback; the caller itself stays
fast throughout. The end-to-end latency from log call to broker ack is
caller path plus queue wait plus `send.duration`, and the middle leg is
what no metric covers. A consumer can reconstruct it: the appender
sets no record timestamp, so the client stamps `CreateTime` at the
`send` call, and `CreateTime` minus the event's own timestamp in the
payload is caller path plus queue wait.

### Additional bindings

When `bindMeterRegistry()` is called, two additional metric sources
are bound:

- **Circuit-breaker metrics** (`resilience4j.circuitbreaker.*`) are
  published by the appender's own binder — no `resilience4j-micrometer`
  needed. Metric names and tags mirror `TaggedCircuitBreakerMetrics`,
  plus the same `appender` tag the appender's own meters carry, so
  multiple appender instances on one registry never collide.
- **Micrometer Kafka binder** (part of `micrometer-core` for older
  versions, `micrometer-binders-kafka` for newer) publishes the
  underlying Kafka producer's internal metrics (`kafka.producer.*`)
  if it is on the classpath. One binding per active topic class,
  tagged with `topic.class` and `appender`.

A missing Kafka binder is silently skipped — that binding is
best-effort, not fail-fast.

### Wiring up: Spring applications

For Spring Boot applications, the library ships a small
`@Configuration` helper class, [`KafkaAppenderMetricsBinding`].
Add it as a bean in any `@Configuration` class:

```kotlin
@Configuration
class LoggingConfig {
    @Bean
    fun kafkaAppenderMetricsBinding(registry: MeterRegistry) =
        KafkaAppenderMetricsBinding(registry)
}
```

Or import it directly:

```kotlin
@Configuration
@Import(KafkaAppenderMetricsBinding::class)
class LoggingConfig
```

The binding listens for `ContextRefreshedEvent`, walks the Logback
`LoggerContext`, finds every `KafkaAppender`, and calls
`bindMeterRegistry()` on each. No further application code required.

One limitation: a **Logback reconfiguration** at runtime
(`<configuration scan="true">`, or a programmatic reset) replaces the
appender instances, and neither Spring nor Logback offers a callback
that fires *after* the new configuration is in place - the metrics of
the replacement appenders stay dark until `bindAppenders()` on the
binding bean is called again (it is public and idempotent). Wire that
call from wherever your reconfiguration is triggered, or avoid runtime
reconfiguration for the Kafka appender.

To add application-specific common tags (e.g. service name,
environment), pass them to the constructor:

```kotlin
@Bean
fun kafkaAppenderMetricsBinding(
    registry: MeterRegistry,
    @Value("\${spring.application.name}") app: String,
) = KafkaAppenderMetricsBinding(
    registry,
    Tags.of("application", app),
)
```

The Spring integration is deliberately not Spring Boot
auto-configuration — operators import it explicitly. This keeps the
appender's dependency tree honest (no transitive Spring pull) and
makes it obvious in application code where the binding happens.

### Wiring up: non-Spring applications

Call `bindMeterRegistry` directly from any lifecycle point that
happens after the `MeterRegistry` is available:

```kotlin
val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
loggerContext.loggerList.asSequence()
    .flatMap { logger -> logger.iteratorForAppenders().asSequence() }
    .filterIsInstance<KafkaAppender>()
    .distinct()
    .forEach { it.bindMeterRegistry(meterRegistry, Tags.empty()) }
```

(Descend into `AsyncAppender` wrappers yourself if you use them; the
Spring binding does.) A repeated `bindMeterRegistry` call replaces the
previous binding, and a call on a stopped appender is ignored with a
status warning.

Pre-Spring log events (Logback initialization, Spring bootstrap
logging) are not counted in either setup — this is a deliberate
trade-off, since capturing them would require a static
`MeterRegistry` reference that conflicts with Spring's lifecycle.

### Grafana / dashboard pointers

A minimal dashboard typically shows:

- **Throughput per topic class** — `rate(kafka_appender_events_accepted[1m])`,
  stacked by `topic_class`.
- **Loss rate** — `rate(kafka_appender_events_fallback[1m])`,
  stacked by `reason`. A sudden spike in `breaker.open` means the
  cluster failed; in `throttle` means a sustained recovery probe;
  in `send.error` means individual send rejections (e.g.
  RecordTooLargeException after the deliberate exclusion).
- **Send latency** — `histogram_quantile(0.99, sum by (topic_class, le)
  (rate(kafka_appender_send_duration_seconds_bucket[5m])))`, one series
  per `topic_class`, never aggregated across classes (see
  [Reading the send-duration timer](#reading-the-send-duration-timer)).
  The healthy baseline is the class's linger floor plus the round
  trip: roughly 50 ms for `AUDIT`, `FUNCTIONAL` and `TECHNICAL`,
  roughly 100 ms for `PERFORMANCE`.
- **Fallback queue saturation** — `kafka_appender_fallback_queue_size /
  kafka_appender_fallback_queue_capacity`. Sustained values > 0.5 mean
  the fallback appender (typically a `FileAppender`) is slower than
  the event arrival rate — operator action needed.
- **Dropped event total** — `kafka_appender_fallback_dropped_total`.
  Any non-zero rate is a data-loss signal; if the operator configured
  a fallback, this number should stay at zero.

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                      KafkaAppender (orchestrator)                  │
│                                                                    │
│  start() ─→ validateConfiguration ─→ AppenderPipeline.build        │
│      ┌─────────────┬──────────────┬─────────────────┐              │
│      ▼             ▼              ▼                 ▼              │
│  TopicRouter   TopicTable   MessageEnricher   ProducerRegistry     │
│  (marker →     (topic →     (meta.* headers,  (one producer per    │
│   topic)        class)       traceId key)      active class)       │
│                                                                    │
│  append(event) — caller thread, CPU-bound only, never blocks:      │
│      encode ─→ route ─→ classify ─→ enrich ─→ dispatch O(1)        │
│                                                  │                 │
│         one per active topic class               │ queue full /    │
│      ┌───────────────────────────┐               │ stopped         │
│      │ SendDispatcher            │◀──────────────┤                 │
│      │ bounded queue + worker    │               │                 │
│      └─────────────┬─────────────┘               │                 │
│                    ▼ producer.send on the worker │                 │
│      ┌───────────────────────────┐               │                 │
│      │ ResilientMessageSender    │  breaker open /                 │
│      │ half-open throttle +      │  throttled / send               │
│      │ circuit breaker per class │  failed ─────────┐              │
│      └─────────────┬─────────────┘               │  │              │
│                    ▼ async callback              ▼  ▼              │
│               Kafka broker            ┌───────────────────────┐    │
│                                       │ FallbackDispatcher    │    │
│                                       │ bounded queue + worker│    │
│                                       └───────────┬───────────┘    │
│                                                   ▼                │
│                                        fallback appender           │
│                                        (<appender-ref>, optional;  │
│                                         overflow drops, counted)   │
└────────────────────────────────────────────────────────────────────┘
```

Every component below `KafkaAppender` is **internal implementation**
with its own dedicated unit test. The supported public API is the
operator surface only — the appender with its XML elements
(`KafkaAppender`, `TopicMappingConfig`/`TopicMappingEntry`), the
`TopicClass` enum, and the optional `KafkaAppenderMetricsBinding`;
see [ADR-0002](docs/adr/ADR-0002-public-api-is-the-operator-surface.md).
Substitution seams (producer factory, breaker registry, injected
clocks) exist module-internally and carry the test suite; they are
deliberately not a consumer contract.

## Extension points

### Custom partitioning key

The partitioning key is read from MDC `traceId` by default (its
effects are described under
[What the partitioning key does](#what-the-partitioning-key-does)).
There is currently no configuration surface for a different key (session id,
user id, account id) — most deployments use the trace-id default. Per
[ADR-0002](docs/adr/ADR-0002-public-api-is-the-operator-surface.md),
such an override would be added as an XML-bindable `KafkaAppender`
property (e.g. a partitioning-key MDC name), not by exposing the
internal enricher — open an issue if your deployment needs it. The
configuration guide lists
[everything that is deliberately not configurable](docs/config/kafka-appender-config-guide.md#13-what-is-deliberately-not-configurable)
and the reason for each item.

## Future work

Items deferred from the current revision, in roughly decreasing
priority order:

### Producer-registry consolidation

The current design instantiates one Kafka producer per active
`TopicClass`. For Kubernetes deployments where the broker enforces
per-IP producer-connection limits, or for memory-constrained
environments where 4 × 32 MB of producer buffer is a noticeable
share of the pod's memory budget, the registry could be extended to
share a single producer across classes whose configurations are
compatible (same `acks`, same idempotence setting, etc.).

This is a non-trivial change because it interacts with the per-class
circuit-breaker isolation: if AUDIT and FUNCTIONAL share a producer,
a fault that affects the shared producer trips both circuit breakers
together, partially defeating the isolation guarantee. Probably worth
doing only if a concrete deployment hits the producer-count ceiling.

## How it is tested

The suite is layered so the fast loop stays offline and the expensive
guarantees still get proven:

- **Offline unit/component base** — the default `mvn verify` run needs
  no broker and no Docker: `MockProducer`, hand-built fakes, injected
  clocks, and latch-pinned concurrency scenarios cover routing,
  property composition, breaker/throttle behavior, the asynchronous
  dispatch and shutdown accounting, and the metrics lifecycle.
  Appender-level tests exercise the real asynchronous worker path
  (there is no synchronous test mode in production code).
- **Declarative-contract layer** — Joran round-trip tests feed real XML
  through `JoranConfigurator` and bind every documented element, so the
  operator-facing configuration surface is executable, not asserted.
- **Real-broker stage** — `mvn -Pintegration test` (Testcontainers,
  needs Docker) proves a successful TECHNICAL and AUDIT record against
  an Apache Kafka container: real serializers, LZ4, headers,
  partitioning key, and the AUDIT acks/idempotence handshake.
- **External-contract stage** — `mvn -Pexternal-contract test` runs
  characterization tests of third-party behavior that are excluded from
  the default loop on purpose.

The full inventory — every test sentence plus the rationale block
explaining what it pins and why — is generated from each build and
published as [Test evidence](https://inqudium.github.io/tabellarium/tests/test-evidence/);
the [coverage report](https://inqudium.github.io/tabellarium/coverage/)
and the badge above come from the same run. Both are generated
artifacts: no number in them is maintained by hand.

## Build

Standard Maven module:

```bash
mvn verify                                      # build + run all offline tests
mvn -Dtest=KafkaAppenderTest test               # run a single test class
mvn -Dtest='*MessageEnricher*' test             # pattern-match
mvn -Pintegration test                          # + real-broker tests (needs Docker)
```

The default test run is offline and fast (`MockProducer`, no Docker).
The `integration` profile adds the Testcontainers-based real-broker
tests (`@Tag("integration")`), which verify a successful TECHNICAL and
AUDIT record end-to-end against an Apache Kafka container — real
serializers, compression, headers, and the AUDIT acks/idempotence
handshake.

The artifact targets Java 21 (Kotlin 2.4.10); building needs JDK 24+
because of the JVM flags in `.mvn/jvm.config`. The quality gates that
run with `mvn verify` (ktlint, JaCoCo, the documentation-contract test
that pins the guide's tables to the constants, Jazzer regression
inputs) and the CI-only scans (OSV via CycloneDX SBOM, CodeQL, nightly
fuzzing) are described in [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for
the build setup, code-style expectations, and pull-request process.
Security vulnerabilities should be reported privately as described in
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
