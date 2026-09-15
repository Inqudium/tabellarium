# Defect analysis

## Metadata

### Identification

| Field | Value |
|---|---|
| Ticket | `[MISSING - please supply]` |
| Work directory | `/home/dirk/IdeaProjects/tabellarium` |
| Git branch | `main` |
| Analysed commit | `9910a49676fc9c960705fa4a3fc405928ba40e5b` |
| Analysis time | `2026-09-15T22:05:50+02:00` |

### Scope

Included: `pom.xml`, the production code in `./src/main/kotlin/`, all Kotlin tests in
`./src/test/kotlin/`, all Java Jazzer fuzz targets in `./src/test/java/`, relevant
resources and documentation contracts (`README.md` and current assessment history).
Tests are explicitly in scope and were assessed as code.

Excluded: `./benchmarks/`, generated output (`./target/`), Git internals, dependency
caches, and historical assessment files as defect subjects. Historical assessments were
read only to avoid reopening findings fixed by commits `2478232` and `3cc019b`.

### Environment

| Component | Observed value |
|---|---|
| JDK | Oracle JDK 26.0.1 |
| Maven | 3.9.15 |
| Production target | Java 21 |
| Kotlin | 2.4.20 |
| Spring Boot BOM | 4.1.1 |
| Kafka client | 4.2.1 |

### Placement

This is the only file created by this analysis. No existing file was changed. The report
is intentionally a new timestamped document under `./docs/assessment/`.

## 1. Executive summary

The current revision is a well-tested, deliberately bounded asynchronous Kafka Logback
appender. Earlier findings from the immediately preceding defect analysis were verified
as remediated. This pass found **five current defects**: three Medium and two Low. There
is no Critical or High finding.

The most consequential defects are at extensibility and shutdown boundaries: a Kafka
producer interceptor can mutate static header arrays reused by every record; a forced
dispatcher shutdown can mark an already handed-off record as a shutdown fallback; and
two appenders on the same logger can amplify each other's Kafka client logging during a
broker incident. Two programmatic configuration edge cases additionally expose a
fallback that is not wired into the running transport and metrics that disappear when
same-named appenders share a registry.

**Test verdict: PARTIAL.** The suite has 321 JUnit test methods plus three Jazzer fuzz
targets, deterministic latches for most concurrent paths, a broker integration test,
and contract tests. It does not cover the five triggering conditions above. No Maven
build, unit test, integration test, fuzz run, or static-analysis command was executed:
the requested procedure explicitly reserves those runs for a later go-ahead.

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 0 |
| Medium | 3 |
| Low | 2 |

## 2. Scope and methodology

### Phase 0 — project map

This is a Maven library, not a Spring application. Its central flow is Logback ingress
→ `KafkaAppender` → `KafkaTransport` → per-topic `SendDispatcher` →
`ResilientMessageSender`/Kafka producer, with a bounded `FallbackDispatcher`. Micrometer
and the Spring binding are optional. The default test loop excludes the Docker-backed
integration test and external-contract tests; Jazzer regression inputs are in the normal
test tree.

The build config supplies ktlint, JaCoCo, CodeQL, and Jazzer. No Detekt, SpotBugs,
Error Prone, NullAway, or architecture-rule tool is configured.

### Phase 1 — risk ranking

Every relevant production and test file was ranked before deep review. Ranking used the
requested 1–5 scale: asynchronous ownership, lifecycle, Kafka and metrics wiring rank
highest; value objects and test helpers rank lowest. The complete table is in section 4.

### Phase 2 — source and test review

Production code and tests were read in descending risk order. The review traced event
ownership, queue shutdown, producer callbacks, lifecycle transitions, fallback handling,
metrics registration, Logback attachment semantics, input parsing and routing. It also
examined test assertions, latches, polling, exclusions and fuzz targets.

### Phase 3 — false-positive checks

The following checks were applied before recording findings:

- The latest historical report and the two remediation commits were compared with the
  current source; its fixed findings are not repeated.
- Kafka 4.2.1 sources from the local Maven cache were inspected. `ProducerRecord` makes
  a new list but retains each `Header`; `RecordHeader.value()` exposes its original
  `byte[]`; and `ProducerInterceptor.onSend` is explicitly allowed to mutate a record.
- Existing tests for forced shutdown, foreign client IDs, shared header lists,
  appender-ref attachment and same-name metric bindings were read. They establish only
  adjacent behaviour, not the reported trigger.
- Documentation was treated as a contract where it made a runtime assertion. Two
  findings are already described there as limitations, which confirms their trigger but
  does not remove their operational effect.

## 3. Statistics

| Measure | Result |
|---|---:|
| Production files | 24 Kotlin |
| Production lines / code lines | 5,510 / 2,240 |
| Test files | 29 Kotlin + 3 Java |
| Test lines / code lines | 13,547 / 6,729 |
| Total analysed files | 56 |
| Total lines / code lines | 19,057 / 8,969 |
| JUnit test methods | 321 |
| Jazzer fuzz targets | 3 |
| Findings | 5 (3 Medium, 2 Low) |

The test suite is disproportionately large because it documents each test's intent and
contains detailed component fixtures. This is useful evidence, but it does not replace
coverage of asynchronous ownership after a successful hand-off or mutation by configured
Kafka extensions.

## 4. File-ranking table

| Score | File | Rationale |
|---:|---|---|
| 5 | `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | Public lifecycle, ingress, fallback and metrics wiring. |
| 5 | `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | Shared queue, worker ownership and forced close protocol. |
| 5 | `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | Producer callbacks, breakers and fallback hand-off. |
| 4 | `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | Resource construction and reverse close order. |
| 4 | `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | Producer lifecycle and parallel close. |
| 4 | `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | Kafka queue semantics and diversion claim. |
| 4 | `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | Bind/unbind ownership and shared registries. |
| 4 | `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | Meter identity, registration and removal. |
| 4 | `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | Shared metadata and external Kafka boundary. |
| 3 | `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | Asynchronous fallback delivery. |
| 3 | `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | Spring lifecycle retry and discovery. |
| 3 | `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | Loop prevention across producer threads. |
| 3 | `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | Kafka reliability and security property composition. |
| 3 | `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | Breaker concurrency and time gate. |
| 3 | `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | Concurrent shutdown budget. |
| 2 | `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | Route/encode/enrich composition. |
| 2 | `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | Configuration validation and topic policy. |
| 2 | `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | Marker-driven routing. |
| 2 | `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | Sensitive configuration diagnostics. |
| 2 | `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | Untrusted textual producer configuration. |
| 1 | `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | Small validated lookup. |
| 1 | `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | Enum constants and defaults. |
| 1 | `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | Metrics interface and no-op. |
| 1 | `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | Small guard interface. |
| 5 | `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | End-to-end lifecycle and concurrency contracts. |
| 5 | `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | Forced shutdown and diversion ownership. |
| 5 | `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | Queue close and loss accounting. |
| 5 | `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | Kafka send/callback/breaker paths. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | Producer construction and teardown. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | Reliability property precedence. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | Meter lifecycle and exception handling. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | Spring binding/retry behaviour. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | Concurrent permit throttling. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | Close timing and interruption. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | Validation and cross-class invariants. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | Routing precedence and marker traversal. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | Shared header contract. |
| 4 | `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | Real Logback XML wiring. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt` | Producer-thread loop guard boundary. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | Kafka wire-level contract; separately tagged. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | Published defaults. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | Compatibility with Resilience4j metrics. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | Kafka thread-name dependency. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | Diagnostic safety. |
| 3 | `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | Parser boundary cases. |
| 3 | `./src/test/java/eu/inqudium/tabellarium/TopicRouterFuzzTest.java` | Fuzzed routing input. |
| 3 | `./src/test/java/eu/inqudium/tabellarium/MessageEnricherFuzzTest.java` | Fuzzed enrichment input. |
| 2 | `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | Plan composition. |
| 2 | `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | Lookup validation. |
| 2 | `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | Interface/no-op semantics. |
| 2 | `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | External encoder contract. |
| 2 | `./src/test/java/eu/inqudium/tabellarium/KafkaProducerPropertiesFuzzTest.java` | Fuzzed configuration parser. |
| 1 | `./src/test/kotlin/eu/inqudium/tabellarium/TestEvents.kt` | Test event and polling helper. |
| 1 | `./src/test/kotlin/eu/inqudium/tabellarium/TestSupport.kt` | Shared fixtures. |
| 1 | `./src/test/kotlin/eu/inqudium/tabellarium/ThreadSafeListAppender.kt` | Test-only recorder. |
| 1 | `./src/test/kotlin/eu/inqudium/tabellarium/FixedZeroPartitioner.kt` | Test-only Kafka partitioner. |

## 5. Findings checklist

### Medium

- [x] **M-1 — Shared mutable header arrays cross the Kafka extension boundary**  
  **Confidence:** High · **Category:** data integrity / extensibility  
  **Location:** `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt:88`, `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt:336`  
  **Symptom and cause:** `MessageEnricher` creates five `RecordHeader` objects and
  their `byte[]` values once, then returns the same list and objects for every record.
  The list is immutable but the arrays are not. `ProducerRecord` copies only the list;
  Kafka `ProducerInterceptor.onSend` is allowed to mutate the record and can mutate a
  header value in place. That changes metadata for later records and records still
  waiting for serialization.  
  **Trigger:** Configure an interceptor (or equivalent Kafka extension) that modifies
  a header value returned by `record.headers()`.  
  **Impact:** Component, CMDB, environment or agent metadata can be silently
  misattributed across many log events, including audit records.  
  **Fix strategy:** Preserve the allocation optimisation only behind an immutable
  representation; create fresh `Header` wrappers and clone values at the external
  `ProducerRecord` boundary, or otherwise pass a defensive copy that extensions cannot
  corrupt. Add an interceptor-based regression test that mutates one record and proves
  the next record's headers remain unchanged.
  **Status:** Fixed in `1c11ee9` (2026-09-15). Assessed as a real but narrow trigger: the serializers are
  forced to `ByteArraySerializer` and the partitioner never sees headers, so a configured
  `ProducerInterceptor` (`interceptor.classes`) is the only third party that receives the
  record before serialization. `ResilientMessageSender.buildRecord` therefore builds fresh
  `RecordHeader` wrappers over cloned value arrays exactly when
  `ProducerRegistry.hasProducerInterceptors` is true (derived from the effective producer
  properties, wired by `KafkaTransport.open`); without interceptors the shared instances
  remain, so the measured saving of `PERF_ANALYSIS-2026-08-29T11-01-08` finding 2 (160 B/op)
  is kept for the default deployment. `ResilientMessageSenderTest` writes into a sent
  record's header value with an interceptor configured and proves the shared array and the
  next record are untouched, and pins the shared-instance contract for the no-interceptor
  case. `EnrichedRecord.headers` and README ("Traceability") describe the boundary.

- [ ] **M-2 — Forced close can divert a record after it was handed to Kafka**  
  **Confidence:** High · **Category:** concurrency / delivery correctness  
  **Location:** `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:250`, `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt:205`  
  **Symptom and cause:** `deliverGuarded` leaves an item in `inFlight` between a normal
  return from `deliver(item)` and its `compareAndSet(item, null)`. If the close thread
  exhausts its budget in that interval, it claims the item as `SHUTDOWN_REMAINDER`.
  For a `SendDispatcher`, `ResilientMessageSender.send` has already returned after
  `producer.send` and recorded `eventDispatched`; the shutdown claim is still unused
  and enqueues the original event to the fallback.  
  **Trigger:** A forced shutdown overlaps the small post-send/pre-CAS scheduling
  window.  
  **Impact:** One log event can reach Kafka and the fallback, yielding duplicate
  downstream data. `events.dispatched` and `events.fallback{reason=shutdown}` can also
  describe the same event despite their documented mutually exclusive hand-off meaning.
  The same skeleton can count an already delivered fallback event as dropped.  
  **Fix strategy:** Make the delivery outcome and shutdown ownership transition atomic
  from the closer's perspective. For example, record a distinct completed state before
  returning from delivery and let close divert only a state that has not been handed
  off. Add a deterministic test hook/barrier immediately after successful delivery and
  before ownership release.
  **Status:** Deferred (2026-09-15). Excluded from this remediation cycle on request; the finding stays
  open and unassessed here.

- [x] **M-3 — Two appenders on one logger can form a cross-instance Kafka logging loop**  
  **Confidence:** High · **Category:** resilience / feedback loop  
  **Location:** `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt:84`  
  **Symptom and cause:** The self-logging guard matches only the current transport's
  producer client IDs. A Kafka network-thread event from a second appender is therefore
  accepted by the first, while the reverse is also true. Each appender can ship the
  other's Kafka client logging during broker trouble.  
  **Trigger:** Attach two `KafkaAppender` instances to the same effective logger and
  allow `org.apache.kafka` messages to reach them while producers are reporting a
  connection failure.  
  **Impact:** An outage can amplify logging, consume queue/fallback capacity and make
  the degraded path noisier precisely when it must remain bounded.  
  **Fix strategy:** Maintain a process-wide registry of active producer client IDs,
  entered on start and removed on stop, and consult it in the guard. Until then, enforce
  or prominently warn about disjoint logger attachment. Add a two-appender integration
  test with distinct client IDs.
  **Status:** Fixed in `1c11ee9` (2026-09-15), per the fix strategy. `ClientIdSelfLoggingGuard` now keeps a
  process-wide `ConcurrentHashMap<String, Int>` of every live instance's producer
  network-thread and worker names, reference-counted per name (two instances sharing an
  operator-supplied `client.id`, or the fixed worker names, keep the entry until the last
  of them stops); the hot path is one `containsKey`. Instances enter on construction and
  leave in the new `SelfLoggingGuard.close`, which `KafkaTransport.closeAll` calls last -
  after the producers and workers whose echo it recognizes - and which the `open` rollback
  also runs. `ClientIdSelfLoggingGuardTest` covers the cross-instance drop, the leave on
  close and the reference count; `KafkaAppenderTest` runs two started appenders with
  distinct derived client ids against each other's producer echo (unit-level with
  `MockProducer`, since the echo is a thread name, not a broker interaction). README's
  "Cross-instance guards" now lists only the topic/class exclusivity check as future work.

### Low

- [x] **L-1 — `addAppender` accepts a fallback after startup although the transport cannot use it**  
  **Confidence:** High · **Category:** lifecycle / configuration correctness  
  **Location:** `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:730`  
  **Symptom and cause:** The transport captures `fallbackAppender` once during `start`.
  `detachAppender` correctly refuses changes while started, but `addAppender` accepts a
  first fallback whenever the public slot is empty. The accessor then reports an
  attached fallback even though the running `KafkaTransport` has no fallback dispatcher.  
  **Trigger:** Programmatically call `addAppender` on a started appender that began
  without a fallback.  
  **Impact:** Diversions continue to be dropped while inspection reports a fallback;
  fallback queue metrics are absent and `stop()` later stops an appender that was never
  wired.  
  **Fix strategy:** Refuse `addAppender` while started with the same lifecycle message
  used for detach, or support a fully synchronized transport rewire. Add an
  add-after-start regression test.
  **Status:** Fixed in `1c11ee9` (2026-09-15), first option: `addAppender` on a started appender is refused
  with a status warning that names the refused appender and the wiring the pipeline
  actually has ("without a fallback" or the wired appender's name), mirroring
  `refuseDetachWhileStarted`. Joran is unaffected: `<appender-ref>` is processed before
  the enclosing `<appender>` is started (`JoranXmlConfigurationTest`). Two tests in
  `KafkaAppenderTest` ("Appender-ref handling") cover both warning variants and prove the
  slot stays empty, diversions keep the drop policy and `stop()` leaves the never-attached
  appender alone.

- [x] **L-2 — Same-name appender instances share meters and one stop removes the other's series**  
  **Confidence:** High · **Category:** observability / lifecycle  
  **Location:** `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:200`  
  **Symptom and cause:** Meter identity uses the appender name (or `unnamed`) as its
  only instance discriminator. Two instances with identical names and common tags get
  the same Micrometer meter objects. Both retain those objects in their local removal
  lists, so stopping either removes series the other is still using. The existing
  collision warning only calls out circuit-breaker meters.  
  **Trigger:** Bind two programmatically created or otherwise same-named appenders to
  one `MeterRegistry`.  
  **Impact:** Counters are mixed until one instance stops, then the surviving appender
  silently stops publishing its appender metrics. Dashboards can show an outage as no
  data.  
  **Fix strategy:** Add a per-instance immutable tag (or reject duplicate bindings),
  and test that after the first same-name instance stops the second's counters and
  gauges remain registered. Align README wording with the unique-name requirement if
  retaining name-based identity.
  **Status:** Fixed in `1c11ee9` (2026-09-15), second option: name-based identity is retained and
  `MetricsBindings.bind` refuses a binding whose identity (`appender` tag plus common
  tags) is already live in the registry - probed on the accepted-events counter, which
  every binding registers first and removes on unbind - with one status warning, binding
  neither appender, breaker nor producer meters (`isMeterRegistryBound` stays false). A
  per-instance tag was rejected because it would change the documented inventory and
  cardinality for every deployment to serve a misconfiguration whose fix is a distinct
  name. The former breaker-only collision warning is folded into this check. The
  same-name test in `KafkaAppenderTest` now proves the refusal, that the second instance's
  events reach no shared counter, and that the first instance's counters and gauges
  survive the second instance's stop. README ("Metric inventory") and
  `docs/metrics/metrics-overview.md` state the unique-name requirement.

## 6. Systemic patterns

1. **Ownership is represented by one mutable slot across an asynchronous boundary.**
   `BoundedWorkerDispatcher` uses `inFlight` both as delivery state and shutdown claim;
   that leaves a post-delivery ownership gap (M-2). Model explicit states such as queued,
   delivering, handed-off and rejected, and test each legal transition.
   **Status:** Open together with M-2 (deferred on request, 2026-09-15).

2. **Performance-oriented sharing relies on a convention at a mutable extension
   boundary.** `EnrichedRecord` documents that its header arrays must be read-only, but
   Kafka deliberately permits interceptors to mutate records (M-1). A convention is not
   a safety boundary when third-party extensions receive the object.
   **Status:** Addressed with M-1 (`1c11ee9`): the convention now ends where a third party
   can stand, and the sender copies at that boundary.

3. **Lifecycle state is captured internally while a public mutable representation stays
   editable.** The fallback is snapshotted into `KafkaTransport`, yet `addAppender`
   changes the public slot after start (L-1). Use one source of truth or reject changes
   for the entire running lifetime.
   **Status:** Addressed with L-1 (`1c11ee9`): the slot rejects changes in both directions
   for the running lifetime.

4. **Instance identity is inferred from user configuration.** The appender name avoids
   ordinary metric collisions but cannot uniquely identify programmatic or duplicate
   appenders (L-2). Runtime resource ownership needs an internally generated identity.
   **Status:** Deliberately not followed (`1c11ee9`): the configured name stays the meter
   identity, and a duplicate is refused instead of disambiguated - see L-2.

5. **The test suite uses polling and elapsed-time assertions where deterministic
   synchronization is not available.** `pollUntil` is used throughout the asynchronous
   suite and several shutdown tests assert broad elapsed-time bounds. This is currently
   tolerable because most critical paths use latches, but new race tests should use an
   explicit test hook/barrier rather than timing. The missing post-success ownership
   barrier is the direct reason M-2 escaped.
   **Status:** Open together with M-2 (deferred on request, 2026-09-15).

6. **Known limitations are documented but remain deployable configurations.** The
   cross-instance guard caveat is accurately described in `README.md`; it is still a
   Medium operational defect for a supported programmatic configuration. Documentation
   and a workaround reduce surprise, but do not enforce the safety property.
   **Status:** Addressed with M-3 (`1c11ee9`) for the guard; the topic/class exclusivity
   check remains a documented limitation.

