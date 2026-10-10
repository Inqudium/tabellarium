# Code & Defect Analysis: tabellarium

1. Identification
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `7e70ade3da6f570aa5c6f227f462c855502a9aac` (full)
   - **Reference (branch/tag):** `refs/heads/main` (merge of PR #90; no tag - `git describe` reports `v1.1.2-85-g7e70ade`)
   - **Project version:** `1.1.3-SNAPSHOT`
2. Scope
   - **Included:** production code in `./src/main/kotlin/` and `./src/main/resources/`; test code in `./src/test/kotlin/`, `./src/test/java/` and `./src/test/resources/`; `./pom.xml`; the operator-facing contracts in `./README.md` and `./docs/config/kafka-appender-config-guide.md` where a finding touches them.
   - **Test-code decision:** test code **is in scope**. It counts both as a risk indicator for the production code and as an analysis subject in its own right under the Phase 2 test heuristics.
   - **Excluded:**
     - build output in `./target/`;
     - the standalone JMH module `./benchmarks/`;
     - `./.git/`, `./.idea/`, `./.github/` (workflows were read only to establish the CI timeouts);
     - historical reports in `./docs/assessment/`. These are read-only history. The preceding report (`DEFECT_ANALYSIS-2026-10-10T17-20-10`) was consulted for its findings and ranking; every adopted statement was re-verified against the current tree.
3. Environment
   - **Target environment:** Java 21 bytecode (`maven.compiler.release`/`jvmTarget` 21), Kotlin 2.4.20, built with JDK 24+ (CI: JDK 25).
   - **Resolved key dependencies:** Spring Boot BOM 4.1.1, Logback 1.5.38, kafka-clients 4.2.1, Resilience4j circuit breaker 2.4.0, the optional Micrometer 1.17.1 and Spring Context 7.0.9.
   - **Framework/model:** a Logback appender library, not a Spring MVC/WebFlux application. No coroutines, no persistence, no transactions, no controllers or message listeners. Its concurrency comes from:
     - dedicated platform daemon workers (one send worker per topic class, one fallback worker);
     - Kafka producer network threads (send callbacks);
     - parallel closer threads at teardown.

     Logging callers may be virtual threads; the hot path is lock-free by convention (CONTRIBUTING.md).
   - **Build system:** Maven. Inspection host: Apache Maven 3.9.15, Oracle JDK 26.0.1, Linux x86_64.
   - **Analysis tools used:**
     - manual data-flow, lifecycle, failure-path and concurrency review of all 28 production files and of the test files named in the ranking table;
     - `grep`, `sed`, `git`;
     - `javap` bytecode inspection of Logback 1.5.38 (`UnsynchronizedAppenderBase.started` is `volatile`; the class carries no reentry guard field in this version, so the library's own `ThreadLocal` guard is not redundant) and kafka-clients 4.2.1 (`InterruptException` restores the interrupt flag in every constructor);
     - two read-only sub-reviews of the test suite (lifecycle/concurrency tests; metrics/configuration tests), whose candidates were re-verified individually against the source before adoption. Candidates that did not survive verification were dropped or downgraded.

     Project-configured analyzers: ktlint, JaCoCo, CodeQL, OSV/CycloneDX and Jazzer. Not configured: detekt, SpotBugs, Error Prone, NullAway, ArchUnit - manual depth was required.
   - **Execution status:** no build, test, fuzz run or reproducer was executed, because the governing prompt requires an explicit go-ahead first. Every finding is verified statically; mutation statements are thought experiments against the source.
4. Placement
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium`
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T19-11-11.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`

## 1. Executive summary

The analyzed revision merges the remediation of the preceding analysis (`ce72631`): the fallback-sharing hazard is documented and warned at `stop()`, the info gauges register all-or-none, the three timing assertions use the monotonic clock, the broker tests register their appenders for cleanup, and the transport reference is volatile. All six prior findings were re-verified as fixed in the current code, and the systemic polling pattern is unchanged.

The production code yields **no Critical, High or Medium finding** in this cycle. The delivery pipeline - non-blocking hand-off, bounded queues, the three-state ownership protocol, ordered and budgeted teardown, the process-wide self-logging registry, the transactional metrics binding - was traced end to end once more, including the context-reset path and the Micrometer mirror. The two production findings are Low: the teardown is not shielded against a thread-creation `Error` the way the start-up transaction is, and the dispatcher skeleton accepts a non-positive drain budget that would turn its bounded join into an unbounded one.

The weight of this cycle therefore lies on the **test suite**, which after six analysis cycles is the component with the most remaining defects. Seven Medium findings concern evidential value and determinism:

- a test whose rationale claims the pre-routing accounting branch while it exercises the post-routing one, leaving the claimed branch untested;
- the three all-or-none rollback tests, which only ever roll back a group of exactly one meter, so a rollback that removes too little is undetectable;
- the meter-inventory test, which pins the set of names and tag keys but not the number of series per name, so a dropped enum member in the pre-resolved tables is silently swallowed by the `safe` wrapper;
- the circuit-breaker call meters, value-tested for the successful kind only, with the consumer/holder lifecycle unpinned;
- the appender-level status reports for a dead worker and for close failures, which no test asserts;
- the nested-appender discovery of the Spring binding, the KDoc's "common case", which no test exercises;
- the broker test that starts the container, creates topics and waits up to 90 s for the Micrometer refresh inside one method under the suite's 2-minute timeout.

**Compact test verdict.** The suite remains a strong, unit/component-heavy safety net:
- 393 `@Test`, 4 `@ParameterizedTest` and 4 `@FuzzTest` declarations, every one with the three-question rationale block;
- latch-forced interleavings for the score-5 protocols (offer-vs-close, forced close with a pinned send, synchronous vs asynchronous callback, the throttle's CAS race);
- per-test cleanup of appenders, contexts and guards; a suite-wide 2-minute method timeout; two tagged real-broker tests.

Fast deterministic tests carry the load; the pyramid is healthy. The gaps are of two kinds:

- **Structure instead of value.** The metrics tests pin names, types and tag keys, not series counts, tag values or call-meter values. A regression that keeps the structure but loses a series or an event wiring stays green (findings 3, 4, 10, 17).
- **Claimed but not exercised.** Four test rationales or KDoc claims name a path the test does not drive: the pre-routing accounting branch (finding 1), the "roll back too little" direction (finding 2), the never-block regression mode (finding 11), and the NO_OP "every hook" claim (finding 16).
- **Residual flake surface.** The 90-second broker poll under the 2-minute timeout (finding 7) and the 500 ms interrupt-grace assumption of two park tests (pattern 1) can go red without a production defect.

## 2. Scope & methodology

**Recon (Phase 1).** The single Maven module has three entry points: `KafkaAppender` (Joran lifecycle and `append` hot path), `KafkaAppenderMetricsBinding` (Spring `ContextRefreshedEvent`/`ContextClosedEvent` listeners) and the Micrometer seam in `KafkaAppenderMeterRegistry.kt`. No controllers, schedulers, message listeners, Reactor chains, coroutines, repositories or transactions exist. Static analysis beyond ktlint/CodeQL/Jazzer is absent (no detekt/SpotBugs/NullAway), so the review relied on manual depth.

**Production review (Phase 2).** All 28 production files were read completely, score 5 first. The pipeline was traced through ingress, routing/encoding, hand-off, send, callback, fallback, close and metrics binding, with emphasis on:
- the paths changed in `ce72631` (the stop-time fallback warning, the volatile transport, `registerAllOrNone`);
- the failure paths: forced close, worker death, synchronous callback, re-bind to the same or another registry, the Logback context-reset order, the Micrometer mirror's close race;
- virtual-thread pinning on the hot path (none: `ReentrantLock`, atomics and `ConcurrentHashMap` only; the `synchronized` blocks of the metrics implementation sit on the bind/unbind path).

**Test review.** The two sub-reviews read every test file in the ranking table completely, together with the production class under test, under the "How can this test lie?" heuristic. Every candidate was re-verified by the main review against the source (file and line), and a number of candidates were dropped: the claimed mutation was caught by another test, or the line reference was wrong.

**Prior reports.** The six findings and the systemic pattern of `DEFECT_ANALYSIS-2026-10-10T17-20-10` were checked against the current code; all are resolved as stated. Claims adopted from that report (the Logback stop order, the 50-site polling count) were re-verified: `pollUntil` has 55 textual hits in `./src/test/` including its definition and comments.

**Limitations.**
- No dynamic execution was authorized: no build result, coverage or mutation run is claimed; every "mutation stays green" statement is a static argument.
- Finding 7 is a budget analysis (container start plus a 90-second poll under a 120-second timeout), not an observed failure.
- The two production findings are latent: finding 8 needs a JVM that can no longer create threads, finding 9 needs a caller that passes a non-positive budget, which no current caller does.

## 3. Statistics

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 0 |
| Medium | 7 |
| Low | 16 |
| **Total findings** | **23** |
| **Systemic patterns** | **2** |

Of the 23 findings, 2 are production findings (both Low) and 21 are test-quality findings (finding 23 was added during the remediation of this report; see its status line).

## 4. File-ranking table

Production scores rate defect likelihood. Test scores rate weight as a safety proof. The table follows the preceding report where a file is unchanged in substance and was re-scored for the files touched by `ce72631` and for those that carry findings in this cycle.

| File | Score | Rationale |
|---|---:|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root, hot path, start/stop publication, fallback ownership; findings 1 (untested branch), 10, 14. |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Queue/worker protocol, offer-close race, worker death, two-phase close; finding 9. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Breaker permissions, synchronous/asynchronous callback races, outcome accounting. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | 5 | Main lifecycle/concurrency/metrics proof; findings 1, 4, 10, 11, 12. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Callback, breaker, throttle and ownership evidence. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Queue/worker/forced-close evidence; finding 13; pattern 1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Fallback queue, interruption, worker death, teardown; pattern 1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcherTest.kt` | 5 | Latch-forced offer-after-final-drain proofs (sound). |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Fallback delivery refusal on a stopped appender. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Transactional open, reverse-order close; findings 5 (untested reporting), 8. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 4 | Producer creation rollback, bounded parallel close. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Rejection classification, exactly-once diversion. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Three-state CAS protocol; finding 15 (untested under contention). |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Bind transaction, identity probe, breaker consumers; finding 4 (untested lifecycle). |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 4 | Hot-path sinks, all-or-none groups, info gauges; findings 2, 3 (test gaps), 17. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | Cross-thread mirror/close race; finding 18 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | 4 | Process-wide ref-counted registry, ThreadLocal guard. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter inventory, hostile and fail-once registries; findings 2, 3, 17. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring discovery, idempotence, context ownership; finding 6. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Only wire-level evidence; finding 7. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | 4 | Deadline/interrupt proof (monotonic clock since `ce72631`). No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | 4 | Deterministic CAS interleavings. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | 4 | Construction rollback, close budgets. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | Registration/removal concurrency; finding 18. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt` | 4 | Cross-instance registry lifecycle; finding 22. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | 4 | Compliance overrides, Kafka cross-property validation; finding 21. |
| `./src/test/kotlin/eu/inqudium/tabellarium/OptionalDependencyClasspathTest.kt` | 4 | Optional-dependency contract in isolated classloaders. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Layered merge, caps, idempotence/delivery-timeout validation; finding 21 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | Lock-free monotonic gate. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Shared deadline, interrupt restoration; finding 8 (caller side). |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Spring events, recursive appender discovery; finding 6 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMeterRegistry.kt` | 3 | Optional Micrometer public API. No finding. |
| `./docs/config/kafka-appender-config-guide.md` | 3 | Operator contract; re-verified against the code (budgets, order, fallback ownership). No finding. |
| `./pom.xml` | 3 | Dependency, profile and verification lifecycle. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DeliveryOwnershipTest.kt` | 3 | Sequential state-machine transitions; finding 15. |
| `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | 3 | Real Joran binding; verified to bind every public setter. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | 3 | Docs/constant drift; finding 20. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | 3 | Mapping validation. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | 3 | Routing and topic-name edges. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | 3 | Route/materialize, encoder failures. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | 3 | Header/key bounds. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | 3 | Own breaker binder vs. official binder (names/types/tags only); finding 4. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt` | 3 | Recommended encoder through the pipeline. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Pure enrichment, bounded key. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | 2 | Immutable route/materialize. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | 2 | Joran configuration validation. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | 2 | Pure routing, single-level marker hierarchy. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | Info-label projection. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | 2 | Startup messages. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | 2 | Properties parsing wrapper. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt` | 2 | Projection examples. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | 2 | Parser examples. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | 2 | Diagnostic wording/conditions. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | 2 | Lookup behavior. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | 2 | No-op contract; finding 16. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | 2 | Kafka thread-name compatibility. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | External ingest contract (profile-gated); restores logger state. No finding. |
| `./src/test/java/eu/inqudium/tabellarium/*FuzzTest.java` (3) | 2 | Parser/enricher/router fuzz targets with real oracles. No finding. |
| `./README.md` | 2 | Public delivery/config contract. No finding. |
| `./src/test/resources/junit-platform.properties` | 2 | Suite-wide timeout; the bound of finding 7. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | 1 | Enum and override tables. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | 1 | Immutable lookup. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | 1 | Interface and no-op. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | 1 | Interface and factory seam. |
| `./src/main/resources/tabellarium-version.properties` | 1 | Build-filtered constant. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestSupport.kt`, `TestEvents.kt`, `ThreadSafeListAppender.kt`, `FixedZeroPartitioner.kt`; `./src/test/java/eu/inqudium/tabellarium/FuzzOracles.java` | 1 | Fixtures and helpers (checked: `pollUntil` throws on timeout, `awaitUninterruptibly` restores the flag, lookup helpers fail loudly). |

## 5. Findings checklist

### 🔴 Critical

No Critical findings.

### 🟠 High

No High findings.

### 🟡 Medium

- [x] 1. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:1707] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The test "should keep accepted and fallback counters conserved when the encoder throws" states in its rationale (`:1708-1712`) that it pins "an event that fails *before routing resolves a class*", i.e. the `topicClassForFailure == null` branch at `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:568-577`.
    - It drives the appender with `ThrowingEncoder`, which fails in `plan.materialize` (`KafkaAppender.kt:539`) *after* `plan.route` succeeded and `m.eventAccepted` fired at `:538`. The branch the rationale names is never entered.
    - Mutation: delete `KafkaAppender.kt:569-577` (the late `eventAccepted` for the null-class case) - the test stays green. No test in `./src/test/` drives a routing failure (a `Marker` whose `getName()` or iterator throws; grep found none), so the branch is untested everywhere.
    - Triggering condition: only on a routing exception (a malformed third-party `Marker`); the accounting branch is then unverified.
    - Impact: false confidence - the test documents coverage of a branch it does not cover. Blast radius is one counter (`events.accepted{topic.class=technical}`), so the severity stays Medium rather than High.
  - Fix strategy: drive the test (or a new one) with an event carrying a throwing `Marker` so that `route` fails, and keep the existing test with a corrected rationale for the materialize path.
  - **Status:** Fixed in 333a415 (2026-10-10). New test "should count an event as accepted under the technical default when routing itself fails" (a `Marker` whose `getName()` throws, added via `LoggingEvent.addMarker`); the encoder test's rationale now names the materialize path. Mutation-checked: deleting the null-class `eventAccepted` turns the new test red.

- [x] 2. [./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:603] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - All three all-or-none tests (fallback pair `:539-562`, send-queue pair `:566-601`, info set `:603-646`) fail the *second* registration of their group: `FailOnceGaugeRegistry` (`:492-515`) can only fail the first registration of a *name*, and the info test configures exactly one topic, so the partial set is always exactly one meter (`[size]` or `[info]`).
    - The rollback in `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:415` is `registeredMeters.drop(before)`. Mutation: `partial = listOf(registeredMeters.last())` - all three tests stay green. With two configured topics and a failure on the second `topic.info`, `kafka.appender.info` plus the first `topic.info` would survive - exactly the "partial info set drops series from joins" symptom that the preceding report's finding 3 fixed.
    - The baseline comparisons added in `ce72631` (`:553`, `:592`) do prove that nothing *else* is rolled back ("too much"); the "too little" direction has no evidence.
    - Triggering condition: a regression in the rollback slice combined with one transient gauge failure past the first meter of a group.
    - Impact: false confidence at the transaction that was just introduced to fix a Medium finding.
  - Fix strategy: add a fail-on-Nth variant of the registry (or a counter on `newGauge`) and run the info test with two topics failing on the second `topic.info`, asserting that neither `info` nor the first `topic.info` remains.
  - **Status:** Fixed in 333a415 (2026-10-10). `FailOnceGaugeRegistry` generalized (`failOnAttempt`, `failingRemoval`); new test "should roll back every info gauge registered before a failure in the middle of the set" (two topics, the second `topic.info` fails; asserts neither `info` nor the first `topic.info` remains, the baseline otherwise untouched, one warning, removals == survivors after `deregisterFrom`). Mutation-checked: `partial = listOf(registeredMeters.last())` turns it red.

- [x] 3. [./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:761] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The inventory test "should attach the appender tag and exactly the documented dimensions to every registered meter" asserts the *set* of `kafka.appender.*` names (`:807`) and the tag *keys* per meter (`:808-817`), never the number of series per name.
    - The pre-resolved tables in `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:122-196` are built from `TopicClass.entries`, `FallbackReason.entries` and `SendOutcome.entries`; every hook wraps `getValue(...)` in `safe {}` (`:291-328`), so a missing key throws `NoSuchElementException` and is swallowed.
    - Across the suite, counters are value-asserted only for `audit`/`technical` and for the reasons `breaker.open`, `throttle`, `encoder.error`, `send.error`, `shutdown`; `functional`, `performance` and `queue.full` are exercised (`:32`, `:477`) but never asserted at value level. `DocumentationContractTest` checks the guide's cardinality against enum sizes, not against a registry.
    - Mutation: `TopicClass.entries.filter { it != PERFORMANCE }` at `:123` (or dropping `QUEUE_FULL` at `:151`) - the suite stays green while `events.accepted{topic.class=performance}` never exists and every increment for that class is silently swallowed.
    - Triggering condition: a regression in the table construction; in production the affected series vanish without any status entry.
    - Impact: false confidence - the documented inventory can be incomplete with the inventory test green.
  - Fix strategy: after `exerciseEveryHook`, assert per name that `registry.find(name).meters().size` equals the expected product of the enum sizes (classes, classes × reasons, classes × outcomes), and assert at least one value per class and per reason.
  - **Status:** Fixed in 333a415 (2026-10-10). New test "should register exactly the series the enum sizes imply and reach every one of them": every hook once per enum combination, series count per name equals the expected product, every per-class/reason/outcome series asserted at value 1. Mutation-checked: filtering `PERFORMANCE` out of the accepted table and dropping `QUEUE_FULL` each turn it red.

- [x] 4. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:3303] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The breaker call meters are wired by hand in `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:364-391`: four event consumers attached once per breaker through `callMeterHolders.getOrPut`, writing through a holder that `unbind` clears (`:269`).
    - Only the `kind=successful` timer is value-tested (`KafkaAppenderTest.kt:3303-3338`). `CircuitBreakerMetricsMirrorTest.kt` compares names, types and tags against the official binder, not values or event semantics (its class KDoc at `MetricsBindings.kt:46-50` says so). No test in `./src/test/` asserts a `kind=failed`, `kind=ignored` or `not.permitted.calls` value (grep).
    - Mutations that stay green: swap `h.failedCalls`/`h.ignoredCalls` (`:370-375`); replace `getOrPut` by an unconditional attach (consumers stack per re-bind, every success counts twice after a re-bind); delete the `clear()` at `:269` (events after unbind keep recording into removed meters). The re-bind test (`KafkaAppenderTest.kt:2936-3008`) checks meter ids and the queue gauge, not breaker timer counts.
    - Triggering condition: a regression in the consumer wiring; a re-bind in production (Spring context cache, manual re-wiring).
    - Impact: false confidence at a score-4 spot - the breaker telemetry operators read during an outage ("failed calls", "not permitted") can be mis-wired or double-counted with the suite green.
  - Fix strategy: one appender-level test that drives one failing send, one ignored exception and one not-permitted call and asserts the three meters; one re-bind test asserting that a success after the re-bind counts exactly once and that a success after unbind counts nowhere.
  - **Status:** Fixed in 333a415 (2026-10-10). Four new `Metrics lifecycle` tests: failed-calls (not ignored), ignored-calls (`RecordTooLargeException`, not failed), not-permitted (breaker forced OPEN through the registry seam), and exactly-once across a re-bind to the same registry plus nowhere after `unbindMeterRegistry`. Mutation-checked: swapping `failedCalls`/`ignoredCalls`, stacking consumers per bind, deleting the `clear()`, dropping the not-permitted increment - each turns a test red.

- [x] 5. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt:190] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The appender's only diagnostics for a dead worker are the `warn(...)` lambdas in `KafkaTransport.kt:190-197` (fallback worker) and `:237-244` (send worker), routed to the status manager; the close-failure reports live in `:303-306`, `:312-325`, `:330-333` and in `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:659-668`.
    - The death hooks are pinned at dispatcher level (`SendDispatcherTest.kt:562`, `FallbackDispatcherTest.kt:582`), and `ProducerRegistryTest.kt:540` proves the registry throws its aggregate - but no test asserts that the *appender* surfaces any of it: `grep -rn "died from|worker died|Error stopping|Error closing|failed to close cleanly" src/test` finds nothing.
    - Mutation: make both `warn(...)` bodies no-ops, or swallow silently in `closeAll` - the suite stays green.
    - Triggering condition: a regression in the wiring; in production a dead worker then masquerades as a slow consumer (the exact failure the hook exists to prevent, per its KDoc).
    - Impact: missing test at a risky spot - the only operator-visible signal for two terminal conditions is unverified.
  - Fix strategy: an appender test with a fallback whose `doAppend` throws an `Error`, asserting the "died from" status warning; one with a producer whose `close` throws, asserting the aggregated close warning.
  - **Status:** Fixed in 333a415 (2026-10-10). Three new `Shutdown` tests: a fallback worker killed by an `Error` from `doAppend`, a send worker killed by an `Error` from the send, and a producer whose `close` throws - each asserting the status warning. Mutation-checked: no-op `onWorkerDeath` warn lambdas and a swallowed close warning turn them red.

- [x] 6. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt:68] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - `KafkaAppenderMetricsBinding.visit` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt:261-280`) recurses into `AppenderAttachable` appenders with an identity-set cycle guard; the class KDoc calls the `AsyncAppender`-wrapped KafkaAppender "the common case".
    - Every fixture in `KafkaAppenderMetricsBindingTest.kt` attaches the KafkaAppender directly to a logger (`:68`, `:317`, `:360`); no test in `./src/test/` builds an `AsyncAppender` or any other `AppenderAttachable` wrapper (grep). The cycle guard (`:266`) and the `unbindAppenders` catch (`:223-229`) are untested as well.
    - Mutation: delete `:273-279` - the suite stays green, and an `AsyncAppender`-wrapped KafkaAppender is never bound in production.
    - Triggering condition: a regression in the walk; any deployment wrapping the appender.
    - Impact: missing test for a documented path; metrics silently dark for wrapped appenders.
  - Fix strategy: one test with the KafkaAppender attached to an `AsyncAppender` on the logger, asserting the bind; one with a two-appender attachment cycle, asserting termination.
  - **Status:** Fixed in 333a415 (2026-10-10). New nested `Appender discovery` in `KafkaAppenderMetricsBindingTest`: a `KafkaAppender` wrapped in a real `AsyncAppender` is bound (meters tagged with its name), and a cyclic attachment of two `AppenderAttachable` wrappers terminates and binds the inner appender once. Mutation-checked: skipping the `AppenderAttachable` recursion turns both red.

- [x] 7. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt:179] {Medium} {Confidence: Medium} {Test quality}
  - Symptom → cause:
    - "should mirror the producer's broker-dependent meters and remove them on stop" starts the Testcontainers broker (`:179-181`, including the image pull on a cold runner), creates the topics, starts the appender and then polls up to `METER_REFRESH_TIMEOUT` = 90 s (`:202-204`, `:284`) for Micrometer's 60-second `KafkaClientMetrics` refresh - all inside one `@Test` method.
    - `./src/test/resources/junit-platform.properties` sets `junit.jupiter.execution.timeout.test.method.default = 2 m`; no `@Timeout` override exists anywhere (grep). CI runs the class (`./.github/workflows/ci.yml:82`). JUnit's default method order is not declaration order, so the pull can land in this test.
    - Triggering condition: only when container start plus pull exceeds ~30 s on the runner (the refresh alone consumes ≥ 60 s of the 120 s).
    - Impact: a red integration job without a production defect; after the first failure the appender is stopped by the new `@AfterEach`, so no knock-on.
  - Fix strategy: start the container once per class in a `@BeforeAll` (or pre-pull), or raise the bound for this test with a class-level `@Timeout` that covers start plus refresh.
  - **Status:** Fixed in 333a415 (2026-10-10). Class-level `@Timeout(5, MINUTES)` with a KDoc rationale (container start plus image pull plus the 60-second `KafkaClientMetrics` refresh against the 2-minute default). A `@BeforeAll` container was rejected: each test owns its container through `use`. Verified against a real broker (`-Pintegration -Dgroups=integration`): 2 tests green in 67 s.

### 🟢 Low

- [x] 8. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt:290] {Low} {Confidence: Medium} {Resources}
  - Symptom → cause: `closeAll` calls `closeInParallel` (`./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt:34-40`) outside any `try`; `Thread.start()` there can throw `OutOfMemoryError: unable to create native thread`. The `Error` propagates out of `closeAll` and out of `KafkaAppender.stop()` (`:635`), skipping the producer close, the fallback dispatcher close, the guard's registry exit, the metrics unbind and the fallback/encoder stop. `KafkaTransport.open` deliberately shields its rollback against exactly this `Error` (`:254-262`); the teardown does not.
    - Triggering condition: only under thread exhaustion at shutdown.
    - Impact: leak of producers, workers and registry entries on that path; a `LoggerContext.reset()` aborts mid-way.
  - Fix strategy: treat the parallel close like the open transaction - contain `Throwable` around `closeInParallel` (fall back to sequential close or to accounting the remainder on the calling thread) and let `closeAll` finish its sequence.
  - **Status:** Fixed in 333a415 (2026-10-10). `closeInParallel` got a `startCloser` seam and runs a task whose closer thread cannot be created (`OutOfMemoryError`) on the calling thread, after the closers that did start; `closeAll` and `ProducerRegistry.close` therefore complete their sequence. New test "should run a task on the calling thread when its closer thread cannot be created" (seam throws for the second of three tasks). Mutation-checked: letting the `Error` propagate turns the build red - as a Surefire fork error, because an escaping `OutOfMemoryError` ends the fork, which is exactly the teardown abort the fix prevents. CHANGELOG entry added.

- [x] 9. [./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:97] {Low} {Confidence: High} {Robustness}
  - Symptom → cause: `drainTimeoutMs` is not validated. `0` turns `worker.join(drainTimeoutMs)` (`:199`) into `join(0)` = wait forever (the CAUTION at `:211-213` covers only the grace constant), so a close with a delivery parked in a blocking appender never reaches the interrupt phase; a negative value makes `join` throw `IllegalArgumentException`, so `close()` aborts before `rejectRemaining` (`:228`) and the queued items are neither delivered nor accounted. Production passes the constants 1000/5000 and tests pass 100..60 000 (grep), so the path is latent.
    - Triggering condition: only a future caller passing a non-positive budget.
    - Impact: hang or unaccounted loss on that path.
  - Fix strategy: `require(drainTimeoutMs > 0)` in the constructor with a message naming the join semantics.
  - **Status:** Fixed in 333a415 (2026-10-10). `require(drainTimeoutMs > 0)` in an `init` block placed before the worker starts; parameterized test for `0` and `-1` asserting the message (names `join(0)` and the value) and that no worker thread was started. Mutation-checked: `require(true)` turns both cases red. CHANGELOG entry added.

- [x] 10. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:642] {Low} {Confidence: High} {Maintainability}
  - Symptom → cause: `stop()` resets the appender's own `metrics` to `NO_OP` (`:644`) but, unlike `withdrawMetrics` (`:772-773`), never calls `transport.setMetrics(NO_OP)`; the sender and dispatchers keep recording into meters the unbind already removed. Harmless in itself (detached meter objects), but `KafkaAppenderTest.kt:2650-2656` relies on this unstated property as the observable for the late-path proof ("the sender still holds its Micrometer implementation, whose meter objects keep counting after stop()"). Making `stop()` symmetric breaks that test loudly, and nothing documents the dependency on the production side.
    - Triggering condition: a future symmetry fix.
    - Impact: maintainability - an implicit contract between a test and an accidental property.
  - Fix strategy: decide the property (keep and document it as intended at `stop()`, or make `stop()` symmetric) and give the test an explicit observable (a recording metrics implementation installed via `setMetrics`).
  - **Status:** Decided in 333a415 (2026-10-10): the property is intended. `stop()` now carries the rationale (the hot path's own reference is reset; the transport's recorders keep the bound implementation, unlike `withdrawMetrics` which serves a still-running appender; the closed components record into removed, detached meters, which is what the pinned-send test observes), and the test comment references that rationale. No behavior change.

- [x] 11. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:2023] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the two context-reset tests (`:2023-2059`, `:2062-2090`) prove that the stop-time warning is emitted only in the hazardous attach order; no event is in the teardown in either, so the warning's claim ("diverted events will be counted as dropped instead of written") is not exercised through the real reset. The consequence itself is pinned for a never-started fallback (`:1573`), so a regression of the refusal is caught elsewhere; only the combination reset + shared fallback + event in flight is unproven.
  - Fix strategy: park one send uninterruptibly before `context.reset()`, assert the drop warning in the hazardous order and the event in the shared recorder in the remedied order.
  - **Status:** Fixed in 333a415 (2026-10-10). Two new `Shutdown` tests with an interruptibly parked send across the real `LoggerContext.reset()` (new `InterruptiblyParkedProducerFactory` fixture; the dispatcher's interrupt after the drain budget unblocks the send with an `InterruptedException` the sender routes to the fallback): the hazardous order asserts "Fallback dispatcher dropped 1 event(s)" and an empty recorder, the remedied order asserts the event in the shared recorder and no drop warning. Both warning-only tests kept.

- [x] 12. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:2462] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the never-block test's rationale claims that "a regression to a synchronous send parks the helper, not the test thread". The first `doAppend("first")` (`:2490`) runs on the test thread; with a synchronous send it would park in `release.await()` (`:2431`). The await is interruptible, so the suite's 2-minute timeout ends it as a bounded red (and `append` routes the `InterruptedException` to the fallback), but the red arrives after 2 minutes instead of 2 seconds and the comment misleads.
  - Fix strategy: issue the first append on the helper as well (as `SendDispatcherTest.kt:115-120` does), or correct the rationale.
  - **Status:** Fixed in 333a415 (2026-10-10). Both appends of the never-block test run on helper threads with asserted latch results; the rationale's claim is now true.

- [x] 13. [./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt:812] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: `assertThat(item.ownership.tryHandOff()).isTrue()` runs inside `sendAction` on the worker thread. An `AssertionError` there is a worker death (`BoundedWorkerDispatcher.kt:150-156`), surfaces only as the `handedOff` latch timeout at `:821`, and the failure message names the latch, not the hand-off.
  - Fix strategy: record the result in an `AtomicBoolean` and assert it on the test thread.
  - **Status:** Fixed in 333a415 (2026-10-10). The `tryHandOff` result is recorded in an `AtomicBoolean` on the worker and asserted on the test thread after the `handedOff` latch.

- [x] 14. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:603] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the `stopExecuted` guard (`:603-607`) exists so a repeated `stop()` does not re-run the teardown and re-emit the drop warning. `@AfterEach` re-stops every appender, but no test asserts single emission of "Fallback dispatcher dropped N" or a single unbind after a double stop (`:1703` uses `anyMatch`). Mutation: drop the early return - `closeAll` re-runs, the warning (`KafkaTransport.kt:315-321`) is emitted twice, the suite stays green. Dispatcher-level idempotence is pinned (`FallbackDispatcherTest.kt:480`), so the double-count itself is covered; only the appender-level guard is unpinned.
  - Fix strategy: in one shutdown test, call `stop()` twice and assert exactly one drop warning.
  - **Status:** Fixed in 333a415 (2026-10-10). New test "should run the teardown and its drop warning only once when stopped twice". Mutation-checked: removing the `stopExecuted` early return turns it red.

- [x] 15. [./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt:59] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: `DeliveryOwnershipTest.kt` is sequential, and every concurrent ownership test serializes the close thread and the worker through latches (`SendDispatcherTest.kt:726`, `:781`; `KafkaAppenderTest.kt:2602`). Mutation: rewrite the three transitions as `if (state.get() == X) { state.set(Y); true } else false` - every test stays green. The protocol's atomicity, its whole point, is never contended. A deterministic seam like the throttle's barrier-in-clock does not exist here; a probabilistic N-thread race asserting exactly one `true` per ownership would catch the naive rewrite with high probability.
  - Fix strategy: add an N-thread latch race on one ownership per transition, asserting exactly one winner.
  - **Status:** Fixed in 333a415 (2026-10-10). New nested `Contended transitions` in `DeliveryOwnershipTest`: up to 8 racers (bounded by the core count), 5000 spin-released rounds per transition, exactly one winner per round for `tryDivert`, `tryHandOff` and both legs of `tryDivertAfterSend`; 17-78 ms per test. Mutation-checked: check-then-set rewrites of `tryDivert`, `tryHandOff` and the HANDED_OFF leg turn the tests red in 3 of 3 runs each (a `CyclicBarrier` release caught the mutation in only 1-2 of 3 runs and was replaced by the spin release).

- [x] 16. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt:12] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: "should accept all hook calls without throwing or allocating" (`:12-46`) omits `eventSize` and `registerSendQueueGauges` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt:99`, `:137`), so its "all hook calls" claim is false; it carries no assertion (acceptable for a no-op, the omission is the defect).
  - Fix strategy: call every method of the interface in the test.
  - **Status:** Fixed in 333a415 (2026-10-10). The NO_OP test calls `eventSize` and `registerSendQueueGauges` for every class.

- [x] 17. [./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:417] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the rollback-failure branches (`:417-423`, "stays tracked, so deregisterFrom still tries again"; `:471-477`) have no test. `HostileRegistry` (`MicrometerKafkaAppenderMetricsTest.kt:393-427`) throws on `newGauge`, so the partial list is always empty and its throwing `remove` (`:426`) is never reached by a rollback. Dropping the `addSuppressed`/keep-tracked logic is undetectable. Also, only the send-queue test proves the rolled-back meter untracked (`:595-600`); the fallback-pair and info tests do not.
  - Fix strategy: a registry that registers successfully and fails on `remove`, asserting the suppressed cause and that `deregisterFrom` retries the meter.
  - **Status:** Fixed in 333a415 (2026-10-10). New test "should keep a meter whose rollback removal failed tracked and report the removal failure" (capacity registration fails, the first `remove` throws: the size gauge survives, the warned cause carries the removal failure as suppressed, `deregisterFrom` removes it later); the fallback-pair and info rollback tests now also assert removals == survivors after `deregisterFrom`. Mutation-checked: dropping `addSuppressed` and untracking before the removal each turn it red.

- [x] 18. [./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt:162] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: "should remove every mirror on close, drop later additions" registers into `mirror.source` after `close()`; `close()` closes the source (`RenamingMeterMirror.kt:155`), and Micrometer 1.17.1 returns a no-op meter for a closed registry before any `onMeterAdded` listener runs (verified by the sub-review via `javap`). The mirror's own guard (`:79`) is therefore never exercised; deleting it stays green (the post-put re-check at `:86` is covered by the race test `:189-246`). Related: the race test waits for the closer thread to be `BLOCKED`/`WAITING` (`:236`), which encodes that `MeterRegistry.close()` takes `meterMapLock`; a lock-free Micrometer close would turn that test red without a mirror defect.
  - Fix strategy: test the guard with a source that is not closed (a second mirror over the same source, or a late `onMeterAdded` invoked directly); accept `TERMINATED` in the race test.
  - **Status:** Partly fixed in 333a415 (2026-10-10): the close-race poll accepts `TERMINATED`, and its rationale names the post-put re-check as the subject. Declined: exercising the guard at `RenamingMeterMirror.kt:79` needs a production seam (the source registry is created internally and closed right after the sweep); the re-check at `:86` covers the observable outcome, so no production change was made.

- [x] 19. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:544] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: three smaller coverage gaps in the hot path and the skeleton: (a) an `Error` escaping `append` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:544-551`: counted as accepted, not dispatched, propagates) has no test; (b) `includeCallerData = false` has no negative control (only the positive at `KafkaAppenderTest.kt:1493`), so a regression that always walks the stack passes; (c) the catch-path CAS in `BoundedWorkerDispatcher.kt:285` has no test where the fallback `doAppend` throws *after* a forced close claimed the item - for the `FallbackDispatcher` a mutation to `inFlight.set(null); reject(...)` double-counts and nothing catches it (`FallbackDispatcherTest.kt:442` covers the throw without a prior claim).
  - Fix strategy: one test per gap; (c) needs a `doAppend` that parks until the close claimed the item and then throws.
  - **Status:** Fixed in 333a415 (2026-10-10). (a) "should let an Error escape doAppend and count the event as accepted but neither dispatched nor diverted" (a `LinkageError` from the encoder); (b) "should not compute caller data on the caller thread when includeCallerData is off" (`hasCallerData()` false); (c) "should not count a delivery that throws after a forced close claimed it a second time" (a started appender parks uninterruptibly, then throws on release; dropped count stays 1). Mutation-checked: widening the hot-path catch to `Throwable`, always computing `callerData`, and `inFlight.set(null); reject(...)` each turn the respective test red.

- [x] 20. [./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt:67] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: the budget checks at `:67`, `:71`, `:100`, `:108` format with integer division (`/ 1000`), so a constant changed to 2500 ms still matches a stale "2 s" (`:72` uses `1000.0` correctly). The metrics contract pins names and tag *keys*, not tag *values*; renaming a `FallbackReason.tag` or `SendOutcome.tag` leaves `./docs/metrics/metrics-guide.md:188-209` stale with the test green.
  - Fix strategy: format with the same precision the guide uses, and pin the enumerated tag values.
  - **Status:** Fixed in 333a415 (2026-10-10). A `seconds(ms)` helper renders the budgets with the guide's precision (1000 → `1`, 500 → `0.5`, 2500 → `2.5`) at every former `/ 1000` site; new test "should list exactly the tag values the enums define" parses the guide's tag-value subsections for `topic.class`, `reason` and `outcome` and pins the value sets and the "N possible values" counts. The guide was not stale; no docs edit. Mutation-checked: `DEFAULT_DRAIN_TIMEOUT_MS = 1500` and `THROTTLE("throttled")` each turn a test red.

- [x] 21. [./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt:448] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: untested validation branches in `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt`: `acks=-1` accepted under idempotence (`:197`; mutation `acks == "all"` only stays green - no test uses `-1`), unparseable `retries`/`max.in.flight` skipped (`:202`, `:208`), unparseable `delivery.timeout.ms`/`linger.ms`/`request.timeout.ms` skipped (`:232-247`).
  - Fix strategy: one parameterized case per branch.
  - **Status:** Fixed in 333a415 (2026-10-10). Three new tests: `acks=-1` accepted as the alias of `all` under idempotence, unparseable `retries`/`max.in.flight` left to the Kafka client, and a parameterized case per unparseable timeout (`delivery.timeout.ms`, `linger.ms`, `request.timeout.ms`) against a pair the rule otherwise refuses. Mutation-checked: `acks == "all"` only turns the alias test red.

- [x] 22. [./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt:252] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the test asserts that the worker names leave the static `liveEchoThreadNames` after the last close - true only while no other guard is live JVM-wide. Every appender-starting class cleans up today (`@AfterEach` or `finally`, verified), so it holds, but any future leaked appender anywhere in the suite turns this test order-dependent; the comment at `:281-285` acknowledges it.
  - Fix strategy: assert on the count delta of this test's own names rather than on absence, or isolate the registry behind a test seam.
  - **Status:** Declined (2026-10-10): the registry is private static and the absence after the last close is the contract under test; every appender-starting class cleans up (re-verified in this cycle), and a leaked appender anywhere in the suite would be a defect worth a red test rather than one to tolerate. Revisit if the test ever goes red in CI without a leak.

- [x] 23. [./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt:442] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: *added during remediation (observed by the dispatcher sub-task, re-verified).* "should count an event as dropped when the fallback appender throws" built its throwing appender without starting it. `FallbackDispatcher.deliver` (`./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt:134`) refuses a not-started appender *before* calling `doAppend`, so the asserted drop came from that refusal and the throw the rationale names was never reached. Mutation: dropping the `DELIVERY_FAILED` rejection in `BoundedWorkerDispatcher.deliverGuarded` (`:285-287`) kept the test green.
  - Fix strategy: start the appender so the throw is what produces the drop.
  - **Status:** Fixed in 333a415 (2026-10-10). The appender is started and the Given comment explains why. Mutation-checked: dropping the `DELIVERY_FAILED` rejection now turns the test red ("Condition did not become true within 2000ms").

## 6. Systemic patterns

1. **Scheduling-time-dependent asynchronous observation in tests remains widespread but bounded.**
   - Occurrences, counted with `grep` over `./src/test/`:
     - 55 textual `pollUntil` hits (definition and comments included): SendDispatcherTest 18, FallbackDispatcherTest 12, KafkaAppenderTest 11, ResilientMessageSenderTest 8, one each in five other classes;
     - 2 `Thread.sleep` (one inside `pollUntil`, one deliberate 20 ms slow-appender fixture in `FallbackDispatcherTest.kt:380`);
     - 2 tests that assume the interrupted worker finishes within `INTERRUPT_GRACE_MS` = 500 ms before `rejectRemaining` claims the item (`SendDispatcherTest.kt:391-455` asserts `containsExactly(SEND_ERROR)`, `FallbackDispatcherTest.kt:396-439` asserts `droppedEventCount == 0`): a ≥ 500 ms scheduling stall after `interrupt()` flips the reason or the count.
   - The critical orderings are forced with latches; polling only observes eventual completion; the 2-minute JUnit method timeout and the 30-minute CI job timeout contain hangs. The wall-clock measurements of the preceding cycle are gone.
   - Residual risk: on a starved CI worker, individual bounds can go red without a production defect. As in the preceding cycles, recorded as a pattern rather than as individual findings; finding 7 is the one case where the bound is tight enough to warrant its own entry.
   - **Status:** Deliberately tolerated (2026-10-10), as in the preceding cycles. Finding 7's bound is fixed in 333a415; the two grace-dependent park tests are unchanged - the 500 ms grace is a production constant, and a seam only for the tests was not worth the surface.

2. **Metrics tests pin structure, not values.**
   - Occurrences (~4, by reading): the inventory test (`MicrometerKafkaAppenderMetricsTest.kt:761`, names and tag keys, no series counts - finding 3); the breaker mirror test (`CircuitBreakerMetricsMirrorTest.kt:352-367`, names/types/tags, no event semantics - finding 4); the docs contract (`DocumentationContractTest.kt:127-198`, names and tag keys, no tag values - finding 20); the re-bind test (`KafkaAppenderTest.kt:2936-3008`, meter ids and one gauge, no counter values after the re-bind - finding 4).
   - Consequence: a regression that keeps every meter id but loses a series, a value or an event wiring stays green. The fix is the same shape everywhere: one value-level assertion per dimension next to the structural one.
   - **Status:** Fixed in 333a415 (2026-10-10) through findings 3 (series counts and values per dimension), 4 (call-meter values across kinds and re-bind) and 20 (tag values in the docs contract); the breaker mirror test keeps its name/type/tag scope, the event semantics are now covered at appender level.
