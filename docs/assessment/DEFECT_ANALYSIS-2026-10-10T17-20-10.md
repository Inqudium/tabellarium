# Code & Defect Analysis: tabellarium

1. Identification
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `5a064f14f12b12c758e3d245d29d7c2bff16eb8b` (full)
   - **Reference (branch/tag):** `refs/heads/main` (merge of PR #89; no tag)
   - **Project version:** `1.1.3-SNAPSHOT`
2. Scope
   - **Included:** production code in `./src/main/kotlin/` and `./src/main/resources/`; test code in `./src/test/kotlin/`, `./src/test/java/` and `./src/test/resources/`; `./pom.xml`; operator-facing contracts in `./README.md` and `./docs/config/kafka-appender-config-guide.md`.
   - **Test-code decision:** test code **is in scope**. It counts both as a risk indicator for the production code and as an analysis subject in its own right under the Phase 2 test heuristics.
   - **Excluded:**
     - build output in `./target/`;
     - the standalone JMH module `./benchmarks/`;
     - `./.git/`;
     - historical reports in `./docs/assessment/`. These are read-only history. They were consulted for prior findings, and those findings were re-verified against the current tree.
3. Environment
   - **Target environment:** Java 21 bytecode (`maven.compiler.release`/`jvmTarget` 21), Kotlin 2.4.20, built with JDK 24+ (CI: JDK 25).
   - **Resolved key dependencies:** Spring Boot BOM 4.1.1, Logback 1.5.38, kafka-clients 4.2.1, Resilience4j circuit breaker 2.4.0, and the optional Micrometer 1.17.1 and Spring Context 7.0.9.
   - **Framework/model:** this is a Logback appender library, not a Spring MVC/WebFlux application. It has no coroutines, no persistence and no transactions. Its concurrency comes from:
     - dedicated platform daemon workers (one send worker per topic class, one fallback worker);
     - Kafka network threads;
     - parallel closer threads.
     
     Logging callers may be virtual threads.
   - **Build system:** Maven. Inspection host: Apache Maven 3.9.15, Oracle JDK 26.0.1, Linux x86_64.
   - **Analysis tools used:**
     - manual data-flow, lifecycle, failure-path and concurrency review;
     - `grep`, `sed`, `git`;
     - `javap` bytecode inspection of Logback 1.5.38 (`LoggerContext.reset`, `Logger.recursiveReset`, `AppenderAttachableImpl.detachAndStopAllAppenders`, `ContextBase.reset`) to verify finding 1;
     - a read-only sub-review of the test suite, whose candidates were re-verified individually against the source.
     
     Project-configured analyzers: ktlint, JaCoCo, CodeQL, OSV/CycloneDX and Jazzer. Not configured: detekt, SpotBugs, Error Prone, NullAway, ArchUnit.
   - **Execution status:** no build, test, fuzz run or reproducer was executed, because the governing prompt requires an explicit go-ahead first. Every finding is verified statically, and finding 1 additionally at the bytecode level of the resolved Logback version.
4. Placement
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium`
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T17-20-10.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`

## 1. Executive summary

The analyzed revision merges two remediation cycles: the fixes for `DEFECT_ANALYSIS-2026-10-10T15-52-19` (`3bf1856`) and a broad code-style refactoring (`1565afa`).

Both findings of the preceding defect analysis are resolved in the current code:
- The offer/re-check window in `BoundedWorkerDispatcher` is now forced deterministically by `BoundedWorkerDispatcherTest`.
- The fallback/send queue gauge pairs register transactionally, with a status warning on failure.

The style refactoring did not weaken the evidential value of the suite in any material way. Helper extraction, constant substitution and `@Nested` grouping were checked for vacuous assertions, and none were found.

No Critical or High defect was found. The core delivery pipeline remains carefully bounded:
- non-blocking caller hand-off;
- finite queues;
- a three-state per-event ownership;
- ordered, budgeted teardown.

The most relevant new finding concerns the operating environment rather than the pipeline itself. Logback's full-context reset stops each logger's appenders in attach order. A fallback appender that is also attached to the root logger *before* the KafkaAppender (a plausible "CONSOLE as fallback" setup) is therefore already stopped when the KafkaAppender's shutdown drain diverts into it. Every shutdown-time diversion is then counted as dropped instead of being written. The configuration guide explicitly calls that sharing safe under full-context shutdown. Two Low production findings concern a silently swallowed info-gauge registration failure and a latent visibility gap in `stop()`.

**Compact test verdict.** The suite is a strong, unit/component-heavy safety net:
- 389 `@Test`, 4 `@ParameterizedTest` and 3 `@FuzzTest` declarations;
- latch-forced concurrency tests for the score-5 protocols;
- two tagged real-broker tests;
- per-test resource cleanup;
- a suite-wide two-minute method timeout.

Score-4/5 lifecycle, ownership and dispatcher paths are covered with tests that would turn red under the obvious mutations. The pyramid is healthy: fast deterministic tests carry the load, and container tests are few and profile-gated.

- The new gauge-pair rollback tests pin *that* the pair disappears, but not *how much* the rollback removes. A mutation that wipes the instance's entire meter set stays green (finding 2).
- Three timing assertions in `ParallelCloseTest` measure with the wall clock (`measureTimeMillis`) against a monotonic production deadline. This is a small flake surface (finding 4).
- The broker integration tests stop their appender outside `finally`. A failing assertion leaks a started appender into the process-wide self-logging registry and causes knock-on failures (finding 5).
- The full-context stop-order hazard of finding 1 has no test. Every existing test stops the KafkaAppender directly, never through `LoggerContext.reset()` with a shared fallback.

## 2. Scope & methodology

**Recon (Phase 1).** Phase 1 mapped the single Maven module and its entry points:
- `KafkaAppender` (Joran/Logback lifecycle and `append` hot path);
- `KafkaAppenderMetricsBinding` (Spring `ContextRefreshedEvent`/`ContextClosedEvent` listeners);
- the Micrometer API seam `KafkaAppenderMeterRegistry.kt`.

There are no controllers, schedulers, message listeners, Reactor chains, coroutines, repositories or transactions. Static analysis beyond ktlint/CodeQL/Jazzer is absent (no detekt/SpotBugs/NullAway), so the review relied on manual depth.

**Production review (Phase 2).** All 28 production files were ranked. The score-5/4 files were read completely:
- `KafkaAppender`, `BoundedWorkerDispatcher`, `SendDispatcher`, `FallbackDispatcher`;
- `ResilientMessageSender`, `DeliveryOwnership`, `HalfOpenThrottle`;
- `KafkaTransport`, `ProducerRegistry`, `ParallelClose`, `ProducerPropertiesBuilder`;
- `MetricsBindings`, `MicrometerKafkaAppenderMetrics`, `RenamingMeterMirror`, `KafkaAppenderMetricsBinding`, `ClientIdSelfLoggingGuard`.

Score-2/1 files were read or skimmed, with emphasis on the code changed since `9d5f5358186cd7f1e34a98d722965cfaadcc8e20`. The pipeline was traced end to end through ingress, routing/encoding, hand-off, send, callback, fallback, close and metrics binding, including the following failure paths:
- forced close;
- worker death;
- synchronous callback;
- re-bind to the same or another registry;
- the Logback context-reset order.

**Test review.** Test code was reviewed with the "How can this test lie?" heuristic and mutation thought experiments. The focus was on the tests added or refactored since the preceding analysis (`BoundedWorkerDispatcherTest`, the fail-once registry tests, the shared helpers in `TestSupport.kt`/`TestEvents.kt`, and `FuzzOracles.java`) and on the score-4/5 concurrency tests.

**Prior reports.** The findings of `DEFECT_ANALYSIS-2026-10-10T13-00-00`, `-13-46-50`, `-15-19-31` and `-15-52-19` were spot-checked against the current code. None has regressed. The Phase 1 ranking below follows the preceding report's table where the files are unchanged in substance, and was re-scored for the changed ones.

**Limitations.**
- No dynamic execution was authorized: no build result, coverage or mutation run is claimed.
- Finding 1 depends on the operator's Logback configuration (attach order of a shared fallback appender).
- The JMM gap in finding 6 is latent and depends on the thread that calls `stop()`.

## 3. Statistics

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 0 |
| Medium | 2 |
| Low | 4 |
| **Total findings** | **6** |
| **Systemic patterns** | **1** |

## 4. File-ranking table

Production scores rate defect likelihood. Test scores rate weight as a safety proof.

| File | Score | Rationale |
|---|---:|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root, hot path, start/stop publication, fallback ownership; findings 1 and 6. |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Queue/worker protocol, offer-close race, worker death, two-phase close. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Breaker permissions, synchronous/asynchronous callback races, outcome accounting. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | 5 | Main lifecycle/concurrency/metrics proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Callback, breaker, throttle and ownership evidence. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Queue/worker/forced-close evidence. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Fallback queue, interruption, worker death, teardown. |
| `./src/test/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcherTest.kt` | 5 | New; latch-forced offer-after-final-drain proofs (sound). |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Fallback delivery refusal on a stopped appender; mechanism of finding 1. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Transactional open, reverse-order close. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 4 | Producer creation rollback, bounded parallel close. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Rejection classification, exactly-once diversion. |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Three-state CAS protocol. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Bind transaction, identity probe, symmetric teardown. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 4 | Hot-path sinks, gauge-pair transaction, info gauges; finding 3. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | Cross-thread mirror/close race. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | 4 | Process-wide ref-counted registry, ThreadLocal guard. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter inventory, hostile and fail-once registries; finding 2. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | 4 | Deadline/interrupt proof; finding 4. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Only wire-level evidence; finding 5. |
| `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | 4 | Deterministic CAS interleavings. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | 4 | Construction rollback, close budgets. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring discovery, idempotence, context ownership. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | Registration/removal concurrency. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt` | 4 | Cross-instance registry lifecycle. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | 4 | Compliance overrides, Kafka cross-property validation. |
| `./src/test/kotlin/eu/inqudium/tabellarium/OptionalDependencyClasspathTest.kt` | 4 | Optional-dependency contract in isolated classloaders. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Layered merge, caps, idempotence/delivery-timeout validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | Lock-free monotonic gate. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Shared deadline, interrupt restoration. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Spring events, recursive appender discovery. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMeterRegistry.kt` | 3 | Optional Micrometer public API. |
| `./docs/config/kafka-appender-config-guide.md` | 3 | Operator contract; the fallback-sharing statement of finding 1. |
| `./pom.xml` | 3 | Dependency, profile and verification lifecycle. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DeliveryOwnershipTest.kt` | 3 | State-machine transitions. |
| `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | 3 | Real Joran binding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | 3 | Docs/constant drift. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | 3 | Mapping validation. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | 3 | Routing and topic-name edges. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | 3 | Route/materialize, encoder failures. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | 3 | Header/key bounds. |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | 3 | Own breaker binder vs. official binder. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt` | 3 | Recommended encoder through the pipeline. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Pure enrichment, bounded key. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | 2 | Immutable route/materialize. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | 2 | Joran configuration validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | 2 | Pure routing, single-level marker hierarchy (documented). |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | Info-label projection. |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | 2 | Startup messages (refactored to top-level functions; behavior unchanged). |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | 2 | Properties parsing wrapper. |
| `./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt` | 2 | Projection examples. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | 2 | Parser examples. |
| `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | 2 | Diagnostic wording/conditions. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | 2 | Lookup behavior. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | 2 | No-op contract. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | 2 | Kafka thread-name compatibility. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | External ingest contract (profile-gated). |
| `./src/test/java/eu/inqudium/tabellarium/*FuzzTest.java` (3) | 2 | Parser/enricher/router fuzz targets. |
| `./README.md` | 2 | Public delivery/config contract. |
| `./src/test/resources/junit-platform.properties` | 2 | Suite-wide timeout. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | 1 | Enum and override tables. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | 1 | Immutable lookup. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | 1 | Interface and no-op. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | 1 | Interface and factory seam. |
| `./src/main/resources/tabellarium-version.properties` | 1 | Build-filtered constant. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestSupport.kt`, `TestEvents.kt`, `ThreadSafeListAppender.kt`, `FixedZeroPartitioner.kt`; `./src/test/java/eu/inqudium/tabellarium/FuzzOracles.java` | 1 | Fixtures and helpers (checked: `pollUntil` throws on timeout, lookup helpers fail loudly). |

## 5. Findings checklist

### 🔴 Critical

No Critical findings.

### 🟠 High

No High findings.

### 🟡 Medium

- [x] 1. [./docs/config/kafka-appender-config-guide.md:583] {Medium} {Confidence: High} {Robustness}
  - Symptom → cause:
    - The guide (and the `addAppender` KDoc at `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:771`) states that sharing the fallback appender with other loggers is fine "unless a full-context shutdown is the only stop path". That is not true in general.
    - `LoggerContext.reset()` (Logback 1.5.38, verified by bytecode) stops appenders via `Logger.recursiveReset()` → `AppenderAttachableImpl.detachAndStopAllAppenders()`, in each logger's attach order. Joran does not register appenders in the `LifeCycleManager`, so nothing stops them earlier.
    - With `<root><appender-ref ref="CONSOLE"/><appender-ref ref="KAFKA"/></root>` and `CONSOLE` also as KAFKA's fallback, `CONSOLE` is stopped first.
    - The KafkaAppender's subsequent `stop()` then drains the send queues and closes the producers. Every diversion produced there (`shutdown` remainder during an outage, callback errors of records aborted by the producer close) reaches `FallbackDispatcher.deliver` (`./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt:134`), which refuses the stopped appender and counts a drop.
    - No test resets a `LoggerContext` with a shared fallback, so the hazard is unexercised.
    - Triggering condition: only on full-context shutdown/reconfiguration when the fallback appender is also attached to a logger that is reset before the KafkaAppender (e.g. listed earlier on root).
    - Impact: data loss of exactly the events the fallback exists for (shutdown during a broker outage). They are counted in `fallback.dropped` and in the stop-time drop warning, but not written. The documented safety guarantee is wrong.
  - Fix strategy: correct the documentation, either by requiring a dedicated (unshared) fallback appender or by requiring the KafkaAppender to be attached before the shared appender. Optionally detect at `start()` that the fallback is also attached to a logger and emit a status warning; or, at `stop()`, report an already-stopped fallback appender specifically instead of as generic drops.
  - **Status:** Fixed in ce72631 (2026-10-10). The guide (§7, "Owned by the appender - use a dedicated one") and the `addAppender` KDoc now require a dedicated fallback appender. They explain that a context reset stops appenders in attach order, and name the remedy for an unavoidable shared fallback (attach the KafkaAppender ahead of it). `stop()` emits a status warning naming the fallback when it is already stopped at teardown. A start-time detection was rejected: Joran attaches the logger appender-refs after the appenders have started, so `start()` cannot see the sharing reliably. Two new `KafkaAppenderTest` Shutdown tests run the real `LoggerContext.reset()`: one with the hazardous order (warning) and one with the remedied order (no warning). Mutation-checked: suppressing the warning turns the first test red.

- [x] 2. [./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:510] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - Both fail-once tests (`:510` fallback pair, `:554` send-queue pair) assert only that the size/capacity pair is absent after the failure, plus the warning (and, at `:510`, the successful retry).
    - Nothing pins the *scope* of the rollback in `registerGaugePair` (`./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:407`). Replacing `registeredMeters.drop(before)` by `registeredMeters.toList()`, or using an off-by-one index, makes the rollback remove every counter, timer and summary of the instance, and both tests stay green.
    - The `:554` test's own description claims "the removed size gauge is no longer tracked", but this is never asserted. Deleting `registeredMeters.remove(meter)` also stays green.
    - Triggering condition: a regression in the rollback slice or tracking, combined with one transient gauge failure in production.
    - Impact: false confidence at a score-4 spot. One transient gauge failure could silently erase the binding's whole appender telemetry while the tests report the transaction as correct.
  - Fix strategy: snapshot the instance's other meters before the failing call and assert they are unchanged afterwards. After a `deregisterFrom`, assert that no meter of the pair remains and that the remaining set equals the snapshot's removal.
  - **Status:** Fixed in ce72631 (2026-10-10). Both fail-once tests now snapshot the meter ids before the failing call and assert that exactly that set remains afterwards. The send-queue test also records every `remove` on the registry and asserts that `deregisterFrom` removes exactly the surviving meters, so the rolled-back gauge is provably untracked. Mutation-checked: `drop(before)` → `toList()` and dropping the untrack step each turn the tests red.

### 🟢 Low

- [x] 3. [./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:231] {Low} {Confidence: High} {Robustness}
  - Symptom → cause: the info gauges (`kafka.appender.info`, one `kafka.appender.topic.info` per topic) are registered inside `safe { … }`, which swallows any exception without a status entry. The fix for the queue gauges (`registerGaugePair` with a `warn` sink, `DEFECT_ANALYSIS-2026-10-10T15-52-19` finding 2) was not applied here. A registry or meter filter that throws partway leaves an incomplete info set (e.g. the appender series without some topic series), and the binding still reports success.
    - Triggering condition: only when the registry/filter throws during info-gauge registration.
    - Impact: dashboards joining on the info labels silently lose series; no diagnostic.
  - Fix strategy: route the info-gauge failure through the existing `warn` sink, and treat the set like the queue pairs (roll back or report the partial set).
  - **Status:** Fixed in ce72631 (2026-10-10). `registerGaugePair` is generalized to `registerAllOrNone(what, consequence, block)`, and the info gauges use it: a failure rolls back the info gauges registered so far and reaches the `warn` sink (Logback status), while the rest of the instance is built normally. New test "should roll back a half-registered info gauge set and warn". Mutation-checked: restoring the silent swallow turns it red.

- [x] 4. [./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt:78] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the three timing assertions (`:78`, `:124`/`:131`, `:147`) measure with `kotlin.system.measureTimeMillis`, which uses the wall clock (`System.currentTimeMillis`). The production deadline in `closeInParallel` runs on `System.nanoTime`. The lower bound at `:131` (`>= budgetMs - 50`) is the tightest. `ProducerRegistryTest` already uses `TimeSource.Monotonic` for the same purpose.
    - Triggering condition: only when an NTP step or VM clock correction occurs during the measured window.
    - Impact: a rare red build without a production defect.
  - Fix strategy: measure with `kotlin.time.measureTime`/`TimeSource.Monotonic`, as `ProducerRegistryTest` does.
  - **Status:** Fixed in ce72631 (2026-10-10). All three measurements use `kotlin.time.measureTime { … }.inWholeMilliseconds` (monotonic clock).

- [x] 5. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt:196] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: both broker tests call `appender.stop()` inline (`:114`, `:196`), not in `finally`. In the second test a 60-second `pollUntil` and two `allSatisfy`/`noneMatch` assertions (`:186`–`:194`) precede the stop. If one fails, a started appender with real producers and workers stays alive. Its thread names also stay registered in the process-wide `ClientIdSelfLoggingGuard` registry, and `-Pintegration` runs in the same JVM as the unit suite. This is the pattern fixed for `LogstashEncoderPipelineTest` (`DEFECT_ANALYSIS-2026-10-10T15-19-31`, finding 11) but not applied here.
    - Triggering condition: only when an assertion in an integration test fails.
    - Impact: knock-on failures (e.g. in the guard's release assertions) that mask the original failure; leaked threads.
  - Fix strategy: wrap start-through-assertions in `try`/`finally { appender.stop() }`, or register the appender in an `@AfterEach` cleanup list like the unit tests.
  - **Status:** Fixed in ce72631 (2026-10-10). `KafkaBrokerIntegrationTest` keeps an `@AfterEach` cleanup list, the same convention the unit tests use: `newBrokerAppender` registers every appender and the cleanup stops it (idempotent) even when an assertion failed before the test's own `stop()`. Verified against a real broker with `-Pintegration` (2 tests green).

- [x] 6. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:585] {Low} {Confidence: Medium} {Concurrency}
  - Symptom → cause:
    - `stop()` reads the plain (non-volatile) `transport` field before touching any volatile state. The field KDoc (`:229`–`:232`) says publication happens through Logback's volatile `started` flag, but `stop()` does not read that flag first.
    - If `stop()` runs on a thread with no happens-before edge to the thread that ran `start()`, the JMM allows it to see `null`. It then takes the "nothing was built" branch, so the producers, workers and guard registration leak, and `stopExecuted` stays unset.
    - In practice the shutdown paths (shutdown-hook registration, context locks) usually provide the edge, so this is latent.
    - Triggering condition: only a cross-thread `stop()` without synchronization to `start()`.
    - Impact: resource leak on that path. The workers are daemons and do not block JVM exit, but the producers keep their broker connections, buffers and MBeans until then, and buffered records are never flushed.
  - Fix strategy: read `isStarted` (volatile) before snapshotting `transport` in `stop()`, or mark `transport`/`plan` `@Volatile`, so that the documented publication contract also holds for the stop path.
  - **Status:** Fixed in ce72631 (2026-10-10). `transport` is `@Volatile`, and its KDoc states why: `stop()` reads it without passing Logback's `started` flag. The hot path pays one volatile read per event (a plain load on x86, a load-acquire on ARM64), which is negligible next to encoding. A JMM visibility gap cannot be shown deterministically by a test; none was added.

## 6. Systemic patterns

1. **Scheduling-time-dependent asynchronous observation in tests remains widespread but bounded.**
   - Occurrences, counted with `grep` over `./src/test/`:
     - 50 `pollUntil` call sites (54 raw textual hits, including the definition and comments): SendDispatcherTest 18, FallbackDispatcherTest 12, KafkaAppenderTest 11, ResilientMessageSenderTest 8, and one each in five other classes;
     - 2 `Thread.sleep` (one inside `pollUntil`, one deliberate 20 ms slow-appender fixture in `FallbackDispatcherTest`);
     - 3 wall-clock `measureTimeMillis` (finding 4).
   - The critical orderings are forced with latches. Polling only observes eventual completion, and the two-minute JUnit method timeout plus CI job timeouts contain hangs.
   - Residual risk: on a starved CI worker, individual two-to-ten-second observation bounds, and the 500 ms interrupt-grace assumption in `SendDispatcherTest` ("should interrupt a worker parked in an interruptible send"), can go red without a production defect. As in the preceding cycles, this is recorded as a pattern rather than as individual findings.
   - **Status:** Deliberately tolerated (2026-10-10), as in the preceding cycles. The wall-clock part is fixed with finding 4 in ce72631; the new shutdown-order tests are synchronous and use no polling.
