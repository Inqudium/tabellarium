# Code & Defect Analysis: tabellarium

1. Identification
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `9d5f5358186cd7f1e34a98d722965cfaadcc8e20` (full)
   - **Reference (branch/tag):** `refs/heads/main`
   - **Project version:** `1.1.3-SNAPSHOT`
2. Scope
   - **Included:** production code in `./src/main/kotlin/` and `./src/main/resources/`; test code in `./src/test/kotlin/`, `./src/test/java/`, and `./src/test/resources/`; build and CI configuration in `./pom.xml` and `./.github/workflows/ci.yml`; operator-facing contracts in `./README.md` and `./docs/config/kafka-appender-config-guide.md`.
   - **Test-code decision:** test code is explicitly in scope, both as a risk indicator for production code and as an analysis subject under the test-specific heuristics.
   - **Excluded:** build output in `./target/`; the standalone JMH module in `./benchmarks/`; generated documentation/assets; historical reports in `./docs/assessment/` (read-only history, consulted only for candidates and re-verified against the current tree); `./.git/`.
3. Environment
   - **Target environment:** Java 21 bytecode, Kotlin 2.4.20; CI uses JDK 25; dependencies are managed by the Spring Boot BOM 4.1.1.
   - **Framework/model:** Logback appender library using Kafka producers, Resilience4j circuit breakers, and optional Micrometer/Spring integration. It is not a Spring MVC/WebFlux application; there are no coroutines and no persistence layer. Potentially blocking Kafka and fallback work uses dedicated platform daemon threads; logging callers may be virtual threads.
   - **Build system:** Maven; inspection host Apache Maven 3.9.15 on Oracle JDK 26.0.1, Linux x86_64.
   - **Analysis tools used:** manual static data-flow, lifecycle, failure-path and concurrency review; `rg`, `sed`, `nl`, `git`, and Maven/JDK version inspection. Project-configured analyzers: ktlint, JaCoCo, CodeQL, OSV/CycloneDX, and Jazzer. Not configured: detekt, SpotBugs, Error Prone, NullAway, or ArchUnit.
   - **Execution status:** no build, test, integration-test, fuzz run, or reproducer was executed; the governing prompt requires explicit go-ahead before local execution. Findings are statically verified.
4. Placement
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium`
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-52-19.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`

## 1. Executive summary

The current revision is the merge of the remediation for the preceding defect analysis. The twelve findings recorded in `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md` were checked against the current code and are resolved in the implementation: the Kafka timeout guidance and pre-validation agree, the metrics re-bind preserves a working binding, shared `client.id` use is signalled, startup rollback observes the additional resources, and the concurrency tests added by that remediation force the intended interleavings. No Critical or High production defect was found. The delivery pipeline remains carefully bounded: caller-side hand-off is non-blocking, dispatcher queues are finite, the producer and fallback lifecycles are ordered, and per-event ownership prevents the ordinary shutdown/send-error races from double-diverting an event.

Two residual weaknesses remain. The Medium finding is a test-evidence gap: the `running` re-check after a successful queue offer is the only protection against an offer landing after close/death performed its final drain, but no test reaches that exact interleaving. The Low finding is in optional metrics robustness: fallback-queue gauge registration marks itself complete before either gauge is known to have registered, while the exception wrapper suppresses the failure without a Logback status entry; a selective or transient registry failure can therefore leave queue observability permanently incomplete for that binding.

**Compact test verdict.** The suite is a strong, unit/component-heavy safety net: 394 test/parameterized/fuzz declarations, dedicated concurrency tests, two tagged real-broker integration tests, three fuzz targets, per-test appender cleanup, and a suite-wide two-minute JUnit timeout plus 30-minute CI job timeouts. The score-4/5 lifecycle and delivery paths are unusually well covered, and the recent remediation tests are materially stronger than timing-only tests. The suite nevertheless cannot yet be considered complete proof of the queue hand-off protocol.

- The exact offer-after-final-drain race in `BoundedWorkerDispatcher` remains unforced and could regress without a deterministic red test (finding 1).
- The hostile-registry test proves only that metric hooks do not throw; it does not detect permanently missing fallback gauges or the absent diagnostic (finding 2).
- Asynchronous assertions still rely broadly on bounded polling and scheduling time, but global method/job timeouts contain hangs; this remains a flake surface rather than a separate confirmed defect.

## 2. Scope & methodology

Phase 1 mapped the single root Maven module and its operator surface: `KafkaAppender` is the Logback/Joran entry point; `KafkaAppenderMetricsBinding` listens for Spring context events; `KafkaAppenderMeterRegistry.kt` is the optional Micrometer API seam. There are no controllers, schedulers, messaging listener containers, Reactor chains, transactions, repositories, or database resources. The concurrency model consists of one send worker per active topic class, one optional fallback worker, Kafka network threads, and bounded parallel closer threads.

All 28 production Kotlin files were considered and ranked before detailed review. The score-5/4 path was traced across `KafkaAppender` → `RecordPlan` / `KafkaTransport` → `SendDispatcher` / `ResilientMessageSender` / `DeliveryOwnership` → `ProducerRegistry` / `FallbackDispatcher`, including startup rollback, worker death, forced close, callback races, and metrics binding. Because the current commit is a remediation merge, the full diff from `3323614ffb8955a804dc111eaa4362470d837c15` to the analyzed commit was reviewed in depth. Unchanged core files were cross-checked against the preceding full analysis; prior conclusions were not adopted unless the present source still supported them.

Test code is in scope. The review applied the “How can this test lie?” heuristic to the 33 Kotlin and 3 Java test files, concentrating on concurrency/lifecycle tests and every test changed by the remediation. Assertions, worker-failure propagation, resource cleanup, timeout bounds, static-state isolation, and real-infrastructure coverage were checked. Counts used for the systemic observation were obtained with `rg` over `./src/test/`: 55 textual `pollUntil` occurrences (the helper plus its call sites) and two `Thread.sleep` occurrences (one in the polling helper and one deliberate slow-appender test).

Known limitations: no dynamic execution was authorized, so no build result, mutation run, coverage report, scheduler-stress result, or real-broker observation is claimed. The metrics finding requires a registry implementation or filter path that throws selectively/transiently during the fallback-gauge registration; ordinary `SimpleMeterRegistry` does not trigger it. The dispatcher test gap is a statement about missing evidence, not a claim that the current implementation loses an event.

## 3. Statistics

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 0 |
| Medium | 1 |
| Low | 1 |
| **Total findings** | **2** |
| **Systemic patterns** | **1** |

## 4. File-ranking table

Production scores reflect defect likelihood. Test scores reflect evidential weight and the risk of false confidence. Documentation/configuration is ranked where it defines executable operator or verification behavior.

| File | Score | Rationale |
|---|---:|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root, lock-free hot path, start/stop publication, rollback and metrics seam. |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Shared queue/worker protocol, offer-close race, worker death and forced shutdown; finding 1. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Circuit-breaker permissions, synchronous/asynchronous callback races and outcome accounting. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | 5 | Main lifecycle, concurrency, rollback and metrics integration proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Callback, breaker, throttle and delivery-ownership evidence. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Queue/worker/forced-close evidence; does not force the offer-close window in finding 1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Fallback queue, interruption, worker-death and teardown evidence. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Transactional resource construction and reverse-order teardown. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 4 | External resource creation, Throwable rollback and bounded parallel close. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Rejection classification and exactly-once diversion hand-off. |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Asynchronous fallback lifecycle and drop accounting. |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Three-state atomic delivery/diversion protocol. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Multi-binder transaction, identity collision prevention and symmetric teardown. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 4 | Hot-path metric sinks, registration lifecycle and fallback-gauge state; finding 2. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | Cross-thread meter mirroring and close/register race. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | 4 | Process-wide ref-counted registry and ThreadLocal recursion guard. |
| `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | 4 | Deterministic CAS interleaving proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | 4 | Construction rollback, close budgets and failure aggregation. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring discovery, idempotence, retry and context ownership. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter inventory and hostile-registry behavior; misses the persistence in finding 2. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | Registration/removal concurrency and teardown evidence. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt` | 4 | Cross-instance registry and ref-count lifecycle evidence. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | 4 | Deadline, interruption and parallel-close proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | 4 | Compliance overrides and Kafka cross-property validation. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Only wire-level real-broker evidence; profile-gated. |
| `./src/test/kotlin/eu/inqudium/tabellarium/OptionalDependencyClasspathTest.kt` | 4 | Isolated-classloader proof of the optional-dependency contract. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Layered merge, caps, idempotence and delivery-timeout validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | Lock-free monotonic timing gate. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Shared deadline arithmetic and interrupt restoration. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Spring events, recursive appender discovery and cross-context ownership. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMeterRegistry.kt` | 3 | Optional Micrometer public API boundary. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DeliveryOwnershipTest.kt` | 3 | Atomic state-machine transition proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | 3 | Real Logback/Joran binding proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | 3 | Documentation/constant drift checks. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | 3 | Mapping validation and classification invariants. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | 3 | Routing, marker and topic-name edge cases. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | 3 | Route/materialize composition and encoder failures. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | 3 | Header/key bounds and defensive-copy behavior. |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | 3 | Own breaker binder checked against the official binder. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt` | 3 | Recommended encoder/Jackson compatibility through the full pipeline. |
| `./pom.xml` | 3 | Dependency, compiler, test-profile and verification lifecycle contract. |
| `./docs/config/kafka-appender-config-guide.md` | 3 | Operator-provided producer settings directly affect startup and delivery. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Pure enrichment, bounded trace key and immutable headers. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | 2 | Immutable route/materialize composition. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | 2 | Joran configuration aggregation and validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | 2 | Pure routing and Kafka topic validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | Credential-free info-label projection. |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | 2 | Startup warning and debug-message derivation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | 2 | Standard-properties parsing wrapper. |
| `./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt` | 2 | Info-label projection examples. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | 2 | Parser examples and malformed escapes. |
| `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | 2 | Diagnostic condition and wording assertions. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | 2 | Immutable lookup behavior. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | 2 | No-op metric contract. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | 2 | Kafka client thread-name compatibility. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | External ingest compatibility; profile-gated. |
| `./src/test/java/eu/inqudium/tabellarium/KafkaProducerPropertiesFuzzTest.java` | 2 | Parser robustness fuzz target. |
| `./src/test/java/eu/inqudium/tabellarium/MessageEnricherFuzzTest.java` | 2 | Enrichment invariant fuzz target. |
| `./src/test/java/eu/inqudium/tabellarium/TopicRouterFuzzTest.java` | 2 | Topic/routing invariant fuzz target. |
| `./README.md` | 2 | Public delivery and configuration contract. |
| `./.github/workflows/ci.yml` | 2 | Default, real-broker and dependency-scan gates. |
| `./src/test/resources/junit-platform.properties` | 2 | Suite-wide test timeout. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | 1 | Enum constants and immutable override tables. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | 1 | Small immutable lookup table. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | 1 | Interface and no-op implementation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | 1 | Interface and factory seam. |
| `./src/main/resources/tabellarium-version.properties` | 1 | Build-filtered version constant. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestSupport.kt` | 1 | Test doubles and helper functions. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestEvents.kt` | 1 | Event fixture and bounded polling helper. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ThreadSafeListAppender.kt` | 1 | Thread-safe recording fixture. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FixedZeroPartitioner.kt` | 1 | Deterministic Kafka test partitioner. |

## 5. Findings checklist

### 🔴 Critical

No Critical findings.

### 🟠 High

No High findings.

### 🟡 Medium

- [x] 1. [./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:173] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause: the branch at lines 173–180 is the only code that closes the check-then-act window between the initial `running` check and `queue.offer`: if close or worker death performs its last drain in that window, the second `running` read must remove and reject the newly queued item. No test forces an offer to pause between those operations; the concurrent hot-path test exercises general contention but does not synchronize against `close()` at this point. A mutant deleting the re-check can therefore leave an item stranded and still plausibly keep the suite green.
    - Triggering condition: a regression of the re-check, followed by an offer racing exactly with close or the worker-death drain.
    - Impact: false confidence around a score-5 concurrency invariant; under the regressed code an event can be neither delivered nor accounted as fallback/drop.
  - Fix strategy: introduce a test-only scheduling seam around the offer/re-check boundary, or extract the state/queue protocol behind an injectable queue, then force close/death to finish its drain before the offer resumes and assert exactly one rejection/accounting outcome. Keep the production hot path free of additional blocking in normal builds.
  - **Status:** Fixed in 3bf1856 (2026-10-10). `BoundedWorkerDispatcher` takes a `queueFactory` constructor parameter (default `::LinkedBlockingQueue`, so the production path is unchanged). The new `BoundedWorkerDispatcherTest` parks the offer inside the queue between the `running` check and the insertion, lets `close()` respectively the worker-death handler (after `onWorkerDeath`) finish their final drain, then resumes the offer and asserts `false`, exactly one `NOT_ACCEPTING` rejection, no delivery and an empty queue. Mutation-checked: removing the re-check turns both tests red.

### 🟢 Low

- [x] 2. [./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:326] {Low} {Confidence: High} {Robustness}
  - Symptom → cause: `registerFallbackQueueGauges` sets `fallbackGaugesRegistered=true` before registering either gauge, and the surrounding `safe` block silently swallows registration exceptions. If the size gauge or capacity gauge throws once, later calls update the supplier but immediately return because the flag is already true; one or both queue gauges remain absent for the lifetime of that metrics binding, `bindMeterRegistry` still reports success, and no Logback status identifies the missing observability. The hostile-registry test at `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:480` asserts only that hooks do not throw, so this persistent partial state is not detected.
    - Triggering condition: only when a MeterRegistry, meter filter, or custom registry path throws selectively or transiently during initial fallback-queue gauge registration.
    - Impact: persistent missing/stale queue saturation telemetry and alerts until a complete re-bind; Kafka/fallback delivery itself continues.
  - Fix strategy: make the two-gauge registration transactional and mark it complete only after both registrations succeed; roll back a partial pair and emit one status warning while preserving the non-throwing metrics-hook contract. Add a fail-once registry test that retries and verifies both gauges plus the diagnostic.
  - **Status:** Fixed in 3bf1856 (2026-10-10). A new `registerGaugePair` helper registers size and capacity gauge as one transaction: on failure the gauges registered so far are removed again (a meter whose removal fails stays tracked for `deregisterFrom`) and a `warn` sink - wired by `MetricsBindings` to the Logback status manager - reports the unobserved queue; the hooks still never throw. `fallbackGaugesRegistered` is reset when the pair failed, so a repeated call retries. The send-queue gauge pair uses the same helper. Two fail-once-registry tests in `MicrometerKafkaAppenderMetricsTest` verify rollback, warning and the successful retry; mutation-checked (flag reset and rollback removal).

## 6. Systemic patterns

1. **Scheduling-time-dependent asynchronous tests remain widespread but bounded.**
   - Occurrences: 55 textual `pollUntil` occurrences (helper plus call sites) across `./src/test/kotlin/`, counted with `rg`; two `Thread.sleep` occurrences, one inside the bounded polling helper and one deliberate slow-fallback fixture.
   - Most tests use latches to establish the important ordering and polling only to observe eventual completion. The suite-wide two-minute JUnit timeout in `./src/test/resources/junit-platform.properties` and 30-minute CI job timeouts prevent indefinite hangs.
   - Residual risk: a heavily loaded or paused CI worker can still produce false red results at individual two-/five-/ten-second observation bounds. This was not promoted to another finding because no currently inspected test relies on sleep alone for the correctness ordering, and the high-risk “caller must not block” tests now use helper threads plus bounded latches.
   - **Status:** Deliberately tolerated (2026-10-10), as in the preceding cycles: the new `BoundedWorkerDispatcherTest` forces its interleavings with latches and uses timeouts only as hang guards.
