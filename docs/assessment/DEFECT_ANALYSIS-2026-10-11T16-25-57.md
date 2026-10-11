# Code & Defect Analysis: tabellarium

1. Identification
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `2cde5a65018514e4da75d1e5b156bbd0e128f701` (full)
   - **Reference (branch/tag):** `refs/heads/main` (merge of PR #102; no tag - `git describe` reports `v1.1.2-114-g2cde5a6`)
   - **Project version:** `1.1.3-SNAPSHOT`
   - **Working tree:** clean at analysis time
2. Scope
   - **Included:** production code in `./src/main/kotlin/` and `./src/main/resources/`; test code in `./src/test/kotlin/`, `./src/test/java/` and `./src/test/resources/`; `./pom.xml`; the operator-facing contracts in `./README.md`, `./docs/config/kafka-appender-config-guide.md` and `./docs/metrics/metrics-guide.md` where a finding touches them.
   - **Test-code decision:** test code **is in scope**. It counts both as a risk indicator for the production code and as an analysis subject in its own right under the Phase 2 test heuristics.
   - **Excluded:**
     - build output in `./target/`;
     - the standalone JMH module `./benchmarks/`;
     - `./.git/`, `./.idea/`, `./.github/` (the CI workflow was read only to establish the job timeouts and the integration-test stage);
     - historical reports in `./docs/assessment/`. These are read-only history. The preceding report (`DEFECT_ANALYSIS-2026-10-10T19-11-11`) was consulted for its findings, ranking and remediation status; every adopted statement was re-verified against the current tree.
3. Environment
   - **Target environment:** Java 21 bytecode (`maven.compiler.release`/`jvmTarget` 21), Kotlin 2.4.21, built with JDK 24+ (CI: Temurin 25).
   - **Resolved key dependencies:** Spring Boot BOM 4.1.1; Logback 1.5.38; SLF4J 2.0.18; kafka-clients 4.2.1; Resilience4j 2.4.0; optional Micrometer 1.17.1 and Spring Context 7.0.9; optional logstash-logback-encoder 9.0. Test scope: JUnit Jupiter 6.0.3, AssertJ 3.27.7, Spring Boot Test 4.1.1, Testcontainers Kafka 2.0.5, Jazzer 0.30.0.
   - **Framework/model:** a Logback appender library, not a Spring MVC/WebFlux application. No coroutines, no persistence, no transactions, no controllers or message listeners. Its concurrency comes from:
     - dedicated platform daemon workers (one send worker per active topic class, one fallback worker), each a `BoundedWorkerDispatcher`;
     - the Kafka producer network thread per producer (send callbacks);
     - parallel closer threads at teardown (`closeInParallel`);
     - Micrometer's per-binding `KafkaClientMetrics` refresh scheduler while metrics are bound.

     Logging callers may be virtual threads; the hot path is lock-free by convention (CONTRIBUTING.md).
   - **Build system:** Maven. Inspection host: Apache Maven 3.9.15, Oracle JDK 26.0.1, Linux x86_64.
   - **Analysis tools used:**
     - manual data-flow, lifecycle, failure-path and concurrency review of all 27 production files (read completely) and of the test files named in the ranking table;
     - `grep`, `sed`, `git diff`/`git log` (the production and test deltas since the preceding report's commit `7e70ade3da6f570aa5c6f227f462c855502a9aac` were read line by line: the self-logging guard restructure, the `startCloser` seam, the drain-budget guard, the mirror's type refusal, the `appenderTag` helper);
     - `javap` bytecode inspection of Logback 1.5.38 (`UnsynchronizedAppenderBase.buildReentryGuard` builds `ReentryGuardFactory.GuardType.NOP`, so the library's own `ThreadLocal` reentry mark is required), kafka-clients 4.2.1 (`KafkaProducer.doSend` invokes the user callback synchronously in its `ApiException` handler and returns a failed future without throwing; `InterruptException` restores the interrupt flag in its constructor; `throwIfProducerClosed` throws `IllegalStateException`), Resilience4j 2.4.0 (`EventProcessor.registerConsumer` adds to a `CopyOnWriteArraySet`, so a repeated registration would stack consumers; `CircuitBreakerStateMachine.onError` releases the permission for an ignored exception), Micrometer 1.17.1 (`NamedThreadFactory` creates daemon threads for the `KafkaMetrics` refresh scheduler) and the JDK's `LinkedBlockingQueue.offer` (acquires `putLock`, a `ReentrantLock`);
     - four read-only sub-reviews of the test suite (appender lifecycle tests; dispatcher/sender/concurrency tests; metrics tests; configuration/routing/docs-contract/fuzz tests), whose candidates were re-verified individually against the source before adoption. Candidates that did not survive verification were dropped (for example "the not-started fallback refusal is untested": `KafkaAppenderShutdownTest.kt:232` observes exactly that branch through the drop warning after a real context reset) or downgraded.

     Project-configured analyzers: ktlint, JaCoCo, CodeQL, OSV/CycloneDX, `dependency:analyze` (`failOnWarning`) and Jazzer. Not configured: detekt, SpotBugs, Error Prone, NullAway, ArchUnit - manual depth was required.
   - **Execution status:** no build, test, fuzz run or reproducer was executed, because the governing prompt requires an explicit go-ahead first. Every finding is verified statically; mutation statements are thought experiments against the source.
4. Placement
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium`
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-11T16-25-57.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`

## 1. Executive summary

The analyzed revision carries the remediation of the preceding defect analysis (`333a415`), the architecture-review remediation (`9bbea68`: the self-logging guard became one class with name-based worker recognition, the metric namespace is ADR-0005, `DeliveryOwnership.reset()` is gone), the split of the former `KafkaAppenderTest` into eight classes on a shared fixture (`05bd539`) and the comment-audit remediation (`6bd0cc5`). All 23 findings and both systemic patterns of the preceding report were re-verified: every item recorded as fixed is fixed in the current code, the two declined items are unchanged, and none of the fixes is defective.

The production code again yields **no Critical, High or Medium finding**. The delivery pipeline - lock-free hand-off, bounded queues, the three-state ownership protocol, the breaker permission accounting, the ordered and budgeted teardown, the process-wide echo registry, the transactional metrics binding and the renaming mirror - was traced end to end once more against the resolved library bytecode. Five production findings are Low: the info gauge publishes `fallback.type="none"` for an anonymous fallback appender although `none` is defined as "no fallback"; a delivery that leaves the interrupt flag set outside a close retires a dispatcher worker silently, with the dispatcher still accepting; the mirror close is the one unguarded step of the metrics unbind; three operator-facing or KDoc statements are stale or imprecise (the worker "carries the reentry guard", "no locks in the hot path", the throttle's residual wrap case); and the send-duration measurement is neither injectable nor value-tested.

The weight of this cycle lies on the **test suite** once more. Eleven Medium findings concern evidential value at the pipeline's seams:

- the synchronous-throw path of the sender never asserts that the breaker saw the failure, so a dropped `onError` - a permit leak that can lock a HALF_OPEN breaker out of recovery - stays green;
- a failing or refused re-bind to *another* registry is proven at binding level only; the appender-level promise that the previous recorders stay in place has no test, and two mutations that turn monitoring dark stay green;
- seven binding tests never unbind, so the binder close loop - the only thing that stops a `KafkaClientMetrics` refresh scheduler per topic class - has no observer and the tests leak those schedulers;
- the warn sink that reports a failed queue-gauge or info registration to the status manager is wired but unverified;
- the withheld-stack-trace promise of SECURITY.md is asserted on the message only; attaching the credential-bearing cause to the status would stay green;
- the encoder/fallback release on the refused-start and never-started paths, the stop-during-append residual window, the rollback-close-failure branch of the producer registry, the equality boundary of the delivery-timeout rule, the accept side of the Kafka topic character set (unit *and* fuzz regression mode) and roughly a third of the "code" rows of the configuration guide have no test.

**Compact test verdict.** The suite remains a strong, unit/component-heavy safety net:
- 422 `@Test`, 6 `@ParameterizedTest` and 4 `@FuzzTest` declarations in 42 classes, every one with the three-question rationale block; a suite-wide 2-minute method timeout; two tagged real-broker tests with a 5-minute class timeout;
- latch-forced interleavings for the score-5 protocols (offer-vs-close through the queue seam, forced close with a pinned send, synchronous vs asynchronous callback, the throttle's CAS race, the contended ownership transitions, the mirror's close race), each mutation-checked in the preceding cycle;
- per-test cleanup of appenders, contexts and guards in every appender-level class.

Fast deterministic tests carry the load; the pyramid is healthy. The gaps are of three kinds:

- **Seams proven on one side only.** The breaker is asserted on the callback paths but not on the synchronous throw (finding 1); the re-bind is proven on the binding but not on the appender's recorders (finding 2); the binder close is reached only through an unbind nothing performs (finding 3).
- **Diagnostics wired but never observed.** Status warnings and the stack-trace policy are asserted on message text or not at all (findings 4, 5, 6; pattern 2).
- **Validation tested on the reject side.** Boundaries and accept sides are missing where the reject side is pinned (findings 9, 10, 11; pattern 3).

## 2. Scope & methodology

**Recon (Phase 1).** The single Maven module has three entry points: `KafkaAppender` (Joran lifecycle and `append` hot path), `KafkaAppenderMetricsBinding` (Spring `ContextRefreshedEvent`/`ContextClosedEvent` listeners) and the Micrometer seam in `KafkaAppenderMeterRegistry.kt`. No controllers, schedulers, message listeners, Reactor chains, coroutines, repositories or transactions exist. Static analysis beyond ktlint/CodeQL/Jazzer/`dependency:analyze` is absent (no detekt/SpotBugs/NullAway), so the review relied on manual depth.

**Production review (Phase 2).** All 27 production files were read completely, score 5 first. The pipeline was traced through ingress, routing/encoding, hand-off, send, callback, fallback, close and metrics binding, with emphasis on:
- the paths changed since `7e70ade` (the guard restructure, the `startCloser` seam, the drain-budget guard, the mirror's type refusal);
- the failure paths: forced close, worker death, synchronous callback, interrupted closer, re-bind to the same or another registry, the Logback context-reset order, the mirror's close race, the start-up rollbacks;
- the library contracts the invariants rest on, each confirmed with `javap` (see Analysis tools);
- virtual-thread pinning on the hot path (none: `ReentrantLock`, atomics, a `ThreadLocal` and `ConcurrentHashMap.containsKey` only; the `synchronized` blocks of the metrics implementation sit on the bind/unbind path).

**Test review.** Four sub-reviews read every test file in the ranking table completely, together with the production class under test, under the "How can this test lie?" heuristic. Every candidate was re-verified by the main review against the source (file and line); candidates were dropped when the claimed mutation is caught by another test or the premise was wrong.

**Prior reports.** The 23 findings and the 2 systemic patterns of `DEFECT_ANALYSIS-2026-10-10T19-11-11` were checked against the current code; all are resolved or declined as stated. Note: the status text of its finding 4 names "four new `Metrics lifecycle` tests"; after `9bbea68` two of them (failed/ignored kinds, exactly-once across re-bind/unbind) live in `MetricsBindingsTest.kt:411` and `:456`, the other two in `KafkaAppenderMetricsLifecycleTest.kt:230` and `:268`. The coverage is present and value-level; only the location moved.

**Limitations.**
- No dynamic execution was authorized: no build result, coverage or mutation run is claimed; every "mutation stays green" statement is a static argument.
- Finding 7 (stop during append) is a coverage statement about a documented race window, not an observed failure.
- The production findings are latent: 25 needs an anonymous fallback appender, 26 a delivery that sets the interrupt flag on its own, 27 a registry whose `remove` throws.

## 3. Statistics

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 0 |
| Medium | 11 |
| Low | 20 |
| **Total findings** | **31** |
| **Systemic patterns** | **3** |

Of the 31 findings, 5 are production findings (all Low) and 26 are test-quality findings (11 Medium, 15 Low).

## 4. File-ranking table

Production scores rate defect likelihood. Test scores rate weight as a safety proof. The table follows the preceding report where a file is unchanged in substance and is re-scored for the files split or created since `7e70ade` and for those that carry findings in this cycle.

| File | Score | Rationale |
|---|---:|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root, hot path, start/stop publication, bind/unbind under `bindLock`; findings 2, 5, 6, 7, 14 (test gaps). |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Queue/worker protocol, offer-close race, worker death, two-phase close; finding 26. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Breaker permissions, synchronous/asynchronous callback races, outcome accounting; findings 1 (test gap), 29. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Callback, breaker, throttle and ownership evidence; findings 1, 17. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Queue/worker/forced-close evidence; finding 15; pattern 1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Fallback queue, interruption, worker death, teardown; finding 16; pattern 1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcherTest.kt` | 5 | Latch-forced offer-after-final-drain proofs through the queue seam (sound). |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderDispatchTest.kt` | 5 | Never-block, exactly-once with a pinned send, concurrency; finding 12. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderShutdownTest.kt` | 5 | Teardown order, context reset, worker death reports, stop-twice guard; finding 6. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderHotPathTest.kt` | 5 | Routing failure accounting, deferred processing, Error escape. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Fallback delivery refusal on a stopped appender. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Transactional open, reverse-order close. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 4 | Producer creation rollback, bounded parallel close; finding 8 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Rejection classification, exactly-once diversion. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Three-state CAS protocol, now contended in its test. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Bind transaction, identity probe, breaker consumers, binder lifecycle; findings 3, 4, 20 (test gaps), 27. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 4 | Hot-path sinks, all-or-none groups, info gauges; finding 19 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | Cross-thread mirror/close race, type refusal; finding 27 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | 4 | Process-wide ref-counted name registry, ThreadLocal mark (restructured in `9bbea68`). No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsLifecycleTest.kt` | 4 | Bind/re-bind/unbind through the appender, call-meter kinds; finding 2. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MetricsBindingsTest.kt` | 4 | Binding-level identity, re-bind, failures, Kafka client mirror (new in `9bbea68`); findings 3, 4, 20. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter inventory, series counts, hostile and fail-on-Nth registries; finding 19. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring discovery, nested appenders, retry, context ownership; finding 21. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderStartupTest.kt` | 4 | Start-up failures, withheld causes, no-restart latch; findings 5, 6, 31. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderGuardTest.kt` | 4 | Self-logging and reentry at appender level; finding 13. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Only wire-level evidence; finding 22. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | 4 | Deadline, interrupt, thread-creation failure. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | 4 | Deterministic CAS interleavings; finding 18. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | 4 | Construction rollback, close budgets, aggregate; finding 8. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | Registration/removal concurrency, type refusal; finding 27. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DeliveryOwnershipTest.kt` | 4 | Sequential and contended transitions (sound harness). No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SelfLoggingGuardTest.kt` | 4 | Cross-instance registry lifecycle (declined item of the preceding report unchanged). No new finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | 4 | Compliance overrides, Kafka cross-property validation; findings 9, 11, 23. |
| `./src/test/kotlin/eu/inqudium/tabellarium/OptionalDependencyClasspathTest.kt` | 4 | Optional-dependency contract in isolated classloaders; finding 24. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Layered merge, caps, idempotence/delivery-timeout validation; finding 9 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | Lock-free monotonic gate; finding 28 (KDoc). |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Shared deadline, interrupt restoration, thread-creation fallback. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Spring events, recursive appender discovery; finding 21 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMeterRegistry.kt` | 3 | Optional Micrometer public API. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | 3 | Pure routing, Kafka topic-name validation; finding 10 (test gap). |
| `./docs/config/kafka-appender-config-guide.md` | 3 | Operator contract; findings 11 (unpinned rows), 28 (stale guard wording). |
| `./README.md` | 3 | Public delivery/config contract; finding 28 ("no locks"). |
| `./pom.xml` | 3 | Dependency, profile and verification lifecycle. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderConfigurationTest.kt` | 3 | Validation refusals, warnings; finding 14. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderFallbackWiringTest.kt` | 3 | Slot rules while started; finding 14. |
| `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | 3 | Real Joran binding of every public setter. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | 3 | Docs/constant drift; finding 11; finding 24. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | 3 | Mapping validation; finding 23. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | 3 | Routing and topic-name edges; findings 10, 23. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | 3 | Route/materialize, encoder failures. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | 3 | Header/key bounds, both sides of the 128 limit. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | 3 | Own breaker binder vs. official binder (names/types/tags, by design). No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt` | 3 | Recommended encoder through the pipeline. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | 3 | Diagnostic wording/conditions; finding 25 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Pure enrichment, bounded key. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | 2 | Immutable route/materialize. No finding. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | 2 | Joran configuration validation; finding 23 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | Info-label projection; finding 25. |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | 2 | Startup messages; finding 25 (test gap). |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | 2 | Properties parsing wrapper; finding 24 (test gap). |
| `./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt` | 2 | Projection examples; finding 25. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | 2 | Parser examples; finding 24. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | 2 | Lookup behavior. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | 2 | No-op contract (every hook called). No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | 2 | Kafka thread-name compatibility. No finding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | External ingest contract (profile-gated); finding 24. |
| `./src/test/java/eu/inqudium/tabellarium/*FuzzTest.java` (3) | 2 | Parser/enricher/router fuzz targets; finding 10 (router oracle in regression mode), finding 24 (enricher oracle). |
| `./src/test/resources/junit-platform.properties` | 2 | Suite-wide 2-minute method timeout. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | 1 | Enum and override tables; finding 11 (unpinned). |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | 1 | Immutable lookup. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | 1 | Interface and no-op. |
| `./src/main/resources/tabellarium-version.properties` | 1 | Build-filtered constant. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTestBase.kt`, `TestSupport.kt`, `TestEvents.kt`, `ThreadSafeListAppender.kt`, `FixedZeroPartitioner.kt`; `./src/test/java/eu/inqudium/tabellarium/FuzzOracles.java` | 1 | Fixtures and helpers (checked: `pollUntil` throws on timeout, `awaitUninterruptibly` restores the flag, lookup helpers fail loudly, every appender built through the base is stopped in `@AfterEach`). |

## 5. Findings checklist

### 🔴 Critical

No Critical findings.

### 🟠 High

No High findings.

### 🟡 Medium

- [x] 1. [./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt:583] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The two synchronous-throw tests ("should route to the fallback appender when the producer send throws synchronously", `:583-616`; "should silently drop the event when send throws and no fallback is configured", `:619-654`) and the `SYNCHRONOUS_THROW` leg of the stand-down test assert the recorder and the metrics only; none reads the breaker afterwards.
    - The only `numberOfFailedCalls` assertion in the file (`:707`) belongs to the synchronous-*callback* path. Mutation: delete `circuitBreaker.onError(elapsed, TimeUnit.NANOSECONDS, e)` at `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt:271` - the suite stays green (grep over `./src/test/`: no test closes a producer and then reads breaker state or a breaker meter).
    - Consequence of the unverified call: the permission acquired at `:225` is never settled. In CLOSED state synchronous throws (closed producer, `InterruptException`, a non-API `KafkaException`) would no longer count toward the failure rate; in HALF_OPEN every such send consumes one of the 10 permits without an outcome, and after 10 of them the breaker can neither close nor re-open - the topic class stays in permanent fallback with a healthy-looking breaker.
    - Triggering condition: a regression at that one line; the consequence surfaces only during a producer-side failure mode.
    - Impact: false confidence at a score-5 spot (the breaker's accounting is the pipeline's recovery mechanism).
  - Fix strategy: in the no-fallback synchronous-throw test use the default breaker registry and assert one failed call on the breaker; add one HALF_OPEN case in which synchronous throws still complete the probe window.
  - **Status:** Fixed in 07ee063 (2026-10-11). The no-fallback synchronous-throw test uses the default breaker registry and asserts one failed, zero successful calls; new test "should complete the half-open probe window with synchronous throws and re-open the breaker" (HALF_OPEN, probe gap zero, ten closed-producer sends → OPEN, ten `fallback(send.error)`, nothing dispatched). Mutation-checked: deleting `onError` in the synchronous catch turns both red.

- [x] 2. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsLifecycleTest.kt:184] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - `installMetrics` promises that a refused or throwing bind to *another* registry leaves the previous binding and its recorders in place (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:733-746`, KDoc `:700-704`). The only appender-level failing re-bind (`:184`) is to the *same* registry (expects `NO_OP`), and the only refused re-bind (`:364`) refuses an appender that was never bound; `MetricsBindingsTest.kt:267` and `:302` prove `isBoundTo(first)` at binding level but cannot see the appender's `metrics` field or the transport's recorders. Grep: `bindMeterRegistry(second` appears only at `:136`, the successful re-bind.
    - Mutations that stay green: (a) drop the `if (!bindings.isBound)` guard at `KafkaAppender.kt:737`, so every throwing bind installs `NO_OP`; (b) install `NO_OP` before the `return false` at `:745`. In both, `isMeterRegistryBound` stays true while the hot path records nothing - monitoring dark with a green suite, the symptom of `DEFECT_ANALYSIS-2026-10-10T15-19-31`, finding 6.
    - Triggering condition: a regression in `installMetrics`; in production a second `MeterRegistry` (test context cache, a management child context, manual re-wiring) whose bind fails or is refused.
    - Impact: false confidence at a score-5 spot.
  - Fix strategy: two appender-level tests - bind to a first registry, attempt a bind to a second registry that throws, respectively one already holding a same-named sibling, then append once and assert the first registry's accepted counter advanced and the hot path's counters are not `NO_OP`.
  - **Status:** Fixed in 07ee063 (2026-10-11). Two new `KafkaAppenderMetricsLifecycleTest` tests, "should keep recording into the first registry when a re-bind to another registry throws" / "… is refused": bind, failed or refused re-bind to a second registry, one append, then the first registry's accepted counter and the transport's dispatched counter advanced and `isMeterRegistryBound` true. Mutation-checked: dropping the `isBound` guard and installing `NO_OP` before `return false` each turn them red.

- [x] 3. [./src/test/kotlin/eu/inqudium/tabellarium/MetricsBindingsTest.kt:132] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - Seven tests (`:132`, `:227` for the green fixture, `:267`, `:302`, `:411`, `:505`, `:708`) leave their `MetricsBindings` bound; `@AfterEach closeRegistries` (`:64-68`) closes the producer registries, not the bindings. Each bind constructs one `KafkaClientMetrics` per active class (`./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:465-467`), whose `bindTo` starts a daemon refresh scheduler (`micrometer-kafka-metrics`) that only `close()` stops - reachable solely through `unbind()` (`:243-250`). Those tests therefore leak a scheduler thread and an open `RenamingMeterMirror` per bind for the rest of the fork, polling the metrics of a closed producer.
    - Evidential side: nothing observes the binder close. The two unbind assertions (`:664-666`, `:700-704`) check that the mirrored meters left the target, which `producerMetricsMirror.close()` does regardless. Mutation: delete the close loop at `MetricsBindings.kt:243-250` - the suite stays green (grep `micrometer-kafka-metrics` over `./src/test/`: no hit) while every production unbind leaks one scheduler per topic class.
    - Triggering condition: a regression in the unbind sequence; every Logback reconfiguration or context close in production.
    - Impact: an unobserved removal step at a score-4 spot plus test isolation (daemon threads, so no hang).
  - Fix strategy: track every fixture and unbind in `@AfterEach`; in the Kafka-client-metrics test, assert after `unbind()` that no thread named `micrometer-kafka-metrics` survives (the suite already scans thread names for the self-logging guard).
  - **Status:** Fixed in 07ee063 (2026-10-11). Every `MetricsBindingsTest` fixture registers itself and `@AfterEach unbindAndCloseRegistries` unbinds before the producer registries close; the Kafka-client-metrics test snapshots the live `micrometer-kafka-metrics-*` threads before the bind, asserts the bind started new ones and polls after `unbind()` until none is alive. Mutation-checked: deleting the binder close loop turns it red.

- [x] 4. [./src/test/kotlin/eu/inqudium/tabellarium/MetricsBindingsTest.kt:505] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - `MetricsBindings.bind` passes `{ message, cause -> status.addWarn(message, cause) }` into `MicrometerKafkaAppenderMetrics.register` (`./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:224-226`); this is how a failed queue-gauge pair or info set reaches the operator ("the operator learns what is unobserved", `MicrometerKafkaAppenderMetrics.kt:374-383`, introduced for `DEFECT_ANALYSIS-2026-10-10T15-52-19`, finding 2). Every rollback test captures `warn` with its own lambda; no test drives the hook through the binding and reads the status manager. Grep over `./src/test/`: "Failed to register the", "queue saturation will be unobserved" and "configuration labels will be missing" have no hit.
    - Mutations that stay green: drop the lambda at `:224-226` (the default silent sink at `MicrometerKafkaAppenderMetrics.kt:446` applies); `status.addInfo` instead of `addWarn`. The `Failures` test at `:505` rejects every gauge but never calls a hook on the returned implementation, so the queue-gauge path is not driven there.
    - Triggering condition: a regression in the wiring; in production a registry or filter that rejects a gauge.
    - Impact: the diagnostic exists in the code and is unverified end to end - the failure it was added to make visible can become invisible again with the suite green.
  - Fix strategy: in `Failures`, bind a registry whose gauge registration fails only for the fallback queue capacity, call `registerFallbackQueueGauges` on the returned implementation and assert a `WARN` status naming the fallback queue gauges.
  - **Status:** Fixed in 07ee063 (2026-10-11). New `Failures` test "should report a failed queue gauge registration through the binding's status sink as a warning": a registry rejecting only the fallback-capacity gauge, `registerFallbackQueueGauges` on the implementation `bind` returned, one WARN with both message fragments and the cause attached. Mutation-checked: dropping the warn lambda and `addInfo` instead of `addWarn` each turn it red.

- [x] 5. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderStartupTest.kt:77] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - "should withhold the cause and stack trace when debug is disabled" asserts `statusMessages()` only, which maps each status entry to its `message` (`KafkaAppenderTestBase.kt:69`). The "stack trace withheld" half of the rationale - and of the SECURITY.md promise it cites - is not asserted; `statusEntries()` (`:71`) exists for exactly this and is unused here.
    - Mutation: change `addError("...")` at `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:415-421` to `addError("...", e)`, attaching the credential-bearing cause and its stack trace to the status entry - the test stays green, because the message text is unchanged. The mirror test ("should include the cause when debug is enabled", `:112`) likewise checks the message only, so dropping the throwable at `:423` stays green too.
    - Triggering condition: a regression at the one reporting site the appender deliberately withholds.
    - Impact: false confidence on a documented security property (credential leakage through status output).
  - Fix strategy: assert on `statusEntries()` that the error entry carries no throwable when `debug` is off and carries the cause when it is on.
  - **Status:** Fixed in 07ee063 (2026-10-11). The debug-off test reads the entry through `statusEntries()` and asserts level ERROR, the message fragments and `throwable == null`. Mutation-checked: attaching `e` on the debug-off path turns it red.

- [x] 6. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderShutdownTest.kt:80] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - Two release paths of the appender are unobserved. (a) "should not throw when stopping an appender that never started" names the null-transport branch as its contract but asserts nothing after `stop()` and builds the appender with `encoder = null` and no fallback, so the branch's actual work - `stopFallbackAppenderAndEncoder()` at `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:589` - is never checked; `KafkaAppenderStartupTest.kt:439` (stop before start, then start) attaches no fallback and reads no encoder. (b) `failStartup` releases the encoder at `KafkaAppender.kt:413`; every Exception-path start-up failure in `KafkaAppenderStartupTest.kt` (`:51`, `:89`, `:312`) and `KafkaAppenderConfigurationTest.kt` (`:126`, `:158`, `:192`, `:225`) asserts `appender.isStarted` only - the Error path (`:47`) is the one test that reads `encoder.isStarted`.
    - Mutations that stay green: delete `KafkaAppender.kt:589`; delete `KafkaAppender.kt:413`.
    - Triggering condition: a regression at either line; in production a refused start (Joran started the encoder and the fallback) followed by a context reset.
    - Impact: the file handles and worker threads of a Joran-started fallback appender and the encoder's resources outlive a refused start with the suite green - the leak the KDoc at `:581-587` names.
  - Fix strategy: give the never-started test a started `RecordingAppender` fallback and a `RecordingEncoder` and assert both stopped after `stop()`; in one Exception-path start-up test assert `encoder.isStarted` is false.
  - **Status:** Fixed in 07ee063 (2026-10-11). (a) "should release the encoder and the fallback when stopping an appender that never started": Joran-started `RecordingEncoder` and `RecordingAppender`, refused start, both asserted stopped after `stop()`. (b) The debug-off start-up test asserts `encoder.isStarted` is false. Mutation-checked: deleting `stopFallbackAppenderAndEncoder()` in the null-transport branch and deleting the encoder stop in `failStartup` each turn a test red.

- [x] 7. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:613] {Medium} {Confidence: Medium} {Test quality}
  - Symptom → cause:
    - The teardown comment states the one race the lifecycle tolerates: "An append that already passed the gate can overlap for microseconds - the dispatchers' post-close accounting covers that residual window" (`:613-618`). The covering path is `BoundedWorkerDispatcher.offer` → `NOT_ACCEPTING` (`./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:179-181`, `:191-194`) → `SendDispatcher.reject` mapping it to `shutdown` (`./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt:170-175`) → an enqueue into the already-closed fallback dispatcher, counted as dropped.
    - No test drives an append across `stop()`: grep over `./src/test/kotlin/` for a parked encoder or a stop issued while an append is in flight finds nothing; the dispatcher-level tests prove the skeleton's reclaim, the appender-level tests prove stop and append separately.
    - The window is forceable with test doubles only: an encoder parked on a latch on a helper thread, `stop()` from the test thread, release, then assert no exception on the helper and `events.fallback{reason=shutdown}` plus `fallback.dropped` on meters captured before the stop (the transport's recorders stay bound after `stop()`, `KafkaAppender.kt:627-639`).
    - Triggering condition: only concurrent (a stop during logging); the accounting is unverified, not known to be wrong.
    - Impact: missing test at a score-5 spot - the one documented race of the lifecycle has no evidence.
  - Fix strategy: one appender-level test with the interleaving above, asserting the `shutdown` diversion and the drop count.
  - **Status:** Fixed in 07ee063 (2026-10-11). New `KafkaAppenderShutdownTest` test "should divert an append that passed the gate before stop as a shutdown remainder": an encoder parked on a latch on a helper thread, `stop()` from the test thread, release; asserts no throwable on the helper, `events.fallback{technical,shutdown}` = 1 and `fallback.dropped` = 1 on counters captured before the stop, recorder empty. No production seam was needed. Mutation-checked: mapping `NOT_ACCEPTING` to `SEND_ERROR` in `SendDispatcher.reject` turns it red.

- [x] 8. [./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt:309] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - `ProducerRegistry.create` rolls back already-created producers when a later factory call throws and attaches a failing rollback close as suppressed to the original cause (`./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt:254-260`). Both rollback tests (`:309`, `:363`) use `MockProducer`s whose `close` never throws; the file's `ThrowingOnCloseProducer` (`:29`) is used only by the close-aggregate test (`:567`). Grep `suppressed` over `./src/test/`: `ProducerRegistryTest.kt:591` (aggregate) and `MicrometerKafkaAppenderMetricsTest.kt:782` only.
    - Mutation: delete `e.addSuppressed(rollbackFailure)` at `:258`, or remove the inner try/catch so the rollback failure replaces the original cause and aborts the rollback of the remaining producers - the suite stays green. The operator would then read "producer close failed" instead of the configuration error that broke the start, and the producers created after the failing one would stay open.
    - Triggering condition: a regression combined with a producer whose rollback close throws (a client-side failure at start-up).
    - Impact: error-path masking and a producer leak on the start-up transaction, with the suite green.
  - Fix strategy: a factory that returns a producer throwing on `close`, then a healthy one, then throws; assert the original exception propagates with the close failure as its single suppressed and the healthy producer closed.
  - **Status:** Fixed in 07ee063 (2026-10-11). New test "should keep the original cause and roll back the remaining producers when a rollback close throws" (factory: throwing-on-close producer, healthy producer, then a throw): the `ProducerConstructionException` carries the factory failure as cause and exactly one suppressed close failure, the healthy producer is closed. Mutation-checked: dropping `addSuppressed` and dropping the inner try/catch each turn it red.

- [x] 9. [./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt:662] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The delivery-timeout rule `require(deliveryTimeoutMs >= lingerMs + requestTimeoutMs)` (`./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt:231`) is tested clearly below (`10000` vs `50 + 30000`, `:662`) and clearly above (`10000` vs `50 + 5000`, `:692`); the equality case is absent (grep `DELIVERY_TIMEOUT_MS_CONFIG` over `./src/test/`: only this file, values `10000`/`1000`).
    - Mutation: `>=` → `>` - the suite stays green, and the library would refuse `delivery.timeout.ms = linger.ms + request.timeout.ms`, a pair the Kafka client accepts (it throws only for strictly less). That is the exact failure the test's own rationale (`:729-731`) warns against: a rule stricter than Kafka's turns a working configuration into a named start-up refusal.
    - Triggering condition: a regression at the comparison; an operator pair at equality.
    - Impact: wrong result at a validation that refuses operator configurations by name, undetectable by the suite.
  - Fix strategy: add the equality pair (for example `request.timeout.ms=5000`, `linger.ms=50`, `delivery.timeout.ms=5050`) to the accepting test.
  - **Status:** Fixed in 07ee063 (2026-10-11). New test "should accept a delivery timeout exactly equal to linger plus request timeout" (5050 accepted for every class, 5049 refused with the named message). Mutation-checked: `>=` → `>` turns it red.

- [x] 10. [./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt:347] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - `TopicRouter` validates topic names against `[a-zA-Z0-9._-]+` (`./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt:132`). Every topic literal in the default test run is lowercase letters, `.` and `-` (grep over `./src/test/kotlin/` for a `defaultTopic`/`topic` literal containing `A-Z`, `0-9` or `_`: no hit; the one such topic, `TECHNICAL_TOPIC` in `KafkaBrokerIntegrationTest.kt:112`, is excluded from the default run). The rejection tests (`:347`, `:410`) prove the reject side; the accept side for three of the five character classes has no test.
    - The fuzz target does not close the gap in regression mode: `arbitraryNames` accepts any `IllegalArgumentException` unconditionally (`./src/test/java/eu/inqudium/tabellarium/TopicRouterFuzzTest.java:96-100`, `catch (IllegalArgumentException expected) { return; }`) and the only seed reaching the positive oracle (`positive-oracle-single-char-topic-no-markers`, bytes `00 00 00 00 01`) yields the topic `"a"`. The class KDoc's "validation cannot silently tighten" holds only for the nightly `JAZZER_FUZZ=1` run.
    - Mutation: `Regex("[a-z._\\-]+")` at `TopicRouter.kt:132` - `mvn verify` stays green, and every deployment with a topic such as `app_logs` or `logs-2026` is refused at start with a named error.
    - Triggering condition: a regression in the pattern; common operator topic names.
    - Impact: wrong result (a valid configuration refused) at a start-up validation, undetectable in the default build.
  - Fix strategy: one acceptance test with a mixed-case/digit/underscore default topic and mapping; in `arbitraryNames`, after a rejection assert that at least one of the names was actually Kafka-invalid or the marker blank, and add positive-oracle seeds with such names.
  - **Status:** Fixed in 07ee063 (2026-10-11). Unit: "should accept topic names using every character class Kafka permits" (`App_Logs-2026.v1`, mapped `Audit_Events-01`, routed verbatim). Fuzz target: `arbitraryNames` computes `anyInvalid` with the independent Kafka oracle (character set, length, reserved names, blank marker) and throws on a rejection without an invalid input as well as on an acceptance with one; four new seeds (two `arbitraryNames`, two positive-oracle) with mixed-case/digit/underscore names, 13 regression cases instead of 9. Mutation-checked: `[a-z._\\-]+` turns the unit test and four fuzz regression cases red.

- [x] 11. [./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt:58] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - CONTRIBUTING.md and the class KDoc promise that "a changed constant fails `mvn verify` until the tables follow". The test pins the budgets, the capacities, the breaker table, the probe gap and the MDC key, and (since the preceding cycle) the tag values. Not pinned although a constant exists: the partitioning-key bound `128` (`./docs/config/kafka-appender-config-guide.md:892` ↔ `MessageEnricher.MAX_PARTITIONING_KEY_LENGTH`), the `max.block.ms` caps (`:893` ↔ `TopicClass.maxBlockMsCap`), the `client.id` pattern (`:889` ↔ `ProducerPropertiesBuilder.kt:111`), the fallback class (`:890` ↔ `TopicMappingConfig.kt:75`), the `<sendQueueCapacity>` default in the element table (`:70`), the per-class defaults table (`:1026-1034`), the mandatory-override table (`:993-998`) and the metrics guide's per-class floor table (`./docs/metrics/metrics-guide.md:380-385`) ↔ `TopicClass.defaultOverrides`/`mandatoryOverrides`. Grep over `./src/test/`: `MAX_PARTITIONING_KEY_LENGTH`, `maxBlockMsCap`, `defaultOverrides`, `mandatoryOverrides` have no hit; no test anywhere pins the `TopicClass` tables (`"65536"`, `"32768"`, the PERFORMANCE `linger.ms=100`, the AUDIT `retries=10` have no hit).
    - Mutations that stay green with stale docs: `MAX_PARTITIONING_KEY_LENGTH = 64`; `PERFORMANCE.maxBlockMsCap = 300`; PERFORMANCE `linger.ms` `"100"` → `"50"`; dropping AUDIT `retries=10`. A related invariant has no test either: every class's `max.block.ms` default is at or below its cap (`TopicClass.kt:94/99`, `:115/119`, `:133/138`, `:152/157`); raising a default above the cap would record a spurious `MandatoryOverrideViolation` on every start (`ProducerPropertiesBuilder.kt:164-174`).
    - Verified by hand: every table currently matches the code.
    - Triggering condition: a future change of any unpinned constant or table.
    - Impact: false confidence in the documentation gate the project advertises; stale operator defaults.
  - Fix strategy: extend the contract test to the quick-reference rows above and to the per-class tables (render them from `TopicClass`), and add one unit test asserting `defaultOverrides[max.block.ms] <= maxBlockMsCap` per class.
  - **Status:** Fixed in 07ee063 (2026-10-11). `DocumentationContractTest` pins the quick-reference rows (`128` ↔ `MAX_PARTITIONING_KEY_LENGTH`, the cap row rendered from `TopicClass.maxBlockMsCap`, the fallback class from a fresh `TopicMappingConfig`, the `client.id` pattern against the id a started appender hands its factory, the `<sendQueueCapacity>` element default) and, in a new `Per-class tables` group, renders A.3 (mandatory overrides), A.4 (default overrides), the "default is also its cap" sentence and the metrics guide's floor table from `TopicClass.defaultOverrides`/`mandatoryOverrides`. New `TopicClassTest` pins `defaultOverrides[max.block.ms] <= maxBlockMsCap` per class. Every table matched on the first run. Mutation-checked: `MAX_PARTITIONING_KEY_LENGTH = 64`, `PERFORMANCE.maxBlockMsCap = 300`, PERFORMANCE `linger.ms` 100 → 50, dropping AUDIT `retries`, AUDIT `max.block.ms` default 600, dropping FUNCTIONAL mandatory `acks` - each turns at least one test red.

### 🟢 Low

- [x] 12. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderDispatchTest.kt:143] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: three small evidential defects in one file. `:143` asserts `factory.release.count == 1`, which holds by construction (the latch is counted down only in the `finally` at `:145`); the latch assertions at `:134-142` carry the proof. In the pinned-send test, `release.countDown()` at `:335` follows two assertions outside any `finally`, so a red assertion leaves the send worker parked forever in `awaitUninterruptibly` (a daemon thread, leaking into the rest of the fork; every other park fixture of the group releases in `finally`). The concurrency test's `catch (_: Throwable) { failures.incrementAndGet() }` (`:436`) discards the cause, so a red run reports a count without a reason.
  - Fix strategy: drop the tautological assertion, move the release into a `finally`, and record the first caught throwable for the failure message.
  - **Status:** Fixed in 07ee063 (2026-10-11). The tautological `release.count == 1` assertion is gone, the pinned-send test releases the worker in a `finally`, and the concurrency test records the first throwable in an `AtomicReference` and asserts it null before the count. Harness-only, no mutation.

- [x] 13. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderGuardTest.kt:150] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: "should not drop events when the operator sets a blank client id" and its rationale ("a blank id in the guard would be contained in every thread name") describe the former substring semantics; since the guard matches exact names (`./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt:133-134`), the event from thread `main` (`:176`) passes whether or not the blank filter at `SelfLoggingGuard.kt:105` exists - the mutation is caught by `SelfLoggingGuardTest.kt:87` (a thread named exactly the prefix), so this appender-level test is redundant, not a hole. The sibling's name at `:26` ("whose name contains a producer client id") is stale for the same reason; its body uses the exact name.
  - Fix strategy: retire the redundant test or give it an event from a thread named exactly the bare prefix, and rename the sibling.
  - **Status:** Fixed in 07ee063 (2026-10-11). The blank-client-id test appends from a thread named exactly `PRODUCER_NETWORK_THREAD_PREFIX`; the sibling is renamed "should ignore events from the network thread of one of its own producers". Mutation-checked: dropping only the registry's blank filter stays green (the guard's own filter catches it, as predicted), dropping both turns it red - the test now pins the appender-level behaviour.

- [x] 14. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:462] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: smaller untested branches of the composition root. (a) The `sendQueueCapacity <= 0` refusal (`:462-465`) has no test (grep: only the Joran binding of `2048` and positive values); deleting it still fails a start, but late (after the producers were built and rolled back) and with the generic pipeline message from `LinkedBlockingQueue` instead of the named error. (b) `detachAndStopAllAppenders()` on a not-started appender (`:849-855`) and `detachAppender(name)` (`:866-873`) are untested; only the instance variant (`KafkaAppenderFallbackWiringTest.kt:124`) and the refused-while-started path (`:166-167`) are. (c) The failure branches of `stopFallbackAppenderAndEncoder` (`:657-666`, "Error stopping …") have no test (grep over `./src/test/`: no hit); a fallback whose `stop()` throws must not skip the encoder stop.
  - Fix strategy: one test per branch; (c) with a fallback whose `stop` throws, asserting the warning and the stopped encoder.
  - **Status:** Fixed in 07ee063 (2026-10-11). (a) `KafkaAppenderConfigurationTest` "should refuse to start when the send queue capacity is not positive" (named error, no producer created). (b) `KafkaAppenderFallbackWiringTest` "should stop and clear the fallback when detachAndStopAllAppenders is called before start" and "should detach by name without stopping when detachAppender(name) is called before start". (c) `KafkaAppenderShutdownTest` "should still stop the encoder and warn when the fallback appender fails to stop". Mutation-checked: deleting the gate, skipping `it.stop()`, a by-name match that never matches, and removing the try/catch around the fallback stop each turn a test red.

- [x] 15. [./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt:458] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: "should account for the remainder and keep the interrupt when closed from an interrupted thread" claims "the waits end early" but bounds nothing: with `drainTimeoutMs = 60_000` (`:493`) and `close()` on the test thread (`:502-503`), a mutation that consumes the flag at the top of `close()` and restores it at the end (so the first join waits the full budget) passes all four assertions after about 61 s, inside the 2-minute method timeout - a slow green instead of a red. Separately, "should drain the queue by sending when closed gracefully" (`:323`) reads "nothing landed in the fallback" from the asynchronous recorder without closing the fallback dispatcher first, against the file's own convention (`:53-60`); no mutation was found that this alone lets through.
  - Fix strategy: run `close()` on a helper thread and assert it returns within a few seconds (or raise the drain budget above the method timeout); close the fallback dispatcher before the emptiness assertion.
  - **Status:** Fixed in 07ee063 (2026-10-11). The interrupted-close test runs `close()` on a helper thread, joins it with a 5 s bound and asserts the closer is gone, the flag kept and the accounting unchanged; the graceful-drain test closes the fallback dispatchers before the emptiness assertion. Mutation-checked: consuming the flag at the top of `close()` and restoring it at the end (a full 60 s first join) turns it red.

- [x] 16. [./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt:442] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the rationale of "should count an event as dropped when the fallback appender throws" promises "the dispatcher keeps working afterwards", but the body enqueues one event and polls the drop count (`:473-476`); worker survival is not observed. Mutation: narrow the `catch (e: Exception)` in `deliverGuarded` (`./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:291-302`) so the `RuntimeException` kills the worker - the death handler's `rejectRemaining(WORKER_DEATH)` produces the same single drop and the test stays green. The survival half of the Exception path is pinned only indirectly (`SendDispatcherTest.kt:667-720`).
  - Fix strategy: flip the appender to succeed after the first throw and assert the second event is delivered, or trim the rationale.
  - **Status:** Fixed in 07ee063 (2026-10-11). The appender throws once and then records; the test asserts one drop and that the second event is delivered. Mutation-checked: rethrowing the `RuntimeException` from `deliverGuarded` (the worker dies) turns it red. Note: the mutation named above (`catch (e: IllegalStateException)`) does not compile because of the `e is InterruptedException` check below it; the equivalent rethrow was used.

- [x] 17. [./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt:502] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: "should silently drop the event when the circuit is open and no fallback is configured" asserts "no throw" and an empty producer history only (`:520-530`); the file's own standard for the no-fallback variant of the throw gate (`:619`) is that the metrics still fire. Mutation: short-circuit on `fallbackDispatcher == null` before `m.eventFallback(...)` at `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt:227-229` - stays green (the `BREAKER_OPEN` reason test at `:1481` runs with a recorder).
  - Fix strategy: install `CapturingMetrics` and assert exactly one `fallback(breaker.open)`.
  - **Status:** Fixed in 07ee063 (2026-10-11). The test installs `CapturingMetrics` and asserts exactly one `fallback(breaker.open)`. Mutation-checked: short-circuiting on the null dispatcher before the hook turns it red.

- [x] 18. [./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt:191] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: "should spread ten probes evenly when called at maximum rate" advances the clock by exactly one gap per call (`:213-216`), so every call is admitted and the test never gates anything - it is "allow a new probe after the gap" (`:153`) repeated ten times. Mutation: make `mayAttemptProbe` return `true` after the state check (`./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt:115-123`) - this test stays green; `:120` and `:224` catch it, so no false confidence suite-wide.
  - Fix strategy: call at a fraction of the gap (for example 50 calls at 1 ms) and assert exactly 10 admissions.
  - **Status:** Fixed in 07ee063 (2026-10-11). The test now calls 50 times at 1 ms steps and asserts admissions exactly at 0, 5, …, 45 ms. Mutation-checked: admitting everything after the state check turns it red.

- [x] 19. [./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:795] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: "should attach common tags to every metric" value-asserts the common tag on two of the twelve meter names (`:822`, `:831`); the inventory test (`:904`) runs without common tags. Mutation: build the send-queue gauges or the send timers from `Tags.of(TAG_APPENDER, …)` instead of `tagsWith(...)` (`./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:153`, `:359`, `:366`) - the `appender` tag and the tag keys stay as the inventory expects, the common tag is gone, the suite stays green. Also untested: the rollback-failure branch of the companion `register` (`:451-457`, `rollbackFailure` → `addSuppressed`) - the preceding cycle's finding 17 covered the identical branch in `registerAllOrNone` only; no registry in the suite registers counters successfully and then throws on `remove` during the constructor rollback.
  - Fix strategy: give the inventory test a common tag and add its key to the expected tag keys; one test with a registry that fails a counter registration and then throws on the first removal, asserting the suppressed cause.
  - **Status:** Fixed in 07ee063 (2026-10-11). The inventory test runs with a `service` common tag, adds it to every expected key set and value-asserts it on every meter; new test "should attach a failing constructor rollback as suppressed and keep removing the rest" (third counter rejected, first `remove` throws: original message, one suppressed, one meter left). Mutation-checked: a send-queue gauge built from `Tags.of(TAG_APPENDER, …)` and a rollback without try/catch each turn a test red.

- [x] 20. [./src/test/kotlin/eu/inqudium/tabellarium/MetricsBindingsTest.kt:505] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the rationale of "should report a failing breaker metrics binding as a warning and stay bound" promises "the appender's own meters stand, the breaker meters are reported missing"; the assertions (`:537-540`) check the returned implementation, `isBound` and one `WARN`. The registry rejects the *first* breaker gauge, so `boundResilience4jMeters` is empty when the catch at `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:315` runs; the KDoc claim "every meter registered before a failure is tracked and torn down by unbind" (`:303`) has no mid-binding-failure test. Mutation: collect the breaker meters into a local list in `bindBreakerMeters` and append to the tracked list only on normal completion - a failure on the `not.permitted.calls` counter then orphans ten gauges and three timers, and no test unbinds after such a failure.
  - Fix strategy: a registry that throws on the first counter under the breaker prefix; assert the `WARN`, then that `unbind()` leaves the registry empty.
  - **Status:** Fixed in 07ee063 (2026-10-11). New `Failures` test "should track the breaker meters registered before a failure so the unbind removes them": a registry rejecting the counters under the breaker prefix, WARN asserted, breaker meters present after the bind, registry empty after `unbind()`. Mutation-checked: collecting the breaker meters only on normal completion turns it red.

- [x] 21. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt:151] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: three small items. `:151` asserts `isNotEmpty` on the accepted counters of the global `LoggerContext` where the exact count under the `appender` tag is checkable (as `:353-358` does); any `KafkaAppender` another class leaves attached to the global context would satisfy it. The rationale at `:522-524` ("ordinal 4: after all of them") misdescribes the case - `TopicClass` has four entries, so counter #4 is the last accepted counter and 29 counters follow; the case is still a valid mid-constructor failure. The null-context branch of `unbindAppenders` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt:220-221`, a binding constructed outside Spring) has no test (grep `setApplicationContext` over `./src/test/`: no hit); dropping the null guard would ignore every close event outside Spring with the suite green.
  - Fix strategy: assert the exact per-class count under the appender tag; correct the rationale; one test that constructs the binding manually and calls `unbindAppenders`.
  - **Status:** Fixed in 07ee063 (2026-10-11). Line 151 asserts exactly `TopicClass.entries.size` accepted counters under `appender=TEST_KAFKA`; the "ordinal 4" rationale is corrected; new test "should unbind on a close event when constructed outside Spring" (manual construction, no `setApplicationContext`). Mutation-checked: dropping the null guard turns it red.

- [x] 22. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt:217] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: `getTag(TAG_APPENDER)).isNotBlank()` where the exact value is known - the appender is unnamed, so the tag must be `unnamed` (`MicrometerKafkaAppenderMetrics.appenderTag`); `isNotBlank` would also accept a leaked `client.id`.
  - Fix strategy: assert the exact value.
  - **Status:** Fixed in 07ee063 (2026-10-11). The integration test asserts the exact `unnamed` tag value; run against the real broker (`-Pintegration`, 2 tests green).

- [x] 23. [./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt:747] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: smaller evidential gaps in the configuration tests. `:747` ("should not check a delivery timeout the operator did not set") asserts only `doesNotContainKey(delivery.timeout.ms)`, which holds for any builder because the builder never writes that key; the path is covered by accident of the exception, not by the assertion. `:533` asserts only that `acks` is absent under explicit idempotence; a skip that is too wide (dropping `linger.ms` or `compression.type` as well, `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt:101`) stays green. `:692` hard-codes the "documented pair" (`5000`/`10000`) instead of reading it from the guide (`./docs/config/kafka-appender-config-guide.md:689-690`). The `defaultTopicClass` setter's `trim()` (`./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt:76-78`) has no padded input anywhere (its two siblings have). The documented "first marker that matches, direct or hierarchical" rule (`./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt:105-106`) is tested with two direct matches only (`TopicRouterTest.kt:148`); a hierarchical match on the first marker beating a direct match on a later one is untested.
  - Fix strategy: one assertion or case per item.
  - **Status:** Fixed in 07ee063 (2026-10-11). The explicit-idempotence test asserts every TECHNICAL default except `acks` survives (mutation widening the skip to `linger.ms`: red); the documented pair is read from the guide's code block; the unset-delivery-timeout test asserts the kept request timeout plus a control case in which the rule fires once the key is explicit; `TopicMappingConfigTest` "should trim whitespace from the default topic class and still classify by it" (mutation removing the trim: red); `TopicRouterTest` "should let a hierarchical match on an earlier marker beat a direct match on a later one" (mutation "all direct matches first": red).

- [x] 24. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt:249] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: weak assertions and small gaps across the parsing, docs and classpath tests. `:249` asserts four fragments of the joined JAAS value with `contains` instead of the exact value, so a value keeping the indentation or the backslashes passes; `:135` asserts `hasSize(2)` without keys; a key-only line (documented at `KafkaProducerPropertiesParser.kt:52-54`) has no unit test (fuzz seed only). `MessageEnricherFuzzTest.java:74-80` checks that each identity value appears in *some* header, not under its key (a key swap stays green in the fuzz target; the unit test catches it). `DocumentationContractTest.kt:308` compares the live breaker meter set with the documented one via `containsExactlyInAnyOrderElementsOf` without a non-emptiness guard (its producer-bridge sibling at `:369` has one), so two empty sets pass. `OptionalDependencyClasspathTest.kt:167-170` hides Spring, the encoder and Jackson 3 but not Jackson 2 (`com.fasterxml.jackson`, test scope), so a future production reference to it would pass the "runs without the optional dependencies" check (no such reference exists today). `LogstashHttpMethodKeyValueIngestTest.kt:163` (tagged `external-contract`): every assertion also holds when the encoder emits nothing - no encode error, no field, a missing node, no conflict - so the test cannot distinguish "field omitted" from "event dropped".
  - Fix strategy: exact values and non-emptiness guards; hide Jackson 2 as well; add a presence check for the rest of the document.
  - **Status:** Fixed in 07ee063 (2026-10-11). Parser: exact entries at `:135`, exact joined JAAS value at `:249`, new "should parse a key-only line as an empty value". `MessageEnricherFuzzTest` checks each identity under its own header key (key-swap mutation: red in five cases). `DocumentationContractTest:308` has non-emptiness guards on both sides. `OptionalDependencyClasspathTest` hides `com.fasterxml.jackson` too (still green: nothing in production reaches Jackson 2). `LogstashHttpMethodKeyValueIngestTest` asserts message, `lap.url.path` and status code are present in the null-method case (run with `-Pexternal-contract`, 4 tests green).

- [x] 25. [./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt:52] {Low} {Confidence: High} {Correctness}
  - Symptom → cause: `fallbackType` maps a blank `simpleName` to `NONE`, and `NONE` is defined as the label of an *absent* fallback (`:23-24`, `:36`). An anonymous fallback appender (`object : AppenderBase<ILoggingEvent>() { … }`, the shape several tests use) has an empty `simpleName`, so the info gauge then carries `fallback="<name or unnamed>"` next to `fallback.type="none"` - two labels that contradict each other on the series dashboards join on. No test passes an anonymous appender to `AppenderInfo.of` (grep); the blank-value branch of `effectiveSetting` (`:77`) is untested as well. Noted for completeness: the `"unknown"` fallback at `:79` is dead for `acks` and `linger.ms`, both of which have `ConfigDef` defaults.
    - Triggering condition: only an anonymous (or otherwise nameless) fallback appender class.
    - Impact: wrong result in an info label.
  - Fix strategy: derive the type label from the class name with a distinct value for nameless classes (for example `anonymous`), and pin it with a test.
  - **Status:** Fixed in 07ee063 (2026-10-11). `AppenderInfo.ANONYMOUS` (`anonymous`) is the type label for a fallback appender class without a simple name; `NONE` is reserved for the absent fallback. Metrics guide label table updated. New `AppenderInfoTest` tests: "should report an anonymous fallback appender class as anonymous, never as none" and "should treat a blank merged value like an absent one" (the blank-value branch of `effectiveSetting`). Mutation-checked: mapping the blank name back to `NONE` turns the first red.

- [x] 26. [./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:271] {Low} {Confidence: Medium} {Robustness}
  - Symptom → cause: the worker treats an interrupt as the close signal: a `poll` ending in `InterruptedException` (`:263-268`) or a delivery that returns with the flag set (`:271-273`, `:280-282`) ends the worker. Outside a `close()` nothing in the library interrupts the worker, but the code assumes nothing else does either: a delivery that *sets* the flag on its own (a fallback appender or an interceptor calling `Thread.currentThread().interrupt()` on an internal timeout) retires the worker while `running` is still `true` - no `workerDied`, no `running = false`, no `onWorkerDeath`. The dispatcher then keeps accepting into a queue nothing drains until it is full, after which every event is diverted as `queue.full` (send) or dropped (fallback) - the dead worker masquerading as a slow consumer, which is the failure the death hook exists to prevent (`:88-90`). The symmetric case (a delivery that *swallows* the close interrupt) is documented as harmless (`:62-68`); this one is not mentioned. No test sets the flag from inside a delivery without a close (`FallbackDispatcherTest.kt:92` re-sets it only after the close's interrupt).
    - Triggering condition: only a third-party delivery that leaves the interrupt flag set spuriously.
    - Impact: silent loss of delivery capability with misattributed accounting.
  - Fix strategy: distinguish the close interrupt from a spurious one by the `running` flag - clear and continue while still running, or treat it as a worker death (leave the accepting state, reject the remainder, report) - and pin the choice with a test.
  - **Status:** Fixed in 07ee063 (2026-10-11). The worker distinguishes the close interrupt from a spurious one by `running`: `close()` clears `running` before it interrupts and the JDK's interrupt status is volatile, so an interrupt observed while `running` is still true is cleared and the worker keeps delivering (both after a delivery and on an `InterruptedException` from the poll); the class KDoc documents the case. New `BoundedWorkerDispatcherTest` group `Interrupts the worker does not own`: a delivery that sets its own flag, an idle worker interrupted from outside, and the converse (a parked delivery restoring the close interrupt still ends the drain, the queued item is claimed as `SHUTDOWN_REMAINDER`). Mutation-checked: exiting after the delivery, exiting on the poll interrupt and clearing every interrupt each turn one of the three red.

- [x] 27. [./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:251] {Low} {Confidence: High} {Robustness}
  - Symptom → cause: `producerMetricsMirror?.close()` is the one step of `unbind()` outside a `try`, unlike the binder closes (`:244-248`), the breaker removals (`:257-261`) and the deregistration (`:267-271`). It is safe today only because `RenamingMeterMirror.removeFromTarget` swallows a throwing `target.remove` (`./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt:143-149`) and `SimpleMeterRegistry.close()` does not throw. That swallow has no test: no mirror in the suite targets a registry whose `remove` throws (grep; the throwing-`remove` registries in `MicrometerKafkaAppenderMetricsTest` and `MetricsBindingsTest.kt:562` serve other classes). Mutation: delete the `try/catch` at `:144-148` - the suite stays green, and with a throwing registry `unbind()` would abort before the breaker removals and `deregisterFrom`, orphan every meter, leave `boundRegistry` set and make the identity probe refuse every later bind.
    - Triggering condition: a regression combined with a registry whose `remove` throws.
    - Impact: an unbind that leaves the appender permanently unbindable; latent.
  - Fix strategy: guard the mirror close like its siblings, and add a mirror test against a target whose `remove` throws.
  - **Status:** Fixed in 07ee063 (2026-10-11). `producerMetricsMirror?.close()` is guarded like its siblings (WARN "Error closing Kafka producer metrics mirror"). New `RenamingMeterMirrorTest` "should swallow a failing removal on the target and still close" (mutation deleting the try/catch in `removeFromTarget`: red) and `MetricsBindingsTest` "should complete the unbind and allow a later bind when the registry refuses every removal" (real producers, registry throwing on every `remove`: `unbind()` returns, `isBound` false, a fresh registry binds). With the production mirror the new guard is unreachable, so it was verified by a double mutation: mirror swallow deleted plus guard kept stays green, mirror swallow deleted plus guard deleted turns that test red.

- [x] 28. [./docs/config/kafka-appender-config-guide.md:601] {Low} {Confidence: High} {Maintainability}
  - Symptom → cause: three statements drifted from the code. (a) The guide says the fallback's `doAppend` "runs on the dispatcher's worker thread, which carries the appender's reentry guard" (`:601-603`); since `9bbea68` the workers carry no guard state - they are recognized by their fixed thread names (`./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt:32-44`); the operator-visible behaviour is unchanged. (b) README.md:761 states "There are no locks in the hot path; only atomics, volatiles and a per-thread reentry flag", and CONTRIBUTING.md asks for "no blocking waits"; the hand-off `BoundedWorkerDispatcher.offer` → `LinkedBlockingQueue.offer` acquires `putLock`, a `ReentrantLock` (verified with `javap`): brief and non-pinning, but a lock on which the logging threads of one class serialize under contention. (c) The `HalfOpenThrottle` KDoc names "a clock value within one gap of `Long.MIN_VALUE`" as the one remaining wrap case (`./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt:87-89`); the initializer's `now - gap` and the comparison's `now - last` wrap symmetrically in two's complement, so that case is handled too - `HalfOpenThrottleTest.kt:379` passes at `Long.MIN_VALUE + 1_000_000` for exactly this reason.
  - Fix strategy: reword (a) to the name-based mechanism, (b) to "no `synchronized`, no pinning; one brief `ReentrantLock` in the queue hand-off", and drop the residual case from (c).
  - **Status:** Fixed in 07ee063 (2026-10-11). (a) The guide now says the worker is recognized by its fixed thread name. (b) README: "no `synchronized` and no blocking wait in the hot path: atomics, volatiles, a per-thread reentry flag and one brief `ReentrantLock` inside the bounded queue's non-blocking hand-off"; CONTRIBUTING reworded the same way. (c) The `HalfOpenThrottle` KDoc states that both subtractions wrap symmetrically and cites the `Long.MIN_VALUE + 1_000_000` test instead of naming a residual case.

- [x] 29. [./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt:297] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: the class injects a `nanoTimeSource` for the throttle but measures the send duration with `System.nanoTime()` directly (`:270`, `:297`, `:323`); the duration feeds `send.duration` and the breaker's slow-call input. No test asserts a sender-measured duration at value level (the `totalTime` assertion at `MicrometerKafkaAppenderMetricsTest.kt:208` records an injected duration into the implementation). Mutation: `Duration.ofMillis(elapsed)` instead of `ofNanos` at `:272`/`:324` - a million-fold inflation of every send-duration series with the suite green.
  - Fix strategy: route the duration through the injected clock and assert one recorded duration against the clock's delta.
  - **Status:** Fixed in 07ee063 (2026-10-11). `nanoTimeSource` is stored and used for `SendCallback.startNanos` and both `elapsed` computations; the KDoc names the send duration as its second use. New `ResilientMessageSenderTest` group `Send duration measurement`: callback path (clock advanced 7 ms between send and completion → exactly 7 ms) and synchronous-throw path (the producer double advances the clock 3 ms and throws → exactly 3 ms, outcome error); `CapturingMetrics` records durations. Mutation-checked: `ofNanos` → `ofMillis` and `startNanos = System.nanoTime()` each turn both red.

- [x] 30. [./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt:231] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: "should list generated settings per class and never repeat operator values" uses base properties the builder never changes, so the diff semantics of `debugMessages` ("value differs from the operator's", `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt:107`) are not exercised; a filter that merely excludes operator-supplied keys stays green here and in `KafkaAppenderStartupTest.kt:203-209` (where `linger.ms=10` equals the merged value). Both filters keep credentials out, so the security property holds either way; an overruled value (operator `acks=1` for AUDIT shown as `acks=all`) would then vanish from the diagnostic. Also, `sharedClientIdWarning` (`StartupDiagnostics.kt:63-71`) is the one message function without a unit test (grep: substring coverage at `KafkaAppenderConfigurationTest.kt:660`, `:671` only).
  - Fix strategy: one case with an overruled operator key asserting the enforced value appears; one unit test for the shared-id warning.
  - **Status:** Fixed in 07ee063 (2026-10-11). New test "should list an enforced value that overruled the operator's own" (operator `acks=1` on AUDIT is shown as `acks=all`, never `acks=1`, no `bootstrap.servers`) and a new `Shared client id warning` group with two unit tests for `sharedClientIdWarning` (two live guards holding the id → named; one → null). Mutation-checked: filtering by key presence instead of value difference, and dropping the id from the message, each turn a test red.

- [x] 31. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderStartupTest.kt:112] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the debug-enabled mirror of finding 5 asserts the cause's message text only; that the throwable is attached to the status entry (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:423`) is not asserted, so dropping `e` there stays green while the rationale promises "message and stack trace behind the explicit flag".
  - Fix strategy: assert on `statusEntries()` that the error entry's throwable is the injected cause.
  - **Status:** Fixed in 07ee063 (2026-10-11). The debug-on test asserts the attached throwable is a `ProducerConstructionException` whose cause is the injected failure. Mutation-checked: dropping `e` from the debug-on `addError` turns it red.

## 6. Systemic patterns

1. **Scheduling-time-dependent asynchronous observation in tests remains widespread but bounded.**
   - Occurrences, counted with `grep` over `./src/test/`: 60 textual `pollUntil` hits (definition and comments included; `SendDispatcherTest` 18, `FallbackDispatcherTest` 13, `ResilientMessageSenderTest` 8, `KafkaAppenderDispatchTest` 5, the rest spread over ten classes); 2 `Thread.sleep` (one inside `pollUntil`, one deliberate 20 ms slow-appender fixture at `FallbackDispatcherTest.kt:380`); 2 tests that assume the interrupted worker finishes within `INTERRUPT_GRACE_MS` (`SendDispatcherTest.kt:391-455`, `FallbackDispatcherTest.kt:396-439`).
   - The critical orderings are forced with latches and the two production seams; polling only observes eventual completion; the 2-minute JUnit method timeout and the 30-minute CI job timeout contain hangs.
   - Residual risk: on a starved CI worker individual bounds can go red without a production defect. Deliberately tolerated in the preceding cycles; unchanged, recorded as a pattern rather than as individual findings.
   - **Status:** Deliberately tolerated (2026-10-11), as in the preceding cycles. Finding 15's unbounded wait is fixed in 07ee063; the two grace-dependent park tests are unchanged (the 500 ms grace is a production constant).

2. **Diagnostics are wired but not observed.**
   - Occurrences (~9 warning/error sites by `grep` over `./src/main/` for the message prefixes, plus the stack-trace policy): the withheld/attached cause of the start-up error (findings 5, 31); the queue-gauge/info registration warning through the binding (finding 4); "Error stopping fallback appender"/"Error stopping encoder" (finding 14); "Error closing Kafka producer metric binding", "Failed to bind Kafka producer metrics", "Failed to bind KafkaAppender", "Failed to unbind KafkaAppender" (no test asserts the record; grep); the shared-client-id warning as a unit (finding 30).
   - Consequence: a regression that silences, downgrades or over-shares a diagnostic stays green; the worker-death and close-failure reports fixed in the preceding cycle (its finding 5) show the shape of the fix - one test per site that drives the failure and reads the status manager.
   - **Status:** Fixed in 07ee063 (2026-10-11) through findings 4, 5, 14c, 27, 30 and 31 - each of those sites now has a test that drives the failure and reads the status manager or the entry's throwable. The four warnings named only by grep above ("Error closing Kafka producer metric binding", "Failed to bind Kafka producer metrics", "Failed to bind KafkaAppender", "Failed to unbind KafkaAppender") still have no test asserting the record - carried forward.

3. **Validation is tested on the reject side; boundaries and accept sides are missing.**
   - Occurrences (~5, by reading): the delivery-timeout equality (finding 9); the topic character set's accept side in unit and fuzz regression mode (finding 10); the `defaultTopicClass` trim and the hierarchical-before-direct order (finding 23); the `sendQueueCapacity` refusal (finding 14). The key-length bound of the enricher is the counter-example: both sides of `128` are pinned.
   - Consequence: a validation that tightens by one character or one unit refuses a working configuration at start-up with a named error and a green build. The fix is the same shape everywhere: one case exactly at the boundary and one on the accept side next to the existing rejection.
   - **Status:** Fixed in 07ee063 (2026-10-11) through findings 9, 10, 14a and 23: the equality pair, the accept side of the topic character set (unit and fuzz regression mode), the `sendQueueCapacity` refusal, the `defaultTopicClass` trim and the hierarchical-before-direct order each have a case now.
