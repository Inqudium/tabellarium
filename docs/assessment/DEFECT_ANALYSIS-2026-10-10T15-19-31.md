# Code & Defect Analysis: tabellarium

1. Identification
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `3323614ffb8955a804dc111eaa4362470d837c15` (full)
   - **Reference (branch/tag):** `refs/heads/main` (`git describe`: `v1.1.2-73-g3323614`)
   - **Project version:** `1.1.3-SNAPSHOT`
2. Scope
   - **Included:** production code in `./src/main/kotlin/` and `./src/main/resources/`; test code in `./src/test/kotlin/` and `./src/test/java/`; build/verification configuration in `./pom.xml` and `./.github/workflows/ci.yml`; the operator-facing contract in `./README.md` and `./docs/config/kafka-appender-config-guide.md` (included this time because finding 1 lives in configuration guidance the guide and the KDoc give).
   - **Test-code decision:** test code is explicitly in scope, both as a risk indicator for production code and as an analysis subject under the test-specific heuristics.
   - **Excluded:** build output in `./target/`; the JMH harness in `./benchmarks/` (not part of the root build); historical reports in `./docs/assessment/` (read-only history, consulted for known findings only); other documentation under `./docs/`; `./.git/`.
3. Environment
   - **Target environment:** Java 21 bytecode, Kotlin 2.4.20; CI runs on JDK 25; dependency versions managed by the Spring Boot BOM 4.1.1 (kafka-clients 4.2.1, Logback 1.5.38, Micrometer 1.17.1).
   - **Framework/model:** Logback appender library with Kafka producers, Resilience4j circuit breakers and optional Micrometer/Spring integration. No Spring MVC/WebFlux application, no coroutines, no persistence layer. Blocking Kafka work runs on dedicated platform daemon threads; callers may be virtual threads.
   - **Build system:** Maven; local inspection environment Apache Maven 3.9.15 on Oracle JDK 26.0.1, Linux x86_64.
   - **Analysis tools used:** manual static data-flow, lifecycle and concurrency review; `grep`/`sed`/`git`; `javap -c` bytecode inspection of `org.apache.kafka.clients.producer.KafkaProducer` from kafka-clients 4.2.1; one delegated read-only sub-review of the test suite, whose candidate findings were re-checked against the source before inclusion. Project-configured analyzers: ktlint, JaCoCo, CodeQL, OSV/CycloneDX, Jazzer. Not configured: detekt, SpotBugs, Error Prone, NullAway, ArchUnit.
   - **Execution status:** no build, test, fuzz run or reproducer was executed (the governing prompt requires an explicit go-ahead for local execution). All findings are statically verified.
4. Placement
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium`
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T15-19-31.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`

## 1. Executive summary

This is the third defect analysis today. It runs against the state after fix commit `30e052b` and the merge `3323614`. All six findings of `DEFECT_ANALYSIS-2026-10-10T13-46-50` were re-checked in the current tree and are resolved: the Micrometer-free public appender surface, transactional base-meter registration, `Throwable` rollback in `ProducerRegistry.create`, per-meter breaker removal and the honest "Bound" log. The core pipeline is unusually carefully engineered: per-event ownership (`DeliveryOwnership`), the shared `BoundedWorkerDispatcher` skeleton, the transactional `KafkaTransport.open`, and the bounded parallel teardown. A full read of all 28 production files found no data-corruption, deadlock or hot-path delivery-loss defect.

The one High finding is in the configuration guidance, not in the pipeline code. The configuration guide and the `ProducerRegistry` KDoc recommend `delivery.timeout.ms=10000` for AUDIT topics. With Kafka's default `request.timeout.ms` of 30 s, kafka-clients 4.2.1 rejects that value in the producer constructor with a `ConfigException`. An operator following the advice therefore gets an appender that refuses to start. The status error withholds the cause by default, and the library pre-validates other cross-property constraints but not this one. The production Low findings are a metrics re-bind that tears down a working binding before it knows the replacement will succeed, and the guide's claim that derived `client.id`s "never collide", which is false for two appenders sharing a `<component>`. The remaining findings concern test evidential value and robustness.

**Compact test verdict.** The suite is a strong, mostly deterministic safety net: 37 test/fuzz source files with 380 `@Test`/`@ParameterizedTest`/`@FuzzTest` declarations, a consistent Given/When/Then and rationale structure, AssertJ, latch-anchored doubles, a frozen clock for the throttle, and no swallowed worker-thread assertions found. The tests added in `30e052b` kill their target mutants. The pyramid is healthy and unit/component-heavy (MockProducer throughout, 2 tagged real-broker tests, 3 nightly fuzz targets). Its weaknesses are concentrated:

- The only test of the `HalfOpenThrottle` CAS (a score-5 race) creates almost no real contention and would let a non-atomic mutant pass most of the time (finding 2).
- Two "caller never blocks" tests would hang the build, not fail it, on exactly the regression they guard. No JUnit, Surefire or CI timeout exists anywhere (finding 3).
- The startup-rollback tests assert only the producers, not the self-logging guard or the fallback worker the rollback also releases (finding 4). Several shutdown and bind branches have no test at all (finding 5).
- No test covers the `delivery.timeout.ms` guidance (finding 1). Static guard state leaks between test classes (finding 8).

## 2. Scope & methodology

Phase 1 mapped the single Maven module:
- the public Logback entry point `KafkaAppender` (Joran setters, `start`/`append`/`stop`);
- the Micrometer extension seam `KafkaAppenderMeterRegistry.kt`;
- the Spring helper `KafkaAppenderMetricsBinding` (`ContextRefreshedEvent`/`ContextClosedEvent` listeners);
- the asynchronous model: one `SendDispatcher` worker per topic class, one `FallbackDispatcher` worker, Kafka network threads, and closer threads in `closeInParallel`.

There are no controllers, schedulers, listener containers, transactions, Reactor chains or coroutines. All 28 production Kotlin files were read completely and ranked, then analyzed from score 5 downwards with the five-question heuristic. Cross-file state was traced through `KafkaAppender` → `KafkaTransport` → `ProducerRegistry` / `ResilientMessageSender` / dispatchers → `DeliveryOwnership`, and through `KafkaAppender.installMetrics` → `MetricsBindings` → `MicrometerKafkaAppenderMetrics` / `RenamingMeterMirror`.

Test code (33 Kotlin and 3 Java files plus helpers) was reviewed under "How can this test lie?". A delegated sub-review screened every test file and read the score-4/5 tests and the tests changed in `30e052b` in depth. Each of its candidate findings was re-verified against the source here; candidates that did not survive verification were dropped. One example is the claim that the no-op metrics test asserts nothing: its KDoc explicitly declares that allocation is checked by inspection, so this is intended behavior.

Stack: Kotlin 2.4.20 on Java 21 bytecode; kafka-clients 4.2.1; Resilience4j circuit breakers; Micrometer 1.17.1 and Spring 7 optional. Platform threads only inside the library; virtual-thread callers are supported (lock-free hot path). Existing static analysis is ktlint, CodeQL and fuzzing; no JVM bug-pattern analyzer is configured, which justified full manual depth.

Prior reports were treated as read-only history; their findings were checked against the current tree, not copied. Limitations: no execution. Finding 1 rests on bytecode inspection of `KafkaProducer.configureDeliveryTimeout` (kafka-clients 4.2.1): it throws a `ConfigException` "delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms" when the user explicitly set the value. The flaky/hang findings (2, 3) are argued from code structure, not from observed CI failures.

## 3. Statistics

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 1 |
| Medium | 4 |
| Low | 7 |
| **Total findings** | **12** |
| **Systemic patterns** | **2** |

## 4. File-ranking table

Production scores reflect defect likelihood. Test scores reflect evidential weight and the risk of false confidence.

| File | Score | Rationale |
|---|---:|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root: Joran surface, hot path, start/stop ownership, metrics seam under lock. |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Worker lifecycle, in-flight CAS ownership, death handler, two-phase close. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Breaker permissions, sync/async callback paths, exactly-once accounting. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | 5 | Main lifecycle and hot-path proof (85 tests); findings 4, 9, 10. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Breaker, callback and ownership race proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Queue, worker-death and close proof; findings 3, 8. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Fallback queue and teardown proof; findings 3, 8. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Transactional open, reverse-order close, per-class dispatcher wiring. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 4 | External resource creation, rollback, parallel bounded close; finding 1 (KDoc). |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Rejection-to-diversion mapping, ownership stand-down. |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Drop accounting, not-started refusal. |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Three-state CAS that keeps fallback/dispatch exclusive. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Bind/unbind transaction, identity probe, breaker meter tracking; finding 6. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 4 | Transactional registration, gauge holders, deregistration. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | Listener-driven mirroring on a foreign scheduler thread, close race. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | 4 | Process-wide ref-counted registry, ThreadLocal reentry mark. |
| `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | 4 | Only proof of the throttle CAS; finding 2. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | 4 | Creation, rollback (incl. `Error`) and close proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring discovery, retry and refused-bind proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter identity, values and deregistration proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | Mirror concurrency and close/register interleaving. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt` | 4 | Shared registry and ref-count proof; affected by finding 8. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | 4 | Timeout, interrupt and parallel-close proof; pattern P1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | 4 | Compliance-critical merge proof; gap behind finding 1. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Only real-broker proof (tagged `integration`). |
| `./src/test/kotlin/eu/inqudium/tabellarium/OptionalDependencyClasspathTest.kt` | 4 | Micrometer-free Joran proof; finding 12. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Layered merge, caps and pre-validation of Kafka constraints; findings 1, 7. |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | Lock-free timing gate. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Thread start, deadline arithmetic, interrupt restore. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Context events, appender-graph walk, cross-context ownership. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMeterRegistry.kt` | 3 | Optional-dependency API boundary. |
| `./docs/config/kafka-appender-config-guide.md` | 3 | Operator guidance that is executed as configuration; findings 1, 7. |
| `./pom.xml` | 3 | Dependency contract and verification lifecycle (no test timeout). |
| `./src/test/kotlin/eu/inqudium/tabellarium/DeliveryOwnershipTest.kt` | 3 | State-machine proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | 3 | Real Joran binding proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | 3 | Keeps docs and constants in step (not the advice in finding 1). |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | 3 | Mapping validation. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | 3 | Routing semantics. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | 3 | Route/materialize composition. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | 3 | Enrichment and key bounding. |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | 3 | Mirror vs. official binder. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt` | 3 | Encoder compatibility; finding 11. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Pure enrichment, bounded key. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | 2 | Immutable composition. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | 2 | Joran bean plus validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | 2 | Pure routing, single-level marker references. |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | Info-gauge label derivation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | 2 | Diagnostic text only. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | 2 | `Properties.load` wrapper. |
| `./README.md` | 2 | Public contract. |
| `./.github/workflows/ci.yml` | 2 | No `timeout-minutes` (finding 3). |
| `./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt` | 2 | Label mapping. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | 2 | Parser examples. |
| `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | 2 | Text assertions. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | 2 | Lookup behavior. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | 2 | No-op contract. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | 2 | Thread-name contract against the real client. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | Narrow ingest compatibility. |
| `./src/test/java/eu/inqudium/tabellarium/*FuzzTest.java` (3) | 2 | Parser/router/enricher robustness. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | 1 | Enum with constant override tables. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | 1 | Immutable lookup. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | 1 | Interface plus no-op. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | 1 | Interface and factory. |
| `./src/main/resources/tabellarium-version.properties` | 1 | Build-filtered constant. |
| `./src/test/kotlin/eu/inqudium/tabellarium/{TestSupport,TestEvents,ThreadSafeListAppender,FixedZeroPartitioner}.kt` | 1 | Helpers. |

## 5. Findings checklist

### 🔴 Critical

No Critical findings.

### 🟠 High

- [x] 1. [./docs/config/kafka-appender-config-guide.md:669] {High} {Confidence: High} {Robustness}
  - Symptom → cause:
    - The guide (section "Interaction with `delivery.timeout.ms`") and the `ProducerRegistry.DEFAULT_CLOSE_TIMEOUT` KDoc (`./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt:164`–`168`) tell operators who "need stronger guarantees (especially for AUDIT topics)" to set `delivery.timeout.ms=10000`.
    - kafka-clients 4.2.1 (`KafkaProducer.configureDeliveryTimeout`, verified by `javap`) throws `ConfigException("delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms")` when the value is set explicitly and is below `linger.ms + request.timeout.ms`. That threshold is 30 050 ms with the defaults, 30 100 ms for PERFORMANCE.
    - Every producer construction therefore fails. `ProducerRegistry.create` wraps the error as `ProducerConstructionException`, and `KafkaAppender.failStartup` reports only "could not be constructed (org.apache.kafka.common.config.ConfigException)" unless `<debug>` is on.
    - `ProducerPropertiesBuilder.validateIdempotenceCompatibility` (`./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt:183`) pre-validates exactly this kind of Kafka cross-property constraint with a named message, but not this one. No test exercises the advice.
    - Triggering condition: deterministic, whenever an operator follows the documented recommendation without also lowering `request.timeout.ms`.
    - Impact: the appender refuses to start. No event reaches Kafka or the fallback appender (an unstarted appender drops in `doAppend`), and the actionable cause is hidden behind `<debug>`.
  - Fix strategy: correct the guidance (state the `linger.ms + request.timeout.ms ≤ delivery.timeout.ms` constraint and give a consistent value pair). Add the constraint to the builder's named pre-validation so a violation fails with a non-withheld, actionable message, and back it with a test.
  - **Status:** Fixed in 2b2a627 (2026-10-10). Guide (`Interaction with delivery.timeout.ms`) and `ProducerRegistry` KDoc now recommend `request.timeout.ms=5000` + `delivery.timeout.ms=10000` and name the constraint. New `ProducerPropertiesBuilder.validateDeliveryTimeout` rejects an explicit `delivery.timeout.ms` below `linger.ms + request.timeout.ms` (unset values taken from Kafka's `configDef` defaults, unparseable ones left to Kafka) with a named, non-withheld message. Tests: rejection of the old advice, the documented pair accepted by the builder and by a real `KafkaProducer` constructor, the refused pair refused by Kafka itself (guards against a client relaxing the rule), no check without an explicit value. Mutation (validation call removed) red.

### 🟡 Medium

- [x] 2. [./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt:251] {Medium} {Confidence: Medium} {Test quality}
  - Symptom → cause:
    - `should admit at most one probe per gap when threads race` is the only test of the `HalfOpenThrottle` compare-and-set; the `ResilientMessageSender` throttle tests are sequential.
    - It releases 16 pool threads from one latch against a frozen clock. A non-atomic mutant (`lastProbeNanos.set(now); return true`) admits a second probe only if a thread reads between another's `get()` and `set()`, a nanosecond window, while latch wake-ups are staggered by microseconds. The mutant would usually survive.
    - Triggering condition: only under a regression of the CAS.
    - Impact: false confidence at a score-5 race. A regression would release burst probes in HALF_OPEN, the behavior the throttle exists to prevent.
  - Fix strategy: make the race deterministic, e.g. a time source that parks the first caller between read and CAS until a second caller has read, or many repetitions with a barrier inside the clock call.
  - **Status:** Fixed in 2b2a627 (2026-10-10). `HalfOpenThrottle.mayAttemptProbe` reads the slot before the clock (semantics unchanged: a racer claiming in between makes the CAS fail). New test parks two callers inside the clock via a `CyclicBarrier`, i.e. after both read the free slot, and asserts exactly one admission. Mutation (`set` instead of `compareAndSet`) red.

- [x] 3. [./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt:110] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The "dispatch does not block the caller" test (`SendDispatcherTest.kt:110`–`115`) and its sibling `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt:134`–`140` call `dispatch`/`enqueue` directly on the test thread, while the worker parks in an untimed `release.await()` or a forever-blocking appender.
    - If the hand-off became synchronous (the very regression under test), the test thread itself would park there. The `finally` that releases the latch would never run, so the build hangs instead of failing.
    - No JUnit default timeout (`junit-platform.properties`), Surefire timeout, or `timeout-minutes` in `./.github/workflows/ci.yml` bounds that, so CI runs into GitHub's 6-hour job limit.
    - Independently, the `elapsedMs < 200` assertion depends on scheduling and can fail under GC/JaCoCo load.
    - The equivalent `KafkaAppenderTest` case was already moved to a helper thread plus latch (CODE_ANALYSIS-2026-09-15 finding 13); these siblings were not.
    - Triggering condition: hang on regression; spurious red only under load.
    - Impact: CI blockage and masked diagnosis; occasional flake.
  - Fix strategy: run the hand-off on a helper thread and assert completion via a bounded latch wait, as in the appender test. Add a suite-wide safety net (JUnit default timeout or CI `timeout-minutes`).
  - **Status:** Fixed in 2b2a627 (2026-10-10). Both tests dispatch/enqueue on a helper thread and assert completion through a bounded latch, no wall-clock bound. Safety net: `src/test/resources/junit-platform.properties` sets a 2-minute default for `@Test` methods (not for `@FuzzTest` templates, which have their own `maxDuration`), and every job in `ci.yml` has `timeout-minutes: 30`.

- [x] 4. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:1745] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause:
    - The two startup-rollback tests (`KafkaAppenderTest.kt:1745`–`1813`, failing breaker wiring and failing guard factory) assert only `createdProducers.allMatch { it.closed() }`.
    - The rollback in `KafkaTransport.open` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt:373`–`381`) also closes the `FallbackDispatcher` (a started daemon thread) and the `ClientIdSelfLoggingGuard`, which entered names into the process-wide registry.
    - Neither test configures a fallback appender or observes the guard. A mutant dropping `fallbackDispatcher` or `selfLoggingGuard` from the rollback `closeAll` stays green.
    - Triggering condition: regression in the failure path of `KafkaTransport.open`.
    - Impact: false confidence on a score-4 transaction. The real effect of such a regression would be a leaked worker thread per failed (re)configuration, plus "live" client-id names that silently drop a later appender's events.
  - Fix strategy: run the failing-breaker case with a fallback appender and a recording `SelfLoggingGuardFactory`. Assert that the guard was closed and the fallback worker thread has terminated.
  - **Status:** Fixed in 2b2a627 (2026-10-10). New test runs the failing-breaker start with a fallback appender and a guard factory recording `close()`: asserts the guard was closed and no new `kafka-appender-fallback-dispatcher` thread is alive. Mutations (guard or fallback dispatcher dropped from the rollback `closeAll`) both red.

- [x] 5. [./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt:184] {Medium} {Confidence: High} {Test quality}
  - Symptom → cause: several risky branches have no test (grep plus reading):
    - `BoundedWorkerDispatcher.close()` on an already-interrupted thread (`:197`–`216`: skip the grace join, restore the flag). `stop()` on interrupted shutdown threads is common; only `ParallelClose` has an interrupt test.
    - The post-offer re-check `if (!running && queue.remove(item))` (`:177`), i.e. the offer/close race.
    - A metrics re-bind whose registration fails after the previous binding was torn down (see finding 6).
    - The not-started branch of `KafkaAppender.installMetrics` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:689`–`692`), which carries the new boolean contract of `bindMeterRegistry`.
    - The `Error` branch after `RecordPlan.from` in `KafkaAppender.start` (`KafkaAppender.kt:343`).
    - Triggering condition: regressions in shutdown/interrupt and lifecycle edge paths.
    - Impact: unguarded accounting and lifecycle invariants on score-5 code.
  - Fix strategy: add focused tests per branch. For the offer race either add a test seam, or document it explicitly as accepted-untested.
  - **Status:** Fixed in 2b2a627 (2026-10-10). New tests: `close()` from an interrupted thread (remainder accounted as SHUTDOWN, interrupt kept; mutation without restore red), bind before start and after stop (false, warning, empty registry), refused and failing re-binds (see 6). Deliberately not tested: the post-offer re-check in `BoundedWorkerDispatcher.offer` (the window between `queue.offer` and the `running` re-read has no injection point; a test seam in the hot hand-off is not worth it) and the `Error` branch after `RecordPlan.from` (no injectable component can throw an `Error` there; it shares `releaseEncoderAndRethrow` with the tested transport branch).

### 🟢 Low

- [x] 6. [./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:198] {Low} {Confidence: High} {Robustness}
  - Symptom → cause: `bind` calls `unbind()` before it knows whether the replacement will succeed. A repeated bind to a registry where the identity probe refuses, or where `MicrometerKafkaAppenderMetrics.register` throws, removes the working binding and installs none.
    - On refusal, `installMetrics` installs `NO_OP` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:698`–`711`).
    - On a throw, the appender and transport keep recording into the old, already-deregistered meters.
    - Triggering condition: manual re-bind to a second registry with a same-named appender, or a registry failure during re-bind.
    - Impact: monitoring goes dark although a valid binding existed.
  - Fix strategy: probe and build the new binding before tearing down the old one, or restore it on failure; in the throw case, at least switch the recorders to no-op.
  - **Status:** Fixed in 2b2a627 (2026-10-10). `MetricsBindings.bind` returns `null` on refusal, unbinds first only for a re-bind to the same registry (identical meter IDs), otherwise registers the new binding and tears the old one down only afterwards. `KafkaAppender.installMetrics` keeps the current recorders on refusal and switches to no-op when a failure left it unbound. Tests: refused re-bind keeps the first registry's meters counting, failing re-bind to another registry keeps them, failing same-registry re-bind stops recording into the removed counter. Mutations (unconditional unbind first, no no-op switch) red.

- [x] 7. [./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt:101] {Low} {Confidence: Medium} {Robustness}
  - Symptom → cause:
    - The derived `client.id` is `tabellarium-<component>-<class>` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt:284`), and the guide (`./docs/config/kafka-appender-config-guide.md:313`) promises "producers never collide on JMX names".
    - Two `KafkaAppender` instances with the same `<component>` (e.g. two appenders in one service, both using `${ARTIFACT_ID}`) produce identical client ids per class.
    - Kafka then fails to register the second `app-info` MBean (WARN at construction). Closing either producer unregisters the JMX MBeans the other still uses, and broker-side attribution/quotas merge. The Micrometer meters are unaffected thanks to the `appender` tag.
    - Triggering condition: only with multiple appender instances sharing a component.
    - Impact: misleading/missing JMX producer metrics and noisy startup warnings.
  - Fix strategy: include the appender name in the derived default, or correct the guide and warn at start when the derived id is already in use by a live instance (the guard registry already knows them).
  - **Status:** Fixed in 2b2a627 (2026-10-10). Chose a warning over changing the derived id format (that would alter broker attribution and quotas for every deployment). `ClientIdSelfLoggingGuard.clientIdsHeldByOtherInstances` reads the process-wide registry; `KafkaAppender.start` emits `StartupDiagnostics.sharedClientIdWarning` naming the shared ids, the consequences and the fix. Guide corrected. Test: second same-component appender warned, first and a distinct one not, a successor after both stopped not. Mutation red.

- [x] 8. [./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt:180] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: this test and `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt:537` create `ClientIdSelfLoggingGuard(emptySet())` and never close it.
    - Every worker name stays in the static `liveEchoThreadNames` for the rest of the single Surefire JVM.
    - Ref-count off-by-one regressions on the worker names are therefore masked whenever these classes ran first.
    - "Worker names are released after the last close" becomes untestable; `ClientIdSelfLoggingGuardTest.kt:281`–`283` explicitly skips that assertion for this reason.
    - Triggering condition: class-order dependent.
    - Impact: test isolation breach on shared static state.
  - Fix strategy: close the guards in `finally`/`@AfterEach`, then add the released-worker-names assertion where no appender is alive.
  - **Status:** Fixed in 2b2a627 (2026-10-10). Both tests close their guard in `finally`; `ClientIdSelfLoggingGuardTest` now also asserts that the worker names are released after the last close (passes in the full suite, so no other test leaks a guard).

- [x] 9. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:2068] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: `should keep appending normally after a reentrant event was dropped` appends three ordinary events. No reentrant event is ever produced or dropped, so it proves only that `exit()` runs, not that the mark is correct after a nested drop.
    - Triggering condition: always.
    - Impact: the test name overstates the evidence.
  - Fix strategy: use an echoing encoder that triggers one nested drop, then assert that subsequent events pass.
  - **Status:** Fixed in 2b2a627 (2026-10-10). The test now uses an encoder that echoes once on the caller thread while encoding the first event; asserts the echo was dropped and the next three events were sent, in order. Mutation (`guard.exit()` removed) red.

- [x] 10. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:2619] {Low} {Confidence: Medium} {Test quality}
  - Symptom → cause: the concurrent hot-path test claims to detect lost and duplicate events but checks only `historySize() == threadCount * eventsPerThread`; one loss plus one duplicate cancel out.
    - Triggering condition: only a combined regression.
    - Impact: weaker evidence than stated.
  - Fix strategy: compare the delivered message set against the expected set.
  - **Status:** Fixed in 2b2a627 (2026-10-10). Compares the sent message set against all `t$t-e$i` values (`containsExactlyInAnyOrderElementsOf`).

- [x] 11. [./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt:59] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the appender is started and stopped without `try`/`finally`. An assertion failing before `stop()` leaks its workers, producers and process-wide guard names into later tests.
    - Triggering condition: only on a failing assertion.
    - Impact: cascading, misleading failures.
  - Fix strategy: wrap the body in `try`/`finally` (or an `@AfterEach` cleanup list, the project convention).
  - **Status:** Fixed in 2b2a627 (2026-10-10). Start-to-assert body in `try`, `stop()` in `finally` (idempotent after the regular stop).

- [x] 12. [./src/test/kotlin/eu/inqudium/tabellarium/OptionalDependencyClasspathTest.kt:89] {Low} {Confidence: High} {Test quality}
  - Symptom → cause: the status list is collected before `context.stop()` (`:89`–`97` vs. `:98`). A WARN/ERROR raised by the Micrometer-free `stop()` path (e.g. a teardown that touched `MetricsBindings`) would not fail the test; a thrown `Error` still would.
    - Triggering condition: regression in the stop path without Micrometer.
    - Impact: partial coverage of the optional-dependency contract.
  - Fix strategy: stop the context first, then evaluate the status list.
  - **Status:** Fixed in 2b2a627 (2026-10-10). The context is stopped first; the WARN/ERROR status list is evaluated afterwards. Green: the Micrometer-free stop path raises no warning.

## 6. Systemic patterns

1. **Wall-clock-dependent asynchronous tests without a global timeout.**
   - Occurrences: 47 `pollUntil` call sites in 9 test files, plus 6 elapsed-time assertions (`SendDispatcherTest:115`, `FallbackDispatcherTest:140`, `ProducerRegistryTest:538`, `ParallelCloseTest:83/136–137/157`). Counted by grep over `./src/test/kotlin/**/*.kt`, excluding the helper definition.
   - No suite-level timeout exists (JUnit, Surefire or CI).
   - The polls are bounded and mostly observe genuine worker completion, so this is a flake/latency surface rather than a defect per site. Finding 3 is the instance where the missing safety net turns a regression into a hang.
   - **Status:** Partly addressed in 2b2a627: suite-wide JUnit default timeout and CI `timeout-minutes` (finding 3), the two hang-prone elapsed-time assertions replaced by latches. The bounded polls and the `ParallelCloseTest`/`ProducerRegistryTest` elapsed bounds stay tolerated, as in earlier reports.
2. **Process-wide static state shared across test classes in one JVM.**
   - `ClientIdSelfLoggingGuard.liveEchoThreadNames` is entered by every guard and every started appender.
   - Three sites can leave entries behind: two unclosed guards (finding 8) and one appender without `finally` (finding 11). Found by reading every `ClientIdSelfLoggingGuard(` and `start()` without a cleanup guarantee in `./src/test/kotlin/`.
   - Surefire runs all classes in one forked JVM without parallelism, so the effect is order dependence, not races.
   - **Status:** Addressed with findings 8 and 11 (all three sites fixed); the new released-worker-names assertion in `ClientIdSelfLoggingGuardTest` now detects any future leak.
