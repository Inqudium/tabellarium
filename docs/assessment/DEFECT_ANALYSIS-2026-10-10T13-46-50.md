# Code & Defect Analysis: tabellarium

1. Identification
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `0448a31925dd18dfccc3fc0a1486eab95adbe32b` (full)
   - **Reference (branch/tag):** `refs/heads/main`
   - **Project version:** `1.1.3-SNAPSHOT`
2. Scope
   - **Included:** production code in `./src/main/kotlin/` and `./src/main/resources/`; test code in `./src/test/kotlin/` and `./src/test/java/`; build and runtime configuration in `./pom.xml`, `./.mvn/jvm.config`, `./.github/workflows/ci.yml`, and `./.github/workflows/fuzz.yml`; the public contract in `./README.md`.
   - **Test-code decision:** test code is explicitly in scope both as evidence for production risks and as an analysis subject under the test-specific heuristics.
   - **Excluded:** generated/build output in `./target/`; the standalone performance harness in `./benchmarks/` (consulted only for architectural context); historical reports in `./docs/assessment/` (read-only context, not defect subjects); documentation other than `./README.md`; repository metadata in `./.git/`.
3. Environment
   - **Target environment:** Java 21 bytecode/runtime baseline, Kotlin 2.4.20; CI uses Eclipse Temurin 25; Spring Boot BOM 4.1.1.
   - **Framework/model:** Logback library with Kafka messaging and optional Spring/Micrometer integration; neither Spring MVC nor WebFlux application, no coroutines and no persistence layer. Blocking Kafka work is moved to dedicated platform threads; callers may use virtual threads.
   - **Build system:** Maven; local inspection environment Apache Maven 3.9.15 on Oracle JDK 26.0.1, Linux x86_64.
   - **Analysis tools used:** manual static data-flow/lifecycle review, `rg`, `git`, `tokei`, Maven metadata inspection, and source inspection of the resolved Logback 1.5.38 dependency. Ktlint, JaCoCo reporting, CodeQL, OSV/CycloneDX and Jazzer are configured by the project; detekt, SpotBugs, Error Prone, NullAway and ArchUnit are not configured.
   - **Execution status:** no build, test, fuzz run, or reproducer was executed; the governing prompt requires an explicit go-ahead before local execution. Findings are therefore statically verified against the current source and dependency contracts.
4. Placement
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium`
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T13-46-50.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`

## 1. Executive summary

The core dispatch, fallback, circuit-breaker and shutdown logic is deliberately structured and has unusually broad unit-level coverage for a logging library. Nevertheless, the analysis found one High-severity packaging/runtime defect: the advertised optional Micrometer dependency leaks into the public `KafkaAppender` signature, so ordinary Logback/Joran bean introspection can fail before an appender without Micrometer ever starts. The metrics lifecycle also has a non-transactional construction path that can leave partial meters behind and permanently defeat the documented retry behavior. A green retry test creates false confidence because it forces failure before the first meter is registered and therefore cannot expose that state. Startup rollback is mostly careful, but the producer-construction boundary catches only `Exception`, contradicting the explicit `Error` rollback contract and permitting resource leakage on rare VM/linkage failures. Two Low findings concern incomplete best-effort meter removal and a misleading Spring success log after a refused bind. No evidence of data corruption, deadlock, or a normal-path Kafka delivery loss was found.

**Compact test verdict.** The suite is a generally strong safety net: 36 test/fuzz source files and 375 `@Test`, `@ParameterizedTest`, or `@FuzzTest` declarations put most weight on deterministic unit tests, supported by a tagged real-broker integration test and nightly fuzzing. It is not fully reliable for dependency-boundary and partially completed lifecycle failures, because normal test execution supplies all optional dependencies and the retry fixture fails too early.

- The highest-impact gap is a consumer-like Joran startup test with Micrometer absent from the runtime classpath.
- The metrics retry test does not cover failure after one or more successful meter registrations and conceals finding 2.
- Producer rollback tests cover `Exception`, but not a synthetic `Error` after an earlier producer has been acquired.

## 2. Scope & methodology

Phase 1 mapped the single Maven module, its public Logback appender entry point, Kafka producer topology, asynchronous worker ownership, dependency boundaries and CI/static-analysis setup. All 27 production Kotlin files were read completely and ranked before detailed review. All 36 test/fuzz files were structurally screened; the score-4/5 tests, changed tests, fixtures, concurrency helpers, integration/fuzz boundaries and tests adjacent to candidate defects were read in depth. Files were then analyzed from score 5 downward using input/invariant, shared-state, resource-lifecycle, abort-path and hidden-assumption checks; cross-file state was traced through `KafkaAppender`, `KafkaTransport`, `ProducerRegistry` and the metrics binders.

The project is a library rather than a Spring Boot application: Spring is an optional integration surface, Kafka is the only external I/O system, and there are no controller, scheduler, listener-container, transaction, database, Reactor or coroutine paths to assess. Java is targeted at 21 while CI currently uses 25; `./.mvn/jvm.config` carries newer-JDK native/unsafe-access switches. The default verification lifecycle binds ktlint and JaCoCo reporting, with CodeQL, dependency scanning and nightly Jazzer workflows; no branch/line coverage threshold or deeper JVM static analyzer is enforced.

The current commit includes fixes documented in the preceding assessment. Those historical claims were not copied blindly: the changed code and tests were checked in the current tree, and resolved items were not repeated. The principal blind spot is execution: by instruction, neither the build nor a minimal-classpath reproducer was run. For finding 1, the static conclusion additionally rests on Logback 1.5.38's bean introspection enumerating public methods and resolving their parameter types; for findings 2–6, the relevant control and ownership paths are entirely visible in project source.

## 3. Statistics

| Severity | Count |
|---|---:|
| Critical | 0 |
| High | 1 |
| Medium | 3 |
| Low | 2 |
| **Total findings** | **6** |
| **Systemic patterns** | **3** |

## 4. File-ranking table

Scores reflect defect likelihood for production files and evidential weight/risk of false confidence for test files. Build and workflow files are included where they define runtime or verification behavior; read-only historical/context documents are excluded as stated above.

| File | Score | Rationale |
|---|---:|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Public configuration surface, hot path, startup/stop ownership and concurrency publication. |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Thread lifecycle, bounded queue, shutdown and ownership transfer. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Async Kafka completion, breaker state and exactly-once fallback ownership. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | 5 | Main lifecycle/hot-path safety proof with substantial concurrency coverage. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Critical breaker/callback/race safety proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Critical queue, worker-death and close-race safety proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Critical fallback queue and teardown safety proof. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Multi-resource construction transaction and reverse-order teardown. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Cross-library registration, duplicate identity and teardown lifecycle. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 4 | Large eager multi-meter registration and mutable metric ownership. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | Concurrent meter event mirroring and close/register race. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Per-class async handoff and bounded shutdown. |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Atomic single-owner transition prevents duplicate delivery/fallback. |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Async loss semantics and worker lifecycle. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | 4 | Process-wide/shared guard state and recursion prevention. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 4 | Multiple external resources, rollback and timeout-bounded parallel close. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt` | 4 | Main producer creation/rollback/close proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring discovery and metrics retry/ownership proof; finding 3. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter identity, values and deregistration proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | Concurrency and close/register interleaving proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Real Kafka producer and metric integration boundary. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilderTest.kt` | 4 | Compliance-critical override and per-class configuration proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/HalfOpenThrottleTest.kt` | 4 | Concurrency/rate-gate proof with injected time. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ParallelCloseTest.kt` | 4 | Timeout, interruption and parallel resource closure proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuardTest.kt` | 4 | Shared registry and concurrent lifecycle proof. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Spring context events, graph traversal and cross-context ownership. |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | Atomic timing gate and boundary arithmetic. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Thread creation, deadline arithmetic and interrupt handling. |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Configuration merge, mandatory invariants and externally consumed values. |
| `./pom.xml` | 3 | Published dependency contract, compiler target and verification lifecycle. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DeliveryOwnershipTest.kt` | 3 | Atomic ownership state proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/JoranXmlConfigurationTest.kt` | 3 | Real XML/Joran public configuration contract, but full classpath only. |
| `./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt` | 3 | Guards executable README/configuration claims. |
| `./src/test/kotlin/eu/inqudium/tabellarium/MessageEnricherTest.kt` | 3 | Serialization/enrichment boundary and input-shape checks. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicMappingConfigTest.kt` | 3 | Mapping validation and configuration edge cases. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicRouterTest.kt` | 3 | Routing semantics and boundary cases. |
| `./src/test/kotlin/eu/inqudium/tabellarium/RecordPlanTest.kt` | 3 | Cross-component route/materialization proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt` | 3 | Breaker meter compatibility and lifecycle proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashEncoderPipelineTest.kt` | 3 | Runtime encoder/Jackson compatibility proof. |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Focused JSON transformation with bounded state. |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | Small metadata normalization/mapping logic. |
| `./src/main/kotlin/eu/inqudium/tabellarium/RecordPlan.kt` | 2 | Mostly immutable composition after validation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicMappingConfig.kt` | 2 | Configuration validation and normalized mapping. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt` | 2 | Pure routing decision logic. |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt` | 2 | Pure diagnostic text derivation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt` | 2 | Boundary parser with malformed-input handling. |
| `./README.md` | 2 | Public runtime/dependency contract used to verify behavior. |
| `./.mvn/jvm.config` | 2 | JVM compatibility flags. |
| `./.github/workflows/ci.yml` | 2 | Defines default and integration verification coverage. |
| `./.github/workflows/fuzz.yml` | 2 | Defines nightly fuzz execution and scope. |
| `./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt` | 2 | Focused metadata mapping proof. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParserTest.kt` | 2 | Parser examples and malformed inputs. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsTest.kt` | 2 | Small no-op/default metric contract. |
| `./src/test/kotlin/eu/inqudium/tabellarium/StartupDiagnosticsTest.kt` | 2 | Diagnostic formatting assertions. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TopicTableTest.kt` | 2 | Immutable lookup behavior. |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt` | 2 | Dependency-level thread naming contract. |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | Narrow logstash ingestion compatibility. |
| `./src/test/java/eu/inqudium/tabellarium/KafkaProducerPropertiesFuzzTest.java` | 2 | Parser robustness under generated input. |
| `./src/test/java/eu/inqudium/tabellarium/TopicRouterFuzzTest.java` | 2 | Routing robustness under generated input. |
| `./src/test/java/eu/inqudium/tabellarium/MessageEnricherFuzzTest.java` | 2 | JSON enrichment robustness under generated input. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicTable.kt` | 1 | Small immutable lookup wrapper. |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt` | 1 | Enum/constants without mutable state. |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt` | 1 | Small interface and no-op implementation. |
| `./src/main/kotlin/eu/inqudium/tabellarium/SelfLoggingGuard.kt` | 1 | Small interface/factory contract. |
| `./src/main/resources/tabellarium-version.properties` | 1 | Static generated-at-build version value. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestSupport.kt` | 1 | Test factories/helpers with little independent behavior. |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestEvents.kt` | 1 | Shared event/poll fixture. |
| `./src/test/kotlin/eu/inqudium/tabellarium/ThreadSafeListAppender.kt` | 1 | Small synchronized test recorder. |
| `./src/test/kotlin/eu/inqudium/tabellarium/FixedZeroPartitioner.kt` | 1 | Trivial integration-test partitioner. |

## 5. Findings checklist

### 🔴 Critical

No Critical findings.

### 🟠 High

- [x] 1. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:677] {High} {Confidence: High} {Robustness}
  - The optional-Micrometer contract is not classpath-safe: the primary Logback component exposes `MeterRegistry`, `Tag` and `Tags` in a public method signature and eagerly references `MetricsBindings`, while `./pom.xml:371` declares `micrometer-core` optional and `./README.md:902` promises operation without it. Standard Joran bean discovery enumerates public methods and resolves their parameter types, so a normal consumer that receives the published POM without optional Micrometer can hit `NoClassDefFoundError` during Logback configuration before the appender can run.
    - Triggering condition: normal XML/Joran configuration with the documented dependency set and no Micrometer runtime artifact.
    - Impact: application/logging initialization failure; the main advertised optional-dependency mode is unusable on this path.
  - Fix strategy: isolate every Micrometer-typed API and implementation behind a separately loaded integration boundary, or make Micrometer a required transitive dependency and correct the public contract. Add a forked consumer/Joran test whose runtime classpath deliberately omits Micrometer.
  - **Status:** Fixed in 30e052b (2026-10-10). Reproduced first (Joran config on a runtime classpath without Micrometer: `NoClassDefFoundError: io/micrometer/core/instrument/MeterRegistry`, appender never created). `bindMeterRegistry` is now a top-level extension function in `KafkaAppenderMeterRegistry.kt` (`@file:JvmName`; Kotlin source-compatible, Java: `KafkaAppenderMeterRegistry.bindMeterRegistry(appender, registry)`, binary break noted in CHANGELOG), `unbindMeterRegistry` likewise; `KafkaAppender` keeps no Micrometer type in any signature and reaches `MetricsBindings` only through the `installMetrics`/`withdrawMetrics` seam (erased `Function*` parameters), with `MetricsBindings` created on the first bind. Micrometer stays optional as the README promises. New `OptionalDependencyClasspathTest` runs the Joran configuration in a child-first class loader that hides `io.micrometer`/`org.springframework` (classes and service registrations) and asserts start, fallback delivery and a clean status list. Mutation: a member with a `MeterRegistry` parameter on `KafkaAppender` turns it red with the original `NoClassDefFoundError`.

### 🟡 Medium

- [x] 2. [./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:212] {Medium} {Confidence: High} {Resources}
  - Base-meter registration is not transactional. `MicrometerKafkaAppenderMetrics` registers and tracks meters incrementally in property initializers, but `MetricsBindings` stores the only reference capable of deregistration only after the constructor returns; if a later registration throws, earlier meters remain while `boundRegistry` stays null. The next retry then sees the orphaned first `events.accepted` meter at `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:263`, mistakes it for another live appender, and permanently refuses the bind.
    - Triggering condition: a registry/filter throws after at least one base meter has been accepted but before construction completes.
    - Impact: leaked/stale meters and monitoring that remains permanently dark despite the documented retry path; Kafka log delivery itself continues.
  - Fix strategy: make base registration commit-or-rollback, retaining cleanup ownership from the first successful registration even when construction aborts. The duplicate-identity probe must distinguish a failed local attempt from a genuinely live second appender.
  - **Status:** Fixed in 30e052b (2026-10-10). New `MicrometerKafkaAppenderMetrics.register` builds the instance as one transaction: meters are tracked in a caller-owned list from the first registration on and removed again (rollback failures suppressed onto the original) before the failure is rethrown; `MetricsBindings.bind` uses it. No orphan remains, so the identity probe no longer sees the aborted attempt.

- [x] 3. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt:114] {Medium} {Confidence: High} {Test quality}
  - The retry test gives false confidence for finding 2 because `ThrowOnceMeterRegistry` deliberately throws on the first counter registration, “before anything is bound.” It proves only the zero-progress failure case; moving the injected failure to the second or later registration exposes the orphaned identity and makes the asserted retry fail.
    - Triggering condition: production registration makes partial progress before failing, which the fixture cannot express.
    - Impact: a green score-4 lifecycle test masks a real resource/robustness defect in the metrics binding.
  - Fix strategy: parameterize or otherwise inject the failure ordinal so at least one case fails after a meter has been accepted. Assert both that no orphan remains after the first failure and that the second bind succeeds with a complete meter set.
  - **Status:** Fixed in 30e052b (2026-10-10). `ThrowOnceMeterRegistry` takes the failing counter ordinal; the retry test is parameterized over 1, 2 and 4 and asserts an empty registry after the failed bind plus, after the retry, exactly the meter-ID set of a clean bind. Mutation (rollback disabled): ordinals 2 and 4 red, 1 green.

- [x] 4. [./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt:224] {Medium} {Confidence: High} {Resources}
  - Producer creation rolls back only on `Exception`, although `KafkaTransport` explicitly guarantees rollback for any construction failure including `Error`. An `Error` from a later producer factory/build step bypasses the registry cleanup; moreover, registry creation occurs before `KafkaTransport.open` enters its `catch (Throwable)` region, and `KafkaAppender.start` catches only `Exception`, so the already-started encoder also remains unreleased.
    - Triggering condition: rare VM, linkage, initializer, or thread/resource-exhaustion `Error` after at least one producer has been created.
    - Impact: leaked Kafka network thread, buffers/MBeans and encoder state during an already degraded startup, plus violation of the documented lifecycle invariant.
  - Fix strategy: make the acquisition boundary rollback-safe for every throwable while preserving and rethrowing the original failure, and align the caller's encoder cleanup with the same contract. Verify with a synthetic `Error` after the first producer rather than inducing a real OOM.
  - **Status:** Fixed in 30e052b (2026-10-10). `ProducerRegistry.create` rolls back on any `Throwable` (rollback failures added as suppressed; an `Error` propagates unwrapped), and `KafkaAppender.start` releases the started encoder and rethrows on an `Error` from either startup stage (`releaseEncoderAndRethrow`). Tests with a synthetic `LinkageError` after two producers (`ProducerRegistryTest`) and from the factory (`KafkaAppenderTest`, encoder stopped); both mutation-checked.

### 🟢 Low

- [x] 5. [./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:236] {Low} {Confidence: High} {Resources}
  - A single `try` surrounds removal of the entire Resilience4j meter collection. If one registry removal throws, all later meters are skipped and the list is nevertheless cleared, making the remaining registrations permanently unreachable for cleanup.
    - Triggering condition: edge-case custom or failing registry behavior during unbind.
    - Impact: orphaned circuit-breaker meters and stale observations after stop/rebind.
  - Fix strategy: isolate failure per meter, continue best-effort removal, and report the failures without discarding cleanup ownership prematurely.
  - **Status:** Fixed in 30e052b (2026-10-10). Breaker meters are removed one by one, each failure reported with the meter name. The tracking list is still cleared afterwards: the unbind ends the binding's ownership, and a retained meter would have no later unbind to reach it. New test with a registry failing one removal: exactly that meter remains, WARN present; mutation (single try) red.

- [x] 6. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt:176] {Low} {Confidence: High} {Correctness}
  - The Spring helper always logs “Bound” when `bindMeterRegistry` returns normally, but duplicate meter identity is represented by a warning plus a normal `NO_OP` return and leaves `isMeterRegistryBound` false. A same-named second appender therefore produces both a refusal warning and a contradictory success message.
    - Triggering condition: two live appenders with the same name/common-tag identity in one registry.
    - Impact: misleading operator diagnosis; monitoring for the second appender is absent despite an INFO success record.
  - Fix strategy: make the bind result explicit or verify bound state before emitting success; retain the existing refusal warning as the authoritative diagnostic.
  - **Status:** Fixed in 30e052b (2026-10-10). `bindMeterRegistry` returns whether the appender is bound afterwards; `KafkaAppenderMetricsBinding` logs "Bound" only on `true`, otherwise a DEBUG pointing at the status warning. New test with a same-named sibling captures the binding's log; mutation (always log success) red.

## 6. Systemic patterns

1. **Optional-integration types cross the core boundary.** Five production Kotlin files directly reference Micrometer types (`KafkaAppender`, `MetricsBindings`, `MicrometerKafkaAppenderMetrics`, `RenamingMeterMirror`, and `KafkaAppenderMetricsBinding`), counted by import/reference scan over `./src/main/kotlin/**/*.kt`. The critical crossing is the public core-appender signature in finding 1; the spread makes classpath isolation difficult and explains why an all-dependencies test suite misses the consumer mode.
   - **Status:** Addressed with finding 1: the core appender class no longer carries a Micrometer type in any signature; the remaining four files are the metrics integration itself and are loaded only by a bind. Guarded by `OptionalDependencyClasspathTest`.
2. **Multi-resource lifecycle work is only partly transactional.** Three representative acquisition/removal sites were identified by manual ownership tracing: `MetricsBindings.bind`, `MetricsBindings.unbind`, and `ProducerRegistry.create`. They commit ownership after incremental work, wrap a whole cleanup loop in one `try`, or protect only `Exception`; findings 2, 4 and 5 are separate manifestations of that pattern.
   - **Status:** Addressed with findings 2, 4 and 5 (all three named sites fixed).
3. **Asynchronous tests depend heavily on bounded wall-clock polling.** There are 47 `pollUntil` call sites across eight Kotlin test files, counted by call-site scan excluding comments and the helper definition. The shared helper is bounded and most uses appropriately observe worker completion, so this is not a standalone defect; it remains a broad flake/latency surface, and latch-driven deterministic handshakes should be preferred whenever the production seam permits them.
   - **Status:** Tolerated, unchanged (as in earlier reports); the new tests use stop-then-assert or a single bounded poll.
