# Code & Defect Analysis: tabellarium (Logback Kafka appender)

1. Identification of the codebase
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit hash:** `1c6c59b8fbd51bf971b5c34a93876c0c416948a0` (full)
   - **Reference (branch/tag):** `refs/heads/main` / tag: none (version `1.1.3-SNAPSHOT`)
   - **Ticket / change request:** [MISSING - please supply]
2. Scope of the analysis
   - **Included:** `./pom.xml`, production code `./src/main/kotlin/`, resources `./src/main/resources/`, **test code** `./src/test/kotlin/`, Jazzer fuzz targets `./src/test/java/`, test resources `./src/test/resources/`; `./.github/workflows/ci.yml` only as evidence of what CI executes; `./docs/metrics/metrics-guide.md` and `./README.md` only where they make claims about the code.
   - **Excluded:** JMH benchmarks (`./benchmarks/`), build output (`./target/`), Grafana dashboards (`./docs/metrics/*.json`), historical assessment reports (`./docs/assessment/`, read only to avoid re-reporting fixed or tolerated items).
   - **Test code in scope:** **yes** - assessed both as a risk indicator for the production code and as an analysis subject in its own right.
3. Analysis environment & tools
   - **Target environment:** artifact targets Java 21; the build needs JDK 24+ (`.mvn/jvm.config`, `jdk-24-plus-test-jvm` profile); observed JDK 26.0.1 (Oracle) locally, Temurin 25 in CI. Kotlin 2.4.20.
   - **Libraries:** Spring Boot BOM 4.1.1 (imported, not inherited), Kafka clients 4.2.1, Micrometer 1.17.1, Resilience4j 2.4.0, Logback 1.5.38, Jackson 3 pinned to 3.2.3 via `tools.jackson:jackson-bom`.
   - **Build system:** Maven 3.9.15.
   - **Analysis tools used:** manual review (complete read of every production file and of every test file, the latter split over two parallel review passes whose candidates were re-verified against the code before inclusion), `grep`-based pattern counts; static analysis present in the repo: ktlint (build), CodeQL, OSV scan over the CycloneDX SBOM, Jazzer fuzzing (nightly). No detekt, SpotBugs, Error Prone or ArchUnit.
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium` (absolute reference point; all relative paths refer to it)
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-10T13-00-00.md` (relative to the workdir; prefix + ISO 8601 timestamp)
   - **Scope root (relative to the workdir):** `./` (single-module Maven project; production sources under `./src/main/kotlin/eu/inqudium/tabellarium/`)
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>` (e.g. `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:514`)

---

## 1. Executive summary

The codebase is a mature, heavily reviewed asynchronous Logback appender: one bounded queue and one worker per topic class, per-class Resilience4j breakers, a three-state delivery-ownership protocol for exactly-once accounting, and a bounded fallback dispatcher. All five findings of the previous defect analysis (`DEFECT_ANALYSIS-2026-09-15T22-05-50`, M-1..M-3, L-1, L-2) were verified as remediated in the current code. Since then, the metrics layer has grown substantially (info gauges, the `event.size` summary, the `kafka.appender.*` namespace for breaker and producer metrics via the new `RenamingMeterMirror`); this pass found **no Critical defect and no High production defect**.

The weight of this pass lies on the safety net rather than on the production code. The most consequential finding is a **false-confidence test** for the breaker's `ignoreExceptions` list: a regression that removes any single one of three ignored exception types would leave the test green, although the effect would be a whole topic class failing over to the fallback on an ACL or topic-name error. Second, **no test in the default run or in CI exercises the optional `LogstashEncoder` or Jackson 3** - the only such test is excluded by its `external-contract` tag - while the Jackson security pin claims to fix "this build, its tests" and was bumped across a minor line (3.1.7 → 3.2.3) today on the strength of green checks that cannot detect an encoder/Jackson incompatibility. Third, the new `kafka.appender.topic.info` test filters out exactly the join-key tag it should prove.

In production code, three Low items remain: the Spring binding treats "bound" as "bound to any registry" and never unbinds on context close; documentation claims the hot path's `encoder.error` path covers OOM although only `Exception` is caught; and public KDoc still describes the breaker metric names as mirroring `resilience4j.*` after today's rename. A systemic pattern of documentation and rationale drift after the 2026-10-10 rename spans production KDoc, test rationale blocks and the POM.

**Correction of an earlier statement:** in the change sessions for PRs #78 and #83, the encoder tests were described as having run green against the pinned Jackson versions. That was wrong - `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` is excluded from both local `mvn verify` and CI (finding 2).

**Test verdict.**

1. **Reliability as a safety net / testability:** high for the score-5 core. Accounting (`accepted = dispatched + fallback`), diversion stand-down, hand-off versus forced close, throttle (injected clock), self-logging guard, shutdown exactly-once and the renamed producer metrics (real producers, composite target) are asserted exactly and largely mutation-proof; the risky logic is checkable deterministically and in isolation thanks to injected producer factories, guard factories and time sources. Weak where an assertion is broader than its rationale (findings 1, 3, 12, 13).
2. **Usage & coverage (test pyramid):** healthy shape - 365 test/fuzz annotations, overwhelmingly fast deterministic unit tests, a thin layer of real-producer tests against an unreachable broker, one Testcontainers broker test under `-Pintegration`, three Jazzer targets with seed corpora. The gap is not depth but breadth at the edges: the optional encoder dependency and the mirror's dynamic (refresh-driven) path against a real broker are not covered.
3. **Most significant gaps & anomalies:**
   - False confidence: three of four ignored breaker exceptions are not individually proven (finding 1).
   - No default-run/CI coverage of `LogstashEncoder` + Jackson 3, while the dependency pin is maintained by Dependabot (finding 2).
   - `topic.info` join-key tag filtered out of the only assertions that could catch its loss (finding 3).
   - One exact-delta assertion on the global root logger is exposed to cross-test log events (finding 11, heisenbug).
   - Rationale blocks describe mechanics that were replaced today (pattern P1).

## 2. Scope & methodology

- **Analyzed:** all 28 production files under `./src/main/kotlin/eu/inqudium/tabellarium/` (6,212 lines incl. KDoc), all 32 Kotlin test files (14,869 lines), 3 Java fuzz targets (275 lines), `./src/test/resources/logback-test.xml`, `./pom.xml`.
- **Not analyzed:** `./benchmarks/` (JMH; compiled by CI against the snapshot), Grafana dashboard JSON, workflows beyond the CI commands.
- **Test code in scope:** yes.
- **Detected stack:** library, not an application - no controllers or listeners of its own. Concurrency model: blocking JVM threads (one daemon worker per topic class plus a fallback worker, closer threads on shutdown, the Kafka client's network threads, Micrometer's `KafkaMetrics` refresh schedulers); no Reactor/WebFlux, no coroutines; virtual threads are possible on the *caller* side (the hot path is lock-free by design, `UnsynchronizedAppenderBase`). Spring is optional (`KafkaAppenderMetricsBinding`, `@EventListener(ContextRefreshedEvent)`). No persistence. Messaging: Kafka producer only.
- **Entry points:** `KafkaAppender.start/append/stop/bindMeterRegistry/addAppender` (Logback/Joran), `KafkaAppenderMetricsBinding.bindAppenders` (Spring event), Kafka producer callbacks, dispatcher worker threads, Micrometer listener callbacks of the producer-metrics mirror.
- **Static analysis present:** ktlint, CodeQL, OSV, Jazzer; absent: detekt/SpotBugs/Error Prone/NullAway/ArchUnit - manual depth was applied accordingly. `grep` found no `!!`, no `lateinit`, no TODO/FIXME in production code.
- **Prior reports:** the previous defect analysis and `CODE_ANALYSIS-2026-09-15T21-09-11` were used to avoid re-reporting fixed items and to respect tolerated patterns (wall-clock waits P2, non-injected shutdown budgets P3).
- **Local verification:** no Maven build, test run or reproduction was executed for this analysis (go-ahead not given). One throwaway probe (outside the repository) checked how `micrometer-registry-prometheus` 1.17.1 exports gauges named `*.info`, to rule out a suspected naming defect in the info gauges - result: exported as `kafka_appender_info` / `kafka_appender_topic_info` in text format and as OpenMetrics `info` families, so no finding. The CI checks of the analyzed commit's PR were green.
- **Known limitations:** severity of the heisenbug candidate (finding 11) is estimated, not reproduced; the Spring multi-context scenario (finding 4) was reasoned from the code and the binding's KDoc, not exercised.

## 3. Statistics

| Severity | Count |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 1 |
| 🟡 Medium | 2 |
| 🟢 Low | 12 |
| **Total findings** | **15** |
| **Systemic patterns** | **4** (P1-P4) |

By category: test quality 10, robustness 1, correctness 1, maintainability 3.

## 4. File-ranking table

Production code (score = likelihood of containing defects):

| File | Score | Rationale |
|---|---|---|
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root, lock-free hot path, lifecycle, bind lock, fallback slot |
| `./src/main/kotlin/eu/inqudium/tabellarium/BoundedWorkerDispatcher.kt` | 5 | Queue, worker, in-flight CAS, two-phase close, death handler |
| `./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt` | 5 | Breaker/throttle gates, callback on the I/O thread, ownership transitions |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaTransport.kt` | 4 | Transactional open, reverse-order close |
| `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt` | 4 | Bind/unbind symmetry, breaker event consumers, producer-metrics mirror (changed today) |
| `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt` | 4 | New: listener callbacks on foreign scheduler threads, close race |
| `./src/main/kotlin/eu/inqudium/tabellarium/SendDispatcher.kt` | 4 | Rejection-to-reason mapping, ownership stand-down |
| `./src/main/kotlin/eu/inqudium/tabellarium/DeliveryOwnership.kt` | 4 | Three-state CAS protocol |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 4 | Drop accounting, not-started refusal |
| `./src/main/kotlin/eu/inqudium/tabellarium/ClientIdSelfLoggingGuard.kt` | 4 | Process-wide echo registry, ThreadLocal reentry mark |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt` | 3 | Parallel close, rollback |
| `./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt` | 3 | Pre-resolved meters, info gauges, exception safety |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt` | 3 | Spring lifecycle, appender discovery |
| `./src/main/kotlin/eu/inqudium/tabellarium/HalfOpenThrottle.kt` | 3 | CAS gate, monotonic arithmetic |
| `./src/main/kotlin/eu/inqudium/tabellarium/ParallelClose.kt` | 3 | Shared deadline, interrupt restore |
| `./src/main/kotlin/eu/inqudium/tabellarium/ProducerPropertiesBuilder.kt` | 3 | Override layering, caps, idempotence validation |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Shared header arrays, key bound |
| `./src/main/kotlin/eu/inqudium/tabellarium/AppenderInfo.kt` | 2 | New: metadata extraction, defaults lookup |
| `./src/main/kotlin/eu/inqudium/tabellarium/TopicRouter.kt`, `TopicMappingConfig.kt`, `TopicTable.kt`, `RecordPlan.kt` | 2 | Pure configuration validation and routing |
| `./src/main/kotlin/eu/inqudium/tabellarium/StartupDiagnostics.kt`, `KafkaProducerPropertiesParser.kt` | 2 | Pure functions |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt`, `TopicClass.kt`, `SelfLoggingGuard.kt` | 1 | Interfaces, enums, constants |

Test code (score = weight as a safety proof):

| File | Score | Rationale |
|---|---|---|
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt` | 5 | 83 tests over the composition root: lifecycle, reentry, shutdown exactly-once, concurrency, metrics |
| `./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt` | 5 | Breaker, throttle, ownership, accounting |
| `./src/test/kotlin/eu/inqudium/tabellarium/SendDispatcherTest.kt` | 5 | Worker, overflow, forced close, ownership race |
| `./src/test/kotlin/eu/inqudium/tabellarium/FallbackDispatcherTest.kt` | 5 | Drain budget, interrupt phase, drop accounting |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt` | 4 | Spring binding on the global LoggerContext |
| `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt` | 4 | Only proof against a real broker (`-Pintegration`) |
| `./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt` | 4 | Meter inventory, tags, exception safety, info and size meters |
| `./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt` | 4 | New mirror: lifecycle, close, failure path |
| `./src/test/kotlin/eu/inqudium/tabellarium/ProducerRegistryTest.kt`, `ClientIdSelfLoggingGuardTest.kt`, `HalfOpenThrottleTest.kt` | 4 | Parallel close/rollback, process-wide registry, injected clock |
| `./src/test/kotlin/eu/inqudium/tabellarium/CircuitBreakerMetricsMirrorTest.kt`, `JoranXmlConfigurationTest.kt`, `DocumentationContractTest.kt`, `ParallelCloseTest.kt`, `ProducerPropertiesBuilderTest.kt`, `MessageEnricherTest.kt`, `DeliveryOwnershipTest.kt` | 3 | Contracts, XML surface, budgets, override rules |
| `./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt` | 2 | External-contract characterization; excluded from the default run |
| Routing/config/diagnostics tests, `AppenderInfoTest.kt`, `KafkaAppenderMetricsTest.kt`, `KafkaProducerThreadNamingContractTest.kt`, fuzz targets in `./src/test/java/` | 2 | Pure functions, contracts, seed-corpus fuzzing |
| `./src/test/kotlin/eu/inqudium/tabellarium/TestSupport.kt`, `TestEvents.kt`, `ThreadSafeListAppender.kt`, `FixedZeroPartitioner.kt`, `./src/test/resources/logback-test.xml` | 1-2 | Fixtures |

## 5. Findings

### 🔴 Critical

None.

### 🟠 High

- [x] 1. [./src/test/kotlin/eu/inqudium/tabellarium/ResilientMessageSenderTest.kt:1107] {High} {Confidence: high} {test quality}
  - Symptom → cause: the test claims to prove that the full `ignoreExceptions` list is wired (`InvalidTopicException`, `SerializationException`, `TopicAuthorizationException`, besides `RecordTooLargeException`), but it cycles the four types over 20 failing sends - 5 per type. Resilience4j does not record ignored calls in the sliding window, and with `minimumNumberOfCalls(10)` (`./src/main/kotlin/eu/inqudium/tabellarium/ResilientMessageSender.kt:439`) a regression that drops exactly one type from the list records only 5 failures, below the minimum, so the breaker stays CLOSED and the test stays green; only two missing types would turn it red. `RecordTooLargeException` has its own 30-send test; the other three types are tested nowhere else (grep over `./src/test/kotlin`).
    - Triggering condition: only on a regression that removes one of the three types from `defaultCircuitBreakerConfig().ignoreExceptions`.
    - Impact: concealed production regression - an ACL misconfiguration or an invalid topic would open the breaker for the whole topic class and divert all its events to the fallback (the "breaker poisoning" the list exists to prevent), with a green suite.
  - Fix strategy: flood each ignored type in its own (parameterized) run with at least `minimumNumberOfCalls` failures and assert CLOSED, keeping the existing timeout flood as the counter-check that the same volume of a counted exception does open the breaker.
  - **Status:** Fixed in f93e09e (2026-10-10). Each ignored type now gets its own parameterized 20-call flood asserting CLOSED; the timeout flood stays as counter-check. Mutation: dropping `TopicAuthorizationException` or `InvalidTopicException` from the list turns the respective case red.

### 🟡 Medium

- [x] 2. [./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt:52] {Medium} {Confidence: high} {test quality}
  - Symptom → cause: the only test that drives the optional `logstash-logback-encoder` (and with it Jackson 3) carries `@Tag("external-contract")` and is excluded by `surefire.excludedGroups` (`./pom.xml:158`); CI runs `mvn verify` and `-Pintegration -Dgroups=integration` only (`./.github/workflows/ci.yml:38`, `:79`). Nothing else in `./src/test` references `LogstashEncoder` except a comment. Yet the Jackson security pin states that it "fixes this build, its tests" (`./pom.xml:229`), the README recommends `LogstashEncoder`, Dependabot maintains the pin (`minor-and-patch` group), and it was bumped from 3.1.7 to 3.2.3 today - across a minor line that diverges from the Boot BOM - on the strength of checks that cannot detect an encoder/Jackson incompatibility.
    - Triggering condition: always (default run and CI).
    - Impact: false confidence on dependency bumps; a binary or behavioral break between encoder 9.0 and Jackson 3.x would surface only at a consumer's runtime, as a hot-path `encoder.error` (or an `Error` escaping `append`, see finding 5) on every event.
  - Fix strategy: add a default-run test that drives `KafkaAppender` with a real `LogstashEncoder` through to the record payload and parses the JSON, or run the `external-contract` group in a CI job; correct the POM comment's claim until then.
  - **Status:** Fixed in f93e09e (2026-10-10). New untagged `LogstashEncoderPipelineTest` drives `KafkaAppender` with a real `LogstashEncoder` (Jackson 3) through to the record payload, parses the JSON (message, level, logger, MDC, key-value) and asserts zero fallback; the POM comment names it.

- [x] 3. [./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:877] {Medium} {Confidence: high} {test quality}
  - Symptom → cause: the topic-info test removes the `appender` tag before comparing (`.filterKeys { it != TAG_APPENDER }`); the end-to-end check (`./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:3199-3205`) reads only `topic`, `topic.class`, `acks`, `linger.ms`; the inventory tests construct without `AppenderInfo`. If `registerInfoGauges` built the topic-info tags from `Tags.of(...)` instead of `tagsWith(...)` - the exact shadowing trap the `fullTags` KDoc warns about - the whole suite would stay green. (The appender-level `kafka.appender.info` gauge does assert the tag.)
    - Triggering condition: only on a regression in the topic-info tag composition.
    - Impact: `appender` is the documented join key (`on (job, instance, appender, topic_class)`); without it the documented joins and dashboard tables break, and two appenders on one registry would share topic-info gauges.
  - Fix strategy: assert the `appender` (and a common tag) value in both topic-info tests instead of filtering it out, or include the info gauges in the "documented dimensions" inventory test.
  - **Status:** Fixed in f93e09e (2026-10-10). The topic-info test compares the full tag set including `appender` and a common tag. Mutation `Tags.of` instead of `tagsWith`: red.

### 🟢 Low

- [x] 4. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBinding.kt:147] {Low} {Confidence: medium} {robustness}
  - Symptom → cause: the binding skips every appender that `isMeterRegistryBound` - bound to *any* registry - and never unbinds on `ContextClosedEvent`. With a second application context in the same JVM that brings its own `MeterRegistry` (Spring test-context caching with differing configurations, a context restart that does not re-initialize Logback), the appender stays bound to the first context's registry, possibly already closed, and the second context's binding silently skips it.
    - Triggering condition: only with multiple/successive application contexts sharing one Logback context.
    - Impact: appender metrics missing from the live registry (wrong monitoring result, not data loss); the old registry stays referenced by the appender.
  - Fix strategy: unbind on context close and/or rebind when the appender is bound to a different registry than the binding's own.
  - **Status:** Fixed in f93e09e (2026-10-10). `KafkaAppenderMetricsBinding` unbinds every appender bound to its registry on `ContextClosedEvent` of its own context (`ApplicationContextAware`; child-context close events ignored) via the new internal `KafkaAppender.unbindMeterRegistry(registry)`, which leaves bindings to other registries untouched. Three new tests (each mutation-checked); README and both guides document the behavior. Two contexts open at the same time still share one binding (documented).

- [x] 5. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:514] {Low} {Confidence: high} {correctness}
  - Symptom → cause: the hot path catches `Exception` only (the documented policy), but the comment at `:515`, `FallbackReason.ENCODER_ERROR` (`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetrics.kt:164`) and the metrics guide (`./docs/metrics/metrics-guide.md:199`) state that `encoder.error` covers OOM. An `Error` from the encoder (OOM, a `LinkageError` from an optional encoder dependency mismatch - cf. finding 2) escapes `append` into the application's logging call after `eventAccepted` was counted, so the event is neither dispatched nor diverted.
    - Triggering condition: only when the encoder or routing throws an `Error`.
    - Impact: `accepted = dispatched + fallback` no longer balances and the documentation misleads operators about which failures are counted.
  - Fix strategy: correct the three statements, or deliberately divert selected non-fatal `Error`s (`LinkageError`, `StackOverflowError`) as `encoder.error` while letting `VirtualMachineError`s propagate.
  - **Status:** Fixed in f93e09e (2026-10-10). Documentation corrected (code comment, `FallbackReason.ENCODER_ERROR`, metrics guide, config guide): `Error`s are deliberately not caught, as in Logback's own `doAppend`, and propagate to the logging call. The alternative of diverting `LinkageError` was not adopted; finding 2's default-run encoder test now catches the most likely source.

- [x] 6. [./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:641] {Low} {Confidence: high} {maintainability}
  - Symptom → cause: the public KDoc of `bindMeterRegistry` (rendered into the Dokka API reference) still says the breaker metrics mirror "`resilience4j-micrometer`'s metric names", and `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:273` says the same; since `1e8717a` the names are `kafka.appender.circuitbreaker.*` and the producer metrics are renamed too.
    - Triggering condition: always.
    - Impact: operators reading the API reference build queries on names that no longer exist.
  - Fix strategy: state the `kafka.appender.*` namespace in both KDoc blocks (see pattern P1).
  - **Status:** Fixed in f93e09e (2026-10-10). `bindMeterRegistry` and `bindResilience4jMetrics` KDoc state the `kafka.appender.circuitbreaker.*` / `kafka.appender.producer.*` names.

- [x] 7. [./pom.xml:229] {Low} {Confidence: high} {maintainability}
  - Symptom → cause: the Jackson pin comment still says the advisories are "fixed in 3.1.7" and that the pin is dropped "once the Boot BOM manages 3.1.7 or newer", while `jackson3.version` is 3.2.3 (`./pom.xml:134`) since PR #83; the drop condition is now wrong (the Boot BOM reaching 3.1.7 would downgrade a removed pin from 3.2.3), and the "fixes its tests" claim is unfounded (finding 2).
    - Triggering condition: always; bites when someone acts on the stated drop condition.
    - Impact: an unintended minor-version downgrade of Jackson when the pin is removed per the comment.
  - Fix strategy: update the comment to the current version and a drop condition of "Boot BOM manages ≥ 3.2.3" (or the current pin).
  - **Status:** Fixed in f93e09e (2026-10-10). The comment names the current pin (`${jackson3.version}`) and drops the pin only once the Boot BOM manages at least that version.

- [x] 8. [./src/test/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetricsTest.kt:481] {Low} {Confidence: high} {test quality}
  - Symptom → cause: the hostile-registry test covers every hook but never constructs with an `AppenderInfo`, so `safe { registerInfoGauges(info) }` in the constructor (`./src/main/kotlin/eu/inqudium/tabellarium/MicrometerKafkaAppenderMetrics.kt:247-250`) is unproven; removing `safe` would make the constructor - and `bindMeterRegistry` - throw against a failing registry without any test noticing.
    - Triggering condition: only on that regression.
    - Impact: a broken registry would fail the whole bind instead of only the info gauges.
  - Fix strategy: construct once with an `AppenderInfo` in the hostile test.
  - **Status:** Fixed in f93e09e (2026-10-10). The hostile-registry test also constructs with an `AppenderInfo`. Mutation removing `safe { }`: red.

- [x] 9. [./src/test/kotlin/eu/inqudium/tabellarium/RenamingMeterMirrorTest.kt:163] {Low} {Confidence: medium} {test quality}
  - Symptom → cause: "drop later additions" proves only an addition *after* `close()` (guard at `./src/main/kotlin/eu/inqudium/tabellarium/RenamingMeterMirror.kt:79`); the real race - an addition passing that guard while `close()` sweeps - and its re-check at `:85` are untested; removing `:85` keeps the suite green.
    - Triggering condition: only on that regression, and then only when a `KafkaMetrics` refresh coincides with unbind.
    - Impact: an orphaned producer meter left in the operator's registry after stop.
  - Fix strategy: hold the target registration on a latch, run `close()` concurrently, then assert an empty target.
  - **Status:** Fixed in f93e09e (2026-10-10). New deterministic race test: the target's gauge registration parks on a latch, `close()` runs on a second thread until it blocks after its sweep, then the registration is released; asserts an empty target. Mutation removing the re-check at `:85`: red.

- [x] 10. [./src/test/kotlin/eu/inqudium/tabellarium/AppenderInfoTest.kt:130] {Low} {Confidence: medium} {test quality}
  - Symptom → cause: the expected default is read from `ProducerConfig.configDef().defaultValues()` - the same source the implementation uses - so the test only rules out blank/`unknown`, not a wrong lookup key; its rationale ("would hide that an idempotent producer runs with acks=all") is not asserted with the concrete value.
    - Triggering condition: only on a regression in the defaults lookup.
    - Impact: a mislabelled `acks` in `kafka.appender.topic.info`.
  - Fix strategy: additionally assert the concrete `"all"` for the idempotence case.
  - **Status:** Fixed in f93e09e (2026-10-10). Additionally asserts the concrete `"all"`. No mutation turns only this line red (the ConfigDef comparison already catches a wrong key); it pins the rationale's value against a Kafka default change.

- [x] 11. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderMetricsBindingTest.kt:185] {Low} {Confidence: medium} {test quality}
  - Symptom → cause: exact deltas (`isEqualTo(1.0)`, also `:240`, `:293`) are asserted while the appender is attached to the **root logger of the global LoggerContext** (`:49`); any SLF4J event from another thread in the JVM between the two reads is counted too - e.g. late WARNs from a producer network thread closed without join elsewhere (`./src/test/kotlin/eu/inqudium/tabellarium/KafkaProducerThreadNamingContractTest.kt:63`), whose client id the self-logging guard does not know.
    - Triggering condition: only on an unlucky overlap in execution order (heisenbug, low probability).
    - Impact: sporadic red build.
  - Fix strategy: attach the appender to a dedicated non-additive logger (the binding walks all loggers) instead of root.
  - **Status:** Fixed in f93e09e (2026-10-10). The fixture appenders hang on a dedicated non-additive logger instead of root, so no foreign event reaches them.

- [x] 12. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:2723] {Low} {Confidence: high} {test quality}
  - Symptom → cause: the rebind test's rationale promises that "the fallback gauges of the second read the live dispatcher", but only meter-id equality is asserted (`:2750-2752`); gauge liveness after the rebind is unproven.
    - Triggering condition: only on a regression where rebound gauges read a stale dispatcher.
    - Impact: green suite for dead queue gauges after a context refresh.
  - Fix strategy: assert a gauge value in the second registry, or drop the claim.
  - **Status:** Fixed in f93e09e (2026-10-10). After the rebind, events are diverted into a blocked fallback and the second registry's `fallback.queue.size` must reach 2. Mutation with a stale queue gauge: red.

- [x] 13. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:1175] {Low} {Confidence: high} {test quality}
  - Symptom → cause: `history()).hasSize(1)` does not establish *which* of the two events was delivered; an inverted guard (dropping the application thread, passing the producer thread) stays green.
    - Triggering condition: only on an inverted-guard regression.
    - Impact: narrow false confidence.
  - Fix strategy: assert the delivered record's value or originating thread.
  - **Status:** Fixed in f93e09e (2026-10-10). The two events carry different messages; the delivered payload must be the application thread's. Mutation with an inverted guard: red.

- [x] 14. [./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt:69] {Low} {Confidence: high} {test quality}
  - Symptom → cause: the mirror's dynamic path - meters `KafkaMetrics` adds/removes on its refresh schedule (node and topic metrics after connecting) - is covered only by unit tests; the real-producer tests never connect and the broker test binds no registry.
    - Triggering condition: only against a real broker.
    - Impact: a mirroring/removal gap under real conditions would go unnoticed.
  - Fix strategy: bind a registry in the integration test and assert that `kafka.appender.producer.topic.*` appears and disappears after `stop()`.
  - **Status:** Fixed in f93e09e (2026-10-10). New integration test binds a registry, delivers to a real broker and waits for `kafka.appender.producer.topic.*` and `.node.*` meters (no raw `kafka.producer*`), then asserts no `kafka.appender.*` meter after `stop()`. Takes ~62 s because `KafkaClientMetrics` refreshes every 60 s.

- [x] 15. [./src/test/kotlin/eu/inqudium/tabellarium/DocumentationContractTest.kt:118] {Low} {Confidence: high} {maintainability}
  - Symptom → cause: the contract checks only the "Metric inventory" section of the guide; the duplicated inventories in `./README.md` and `./docs/config/kafka-appender-config-guide.md` and the guide's breaker/producer tables are unchecked.
    - Triggering condition: on every metric change.
    - Impact: documentation drift (as in pattern P1).
  - Fix strategy: extend the parser to the other tables or generate them from one source.
  - **Status:** Fixed in f93e09e (2026-10-10). `DocumentationContractTest` now also checks the README and config-guide inventories (name, type, tags) and the metrics guide's breaker and producer tables against a bound appender. No current drift found.

## 6. Systemic patterns

- **P1 - Documentation and rationale drift after the 2026-10-10 metric rename** (~8 occurrences; counting basis: grep for `resilience4j\.`, `metric names`, `renaming composite`, `3.1.7` over `./src/main/kotlin`, `./src/test/kotlin` and `./pom.xml`). Representative locations: `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:641`, `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt:273`, `./src/test/kotlin/eu/inqudium/tabellarium/KafkaAppenderTest.kt:2723`, `:2801`, `:2809`, `:3029`, `./pom.xml:229`. Same-day changes (namespace rename, mirror replacing the composite approach, Jackson bump) left statements describing the previous state. Remediation: one sweep over these blocks; findings 6 and 7 are the user-facing ones.
  - **Status:** Fixed in f93e09e (2026-10-10): production KDoc, POM comments and the test rationale blocks (`KafkaAppenderTest`) now describe the `kafka.appender.*` namespace and the mirror mechanics.
- **P2 - Wall-clock waits in tests (tolerated)** (~47 `pollUntil` calls, grep `\bpollUntil\b` over `./src/test/kotlin` excluding the definition, plus ~6 `isLessThan(ms)` duration bounds). Anchored and generously sized; tolerated per the earlier reports, listed for the blast radius only.
  - **Status:** Tolerated, unchanged (as in earlier reports).
- **P3 - Non-injected time in production timing (tolerated)** (5 occurrences, grep `System.nanoTime` over `./src/main/kotlin`: `ResilientMessageSender.kt:286`, `:313`, `:339`, `ParallelClose.kt:42`, `:44`). Used for the send timer and shutdown budgets; the throttle, the one decision-making clock, is injected. Tolerated per the earlier reports.
  - **Status:** Tolerated, unchanged (as in earlier reports).
- **P4 - Real `KafkaProducer` instances against an unreachable broker in the default run** (5 places, grep `localhost:1` over `./src/test/kotlin`: the real-producer helper in `KafkaAppenderTest.kt` (3 tests), `JoranXmlConfigurationTest.kt`, `KafkaProducerThreadNamingContractTest.kt`). Deliberate and deterministic as long as nothing listens on port 1; producers closed without join feed cross-test log events (finding 11).
  - **Status:** Kept deliberately; its cross-test effect on the binding test is removed by finding 11's dedicated logger.
