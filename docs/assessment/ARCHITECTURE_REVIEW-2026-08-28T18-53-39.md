# Architecture & Appropriateness Analysis: tabellarium (Logback→Kafka appender)

1. Identification of the Codebase
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit-Hash:** `62669fcf60e3d25825765ce4d7770dc36c8bba80` (Full)
   - **Reference (Branch/Tag):** `refs/heads/main` (no tag); working tree clean
2. Scope of the Analysis
   - **Included:** `./src/main/kotlin/` (production, 15 files, 3 315 lines) and `./src/test/kotlin/` + `./src/test/resources/` — **test code IS included as an analysis subject in full depth** (the test-architecture section of Phase 2 applies); testability of the production code is additionally read as an architecture signal
   - **Excluded:** `./target/` (build output), `./docs/` (guides, dashboards, prior analysis reports — consulted as *documented architectural intent*, not analyzed as subjects), `./.github/`, `./mkdocs.yml`; `./pom.xml` consulted for stack/dependency appropriateness only
3. Analysis Environment & Tools
   - **Target Environment:** OpenJDK 21 (Temurin in CI), Kotlin 2.4.10
   - **Build system:** Maven (parent `spring-boot-starter-parent` 4.1.1), single module
   - **Analysis tools used:** manual architectural review (every in-scope file read completely in this session); grep-based counting for pattern reach. No SonarQube/ArchUnit/dependency-graph tooling configured in the project. The prior defect analysis `./docs/assessment/CODE_ANALYSIS-2026-08-28T18-21-41.md` was incorporated **after verification against the current commit** (its 18 findings were remediated in `7cd08d3d53a544665ba945b57a0aefb82f365509`; the remediated state is the state under review here, CI-green with 201 tests).
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium` (absolute reference point; all relative paths refer to it)
   - **Report output path:** `./docs/assessment/ARCHITECTURE_REVIEW-2026-08-28T18-53-39.md` (relative to the workdir; prefix + ISO 8601 timestamp per the naming rule; the existing `CODE_ANALYSIS-*` document remains untouched)
   - **Scope root (relative to the workdir):** `./src/`
   - **Path convention for findings:** `<path relative to the workdir>:<line>` (e.g. `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:42`)

---

> **Remediation status (addendum, 2026-08-28):** All 5 findings — including the systemic
> pattern of section 6, resolved in the documented direction by shipping the marker-mapping
> configuration surface — were fixed in commit
> `53cbfb1d1541e6223be89c225b8f4eb702a85e56` ("Fix all findings from the 2026-08-28
> architecture review") in a separate fix session following this analysis; the checkboxes
> below are ticked accordingly. The analysis content above is otherwise unchanged and
> describes the state at commit `62669fcf60e3d25825765ce4d7770dc36c8bba80` — this addendum
> and the ticked checkboxes are the only post-analysis edits.

---

## 1. Executive Summary

Measured against its problem — a resilient, compliance-aware Logback→Kafka appender for a regulated (banking) environment — this codebase is **predominantly well-proportioned, with one deliberate and clearly bounded exception**. The load-bearing complexity is genuinely load-bearing: the per-class circuit breakers, the half-open probe throttle, the queue-decoupled fallback dispatcher, and the optional-dependency seams (metrics interface + no-op, `Class.forName` probes for Micrometer/Resilience4j binders) each answer a real, named force — lock-free hot path for virtual-thread/reactive callers, a documented probe-burst problem, the Kafka I/O-thread blocking hazard, and a deliberately small transitive dependency tree. There is no reactive stack without need, no premature distribution, no pattern decoration, and — remarkable for a library of this ambition — exactly **two** interfaces in production, both with multiple real implementations. Under-engineering is essentially absent: logic and I/O are cleanly separated (pure functions + side-effect wrappers), and every boundary the domain has is drawn in code.

The one substantial appropriateness question is the **prepared-but-inert four-class compliance machinery**: AUDIT/FUNCTIONAL/PERFORMANCE classes, mandatory overrides, violation reporting, multi-producer registry, per-class breakers — fully built, fully tested, fully documented, and reachable by **no configuration**. The project applies YAGNI explicitly to the configuration surface while exempting the machinery behind it from the same rule; the cost is real (an entire docs appendix exists to explain what does *not* run, ~a third of the classification-related test budget exercises inert classes, and every maintenance change must honor four classes). Because the intent is documented and the machinery is cohesive and cheap at runtime, this lands at **Medium**, not High. The second Medium is a test-architecture gap at the product's actual contract: the declarative Joran/XML surface — the whole point of a Logback appender — has no executable evidence; everything is wired programmatically in tests.

**Test verdict:**

1. **Testability of the architecture:** Excellent by construction. The architecture offers real seams everywhere it is hard to instantiate infrastructure (`ProducerFactory`, injected clocks, the metrics interface, the synchronous-dispatcher hook), and core logic is verifiable without any container, broker, or Spring context. This is a structural property, not an accident.
2. **Utilization & shape of the pyramid:** The testability is actually used — 201 tests, all fast and isolated, zero `@SpringBootTest` (the two Spring tests use the lightweight `ApplicationContextRunner`), fakes instead of mock libraries throughout. The pyramid's shape matches the architecture's testability: no wasted potential.
3. **Most significant gaps & anomalies:**
   - The **declarative XML boundary is unexercised** — no test parses a real `logback.xml` through Joran (finding 2).
   - A visible share of the suite safeguards **machinery no configuration can reach** (part of finding 1).
   - The four-question comment convention (~80 blocks) is heavy ceremony, but a *documented* convention (`./CONTRIBUTING.md`) — a justifying force, therefore a note, not a finding.

## 2. Problem Baseline & Methodology

- **Core domain:** ship structured log events from Logback to Kafka without ever blocking the caller; enforce compliance-graded delivery per topic class; divert undeliverable events to a fallback; expose operational metrics. Code mass (3.3 k lines production) is proportionate to this — nothing suggests a system pretending to be bigger than its task.
- **Real requirements & scale:** hot path executed per log event in high-volume microservices; regulated environment (BaFin/MaRisk named in KDoc/README) mandating audit-grade delivery — a genuine, non-CRUD invariant set (never block, never feed back, never silently weaken `acks`). Team context: single maintainer (git history). Operational maturity: high (metrics catalogue, Grafana dashboards, migration-grade docs).
- **Documented architectural intent:** no formal ADR directory, but the intent lives — unusually densely — in README design sections, the config guide (incl. Appendix A explicitly labeling the four-class model "prepared, not yet active"), `./CONTRIBUTING.md` (lock-free hot path as a hard rule, no-Docker test rule, four-question test-comment convention, no mock libraries in practice), and extensive KDoc with rationale. These documents are treated as justifying forces per the review rules.
- **Technology coherence:** deliberately blocking, framework-agnostic library; Spring appears only as an optional, explicitly-imported binding class; virtual-thread-awareness via `UnsynchronizedAppenderBase` is coherent with the "never block the caller" invariant. Kafka client and Resilience4j are the only substantial runtime dependencies; Micrometer/Spring/logstash-encoder are `optional`. The stack fits the need with no paradigm mixing.
- **Test topology observed (baseline signal):** 201 tests across 17 files; unit-dominant; zero `@SpringBootTest`; two `ApplicationContextRunner` tests for the Spring binding; no Testcontainers (documented decision in README Future work); deterministic time/callback injection throughout. Infrastructure load per test: none to minimal.
- **Analyzed vs. not analyzed:** all production and test files read completely; `./docs/` guides read as intent, not audited line-by-line; build plumbing (CI, MkDocs, Dokka) out of scope.
- **Blind spots:** real-world consumer configurations (actual `logback-spring.xml` files of using services) are not visible from this repo, so the judgment on the unreachable machinery relies on the repo's own statement that no deployment uses marker mappings yet; runtime load figures are not available (no profiling data), so performance-motivated choices are judged by plausibility, not measurement.

## 3. Statistics

| Severity | Count |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 0 |
| 🟡 Medium | 2 |
| 🟢 Low | 3 |
| **Total findings** | **5** |
| **Systemic patterns** | **1** (see section 6) |

## 4. Ranking Table (Phase 1)

| Unit | Score | Rationale |
|---|---|---|
| Topic-classification subsystem (`./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt`, `TopicRouter.kt`, `TopicTable.kt`, `TopicMappingConfig.kt`, `ProducerPropertiesBuilder.kt`) | 5 | Highest structural density relative to *reachable* function; the inert-machinery question lives here |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt` | 5 | Composition root with the most dependencies; accumulates config surface, lifecycle, hot path, guard, and the metrics bind/unbind subsystem |
| Resilience subsystem (`ResilientMessageSender.kt`, `HalfOpenThrottle.kt`) | 4 | Custom concurrency primitive on top of Resilience4j — classic over-engineering suspect, must be checked against its stated force |
| Metrics subsystem (`KafkaAppenderMetrics.kt`, `MicrometerKafkaAppenderMetrics.kt`, `KafkaAppenderMetricsBinding.kt`) | 4 | Interface + no-op + reflection probes + optional Spring class — several deliberate indirections to justify |
| Test architecture (suite shape, conventions, fixtures) | 3 | Subject in its own right: heavy comment convention, fake-based seams, one out-of-scope external-contract test |
| `./src/main/kotlin/eu/inqudium/tabellarium/FallbackDispatcher.kt` | 3 | Own thread + queue machinery — heavy, but against a named hazard |
| `./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt` | 2 | Pure function; one micro-optimization to judge |
| `./src/main/kotlin/eu/inqudium/tabellarium/KafkaProducerPropertiesParser.kt`, `ProducerRegistry.kt` | 2 | Thin, purpose-shaped; registry's multi-producer generality belongs to the score-5 question |

**Explicitly judged appropriate (no findings, verified against their stated forces):** the half-open throttle (documented probe-burst problem, injectable clock, real contention test); the fallback dispatcher (Kafka I/O-thread hazard is real and documented); the metrics interface + NO_OP (optional-dependency force, two real implementations plus a test fake); `ProducerFactory` (a genuine seam — a real `KafkaProducer` cannot exist without a broker; used by every appender-level test, consistent with the no-mock-library practice); the `Class.forName` probe pattern (keeps three dependencies optional, documented); the deliberately non-auto-configured Spring binding class (documented rationale); the documentation mass itself (high, but pure operator value at near-zero structural cost).

## 5. Findings

### 🔴 Critical

Nothing to report in this section.

### 🟠 High

Nothing to report in this section.

### 🟡 Medium

- [x] 1. [Topic-classification subsystem — `./src/main/kotlin/eu/inqudium/tabellarium/TopicClass.kt:38`, `TopicRouter.kt:52`, `TopicTable.kt:47`, `ProducerPropertiesBuilder.kt:57`, `ProducerRegistry.kt:131`] {Medium} {Confidence: high} {Speculative Generality / Consistency} Fully built four-class compliance machinery that no configuration can reach
  - Actual structure: three of four `TopicClass`es (AUDIT, FUNCTIONAL, PERFORMANCE) with per-class mandatory/default overrides, violation recording and operator warnings, idempotence pre-validation, marker-based routing including hierarchical marker resolution, a multi-producer registry with per-class breakers, throttles, and client-ids — all implemented, documented (a dedicated docs appendix), and tested (90 references to `TopicClass.AUDIT` alone across 6 test files), while the XML surface (`TopicMappingConfig`) deliberately exposes only `<defaultTopic>`, which activates exactly one class (TECHNICAL) with zero mandates.
  - Solved problem / justifying force: partially documented. The compliance model itself is a real, regulatory-shaped requirement, and README/guide state the deferral of the config surface explicitly ("no production deployment uses it yet … adding speculative Joran setters would mean speculative tests" — YAGNI, verbatim). The inconsistency is that this YAGNI rationale is applied to ~40 lines of setters but *not* to the several-hundred-line machinery behind them, which is precisely the "speculative tests" case the KDoc argues against.
  - Cost: every maintenance change must honor four classes and their interactions (observable in this repo's own history: the client-id feature, the idempotence validation, and the self-logging guard all carried per-class handling for classes nobody can activate); a full docs appendix exists solely to explain what does not run; a visible share of the suite (6 of 17 test files touch inert classes) safeguards unreachable paths; each new reader must first learn which half of the subsystem is live. A secondary cost has already materialized: `TopicMappingConfig` was made `open` purely so a test could reach the otherwise-inert violation-warning path — a test seam whose only purpose is to activate machinery the product cannot.
  - Simpler alternative: either finish the last mile (the collection setters are, by the project's own estimate, small) so the machinery earns its keep, or cut the inert classes down to the one active class and keep the *design* (not the code) in the appendix until a deployment needs it. Both directions resolve the inconsistency; carrying both halves indefinitely is the only losing option.
  - Reversibility: moderate in either direction — the subsystem is cohesive and behind stable seams, so activating is cheap and dismantling is mechanical; severity stays Medium precisely because leaving it standing costs steadily but not steeply, and the documented intent is a genuine (if partially inconsistent) force.

- [x] 2. [Declarative configuration boundary — `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:80` (Joran-populated surface), suite-wide] {Medium} {Confidence: high} {Testability & Test Architecture} The product's actual contract — the Joran/XML surface — has no executable evidence
  - Actual structure: every appender-level test wires the appender programmatically (direct setter calls, `addAppender`); no test feeds a real `logback.xml` through `JoranConfigurator`. The XML surface (element names, nested `TopicMappingConfig` binding, `<appender-ref>` action, text-content trimming) is what operators consume — it is the reason this library exists — yet its correctness rests on naming conventions Joran resolves reflectively at runtime.
  - Solved problem / justifying force: none documented. This is not a deliberate decision anywhere in README/CONTRIBUTING; the defect analysis had already listed it as a blind spot.
  - Cost: a rename/type change on any Joran-visible setter, or a regression in the `AppenderAttachable` wiring, passes the entire green suite and surfaces only as a broken operator configuration in production — the most expensive place to discover it. The cost is amplified by the library's own "every Joran setter must be tested" convention, which is currently honored in letter (setters are tested) but not in the binding that gives them meaning.
  - Simpler alternative: a single round-trip test that runs `JoranConfigurator` over an in-repo XML fixture (the documented minimal configuration) with the test producer factory injected, asserting the appender starts and routes one event — no container, no broker, minimal fixed cost.
  - Reversibility: n/a (additive); the gap grows more expensive the longer the surface evolves without it.

### 🟢 Low

- [x] 3. [`./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:449`–`:640` (metrics binding/unbinding region)] {Low} {Confidence: medium} {Coupling & Cohesion} The composition root has absorbed a complete metrics-binding subsystem
  - Actual structure: ~190 of the orchestrator's ~700 lines are Micrometer wiring — bind, rebind, unbind, two `Class.forName` probe pairs, Resilience4j meter removal by tag-matching. Each piece is individually justified; collectively they give the appender a second responsibility with its own lifecycle.
  - Solved problem / justifying force: the *content* is forced (optional dependencies, deregistration on stop); its *location* is convenience — the appender is where the pipeline references live.
  - Cost: the class-level cognitive load of the central unit grows with every metrics concern; the recent meter-deregistration work had to be threaded through the appender's lifecycle rather than a dedicated binder's.
  - Simpler alternative: an internal `MetricsBindings` holder owning bind/unbind and the probes, constructed by the appender — one extraction, no new public surface.
  - Reversibility: cheap and low-risk (internal refactor behind existing tests) — which is also why this stays Low: it can be done opportunistically or not at all.

- [x] 4. [`./src/main/kotlin/eu/inqudium/tabellarium/MessageEnricher.kt:62`–`:96`] {Low} {Confidence: medium} {Premature Optimization} Shared pre-encoded header byte arrays trade an enforced invariant for a by-convention one — on an unmeasured saving
  - Actual structure: header values are UTF-8-encoded once and the same mutable `ByteArray` instances are passed by reference into every Kafka record; three KDoc blocks warn that mutating them "would corrupt subsequent events".
  - Solved problem / justifying force: ~5 small allocations saved per log event in a hot path — plausible for high-volume logging, but not measured, and the project's own yardstick elsewhere (README removed per-event debug formatting *with* an audit-finding rationale) shows it normally demands a named force.
  - Cost: an immutability contract that exists only in comments, across three classes; any future header consumer must rediscover it.
  - Simpler alternative: encode per event (and measure whether it ever matters), or keep the sharing and accept the documented convention — the current state is defensible, which is why this is Low.
  - Reversibility: trivial in either direction.

- [x] 5. [`./src/test/kotlin/eu/inqudium/tabellarium/LogstashHttpMethodKeyValueIngestTest.kt:39`] {Low} {Confidence: high} {Boundaries & Responsibilities} An external-contract characterization test lives in the library whose code it does not exercise
  - Actual structure: a well-built test pinning LogstashEncoder+Jackson behavior against an Elasticsearch mapping for an emitter call-site in *consuming services*; it imports no tabellarium type and is the sole reason for the `spring-web` test dependency. Since the defect-analysis remediation it is clearly marked (`@Tag("external-contract")`, KDoc banner), which caps the confusion cost.
  - Solved problem / justifying force: executable incident documentation — real value, wrong module.
  - Cost: a foreign dependency in the test classpath; upgrade failures of `logstash-logback-encoder` will fire in this repo and point maintainers at the wrong codebase (the KDoc now redirects them, at the price of reading it).
  - Simpler alternative: relocate to the emitting service's repository (or a shared contract-test module) when one exists.
  - Reversibility: trivial (move a file).

## 6. Systemic Patterns

1. **"Machinery first, surface never (yet)"** — the same asymmetry as finding 1, listed as the pattern it is because it spans units: inert capability exists in `TopicClass` (3 of 4 classes), `TopicRouter` (marker mappings + hierarchical resolution), `TopicTable` (multi-class tables and non-default fallbacks), `ProducerRegistry` (multi-producer lifecycle), `ProducerPropertiesBuilder` (mandatory-override and idempotence handling for inert classes), `ResilientMessageSender` (per-class breaker/throttle maps), and the per-class `client.id` scheme — **~7 production units** carrying capability the configuration surface cannot activate (counting basis: complete read of `./src/main/kotlin/`), mirrored by inert-class coverage in **6 of 17 test files** (grep for inert `TopicClass` references). One resolution — in either direction (ship the surface, or shrink to the active class) — collapses the entire pattern at once; that leverage is why it is worth seeing as one decision, not seven findings.

Positive counter-observation (not a finding, recorded for balance): the optional-dependency handling is executed as a *consistent, disciplined* pattern (interface + no-op default, `Class.forName` probe before typed call, `<optional>true</optional>` in the pom, explicit-import Spring class) across all three optional integrations — this is what appropriate seam-building looks like, and it is the reason the library's dependency footprint stays honest.

---

*Read-only analysis at commit `62669fcf60e3d25825765ce4d7770dc36c8bba80`; this report is the only write operation. Re-architecture is deliberately not performed (scope boundary); each finding carries a directional strategy only.*
