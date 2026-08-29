# Architecture & Appropriateness Analysis: tabellarium — 3rd round

1. Identification of the Codebase
   - **Repository:** `https://github.com/Inqudium/tabellarium.git`
   - **Commit-Hash:** `6841d0fe7bdd59e57451ed6183059601274c3fcd` (Full)
   - **Reference (Branch/Tag):** `refs/heads/main`; no tag at the audited commit
   - **Working-tree state:** clean (`git status --short` empty)
2. Scope of the Analysis
   - **Included:** `./pom.xml`, `./src/main/kotlin/` (17 files, 4 784 lines), `./src/test/kotlin/` (21 files, 8 647 lines), `./README.md`, `./CONTRIBUTING.md`, `./docs/adr/`, `./docs/config/`, `./docs/metrics/`, and — as operational-architecture context — `./.github/workflows/` and `./.github/scripts/`
   - **Test code as analysis subject:** yes, in full depth (test architecture assessed as its own appropriateness subject, Phase 2 section B), in addition to the always-on reading of testability as an architecture signal
   - **Excluded:** `./target/` and `./site/` (build outputs), `./docs/assets/`, license/community files, and prior analysis documents under `./docs/assessment/` as assessment subjects — the two earlier architecture reviews (2026-08-28T18-53-39, 2026-08-29T00-34-03) and the defect/style/comment reports were used as history only; every adopted claim was re-verified against the current code state (verification results in section 2)
3. Analysis Environment & Tools
   - **Target Environment:** Java 21 (Temurin in CI), Kotlin 2.4.10, Spring Boot parent 4.1.1 (Spring optional at runtime); non-reactive Kafka client behind per-class worker threads
   - **Inspection runtime:** Oracle JDK 26.0.1 on Linux amd64
   - **Build system:** Apache Maven 3.9.15, single-module JAR; ktlint + JaCoCo + Dokka (`failOnWarning`) enforced in the build
   - **Analysis tools used:** complete manual structure/dependency/control-flow/lifecycle analysis (all production files and the majority of test files known from full reads in this session, re-verified at this commit), ripgrep with stated counting bases, Git metadata. Build/test execution: not run for this analysis (pure-analysis write boundary); the suite's state is CI-verified green at this exact commit (run `33222440180`).
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/tabellarium` (absolute reference point; all relative paths refer to it)
   - **Report output path:** `./docs/assessment/ARCHITECTURE_REVIEW-2026-08-29T02-16-27.md`
   - **Scope root (relative to the workdir):** `./`
   - **Path convention for findings:** `<path relative to the workdir>:<line>`

---

## 1. Executive Summary

This third round examines the architecture after the heaviest structural day in the repository's short history: since the second review (2026-08-29T00-34), the test-only synchronous execution modes were removed from production, the appender test layer migrated onto the real asynchronous path, a Testcontainers broker stage and an external-contract stage were separated out, the all-open plugin was dropped, delivery guarantees were re-scoped to the producer-policy level, an ADR series was seeded, and a generated test-evidence/coverage pipeline was added. **All five fixed findings of round 2 were re-verified as holding, and both deliberately-retained decisions (own metrics binder, external-contract test placement) remain documented and unchanged in substance.** The overall verdict improves accordingly: the production architecture now has exactly **one** execution model, its complexity is almost entirely load-bearing (bounded queues, per-class isolation, exactly-once shutdown accounting — each answering a named force), and the observability/documentation machinery around it is generated rather than hand-maintained. The residual mismatches this round finds are **boundary and evidence questions, not structure questions**: the library's *published* API surface (16 public top-level declarations) is roughly twice its *composable* surface — eleven public building blocks have no public wiring path into the appender, while the README claims constructor-injection substitutability that the `internal` seams contradict (finding 1, cheap to decide now at `1.0.0-SNAPSHOT`, expensive after a release); the new broker stage — the only wire-level evidence — is executed by no automation and will rot silently (finding 2); and the README's architecture self-description still draws the pre-`SendDispatcher` pipeline (finding 3). Nothing Critical, nothing High; the trend across the three rounds is clearly toward a system whose means match its stated purpose.

**Test verdict:**

1. **Testability of the architecture:** very good, and now *proven* rather than merely available: the async migration removed the last bypass, so the seams (`ProducerFactory`, injected clocks, function-shaped dispatch actions, `KafkaAppenderMetrics`) carry the entire appender-level suite against the real worker architecture.
2. **Utilization & coverage (test pyramid):** healthy and fully layered — 253 fast offline tests (no broker, no Docker, ~55 s), a real-broker stage (`-Pintegration`, 1 end-to-end proof incl. the AUDIT acks/idempotence handshake), an external-contract side stage, 91 % instruction coverage, and the suite's shape is now *published* (generated test-evidence page + coverage report) instead of latent.
3. **Most significant gaps & anomalies:**
   - The broker stage runs only when a human remembers it — no workflow executes `-Pintegration` (finding 2).
   - Combined profile activation (`-Pexternal-contract,integration`) silently drops one tag group (finding 4, Low).
   - No untested score-4/5 logic remains; the round-2 anomaly (default fixture bypassing the production path) is verified gone.

## 2. Problem Baseline & Methodology

- **Core domain (unchanged since round 2, re-verified):** a Logback appender that encodes structured events, routes them by SLF4J marker to Kafka topics, applies per-topic-class producer policies with mandatory overrides, isolates classes via circuit breakers and per-class send workers, and diverts undeliverable events to an optional fallback appender through bounded queues. A library, not a service; Spring only as optional metrics glue.
- **Delivery-guarantee decision (round-2 finding 1, now the baseline):** the product is explicitly a **best-effort transport with visible loss**; topic classes govern producer policy only. Naming, README ("Delivery guarantees" section), config guide, and `TopicClass` KDoc state this consistently — verified at this commit. Appropriateness is measured against *this* declared purpose, and structure and claim now match.
- **Real requirements & scale:** unchanged evidence situation — plausible, documented forces (non-blocking callers, bounded memory, per-class isolation, visible loss, clean shutdown, optional dependencies); concrete production rates, queue-sizing data, and consumer deployments remain **[MISSING - please supply]**. Team context: single maintainer (git history, 70+ commits in ~35 hours — an AI-assisted remediation cadence; noted because it makes "documentation lags structure" the dominant drift mode, which findings 1 and 3 confirm).
- **Documented architectural intent:** now includes a real anchor set — `./docs/adr/ADR-0001-comment-prefix-vocabulary.md` (process-level), `./CONTRIBUTING.md` (test conventions, generated-artifact rules), the delivery-guarantees section, and the standing decisions recorded in the round-2 review's status entries (own Resilience4j binder for its teardown lifecycle; external-contract test kept until its target repository exists). Both standing decisions were re-checked: the binder still carries its corrected justification in `./src/main/kotlin/eu/inqudium/tabellarium/MetricsBindings.kt`, and no new fact undermines either decision. They are **not** re-litigated here.
- **Prior-finding verification (adoption basis):** round-2 findings 1, 3, 4, 5, 7 — verified fixed at this commit (grep: zero `synchronous`/`useSynchronous*` occurrences in `./src/`; `KafkaBrokerIntegrationTest` present and CI-green locally per its commit trail; no all-open plugin in `./pom.xml`; scoped guarantee language in all named surfaces; worker-based `max.block.ms` rationale). Round-1 findings likewise still resolved (the `<mapping>` surface is active and Joran-tested).
- **Technology coherence:** unchanged and sound — blocking Kafka client behind dedicated workers, no reactive theater, optional dependencies consistently gated. New since round 2: the build/docs pipeline gained three small stdlib-only Python generators and always-on JaCoCo; proportionate to their job (each < 160 lines, no dependencies), and every published number is generated from a build rather than maintained by hand — a deliberate anti-drift architecture worth naming as such.
- **Test topology:** 258 tests total — 253 in the default offline run, 1 broker-stage test (`integration` tag), 4 external-contract tests; 91 % instruction coverage; per-test infrastructure load: zero containers/contexts in the default run except 4 lightweight `ApplicationContextRunner` uses and 3 Joran tests with a real (unreachable-broker) producer. The pyramid's shape and its published evidence match the architecture's seams.
- **Known blind spots:** no consumer repositories, runtime metrics, or incident data; the broker stage's greenness is asserted from its last manual run, not from CI (that gap is finding 2); the Python generators were reviewed for proportionality, not line-by-line audited (outside the Kotlin scope).

## 3. Statistics

| Severity | Count |
|---|---:|
| 🔴 Critical | 0 |
| 🟠 High | 0 |
| 🟡 Medium | 3 |
| 🟢 Low | 1 |
| **Total findings** | **4** |
| **Systemic patterns** | **2** |

## 4. Ranking Table (Phase 1)

| Unit | Score | Rationale |
|---|---:|---|
| Public API surface / module boundary (16 public declarations vs. operator surface) | 4 | A library's published boundary is its most expensive-to-change artifact; README substitutability claim contradicts `internal` wiring (→ finding 1) |
| Evidence architecture: broker stage + external-contract stage + generated docs pipeline | 4 | New since round 2; the only wire-level proof has no automation (→ findings 2, 4) |
| README architecture self-description | 3 | Diagram and component contract predate the `SendDispatcher` architecture (→ finding 3) |
| Metrics subsystem (4 classes, 1 188 lines ≈ 25 % of production) | 3 | Standing documented decision from round 2 — re-verified, not re-litigated; residual upgrade-tracking cost noted |
| Delivery core (`KafkaAppender` + dispatchers + sender + registry) | 2 | Round-2 remediation verified; single execution model, load-bearing complexity, no new mass |
| Routing/composition (`TopicMappingConfig`, `TopicRouter`, `TopicTable`, `ProducerPropertiesBuilder`) | 1–2 | Pure, validated, proportionate — unchanged |
| Test architecture (post-migration) | 2 | The round-2 score-4 concern is resolved; assessed healthy (see test verdict) |

**Explicitly judged appropriate this round (no findings):** the async-only execution model and its stop-drain/poll test strategy (the migration cost was paid once, the dual-architecture maintenance cost is gone); the tag+profile solution for the broker stage *as a build structure* (simpler than the once-mooted separate module — only its automation is missing); always-on JaCoCo; the three stdlib-only generator scripts and the generated-not-maintained principle behind the docs pipeline; Dokka `failOnWarning` as a reference guardrail; the duplicate `verify` in the Docs workflow (documented force: evidence must describe the deployed commit, red suites must block evidence publishing); the ADR series at its current minimal size.

## 5. Findings

### 🔴 Critical

Nothing to report in this section.

### 🟠 High

Nothing to report in this section.

### 🟡 Medium

- [x] 1. [Public API boundary — `./src/main/kotlin/eu/inqudium/tabellarium/` (11 declarations, see counting basis), `./src/main/kotlin/eu/inqudium/tabellarium/KafkaAppender.kt:167` (`internal var producerFactory`), `./src/main/kotlin/eu/inqudium/tabellarium/ProducerRegistry.kt:60` (`effectiveProperties`), `./README.md:720`] {Medium} {Confidence: high} {Boundaries & Responsibilities / Consistency} The published API surface is roughly twice the composable surface — public building blocks without a public way to use them
  - Actual structure: 16 public top-level declarations exist; the documented operator surface needs about five (`KafkaAppender`, `TopicMappingConfig`/`TopicMappingEntry`, `TopicClass`, `KafkaAppenderMetricsBinding`). The other eleven (`TopicRouter`, `TopicTable`, `ProducerRegistry`, `ProducerFactory`, `ProducerPropertiesBuilder`, `TopicClassProperties`, `MandatoryOverrideViolation`, `MessageEnricher`, `EnrichedRecord`, `parseKafkaProducerProperties`, `KafkaAppenderMetrics`) are published by Dokka as API but have **no public composition path**: the appender's substitution seams are `internal` (`producerFactory`, `circuitBreakerRegistry`), `ResilientMessageSender` — without which no pipeline can be assembled — is `internal`, `KafkaAppenderMetrics` has no public install point, and `MessageEnricher`'s extractor parameter is reachable only from inside the module. The README's architecture section nevertheless states every component "can be substituted via constructor injection in `KafkaAppender` for testing or extension", and the extension-points section advises injecting a custom extractor "when the appender is built" — neither is possible for an external consumer.
  - Solved problem / justifying force: none documented for the width. Tests do not need it (same module, `internal` suffices — and the actually-used test seams *are* `internal`). The round-1 review's substitutability praise described module-internal reality, not a consumer contract. A deliberate building-blocks API would be a valid force, but then the wiring would have to deliver it — today it does neither.
  - Cost: a semver compatibility commitment on eleven types the library cannot honor as a toolkit; misleading extension documentation (README sends consumers to a door that is locked); and one concrete sharp edge — `ProducerRegistry.effectiveProperties` exposes the raw, credential-bearing producer property maps on a public type (security overlap, noted not pursued: today unreachable from outside because nothing public creates a registry, which is exactly the inconsistency). Every future release hardens this surface further.
  - Simpler alternative: decide the boundary once, before the first release — either narrow visibility to the operator surface (`internal` for the eleven, matching the wiring that already exists), or commit to a toolkit API and open the corresponding seams publicly. The first is a few keywords plus a README paragraph; the second is a designed feature.
  - Reversibility: **asymmetric in time** — at `1.0.0-SNAPSHOT` this is one cheap decision; after a consumer-facing release it becomes a breaking change with deprecation cycles. That timing, not today's runtime cost, is why this is Medium rather than Low.
  - **Status:** Fixed in `0a7da9b` (2026-08-29), narrow-the-surface direction per the maintainer's decision. The eleven declarations (plus `TopicMappingConfig`'s `toTopicRouter`/`toTopicTable` factories) are `internal`; the supported public API is the operator surface (`KafkaAppender`, `TopicMappingConfig`/`TopicMappingEntry`, `TopicClass`, `KafkaAppenderMetricsBinding`), recorded as `docs/adr/ADR-0002-public-api-is-the-operator-surface.md` with the boundary table and the extension rule (widen the operator surface instead of exposing internals). The README architecture paragraph and extension-points section describe the boundary instead of the former substitutability claim; `ProducerRegistry.effectiveProperties` is no longer reachable from outside the module. Verified end-to-end: compiler-clean (no public signature exposes an internal type), 253 tests green (test compilation remains a friend module), and a clean `dokka:dokka` run with `failOnWarning` renders exactly the five operator-surface types. This also resolves systemic pattern 1 and the substitution/extension instances of pattern 2; finding 3's diagram remains open.

- [ ] 2. [Evidence automation — `./.github/workflows/ci.yml`, `./.github/workflows/docs.yml`, `./src/test/kotlin/eu/inqudium/tabellarium/KafkaBrokerIntegrationTest.kt`, `./pom.xml:538`] {Medium} {Confidence: high} {Testability & Test Architecture} The only wire-level evidence (broker stage) is executed by no automation
  - Actual structure: the round-2 remediation created the Testcontainers broker stage exactly as recommended (tag `integration`, profile `-Pintegration`, default loop offline). But no workflow invokes it: CI and Docs both run plain `verify` (grep over `./.github/workflows/` finds no `-Pintegration`). The stage has run only by hand, on one machine, at two commits.
  - Solved problem / justifying force: keeping Docker out of the default loop is documented and correct; nothing documents *never* running the stage in CI. GitHub's `ubuntu-latest` runners support Testcontainers natively, so the historical obstacle does not exist.
  - Cost: the classic fate of unexecuted test stages — silent rot. A kafka-clients or Testcontainers bump, an image retag, or a wire-relevant producer change merges green while the only broker-acceptance proof breaks unnoticed; the stage's value then evaporates precisely when someone finally needs it. The cost is small today and grows with every dependency bump (Dependabot is active — bumps are frequent).
  - Simpler alternative: one scheduled (e.g. weekly, like the existing CI cron) or push-triggered CI job running `mvn -Pintegration test` — additive, no effect on the fast loop; the existing weekly-scan rationale in `ci.yml` ("a scan that only runs on changes cannot see the world changing") applies verbatim.
  - Reversibility: trivially additive; only CI minutes are at stake.

- [ ] 3. [Architecture self-description — `./README.md:687` (diagram), `./README.md:718` (component contract paragraph)] {Medium} {Confidence: high} {Consistency} The README architecture diagram still describes the pre-SendDispatcher system
  - Actual structure: the diagram shows `append → encode → route → enrich → send` flowing directly into `ResilientMessageSender`, with `TopicRouter`/`TopicTable`/`MessageEnricher`/`ProducerRegistry`/`ResilientMessageSender` as the complete component set. `SendDispatcher` and `FallbackDispatcher` — the bounded-queue workers that *are* the never-block promise and roughly a quarter of the delivery core — do not appear. The paragraph beneath calls every component "a pure function or a side-effect-isolated wrapper" with "its own dedicated unit test", which the two thread-owning dispatchers strain, and repeats the substitution claim of finding 1.
  - Solved problem / justifying force: none — this is the same rationale-drift mode round 2 flagged as systemic (its prose instances were fixed; the diagram was not part of that finding's named surfaces and survived).
  - Cost: the architecture section is the onboarding entry point; a newcomer building their mental model from it misses the system's central asynchronous element and must un-learn it in the code — the exact cost class the round-2 systemic pattern described.
  - Simpler alternative: redraw the diagram with the two dispatchers and the hand-off point, and reword the component paragraph to the actual component taxonomy (pure functions, side-effect wrappers, queue-owning workers).
  - Reversibility: trivial documentation change.

### 🟢 Low

- [ ] 4. [Build profiles — `./pom.xml:527`, `./pom.xml:538`] {Low} {Confidence: medium} {Dependency & Build Appropriateness} Combined profile activation silently drops a tag group — `-Pexternal-contract,integration` resolves `surefire.excludedGroups` to the later-declared profile's value (`external-contract`), so the explicitly requested external-contract tests are excluded without any signal; the symmetric case drops the integration tests. Rare invocation, surprising outcome. Simpler alternative: an additional combined profile, or excluding via two independent properties so the profiles compose. Reversibility: trivial.

## 6. Systemic Patterns

1. **Published surface exceeds composable surface** — one boundary decision, visible at ~11 locations: eleven public top-level declarations without a public composition path (counting basis: `rg` for non-`internal` top-level declarations in `./src/main/kotlin/` = 16, minus the five operator-surface types), plus the two `internal` seams and one `internal` class that block external composition, plus two README passages promising it anyway. Finding 1 is the anchor; the pattern statement is that this is *one* decision (what is this library's API?) whose absence surfaces in many places — resolving it collapses all of them at once, and `1.0.0-SNAPSHOT` is the last cheap moment.
2. **Architecture self-description lags structural evolution** — the continuation of round 2's rationale-drift pattern, now confined to the README architecture section: **3 locations** (the diagram, the component-contract paragraph, the extension-points advice; counting basis: full read of `./README.md` against the current pipeline). The KDoc layer, by contrast, was verified current in this session's comment audit — the drift now lives exclusively in the high-level self-description, which is also the least tool-guarded artifact (Dokka checks KDoc references; nothing checks the README's claims). Findings 1 and 3 carry the instances.

**Positive counter-patterns (no findings, recorded for balance):** the remediation rounds have *removed* architecture rather than added it (dual execution model deleted, all-open plugin deleted, sync test hooks deleted) — the rare direction; every new operational number (coverage, test evidence, badges, CI summaries) is generated from builds with the hand-maintained variant structurally impossible (gitignored + strict-build-guarded); bounded queues, per-active-class resources, and optional-dependency gating remain uniformly applied; and the standing decisions from prior rounds are documented where the next maintainer will look, which is what makes this round's "not re-litigated" adoptions legitimate.

---

*Pure analysis of commit `6841d0fe7bdd59e57451ed6183059601274c3fcd`. This report is the only write operation; no code, configuration, or existing analysis document was modified. All improvement directions are deliberately described as strategy only.*
