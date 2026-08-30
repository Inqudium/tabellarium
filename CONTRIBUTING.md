# Contributing to Tabellarium

Thank you for considering a contribution! This document describes how to
build the project, what the code is expected to look like, and how to get
a change merged.

## Building

Standard Maven module, Java 21 and Kotlin (building needs JDK 24+ - CI uses
25, and `.mvn/jvm.config` passes flags a pre-24 JVM rejects; the published
artifact still targets Java 21):

```bash
mvn verify                                      # build + run all tests + ktlint
mvn -Dtest=KafkaAppenderTest test               # run a single test class
mvn -Dtest='*MessageEnricher*' test             # pattern-match
```

`mvn verify` must pass before a pull request is opened — it runs the full
test suite and the [ktlint](https://pinterest.github.io/ktlint/) check.

### Dependency vulnerability scan

CI additionally scans the **resolved** dependency graph against the
[OSV](https://osv.dev/) database and fails on any known advisory. It runs
on every push and pull request, and weekly — a newly published advisory
has to surface even when nothing was committed. To reproduce it locally
(requires a container runtime):

```bash
mvn cyclonedx:makeBom                 # SBOM of the resolved graph → target/bom.json
docker run --rm -v "$PWD:/repo" \
  ghcr.io/google/osv-scanner-action:v2.5.1 --lockfile=/repo/target/bom.json
```

The scan uses an SBOM rather than `pom.xml` because most versions come
from the Spring Boot BOM and never appear in `pom.xml`; test-scoped
dependencies are excluded, since they reach no consumer.

When an advisory appears, prefer fixing it — usually a version pin in
`<dependencyManagement>` even when the affected artifact is transitive
(see the `lz4-java` entry in `pom.xml` for the shape, including the
rationale comment such a pin is expected to carry). Only if an advisory
is genuinely unfixable *and* provably not exploitable here, record it in
an `osv-scanner.toml` with the reason and the date it was assessed — do
not remove the gate.

### Static analysis (CodeQL)

The dependency scan covers *published advisories in dependencies*; the
`CodeQL` workflow covers the complementary half — this project's own
code (`java-kotlin`) and the workflow definitions themselves
(`actions`). It runs on every push and pull request and weekly, and
results land in the repository's **Security → Code scanning** tab
rather than in the build log. A finding there is triaged like a review
comment: fix it, or dismiss it in the UI with a written reason.

### Fuzzing (Jazzer `@FuzzTest`)

The components that parse or bound externally influenced data - the
`<kafkaProducerProperties>` parser, topic-name validation and marker
routing, and the MDC-derived partitioning key - are fuzzed with
[Jazzer](https://github.com/CodeIntelligenceTesting/jazzer)'s JUnit 5
integration: `*FuzzTest.java` classes under `src/test/java`, stating their
invariants in the test body. They run in two modes:

- **Regression mode, in every build.** `mvn verify` executes each fuzz test
  against its checked-in inputs (`src/test/resources/**/<Class>Inputs/`)
  plus the empty input - cheap, deterministic, part of the normal suite.
- **Fuzzing mode, nightly.** The `Fuzz` workflow sets `JAZZER_FUZZ=1` and
  runs each target in its own job (Jazzer fuzzes only one `@FuzzTest` per
  JVM), each capped by its `@FuzzTest(maxDuration = ...)`.

A finding is written into the seed-corpus directory next to the test
sources (the nightly run also uploads it as a workflow artifact): commit it
there and it becomes a permanent regression input; then fix the code. New
parsing/validation surface should bring a fuzz target stating its
invariants, like the existing ones do - in JAVA, not Kotlin: the OpenSSF
Scorecard fuzzing detector only recognizes Jazzer in `*.java` files.

To fuzz locally (no Docker needed):

```bash
JAZZER_FUZZ=1 mvn -Dtest=TopicRouterFuzzTest test
```

## Code style

- Kotlin sources follow the default ktlint rule set; the build fails on
  violations. Run `mvn ktlint:format` to auto-fix most issues.
- KDoc on public classes explains *why* the component exists and which
  constraints it upholds, not just what each method does. Match the
  existing density and tone.
- Comments that state a rationale, an invariant, a workaround, or a
  compatibility constraint use the documented prefix vocabulary — see
  [ADR-0001](docs/adr/ADR-0001-comment-prefix-vocabulary.md). Prefixes
  make the stock greppable (`Workaround:` is a work list on every
  dependency upgrade; `Invariant:` yields a module's invariant catalog).
- The appender hot path (`KafkaAppender.append` and everything it calls)
  must stay lock-free: no `synchronized`, no blocking waits. Atomics and
  volatiles only.

## Tests

- Every Joran-visible setter and every behavioral guarantee has a test.
  New configuration surface without a test will not be merged.
- Test comments follow the existing three-question pattern (what is
  tested, how success is determined, why it matters).
- Unit tests use `MockProducer` from `kafka-clients`; no test in the
  **default run** may require a running Kafka broker or Docker.
- Tests tagged `integration` (Testcontainers-based real-broker tests)
  are excluded from the default run; execute them deliberately with
  `mvn -Pintegration test` (needs a Docker daemon). CI runs this stage
  as its own job on every push/PR and weekly, so it cannot rot
  silently. The stage profiles compose:
  `mvn -Pexternal-contract,integration test` runs everything.
- Tests tagged `external-contract` (characterization tests of third-party
  behavior that exercise no tabellarium code) are excluded from the
  default run; execute them deliberately with `mvn -Pexternal-contract test`.
- Appender-level tests run the real asynchronous dispatch (there is no
  synchronous test mode in production code): either `stop()` the appender
  before asserting - the stop sequence drains the queues - or poll with
  the shared `pollUntil` helper.
- The [test-evidence page](https://inqudium.github.io/tabellarium/tests/test-evidence/)
  and the [coverage report](https://inqudium.github.io/tabellarium/coverage/)
  on the docs site are GENERATED by the Docs workflow from the Surefire
  and JaCoCo output (`.github/scripts/`); never edit `docs/tests/` by
  hand or check it in. Your test's rationale comment is what appears
  there - another reason to keep the three-question pattern intact.

## Pull requests

1. Fork and create a feature branch from `main`.
2. Keep the change focused — one logical change per pull request.
3. Make sure `mvn verify` passes.
4. Update the README / `docs/` when the configuration surface or the
   metrics inventory changes.
5. Open the pull request with a description of *what* changed and *why*.

## Reporting bugs and requesting features

Use the [issue templates](https://github.com/Inqudium/tabellarium/issues/new/choose).
For suspected security vulnerabilities, follow [SECURITY.md](SECURITY.md)
instead of opening a public issue.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
