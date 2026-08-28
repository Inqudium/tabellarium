# Contributing to Tabellarium

Thank you for considering a contribution! This document describes how to
build the project, what the code is expected to look like, and how to get
a change merged.

## Building

Standard Maven module, Java 21 and Kotlin:

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

## Code style

- Kotlin sources follow the default ktlint rule set; the build fails on
  violations. Run `mvn ktlint:format` to auto-fix most issues.
- KDoc on public classes explains *why* the component exists and which
  constraints it upholds, not just what each method does. Match the
  existing density and tone.
- The appender hot path (`KafkaAppender.append` and everything it calls)
  must stay lock-free: no `synchronized`, no blocking waits. Atomics and
  volatiles only.

## Tests

- Every Joran-visible setter and every behavioral guarantee has a test.
  New configuration surface without a test will not be merged.
- Test comments follow the existing four-question pattern (what is
  tested, how success is determined, why it matters).
- Unit tests use `MockProducer` from `kafka-clients`; no test in the
  **default run** may require a running Kafka broker or Docker.
- Tests tagged `integration` (Testcontainers-based real-broker tests)
  are excluded from the default run; execute them deliberately with
  `mvn -Pintegration test` (needs a Docker daemon).
- Tests tagged `external-contract` (characterization tests of third-party
  behavior that exercise no tabellarium code) are excluded from the
  default run; execute them deliberately with `mvn -Pexternal-contract test`.
- Appender-level tests run the real asynchronous dispatch (there is no
  synchronous test mode in production code): either `stop()` the appender
  before asserting - the stop sequence drains the queues - or poll with
  the shared `pollUntil` helper.

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
