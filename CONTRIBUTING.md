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
- Unit tests use `MockProducer` from `kafka-clients`; no test may require
  a running Kafka broker or Docker.
- Tests tagged `external-contract` (characterization tests of third-party
  behavior that exercise no tabellarium code) are excluded from the
  default run; execute them deliberately with `mvn -Pexternal-contract test`.

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
