# Security Policy

## Supported versions

| Version        | Supported |
| -------------- | --------- |
| latest release | yes       |
| older releases | no        |

## Reporting a vulnerability

Please do **not** open a public issue for suspected security
vulnerabilities.

Instead, use GitHub's private vulnerability reporting:
[Report a vulnerability](https://github.com/Inqudium/tabellarium/security/advisories/new).

You can expect an acknowledgment within a few days. Please include a
description of the issue, the affected version, and — if possible — a
minimal reproduction.

## Scope notes

Tabellarium is a logging appender: it handles log event content and the
Kafka producer configuration (which may contain credentials such as SSL
keystore passwords in `<kafkaProducerProperties>`). Reports about
credential leakage through status messages, metrics, or fallback output
are particularly relevant.

Measures already in place, so you know what is expected behaviour:

- **Status output never echoes your configuration.** The `<debug>`
  diagnostics print only the settings the appender *generated* (a diff
  against your base properties), and a failing producer construction
  reports the exception type only — its Kafka-authored message and stack
  trace stay behind `<debug>`.
- **The partitioning key is length-bounded** (128 characters), because it
  originates in the MDC and is therefore potentially attacker-influenced.
- **Compliance-graded classes warn about cleartext transport.** An `AUDIT`
  or `FUNCTIONAL` topic served over `PLAINTEXT` produces a startup
  warning; the appender signals rather than enforces, since certificates
  are the operator's to supply.
- **Log forging resistance is an encoder property.** The appender ships
  the encoder's bytes verbatim; a JSON encoder is recommended for that
  reason, not only for parseability.
- **Dependencies are scanned continuously.** CI builds a CycloneDX SBOM
  of the resolved graph and fails on any advisory known to OSV — on
  every change and weekly, so newly published advisories surface without
  a commit. Dependabot proposes the version bumps.
- **The code itself is statically analysed.** A CodeQL workflow analyses
  the library sources and the CI workflow definitions on every change
  and weekly; results appear under Security → Code scanning.
- **The configuration parsers and the MDC-derived key are fuzzed.** The
  `<kafkaProducerProperties>` parser, topic-name validation/routing, and
  the partitioning-key bounding are Jazzer `@FuzzTest` targets with their
  invariants asserted in the test body: explored nightly by the Fuzz
  workflow, and replayed against the checked-in findings in every build.
- **Release assets carry SLSA build provenance.** The Release workflow
  rebuilds the jar and the SBOM from the release tag, uploads them to the
  GitHub release, and attaches Sigstore-signed SLSA provenance
  (`*.intoto.jsonl`) — verifiable with
  [slsa-verifier](https://github.com/slsa-framework/slsa-verifier). Maven
  Central artifacts are additionally GPG-signed by the release-central
  profile.
- **The repository's supply-chain posture is scored publicly.** The
  OpenSSF Scorecard badge in the README links to the current per-check
  breakdown. Read it as a posture indicator, not as a grade: several
  checks assume a multi-maintainer, pull-request-based project and score
  low by construction for a single-maintainer one. Where a deduction is
  a deliberate trade-off, the reason sits next to the decision — see the
  `repo_token` note in `.github/workflows/scorecard.yml`. The **Fuzzing**
  check in particular can read 0 despite the nightly Jazzer fuzzing
  described above: Scorecard only scans for Jazzer targets when Java
  holds a "prominent" share of the repository's bytes, and this
  Kotlin-dominated codebase sits below that threshold — so that score
  tracks the language ratio, not the actual fuzzing coverage. The Fuzz
  workflow's run history is the authoritative signal.

Things outside this library's control, which the consuming application
owns: what it puts into MDC and log messages, its TLS configuration, and
where the fallback appender writes.
