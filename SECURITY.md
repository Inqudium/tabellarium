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

Things outside this library's control, which the consuming application
owns: what it puts into MDC and log messages, its TLS configuration, and
where the fallback appender writes.
