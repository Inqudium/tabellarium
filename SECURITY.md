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
