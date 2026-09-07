# ADR-0004: Appender instances are not restartable

- **Status:** accepted
- **Date:** 2026-09-07
- **Context:** The 2026-09-07 defect analysis
  (`docs/assessment/CODE_ANALYSIS-2026-09-07T19-09-00.md`, finding 4)
  found that `start()` after `stop()` silently lost the fallback
  appender. Its remediation chose the symmetric direction - restart the
  fallback, reset the breakers, re-arm the error guard, rebind the
  metrics - and the same day's follow-up pass (`.R2.md`, findings R2-4
  and R2-5) and architecture review
  (`docs/assessment/ARCHITECTURE_REVIEW-2026-09-07T20-23-00.md`,
  finding 3) showed what that buys: every per-appender resource now
  needs a "what happens on restart?" answer, and no consumer, issue or
  README passage asks for a programmatic restart.

## Decision

**A `KafkaAppender` instance is started once.** `start()` after
`stop()` is refused with an `addError` naming this ADR; the operator
or program creates a new instance instead.

Rationale: this is Logback's own lifecycle. A reconfiguration
(`<configuration scan="true">`, `LoggerContext.reset()` plus Joran,
Spring Boot's logging-system re-initialization) stops and detaches
every appender and builds **new instances**; `LoggerContext.stop()` is
terminal; Logback's `AsyncAppender` detaches its appenders on `stop()`
and is not restartable in practice. A same-instance restart therefore
only ever originates in application code that holds a reference and
toggles it - a path for which there is no known user. Supporting it
means keeping fallback, circuit-breaker state, metrics binding,
one-shot error report and every future stateful component symmetric
across a second life; refusing it removes those branches and the
interactions between them.

## Consequences

- `KafkaAppender.start()` checks the stop guard first and refuses with
  a message that names the alternative (a new instance); the guard is
  never reset. The Joran round trip and Logback reconfiguration are
  unaffected - they never restart an instance.
- The restart-symmetry code introduced on 2026-09-07 (fallback
  restart in `start()`, breaker reset in `buildPipeline`, error-guard
  re-arm) is removed; the `KafkaAppenderMetricsBinding` keeps deciding
  on the appender's bound state, which is simpler than its former
  identity set regardless of restart.
- Reversal is a conscious API addition: if a consumer documents a need
  for stop/start toggling, a follow-up ADR reinstates the symmetric
  lifecycle as a supported contract - with the per-resource answers
  written down, not rediscovered.
