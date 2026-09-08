# Architecture Decision Records

This directory holds the Architecture Decision Records for
**tabellarium**. Every record follows the shape in
[ADR-FORMAT.md](ADR-FORMAT.md). ADRs are numbered
`ADR-NNNN-short-kebab-slug.md`; the number is never reused or changed,
because it is cited from code comments, error messages, the CHANGELOG,
and review reports. The **code is authoritative**: where an ADR and the
code disagree, the code wins and the ADR is corrected.

New to the project? Start with
[ADR-0002](ADR-0002-public-api-is-the-operator-surface.md), which
draws the line between the supported operator surface and the
`internal` implementation, then read the cluster you are touching.

## Index by topic

### API boundary & lifecycle

| ADR | Title | Status |
|-----|-------|--------|
| [0002](ADR-0002-public-api-is-the-operator-surface.md) | The public API is the operator surface | Accepted |
| [0004](ADR-0004-appender-instances-are-not-restartable.md) | Appender instances are not restartable | Accepted |

### Conventions & project process

| ADR | Title | Status |
|-----|-------|--------|
| [0001](ADR-0001-comment-prefix-vocabulary.md) | Comment prefix vocabulary | Accepted; also establishes the ADR series as reference target |
| [0003](ADR-0003-fuzz-workflow-is-the-fuzzing-signal.md) | The Fuzz workflow, not the Scorecard score, is the fuzzing signal | Accepted |

## Superseded

None yet. A fully superseded record keeps its number, gets the status
`Superseded by ADR-NNNN (YYYY-MM-DD)`, and moves to this table; it
must not be treated as current guidance.

## Notes

- **Format & conventions:** [ADR-FORMAT.md](ADR-FORMAT.md).
- **Evidence:** the review reports that ADRs cite live in
  [`../assessment/`](../assessment/); they are dated snapshots and are
  not updated when an ADR changes.
