# ADR-0001: Comment prefix vocabulary

**Status:** Accepted  
**Date:** 2026-08-29  
**Deciders:** Dirk Haase (maintainer)

## Context

The 2026-08-29 comment audit
(`docs/assessment/COMMENT_AUDIT-2026-08-29T01-25-22.md`, finding 15)
found that the codebase carries substantial rationale, invariant, and
compatibility knowledge in comments, but with no shared vocabulary: the
emergent stock was a handful of `Note:`/`CAUTION:` markers.

Without a fixed prefix set this knowledge is not greppable. There is no
way to list every workaround before a dependency upgrade, or every
invariant of a class, and a new comment has no form to follow. The
audit also found that design decisions had no stable place to live:
they were spread across commit messages, review reports, and README
passages, none of which is a citable, immutable target.

## Decision

Comments that state one of the following carry the matching prefix.
The vocabulary is deliberately small; **do not invent new prefixes
without a follow-up ADR.**

### Prefixes

| Prefix | Use for | Example shape |
|---|---|---|
| `Rationale:` | The reason for a decision that the code cannot show | `// Rationale: wall-clock time, because values are persisted across restarts.` |
| `Invariant:` | A condition the surrounding code maintains and relies on | `// Invariant: writeOffset <= readOffset <= capacity.` |
| `Workaround:` | Code shaped around a defect or limitation elsewhere; names the external cause | `// Workaround: Kafka 4 removed the default partitioner (KAFKA-xxxxx).` |
| `Safety:` | A guard against a concrete failure mode (concurrency, security, resource) | `// Safety: bounded, because the value can be attacker-influenced.` |
| `Compatibility:` | A constraint imposed by an external contract or version | `// Compatibility: Joran needs a public no-arg constructor here.` |
| `CAUTION:` | A non-obvious trap for the person editing the next line | `// CAUTION: Thread.join(0) means "wait forever".` |

### Rules

- The prefix replaces the introductory sentence (`// Rationale: X`
  instead of `// This is done because X`); the target form stays
  *decision + reason* in one precise sentence.
- Prefixes are for statement-shaped comments. Narrative KDoc sections,
  test rationale blocks (three-question pattern), and `Given/When/Then`
  markers keep their existing forms.
- Existing comments are converted opportunistically, when the line
  below them is touched anyway. There is no big-bang rewrite, so
  `git blame` stays meaningful.
- A `TODO` needs a phase tag or a ticket reference. This rule is
  unchanged and restated here because it belongs to the same
  greppable-inventory idea; the codebase currently has zero TODOs and
  should stay near that.

### The ADR series as reference target

This ADR also establishes the ADR series itself. ADR numbers
(`ADR-nnnn`) are the stable, immutable reference targets for design
decisions. In-code comments may cite them (`see ADR-0001`) but must
still carry the local one-line rule, so the reference is never bare.

## Consequences

**Positive:**

- `rg "Workaround:" src/` is the checklist for every dependency
  upgrade; `rg "Invariant:" src/` yields the invariant catalog of the
  module.
- New comments have a form to follow, and reviews have a fixed
  question to ask: *can this comment become wrong without something
  turning red? If yes, can a name, a type, a `check`, a test, or a
  checked reference carry it? If prose remains, does it carry the right
  prefix?*
- Design decisions gain a citable home; later ADRs, review reports,
  and error messages can point at a number that does not move.

**Negative:**

- Opportunistic conversion means the comment stock is mixed for a
  while: greps over a prefix are complete only for comments written or
  touched after this date.
- A deliberately small vocabulary leaves some comments without a
  fitting prefix; they must be reworded into one of the six shapes or
  stay unprefixed and invisible to the inventories.

**Neutral:**

- The prefix set is closed by convention, not by tooling; nothing in
  the build rejects an unknown prefix.
