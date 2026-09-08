# ADR-0003: The Fuzz workflow, not the Scorecard score, is the fuzzing signal

**Status:** Accepted  
**Date:** 2026-08-30  
**Deciders:** Dirk Haase (maintainer)

## Context

The OpenSSF Scorecard **Fuzzing** check dropped to 0 ("project is not
fuzzed") although the nightly Fuzz workflow runs the Jazzer `@FuzzTest`
targets green.

The cause was verified against the Scorecard v5.5.0 source
(`checks/raw/fuzzing.go`, commit `c395761d`). Scorecard does support
Jazzer: it greps `*.java` files for
`com.code_intelligence.jazzer.api.FuzzedDataProvider;`, which this
repository's fuzz tests import verbatim. But the language-specific scan
only runs for "prominent" languages, defined as a byte share of at
least (total ÷ languages) ÷ 4 per GitHub's linguist statistics. With
exactly two detected languages (Kotlin + Java) that means Java needs
≥ 12.5 % of the repository's bytes; it sits at ~5.4 % (34,073 of
631,805 on 2026-08-30). The Java fuzz tests are therefore never
scanned.

The score was 10 in the ClusterFuzzLite era because that integration is
detected by file presence (`.clusterfuzzlite/Dockerfile`). The
migration to Jazzer `@FuzzTest` (commit `aab88fa`) removed that file
and tied the score to the Kotlin:Java byte ratio instead: it flips
10↔0 whenever the ratio crosses 7:1, with ordinary commits.

## Decision

**The Fuzz workflow's run history is the authoritative fuzzing signal;
the Scorecard Fuzzing score is accepted as 0 (or flapping) and is not
acted on.**

The reader-facing consequence lives in `SECURITY.md` (Scorecard scope
note, PR #8): the badge's Fuzzing line tracks the language ratio, not
the fuzzing coverage.

**Revisit when:** Scorecard drops the prominent-language gate for fuzz
detection, adds jazzer-junit `@FuzzTest` or Kotlin detection, or the
project joins OSS-Fuzz (detected independently of language).

## Consequences

**Positive:**

- No engineering effort is spent on satisfying a detector; the fuzzing
  setup stays the one that actually runs on the project's JDK.
- "Fuzzing is 0 again" has a standing answer: this ADR. Neither a flip
  to 10 nor a flip back to 0 warrants action.

**Negative:**

- The overall Scorecard score carries a standing deduction of medium
  weight, alongside the other single-maintainer deductions already
  documented in `SECURITY.md`.
- The public badge misrepresents the project's fuzzing coverage to
  anyone who does not read the `SECURITY.md` note.

**Neutral:**

- The Fuzzing score may flip in either direction without any change in
  fuzzing coverage; the workflow's run history is the only signal that
  is looked at.

## Considered and rejected

- **Reintroducing ClusterFuzzLite** just to satisfy the detector. It
  was removed deliberately: its OSS-Fuzz base images are pinned to
  JDK 17, while this project builds on a newer JDK.
- **Gaming the linguist statistics** (`.gitattributes` overrides, or
  inflating the Java share) so that Java crosses the 12.5 % line. The
  language statistics would then misrepresent the codebase to fix a
  number that misrepresents the fuzzing.
- **Converting the fuzz tests to Kotlin** would not help either way:
  Scorecard has no Kotlin fuzzer spec at all.
