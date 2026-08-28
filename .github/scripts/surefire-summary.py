#!/usr/bin/env python3
"""Write a test-run summary as GitHub-flavored Markdown.

Parses target/surefire-reports/TEST-*.xml (and, when present, JaCoCo's
CSV) into a per-class table for $GITHUB_STEP_SUMMARY, so every CI run
shows what the suite did without opening logs. Prints to stdout when
GITHUB_STEP_SUMMARY is unset (local use). Deliberately a repo-local
script instead of a third-party action: this repo pins its actions to
SHAs and keeps the supply-chain surface small.

Usage: python3 .github/scripts/surefire-summary.py
"""

import csv
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

REPORTS_DIR = Path("target/surefire-reports")
JACOCO_CSV = Path("target/site/jacoco/jacoco.csv")


def main() -> None:
    rows = []
    totals = [0, 0, 0, 0, 0.0]
    for report in sorted(REPORTS_DIR.glob("TEST-*.xml")):
        root = ET.parse(report).getroot()
        name = root.get("name", "?").rsplit(".", 1)[-1]
        vals = [
            int(root.get("tests", 0)),
            int(root.get("failures", 0)),
            int(root.get("errors", 0)),
            int(root.get("skipped", 0)),
            float(root.get("time", 0.0)),
        ]
        rows.append((name, vals))
        totals = [a + b for a, b in zip(totals, vals)]

    out = ["## Test results", ""]
    if not rows:
        out.append("No Surefire reports found (build failed before the test phase?).")
    else:
        ok = totals[1] == 0 and totals[2] == 0
        out.append(
            f"{'✅' if ok else '❌'} **{totals[0]} tests** — "
            f"{totals[1]} failures, {totals[2]} errors, {totals[3]} skipped, "
            f"{totals[4]:.1f}s"
        )
        out.append("")
        out.append("| Test class | Tests | Failures | Errors | Skipped | Time |")
        out.append("|---|---:|---:|---:|---:|---:|")
        for name, vals in sorted(rows, key=lambda r: -r[1][0]):
            marker = "" if vals[1] + vals[2] == 0 else " ❌"
            out.append(
                f"| `{name}`{marker} | {vals[0]} | {vals[1]} | {vals[2]} | {vals[3]} | {vals[4]:.1f}s |"
            )
    if JACOCO_CSV.is_file():
        missed = covered = 0
        with JACOCO_CSV.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                missed += int(row["INSTRUCTION_MISSED"])
                covered += int(row["INSTRUCTION_COVERED"])
        if missed + covered:
            pct = 100.0 * covered / (missed + covered)
            out.append("")
            out.append(f"**Instruction coverage:** {pct:.1f}% ({covered}/{missed + covered})")
    out.append("")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    text = "\n".join(out)
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    else:
        print(text)
    # Reporting must not mask the build result: always exit 0.
    sys.exit(0)


if __name__ == "__main__":
    main()
