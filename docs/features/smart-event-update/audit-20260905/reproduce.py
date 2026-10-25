#!/usr/bin/env python3
"""Reproduce a prepared patch in temporary files; never change the checkout/DB."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

EXPECTED_BLOB = "af0ebafdd4e7f6a18ade11e083eb9cb031eabfb9"


def git_blob(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def run_tests(work: Path, out: Path, name: str) -> dict[str, int]:
    junit = out / f"{name}.xml"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--noconftest", "-q",
         "tests/test_linked_occurrence_integrity.py", f"--junitxml={junit}"],
        cwd=work, text=True, capture_output=True, timeout=120,
    )
    (out / f"{name}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode not in (0, 1) or not junit.exists():
        raise RuntimeError(f"{name}: pytest did not complete normally; inspect {out}")
    root = ET.parse(junit).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    counts = {key: sum(int(s.attrib.get(key, "0")) for s in suites)
              for key in ("tests", "failures", "errors", "skipped")}
    return counts | {"exit_code": result.returncode}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    repo, out = args.repo.resolve(), args.out.resolve()
    baseline = (repo / "linked_events.py").read_bytes()
    actual = git_blob(baseline)
    if actual != EXPECTED_BLOB:
        raise ValueError(f"Baseline differs: {actual}; expected {EXPECTED_BLOB}. No checkout reset was performed.")
    patch = Path(__file__).with_name("occurrence-integrity.patch")
    if not patch.is_file() or not shutil.which("git"):
        raise RuntimeError("The adjacent patch and git executable are required.")
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="smart-update-regression-") as directory:
        work = Path(directory)
        (work / "linked_events.py").write_bytes(baseline)
        shutil.copyfile(repo / "CHANGELOG.md", work / "CHANGELOG.md")
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=work, check=True, timeout=15)
        subprocess.run(["git", "apply", str(patch)], cwd=work, check=True, timeout=15)
        patched = (work / "linked_events.py").read_bytes()
        (work / "linked_events.py").write_bytes(baseline)
        before = run_tests(work, out, "baseline")
        (work / "linked_events.py").write_bytes(patched)
        after = run_tests(work, out, "patched")
    expected_before = {"tests": 39, "failures": 31, "errors": 0, "skipped": 0, "exit_code": 1}
    expected_after = {"tests": 39, "failures": 0, "errors": 0, "skipped": 0, "exit_code": 0}
    passed = before == expected_before and after == expected_after
    report = {"status": "RED_GREEN_REPRODUCED" if passed else "UNEXPECTED_TEST_RESULT",
              "baseline_blob": actual, "patched_blob": git_blob(patched),
              "patch_sha256": hashlib.sha256(patch.read_bytes()).hexdigest(),
              "baseline": before, "patched": after,
              "boundary": "temporary SQLite; injected ORM/async facade; not production integration"}
    (out / "receipt.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, ET.ParseError) as exc:
        print(f"Reproduction failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
