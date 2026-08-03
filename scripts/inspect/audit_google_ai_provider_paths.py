#!/usr/bin/env python3
"""Audit production-capable source paths for direct Google AI provider access.

The audit is deliberately static and offline: it reads repository source files,
matches provider SDK/REST boundary patterns, and never imports provider packages,
reads environment variables, or performs network calls.  Approved central
gateway implementations are inventoried separately from narrowly allowlisted
migration debt.  Any other finding makes the command exit non-zero.

Allowlist entries are intentionally path + detector + source-line-pattern +
maximum-count contracts.  Moving a call, changing its shape, or adding another
copy therefore requires an explicit review rather than silently widening debt.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence


@dataclass(frozen=True)
class Detector:
    detector_id: str
    regex: re.Pattern[str]
    description: str
    requires_sdk_context: bool = False


DETECTORS: tuple[Detector, ...] = (
    Detector(
        "legacy_sdk_import",
        re.compile(
            r"\b(?:import\s+google\.generativeai(?:\s+as\s+\w+)?|"
            r"from\s+google\s+import\s+generativeai(?:\s+as\s+\w+)?)"
        ),
        "legacy google.generativeai SDK import",
    ),
    Detector(
        "current_sdk_import",
        re.compile(
            r"\b(?:import\s+google\.genai(?:\s+as\s+\w+)?|"
            r"from\s+google\s+import\s+genai(?:\s+as\s+\w+)?)"
        ),
        "current google.genai SDK import",
    ),
    Detector(
        "vertex_sdk_import",
        re.compile(
            r"\b(?:import\s+(?:vertexai|google\.cloud\.aiplatform)|"
            r"from\s+(?:vertexai(?:\.generative_models)?|google\.cloud)\s+import\s+"
            r"(?:aiplatform|generative_models|GenerativeModel))"
        ),
        "Vertex AI SDK import",
    ),
    Detector(
        "legacy_sdk_configure",
        re.compile(r"\b(?:genai|google\.generativeai)\.configure\s*\("),
        "legacy SDK credential configuration call",
        requires_sdk_context=True,
    ),
    Detector(
        "legacy_sdk_model",
        re.compile(r"\b(?:genai|google\.generativeai)\.GenerativeModel\s*\("),
        "legacy SDK model constructor",
        requires_sdk_context=True,
    ),
    Detector(
        "current_sdk_client",
        re.compile(r"\b(?:genai|google\.genai)\.Client\s*\("),
        "current SDK client constructor",
        requires_sdk_context=True,
    ),
    Detector(
        "current_sdk_generate",
        re.compile(r"\.models\.generate_content(?:_stream)?\s*\("),
        "current SDK generate-content call",
        requires_sdk_context=True,
    ),
    Detector(
        "current_sdk_embed",
        re.compile(r"\.models\.embed_content\s*\("),
        "current SDK embedding call",
        requires_sdk_context=True,
    ),
    Detector(
        "vertex_sdk_init",
        re.compile(r"\b(?:vertexai|aiplatform)\.init\s*\("),
        "Vertex AI SDK initialization call",
        requires_sdk_context=True,
    ),
    Detector(
        "vertex_model_constructor",
        re.compile(r"(?<![.A-Za-z0-9_])GenerativeModel\s*\("),
        "Vertex AI generative model constructor",
        requires_sdk_context=True,
    ),
    Detector(
        "google_provider_endpoint",
        re.compile(
            r"\b(?:generativelanguage|[A-Za-z0-9.-]*aiplatform)\.googleapis\.com\b",
            re.IGNORECASE,
        ),
        "Google AI REST endpoint host",
    ),
)

DETECTOR_BY_ID = {detector.detector_id: detector for detector in DETECTORS}
SDK_CONTEXT_DETECTORS = frozenset(
    {"legacy_sdk_import", "current_sdk_import", "vertex_sdk_import"}
)

# Only these files implement the central provider boundary.  A new file under
# google_ai/ is not implicitly approved: it must be reviewed and added here.
APPROVED_GATEWAY_PATHS = frozenset(
    {
        "google_ai/client.py",
        "google_ai/interactions.py",
        "supabase/functions/event-search/google-quota.ts",
    }
)

# Kaggle notebooks carry serialized snapshots of the central gateway package in
# one assignment line.  Matches elsewhere in the same notebooks are not exempt.
APPROVED_EMBEDDED_GATEWAYS = {
    "kaggle/GuideExcursionsMonitor/guide_excursions_monitor.ipynb":
        "_GUIDE_EMBEDDED_GOOGLE_AI =",
    "kaggle/TelegramMonitor/telegram_monitor.ipynb":
        "_TG_EMBEDDED_GOOGLE_AI =",
}


@dataclass(frozen=True)
class AllowRule:
    path: str
    detector_id: str
    line_pattern: str
    max_occurrences: int
    rationale: str

    def accepts_line(self, line: str) -> bool:
        return re.fullmatch(self.line_pattern, line.strip()) is not None


def _exact_line(text: str) -> str:
    return re.escape(text)


# Dependency probes confirm that the SDK needed by GoogleAIClient is installed;
# they do not construct a provider client or issue a provider request.
APPROVED_DEPENDENCY_PROBES: tuple[AllowRule, ...] = (
    AllowRule(
        "contour_svg/llm_gateway.py",
        "current_sdk_import",
        _exact_line("from google import genai as _genai  # noqa: F401"),
        1,
        "SDK availability probe before calls through GoogleAIClient",
    ),
    AllowRule(
        "kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py",
        "current_sdk_import",
        _exact_line("from google import genai as _genai  # noqa: F401"),
        1,
        "SDK availability probe before calls through the Region Talk gateway",
    ),
    AllowRule(
        "kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py",
        "current_sdk_import",
        _exact_line("from google import genai as _genai  # noqa: F401"),
        1,
        "SDK availability probe before calls through the Region Talk gateway",
    ),
)


# No direct provider bypass remains intentionally callable. Keeping this tuple
# empty is deliberate: reintroducing any former legacy line must fail the audit
# instead of silently matching a stale migration allowlist.
KNOWN_BYPASS_DEBT: tuple[AllowRule, ...] = ()


SOURCE_SUFFIXES = frozenset(
    {
        ".py",
        ".ts",
        ".tsx",
        ".js",
        ".jsx",
        ".mjs",
        ".cjs",
        ".sh",
        ".ipynb",
        ".yaml",
        ".yml",
        ".toml",
    }
)
SOURCE_FILENAMES = frozenset({"Dockerfile", "Makefile"})
EXCLUDED_DIRS = frozenset(
    {
        ".git",
        ".codex",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "artifacts",
        "docs",
        "node_modules",
        "site/public",
        "site/src/data",
        "test-results",
        "tests",
        "venv",
    }
)
SELF_PATH = "scripts/inspect/audit_google_ai_provider_paths.py"


@dataclass(frozen=True)
class SourceUnit:
    cell_index: int | None
    text: str


@dataclass(frozen=True)
class RawFinding:
    path: str
    detector_id: str
    line: int
    column: int
    cell_index: int | None
    matched_token: str
    source_line: str


@dataclass(frozen=True)
class Finding:
    path: str
    detector_id: str
    line: int
    column: int
    cell_index: int | None
    matched_token: str
    disposition: str
    rationale: str

    @property
    def location(self) -> str:
        if self.cell_index is None:
            return f"{self.path}:{self.line}:{self.column}"
        return f"{self.path}:cell[{self.cell_index}]:{self.line}:{self.column}"

    def public_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["location"] = self.location
        return data


@dataclass(frozen=True)
class AuditReport:
    root: str
    findings: tuple[Finding, ...]
    scanned_files: int
    unreadable_files: tuple[str, ...]

    @property
    def summary(self) -> dict[str, int]:
        counts = Counter(finding.disposition for finding in self.findings)
        return {
            "approved_gateway": counts["approved_gateway"],
            "approved_embedded_gateway": counts["approved_embedded_gateway"],
            "approved_dependency_probe": counts["approved_dependency_probe"],
            "allowlisted_debt": counts["allowlisted_debt"],
            "unapproved": counts["unapproved"],
            "unreadable_files": len(self.unreadable_files),
        }

    @property
    def passed(self) -> bool:
        return self.summary["unapproved"] == 0 and not self.unreadable_files

    def public_dict(self) -> dict[str, object]:
        return {
            "status": "pass" if self.passed else "fail",
            "root": self.root,
            "scanned_files": self.scanned_files,
            "summary": self.summary,
            "unreadable_files": list(self.unreadable_files),
            "findings": [finding.public_dict() for finding in self.findings],
        }


def _is_excluded_dir(relative: str) -> bool:
    return any(relative == item or relative.startswith(f"{item}/") for item in EXCLUDED_DIRS)


def _iter_candidate_files(root: Path) -> Iterator[tuple[str, Path]]:
    for current, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        current_rel = current_path.relative_to(root).as_posix()
        dirnames[:] = sorted(
            dirname
            for dirname in dirnames
            if not _is_excluded_dir(
                dirname if current_rel == "." else f"{current_rel}/{dirname}"
            )
        )
        for filename in sorted(filenames):
            path = current_path / filename
            relative = path.relative_to(root).as_posix()
            if relative == SELF_PATH:
                continue
            if path.suffix.lower() not in SOURCE_SUFFIXES and filename not in SOURCE_FILENAMES:
                continue
            yield relative, path


def _source_units(path: Path, text: str) -> Iterable[SourceUnit]:
    if path.suffix.lower() != ".ipynb":
        yield SourceUnit(cell_index=None, text=text)
        return
    payload = json.loads(text)
    cells = payload.get("cells")
    if not isinstance(cells, list):
        raise ValueError("notebook has no cells list")
    for cell_index, cell in enumerate(cells):
        if not isinstance(cell, dict):
            continue
        source = cell.get("source", "")
        if isinstance(source, list):
            source_text = "".join(str(part) for part in source)
        else:
            source_text = str(source)
        yield SourceUnit(cell_index=cell_index, text=source_text)


def _line_details(text: str, offset: int) -> tuple[int, int, str]:
    line = text.count("\n", 0, offset) + 1
    line_start = text.rfind("\n", 0, offset) + 1
    line_end = text.find("\n", offset)
    if line_end < 0:
        line_end = len(text)
    return line, offset - line_start + 1, text[line_start:line_end]


def _scan_unit(path: str, unit: SourceUnit) -> list[RawFinding]:
    raw_matches: list[tuple[Detector, re.Match[str]]] = []
    context_ids: set[str] = set()
    for detector in DETECTORS:
        matches = list(detector.regex.finditer(unit.text))
        if matches and detector.detector_id in SDK_CONTEXT_DETECTORS:
            context_ids.add(detector.detector_id)
        raw_matches.extend((detector, match) for match in matches)

    findings: list[RawFinding] = []
    for detector, match in raw_matches:
        if detector.requires_sdk_context and not context_ids:
            continue
        line, column, source_line = _line_details(unit.text, match.start())
        findings.append(
            RawFinding(
                path=path,
                detector_id=detector.detector_id,
                line=line,
                column=column,
                cell_index=unit.cell_index,
                matched_token=match.group(0),
                source_line=source_line,
            )
        )
    return findings


def _raw_sort_key(finding: RawFinding) -> tuple[object, ...]:
    return (
        finding.path,
        -1 if finding.cell_index is None else finding.cell_index,
        finding.line,
        finding.column,
        finding.detector_id,
    )


def _is_embedded_gateway(finding: RawFinding, root: Path) -> bool:
    prefix = APPROVED_EMBEDDED_GATEWAYS.get(finding.path)
    if not prefix or not finding.source_line.lstrip().startswith(prefix):
        return False
    # A notebook assignment is approved only while its serialized gateway is
    # byte-for-byte current.  Merely using the expected variable name used to
    # let old fail-open gateway snapshots pass this audit indefinitely.
    try:
        payload = json.loads(finding.source_line.lstrip()[len(prefix):].strip())
        if not isinstance(payload, dict):
            return False
        for relative in ("client.py", "limiter_supabase.py"):
            canonical = (root / "google_ai" / relative).read_text(encoding="utf-8")
            if payload.get(relative) != canonical:
                return False
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return True


def _apply_rules(
    raw_findings: Sequence[RawFinding],
    rules: Sequence[AllowRule],
) -> dict[int, AllowRule]:
    accepted: dict[int, AllowRule] = {}
    for rule in rules:
        candidates = [
            (index, finding)
            for index, finding in enumerate(raw_findings)
            if finding.path == rule.path
            and finding.detector_id == rule.detector_id
            and rule.accepts_line(finding.source_line)
        ]
        for index, _finding in candidates[: rule.max_occurrences]:
            accepted[index] = rule
    return accepted


def audit_repository(root: str | Path) -> AuditReport:
    root_path = Path(root).resolve()
    raw_findings: list[RawFinding] = []
    unreadable: list[str] = []
    scanned_files = 0
    for relative, path in _iter_candidate_files(root_path):
        scanned_files += 1
        try:
            text = path.read_text(encoding="utf-8")
            units = tuple(_source_units(path, text))
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            unreadable.append(relative)
            continue
        for unit in units:
            raw_findings.extend(_scan_unit(relative, unit))

    raw_findings.sort(key=_raw_sort_key)
    probe_matches = _apply_rules(raw_findings, APPROVED_DEPENDENCY_PROBES)
    debt_matches = _apply_rules(raw_findings, KNOWN_BYPASS_DEBT)

    findings: list[Finding] = []
    for index, raw in enumerate(raw_findings):
        if raw.path in APPROVED_GATEWAY_PATHS:
            disposition = "approved_gateway"
            rationale = "central Google AI provider gateway implementation"
        elif _is_embedded_gateway(raw, root_path):
            disposition = "approved_embedded_gateway"
            rationale = "serialized central gateway snapshot in a Kaggle notebook"
        elif index in probe_matches:
            disposition = "approved_dependency_probe"
            rationale = probe_matches[index].rationale
        elif index in debt_matches:
            disposition = "allowlisted_debt"
            rationale = debt_matches[index].rationale
        else:
            disposition = "unapproved"
            rationale = "direct provider boundary outside an approved gateway or narrow debt rule"
        findings.append(
            Finding(
                path=raw.path,
                detector_id=raw.detector_id,
                line=raw.line,
                column=raw.column,
                cell_index=raw.cell_index,
                matched_token=raw.matched_token,
                disposition=disposition,
                rationale=rationale,
            )
        )

    return AuditReport(
        root=str(root_path),
        findings=tuple(findings),
        scanned_files=scanned_files,
        unreadable_files=tuple(sorted(unreadable)),
    )


def render_text(report: AuditReport) -> str:
    summary = report.summary
    lines = [
        f"Google AI provider path audit: {'PASS' if report.passed else 'FAIL'}",
        f"root={report.root}",
        f"scanned_files={report.scanned_files}",
        "summary="
        + " ".join(f"{key}={value}" for key, value in summary.items()),
    ]
    grouped: dict[str, list[Finding]] = defaultdict(list)
    for finding in report.findings:
        grouped[finding.disposition].append(finding)
    for disposition in (
        "unapproved",
        "allowlisted_debt",
        "approved_dependency_probe",
        "approved_gateway",
        "approved_embedded_gateway",
    ):
        if not grouped[disposition]:
            continue
        lines.append(f"[{disposition}]")
        for finding in grouped[disposition]:
            lines.append(
                f"- {finding.location} detector={finding.detector_id} "
                f"token={finding.matched_token!r} reason={finding.rationale}"
            )
    if report.unreadable_files:
        lines.append("[unreadable_files]")
        lines.extend(f"- {path}" for path in report.unreadable_files)
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="repository root to scan (default: inferred from this script)",
    )
    parser.add_argument("--json", action="store_true", help="emit deterministic JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = audit_repository(args.root)
    if args.json:
        print(json.dumps(report.public_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(report))
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
