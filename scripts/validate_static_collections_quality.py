#!/usr/bin/env python3
"""Fail-closed static-collection policy/review validator.

Baseline mode supports the current provisional legacy fixture and reports the
missing target provenance/family/score fields as warnings. Strict mode is the
post-migration contract and requires a separate owner gold.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "site" / "scripts" / "static_collection_policy.v1.json"
DEFAULT_SEED = ROOT / "tests" / "fixtures" / "static_collections_gold_v1.json"
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
TARGET_FIELDS = (
    "family_id",
    "source_refs",
    "source_quote",
    "model_document_hash",
    "positive_score",
    "negative_score",
    "margin",
    "winning_positive_prototype_id",
    "winning_negative_prototype_id",
)
HASH_FIELDS = (
    "catalog_hash",
    "snapshot_sha256",
    "policy_sha256",
    "prototype_bank_sha256",
    "vector_artifact_sha256",
    "score_artifact_sha256",
    "generator_repo_sha",
)


@dataclass(frozen=True)
class Issue:
    severity: str
    code: str
    message: str
    label: str | None = None
    event_id: int | None = None


class Collector:
    def __init__(self) -> None:
        self.issues: list[Issue] = []

    def add(
        self,
        severity: str,
        code: str,
        message: str,
        *,
        label: str | None = None,
        event_id: int | None = None,
    ) -> None:
        self.issues.append(Issue(severity, code, message, label, event_id))

    def error(self, code: str, message: str, **location: Any) -> None:
        self.add("error", code, message, **location)

    def warning(self, code: str, message: str, **location: Any) -> None:
        self.add("warning", code, message, **location)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("baseline", "strict"), default="baseline")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--owner-gold", type=Path)
    parser.add_argument("--json-report", type=Path)
    parser.add_argument("--markdown-report", type=Path)
    return parser.parse_args(argv)


def load_json(path: Path, role: str, issues: Collector) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        issues.error(f"{role}_missing", f"{role} file is missing: {path}")
        return {}
    except json.JSONDecodeError as exc:
        issues.error(
            f"{role}_invalid_json",
            f"{role} JSON line {exc.lineno}: {exc.msg}",
        )
        return {}
    if not isinstance(value, dict):
        issues.error(f"{role}_not_object", f"{role} must be a JSON object")
        return {}
    return value


def text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(HEX_64.fullmatch(value))


def validate_policy(policy: Mapping[str, Any], issues: Collector) -> set[str]:
    labels = policy.get("labels")
    if not isinstance(labels, Mapping) or not labels:
        issues.error("policy_labels_missing", "policy.labels must be non-empty")
        return set()
    gate = policy.get("semantic_quality_gate")
    if not isinstance(gate, Mapping):
        issues.error("semantic_gate_missing", "semantic_quality_gate is missing")
    else:
        for field in (
            "minimum_positives",
            "minimum_hard_negatives",
            "minimum_recall",
            "maximum_hard_negative_fpr",
        ):
            if not number(gate.get(field)):
                issues.error("semantic_gate_invalid", f"{field} must be numeric")

    semantic: set[str] = set()
    for raw_label, raw_config in labels.items():
        label = str(raw_label)
        if not isinstance(raw_config, Mapping):
            issues.error("policy_label_invalid", "label policy must be object", label=label)
            continue
        if not str(raw_config.get("strategy") or "").startswith("semantic_bge"):
            continue
        semantic.add(label)
        if raw_config.get("publication") != "blocked":
            issues.error(
                "policy_publication_leak",
                "semantic/BGE label must remain publication=blocked",
                label=label,
            )
        for field in ("positive_prefix", "negative_prefix"):
            if not text(raw_config.get(field)):
                issues.error(
                    "policy_prototype_prefix_missing",
                    f"{field} is required",
                    label=label,
                )
        for field in ("minimum_positive_similarity", "minimum_margin"):
            if not number(raw_config.get(field)):
                issues.error(
                    "policy_threshold_invalid",
                    f"{field} must be numeric",
                    label=label,
                )
    return semantic


def rows(value: Any, side: str, label: str, issues: Collector) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        issues.error(f"{side}_invalid", f"{side} must be list", label=label)
        return []
    result = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            issues.error(
                f"{side}_invalid",
                f"{side}[{index}] must be object",
                label=label,
            )
        else:
            result.append(item)
    return result


def validate_source_refs(
    value: Any,
    label: str,
    event_id: int | None,
    issues: Collector,
) -> None:
    if not isinstance(value, list) or not value:
        issues.error(
            "source_refs_missing",
            "strict row requires source_refs",
            label=label,
            event_id=event_id,
        )
        return
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            issues.error(
                "source_ref_invalid",
                f"source_refs[{index}] must be object",
                label=label,
                event_id=event_id,
            )
            continue
        if not text(item.get("source_type")):
            issues.error(
                "source_ref_type_missing",
                f"source_refs[{index}] requires source_type",
                label=label,
                event_id=event_id,
            )
        if not (text(item.get("source_url")) or text(item.get("source_id"))):
            issues.error(
                "source_ref_identity_missing",
                f"source_refs[{index}] requires source_url or source_id",
                label=label,
                event_id=event_id,
            )


def validate_row(
    row: Mapping[str, Any],
    *,
    label: str,
    expected: str,
    strict: bool,
    issues: Collector,
) -> tuple[int | None, str | None]:
    raw_id = row.get("event_id")
    event_id = (
        raw_id
        if isinstance(raw_id, int) and not isinstance(raw_id, bool) and raw_id > 0
        else None
    )
    if event_id is None:
        issues.error("event_id_invalid", "event_id must be positive integer", label=label)

    for field in ("title", "reason_code", "evidence_excerpt"):
        if not text(row.get(field)):
            issues.error(
                "row_field_missing",
                f"{field} must be non-empty",
                label=label,
                event_id=event_id,
            )
    if not text(row.get("occurrence_date", row.get("date"))):
        issues.error("row_date_missing", "row requires date", label=label, event_id=event_id)
    if row.get("expected") != expected:
        issues.error(
            "row_expected_mismatch",
            f"expected must equal {expected}",
            label=label,
            event_id=event_id,
        )
    if not isinstance(row.get("bge_selected"), bool):
        issues.error(
            "row_bge_selected_invalid",
            "bge_selected must be boolean",
            label=label,
            event_id=event_id,
        )

    family_id = row.get("family_id")
    family = family_id.strip() if text(family_id) else None
    if not strict:
        return event_id, family

    if not family:
        issues.error("family_id_missing", "strict row requires family_id", label=label, event_id=event_id)
    if not text(row.get("source_quote")):
        issues.error("source_quote_missing", "strict row requires source_quote", label=label, event_id=event_id)
    validate_source_refs(row.get("source_refs"), label, event_id, issues)
    if not sha256(row.get("model_document_hash")):
        issues.error(
            "model_document_hash_invalid",
            "model_document_hash must be SHA-256",
            label=label,
            event_id=event_id,
        )
    for field in ("positive_score", "negative_score", "margin"):
        if not number(row.get(field)):
            issues.error("score_missing", f"strict row requires {field}", label=label, event_id=event_id)
    if all(number(row.get(field)) for field in ("positive_score", "negative_score", "margin")):
        calculated = float(row["positive_score"]) - float(row["negative_score"])
        if not math.isclose(calculated, float(row["margin"]), abs_tol=1e-6):
            issues.error(
                "margin_mismatch",
                f"margin={row['margin']}, calculated={calculated}",
                label=label,
                event_id=event_id,
            )
    for field in (
        "winning_positive_prototype_id",
        "winning_negative_prototype_id",
    ):
        if not text(row.get(field)):
            issues.error(
                "winning_prototype_missing",
                f"strict row requires {field}",
                label=label,
                event_id=event_id,
            )
    return event_id, family


def validate_label(
    label: str,
    payload: Mapping[str, Any],
    *,
    strict: bool,
    minimum_positives: int,
    minimum_negatives: int,
    issues: Collector,
) -> dict[str, int]:
    if not text(payload.get("definition")):
        issues.error("label_definition_missing", "definition is required", label=label)
    positives = rows(payload.get("positives"), "positives", label, issues)
    negatives = rows(payload.get("hard_negatives"), "hard_negatives", label, issues)

    positive_ids: list[int] = []
    negative_ids: list[int] = []
    positive_families: list[str] = []
    negative_families: list[str] = []
    for item in positives:
        event_id, family = validate_row(
            item,
            label=label,
            expected="positive_candidate",
            strict=strict,
            issues=issues,
        )
        if event_id is not None:
            positive_ids.append(event_id)
        if family:
            positive_families.append(family)
    for item in negatives:
        event_id, family = validate_row(
            item,
            label=label,
            expected="hard_negative",
            strict=strict,
            issues=issues,
        )
        if event_id is not None:
            negative_ids.append(event_id)
        if family:
            negative_families.append(family)

    if not strict:
        legacy = sum(
            any(item.get(field) in (None, "", []) for field in TARGET_FIELDS)
            for item in [*positives, *negatives]
        )
        if legacy:
            issues.warning(
                "legacy_rows_not_reproducible",
                f"{legacy}/{len(positives) + len(negatives)} rows lack target provenance/family/score fields",
                label=label,
            )

    for values, code, description in (
        (positive_ids, "positive_event_duplicate", "positive event IDs"),
        (negative_ids, "negative_event_duplicate", "negative event IDs"),
    ):
        if len(values) != len(set(values)):
            issues.error(code, f"{description} must be unique", label=label)
    overlap = sorted(set(positive_ids) & set(negative_ids))
    if overlap:
        issues.error("event_side_overlap", f"event IDs on both sides: {overlap[:10]}", label=label)

    if positive_families or negative_families:
        if len(positive_families) != len(set(positive_families)):
            issues.add(
                "error" if strict else "warning",
                "positive_family_duplicate",
                "positive rows repeat a family",
                label=label,
            )
        if len(negative_families) != len(set(negative_families)):
            issues.add(
                "error" if strict else "warning",
                "negative_family_duplicate",
                "negative rows repeat a family",
                label=label,
            )
        overlap = sorted(set(positive_families) & set(negative_families))
        if overlap:
            issues.error("family_side_overlap", f"families on both sides: {overlap[:10]}", label=label)

    counts = payload.get("counts")
    if isinstance(counts, Mapping):
        expected_counts = {
            "positive_candidates": len(positives),
            "hard_negatives": len(negatives),
            "high_confidence_positives": sum(
                item.get("confidence") == "high" for item in positives
            ),
        }
        for field, actual in expected_counts.items():
            if field in counts and counts.get(field) != actual:
                issues.error(
                    "declared_count_mismatch",
                    f"counts.{field}={counts.get(field)!r}, actual={actual}",
                    label=label,
                )
    else:
        issues.warning("declared_counts_missing", "counts object is missing", label=label)

    positive_units = len(set(positive_families)) if positive_families else len(positive_ids)
    negative_units = len(set(negative_families)) if negative_families else len(negative_ids)
    if negative_units < minimum_negatives:
        issues.error(
            "hard_negative_supply_shortfall",
            f"{negative_units} negatives < required {minimum_negatives}",
            label=label,
        )
    if positive_units < minimum_positives:
        issues.add(
            "error" if strict else "warning",
            "positive_supply_shortfall",
            f"{positive_units} positives < required {minimum_positives}",
            label=label,
        )
    return {
        "positive_rows": len(positives),
        "negative_rows": len(negatives),
        "positive_units": positive_units,
        "negative_units": negative_units,
    }


def validate_hashes(
    value: Mapping[str, Any],
    *,
    role: str,
    strict: bool,
    issues: Collector,
) -> None:
    source = value.get("source") if isinstance(value.get("source"), Mapping) else value
    for field in HASH_FIELDS:
        if sha256(source.get(field)):
            continue
        issues.add(
            "error" if strict else "warning",
            "hash_binding_missing" if strict else "legacy_hash_binding_missing",
            f"{role} lacks SHA-256 field {field}",
        )


def validate_seed(
    seed: Mapping[str, Any],
    policy: Mapping[str, Any],
    semantic: set[str],
    *,
    strict: bool,
    seed_path: Path,
    issues: Collector,
) -> dict[str, dict[str, int]]:
    provisional = seed.get("status") == "provisional_agent_seed_not_owner_approved"
    if provisional and seed.get("publication_eligible") is not False:
        issues.error(
            "provisional_seed_publishable",
            "provisional review seed must set publication_eligible=false",
        )
    if strict:
        if seed.get("schema_version") != "static-collections-review-seed-v1":
            issues.error("review_seed_schema_legacy", "strict mode requires review-seed-v1 schema")
        if "gold" in seed_path.name.casefold():
            issues.error("review_seed_named_gold", "strict review seed filename must not contain gold")
    elif (
        seed.get("schema_version") == "static-collections-gold-v1"
        or "gold" in seed_path.name.casefold()
    ):
        issues.warning(
            "legacy_review_seed_named_gold",
            "provisional review seed is still named/schema'd as gold",
        )

    labels = seed.get("labels")
    if not isinstance(labels, Mapping) or not labels:
        issues.error("seed_labels_missing", "seed.labels must be non-empty")
        return {}
    policy_labels = policy.get("labels") if isinstance(policy.get("labels"), Mapping) else {}
    acceptance = seed.get("acceptance") if isinstance(seed.get("acceptance"), Mapping) else {}
    gate = policy.get("semantic_quality_gate") if isinstance(policy.get("semantic_quality_gate"), Mapping) else {}
    minimum_positives = int(
        acceptance.get("semantic_minimum_positives")
        or gate.get("minimum_positives")
        or 15
    )
    minimum_negatives = int(
        acceptance.get("semantic_minimum_hard_negatives")
        or gate.get("minimum_hard_negatives")
        or 20
    )

    summary = {}
    for raw_label, payload in labels.items():
        label = str(raw_label)
        if label not in policy_labels:
            issues.error("seed_label_missing_policy", "label absent from policy", label=label)
        elif label in semantic and policy_labels[label].get("publication") != "blocked":
            issues.error("policy_publication_leak", "semantic review label is not blocked", label=label)
        if not isinstance(payload, Mapping):
            issues.error("seed_label_invalid", "label payload must be object", label=label)
            continue
        summary[label] = validate_label(
            label,
            payload,
            strict=strict,
            minimum_positives=minimum_positives,
            minimum_negatives=minimum_negatives,
            issues=issues,
        )
    missing = sorted(label for label in semantic if label != "unusual" and label not in labels)
    if missing:
        issues.warning(
            "semantic_labels_without_review_seed",
            "semantic labels absent from review seed: " + ", ".join(missing),
        )
    validate_hashes(seed, role="review seed", strict=strict, issues=issues)
    return summary


def validate_owner_gold(
    gold: Mapping[str, Any],
    *,
    strict: bool,
    issues: Collector,
) -> None:
    if not gold:
        if strict:
            issues.error("owner_gold_missing", "strict mode requires --owner-gold")
        return
    if gold.get("schema_version") != "static-collections-owner-gold-v1":
        issues.error("owner_gold_schema_invalid", "owner gold schema is invalid")
    if gold.get("status") != "owner_reviewed":
        issues.error("owner_gold_status_invalid", "owner gold status must be owner_reviewed")
    if gold.get("publication_eligible") is not False:
        issues.error(
            "owner_gold_publishable",
            "owner gold calibrates quality but cannot directly publish",
        )
    if not isinstance(gold.get("labels"), Mapping) or not gold.get("labels"):
        issues.error("owner_gold_labels_missing", "owner gold labels are missing")
    validate_hashes(gold, role="owner gold", strict=True, issues=issues)


def render_report(
    *,
    mode: str,
    inputs: Mapping[str, str | None],
    summary: Mapping[str, Mapping[str, int]],
    issues: Collector,
) -> tuple[dict[str, Any], str]:
    errors = [item for item in issues.issues if item.severity == "error"]
    warnings = [item for item in issues.issues if item.severity == "warning"]
    report = {
        "schema_version": "static-collections-quality-report-v1",
        "mode": mode,
        "ok": not errors,
        "inputs": dict(inputs),
        "summary": {
            "errors": len(errors),
            "warnings": len(warnings),
            "labels": dict(summary),
        },
        "issues": [asdict(item) for item in issues.issues],
    }
    lines = [
        "# Static collections quality report",
        "",
        f"- Mode: `{mode}`",
        f"- Result: **{'PASS' if report['ok'] else 'FAIL'}**",
        f"- Errors: `{len(errors)}`",
        f"- Warnings: `{len(warnings)}`",
        "",
        "## Label inventory",
        "",
        "| Label | Positive rows | Positive units | Hard-negative rows | Hard-negative units |",
        "|---|---:|---:|---:|---:|",
    ]
    for label, values in sorted(summary.items()):
        lines.append(
            f"| `{label}` | {values.get('positive_rows', 0)} | "
            f"{values.get('positive_units', 0)} | "
            f"{values.get('negative_rows', 0)} | "
            f"{values.get('negative_units', 0)} |"
        )
    lines.extend(["", "## Issues", ""])
    if not issues.issues:
        lines.append("No issues.")
    for item in issues.issues:
        location = f" label=`{item.label}`" if item.label else ""
        location += f" event_id=`{item.event_id}`" if item.event_id is not None else ""
        lines.append(
            f"- **{item.severity.upper()} `{item.code}`**{location}: {item.message}"
        )
    return report, "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    issues = Collector()
    policy = load_json(args.policy, "policy", issues)
    seed = load_json(args.seed, "seed", issues)
    gold = load_json(args.owner_gold, "owner_gold", issues) if args.owner_gold else {}
    semantic = validate_policy(policy, issues) if policy else set()
    summary = (
        validate_seed(
            seed,
            policy,
            semantic,
            strict=args.mode == "strict",
            seed_path=args.seed,
            issues=issues,
        )
        if policy and seed
        else {}
    )
    validate_owner_gold(gold, strict=args.mode == "strict", issues=issues)
    report, markdown = render_report(
        mode=args.mode,
        inputs={
            "policy": str(args.policy),
            "seed": str(args.seed),
            "owner_gold": str(args.owner_gold) if args.owner_gold else None,
        },
        summary=summary,
        issues=issues,
    )
    if args.json_report:
        args.json_report.parent.mkdir(parents=True, exist_ok=True)
        args.json_report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if args.markdown_report:
        args.markdown_report.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_report.write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
