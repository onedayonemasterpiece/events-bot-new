#!/usr/bin/env python3
"""Fail-closed static-collection policy/review validator.

Baseline mode preserves the bootstrap contract from PR #207. Review mode is
the PR-A gate: it requires ontology v2, migrated provisional review data,
source provenance, source-review receipts and occurrence families, but does
not pretend that PR-B scores or owner gold already exist. Strict mode is the
later evaluation contract and requires those PR-B artifacts.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "site" / "scripts" / "static_collection_policy.v2.json"
DEFAULT_SEED = ROOT / "docs" / "review-data" / "static_collections_review_seed_v1.json"
DEFAULT_SOURCE_REVIEW_INDEX = (
    ROOT
    / "docs"
    / "review-data"
    / "static-collections-source-reviews-v1"
    / "index.json"
)
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
ONTOLOGY_V2_LABELS = frozenset(
    {
        "child_directed",
        "family_suitable",
        "joint_family_activity",
        "science_pop",
        "research_in_action",
        "strong_impressions",
        "medieval",
    }
)
LEGACY_ONTOLOGY_LABELS = frozenset(
    {"science", "audience_kids_candidate", "audience_family_candidate"}
)
REQUIRED_SOURCE_REVIEW_IDS = frozenset(
    {5757, 5781, 6696, 6766, 6871, 6878, 7054, 7113, 7114, 7237, 7238, 7307, 7326, 7333, 7344, 7373, 7374}
)
SOURCE_REF_HASH_FIELDS = (
    "event_id",
    "source_id",
    "source_type",
    "source_url",
    "trust_level",
    "source_chat_username",
    "source_message_id",
    "source_text_sha256",
    "source_text_char_count",
)
SNAPSHOT_SCHEMA = "static-collections-evidence-snapshot-v1"
SNAPSHOT_SERIALIZATION = "canonical-json-v1"
SNAPSHOT_QUERY_CONTRACT = "event-review-source-v1"
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
    parser.add_argument(
        "--mode", choices=("baseline", "review", "strict"), default="review"
    )
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--owner-gold", type=Path)
    parser.add_argument("--source-review-index", type=Path, default=DEFAULT_SOURCE_REVIEW_INDEX)
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


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def utc_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return True


def expected_source_ref_sha256(value: Mapping[str, Any]) -> str:
    return canonical_json_sha256({field: value.get(field) for field in SOURCE_REF_HASH_FIELDS})


def validate_policy(
    policy: Mapping[str, Any], *, migrated: bool, issues: Collector
) -> set[str]:
    labels = policy.get("labels")
    if not isinstance(labels, Mapping) or not labels:
        issues.error("policy_labels_missing", "policy.labels must be non-empty")
        return set()
    if migrated:
        if policy.get("schema_version") != "static-collection-policy-v2":
            issues.error("policy_schema_legacy", "review mode requires policy v2")
        if policy.get("policy_version") != "static-collection-ontology-v2":
            issues.error("policy_version_invalid", "ontology-v2 policy version is required")
        definitions = policy.get("definitions")
        if not isinstance(definitions, Mapping):
            issues.error("policy_definitions_missing", "policy.definitions is required")
            definitions = {}
        missing_definitions = sorted(ONTOLOGY_V2_LABELS - set(definitions))
        if missing_definitions:
            issues.error(
                "policy_definitions_incomplete",
                "missing ontology definitions: " + ", ".join(missing_definitions),
            )
        legacy = sorted(LEGACY_ONTOLOGY_LABELS & set(labels))
        if legacy:
            issues.error(
                "policy_legacy_labels",
                "legacy ontology labels remain: " + ", ".join(legacy),
            )
        projection = policy.get("public_projections")
        kids = projection.get("kids") if isinstance(projection, Mapping) else None
        if not isinstance(kids, Mapping) or kids.get("members") != [
            "child_directed",
            "family_suitable",
        ]:
            issues.error(
                "kids_projection_invalid",
                "kids must be the ordered union of child_directed and family_suitable",
            )
        elif kids.get("publication") != "blocked":
            issues.error("kids_projection_publishable", "ontology-v2 kids must remain blocked")

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
        if migrated and label in ONTOLOGY_V2_LABELS:
            expected_ref = f"ontology-v2.{label}"
            if raw_config.get("definition_ref") != expected_ref:
                issues.error(
                    "policy_definition_ref_invalid",
                    f"definition_ref must equal {expected_ref}",
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
        source_id = item.get("source_id")
        valid_source_id = text(source_id) or (
            isinstance(source_id, int) and not isinstance(source_id, bool) and source_id > 0
        )
        if not (text(item.get("source_url")) or valid_source_id):
            issues.error(
                "source_ref_identity_missing",
                f"source_refs[{index}] requires source_url or source_id",
                label=label,
                event_id=event_id,
            )
        if item.get("event_id") != event_id:
            issues.error(
                "source_ref_event_mismatch",
                f"source_refs[{index}].event_id must equal row/event evidence id",
                label=label,
                event_id=event_id,
            )
        if not isinstance(item.get("source_text_char_count"), int) or item.get(
            "source_text_char_count", 0
        ) <= 0:
            issues.error(
                "source_ref_text_length_invalid",
                f"source_refs[{index}].source_text_char_count must be positive",
                label=label,
                event_id=event_id,
            )
        for field in ("source_text_sha256", "source_record_sha256", "source_ref_sha256"):
            if not sha256(item.get(field)):
                issues.error(
                    "source_ref_hash_invalid",
                    f"source_refs[{index}].{field} must be SHA-256",
                    label=label,
                    event_id=event_id,
                )
        if item.get("source_ref_sha256") != expected_source_ref_sha256(item):
            issues.error(
                "source_ref_hash_mismatch",
                f"source_refs[{index}].source_ref_sha256 does not bind the canonical source ref",
                label=label,
                event_id=event_id,
            )


def validate_quote_metadata(
    value: Mapping[str, Any],
    *,
    prefix: str,
    source_ref: Mapping[str, Any] | None,
    label: str,
    event_id: int | None,
    issues: Collector,
) -> None:
    quote_field = f"{prefix}quote"
    kind_field = f"{prefix}quote_kind"
    truncated_field = f"{prefix}quote_truncated"
    start_field = f"{prefix}quote_start_char"
    end_field = f"{prefix}quote_end_char"
    count_field = f"{prefix}quote_char_count"
    prefix_omitted_field = f"{prefix}quote_omitted_prefix_chars"
    suffix_omitted_field = f"{prefix}quote_omitted_suffix_chars"
    quote = value.get(quote_field)
    if not text(quote):
        return
    kind = value.get(kind_field)
    truncated = value.get(truncated_field)
    start = value.get(start_field)
    end = value.get(end_field)
    count = value.get(count_field)
    omitted_prefix = value.get(prefix_omitted_field)
    omitted_suffix = value.get(suffix_omitted_field)
    source_count = source_ref.get("source_text_char_count") if source_ref else None
    valid_ints = all(
        isinstance(item, int) and not isinstance(item, bool)
        for item in (start, end, count, omitted_prefix, omitted_suffix, source_count)
    )
    if kind not in {"full", "excerpt"} or not isinstance(truncated, bool) or not valid_ints:
        issues.error(
            "source_quote_metadata_invalid",
            f"{prefix}quote requires full/excerpt kind, truncation flag and character offsets",
            label=label,
            event_id=event_id,
        )
        return
    if start < 0 or end < start or count != len(str(quote)) or end - start != count:
        issues.error(
            "source_quote_offsets_invalid",
            f"{prefix}quote character offsets/count are inconsistent",
            label=label,
            event_id=event_id,
        )
    if omitted_prefix != start or omitted_suffix != source_count - end or end > source_count:
        issues.error(
            "source_quote_truncation_invalid",
            f"{prefix}quote omitted-character metadata is inconsistent",
            label=label,
            event_id=event_id,
        )
    is_full = start == 0 and end == source_count
    if (kind == "full") != is_full or truncated == is_full:
        issues.error(
            "source_quote_kind_invalid",
            f"{prefix}quote kind/truncation does not match its bounds",
            label=label,
            event_id=event_id,
        )
    if is_full and source_ref and source_ref.get("source_text_sha256") != hashlib.sha256(
        str(quote).encode("utf-8")
    ).hexdigest():
        issues.error(
            "full_source_quote_hash_mismatch",
            f"full {prefix}quote does not match source_text_sha256",
            label=label,
            event_id=event_id,
        )


def validate_row(
    row: Mapping[str, Any],
    *,
    label: str,
    expected: str,
    strict: bool,
    migrated: bool,
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

    required_text = ("title", "reason_code") if migrated else (
        "title",
        "reason_code",
        "evidence_excerpt",
    )
    for field in required_text:
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
    if migrated:
        if not text(row.get("source_quote")):
            issues.error(
                "source_quote_missing",
                "migrated row requires source_quote",
                label=label,
                event_id=event_id,
            )
        if row.get("review_decision") != "keep":
            issues.error(
                "review_decision_invalid",
                "rows counted in the review seed require review_decision=keep",
                label=label,
                event_id=event_id,
            )
        if not family:
            issues.error(
                "family_id_missing", "migrated row requires family_id", label=label, event_id=event_id
            )
        quote = row.get("source_quote")
        if text(quote):
            expected_quote_hash = hashlib.sha256(str(quote).encode("utf-8")).hexdigest()
            if row.get("source_quote_sha256") != expected_quote_hash:
                issues.error(
                    "source_quote_hash_mismatch",
                    "source_quote_sha256 does not bind source_quote",
                    label=label,
                    event_id=event_id,
                )
        validate_source_refs(row.get("source_refs"), label, event_id, issues)
        first_source_ref = (
            row["source_refs"][0]
            if isinstance(row.get("source_refs"), list)
            and row["source_refs"]
            and isinstance(row["source_refs"][0], Mapping)
            else None
        )
        validate_quote_metadata(
            row,
            prefix="source_",
            source_ref=first_source_ref,
            label=label,
            event_id=event_id,
            issues=issues,
        )
        festival_kind = row.get("festival_scope_kind")
        if festival_kind is not None:
            if festival_kind != "festival_child_event":
                issues.error(
                    "festival_row_scope_invalid",
                    "review rows may contain only occurrence-specific festival child events",
                    label=label,
                    event_id=event_id,
                )
            if row.get("occurrence_specific_source") is not True:
                issues.error(
                    "festival_child_source_missing",
                    "festival child row requires occurrence_specific_source=true",
                    label=label,
                    event_id=event_id,
                )
        if not sha256(row.get("model_document_hash")):
            issues.error(
                "model_document_hash_invalid",
                "model_document_hash must be SHA-256",
                label=label,
                event_id=event_id,
            )

    if not strict:
        return event_id, family

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
    migrated: bool,
    starter: bool,
    minimum_positives: int,
    minimum_negatives: int,
    issues: Collector,
) -> dict[str, int]:
    if not text(payload.get("definition")):
        issues.add(
            "warning" if not strict else "error",
            "label_definition_missing",
            "definition is required",
            label=label,
        )
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
            migrated=migrated,
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
            migrated=migrated,
            issues=issues,
        )
        if event_id is not None:
            negative_ids.append(event_id)
        if family:
            negative_families.append(family)

    if not migrated:
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
        issues.add(
            "warning" if starter and not strict else "error",
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
    migrated: bool = False,
) -> None:
    source = value.get("source") if isinstance(value.get("source"), Mapping) else value
    for field in HASH_FIELDS:
        if sha256(source.get(field)):
            continue
        issues.add(
            "error" if strict else "warning",
            (
                "hash_binding_missing"
                if strict
                else "review_artifact_binding_pending"
                if migrated
                else "legacy_hash_binding_missing"
            ),
            f"{role} lacks SHA-256 field {field}",
        )


def validate_seed(
    seed: Mapping[str, Any],
    policy: Mapping[str, Any],
    semantic: set[str],
    *,
    strict: bool,
    migrated: bool,
    seed_path: Path,
    issues: Collector,
) -> dict[str, dict[str, int]]:
    status = str(seed.get("status") or "")
    starter = status == "starter_not_approved"
    provisional = status == "provisional_agent_seed_not_owner_approved"
    publication_eligible = seed.get("publication_eligible")
    if provisional and publication_eligible is not False:
        issues.error(
            "provisional_seed_publishable",
            "provisional review seed must set publication_eligible=false",
        )
    if starter:
        if publication_eligible is True:
            issues.error(
                "starter_seed_publishable",
                "starter seed cannot authorize publication",
            )
        elif publication_eligible is None:
            issues.warning(
                "starter_publication_flag_missing",
                "starter seed should explicitly set publication_eligible=false",
            )
    if migrated:
        if seed.get("schema_version") != "static-collections-review-seed-v1":
            issues.error("review_seed_schema_legacy", "migrated mode requires review-seed-v1 schema")
        if "gold" in seed_path.name.casefold():
            issues.error("review_seed_named_gold", "migrated review seed filename must not contain gold")
        if status != "provisional_agent_seed_not_owner_approved":
            issues.error("review_seed_status_invalid", "review seed must remain provisional")
        if publication_eligible is not False:
            issues.error("review_seed_publishable", "review seed must set publication_eligible=false")
        if seed.get("ontology_version") != "static-collection-ontology-v2":
            issues.error("review_seed_ontology_invalid", "review seed must bind ontology v2")
        review_scope = seed.get("review_scope")
        if not isinstance(review_scope, Mapping) or any(
            review_scope.get(field) != expected
            for field, expected in {
                "cinema_events": "excluded_from_review_rows",
                "festival_parent_rows": "excluded_from_review_rows",
                "festival_child_events": "allowed_with_occurrence_specific_source",
                "festival_extraction_pages": "out_of_scope",
            }.items()
        ):
            issues.error(
                "review_seed_scope_invalid",
                "PR A must distinguish festival parents, child events and extraction/pages",
            )
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
        if migrated and label in ONTOLOGY_V2_LABELS:
            expected_definition_id = f"ontology-v2.{label}"
            definitions = (
                policy.get("definitions")
                if isinstance(policy.get("definitions"), Mapping)
                else {}
            )
            definition = definitions.get(label)
            if payload.get("definition_id") != expected_definition_id:
                issues.error(
                    "seed_definition_ref_invalid",
                    f"definition_id must equal {expected_definition_id}",
                    label=label,
                )
            if not isinstance(definition, Mapping) or payload.get(
                "definition"
            ) != definition.get("definition"):
                issues.error(
                    "seed_definition_drift",
                    "review seed definition must equal the canonical policy definition",
                    label=label,
                )
        summary[label] = validate_label(
            label,
            payload,
            strict=strict,
            migrated=migrated,
            starter=starter,
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
    if migrated:
        seed_labels = set(labels)
        missing_v2 = sorted(ONTOLOGY_V2_LABELS - seed_labels)
        if missing_v2:
            issues.error(
                "review_seed_labels_incomplete",
                "missing ontology-v2 review labels: " + ", ".join(missing_v2),
            )
        legacy = sorted(LEGACY_ONTOLOGY_LABELS & seed_labels)
        if legacy:
            issues.error(
                "review_seed_legacy_labels",
                "legacy review labels remain: " + ", ".join(legacy),
            )
        positive_ids = {
            label: {
                row.get("event_id")
                for row in payload.get("positives", [])
                if isinstance(row, Mapping)
            }
            for label, payload in labels.items()
            if isinstance(payload, Mapping)
        }
        if 4648 in positive_ids.get("science_pop", set()) or 4648 in positive_ids.get(
            "family_suitable", set()
        ):
            issues.error(
                "known_semantic_leak_4648",
                "event 4648 is not a source-supported science_pop/family_suitable positive",
            )
        selected_ids = {
            row.get("event_id")
            for payload in labels.values()
            if isinstance(payload, Mapping)
            for side in ("positives", "hard_negatives")
            for row in payload.get(side, [])
            if isinstance(row, Mapping)
        }
        if 6871 in selected_ids:
            issues.error(
                "occurrence_source_review_leak_6871",
                "event 6871 must stay out of semantic supply pending occurrence-specific review",
            )
        source = seed.get("source") if isinstance(seed.get("source"), Mapping) else {}
        if "generator_repo_sha" in source:
            issues.error(
                "generator_repo_sha_legacy",
                "generator_repo_sha is ambiguous; use the three role-specific repo SHAs",
            )
        for field in (
            "extraction_repo_sha",
            "seed_builder_repo_sha",
            "integration_repo_sha",
        ):
            repo_sha = source.get(field)
            if not isinstance(repo_sha, str) or not GIT_COMMIT.fullmatch(repo_sha):
                issues.error(
                    "generator_provenance_invalid",
                    f"source.{field} must be a 40/64 hex commit id",
                )
        command = source.get("generator_command")
        if not text(command) or not str(command).startswith(
            "python3 scripts/build_static_collections_review_seed.py "
        ) or " --snapshot " not in str(command) or " --output " not in str(command):
            issues.error(
                "generator_provenance_invalid",
                "generator_command must be the exact reproducible review-seed builder command",
            )
        if not (ROOT / "scripts" / "build_static_collections_review_seed.py").is_file():
            issues.error(
                "generator_provenance_invalid",
                "generator_command references a missing review-seed builder",
            )
        if not utc_timestamp(source.get("extracted_at")) or not utc_timestamp(
            source.get("reviewed_at")
        ):
            issues.error(
                "generator_provenance_invalid",
                "source.extracted_at and source.reviewed_at must be exact UTC timestamps",
            )
        snapshot_contract = seed.get("snapshot_contract")
        if not isinstance(snapshot_contract, Mapping) or any(
            snapshot_contract.get(field) != expected
            for field, expected in {
                "schema_version": SNAPSHOT_SCHEMA,
                "serialization_contract": SNAPSHOT_SERIALIZATION,
                "query_contract": SNAPSHOT_QUERY_CONTRACT,
                "encoding": "utf-8",
                "event_order": "id_ascending",
                "event_source_order": "id_ascending_per_event",
            }.items()
        ) or snapshot_contract.get("json_options") != {
            "allow_nan": False,
            "ensure_ascii": False,
            "separators": [",", ":"],
            "sort_keys": True,
            "trailing_newline": False,
        } or snapshot_contract.get("artifact_path") != (
            "artifacts/codex/static-collections-pr-a/"
            "static-collections-evidence-snapshot-v1.json"
        ) or any(
            not isinstance(snapshot_contract.get(field), int)
            or isinstance(snapshot_contract.get(field), bool)
            or snapshot_contract.get(field) <= 0
            for field in ("event_count", "event_source_count")
        ) or not utc_timestamp(snapshot_contract.get("db_file_mtime")):
            issues.error(
                "snapshot_contract_invalid",
                "canonical snapshot schema/serialization contract is missing or invalid",
            )
        if not sha256(seed.get("evidence_snapshot_sha256")):
            issues.error(
                "snapshot_contract_invalid",
                "seed.evidence_snapshot_sha256 must be SHA-256",
            )
    validate_hashes(
        seed, role="review seed", strict=strict, migrated=migrated, issues=issues
    )
    return summary


def validate_source_reviews(
    index: Mapping[str, Any], *, index_path: Path, required: bool, issues: Collector
) -> None:
    if not required:
        return
    if index.get("schema_version") != "static-collection-source-review-index-v1":
        issues.error("source_review_index_schema_invalid", "source-review index v1 is required")
        return
    receipt_rows = index.get("receipts")
    if not isinstance(receipt_rows, list) or not receipt_rows:
        issues.error("source_review_receipts_missing", "source-review receipts are required")
        return
    required_ids = index.get("required_event_ids")
    if required_ids != sorted(REQUIRED_SOURCE_REVIEW_IDS):
        issues.error(
            "source_review_required_ids_mismatch",
            "index.required_event_ids must exactly equal the mandatory review set",
        )
    if not sha256(index.get("source_snapshot_sha256")):
        issues.error(
            "source_review_snapshot_invalid",
            "index.source_snapshot_sha256 must be SHA-256",
        )
    covered: set[int] = set()
    receipt_ids: set[str] = set()
    receipt_paths: set[str] = set()
    for entry in receipt_rows:
        if not isinstance(entry, Mapping) or not text(entry.get("path")):
            issues.error("source_review_entry_invalid", "receipt index entry is invalid")
            continue
        entry_receipt_id = entry.get("receipt_id")
        entry_path = str(entry["path"])
        if not text(entry_receipt_id) or str(entry_receipt_id) in receipt_ids:
            issues.error("source_review_duplicate_receipt_id", "receipt_id must be unique")
        else:
            receipt_ids.add(str(entry_receipt_id))
        if entry_path in receipt_paths:
            issues.error("source_review_duplicate_path", "receipt path must be unique")
        else:
            receipt_paths.add(entry_path)
        if Path(entry_path).name != entry_path:
            issues.error("source_review_entry_path_invalid", "receipt path must be a safe basename")
            continue
        path = index_path.parent / str(entry["path"])
        receipt = load_json(path, "source_review_receipt", issues)
        if receipt.get("schema_version") != "static-collection-source-review-v1":
            issues.error("source_review_receipt_schema_invalid", f"invalid receipt schema: {path.name}")
            continue
        if entry.get("receipt_id") != receipt.get("receipt_id"):
            issues.error(
                "source_review_entry_receipt_id_mismatch",
                f"index receipt_id does not match {path.name}",
            )
        if entry.get("status") != receipt.get("status"):
            issues.error(
                "source_review_entry_status_mismatch",
                f"index status does not match {path.name}",
            )
        if entry.get("event_ids") != receipt.get("event_ids"):
            issues.error(
                "source_review_entry_event_ids_mismatch",
                f"index event_ids do not match {path.name}",
            )
        status = receipt.get("status")
        if status not in {"pass", "needs_source_review", "corrected"}:
            issues.error("source_review_status_invalid", f"invalid status in {path.name}")
        ids = receipt.get("event_ids")
        if not isinstance(ids, list) or any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in ids
        ):
            issues.error("source_review_event_ids_invalid", f"invalid event_ids in {path.name}")
            ids = []
        covered.update(ids)
        evidence = receipt.get("source_evidence")
        if not isinstance(evidence, list) or not evidence:
            issues.error("source_review_evidence_missing", f"raw source evidence missing in {path.name}")
        else:
            for item in evidence:
                quote = item.get("raw_source_quote") if isinstance(item, Mapping) else None
                if not text(quote):
                    issues.error("source_review_quote_missing", f"raw quote missing in {path.name}")
                    continue
                if item.get("raw_source_quote_sha256") != hashlib.sha256(
                    str(quote).encode("utf-8")
                ).hexdigest():
                    issues.error("source_review_quote_hash_mismatch", f"raw quote hash mismatch in {path.name}")
                evidence_event_id = item.get("event_id") if isinstance(item, Mapping) else None
                if evidence_event_id not in ids:
                    issues.error(
                        "source_review_evidence_event_mismatch",
                        f"evidence event_id is outside receipt.event_ids in {path.name}",
                    )
                source_ref = item.get("source_ref") if isinstance(item, Mapping) else None
                validate_source_refs(
                    [source_ref] if isinstance(source_ref, Mapping) else source_ref,
                    f"receipt:{receipt.get('receipt_id')}",
                    evidence_event_id if isinstance(evidence_event_id, int) else None,
                    issues,
                )
                validate_quote_metadata(
                    item,
                    prefix="raw_source_",
                    source_ref=source_ref if isinstance(source_ref, Mapping) else None,
                    label=f"receipt:{receipt.get('receipt_id')}",
                    event_id=evidence_event_id if isinstance(evidence_event_id, int) else None,
                    issues=issues,
                )
        expected_hash = receipt.get("receipt_sha256")
        unhashed = dict(receipt)
        unhashed.pop("receipt_sha256", None)
        calculated = canonical_json_sha256(unhashed)
        if expected_hash != calculated or entry.get("receipt_sha256") != calculated:
            issues.error("source_review_receipt_hash_mismatch", f"receipt hash mismatch in {path.name}")
    if covered != REQUIRED_SOURCE_REVIEW_IDS:
        issues.error(
            "source_review_ids_mismatch",
            "receipt coverage must exactly equal mandatory source-review IDs; "
            f"missing={sorted(REQUIRED_SOURCE_REVIEW_IDS - covered)}, "
            f"extra={sorted(covered - REQUIRED_SOURCE_REVIEW_IDS)}",
        )


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
    migrated = args.mode in {"review", "strict"}
    source_review_index = (
        load_json(args.source_review_index, "source_review_index", issues)
        if migrated
        else {}
    )
    if migrated and isinstance(seed.get("source"), Mapping):
        source = seed["source"]
        try:
            policy_file_sha = hashlib.sha256(args.policy.read_bytes()).hexdigest()
        except OSError:
            policy_file_sha = None
        if source.get("policy_sha256") != policy_file_sha:
            issues.error(
                "review_seed_policy_hash_mismatch",
                "review seed policy_sha256 does not bind the selected policy file",
            )
        if source_review_index:
            unhashed_index = dict(source_review_index)
            declared_index_hash = unhashed_index.pop("index_sha256", None)
            calculated_index_hash = hashlib.sha256(
                json.dumps(
                    unhashed_index,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if declared_index_hash != calculated_index_hash:
                issues.error(
                    "source_review_index_hash_mismatch",
                    "source-review index self hash is invalid",
                )
            if source.get("source_review_index_sha256") != declared_index_hash:
                issues.error(
                    "review_seed_source_index_hash_mismatch",
                    "review seed does not bind the selected source-review index",
                )
            if seed.get("evidence_snapshot_sha256") != source_review_index.get(
                "source_snapshot_sha256"
            ):
                issues.error(
                    "source_review_snapshot_mismatch",
                    "seed.evidence_snapshot_sha256 must equal index.source_snapshot_sha256",
                )
    semantic = (
        validate_policy(policy, migrated=migrated, issues=issues) if policy else set()
    )
    summary = (
        validate_seed(
            seed,
            policy,
            semantic,
            strict=args.mode == "strict",
            migrated=migrated,
            seed_path=args.seed,
            issues=issues,
        )
        if policy and seed
        else {}
    )
    validate_source_reviews(
        source_review_index,
        index_path=args.source_review_index,
        required=migrated,
        issues=issues,
    )
    validate_owner_gold(gold, strict=args.mode == "strict", issues=issues)
    report, markdown = render_report(
        mode=args.mode,
        inputs={
            "policy": str(args.policy),
            "seed": str(args.seed),
            "owner_gold": str(args.owner_gold) if args.owner_gold else None,
            "source_review_index": (
                str(args.source_review_index) if migrated else None
            ),
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
