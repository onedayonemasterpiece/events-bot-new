#!/usr/bin/env python3
"""Deterministic unusual-event scoring over a precomputed shared BGE artifact.

There is intentionally no encoder import or invocation in this module.  The
public scorer accepts hash-bound event/prototype vectors from
``static_event_bge.py` and fails closed when any vector-space, document, bank,
classifier, or payload hash differs.  The checked-in head remains shadow-only
until a real BGE canary supplies all frozen quality-gate measurements.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from static_event_bge import (
        ARTIFACT_SCHEMA_VERSION,
        DOCUMENT_VERSION,
        EMBEDDING_DIM,
        ENCODER_CONTRACT,
        MODEL_ID,
        MODEL_REVISION,
        VECTOR_NORMALIZATION,
        build_related_v1_document,
        stable_hash,
    )
except ImportError:  # pragma: no cover - package-style imports in some runners
    from .static_event_bge import (  # type: ignore
        ARTIFACT_SCHEMA_VERSION,
        DOCUMENT_VERSION,
        EMBEDDING_DIM,
        ENCODER_CONTRACT,
        MODEL_ID,
        MODEL_REVISION,
        VECTOR_NORMALIZATION,
        build_related_v1_document,
        stable_hash,
    )

SCORER_SCHEMA_VERSION = "unusual-event-scorer-v1"
POLICY_VERSION = "unusual-event-policy-v1"
MANIFEST_SCHEMA_VERSION = "unusual-event-manifest-v1"
CACHE_SCHEMA_VERSION = "unusual-event-score-cache-v1"
_HERE = Path(__file__).resolve().parent
DEFAULT_PROTOTYPE_BANK = _HERE / "unusual_event_prototypes.v1.json"
DEFAULT_CLASSIFIER = _HERE / "unusual_event_classifier.v1.json"
FAMILY_IDS = (
    "open_dialogue",
    "participatory",
    "co_creation",
    "behind_scenes",
    "restricted_access",
    "site_specific",
    "after_hours",
    "hybrid_format",
    "living_history",
    "field_science",
    "rare_practice",
    "gastro_experience",
    "sensory_wellbeing",
    "community_exchange",
    "quirky_ritual",
)
DECISION_ORDER = {"core": 0, "adjacent": 1, "abstain": 2, "ordinary": 3}
PUBLIC_TIER = {
    "core": "core_unusual",
    "adjacent": "adjacent",
    "ordinary": "ordinary",
    "abstain": "abstain",
}


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def load_unusual_prototype_bank(
    path: Path | str = DEFAULT_PROTOTYPE_BANK,
) -> dict[str, Any]:
    """Load and structurally validate the frozen 15-family prototype bank."""

    bank = _load_object(Path(path))
    if bank.get("schema_version") != "unusual-event-prototype-bank-v1":
        raise ValueError("unsupported unusual-event prototype bank")
    if bank.get("document_version") != DOCUMENT_VERSION:
        raise ValueError("prototype bank uses another document version")
    families = bank.get("families")
    actual_families = tuple(
        str(row.get("id") or "")
        for row in families or []
        if isinstance(row, Mapping)
    )
    if actual_families != FAMILY_IDS:
        raise ValueError("prototype bank must contain the canonical 15-family order")
    prototypes = bank.get("prototypes")
    if not isinstance(prototypes, list) or not prototypes:
        raise ValueError("prototype bank has no prototypes")
    ids: set[str] = set()
    family_kinds = {family: set() for family in FAMILY_IDS}
    neutral_count = 0
    for row in prototypes:
        if not isinstance(row, Mapping):
            raise ValueError("prototype entry must be an object")
        prototype_id = str(row.get("id") or "")
        kind = str(row.get("kind") or "")
        family = row.get("family")
        if not prototype_id or prototype_id in ids or not str(row.get("text") or "").strip():
            raise ValueError("prototype ids must be unique and text must be non-empty")
        ids.add(prototype_id)
        if kind == "neutral":
            if family is not None:
                raise ValueError("neutral prototypes cannot declare a family")
            neutral_count += 1
        elif kind in {"positive", "hard_negative"} and family in family_kinds:
            family_kinds[str(family)].add(kind)
        else:
            raise ValueError(f"invalid prototype kind/family for {prototype_id}")
    if neutral_count < 5:
        raise ValueError("prototype bank needs at least five neutral anchors")
    if any(kinds != {"positive", "hard_negative"} for kinds in family_kinds.values()):
        raise ValueError("every family needs positive and hard-negative prototypes")
    return bank


def load_unusual_classifier(
    path: Path | str = DEFAULT_CLASSIFIER,
) -> dict[str, Any]:
    """Load and structurally validate the deterministic margin head."""

    classifier = _load_object(Path(path))
    if classifier.get("schema_version") != "unusual-event-classifier-v1":
        raise ValueError("unsupported unusual-event classifier")
    if classifier.get("classifier_kind") != "margin_logistic_head_v1":
        raise ValueError("unsupported unusual-event classifier kind")
    features = classifier.get("feature_order")
    weights = classifier.get("weights")
    if not isinstance(features, list) or not features or not isinstance(weights, Mapping):
        raise ValueError("classifier feature contract is incomplete")
    if set(features) != set(weights):
        raise ValueError("classifier feature order and weights differ")
    for value in [classifier.get("bias"), *weights.values()]:
        if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError("classifier coefficients must be finite")
    required_thresholds = {
        "core_probability",
        "adjacent_probability",
        "ordinary_probability",
        "core_min_positive_hard_negative_margin",
        "core_min_positive_neutral_margin",
        "adjacent_min_positive_hard_negative_margin",
        "adjacent_min_positive_neutral_margin",
        "family_min_margin",
    }
    thresholds = classifier.get("decision_thresholds")
    if not isinstance(thresholds, Mapping) or not required_thresholds.issubset(thresholds):
        raise ValueError("classifier decision thresholds are incomplete")
    return classifier


def _vector_row(
    collection: Mapping[str, Any], key: str, *, expected_dim: int
) -> tuple[list[float], str]:
    raw = collection.get(key)
    if not isinstance(raw, Mapping):
        raise ValueError(f"missing vector row {key}")
    vector = raw.get("vector")
    text_hash = str(raw.get("text_hash") or "")
    if not isinstance(vector, list) or len(vector) != expected_dim or not text_hash:
        raise ValueError(f"invalid vector row {key}")
    values = [float(value) for value in vector]
    if not all(math.isfinite(value) for value in values):
        raise ValueError(f"non-finite vector row {key}")
    norm = math.sqrt(sum(value * value for value in values))
    if abs(norm - 1.0) > 1e-3:
        raise ValueError(f"vector row {key} is not L2-normalised")
    return values, text_hash


def _validate_boundary(
    events: list[dict[str, Any]],
    event_vectors: Mapping[str, Any],
    prototype_vectors: Mapping[str, Any],
    vector_metadata: Mapping[str, Any],
    bank: Mapping[str, Any],
    classifier: Mapping[str, Any],
    build_metadata: Mapping[str, Any],
) -> tuple[list[str], dict[int, dict[str, Any]]]:
    errors: list[str] = []
    expected = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "encoder_contract": ENCODER_CONTRACT,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_version": DOCUMENT_VERSION,
        "vector_normalization": VECTOR_NORMALIZATION,
        "prototype_bank_sha256": stable_hash(bank),
        "classifier_sha256": stable_hash(classifier),
    }
    for key, value in expected.items():
        if vector_metadata.get(key) != value:
            errors.append(f"{key} mismatch")
    try:
        date.fromisoformat(str(build_metadata.get("as_of_date") or ""))
    except ValueError:
        errors.append("build_metadata.as_of_date must be ISO YYYY-MM-DD")
    documents: dict[int, dict[str, Any]] = {}
    seen: set[int] = set()
    for event in events:
        try:
            event_id = int(event.get("id") or event.get("event_id") or 0)
        except (TypeError, ValueError):
            event_id = 0
        if event_id <= 0:
            errors.append("event id must be positive")
            continue
        if event_id in seen:
            errors.append(f"duplicate event id {event_id}")
            continue
        seen.add(event_id)
        try:
            document = build_related_v1_document(event)
            _, text_hash = _vector_row(
                event_vectors, str(event_id), expected_dim=EMBEDDING_DIM
            )
            if text_hash != document["text_hash"]:
                errors.append(f"event {event_id} text_hash mismatch")
            documents[event_id] = document
        except (TypeError, ValueError) as exc:
            errors.append(str(exc))
    bank_prototypes = {
        str(row["id"]): row
        for row in bank["prototypes"]
        if isinstance(row, Mapping)
    }
    for prototype_id, prototype in bank_prototypes.items():
        try:
            _, text_hash = _vector_row(
                prototype_vectors, prototype_id, expected_dim=EMBEDDING_DIM
            )
            expected_text_hash = hashlib.sha256(
                str(prototype["text"]).encode("utf-8")
            ).hexdigest()
            if text_hash != expected_text_hash:
                errors.append(f"prototype {prototype_id} text_hash mismatch")
        except (TypeError, ValueError) as exc:
            errors.append(str(exc))
    declared_hash = vector_metadata.get("artifact_sha256")
    unhashed_metadata = dict(vector_metadata)
    unhashed_metadata.pop("artifact_sha256", None)
    actual_hash = stable_hash(
        {
            "metadata": unhashed_metadata,
            "event_vectors": event_vectors,
            "prototype_vectors": prototype_vectors,
        }
    )
    if declared_hash != actual_hash:
        errors.append("artifact_sha256 mismatch")
    return sorted(set(errors)), documents


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _round(value: float) -> float:
    return round(float(value), 6)


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-min(value, 60.0))
        return 1.0 / (1.0 + z)
    z = math.exp(max(value, -60.0))
    return z / (1.0 + z)


def _semantic_features(
    event_vector: Sequence[float],
    prototype_vectors: Mapping[str, Any],
    bank: Mapping[str, Any],
) -> tuple[
    dict[str, float],
    str,
    dict[str, float],
    list[dict[str, Any]],
]:
    scores: list[dict[str, Any]] = []
    family_scores = {family: -1.0 for family in FAMILY_IDS}
    for prototype in bank["prototypes"]:
        prototype_id = str(prototype["id"])
        vector, _ = _vector_row(
            prototype_vectors, prototype_id, expected_dim=EMBEDDING_DIM
        )
        score = _dot(event_vector, vector)
        row = {
            "prototype_id": prototype_id,
            "prototype_kind": prototype["kind"],
            "family": prototype.get("family"),
            "score": _round(score),
        }
        scores.append(row)
        if prototype["kind"] == "positive":
            family = str(prototype["family"])
            family_scores[family] = max(family_scores[family], score)
    positives = sorted(
        (row for row in scores if row["prototype_kind"] == "positive"),
        key=lambda row: (-row["score"], row["prototype_id"]),
    )
    hard_negatives = sorted(
        (row for row in scores if row["prototype_kind"] == "hard_negative"),
        key=lambda row: (-row["score"], row["prototype_id"]),
    )
    neutrals = sorted(
        (row for row in scores if row["prototype_kind"] == "neutral"),
        key=lambda row: (-row["score"], row["prototype_id"]),
    )
    ranked_families = sorted(family_scores.items(), key=lambda item: (-item[1], item[0]))
    family, family_score = ranked_families[0]
    second_family_score = ranked_families[1][1]
    top_positive = float(positives[0]["score"])
    top_hard_negative = float(hard_negatives[0]["score"])
    top_neutral = float(neutrals[0]["score"])
    support = sum(float(row["score"]) for row in positives[:3]) / min(3, len(positives))
    features = {
        "top_positive": top_positive,
        "positive_support": support,
        "positive_hard_negative_margin": top_positive - top_hard_negative,
        "positive_neutral_margin": top_positive - top_neutral,
        "family_margin": family_score - second_family_score,
    }
    # Retrieval evidence must always show the nearest positive,
    # hard-negative and neutral anchors; a global top-6 can otherwise be
    # monopolised by one kind and hide the actual decision margins.
    evidence = sorted(
        [*positives[:2], *hard_negatives[:2], *neutrals[:2]],
        key=lambda row: (-row["score"], row["prototype_id"]),
    )
    primary_family_positive = next(
        row for row in positives if row["family"] == family
    )
    if primary_family_positive not in evidence:
        evidence.append(primary_family_positive)
    return (
        {key: _round(value) for key, value in features.items()},
        family,
        {key: _round(value) for key, value in family_scores.items()},
        evidence,
    )


def _classify(
    features: Mapping[str, float], classifier: Mapping[str, Any]
) -> tuple[str, float, list[str]]:
    weights = classifier["weights"]
    logit = float(classifier["bias"]) + sum(
        float(weights[name]) * float(features[name])
        for name in classifier["feature_order"]
    )
    probability = _sigmoid(logit)
    threshold = classifier["decision_thresholds"]
    phn = float(features["positive_hard_negative_margin"])
    pn = float(features["positive_neutral_margin"])
    family_margin = float(features["family_margin"])
    if (
        probability >= float(threshold["core_probability"])
        and phn >= float(threshold["core_min_positive_hard_negative_margin"])
        and pn >= float(threshold["core_min_positive_neutral_margin"])
        and family_margin >= float(threshold["family_min_margin"])
    ):
        decision = "core"
    elif (
        probability >= float(threshold["adjacent_probability"])
        and phn >= float(threshold["adjacent_min_positive_hard_negative_margin"])
        and pn >= float(threshold["adjacent_min_positive_neutral_margin"])
    ):
        decision = "adjacent"
    elif probability <= float(threshold["ordinary_probability"]) or (
        phn <= 0 and pn <= 0
    ):
        decision = "ordinary"
    else:
        decision = "abstain"
    reasons = [f"semantic_{decision}"]
    if phn > 0:
        reasons.append("positive_over_hard_negative")
    else:
        reasons.append("hard_negative_not_cleared")
    if pn > 0:
        reasons.append("positive_over_neutral")
    else:
        reasons.append("neutral_not_cleared")
    if family_margin < float(threshold["family_min_margin"]):
        reasons.append("family_ambiguous")
    return decision, _round(probability), reasons


def _eligibility(event: Mapping[str, Any], as_of: date) -> tuple[bool, list[str]]:
    failures: list[str] = []
    title = str(event.get("title") or "").strip()
    if not title:
        failures.append("missing_title")
    if str(event.get("lifecycle_status") or "").strip().lower() != "active":
        failures.append("lifecycle_not_active")
    identity = str(event.get("identity_status") or "").strip().lower()
    if identity and identity != "canonical":
        failures.append("identity_not_canonical")
    if event.get("is_public") is False:
        failures.append("not_public")
    if any(event.get(field) is False for field in (
        "is_searchable", "searchable", "public_searchable"
    )):
        failures.append("not_searchable")
    publication_status = str(event.get("publication_status") or "").strip().lower()
    if event.get("cancelled") is True or publication_status in {
        "cancelled", "canceled", "deleted", "silent", "postponed", "merged"
    }:
        failures.append("publication_unavailable")
    if (
        event.get("silent") is True
        or event.get("is_silent") is True
        or event.get("postponed") is True
        or event.get("is_postponed") is True
        or event.get("merged_into_id")
    ):
        failures.append("publication_unavailable")
    eventness = str(event.get("eventness_status") or "").strip().lower()
    if eventness in {"non_event", "not_event", "rejected"} or event.get(
        "is_event"
    ) is False:
        failures.append("non_event")
    record_kind = str(
        event.get("record_kind")
        or event.get("content_kind")
        or event.get("event_kind")
        or ""
    ).strip().lower()
    if (
        record_kind in {"service", "work_hours", "working_hours", "non_event"}
        or event.get("is_service") is True
        or event.get("is_work_hours") is True
    ):
        failures.append("service_or_work_hours")
    semantic_text = " ".join(
        str(event.get(field) or "").strip()
        for field in (
            "summary",
            "short_description",
            "search_digest",
            "description",
            "description_html",
        )
    ).strip()
    # Structured fail-closed minimum only: no keyword-based semantic rewrite.
    # The actual meaning is still decided exclusively by the BGE classifier.
    if len(semantic_text) < 16 or len(f"{title} {semantic_text}".strip()) < 32:
        failures.append("insufficient_semantic_text")
    raw_date = str(event.get("end_date") or event.get("start_date") or "").strip()
    try:
        last_date = date.fromisoformat(raw_date[:10])
    except ValueError:
        failures.append("invalid_or_missing_date")
    else:
        if last_date < as_of:
            failures.append("past")
    failures = list(dict.fromkeys(failures))
    return not failures, failures


def _concept_ids(
    events: list[dict[str, Any]],
    event_vectors: Mapping[str, Any] | None = None,
) -> dict[int, tuple[str, str]]:
    """Return stable concept ids using explicit identity only.

    Hierarchy: curated concept -> canonical root -> explicit series -> exported
    occurrence family -> reciprocal explicit-link component -> conservative
    BGE presentation cluster -> stable presentation identity.

    The semantic cluster is used only to deduplicate this presentation surface;
    it never creates or mutates an occurrence family in canonical event data.
    """

    by_id = {int(row.get("id") or row.get("event_id")): row for row in events}
    parent = {event_id: event_id for event_id in by_id}

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    links: dict[int, set[int]] = {}
    for event_id, event in by_id.items():
        explicit_ids = {
            int(value)
            for value in (event.get("other_date_ids") or [])
            if str(value).isdigit() and int(value) in by_id
        }
        explicit_ids.update(
            {
            int(value)
            for value in (event.get("occurrence_member_ids") or [])
            if str(value).isdigit() and int(value) in by_id
            }
        )
        explicit_ids.update(
            {
                int(value)
                for value in (event.get("linked_event_ids") or [])
                if str(value).isdigit() and int(value) in by_id
            }
        )
        explicit_ids.discard(event_id)
        links[event_id] = explicit_ids
    for event_id, others in links.items():
        for other in others:
            if event_id in links.get(other, set()):
                union(event_id, other)

    def semantic_identity(event: Mapping[str, Any]) -> str:
        def normalise(value: Any) -> str:
            text = unicodedata.normalize("NFKC", str(value or "")).casefold()
            return " ".join(re.findall(r"[\w]+", text, flags=re.UNICODE))

        # Calendar, admission, price, description and mutable presentation
        # fields are intentionally absent. Repeated rows with the same canonical
        # identity cluster without mutating or writing back to Event.
        identity = {
            "title": normalise(event.get("title")),
            "event_type": normalise(event.get("event_type")),
            "venue": normalise(
                event.get("venue_name") or event.get("location_name")
            ),
            "city": normalise(event.get("city")),
        }
        return f"concept:{stable_hash(identity)[:24]}"
    output: dict[int, tuple[str, str]] = {}
    fallback_ids: list[int] = []
    for event_id, event in by_id.items():
        curated = str(event.get("canonical_concept_id") or "").strip()
        root = str(
            event.get("canonical_root_event_id")
            or event.get("root_event_id")
            or ""
        ).strip()
        series = str(
            event.get("series_id")
            or event.get("event_series_id")
            or event.get("occurrence_family_id")
            or ""
        ).strip()
        if curated:
            safe = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", curated).strip("-")[:80]
            output[event_id] = (
                f"curated:{safe or stable_hash(curated)[:20]}",
                "canonical_concept_id",
            )
        elif root:
            output[event_id] = (
                f"root:{stable_hash(root)[:20]}",
                "canonical_root_event_id",
            )
        elif series:
            output[event_id] = (
                f"series:{stable_hash(series)[:20]}",
                "series_id",
            )
        elif find(event_id) != event_id or any(
            find(other) == find(event_id) and other != event_id for other in by_id
        ):
            output[event_id] = (f"occurrence:{find(event_id)}", "explicit_occurrence")
        else:
            fallback_ids.append(event_id)

    # Conservative BGE-only presentation clustering for near-identical repeated
    # entries that lack explicit identity. Exact stable identity still works
    # when vectors are absent. Venue/type equality plus a very high cosine and
    # title-token overlap prevents broad topic-level merging.
    presentation_parent = {event_id: event_id for event_id in fallback_ids}

    def presentation_find(value: int) -> int:
        while presentation_parent[value] != value:
            presentation_parent[value] = presentation_parent[presentation_parent[value]]
            value = presentation_parent[value]
        return value

    def presentation_union(left: int, right: int) -> None:
        left_root, right_root = presentation_find(left), presentation_find(right)
        if left_root != right_root:
            presentation_parent[max(left_root, right_root)] = min(left_root, right_root)

    def words(value: Any) -> set[str]:
        text = unicodedata.normalize("NFKC", str(value or "")).casefold()
        return set(re.findall(r"[\w]+", text, flags=re.UNICODE))

    presentation_vectors: dict[int, list[float]] = {}

    def presentation_vector(event_id: int) -> list[float] | None:
        if not isinstance(event_vectors, Mapping):
            return None
        if event_id in presentation_vectors:
            return presentation_vectors[event_id]
        try:
            vector, _ = _vector_row(
                event_vectors, str(event_id), expected_dim=EMBEDDING_DIM
            )
        except (KeyError, TypeError, ValueError):
            return None
        presentation_vectors[event_id] = vector
        return vector

    def cosine(left: int, right: int) -> float:
        left_vector = presentation_vector(left)
        right_vector = presentation_vector(right)
        if left_vector is None or right_vector is None:
            return -1.0
        return sum(a * b for a, b in zip(left_vector, right_vector))

    for index, left_id in enumerate(fallback_ids):
        left = by_id[left_id]
        left_words = words(left.get("title"))
        for right_id in fallback_ids[index + 1:]:
            right = by_id[right_id]
            if (
                str(left.get("event_type") or "").casefold()
                != str(right.get("event_type") or "").casefold()
                or str(left.get("venue_name") or left.get("location_name") or "").casefold()
                != str(right.get("venue_name") or right.get("location_name") or "").casefold()
            ):
                continue
            right_words = words(right.get("title"))
            overlap = (
                len(left_words & right_words) / len(left_words | right_words)
                if left_words | right_words
                else 0.0
            )
            if overlap >= 0.84 and cosine(left_id, right_id) >= 0.985:
                presentation_union(left_id, right_id)

    presentation_counts: dict[int, int] = {}
    for event_id in fallback_ids:
        root_id = presentation_find(event_id)
        presentation_counts[root_id] = presentation_counts.get(root_id, 0) + 1
    for event_id in fallback_ids:
        root_id = presentation_find(event_id)
        if presentation_counts[root_id] > 1:
            output[event_id] = (
                f"presentation:{root_id}",
                "bge_presentation_cluster",
            )
        else:
            output[event_id] = (
                semantic_identity(by_id[event_id]),
                "stable_semantic_identity",
            )
    return output


def evaluate_unusual_quality_fixture(
    fixture: Mapping[str, Any],
    artifact: Mapping[str, Any],
    prototype_bank: Mapping[str, Any],
    classifier: Mapping[str, Any],
) -> dict[str, Any]:
    """Measure the frozen semantic gate on already encoded real canary rows.

    The fixture is editorial ground truth, not training input. Required
    ``cases[]`` fields are ``event_id``, ``label`` (``positive``,
    ``hard_negative`` or ``non_event``), ``concept_id``, ``eligible`` and
    optionally ``expected_family``/``frozen_tier``. The artifact must contain
    the exact event ids and prototype vectors and should declare
    ``metadata.build.evidence_kind=real_bge_canary``. Missing samples or frozen
    rebuild tiers remain missing; the downstream gate therefore stays shadow.
    """

    metadata = artifact.get("metadata")
    event_vectors = artifact.get("event_vectors")
    prototype_vectors = artifact.get("prototype_vectors")
    cases = fixture.get("cases")
    if (
        not isinstance(metadata, Mapping)
        or not isinstance(event_vectors, Mapping)
        or not isinstance(prototype_vectors, Mapping)
        or not isinstance(cases, list)
    ):
        raise ValueError("quality fixture or vector artifact has an invalid shape")
    bank_hash = stable_hash(prototype_bank)
    classifier_hash = stable_hash(classifier)
    expected_contract = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "encoder_contract": ENCODER_CONTRACT,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_version": DOCUMENT_VERSION,
        "vector_normalization": VECTOR_NORMALIZATION,
        "prototype_bank_sha256": bank_hash,
        "classifier_sha256": classifier_hash,
    }
    mismatches = [
        key for key, expected in expected_contract.items() if metadata.get(key) != expected
    ]
    unhashed_metadata = dict(metadata)
    declared_artifact_hash = unhashed_metadata.pop("artifact_sha256", None)
    actual_artifact_hash = stable_hash(
        {
            "metadata": unhashed_metadata,
            "event_vectors": event_vectors,
            "prototype_vectors": prototype_vectors,
        }
    )
    if declared_artifact_hash != actual_artifact_hash:
        mismatches.append("artifact_sha256")
    if mismatches:
        raise ValueError(
            "quality evaluation vector contract mismatch: "
            + ", ".join(sorted(set(mismatches)))
        )
    for prototype in prototype_bank["prototypes"]:
        prototype_id = str(prototype["id"])
        _, text_hash = _vector_row(
            prototype_vectors, prototype_id, expected_dim=EMBEDDING_DIM
        )
        expected_text_hash = hashlib.sha256(
            str(prototype["text"]).encode("utf-8")
        ).hexdigest()
        if text_hash != expected_text_hash:
            raise ValueError(f"prototype {prototype_id} text_hash mismatch")

    def run_once() -> list[dict[str, Any]]:
        predictions: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        for case in cases:
            if not isinstance(case, Mapping):
                raise ValueError("quality fixture cases must be objects")
            event_id = int(case.get("event_id") or case.get("id") or 0)
            label = str(case.get("label") or "")
            if event_id <= 0 or event_id in seen_ids:
                raise ValueError("quality fixture event ids must be positive and unique")
            if label not in {"positive", "hard_negative", "non_event"}:
                raise ValueError(f"quality fixture event {event_id} has invalid label")
            seen_ids.add(event_id)
            vector, vector_text_hash = _vector_row(
                event_vectors, str(event_id), expected_dim=EMBEDDING_DIM
            )
            frozen_content_hash = str(case.get("content_hash") or "")
            if frozen_content_hash and frozen_content_hash != vector_text_hash:
                raise ValueError(
                    f"quality fixture event {event_id} content_hash mismatch"
                )
            features, family, family_scores, evidence = _semantic_features(
                vector, prototype_vectors, prototype_bank
            )
            decision, confidence, reason_codes = _classify(features, classifier)
            predictions.append(
                {
                    "event_id": event_id,
                    "label": label,
                    "expected_family": case.get("expected_family"),
                    "concept_id": str(
                        case.get("concept_id") or f"fixture:{event_id}"
                    ),
                    "eligible": case.get("eligible") is True,
                    "decision": decision,
                    "tier": PUBLIC_TIER[decision],
                    "calibrated_confidence": confidence,
                    "family": family,
                    "family_scores": family_scores,
                    "reason_codes": reason_codes,
                    "prototype_evidence": evidence,
                    "frozen_tier": case.get("frozen_tier"),
                }
            )
        return predictions

    first = run_once()
    second = run_once()
    deterministic = first == second
    editorial = [row for row in first if row["label"] != "non_event"]
    predicted_unusual = [
        row for row in first if row["decision"] in {"core", "adjacent"}
    ]
    editorial_top = sorted(
        predicted_unusual,
        key=lambda row: (
            -float(row["calibrated_confidence"]),
            int(row["event_id"]),
        ),
    )[:20]
    precision = (
        sum(row["label"] == "positive" for row in editorial_top)
        / len(editorial_top)
        if editorial_top
        else None
    )
    positives = [row for row in first if row["label"] == "positive"]
    hard_negatives = [row for row in first if row["label"] == "hard_negative"]
    recall = (
        sum(row["decision"] in {"core", "adjacent"} for row in positives)
        / len(positives)
        if positives
        else None
    )
    hard_negative_fpr = (
        sum(
            row["decision"] in {"core", "adjacent"}
            for row in hard_negatives
        )
        / len(hard_negatives)
        if hard_negatives
        else None
    )
    published = [row for row in predicted_unusual if row["eligible"]]
    ranked_published = sorted(
        published,
        key=lambda row: (
            -float(row["calibrated_confidence"]),
            int(row["event_id"]),
        ),
    )[:20]
    duplicate_concepts = len(ranked_published) - len(
        {row["concept_id"] for row in ranked_published}
    )
    frozen_rows = [
        row
        for row in first
        if row["frozen_tier"] in set(PUBLIC_TIER.values())
    ]
    flip_rate = (
        sum(row["tier"] != row["frozen_tier"] for row in frozen_rows)
        / len(frozen_rows)
        if len(frozen_rows) == len(first) and frozen_rows
        else None
    )
    build = metadata.get("build")
    evidence_kind = (
        "real_bge_canary"
        if isinstance(build, Mapping)
        and build.get("evidence_kind") == "real_bge_canary"
        else "non_production_probe"
    )
    return {
        "schema_version": "unusual-event-quality-evaluation-v1",
        "evidence_kind": evidence_kind,
        "fixture_schema_version": fixture.get("schema_version"),
        "fixture_sha256": stable_hash(fixture),
        "artifact_sha256": declared_artifact_hash,
        "prototype_bank_sha256": bank_hash,
        "classifier_sha256": classifier_hash,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_version": DOCUMENT_VERSION,
        "editorial_precision_at_20": _round(precision) if precision is not None else None,
        "editorial_sample_size": len(editorial),
        "editorial_ranked_count": len(editorial_top),
        "hard_negative_false_positive_rate": (
            _round(hard_negative_fpr)
            if hard_negative_fpr is not None
            else None
        ),
        "hard_negative_sample_size": len(hard_negatives),
        "confirmed_unusual_recall": _round(recall) if recall is not None else None,
        "confirmed_unusual_sample_size": len(positives),
        "duplicate_concepts_top20": duplicate_concepts,
        "identical_rebuild_flip_rate": (
            _round(flip_rate) if flip_rate is not None else None
        ),
        "deterministic_repeat_exact": deterministic,
        "single_vector_contract": True,
        "ineligible_publication_count": sum(
            not row["eligible"] for row in predicted_unusual
        ),
        "family_diversity_top20": len(
            {str(row["family"]) for row in ranked_published}
        ),
        "predictions": first,
    }


def _quality_gate(
    evaluation: Any,
    classifier: Mapping[str, Any],
    *,
    vector_metadata: Mapping[str, Any],
    bank_hash: str,
    classifier_hash: str,
) -> dict[str, Any]:
    gate = classifier["quality_gate"]
    required_contract = {
        "artifact_sha256": vector_metadata.get("artifact_sha256"),
        "prototype_bank_sha256": bank_hash,
        "classifier_sha256": classifier_hash,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_version": DOCUMENT_VERSION,
    }
    checks: dict[str, bool] = {}
    reasons: list[str] = []
    if not isinstance(evaluation, Mapping):
        return {
            "approval_status": "not_approved",
            "mode": "shadow",
            "checks": {},
            "reasons": ["real BGE canary quality_evaluation is missing"],
            "thresholds": gate,
            "observed": None,
        }
    if evaluation.get("evidence_kind") != "real_bge_canary":
        reasons.append("quality evidence is not a real_bge_canary")
    for key, expected in required_contract.items():
        checks[f"contract.{key}"] = evaluation.get(key) == expected
    numeric_checks = (
        ("editorial_sample", "editorial_sample_size", ">=", gate["minimum_editorial_sample"]),
        ("hard_negative_sample", "hard_negative_sample_size", ">=", gate["minimum_hard_negative_sample"]),
        (
            "confirmed_unusual_sample",
            "confirmed_unusual_sample_size",
            ">=",
            gate["minimum_confirmed_unusual_sample"],
        ),
        ("hard_negative_fpr", "hard_negative_false_positive_rate", "<=", gate["hard_negative_fpr_max"]),
        ("confirmed_unusual_recall", "confirmed_unusual_recall", ">=", gate["confirmed_unusual_recall_min"]),
        ("duplicates_top20", "duplicate_concepts_top20", "<=", gate["duplicate_concepts_top20_max"]),
        ("family_diversity_top20", "family_diversity_top20", ">=", gate["minimum_family_diversity_top20"]),
        (
            "ineligible_publication_count",
            "ineligible_publication_count",
            "<=",
            gate["ineligible_publication_count_max"],
        ),
    )
    for name, field, operator, threshold in numeric_checks:
        value = evaluation.get(field)
        valid_number = isinstance(value, (int, float)) and math.isfinite(float(value))
        checks[name] = bool(
            valid_number
            and (
                float(value) >= float(threshold)
                if operator == ">="
                else float(value) <= float(threshold)
            )
        )
    flip = evaluation.get("identical_rebuild_flip_rate")
    checks["identical_rebuild_flip_rate"] = bool(
        isinstance(flip, (int, float))
        and math.isfinite(float(flip))
        and float(flip) < float(gate["identical_rebuild_flip_rate_max_exclusive"])
    )
    precision = evaluation.get("editorial_precision_at_20")
    checks["editorial_precision_at_20"] = bool(
        isinstance(precision, (int, float))
        and math.isfinite(float(precision))
        and (
            float(precision) >= float(gate["editorial_precision_at_20_min"])
            or float(precision)
            >= float(gate["frozen_reference_precision_at_20"])
            - float(gate["frozen_reference_max_drop"])
        )
    )
    checks["deterministic_repeat_exact"] = (
        evaluation.get("deterministic_repeat_exact") is True
    )
    checks["single_vector_contract"] = (
        evaluation.get("single_vector_contract") is True
        and vector_metadata.get("model_id") == MODEL_ID
    )
    failed = sorted(name for name, passed in checks.items() if not passed)
    reasons.extend(f"failed:{name}" for name in failed)
    approved = not reasons
    return {
        "approval_status": "approved" if approved else "not_approved",
        "mode": "approved" if approved else "shadow",
        "checks": checks,
        "reasons": reasons,
        "thresholds": gate,
        "observed": dict(evaluation),
    }


def _empty_result(
    *,
    errors: list[str],
    build_metadata: Mapping[str, Any],
    bank_hash: str,
    classifier_hash: str,
) -> dict[str, Any]:
    metrics = {
        "status": "blocked",
        "boundary_errors": errors,
        "quality_gate": {
            "approval_status": "not_approved",
            "mode": "shadow",
            "reasons": ["vector boundary validation failed"],
        },
        "events_total": 0,
        "published_count": 0,
        "provider_calls": 0,
    }
    return {
        "manifest": {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "status": "blocked",
            "evaluation_approval_status": "not_approved",
            "items": [],
            "shadow_items": [],
            "migration": {"notify": False, "mode": "shadow"},
            "build": dict(build_metadata),
            "prototype_bank_sha256": bank_hash,
            "classifier_sha256": classifier_hash,
            "errors": errors,
        },
        "cache": {
            "schema_version": CACHE_SCHEMA_VERSION,
            "status": "blocked",
            "records": {},
        },
        "metrics": metrics,
    }


def score_unusual_manifest(
    events: Iterable[Mapping[str, Any]],
    event_vectors: Mapping[str, Any],
    prototype_vectors: Mapping[str, Any],
    vector_metadata: Mapping[str, Any],
    previous_cache: Mapping[str, Any] | None = None,
    build_metadata: Mapping[str, Any] | None = None,
    *,
    prototype_bank: Mapping[str, Any] | None = None,
    classifier: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Score, gate and concept-deduplicate unusual event candidates.

    Return shape is always ``{"manifest": ..., "cache": ..., "metrics": ...}``.
    Until a complete real-BGE quality evaluation passes the frozen thresholds,
    ``manifest.items`` is empty and inspectable decisions live only in
    ``manifest.shadow_items``.
    """

    rows = [dict(event) for event in events]
    bank = dict(prototype_bank or load_unusual_prototype_bank())
    head = dict(classifier or load_unusual_classifier())
    build = dict(build_metadata or {})
    bank_hash = stable_hash(bank)
    classifier_hash = stable_hash(head)
    boundary_errors, documents = _validate_boundary(
        rows,
        event_vectors,
        prototype_vectors,
        vector_metadata,
        bank,
        head,
        build,
    )
    if boundary_errors:
        return _empty_result(
            errors=boundary_errors,
            build_metadata=build,
            bank_hash=bank_hash,
            classifier_hash=classifier_hash,
        )
    as_of = date.fromisoformat(str(build["as_of_date"]))
    concepts = _concept_ids(rows, event_vectors)
    cache_contract = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "scorer_schema_version": SCORER_SCHEMA_VERSION,
        # The artifact hash changes when any event vector or build receipt
        # changes. Per-event input hashes below are the incremental boundary;
        # keeping the whole artifact hash here would invalidate every decision.
        "model_id": vector_metadata["model_id"],
        "model_revision": vector_metadata["model_revision"],
        "embedding_dim": vector_metadata["embedding_dim"],
        "document_version": vector_metadata["document_version"],
        "prototype_bank_sha256": bank_hash,
        "classifier_sha256": classifier_hash,
        "as_of_date": as_of.isoformat(),
    }
    cache_valid = bool(
        isinstance(previous_cache, Mapping)
        and all(previous_cache.get(key) == value for key, value in cache_contract.items())
        and isinstance(previous_cache.get("records"), Mapping)
    )
    prior_records = previous_cache.get("records", {}) if cache_valid else {}
    new_records: dict[str, Any] = {}
    decisions: list[dict[str, Any]] = []
    cache_hits = 0
    by_id = {int(row.get("id") or row.get("event_id")): row for row in rows}
    for event_id in sorted(by_id):
        event = by_id[event_id]
        vector, _ = _vector_row(
            event_vectors, str(event_id), expected_dim=EMBEDDING_DIM
        )
        concept_id, concept_source = concepts[event_id]
        input_hash = stable_hash(
            {
                "document_text_hash": documents[event_id]["text_hash"],
                "vector_sha256": stable_hash(vector),
                "eligibility": {
                    key: event.get(key)
                    for key in (
                        "lifecycle_status",
                        "identity_status",
                        "is_public",
                        "is_searchable",
                        "searchable",
                        "public_searchable",
                        "cancelled",
                        "silent",
                        "is_silent",
                        "postponed",
                        "is_postponed",
                        "merged_into_id",
                        "publication_status",
                        "eventness_status",
                        "is_event",
                        "record_kind",
                        "content_kind",
                        "event_kind",
                        "is_service",
                        "is_work_hours",
                        "start_date",
                        "end_date",
                    )
                },
                "concept_id": concept_id,
            }
        )
        cached = prior_records.get(str(event_id)) if cache_valid else None
        if isinstance(cached, Mapping) and cached.get("input_hash") == input_hash:
            decision_row = dict(cached["decision"])
            cache_hits += 1
        else:
            eligible, eligibility_failures = _eligibility(event, as_of)
            features, family, family_scores, prototype_evidence = _semantic_features(
                vector, prototype_vectors, bank
            )
            decision, probability, reason_codes = _classify(features, head)
            if not eligible:
                decision = "abstain"
                reason_codes = ["hard_eligibility_gate", *eligibility_failures]
            primary_prototype = next(
                (
                    row
                    for row in prototype_evidence
                    if row["prototype_kind"] == "positive"
                    and row["family"] == family
                ),
                None,
            )
            decision_row = {
                "event_id": event_id,
                "source_prod_id": event.get("source_prod_id"),
                "slug": event.get("slug"),
                "title": event.get("title"),
                "start_date": event.get("start_date"),
                "end_date": event.get("end_date"),
                "lifecycle_status": event.get("lifecycle_status"),
                "concept_id": concept_id,
                "concept_id_source": concept_source,
                "family": family,
                "families": [
                    {"id": family_id, "score": score}
                    for family_id, score in sorted(
                        family_scores.items(),
                        key=lambda item: (-item[1], item[0]),
                    )[:5]
                ],
                "family_scores": family_scores,
                "decision": decision,
                "tier": PUBLIC_TIER[decision],
                "score": probability,
                "unusual_score": probability,
                "calibrated_confidence": probability,
                "confidence_status": "shadow",
                "eligible": eligible,
                "eligibility": {
                    "eligible": eligible,
                    "failures": eligibility_failures,
                    "as_of_date": as_of.isoformat(),
                },
                "eligibility_failures": eligibility_failures,
                "reason_codes": reason_codes,
                "reason": {
                    "code": reason_codes[0],
                    "family": family,
                    "prototype_id": (
                        primary_prototype["prototype_id"]
                        if primary_prototype is not None
                        else None
                    ),
                    "prototype_score": (
                        primary_prototype["score"]
                        if primary_prototype is not None
                        else None
                    ),
                },
                "features": features,
                "prototype_evidence": prototype_evidence,
                "policy_version": POLICY_VERSION,
                "model_id": MODEL_ID,
                "model_revision": MODEL_REVISION,
                "embedding_dim": EMBEDDING_DIM,
                "document_version": DOCUMENT_VERSION,
                "prototype_bank_version": bank["schema_version"],
                "prototype_bank_sha256": bank_hash,
                "classifier_version": head["schema_version"],
                "classifier_kind": head["classifier_kind"],
                "classifier_sha256": classifier_hash,
                "input_hash": input_hash,
                "content_hash": documents[event_id]["text_hash"],
                "notify": False,
            }
        new_records[str(event_id)] = {
            "input_hash": input_hash,
            "decision": decision_row,
        }
        decisions.append(decision_row)
    horizon_30 = date.fromordinal(as_of.toordinal() + 30)

    def rank_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        start_raw = str(by_id[int(row["event_id"])].get("start_date") or "")
        try:
            start = date.fromisoformat(start_raw[:10])
        except ValueError:
            start = date.max
        return (
            DECISION_ORDER[str(row["decision"])],
            0 if start <= horizon_30 else 1,
            -float(row["score"]),
            start_raw,
            int(row["event_id"]),
        )

    decisions.sort(key=rank_key)
    selected: list[dict[str, Any]] = []
    duplicate_rows: list[dict[str, Any]] = []
    seen_concepts: set[str] = set()
    candidates: list[dict[str, Any]] = []
    for row in decisions:
        if row["decision"] not in {"core", "adjacent"} or not row["eligible"]:
            continue
        if row["concept_id"] in seen_concepts:
            duplicate_rows.append(
                {
                    "event_id": row["event_id"],
                    "concept_id": row["concept_id"],
                    "reason": "concept_duplicate",
                }
            )
            continue
        seen_concepts.add(row["concept_id"])
        candidates.append(row)
    family_counts: dict[str, int] = {}
    venue_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    deferred: list[dict[str, Any]] = []
    for row in candidates:
        event = by_id[int(row["event_id"])]
        family = str(row.get("family") or "")
        venue = str(event.get("venue_id") or event.get("venue_name") or "").casefold()
        event_type = str(event.get("event_type") or "").casefold()
        if (
            family_counts.get(family, 0) >= 6
            or (venue and venue_counts.get(venue, 0) >= 4)
            or (event_type and type_counts.get(event_type, 0) >= 8)
        ):
            deferred.append(row)
            continue
        selected.append(row)
        family_counts[family] = family_counts.get(family, 0) + 1
        if venue:
            venue_counts[venue] = venue_counts.get(venue, 0) + 1
        if event_type:
            type_counts[event_type] = type_counts.get(event_type, 0) + 1
        if len(selected) >= 30:
            break
    if len(selected) < 30:
        selected.extend(deferred[: 30 - len(selected)])
    quality = _quality_gate(
        build.get("quality_evaluation"),
        head,
        vector_metadata=vector_metadata,
        bank_hash=bank_hash,
        classifier_hash=classifier_hash,
    )
    approved = quality["approval_status"] == "approved"
    published = selected if approved else []
    for row in decisions:
        row["confidence_status"] = "approved" if approved else "shadow"
    top20 = selected[:20]
    metrics = {
        "status": "approved" if approved else "shadow",
        "events_total": len(rows),
        "eligible_count": sum(bool(row["eligible"]) for row in decisions),
        "decision_counts": {
            PUBLIC_TIER[tier]: sum(row["decision"] == tier for row in decisions)
            for tier in ("core", "adjacent", "ordinary", "abstain")
        },
        "concept_candidates": len(selected),
        "concept_duplicates_removed": len(duplicate_rows),
        "candidate_family_diversity_top20": len(
            {str(row["family"]) for row in top20}
        ),
        "published_count": len(published),
        "cache_hits": cache_hits,
        "cache_misses": len(rows) - cache_hits,
        "provider_calls": 0,
        "quality_gate": quality,
    }
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "scorer_schema_version": SCORER_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "status": "approved" if approved else "shadow",
        "evaluation_approval_status": quality["approval_status"],
        "taxonomy_version": bank["taxonomy_version"],
        "families": bank["families"],
        "vector_contract": {
            key: vector_metadata[key]
            for key in (
                "encoder_contract",
                "model_id",
                "model_revision",
                "embedding_dim",
                "document_version",
                "vector_normalization",
                "artifact_sha256",
            )
        },
        "prototype_bank_sha256": bank_hash,
        "classifier_sha256": classifier_hash,
        "items": published,
        "shadow_items": decisions if not approved else [],
        "decisions": decisions,
        "candidate_items": selected,
        "duplicates": duplicate_rows,
        "quality_gate": quality,
        "migration": {
            "notify": False,
            "mode": "approved_silent" if approved else "shadow",
        },
        "build": build,
    }
    cache = {
        **cache_contract,
        "status": "ready",
        "records": new_records,
    }
    cache["cache_sha256"] = stable_hash(cache)
    return {"manifest": manifest, "cache": cache, "metrics": metrics}


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--vectors", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--output-cache", type=Path, required=True)
    parser.add_argument("--output-metrics", type=Path, required=True)
    parser.add_argument("--previous-cache", type=Path)
    parser.add_argument("--build-metadata", type=Path, required=True)
    parser.add_argument("--prototype-bank", type=Path, default=DEFAULT_PROTOTYPE_BANK)
    parser.add_argument("--classifier", type=Path, default=DEFAULT_CLASSIFIER)
    args = parser.parse_args(argv)
    event_payload = _read_json(args.events)
    events = event_payload.get("events", event_payload) if isinstance(event_payload, dict) else event_payload
    if not isinstance(events, list):
        raise ValueError("--events must contain a list or {events: [...]}")
    vectors = _read_json(args.vectors)
    result = score_unusual_manifest(
        events,
        vectors["event_vectors"],
        vectors["prototype_vectors"],
        vectors["metadata"],
        _read_json(args.previous_cache) if args.previous_cache else None,
        _read_json(args.build_metadata),
        prototype_bank=load_unusual_prototype_bank(args.prototype_bank),
        classifier=load_unusual_classifier(args.classifier),
    )
    for path, key in (
        (args.output_manifest, "manifest"),
        (args.output_cache, "cache"),
        (args.output_metrics, "metrics"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                result[key],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
    return 0 if result["manifest"]["status"] != "blocked" else 2


if __name__ == "__main__":
    raise SystemExit(main())
