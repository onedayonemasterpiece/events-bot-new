#!/usr/bin/env python3
"""Compact exporter-side adapters for static collection data preparation.

The module deliberately separates exact database facts from rebuildable BGE
candidate scores.  It never publishes a semantic label whose owner-reviewed
gold gate is missing, and it never treats a BGE audience candidate as an
audience fact.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import static_collection_batch as batch_module
import static_event_bge as bge_module
from static_place_org_registry import registry_hash, resolve_event_memberships

HERE = Path(__file__).resolve().parent
DEFAULT_POLICY_PATH = HERE / "static_collection_policy.v1.json"
DEFAULT_PROTOTYPES_PATH = HERE / "static_collection_prototypes.v1.json"
DEFAULT_UNUSUAL_INCIDENT_REGRESSIONS_PATH = (
    HERE / "unusual_event_incident_regressions.v1.json"
)


def load_object(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object")
    return value


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def merged_prototype_bank(
    unusual_bank: Mapping[str, Any],
    extension: Mapping[str, Any],
) -> dict[str, Any]:
    """Namespace the existing unusual bank and append new label prototypes."""

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in unusual_bank.get("prototypes") or []:
        if not isinstance(raw, Mapping):
            continue
        old_id = str(raw.get("id") or "").strip()
        text = str(raw.get("text") or "").strip()
        if not old_id or not text:
            continue
        prototype_id = f"unusual.{old_id}"
        rows.append({**dict(raw), "id": prototype_id, "text": text})
        seen.add(prototype_id)
    for raw in extension.get("prototypes") or []:
        if not isinstance(raw, Mapping):
            continue
        prototype_id = str(raw.get("id") or "").strip()
        text = str(raw.get("text") or "").strip()
        if not prototype_id or not text or prototype_id in seen:
            raise ValueError("collection prototype ids must be unique and non-empty")
        rows.append({**dict(raw), "id": prototype_id, "text": text})
        seen.add(prototype_id)
    if not rows:
        raise ValueError("collection prototype bank is empty")
    return {
        "schema_version": "static-collection-prototype-bank-v1",
        "document_kind": bge_module.COLLECTION_DOCUMENT_KIND,
        "document_version": bge_module.COLLECTION_DOCUMENT_VERSION,
        "sources": {
            "unusual_bank_sha256": stable_hash(unusual_bank),
            "extension_sha256": stable_hash(extension),
        },
        "prototypes": rows,
    }


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        raise ValueError("semantic vectors use different dimensions")
    return float(sum(float(a) * float(b) for a, b in zip(left, right)))


def score_semantic_candidates(
    artifact: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Return high-recall candidates; this function never grants publication."""

    events = artifact.get("event_vectors")
    prototypes = artifact.get("prototype_vectors")
    if not isinstance(events, Mapping) or not isinstance(prototypes, Mapping):
        raise TypeError("collection artifact vectors are missing")
    result: dict[str, dict[str, Any]] = {}
    for label, config in sorted((policy.get("labels") or {}).items()):
        if not isinstance(config, Mapping) or not str(config.get("strategy") or "").startswith(
            "semantic_bge"
        ):
            continue
        positive_prefix = str(config.get("positive_prefix") or "")
        negative_prefix = str(config.get("negative_prefix") or "")
        positive = [
            (str(prototype_id), row["vector"])
            for prototype_id, row in prototypes.items()
            if str(prototype_id).startswith(positive_prefix) and isinstance(row, Mapping)
        ]
        negative = [
            (str(prototype_id), row["vector"])
            for prototype_id, row in prototypes.items()
            if str(prototype_id).startswith(negative_prefix) and isinstance(row, Mapping)
        ]
        if not positive or not negative:
            result[str(label)] = {
                "item_ids": [],
                "scores": {},
                "failure_codes": ["prototype_family_incomplete"],
            }
            continue
        minimum = float(config.get("minimum_positive_similarity") or 0.0)
        margin_minimum = float(config.get("minimum_margin") or 0.0)
        bank_by_id = {
            str(row.get("id") or ""): row
            for row in (policy.get("_prototype_rows") or [])
            if isinstance(row, Mapping)
        }
        # ``merged_prototype_bank`` namespaces the canonical Unusual anchors.
        # The optional rows are injected by the caller so this adapter can
        # expose explainable family evidence without ever invoking an encoder.
        neutral = [
            (str(prototype_id), row["vector"])
            for prototype_id, row in prototypes.items()
            if str(prototype_id).startswith("unusual.neutral.")
            and isinstance(row, Mapping)
        ] if str(label) == "unusual" else []
        scores: dict[int, dict[str, Any]] = {}
        all_scores: dict[int, dict[str, Any]] = {}
        for raw_event_id, row in events.items():
            if not isinstance(row, Mapping):
                continue
            try:
                event_id = int(raw_event_id)
            except (TypeError, ValueError):
                continue
            vector = row.get("vector")
            if not isinstance(vector, list):
                continue
            positive_ranked = sorted(
                ((_dot(vector, candidate), prototype_id) for prototype_id, candidate in positive),
                key=lambda item: (-item[0], item[1]),
            )
            negative_ranked = sorted(
                ((_dot(vector, candidate), prototype_id) for prototype_id, candidate in negative),
                key=lambda item: (-item[0], item[1]),
            )
            positive_score, positive_id = positive_ranked[0]
            negative_score, negative_id = negative_ranked[0]
            margin = positive_score - negative_score
            row_score: dict[str, Any] = {
                "positive": round(positive_score, 6),
                "negative": round(negative_score, 6),
                "margin": round(margin, 6),
                "top_positive_prototype_id": positive_id,
                "top_hard_negative_prototype_id": negative_id,
            }
            if neutral:
                neutral_ranked = sorted(
                    ((_dot(vector, candidate), prototype_id) for prototype_id, candidate in neutral),
                    key=lambda item: (-item[0], item[1]),
                )
                neutral_score, neutral_id = neutral_ranked[0]
                row_score.update(
                    {
                        "neutral": round(neutral_score, 6),
                        "positive_neutral_margin": round(positive_score - neutral_score, 6),
                        "top_neutral_prototype_id": neutral_id,
                    }
                )
                family_scores: dict[str, float] = {}
                for score, prototype_id in positive_ranked:
                    original_id = prototype_id.removeprefix("unusual.")
                    prototype = bank_by_id.get(original_id) or {}
                    family = str(prototype.get("family") or "").strip()
                    if family:
                        family_scores[family] = max(family_scores.get(family, -1.0), score)
                family_top3 = sorted(
                    family_scores.items(), key=lambda item: (-item[1], item[0])
                )[:3]
                row_score["family"] = family_top3[0][0] if family_top3 else "unknown"
                row_score["family_margin"] = round(
                    family_top3[0][1] - family_top3[1][1], 6
                ) if len(family_top3) >= 2 else None
                row_score["family_top3"] = [
                    {"id": family, "score": round(score, 6)}
                    for family, score in family_top3
                ]
            all_scores[event_id] = row_score
            if positive_score >= minimum and margin >= margin_minimum:
                scores[event_id] = row_score
        result[str(label)] = {
            "item_ids": sorted(scores),
            "scores": scores,
            "all_scores": all_scores,
            "failure_codes": [],
        }
    return result


def _topics(event: Mapping[str, Any]) -> set[str]:
    return {
        str(value or "").strip().upper()
        for value in (event.get("topics") or [])
        if str(value or "").strip()
    }


def _decisions(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def build_exact_label_ids(
    events: Sequence[Mapping[str, Any]],
    *,
    collection_decisions_by_id: Mapping[int, Any],
    theatre_event_ids: Iterable[int],
    facts_policy_version: str,
) -> dict[str, list[int]]:
    labels: dict[str, list[int]] = defaultdict(list)
    theatre_ids = {int(value) for value in theatre_event_ids}
    popular: list[tuple[float, int]] = []
    for event in events:
        event_id = int(event["id"])
        topics = _topics(event)
        event_type = str(event.get("event_type") or "").strip().casefold()
        ticket = event.get("ticket") if isinstance(event.get("ticket"), Mapping) else {}
        if ticket.get("is_free") is True:
            labels["free"].append(event_id)
        if event_id in theatre_ids:
            labels["theatre"].append(event_id)
        if "PERFORMANCES" in topics or event_type in {"спектакль", "театр"}:
            labels["performances"].append(event_id)
        if "EXHIBITIONS" in topics or event_type == "выставка":
            labels["exhibitions"].append(event_id)
        if "SCIENCE_POP" in topics:
            labels["science_pop"].append(event_id)
        score = float(event.get("popularity_signal_score") or 0.0)
        if score > 0:
            popular.append((score, event_id))

        decisions = _decisions(collection_decisions_by_id.get(event_id))
        audience = decisions.get("audience_decision")
        if (
            isinstance(audience, Mapping)
            and audience.get("policy_version") == facts_policy_version
            and audience.get("value") in {"kids", "family"}
        ):
            labels["kids"].append(event_id)
        for person in decisions.get("people_appearances") or []:
            if (
                not isinstance(person, Mapping)
                or person.get("policy_version") != facts_policy_version
                or person.get("appearance") != "confirmed"
            ):
                continue
            if person.get("origin_scope") == "russia_nonlocal":
                labels["guests_russia"].append(event_id)
            elif person.get("origin_scope") == "foreign":
                labels["guests_foreign"].append(event_id)
    labels["popular"] = [event_id for _score, event_id in sorted(popular, reverse=True)[:30]]
    for key in (
        "free",
        "kids",
        "theatre",
        "performances",
        "exhibitions",
        "science_pop",
        "popular",
        "guests_russia",
        "guests_foreign",
    ):
        labels[key] = sorted(set(labels.get(key, [])))
    return dict(labels)


def build_registry_projection(
    events: Sequence[Mapping[str, Any]],
    *,
    source_records_by_id: Mapping[int, Sequence[Mapping[str, Any]]],
    registry: Mapping[str, Any],
    generated_at: str,
    catalog_hash: str,
) -> tuple[dict[str, Any], set[int]]:
    """Resolve eight theatres and six venue schedules from exact evidence."""

    organization_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    venue_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    theatre_event_ids: set[int] = set()
    for event in events:
        event_id = int(event["id"])
        resolver_event = {
            **dict(event),
            "location_name": event.get("venue_name"),
            "location_address": event.get("address"),
            "source_records": [dict(row) for row in source_records_by_id.get(event_id, ())],
        }
        memberships = resolve_event_memberships(resolver_event, registry)
        for membership in memberships.get("theatre_memberships") or []:
            theatre_event_ids.add(event_id)
            organization_events[str(membership["entity_id"])].append(
                {"event_id": event_id, "reasons": membership.get("reasons") or []}
            )
        for membership in memberships.get("venue_memberships") or []:
            venue_events[str(membership["entity_id"])].append(
                {"event": event, "reasons": membership.get("reasons") or []}
            )

    theatre_organizations: list[dict[str, Any]] = []
    venues: list[dict[str, Any]] = []
    for entity in registry.get("entities") or []:
        if not isinstance(entity, Mapping):
            continue
        entity_id = str(entity.get("id") or "")
        flags = entity.get("flags") if isinstance(entity.get("flags"), Mapping) else {}
        if flags.get("official_theatre"):
            memberships = sorted(
                organization_events.get(entity_id, []), key=lambda row: int(row["event_id"])
            )
            theatre_organizations.append(
                {
                    "entity_id": entity_id,
                    "slug": entity.get("slug"),
                    "name": entity.get("canonicalName"),
                    "medallion_slug": entity.get("medallionSlug"),
                    "event_count": len(memberships),
                    "event_memberships": memberships,
                }
            )
        if not flags.get("venue_page_candidate"):
            continue
        rows = venue_events.get(entity_id, [])
        occurrences: list[dict[str, Any]] = []
        family_counts: Counter[str] = Counter()
        for row in rows:
            event = row["event"]
            normalized_title = re.sub(
                r"[^0-9a-zа-яё]+", " ", str(event.get("title") or "").casefold()
            ).strip()
            family_id = "title-" + hashlib.sha256(normalized_title.encode("utf-8")).hexdigest()[:16]
            family_counts[family_id] += 1
            occurrences.append(
                {
                    "event_id": int(event["id"]),
                    "family_id": family_id,
                    "start_date": event.get("start_date"),
                    "start_time": event.get("start_time"),
                    "reasons": row.get("reasons") or [],
                }
            )
        occurrences.sort(
            key=lambda row: (str(row.get("start_date") or ""), str(row.get("start_time") or ""), row["event_id"])
        )
        venue = entity.get("canonicalVenue") if isinstance(entity.get("canonicalVenue"), Mapping) else {}
        venues.append(
            {
                "entity_id": entity_id,
                "slug": entity.get("slug"),
                "name": entity.get("canonicalName"),
                "medallion_slug": entity.get("medallionSlug"),
                "venue": dict(venue),
                "status": "ready" if occurrences else "review_empty",
                "event_count": len(occurrences),
                "family_count": len(family_counts),
                "date_count": len({row.get("start_date") for row in occurrences if row.get("start_date")}),
                "occurrences": occurrences,
            }
        )
    payload = {
        "schema_version": "venue-pages-v1",
        "generated_at": generated_at,
        "catalog_hash": catalog_hash,
        "registry_hash": registry_hash(registry),
        "theatre_organizations": sorted(theatre_organizations, key=lambda row: str(row["entity_id"])),
        "venues": sorted(venues, key=lambda row: str(row["entity_id"])),
    }
    payload["manifest_sha256"] = stable_hash(payload)
    return payload, theatre_event_ids


def build_collection_batch_payload(
    *,
    events: Sequence[Mapping[str, Any]],
    collection_decisions_by_id: Mapping[int, Any],
    theatre_event_ids: Iterable[int],
    semantic_candidates: Mapping[str, Mapping[str, Any]],
    artifact: Mapping[str, Any],
    policy: Mapping[str, Any],
    catalog_hash: str,
    generated_at: str,
    snapshot: Mapping[str, Any],
    registry_sha256: str,
) -> dict[str, Any]:
    metadata = artifact.get("metadata") if isinstance(artifact.get("metadata"), Mapping) else {}
    exact = build_exact_label_ids(
        events,
        collection_decisions_by_id=collection_decisions_by_id,
        theatre_event_ids=theatre_event_ids,
        facts_policy_version=str(policy.get("facts_policy_version") or ""),
    )
    hashes = {
        "catalog_input_sha256": catalog_hash,
        "model_sha256": stable_hash(
            {
                "model_id": metadata.get("model_id"),
                "model_revision": metadata.get("model_revision"),
                "encoder_contract": metadata.get("encoder_contract"),
            }
        ),
        "document_contract_sha256": stable_hash(
            {
                "kind": metadata.get("document_kind"),
                "version": metadata.get("document_version"),
            }
        ),
        "prototype_bank_sha256": str(metadata.get("prototype_bank_sha256") or ""),
        "head_sha256": stable_hash({"policy": policy, "registry": registry_sha256}),
    }
    labels: dict[str, dict[str, Any]] = {}
    for label, ids in sorted(exact.items()):
        config = (policy.get("labels") or {}).get(label) or {}
        labels[label] = batch_module.build_collection_label(
            strategy=str(config.get("strategy") or "exact"),
            compute_status="pass",
            quality_status="pass",
            publication_status="shadow",
            item_ids=ids,
            hashes=hashes,
            verified_supply_count=len(ids),
        )
    for label, result in sorted(semantic_candidates.items()):
        config = (policy.get("labels") or {}).get(label) or {}
        failures = list(result.get("failure_codes") or [])
        if label == "unusual":
            failures.append("collection_document_recalibration_required")
        else:
            failures.append("owner_gold_missing")
        labels[label] = batch_module.build_collection_label(
            strategy=str(config.get("strategy") or "semantic_bge"),
            compute_status="pass" if not result.get("failure_codes") else "blocked",
            quality_status="not_evaluated" if not result.get("failure_codes") else "blocked",
            publication_status="blocked",
            item_ids=result.get("item_ids") or [],
            hashes=hashes,
            failure_codes=failures,
            verified_supply_count=len(result.get("item_ids") or []),
        )
    # Every policy label is explicit in the batch.  Exact-ID collections such
    # as gastronomy fail closed until their checked manifest is supplied; an
    # omitted label must never be mistaken for an approved empty collection.
    for label, config in sorted((policy.get("labels") or {}).items()):
        if label in labels:
            continue
        labels[label] = batch_module.build_collection_label(
            strategy=str(config.get("strategy") or "exact"),
            compute_status="blocked",
            quality_status="not_evaluated",
            publication_status="blocked",
            item_ids=[],
            hashes=hashes,
            failure_codes=["checked_exact_id_manifest_missing"],
            verified_supply_count=0,
        )
    return batch_module.build_collection_batch(
        catalog_hash=catalog_hash,
        labels=labels,
        generated_at=generated_at,
        snapshot=snapshot,
        policy_hash=stable_hash(policy),
        model={
            "id": metadata.get("model_id"),
            "revision": metadata.get("model_revision"),
            "artifact_sha256": metadata.get("artifact_sha256"),
            "event_cache_identity_sha256": metadata.get("event_cache_identity_sha256"),
        },
        document_contract={
            "kind": metadata.get("document_kind"),
            "version": metadata.get("document_version"),
            "encoder_contract": metadata.get("encoder_contract"),
        },
        egress_receipt={
            "core_source": "fly_sqlite_snapshot",
            "supabase_core_reads": 0,
            "additional_external_requests": 0,
        },
    )


def unusual_shadow_manifest(
    *,
    events: Sequence[Mapping[str, Any]],
    candidate_ids: Iterable[int],
    candidate_scores: Mapping[int, Mapping[str, Any]] | None = None,
    incident_regressions: Mapping[str, Any] | None = None,
    selection_policy: Mapping[str, Any] | None = None,
    generated_at: str,
    build_metadata: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    by_id = {int(event["id"]): event for event in events}
    metadata = artifact.get("metadata") if isinstance(artifact.get("metadata"), Mapping) else {}
    scores = candidate_scores or {}
    selection = dict(selection_policy or {})
    target_count = int(selection.get("target_count") or 20)
    minimum_publish_count = int(selection.get("minimum_publish_count") or 12)
    family_cap = int(selection.get("maximum_per_family") or 6)
    venue_cap = int(selection.get("maximum_per_venue") or 4)
    type_cap = int(selection.get("maximum_per_event_type") or 8)
    incident_hashes = {
        str(row.get("document_text_sha256") or ""): str(row.get("reason_code") or "incident_regression")
        for row in ((incident_regressions or {}).get("cases") or [])
        if isinstance(row, Mapping) and str(row.get("document_text_sha256") or "")
    }

    def normalise(value: Any) -> str:
        return " ".join(re.findall(r"[\w]+", str(value or "").casefold(), flags=re.UNICODE))

    def concept(event: Mapping[str, Any]) -> tuple[str, str]:
        curated = str(event.get("canonical_concept_id") or "").strip()
        root = str(event.get("canonical_root_event_id") or event.get("root_event_id") or "").strip()
        series = str(event.get("series_id") or event.get("event_series_id") or event.get("occurrence_family_id") or "").strip()
        if curated:
            return f"curated:{stable_hash(curated)[:20]}", "canonical_concept_id"
        if root:
            return f"root:{stable_hash(root)[:20]}", "canonical_root_event_id"
        if series:
            return f"series:{stable_hash(series)[:20]}", "series_id"
        identity = {
            "title": normalise(event.get("title")),
            "event_type": normalise(event.get("event_type")),
            "venue": normalise(event.get("venue_name") or event.get("location_name")),
            "city": normalise(event.get("city")),
        }
        return f"concept:{stable_hash(identity)[:24]}", "stable_presentation_identity"

    ranked_ids = sorted(
        ({int(value) for value in candidate_ids} & set(by_id)),
        key=lambda event_id: (
            -float((scores.get(event_id) or scores.get(str(event_id)) or {}).get("margin") or -1.0),
            -float((scores.get(event_id) or scores.get(str(event_id)) or {}).get("positive") or -1.0),
            str(by_id[event_id].get("start_date") or ""),
            event_id,
        ),
    )
    selected: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    seen_concepts: set[str] = set()
    family_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    duplicate_concept_count = 0
    incident_regression_count = 0
    event_vectors = artifact.get("event_vectors") if isinstance(artifact.get("event_vectors"), Mapping) else {}
    for event_id in ranked_ids:
        event = by_id[event_id]
        score = dict(scores.get(event_id) or scores.get(str(event_id)) or {})
        concept_id, concept_rule = concept(event)
        family = str(score.get("family") or "unknown")
        venue = normalise(event.get("venue_name") or event.get("location_name")) or "unknown"
        event_type = normalise(event.get("event_type")) or "unknown"
        vector_row = event_vectors.get(str(event_id)) if isinstance(event_vectors, Mapping) else None
        text_hash = str((vector_row or {}).get("text_hash") or "") if isinstance(vector_row, Mapping) else ""
        reasons = ["semantic_candidate", "positive_over_hard_negative"]
        excluded_reason: str | None = None
        if text_hash in incident_hashes:
            excluded_reason = incident_hashes[text_hash]
            incident_regression_count += 1
        elif concept_id in seen_concepts:
            excluded_reason = "duplicate_concept"
            duplicate_concept_count += 1
        elif family_counts[family] >= family_cap:
            excluded_reason = "family_cap"
        elif venue_counts[venue] >= venue_cap:
            excluded_reason = "venue_cap"
        elif type_counts[event_type] >= type_cap:
            excluded_reason = "event_type_cap"
        elif len(selected) >= target_count:
            excluded_reason = "target_filled"
        include = excluded_reason is None
        decision = {
            "event_id": event_id,
            "concept_id": concept_id,
            "concept_rule": concept_rule,
            "title": str(event.get("title") or "")[:300],
            "date": str(event.get("start_date") or ""),
            "end_date": str(event.get("end_date") or event.get("start_date") or ""),
            "path": f"/sobytiya/{event.get('slug')}/" if event.get("slug") else None,
            "score": score.get("positive"),
            "hard_negative_score": score.get("negative"),
            "margin": score.get("margin"),
            "neutral_score": score.get("neutral"),
            "family": family,
            "family_margin": score.get("family_margin"),
            "family_top3": list(score.get("family_top3") or [])[:3],
            "include": include,
            "reason_codes": reasons if include else [excluded_reason],
            "content_hash": text_hash or stable_hash({"event_id": event_id, "title": event.get("title"), "date": event.get("start_date")}),
        }
        decisions.append(decision)
        if include:
            seen_concepts.add(concept_id)
            family_counts[family] += 1
            venue_counts[venue] += 1
            type_counts[event_type] += 1
            selected.append(decision)

    shadow = [
        {
            "event_id": row["event_id"],
            "concept_id": row["concept_id"],
            "representative_event_id": row["event_id"],
            "tier": "shadow_candidate",
            "unusual_score": max(0.0, min(1.0, float(row.get("score") or 0.0))),
            "confidence": max(0.0, min(1.0, float(row.get("score") or 0.0))),
            "families": [entry.get("id") for entry in row.get("family_top3") or [] if entry.get("id")],
            "reason_codes": row["reason_codes"],
            "prototype_evidence": [],
            "notify_eligible": False,
            "content_hash": row["content_hash"],
            "date": row["date"],
            "lifecycle": "active",
            "event_snapshot": by_id[row["event_id"]],
        }
        for row in selected
    ]
    included_review = [row for row in decisions if row["include"]]
    excluded_review = [row for row in decisions if not row["include"]][:20]
    mandatory_review = [
        row
        for row in decisions
        if any(
            str(reason).startswith("incident_hard_negative")
            or reason == "duplicate_concept"
            for reason in row.get("reason_codes") or []
        )
    ]
    family_disputes = sorted(
        (
            row
            for row in decisions
            if isinstance(row.get("family_margin"), (int, float))
            and len(row.get("family_top3") or []) >= 2
        ),
        key=lambda row: (abs(float(row["family_margin"])), int(row["event_id"])),
    )[:20]
    review_by_event: dict[int, dict[str, Any]] = {}
    for row in [*included_review, *excluded_review, *mandatory_review, *family_disputes]:
        review_by_event[int(row["event_id"])] = row
    review_decisions = [
        review_by_event[int(row["event_id"])]
        for row in decisions
        if int(row["event_id"]) in review_by_event
    ]
    return {
        "schema_version": "static_unusual_events_v1",
        **dict(build_metadata),
        "generated_at": generated_at,
        "delivery_status": "blocked",
        "quality_gate": {
            "status": "blocked",
            "reason": "independent_acceptance_holdout_missing",
            "metrics": {
                "candidate_count": len(ranked_ids),
                "selected_count": 0,
                "review_shortlist_count": len(selected),
                "target_count": target_count,
                "minimum_publish_count": minimum_publish_count,
                "duplicate_concept_count": duplicate_concept_count,
                "incident_regression_count": incident_regression_count,
            },
        },
        "taxonomy_version": str((incident_regressions or {}).get("taxonomy_version") or "unusual-event-taxonomy-v1"),
        "policy_version": str(selection.get("policy_version") or "unusual-event-selection-v1-provisional"),
        "embedding_model": metadata.get("model_id"),
        "embedding_revision": metadata.get("model_revision"),
        "embedding_dim": metadata.get("embedding_dim"),
        "doc_kind": metadata.get("document_kind"),
        "document_version": metadata.get("document_version"),
        "prototype_bank_hash": metadata.get("prototype_bank_sha256"),
        "classifier_hash": metadata.get("classifier_sha256"),
        "provider_calls": 0,
        "migration": {"enabled": False, "notify": False},
        "items": [],
        "shadow_items": shadow,
        "candidate_count": len(ranked_ids),
        "selected_count": 0,
        "review_shortlist_count": len(selected),
        "target_count": target_count,
        "minimum_publish_count": minimum_publish_count,
        "selected_event_ids": [],
        "selected_concept_ids": [],
        "review_shortlist_event_ids": [row["event_id"] for row in selected],
        "review_shortlist_concept_ids": [row["concept_id"] for row in selected],
        "decisions": review_decisions,
    }
