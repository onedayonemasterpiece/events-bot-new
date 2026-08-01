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
            row["vector"]
            for prototype_id, row in prototypes.items()
            if str(prototype_id).startswith(positive_prefix) and isinstance(row, Mapping)
        ]
        negative = [
            row["vector"]
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
        scores: dict[int, dict[str, float]] = {}
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
            positive_score = max(_dot(vector, candidate) for candidate in positive)
            negative_score = max(_dot(vector, candidate) for candidate in negative)
            margin = positive_score - negative_score
            if positive_score >= minimum and margin >= margin_minimum:
                scores[event_id] = {
                    "positive": round(positive_score, 6),
                    "negative": round(negative_score, 6),
                    "margin": round(margin, 6),
                }
        result[str(label)] = {
            "item_ids": sorted(scores),
            "scores": scores,
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
        if isinstance(audience, Mapping) and audience.get("value") in {"kids", "family"}:
            labels["kids"].append(event_id)
        for person in decisions.get("people_appearances") or []:
            if not isinstance(person, Mapping) or person.get("appearance") != "confirmed":
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
    generated_at: str,
    build_metadata: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    by_id = {int(event["id"]): event for event in events}
    metadata = artifact.get("metadata") if isinstance(artifact.get("metadata"), Mapping) else {}
    shadow = [
        {
            "event_id": event_id,
            "event_snapshot": by_id[event_id],
            "notify_eligible": False,
        }
        for event_id in sorted({int(value) for value in candidate_ids})
        if event_id in by_id
    ]
    return {
        "schema_version": "static_unusual_events_v1",
        **dict(build_metadata),
        "generated_at": generated_at,
        "delivery_status": "blocked",
        "quality_gate": {
            "status": "blocked",
            "reason": "collection_document_recalibration_required",
        },
        "embedding_model": metadata.get("model_id"),
        "embedding_revision": metadata.get("model_revision"),
        "document_version": metadata.get("document_version"),
        "prototype_bank_hash": metadata.get("prototype_bank_sha256"),
        "provider_calls": 0,
        "migration": {"enabled": False, "notify": False},
        "items": [],
        "shadow_items": shadow,
    }
