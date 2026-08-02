#!/usr/bin/env python3
"""Shared publisher-profile identities and monotonic merge helpers.

The durable publisher identity is the web domain, never an article identity.
This module intentionally contains no YDB or publication side effects so both
the guarded profile importer and normal external-research intake can use the
same fail-closed merge contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any


PROFILE_MERGE_VERSION = "region_talk_publisher_profile_merge.v1"


class PublisherProfileConflict(ValueError):
    """Raised when two rows cannot safely represent the same publisher."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def canonical_publisher_domain(value: Any) -> str:
    raw = str(value or "").strip().strip(".").lower()
    if not raw or "/" in raw or ":" in raw or "@" in raw:
        raise PublisherProfileConflict("publisher domain must be a bare DNS name")
    try:
        domain = raw.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise PublisherProfileConflict("publisher domain is not valid IDNA") from exc
    if domain.startswith("www."):
        domain = domain[4:]
    if (
        len(domain) > 253
        or not re.fullmatch(r"[a-z0-9.-]+", domain)
        or ".." in domain
        or any(not label or len(label) > 63 or label.startswith("-") or label.endswith("-") for label in domain.split("."))
        or "." not in domain
    ):
        raise PublisherProfileConflict("publisher domain is not canonical")
    return domain


def runtime_publisher_source_key(domain: Any) -> str:
    return "web:" + canonical_publisher_domain(domain)


def publisher_profile_id(source_key: Any) -> str:
    key = str(source_key or "").strip().lower()
    if not key.startswith("web:"):
        raise PublisherProfileConflict("runtime publisher source key must start with web:")
    domain = canonical_publisher_domain(key.split(":", 1)[1])
    if key != "web:" + domain:
        raise PublisherProfileConflict("runtime publisher source key is not canonical")
    return "rtpublisher_" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]


def publisher_evidence_fingerprint(evidence: Any) -> str:
    rows = evidence if isinstance(evidence, list) else []
    hashes = sorted({canonical_json_sha256(row) for row in rows if isinstance(row, dict)})
    return "rtpublisher_evidence_" + hashlib.sha256("\n".join(hashes).encode("ascii")).hexdigest()


def _evidence_union(*groups: Any) -> list[dict[str, Any]]:
    by_hash: dict[str, dict[str, Any]] = {}
    for group in groups:
        for row in group if isinstance(group, list) else []:
            if isinstance(row, dict):
                by_hash.setdefault(canonical_json_sha256(row), deepcopy(row))
    return [by_hash[key] for key in sorted(by_hash)]


def _scope_merge(existing: str, incoming: str) -> str:
    old = existing or "unknown"
    new = incoming or "unknown"
    if old == new:
        return old
    if old == "unknown":
        return new
    if new == "unknown":
        return old
    # Any two different asserted scopes are decision-critical. In particular,
    # an article claiming ``external`` cannot silently clean a durable
    # ``mixed`` publisher (or vice versa).
    raise PublisherProfileConflict(f"conflicting publisher scope/locality: {old} vs {new}")


def _profile_score(row: dict[str, Any]) -> tuple[int, int, int]:
    dimensions = row.get("profile_dimensions") if isinstance(row.get("profile_dimensions"), dict) else {}
    dimension_count = sum(bool(dimensions.get(name)) for name in (
        "outlet_identity", "intended_audience", "distinctive_value",
        "editorial_scope", "recurring_formats", "locality_guard",
    ))
    origin_score = 2 if row.get("profile_origin") == "publisher_profile_sidecar" else 1
    evidence_count = len(row.get("evidence") or [])
    return origin_score, dimension_count, evidence_count


def merge_publisher_profile_rows(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    """Merge one publisher row without allowing a seed to erase a dossier.

    Evidence is unioned by its canonical fingerprint.  A normal article seed
    can enrich a full imported dossier, including on candidate replay, but it
    cannot replace the dossier's dimensions, readiness, or copy projection.
    """
    if not existing:
        return deepcopy(incoming)
    old_key = str(existing.get("canonical_source_key") or "")
    new_key = str(incoming.get("canonical_source_key") or "")
    if old_key != new_key:
        raise PublisherProfileConflict("profile replay maps to another source key")
    if str(existing.get("publisher_profile_id") or "") != str(incoming.get("publisher_profile_id") or ""):
        raise PublisherProfileConflict("publisher profile id maps to another source key")
    if canonical_publisher_domain(existing.get("source_domain")) != canonical_publisher_domain(incoming.get("source_domain")):
        raise PublisherProfileConflict("publisher profile domain conflict")

    richer = incoming if _profile_score(incoming) > _profile_score(existing) else existing
    other = existing if richer is incoming else incoming
    merged = deepcopy(richer)
    merged["scope"] = _scope_merge(str(existing.get("scope") or ""), str(incoming.get("scope") or ""))
    merged["evidence"] = _evidence_union(existing.get("evidence"), incoming.get("evidence"))
    merged["evidence_fingerprint"] = publisher_evidence_fingerprint(merged["evidence"])
    merged["evidence_json_sha256"] = canonical_json_sha256(merged["evidence"])
    merged["evidence_item_hashes"] = sorted(canonical_json_sha256(row) for row in merged["evidence"])
    merged["profile_hashes"] = sorted({
        str(value)
        for value in list(existing.get("profile_hashes") or [])
        + list(incoming.get("profile_hashes") or [])
        + [existing.get("profile_hash"), incoming.get("profile_hash")]
        if str(value or "")
    })
    merged["evidence_fingerprints"] = sorted({
        str(value)
        for value in list(existing.get("evidence_fingerprints") or [])
        + list(incoming.get("evidence_fingerprints") or [])
        + [existing.get("evidence_fingerprint"), incoming.get("evidence_fingerprint")]
        if str(value or "")
    })
    observations: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in list(existing.get("provenance_observations") or []) + list(incoming.get("provenance_observations") or []):
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("request_id") or ""),
            str(row.get("input_json_sha256") or ""),
            str(row.get("profile_hash") or row.get("evidence_fingerprint") or ""),
        )
        observations.setdefault(key, deepcopy(row))
    merged["provenance_observations"] = [observations[key] for key in sorted(observations)]
    merged["profile_merge_version"] = PROFILE_MERGE_VERSION
    merged["first_imported_at"] = existing.get("first_imported_at") or existing.get("imported_at") or incoming.get("imported_at")
    merged["updated_at"] = incoming.get("updated_at") or existing.get("updated_at")

    if existing.get("profile_origin") == incoming.get("profile_origin") == "external_research_seed":
        old_dimensions = existing.get("profile_dimensions") if isinstance(existing.get("profile_dimensions"), dict) else {}
        new_dimensions = incoming.get("profile_dimensions") if isinstance(incoming.get("profile_dimensions"), dict) else {}
        dimensions = deepcopy(merged.get("profile_dimensions") or {})
        if len(str(new_dimensions.get("outlet_identity") or "")) > len(str(old_dimensions.get("outlet_identity") or "")):
            dimensions["outlet_identity"] = new_dimensions.get("outlet_identity") or ""
        old_guard = old_dimensions.get("locality_guard") if isinstance(old_dimensions.get("locality_guard"), dict) else {}
        new_guard = new_dimensions.get("locality_guard") if isinstance(new_dimensions.get("locality_guard"), dict) else {}
        dimensions["locality_guard"] = {
            "article_producer_check": max(
                (str(old_guard.get("article_producer_check") or ""), str(new_guard.get("article_producer_check") or "")),
                key=len,
            ),
            "evidence_refs": sorted({
                str(value) for value in list(old_guard.get("evidence_refs") or []) + list(new_guard.get("evidence_refs") or [])
                if str(value)
            }),
        }
        merged["profile_dimensions"] = dimensions

    # A research seed is never allowed to downgrade a reviewed/full dossier.
    if incoming.get("profile_origin") == "external_research_seed" and existing.get("profile_origin") == "publisher_profile_sidecar":
        for key in (
            "profile_origin", "profile_status", "usable_without_profile_llm",
            "profile_dimensions", "profile_payload", "profile_hash",
            "copy_projection", "public_copy_eligibility",
        ):
            if key in existing:
                merged[key] = deepcopy(existing[key])
    elif other.get("profile_origin") == "publisher_profile_sidecar" and richer is incoming:
        # Defensive counterpart if a scoring change ever ranks a seed higher.
        merged.update({
            key: deepcopy(other[key])
            for key in (
                "profile_origin", "profile_status", "usable_without_profile_llm",
                "profile_dimensions", "profile_payload", "profile_hash",
                "copy_projection", "public_copy_eligibility",
            )
            if key in other
        })
    return merged
