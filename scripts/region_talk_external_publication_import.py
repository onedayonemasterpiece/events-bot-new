#!/usr/bin/env python3
"""Validate and stage external web-research results for Region Talk.

This is intentionally an intake adapter, not a crawler and not a publisher.
Valid rows are idempotently upserted to the Region Talk YDB sidecar as
``external_publication_intake_item`` records.  They must still pass the normal
Region Talk text/vector, image, verifier, rights, and operator gates.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import re
import sys
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)


SCHEMA_VERSION = "region_talk_external_research.v1"
IMPORT_VERSION = "region_talk_external_publication_import.v1"
SCHEMA_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research.schema.json"
REQUEST_SCHEMA_VERSION = "region_talk_external_research_request.v1"
REQUEST_SCHEMA_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research-request.schema.json"
SEEN_GUARD_VERSION = "region_talk_external_seen_guard_v1"
IDENTITY_LEDGER_VERSION = "region_talk_external_identity_v1"
TRACKS = {"scholarly", "professional_editorial", "popular_editorial", "reference_or_project_catalog"}
RESEARCH_DECISIONS = {"candidate", "needs_review", "exclude"}
READINESS = {"candidate_report", "manual_review_required", "blocked"}
CENTRALITY = {"central", "substantial", "secondary", "episodic"}
SOURCE_SCOPES = {"external", "mixed", "regional", "unknown"}
TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "yclid", "mc_cid", "mc_eid", "ref", "referrer",
}


class ContractError(ValueError):
    pass


def schema_errors_by_candidate(payload: dict[str, Any]) -> dict[int, list[str]]:
    """Validate the producer contract while retaining row-level rejection.

    Candidate-local schema errors are returned by array index so one malformed
    model result cannot discard otherwise valid research. Contract errors
    outside ``candidates[]`` abort the batch because run/coverage/exclusion
    metadata cannot be safely attributed to a single row.
    """
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    candidate_errors: dict[int, list[str]] = {}
    batch_errors: list[str] = []
    for error in sorted(validator.iter_errors(payload), key=lambda item: [str(value) for value in item.absolute_path]):
        path = list(error.absolute_path)
        rendered_path = ".".join(str(value) for value in path) or "$"
        message = f"schema {rendered_path}: {error.message}"
        if len(path) >= 2 and path[0] == "candidates" and isinstance(path[1], int):
            candidate_errors.setdefault(path[1], []).append(message)
        else:
            batch_errors.append(message)
    if batch_errors:
        raise ContractError("; ".join(batch_errors))
    return candidate_errors


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_hash(*parts: Any, length: int = 24) -> str:
    digest = hashlib.sha256("\0".join(str(part or "").strip().lower() for part in parts).encode("utf-8")).hexdigest()
    return digest[:length]


def _is_forbidden_host(host: str) -> bool:
    value = (host or "").strip(".[]").lower()
    if not value or value == "localhost" or value.endswith((".localhost", ".local", ".internal")):
        return True
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return False
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def canonicalize_http_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
    except ValueError as exc:
        raise ContractError(f"invalid URL: {raw[:160]}") from exc
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ContractError("URL scheme must be http or https")
    host = (parsed.hostname or "").lower()
    if _is_forbidden_host(host):
        raise ContractError("URL host is empty, local, private, or reserved")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ContractError("invalid URL port") from exc
    if port not in (None, 80, 443):
        raise ContractError("URL uses a non-web port")
    netloc = host.encode("idna").decode("ascii")
    if port and not ((parsed.scheme.lower() == "http" and port == 80) or (parsed.scheme.lower() == "https" and port == 443)):
        netloc += f":{port}"
    query = sorted([
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in TRACKING_QUERY_KEYS
    ])
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), netloc, path, urlencode(query, doseq=True), ""))


def canonical_url_identity(value: Any) -> str:
    """Return the conservative publication identity for an already-public URL.

    HTTP/HTTPS and a leading ``www.`` are transport aliases for this dedupe
    ledger.  Path, non-tracking query names/values, and every other hostname
    label remain exact; this is deliberately not fuzzy URL matching.
    """
    canonical = canonicalize_http_url(value)
    parsed = urlsplit(canonical)
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return urlunsplit(("https", host, parsed.path, parsed.query, ""))


def normalize_exact_text(value: Any) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).casefold().split())


def normalize_title_authors(title: Any, authors: Any) -> tuple[str, list[str]]:
    normalized_title = normalize_exact_text(title)
    normalized_authors = [
        normalized
        for value in (authors if isinstance(authors, list) else [])
        if (normalized := normalize_exact_text(value))
    ]
    return normalized_title, normalized_authors


def title_authors_identity(title: Any, authors: Any) -> str:
    normalized_title, normalized_authors = normalize_title_authors(title, authors)
    if not normalized_title or not normalized_authors:
        return ""
    return "title_authors:" + normalized_title + "\0" + "\0".join(normalized_authors)


def publication_identity_keys(*, canonical_url: Any, doi: Any, title: Any, authors: Any) -> list[str]:
    keys = ["url:" + canonical_url_identity(canonical_url)] if canonical_url else []
    normalized_doi = normalize_doi(doi) if doi else ""
    if normalized_doi:
        keys.append("doi:" + normalized_doi)
    title_key = title_authors_identity(title, authors)
    if title_key:
        keys.append(title_key)
    return sorted(set(keys))


def normalize_doi(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", raw)
    if not raw:
        return ""
    if not re.fullmatch(r"10\.\d{4,9}/\S+", raw):
        raise ContractError("invalid DOI")
    return raw.rstrip(".,; ")


def load_duplicate_guard(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    schema = json.loads(REQUEST_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
    if payload.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise ContractError(f"request sidecar schema_version must be {REQUEST_SCHEMA_VERSION}")
    guard = payload.get("duplicate_guard") if isinstance(payload.get("duplicate_guard"), dict) else {}
    seen = guard.get("seen_publications") if isinstance(guard.get("seen_publications"), list) else []
    canonical_seen = sorted(seen, key=lambda item: (str(item.get("doi") or ""), str(item.get("canonical_url") or "")))
    raw = json.dumps(canonical_seen, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    expected_snapshot = "rtseen_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    if str(guard.get("snapshot_id") or "") != expected_snapshot:
        raise ContractError("request sidecar duplicate_guard.snapshot_id does not match its seen_publications")
    if int(guard.get("seen_publication_count") or 0) != len(canonical_seen):
        raise ContractError("request sidecar seen_publication_count mismatch")
    urls: set[str] = set()
    dois: set[str] = set()
    titles_authors: set[str] = set()
    identity_map: dict[str, str] = {}
    for item in canonical_seen:
        try:
            if item.get("canonical_url"):
                urls.add(canonical_url_identity(item.get("canonical_url")))
            if item.get("doi"):
                dois.add(normalize_doi(item.get("doi")))
            title_key = title_authors_identity(item.get("title"), item.get("authors") or item.get("normalized_authors"))
            if title_key:
                titles_authors.add(title_key)
            external_id = str(item.get("external_publication_id") or "").strip()
            for key in publication_identity_keys(
                canonical_url=item.get("canonical_url"),
                doi=item.get("doi"),
                title=item.get("title") or item.get("normalized_title"),
                authors=item.get("authors") or item.get("normalized_authors"),
            ):
                if external_id:
                    current = identity_map.setdefault(key, external_id)
                    if current != external_id:
                        raise ContractError(f"request duplicate guard maps {key.split(':', 1)[0]} to multiple publications")
        except ContractError as exc:
            raise ContractError(f"invalid identity in request duplicate guard: {exc}") from exc
    return {
        "snapshot_id": expected_snapshot,
        "request": payload.get("request") or {},
        "urls": urls,
        "dois": dois,
        "titles_authors": titles_authors,
        "identity_map": identity_map,
    }


def duplicate_guard_from_seen_publications(seen_publications: list[dict[str, Any]]) -> dict[str, Any]:
    """Build an import-time guard from the current durable YDB projection."""
    canonical_seen = sorted(
        seen_publications,
        key=lambda item: (str(item.get("doi") or ""), str(item.get("canonical_url") or "")),
    )
    raw = json.dumps(canonical_seen, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    urls: set[str] = set()
    dois: set[str] = set()
    titles_authors: set[str] = set()
    identity_map: dict[str, str] = {}
    for item in canonical_seen:
        if item.get("canonical_url"):
            urls.add(canonical_url_identity(item.get("canonical_url")))
        if item.get("doi"):
            dois.add(normalize_doi(item.get("doi")))
        title_key = title_authors_identity(item.get("title"), item.get("authors") or item.get("normalized_authors"))
        if title_key:
            titles_authors.add(title_key)
        external_id = str(item.get("external_publication_id") or "").strip()
        for key in publication_identity_keys(
            canonical_url=item.get("canonical_url"),
            doi=item.get("doi"),
            title=item.get("title") or item.get("normalized_title"),
            authors=item.get("authors") or item.get("normalized_authors"),
        ):
            if external_id:
                current = identity_map.setdefault(key, external_id)
                if current != external_id:
                    raise ContractError(f"live duplicate guard maps {key.split(':', 1)[0]} to multiple publications")
    return {
        "snapshot_id": "rtseen_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24],
        "request": {},
        "urls": urls,
        "dois": dois,
        "titles_authors": titles_authors,
        "identity_map": identity_map,
    }


def merge_duplicate_guards(*guards: dict[str, Any] | None) -> dict[str, Any] | None:
    present = [guard for guard in guards if guard]
    if not present:
        return None
    explicit_request = next(
        (
            guard.get("request")
            for guard in present
            if isinstance(guard.get("request"), dict) and guard.get("request")
        ),
        {},
    )
    merged = {
        "snapshot_id": "+".join(str(guard.get("snapshot_id") or "unknown") for guard in present),
        "request": explicit_request,
        "urls": set().union(*(guard.get("urls", set()) for guard in present)),
        "dois": set().union(*(guard.get("dois", set()) for guard in present)),
        "titles_authors": set().union(*(guard.get("titles_authors", set()) for guard in present)),
        "identity_map": {},
    }

    for guard in present:
        for identity, external_id in (guard.get("identity_map") or {}).items():
            current = merged["identity_map"].setdefault(identity, external_id)
            if current != external_id:
                raise ContractError(f"duplicate guards map {identity.split(':', 1)[0]} to multiple publications")
    return merged


def _require_mapping(value: Any, field: str, errors: list[str]) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    errors.append(f"{field}: expected object")
    return {}


def _score(value: Any, field: str, errors: list[str]) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 4:
        errors.append(f"{field}: expected integer 0..4")
        return 0
    return value


def _date_in_window(value: str, start: str, end: str) -> bool:
    try:
        parsed = date.fromisoformat(value)
        return date.fromisoformat(start) <= parsed <= date.fromisoformat(end)
    except (TypeError, ValueError):
        return False


def _evidence_index(candidate: dict[str, Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    raw = candidate.get("evidence")
    if not isinstance(raw, list) or not raw:
        errors.append("evidence: expected non-empty list")
        return {}
    out: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            errors.append(f"evidence[{index}]: expected object")
            continue
        evidence_id = str(item.get("evidence_id") or "").strip()
        if not evidence_id or evidence_id in out:
            errors.append(f"evidence[{index}].evidence_id: missing or duplicate")
            continue
        try:
            item = {**item, "url": canonicalize_http_url(item.get("url"))}
        except ContractError as exc:
            errors.append(f"evidence[{index}].url: {exc}")
            continue
        if not item["url"]:
            errors.append(f"evidence[{index}].url: required")
            continue
        item["paraphrase"] = str(item.get("paraphrase") or "").strip()[:600]
        item["quote_short"] = str(item.get("quote_short") or "").strip()[:240]
        out[evidence_id] = item
    return out


def _collect_refs(value: Any) -> list[str]:
    refs: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key.endswith("evidence_refs") and isinstance(item, list):
                refs.extend(str(ref).strip() for ref in item if str(ref).strip())
            else:
                refs.extend(_collect_refs(item))
    elif isinstance(value, list):
        for item in value:
            refs.extend(_collect_refs(item))
    return refs


def validate_candidate(candidate: Any, run: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    if not isinstance(candidate, dict):
        return None, ["candidate: expected object"]
    try:
        canonical_url = canonicalize_http_url(candidate.get("canonical_url"))
    except ContractError as exc:
        canonical_url = ""
        errors.append(f"canonical_url: {exc}")
    if not canonical_url:
        errors.append("canonical_url: required")
    try:
        doi = normalize_doi(candidate.get("doi"))
    except ContractError as exc:
        doi = ""
        errors.append(f"doi: {exc}")

    publication = _require_mapping(candidate.get("publication"), "publication", errors)
    source = _require_mapping(candidate.get("source_assessment"), "source_assessment", errors)
    relevance = _require_mapping(candidate.get("region_relevance"), "region_relevance", errors)
    policy = _require_mapping(candidate.get("policy_classification"), "policy_classification", errors)
    quality = _require_mapping(candidate.get("quality_assessment"), "quality_assessment", errors)
    editorial = _require_mapping(candidate.get("editorial_pack"), "editorial_pack", errors)
    media = _require_mapping(candidate.get("media_and_rights"), "media_and_rights", errors)
    decision = _require_mapping(candidate.get("decision"), "decision", errors)
    evidence = _evidence_index(candidate, errors)

    title = str(publication.get("title") or "").strip()
    authors = [str(value).strip() for value in publication.get("authors") or [] if str(value).strip()]
    normalized_title, normalized_authors = normalize_title_authors(title, authors)
    source_name = str(publication.get("source_name") or "").strip()
    if not title:
        errors.append("publication.title: required")
    if not source_name:
        errors.append("publication.source_name: required")
    centrality = str(relevance.get("centrality") or "")
    if centrality not in CENTRALITY:
        errors.append("region_relevance.centrality: invalid enum")
    scope = str(source.get("scope") or "")
    if scope not in SOURCE_SCOPES:
        errors.append("source_assessment.scope: invalid enum")
    track = str(quality.get("track") or "")
    if track not in TRACKS:
        errors.append("quality_assessment.track: invalid enum")
    research_decision = str(decision.get("research_decision") or "")
    readiness = str(decision.get("downstream_readiness") or "")
    if research_decision not in RESEARCH_DECISIONS:
        errors.append("decision.research_decision: invalid enum")
    if readiness not in READINESS:
        errors.append("decision.downstream_readiness: invalid enum")

    hard_codes = sorted({str(code).strip() for code in policy.get("hard_exclusion_codes") or [] if str(code).strip()})
    product_match = policy.get("product_policy_match") is True
    research_match = policy.get("research_match") is True
    language_match = policy.get("language_policy_match") is True
    published_at = str(publication.get("published_at") or "").strip()
    if research_decision == "candidate":
        if scope != "external":
            errors.append("candidate requires source_assessment.scope=external")
        if centrality not in {"central", "substantial"}:
            errors.append("candidate requires central or substantial region relevance")
        if not research_match or not product_match or not language_match:
            errors.append("candidate requires research/product/language policy matches")
        if hard_codes:
            errors.append("candidate has hard exclusion: " + ",".join(hard_codes))
        if str(policy.get("newsiness") or "") != "non_news":
            errors.append("candidate requires policy_classification.newsiness=non_news")
        if str(policy.get("commerciality") or "") not in {"independent", "institutional_noncommercial"}:
            errors.append("candidate requires noncommercial policy classification")
        if str(publication.get("date_basis") or "") in {"", "search_snippet", "unknown"}:
            errors.append("candidate requires a verified primary/issue/DOI date basis")
        if str(publication.get("access_status") or "") != "full_text":
            errors.append("candidate requires full_text access; otherwise use needs_review")
        if not source.get("externality_evidence_refs"):
            errors.append("candidate requires source externality evidence")
        if not _date_in_window(published_at, str(run.get("window_start") or ""), str(run.get("window_end") or "")):
            errors.append("candidate requires verified published_at inside the run window")
        if readiness != "candidate_report":
            errors.append("clean candidate readiness must be candidate_report")

    score_names = [
        "source_authority", "evidence_depth", "editorial_independence", "originality",
        "kaliningrad_centrality", "public_interest", "accessibility",
    ]
    normalized_scores: dict[str, int] = {}
    for name in score_names:
        item = _require_mapping(quality.get(name), f"quality_assessment.{name}", errors)
        normalized_scores[name] = _score(item.get("score"), f"quality_assessment.{name}.score", errors)

    if research_decision == "candidate":
        if str(quality.get("quality_tier") or "") not in {"strong", "credible"}:
            errors.append("candidate requires quality_tier=strong or credible")
        for name in ("kaliningrad_centrality", "public_interest", "accessibility"):
            if normalized_scores.get(name, 0) < 2:
                errors.append(f"candidate requires quality_assessment.{name}.score>=2")
        if track == "scholarly":
            scholarly = quality.get("scholarly_details") if isinstance(quality.get("scholarly_details"), dict) else {}
            if scholarly.get("publication_status") != "peer_reviewed":
                errors.append("scholarly candidate requires verified peer_reviewed status")
            if scholarly.get("correction_status") != "none_found":
                errors.append("scholarly correction/retraction uncertainty requires needs_review")
        else:
            details = quality.get("editorial_details") if isinstance(quality.get("editorial_details"), dict) else {}
            if details.get("original_reporting_or_analysis") is not True:
                errors.append("editorial candidate requires original reporting or analysis")

    all_refs = _collect_refs({
        "source_assessment": source,
        "region_relevance": relevance,
        "quality_assessment": quality,
        "editorial_pack": editorial,
        "media_and_rights": media,
    })
    missing_refs = sorted({ref for ref in all_refs if ref not in evidence})
    if missing_refs:
        errors.append("unresolved evidence refs: " + ",".join(missing_refs))
    copy_support = editorial.get("copy_support")
    if not isinstance(copy_support, list) or not copy_support:
        errors.append("editorial_pack.copy_support: required for grounded copy")
    else:
        supported_surfaces: set[str] = set()
        for index, item in enumerate(copy_support):
            if not isinstance(item, dict) or not item.get("evidence_refs"):
                errors.append(f"editorial_pack.copy_support[{index}]: evidence_refs required")
                continue
            supported_surfaces.add(str(item.get("surface") or ""))
        for surface in ("teaser", "source_overview", "reader_takeaway", "why_selected", "caveat"):
            if str(editorial.get(surface) or "").strip() and surface not in supported_surfaces:
                errors.append(f"editorial_pack.{surface}: missing copy_support coverage")

    candidate_urls = media.get("candidate_urls")
    normalized_media_urls: list[str] = []
    if not isinstance(candidate_urls, list):
        errors.append("media_and_rights.candidate_urls: expected array")
    else:
        for index, value in enumerate(candidate_urls):
            try:
                normalized_media_urls.append(canonicalize_http_url(value))
            except ContractError as exc:
                errors.append(f"media_and_rights.candidate_urls[{index}]: {exc}")

    media_reuse = media.get("media_reuse_allowed") is True
    rights_policy = str(media.get("rights_policy") or "unknown")
    if media_reuse and rights_policy != "reuse_verified":
        errors.append("media reuse requires rights_policy=reuse_verified")

    if errors:
        return None, errors
    identity_keys = publication_identity_keys(
        canonical_url=canonical_url,
        doi=doi,
        title=title,
        authors=authors,
    )
    identity = "doi:" + doi if doi else "url:" + canonical_url_identity(canonical_url)
    candidate_id = "extpub_" + stable_hash(identity)
    quality_total = round(sum(normalized_scores.values()) / (4 * len(normalized_scores)), 3)
    status = (
        "ready_for_region_talk_scoring"
        if research_decision == "candidate" and readiness == "candidate_report"
        else "manual_review_required"
        if research_decision == "needs_review" or readiness == "manual_review_required"
        else "research_only_blocked"
    )
    normalized = {
        **candidate,
        "external_publication_id": candidate_id,
        "canonical_url": canonical_url,
        "doi": doi or None,
        "content_origin_type": "academic_publication" if track == "scholarly" else "editorial_publication",
        "publication": {**publication, "title": title, "authors": authors, "source_name": source_name},
        "normalized_title": normalized_title,
        "normalized_authors": normalized_authors,
        "identity_keys": identity_keys,
        "quality_assessment": {**quality, "normalized_score": quality_total},
        "media_and_rights": {
            **media,
            "candidate_urls": normalized_media_urls,
            "rights_policy": rights_policy,
            "media_reuse_allowed": media_reuse,
            "media_use_policy": "score_only_no_reuse" if not media_reuse else "reuse_verified",
        },
        "decision": {**decision, "import_status": status},
        "evidence": list(evidence.values()),
        "canonical_evidence_urls": sorted({str(item.get("url") or "") for item in evidence.values() if item.get("url")}),
        "import_contract_version": IMPORT_VERSION,
    }
    return normalized, []


def prepare_import(
    payload: Any,
    *,
    imported_at: str | None = None,
    duplicate_guard: dict[str, Any] | None = None,
    input_json_sha256: str | None = None,
    raw_input_sha256: str | None = None,
) -> dict[str, Any]:
    imported_at = imported_at or utc_now_iso()
    if not isinstance(payload, dict):
        raise ContractError("top-level JSON must be an object")
    if input_json_sha256 and raw_input_sha256 and input_json_sha256 != raw_input_sha256:
        raise ContractError("input_json_sha256 aliases disagree")
    raw_input_sha256 = input_json_sha256 or raw_input_sha256
    if raw_input_sha256 is None:
        canonical_bytes = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        raw_input_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    raw_input_sha256 = str(raw_input_sha256).strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", raw_input_sha256):
        raise ContractError("raw_input_sha256 must be a lowercase SHA-256 hex digest")
    candidate_schema_errors = schema_errors_by_candidate(payload)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ContractError(f"schema_version must be {SCHEMA_VERSION}")
    run = payload.get("run")
    if not isinstance(run, dict):
        raise ContractError("run must be an object")
    request_id = str(run.get("request_id") or "").strip()
    if not request_id:
        raise ContractError("run.request_id is required")
    try:
        date.fromisoformat(str(run.get("window_start") or ""))
        date.fromisoformat(str(run.get("window_end") or ""))
    except ValueError as exc:
        raise ContractError("run window must use YYYY-MM-DD") from exc
    if str(run.get("window_start")) > str(run.get("window_end")):
        raise ContractError("run.window_start must not be after window_end")
    if duplicate_guard and duplicate_guard.get("request"):
        request = duplicate_guard.get("request") if isinstance(duplicate_guard.get("request"), dict) else {}
        expected = {
            "request_id": request.get("request_id"),
            "window_start": request.get("window_start"),
            "window_end": request.get("window_end"),
            "research_languages": request.get("research_languages"),
            "product_language_policy": request.get("product_language_policy"),
        }
        actual = {
            "request_id": run.get("request_id"),
            "window_start": run.get("window_start"),
            "window_end": run.get("window_end"),
            "research_languages": run.get("research_languages"),
            "product_language_policy": run.get("product_language_policy"),
        }
        if actual != expected:
            raise ContractError("research result run fields do not match the generated request sidecar")

    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise ContractError("candidates must be an array")
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    replayed: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    identity_owner: dict[str, str] = dict((duplicate_guard or {}).get("identity_map") or {})
    anonymous_guard_keys: set[str] = set()
    for url in (duplicate_guard or {}).get("urls", set()):
        key = "url:" + canonical_url_identity(url)
        if key not in identity_owner:
            anonymous_guard_keys.add(key)
    for doi in (duplicate_guard or {}).get("dois", set()):
        key = "doi:" + normalize_doi(doi)
        if key not in identity_owner:
            anonymous_guard_keys.add(key)
    for title_key in (duplicate_guard or {}).get("titles_authors", set()):
        key = str(title_key)
        if key and key not in identity_owner:
            anonymous_guard_keys.add(key)
    duplicate_seen_count = 0
    for index, raw in enumerate(candidates):
        if candidate_schema_errors.get(index):
            rejected.append({
                "index": index,
                "canonical_url": str(raw.get("canonical_url") or "") if isinstance(raw, dict) else "",
                "errors": candidate_schema_errors[index],
            })
            continue
        normalized, errors = validate_candidate(raw, run)
        if normalized is None:
            rejected.append({
                "index": index,
                "canonical_url": str(raw.get("canonical_url") or "") if isinstance(raw, dict) else "",
                "errors": errors,
            })
            continue
        identity_keys = list(normalized.get("identity_keys") or [])
        matched_owners = {identity_owner[key] for key in identity_keys if key in identity_owner}
        anonymous_matches = [key for key in identity_keys if key in anonymous_guard_keys]
        if len(matched_owners) > 1 or (matched_owners and anonymous_matches) or len(anonymous_matches) > 1:
            conflicts.append({
                "index": index,
                "canonical_url": normalized["canonical_url"],
                "identity_keys": identity_keys,
                "external_publication_ids": sorted(matched_owners),
                "errors": ["identity keys resolve to different or unverifiable existing publications"],
            })
            continue
        if matched_owners or anonymous_matches:
            duplicate_seen_count += 1
            external_id = next(iter(matched_owners), normalized["external_publication_id"])
            replayed.append({
                "index": index,
                "canonical_url": normalized["canonical_url"],
                "external_publication_id": external_id,
                "matched_identity_keys": sorted(
                    key for key in identity_keys if key in identity_owner or key in anonymous_guard_keys
                ),
                "reason": "already_seen",
            })
            for key in identity_keys:
                identity_owner.setdefault(key, external_id)
            existing = next((row for row in valid if row["external_publication_id"] == external_id), None)
            if existing is not None:
                existing["identity_keys"] = sorted(set(existing.get("identity_keys") or []).union(identity_keys))
            continue
        external_id = normalized["external_publication_id"]
        for key in identity_keys:
            identity_owner[key] = external_id
        normalized.update({
            "request_id": request_id,
            "research_request_id": request_id,
            "input_json_sha256": raw_input_sha256,
            "raw_input_json_sha256": raw_input_sha256,
            "research_executed_at": run.get("executed_at") or "",
            "research_window_start": run.get("window_start"),
            "research_window_end": run.get("window_end"),
            "intake_status": "new_intake",
            "review_status": "unreviewed",
            "publication_permission": "not_granted",
            "intake_at": imported_at,
            "intake_received_at": imported_at,
            "imported_at": imported_at,
            "updated_at": imported_at,
            "next_action": (
                "run_region_talk_text_vector_and_image_scoring"
                if normalized["decision"]["import_status"] == "ready_for_region_talk_scoring"
                else "operator_review_external_research"
            ),
        })
        valid.append(normalized)

    batch_id = "extpubrun_" + stable_hash(request_id)
    rows: list[tuple[str, str, dict[str, Any]]] = []
    for row in valid:
        rows.append((
            "external_publication_intake_item:" + row["external_publication_id"],
            "external_publication_intake_item",
            row,
        ))
        for identity_key in row.get("identity_keys") or []:
            identity_sha256 = hashlib.sha256(identity_key.encode("utf-8")).hexdigest()
            identity_row = {
                "identity_key_sha256": identity_sha256,
                "identity_type": identity_key.split(":", 1)[0],
                "identity_value": identity_key.split(":", 1)[1],
                "external_publication_id": row["external_publication_id"],
                "request_id": request_id,
                "input_json_sha256": raw_input_sha256,
                "raw_input_json_sha256": raw_input_sha256,
                "reserved_at": imported_at,
                "identity_ledger_version": IDENTITY_LEDGER_VERSION,
                "updated_at": imported_at,
            }
            rows.append((
                "external_publication_identity_item:" + identity_sha256,
                "external_publication_identity_item",
                identity_row,
            ))
    seen_rows: dict[str, dict[str, Any]] = {}

    def add_seen(
        *,
        canonical_url: Any,
        doi: Any = None,
        title: Any = "",
        authors: Any = None,
        source_name: Any = "",
        disposition: str,
    ) -> None:
        try:
            url = canonicalize_http_url(canonical_url) if canonical_url else ""
            normalized_doi = normalize_doi(doi) if doi else ""
        except ContractError:
            return
        if not url and not normalized_doi:
            return
        identity = "doi:" + normalized_doi if normalized_doi else "url:" + url
        seen_id = "extseen_" + stable_hash(identity)
        normalized_title, normalized_authors = normalize_title_authors(title, authors)
        seen_rows.setdefault(seen_id, {
            "external_publication_seen_id": seen_id,
            "identity": identity,
            "canonical_url": url or None,
            "doi": normalized_doi or None,
            "title": str(title or "")[:500],
            "authors": [str(value).strip()[:240] for value in (authors if isinstance(authors, list) else []) if str(value).strip()],
            "normalized_title": normalized_title,
            "normalized_authors": normalized_authors,
            "source_name": str(source_name or "")[:300],
            "external_publication_id": "",
            "seen_disposition": disposition,
            "first_research_request_id": request_id,
            "latest_research_request_id": request_id,
            "input_json_sha256": raw_input_sha256,
            "raw_input_json_sha256": raw_input_sha256,
            "first_seen_at": imported_at,
            "last_seen_at": imported_at,
            "seen_guard_version": SEEN_GUARD_VERSION,
            "updated_at": imported_at,
        })

    for row in valid:
        publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
        status = str((row.get("decision") or {}).get("import_status") or "")
        disposition = (
            "candidate" if status == "ready_for_region_talk_scoring"
            else "manual_review" if status == "manual_review_required"
            else "excluded"
        )
        add_seen(
            canonical_url=row.get("canonical_url"),
            doi=row.get("doi"),
            title=publication.get("title"),
            authors=publication.get("authors"),
            source_name=publication.get("source_name"),
            disposition=disposition,
        )
        identity = "doi:" + str(row.get("doi") or "") if row.get("doi") else "url:" + str(row.get("canonical_url") or "")
        seen_id = "extseen_" + stable_hash(identity)
        if seen_id in seen_rows:
            seen_rows[seen_id]["external_publication_id"] = row["external_publication_id"]
            seen_rows[seen_id]["canonical_evidence_urls"] = list(row.get("canonical_evidence_urls") or [])
    for item in payload.get("excluded") or []:
        if isinstance(item, dict):
            add_seen(
                canonical_url=item.get("canonical_url"),
                title=item.get("title"),
                source_name=item.get("source_name"),
                disposition="excluded",
            )
    for item in payload.get("unresolved") or []:
        if isinstance(item, dict):
            add_seen(
                canonical_url=item.get("url"),
                title=item.get("title_guess"),
                disposition="unresolved",
            )
    for seen_id, seen_row in sorted(seen_rows.items()):
        rows.append((
            "external_publication_seen_item:" + seen_id,
            "external_publication_seen_item",
            seen_row,
        ))
    sources_by_key: dict[str, dict[str, Any]] = {}
    for row in valid:
        publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
        source_assessment = row.get("source_assessment") if isinstance(row.get("source_assessment"), dict) else {}
        quality = row.get("quality_assessment") if isinstance(row.get("quality_assessment"), dict) else {}
        editorial_pack = row.get("editorial_pack") if isinstance(row.get("editorial_pack"), dict) else {}
        publisher_refs = {
            str(ref)
            for support in (editorial_pack.get("copy_support") or [])
            if isinstance(support, dict) and str(support.get("surface") or "") == "source_overview"
            for ref in (support.get("evidence_refs") or [])
            if str(ref)
        }
        publisher_evidence = [
            item for item in (row.get("evidence") or [])
            if isinstance(item, dict) and str(item.get("evidence_id") or "") in publisher_refs
        ]
        domain = str(publication.get("source_domain") or "").strip().lower()
        if not domain:
            continue
        canonical_key = "web:" + domain
        topic = "academic_publication" if str(quality.get("track") or "") == "scholarly" else "editorial_publication"
        source = sources_by_key.setdefault(canonical_key, {
            "external_publication_source_id": "extpubsrc_" + stable_hash(canonical_key),
            "canonical_source_key": canonical_key,
            "platform": "web",
            "source_title": publication.get("source_name") or domain,
            "source_url": "https://" + domain,
            "canonical_url": "https://" + domain,
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "source_topic_class": topic,
            "source_quick_class": "candidate_keep",
            "source_queue_status": "confirmed_external_publication_research",
            "fetch_status": "confirmed_external_publication_research",
            "source_externality_basis": source_assessment.get("externality_basis") or "",
            "externality_evidence_refs": source_assessment.get("externality_evidence_refs") or [],
            "publisher_source_overview": editorial_pack.get("source_overview") or "",
            "publisher_source_overview_evidence_refs_json": json.dumps(sorted(publisher_refs), ensure_ascii=False),
            "publisher_profile_evidence_json": json.dumps(publisher_evidence, ensure_ascii=False, separators=(",", ":")),
            "publisher_profile_seed_version": "region_talk_publisher_profile_seed_v1",
            "research_request_ids": [],
            "external_publication_ids": [],
            "updated_at": imported_at,
        })
        if request_id not in source["research_request_ids"]:
            source["research_request_ids"].append(request_id)
        if row["external_publication_id"] not in source["external_publication_ids"]:
            source["external_publication_ids"].append(row["external_publication_id"])
    for canonical_key, source in sorted(sources_by_key.items()):
        rows.append((
            "external_publication_source_item:" + source["external_publication_source_id"],
            "external_publication_source_item",
            source,
        ))
    for item in rejected:
        error_id = stable_hash(batch_id, item["index"], item.get("canonical_url"), item.get("errors"))
        rows.append((
            "external_publication_import_error_item:" + error_id,
            "external_publication_import_error_item",
            {**item, "batch_id": batch_id, "request_id": request_id, "imported_at": imported_at},
        ))
    for item in conflicts:
        error_id = stable_hash(batch_id, "conflict", item["index"], item.get("identity_keys"))
        rows.append((
            "external_publication_import_error_item:" + error_id,
            "external_publication_import_error_item",
            {**item, "batch_id": batch_id, "request_id": request_id, "imported_at": imported_at},
        ))
    new_intake_ids = sorted(row["external_publication_id"] for row in valid)
    replay_ids = sorted({str(item.get("external_publication_id") or "") for item in replayed if item.get("external_publication_id")})
    batch = {
        "batch_id": batch_id,
        "request_id": request_id,
        "schema_version": payload.get("schema_version"),
        "import_version": IMPORT_VERSION,
        "input_json_sha256": raw_input_sha256,
        "raw_input_json_sha256": raw_input_sha256,
        "imported_at": imported_at,
        "candidate_rows_received": len(candidates),
        "candidate_rows_valid": len(valid),
        "candidate_rows_rejected": len(rejected),
        "identity_conflict_count": len(conflicts),
        "replay_count": len(replayed),
        "replay_ids": replay_ids,
        "new_intake_count": len(new_intake_ids),
        "new_intake_ids": new_intake_ids,
        "execution_blocked": bool(rejected or conflicts),
        "external_sources_staged": len(sources_by_key),
        "ready_for_region_talk_scoring": sum(1 for row in valid if row["decision"]["import_status"] == "ready_for_region_talk_scoring"),
        "manual_or_blocked": sum(1 for row in valid if row["decision"]["import_status"] != "ready_for_region_talk_scoring"),
        "duplicate_seen_count": duplicate_seen_count,
        "duplicate_seen_rejected": duplicate_seen_count,
        "seen_publication_rows_staged": len(seen_rows),
        "seen_guard_snapshot_id": str(duplicate_guard.get("snapshot_id") or "") if duplicate_guard else "",
        "coverage": payload.get("coverage") if isinstance(payload.get("coverage"), list) else [],
        "run_uncertainties": payload.get("run_uncertainties") if isinstance(payload.get("run_uncertainties"), list) else [],
    }
    rows.append(("external_publication_import_batch:" + batch_id, "external_publication_import_batch", batch))
    return {
        "batch": batch,
        "valid": valid,
        "rejected": rejected,
        "replayed": replayed,
        "conflicts": conflicts,
        "ydb_rows": rows,
    }


def write_ydb(rows: list[tuple[str, str, dict[str, Any]]]) -> int:
    """Atomically write generic JSON rows (used by the separate review tool)."""
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    query_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> int:
        query = session.prepare(query_text)
        if not rows:
            return 0
        tx = session.transaction(ydb.SerializableReadWrite())
        for index, (pk, kind, row) in enumerate(rows):
            tx.execute(
                query,
                {
                    "$pk": pk,
                    "$kind": kind,
                    "$payload_json": json.dumps(row, ensure_ascii=False, separators=(",", ":")),
                    "$updated_at": str(row.get("updated_at") or row.get("imported_at") or utc_now_iso()),
                },
                commit_tx=index == len(rows) - 1,
            )
        return len(rows)

    try:
        return int(pool.retry_operation_sync(op) or 0)
    finally:
        driver.stop(timeout=5)


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return dict(value or {})


def execute_import(prepared: dict[str, Any]) -> dict[str, Any]:
    """Commit one prepared intake batch with serializable reservations.

    The request-id receipt and all exact identity reservations are read before
    any mutation and every row is committed in the same transaction.  Thus a
    rejected batch, byte conflict, identity race, or transaction failure cannot
    leave a partial intake behind.
    """
    batch = prepared.get("batch") if isinstance(prepared.get("batch"), dict) else {}
    if batch.get("execution_blocked"):
        raise ContractError("execute blocked: batch contains rejected or conflicting candidates")
    rows = list(prepared.get("ydb_rows") or [])
    batch_pk = "external_publication_import_batch:" + str(batch.get("batch_id") or "")
    raw_sha256 = str(batch.get("input_json_sha256") or batch.get("raw_input_json_sha256") or "")
    identity_rows = [(pk, row) for pk, kind, row in rows if kind == "external_publication_identity_item"]

    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    select_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    upsert_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> dict[str, Any]:
        select = session.prepare(select_text)
        upsert = session.prepare(upsert_text)
        tx = session.transaction(ydb.SerializableReadWrite())

        response = tx.execute(select, {"$pk": batch_pk}, commit_tx=False)
        existing_rows = response[0].rows if response else []
        if existing_rows:
            existing = _json_payload(existing_rows[0].payload_json)
            if str(existing.get("input_json_sha256") or existing.get("raw_input_json_sha256") or "") != raw_sha256:
                tx.rollback()
                raise ContractError(
                    "request_id conflict: durable batch has different raw input SHA-256"
                )
            tx.rollback()
            replay_ids = sorted({
                str(value) for value in (existing.get("new_intake_ids") or [])
                if str(value)
            }.union(str(value) for value in (existing.get("replay_ids") or []) if str(value)))
            return {
                "status": "identical_replay",
                "written_ydb_rows": 0,
                "new_intake_count": 0,
                "new_intake_ids": [],
                "replay_count": len(replay_ids),
                "replay_ids": replay_ids,
                "conflict_count": 0,
            }

        for pk, intended in identity_rows:
            response = tx.execute(select, {"$pk": pk}, commit_tx=False)
            existing_rows = response[0].rows if response else []
            if not existing_rows:
                continue
            existing = _json_payload(existing_rows[0].payload_json)
            tx.rollback()
            raise ContractError(
                "identity reservation conflict: "
                + str(intended.get("identity_type") or "unknown")
                + " already belongs to "
                + str(existing.get("external_publication_id") or "another publication")
            )

        if not rows:
            tx.rollback()
            raise ContractError("prepared import has no durable rows")
        for index, (pk, kind, row) in enumerate(rows):
            tx.execute(
                upsert,
                {
                    "$pk": pk,
                    "$kind": kind,
                    "$payload_json": json.dumps(row, ensure_ascii=False, separators=(",", ":")),
                    "$updated_at": str(row.get("updated_at") or row.get("imported_at") or utc_now_iso()),
                },
                commit_tx=index == len(rows) - 1,
            )
        new_ids = sorted(str(value) for value in (batch.get("new_intake_ids") or []) if str(value))
        replay_ids = sorted(str(value) for value in (batch.get("replay_ids") or []) if str(value))
        return {
            "status": "committed",
            "written_ydb_rows": len(rows),
            "new_intake_count": len(new_ids),
            "new_intake_ids": new_ids,
            "replay_count": len(replay_ids),
            "replay_ids": replay_ids,
            "conflict_count": 0,
        }

    try:
        return dict(pool.retry_operation_sync(op) or {})
    finally:
        driver.stop(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate/stage external publication research for Region Talk")
    parser.add_argument("input", type=Path, help="JSON result conforming to region_talk_external_research.v1")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--report", type=Path, default=ROOT / "artifacts" / "codex" / "region-talk-external-publication-import.json")
    parser.add_argument("--request-input", type=Path, help="Legacy optional request sidecar; live YDB duplicate checking is always applied on --execute")
    parser.add_argument(
        "--expected-input-sha256",
        help="Optional externally computed SHA-256 of the exact input bytes; mismatch fails before preparation",
    )
    parser.add_argument("--execute", action="store_true", help="Write idempotent staging rows to YDB; default is validation/dry-run")
    parser.add_argument(
        "--no-publish-registry",
        dest="publish_registry",
        action="store_false",
        default=True,
        help=(
            "Skip the post-import Object Storage registry publish. Use this with a "
            "YDB-only service account; YDB staging remains durable and the registry "
            "can be published separately."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    raw_bytes = args.input.read_bytes()
    input_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    if args.expected_input_sha256 and str(args.expected_input_sha256).strip().lower() != input_sha256:
        raise ContractError("exact input SHA-256 does not match --expected-input-sha256")
    try:
        duplicate_guard = load_duplicate_guard(args.request_input) if args.request_input else None
        if args.execute:
            # Import-time YDB state is authoritative.  This closes the race between
            # an older research snapshot and a later import, and removes any need
            # for the operator to prepare a sidecar before launching saved prompts.
            from scripts.region_talk_external_research_request import read_seen_from_ydb

            live_seen = read_seen_from_ydb(20000)
            duplicate_guard = merge_duplicate_guards(
                duplicate_guard,
                duplicate_guard_from_seen_publications(live_seen),
            )
        payload = json.loads(raw_bytes.decode("utf-8"))
        result = prepare_import(payload, duplicate_guard=duplicate_guard, input_json_sha256=input_sha256)
    except ContractError as exc:
        identity_conflict = "identity" in str(exc).lower() or "map" in str(exc).lower()
        report = {
            "input_json_sha256": input_sha256,
            "executed": bool(args.execute),
            "execution_status": (
                "conflict_no_write" if args.execute and identity_conflict
                else "validation_failed_no_write" if args.execute
                else "validation_failed"
            ),
            "execution_error": str(exc),
            "planned_ydb_rows": 0,
            "written_ydb_rows": 0,
            "new_intake_count": 0,
            "new_intake_ids": [],
            "replay_count": 0,
            "replay_ids": [],
            "conflict_count": 1 if args.execute and identity_conflict else 0,
            "registry_publication_enabled": False,
            "registry_publication": None,
            "registry_publication_error": None,
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 4 if args.execute and identity_conflict else 2
    execution = {
        "status": "validated",
        "written_ydb_rows": 0,
        "new_intake_count": int(result["batch"].get("new_intake_count") or 0),
        "new_intake_ids": list(result["batch"].get("new_intake_ids") or []),
        "replay_count": int(result["batch"].get("replay_count") or 0),
        "replay_ids": list(result["batch"].get("replay_ids") or []),
        "conflict_count": int(result["batch"].get("identity_conflict_count") or 0),
    }
    exit_code = 0
    execution_error = ""
    if args.execute:
        if result["batch"].get("execution_blocked"):
            execution.update({
                "status": "rejected_no_write",
                "new_intake_count": 0,
                "new_intake_ids": [],
            })
            exit_code = 2
        else:
            try:
                execution = execute_import(result)
            except ContractError as exc:
                execution = {
                    "status": "conflict_no_write",
                    "written_ydb_rows": 0,
                    "new_intake_count": 0,
                    "new_intake_ids": [],
                    "replay_count": int(result["batch"].get("replay_count") or 0),
                    "replay_ids": list(result["batch"].get("replay_ids") or []),
                    "conflict_count": max(1, int(result["batch"].get("identity_conflict_count") or 0)),
                }
                execution_error = str(exc)
                exit_code = 4
    registry_publication: dict[str, Any] | None = None
    registry_error = ""
    execution_succeeded = execution["status"] in {"committed", "identical_replay"}
    registry_publication_enabled = bool(args.execute and args.publish_registry and execution_succeeded)
    if registry_publication_enabled:
        try:
            from scripts.region_talk_external_research_registry import publish_current_registry

            registry_publication = publish_current_registry(seen_limit=20000)
        except Exception as exc:  # YDB rows are already durable; expose repair evidence.
            registry_error = f"{type(exc).__name__}: {exc}"
    report = {
        "batch": result["batch"],
        "input_json_sha256": input_sha256,
        "valid_ids": sorted(row["external_publication_id"] for row in result["valid"]),
        "rejected": result["rejected"],
        "replayed": result["replayed"],
        "conflicts": result["conflicts"],
        "planned_ydb_rows": len(result["ydb_rows"]),
        "written_ydb_rows": int(execution.get("written_ydb_rows") or 0),
        "executed": bool(args.execute),
        "execution_status": execution.get("status"),
        "execution_error": execution_error or None,
        "new_intake_count": int(execution.get("new_intake_count") or 0),
        "new_intake_ids": sorted(execution.get("new_intake_ids") or []),
        "replay_count": int(execution.get("replay_count") or 0),
        "replay_ids": sorted(execution.get("replay_ids") or []),
        "conflict_count": int(execution.get("conflict_count") or 0),
        "live_duplicate_guard_applied": bool(args.execute),
        "registry_publication_enabled": registry_publication_enabled,
        "registry_publication": registry_publication,
        "registry_publication_error": registry_error or None,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if registry_error:
        return 3
    if exit_code:
        return exit_code
    return 0 if not result["rejected"] and not result["conflicts"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
