#!/usr/bin/env python3
"""Fail-closed, research-only source-local Telegram keyword harness.

The module is standalone and uses only the standard library for manifest,
replay, and reporting.  The optional live adapter imports Telethon lazily.
Network access is possible only with an explicit read-consent flag and a
role-scoped DISCOVERY1/2 bundle.  Replay and reporting are always offline.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import random
import re
import shlex
import subprocess
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit, urlunsplit


SCHEMA_VERSION = "region_talk_low_frequency_research_v1"
RAW_SCHEMA_VERSION = "region_talk_low_frequency_raw_v1"
WINDOW_DAYS = 365
STRUCTURE_A = "A_current"
STRUCTURE_B = "B_expanded"
STRUCTURE_C = "C_continuation"
STRUCTURE_D = "D_anchor_window_replay"
STRUCTURE_NAMES = (STRUCTURE_A, STRUCTURE_B, STRUCTURE_C, STRUCTURE_D)
URL_TAXONOMY = (
    "baseline_replay",
    "experiment_repeat",
    "dedup_eligible",
    "new",
    "new_KO",
    "downstream_accepted",
)

ALLOWED_AUTH_ENV_BY_ROLE = {
    "DISCOVERY1": "TELEGRAM_AUTH_BUNDLE_DISCOVERY1",
    "DISCOVERY2": "TELEGRAM_AUTH_BUNDLE_DISCOVERY2",
}
FORBIDDEN_AUTH_ENVS = (
    "TELEGRAM_AUTH_BUNDLE_E2E",
    "TELEGRAM_SESSION",
    "TELEGRAM_AUTH_BUNDLE_S22",
)

# These are absolute safety limits, not defaults that an input manifest may
# raise.  A manifest can only reduce them.
HARD_REQUEST_CEILINGS: dict[str, int | float] = {
    "total_requests": 40,
    "per_source_requests": 10,
    "per_source_query_requests": 1,
    "results_per_request": 20,
    "request_timeout_seconds": 30,
    "error_rate_percent": 5.0,
    "error_rate_min_requests": 20,
    "max_timeouts": 1,
    "max_entity_resolves": 0,
    "concurrency": 1,
    "retries": 0,
}
HUMAN_PACING = {
    "same_source_min_seconds": 5.0,
    "same_source_max_seconds": 9.0,
    "source_change_min_seconds": 10.0,
    "source_change_max_seconds": 20.0,
    "no_new_request_after_seconds": 900.0,
    "batch_wall_seconds": 1200.0,
}


class ResearchHarnessError(RuntimeError):
    """Base exception for a fail-closed harness stop."""


class SafetyGateError(ResearchHarnessError):
    """A required explicit safety gate was absent or invalid."""


class ManifestError(ResearchHarnessError):
    """The research manifest is incomplete, invalid, or tampered with."""


class RequestCeilingReached(ResearchHarnessError):
    """A hard request ceiling was reached before another request."""


class CaptureAborted(ResearchHarnessError):
    """A mandatory stop rule aborted capture."""

    def __init__(self, reason: str, summary: Mapping[str, Any] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.summary = dict(summary or {})


class TelegramReadAdapter(Protocol):
    """Minimal one-request-at-a-time adapter contract used by ``run_capture``."""

    def search(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return one raw response for one cached-peer source-local search."""

    def close(self) -> None:
        """Release the one role-scoped client without changing its session."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    payload = value if isinstance(value, str) else canonical_json(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("timestamp is required")
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: str | datetime) -> str:
    return parse_utc(value).isoformat().replace("+00:00", "Z")


def canonical_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("t.me/"):
        raw = "https://" + raw
    parts = urlsplit(raw)
    if not parts.netloc:
        return raw.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    host = parts.netloc.lower()
    if host in {"telegram.me", "www.telegram.me", "www.t.me"}:
        host = "t.me"
    scheme = "https" if host == "t.me" else parts.scheme.lower()
    path = "/".join(segment for segment in parts.path.split("/") if segment)
    if host == "t.me" and path:
        segments = path.split("/")
        segments[0] = segments[0].lower()
        path = "/".join(segments)
    return urlunsplit((scheme, host, "/" + path if path else "", "", "")).rstrip("/")


def _normalise_queries(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        query = " ".join(str(value or "").split())
        if query and query not in seen:
            seen.add(query)
            result.append(query)
    return result


def query_hashes(queries: Mapping[str, Sequence[str]]) -> dict[str, list[dict[str, str]]]:
    return {
        name: [{"query": query, "sha256": stable_hash(query)} for query in _normalise_queries(queries.get(name, []))]
        for name in (STRUCTURE_A, STRUCTURE_B, STRUCTURE_C)
    }


def bounded_request_ceilings(overrides: Mapping[str, Any] | None = None) -> dict[str, int | float]:
    result = dict(HARD_REQUEST_CEILINGS)
    for name, value in dict(overrides or {}).items():
        if name not in HARD_REQUEST_CEILINGS:
            raise ManifestError(f"unknown request ceiling: {name}")
        hard = HARD_REQUEST_CEILINGS[name]
        try:
            candidate: int | float = float(value) if isinstance(hard, float) else int(value)
        except (TypeError, ValueError) as exc:
            raise ManifestError(f"invalid request ceiling {name}={value!r}") from exc
        if candidate < 0 or candidate > hard:
            raise ManifestError(f"request ceiling {name}={candidate} exceeds hard maximum {hard}")
        result[name] = candidate
    if result["concurrency"] != 1 or result["retries"] != 0 or result["max_entity_resolves"] != 0:
        raise ManifestError("capture requires concurrency=1, retries=0, and max_entity_resolves=0")
    if result["total_requests"] < 1 or result["results_per_request"] < 1:
        raise ManifestError("positive total_requests and results_per_request are required")
    return result


def _normalise_sources(sources: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not sources:
        raise ManifestError("at least one cached source identity is required")
    normalised: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_peers: set[int] = set()
    for raw in sources:
        source_id = str(raw.get("source_id") or "").strip()
        if not source_id or source_id in seen_ids:
            raise ManifestError("every source_id must be non-empty and unique")
        try:
            peer_id = int(raw.get("peer_id"))
            access_hash = int(raw.get("access_hash"))
        except (TypeError, ValueError) as exc:
            raise ManifestError(f"source {source_id!r} requires cached peer_id/access_hash") from exc
        if not peer_id or not access_hash or peer_id in seen_peers:
            raise ManifestError(f"source {source_id!r} has an invalid or duplicate cached peer")
        seen_ids.add(source_id)
        seen_peers.add(peer_id)
        normalised.append(
            {
                "source_id": source_id,
                "username": str(raw.get("username") or "").strip().lstrip("@").lower(),
                "peer_id": peer_id,
                "access_hash": access_hash,
                "identity_hash": stable_hash([source_id, peer_id, access_hash]),
                "continuation_cursor": max(0, int(raw.get("continuation_cursor") or 0)),
            }
        )
    return sorted(normalised, key=lambda item: item["source_id"])


def build_manifest(
    *,
    code_sha: str,
    t0: str | datetime,
    sources: Sequence[Mapping[str, Any]],
    current_queries: Sequence[str],
    expanded_queries: Sequence[str],
    continuation_queries: Sequence[str] | None = None,
    anchors: Sequence[Mapping[str, Any]] | None = None,
    k0_urls: Sequence[str] = (),
    request_ceilings: Mapping[str, Any] | None = None,
    window_days: int = WINDOW_DAYS,
    anchor_window_days: int = 7,
) -> dict[str, Any]:
    """Build a byte-stable manifest from explicit experiment inputs."""

    sha = str(code_sha or "").strip().lower()
    if not sha:
        raise ManifestError("code_sha is required")
    if int(window_days) != WINDOW_DAYS:
        raise ManifestError("the research window is fixed at 365 days")
    if not 0 <= int(anchor_window_days) <= 30:
        raise ManifestError("anchor_window_days must be between 0 and 30")
    t0_dt = parse_utc(t0)
    identities = _normalise_sources(sources)
    queries = {
        STRUCTURE_A: _normalise_queries(current_queries),
        STRUCTURE_B: _normalise_queries(expanded_queries),
        STRUCTURE_C: _normalise_queries(continuation_queries if continuation_queries is not None else expanded_queries),
    }
    if not queries[STRUCTURE_A] or not queries[STRUCTURE_B]:
        raise ManifestError("current and expanded query banks must be non-empty")
    k0 = sorted({url for url in (canonical_url(value) for value in k0_urls) if url})
    source_ids = {source["source_id"] for source in identities}
    normalised_anchors: list[dict[str, Any]] = []
    for raw in anchors or []:
        source_id = str(raw.get("source_id") or "").strip()
        url = canonical_url(raw.get("url") or raw.get("post_url"))
        published_at = iso_utc(str(raw.get("published_at") or raw.get("post_date") or ""))
        if source_id not in source_ids or not url:
            raise ManifestError("every anchor requires a known source_id, URL, and timestamp")
        normalised_anchors.append({"source_id": source_id, "url": url, "published_at": published_at})
    normalised_anchors.sort(key=lambda row: (row["source_id"], row["published_at"], row["url"]))

    body: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "code_sha": sha,
        "T0": iso_utc(t0_dt),
        "window_days": WINDOW_DAYS,
        "window_start": iso_utc(t0_dt - timedelta(days=WINDOW_DAYS)),
        "K0_urls": k0,
        "K0_hash": stable_hash(k0),
        "source_identities": identities,
        "source_identities_hash": stable_hash(identities),
        "query_hashes": query_hashes(queries),
        "request_ceilings": bounded_request_ceilings(request_ceilings),
        "human_pacing": dict(HUMAN_PACING),
        "structures": {
            STRUCTURE_A: {"kind": "source_local_current", "queries": queries[STRUCTURE_A]},
            STRUCTURE_B: {"kind": "source_local_expanded", "queries": queries[STRUCTURE_B]},
            STRUCTURE_C: {
                "kind": "source_local_continuation",
                "scheduler": "source_round_robin_v1",
                "queries": queries[STRUCTURE_C],
                "cursor_by_source": {source["source_id"]: source["continuation_cursor"] for source in identities},
            },
            STRUCTURE_D: {
                "kind": "offline_anchor_window_replay",
                "network_requests": 0,
                "anchor_window_days": int(anchor_window_days),
                "anchors": normalised_anchors,
            },
        },
    }
    body["manifest_hash"] = stable_hash(body)
    return body


create_manifest = build_manifest


def validate_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    copy = dict(manifest)
    expected_hash = str(copy.pop("manifest_hash", ""))
    if copy.get("schema") != SCHEMA_VERSION or not expected_hash:
        raise ManifestError("unsupported or unhashed manifest")
    if stable_hash(copy) != expected_hash:
        raise ManifestError("manifest hash mismatch")
    if int(copy.get("window_days") or 0) != WINDOW_DAYS:
        raise ManifestError("manifest does not use the fixed 365-day window")
    if not str(copy.get("code_sha") or "").strip() or iso_utc(str(copy.get("T0") or "")) != copy.get("T0"):
        raise ManifestError("manifest requires canonical code_sha and T0")
    expected_start = iso_utc(parse_utc(str(copy["T0"])) - timedelta(days=WINDOW_DAYS))
    if copy.get("window_start") != expected_start:
        raise ManifestError("manifest 365-day window_start mismatch")
    ceilings = bounded_request_ceilings(copy.get("request_ceilings") or {})
    if ceilings != copy.get("request_ceilings"):
        raise ManifestError("manifest request ceilings are not canonical")
    if copy.get("human_pacing") != HUMAN_PACING:
        raise ManifestError("manifest human pacing contract mismatch")
    sources = _normalise_sources(copy.get("source_identities") or [])
    if sources != copy.get("source_identities"):
        raise ManifestError("manifest source identities are not canonical cached peers")
    if stable_hash(sources) != copy.get("source_identities_hash"):
        raise ManifestError("manifest source identity hash mismatch")
    k0 = sorted({url for url in (canonical_url(value) for value in copy.get("K0_urls") or []) if url})
    if k0 != copy.get("K0_urls") or stable_hash(k0) != copy.get("K0_hash"):
        raise ManifestError("manifest K0 hash mismatch")
    structures = copy.get("structures")
    if not isinstance(structures, Mapping) or set(structures) != set(STRUCTURE_NAMES):
        raise ManifestError("manifest must contain exactly the A/B/C/D research structures")
    if any(not isinstance(structures[name], Mapping) for name in STRUCTURE_NAMES):
        raise ManifestError("every research structure must be an object")
    queries = {
        name: _normalise_queries(structures[name].get("queries") or [])
        for name in (STRUCTURE_A, STRUCTURE_B, STRUCTURE_C)
    }
    if query_hashes(queries) != copy.get("query_hashes"):
        raise ManifestError("manifest query hash mismatch")
    structure_d = structures[STRUCTURE_D]
    if structure_d.get("kind") != "offline_anchor_window_replay" or structure_d.get("network_requests") != 0:
        raise ManifestError("structure D must remain offline-only")
    return dict(manifest)


class ContinuationScheduler:
    """Deterministic fair scheduler: at most one query per source per round."""

    def __init__(self, sources: Sequence[Mapping[str, Any]], queries: Sequence[str], cursors: Mapping[str, Any] | None = None):
        bank = _normalise_queries(queries)
        cursor_map = dict(cursors or {})
        lanes: deque[dict[str, Any]] = deque()
        for source in sorted(sources, key=lambda item: str(item.get("source_id") or "")):
            source_id = str(source.get("source_id") or "")
            start = max(0, int(cursor_map.get(source_id, source.get("continuation_cursor") or 0)))
            if start < len(bank):
                lanes.append({"source": source, "index": start})
        self._queries = bank
        self._lanes = lanes

    def __iter__(self) -> "ContinuationScheduler":
        return self

    def __next__(self) -> dict[str, Any]:
        if not self._lanes:
            raise StopIteration
        lane = self._lanes.popleft()
        index = int(lane["index"])
        source = lane["source"]
        item = {
            "source_id": str(source["source_id"]),
            "username": str(source.get("username") or ""),
            "peer_id": int(source["peer_id"]),
            "access_hash": int(source["access_hash"]),
            "structure": STRUCTURE_C,
            "query": self._queries[index],
            "continuation_index": index,
        }
        lane["index"] = index + 1
        if lane["index"] < len(self._queries):
            self._lanes.append(lane)
        return item


def schedule_continuations(
    sources: Sequence[Mapping[str, Any]],
    queries: Sequence[str],
    *,
    cursors: Mapping[str, Any] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in ContinuationScheduler(sources, queries, cursors):
        if limit is not None and len(result) >= max(0, int(limit)):
            break
        result.append(item)
    return result


def build_request_schedule(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    checked = validate_manifest(manifest)
    sources = checked["source_identities"]
    structures = checked["structures"]
    schedule: list[dict[str, Any]] = []
    for structure_name in (STRUCTURE_A, STRUCTURE_B):
        for query in structures[structure_name]["queries"]:
            for source in sources:
                schedule.append(
                    {
                        "source_id": source["source_id"],
                        "username": source.get("username") or "",
                        "peer_id": source["peer_id"],
                        "access_hash": source["access_hash"],
                        "structure": structure_name,
                        "query": query,
                    }
                )
    continuation = structures[STRUCTURE_C]
    schedule.extend(
        schedule_continuations(
            sources,
            continuation["queries"],
            cursors=continuation.get("cursor_by_source") or {},
        )
    )
    return schedule


@dataclass
class RequestBudget:
    ceilings: Mapping[str, Any]
    attempted: int = 0
    errors: int = 0
    timeouts: int = 0
    per_source: Counter[str] = field(default_factory=Counter)
    per_source_query: Counter[tuple[str, str, str]] = field(default_factory=Counter)

    def __post_init__(self) -> None:
        self.ceilings = bounded_request_ceilings(self.ceilings)

    def reserve(self, request: Mapping[str, Any]) -> int:
        source_id = str(request.get("source_id") or "")
        structure = str(request.get("structure") or "")
        query = str(request.get("query") or "")
        if self.attempted >= int(self.ceilings["total_requests"]):
            raise RequestCeilingReached("total_requests")
        if self.per_source[source_id] >= int(self.ceilings["per_source_requests"]):
            raise RequestCeilingReached(f"per_source_requests:{source_id}")
        key = (source_id, structure, query)
        if self.per_source_query[key] >= int(self.ceilings["per_source_query_requests"]):
            raise RequestCeilingReached(f"per_source_query_requests:{source_id}:{structure}:{query}")
        self.attempted += 1
        self.per_source[source_id] += 1
        self.per_source_query[key] += 1
        return self.attempted

    def record_error(self, *, timeout: bool = False) -> None:
        self.errors += 1
        if timeout:
            self.timeouts += 1
            if self.timeouts > int(self.ceilings["max_timeouts"]):
                raise CaptureAborted("second_timeout", self.summary())
        minimum = int(self.ceilings["error_rate_min_requests"])
        if self.attempted >= minimum:
            self.assert_error_rate()

    def assert_error_rate(self) -> None:
        rate = 100.0 * self.errors / max(1, self.attempted)
        if rate > float(self.ceilings["error_rate_percent"]):
            raise CaptureAborted("error_rate_above_5_percent", self.summary())

    def summary(self) -> dict[str, Any]:
        return {
            "requests_attempted": self.attempted,
            "errors": self.errors,
            "timeouts": self.timeouts,
            "error_rate_percent": round(100.0 * self.errors / max(1, self.attempted), 6),
            "per_source": dict(sorted(self.per_source.items())),
        }


RequestGovernor = RequestBudget


def select_auth_bundle(role: str, environ: Mapping[str, str] | None = None) -> tuple[str, str]:
    env = os.environ if environ is None else environ
    normalised = str(role or "").strip().upper()
    env_name = ALLOWED_AUTH_ENV_BY_ROLE.get(normalised)
    if not env_name:
        raise SafetyGateError("auth role must be explicitly DISCOVERY1 or DISCOVERY2")
    bundle = str(env.get(env_name) or "")
    if not bundle:
        raise SafetyGateError(f"required role-scoped environment variable is missing: {env_name}")
    return env_name, bundle


def _error_text(value: Any) -> str:
    if isinstance(value, BaseException):
        return f"{value.__class__.__name__}: {value}"
    if isinstance(value, Mapping):
        candidates = [value.get("error"), value.get("status"), value.get("event"), value.get("error_type")]
        return " ".join(str(item or "") for item in candidates)
    return str(value or "")


def is_flood_wait(value: Any) -> bool:
    compact = "".join(ch for ch in _error_text(value).lower() if ch.isalnum())
    return "floodwait" in compact or "floodcontrol" in compact


def is_entity_resolve_attempt(value: Any) -> bool:
    compact = "".join(ch for ch in _error_text(value).lower() if ch.isalnum())
    return "entityresolve" in compact or "resolveentity" in compact or "usernameResolve".lower() in compact


def append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    """Append one complete JSON object without truncating existing bytes."""

    path = Path(path)
    if not path.parent.exists():
        raise FileNotFoundError(f"capture parent directory does not exist: {path.parent}")
    prefix = b""
    if path.exists() and path.stat().st_size:
        with path.open("rb") as existing:
            existing.seek(-1, os.SEEK_END)
            if existing.read(1) != b"\n":
                prefix = b"\n"
    payload = prefix + canonical_json(dict(row)).encode("utf-8") + b"\n"
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _public_request(request: Mapping[str, Any], *, request_index: int, result_limit: int) -> dict[str, Any]:
    return {
        "request_index": request_index,
        "source_id": str(request["source_id"]),
        "username": str(request.get("username") or ""),
        "peer_id": int(request["peer_id"]),
        "structure": str(request["structure"]),
        "query": str(request["query"]),
        "query_hash": stable_hash(str(request["query"])),
        "result_limit": int(result_limit),
    }


def _adapter_request(request: Mapping[str, Any], *, request_index: int, result_limit: int, auth_env_name: str) -> dict[str, Any]:
    public = _public_request(request, request_index=request_index, result_limit=result_limit)
    public.update(
        {
            "access_hash": int(request["access_hash"]),
            "auth_env_name": auth_env_name,
            "cached_peer_only": True,
            "allow_entity_resolve": False,
            "retry_count": 0,
        }
    )
    return public


def _capture_row(public_request: Mapping[str, Any], manifest_hash: str, *, response: Mapping[str, Any] | None = None, error: Any = None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "schema": RAW_SCHEMA_VERSION,
        "manifest_hash": manifest_hash,
        "request": dict(public_request),
    }
    if response is not None:
        row["response"] = dict(response)
    if error is not None:
        row["error"] = {"type": error.__class__.__name__, "message": str(error)}
    return row


def run_capture(
    manifest: Mapping[str, Any],
    raw_capture_path: Path,
    adapter: TelegramReadAdapter,
    *,
    allow_telegram_read: bool = False,
    auth_role: str = "",
    environ: Mapping[str, str] | None = None,
    pace_requests: bool = False,
    sleep_fn: Any = time.sleep,
    monotonic_fn: Any = time.monotonic,
    uniform_fn: Any = random.SystemRandom().uniform,
) -> dict[str, Any]:
    """Execute bounded serial reads, append every raw outcome, and never retry."""

    if not allow_telegram_read:
        raise SafetyGateError("capture requires explicit --allow-telegram-read")
    checked = validate_manifest(manifest)
    auth_env_name, _ = select_auth_bundle(auth_role, environ)
    if bool(getattr(adapter, "requires_human_pacing", False)) and not pace_requests:
        raise SafetyGateError("the live Telegram adapter requires human-like pacing")
    schedule = build_request_schedule(checked)
    budget = RequestBudget(checked["request_ceilings"])
    result_limit = int(checked["request_ceilings"]["results_per_request"])
    stop_reason = "schedule_exhausted"
    capture_started = monotonic_fn()
    last_source_id = ""

    for request in schedule:
        elapsed = monotonic_fn() - capture_started
        if elapsed >= float(HUMAN_PACING["no_new_request_after_seconds"]):
            stop_reason = "no_new_request_after_wall_cutoff"
            break
        try:
            request_index = budget.reserve(request)
        except RequestCeilingReached as exc:
            if str(exc) == "total_requests":
                stop_reason = "total_request_ceiling"
                break
            # A source/query ceiling skips that item; it never causes a call.
            stop_reason = "partial_request_ceiling"
            continue
        public = _public_request(request, request_index=request_index, result_limit=result_limit)
        if pace_requests:
            source_changed = bool(last_source_id and last_source_id != str(request.get("source_id") or ""))
            minimum = HUMAN_PACING["source_change_min_seconds" if source_changed else "same_source_min_seconds"]
            maximum = HUMAN_PACING["source_change_max_seconds" if source_changed else "same_source_max_seconds"]
            delay = round(float(uniform_fn(float(minimum), float(maximum))), 3)
            sleep_fn(delay)
            public["paced_delay_seconds"] = delay
            public["pacing_class"] = "source_change" if source_changed else "same_source_or_first"
        private = _adapter_request(
            request,
            request_index=request_index,
            result_limit=result_limit,
            auth_env_name=auth_env_name,
        )
        try:
            response = adapter.search(private)
            last_source_id = str(request.get("source_id") or "")
            if not isinstance(response, Mapping):
                raise TypeError("adapter response must be a mapping")
            append_jsonl(raw_capture_path, _capture_row(public, checked["manifest_hash"], response=response))
            if is_flood_wait(response):
                raise CaptureAborted("flood_wait", budget.summary())
            if is_entity_resolve_attempt(response) or response.get("entity_resolved") is True:
                raise CaptureAborted("entity_resolve_attempt", budget.summary())
            messages = response.get("messages") or []
            if not isinstance(messages, list) or len(messages) > result_limit:
                raise CaptureAborted("adapter_result_limit_violation", budget.summary())
            if response.get("error"):
                budget.record_error(timeout="timeout" in _error_text(response).lower())
        except CaptureAborted:
            raise
        except BaseException as exc:  # one raw error record, then stop-rule accounting
            append_jsonl(raw_capture_path, _capture_row(public, checked["manifest_hash"], error=exc))
            if is_flood_wait(exc):
                raise CaptureAborted("flood_wait", budget.summary()) from exc
            if is_entity_resolve_attempt(exc):
                raise CaptureAborted("entity_resolve_attempt", budget.summary()) from exc
            budget.record_error(timeout=isinstance(exc, (TimeoutError, subprocess.TimeoutExpired)))

    # The minimum-request guard prevents one early transient from being
    # misrepresented as a stable rate while capture is still in progress.  At
    # normal exhaustion, however, the observed final rate is always enforced.
    budget.assert_error_rate()
    summary = budget.summary()
    summary.update({
        "stop_reason": stop_reason,
        "manifest_hash": checked["manifest_hash"],
        "capture_elapsed_seconds": round(monotonic_fn() - capture_started, 3),
        "human_pacing_enabled": bool(pace_requests),
    })
    return summary


capture = run_capture


class CommandAdapter:
    """One subprocess invocation per request using a JSON stdin/stdout protocol."""

    def __init__(self, command: Sequence[str], *, timeout_seconds: int, selected_auth_env: str, environ: Mapping[str, str] | None = None):
        if not command:
            raise SafetyGateError("an explicit adapter command is required")
        self.command = list(command)
        self.timeout_seconds = int(timeout_seconds)
        source_env = os.environ if environ is None else environ
        # Do not inherit unrelated Telegram sessions into the adapter process.
        allowed_base = ("PATH", "HOME", "LANG", "LC_ALL", "PYTHONPATH", "SSL_CERT_FILE", "SSL_CERT_DIR")
        self.env = {key: str(source_env[key]) for key in allowed_base if source_env.get(key)}
        self.env[selected_auth_env] = str(source_env[selected_auth_env])

    def search(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        adapter_request = dict(request)
        adapter_request.pop("auth_bundle", None)
        completed = subprocess.run(
            self.command,
            input=canonical_json(adapter_request) + "\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout_seconds,
            check=False,
            env=self.env,
        )
        if completed.returncode:
            raise RuntimeError(f"adapter exited {completed.returncode}: {completed.stderr.strip()[:500]}")
        lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if len(lines) != 1:
            raise RuntimeError("adapter must emit exactly one JSON response line")
        response = json.loads(lines[0])
        if not isinstance(response, dict):
            raise TypeError("adapter JSON response must be an object")
        return response

    def close(self) -> None:
        return None


def _decode_role_bundle(bundle_value: str) -> dict[str, Any]:
    raw = str(bundle_value or "").strip()
    if not raw:
        raise SafetyGateError("empty role-scoped Telegram bundle")
    try:
        padded = raw + "=" * ((4 - len(raw) % 4) % 4)
        value = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise SafetyGateError("role-scoped Telegram bundle is not valid base64 JSON") from exc
    if not isinstance(value, dict) or not str(value.get("session") or "").strip():
        raise SafetyGateError("role-scoped Telegram bundle has no StringSession")
    return value


class TelethonCachedPeerAdapter:
    """One persistent, read-only Telethon client using direct cached peers.

    Every ``search`` call issues exactly one ``messages.SearchRequest``.  It
    never calls ``get_entity``, ``get_input_entity`` or ``ResolveUsername`` and
    cannot fall back to a username.  Telethon's request/connection retries and
    automatic FloodWait sleep are disabled so the outer harness sees and
    records the first failure.
    """

    requires_human_pacing = True

    def __init__(
        self,
        *,
        selected_auth_env: str,
        timeout_seconds: int,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        env = os.environ if environ is None else environ
        if selected_auth_env not in ALLOWED_AUTH_ENV_BY_ROLE.values():
            raise SafetyGateError("Telethon adapter requires a DISCOVERY1/2 bundle")
        bundle = _decode_role_bundle(str(env.get(selected_auth_env) or ""))
        api_id = str(env.get("TELEGRAM_API_ID") or env.get("TG_API_ID") or "").strip()
        api_hash = str(env.get("TELEGRAM_API_HASH") or env.get("TG_API_HASH") or "").strip()
        if not api_id or not api_hash:
            raise SafetyGateError("TELEGRAM_API_ID/API_HASH (or TG_ aliases) are required")
        try:
            from telethon import TelegramClient  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
        except Exception as exc:
            raise SafetyGateError("Telethon is required for --adapter-mode telethon-cached") from exc
        self._timeout_seconds = int(timeout_seconds)
        self._loop = asyncio.new_event_loop()
        self._client = TelegramClient(
            StringSession(str(bundle["session"])),
            int(api_id),
            api_hash,
            loop=self._loop,
            request_retries=0,
            connection_retries=0,
            retry_delay=0,
            auto_reconnect=False,
            flood_sleep_threshold=0,
            raise_last_call_error=True,
            receive_updates=False,
            sequential_updates=True,
            device_model=str(bundle.get("device_model") or "Region Talk research"),
            system_version=str(bundle.get("system_version") or "Linux"),
            app_version=str(bundle.get("app_version") or "1.0"),
            lang_code=str(bundle.get("lang_code") or "ru"),
            system_lang_code=str(bundle.get("system_lang_code") or "ru"),
        )
        try:
            self._loop.run_until_complete(
                asyncio.wait_for(self._client.connect(), timeout=self._timeout_seconds)
            )
        except Exception:
            self.close()
            raise

    def search(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        if not request.get("cached_peer_only") or request.get("allow_entity_resolve"):
            raise CaptureAborted("entity_resolve_attempt")
        try:
            from telethon import functions, types  # type: ignore
        except Exception as exc:
            raise SafetyGateError("Telethon import disappeared during capture") from exc
        peer = types.InputPeerChannel(
            channel_id=int(request["peer_id"]),
            access_hash=int(request["access_hash"]),
        )
        rpc = functions.messages.SearchRequest(
            peer=peer,
            q=str(request["query"]),
            filter=types.InputMessagesFilterEmpty(),
            min_date=None,
            max_date=None,
            offset_id=0,
            add_offset=0,
            limit=int(request["result_limit"]),
            max_id=0,
            min_id=0,
            hash=0,
        )
        result = self._loop.run_until_complete(
            asyncio.wait_for(self._client(rpc), timeout=self._timeout_seconds)
        )
        username = str(request.get("username") or request.get("source_id") or "").strip().lstrip("@")
        if username.startswith("telegram:"):
            username = username.split(":", 1)[1]
        messages: list[dict[str, Any]] = []
        for message in list(getattr(result, "messages", None) or [])[: int(request["result_limit"])]:
            message_id = int(getattr(message, "id", 0) or 0)
            if not message_id:
                continue
            date = getattr(message, "date", None)
            media = getattr(message, "media", None)
            messages.append({
                "url": f"https://t.me/{username}/{message_id}" if username else "",
                "message_id": message_id,
                "post_date": iso_utc(date) if date else "",
                "text": str(getattr(message, "message", "") or ""),
                "has_media": bool(media),
                "media_kind": media.__class__.__name__ if media is not None else "",
            })
        return {
            "messages": messages,
            "rpc_method": "messages.SearchRequest",
            "underlying_rpc_requests": 1,
            "entity_resolved": False,
            "cached_peer_only": True,
        }

    def close(self) -> None:
        client = getattr(self, "_client", None)
        loop = getattr(self, "_loop", None)
        if client is not None and loop is not None and not loop.is_closed():
            try:
                loop.run_until_complete(asyncio.wait_for(client.disconnect(), timeout=30))
            except Exception:
                pass
            # Telethon starts update/keepalive tasks even with receive_updates
            # disabled.  A timed-out disconnect must not leave them pending
            # when this private loop is closed.
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        if loop is not None and not loop.is_closed():
            loop.close()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"JSONL row {line_number} is not an object")
            rows.append(value)
    return rows


def _row_timestamp(row: Mapping[str, Any]) -> datetime | None:
    for field_name in ("post_date", "published_at", "date"):
        value = row.get(field_name)
        if value:
            try:
                return parse_utc(str(value))
            except (TypeError, ValueError):
                return None
    return None


def truncate_to_window(rows: Sequence[Mapping[str, Any]], t0: str | datetime, *, days: int = WINDOW_DAYS) -> list[dict[str, Any]]:
    end = parse_utc(t0)
    start = end - timedelta(days=int(days))
    kept: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _row_timestamp(row)
        if timestamp is not None and start <= timestamp <= end:
            kept.append(dict(row))
    return kept


def _flatten_capture_rows(raw_rows: Sequence[Mapping[str, Any]], manifest_hash: str) -> tuple[list[dict[str, Any]], int]:
    messages: list[dict[str, Any]] = []
    ignored = 0
    for raw in raw_rows:
        if raw.get("schema") != RAW_SCHEMA_VERSION or raw.get("manifest_hash") != manifest_hash:
            ignored += 1
            continue
        request = raw.get("request") if isinstance(raw.get("request"), Mapping) else {}
        response = raw.get("response") if isinstance(raw.get("response"), Mapping) else {}
        response_messages = response.get("messages") if isinstance(response.get("messages"), list) else []
        for index, message in enumerate(response_messages):
            if not isinstance(message, Mapping):
                continue
            row = dict(message)
            row.update(
                {
                    "source_id": str(request.get("source_id") or row.get("source_id") or ""),
                    "structure": str(request.get("structure") or ""),
                    "query": str(request.get("query") or ""),
                    "query_hash": str(request.get("query_hash") or ""),
                    "request_index": int(request.get("request_index") or 0),
                    "result_index": index,
                }
            )
            url = canonical_url(row.get("url") or row.get("post_url"))
            if url:
                row["url"] = url
            messages.append(row)
    return messages, ignored


def source_query_matches_visible_text(query: str, text: str) -> bool:
    """Reject Telegram stemming noise (notably ``Советск``/``советский``)."""
    normalise = lambda value: re.sub(
        r"\s+", " ", re.sub(r"[^0-9a-zа-я]+", " ", str(value or "").lower().replace("ё", "е"))
    ).strip()
    q = normalise(query)
    body = normalise(text)
    if not q or not body:
        return False
    phrase_patterns = {
        "калининградская область": r"(?<![0-9a-zа-я])калининградск(?:ая|ой|ую)\s+област(?:ь|и|ью)(?![0-9a-zа-я])",
        "куршская коса": r"(?<![0-9a-zа-я])куршск(?:ая|ой|ую)\s+кос(?:а|е|ы|у|ой)(?![0-9a-zа-я])",
        "балтийская коса": r"(?<![0-9a-zа-я])балтийск(?:ая|ой|ую)\s+кос(?:а|е|ы|у|ой)(?![0-9a-zа-я])",
        "роминтенская пуща": r"(?<![0-9a-zа-я])роминтенск(?:ая|ой|ую)\s+пущ(?:а|е|и|у|ей)(?![0-9a-zа-я])",
        "куршский залив": r"(?<![0-9a-zа-я])куршск(?:ий|ого|ом|ому|им)\s+залив(?:а|е|ом|у)?(?![0-9a-zа-я])",
    }
    if q in phrase_patterns:
        return re.search(phrase_patterns[q], body, flags=re.I) is not None
    if q == "янтарный":
        return re.search(
            r"(?<![0-9a-zа-я])(?:поселок|город)\s+янтарный(?![0-9a-zа-я])"
            r"|(?<![0-9a-zа-я])(?:в|из|до|под|около)\s+янтарн(?:ом|ого|ому)(?![0-9a-zа-я])",
            body,
            flags=re.I,
        ) is not None
    simple_place_patterns = {
        "калининград": r"калининград(?:а|е|ом|у)?",
        "нойхаузен": r"нойхаузен(?:а|е|ом|у)?",
        "бальга": r"бальг(?:а|е|и|у|ой)",
        "тапиау": r"тапиау",
        "талпаки": r"талпак(?:и|ах|ами|ов)",
        "виштынец": r"виштын(?:ец|ца|це|цем|цу)",
    }
    if q in simple_place_patterns:
        return re.search(
            rf"(?<![0-9a-zа-я]){simple_place_patterns[q]}(?![0-9a-zа-я])",
            body,
            flags=re.I,
        ) is not None
    if re.fullmatch(r"[a-zа-я]+ск", q, flags=re.I):
        return re.search(rf"(?<![0-9a-zа-я]){re.escape(q)}(?:а|е|ом|у)?(?![0-9a-zа-я])", body, flags=re.I) is not None
    return re.search(rf"(?<![0-9a-zа-я]){re.escape(q)}(?![0-9a-zа-я])", body, flags=re.I) is not None


def _mark_anchor_windows(rows: Sequence[Mapping[str, Any]], structure_d: Mapping[str, Any]) -> list[dict[str, Any]]:
    window = timedelta(days=int(structure_d.get("anchor_window_days") or 0))
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for anchor in structure_d.get("anchors") or []:
        if isinstance(anchor, Mapping):
            by_source[str(anchor.get("source_id") or "")].append(dict(anchor))
    marked: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        timestamp = _row_timestamp(row)
        matched: list[str] = []
        if timestamp is not None:
            for anchor in by_source.get(str(row.get("source_id") or ""), []):
                if abs(timestamp - parse_utc(str(anchor["published_at"]))) <= window:
                    matched.append(str(anchor["url"]))
        row["anchor_window_replay"] = bool(matched)
        row["anchor_urls"] = sorted(set(matched))
        marked.append(row)
    return marked


def replay_capture(manifest: Mapping[str, Any], raw_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    checked = validate_manifest(manifest)
    flattened, ignored = _flatten_capture_rows(raw_rows, checked["manifest_hash"])
    kept = truncate_to_window(flattened, checked["T0"], days=WINDOW_DAYS)
    for row in kept:
        row["exact_query_text_match"] = source_query_matches_visible_text(
            str(row.get("query") or ""),
            str(row.get("text") or ""),
        )
    kept = _mark_anchor_windows(kept, checked["structures"][STRUCTURE_D])
    kept.sort(key=lambda row: (int(row.get("request_index") or 0), int(row.get("result_index") or 0), str(row.get("url") or "")))
    request_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for raw in raw_rows:
        if raw.get("schema") != RAW_SCHEMA_VERSION or raw.get("manifest_hash") != checked["manifest_hash"]:
            continue
        request = raw.get("request") if isinstance(raw.get("request"), Mapping) else {}
        structure = str(request.get("structure") or "")
        if structure not in (STRUCTURE_A, STRUCTURE_B, STRUCTURE_C):
            continue
        request_counts[structure]["attempted"] += 1
        if isinstance(raw.get("error"), Mapping):
            request_counts[structure]["errors"] += 1
        else:
            request_counts[structure]["succeeded"] += 1
    request_metrics = {
        structure: {
            "attempted": request_counts[structure]["attempted"],
            "succeeded": request_counts[structure]["succeeded"],
            "errors": request_counts[structure]["errors"],
        }
        for structure in (STRUCTURE_A, STRUCTURE_B, STRUCTURE_C)
    }
    return {
        "schema": SCHEMA_VERSION,
        "manifest_hash": checked["manifest_hash"],
        "T0": checked["T0"],
        "window_start": checked["window_start"],
        "rows": kept,
        "raw_rows_ignored": ignored,
        "messages_before_window_truncation": len(flattened),
        "messages_after_window_truncation": len(kept),
        "messages_truncated": len(flattened) - len(kept),
        "exact_guard_accepted": sum(1 for row in kept if row.get("exact_query_text_match") is True),
        "exact_guard_rejected": sum(1 for row in kept if row.get("exact_query_text_match") is False),
        "request_metrics": request_metrics,
    }


def _truthy_ko(row: Mapping[str, Any]) -> bool:
    if row.get("new_KO") is True or row.get("is_KO") is True or row.get("ko_match") is True:
        return True
    return str(row.get("geo_class") or row.get("classification") or "").strip().lower() in {"ko", "kaliningrad_oblast"}


def _truthy_downstream(row: Mapping[str, Any]) -> bool:
    if row.get("downstream_accepted") is True:
        return True
    return str(row.get("downstream_status") or "").strip().lower() in {"accepted", "downstream_accepted"}


def classify_url_taxonomy(
    rows: Sequence[Mapping[str, Any]],
    baseline_urls: Sequence[str],
    *,
    downstream_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    baseline = {url for url in (canonical_url(value) for value in baseline_urls) if url}
    occurrences: Counter[str] = Counter()
    eligible_urls: set[str] = set()
    ko_urls: set[str] = set()
    downstream: set[str] = set()
    for row in rows:
        url = canonical_url(row.get("url") or row.get("post_url"))
        if not url:
            continue
        occurrences[url] += 1
        if row.get("exact_query_text_match") is not False:
            eligible_urls.add(url)
        if _truthy_ko(row):
            ko_urls.add(url)
        if _truthy_downstream(row):
            downstream.add(url)
    for row in downstream_rows:
        url = canonical_url(row.get("url") or row.get("post_url"))
        if url and _truthy_downstream(row):
            downstream.add(url)

    buckets: dict[str, list[str]] = {name: [] for name in URL_TAXONOMY}
    result_rows: list[dict[str, Any]] = []
    for url in sorted(occurrences):
        flags = {
            "baseline_replay": url in baseline,
            "experiment_repeat": occurrences[url] > 1,
            "dedup_eligible": url in eligible_urls,
            "new": url in eligible_urls and url not in baseline,
            "new_KO": url in eligible_urls and url not in baseline and url in ko_urls,
            "downstream_accepted": url in eligible_urls and url in downstream,
        }
        for name in URL_TAXONOMY:
            if flags[name]:
                buckets[name].append(url)
        result_rows.append({"url": url, "occurrences": occurrences[url], **flags})
    return {"taxonomy": list(URL_TAXONOMY), "buckets": buckets, "rows": result_rows}


build_url_taxonomy = classify_url_taxonomy


def build_report(
    manifest: Mapping[str, Any],
    replay: Mapping[str, Any],
    *,
    downstream_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    checked = validate_manifest(manifest)
    if replay.get("manifest_hash") != checked["manifest_hash"]:
        raise ManifestError("replay/manifest hash mismatch")
    rows = replay.get("rows") if isinstance(replay.get("rows"), list) else []
    taxonomy = classify_url_taxonomy(rows, checked["K0_urls"], downstream_rows=downstream_rows)
    request_metrics = replay.get("request_metrics") if isinstance(replay.get("request_metrics"), Mapping) else {}
    by_structure: dict[str, Any] = {}
    for structure in STRUCTURE_NAMES:
        if structure == STRUCTURE_D:
            structure_rows = [row for row in rows if row.get("anchor_window_replay") is True]
            requests = {"attempted": 0, "succeeded": 0, "errors": 0}
        else:
            structure_rows = [row for row in rows if row.get("structure") == structure]
            raw_requests = request_metrics.get(structure) if isinstance(request_metrics, Mapping) else {}
            requests = {
                "attempted": int((raw_requests or {}).get("attempted") or 0),
                "succeeded": int((raw_requests or {}).get("succeeded") or 0),
                "errors": int((raw_requests or {}).get("errors") or 0),
            }
        structure_taxonomy = classify_url_taxonomy(
            structure_rows,
            checked["K0_urls"],
            downstream_rows=downstream_rows,
        )
        structure_counts = {
            name: len(structure_taxonomy["buckets"][name]) for name in URL_TAXONOMY
        }
        attempted = requests["attempted"]
        by_structure[structure] = {
            "requests": requests,
            "message_occurrences": len(structure_rows),
            "distinct_urls": len(structure_taxonomy["rows"]),
            "exact_guard_accepted_occurrences": sum(
                1 for row in structure_rows if row.get("exact_query_text_match") is True
            ),
            "exact_guard_rejected_occurrences": sum(
                1 for row in structure_rows if row.get("exact_query_text_match") is False
            ),
            "counts": structure_counts,
            "new_urls_per_100_requests": (
                round(100.0 * structure_counts["new"] / attempted, 3) if attempted else None
            ),
            "new_KO_urls_per_100_requests": (
                round(100.0 * structure_counts["new_KO"] / attempted, 3) if attempted else None
            ),
        }
    return {
        "schema": SCHEMA_VERSION,
        "manifest_hash": checked["manifest_hash"],
        "K0_hash": checked["K0_hash"],
        "URL_taxonomy": taxonomy,
        "counts": {name: len(taxonomy["buckets"][name]) for name in URL_TAXONOMY},
        "by_structure": by_structure,
        "window": {
            "T0": checked["T0"],
            "window_start": checked["window_start"],
            "messages_truncated": int(replay.get("messages_truncated") or 0),
        },
    }


def _read_json(path: Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _emit_json(value: Any, output: Path | None) -> None:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if output is None:
        sys.stdout.write(payload)
        return
    Path(output).write_text(payload, encoding="utf-8")


def current_code_sha() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        cwd=Path(__file__).resolve().parents[1],
    )
    if completed.returncode:
        raise ManifestError("unable to determine code SHA; pass --code-sha")
    return completed.stdout.strip()


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")

    manifest_parser = subparsers.add_parser("manifest", help="build a deterministic experiment manifest")
    manifest_parser.add_argument("--config", type=Path, required=True)
    manifest_parser.add_argument("--t0", required=True)
    manifest_parser.add_argument("--code-sha")
    manifest_parser.add_argument("--output", type=Path, help="explicitly write JSON; default is stdout only")

    capture_parser = subparsers.add_parser("capture", help="perform explicitly authorized bounded Telegram reads")
    capture_parser.add_argument("--manifest", type=Path, required=True)
    capture_parser.add_argument("--raw-capture", type=Path, required=True)
    capture_parser.add_argument("--adapter-mode", choices=("command", "telethon-cached"), default="command")
    capture_parser.add_argument("--adapter-command")
    capture_parser.add_argument("--auth-role", choices=sorted(ALLOWED_AUTH_ENV_BY_ROLE), required=True)
    capture_parser.add_argument("--allow-telegram-read", action="store_true")

    replay_parser = subparsers.add_parser("replay", help="replay a raw capture offline")
    replay_parser.add_argument("--manifest", type=Path, required=True)
    replay_parser.add_argument("--raw-capture", type=Path, required=True)
    replay_parser.add_argument("--output", type=Path, help="explicitly write JSON; default is stdout only")

    report_parser = subparsers.add_parser("report", help="build the exact URL-taxonomy report offline")
    report_parser.add_argument("--manifest", type=Path, required=True)
    report_parser.add_argument("--replay", type=Path, required=True)
    report_parser.add_argument("--downstream-jsonl", type=Path)
    report_parser.add_argument("--output", type=Path, help="explicitly write JSON; default is stdout only")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help(sys.stderr)
        return 2
    try:
        if args.command == "manifest":
            config = _read_json(args.config)
            if not isinstance(config, dict):
                raise ManifestError("manifest config must be a JSON object")
            manifest = build_manifest(code_sha=args.code_sha or current_code_sha(), t0=args.t0, **config)
            _emit_json(manifest, args.output)
            return 0
        if args.command == "capture":
            if not args.allow_telegram_read:
                raise SafetyGateError("capture requires explicit --allow-telegram-read")
            manifest = _read_json(args.manifest)
            checked = validate_manifest(manifest)
            auth_env_name, _ = select_auth_bundle(args.auth_role)
            if args.adapter_mode == "command":
                if not args.adapter_command:
                    raise SafetyGateError("--adapter-command is required in command mode")
                adapter: TelegramReadAdapter = CommandAdapter(
                    shlex.split(args.adapter_command),
                    timeout_seconds=int(checked["request_ceilings"]["request_timeout_seconds"]),
                    selected_auth_env=auth_env_name,
                )
                pace = False
            else:
                adapter = TelethonCachedPeerAdapter(
                    selected_auth_env=auth_env_name,
                    timeout_seconds=int(checked["request_ceilings"]["request_timeout_seconds"]),
                )
                pace = True
            try:
                summary = run_capture(
                    checked,
                    args.raw_capture,
                    adapter,
                    allow_telegram_read=True,
                    auth_role=args.auth_role,
                    pace_requests=pace,
                )
            finally:
                close = getattr(adapter, "close", None)
                if callable(close):
                    close()
            _emit_json(summary, None)
            return 0
        if args.command == "replay":
            replay = replay_capture(_read_json(args.manifest), read_jsonl(args.raw_capture))
            _emit_json(replay, args.output)
            return 0
        if args.command == "report":
            downstream = read_jsonl(args.downstream_jsonl) if args.downstream_jsonl else []
            report = build_report(_read_json(args.manifest), _read_json(args.replay), downstream_rows=downstream)
            _emit_json(report, args.output)
            return 0
    except ResearchHarnessError as exc:
        sys.stderr.write(f"fail-closed: {exc}\n")
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
