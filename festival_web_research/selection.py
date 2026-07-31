"""Deterministic current-URL selection, grouping and input fingerprints.

No festival identity or topology is inferred here.  Date parsing is limited to
explicit ISO date fields/signals; grouping consumes caller-supplied identity
hints and leaves missing identities unresolved per row.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping

from pydantic import Field, field_validator, model_validator

from .contracts import ClosedModel
from .evidence import canonical_json_sha256
from .sources import canonicalize_public_url


class SelectionError(ValueError):
    pass


class CurrentUrlRow(ClosedModel):
    queue_item_id: int = Field(gt=0)
    canonical_url: str = Field(min_length=1, max_length=4096)
    event_start_date: date | None = None
    event_end_date: date
    series_identity_hint: str | None = Field(default=None, max_length=256)
    edition_identity_hint: str | None = Field(default=None, max_length=256)
    seed_subject_hint: str | None = Field(default=None, max_length=128)
    snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def date_order(self) -> "CurrentUrlRow":
        if self.event_start_date and self.event_end_date < self.event_start_date:
            raise ValueError("event_end_date precedes event_start_date")
        return self




class UrlTargetGroup(ClosedModel):
    target_key: str = Field(min_length=1, max_length=1024)
    series_identity_hint: str | None
    edition_identity_hint: str | None
    queue_item_ids: list[int] = Field(min_length=1, max_length=512)
    canonical_urls: list[str] = Field(min_length=1, max_length=512)
    input_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("queue_item_ids", "canonical_urls")
    @classmethod
    def unique_values(cls, value: list[Any]) -> list[Any]:
        if len(value) != len(set(value)):
            raise ValueError("target group contains duplicates")
        return value


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise SelectionError(f"date must be explicit ISO YYYY-MM-DD: {value!r}") from exc
    raise SelectionError(f"unsupported explicit date type: {type(value).__name__}")


def _extract_period(row: Mapping[str, Any]) -> tuple[date | None, date]:
    signals = row.get("explicit_date_signals")
    periods: list[tuple[date | None, date]] = []
    if signals is not None:
        if not isinstance(signals, list) or not signals:
            raise SelectionError("explicit_date_signals must be a non-empty list")
        for signal in signals:
            if not isinstance(signal, Mapping):
                raise SelectionError("date signal must be an object")
            start = _parse_date(signal.get("start_date") or signal.get("event_start_date") or signal.get("date"))
            end = _parse_date(signal.get("end_date") or signal.get("event_end_date")) or start
            if end is None:
                raise SelectionError("date signal has no explicit date")
            periods.append((start, end))
    else:
        start = _parse_date(row.get("event_start_date") or row.get("start_date") or row.get("event_date"))
        end = _parse_date(row.get("event_end_date") or row.get("end_date")) or start
        if end is None:
            raise SelectionError("no explicit event period")
        periods.append((start, end))
    unique = set(periods)
    if len(unique) != 1:
        raise SelectionError("ambiguous explicit event periods")
    start, end = periods[0]
    if start and end < start:
        raise SelectionError("event end precedes start")
    return start, end


def select_current_url_rows(rows: Iterable[Mapping[str, Any]], *, cutoff: date) -> tuple[list[CurrentUrlRow], dict[int, str]]:
    selected: list[CurrentUrlRow] = []
    rejected: dict[int, str] = {}
    seen_ids: set[int] = set()
    for raw in rows:
        item_id = int(raw.get("queue_item_id", raw.get("id", 0)))
        if item_id <= 0 or item_id in seen_ids:
            raise SelectionError("queue item IDs must be unique positive integers")
        seen_ids.add(item_id)
        if raw.get("status") != "pending" or raw.get("source_kind") != "url":
            rejected[item_id] = "not_pending_url"
            continue
        try:
            start, end = _extract_period(raw)
            if end < cutoff:
                rejected[item_id] = "stale_explicit_period"
                continue
            canonical_url = canonicalize_public_url(str(raw.get("url") or raw.get("source_url") or ""))
            selected.append(CurrentUrlRow(
                queue_item_id=item_id,
                canonical_url=canonical_url,
                event_start_date=start,
                event_end_date=end,
                series_identity_hint=raw.get("series_identity_hint"),
                edition_identity_hint=raw.get("edition_identity_hint"),
                seed_subject_hint=raw.get("seed_subject_hint"),
                snapshot_sha256=raw.get("snapshot_sha256"),
            ))
        except (SelectionError, ValueError) as exc:
            rejected[item_id] = f"invalid_or_ambiguous:{exc}"
    selected.sort(key=lambda row: row.queue_item_id)
    return selected, rejected


def _hint(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split()).casefold()
    return normalized or None


def build_input_fingerprint(
    *,
    target_key: str,
    rows: Iterable[CurrentUrlRow],
    contract_version: str,
    taxonomy_sha256: str,
    prompt_sha256: str,
    normalizer_version: str,
) -> str:
    ordered = sorted(rows, key=lambda row: row.queue_item_id)
    if not ordered:
        raise SelectionError("cannot fingerprint an empty target")
    for name, value in (("taxonomy_sha256", taxonomy_sha256), ("prompt_sha256", prompt_sha256)):
        if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
            raise SelectionError(f"{name} must be lowercase SHA-256")
    if not target_key or len(target_key) > 1024 or not contract_version or not normalizer_version:
        raise SelectionError("fingerprint versions and target key are required and bounded")
    payload = {
        "target_key": target_key,
        "queue_items": [{
            "id": row.queue_item_id,
            "url": row.canonical_url,
            "snapshot_sha256": row.snapshot_sha256,
            "seed_subject_hint": row.seed_subject_hint,
        } for row in ordered],
        "contract_version": contract_version,
        "taxonomy_sha256": taxonomy_sha256,
        "prompt_sha256": prompt_sha256,
        "normalizer_version": normalizer_version,
    }
    return canonical_json_sha256(payload)


def group_current_url_rows(
    rows: Iterable[CurrentUrlRow],
    *,
    contract_version: str,
    taxonomy_sha256: str,
    prompt_sha256: str,
    normalizer_version: str,
) -> list[UrlTargetGroup]:
    buckets: dict[tuple[str | None, str | None, int | None], list[CurrentUrlRow]] = {}
    for row in rows:
        series = _hint(row.series_identity_hint)
        edition = _hint(row.edition_identity_hint)
        # Missing supplied identity is deliberately not guessed from title/URL.
        unresolved_id = row.queue_item_id if not (series and edition) else None
        buckets.setdefault((series, edition, unresolved_id), []).append(row)
    groups: list[UrlTargetGroup] = []
    for (series, edition, unresolved_id), members in sorted(buckets.items(), key=lambda pair: min(r.queue_item_id for r in pair[1])):
        target_key = f"hint:{series}:{edition}" if unresolved_id is None else f"unresolved:queue:{unresolved_id}"
        fingerprint = build_input_fingerprint(
            target_key=target_key, rows=members, contract_version=contract_version,
            taxonomy_sha256=taxonomy_sha256, prompt_sha256=prompt_sha256,
            normalizer_version=normalizer_version,
        )
        groups.append(UrlTargetGroup(
            target_key=target_key,
            series_identity_hint=series,
            edition_identity_hint=edition,
            queue_item_ids=sorted(row.queue_item_id for row in members),
            canonical_urls=sorted({row.canonical_url for row in members}),
            input_fingerprint=fingerprint,
        ))
    return groups
