#!/usr/bin/env python3
"""Product-facing quality monitor for static event collections.

The checker intentionally consumes a small normalized projection instead of the
internal collection schemas. Unknown fields are ignored. A single adapter can
therefore evolve with production internals while this report keeps answering
stable product questions:

* is the collection populated and current;
* did known bad examples reappear or known good examples disappear;
* did the visible result degrade relative to an accepted baseline;
* did a failed rebuild erase the last-good result.

WATCH is actionable but non-blocking by default. Only clear product breakage is
reported as FAIL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

HORIZONS = (14, 30, 90)
PUBLIC_MODES = {"public"}
BAD_REVIEW_STATES = {"blocked", "needs_source_review", "unresolved", "rejected"}
BAD_SOURCE_STATES = {"missing", "unbound", "invalid", "blocked"}
DEGRADED_STATES = {"failed", "degraded", "blocked"}


@dataclass(frozen=True)
class Issue:
    severity: str  # fail | watch
    code: str
    message: str
    collection: str | None = None
    event_ids: tuple[int, ...] = ()
    family_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class Item:
    event_id: int
    family_id: str | None
    start_date: date | None
    end_date: date | None
    venue: str | None
    organizer: str | None
    event_type: str | None
    source_status: str | None
    review_status: str | None
    rank: int


def load_object(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: top-level JSON value must be an object")
    return value


def _parse_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _text(value: Any) -> str | None:
    if isinstance(value, (list, tuple)):
        for item in value:
            clean = str(item or "").strip()
            if clean:
                return clean
        return None
    clean = str(value or "").strip()
    return clean or None


def _first(mapping: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in mapping and mapping.get(name) not in (None, ""):
            return mapping.get(name)
    return None


def normalize_item(raw: Mapping[str, Any], *, rank: int) -> Item | None:
    raw_id = _first(raw, ("event_id", "id"))
    try:
        event_id = int(raw_id)
    except (TypeError, ValueError, OverflowError):
        return None
    if event_id <= 0:
        return None
    family_id = _text(_first(raw, ("family_id", "occurrence_family_id", "family")))
    start = _parse_date(_first(raw, ("start_date", "date", "occurrence_date")))
    end = _parse_date(_first(raw, ("end_date", "finish_date"))) or start
    return Item(
        event_id=event_id,
        family_id=family_id,
        start_date=start,
        end_date=end,
        venue=_text(_first(raw, ("venue", "venue_name", "location_name"))),
        organizer=_text(_first(raw, ("organizer", "organizer_name", "organizers"))),
        event_type=_text(_first(raw, ("event_type", "type"))),
        source_status=_text(_first(raw, ("source_status", "source_grounding", "evidence_status"))),
        review_status=_text(_first(raw, ("review_status", "review_decision", "quality_status"))),
        rank=rank,
    )


def normalize_collection(raw: Any) -> tuple[dict[str, Any], list[Item], int]:
    if isinstance(raw, list):
        config: dict[str, Any] = {}
        rows = raw
    elif isinstance(raw, Mapping):
        config = dict(raw)
        rows = raw.get("items")
        if rows is None:
            rows = raw.get("events")
        if rows is None:
            rows = raw.get("results")
    else:
        return {}, [], 0
    if not isinstance(rows, list):
        rows = []
    items: list[Item] = []
    invalid = 0
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            invalid += 1
            continue
        item = normalize_item(row, rank=index)
        if item is None:
            invalid += 1
        else:
            items.append(item)
    return config, items, invalid


def normalized_output_fingerprint(snapshot: Mapping[str, Any]) -> str:
    collections = snapshot.get("collections")
    normalized: dict[str, list[tuple[str, int]]] = {}
    if isinstance(collections, Mapping):
        for label, raw in sorted(collections.items()):
            _config, items, _invalid = normalize_collection(raw)
            normalized[str(label)] = [
                (item.family_id or f"event:{item.event_id}", item.event_id)
                for item in sorted(items, key=lambda value: value.rank)
            ]
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _ratio(counter: Counter[str], denominator: int) -> tuple[float, str | None]:
    if denominator <= 0 or not counter:
        return 0.0, None
    key, count = counter.most_common(1)[0]
    return count / denominator, key


def _regression_for(regression: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    collections = regression.get("collections")
    if not isinstance(collections, Mapping):
        return {}
    value = collections.get(label)
    return value if isinstance(value, Mapping) else {}


def _id_set(value: Any) -> set[int]:
    if not isinstance(value, list):
        return set()
    result: set[int] = set()
    for item in value:
        try:
            parsed = int(item)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed > 0:
            result.add(parsed)
    return result


def _text_set(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {str(item).strip() for item in value if str(item or "").strip()}


def evaluate_snapshot(
    snapshot: Mapping[str, Any],
    *,
    baseline: Mapping[str, Any] | None = None,
    regression: Mapping[str, Any] | None = None,
    today: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    baseline = baseline or {}
    regression = regression or {}
    today = today or datetime.now(timezone.utc).date()
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)

    issues: list[Issue] = []
    collections = snapshot.get("collections")
    if not isinstance(collections, Mapping) or not collections:
        issues.append(Issue("fail", "collections_missing", "Snapshot contains no collections."))
        collections = {}

    coverage = snapshot.get("coverage")
    coverage_status = "unknown"
    if isinstance(coverage, Mapping):
        coverage_status = str(coverage.get("status") or "unknown").strip().casefold()
    if coverage_status not in {"complete", "partial", "unknown"}:
        issues.append(
            Issue("fail", "coverage_invalid", f"Unsupported coverage status: {coverage_status}.")
        )
    elif coverage_status != "complete":
        issues.append(
            Issue(
                "watch",
                f"coverage_{coverage_status}",
                "Current/future audience candidate coverage is not complete; supply is provisional.",
            )
        )

    baseline_collections = baseline.get("collections")
    if not isinstance(baseline_collections, Mapping):
        baseline_collections = {}
        if collections:
            issues.append(
                Issue(
                    "watch",
                    "accepted_baseline_missing",
                    "No accepted baseline was supplied; degradation can only be assessed after one is approved.",
                )
            )

    generated_at = _parse_datetime(snapshot.get("generated_at"))
    stale_after_hours = float(snapshot.get("stale_after_hours") or 72)
    if generated_at is None:
        issues.append(Issue("watch", "generated_at_missing", "Snapshot has no valid generated_at."))
    else:
        age_hours = max(0.0, (now - generated_at).total_seconds() / 3600)
        if age_hours > stale_after_hours:
            issues.append(
                Issue(
                    "watch",
                    "snapshot_stale",
                    f"Snapshot is {age_hours:.1f}h old; configured watch threshold is {stale_after_hours:.1f}h.",
                )
            )

    current_input = _text(snapshot.get("input_fingerprint"))
    baseline_input = _text(baseline.get("input_fingerprint"))
    if current_input and baseline_input and current_input == baseline_input:
        if normalized_output_fingerprint(snapshot) != normalized_output_fingerprint(baseline):
            issues.append(
                Issue(
                    "fail",
                    "same_input_changed_output",
                    "The normalized visible collection output changed while input_fingerprint stayed identical.",
                )
            )

    summaries: dict[str, dict[str, Any]] = {}
    for raw_label, raw_collection in sorted(collections.items()):
        label = str(raw_label)
        config, items, invalid_rows = normalize_collection(raw_collection)
        mode = str(config.get("mode") or "experimental").strip().lower()
        source_grounding_required = config.get("source_grounding_required") is True
        state = str(config.get("state") or "ready").strip().lower()
        using_last_good = config.get("using_last_good") is True
        approved_empty = config.get("approved_empty") is True
        top_n = max(1, int(config.get("top_n") or 20))
        concentration_watch = float(config.get("concentration_watch_ratio") or 0.60)
        drop_watch = float(config.get("drop_watch_ratio") or 0.35)
        churn_watch = float(config.get("churn_watch_ratio") or 0.60)
        watch_below = config.get("watch_below_families")
        try:
            watch_below_int = max(0, int(watch_below)) if watch_below is not None else None
        except (TypeError, ValueError):
            watch_below_int = None

        event_ids = [item.event_id for item in items]
        family_ids = [item.family_id for item in items if item.family_id]
        missing_family_events = [item.event_id for item in items if not item.family_id]
        duplicate_events = sorted(key for key, count in Counter(event_ids).items() if count > 1)
        duplicate_families = sorted(key for key, count in Counter(family_ids).items() if count > 1)
        family_set = {item.family_id or f"event:{item.event_id}" for item in items}

        if invalid_rows:
            issues.append(
                Issue(
                    "fail" if mode in PUBLIC_MODES or source_grounding_required else "watch",
                    "invalid_result_rows",
                    f"{invalid_rows} result rows do not contain a valid positive event id.",
                    collection=label,
                )
            )
        if missing_family_events:
            issues.append(
                Issue(
                    "fail" if mode in PUBLIC_MODES or source_grounding_required else "watch",
                    "family_identity_missing",
                    "Family identity is missing, so duplicate occurrences cannot be ruled out.",
                    collection=label,
                    event_ids=tuple(missing_family_events[:20]),
                )
            )
        if duplicate_events:
            issues.append(
                Issue(
                    "fail",
                    "duplicate_event_ids",
                    "The same event is shown more than once.",
                    collection=label,
                    event_ids=tuple(duplicate_events[:20]),
                )
            )
        if duplicate_families:
            issues.append(
                Issue(
                    "fail",
                    "duplicate_families",
                    "Multiple occurrences from the same family are visible as separate collection results.",
                    collection=label,
                    family_ids=tuple(duplicate_families[:20]),
                )
            )

        expired = [item.event_id for item in items if item.end_date is not None and item.end_date < today]
        unresolved = [
            item.event_id
            for item in items
            if str(item.review_status or "").strip().lower() in BAD_REVIEW_STATES
        ]
        bad_sources = [
            item.event_id
            for item in items
            if str(item.source_status or "").strip().lower() in BAD_SOURCE_STATES
        ]
        if expired:
            issues.append(
                Issue(
                    "fail" if mode in PUBLIC_MODES or source_grounding_required else "watch",
                    "expired_results",
                    "Completed events are still present in the collection result.",
                    collection=label,
                    event_ids=tuple(expired[:20]),
                )
            )
        if unresolved:
            issues.append(
                Issue(
                    "fail" if mode in PUBLIC_MODES or source_grounding_required else "watch",
                    "review_blocked_results",
                    "Results that are blocked or still require source review are visible.",
                    collection=label,
                    event_ids=tuple(unresolved[:20]),
                )
            )
        if bad_sources:
            issues.append(
                Issue(
                    "fail" if mode in PUBLIC_MODES or source_grounding_required else "watch",
                    "source_grounding_missing",
                    "Results with missing or invalid source grounding are visible.",
                    collection=label,
                    event_ids=tuple(bad_sources[:20]),
                )
            )

        if not family_set and not approved_empty:
            if mode in PUBLIC_MODES:
                issues.append(
                    Issue(
                        "fail",
                        "public_collection_empty",
                        "A public collection is unexpectedly empty.",
                        collection=label,
                    )
                )
            else:
                issues.append(
                    Issue(
                        "watch",
                        "nonpublic_collection_empty",
                        f"The {mode} collection currently has no independent families.",
                        collection=label,
                    )
                )

        horizon_counts: dict[str, int] = {}
        for horizon in HORIZONS:
            cutoff = today.fromordinal(today.toordinal() + horizon)
            horizon_counts[str(horizon)] = sum(
                1
                for item in items
                if item.end_date is not None
                and item.end_date >= today
                and (item.start_date or item.end_date) <= cutoff
            )
        future_dates = [item.start_date for item in items if item.start_date is not None and item.start_date >= today]
        nearest = min(future_dates).isoformat() if future_dates else None

        top_items = sorted(items, key=lambda item: item.rank)[:top_n]
        venue_ratio, top_venue = _ratio(Counter(item.venue for item in top_items if item.venue), len(top_items))
        organizer_ratio, top_organizer = _ratio(
            Counter(item.organizer for item in top_items if item.organizer), len(top_items)
        )
        type_ratio, top_type = _ratio(
            Counter(item.event_type for item in top_items if item.event_type), len(top_items)
        )
        if len(top_items) >= 5:
            for dimension, ratio, value in (
                ("venue", venue_ratio, top_venue),
                ("organizer", organizer_ratio, top_organizer),
                ("event type", type_ratio, top_type),
            ):
                if value and ratio >= concentration_watch:
                    issues.append(
                        Issue(
                            "watch",
                            f"{dimension.replace(' ', '_')}_concentration",
                            f"{ratio:.0%} of top-{len(top_items)} results come from one {dimension}: {value}.",
                            collection=label,
                        )
                    )

        _baseline_config, baseline_items, _baseline_invalid = normalize_collection(
            baseline_collections.get(label)
        ) if label in baseline_collections else ({}, [], 0)
        baseline_family_set = {
            item.family_id or f"event:{item.event_id}" for item in baseline_items
        }
        added = sorted(family_set - baseline_family_set)
        removed = sorted(baseline_family_set - family_set)
        drop_ratio = (
            max(0.0, (len(baseline_family_set) - len(family_set)) / len(baseline_family_set))
            if baseline_family_set
            else 0.0
        )
        churn_ratio = (
            (len(added) + len(removed)) / max(len(baseline_family_set), len(family_set), 1)
            if baseline_family_set
            else 0.0
        )
        if baseline_family_set and len(removed) >= 2 and drop_ratio >= drop_watch:
            issues.append(
                Issue(
                    "watch",
                    "supply_drop",
                    f"Independent-family supply fell by {drop_ratio:.0%}: {len(baseline_family_set)} -> {len(family_set)}.",
                    collection=label,
                    family_ids=tuple(removed[:20]),
                )
            )
        if baseline_family_set and len(baseline_family_set) >= 5 and churn_ratio >= churn_watch:
            issues.append(
                Issue(
                    "watch",
                    "composition_churn",
                    f"Collection composition churn is {churn_ratio:.0%} relative to the accepted baseline.",
                    collection=label,
                    family_ids=tuple((removed + added)[:20]),
                )
            )
        if (
            state in DEGRADED_STATES
            and baseline_family_set
            and not family_set
            and not using_last_good
        ):
            issues.append(
                Issue(
                    "fail",
                    "last_good_lost",
                    "A degraded/failed rebuild erased a previously non-empty result instead of retaining last-good.",
                    collection=label,
                )
            )
        if watch_below_int is not None and len(family_set) < watch_below_int:
            issues.append(
                Issue(
                    "watch",
                    "low_supply",
                    f"Collection has {len(family_set)} independent families; product watch level is {watch_below_int}.",
                    collection=label,
                )
            )

        regression_config = _regression_for(regression, label)
        current_events = set(event_ids)
        must_exclude_events = _id_set(regression_config.get("must_exclude_event_ids"))
        must_exclude_families = _text_set(regression_config.get("must_exclude_family_ids"))
        must_include_events = _id_set(regression_config.get("must_include_event_ids"))
        must_include_families = _text_set(regression_config.get("must_include_family_ids"))
        bad_event_hits = sorted(current_events & must_exclude_events)
        bad_family_hits = sorted(family_set & must_exclude_families)
        missing_event_examples = sorted(must_include_events - current_events)
        missing_family_examples = sorted(must_include_families - family_set)
        if bad_event_hits or bad_family_hits:
            issues.append(
                Issue(
                    "fail",
                    "known_false_positive_returned",
                    "A known clearly irrelevant regression example re-entered the result.",
                    collection=label,
                    event_ids=tuple(bad_event_hits[:20]),
                    family_ids=tuple(bad_family_hits[:20]),
                )
            )
        if missing_event_examples or missing_family_examples:
            issues.append(
                Issue(
                    "watch",
                    "known_positive_missing",
                    "A known relevant example is absent; verify expiry, catalog changes, routing and classification.",
                    collection=label,
                    event_ids=tuple(missing_event_examples[:20]),
                    family_ids=tuple(missing_family_examples[:20]),
                )
            )

        summaries[label] = {
            "mode": mode,
            "state": state,
            "using_last_good": using_last_good,
            "item_count": len(items),
            "family_count": len(family_set),
            "upcoming": horizon_counts,
            "nearest_date": nearest,
            "expired_count": len(expired),
            "review_blocked_count": len(unresolved),
            "source_blocked_count": len(bad_sources),
            "duplicate_event_count": len(duplicate_events),
            "duplicate_family_count": len(duplicate_families),
            "added_families": added,
            "removed_families": removed,
            "supply_drop_ratio": round(drop_ratio, 6),
            "composition_churn_ratio": round(churn_ratio, 6),
            "top_concentration": {
                "venue": {"ratio": round(venue_ratio, 6), "value": top_venue},
                "organizer": {"ratio": round(organizer_ratio, 6), "value": top_organizer},
                "event_type": {"ratio": round(type_ratio, 6), "value": top_type},
            },
        }

    collection_statuses: dict[str, str] = {}
    for label in summaries:
        local = [issue for issue in issues if issue.collection == label]
        collection_statuses[label] = (
            "FAIL"
            if any(issue.severity == "fail" for issue in local)
            else "WATCH"
            if any(issue.severity == "watch" for issue in local)
            else "HEALTHY"
        )
        summaries[label]["status"] = collection_statuses[label]

    overall = (
        "FAIL"
        if any(issue.severity == "fail" for issue in issues)
        else "WATCH"
        if any(issue.severity == "watch" for issue in issues)
        else "HEALTHY"
    )
    report = {
        "report_kind": "static-collections-product-quality",
        "status": overall,
        "evaluated_at": now.isoformat().replace("+00:00", "Z"),
        "today": today.isoformat(),
        "snapshot_generated_at": generated_at.isoformat().replace("+00:00", "Z") if generated_at else None,
        "source_scope": snapshot.get("source_scope"),
        "coverage": dict(coverage) if isinstance(coverage, Mapping) else {"status": "unknown"},
        "input_fingerprint": current_input,
        "normalized_output_sha256": normalized_output_fingerprint(snapshot),
        "collections": summaries,
        "issues": [asdict(issue) for issue in issues],
    }
    return report


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Static collections product quality",
        "",
        f"- Overall: **{report.get('status')}**",
        f"- Today: `{report.get('today')}`",
        f"- Source scope: `{report.get('source_scope') or 'unspecified'}`",
        f"- Coverage: `{(report.get('coverage') or {}).get('status', 'unknown')}`",
        f"- Output SHA-256: `{report.get('normalized_output_sha256')}`",
        "",
        "| Collection | Mode | Status | Families | 14d | 30d | 90d | Nearest | Added | Removed | Duplicates | Review/source blockers |",
        "|---|---|---|---:|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    collections = report.get("collections") or {}
    if isinstance(collections, Mapping):
        for label, value in sorted(collections.items()):
            if not isinstance(value, Mapping):
                continue
            upcoming = value.get("upcoming") if isinstance(value.get("upcoming"), Mapping) else {}
            duplicates = int(value.get("duplicate_event_count") or 0) + int(
                value.get("duplicate_family_count") or 0
            )
            blockers = int(value.get("review_blocked_count") or 0) + int(
                value.get("source_blocked_count") or 0
            )
            lines.append(
                f"| `{label}` | `{value.get('mode')}` | **{value.get('status')}** | "
                f"{value.get('family_count', 0)} | {upcoming.get('14', 0)} | "
                f"{upcoming.get('30', 0)} | {upcoming.get('90', 0)} | "
                f"{value.get('nearest_date') or '—'} | "
                f"{len(value.get('added_families') or [])} | "
                f"{len(value.get('removed_families') or [])} | {duplicates} | {blockers} |"
            )
    lines.extend(["", "## Actionable signals", ""])
    issues = report.get("issues") or []
    if not issues:
        lines.append("No product-quality signals.")
    for raw in issues:
        if not isinstance(raw, Mapping):
            continue
        location = f" collection=`{raw.get('collection')}`" if raw.get("collection") else ""
        details: list[str] = []
        if raw.get("event_ids"):
            details.append("events=" + ",".join(str(value) for value in raw["event_ids"]))
        if raw.get("family_ids"):
            details.append("families=" + ",".join(str(value) for value in raw["family_ids"]))
        suffix = f" ({'; '.join(details)})" if details else ""
        lines.append(
            f"- **{str(raw.get('severity')).upper()} `{raw.get('code')}`**{location}: "
            f"{raw.get('message')}{suffix}"
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--regression", type=Path)
    parser.add_argument("--today")
    parser.add_argument("--json-report", type=Path)
    parser.add_argument("--markdown-report", type=Path)
    parser.add_argument("--fail-on-watch", action="store_true")
    parser.add_argument("--expect-status", choices=("HEALTHY", "WATCH", "FAIL"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    snapshot = load_object(args.snapshot)
    baseline = load_object(args.baseline) if args.baseline else {}
    regression = load_object(args.regression) if args.regression else {}
    today = date.fromisoformat(args.today) if args.today else None
    now = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc) if today else None
    report = evaluate_snapshot(
        snapshot,
        baseline=baseline,
        regression=regression,
        today=today,
        now=now,
    )
    markdown = render_markdown(report)
    print(markdown, end="")
    if args.json_report:
        args.json_report.parent.mkdir(parents=True, exist_ok=True)
        args.json_report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    if args.markdown_report:
        args.markdown_report.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_report.write_text(markdown, encoding="utf-8")
    if args.expect_status and report["status"] != args.expect_status:
        print(
            f"Expected product status {args.expect_status}, got {report['status']}",
            file=sys.stderr,
        )
        return 2
    if report["status"] == "FAIL":
        return 1
    if report["status"] == "WATCH" and args.fail_on_watch:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
