"""Scheduled acceptance audit for public exhibition duplicate regressions."""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

from admin_chat import resolve_superadmin_chat_id
from db import Database
from ops_run import finish_ops_run, start_ops_run
from scripts.inspect.audit_identity_gate_rollout import build_rollout_payload
from scripts.inspect.audit_public_exhibition_duplicates import build_audit_payload

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_enabled(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _date_env(name: str) -> date | None:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        logger.warning("invalid %s=%r; falling back to since_days", name, raw)
        return None


def _status_from_payload(payload: dict[str, Any]) -> str:
    confirmed = int(payload.get("confirmed_duplicate_count") or 0)
    unresolved = int(payload.get("unresolved_count") or 0)
    return "failed" if confirmed > 0 or unresolved > 0 else "success"


def _metrics_from_payload(
    payload: dict[str, Any],
    *,
    rollout_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metrics = {
        "public_exhibition_count": int(payload.get("public_exhibition_count") or 0),
        "candidate_pair_count": int(payload.get("candidate_pair_count") or 0),
        "confirmed_duplicate_count": int(payload.get("confirmed_duplicate_count") or 0),
        "keep_distinct_count": int(payload.get("keep_distinct_count") or 0),
        "unresolved_count": int(payload.get("unresolved_count") or 0),
        "candidate_pair_window_count": int(payload.get("candidate_pair_window_count") or 0),
        "confirmed_duplicate_window_count": int(
            payload.get("confirmed_duplicate_window_count") or 0
        ),
        "keep_distinct_window_count": int(payload.get("keep_distinct_window_count") or 0),
        "unresolved_window_count": int(payload.get("unresolved_window_count") or 0),
        "candidate_pair_total_count": int(payload.get("candidate_pair_total_count") or 0),
        "confirmed_duplicate_total_count": int(
            payload.get("confirmed_duplicate_total_count") or 0
        ),
        "keep_distinct_total_count": int(payload.get("keep_distinct_total_count") or 0),
        "unresolved_total_count": int(payload.get("unresolved_total_count") or 0),
        "high_confidence_duplicate_count": int(payload.get("high_confidence_duplicate_count") or 0),
        "high_confidence_duplicate_cluster_count": int(
            payload.get("high_confidence_duplicate_cluster_count") or 0
        ),
        "high_confidence_duplicate_total_count": int(
            payload.get("high_confidence_duplicate_total_count") or 0
        ),
        "high_confidence_duplicate_total_cluster_count": int(
            payload.get("high_confidence_duplicate_total_cluster_count") or 0
        ),
        "since_days": int(payload.get("since_days") or 14),
    }
    if rollout_payload:
        for key in (
            "identity_gate_decision_count",
            "identity_gate_veto_create_count",
            "identity_gate_allow_create_count",
            "identity_gate_fail_safe_count",
            "identity_gate_vector_error_count",
            "identity_gate_vector_available_count",
            "identity_gate_final_probe_veto_count",
            "identity_gate_matched_event_count",
        ):
            metrics[key] = int(rollout_payload.get(key) or 0)
        env_readiness = rollout_payload.get("env_readiness") or {}
        metrics["identity_gate_env_ready"] = 1 if bool(env_readiness.get("ready")) else 0
        for env_key, env_value in env_readiness.items():
            if env_key == "smart_update_identity_google_key_env":
                continue
            metrics[f"identity_gate_env_{env_key}"] = 1 if bool(env_value) else 0
    return metrics


def _details_from_payload(
    payload: dict[str, Any],
    *,
    run_id: str | None,
    rollout_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    max_pairs = _int_env("EXHIBITION_DUPLICATE_AUDIT_MAX_DETAILS_PAIRS", 20, minimum=1)
    candidate_pairs = list(payload.get("candidate_pairs") or [])
    confirmed = list(payload.get("confirmed_duplicates") or [])
    keep_distinct = list(payload.get("keep_distinct_pairs") or [])
    unresolved = list(payload.get("unresolved_pairs") or [])
    all_candidates = list(payload.get("all_candidate_pairs") or [])
    details = {
        "scheduler_run_id": run_id,
        "current_date": payload.get("current_date"),
        "since_date": payload.get("since_date"),
        "since_days": payload.get("since_days"),
        "candidate_pairs_truncated": len(candidate_pairs) > max_pairs,
        "candidate_pairs": candidate_pairs[:max_pairs],
        "confirmed_duplicates_truncated": len(confirmed) > max_pairs,
        "confirmed_duplicates": confirmed[:max_pairs],
        "keep_distinct_pairs_truncated": len(keep_distinct) > max_pairs,
        "keep_distinct_pairs": keep_distinct[:max_pairs],
        "unresolved_pairs_truncated": len(unresolved) > max_pairs,
        "unresolved_pairs": unresolved[:max_pairs],
        "all_candidate_pair_count": len(all_candidates),
        "all_candidate_pairs_truncated": len(all_candidates) > max_pairs,
        "all_candidate_pairs": all_candidates[:max_pairs],
        # Compatibility aliases contain only actionable pairs.
        "duplicates_truncated": len(confirmed) + len(unresolved) > max_pairs,
        "duplicates": (confirmed + unresolved)[:max_pairs],
        "all_duplicate_count": int(payload.get("high_confidence_duplicate_total_count") or 0),
        "all_duplicates_truncated": len(list(payload.get("all_duplicates") or [])) > max_pairs,
        "all_duplicates": list(payload.get("all_duplicates") or [])[:max_pairs],
    }
    if rollout_payload:
        details["identity_gate"] = {
            "decision_count": rollout_payload.get("identity_gate_decision_count"),
            "veto_create_count": rollout_payload.get("identity_gate_veto_create_count"),
            "fail_safe_count": rollout_payload.get("identity_gate_fail_safe_count"),
            "vector_error_count": rollout_payload.get("identity_gate_vector_error_count"),
            "modes": rollout_payload.get("identity_gate_modes"),
            "reasons": rollout_payload.get("identity_gate_reasons"),
            "recent_fail_safes": rollout_payload.get("recent_fail_safes"),
            "recent_vector_errors": rollout_payload.get("recent_vector_errors"),
            "env_readiness": rollout_payload.get("env_readiness"),
        }
    return details


async def _notify_admin(
    db: Database,
    bot: Any,
    *,
    payload: dict[str, Any],
    status: str,
) -> None:
    candidate_count = int(payload.get("candidate_pair_count") or 0)
    confirmed_count = int(payload.get("confirmed_duplicate_count") or 0)
    keep_distinct_count = int(payload.get("keep_distinct_count") or 0)
    unresolved_count = int(payload.get("unresolved_count") or 0)
    actionable_count = confirmed_count + unresolved_count
    notify_success = _env_enabled("EXHIBITION_DUPLICATE_AUDIT_NOTIFY_ON_SUCCESS", default=False)
    if actionable_count <= 0 and not notify_success:
        return
    if bot is None or not hasattr(bot, "send_message"):
        return
    chat_id = await resolve_superadmin_chat_id(db)
    if not chat_id:
        return

    max_pairs = _int_env("EXHIBITION_DUPLICATE_AUDIT_MAX_ALERT_PAIRS", 8, minimum=1)
    lines = [
        (
            "🚨 Exhibition duplicate audit: есть подтверждённые/неразрешённые пары"
            if actionable_count > 0
            else "✅ Exhibition duplicate audit: все кандидаты доказанно различны"
        ),
        f"status={status}",
        f"current_date={payload.get('current_date')} window_days={payload.get('since_days')}",
        f"public_exhibitions={payload.get('public_exhibition_count')} candidates={candidate_count} confirmed={confirmed_count} keep_distinct={keep_distinct_count} unresolved={unresolved_count}",
        f"actionable_clusters={payload.get('high_confidence_duplicate_cluster_count')}",
    ]
    for pair in (list(payload.get("confirmed_duplicates") or []) + list(payload.get("unresolved_pairs") or []))[:max_pairs]:
        lines.append(
            f"• #{pair.get('left_id')} ↔ #{pair.get('right_id')} "
            f"{float(pair.get('confidence') or 0):.3f}: "
            f"{pair.get('left_title')} / {pair.get('right_title')}"
        )
    if actionable_count > max_pairs:
        lines.append(f"… ещё {actionable_count - max_pairs} пар(ы) в ops_run.details_json")
    try:
        await bot.send_message(chat_id, "\n".join(lines), disable_web_page_preview=True)
    except Exception:
        logger.warning("exhibition_duplicate_audit: admin notification failed", exc_info=True)


async def run_exhibition_duplicate_audit_scheduler(
    db: Database,
    bot: Any | None = None,
    *,
    trigger: str = "scheduled",
    run_id: str | None = None,
    current_date: date | None = None,
    raise_on_duplicates: bool | None = None,
) -> dict[str, Any]:
    """Run the read-only /vystavki/ duplicate acceptance audit and record ops_run.

    The audit is intentionally non-mutating. Confirmed duplicates and candidates
    without authoritative evidence are rollout/quality regressions. A heuristic
    candidate is non-actionable only after a correlated, source-grounded
    ``FINAL_DISTINCT`` verdict.
    """

    db_path = Path(getattr(db, "path", "") or "")
    since_days = _int_env("EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS", 14, minimum=1)
    rollout_since_date = _date_env("EXHIBITION_DUPLICATE_AUDIT_SINCE_DATE")
    should_raise = _env_enabled("EXHIBITION_DUPLICATE_AUDIT_RAISE_ON_DUPLICATES", default=True)
    if raise_on_duplicates is not None:
        should_raise = bool(raise_on_duplicates)

    ops_details_start = {
        "scheduler_run_id": run_id,
        "db_path": str(db_path),
        "since_days": since_days,
        "since_date": rollout_since_date.isoformat() if rollout_since_date else None,
    }
    ops_run_id = await start_ops_run(
        db,
        kind="exhibition_duplicate_audit",
        trigger=trigger,
        operator_id=0 if trigger == "scheduled" else None,
        details=ops_details_start,
    )

    try:
        if not db_path or str(db_path) in {":memory:", ""}:
            raise RuntimeError("exhibition duplicate audit requires a file-backed SQLite DB")
        payload = build_audit_payload(
            db_path, current=current_date, since_days=since_days, since_date=rollout_since_date
        )
        rollout_payload = build_rollout_payload(
            db_path, current=current_date, since_days=since_days, since_date=rollout_since_date
        )
        status = _status_from_payload(payload)
        metrics = _metrics_from_payload(payload, rollout_payload=rollout_payload)
        details = _details_from_payload(payload, run_id=run_id, rollout_payload=rollout_payload)
        await finish_ops_run(db, run_id=ops_run_id, status=status, metrics=metrics, details=details)
        logger.info(
            "exhibition_duplicate_audit status=%s public_exhibitions=%s candidates=%s confirmed=%s unresolved=%s",
            status,
            metrics["public_exhibition_count"],
            metrics["candidate_pair_count"],
            metrics["confirmed_duplicate_count"],
            metrics["unresolved_count"],
        )
        await _notify_admin(db, bot, payload=payload, status=status)
        if status == "failed" and should_raise:
            raise RuntimeError(
                "public exhibition duplicates require action: "
                f"confirmed={metrics['confirmed_duplicate_count']} "
                f"unresolved={metrics['unresolved_count']}"
            )
        return payload
    except Exception as exc:
        if "payload" not in locals():
            await finish_ops_run(
                db,
                run_id=ops_run_id,
                status="error",
                metrics={"since_days": since_days},
                details={**ops_details_start, "error": str(exc)},
            )
        raise
