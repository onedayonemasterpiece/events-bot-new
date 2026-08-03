#!/usr/bin/env python3
"""Production wrapper for autonomous Region Talk discovery cycles.

The APScheduler job calls :func:`run_region_talk_scheduled`.  This wrapper owns
the production-only concerns which do not belong in the queue orchestrator:
non-interactive credential preflight, a cross-process single-flight lock,
durable JSONL logs and ``ops_run`` lifecycle accounting.
"""
from __future__ import annotations

import asyncio
import argparse
import fcntl
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_run import finish_ops_run, start_ops_run  # noqa: E402


LOGGER = logging.getLogger(__name__)

DEFAULT_LOCK_FILE = "/data/region_talk_orchestrator.lock"
DEFAULT_LOG_DIR = "/data/runtime_logs/region_talk"


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int((os.getenv(name) or str(default)).strip())
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or ("1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def missing_autonomy_config(env: Mapping[str, str] | None = None) -> list[str]:
    """Return missing credential groups without ever returning secret values."""

    values = env if env is not None else os.environ

    def present(name: str) -> bool:
        return bool(str(values.get(name) or "").strip())

    groups: tuple[tuple[str, ...], ...] = (
        ("REGION_TALK_YDB_ENDPOINT",),
        ("REGION_TALK_YDB_DATABASE",),
        ("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON",),
        ("KAGGLE_USERNAME",),
        ("KAGGLE_KEY",),
        ("TELEGRAM_AUTH_BUNDLE_DISCOVERY1",),
        ("TELEGRAM_AUTH_BUNDLE_DISCOVERY2",),
        ("TG_API_ID", "TELEGRAM_API_ID"),
        ("TG_API_HASH", "TELEGRAM_API_HASH"),
        ("SUPABASE_URL",),
        ("SUPABASE_SERVICE_KEY", "SUPABASE_KEY"),
        ("GOOGLE_AI_LIMITER_SUPABASE_URL",),
        ("GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",),
    )
    missing = ["|".join(group) for group in groups if not any(present(name) for name in group)]

    database = str(values.get("REGION_TALK_YDB_DATABASE") or "").strip().rstrip("/")
    expected_database = str(
        values.get("REGION_TALK_YDB_EXPECTED_DATABASE") or ""
    ).strip().rstrip("/")
    if expected_database and database and database != expected_database:
        # Do not leak either database path into logs: the stable marker is
        # enough for operators and prevents a stale/wrong billing-account YDB
        # from being used after a secret or deploy rollback.
        missing.append("REGION_TALK_YDB_DATABASE(expected_database_mismatch)")

    default_key_name = str(values.get("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3").strip()
    key_aliases = {default_key_name}
    if default_key_name == "GOOGLE_API_KEY3":
        key_aliases.add("GOOGLE_API_KEY_3")
    if not any(present(name) for name in key_aliases):
        missing.append("|".join(sorted(key_aliases)))
    notify_transport = str(values.get("REGION_TALK_NOTIFY_TRANSPORT") or "telethon_discovery2").strip()
    if notify_transport not in {"bot_api", "telethon_discovery1", "telethon_discovery2"}:
        missing.append("REGION_TALK_NOTIFY_TRANSPORT(valid)")
    elif notify_transport == "bot_api" and not present("TELEGRAM_BOT_TOKEN"):
        missing.append("TELEGRAM_BOT_TOKEN")
    selected_bundle = str(
        values.get("REGION_TALK_AUTH_BUNDLE_ENV")
        or "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"
    ).strip()
    if selected_bundle not in {
        "TELEGRAM_AUTH_BUNDLE_DISCOVERY1",
        "TELEGRAM_AUTH_BUNDLE_DISCOVERY2",
    }:
        missing.append("REGION_TALK_AUTH_BUNDLE_ENV(dedicated_discovery_bundle)")
    elif not present(selected_bundle):
        missing.append(selected_bundle)
    return missing


def build_orchestrator_command(env: Mapping[str, str] | None = None) -> list[str]:
    values = env if env is not None else os.environ
    python_bin = str(values.get("REGION_TALK_ORCHESTRATOR_PYTHON") or sys.executable or "python3").strip()
    cmd = [
        python_bin,
        str(ROOT / "scripts" / "region_talk_orchestrator.py"),
        "--loop",
        "--execute-ready",
        "--max-actions-per-cycle",
        str(_env_int("REGION_TALK_SCHEDULED_MAX_ACTIONS_PER_CYCLE", 4, minimum=1, maximum=8)),
        "--max-runtime-minutes",
        str(_env_int("REGION_TALK_SCHEDULED_MAX_RUNTIME_MINUTES", 90, minimum=15, maximum=240)),
        "--no-progress-cycles",
        str(_env_int("REGION_TALK_SCHEDULED_NO_PROGRESS_CYCLES", 4, minimum=1, maximum=20)),
        "--cycle-sleep-seconds",
        str(_env_int("REGION_TALK_SCHEDULED_POLL_SECONDS", 180, minimum=30, maximum=900)),
        "--downstream-backlog-poll-seconds",
        str(_env_int("REGION_TALK_SCHEDULED_DOWNSTREAM_POLL_SECONDS", 60, minimum=15, maximum=300)),
        "--limit",
        str(_env_int("REGION_TALK_SCHEDULED_SCAN_LIMIT", 20000, minimum=1000, maximum=100000)),
        "--target-confirmed",
        "0",
    ]
    env_file = str(values.get("REGION_TALK_ORCHESTRATOR_ENV_FILE") or "").strip()
    if env_file:
        cmd.extend(["--env-file", str(Path(env_file).expanduser())])
    return cmd


def build_external_research_command(env: Mapping[str, str] | None = None) -> list[str]:
    values = env if env is not None else os.environ
    python_bin = str(values.get("REGION_TALK_ORCHESTRATOR_PYTHON") or sys.executable or "python3").strip()
    return [
        python_bin,
        str(ROOT / "scripts" / "region_talk_external_research_autorun.py"),
        "--execute",
    ]


def build_publication_plan_command(env: Mapping[str, str] | None = None) -> list[str]:
    values = env if env is not None else os.environ
    python_bin = str(values.get("REGION_TALK_ORCHESTRATOR_PYTHON") or sys.executable or "python3").strip()
    try:
        days = int(str(values.get("REGION_TALK_PUBLICATION_PLAN_DAYS") or "14").strip())
    except ValueError:
        days = 14
    days = max(1, min(60, days))
    return [
        python_bin,
        str(ROOT / "scripts" / "region_talk_publication_plan.py"),
        "--execute",
        "--days",
        str(days),
    ]


def reaction_sync_script_path() -> Path:
    return ROOT / "scripts" / "region_talk_reaction_sync.py"


def build_reaction_sync_command(env: Mapping[str, str] | None = None) -> list[str]:
    """Build the fail-closed D2 reaction sync command without loading .env."""

    values = env if env is not None else os.environ
    python_bin = str(values.get("REGION_TALK_ORCHESTRATOR_PYTHON") or sys.executable or "python3").strip()
    try:
        limit = int(str(values.get("REGION_TALK_REACTION_SYNC_LIMIT") or "200").strip())
    except ValueError:
        limit = 200
    return [
        python_bin,
        str(reaction_sync_script_path()),
        "--env-file",
        "/dev/null",
        "--execute",
        "--limit",
        str(max(1, min(1000, limit))),
    ]


def reaction_sync_status(payload: Mapping[str, Any], exit_code: int | None) -> str:
    if bool(payload.get("ok")) and exit_code in {None, 0}:
        return "complete"
    error = str(payload.get("error") or "").lower()
    if any(token in error for token in (
        " is running", " is queued", " is initializing", " is active",
        "refusing concurrent use", "owns telegram_auth_bundle_discovery2",
        "cannot verify that region-talk-image-diagnostic is idle",
    )):
        return "deferred_d2_or_image_diagnostic_busy"
    return "failed"


def _compact_metrics(payload: Mapping[str, Any]) -> dict[str, int]:
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    keys = (
        "publication_candidate_total",
        "publication_unsent_confirmed_total",
        "publication_draft_ready_confirmed_total",
        "publication_draft_missing_confirmed_total",
        "publication_draft_backfill_actionable_total",
        "publication_draft_backfill_actionable_telegram_total",
        "publication_draft_backfill_actionable_vk_total",
        "publication_onboarding_ready_total",
        "bge_missing_current_sample_total",
        "image_pending_total",
        "finalizer_pending_url_total",
    )
    return {key: int(metrics.get(key) or 0) for key in keys}


def _prune_logs(log_dir: Path, *, retention_days: int) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, retention_days))
    for path in log_dir.glob("region-talk-*.jsonl"):
        try:
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if modified < cutoff:
                path.unlink()
        except OSError:
            LOGGER.warning("region_talk log retention failed path=%s", path, exc_info=True)


async def _finish(
    db_obj: Any,
    *,
    ops_run_id: int | None,
    status: str,
    metrics: Mapping[str, Any] | None,
    details: Mapping[str, Any],
) -> None:
    if db_obj is not None and hasattr(db_obj, "raw_conn"):
        await finish_ops_run(
            db_obj,
            run_id=ops_run_id,
            status=status,
            metrics=metrics,
            details=details,
        )


async def run_region_talk_scheduled(
    db_obj: Any,
    bot_obj: Any = None,
    *,
    scheduler_run_id: str | None = None,
    ops_trigger: str = "scheduled",
) -> dict[str, Any]:
    """Run one bounded autonomous discovery/finalization/delivery session."""

    del bot_obj  # Delivery is performed by the orchestrator's notifier action.
    started_at = datetime.now(timezone.utc)
    run_stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    base_details: dict[str, Any] = {
        "scheduler_run_id": scheduler_run_id,
        "started_at": started_at.isoformat(),
    }
    ops_run_id = None
    if db_obj is not None and hasattr(db_obj, "raw_conn"):
        ops_run_id = await start_ops_run(
            db_obj,
            kind="region_talk",
            trigger=str(ops_trigger or "scheduled"),
            operator_id=0,
            details=base_details,
        )

    missing = missing_autonomy_config()
    if missing:
        details = {**base_details, "error": "missing_autonomy_config", "missing": missing}
        await _finish(db_obj, ops_run_id=ops_run_id, status="failed", metrics={}, details=details)
        LOGGER.error("region_talk scheduled preflight failed missing=%s", ",".join(missing))
        return {"ok": False, "status": "failed", **details}

    lock_handle = None
    try:
        lock_path = Path(os.getenv("REGION_TALK_SCHEDULED_LOCK_FILE") or DEFAULT_LOCK_FILE)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            details = {**base_details, "skip_reason": "singleflight_locked", "lock_file": str(lock_path)}
            await _finish(db_obj, ops_run_id=ops_run_id, status="skipped", metrics={}, details=details)
            LOGGER.info("region_talk scheduled skipped: lock busy path=%s", lock_path)
            return {"ok": True, "status": "skipped", **details}

        log_dir = Path(os.getenv("REGION_TALK_SCHEDULED_LOG_DIR") or DEFAULT_LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        output_path = log_dir / f"region-talk-{run_stamp}-{scheduler_run_id or 'scheduled'}.jsonl"
        _prune_logs(
            log_dir,
            retention_days=_env_int("REGION_TALK_SCHEDULED_LOG_RETENTION_DAYS", 14, minimum=1, maximum=90),
        )

        cmd = build_orchestrator_command()
        child_env = os.environ.copy()
        child_env["REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL"] = "1"
        child_env["REGION_TALK_ALLOW_LOCAL_YC_FALLBACK"] = "0"
        # Human sessions are outside the Region Talk functional pipeline.
        # Strip them even when an operator shell inherited either variable:
        # delivery/discovery may use only their explicitly role-scoped
        # DISCOVERY1/DISCOVERY2 bundles (or the independently scoped bot).
        child_env.pop("TELEGRAM_AUTH_BUNDLE_E2E", None)
        child_env.pop("TELEGRAM_SESSION", None)
        child_env.pop("TG_SESSION", None)
        child_env["REGION_TALK_AUTH_BUNDLE_ENV"] = str(
            child_env.get("REGION_TALK_AUTH_BUNDLE_ENV")
            or "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"
        ).strip()
        child_env["REGION_TALK_NOTIFY_TRANSPORT"] = str(
            child_env.get("REGION_TALK_NOTIFY_TRANSPORT") or "telethon_discovery2"
        ).strip()
        child_env["PYTHONUNBUFFERED"] = "1"
        max_runtime_minutes = _env_int(
            "REGION_TALK_SCHEDULED_MAX_RUNTIME_MINUTES", 90, minimum=15, maximum=240
        )
        hard_timeout = (max_runtime_minutes + 15) * 60
        last_payload: dict[str, Any] = {}
        publication_plan: dict[str, Any] = {
            "ok": True,
            "stage": "publication_plan",
            "status": "disabled",
        }
        publication_plan_exit_code: int | None = None
        reaction_sync: dict[str, Any] = {
            "ok": True,
            "stage": "operator_reaction_sync",
            "status": "disabled",
        }
        reaction_sync_exit_code: int | None = None

        with output_path.open("w", encoding="utf-8") as output:
            os.chmod(output_path, 0o600)
            external_research: dict[str, Any] = {
                "ok": True,
                "stage": "external_research",
                "status": "disabled",
            }
            external_research_exit_code: int | None = None
            # Opt in only after the provider project has non-zero web-search
            # quota.  Ordinary text quota is not sufficient for grounding and
            # a disabled web lane must not degrade social discovery cycles.
            if _env_bool("REGION_TALK_EXTERNAL_RESEARCH_ENABLED", False):
                research_process = await asyncio.create_subprocess_exec(
                    *build_external_research_command(),
                    cwd=str(ROOT),
                    env=child_env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    research_stdout, _ = await asyncio.wait_for(
                        research_process.communicate(),
                        timeout=_env_int(
                            "REGION_TALK_EXTERNAL_RESEARCH_TIMEOUT_SECONDS",
                            900,
                            minimum=60,
                            maximum=1800,
                        ),
                    )
                except asyncio.TimeoutError:
                    research_process.terminate()
                    try:
                        await asyncio.wait_for(research_process.wait(), timeout=20)
                    except asyncio.TimeoutError:
                        research_process.kill()
                        await research_process.wait()
                    research_stdout = b""
                    external_research = {
                        "ok": False,
                        "stage": "external_research",
                        "status": "timed_out",
                    }
                external_research_exit_code = int(research_process.returncode or 0)
                for raw in research_stdout.decode("utf-8", errors="replace").splitlines():
                    line = raw.strip()
                    if not line:
                        continue
                    output.write(line + "\n")
                    output.flush()
                    try:
                        candidate = json.loads(line)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(candidate, dict) and candidate.get("stage") == "external_research":
                        external_research = candidate
                LOGGER.info(
                    "region_talk external research status=%s exit_code=%s valid=%s ready=%s",
                    external_research.get("status"),
                    external_research_exit_code,
                    external_research.get("candidate_rows_valid"),
                    external_research.get("ready_for_region_talk_scoring"),
                )

            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(ROOT),
                env=child_env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                # One orchestrator cycle is emitted as a single JSON line and
                # can legitimately exceed asyncio's 64 KiB StreamReader
                # default.  A larger transport buffer avoids pausing the child
                # before the consumer gets a chance to drain that line.  The
                # consumer below is chunk-based as the primary protection, so
                # correctness does not depend on this limit alone.
                limit=_env_int(
                    "REGION_TALK_SCHEDULED_STDOUT_LIMIT_BYTES",
                    8 * 1024 * 1024,
                    minimum=256 * 1024,
                    maximum=64 * 1024 * 1024,
                ),
            )

            async def consume() -> None:
                nonlocal last_payload
                assert process.stdout is not None
                pending = bytearray()

                def consume_line(raw_line: bytes) -> None:
                    nonlocal last_payload
                    line = raw_line.decode("utf-8", errors="replace").rstrip("\r")
                    output.write(line + "\n")
                    output.flush()
                    try:
                        payload = json.loads(line)
                    except (TypeError, ValueError):
                        return
                    if isinstance(payload, dict):
                        last_payload = payload
                        LOGGER.info(
                            "region_talk cycle=%s actions=%s metrics=%s",
                            payload.get("cycle"),
                            payload.get("selected_actions") or [],
                            _compact_metrics(payload),
                        )

                while True:
                    chunk = await process.stdout.read(64 * 1024)
                    if not chunk:
                        break
                    pending.extend(chunk)
                    while True:
                        newline = pending.find(b"\n")
                        if newline < 0:
                            break
                        raw_line = bytes(pending[:newline])
                        del pending[: newline + 1]
                        consume_line(raw_line)
                if pending:
                    consume_line(bytes(pending))

            consumer = asyncio.create_task(consume())
            timed_out = False
            try:
                await asyncio.wait_for(process.wait(), timeout=hard_timeout)
            except asyncio.TimeoutError:
                timed_out = True
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=20)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            await consumer

            # Approval/rewrite decisions must be projected before the daily
            # publication queue is recalculated.  The sync command itself
            # fail-closes on DISCOVERY2 and verifies that ImageDiagnostic is
            # idle; a legitimate D2 busy state is a non-fatal deferral and is
            # retried by the next scheduled Region Talk slot.
            if _env_bool("REGION_TALK_REACTION_SYNC_ENABLED", True):
                if not reaction_sync_script_path().is_file():
                    reaction_sync = {
                        "ok": False,
                        "stage": "operator_reaction_sync",
                        "status": "deferred_script_not_available",
                        "retry": "next_scheduled_slot",
                    }
                else:
                    reaction_process = await asyncio.create_subprocess_exec(
                        *build_reaction_sync_command(child_env),
                        cwd=str(ROOT),
                        env=child_env,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                    )
                    try:
                        reaction_stdout, _ = await asyncio.wait_for(
                            reaction_process.communicate(),
                            timeout=_env_int(
                                "REGION_TALK_REACTION_SYNC_TIMEOUT_SECONDS",
                                180,
                                minimum=30,
                                maximum=600,
                            ),
                        )
                    except asyncio.TimeoutError:
                        reaction_process.terminate()
                        try:
                            await asyncio.wait_for(reaction_process.wait(), timeout=20)
                        except asyncio.TimeoutError:
                            reaction_process.kill()
                            await reaction_process.wait()
                        reaction_stdout = b""
                        reaction_sync = {
                            "ok": False,
                            "stage": "operator_reaction_sync",
                            "status": "deferred_timeout",
                            "retry": "next_scheduled_slot",
                        }
                    reaction_sync_exit_code = int(reaction_process.returncode or 0)
                    for raw in reaction_stdout.decode("utf-8", errors="replace").splitlines():
                        line = raw.strip()
                        if not line:
                            continue
                        output.write(line + "\n")
                        output.flush()
                        try:
                            candidate = json.loads(line)
                        except (TypeError, ValueError):
                            continue
                        if isinstance(candidate, dict) and candidate.get("stage") == "operator_reaction_sync":
                            reaction_sync = candidate
                    if reaction_sync.get("status") not in {"deferred_timeout"}:
                        reaction_sync["status"] = reaction_sync_status(
                            reaction_sync, reaction_sync_exit_code
                        )
                    if str(reaction_sync.get("status") or "").startswith("deferred"):
                        reaction_sync["retry"] = "next_scheduled_slot"
                    LOGGER.info(
                        "region_talk reaction sync status=%s exit_code=%s observed=%s projected=%s",
                        reaction_sync.get("status"),
                        reaction_sync_exit_code,
                        reaction_sync.get("deliveries_observed_complete"),
                        reaction_sync.get("candidate_projections_changed"),
                    )

            reaction_gate_current = (
                not _env_bool("REGION_TALK_REACTION_SYNC_ENABLED", True)
                or str(reaction_sync.get("status") or "") == "complete"
            )
            if _env_bool("REGION_TALK_PUBLICATION_PLAN_ENABLED", True) and reaction_gate_current:
                plan_process = await asyncio.create_subprocess_exec(
                    *build_publication_plan_command(),
                    cwd=str(ROOT),
                    env=child_env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    plan_stdout, _ = await asyncio.wait_for(
                        plan_process.communicate(),
                        timeout=_env_int(
                            "REGION_TALK_PUBLICATION_PLAN_TIMEOUT_SECONDS",
                            300,
                            minimum=30,
                            maximum=900,
                        ),
                    )
                except asyncio.TimeoutError:
                    plan_process.terminate()
                    try:
                        await asyncio.wait_for(plan_process.wait(), timeout=20)
                    except asyncio.TimeoutError:
                        plan_process.kill()
                        await plan_process.wait()
                    plan_stdout = b""
                    publication_plan = {
                        "ok": False,
                        "stage": "publication_plan",
                        "status": "timed_out",
                    }
                publication_plan_exit_code = int(plan_process.returncode or 0)
                for raw in plan_stdout.decode("utf-8", errors="replace").splitlines():
                    line = raw.strip()
                    if not line:
                        continue
                    output.write(line + "\n")
                    output.flush()
                    try:
                        candidate = json.loads(line)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(candidate, dict) and candidate.get("stage") == "publication_plan":
                        publication_plan = candidate
                LOGGER.info(
                    "region_talk publication plan ok=%s exit_code=%s counts=%s",
                    publication_plan.get("ok"),
                    publication_plan_exit_code,
                    publication_plan.get("counts") or {},
                )
            elif _env_bool("REGION_TALK_PUBLICATION_PLAN_ENABLED", True):
                # Keep the last durable plan unchanged when operator reaction
                # evidence could not be synchronized. Recalculating here could
                # silently replace a prepared/reviewed identity using an older
                # reaction revision.
                publication_plan = {
                    "ok": False,
                    "stage": "publication_plan",
                    "status": "deferred_reaction_sync_not_current",
                    "reaction_sync_status": reaction_sync.get("status"),
                    "written_ydb_rows": 0,
                    "retry": "next_scheduled_slot",
                }

        exit_code = int(process.returncode or 0)
        metrics = _compact_metrics(last_payload)
        metrics.update({
            "reaction_sync_attempted": int(reaction_sync.get("status") != "disabled"),
            "reaction_sync_completed": int(reaction_sync.get("status") == "complete"),
            "reaction_sync_deferred": int(str(reaction_sync.get("status") or "").startswith("deferred")),
            "reaction_revisions_changed": int(reaction_sync.get("reaction_revisions_changed") or 0),
            "reaction_candidate_projections_changed": int(reaction_sync.get("candidate_projections_changed") or 0),
            "external_publication_intake_new_count": int(
                external_research.get("new_intake_count")
                or external_research.get("external_publication_intake_new_count")
                or external_research.get("imported_new")
                or 0
            ),
        })
        external_intake_ids = external_research.get("new_intake_ids")
        if not isinstance(external_intake_ids, list):
            external_intake_ids = external_research.get("external_publication_intake_ids")
        if not isinstance(external_intake_ids, list):
            external_intake_ids = external_research.get("imported_external_publication_ids")
        if not isinstance(external_intake_ids, list):
            external_intake_ids = []
        external_intake_ids = sorted({str(value) for value in external_intake_ids if str(value)})
        details = {
            **base_details,
            "output_path": str(output_path),
            "exit_code": exit_code,
            "timed_out": timed_out,
            "last_cycle": last_payload.get("cycle"),
            "last_selected_actions": last_payload.get("selected_actions") or [],
            "external_research_status": external_research.get("status"),
            "external_research_ok": bool(external_research.get("ok")),
            "external_research_exit_code": external_research_exit_code,
            "external_research_ready": int(external_research.get("ready_for_region_talk_scoring") or 0),
            "external_publication_intake_new_count": int(metrics.get("external_publication_intake_new_count") or 0),
            "external_publication_intake_new_ids": external_intake_ids,
            "publication_plan_ok": bool(publication_plan.get("ok")),
            "publication_plan_status": publication_plan.get("status") or (
                "complete" if publication_plan.get("ok") else "failed"
            ),
            "publication_plan_exit_code": publication_plan_exit_code,
            "publication_plan_snapshot_id": publication_plan.get("snapshot_id"),
            "publication_plan_counts": publication_plan.get("counts") or {},
            "reaction_sync_ok": bool(reaction_sync.get("ok")),
            "reaction_sync_status": reaction_sync.get("status"),
            "reaction_sync_exit_code": reaction_sync_exit_code,
            "reaction_sync_retry": reaction_sync.get("retry"),
            "reaction_sync_deliveries_observed": int(reaction_sync.get("deliveries_observed_complete") or 0),
            "reaction_sync_candidate_projections_changed": int(reaction_sync.get("candidate_projections_changed") or 0),
        }
        status = "success" if (
            exit_code == 0
            and not timed_out
            and bool(publication_plan.get("ok"))
            and (publication_plan_exit_code in {None, 0})
        ) else "failed"
        await _finish(db_obj, ops_run_id=ops_run_id, status=status, metrics=metrics, details=details)
        LOGGER.info(
            "region_talk scheduled finished status=%s exit_code=%s output=%s metrics=%s",
            status,
            exit_code,
            output_path,
            metrics,
        )
        return {"ok": status == "success", "status": status, "metrics": metrics, **details}
    except Exception as exc:
        details = {**base_details, "error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        await _finish(db_obj, ops_run_id=ops_run_id, status="failed", metrics={}, details=details)
        LOGGER.exception("region_talk scheduled runner failed")
        return {"ok": False, "status": "failed", **details}
    finally:
        try:
            if lock_handle is not None:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        if lock_handle is not None:
            lock_handle.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one bounded Region Talk autonomous session")
    parser.add_argument("--scheduler-run-id", default="manual-diagnostic")
    parser.add_argument("--db-path", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    missing = missing_autonomy_config()
    if args.preflight_only:
        print(json.dumps({"ok": not missing, "missing": missing}, ensure_ascii=False))
        return 0 if not missing else 2
    from db import Database

    result = asyncio.run(
        run_region_talk_scheduled(
            Database(args.db_path),
            scheduler_run_id=str(args.scheduler_run_id or "manual-diagnostic"),
        )
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
