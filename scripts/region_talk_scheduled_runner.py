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
        ("TELEGRAM_BOT_TOKEN",),
        ("TG_API_ID", "TELEGRAM_API_ID"),
        ("TG_API_HASH", "TELEGRAM_API_HASH"),
        ("SUPABASE_URL",),
        ("SUPABASE_SERVICE_KEY", "SUPABASE_KEY"),
    )
    missing = ["|".join(group) for group in groups if not any(present(name) for name in group)]

    default_key_name = str(values.get("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3").strip()
    key_aliases = {default_key_name}
    if default_key_name == "GOOGLE_API_KEY3":
        key_aliases.add("GOOGLE_API_KEY_3")
    if not any(present(name) for name in key_aliases):
        missing.append("|".join(sorted(key_aliases)))
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


def _compact_metrics(payload: Mapping[str, Any]) -> dict[str, int]:
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    keys = (
        "publication_candidate_total",
        "publication_unsent_confirmed_total",
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
            trigger="scheduled",
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
        # Scheduled delivery must never reuse the local live-E2E human
        # session from a remote Fly machine.  Telegram invalidates an MTProto
        # authorization used concurrently from separate connections/IPs.
        child_env["REGION_TALK_NOTIFY_TRANSPORT"] = "bot_api"
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
            )

            async def consume() -> None:
                nonlocal last_payload
                assert process.stdout is not None
                while True:
                    raw = await process.stdout.readline()
                    if not raw:
                        break
                    line = raw.decode("utf-8", errors="replace").rstrip("\n")
                    output.write(line + "\n")
                    output.flush()
                    try:
                        payload = json.loads(line)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(payload, dict):
                        last_payload = payload
                        LOGGER.info(
                            "region_talk cycle=%s actions=%s metrics=%s",
                            payload.get("cycle"),
                            payload.get("selected_actions") or [],
                            _compact_metrics(payload),
                        )

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

            if _env_bool("REGION_TALK_PUBLICATION_PLAN_ENABLED", True):
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

        exit_code = int(process.returncode or 0)
        metrics = _compact_metrics(last_payload)
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
            "publication_plan_ok": bool(publication_plan.get("ok")),
            "publication_plan_status": publication_plan.get("status") or (
                "complete" if publication_plan.get("ok") else "failed"
            ),
            "publication_plan_exit_code": publication_plan_exit_code,
            "publication_plan_snapshot_id": publication_plan.get("snapshot_id"),
            "publication_plan_counts": publication_plan.get("counts") or {},
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
