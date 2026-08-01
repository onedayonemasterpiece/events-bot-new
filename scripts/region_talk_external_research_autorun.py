#!/usr/bin/env python3
"""Grounded autonomous web-publication discovery for Region Talk.

The worker runs on the Fly control plane before the queue orchestrator.  It
uses Gemini Search + URL Context, validates the existing strict external
publication contract, and hands accepted rows to the same idempotent importer
used for operator-supplied research files.  It never uses Telegram sessions.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from region_talk_llm_runtime import build_google_ai_client  # noqa: E402
from scripts.region_talk_external_publication_import import (  # noqa: E402
    duplicate_guard_from_seen_publications,
    prepare_import,
    write_ydb,
)
from scripts.region_talk_external_research_registry import publish_current_registry  # noqa: E402
from scripts.region_talk_external_research_request import read_seen_from_ydb  # noqa: E402
from scripts.region_talk_goal_notify import load_env  # noqa: E402


# This is a production discovery worker, not an external consultant review.
# The stable model is registered in the shared limiter and officially supports
# Search grounding, URL Context and structured outputs.
DEFAULT_MODEL = "gemini-3.5-flash-lite"
DEFAULT_PROMPT = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research.prompt.txt"
DEFAULT_OUTPUT_DIR = Path("/data/runtime_logs/region_talk")
DEFAULT_MARKER = Path("/data/region_talk_external_research_last_success.json")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int((os.getenv(name) or str(default)).strip())
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def marker_is_fresh(marker_path: Path, *, cooldown_hours: int, now: datetime | None = None) -> bool:
    if not marker_path.is_file():
        return False
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    completed_at = _parse_time(payload.get("completed_at")) if isinstance(payload, dict) else None
    return bool(completed_at and completed_at + timedelta(hours=max(1, cooldown_hours)) > (now or utc_now()))


def build_prompt(*, prompt_path: Path, maximum_candidates: int, maximum_total_rows: int) -> str:
    base = prompt_path.read_text(encoding="utf-8")
    return base + (
        "\n\nAUTONOMOUS INCREMENTAL RUN OVERRIDE\n"
        "Execute the research now and return the one strict JSON object directly in the response. "
        f"This incremental run is bounded to at most {maximum_candidates} clean candidates and at most "
        f"{maximum_total_rows} total candidates+excluded+unresolved rows. "
        "Prefer distinct non-scholarly contours while retaining genuinely strong scholarly work; "
        "do not lower any verification, source-externality, full-text, date, tone, rights, or evidence gate. "
        "The live registry remains authoritative.\n"
    )


def _write_private_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


async def run_autoresearch(
    *,
    execute: bool,
    force: bool,
    input_path: Path | None,
    prompt_path: Path,
    output_dir: Path,
    marker_path: Path,
    model: str,
    default_key_env: str,
    maximum_candidates: int,
    maximum_total_rows: int,
    cooldown_hours: int,
    client: Any | None = None,
) -> dict[str, Any]:
    started = utc_now()
    if input_path is None and not force and marker_is_fresh(
        marker_path, cooldown_hours=cooldown_hours, now=started
    ):
        return {
            "ok": True,
            "stage": "external_research",
            "status": "skipped_cooldown",
            "cooldown_hours": cooldown_hours,
        }

    usage_payload: dict[str, Any] = {}
    actual_model = model
    if input_path is not None:
        raw = input_path.read_text(encoding="utf-8")
    else:
        prompt = build_prompt(
            prompt_path=prompt_path,
            maximum_candidates=maximum_candidates,
            maximum_total_rows=maximum_total_rows,
        )
        if client is None:
            client = build_google_ai_client(
                default_env_var_name=default_key_env,
                consumer="region_talk_external_research",
            )
        client.provider_timeout_seconds = float(
            _env_int("REGION_TALK_EXTERNAL_RESEARCH_PROVIDER_TIMEOUT_SECONDS", 720, minimum=60, maximum=1200)
        )
        raw, usage = await client.generate_content_async(
            model=model,
            prompt=prompt,
            generation_config={
                "temperature": 0.1,
                "response_mime_type": "application/json",
                "tools": [{"google_search": {}}, {"url_context": {}}],
            },
            max_output_tokens=_env_int(
                "REGION_TALK_EXTERNAL_RESEARCH_MAX_OUTPUT_TOKENS", 32768, minimum=4096, maximum=65536
            ),
        )
        usage_payload = {
            "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
            "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
            "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
        }
        actual_model = str(getattr(usage, "model", "") or model)

    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    raw_path = output_dir / f"external-research-{stamp}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(raw, encoding="utf-8")
    os.chmod(raw_path, 0o600)
    payload = json.loads(raw)

    duplicate_guard = None
    if execute:
        duplicate_guard = duplicate_guard_from_seen_publications(read_seen_from_ydb(20000))
    prepared = prepare_import(payload, duplicate_guard=duplicate_guard)
    written = write_ydb(prepared["ydb_rows"]) if execute else 0
    registry: dict[str, Any] | None = None
    registry_error = ""
    if execute:
        try:
            registry = publish_current_registry(seen_limit=20000)
        except Exception as exc:  # imported rows remain durable and retryable
            registry_error = f"{type(exc).__name__}: {str(exc)[:500]}"

    batch = prepared["batch"]
    result = {
        "ok": True,
        "stage": "external_research",
        "status": "executed" if execute else "validated",
        "model": actual_model,
        "requested_model": model,
        "request_id": batch.get("request_id"),
        "raw_path": str(raw_path),
        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "usage": usage_payload,
        "candidate_rows_received": int(batch.get("candidate_rows_received") or 0),
        "candidate_rows_valid": int(batch.get("candidate_rows_valid") or 0),
        "candidate_rows_rejected": int(batch.get("candidate_rows_rejected") or 0),
        "ready_for_region_talk_scoring": int(batch.get("ready_for_region_talk_scoring") or 0),
        "manual_or_blocked": int(batch.get("manual_or_blocked") or 0),
        "seen_publication_rows_staged": int(batch.get("seen_publication_rows_staged") or 0),
        "written_ydb_rows": int(written),
        "registry_seen_publication_count": int((registry or {}).get("seen_publication_count") or 0),
        "registry_publication_error": registry_error,
        "completed_at": utc_now().isoformat(),
    }
    if execute:
        _write_private_json(marker_path, result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run grounded Region Talk external-publication research")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--execute", action="store_true", help="Write validated rows to YDB")
    parser.add_argument("--force", action="store_true", help="Ignore the durable cooldown marker")
    parser.add_argument("--input", type=Path, default=None, help="Validate/import an existing result without an LLM call")
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT)
    parser.add_argument("--output-dir", type=Path, default=Path(os.getenv("REGION_TALK_EXTERNAL_RESEARCH_OUTPUT_DIR") or DEFAULT_OUTPUT_DIR))
    parser.add_argument("--marker", type=Path, default=Path(os.getenv("REGION_TALK_EXTERNAL_RESEARCH_MARKER") or DEFAULT_MARKER))
    parser.add_argument("--model", default=os.getenv("REGION_TALK_EXTERNAL_RESEARCH_MODEL") or DEFAULT_MODEL)
    parser.add_argument("--default-key-env", default=os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3")
    parser.add_argument("--maximum-candidates", type=int, default=_env_int("REGION_TALK_EXTERNAL_RESEARCH_MAX_CANDIDATES", 5, minimum=1, maximum=12))
    parser.add_argument("--maximum-total-rows", type=int, default=_env_int("REGION_TALK_EXTERNAL_RESEARCH_MAX_TOTAL_ROWS", 15, minimum=1, maximum=40))
    parser.add_argument("--cooldown-hours", type=int, default=_env_int("REGION_TALK_EXTERNAL_RESEARCH_COOLDOWN_HOURS", 6, minimum=1, maximum=48))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    try:
        result = asyncio.run(
            run_autoresearch(
                execute=bool(args.execute),
                force=bool(args.force),
                input_path=args.input,
                prompt_path=args.prompt,
                output_dir=args.output_dir,
                marker_path=args.marker,
                model=str(args.model),
                default_key_env=str(args.default_key_env),
                maximum_candidates=max(1, int(args.maximum_candidates)),
                maximum_total_rows=max(1, int(args.maximum_total_rows)),
                cooldown_hours=max(1, int(args.cooldown_hours)),
            )
        )
    except Exception as exc:
        result = {
            "ok": False,
            "stage": "external_research",
            "status": "failed",
            "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
        }
        print(json.dumps(result, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
