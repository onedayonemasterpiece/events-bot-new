#!/usr/bin/env python3
"""Manual collect-only Antigravity festival research runner."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        if name and name not in os.environ:
            os.environ[name] = value.strip().strip('"').strip("'")


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--edition")
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--db", default="artifacts/codex/festival-web-research.sqlite")
    parser.add_argument("--artifact-root", default="artifacts/codex/festival-web-research")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--max-total-tokens", type=int, default=60000)
    parser.add_argument(
        "--key-env",
        action="append",
        help="Explicit registered key env pool (repeatable); defaults to configured GOOGLE_API_KEY[2-5]",
    )
    parser.add_argument("--deadline-seconds", type=float, default=900)
    parser.add_argument("--no-conflict-check", action="store_true", help="Never spend the optional third interaction")
    parser.add_argument("--retry", action="store_true")
    parser.add_argument(
        "--allow-legacy-accounting",
        action="store_true",
        help="Canary only: use the existing limiter finalizer if migration 007 is unavailable",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


async def _main() -> int:
    args = _args()
    _load_env(Path(args.env_file))
    if args.allow_legacy_accounting:
        os.environ["GOOGLE_AI_EXTERNAL_ACCOUNTING_COMPAT"] = "1"
    from supabase import create_client

    from db import Database
    from festival_web_research.coordinator import FestivalResearchCoordinator
    from festival_web_research.formatting import format_research_result
    from festival_web_research.repository import FestivalResearchRepository
    from festival_web_research.service import FestivalWebResearchService
    from festival_web_research.validators import load_taxonomy_registry, taxonomy_registry_hash
    from google_ai.client import GoogleAIClient
    from google_ai.interactions import AntigravityInteractionsClient

    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    supabase_key = os.environ.get("SUPABASE_KEY", "").strip()
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL/SUPABASE_KEY are required for the shared limiter")
    allowed_key_envs = ("GOOGLE_API_KEY", "GOOGLE_API_KEY2", "GOOGLE_API_KEY3", "GOOGLE_API_KEY4", "GOOGLE_API_KEY5")
    requested_key_envs = tuple(args.key_env or allowed_key_envs)
    if any(name not in allowed_key_envs for name in requested_key_envs):
        raise RuntimeError("--key-env must be one of the registered GOOGLE_API_KEY[2-5] names")
    key_envs = tuple(
        name for name in requested_key_envs
        if os.environ.get(name, "").strip()
    )
    if not key_envs:
        raise RuntimeError("no configured Google key envs")
    supabase = create_client(supabase_url, supabase_key)
    limiter = GoogleAIClient(
        supabase_client=supabase,
        consumer="festival_antigravity",
        account_name="festival_web_research",
        reserve_key_envs=key_envs,
    )
    provider = AntigravityInteractionsClient(limiter, key_envs=key_envs)
    db = Database(str(Path(args.db)))
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        registry_path = ROOT / "festival_web_research/schemas/festival-taxonomy-registry-v2.json"
        registry = load_taxonomy_registry(registry_path.read_bytes())
        coordinator = FestivalResearchCoordinator(
            provider=provider,
            repository=repository,
            artifact_root=Path(args.artifact_root),
            taxonomy_sha256=taxonomy_registry_hash(registry),
            max_total_tokens=args.max_total_tokens,
            deadline_seconds=args.deadline_seconds,
        )
        service = FestivalWebResearchService(repository=repository, coordinator=coordinator)
        result = await service.collect(
            name_hint=args.name,
            edition_hint=args.edition,
            urls=args.url,
            allow_c=not args.no_conflict_check,
            force_retry=args.retry,
        )
        if args.json:
            print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))
        else:
            print(format_research_result(result))
        return 0 if result.state == "review" else 2
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
