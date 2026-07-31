"""Strict production factory for the festival Antigravity collect-only service."""
from __future__ import annotations

import os
from pathlib import Path

from db import Database
from google_ai.client import GoogleAIClient
from google_ai.interactions import AntigravityInteractionsClient
from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client

from .coordinator import FestivalResearchCoordinator
from .repository import FestivalResearchRepository
from .service import FestivalWebResearchService
from .validators import load_taxonomy_registry, taxonomy_registry_hash


def build_festival_web_research_service(db: Database) -> FestivalWebResearchService:
    if (os.getenv("GOOGLE_AI_EXTERNAL_ACCOUNTING_COMPAT") or "").strip().lower() in {"1", "true", "yes", "on"}:
        raise RuntimeError("legacy Antigravity accounting is forbidden in queue/scheduler runtime")
    def legacy_limiter_client():
        url = (os.getenv("SUPABASE_URL") or "").strip()
        key = (os.getenv("SUPABASE_KEY") or "").strip()
        if not url or not key:
            raise RuntimeError("SUPABASE_URL/SUPABASE_KEY are required for Antigravity quota accounting")
        from supabase import create_client

        return create_client(url, key)

    allowed = ("GOOGLE_API_KEY", "GOOGLE_API_KEY2", "GOOGLE_API_KEY3", "GOOGLE_API_KEY4", "GOOGLE_API_KEY5")
    key_envs = tuple(name for name in allowed if (os.getenv(name) or "").strip())
    if not key_envs:
        raise RuntimeError("no registered Antigravity Google key envs are configured")
    limiter = GoogleAIClient(
        supabase_client=build_google_ai_limiter_supabase_client(
            fallback_factory=legacy_limiter_client,
            require_configured=True,
        ),
        consumer="festival_antigravity",
        account_name="festival_web_research",
        reserve_key_envs=key_envs,
    )
    provider = AntigravityInteractionsClient(limiter, key_envs=key_envs)
    registry_path = Path(__file__).parent / "schemas/festival-taxonomy-registry-v2.json"
    registry_hash = taxonomy_registry_hash(load_taxonomy_registry(registry_path.read_bytes()))
    coordinator = FestivalResearchCoordinator(
        provider=provider,
        repository=FestivalResearchRepository(db),
        artifact_root=Path(os.getenv("FESTIVAL_WEB_RESEARCH_ARTIFACT_ROOT") or "artifacts/festival-web-research"),
        taxonomy_sha256=registry_hash,
        max_total_tokens=int(os.getenv("FESTIVAL_WEB_RESEARCH_MAX_TOTAL_TOKENS") or "60000"),
        deadline_seconds=float(os.getenv("FESTIVAL_WEB_RESEARCH_DEADLINE_SECONDS") or "900"),
    )
    return FestivalWebResearchService(repository=coordinator.repository, coordinator=coordinator)
