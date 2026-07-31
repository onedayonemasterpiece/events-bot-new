"""Application service for collect/review lifecycle; public apply is absent by design."""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Sequence

from .coordinator import FestivalResearchCoordinator, ResearchResult
from .evidence import canonical_json_sha256
from .prompts import (
    CONTRACT_VERSION,
    NORMALIZER_VERSION,
    PROMPT_VERSION,
    TAXONOMY_VERSION,
    ResearchTarget,
    prompt_sha256,
)
from .repository import FestivalResearchRepository


class FestivalWebResearchService:
    def __init__(self, *, repository: FestivalResearchRepository, coordinator: FestivalResearchCoordinator) -> None:
        self.repository = repository
        self.coordinator = coordinator

    async def collect(
        self,
        *,
        name_hint: str,
        edition_hint: str | None,
        urls: Sequence[str],
        target_key: str | None = None,
        input_fingerprint: str | None = None,
        queue_item_ids: Sequence[int] = (),
        allow_c: bool = True,
        force_retry: bool = False,
    ) -> ResearchResult:
        normalized_urls = tuple(dict.fromkeys(str(url).strip() for url in urls if str(url).strip()))
        if not name_hint.strip() or not normalized_urls:
            raise ValueError("name_hint and at least one URL are required")
        key = target_key or f"manual:{name_hint.strip().casefold()}:{(edition_hint or '').strip().casefold()}"
        fingerprint = input_fingerprint or canonical_json_sha256(
            {
                "target_key": key,
                "name_hint": name_hint.strip(),
                "edition_hint": (edition_hint or "").strip() or None,
                "urls": sorted(normalized_urls),
                "contract_version": CONTRACT_VERSION,
                "prompt_version": PROMPT_VERSION,
                "prompt_sha256": prompt_sha256(),
                "normalizer_version": NORMALIZER_VERSION,
                "taxonomy_sha256": self.coordinator.taxonomy_sha256,
            }
        )
        existing = await self.repository.get_run_by_fingerprint(fingerprint)
        if existing and not force_retry:
            if existing.candidate_json:
                from .coordinator import ResearchResult
                return ResearchResult(
                    run_id=existing.id,
                    run_uid=existing.run_uid,
                    state=existing.state,
                    review_status=existing.review_status,
                    candidate=existing.candidate_json,
                    quality=existing.quality_json,
                    lanes=[],
                )
            raise RuntimeError(f"research run {existing.id} already exists in state {existing.state}; use retry")
        if existing:
            run = existing
            await self.repository.update_run(run.id, state="pending", review_status="pending")
        else:
            run = await self.repository.create_run(
                run_uid=str(uuid.uuid4()),
                target_key=key,
                series_candidate=name_hint.strip(),
                edition_candidate=(edition_hint or "").strip() or None,
                state="pending",
                mode="collect_only",
                review_status="pending",
                input_fingerprint=fingerprint,
                orchestration_version="festival-antigravity-coordinator-v1",
                contract_version=CONTRACT_VERSION,
                taxonomy_version=TAXONOMY_VERSION,
                taxonomy_sha256=self.coordinator.taxonomy_sha256,
            )
        if queue_item_ids:
            await self.repository.attach_queue_items(
                run.id,
                queue_item_ids=queue_item_ids,
                original_status="pending",
            )
        target = ResearchTarget(
            name_hint=name_hint.strip(),
            edition_hint=(edition_hint or "").strip() or None,
            urls=normalized_urls,
            target_key=key,
        )
        return await self.coordinator.collect(run_id=run.id, run_uid=run.run_uid, target=target, allow_c=allow_c)

    async def approve(self, run_id: int, *, operator: str, reason: str | None = None):
        run = await self.repository.get_run(run_id)
        if run is None:
            raise LookupError(f"research run {run_id} not found")
        if run.state != "review" or not run.candidate_json:
            raise ValueError("only a validated review candidate can be approved")
        quality = run.quality_json if isinstance(run.quality_json, dict) else {}
        independently_supported = bool(quality.get("independent_agreement")) or bool(quality.get("c_applied"))
        if not independently_supported:
            raise ValueError("candidate lacks A/B agreement or an applied C adjudication")
        if int(quality.get("unresolved_inventory_count") or 0) != 0:
            raise ValueError("candidate has unresolved programme inventory")
        # Approval records a human revision decision only. No Festival/Event/page mutation exists here.
        return await self.repository.review(run_id, decision="approved", operator=operator, reason=reason)

    async def reject(self, run_id: int, *, operator: str, reason: str):
        if not reason.strip():
            raise ValueError("rejection reason is required")
        return await self.repository.review(run_id, decision="rejected", operator=operator, reason=reason.strip())
