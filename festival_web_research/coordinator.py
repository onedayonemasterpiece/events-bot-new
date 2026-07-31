"""Bounded A+B(+C) Antigravity orchestration and host-side validation."""
from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from google_ai.interactions import ANTIGRAVITY_AGENT, AntigravityInteractionsClient, ProviderInteraction

from .artifacts import build_artifact_manifest
from .contracts import (
    Claim,
    Decision,
    FestivalClassification,
    ProgrammeItem,
    ResearchSubject,
    SourceSnapshot,
)
from .evidence import canonical_json_sha256
from .prompts import (
    CONTRACT_VERSION,
    NORMALIZER_VERSION,
    PROMPT_VERSION,
    TAXONOMY_VERSION,
    ResearchTarget,
    build_conflict_prompt,
    build_lane_prompt,
)
from .repository import FestivalResearchRepository
from .validators import assert_no_agent_apply_authority, validate_reference_graph


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FestivalFacts(_Closed):
    name: str = Field(min_length=1, max_length=512)
    edition_label: str | None = Field(default=None, max_length=256)
    description_facts: list[str] = Field(default_factory=list, max_length=64)
    start_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    official_url: str | None = Field(default=None, max_length=4096)
    venue_names: list[str] = Field(default_factory=list, max_length=128)
    organizer_names: list[str] = Field(default_factory=list, max_length=128)


class LaneCandidate(_Closed):
    schema_version: Literal[CONTRACT_VERSION]
    lane: Literal["A", "B", "C"]
    festival: FestivalFacts
    classification: FestivalClassification
    sources: list[SourceSnapshot] = Field(min_length=1, max_length=256)
    subjects: list[ResearchSubject] = Field(default_factory=list, max_length=4096)
    claims: list[Claim] = Field(default_factory=list, max_length=8192)
    decisions: list[Decision] = Field(min_length=1, max_length=4096)
    programme_items: list[ProgrammeItem] = Field(default_factory=list, max_length=4096)
    uncertainties: list[str] = Field(default_factory=list, max_length=256)
    source_exclusions: list[dict[str, Any]] = Field(default_factory=list, max_length=256)


class AdjudicationResolution(_Closed):
    field: str = Field(min_length=1, max_length=256)
    selected_lane: Literal["A", "B"]
    evidence_claim_ids: list[str] = Field(min_length=1, max_length=256)
    reason: str = Field(min_length=1, max_length=2000)


class ConflictAdjudication(_Closed):
    schema_version: Literal[CONTRACT_VERSION]
    lane: Literal["C"]
    resolutions: list[AdjudicationResolution] = Field(default_factory=list, max_length=256)
    unresolved_fields: list[str] = Field(default_factory=list, max_length=256)


class LaneResult(_Closed):
    lane: Literal["A", "B", "C"]
    provider_status: str
    semantic_status: Literal["passed", "failed"]
    interaction_id: str | None
    environment_id: str | None
    candidate: dict[str, Any] | None
    candidate_sha256: str | None
    artifact_dir: str
    errors: list[str] = Field(default_factory=list)
    key_env: str | None = None


class ResearchResult(_Closed):
    run_id: int
    run_uid: str
    state: str
    review_status: str
    candidate: dict[str, Any]
    quality: dict[str, Any]
    lanes: list[LaneResult]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _candidate_path(root: Path) -> Path:
    matches = sorted(root.rglob("candidate.json"), key=lambda p: (len(p.parts), str(p)))
    if not matches:
        raise ValueError("candidate.json not found in environment snapshot")
    if len(matches) > 1:
        preferred = [p for p in matches if "festival_research_" in p.as_posix()]
        if len(preferred) == 1:
            return preferred[0]
        raise ValueError("multiple candidate.json artifacts found")
    return matches[0]


def _safe_snapshot_path(candidate_path: Path, snapshot_ref: str) -> Path:
    rel = PurePosixPath(snapshot_ref)
    if rel.is_absolute() or ".." in rel.parts or "\\" in snapshot_ref:
        raise ValueError(f"unsafe snapshot_ref: {snapshot_ref}")
    lane_root = candidate_path.parent
    result = lane_root.joinpath(*rel.parts).resolve()
    if lane_root.resolve() not in result.parents:
        raise ValueError(f"snapshot_ref escapes lane root: {snapshot_ref}")
    return result


def load_and_validate_candidate(extracted_root: Path, *, expected_lane: str) -> LaneCandidate:
    path = _candidate_path(extracted_root)
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("candidate.json is too large")
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert_no_agent_apply_authority(raw)
    candidate = LaneCandidate.model_validate(raw)
    if candidate.lane != expected_lane:
        raise ValueError(f"candidate lane mismatch: expected {expected_lane}, got {candidate.lane}")
    snapshots: dict[str, str] = {}
    for source in candidate.sources:
        source_path = _safe_snapshot_path(path, source.snapshot_ref)
        data = source_path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest != source.content_sha256:
            raise ValueError(f"source hash mismatch: {source.source_id}")
        snapshots[source.source_id] = data.decode("utf-8")
    validate_reference_graph(
        sources=candidate.sources,
        snapshot_text_by_source=snapshots,
        claims=candidate.claims,
        decisions=candidate.decisions,
        programme_items=candidate.programme_items,
        subjects=candidate.subjects,
    )
    return candidate


def load_and_validate_adjudication(extracted_root: Path, *, context: list[dict[str, Any]]) -> ConflictAdjudication:
    matches = sorted(extracted_root.rglob("adjudication.json"))
    if len(matches) != 1:
        raise ValueError("exactly one adjudication.json is required")
    raw = json.loads(matches[0].read_text(encoding="utf-8"))
    assert_no_agent_apply_authority(raw)
    result = ConflictAdjudication.model_validate(raw)
    candidates = context[0].get("lane_candidates", {}) if context else {}
    expected_fields = {item.get("field") for item in context[0].get("conflicts", [])} if context else set()
    decided_fields = {item.field for item in result.resolutions}
    unresolved = set(result.unresolved_fields)
    if decided_fields & unresolved or decided_fields | unresolved != expected_fields:
        raise ValueError("adjudication does not conserve the conflict inventory")
    for resolution in result.resolutions:
        selected = candidates.get(resolution.selected_lane) or {}
        claim_ids = {item.get("claim_id") for item in selected.get("claims", [])}
        if not set(resolution.evidence_claim_ids).issubset(claim_ids):
            raise ValueError(f"adjudication cites unknown claim for {resolution.field}")
    return result


def reconcile_candidates(candidates: list[LaneCandidate]) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    if not candidates:
        raise ValueError("no semantically valid lane candidate")
    a = candidates[0]
    conflicts: list[dict[str, Any]] = []
    for other in candidates[1:]:
        comparisons = {
            "festival.name": (a.festival.name.casefold(), other.festival.name.casefold()),
            "festival.start_date": (a.festival.start_date, other.festival.start_date),
            "festival.end_date": (a.festival.end_date, other.festival.end_date),
            "classification.primary_topology": (
                a.classification.primary_topology.value if a.classification.primary_topology else None,
                other.classification.primary_topology.value if other.classification.primary_topology else None,
            ),
            "classification.programme_structure": (
                a.classification.programme_structure.value,
                other.classification.programme_structure.value,
            ),
        }
        for field, values in comparisons.items():
            if values[0] != values[1]:
                conflicts.append({"field": field, "lanes": [a.lane, other.lane], "values": list(values)})
        a_events = sum(item.disposition.value in {"create_event_candidate", "link_existing_event"} for item in a.programme_items)
        b_events = sum(item.disposition.value in {"create_event_candidate", "link_existing_event"} for item in other.programme_items)
        if a_events != b_events:
            conflicts.append({"field": "materialized_event_count", "lanes": [a.lane, other.lane], "values": [a_events, b_events]})
    payload = {
        "schema_version": CONTRACT_VERSION,
        "selected_lane": a.lane,
        "festival": a.festival.model_dump(mode="json"),
        "classification": a.classification.model_dump(mode="json"),
        "programme_items": [item.model_dump(mode="json") for item in a.programme_items],
        "claims": [claim.model_dump(mode="json") for claim in a.claims],
        "decisions": [decision.model_dump(mode="json") for decision in a.decisions],
        "sources": [source.model_dump(mode="json") for source in a.sources],
        "lane_candidate_sha256": {item.lane: canonical_json_sha256(item.model_dump(mode="json")) for item in candidates},
    }
    quality = {
        "valid_lane_count": len(candidates),
        "independent_agreement": len(candidates) >= 2 and not conflicts,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "public_apply_allowed": False,
        "requires_operator_review": True,
    }
    return payload, quality, conflicts


class FestivalResearchCoordinator:
    def __init__(
        self,
        *,
        provider: AntigravityInteractionsClient,
        repository: FestivalResearchRepository,
        artifact_root: Path,
        taxonomy_sha256: str,
        max_total_tokens: int = 60_000,
        deadline_seconds: float = 900,
    ) -> None:
        self.provider = provider
        self.repository = repository
        self.artifact_root = Path(artifact_root)
        self.taxonomy_sha256 = taxonomy_sha256
        self.max_total_tokens = max(1, min(int(max_total_tokens), 100_000))
        self.deadline_seconds = max(30.0, float(deadline_seconds))

    async def _run_lane(self, run_id: int, target: ResearchTarget, lane: Literal["A", "B", "C"], conflicts: list[dict[str, Any]] | None = None) -> LaneResult:
        attempt = await self.repository.next_attempt(run_id, lane)
        row = await self.repository.create_lane(
            run_id=run_id,
            lane=lane,
            attempt_no=attempt,
            request_uid=str(uuid.uuid4()),
            provider_state="creating",
            semantic_state="pending",
            prompt_version=PROMPT_VERSION,
            contract_version=CONTRACT_VERSION,
            taxonomy_version=TAXONOMY_VERSION,
            taxonomy_sha256=self.taxonomy_sha256,
            input_fingerprint=canonical_json_sha256({"target": target.__dict__, "lane": lane, "attempt": attempt}),
            model_id=ANTIGRAVITY_AGENT,
        )
        lane_dir = self.artifact_root / f"run-{run_id}" / f"lane-{lane.lower()}-{attempt}"
        lane_dir.mkdir(parents=True, exist_ok=True)
        prompt = build_conflict_prompt(target, conflicts or []) if lane == "C" else build_lane_prompt(target, lane)
        (lane_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
        interaction: ProviderInteraction | None = None
        errors: list[str] = []
        candidate: LaneCandidate | None = None
        try:
            interaction = await self.provider.create(
                prompt,
                max_total_tokens=self.max_total_tokens,
                tools=[{"type": "code_execution"}] if lane == "C" else None,
            )
            # Persist the handle immediately after the successful create response.
            await self.repository.update_lane(
                row.id,
                request_uid=interaction.lease.request_uid,
                provider_state=interaction.provider_status,
                interaction_ids_json=[interaction.to_checkpoint()],
            )
            interaction = await self.provider.wait(interaction, deadline_seconds=self.deadline_seconds)
            await self.repository.update_lane(
                row.id,
                provider_state=interaction.provider_status,
                interaction_ids_json=[interaction.to_checkpoint()],
                usage_json={
                    "input_tokens": interaction.usage.input_tokens,
                    "output_tokens": interaction.usage.output_tokens,
                    "total_tokens": interaction.usage.total_tokens,
                },
            )
            if interaction.environment_id:
                tar_path = lane_dir / "environment.tar"
                extract_path = lane_dir / "environment"
                if extract_path.exists():
                    shutil.rmtree(extract_path)
                await self.provider.download_environment(interaction, tar_path, extract_to=extract_path)
                candidate = (
                    load_and_validate_adjudication(extract_path, context=conflicts or [])
                    if lane == "C"
                    else load_and_validate_candidate(extract_path, expected_lane=lane)
                )
            else:
                raise ValueError("provider returned no environment_id")
            if interaction.provider_status == "completed":
                await self.provider.rate_limiter.record_external_call_semantic_result(
                    interaction.lease, semantic_status="passed"
                )
            else:
                errors.append(f"candidate recovered but provider terminal status is {interaction.provider_status}")
            semantic_state = "passed" if interaction.provider_status == "completed" else "recovered_incomplete"
            candidate_json = candidate.model_dump(mode="json")
            candidate_hash = canonical_json_sha256(candidate_json)
            manifest = build_artifact_manifest(lane_dir).model_dump(mode="json")
            await self.repository.update_lane(
                row.id,
                semantic_state=semantic_state,
                candidate_json=candidate_json,
                candidate_sha256=candidate_hash,
                artifact_manifest_json=manifest,
                validation_json={"valid": True, "errors": errors},
                completed_at=_utc_now(),
            )
            return LaneResult(
                lane=lane,
                provider_status=interaction.provider_status,
                semantic_status="passed",
                interaction_id=interaction.id,
                environment_id=interaction.environment_id,
                candidate=candidate_json,
                candidate_sha256=candidate_hash,
                artifact_dir=str(lane_dir),
                errors=errors,
                key_env=interaction.lease.env_var_name,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {str(exc)[:1000]}"
            errors.append(error)
            if interaction is not None and interaction.is_terminal:
                try:
                    await self.provider.rate_limiter.record_external_call_semantic_result(
                        interaction.lease, semantic_status="failed", semantic_error=error
                    )
                except Exception as accounting_exc:
                    errors.append(f"semantic accounting: {type(accounting_exc).__name__}: {str(accounting_exc)[:400]}")
            await self.repository.update_lane(
                row.id,
                provider_state=interaction.provider_status if interaction else "failed_before_handle",
                semantic_state="failed",
                semantic_error_code=type(exc).__name__,
                last_error=error,
                validation_json={"valid": False, "errors": errors},
                completed_at=_utc_now(),
            )
            return LaneResult(
                lane=lane,
                provider_status=interaction.provider_status if interaction else "failed_before_handle",
                semantic_status="failed",
                interaction_id=interaction.id if interaction else None,
                environment_id=interaction.environment_id if interaction else None,
                candidate=None,
                candidate_sha256=None,
                artifact_dir=str(lane_dir),
                errors=errors,
                key_env=interaction.lease.env_var_name if interaction else None,
            )

    async def collect(self, *, run_id: int, run_uid: str, target: ResearchTarget, allow_c: bool = True) -> ResearchResult:
        await self.repository.update_run(run_id, state="running", started_at=_utc_now())
        lanes = [
            await self._run_lane(run_id, target, "A"),
            await self._run_lane(run_id, target, "B"),
        ]
        valid = [LaneCandidate.model_validate(item.candidate) for item in lanes if item.semantic_status == "passed" and item.candidate]
        if not valid:
            quality = {
                "valid_lane_count": 0,
                "independent_agreement": False,
                "conflict_count": 0,
                "conflicts": [],
                "public_apply_allowed": False,
                "requires_operator_review": False,
                "lane_errors": {item.lane: item.errors for item in lanes},
            }
            candidate = {
                "schema_version": CONTRACT_VERSION,
                "festival": {"name": target.name_hint},
                "classification": {"primary_topology": None, "programme_structure": "unknown"},
                "programme_items": [],
            }
            await self.repository.update_run(
                run_id,
                state="failed",
                review_status="pending",
                candidate_json={},
                quality_json=quality,
                artifact_manifest_json={"lanes": [item.model_dump(mode="json", exclude={"candidate"}) for item in lanes]},
                completed_at=_utc_now(),
            )
            return ResearchResult(
                run_id=run_id, run_uid=run_uid, state="failed", review_status="pending",
                candidate=candidate, quality=quality, lanes=lanes,
            )
        candidate, quality, conflicts = reconcile_candidates(valid)
        if allow_c and len(valid) >= 2 and conflicts:
            c_context = [{
                "conflicts": conflicts,
                "lane_candidates": {item.lane: item.model_dump(mode="json") for item in valid},
            }]
            c_result = await self._run_lane(run_id, target, "C", c_context)
            lanes.append(c_result)
            if c_result.semantic_status == "passed" and c_result.candidate:
                adjudication = ConflictAdjudication.model_validate(c_result.candidate)
                quality.update({
                    "c_was_used": True,
                    "c_resolutions": adjudication.model_dump(mode="json"),
                    "requires_operator_review": True,
                })
        candidate_hash = canonical_json_sha256(candidate)
        state = "review" if quality.get("valid_lane_count", 0) >= 1 else "failed"
        manifest = {"lanes": [lane.model_dump(mode="json", exclude={"candidate"}) for lane in lanes]}
        await self.repository.update_run(
            run_id,
            state=state,
            review_status="pending",
            candidate_json=candidate,
            candidate_sha256=candidate_hash,
            quality_json=quality,
            artifact_manifest_json=manifest,
            completed_at=_utc_now(),
        )
        return ResearchResult(
            run_id=run_id,
            run_uid=run_uid,
            state=state,
            review_status="pending",
            candidate=candidate,
            quality=quality,
            lanes=lanes,
        )
