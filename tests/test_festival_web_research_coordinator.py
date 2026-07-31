from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import uuid

import pytest
from sqlalchemy import select

from db import Database
from festival_web_research.coordinator import (
    FestivalResearchCoordinator,
    LaneCandidate,
    load_and_validate_adjudication,
    load_and_validate_candidate,
    reconcile_candidates,
)
from festival_web_research.repository import FestivalResearchRepository
from festival_web_research.service import FestivalWebResearchService
from google_ai.client import ExternalCallLease, UsageInfo
from google_ai.exceptions import ProviderError
from google_ai.interactions import ProviderInteraction
from models import FestivalWebResearchLaneRun


def _candidate(root: Path, lane: str = "A", topology: str = "territory") -> dict:
    lane_root = root / f"workspace/festival_research_{lane.lower()}"
    source_dir = lane_root / "sources"
    source_dir.mkdir(parents=True)
    text = "Балтийская Ухана\n7–9 августа 2026"
    source_path = source_dir / "S1.txt"
    source_path.write_text(text, encoding="utf-8")
    digest = hashlib.sha256(text.encode()).hexdigest()
    data = {
        "schema_version": "festival-web-research-v2",
        "lane": lane,
        "festival": {
            "name": "Балтийская Ухана", "edition_label": None, "description_facts": [],
            "start_date": None, "end_date": None,
            "official_url": None, "venue_names": [], "organizer_names": [],
            "claim_ids_by_field": {"name": ["C1"]},
        },
        "classification": {
            "primary_topology": topology, "secondary_topologies": [],
            "programme_structure": "continuous_experience", "claim_ids": ["C1"],
            "decision_ids": ["D1", "D2"],
        },
        "sources": [{
            "source_id": "S1", "requested_url": "https://uhana.ru/", "resolved_url": "https://uhana.ru/",
            "canonical_url": "https://uhana.ru/", "source_role": "official_home", "edition_status": "accepted",
            "content_sha256": digest, "normalizer_version": "festival-text-normalizer-v1",
            "snapshot_ref": "sources/S1.txt", "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "content_type": "text/plain",
        }],
        "subjects": [{"source_id": "S1", "local_subject_id": "festival", "subject_kind": "festival"}],
        "claims": [{
            "claim_id": "C1", "source_id": "S1", "local_subject_id": "festival", "subject_kind": "festival",
            "field": "title", "raw_value": "Балтийская Ухана", "normalized_value": "Балтийская Ухана",
            "normalization": "none", "evidence": {"quote": "Балтийская Ухана", "quote_start": 0, "quote_end": len("Балтийская Ухана")},
            "content_sha256": digest, "normalizer_version": "festival-text-normalizer-v1", "status": "accepted",
        }],
        "decisions": [{
            "decision_id": "D1", "decision_kind": "discovery_topology", "subject_ref": "festival",
            "selected_value": topology, "alternatives_rejected": [], "evidence_claim_ids": ["C1"],
            "reason_codes": [], "status": "supported", "actor_kind": "lane_model",
        }, {
            "decision_id": "D2", "decision_kind": "programme_structure", "subject_ref": "festival",
            "selected_value": "continuous_experience", "alternatives_rejected": [], "evidence_claim_ids": ["C1"],
            "reason_codes": [], "status": "supported", "actor_kind": "lane_model",
        }],
        "programme_items": [], "uncertainties": [], "source_exclusions": [],
    }
    checkpoint_payloads = [
        ("state", "state.json", {}),
        ("source_ledger", "source_ledger.json", []),
        ("source_review", "source_review.json", {}),
        ("claims", "claims_checkpoint.json", []),
        ("subjects", "subjects_checkpoint.json", []),
        ("topology", "topology.json", {}),
        ("programme_inventory", "programme_inventory.json", []),
        ("candidate", "candidate.json", data),
        ("run_summary", "run_summary.json", {}),
    ]
    manifest = []
    parent = None
    for sequence, (kind, filename, payload) in enumerate(checkpoint_payloads):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        (lane_root / filename).write_bytes(raw)
        digest_checkpoint = hashlib.sha256(raw).hexdigest()
        manifest.append({
            "checkpoint_id": f"cp-{sequence}", "kind": kind, "sequence": sequence,
            "relative_path": filename, "content_sha256": digest_checkpoint,
            "byte_count": len(raw), "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "parent_sha256": parent,
        })
        parent = digest_checkpoint
    (lane_root / "checkpoint_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return data


def test_candidate_validates_exact_snapshot_evidence(tmp_path: Path) -> None:
    _candidate(tmp_path)
    result = load_and_validate_candidate(tmp_path, expected_lane="A")
    assert result.festival.name == "Балтийская Ухана"
    assert result.classification.primary_topology.value == "territory"


def test_candidate_rejects_agent_apply_authority(tmp_path: Path) -> None:
    data = _candidate(tmp_path)
    data["festival"]["smart_update"] = "pass"
    next(tmp_path.rglob("candidate.json")).write_text(json.dumps(data), encoding="utf-8")
    try:
        load_and_validate_candidate(tmp_path, expected_lane="A")
    except ValueError as exc:
        assert "agent_supplied_host_authority" in str(exc)
    else:
        raise AssertionError("agent authority must fail closed")


def test_reconcile_detects_independent_taxonomy_conflict(tmp_path: Path) -> None:
    a_root = tmp_path / "a"
    b_root = tmp_path / "b"
    a = LaneCandidate.model_validate(_candidate(a_root, "A", "territory"))
    b = LaneCandidate.model_validate(_candidate(b_root, "B", "lineup"))
    _, quality, conflicts = reconcile_candidates([a, b])
    assert not quality["independent_agreement"]
    assert any(item["field"] == "classification.primary_topology" for item in conflicts)


def test_conflict_adjudication_conserves_fields_and_cites_selected_lane(tmp_path: Path) -> None:
    root = tmp_path / "workspace/festival_research_c"
    root.mkdir(parents=True)
    (root / "adjudication.json").write_text(json.dumps({
        "schema_version": "festival-web-research-v2",
        "lane": "C",
        "resolutions": [{
            "field": "classification.primary_topology",
            "selected_lane": "A",
            "evidence_claim_ids": ["C1"],
            "reason": "A cites the official current-edition title",
        }],
        "unresolved_fields": ["festival.end_date"],
    }), encoding="utf-8")
    context = [{
        "conflicts": [
            {"field": "classification.primary_topology"},
            {"field": "festival.end_date"},
        ],
        "lane_candidates": {
            "A": {"claims": [{"claim_id": "C1"}]},
            "B": {"claims": [{"claim_id": "C2"}]},
        },
    }]
    result = load_and_validate_adjudication(tmp_path, context=context)
    assert result.resolutions[0].selected_lane == "A"
    assert result.unresolved_fields == ["festival.end_date"]


class _PollDeniedProvider:
    rate_limiter = object()

    async def create(self, prompt, *, max_total_tokens, tools=None):
        lease = ExternalCallLease(
            request_uid=str(uuid.uuid4()), attempt_no=1, consumer="test", account_name=None,
            model="antigravity-preview-05-2026", reserved_tpm=max_total_tokens,
            api_key_id="key", env_var_name="GOOGLE_API_KEY", key_alias=None,
            minute_bucket=None, day_bucket=None, started_at=datetime.now(timezone.utc),
        )
        return ProviderInteraction(
            id=f"interaction-{uuid.uuid4().hex}", provider_status="in_progress",
            environment_id="environment-1", steps=(), usage=UsageInfo(), raw={}, lease=lease,
        )

    async def wait(self, interaction, *, deadline_seconds):
        raise ProviderError(
            error_type="http_error", error_code="permission_denied",
            error_message="The caller does not have permission", status_code=403,
        )


@pytest.mark.asyncio
async def test_nonretryable_poll_error_is_persisted_as_provider_failed(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        coordinator = FestivalResearchCoordinator(
            provider=_PollDeniedProvider(), repository=repository,
            artifact_root=tmp_path / "artifacts", taxonomy_sha256="a" * 64,
            deadline_seconds=30,
        )
        service = FestivalWebResearchService(repository=repository, coordinator=coordinator)
        result = await service.collect(
            name_hint="Test", edition_hint="2026", urls=["https://example.org/"], allow_c=False,
        )
        assert result.state == "failed"
        async with db.get_session() as session:
            lanes = list((await session.execute(select(FestivalWebResearchLaneRun))).scalars())
        assert {lane.provider_state for lane in lanes} == {"failed"}
        assert {lane.semantic_state for lane in lanes} == {"failed"}
        assert {lane.provider_error_code for lane in lanes} == {"403"}
    finally:
        await db.close()
