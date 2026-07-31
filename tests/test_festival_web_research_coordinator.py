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


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda data: data["decisions"][0].update(actor_kind="operator"), "non-supported/lane/evidence-backed"),
        (lambda data: data["decisions"][0].update(status="unknown"), "non-supported/lane/evidence-backed"),
        (lambda data: data["festival"].update(name="Недостоверное имя"), "does not equal cited normalized value"),
    ],
)
def test_candidate_rejects_spoofed_semantic_authority_or_fact(
    tmp_path: Path,
    mutation,
    expected: str,
) -> None:
    data = _candidate(tmp_path)
    mutation(data)
    next(tmp_path.rglob("candidate.json")).write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match=expected):
        load_and_validate_candidate(tmp_path, expected_lane="A")


def test_reconcile_detects_independent_taxonomy_conflict(tmp_path: Path) -> None:
    a_root = tmp_path / "a"
    b_root = tmp_path / "b"
    a = LaneCandidate.model_validate(_candidate(a_root, "A", "territory"))
    b = LaneCandidate.model_validate(_candidate(b_root, "B", "lineup"))
    _, quality, conflicts = reconcile_candidates([a, b])
    assert not quality["independent_agreement"]
    assert any(item["field"] == "classification.primary_topology" for item in conflicts)


def test_reconcile_detects_same_count_but_different_programme_subjects(tmp_path: Path) -> None:
    def with_item(data: dict, title: str) -> LaneCandidate:
        digest = data["sources"][0]["content_sha256"]
        data["claims"].append({
            "claim_id": "CI", "source_id": "S1", "local_subject_id": "item",
            "subject_kind": "programme_item", "field": "title", "raw_value": title,
            "normalized_value": title, "normalization": "none",
            "evidence": {"quote": title, "quote_start": 0, "quote_end": len(title)},
            "content_sha256": digest, "normalizer_version": "festival-text-normalizer-v1",
            "status": "accepted",
        })
        data["decisions"].append({
            "decision_id": "DI", "decision_kind": "programme_item_disposition",
            "subject_ref": "programme_item:item", "selected_value": "schedule_slot",
            "alternatives_rejected": [], "evidence_claim_ids": ["CI"],
            "reason_codes": [], "status": "supported", "actor_kind": "lane_model",
        })
        data["programme_items"] = [{
            "item_id": "item", "entity_role": "temporal_anchor", "disposition": "schedule_slot",
            "identity_claim_ids": ["CI"], "logistics_claim_ids": [], "decision_ids": ["DI"],
            "event_gate": {key: "unknown" for key in (
                "current_edition", "independent_choice", "event_grade_occurrence",
                "meaningful_identity", "access_compatibility", "topology_guardrail",
                "evidence_validation",
            )},
        }]
        return LaneCandidate.model_validate(data)

    a = with_item(_candidate(tmp_path / "a", "A"), "Детский концерт")
    b = with_item(_candidate(tmp_path / "b", "B"), "Гала-концерт")
    _, _, conflicts = reconcile_candidates([a, b])
    assert any(item["field"] == "programme_inventory" for item in conflicts)


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


class _ResumeRateLimiter:
    def __init__(self) -> None:
        self.semantic_results: list[str] = []

    async def record_external_call_semantic_result(self, lease, *, semantic_status):
        self.semantic_results.append(semantic_status)


class _ResumeOnlyProvider:
    def __init__(self) -> None:
        self.rate_limiter = _ResumeRateLimiter()
        self.waited_ids: list[str] = []

    async def wait(self, interaction, *, deadline_seconds):
        self.waited_ids.append(interaction.id)
        return ProviderInteraction(
            id=interaction.id,
            provider_status="completed",
            environment_id=interaction.environment_id,
            steps=(),
            usage=UsageInfo(),
            raw={},
            lease=interaction.lease,
        )

    async def download_environment(self, interaction, destination, *, extract_to):
        Path(extract_to).mkdir(parents=True, exist_ok=True)


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


@pytest.mark.asyncio
async def test_resume_lane_uses_persisted_handle_without_create(tmp_path: Path, monkeypatch) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        run = await repository.create_run(
            run_uid=str(uuid.uuid4()), target_key="test", series_candidate="Test",
            edition_candidate="2026", input_fingerprint="f" * 64,
            orchestration_version="test", contract_version="festival-web-research-v2",
            taxonomy_version="festival-taxonomy-registry-v2", taxonomy_sha256="a" * 64,
        )
        lease = ExternalCallLease(
            request_uid=str(uuid.uuid4()), attempt_no=1, consumer="test", account_name=None,
            model="antigravity-preview-05-2026", reserved_tpm=1000,
            api_key_id="key", env_var_name="GOOGLE_API_KEY", key_alias=None,
            minute_bucket=None, day_bucket=None, started_at=datetime.now(timezone.utc),
        )
        checkpoint = ProviderInteraction(
            id="interaction-resume", provider_status="in_progress",
            environment_id="environment-resume", steps=(), usage=UsageInfo(), raw={}, lease=lease,
        ).to_checkpoint()
        lane = await repository.create_lane(
            run_id=run.id, lane="A", attempt_no=1, request_uid=lease.request_uid,
            provider_state="in_progress", semantic_state="pending",
            interaction_ids_json=[checkpoint], prompt_version="test",
            contract_version="festival-web-research-v2",
            taxonomy_version="festival-taxonomy-registry-v2", taxonomy_sha256="a" * 64,
            input_fingerprint="e" * 64,
        )
        candidate = LaneCandidate.model_validate(_candidate(tmp_path / "fixture", "A"))
        monkeypatch.setattr(
            "festival_web_research.coordinator.load_and_validate_candidate",
            lambda *args, **kwargs: candidate,
        )
        provider = _ResumeOnlyProvider()
        coordinator = FestivalResearchCoordinator(
            provider=provider, repository=repository,
            artifact_root=tmp_path / "artifacts", taxonomy_sha256="a" * 64,
            deadline_seconds=30,
        )
        result = await coordinator.resume_lane(lane.id)
        assert result.semantic_status == "passed"
        assert provider.waited_ids == ["interaction-resume"]
        assert provider.rate_limiter.semantic_results == ["passed"]
        persisted = await repository.get_lane(lane.id)
        assert persisted.provider_state == "completed"
        assert persisted.semantic_state == "passed"
    finally:
        await db.close()
