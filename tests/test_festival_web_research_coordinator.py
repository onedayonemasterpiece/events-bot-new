from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from festival_web_research.coordinator import LaneCandidate, load_and_validate_candidate, reconcile_candidates


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
            "name": "Балтийская Ухана", "edition_label": "2026", "description_facts": [],
            "start_date": "2026-08-07", "end_date": "2026-08-09",
            "official_url": "https://uhana.ru/", "venue_names": [], "organizer_names": [],
        },
        "classification": {
            "primary_topology": topology, "secondary_topologies": [],
            "programme_structure": "continuous_experience", "claim_ids": ["C1"],
            "decision_ids": ["D1"],
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
        }],
        "programme_items": [], "uncertainties": [], "source_exclusions": [],
    }
    (lane_root / "candidate.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
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
