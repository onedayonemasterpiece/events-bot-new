from __future__ import annotations

from types import SimpleNamespace

import pytest

import event_identity
import smart_event_update as su
from smart_event_update import EventCandidate
from smart_update_identity import (
    IdentityFinalAction,
    IdentityVectorCandidate,
    IdentityVectorEvidence,
    build_identity_gate_verdict,
)


def _candidate() -> EventCandidate:
    return EventCandidate(
        source_type="telegram",
        source_url="https://t.me/vector_replay/1",
        source_text="22 августа в 21:00 Праздничный SOS в Барне",
        title="Праздничный SOS",
        date="2026-08-22",
        time="21:00",
        location_name="Барн",
        city="Калининград",
        event_type="вечеринка",
        ticket_link="https://barn.timepad.ru/event/4147114",
    )


def test_rank_two_vector_owner_participates_in_identity_gate():
    candidate = _candidate()
    candidate.ticket_link = None
    rank_one = SimpleNamespace(
        id=9001,
        title="Другая вечеринка",
        date="2026-08-22",
        end_date=None,
        time="20:00",
        time_is_default=False,
        location_name="Другой клуб",
        location_address=None,
        city="Калининград",
        event_type="вечеринка",
        ticket_link=None,
        source_post_url=None,
        source_vk_post_url=None,
    )
    owner = SimpleNamespace(
        id=8117,
        title="Тройной день рождения: Барн, Chipi Clo и SOS",
        date="2026-08-22",
        end_date=None,
        time="21:00",
        time_is_default=False,
        location_name="Барн",
        location_address=None,
        city="Калининград",
        event_type="вечеринка",
        ticket_link=None,
        source_post_url=None,
        source_vk_post_url=None,
    )
    evidence = IdentityVectorEvidence(
        available=True,
        nearest_event_id=9001,
        score=0.98,
        candidates=(
            IdentityVectorCandidate(event_id=9001, score=0.98, rank=1),
            IdentityVectorCandidate(event_id=8117, score=0.97, rank=2),
        ),
    )

    verdict = build_identity_gate_verdict(
        candidate,
        [rank_one, owner],
        mode=su.IdentityGateMode.ENFORCE,
        vector_evidence=evidence,
    )

    assert verdict.should_veto_create
    assert verdict.matched_event_id == 8117


@pytest.mark.asyncio
async def test_vector_filter_drift_fallback_reuses_one_embedding(monkeypatch):
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY", "test-secret")
    monkeypatch.setenv(su.SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV, "test-google")
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_VECTOR_RECALL", True)
    calls = {"embedding": 0, "recall": []}

    async def _embed(_doc):
        calls["embedding"] += 1
        return event_identity.EventIdentityEmbeddingResult(ok=True, embedding=(0.1, 0.2))

    def _recall(_client, _embedding, *, city, event_type, **_kwargs):
        calls["recall"].append((city, event_type))
        if city is not None or event_type is not None:
            return event_identity.EventIdentityRecallResult(ok=True, candidates=())
        return event_identity.EventIdentityRecallResult(
            ok=True,
            candidates=(
                event_identity.EventIdentityCandidateEvidence(
                    event_id=8055,
                    similarity=0.96,
                    embedding_doc_kind="search_v3",
                    title="Балтийская Одиссея",
                ),
                event_identity.EventIdentityCandidateEvidence(
                    event_id=8108,
                    similarity=0.95,
                    embedding_doc_kind="related_v1",
                    title="Балтийская Одиссея: напоминание",
                ),
                event_identity.EventIdentityCandidateEvidence(
                    event_id=8117,
                    similarity=0.94,
                    embedding_doc_kind="search_v3",
                    title="SOS",
                ),
            ),
        )

    monkeypatch.setattr(su, "_smart_update_embed_identity_document_with_limiter", _embed)
    monkeypatch.setattr(event_identity, "recall_identity_candidates_across_doc_kinds", _recall)

    evidence = await su._smart_update_identity_vector_evidence(_candidate())

    assert evidence is not None
    assert evidence.nearest_event_id == 8055
    assert [item.event_id for item in evidence.candidates] == [8055, 8108, 8117]
    assert [item.rank for item in evidence.candidates] == [1, 2, 3]
    assert evidence.filter_fallback_used is True
    assert calls["embedding"] == 1
    assert calls["recall"] == [
        ("Калининград", "вечеринка"),
        (None, None),
    ]


def test_vector_handoff_does_not_define_a_second_final_action():
    assert {item.value for item in IdentityFinalAction} == {
        "FINAL_MATCH",
        "FINAL_DISTINCT",
        "FINAL_RETRY",
    }
