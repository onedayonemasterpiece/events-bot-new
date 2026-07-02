from __future__ import annotations

from dataclasses import dataclass

from smart_update_identity import IdentityGateMode, IdentityVectorEvidence, build_identity_gate_verdict


EXHIBITION_DUPLICATE_CLUSTERS = {
    "Билетёры 2.0": (5765, 5766),
    "С чего начинается Родина": (4512, 2781, 6301),
    "Точка и линия": (5370, 6214, 5969, 5971),
    "Альбрехт Дюрер": (5703, 6382),
    "Розовый натюрморт": (6080, 5391, 6296),
}

NON_EXHIBITION_DUPLICATE_LIKE_CLUSTERS = {
    "ЭПИДЕМИЯ. ОГНЕННАЯ РУКОПИСЬ": (4671, 6420),
    "Любовь в стиле джаз": (4779, 4780),
    "Орфей и Эвридика": (5511, 5512),
    "Кот Леопольд": (5995, 5996),
}

NEGATIVE_RECURRING_CONTROLS = {
    "Стендап: Гассан Джабер": (6405, 6406),
}


@dataclass
class _Subject:
    title: str
    date: str
    end_date: str | None = None
    time: str | None = None
    location_name: str | None = None
    event_type: str | None = None
    id: int | None = None


def test_replay_contract_lists_incident_clusters_for_future_import_replays():
    assert EXHIBITION_DUPLICATE_CLUSTERS["Билетёры 2.0"] == (5765, 5766)
    assert 6080 in EXHIBITION_DUPLICATE_CLUSTERS["Розовый натюрморт"]
    assert NON_EXHIBITION_DUPLICATE_LIKE_CLUSTERS["Кот Леопольд"] == (5995, 5996)
    assert NEGATIVE_RECURRING_CONTROLS["Стендап: Гассан Джабер"] == (6405, 6406)


def test_exhibition_replay_contract_vector_candidate_blocks_new_public_duplicate():
    candidate = _Subject(
        title="Розовый натюрморт",
        date="2026-07-02",
        end_date="2026-08-02",
        location_name="Музей",
        event_type="выставка",
    )
    existing = _Subject(
        id=6080,
        title="Розовый натюрморт",
        date="2026-07-01",
        end_date="2026-08-10",
        location_name="Музей",
        event_type="выставка",
    )

    verdict = build_identity_gate_verdict(
        candidate,
        [existing],
        mode=IdentityGateMode.ENFORCE,
        vector_evidence=IdentityVectorEvidence(
            available=True,
            nearest_event_id=6080,
            score=0.956,
            reason="related_v1 replay cluster",
        ),
    )

    assert verdict.should_veto_create
    assert verdict.reason_code == "vector_nearest_identity"
    assert verdict.matched_event_id == 6080


def test_negative_recurring_replay_contract_different_explicit_date_is_distinct():
    candidate = _Subject(
        title="Стендап: Гассан Джабер",
        date="2026-07-20",
        time="20:00",
        location_name="Клуб",
        event_type="стендап",
    )
    existing = _Subject(
        id=6405,
        title="Стендап: Гассан Джабер",
        date="2026-07-10",
        time="20:00",
        location_name="Клуб",
        event_type="стендап",
    )

    verdict = build_identity_gate_verdict(
        candidate,
        [existing],
        mode=IdentityGateMode.ENFORCE,
        vector_evidence=IdentityVectorEvidence(
            available=True,
            nearest_event_id=6405,
            score=0.986,
            reason="known high-similarity recurring control",
        ),
    )

    assert not verdict.should_veto_create
    assert verdict.reason_code == "no_identity_veto"
