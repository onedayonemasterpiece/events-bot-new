from __future__ import annotations

from dataclasses import dataclass, field

import smart_event_update as su
from models import Event
from smart_event_update import EventCandidate
from smart_update_identity import (
    IdentityGateAction,
    IdentityGateMode,
    IdentityVectorEvidence,
    build_identity_gate_verdict,
    identity_gate_fail_safe_verdict,
    parse_identity_gate_mode,
    source_flags_for,
)


@dataclass
class _Poster:
    sha256: str | None = None


@dataclass
class _Cand:
    title: str
    date: str
    time: str | None = None
    source_type: str = "vk"
    source_url: str | None = None
    location_name: str | None = None
    ticket_link: str | None = None
    posters: list[_Poster] = field(default_factory=list)


@dataclass
class _Event:
    id: int
    title: str
    date: str
    time: str | None = None
    end_date: str | None = None
    location_name: str | None = None
    event_type: str | None = None
    ticket_link: str | None = None
    source_post_url: str | None = None
    source_vk_post_url: str | None = None
    posters: list[_Poster] = field(default_factory=list)


def test_parse_identity_gate_mode_defaults_to_off_for_unknown_values():
    assert parse_identity_gate_mode(None) is IdentityGateMode.OFF
    assert parse_identity_gate_mode("shadow") is IdentityGateMode.SHADOW
    assert parse_identity_gate_mode("enforce") is IdentityGateMode.ENFORCE
    assert parse_identity_gate_mode("surprise") is IdentityGateMode.OFF


def test_source_flags_expose_parser_and_transport_kinds():
    assert source_flags_for("parser:qtickets").is_parser
    assert source_flags_for("telegram").is_telegram
    assert source_flags_for("vk").is_vk
    assert source_flags_for("bot").is_bot


def test_off_mode_never_vetoes_even_with_strong_identity_signal():
    cand = _Cand(
        title="Концерт Валерии",
        date="2026-07-01",
        time="19:00",
        source_url="https://t.me/source/10",
        location_name="Янтарь холл",
    )
    ev = _Event(
        id=7,
        title="Концерт Валерии",
        date="2026-07-01",
        time="19:00",
        source_post_url="https://t.me/source/10",
        location_name="Янтарь холл",
    )

    verdict = build_identity_gate_verdict(cand, [ev], mode=IdentityGateMode.OFF)

    assert verdict.action is IdentityGateAction.ALLOW_CREATE
    assert not verdict.should_veto_create
    assert verdict.reason_code == "identity_gate_off"


def test_shadow_mode_reports_would_veto_but_does_not_enforce():
    cand = _Cand(
        title="Концерт Валерии",
        date="2026-07-01",
        time="19:00",
        source_url="https://t.me/source/10",
        location_name="Янтарь холл",
    )
    ev = _Event(
        id=7,
        title="Концерт Валерии",
        date="2026-07-01",
        time="19:00",
        source_post_url="https://t.me/source/10",
        location_name="Янтарь холл",
    )

    verdict = build_identity_gate_verdict(cand, [ev], mode="shadow")

    assert verdict.action is IdentityGateAction.VETO_CREATE
    assert verdict.would_veto_create
    assert not verdict.should_veto_create
    assert verdict.matched_event_id == 7


def test_same_source_same_slot_and_venue_does_not_veto_distinct_sibling_title():
    cand = _Cand(
        title="Кинопоказ «Ангелы Ладоги»",
        date="2026-08-28",
        time="19:00",
        source_url="https://vk.com/wall-53460968_11826",
        location_name="Гусевский музей",
    )
    ev = _Event(
        id=7709,
        title="Кинопоказ «Чебурашка 2»",
        date="2026-08-28",
        time="19:00",
        source_vk_post_url="https://vk.com/wall-53460968_11826",
        location_name="Гусевский музей",
    )

    verdict = build_identity_gate_verdict(cand, [ev], mode=IdentityGateMode.ENFORCE)

    assert not verdict.should_veto_create
    assert verdict.reason_code == "no_identity_veto"


def test_enforce_vetoes_same_ticket_same_slot_without_auto_merge():
    cand = _Cand(
        title="Pianissimo Илья Папоян",
        date="2026-06-26",
        time="20:00",
        location_name="Филиал Третьяковской галереи",
        ticket_link="https://tickets.example/events/123/?utm=feed",
    )
    ev = _Event(
        id=42,
        title="Фестиваль Pianissimo: Илья Папоян",
        date="2026-06-26",
        time="20:00",
        location_name="Филиал Третьяковской галереи",
        ticket_link="https://tickets.example/events/123?utm=feed",
    )

    verdict = build_identity_gate_verdict(cand, [ev], mode=IdentityGateMode.ENFORCE)

    assert verdict.should_veto_create
    assert verdict.reason_code == "deterministic_same_ticket_slot"
    assert verdict.matched_event_id == 42


def test_fail_safe_verdict_only_blocks_when_mode_is_enforce():
    shadow = identity_gate_fail_safe_verdict(mode=IdentityGateMode.SHADOW, reason="boom")
    enforce = identity_gate_fail_safe_verdict(mode=IdentityGateMode.ENFORCE, reason="boom")

    assert shadow.fail_safe
    assert shadow.action is IdentityGateAction.VETO_CREATE
    assert not shadow.should_veto_create
    assert enforce.should_veto_create


def test_enforce_vector_high_confidence_vetoes_create_without_existing_shortlist_match():
    cand = _Cand(
        title="Розовый натюрморт",
        date="2026-07-02",
        location_name="Музей",
        source_type="telegram",
    )

    verdict = build_identity_gate_verdict(
        cand,
        [_Event(id=6080, title="Розовый натюрморт", date="2026-07-01", end_date="2026-08-01", location_name="Музей", event_type="выставка")],
        mode=IdentityGateMode.ENFORCE,
        vector_evidence=IdentityVectorEvidence(
            available=True,
            nearest_event_id=6080,
            score=0.956,
            reason="related_v1/search_v3 union top candidate",
        ),
    )

    assert verdict.should_veto_create
    assert verdict.reason_code == "vector_nearest_identity"
    assert verdict.matched_event_id == 6080
    assert verdict.vector and verdict.vector.score == 0.956


def test_shadow_vector_error_is_logged_as_would_veto_only():
    cand = _Cand(title="Выставка", date="2026-07-02", source_type="parser:example")

    verdict = build_identity_gate_verdict(
        cand,
        [],
        mode=IdentityGateMode.SHADOW,
        vector_evidence=IdentityVectorEvidence(available=False, error="rpc_failed:Timeout"),
    )

    assert verdict.would_veto_create
    assert not verdict.should_veto_create
    assert verdict.fail_safe
    assert verdict.reason_code == "vector_identity_error"


def test_recurring_different_date_is_not_blocked_by_vector_similarity_alone():
    cand = _Cand(
        title="Стендап: Гассан Джабер",
        date="2026-07-20",
        time="20:00",
        location_name="Клуб",
        source_type="telegram",
    )
    ev = _Event(
        id=6405,
        title="Стендап: Гассан Джабер",
        date="2026-07-10",
        time="20:00",
        location_name="Клуб",
        event_type="стендап",
    )

    verdict = build_identity_gate_verdict(
        cand,
        [ev],
        mode=IdentityGateMode.ENFORCE,
        vector_evidence=IdentityVectorEvidence(available=True, nearest_event_id=6405, score=0.986),
    )

    assert not verdict.should_veto_create
    assert verdict.reason_code == "no_identity_veto"


def test_canonical_parser_identity_veto_runs_typed_adjudicator(monkeypatch):
    monkeypatch.setattr(su, "SMART_UPDATE_DEDUP_ADJUDICATOR", True)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    candidate = EventCandidate(
        source_type="parser:qtickets",
        source_url="https://kaliningrad.qtickets.events/251792-amalienau-dusha",
        source_text="Экскурсия 17 августа в 13:30",
        title="Экскурсия «Амалиенау — душа Калининграда»",
        date="2026-08-17",
        time="13:30",
        location_name="Главный вход в Центральный парк",
    )
    owner = Event(
        id=7602,
        title=candidate.title,
        date=candidate.date,
        time=candidate.time,
        location_name=candidate.location_name,
    )

    assert su._should_run_widened_dedup_adjudicator(
        candidate=candidate,
        match_event=None,
        identity_gate_match=owner,
        anchor_forced=False,
        is_canonical_site=True,
    )
    assert not su._should_run_widened_dedup_adjudicator(
        candidate=candidate,
        match_event=None,
        identity_gate_match=None,
        anchor_forced=False,
        is_canonical_site=True,
    )
