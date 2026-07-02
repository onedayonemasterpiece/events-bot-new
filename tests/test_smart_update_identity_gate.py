from __future__ import annotations

from dataclasses import dataclass, field

from smart_update_identity import (
    IdentityGateAction,
    IdentityGateMode,
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
    location_name: str | None = None
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
        title="Валерия",
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
        title="Валерия",
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
