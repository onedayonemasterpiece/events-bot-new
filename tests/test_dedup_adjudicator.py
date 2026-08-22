"""Regression tests for the widened-recall LLM dedup adjudicator (INC-2026-05-30 opt 1).

The adjudicator runs on the create path only, over a recall set that is NOT gated by
exact location/time. The LLM decides match-vs-create; ``_dedup_adjudicator_accept_merge``
is the deterministic guard ladder that must reject a false merge even when the LLM said
``match``. These tests pin the guard ladder (§4) and the blocking key against the
incident's failure buckets and the session-split false positive.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from smart_event_update import (
    _dedup_adjudicator_accept_merge,
    _dedup_adjudicator_block_candidates,
    _dedup_adjudicator_final_result,
)
from smart_update_identity import IdentityFinalAction, IdentityFinalRelation


@dataclass
class _Poster:
    sha256: str | None = None
    ocr_text: str | None = None
    ocr_title: str | None = None


@dataclass
class _Cand:
    title: str
    date: str
    end_date: str | None = None
    time: str | None = None
    time_is_default: bool = False
    location_name: str | None = None
    location_address: str | None = None
    city: str | None = None
    ticket_link: str | None = None
    source_url: str | None = None
    source_type: str = "vk"
    posters: list = field(default_factory=list)


@dataclass
class _Event:
    id: int
    title: str
    date: str
    end_date: str | None = None
    time: str | None = None
    time_is_default: bool = False
    location_name: str | None = None
    location_address: str | None = None
    city: str | None = None
    ticket_link: str | None = None
    source_post_url: str | None = None
    source_vk_post_url: str | None = None


def _decide(action, code, conf, match_id=1):
    return {
        "action": action,
        "match_event_id": match_id if action == "match" else None,
        "confidence": conf,
        "reason_code": code,
        "reason": "test",
    }


def _typed_decide(
    action,
    code,
    conf,
    *,
    match_id=1,
    relation="same_event",
    evidence=None,
    conflicts=None,
):
    decision = _decide(action, code, conf, match_id=match_id)
    decision.update(
        {
            "relation": relation,
            "source_grounded_evidence": list(evidence or []),
            "blocking_conflicts": list(conflicts or []),
        }
    )
    return decision


def test_final_result_accepts_only_guarded_explicit_match():
    cand = _Cand(
        title="SOS — легендарная вечеринка",
        date="2026-08-22",
        time="21:00",
        location_name="Барн",
        ticket_link="https://barn.timepad.ru/event/4147114",
    )
    owner = _Event(
        id=8117,
        title="Тройной день рождения: Барн, Chipi Clo и SOS",
        date="2026-08-22",
        time="21:00",
        location_name="Барн",
        ticket_link="https://barn.timepad.ru/event/4147114",
    )

    result = _dedup_adjudicator_final_result(
        cand,
        [owner],
        _typed_decide(
            "match",
            "identical_anchors_dup",
            0.99,
            match_id=8117,
            evidence=["SOS"],
        ),
    )

    assert result.action is IdentityFinalAction.FINAL_MATCH
    assert result.relation is IdentityFinalRelation.SAME_EVENT
    assert result.owner_event_id == 8117


def test_final_result_create_without_grounded_distinct_is_retry():
    result = _dedup_adjudicator_final_result(
        _Cand(title="Праздничный SOS", date="2026-08-22"),
        [],
        _typed_decide(
            "create",
            "no_candidate_match",
            0.99,
            match_id=None,
            relation="unknown",
        ),
    )

    assert result.action is IdentityFinalAction.FINAL_RETRY
    assert result.reason_code == "distinct_not_grounded"


def test_final_result_explicit_grounded_occurrence_difference_is_distinct():
    owner = _Event(
        id=5426,
        title="Кураторская экскурсия",
        date="2026-08-22",
        time="12:00",
        location_name="Музей",
    )
    result = _dedup_adjudicator_final_result(
        _Cand(
            title="Кураторская экскурсия",
            date="2026-08-22",
            time="15:00",
            location_name="Музей",
        ),
        [owner],
        _typed_decide(
            "create",
            "session_split_keep",
            0.99,
            match_id=None,
            relation="distinct_occurrence",
            evidence=["Кураторская экскурсия"],
            conflicts=["event 5426 starts at 12:00"],
        ),
    )

    assert result.action is IdentityFinalAction.FINAL_DISTINCT
    assert result.relation is IdentityFinalRelation.DISTINCT_OCCURRENCE


def test_final_result_rejected_or_low_confidence_match_is_retry():
    owner = _Event(
        id=1,
        title="Выставка",
        date="2026-08-22",
        time="12:00",
        location_name="Музей",
    )
    result = _dedup_adjudicator_final_result(
        _Cand(
            title="Экскурсия по выставке",
            date="2026-08-22",
            time="12:00",
            location_name="Музей",
        ),
        [owner],
        _typed_decide(
            "match",
            "venue_variant",
            0.2,
            evidence=["Выставка"],
        ),
    )

    assert result.action is IdentityFinalAction.FINAL_RETRY
    assert result.reason_code.startswith("match_rejected:")


def test_final_result_provider_or_schema_abstention_is_retry():
    result = _dedup_adjudicator_final_result(
        _Cand(title="Событие", date="2026-08-22"),
        [],
        None,
    )

    assert result.action is IdentityFinalAction.FINAL_RETRY
    assert result.reason_code == "adjudicator_unavailable"


# --- merge cases (action=match, expected accept=True) ---------------------------


def test_a_doors_start_skew_accepts_within_90min():
    cand = _Cand(title="🎤 Саша Ветров", date="2026-05-30", time="19:00", location_name="Бар Бастион")
    ev = _Event(id=1, title="Саша Ветров", date="2026-05-30", time="20:00", location_name="Бар Бастион")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "doors_start_skew", 0.9), allow_parallel=False
    )
    assert ok and code == "doors_start_skew"


def test_b_venue_variant_accepts():
    cand = _Cand(title="🎤 Трибьют-шоу «Руки вверх»", date="2026-06-05", time="20:00", location_name="Понарт")
    ev = _Event(id=1, title="Трибьют-шоу «Руки вверх!»", date="2026-06-05", time="20:00", location_name="Бар Бастион")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "venue_variant", 0.85), allow_parallel=False
    )
    assert ok and code == "venue_variant"


def test_c_junk_location_accepts_even_with_unrelated_title():
    # Junk-location is the one code where an unrelated title is allowed (venue+text prove identity).
    cand = _Cand(title="Концерт «Эпидемия. Огненная рукопись»", date="2026-07-17", time="20:00",
                 location_name="Поселение викингов Кауп")
    ev = _Event(id=1, title="🤘 ЭПИДЕМИЯ. ОГНЕННАЯ РУКОПИСЬ", date="2026-07-17", location_name="Поселение викингов Кауп")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "junk_location_same_venue", 0.85), allow_parallel=False
    )
    assert ok and code == "junk_location_same_venue"


def test_d_two_vendor_same_slot_accepts_with_distinct_per_event_links():
    cand = _Cand(title="Концерт «ПроСТО век Зацепина»", date="2026-06-21", time="17:00",
                 location_name="Калининградский театр эстрады", ticket_link="https://xn--39.xn--p1ai/?id=889")
    ev = _Event(id=1, title="ПроСТО век Зацепина", date="2026-06-21", time="17:00",
                location_name="Калининградский театр эстрады",
                ticket_link="https://domiskusstv.edinoepole.ru/widget/events/916")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "two_vendor_same_slot", 0.88), allow_parallel=False
    )
    assert ok and code == "two_vendor_same_slot"


def test_d_identical_anchors_dup_accepts():
    cand = _Cand(title="Pianissimo: Илья Папоян", date="2026-06-26", time="20:00", location_name="Филиал Третьяковской")
    ev = _Event(id=1, title="Фестиваль Pianissimo: Илья Папоян", date="2026-06-26", time="20:00",
                location_name="Филиал Третьяковской")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "identical_anchors_dup", 0.9), allow_parallel=False
    )
    assert ok and code == "identical_anchors_dup"


# --- keep-separate / veto cases (expected accept=False) -------------------------


def test_fp_same_source_url_time_split_hard_veto_even_if_llm_says_match():
    # The load-bearing guard: 5426/5427 from t.me/gusmuseum/4509 ("В 11:00 … В 13:00 …").
    # Must create even when the LLM (wrongly) returns action=match.
    post = "https://t.me/gusmuseum/4509"
    cand = _Cand(title="Мастер-класс по созданию открытки «Овечка»", date="2026-06-01", time="13:00",
                 location_name="Гусевский музей", source_url=post)
    ev = _Event(id=1, title="Мастер-класс по созданию открытки «Овечка»", date="2026-06-01", time="11:00",
                location_name="Гусевский музей", source_post_url=post)
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "doors_start_skew", 0.99), allow_parallel=False
    )
    assert not ok and code == "same_source_url_time_split"


def test_fp_matinee_evening_time_gap_over_90min_vetoed():
    cand = _Cand(title="Спектакль «Кот в сапогах»", date="2026-06-01", time="19:00", location_name="Театр кукол")
    ev = _Event(id=1, title="Спектакль «Кот в сапогах»", date="2026-06-01", time="11:00", location_name="Театр кукол")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "doors_start_skew", 0.95), allow_parallel=False
    )
    assert not ok and code == "time_conflict_veto"


def test_fp_unrelated_titles_vetoed_unless_junk_location():
    cand = _Cand(title="Лекция о теории относительности", date="2026-06-01", time="19:00", location_name="Научная библиотека")
    ev = _Event(id=1, title="Концерт «Портрет девушки-бойца»", date="2026-06-01", time="19:00", location_name="Научная библиотека")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "venue_variant", 0.9), allow_parallel=False
    )
    assert not ok and code == "unrelated_titles"




def test_high_confidence_anchor_match_bypasses_title_only_veto():
    cand = _Cand(title="Валерия", date="2026-07-01", time="19:00", location_name="Янтарь холл")
    ev = _Event(id=1, title="Концерт Валерии", date="2026-07-01", time="19:00", location_name="Янтарь холл")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "identical_anchors_dup", 0.97), allow_parallel=False
    )
    assert ok and code == "identical_anchors_dup"


def test_parallel_venue_raises_confidence_threshold():
    # allow_parallel venue needs >=0.90; a 0.85 merge is rejected.
    cand = _Cand(title="Выставка А", date="2026-06-01", time="12:00", location_name="Музей")
    ev = _Event(id=1, title="Выставка А", date="2026-06-01", time="12:00", location_name="Музей")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "venue_variant", 0.85), allow_parallel=True
    )
    assert not ok and code.startswith("low_conf_")


def test_low_confidence_below_threshold_rejected():
    cand = _Cand(title="Событие X", date="2026-06-01", time="19:00", location_name="Клуб")
    ev = _Event(id=1, title="Событие X", date="2026-06-01", time="19:00", location_name="Клуб")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "venue_variant", 0.6), allow_parallel=False
    )
    assert not ok and code.startswith("low_conf_")


def test_keep_code_or_create_action_never_merges():
    cand = _Cand(title="Событие X", date="2026-06-01", time="19:00", location_name="Клуб")
    ev = _Event(id=1, title="Событие X", date="2026-06-01", time="19:00", location_name="Клуб")
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "session_split_keep", 0.99), allow_parallel=False
    )
    assert not ok and code.startswith("non_merge_code")
    ok2, code2 = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("create", "no_candidate_match", 0.99), allow_parallel=False
    )
    assert not ok2 and code2 == "llm_create"


def test_generic_ticket_false_friend_vetoes_two_vendor():
    # Same GENERIC season page on both → not proof of a two-vendor dup.
    season = "https://teatr.ru/season"
    cand = _Cand(title="Балет А", date="2026-06-01", time="19:00", location_name="Театр", ticket_link=season)
    ev = _Event(id=1, title="Балет А", date="2026-06-01", time="19:00", location_name="Театр", ticket_link=season)
    ok, code = _dedup_adjudicator_accept_merge(
        cand, ev, decision=_decide("match", "two_vendor_same_slot", 0.9), allow_parallel=False
    )
    assert not ok and code == "generic_ticket_false_friend"


# --- blocking key ---------------------------------------------------------------


def test_blocking_keeps_title_related_across_drifted_venue_and_time():
    # Recall-biased blocking: a title-related sibling at a DIFFERENT venue and a
    # drifted time still enters the candidate set (the LLM/guard rejects false ones).
    # An event sharing neither title, venue, ticket nor poster is excluded.
    cand = _Cand(title="🎤 Трибьют-шоу «Руки вверх»", date="2026-06-05", time="20:00", location_name="Понарт")
    same = _Event(id=1, title="Трибьют-шоу «Руки вверх!»", date="2026-06-05", time="18:00", location_name="Бар Бастион")
    other = _Event(id=2, title="Лекция о Канте и метафизике", date="2026-06-05", time="20:00", location_name="Дом Канта")
    blocked = _dedup_adjudicator_block_candidates(cand, [same, other], {})
    assert same in blocked
    assert other not in blocked


def test_blocking_keeps_poster_hash_match_even_without_title_or_venue():
    cand = _Cand(title="Нечто загадочное", date="2026-06-05", time="20:00", location_name="Лес",
                 posters=[_Poster(sha256="abc123")])
    same = _Event(id=1, title="Совсем другое название", date="2026-06-05", time="12:00", location_name="Город")
    posters_map = {1: [_Poster(sha256="abc123")]}
    blocked = _dedup_adjudicator_block_candidates(cand, [same], posters_map)
    assert same in blocked
