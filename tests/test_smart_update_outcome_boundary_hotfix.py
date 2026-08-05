from __future__ import annotations

from types import SimpleNamespace

import pytest

import smart_event_update as su
import smart_update_identity as identity
from db import Database
from models import Event, EventSource
from smart_event_update import SmartUpdateResult
from smart_update_identity import (
    IdentityGateMode,
    MergeIdentityAction,
    MergeIdentityRelation,
)


def test_outcome_contract_is_fail_closed_for_unknown_and_review_statuses() -> None:
    assert (
        su.classify_smart_update_status("created")
        is su.SmartUpdateOutcomeKind.ACCEPTED_CHANGED
    )
    assert (
        su.classify_smart_update_status("noop_exact_source_replay")
        is su.SmartUpdateOutcomeKind.ACCEPTED_NO_CHANGE
    )
    for status in (
        "review_required",
        "skipped_identity_gate",
        "rejected_low_confidence",
        "future_status_not_yet_classified",
        "",
        None,
    ):
        assert (
            su.classify_smart_update_status(status)
            is su.SmartUpdateOutcomeKind.NOT_ACCEPTED
        )


@pytest.mark.asyncio
async def test_nonaccepted_result_cannot_export_event_id_to_callers(monkeypatch) -> None:
    async def _review(*_args, **_kwargs):
        return SmartUpdateResult(
            status="review_required",
            event_id=7024,
            reason="source_binding_conflict",
        )

    monkeypatch.setattr(su, "_smart_event_update_impl", _review)
    result = await su.smart_event_update(
        object(),
        SimpleNamespace(source_url="https://example.test/source"),
    )

    assert result.status == "review_required"
    assert result.event_id is None
    assert result.matched_event_id == 7024
    assert result.created is False
    assert result.merged is False
    assert not su.smart_update_result_allows_caller_side_effects(result)


@pytest.mark.asyncio
async def test_accepted_result_keeps_event_id(monkeypatch) -> None:
    async def _created(*_args, **_kwargs):
        return SmartUpdateResult(status="created", event_id=9001, created=True)

    monkeypatch.setattr(su, "_smart_event_update_impl", _created)
    result = await su.smart_event_update(
        object(),
        SimpleNamespace(source_url="https://example.test/source"),
    )

    assert result.status == "created"
    assert result.event_id == 9001
    assert result.created is True
    assert su.smart_update_result_allows_caller_side_effects(result)


def _tretyakov(event_id: int, at: str) -> str:
    return (
        "https://kaliningrad.tretyakovgallery.ru/tickets/"
        f"#/buy/event/{event_id}/2026-08-09/{at}:00"
    )


def test_different_specific_ticket_occurrences_override_false_llm_allow() -> None:
    verdict = identity.build_merge_identity_gate_verdict(
        {
            "title": "Кинопоказ",
            "date": "2026-08-09",
            "time": "14:00",
            "location_name": "Третьяковская галерея",
            "event_type": "кинопоказ",
            "ticket_link": _tretyakov(48801, "14:00"),
        },
        {
            "id": 7024,
            "title": "Кинопоказ",
            "date": "2026-08-09",
            "time": "17:00",
            "location_name": "Третьяковская галерея",
            "event_type": "кинопоказ",
            "ticket_link": _tretyakov(48636, "17:00"),
        },
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_update",
        },
    )

    assert verdict.action is MergeIdentityAction.REVIEW_REQUIRED
    assert verdict.relation is MergeIdentityRelation.UNSAFE_TO_MERGE
    assert verdict.reason_code == "specific_ticket_occurrence_conflict"
    assert verdict.deterministic
    assert verdict.should_skip_side_effects


def test_same_specific_ticket_occurrence_remains_llm_first_allow() -> None:
    url = _tretyakov(48801, "14:00")
    verdict = identity.build_merge_identity_gate_verdict(
        {
            "title": "Право женщин на море",
            "date": "2026-08-09",
            "time": "14:00",
            "event_type": "кинопоказ",
            "ticket_link": url,
        },
        {
            "id": 7244,
            "title": "Право женщин на море",
            "date": "2026-08-09",
            "time": "14:00",
            "event_type": "кинопоказ",
            "ticket_link": url,
        },
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_update",
        },
    )

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects


def test_ticket_occurrence_parser_ignores_generic_landing_page() -> None:
    assert (
        identity.specific_ticket_occurrence_identity(
            "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy"
        )
        is None
    )


@pytest.mark.asyncio
async def test_legacy_unknown_source_owner_forces_review(tmp_path) -> None:
    db = Database(str(tmp_path / "legacy-owner.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            owner = Event(
                title="Первое событие",
                description="Описание",
                date="2026-09-01",
                time="19:00",
                location_name="Музей",
                source_text="Источник",
            )
            target = Event(
                title="Второе событие",
                description="Описание",
                date="2026-09-01",
                time="20:00",
                location_name="Музей",
                source_text="Источник",
            )
            session.add_all([owner, target])
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(owner.id),
                    source_type="telegram",
                    source_url="https://telegram.me/s/Shared_Source/42?single=1",
                    canonical_source_url=None,
                    source_role=None,
                )
            )
            await session.commit()

        canonical = identity.canonicalize_identity_url(
            "https://t.me/shared_source/42"
        )
        assert canonical
        async with db.get_session() as session:
            conflict = await su._source_identity_binding_conflict(
                session,
                event_id=int(target.id),
                canonical_source_url=canonical,
                source_role="identity_bearing",
            )
            context_conflict = await su._source_identity_binding_conflict(
                session,
                event_id=int(target.id),
                canonical_source_url=canonical,
                source_role="context_only",
            )

        assert conflict == int(owner.id)
        assert context_conflict is None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_official_parser_caller_has_zero_side_effects_after_review(
    monkeypatch,
) -> None:
    import sys
    import types

    import vk_intake
    from source_parsing import handlers

    side_effects: list[str] = []

    async def _forbidden(name: str, *_args, **_kwargs):
        side_effects.append(name)
        raise AssertionError(f"caller side effect executed: {name}")

    fake_main = types.ModuleType("main")
    fake_main.normalize_event_type = (
        lambda _title, _description, event_type: event_type
    )
    fake_main.schedule_event_update_tasks = (
        lambda *_args, **_kwargs: _forbidden("schedule_event_update_tasks")
    )
    monkeypatch.setitem(sys.modules, "main", fake_main)

    draft = SimpleNamespace(
        title="Кандидат",
        description="Описание",
        source_text="Источник",
        date="2026-09-01",
        time="19:00",
        ticket_price_min=None,
        ticket_price_max=None,
        venue="Музей",
        ticket_link=None,
        pushkin_card=False,
        festival=None,
        location_address=None,
        city="Калининград",
        event_type="лекция",
        emoji=None,
        is_free=False,
        search_digest=None,
    )

    async def _drafts(*_args, **_kwargs):
        return [draft], None

    async def _review(*_args, **_kwargs):
        return SmartUpdateResult(
            status="review_required",
            event_id=7024,
            reason="specific_ticket_occurrence_conflict",
        )

    monkeypatch.setattr(vk_intake, "build_event_drafts_from_vk", _drafts)
    monkeypatch.setattr(
        handlers,
        "_build_parser_source_text",
        lambda *_args, **_kwargs: "Источник",
    )
    monkeypatch.setattr(
        handlers,
        "update_event_ticket_status",
        lambda *_args, **_kwargs: _forbidden("update_event_ticket_status"),
    )
    monkeypatch.setattr(
        handlers,
        "update_linked_events",
        lambda *_args, **_kwargs: _forbidden("update_linked_events"),
    )
    monkeypatch.setattr(
        handlers,
        "_ensure_telegraph_url",
        lambda *_args, **_kwargs: _forbidden("_ensure_telegraph_url"),
    )
    monkeypatch.setattr(su, "_smart_event_update_impl", _review)

    theatre_event = SimpleNamespace(
        title="Кандидат",
        description="Описание",
        age_restriction=None,
        scene=None,
        location="Музей",
        location_address=None,
        source_type="tretyakov",
        photos=[],
        url=_tretyakov(48801, "14:00"),
        parsed_date="2026-09-01",
        parsed_time="19:00",
        end_date=None,
        ticket_price_min=None,
        ticket_price_max=None,
        pushkin_card=False,
        ticket_status="available",
    )

    event_id, was_added, status = await handlers.add_new_event_via_queue(
        object(),
        None,
        theatre_event,
        1,
        1,
        poster_media=[],
    )

    assert event_id is None
    assert was_added is False
    assert status == "review_required"
    assert side_effects == []


@pytest.mark.asyncio
async def test_vk_persist_caller_does_not_convert_review_into_success(
    monkeypatch,
) -> None:
    import sys
    import types

    import vk_intake

    side_effects: list[str] = []

    async def _forbidden(name: str, *_args, **_kwargs):
        side_effects.append(name)
        raise AssertionError(f"caller side effect executed: {name}")

    fake_main = types.ModuleType("main")
    fake_main.normalize_event_type = (
        lambda _title, _description, event_type: event_type
    )
    fake_main.schedule_event_update_tasks = (
        lambda *_args, **_kwargs: _forbidden("schedule_event_update_tasks")
    )
    fake_main.rebuild_fest_nav_if_changed = (
        lambda *_args, **_kwargs: _forbidden("rebuild_fest_nav_if_changed")
    )
    monkeypatch.setitem(sys.modules, "main", fake_main)

    async def _review(*_args, **_kwargs):
        return SmartUpdateResult(
            status="skipped_identity_gate",
            event_id=3864,
            reason="festival_context_sibling",
        )

    monkeypatch.setattr(su, "_smart_event_update_impl", _review)

    draft = SimpleNamespace(
        title="Кандидат",
        description="Описание",
        source_text="Источник",
        date="2026-09-01",
        time="19:00",
        time_is_default=False,
        end_date=None,
        festival=None,
        venue="Музей",
        location_address=None,
        city="Калининград",
        links=[],
        ticket_price_min=None,
        ticket_price_max=None,
        event_type="лекция",
        emoji=None,
        is_free=False,
        pushkin_card=False,
        search_digest=None,
        poster_media=[],
        organizer_names=[],
    )

    with pytest.raises(RuntimeError, match="returned no event_id"):
        await vk_intake.persist_event_and_pages(
            draft,
            [],
            object(),
            source_post_url="https://vk.com/wall-1_2",
        )

    assert side_effects == []
