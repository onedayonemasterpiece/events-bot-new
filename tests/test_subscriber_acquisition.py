from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import select

from db import Database
from models import AcqDiscoveryRun, AcqLinkTarget, AcqOpportunity, AcqReviewFeedback, AcqSurface
from subscriber_acquisition.config import AcqConfig
from subscriber_acquisition.importer import import_discovery_result
from subscriber_acquisition.link_targets import select_link_target
from subscriber_acquisition.review import build_surface_keyboard, find_opportunity_by_review_message, publish_review_cards, record_feedback, record_surface_feedback
from subscriber_acquisition.safety import AcquisitionSafetyError, ensure_review_chat, ensure_vk_read_only
from subscriber_acquisition.scoring import conservative_reach_low, priority_score
from subscriber_acquisition.service import add_surface_seed, list_surfaces, run_acq_discovery_shadow


@pytest_asyncio.fixture
async def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_SKIP_TG_SOURCES_SEED", "1")
    monkeypatch.setenv("DB_INIT_SKIP_GUIDE_SOURCES_SEED", "1")
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    database = Database(str(tmp_path / "acq.sqlite"))
    await database.init()
    try:
        yield database
    finally:
        await database.close()


@pytest.fixture
def sample_payload():
    return json.loads(Path("tests/fixtures/acq_discovery_result.sample.json").read_text(encoding="utf-8"))


@pytest.mark.asyncio
async def test_import_discovery_result_creates_schema_rows(db, sample_payload):
    result = await import_discovery_result(db, sample_payload)
    assert result.run.id
    assert len(result.surfaces) == 1
    assert len(result.opportunities) == 1
    async with db.get_session() as session:
        surfaces = (await session.execute(select(AcqSurface))).scalars().all()
        opps = (await session.execute(select(AcqOpportunity))).scalars().all()
        targets = (await session.execute(select(AcqLinkTarget))).scalars().all()
        runs = (await session.execute(select(AcqDiscoveryRun))).scalars().all()
    assert surfaces[0].external_id == "tg:example"
    assert opps[0].link_target_kind == "topic_landing"
    assert opps[0].sticker_fit == "possible"
    assert targets[0].kind == "topic_landing"
    assert runs[0].status == "done"


@pytest.mark.asyncio
async def test_import_dedupes_context(db, sample_payload):
    first = await import_discovery_result(db, sample_payload)
    second = await import_discovery_result(db, sample_payload)
    assert len(first.opportunities) == 1
    assert len(second.opportunities) == 0
    assert second.skipped_duplicate_contexts == 1


def test_conservative_reach_and_priority_do_not_override_high_risk():
    cfg = AcqConfig(tg_comment_readthrough_factor=0.02, reach_unknown_group_low=5)
    reach = conservative_reach_low(platform="tg", surface_type="channel", recent_views_p10=1000, config=cfg)
    assert reach["low"] == 20
    assert priority_score(relevance=0.95, reach_low=10000, spam_risk="high", safety_risk="low") == 0.0


def test_link_target_selection_topic_and_event():
    cfg = AcqConfig(default_link_target_url="https://t.me/kenigevents", pka_channel_url="https://t.me/kenigevents")
    topic = select_link_target(topic_cluster="organ_concerts", candidate_events=[], config=cfg)
    assert topic.kind == "topic_landing"
    event = SimpleNamespace(ticket_link="https://tickets.example/1", telegraph_url="https://telegra.ph/x")
    chosen = select_link_target(topic_cluster="organ_concerts", candidate_events=[event], config=cfg)
    assert chosen.kind == "event_site"
    assert chosen.url == "https://tickets.example/1"


class FakeBot:
    def __init__(self):
        self.calls = []

    async def send_message(self, chat_id, text, **kwargs):
        assert chat_id == 777  # no external TG send: review chat only
        self.calls.append((chat_id, text, kwargs))
        return SimpleNamespace(chat=SimpleNamespace(id=chat_id), message_id=100 + len(self.calls))


@pytest.mark.asyncio
async def test_review_cards_and_feedback_capture(db, sample_payload):
    result = await import_discovery_result(db, sample_payload)
    cfg = AcqConfig(review_chat_id=777, review_group_max_cards_per_run=20)
    bot = FakeBot()
    posted = await publish_review_cards(db, bot, result.opportunities, config=cfg)
    assert posted == 1
    assert "✅ Да" in repr(bot.calls[0][2]["reply_markup"])
    async with db.get_session() as session:
        opp = (await session.execute(select(AcqOpportunity))).scalar_one()
    assert opp.review_message_chat_id == 777
    fb = await record_feedback(db, opportunity_id=opp.id, reviewer_id=42, action="approve", review_message_chat_id=777, review_message_id=101)
    assert fb.action == "approve"
    found = await find_opportunity_by_review_message(db, chat_id=777, message_id=101)
    assert found and found.id == opp.id
    comment = await record_feedback(db, opportunity_id=opp.id, reviewer_id=42, action="comment", note="хороший кейс", review_message_chat_id=777, review_message_id=101)
    assert comment.note == "хороший кейс"
    async with db.get_session() as session:
        fbs = (await session.execute(select(AcqReviewFeedback))).scalars().all()
        opp2 = await session.get(AcqOpportunity, opp.id)
    assert len(fbs) == 2
    assert opp2.status == "approved"



@pytest.mark.asyncio
async def test_surface_seed_add_and_review_feedback(db):
    surface = await add_surface_seed(db, "https://t.me/new_public", reviewer_id=42)
    assert surface.external_id == "tg:new_public"
    assert surface.status == "candidate"
    keyboard = build_surface_keyboard(surface)
    texts = [button.text for row in keyboard.inline_keyboard for button in row]
    assert texts == ["✅ Да", "❌ Нет", "🕒 Потом", "🔗 Открыть"]

    feedback = await record_surface_feedback(db, surface_id=surface.id, reviewer_id=42, action="approve")
    surfaces = await list_surfaces(db, limit=5)

    assert feedback.action == "surface_approve"
    assert surfaces[0].status == "approved"
    async with db.get_session() as session:
        stored_feedback = (await session.execute(select(AcqReviewFeedback))).scalars().all()
    assert stored_feedback[0].surface_id == surface.id

@pytest.mark.asyncio
async def test_review_cards_are_hard_capped_at_20_and_have_required_buttons(db, sample_payload):
    bulk = json.loads(json.dumps(sample_payload))
    base = bulk["opportunities"][0]
    bulk["opportunities"] = []
    for i in range(25):
        item = json.loads(json.dumps(base))
        item["context_url"] = f"https://t.me/example/{1000 + i}"
        item["context_external_id"] = str(1000 + i)
        item["context_text_snippet"] = f"Где событие #{i}?"
        bulk["opportunities"].append(item)
    result = await import_discovery_result(db, bulk)
    cfg = AcqConfig(review_chat_id=777, review_group_max_cards_per_run=50)
    bot = FakeBot()

    posted = await publish_review_cards(db, bot, result.opportunities, config=cfg)

    assert posted == 20
    assert len(bot.calls) == 20
    keyboard = bot.calls[0][2]["reply_markup"].inline_keyboard
    texts = [button.text for row in keyboard for button in row]
    assert texts == ["✅ Да", "❌ Нет", "🕒 Потом", "🔗 Контекст", "🎯 Куда"]


@pytest.mark.asyncio
async def test_shadow_run_with_configured_fixture_path_creates_review_artifacts(db, sample_payload, tmp_path, monkeypatch):
    async def fake_report(run, surfaces, opportunities):
        return "https://telegra.ph/acq-report"

    fixture = tmp_path / "acq_result.json"
    fixture.write_text(json.dumps(sample_payload, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr("subscriber_acquisition.service.publish_telegraph_report", fake_report)
    cfg = AcqConfig(review_chat_id=777, fixture_path=str(fixture))
    bot = FakeBot()

    result = await run_acq_discovery_shadow(db, bot=bot, config=cfg)
    opp_id = result.opportunities[0].id
    fb = await record_feedback(db, opportunity_id=opp_id, reviewer_id=42, action="approve", review_message_chat_id=777, review_message_id=101)

    async with db.get_session() as session:
        runs = (await session.execute(select(AcqDiscoveryRun))).scalars().all()
        surfaces = (await session.execute(select(AcqSurface))).scalars().all()
        opportunities = (await session.execute(select(AcqOpportunity))).scalars().all()
        targets = (await session.execute(select(AcqLinkTarget))).scalars().all()
        feedback = (await session.execute(select(AcqReviewFeedback))).scalars().all()

    assert result.run.telegraph_url == "https://telegra.ph/acq-report"
    assert result.run.stats_json["review_cards_posted"] == 1
    assert len(bot.calls) == 1
    assert runs and surfaces and opportunities and targets and feedback
    assert fb.action == "approve"

def test_no_send_guard_blocks_external_targets():
    ensure_review_chat(777, review_chat_id=777)
    with pytest.raises(AcquisitionSafetyError):
        ensure_review_chat(123, review_chat_id=777)
    with pytest.raises(AcquisitionSafetyError):
        ensure_vk_read_only("wall.createComment")


@pytest.mark.asyncio
async def test_shadow_run_publishes_report_and_review_cards(db, sample_payload, monkeypatch):
    async def fake_report(run, surfaces, opportunities):
        return "https://telegra.ph/acq-report"

    monkeypatch.setattr("subscriber_acquisition.service.publish_telegraph_report", fake_report)
    cfg = AcqConfig(review_chat_id=777, review_group_max_cards_per_run=20)
    bot = FakeBot()
    result = await run_acq_discovery_shadow(db, bot=bot, payload=sample_payload, config=cfg)
    assert result.run.telegraph_url == "https://telegra.ph/acq-report"
    assert result.run.stats_json["review_cards_posted"] == 1
    assert len(bot.calls) == 1
