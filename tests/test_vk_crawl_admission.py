from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio

import main
import vk_intake
from db import Database


REPLAY = (
    Path(__file__).parent
    / "replays"
    / "INC-2026-08-15-ingestion-retry-stall-and-wal-growth"
    / "vk_crawl_admission.json"
)


@pytest_asyncio.fixture(autouse=True)
async def _dispose_test_databases(monkeypatch):
    instances: list[Database] = []
    original_init = Database.__init__

    def tracked_init(instance, *args, **kwargs):
        original_init(instance, *args, **kwargs)
        instances.append(instance)

    monkeypatch.setattr(Database, "__init__", tracked_init)
    yield
    for instance in instances:
        await instance.close()


async def _db(tmp_path: Path) -> Database:
    db = Database(str(tmp_path / "vk-admission.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','Source')"
        )
        await conn.commit()
    return db


@pytest.mark.asyncio
async def test_admission_schema_init_is_idempotent(tmp_path):
    db = Database(str(tmp_path / "schema.sqlite"))
    await db.init()
    await db.init()
    async with db.raw_conn() as conn:
        columns = {
            str(row[1])
            for row in await (await conn.execute(
                "PRAGMA table_info(vk_source_packet)"
            )).fetchall()
        }
        quick = await (await conn.execute("PRAGMA quick_check")).fetchone()
    assert {"admission_status", "admission_reason", "admission_receipt_json"} <= columns
    assert quick == ("ok",)


async def _crawl(db: Database, monkeypatch, posts: list[dict]) -> dict:
    async def wall(*_args, offset=0, **_kwargs):
        return posts if offset == 0 else []

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    return await vk_intake.crawl_once(db)


class _AdmissionClient:
    def __init__(self, payload: dict | BaseException):
        self.payload = payload
        self.calls: list[dict] = []

    async def generate_content_async(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.payload, BaseException):
            raise self.payload
        return json.dumps(self.payload, ensure_ascii=False), object()


@pytest.mark.asyncio
async def test_future_keyword_date_is_queued_without_admission_llm(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    tomorrow = datetime.now(main.LOCAL_TZ) + timedelta(days=1)
    month = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля",
        5: "мая", 6: "июня", 7: "июля", 8: "августа",
        9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
    }[tomorrow.month]
    text = f"Концерт завтра, {tomorrow.day} {month} в 19:00"
    client = _AdmissionClient(AssertionError("LLM must not run"))
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    stats = await _crawl(
        db,
        monkeypatch,
        [{"date": int(time.time()), "post_id": 1, "text": text, "photos": []}],
    )

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status FROM vk_inbox WHERE post_id=1"
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT admission_status,admission_reason,admission_receipt_json "
            "FROM vk_source_packet WHERE post_id=1"
        )).fetchone()
    assert inbox == ("pending",)
    assert packet[0:2] == ("admitted", "deterministic_future_event")
    assert json.loads(packet[2])["route"] == "deterministic"
    assert stats["added"] == 1
    assert stats["admission_rejected"] == 0
    assert client.calls == []


@pytest.mark.asyncio
async def test_deterministic_failure_is_llm_checked_and_grounded_non_event_stays_out_of_queue(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    client = _AdmissionClient(
        {
            "decisions": [
                {
                    "id": "1:2",
                    "outcome": "NON_EVENT",
                    "confidence": 0.98,
                    "evidence_quote": "Отчёт о ремонте фасада",
                    "reason": "administrative_news",
                }
            ]
        }
    )
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    stats = await _crawl(
        db,
        monkeypatch,
        [{
            "date": int(time.time()),
            "post_id": 2,
            "text": "Отчёт о ремонте фасада учреждения за прошедший квартал.",
            "photos": [],
        }],
    )

    async with db.raw_conn() as conn:
        inbox_count = await (await conn.execute(
            "SELECT COUNT(*) FROM vk_inbox WHERE post_id=2"
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT status,admission_status,admission_reason,admission_receipt_json "
            "FROM vk_source_packet WHERE post_id=2"
        )).fetchone()
    assert inbox_count == (0,)
    assert packet[:3] == ("completed", "rejected", "llm_non_event")
    assert json.loads(packet[3])["outcome"] == "NON_EVENT"
    assert stats["added"] == 0
    assert stats["admission_rejected"] == 1
    assert stats["admission_llm_checked"] == 1
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_raw_packet_is_committed_before_admission_provider_call(tmp_path, monkeypatch):
    db = await _db(tmp_path)

    async def inspect_durable_boundary(candidates, *, tz):
        assert tz is main.LOCAL_TZ
        async with db.raw_conn() as conn:
            packet = await (await conn.execute(
                "SELECT admission_status FROM vk_source_packet WHERE post_id=22"
            )).fetchone()
            inbox = await (await conn.execute(
                "SELECT COUNT(*) FROM vk_inbox WHERE post_id=22"
            )).fetchone()
        assert packet == ("legacy_unclassified",)
        assert inbox == (0,)
        return {
            candidates[0].key: vk_intake.VKCrawlAdmissionDecision(
                admitted=False,
                outcome="NON_EVENT",
                reason="llm_non_event",
                route="llm",
                confidence=0.99,
                evidence_quote="Отчёт о ремонте",
                prompt_version=vk_intake.VK_CRAWL_ADMISSION_PROMPT_VERSION,
                model=vk_intake.VK_CRAWL_ADMISSION_MODEL,
            )
        }

    monkeypatch.setattr(
        vk_intake, "_call_vk_crawl_admission_llm", inspect_durable_boundary
    )
    await _crawl(
        db,
        monkeypatch,
        [{
            "date": int(time.time()),
            "post_id": 22,
            "text": "Отчёт о ремонте за прошлый квартал.",
            "photos": [],
        }],
    )

    async with db.raw_conn() as conn:
        packet = await (await conn.execute(
            "SELECT status,admission_status FROM vk_source_packet WHERE post_id=22"
        )).fetchone()
    assert packet == ("completed", "rejected")


@pytest.mark.asyncio
async def test_llm_future_event_is_queued_and_crawl_does_not_run_auto_import(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    client = _AdmissionClient(
        {
            "decisions": [
                {
                    "id": "1:3",
                    "outcome": "ADMIT",
                    "confidence": 0.96,
                    "evidence_quote": "Ждём вас в Чеховке",
                    "reason": "future_event",
                }
            ]
        }
    )
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)
    monkeypatch.setattr(
        vk_intake,
        "build_event_drafts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("auto-import ran")),
    )

    stats = await _crawl(
        db,
        monkeypatch,
        [{
            "date": int(time.time()),
            "post_id": 3,
            "text": "Сегодня встречаемся: ждём вас в Чеховке, подробности на афише.",
            "photos": [],
        }],
    )

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status FROM vk_inbox WHERE post_id=3"
        )).fetchone()
    assert inbox == ("pending",)
    assert stats["added"] == 1
    assert stats["admission_llm_admitted"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        RuntimeError("provider unavailable"),
        {"decisions": [{"id": "1:4", "outcome": "NON_EVENT", "confidence": 0.99}]},
        {
            "decisions": [
                {
                    "id": "1:4",
                    "outcome": "NON_EVENT",
                    "confidence": 0.72,
                    "evidence_quote": "Обычная запись",
                }
            ]
        },
        {
            "decisions": [
                {
                    "id": "1:4",
                    "outcome": "PAST_ONLY",
                    "confidence": 0.99,
                    "evidence_quote": "цитата отсутствует в источнике",
                }
            ]
        },
    ],
)
async def test_uncertain_invalid_or_failed_llm_fails_open_into_queue(
    tmp_path, monkeypatch, payload
):
    db = await _db(tmp_path)
    client = _AdmissionClient(payload)
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    stats = await _crawl(
        db,
        monkeypatch,
        [{
            "date": int(time.time()),
            "post_id": 4,
            "text": "Обычная запись с неясным контекстом.",
            "photos": [],
        }],
    )

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status FROM vk_inbox WHERE post_id=4"
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT admission_status,admission_reason FROM vk_source_packet WHERE post_id=4"
        )).fetchone()
    assert inbox == ("pending",)
    assert packet[0] == "admitted"
    assert packet[1].startswith("fail_open_")
    assert stats["admission_fail_open"] == 1


@pytest.mark.asyncio
async def test_blank_visual_post_is_kept_for_later_ocr_without_text_only_llm(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    client = _AdmissionClient(AssertionError("text-only LLM must not reject unseen poster"))
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    await _crawl(
        db,
        monkeypatch,
        [{"date": int(time.time()), "post_id": 5, "text": "", "photos": ["poster"]}],
    )

    async with db.raw_conn() as conn:
        packet = await (await conn.execute(
            "SELECT admission_status,admission_reason FROM vk_source_packet WHERE post_id=5"
        )).fetchone()
        inbox = await (await conn.execute(
            "SELECT matched_kw,status FROM vk_inbox WHERE post_id=5"
        )).fetchone()
    assert packet == ("admitted", "visual_evidence_requires_ocr")
    assert inbox == (vk_intake.OCR_PENDING_SENTINEL, "pending")
    assert client.calls == []


@pytest.mark.asyncio
async def test_text_only_llm_cannot_reject_unseen_visual_evidence(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    client = _AdmissionClient({"decisions": [{
        "id": "1:55",
        "outcome": "NON_EVENT",
        "confidence": 0.99,
        "evidence_quote": "Короткая информационная подпись",
        "reason": "information_only",
    }]})
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    stats = await _crawl(
        db,
        monkeypatch,
        [{
            "date": int(time.time()),
            "post_id": 55,
            "text": "Короткая информационная подпись.",
            "photos": ["https://example.test/poster.jpg"],
        }],
    )

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status FROM vk_inbox WHERE post_id=55"
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT admission_reason FROM vk_source_packet WHERE post_id=55"
        )).fetchone()
    assert inbox == ("pending",)
    assert packet == ("fail_open_unseen_visual_evidence",)
    assert stats["admission_fail_open"] == 1


@pytest.mark.asyncio
async def test_exact_replay_reuses_rejected_admission_without_new_llm_call(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    post = {
        "date": int(time.time()),
        "post_id": 6,
        "text": "Опубликован отчёт о завершённом ремонте здания.",
        "photos": [],
    }
    client = _AdmissionClient(
        {"decisions": [{
            "id": "1:6",
            "outcome": "NON_EVENT",
            "confidence": 0.99,
            "evidence_quote": "отчёт о завершённом ремонте",
            "reason": "administrative_news",
        }]}
    )
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)
    await _crawl(db, monkeypatch, [post])
    await _crawl(db, monkeypatch, [post])

    async with db.raw_conn() as conn:
        packets = await (await conn.execute(
            "SELECT COUNT(*) FROM vk_source_packet WHERE post_id=6"
        )).fetchone()
        inbox = await (await conn.execute(
            "SELECT COUNT(*) FROM vk_inbox WHERE post_id=6"
        )).fetchone()
    assert packets == (1,)
    assert inbox == (0,)
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_legacy_imported_exact_revision_is_preserved_without_reclassification(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    post = {
        "date": int(time.time()),
        "post_id": 65,
        "text": "Старый уже обработанный исходный пост.",
        "photos": [],
    }
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post=post,
        source_url="https://vk.com/wall-1_65",
        keyword_hints=(),
        date_hints=(),
        event_ts_hint=None,
    )
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_inbox SET status='imported',imported_event_id=123 WHERE post_id=65"
        )
        await conn.execute(
            "UPDATE vk_source_packet SET status='completed' WHERE id=?", (packet_id,)
        )
        await conn.commit()
    client = _AdmissionClient(AssertionError("terminal exact revision must be reused"))
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    await _crawl(db, monkeypatch, [post])

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status,imported_event_id FROM vk_inbox WHERE post_id=65"
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT status,admission_status,admission_reason FROM vk_source_packet WHERE id=?",
            (packet_id,),
        )).fetchone()
    assert inbox == ("imported", 123)
    assert packet == ("completed", "admitted", "legacy_terminal_preserved")
    assert client.calls == []


@pytest.mark.asyncio
async def test_normal_crawl_preserves_legacy_pending_for_bounded_operator_requalification(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    post = {
        "date": int(time.time()),
        "post_id": 67,
        "text": "Существующая строка старой очереди без admission receipt.",
        "photos": [],
    }
    await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post=post,
        source_url="https://vk.com/wall-1_67",
        keyword_hints=(),
        date_hints=(),
        event_ts_hint=None,
    )
    client = _AdmissionClient(AssertionError("normal crawl must not bulk-drain legacy"))
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    await _crawl(db, monkeypatch, [post])

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status FROM vk_inbox WHERE post_id=67"
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT admission_status,admission_reason FROM vk_source_packet WHERE post_id=67"
        )).fetchone()
    assert inbox == ("pending",)
    assert packet == ("legacy_unclassified", None)
    assert client.calls == []


@pytest.mark.asyncio
async def test_changed_revision_rejected_by_admission_leaves_selectable_queue(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    future = datetime.now(main.LOCAL_TZ) + timedelta(days=2)
    month = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля",
        5: "мая", 6: "июня", 7: "июля", 8: "августа",
        9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
    }[future.month]
    first = {
        "date": int(time.time()),
        "post_id": 66,
        "text": f"Концерт {future.day} {month} в 19:00, билеты уже доступны.",
        "photos": [],
    }
    await _crawl(db, monkeypatch, [first])
    changed = {
        **first,
        "text": "Отчёт: мероприятие завершилось, благодарим участников.",
    }
    client = _AdmissionClient({"decisions": [{
        "id": "1:66",
        "outcome": "PAST_ONLY",
        "confidence": 0.99,
        "evidence_quote": "мероприятие завершилось",
        "reason": "past_only",
    }]})
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    await _crawl(db, monkeypatch, [changed])

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status,last_typed_reason FROM vk_inbox WHERE post_id=66"
        )).fetchone()
        packets = await (await conn.execute(
            "SELECT revision,admission_status FROM vk_source_packet WHERE post_id=66 ORDER BY revision"
        )).fetchall()
    assert inbox == ("rejected", "VK_ADMISSION_PAST_ONLY")
    assert packets == [(1, "admitted"), (2, "rejected")]


@pytest.mark.asyncio
async def test_unresolved_posts_are_checked_in_one_bounded_batch(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    decisions = []
    posts = []
    for post_id in (10, 11, 12):
        text = f"Информационный отчёт номер {post_id} без анонса."
        posts.append({
            "date": int(time.time()) + post_id,
            "post_id": post_id,
            "text": text,
            "photos": [],
        })
        decisions.append({
            "id": f"1:{post_id}",
            "outcome": "NON_EVENT",
            "confidence": 0.99,
            "evidence_quote": f"Информационный отчёт номер {post_id}",
            "reason": "information_only",
        })
    client = _AdmissionClient({"decisions": decisions})
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    stats = await _crawl(db, monkeypatch, posts)

    assert stats["admission_rejected"] == 3
    assert len(client.calls) == 1
    assert '"posts":[' in client.calls[0]["prompt"]


def test_bare_nominative_concert_is_a_deterministic_keyword():
    matched, tokens = vk_intake.match_keywords("Сегодня концерт в библиотеке")
    assert matched is True
    assert "концерт" in tokens


@pytest.mark.asyncio
async def test_legacy_pending_backlog_is_requalified_without_running_auto_import(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    post = {
        "date": int(time.time()),
        "post_id": 7,
        "text": "Новость о завершённом ремонте без анонса мероприятий.",
        "photos": [],
    }
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post=post,
        source_url="https://vk.com/wall-1_7",
        keyword_hints=(),
        date_hints=(),
        event_ts_hint=None,
    )
    client = _AdmissionClient(
        {"decisions": [{
            "id": "1:7",
            "outcome": "NON_EVENT",
            "confidence": 0.99,
            "evidence_quote": "Новость о завершённом ремонте",
            "reason": "administrative_news",
        }]}
    )
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)
    monkeypatch.setattr(
        vk_intake,
        "build_event_drafts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("auto-import ran")),
    )

    stats = await vk_intake.requalify_vk_inbox_admission(db, limit=10)

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status,last_typed_reason FROM vk_inbox WHERE source_packet_id=?",
            (packet_id,),
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT admission_status FROM vk_source_packet WHERE id=?",
            (packet_id,),
        )).fetchone()
    assert inbox == ("rejected", "VK_ADMISSION_NON_EVENT")
    assert packet == ("rejected",)
    assert stats == {
        "selected": 1,
        "classified": 1,
        "admitted": 0,
        "rejected": 1,
        "llm_checked": 1,
        "fail_open": 0,
        "invalid_source_packets": [],
        "dry_run": False,
        "remaining_legacy_pending": 0,
    }


@pytest.mark.asyncio
async def test_exact_production_positive_and_negative_controls_cross_crawl_boundary(
    tmp_path, monkeypatch
):
    fixture = json.loads(REPLAY.read_text(encoding="utf-8"))
    db = Database(str(tmp_path / "production-controls.sqlite"))
    await db.init()
    all_rows = fixture["positive"] + fixture["negative"]
    async with db.raw_conn() as conn:
        for group_id in sorted({int(row["group_id"]) for row in all_rows}):
            await conn.execute(
                "INSERT INTO vk_source(group_id,screen_name,name) VALUES(?,?,?)",
                (group_id, f"club{group_id}", f"source-{group_id}"),
            )
        await conn.commit()

    posts_by_group: dict[int, list[dict]] = {}
    for row in all_rows:
        posts_by_group.setdefault(int(row["group_id"]), []).append({
            "date": int(row["published_at"]),
            "post_id": int(row["post_id"]),
            "text": str(row["text"]),
            "photos": list(row.get("photos") or []),
            "url": str(row["source_url"]),
        })

    async def wall(group_id, *_args, offset=0, **_kwargs):
        return posts_by_group.get(int(group_id), []) if offset == 0 else []

    async def no_sleep(_delay):
        return None

    llm_decisions = []
    for row in fixture["positive"]:
        llm_decisions.append({
            "id": f"{row['group_id']}:{row['post_id']}",
            "outcome": "ADMIT",
            "confidence": 0.99,
            "evidence_quote": str(row["text"])[:80],
            "reason": "future_event",
        })
    for row in fixture["negative"]:
        quote = str(row["text"]).split("\n", 1)[0][:120]
        llm_decisions.append({
            "id": f"{row['group_id']}:{row['post_id']}",
            "outcome": "NON_EVENT",
            "confidence": 0.99,
            "evidence_quote": quote,
            "reason": "information_only",
        })
    client = _AdmissionClient({"decisions": llm_decisions})
    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake, "_get_vk_crawl_admission_client", lambda: client)

    await vk_intake.crawl_once(db)

    async with db.raw_conn() as conn:
        queue_rows = {
            (int(row[0]), int(row[1])): str(row[2])
            for row in await (await conn.execute(
                "SELECT group_id,post_id,status FROM vk_inbox"
            )).fetchall()
        }
        packet_rows = {
            (int(row[0]), int(row[1])): str(row[2])
            for row in await (await conn.execute(
                "SELECT owner_id,post_id,admission_status FROM vk_source_packet"
            )).fetchall()
        }
    assert all(
        queue_rows[(int(row["group_id"]), int(row["post_id"]))] == "pending"
        for row in fixture["positive"]
    )
    assert all(
        (int(row["group_id"]), int(row["post_id"])) not in queue_rows
        for row in fixture["negative"]
    )
    assert all(
        packet_rows[(int(row["group_id"]), int(row["post_id"]))] == "rejected"
        for row in fixture["negative"]
    )
