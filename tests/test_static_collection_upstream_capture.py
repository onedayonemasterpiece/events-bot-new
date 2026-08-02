from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest

from poster_media import PosterMedia
from source_parsing.parser import TheatreEvent
from vk_intake import EventDraft


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "capture_static_collection_upstream_packet.py"
SPEC = importlib.util.spec_from_file_location("collection_upstream_capture", SCRIPT)
assert SPEC and SPEC.loader
capture = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = capture
SPEC.loader.exec_module(capture)

REPLAY_SCRIPT = ROOT / "scripts" / "run_static_collection_ingestion_replay.py"
REPLAY_SPEC = importlib.util.spec_from_file_location("collection_capture_replay", REPLAY_SCRIPT)
assert REPLAY_SPEC and REPLAY_SPEC.loader
replay = importlib.util.module_from_spec(REPLAY_SPEC)
sys.modules[REPLAY_SPEC.name] = replay
REPLAY_SPEC.loader.exec_module(replay)

REPO_SHA = "1" * 40
CAPTURED_AT = "2026-08-02T20:00:00Z"


def _telegram_input() -> dict:
    return {
        "schema_version": 2,
        "run_id": "fresh-tg-run",
        "generated_at": "2026-08-02T19:59:00Z",
        "sources_meta": [
            {
                "username": "fresh_source",
                "source_type": "channel",
                "title": "Свежий источник",
                "unknown_meta_field": {"preserved": True},
            },
            {"username": "not_selected", "source_type": "channel"},
        ],
        "messages": [
            {
                "source_username": "fresh_source",
                "message_id": 51,
                "source_link": "https://t.me/fresh_source/51",
                "text": "Приходите всей семьёй.",
                "unknown_future_field": {"must": "survive"},
                "events": [
                    {
                        "title": "Свежая семейная встреча",
                        "date": "2026-08-08",
                        "linked_source_urls": [],
                    }
                ],
            },
            {
                "source_username": "not_selected",
                "message_id": 99,
                "events": [{"title": "Не выбран"}],
            },
        ],
        "stats": {
            "sources_total": 2,
            "messages_scanned": 2,
            "messages_with_events": 2,
            "events_extracted": 2,
        },
    }


def _build_telegram(raw: dict | None = None) -> dict:
    return capture.build_capture(
        adapter="telegram",
        raw=raw or _telegram_input(),
        source_username="fresh_source",
        message_id=51,
        repo_sha=REPO_SHA,
        captured_at=CAPTURED_AT,
    )


def test_telegram_capture_keeps_exact_message_and_meta_in_one_packet():
    raw = _telegram_input()
    artifact = _build_telegram(raw)

    assert artifact["packet_count"] == 1
    assert artifact["production_handler"].endswith("process_telegram_results")
    assert artifact["source_url"] == "https://t.me/fresh_source/51"
    assert artifact["payload"]["messages"] == [raw["messages"][0]]
    assert artifact["payload"]["sources_meta"] == [raw["sources_meta"][0]]
    assert artifact["payload"]["stats"] == {
        "sources_total": 1,
        "messages_scanned": 1,
        "messages_with_events": 1,
        "events_extracted": 1,
    }
    assert artifact["payload_sha256"] == capture.canonical_sha256(artifact["payload"])
    capture.validate_capture(artifact)


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda raw: raw["messages"][0]["events"][0].update(
                linked_source_urls=["https://t.me/other/10"]
            ),
            "cross-message dependency",
        ),
        (
            lambda raw: raw["messages"][0].update(reply_to_message_id=50),
            "cross-message dependency",
        ),
        (
            lambda raw: raw["messages"][0].update(
                posters=[{"name": "poster", "owner_message_id": 50}]
            ),
            "differs from selected message",
        ),
        (
            lambda raw: raw["messages"].append(
                {
                    "source_username": "fresh_source",
                    "message_id": 52,
                    "grouped_id": 900,
                    "events": [{"title": "Sibling"}],
                }
            )
            or raw["messages"][0].update(grouped_id=900),
            "depends on sibling messages",
        ),
    ],
)
def test_telegram_capture_fails_closed_on_cross_message_dependencies(mutate, match):
    raw = _telegram_input()
    mutate(raw)
    with pytest.raises(capture.CaptureContractError, match=match):
        _build_telegram(raw)


def test_telegram_capture_requires_matching_source_meta_and_rejects_credentials():
    raw = _telegram_input()
    raw["sources_meta"] = []
    with pytest.raises(capture.CaptureContractError, match="matching sources_meta"):
        _build_telegram(raw)

    raw = _telegram_input()
    raw["messages"][0]["access_token"] = "not-for-an-artifact"
    with pytest.raises(capture.CaptureContractError, match="credential-like"):
        _build_telegram(raw)


def test_vk_capture_omits_binary_with_hash_and_closed_metadata():
    binary = b"real-poster-binary"
    draft = EventDraft(
        title="Семейный день",
        date="2026-08-09",
        source_text="Дети и родители участвуют вместе.",
        poster_media=[
            PosterMedia(
                data=binary,
                name="poster.jpg",
                ocr_text="9 августа, дети и родители",
                total_tokens=25,
            )
        ],
    )
    artifact = capture.build_capture(
        adapter="vk",
        raw={
            "source_post_url": "https://vk.com/wall-1_2",
            "draft": draft,
            "photos": ["https://images.example/poster.jpg"],
        },
        repo_sha=REPO_SHA,
        captured_at=CAPTURED_AT,
    )

    poster = artifact["payload"]["draft"]["poster_media"][0]
    digest = hashlib.sha256(binary).hexdigest()
    assert poster["data_omitted"] is True
    assert poster["data_sha256"] == digest
    assert poster["data_byte_count"] == len(binary)
    assert poster["binary_was_available"] is True
    assert "data" not in poster
    assert artifact["sanitization"]["binary_omissions"] == [
        {
            "json_pointer": "/payload/draft/poster_media/0/data",
            "sha256": digest,
            "byte_count": len(binary),
            "was_available": True,
        }
    ]
    capture.validate_capture(artifact)


def test_parser_capture_keeps_one_theatre_event():
    event = TheatreEvent(
        title="Детский спектакль",
        date_raw="09.08.2026 12:00",
        ticket_status="available",
        url="https://dramteatr39.ru/fresh",
        source_type="dramteatr",
        parsed_date="2026-08-09",
        parsed_time="12:00",
    )
    artifact = capture.build_capture(
        adapter="parser",
        raw={"source": "dramteatr", "event": event},
        repo_sha=REPO_SHA,
        captured_at=CAPTURED_AT,
    )

    assert artifact["source_type"] == "parser:dramteatr"
    assert artifact["payload"]["event"]["title"] == "Детский спектакль"
    assert artifact["packet_count"] == 1
    capture.validate_capture(artifact)


def test_capture_schema_and_hash_tampering_fail():
    schema = json.loads(capture.SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    artifact = _build_telegram()
    artifact["payload"]["messages"][0]["text"] = "tampered"
    with pytest.raises(capture.CaptureContractError, match="payload_sha256"):
        capture.validate_capture(artifact)

    artifact = _build_telegram()
    artifact["packet_count"] = 2
    with pytest.raises(jsonschema.ValidationError):
        capture.validate_capture(artifact)


def test_writer_refuses_data_and_overwrite(tmp_path):
    artifact = _build_telegram()
    with pytest.raises(capture.CaptureContractError, match="under /data"):
        capture.write_capture(Path("/data/static-collection-capture.json"), artifact)

    output = tmp_path / "capture.json"
    capture.write_capture(output, artifact)
    original = output.read_bytes()
    with pytest.raises(capture.CaptureContractError, match="overwrite"):
        capture.write_capture(output, artifact)
    assert output.read_bytes() == original


@pytest.mark.asyncio
async def test_replay_narrowly_deserializes_captured_vk_poster(tmp_path, monkeypatch):
    binary = b"captured-binary"
    artifact = capture.build_capture(
        adapter="vk",
        raw={
            "source_post_url": "https://vk.com/wall-7_8",
            "draft": EventDraft(
                title="Свежая встреча",
                date="2026-08-10",
                source_text="Обычная взрослая встреча.",
                poster_media=[PosterMedia(data=binary, name="fresh.jpg", ocr_text="10 августа")],
            ),
            "photos": [],
        },
        repo_sha=REPO_SHA,
        captured_at=CAPTURED_AT,
    )
    fixture = tmp_path / "vk-capture.json"
    capture.write_capture(fixture, artifact)
    case = {
        "adapter": "vk",
        "fixture_path": str(fixture),
        "adapter_options": {},
        "expected": {
            "source_url": "https://vk.com/wall-7_8",
            "source_type": "vk",
        },
    }
    calls = []

    async def fake_persist(draft, photos, db, **kwargs):
        calls.append((draft, photos, db, kwargs))
        return {"event_id": 10}

    import vk_intake

    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    db = SimpleNamespace()
    await replay.invoke_adapter(case, db)

    replayed = calls[0][0].poster_media[0]
    assert replayed.data == b""
    assert replayed.digest == hashlib.sha256(binary).hexdigest()
    assert replayed.ocr_text == "10 августа"
    assert calls[0][3]["source_post_url"] == "https://vk.com/wall-7_8"


@pytest.mark.asyncio
async def test_replay_unwraps_captured_telegram_handler_envelope(tmp_path, monkeypatch):
    artifact = _build_telegram()
    fixture = tmp_path / "telegram-capture.json"
    capture.write_capture(fixture, artifact)
    calls = []

    async def fake_force(*_args, **kwargs):
        calls.append(("force", kwargs))

    async def fake_process(path, db, *, bot=None, **_kwargs):
        calls.append(("process", json.loads(Path(path).read_text(encoding="utf-8")), db, bot))
        return {"status": "ok"}

    import source_parsing.telegram.handlers as tg_handlers

    monkeypatch.setattr(replay, "_force_telegram_message", fake_force)
    monkeypatch.setattr(tg_handlers, "process_telegram_results", fake_process)
    db = SimpleNamespace()
    case = {
        "adapter": "telegram",
        "fixture_path": str(fixture),
        "adapter_options": {"source_username": "fresh_source", "message_id": 51},
        "expected": {
            "source_url": "https://t.me/fresh_source/51",
            "source_type": "telegram",
        },
    }
    await replay.invoke_adapter(case, db)

    assert calls[0] == (
        "force",
        {"username": "fresh_source", "message_id": 51},
    )
    assert calls[1][1] == artifact["payload"]
    assert calls[1][2] is db
    assert calls[1][3] is None


@pytest.mark.asyncio
async def test_replay_unwraps_captured_parser_theatre_event(tmp_path, monkeypatch):
    artifact = capture.build_capture(
        adapter="parser",
        raw={
            "source": "dramteatr",
            "event": TheatreEvent(
                title="Свежий спектакль",
                date_raw="11.08.2026 19:00",
                ticket_status="available",
                url="https://dramteatr39.ru/new-show",
                source_type="dramteatr",
                parsed_date="2026-08-11",
                parsed_time="19:00",
            ),
        },
        repo_sha=REPO_SHA,
        captured_at=CAPTURED_AT,
    )
    fixture = tmp_path / "parser-capture.json"
    capture.write_capture(fixture, artifact)
    calls = []

    async def fake_process(db, bot, events, **kwargs):
        calls.append((db, bot, events, kwargs))
        return SimpleNamespace(new_added=0), None

    import source_parsing.handlers as parser_handlers

    monkeypatch.setattr(parser_handlers, "process_source_events", fake_process)
    db = SimpleNamespace()
    case = {
        "adapter": "parser",
        "fixture_path": str(fixture),
        "adapter_options": {},
        "expected": {
            "source_url": "https://dramteatr39.ru/new-show",
            "source_type": "parser:dramteatr",
        },
    }
    await replay.invoke_adapter(case, db)

    assert calls[0][0] is db
    assert calls[0][1] is None
    assert calls[0][2][0].title == "Свежий спектакль"
    assert calls[0][3]["source"] == "dramteatr"


def test_capture_module_has_no_database_or_production_entrypoint_imports():
    source = SCRIPT.read_text(encoding="utf-8")
    assert "from db import" not in source
    assert "import db" not in source
    assert "import vk_intake" not in source
    assert "import source_parsing" not in source
