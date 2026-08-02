from __future__ import annotations

import contextlib
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from db import Database
from models import Event, EventSource


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_static_collection_ingestion_replay.py"
SPEC = importlib.util.spec_from_file_location("collection_ingestion_replay", SCRIPT)
assert SPEC and SPEC.loader
replay = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = replay
SPEC.loader.exec_module(replay)


def _manifest(fixture: Path, *, adapter: str = "telegram") -> dict:
    options = {"source_username": "real_source", "message_id": 44} if adapter == "telegram" else {}
    return {
        "schema_version": replay.MANIFEST_SCHEMA_VERSION,
        "run_id": "real-copy-replay-test",
        "cases": [
            {
                "case_id": f"{adapter}-one",
                "adapter": adapter,
                "fixture_path": str(fixture),
                "adapter_options": options,
                "expected": {
                    "event_id": 1,
                    "source_id": 1,
                    "source_url": "https://t.me/real_source/44",
                    "source_type": adapter if adapter != "parser" else "parser:dramteatr",
                    "first_collection_calls": 1,
                    "warm_collection_calls": 0,
                    "first_collection_write": True,
                    "warm_collection_write": False,
                },
            }
        ],
    }


def test_manifest_requires_reproducible_call_and_write_expectations(tmp_path):
    fixture = tmp_path / "telegram.json"
    fixture.write_text("{}", encoding="utf-8")
    path = tmp_path / "manifest.json"
    manifest = _manifest(fixture)

    cases = replay.validate_manifest(manifest, manifest_path=path)

    assert cases[0]["expected"]["first_collection_calls"] == 1
    assert Path(cases[0]["fixture_path"]) == fixture
    del manifest["cases"][0]["expected"]["first_collection_calls"]
    with pytest.raises(ValueError, match="first_collection_calls"):
        replay.validate_manifest(manifest, manifest_path=path)

    manifest = _manifest(fixture)
    manifest["run_id"] = ""
    with pytest.raises(ValueError, match="run_id"):
        replay.validate_manifest(manifest, manifest_path=path)


def test_adapter_result_redacts_source_material_urls_and_secrets():
    safe = replay._safe_adapter_result(
        {
            "status": "created",
            "event_id": 17,
            "source_url": "https://example.test/private-source",
            "source_text": "verbatim private source copy",
            "nested": {"description": "private description", "count": 3},
            "access_token": "never-report-this",
        }
    )

    assert safe["status"] == "created"
    assert safe["event_id"] == 17
    assert safe["nested"]["count"] == 3
    assert safe["source_url"]["redacted"] is True
    assert safe["source_text"]["redacted"] is True
    assert safe["nested"]["description"]["redacted"] is True
    assert safe["access_token"] == {"redacted": True}
    assert "private" not in json.dumps(safe)


@pytest.mark.asyncio
async def test_invoke_adapter_uses_the_three_production_entry_points(tmp_path, monkeypatch):
    db = SimpleNamespace()

    telegram_fixture = tmp_path / "telegram.json"
    telegram_fixture.write_text("{}", encoding="utf-8")
    telegram_calls = []

    async def fake_force(*_args, **kwargs):
        telegram_calls.append(("force", kwargs))

    async def fake_process(path, db_arg, *, bot=None, **_kwargs):
        telegram_calls.append(("process", path, db_arg, bot))
        return {"status": "ok"}

    import source_parsing.telegram.handlers as tg_handlers

    monkeypatch.setattr(replay, "_force_telegram_message", fake_force)
    monkeypatch.setattr(tg_handlers, "process_telegram_results", fake_process)
    telegram_case = replay.validate_manifest(
        _manifest(telegram_fixture), manifest_path=tmp_path / "manifest.json"
    )[0]
    await replay.invoke_adapter(telegram_case, db)
    assert [row[0] for row in telegram_calls] == ["force", "process"]
    assert telegram_calls[-1][1] == telegram_fixture

    vk_fixture = tmp_path / "vk.json"
    vk_fixture.write_text(
        json.dumps(
            {
                "source_post_url": "https://vk.com/wall-1_2",
                "draft": {
                    "title": "Реальное семейное событие",
                    "date": "2026-08-08",
                    "time": "12:00",
                    "source_text": "Приглашаем детей и взрослых.",
                },
                "photos": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    vk_calls = []

    async def fake_persist(draft, photos, db_arg, source_post_url=None, **kwargs):
        vk_calls.append((draft, photos, db_arg, source_post_url, kwargs))
        return {"event_id": 1}

    import vk_intake

    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    vk_manifest = _manifest(vk_fixture, adapter="vk")
    vk_manifest["cases"][0]["expected"].update(
        source_url="https://vk.com/wall-1_2", source_type="vk"
    )
    vk_case = replay.validate_manifest(
        vk_manifest, manifest_path=tmp_path / "manifest.json"
    )[0]
    await replay.invoke_adapter(vk_case, db)
    assert vk_calls[0][0].title == "Реальное семейное событие"
    assert vk_calls[0][3] == "https://vk.com/wall-1_2"
    assert vk_calls[0][4]["wait_for_telegraph_url"] is False

    payload = json.loads(vk_fixture.read_text(encoding="utf-8"))
    payload["draft"]["poster_media"] = []
    vk_fixture.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="poster_media"):
        await replay.invoke_adapter(vk_case, db)

    parser_fixture = tmp_path / "parser.json"
    parser_fixture.write_text(
        json.dumps(
            {
                "source": "dramteatr",
                "event": {
                    "title": "Детский спектакль",
                    "date_raw": "08.08.2026 12:00",
                    "ticket_status": "available",
                    "url": "https://dramteatr39.ru/real",
                    "source_type": "dramteatr",
                    "parsed_date": "2026-08-08",
                    "parsed_time": "12:00",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    parser_calls = []

    async def fake_parser_entry(db_arg, bot, events, **kwargs):
        parser_calls.append((db_arg, bot, events, kwargs))
        return SimpleNamespace(new_added=0), None

    import source_parsing.handlers as parser_handlers

    monkeypatch.setattr(parser_handlers, "process_source_events", fake_parser_entry)
    parser_manifest = _manifest(parser_fixture, adapter="parser")
    parser_manifest["cases"][0]["expected"].update(
        source_url="https://dramteatr39.ru/real", source_type="parser:dramteatr"
    )
    parser_case = replay.validate_manifest(
        parser_manifest, manifest_path=tmp_path / "manifest.json"
    )[0]
    await replay.invoke_adapter(parser_case, db)
    assert parser_calls[0][2][0].title == "Детский спектакль"
    assert parser_calls[0][3]["source"] == "dramteatr"


@pytest.mark.asyncio
async def test_run_manifest_records_first_write_and_identical_warm_noop(tmp_path, monkeypatch):
    fixture = tmp_path / "telegram.json"
    fixture.write_text('{"real_source_binding":true}', encoding="utf-8")
    manifest_value = _manifest(fixture)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_value), encoding="utf-8")
    cases = replay.validate_manifest(manifest_value, manifest_path=manifest_path)

    db_path = tmp_path / "copy.sqlite"
    db = Database(str(db_path))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            id=1,
            title="Семейная практика",
            description="Реальная source-bound запись",
            date="2026-08-08",
            time="12:00",
            location_name="Тестовая площадка",
            source_text="Взрослые и дети вместе выполнят общее задание.",
        )
        session.add(event)
        session.add(
            EventSource(
                id=1,
                event_id=1,
                source_type="telegram",
                source_url="https://t.me/real_source/44",
                source_text="Взрослые и дети вместе выполнят общее задание.",
                trust_level="medium",
            )
        )
        await session.commit()
    await db.close()

    calls = 0

    async def fake_invoke(_case, db_arg):
        nonlocal calls
        calls += 1
        import smart_event_update as collection_core

        if calls == 1:
            collection_core._SMART_UPDATE_LLM_TRACE.set(
                [
                    {
                        "kind": "gemma",
                        "label": "collection_candidate_adjudication",
                        "model": "gemma-4-31b-it",
                        "requested_model": "gemma-4-31b-it",
                        "actual_model": "models/gemma-4-31b-it",
                        "attempts": 1,
                        "physical_sends": 1,
                        "status": "ok_native",
                    }
                ]
            )
            quote = "Взрослые и дети вместе выполнят общее задание."
            provenance = {
                "source_id": 1,
                "source_url": "https://t.me/real_source/44",
                "source_type": "telegram",
                "source_trust": "medium",
                "input_hash": "a" * 64,
                "policy_version": replay.FACTS_POLICY_VERSION,
                "decided_at": "2026-08-02T12:00:00Z",
                "manual_lock": False,
            }
            facts = {
                "child_directed_decision": {
                    "value": "confirmed",
                    "confidence": 1.0,
                    "evidence_quote": quote,
                    "reason_code": "explicit_child_participants",
                    **provenance,
                },
                "family_suitable_decision": {
                    "value": "confirmed",
                    "confidence": 1.0,
                    "evidence_quote": quote,
                    "reason_code": "explicit_children_and_adults",
                    **provenance,
                },
                "joint_family_activity_decision": {
                    "value": "confirmed",
                    "confidence": 1.0,
                    "evidence_quote": quote,
                    "reason_code": "explicit_joint_task",
                    **provenance,
                },
            }
            receipt_payload = {
                "schema_version": "static-collection-adjudication-v2",
                **{
                    key: {
                        field: value
                        for field, value in row.items()
                        if field in {"value", "confidence", "evidence_quote", "reason_code"}
                    }
                    for key, row in facts.items()
                },
                "admission_decision": {
                    "value": "unknown",
                    "evidence_quote": "",
                    "reason_code": "insufficient_evidence",
                },
                "people_appearances": [],
            }
            decisions = {
                "schema_version": "static-collection-adjudication-v2",
                **facts,
                "evaluation_receipts": [
                    {
                        **provenance,
                        "schema_version": "static-collection-adjudication-v2",
                        "evaluated_at": provenance["decided_at"],
                        "payload": receipt_payload,
                    }
                ],
            }
            async with db_arg.get_session() as session:
                stored = await session.get(Event, 1)
                stored.collection_decisions = decisions
                session.add(stored)
                await session.commit()
        else:
            collection_core._SMART_UPDATE_LLM_TRACE.set([])
        return {"adapter": "telegram", "pass": calls}

    @contextlib.contextmanager
    def no_side_effect_context():
        yield

    monkeypatch.setattr(replay, "publication_side_effect_guard", no_side_effect_context)
    monkeypatch.setattr(replay, "invoke_adapter", fake_invoke)

    report = await replay.run_manifest(
        db_path=db_path,
        manifest_path=manifest_path,
        cases=cases,
    )

    assert report["status"] == "PASS"
    first, warm = report["cases"][0]["passes"]
    assert first["provider"]["collection_logical_calls"] == 1
    assert first["writes"]["collection_decisions"] == 1
    assert warm["provider"]["collection_logical_calls"] == 0
    assert warm["writes"]["collection_decisions"] == 0
    assert warm["writes"]["changed_event_source_ids"] == []
    assert warm["evidence"]["source_grounding_errors"] == []


def test_warm_receipt_fails_on_any_event_mutation():
    source = {
        "id": 1,
        "event_id": 1,
        "source_type": "telegram",
        "source_url": "https://t.me/real_source/44",
        "source_text": "Source-bound fact.",
    }
    before = {
        "quick_check": "ok",
        "event_count": 1,
        "event_source_count": 1,
        "logical_sha256": "a" * 64,
        "events": {"1": {"id": 1, "title": "Before", "collection_decisions": None}},
        "event_sources": {"1": source},
    }
    after = {
        **before,
        "logical_sha256": "b" * 64,
        "events": {"1": {"id": 1, "title": "After", "collection_decisions": None}},
    }
    case = {
        "expected": {
            "event_id": 1,
            "source_id": 1,
            "source_url": source["source_url"],
            "source_type": "telegram",
            "first_collection_calls": 0,
            "warm_collection_calls": 0,
            "first_collection_write": False,
            "warm_collection_write": False,
        }
    }

    receipt = replay._pass_receipt(
        case=case,
        pass_name="warm",
        result={"status": "skipped_nochange"},
        trace=[],
        before=before,
        after=after,
    )

    assert receipt["status"] == "FAIL"
    assert "warm_event_changed" in receipt["errors"]


def test_receipt_payload_quote_must_be_grounded_in_bound_source():
    payload = {
        key: {
            "value": "confirmed",
            "confidence": 1.0,
            "evidence_quote": "Invented quote",
            "reason_code": "explicit_joint_task",
        }
        for key in replay.FACT_KEYS
    }
    event = {
        "collection_decisions": {
            "evaluation_receipts": [
                {
                    "source_id": 7,
                    "source_url": "https://t.me/real_source/44",
                    "source_type": "telegram",
                    "input_hash": "a" * 64,
                    "policy_version": replay.FACTS_POLICY_VERSION,
                    "payload": payload,
                }
            ]
        }
    }
    source = {
        "id": 7,
        "source_url": "https://t.me/real_source/44",
        "source_type": "telegram",
        "source_text": "The persisted source says something else.",
    }

    evidence = replay._decision_evidence(event, source)

    assert evidence["matching_receipt_count"] == 1
    assert len(evidence["receipt_grounding_errors"]) == 3
    assert all("evidence_quote" in item for item in evidence["receipt_grounding_errors"])
