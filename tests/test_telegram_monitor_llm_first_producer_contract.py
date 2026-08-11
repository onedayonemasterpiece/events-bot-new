from __future__ import annotations

import ast
import asyncio
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace

import pytest


PRODUCER = Path("kaggle/TelegramMonitor/telegram_monitor.py")


def _safe_json(text: str):
    if not text:
        return None
    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw).strip()
        if raw.endswith("```"):
            raw = raw[:-3].strip()
    start = min([i for i in (raw.find("{"), raw.find("[")) if i != -1] or [-1])
    end = max(raw.rfind("}"), raw.rfind("]"))
    if start != -1 and end != -1 and end > start:
        raw = raw[start : end + 1]
    try:
        return json.loads(raw)
    except Exception:
        return None


def _producer_contract_namespace() -> dict:
    source = PRODUCER.read_text(encoding="utf-8")
    start = source.index("SOURCE_PARSE_DISPOSITIONS =")
    end = source.index("\n\n_VIDEO_ALLOWED_RISK_FLAGS", start)
    namespace = {
        "EVENT_ARRAY_SCHEMA": {"type": "array", "items": {"type": "object"}},
        "_string_schema": lambda description=None: {"type": "string"},
        "_safe_json": _safe_json,
        "_has_strong_event_invitation_signal": lambda text, ocr_text=None: bool(
            re.search(r"(?i)приглашаем|жд[её]м вас|билеты", "\n".join((text or "", ocr_text or "")))
        ),
        "hashlib": hashlib,
        "json": json,
        "re": re,
        "logger": SimpleNamespace(warning=lambda *args, **kwargs: None),
    }
    exec(compile(source[start:end], "<tg-producer-contract>", "exec"), namespace)
    tree = ast.parse(source)
    combine = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_combine_source_parse_decisions"
    )
    exec(
        compile(ast.Module(body=[combine], type_ignores=[]), "<tg-album-contract>", "exec"),
        namespace,
    )
    return namespace


def _provider_payload(
    disposition: str,
    *,
    events: list[dict] | None = None,
    lifecycle_actions: list[dict] | None = None,
    evidence_complete: bool = True,
) -> str:
    return json.dumps(
        {
            "disposition": disposition,
            "events": list(events or []),
            "lifecycle_actions": list(lifecycle_actions or []),
            "evidence_complete": evidence_complete,
            "parse_version": "source-parse-v1",
        },
        ensure_ascii=False,
    )


@pytest.mark.parametrize(
    "carrier",
    [
        "Собираемся у старого дуба. Подробности участия и место встречи сообщим в канале.",
        "Лекция о городской памяти в библиотеке. Следите за обновлениями.",
        "В 2024 году проект начинался с архивной выставки. Теперь ждём вас на новой программе.",
    ],
    ids=["no-keyword", "no-date", "past-hint"],
)
def test_telegram_producer_sends_no_keyword_no_date_and_past_hints_to_primary_llm(
    carrier: str,
) -> None:
    ns = _producer_contract_namespace()
    calls: list[dict] = []

    async def model_call(kind, prompt, **kwargs):
        calls.append(json.loads(prompt))
        return _provider_payload(
            "EVENTS_FOUND",
            events=[{"title": "Городская программа", "date": ""}],
        )

    result = asyncio.run(
        ns["extract_source_parse_decision"](
            carrier,
            [],
            message_date="2026-08-11T10:00:00+00:00",
            source_username="contract_source",
            _model_call=model_call,
        )
    )

    assert len(calls) == 1
    assert calls[0]["task"] == "parse_source"
    assert calls[0]["source_text"] == carrier
    assert result["disposition"] == "EVENTS_FOUND"
    assert [event["title"] for event in result["events"]] == ["Городская программа"]


def test_telegram_producer_forbids_confirmed_no_event_on_incomplete_media_evidence() -> None:
    ns = _producer_contract_namespace()
    calls = 0

    async def model_call(kind, prompt, **kwargs):
        nonlocal calls
        calls += 1
        return _provider_payload("CONFIRMED_NO_EVENT")

    result = asyncio.run(
        ns["extract_source_parse_decision"](
            "Короткая подпись к недоступной афише",
            [],
            attachment_count=1,
            unavailable_attachment_count=1,
            ocr_complete=False,
            _model_call=model_call,
        )
    )

    assert calls == 1
    assert result["disposition"] == "RETRY_REQUIRED"
    assert result["retry_reason"] == "EVIDENCE_INCOMPLETE"
    assert result["evidence_complete"] is False
    assert result["evidence_manifest"]["unavailable_attachment_count"] == 1


def test_telegram_manifest_builder_infers_missing_attachment_ocr() -> None:
    ns = _producer_contract_namespace()
    manifest = ns["_source_evidence_manifest"](
        "caption",
        ["one OCR block"],
        attachment_count=2,
    )
    assert manifest["attachment_count"] == 2
    assert manifest["unavailable_attachment_count"] == 1
    assert manifest["ocr_complete"] is False
    assert manifest["evidence_complete"] is False


def test_telegram_producer_keeps_all_events_and_lifecycle_actions_in_mixed_decision() -> None:
    ns = _producer_contract_namespace()
    response = _provider_payload(
        "MIXED",
        events=[
            {"title": "Лекция", "date": "2026-09-12"},
            {"title": "Экскурсия", "date": "2026-09-13"},
        ],
        lifecycle_actions=[
            {
                "action": "POSTPONE",
                "target_title": "Старый показ",
                "evidence": "Показ переносится",
            }
        ],
    )

    async def model_call(kind, prompt, **kwargs):
        return response

    result = asyncio.run(
        ns["extract_source_parse_decision"](
            "12 сентября — лекция. 13 сентября — экскурсия. Старый показ переносится.",
            [],
            _model_call=model_call,
        )
    )

    assert result["disposition"] == "MIXED"
    assert [event["title"] for event in result["events"]] == ["Лекция", "Экскурсия"]
    assert result["lifecycle_actions"][0]["action"] == "POSTPONE"
    assert result["parse_version"] == "source-parse-v1"
    assert result["evidence_manifest"]["evidence_complete"] is True


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        ("", "EMPTY_PROVIDER_RESPONSE"),
        ("{not-json}", "MALFORMED_JSON"),
        ('{"disposition":', "OUTPUT_TRUNCATED"),
        ("[]", "SCHEMA_MISMATCH"),
    ],
)
def test_telegram_producer_maps_empty_malformed_truncated_and_schema_failures_to_retry(
    response: str,
    reason: str,
) -> None:
    ns = _producer_contract_namespace()
    manifest = ns["_source_evidence_manifest"]("raw", [])

    result = ns["_parse_source_decision_response"](response, manifest)

    assert result["disposition"] == "RETRY_REQUIRED"
    assert result["retry_reason"] == reason
    assert result["events"] == []


def test_telegram_producer_maps_provider_exception_to_typed_retry() -> None:
    ns = _producer_contract_namespace()

    async def model_call(kind, prompt, **kwargs):
        raise TimeoutError("provider timed out")

    result = asyncio.run(
        ns["extract_source_parse_decision"](
            "Приглашаем на встречу",
            [],
            _model_call=model_call,
        )
    )

    assert result["disposition"] == "RETRY_REQUIRED"
    assert result["retry_reason"] == "TECHNICAL_ERROR"


def test_telegram_producer_does_not_accept_free_form_reject_authority() -> None:
    ns = _producer_contract_namespace()
    manifest = ns["_source_evidence_manifest"]("Афиша без словарного keyword", [])
    response = json.dumps(
        {
            "disposition": "REJECTED",
            "events": [],
            "lifecycle_actions": [],
            "evidence_complete": True,
            "parse_version": "source-parse-v1",
            "reject_reason": "past_or_low_confidence",
        }
    )

    result = ns["_parse_source_decision_response"](response, manifest)

    assert result["disposition"] == "RETRY_REQUIRED"
    assert result["retry_reason"] == "SCHEMA_MISMATCH"


@pytest.mark.parametrize("retry_reason", [None, "UNKNOWN_REASON"])
def test_telegram_producer_rejects_missing_or_unknown_retry_reason(retry_reason) -> None:
    ns = _producer_contract_namespace()
    manifest = ns["_source_evidence_manifest"]("raw", [], attachment_count=0)
    payload = {
        "disposition": "RETRY_REQUIRED",
        "events": [],
        "lifecycle_actions": [],
        "evidence_complete": False,
        "parse_version": "source-parse-v1",
    }
    if retry_reason is not None:
        payload["retry_reason"] = retry_reason
    result = ns["_parse_source_decision_response"](json.dumps(payload), manifest)
    assert result["disposition"] == "RETRY_REQUIRED"
    assert result["retry_reason"] == "SCHEMA_MISMATCH"


def test_telegram_producer_verifies_only_explicit_contradictions() -> None:
    ns = _producer_contract_namespace()
    responses = iter(
        [
            _provider_payload("CONFIRMED_NO_EVENT"),
            _provider_payload(
                "EVENTS_FOUND",
                events=[{"title": "Встреча", "date": "2026-08-15"}],
            ),
        ]
    )
    prompts: list[dict] = []

    async def model_call(kind, prompt, **kwargs):
        prompts.append(json.loads(prompt))
        return next(responses)

    result = asyncio.run(
        ns["extract_source_parse_decision"](
            "Ждём вас 15.08 в 18:00, билеты доступны по ссылке",
            [],
            _model_call=model_call,
        )
    )

    assert [item["task"] for item in prompts] == [
        "parse_source",
        "conditionally_verify_source_parse",
    ]
    assert prompts[1]["contradiction_facts"][0]["reason"] == "NO_EVENT_WITH_STRONG_SIGNALS"
    assert result["disposition"] == "EVENTS_FOUND"
    assert result["verification"]["performed"] is True


def test_telegram_album_combiner_balances_all_children_and_ocr_evidence() -> None:
    ns = _producer_contract_namespace()
    first_manifest = ns["_source_evidence_manifest"]("caption", ["poster one"])
    second_manifest = ns["_source_evidence_manifest"]("", ["poster two"])
    first = ns["_parse_source_decision_response"](
        _provider_payload(
            "EVENTS_FOUND",
            events=[{"title": "Лекция", "date": "2026-08-20"}],
        ),
        first_manifest,
    )
    second = ns["_parse_source_decision_response"](
        _provider_payload(
            "LIFECYCLE_ONLY",
            lifecycle_actions=[{"action": "CANCEL", "target_title": "Показ"}],
        ),
        second_manifest,
    )

    combined = ns["_combine_source_parse_decisions"](
        [first, second],
        raw_text_blocks=["caption"],
        ocr_blocks=["poster one", "poster two"],
    )

    assert combined["disposition"] == "MIXED"
    assert [item["title"] for item in combined["events"]] == ["Лекция"]
    assert [item["action"] for item in combined["lifecycle_actions"]] == ["CANCEL"]
    assert combined["evidence_manifest"]["ocr_blocks_available"] == 2
    assert combined["evidence_manifest"]["ocr_blocks_included"] == 2


def test_telegram_album_combiner_missing_manifest_cannot_terminally_confirm_no_event() -> None:
    ns = _producer_contract_namespace()
    combined = ns["_combine_source_parse_decisions"](
        [
            {
                "disposition": "CONFIRMED_NO_EVENT",
                "events": [],
                "lifecycle_actions": [],
                "evidence_complete": True,
                "parse_version": "source-parse-v1",
            }
        ],
        raw_text_blocks=["caption"],
        ocr_blocks=[],
    )
    assert combined["disposition"] == "RETRY_REQUIRED"
    assert combined["retry_reason"] == "SCHEMA_MISMATCH"
    assert combined["evidence_complete"] is False


def test_telegram_primary_call_receives_every_multicard_ocr_block_once() -> None:
    ns = _producer_contract_namespace()
    prompts: list[dict] = []

    async def model_call(kind, prompt, **kwargs):
        prompts.append(json.loads(prompt))
        return _provider_payload(
            "EVENTS_FOUND",
            events=[
                {"title": "Лекция", "date": "2026-08-20"},
                {"title": "Экскурсия", "date": "2026-08-21"},
            ],
        )

    result = asyncio.run(
        ns["extract_source_parse_decision"](
            "Программа на двух карточках",
            ["20 августа — лекция", "21 августа — экскурсия"],
            attachment_count=2,
            _model_call=model_call,
        )
    )

    assert len(prompts) == 1
    assert prompts[0]["ocr_blocks"] == [
        "20 августа — лекция",
        "21 августа — экскурсия",
    ]
    assert result["evidence_manifest"]["ocr_blocks_available"] == 2
    assert result["evidence_manifest"]["ocr_blocks_included"] == 2
    assert len(result["events"]) == 2


def test_telegram_scan_has_no_free_form_or_regex_terminal_skip_authority() -> None:
    source = PRODUCER.read_text(encoding="utf-8")
    tree = ast.parse(source)
    scan = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "scan_source"
    )
    scan_source = ast.unparse(scan)

    assert "await extract_source_parse_decision" in scan_source
    assert "await extract_events" not in scan_source
    assert "strip_promo_lines(text_for_links)" not in scan_source
    assert "message.skip reason=promo_or_congrats" not in scan_source
    assert "cleaned_events = cleaned_events[:MAX_EVENTS_PER_MESSAGE]" not in scan_source
    assert "source_parse_decision" in scan_source
    assert "reject_reason" not in scan_source
    assert "source_parse_pending = bool(grouped_id)" in scan_source
    assert "messages_out = _merge_media_groups(messages_out)" in scan_source
    assert "album_ocr_blocks" in scan_source
