from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re

import pytest


def test_tg_monitor_script_uses_google_ai_key3_and_gemma4() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "GoogleAIClient" in source
    assert "GOOGLE_API_KEY3" in source
    assert "GOOGLE_API_LOCALNAME3" in source
    assert "or GOOGLE_KEY_ENV" in source
    assert "or GOOGLE_ACCOUNT_ENV" in source
    assert "models/gemma-4-31b-it" in source
    assert "response_schema" in source
    assert "SupabaseLimiter" not in source
    assert "import google.generativeai as genai" not in source
    assert "genai.configure(" not in source
    assert "action=local_primary_limiter" in source
    assert "resolved = primary_ids" in source
    assert "primary_ids or fallback_ids" not in source
    assert "return list(_CANDIDATE_KEY_IDS)" in source
    assert "GOOGLE_AI_PROVIDER_TIMEOUT_SEC" in source
    assert "TG_MONITORING_LLM_TIMEOUT_SECONDS" in source
    assert "or '45'" in source


def test_tg_monitor_script_blocks_social_links_as_source_websites() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "_SOURCE_WEBSITE_BLOCK_RE" in source
    assert "instagram\\.com" in source
    assert "linktr\\.ee" in source
    assert "_is_disallowed_source_website_url" in source


def test_tg_monitor_extract_prompt_hardens_gemma4_ocr_merge_rules() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "Never return whitespace-only strings." in source
    assert "Use evidence from both message text and OCR." in source
    assert "Prefer filling location_name and location_address" in source
    assert "Do not invent end_date for single-date events." in source


def test_tg_monitor_extracts_official_bridge_lifting_notices() -> None:
    producer = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    consumer = Path("source_parsing/telegram/handlers.py").read_text(encoding="utf-8")
    sources = Path("docs/features/telegram-monitoring/sources.yml").read_text(encoding="utf-8")

    assert "Official city notices about развод мостов / разводка мостов ARE events" in producer
    assert "For @klgdcity bridge-lifting notices" in producer
    assert "narrow rescue extractor for official @klgdcity bridge-lifting notices" in producer
    assert "_extract_bridge_events_rescue" in producer
    assert "_bridge_event_fallback" in producer
    assert "username != 'klgdcity'" in producer
    assert "развест[и]\\s+мосты" in producer
    assert "source_username=username" in producer
    assert "source_title=(source_meta or {}).get('title')" in producer

    assert "username: klgdcity" in sources
    assert "bridge_notice_daily: true" in sources
    assert "развест[и]\\s+мосты" in consumer


def test_tg_monitor_bridge_fallback_covers_known_klgdcity_shapes() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    start = source.index("MONTHS_MAP = {")
    end = source.index("\n\nasync def extract_events", start)
    ns = {
        "date": date,
        "datetime": datetime,
        "re": re,
        "timedelta": timedelta,
        "timezone": timezone,
        "MAX_EVENTS_PER_MESSAGE": 8,
    }
    exec(source[start:end], ns)

    fallback = ns["_bridge_event_fallback"]
    output_is_usable = ns["_bridge_llm_output_is_usable"]

    assert [
        (item["date"], item["time"], item["title"])
        for item in fallback(
            "В ночь на 1 апреля планируется развести мосты «Юбилейный» и «Высокий». "
            "Будьте готовы, если планируете в промежутке с 23 до 05 часов посетить остров Октябрьский.",
            message_date="2027-03-31T06:50:37+00:00",
            source_username="klgdcity",
        )
    ] == [("2027-04-01", "23:00-05:00", "Развод мостов Юбилейный и Высокий")]

    assert [
        (item["date"], item["time"], item["title"])
        for item in fallback(
            "Сегодня в ночь, пока трамваи будут спать в депо, на острове разведут мосты "
            "“Юбилейный” и “Высокий”.",
            message_date="2026-05-10T06:43:02+00:00",
            source_username="@klgdcity",
        )
    ] == [("2026-05-10", "", "Развод мостов Юбилейный и Высокий")]

    assert [
        (item["date"], item["time"], item["title"])
        for item in fallback(
            "Сегодня в ночь на Острове анонсируется разводка мостов. "
            "Учитывайте это, если собираетесь в промежутке с 11 вечера до 5 утра ехать транзитом.",
            message_date="2026-05-11T12:13:08+00:00",
            source_username="klgdcity",
        )
    ] == [("2026-05-11", "23:00-05:00", "Развод мостов")]

    assert [
        (item["date"], item["time"], item["title"])
        for item in fallback(
            "Вот так мы и узнаём о разводке мостов сегодня в ночь и в ночь с 24 на 25 ноября.",
            message_date="2026-11-20T11:51:45+00:00",
            source_username="klgdcity",
        )
    ] == [
        ("2026-11-20", "", "Развод мостов"),
        ("2026-11-24", "", "Развод мостов"),
    ]

    assert output_is_usable(
        [{"title": "Развод мостов", "date": "2026-05-11", "time": "23:00-05:00"}],
        expected_count=1,
    )
    assert not output_is_usable(
        [{"title": "Развод мостов", "date": "2026-05-10", "time": "night"}],
        expected_count=1,
    )
    assert not output_is_usable([], expected_count=1)

def test_tg_monitor_extract_prompt_blocks_gemma4_known_leaks() -> None:
    """Regression guard against the leakage modes observed in run_id=48fa... artifacts.

    Gemma 4 was producing (a) title/city strings containing `// ...` meta-commentary,
    (b) English `event_type` tokens, (c) cities copied from parenthetical origin notes,
    (d) ghost rows with no title and no date, and (e) the literal string "unknown".
    """
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert 'Never output the literal string "unknown"' in source
    assert "Never include inline comments" in source
    assert "Do not emit placeholder events that have empty title and empty date" in source
    assert "parenthetical origin/collection notes" in source
    assert 'Never emit English event_type tokens like "exhibition"' in source
    assert "Fundraising-only posts" in source
    assert "Pure retrospective reports of completed events" in source


def test_tg_monitor_exhibition_fallback_shares_gemma4_hardening() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert 'Never output the literal "unknown" in any field.' in source
    assert "Do not emit placeholder events with empty title and empty date." in source
    assert 'never English tokens like "exhibition"' in source


def test_tg_monitor_json_fix_prompts_reject_meta_commentary() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    # All three fix prompts must forbid inline comments / meta-commentary / markdown.
    assert source.count("inline comments (//, #), meta-commentary, or markdown markers (**, __)") >= 3


def test_tg_monitor_guardrail_regexes_are_not_double_escaped() -> None:
    """The migration commit shipped `r"\\\\b..."` raw strings that never matched real text.

    This regression test pins the fix: the guardrail regexes must use proper
    Python raw-string metacharacters (``\\b``, ``\\s``, ``\\d``, ``\\w``).
    """
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert r'\\b(open\\s*call' not in source
    assert r'\\d{1,2}[./]\\d{1,2}' not in source
    assert r'r"\b(open\s*call|опен\s*колл|опенколл|конкурсн\w*\s+отбор' in source
    assert r'r"\b(сегодня|завтра|послезавтра)\b"' in source


def test_tg_monitor_event_schema_carries_gemma4_descriptions() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "Single lowercase Russian noun" in source
    assert "parenthetical origin/collection notes" in source
    assert "Human-readable event name. Never include inline comments" in source


def _load_sanitizer_in_isolation():
    """Extract the safety-net helpers straight from source without loading Kaggle-only deps.

    ``telegram_monitor.py`` performs ``load_config()`` at import time (reads
    ``/kaggle/input/config.json``), so a plain import chain cannot run locally.
    We parse the module, pull out just the sanitizer definitions, and execute them
    in a private namespace against the real ``re`` module.
    """
    import ast

    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wanted = {
        "_EVENT_STRING_FIELDS",
        "_UNKNOWN_LITERALS",
        "_LEAKED_COMMENT_TAIL_RE",
        "_MARKDOWN_STRIP_RE",
        "_clean_event_string_value",
        "_sanitize_extracted_events",
    }
    extracted: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = {
                t.id for t in node.targets if isinstance(t, ast.Name)
            }
            if targets & wanted:
                extracted.append(node)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id in wanted:
                extracted.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted:
            extracted.append(node)
    namespace: dict = {"re": re}
    exec(
        compile(ast.Module(body=extracted, type_ignores=[]), filename="<sanitizer>", mode="exec"),
        namespace,
    )
    if "_sanitize_extracted_events" not in namespace:
        pytest.fail("safety-net helper _sanitize_extracted_events missing from source")
    return namespace


def test_tg_monitor_sanitizer_drops_gemma4_ghost_rows_and_strips_leaks() -> None:
    ns = _load_sanitizer_in_isolation()
    sanitize = ns["_sanitize_extracted_events"]
    # Fixtures modeled directly on run_id=48fa... evidence.
    events = [
        {
            # Thought/comment leak into title — truncate at `(//`.
            "title": "Аудиопутешествие «Четверть длиннее восьмой» (день 1/2) (// a single event with multiple dates is usually split into multiple objects",
            "date": "2026-04-24",
            "time": "16:00",
            "location_name": "Барн, Каштановая аллея 1а, Калининград",
            "city": "Kaliningrad",
        },
        {
            # Ghost row — null title AND null date — must be dropped.
            "title": None,
            "date": None,
            "location_name": "Барн, Каштановая аллея 1а, Калининград",
            "city": "// a single event with 여러 dates dates is usually split",
        },
        {
            # "unknown" placeholders — must be normalized to "".
            "title": "Космос красного",
            "date": "2026-04-10",
            "location_name": "unknown",
            "location_address": "Unknown",
            "city": "unknown",
        },
        {
            # Markdown tail leak.
            "title": "Книга «Замок Нойхаузен» Ирины Белинцевой (продажа/покупка)**",
            "date": "2026-05-01",
            "location_name": "Замок Нойхаузен",
        },
        {
            # Well-formed event — must pass unchanged.
            "title": "Лекция Алексея Зыгмонта",
            "date": "2026-04-23",
            "time": "18:30",
            "location_name": "Дом китобоя, Мира 9, Калининград",
            "city": "Калининград",
            "event_type": "лекция",
            "ticket_link": "https://tickets.example.com/event?id=42#buy",
        },
    ]
    cleaned = sanitize(events)
    # Ghost row dropped: 5 -> 4.
    assert len(cleaned) == 4

    # Leak trimmed.
    assert cleaned[0]["title"] == "Аудиопутешествие «Четверть длиннее восьмой» (день 1/2)"

    # Placeholder literals normalized.
    assert cleaned[1]["title"] == "Космос красного"
    assert cleaned[1]["location_name"] == ""
    assert cleaned[1]["location_address"] == ""
    assert cleaned[1]["city"] == ""

    # Markdown trimmed.
    assert cleaned[2]["title"] == "Книга «Замок Нойхаузен» Ирины Белинцевой (продажа/покупка)"

    # Well-formed event survives untouched.
    assert cleaned[3]["title"] == "Лекция Алексея Зыгмонта"
    assert cleaned[3]["event_type"] == "лекция"
    assert cleaned[3]["ticket_link"] == "https://tickets.example.com/event?id=42#buy"


def test_tg_monitor_sanitizer_keeps_urls_when_stripping_comment_tails() -> None:
    ns = _load_sanitizer_in_isolation()
    clean_value = ns["_clean_event_string_value"]

    assert clean_value("https://example.com/tickets#buy") == "https://example.com/tickets#buy"
    assert clean_value("Билеты: https://example.com/tickets") == "Билеты: https://example.com/tickets"
    assert clean_value("Название (// leaked reasoning)") == "Название"
    assert clean_value("Название # leaked reasoning") == "Название"


def test_tg_monitor_eval_pack_tracks_real_gemma4_failure_families() -> None:
    pack = json.loads(
        Path("tests/fixtures/telegram_monitor_gemma4_eval_pack_2026_04_23.json").read_text(
            encoding="utf-8"
        )
    )

    assert pack["pack_id"] == "tg_monitor_gemma4_eval_2026_04_23"
    assert pack["source_run_id"] == "48fa98294333486d94dd0e14785d774f"
    assert pack["source_artifact"].endswith("tg-g4-kaggle-output-full-48fa/telegram_results.json")

    cases = pack["cases"]
    assert len(cases) == 10
    assert {case["case_id"] for case in cases} == {
        "TG-G4-EVAL-01",
        "TG-G4-EVAL-02",
        "TG-G4-EVAL-03",
        "TG-G4-EVAL-04",
        "TG-G4-EVAL-05",
        "TG-G4-EVAL-06",
        "TG-G4-EVAL-07",
        "TG-G4-EVAL-08",
        "TG-G4-EVAL-09",
        "TG-G4-EVAL-10",
    }

    tags = {tag for case in cases for tag in case["observed_problem_tags"]}
    for expected_tag in {
        "thought_leak",
        "ghost_row",
        "unknown_literal",
        "city_drift",
        "english_event_type",
        "retrospective_non_event",
        "positive_control",
    }:
        assert expected_tag in tags

    positive = next(case for case in cases if case["case_id"] == "TG-G4-EVAL-10")
    assert positive["kind"] == "positive"
    assert positive["observed_events"][0]["title"] == "Мир увлечений"
    assert positive["observed_events"][0]["event_type"] == "выставка"


def test_tg_monitor_guardrail_regexes_match_real_anchors_and_open_calls() -> None:
    """After the ``\\b`` double-escape fix the latent guardrails must fire on real text.

    ``test_tg_monitor_guardrail_regexes_are_not_double_escaped`` above pins the source
    shape; this one compiles the same patterns and confirms they actually match the kind
    of Telegram text that the ``extract_events`` guard is supposed to catch or exempt.
    """
    open_call_re = re.compile(
        r"\b(open\s*call|опен\s*колл|опенколл|конкурсн\w*\s+отбор|при[её]м\s+заявок|подать\s+заявк\w*|заявк\w*\s+принима\w*)\b",
        re.IGNORECASE | re.UNICODE,
    )
    anchor_re = re.compile(
        r"\b(сегодня|завтра|послезавтра)\b"
        r"|\b\d{1,2}[./]\d{1,2}(?:[./](?:19|20)\d{2})?\b"
        r"|\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
        re.IGNORECASE | re.UNICODE,
    )
    assert open_call_re.search("Приём заявок до 5 мая")
    assert open_call_re.search("Открыт конкурсный отбор")
    assert not open_call_re.search("Обычная афиша лекции")
    assert anchor_re.search("23 апреля в Доме китобоя")
    assert anchor_re.search("Сегодня в 18:00")
    assert anchor_re.search("12.04 в парке")
    assert not anchor_re.search("Текст без дат и анкоров")


def test_tg_monitor_runner_bootstraps_google_ai_bundle_for_kaggle_notebook() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "bootstrap_google_ai_bundle" in source
    assert "importlib.util.find_spec('google_ai')" in source
    assert "Path('/kaggle/input')" in source
    assert "tg_monitor.google_ai bootstrap" in source


def test_tg_monitor_service_stages_script_built_notebook_and_google_ai_bundle() -> None:
    source = Path("source_parsing/telegram/service.py").read_text(encoding="utf-8")

    assert "_embedded_google_ai_sources" in source
    assert "_build_notebook_payload_from_script" in source
    assert "_sync_notebook_entrypoint" in source
    assert "_stage_google_ai_bundle(prepared)" in source
    assert "_sync_notebook_entrypoint(prepared)" in source
    assert "_TG_EMBEDDED_GOOGLE_AI" in source
    assert "_TG_EMBEDDED_ROOT" in source
    assert "__file__ = str((_TG_NOTEBOOK_ROOT / 'telegram_monitor.py').resolve())" in source
    assert "_tg_run_main_sync" in source
    assert "nest_asyncio.apply(loop)" in source
    assert "loop.run_until_complete(main())" in source
    assert "key.startswith(\"GOOGLE_API_LOCALNAME\")" in source
    assert "Do not ship unrelated GOOGLE_API_KEY* values" in source
    assert "\"TG_MONITORING_GOOGLE_KEY_ENV\": google_key_env" in source
