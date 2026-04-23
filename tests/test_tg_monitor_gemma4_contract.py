from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re


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
