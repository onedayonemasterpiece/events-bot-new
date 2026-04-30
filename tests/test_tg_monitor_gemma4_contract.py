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
    assert "location_name must be a venue/place name, not arbitrary nearby text" in source
    assert "never copy a descriptive sentence" in source
    assert "speaker biography, schedule commentary, film metadata" in source
    assert 'hall/room label such as "Кинозал:" or "Атриум:"' in source
    assert "use the host venue as location_name" in source
    assert "leave location_name empty rather than filling it with prose" in source
    assert "Do not invent end_date for single-date events." in source
    assert "Message date is only context for resolving explicit relative anchors" in source
    assert "return [] rather than using message_date as the event date" in source
    assert 'If a post says "в разделе X на выставке Y"' in source
    assert 'usually return ONE event object for that exhibition' in source
    assert "do NOT return [] only because some venue, city, or ticket fields remain unresolved" in source
    assert "still prefer one best-effort lecture row over [] so downstream OCR/date merge can complete it" in source
    assert 'Choose the final title silently.' in source
    assert "Title must be the attendee-facing event name, not a poster service heading." in source
    assert '"НАЧАЛО В ...", "БИЛЕТЫ", "РЕГИСТРАЦИЯ"' in source
    assert "keep the named event from message text as title and use OCR only to fill date/time/venue/ticket fields" in source
    assert 'caption "Второй Большой киноквиз!" plus' in source
    assert 'must return title "Второй Большой киноквиз", date "2026-04-24", time "19:00"' in source
    assert 'A museum-hosted lecture invitation remains an event even when the venue is only implicit' in source
    assert 'Use source context only as weak hosting context' in source
    assert "Institution work-hours notices are NOT events" in source
    assert "do NOT classify a post as a work-hours notice merely because it mentions a museum/library venue" in source
    assert 'a street/address such as "Музейная аллея"' in source
    assert "extract those events even when they happen at a museum or library" in source
    assert "prefer one ongoing exhibition card over [] or {}" in source
    assert "Do not split one real event into an extra title-only row" in source
    assert "keep the cycle/series label in raw_excerpt/search_digest, not as a second event row" in source
    assert 'Do not use generic placeholder venue names like "музей", "галерея", "пространство", or "площадка"' in source
    assert 'For museum posts spotlighting one artist or one body of work currently shown in the museum' in source


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


def test_tg_monitor_title_review_stage_keeps_caption_event_title_over_ocr_heading() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "TITLE_REVIEW_SCHEMA" in source
    assert "_repair_service_heading_titles" in source
    assert "_SERVICE_HEADING_TITLE_RE" in source
    assert 'choose replacement titles for suspicious poster-service-heading titles' in source
    assert 'A title made only of date/time/service text such as "НАЧАЛО В 19:00"' in source
    assert 'output the named attendee-facing event from the caption as title' in source
    assert "response_schema=TITLE_REVIEW_SCHEMA" in source
    assert "The event\n    title choice remains LLM-owned" in source


def test_tg_monitor_extract_prompt_blocks_gemma4_known_leaks() -> None:
    """Regression guard against the leakage modes observed in run_id=48fa... artifacts.

    Gemma 4 was producing (a) title/city strings containing `// ...` meta-commentary,
    (b) English `event_type` tokens, (c) cities copied from parenthetical origin notes
    and from speaker/author affiliation mentions, (d) ghost rows with no title and no
    date, (e) the literal string "unknown", and (f) empty `{}` objects as list items.
    """
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert 'Never output the literal string "unknown"' in source
    assert "Never include inline comments" in source
    assert "Do not emit placeholder events that have empty title and empty date" in source
    assert "Never emit empty JSON objects ({}) or venue-only rows" in source
    assert "parenthetical origin/collection note" in source
    assert "biographical/affiliation mention" in source
    assert "that venue city wins over every other city mention" in source
    assert 'Never include uncertainty markers like "or something similar"' in source
    assert 'Never emit English event_type tokens like "exhibition"' in source
    assert "Fundraising-only posts" in source
    assert "Pure retrospective reports of completed events" in source


def test_tg_monitor_general_extract_failure_falls_through_to_rescue_prompts() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "logger.warning('extract_events failed: %s', exc)\n            text = '[]'" in source
    assert "extract_events schedule rescue failed" in source
    assert "extract_events named exhibition rescue failed" in source


def test_tg_monitor_exhibition_fallback_shares_gemma4_hardening() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert 'Never output the literal "unknown" in any field.' in source
    assert "Do not emit placeholder events with empty title and empty date." in source
    assert "Never emit empty JSON objects ({}) or venue-only rows" in source
    assert 'never English tokens like "exhibition"' in source
    assert "biographical/affiliation mentions of curators, authors" in source
    assert 'Exception for ongoing named exhibitions' in source
    assert "More generally, for museum/exhibition posts about currently displayed works" in source
    assert 'This includes museum artist/work spotlight posts even when the word "выставка" is not repeated' in source
    assert 'For museum posts spotlighting one artist or one body of work currently shown in the museum' in source
    assert 'Do not return [] solely because the post is written as a museum editorial spotlight' in source
    assert 'Use source context only as weak museum-host context' in source
    assert 'leave location_name empty rather than inventing a generic placeholder like "музей"' in source


def test_tg_monitor_single_lecture_rescue_pass_is_llm_first() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "Extract a single attendable lecture/talk/meetup/excursion event" in source
    assert r'приглашаем\s+на\s+(?:лекци|встреч|экскурс|показ)' in source
    assert 'that is enough to keep one best-effort event row even if venue fields stay empty' in source
    assert "Prefer one row over [] for such a clearly invited single event." in source
    assert "Do not use message_date itself as the event date unless the text/OCR contains an explicit relative date anchor" in source
    assert "neither text nor OCR gives a date or relative date anchor" in source
    assert "_lacks_supported_non_exhibition_date" in source
    assert "Single lectures/talks/excursions need a supported date." in source
    assert "extract_events lecture rescue failed" in source


def test_tg_monitor_schedule_rescue_pass_is_llm_first() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "schedule_like = bool(" in source
    assert "if schedule_like:\n        text = '[]'" in source
    assert "Extract attendable schedule items from one small Telegram timetable chunk as strict JSON array." in source
    assert 'one date header like "18 АПРЕЛЯ" followed by up to three time lines' in source
    assert "range(0, len(timed_lines), 3)" in source
    assert "Each returned event must correspond to one real schedule line" in source
    assert 'Never use placeholder literals like "title" as a title' in source
    assert "location_name must be the shared venue/place for the timetable" in source
    assert "not descriptive prose from surrounding text" in source
    assert "Full message context for shared venue/address facts" in source
    assert 'a trailing "📍Остров Канта" line applies to all schedule rows' in source
    assert "schedule_blocks" in source
    assert "extract_events schedule rescue failed" in source


def test_tg_monitor_named_exhibition_rescue_pass_is_llm_first() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "Extract one named ongoing exhibition event from Telegram text as strict JSON array." in source
    assert 'If the post says "в разделе X на выставке Y", title must be the main exhibition Y' in source
    assert 'Do not require the post to restate the exhibition date range' in source
    assert 'Phrases like "на выставке Y можно увидеть ..." are sufficient evidence of a current display' in source
    assert 'set date to the Message date date part as an as-of merge date' in source
    assert 'set event_type exactly to "выставка"' in source
    assert "extract_events named exhibition rescue failed" in source


def test_tg_monitor_museum_spotlight_rescue_pass_is_llm_first() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "Extract a single ongoing museum exhibition/display card" in source
    assert 'For museum spotlight posts about one artist, artwork, or body of work, prefer one attendee-facing exhibition card' in source
    assert 'with event_type="выставка" and date=message_date as an "as-of" merge date rather than []' in source
    assert 'If you return an event in this rescue path, do not leave date or event_type empty' in source
    assert "Message date date part (YYYY-MM-DD)" in source
    assert "Repair a museum spotlight extraction as strict JSON array." in source
    assert 'with date set exactly to the Message date date part (YYYY-MM-DD)' in source
    assert 'A kept card with an empty date or empty event_type is invalid JSON for this task' in source
    assert "extract_events museum spotlight repair failed" in source
    assert 'If the full venue name is not stated, leave location_name empty rather than generic placeholders like "музей"' in source
    assert "extract_events museum spotlight rescue failed" in source


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
    assert "parenthetical origin/collection note" in source
    assert "biographical/affiliation mention of a speaker" in source
    assert "Human-readable event name. Never include inline comments" in source
    assert "Message date is context for resolving explicit relative anchors, not a default event date." in source
    assert "Never include uncertainty markers like \"or something similar\"" in source
    assert "return one attendee-facing lecture title, not two rows" in source
    assert 'Do not use generic placeholders like "музей", "галерея", "пространство", or "площадка"' in source
    assert "'required': [" in source


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
        "_HTML_TAG_RE",
        "_TRAILING_META_TAIL_RE",
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
            # Placeholder field-name literal — must be treated as missing title and dropped.
            "title": "title",
            "date": "2026-04-18",
            "time": "15:00",
            "location_name": "Калининградский зоопарк",
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
    # Ghost row and placeholder-title row dropped: 6 -> 4.
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
    assert clean_value("Название {// leaked reasoning") == "Название"


def test_tg_monitor_sanitizer_strips_html_tags_and_own_title_meta_leaks() -> None:
    """Regression guard for iter2 leak families observed in local-only Gemma 4 eval.

    Gemma 4 occasionally emits ``</strong>`` HTML tags or trailing ``own title:``
    meta-commentary into structured JSON string values. Those must not reach
    Smart Update / Telegraph.
    """
    ns = _load_sanitizer_in_isolation()
    clean_value = ns["_clean_event_string_value"]

    assert (
        clean_value("Аудиопутешествие «Четверть длиннее восьмой» (24 апреля)</strong> own title:")
        == "Аудиопутешествие «Четверть длиннее восьмой» (24 апреля)"
    )
    assert clean_value("<strong>Концерт</strong>") == "Концерт"
    assert clean_value("Лекция <br> own id:") == "Лекция"
    # Benign title with a colon must not be truncated by the meta-tail regex.
    assert clean_value("Книга: путь к мастерству") == "Книга: путь к мастерству"


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
