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
    assert "telegram\\.me" in source


def test_tg_monitor_script_canonicalizes_telegram_me_linked_posts() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "(?:t\\.me|telegram\\.me)/[^/\\s]+/\\d+" in source
    assert "'https://t.me/'" in source


def test_strip_custom_emoji_preserves_meaningful_unicode_fallback() -> None:
    """Regression for INC-2026-05-11-zoo-lecture (event 4798):

    A channel post used 4 premium custom-emoji glyphs with `\U0001F193` as the
    Unicode fallback to indicate free attendance. The previous
    `strip_custom_emoji_entities` replaced the entire entity range with
    spaces, deleting the free-attendance signal so `is_free=False` for the
    downstream event. The new behaviour must keep the Unicode fallback when
    it falls into a real pictograph Unicode block.
    """
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    assert "_custom_emoji_fallback_is_meaningful" in source
    # The new helper must check the actual pictograph Unicode blocks.
    assert "0x1F300" in source and "0x1FAFF" in source
    assert "0x1F100" in source and "0x1F1FF" in source
    assert "0x2600" in source and "0x27BF" in source
    # The strip function must consult the helper and only replace with spaces
    # when the fallback is NOT meaningful.
    assert "if _custom_emoji_fallback_is_meaningful" in source
    # The space replacement must now sit under the `else:` branch of the
    # meaningful-fallback check, not as an unconditional next statement.
    branch = source[source.index("if _custom_emoji_fallback_is_meaningful"):]
    branch = branch[: branch.index("last = max(last, end)")]
    assert "out.append(span_text)" in branch
    assert "else:" in branch
    assert "out.append(' ' * max(0, end - start))" in branch


def test_strip_custom_emoji_behaviour_via_exec() -> None:
    """Lightweight functional check: extract the strip helper pair from the
    Kaggle source and exec them in an isolated namespace with thin stubs for
    Telethon's surrogate helpers and `MessageEntityCustomEmoji`. Verifies that
    a meaningful Unicode fallback is preserved while a PUA fallback is still
    stripped to a space.
    """
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    # Pull out exactly the two function definitions.
    start = source.index("def _custom_emoji_fallback_is_meaningful")
    end = source.index("def is_ticket_giveaway")
    block = source[start:end]

    ns: dict = {}
    # Thin stubs that match the behaviour we rely on (UTF-16 surrogate roundtrip).
    def add_surrogate(s: str) -> str:
        return s.encode("utf-16-le").decode("utf-16-le", errors="ignore")

    def del_surrogate(s: str) -> str:
        return s

    class _Entity:
        def __init__(self, offset: int, length: int) -> None:
            self.offset = offset
            self.length = length

    ns["add_surrogate"] = add_surrogate
    ns["del_surrogate"] = del_surrogate
    ns["MessageEntityCustomEmoji"] = _Entity
    exec(compile(block, "<strip>", "exec"), ns)

    strip_fn = ns["strip_custom_emoji_entities"]

    # Single 🆓 (free) at offset 0, length 2 in UTF-16 (one surrogate pair).
    free_text = "\U0001F193, по регистрации"
    ent_free = _Entity(0, 2)
    assert strip_fn(free_text, [ent_free]) == free_text  # preserved

    # PUA fallback at offset 0, length 1.
    pua_text = " регистрация"
    ent_pua = _Entity(0, 1)
    out = strip_fn(pua_text, [ent_pua])
    assert out == " " + pua_text[1:], out  # replaced with space


def test_tg_monitor_extract_prompt_hardens_gemma4_ocr_merge_rules() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "Never return whitespace-only strings." in source
    assert "Use evidence from both message text and OCR." in source
    assert "If a named activity explicitly says its start time is being clarified" in source
    assert "Do not copy the enclosing festival, fair, venue, or full-program hours into it." in source
    assert "Prefer filling location_name and location_address" in source
    assert 'Never output literal field-name placeholders like "location_address", "address", "location_name"' in source
    assert "location_name must be a venue/place name, not arbitrary nearby text" in source
    assert 'Never use temporal/date fragments such as "Завтра", "Сегодня", "в пятницу", or "14 июня" as location_name, including emoji/bullet-prefixed forms like "🤗Завтра"' in source
    assert "never copy a descriptive sentence" in source
    assert "discussion-topic line such as" in source
    assert "Never split one prose/list sentence across location_name and location_address" in source
    assert "For online-only livestreams, use an explicit platform/page as location_name" in source
    assert "each event must use venue/address/city facts" in source
    assert "the event-local venue wins" in source
    assert "canonical attendee-facing title rather than the in-character/plot phrase" in source
    assert "speaker biography, schedule commentary" in source
    assert "film metadata" in source
    assert 'hall/room label such as "Кинозал:" or "Атриум:"' in source
    assert "use the host venue as location_name" in source
    assert "leave location_name empty rather than filling it with prose" in source
    assert "Do not invent end_date for single-date events." in source
    assert "teaser or pre-announcement without an exact day/date range/end_date" in source
    assert "do not use the message date or the first day of the mentioned month" in source
    assert "Message date is only context for resolving explicit relative anchors" in source
    assert "return [] rather than using message_date as the event date" in source
    assert 'If a post says "в разделе X на выставке Y"' in source
    assert 'usually return ONE event object for that exhibition' in source
    assert "do NOT return [] only because some venue, city, or ticket fields remain unresolved" in source
    assert "still prefer one best-effort lecture row over [] so downstream OCR/date merge can complete it" in source
    assert 'Choose the final title silently.' in source
    assert "Title must be the attendee-facing event name, not a poster service heading." in source
    assert 'Digest/section labels such as "неделя в театре", "афиша", "репертуар", or "анонс"' in source
    assert 'A compact line like "17.05 | GROZA" means date 17 May and title "GROZA"; never convert "17.05" into time "17:05".' in source
    assert 'Russian numeric dates are always day.month: "10.05" means 10 May, not September 10' in source
    assert '"#13_июня", and "#21_июня" are authoritative' in source
    assert 'Never use nearby address/venue numbers, gates/floors ("гейт 2.6", "2 этаж")' in source
    assert 'Retrospective wording like "17 июня ... прошла лекция"' in source
    assert 'that event-local evidence also wins over the source default' in source
    assert "performance/show/concert/play/film screening with exact future date, start time" in source
    assert '"НАЧАЛО В ...", "БИЛЕТЫ", "РЕГИСТРАЦИЯ"' in source
    assert "keep the named event from message text as title and use OCR only to fill date/time/venue/ticket fields" in source
    assert 'caption "Второй Большой киноквиз!" plus' in source
    assert 'must return title "Второй Большой киноквиз", date "2026-04-24", time "19:00"' in source
    assert 'A museum-hosted lecture invitation remains an event even when the venue is only implicit' in source
    assert 'Use source context only as weak hosting context' in source
    assert 'transfer/reschedule ("перенос", "перенесена") of a future lecture/talk' in source
    assert 'giveaway results, winners, repost mechanics, or congratulatory/promo framing' in source
    assert 'For festival/promo campaign source posts such as @kraftmarket39' in source
    assert 'promo/giveaway-result wrapper repeats a future event date/time' in source
    assert "Festival/campaign anchor contract" in source
    assert 'fill festival with the exact campaign-covering festival name: "Кантата" or "80 историй о главном"' in source
    assert "this is required for downstream promo campaigns" in source
    assert "Institution work-hours notices are NOT events" in source
    assert "do NOT classify a post as a work-hours notice merely because it mentions a museum/library venue" in source
    assert 'a street/address such as "Музейная аллея"' in source
    assert "extract those events even when they happen at a museum or library" in source
    assert "Do not use historical/background dates from exhibit text" in source
    assert '"9 октября 1947 года..." inside an exhibition narrative is historical content' in source
    assert "return [] unless it also gives an explicit future opening" in source
    assert "Ticket/free contract: is_free=true ONLY when the source or OCR explicitly says attendance is free" in source
    assert "Missing price is unknown, not free." in source
    assert "Do not mark zoo/museum/theatre events free merely because" in source
    assert 'Return raw JSON only: the first character must be "[" and the last character must be "]"' in source
    assert "do not wrap the array in markdown/code fences" in source
    assert "prefer one ongoing exhibition card over [] or {}" in source
    assert "Do not split one real event into an extra title-only row" in source
    assert "keep the cycle/series label in raw_excerpt/search_digest, not as a second event row" in source
    assert 'Do not use generic placeholder venue names like "музей", "галерея", "пространство", or "площадка"' in source
    assert 'For museum posts spotlighting one artist or one body of work currently shown in the museum' in source
    assert "_repair_suspicious_locations" in source
    assert "Review extracted Telegram events and repair only the venue fields" in source
    assert "source default location as evidence" in source
    assert "The deterministic part only decides whether the extracted venue field has a" in source
    assert "source_default_location=default_location" in source
    assert "Source default location:" in source
    assert "source default location is provided, treat it as a strong prior" in source
    assert "_event_needs_location_grounding_review" in source
    assert "_LOCATION_REVIEW_VENUE_CUE_RE" in source
    assert "not grounded in the source text/OCR/source context" in source
    assert "дворец спорта Янтарный" in source
    assert "use only the venue nearest the event line" in source
    assert "source_context_line=source_context_line" in source
    assert "_looks_like_clear_single_event_invitation" in source
    assert "single-event rescue failed" in source
    assert "A curator/speaker/artist/person name" in source
    assert "_location_review_looks_like_person_name" in source
    assert "_LOCATION_REVIEW_TEMPORAL_LOCATION_RE" in source
    assert "_strip_location_review_temporal_decoration" in source
    assert "_LOCATION_REVIEW_CITY_INFLECTED_PREFIX_RE" in source
    assert "city must be the place name itself, not an inflected phrase" in source
    assert "_correct_single_event_from_source_datetime" in source
    assert "poster_only_ocr = (not caption_content) and bool(ocr_only_content)" in source
    assert "content = caption_content or ocr_only_content" in source
    assert "OCR-only poster text:" in source
    assert "do not return [] merely because caption text is empty" in source


def test_tg_monitor_source_datetime_guard_corrects_single_event_drift() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    start = source.index("MONTHS_MAP = {")
    end = source.index("\n\nasync def extract_events", start)
    ns = {
        "date": date,
        "datetime": datetime,
        "re": re,
        "timedelta": timedelta,
        "timezone": timezone,
        "logger": type("_L", (), {"info": lambda *a, **k: None})(),
    }
    exec(source[start:end], ns)

    correct = ns["_correct_single_event_from_source_datetime"]

    assert correct(
        [{"title": "Дегустация сыров и вин", "date": "2026-06-26", "time": "19:00"}],
        message_text="Встречаемся 26 июля , в 19:00, в Виниссимо на пр. Мира, 23",
        ocr_text=None,
        message_date="2026-06-23T12:00:00+00:00",
        source_username="terkatalk",
    )[0]["date"] == "2026-07-26"

    yoga = correct(
        [{"title": "Международный день йоги", "date": "2026-07-02", "time": "10:00"}],
        message_text="🗓 #21_июня с 10.00-13.00\n📍 Ростех арена, ул. Триумфальная аллея, гейт 2.6",
        ocr_text=None,
        message_date="2026-06-19T12:00:00+00:00",
        source_username="kulturnaya_chaika",
    )[0]
    assert yoga["date"] == "2026-06-21"
    assert yoga["time"] == "10:00-13:00"

    past = correct(
        [{"title": "Run sos run!", "date": "2026-09-10", "time": "14:00"}],
        message_text="10.05 | Run sos run!\n14:00 разминка\n📍Плохой охотник",
        ocr_text=None,
        message_date="2026-05-10T09:00:00+00:00",
        source_username="meowafisha",
    )[0]
    assert past["date"] == "2026-05-10"
    assert "repertoire/program items, musical work titles" in source
    assert "catalogue number such as \"соч. 16\"" in source
    assert 'temporal/date fragments (including emoji-prefixed values like "🤗Завтра")' in source


def test_tg_monitor_ocr_date_ignores_vinyl_speed_metadata() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    start = source.index("MONTHS_MAP = {")
    end = source.index("\n\nasync def extract_events", start)
    ns = {
        "date": date,
        "datetime": datetime,
        "re": re,
        "timedelta": timedelta,
        "timezone": timezone,
        "logger": type("_L", (), {"info": lambda *a, **k: None})(),
    }
    exec(source[start:end], ns)

    extract_ocr_datetime = ns["_extract_ocr_datetime"]

    assert extract_ocr_datetime(
        "Blues & Roots Lp 33 1/3 RPM Charlie Mingus",
        "2026-06-15T08:00:43+00:00",
    ) == (None, None)
    assert extract_ocr_datetime("Концерт 10.05 начало 14:00", "2026-05-01T09:00:00+00:00") == (
        "2026-05-10",
        "14:00",
    )
    assert "Record/vinyl metadata such as \"LP 33 1/3 RPM\"" in source



def test_tg_monitor_location_review_triggers_on_emoji_prefixed_temporal_location() -> None:
    ns = _load_location_review_helpers_in_isolation()
    needs_review = ns["_needs_llm_location_review"]

    events = [
        {
            "title": "Мультсреда в музее",
            "date": "2026-06-17",
            "time": "15:00-17:00",
            "location_name": "🤗Завтра",
            "location_address": "",
        }
    ]

    assert needs_review(
        events,
        source_default_location="Музей Изобразительных искусств, Ленинский проспект 83, Калининград",
        message_text="🤗Завтра, 17 июня, у нас Мультсреда в музее.",
        source_context_line="Source default location: Музей Изобразительных искусств, Ленинский проспект 83, Калининград",
    ) is True

def test_tg_monitor_location_review_triggers_on_program_item_as_location() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    start = source.index("_LOCATION_REVIEW_TIME_RANGE_RE = re.compile")
    end = source.index("\n\nasync def extract_events", start)
    ns = {"re": re, "json": json}
    exec(source[start:end], ns)

    needs_review = ns["_needs_llm_location_review"]

    source_text = (
        "19 июня в 20:00 в атриуме музея прозвучит первый концерт нового сезона Pianissimo.\n"
        "В программе вечера — шедевры фортепианной музыки:\n"
        "🎵 С. В. Рахманинов – Музыкальные моменты, соч. 16"
    )
    events = [
        {
            "title": "Первый концерт нового сезона Pianissimo",
            "date": "2026-06-19",
            "time": "20:00",
            "location_name": "🎵 С. В. Рахманинов – Музыкальные моменты",
            "location_address": "соч. 16",
            "city": "Калининград",
        }
    ]

    assert needs_review(events, message_text=source_text) is True


def test_tg_monitor_location_review_triggers_on_non_location_bullet_fragment() -> None:
    ns = _load_location_review_helpers_in_isolation()
    needs_review = ns["_needs_llm_location_review"]

    assert needs_review(
        [
            {
                "title": "Ревущий лев, поющий лось",
                "date": "2026-06-18",
                "time": "",
                "end_date": "2026-06-28",
                "location_name": "📩 Зоосад с первого года (напомним",
                "city": "Калининград",
            }
        ],
        source_default_location="Музей Изобразительных искусств, Ленинский проспект 83, Калининград",
        message_text=(
            "📩 Зоосад с первого года (напомним, Кёнигсбергский зоосад открылся "
            "21 мая 1896 года) издавал открытки. Выставка работает до 28 июня."
        ),
        ocr_text="",
        source_context_line="source_username=kaliningradartmuseum",
    )


def test_tg_monitor_location_review_triggers_on_topic_sentence_split_location() -> None:
    ns = _load_location_review_helpers_in_isolation()
    needs_review = ns["_needs_llm_location_review"]

    assert needs_review(
        [
            {
                "title": "Прямой эфир с министром по культуре и туризму Калининградской области Андреем Ермаком",
                "date": "2026-06-19",
                "time": "14:30",
                "location_name": "о концертах",
                "location_address": "организованных в честь 80-летия Калининградской области;",
                "city": "Калининград",
            }
        ],
        message_text=(
            "Андрей Викторович расскажет:\n\n"
            "- о концертах, организованных в честь 80-летия Калининградской области;\n"
            "- об итогах фестиваля классической музыки «Кантата»."
        ),
        ocr_text=(
            "Официальная страница правительства Калининградской области "
            "Вконтакте и в Одноклассниках"
        ),
        source_context_line="source_username=minkultturism_39",
    )


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


def test_tg_monitor_title_review_detects_ocr_only_title_conflict() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    start = source.index("_TITLE_REVIEW_STOPWORDS:")
    end = source.index("\n\nasync def _repair_service_heading_titles", start)
    ns = {"re": re}
    exec(source[start:end], ns)

    needs_review = ns["_title_needs_caption_ocr_review"]
    caption = (
        "🎷31.07-02.08 | Калининград Сити Джаз\n"
        "Главный летний фестиваль Калининграда. Публикуем подробную программу."
    )
    noisy_ocr = (
        "КАЛИНИНГРАД СИТИ ДА БИСТРО ЯНТАРЬ\n"
        "КАЛИНИНГРАД СИТИ ДЖАЗ '26 31.07-02.08 ЦЕНТРАЛЬНЫЙ ПАРК"
    )

    assert needs_review(
        "КАЛИНИНГРАД СИТИ ДА БИСТРО ЯНТАРЬ",
        message_text=caption,
        ocr_text=noisy_ocr,
    )
    assert not needs_review(
        "Калининград Сити Джаз",
        message_text=caption,
        ocr_text=noisy_ocr,
    )
    assert not needs_review(
        "Название только на афише",
        message_text="Скоро расскажем подробнее",
        ocr_text="Название только на афише",
    )


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
    assert 'only says "следующий фестиваль" with dates but "локация/место/адрес уточняется"' in source
    assert "Operational updates for people already attending an event" in source
    assert '"важная информация для гостей/зрителей", entry route, navigation, parking, queue, cloakroom' in source


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
    assert "festival_program_like = bool(" in source
    assert "SCHEDULE_SCREEN_SCHEMA" in source
    assert "institution_hours_or_ticket_terms" in source
    assert "This is a semantic routing decision, not event extraction" in source
    assert "ticket_valid_until" in source
    assert "extract_events schedule screen failed; fail closed" in source
    assert "schedule_screen_decision == 'event_timetable'" in source
    assert "If a festival post lists several dated program items" in source
    assert "Extract attendable schedule items from one small Telegram timetable chunk as strict JSON array." in source
    assert 'one date header like "18 АПРЕЛЯ" followed by up to three time lines' in source
    assert "range(0, len(timed_lines), 3)" in source
    assert "Each returned event must correspond to one real schedule line" in source
    assert "If the chunk/full message is only an institution work-hours or holiday-opening notice" in source
    assert "return [] and do not convert those days/hours into events" in source
    assert 'Never use placeholder literals like "title" as a title' in source
    assert "Ticket/free contract: is_free=true ONLY when the source or OCR explicitly says attendance is free" in source
    assert "Ticket links, ticket sale/status, paid registration, or venue" in source
    assert "location_name must be the shared venue/place for the timetable" in source
    assert 'Never output field-name placeholders like "location_address", "address", "location_name"' in source
    assert "that event-local venue wins over source context or defaults" in source
    assert "not descriptive prose from surrounding text" in source
    assert "Full message context for shared venue/address facts" in source
    assert 'a trailing "📍Остров Канта" line applies to all schedule rows' in source
    assert "schedule_blocks" in source
    assert "extract_events schedule rescue failed" in source


def test_tg_monitor_ticket_validity_work_hours_incident_contract() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    replay = json.loads(
        Path(
            "tests/replays/INC-2026-07-10-zoo-ticket-validity-non-event/source.json"
        ).read_text(encoding="utf-8")
    )

    assert 'open and working normally / открыт и работает в обычном режиме' in source
    assert 'Visitor, venue, museum, zoo, library, park, or cash-desk hours are work_hours' in source
    assert 'A date in "ticket valid until / билет действителен до" is ticket_valid_until' in source
    assert "schedule rescue skipped: no genuine date-header blocks" in source
    assert "schedule_blocks = [content[:1800]]" not in source
    assert "A ticket-validity/expiry date is not an event date" in source
    assert "visitor/cash-desk hours are not event start times" in source
    assert replay["source_url"] == "https://t.me/kldzoo/7641"
    assert replay["expected"]["telegram_monitor_events"] == []
    assert "Входной билет действителен до 31 декабря 2026 года" in replay["text"]


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
    assert "evidence that the event is free" in source
    assert "True only when the source explicitly states free attendance" in source
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
        "_FIELD_NAME_PLACEHOLDER_LITERALS",
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


def _load_location_review_helpers_in_isolation():
    import ast

    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wanted = {
        "_LOCATION_REVIEW_TIME_RANGE_RE",
        "_LOCATION_REVIEW_DATE_RE",
        "_LOCATION_REVIEW_TEMPORAL_LOCATION_RE",
        "_strip_location_review_temporal_decoration",
        "_LOCATION_REVIEW_CITY_INFLECTED_PREFIX_RE",
        "_LOCATION_REVIEW_ADDRESS_HINT_RE",
        "_LOCATION_REVIEW_VENUE_CUE_RE",
        "_LOCATION_REVIEW_GENERIC_ROOM_RE",
        "_LOCATION_REVIEW_NON_VENUE_BULLET_RE",
        "_LOCATION_REVIEW_TOPIC_FRAGMENT_RE",
        "_LOCATION_REVIEW_PROGRAM_ITEM_RE",
        "_LOCATION_REVIEW_CATALOGUE_ADDRESS_RE",
        "_location_review_looks_like_person_name",
        "_location_review_norm",
        "_location_review_value_grounded",
        "_event_needs_location_grounding_review",
        "_needs_llm_location_review",
    }
    extracted: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if targets & wanted:
                extracted.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted:
            extracted.append(node)
    namespace: dict = {"re": re}
    exec(
        compile(ast.Module(body=extracted, type_ignores=[]), filename="<location-review>", mode="exec"),
        namespace,
    )
    if "_needs_llm_location_review" not in namespace:
        pytest.fail("location-review helper _needs_llm_location_review missing from source")
    return namespace


def test_tg_monitor_location_review_triggers_on_temporal_location_name() -> None:
    ns = _load_location_review_helpers_in_isolation()
    needs_review = ns["_needs_llm_location_review"]

    assert needs_review(
        [
            {
                "title": "Экспериментальный пленэр",
                "date": "2026-06-14",
                "time": "12:00",
                "location_name": "Завтра",
                "location_address": "Каштановая аллея 1а",
                "city": "Калининград",
            }
        ],
        source_default_location="Барн, Каштановая аллея 1а, Калининград",
        message_text="Завтра, 14 июня, в 12:00 в рамках ОП!ФЕСТА состоится экспериментальный пленэр.",
        ocr_text="",
        source_context_line="source_username=barn_kaliningrad",
    )


def test_tg_monitor_location_review_triggers_on_inflected_city_phrase() -> None:
    ns = _load_location_review_helpers_in_isolation()
    needs_review = ns["_needs_llm_location_review"]

    assert needs_review(
        [
            {
                "title": "Концерт VI Международного фестиваля классической музыки",
                "date": "2026-06-14",
                "time": "",
                "location_name": "Кирха Гердауэн",
                "location_address": "Первомайская 1",
                "city": "посёлке Железнодорожный",
            }
        ],
        source_default_location=None,
        message_text="14 июня в кирхе Гердауэн в посёлке Железнодорожный пройдёт концерт.",
        ocr_text="",
        source_context_line="source_username=festdir",
    )


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
            "location_address": "location_address",
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


def _load_tg_monitor_signal_helpers_in_isolation():
    import ast

    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wanted = {
        "_CLEAR_SINGLE_EVENT_INVITE_RE",
        "_CLEAR_SINGLE_EVENT_DATE_RE",
        "_CLEAR_SINGLE_EVENT_TIME_RE",
        "_CLEAR_SINGLE_EVENT_VENUE_OR_TICKET_RE",
        "_looks_like_clear_single_event_invitation",
        "_has_strong_event_invitation_signal",
    }
    extracted: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if targets & wanted:
                extracted.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted:
            extracted.append(node)
    namespace: dict = {"re": re}
    exec(
        compile(ast.Module(body=extracted, type_ignores=[]), filename="<tg-monitor-signals>", mode="exec"),
        namespace,
    )
    if "_has_strong_event_invitation_signal" not in namespace:
        pytest.fail("strong event signal helper missing from Telegram monitor source")
    return namespace


def test_tg_monitor_strong_event_signal_keeps_kraftmarket_promo_posts_in_llm_path() -> None:
    ns = _load_tg_monitor_signal_helpers_in_isolation()
    strong = ns["_has_strong_event_invitation_signal"]
    clear = ns["_looks_like_clear_single_event_invitation"]

    kraft_285 = (
        "Друзья, вы этого просили, мы это сделали!\n\n"
        "Лекции Дмитрия Манкевича о становлении здравоохранения в первые годы "
        "становления области перенесена в Историко-художественный музей на ту же дату "
        "и то же время.\n\n"
        "О ЧЁМ БУДЕТ ЛЕКЦИЯ\n"
        "Поговорим о здравоохранении в первые годы.\n\n"
        "Лекция проходит в рамках фестиваля «80 историй о главном».\n\n"
        "бесплатно, по регистрации\n\n"
        "19 июня 18:30\n"
        "Историко-художественный музей, Клиническая 21, #Калининград\n"
        "ПЕРЕНОС, билеты действительны\n"
        "https://kgd80.ru/sobytiya/kaliningradskoe-zdravoohranenie-v-period-stanovleniya-oblasti-osobennosti-vyzovy-pobedy-i-problemy/?register=1"
    )
    kraft_287 = (
        "15.06 • 12:45 «Бородин. Гениальный дилетант» — почему великий учёный смог "
        "стать великим композитором.\n\n"
        "В рамках образовательной программы фестиваля Кантата вы сможете послушать "
        "лекцию о русском композиторе Александре Бородине.\n\n"
        "15 июня 12:45\n"
        "Филиал Третьяковской галереи, Парадная наб. 3, #Калининград\n\n"
        "Бесплатно, по регистрации\n"
        "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46524/2026-06-15/12:45:00"
    )
    raffle_results_with_event = (
        "Поздравляем победителей розыгрыша!\n\n"
        "А тем, кому сегодня чуть-чуть не повезло, не грустите: ждём вас 13 июня "
        "с 11:00 до 16:00 в деревне Холмогорье. Билеты стоят 800 рублей, впереди "
        "конкурс костюмов и запуск воздушных змеев."
    )

    assert strong(kraft_285)
    assert clear(kraft_285)
    assert strong(kraft_287)
    assert clear(kraft_287)


def test_tg_monitor_clear_event_signal_accepts_poster_only_ocr() -> None:
    ns = _load_tg_monitor_signal_helpers_in_isolation()
    strong = ns["_has_strong_event_invitation_signal"]
    clear = ns["_looks_like_clear_single_event_invitation"]

    kraft_317_ocr = (
        "МАСТЕР КЛАСС\n"
        "ПО КАЛЛИГРАФИИ\n"
        "проводит Ламейко Светлана\n"
        "Дата: 1 июля\n"
        "Время: 19:00\n"
        "Место: музей «Восток на Западе», ул. Клиническая, 19А\n"
        "Стоимость: 1000 рублей\n"
        "Запись по телефону: +7 (931) 616 08 88"
    )
    schedule_noise = (
        "АФИША\n"
        "1 июля 19:00 лекция\n"
        "2 июля 19:00 концерт\n"
        "3 июля 19:00 встреча\n"
        "4 июля 19:00 мастер-класс\n"
        "5 июля 19:00 экскурсия\n"
        "музей"
    )
    raffle_results_with_event = (
        "Поздравляем победителей розыгрыша!\n\n"
        "А тем, кому сегодня чуть-чуть не повезло, не грустите: ждём вас 13 июня "
        "с 11:00 до 16:00 в деревне Холмогорье. Билеты стоят 800 рублей, впереди "
        "конкурс костюмов и запуск воздушных змеев."
    )

    assert clear(kraft_317_ocr)
    assert not clear(schedule_noise)
    assert strong(raffle_results_with_event)
    assert not strong("Поздравляем победителей конкурса! Итоги уже в комментариях.")


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


def test_tg_monitor_does_not_promote_sole_donation_link_to_ticket() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")

    assert "Поддержать" in source
    assert "Donation/fundraiser/project-support" in source
    assert "A sole external URL" in source
    assert "if len(cands) == 1" not in source[source.index("def _pick_link"):source.index("def _more_specific_ticket_link")]


def test_tg_monitor_title_prompt_prefers_caption_event_name_over_poster_slogan() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(encoding="utf-8")
    master_prompt = Path("docs/llm/prompts.md").read_text(encoding="utf-8")

    for text in (source, master_prompt):
        assert "Живой сундук" in text
        assert "Читайте бумажные книги" in text
        assert "poster" in text.casefold() or "poster_titles" in text
        assert "slogan" in text.casefold() or "лозунг" in text.casefold()


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
    assert "kaggle_status_client.py" in source
    assert "await_dataset_ready" in source
    assert "\"kaggle_run.json\"" in source
    assert "\"kaggle_status_client.py\"" in source


def test_tg_monitor_poster_objects_use_exact_encoded_identity() -> None:
    source = Path("kaggle/TelegramMonitor/telegram_monitor.py").read_text(
        encoding="utf-8"
    )
    upload = source[
        source.index("def upload_to_supabase_storage") : source.index(
            "def _bucket_item_size_bytes"
        )
    ]

    assert "encoded_sha256 = hashlib.sha256(stored_bytes).hexdigest()" in upload
    assert 'f"{SUPABASE_POSTERS_PREFIX}/image/v2/"' in upload
    assert "/dh16/" not in upload
    assert "max-age=31536000, immutable" in upload
    assert "return public_url, object_path, encoded_sha256" in upload
    assert "'raw_sha256': raw_sha256" in source

    handler = Path("source_parsing/telegram/handlers.py").read_text(encoding="utf-8")
    assert 'raw_sha256=(item or {}).get("raw_sha256")' in handler
    assert 'raw_sha256=item.get("raw_sha256")' in handler

    notebook = json.loads(
        Path("kaggle/TelegramMonitor/telegram_monitor.ipynb").read_text(
            encoding="utf-8"
        )
    )
    built_entrypoint = "".join(notebook["cells"][1]["source"])
    assert "encoded_sha256 = hashlib.sha256(stored_bytes).hexdigest()" in built_entrypoint
    assert 'f"{SUPABASE_POSTERS_PREFIX}/image/v2/"' in built_entrypoint
    assert "return public_url, object_path, encoded_sha256" in built_entrypoint
