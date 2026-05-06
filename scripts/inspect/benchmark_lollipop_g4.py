#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import random
import re
import sys
import textwrap
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import smart_event_update as su
from smart_event_update import EventCandidate
from smart_update_lollipop_lab.full_cascade import run_full_cascade_variant
from smart_update_lollipop_lab import writer_final_4o_family as writer_final_family
from smart_update_lollipop_lab import legacy_writer_family

_LEGACY_GENAI = None


def _legacy_genai():
    global _LEGACY_GENAI
    if _LEGACY_GENAI is None:
        import google.generativeai as genai

        _LEGACY_GENAI = genai
    return _LEGACY_GENAI


ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts" / "codex"
DEFAULT_G3_MODEL = "gemma-3-27b-it"
DEFAULT_G4_MODEL = "gemma-4-31b-it"
DEFAULT_4O_MODEL = "gpt-4o"
DEFAULT_FIXTURES = "audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos"
DEFAULT_VARIANTS = "baseline,lollipop_legacy"
DEFAULT_GEMMA_CALL_GAP_S = 4.0
DEFAULT_GEMMA_DIRECT_TIMEOUT_SEC = 75.0
DEFAULT_GEMMA_WRITER_TIMEOUT_SEC = 12.0
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)


@dataclass(slots=True)
class SourcePacket:
    source_id: str
    source_type: str
    url: str
    text: str


@dataclass(slots=True)
class BenchmarkFixture:
    fixture_id: str
    title: str
    event_type: str
    date: str | None
    time: str | None
    location_name: str | None
    location_address: str | None
    city: str | None
    sources: list[SourcePacket]


def _load_env_file() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _http_get(url: str) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"},
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def _strip_html(value: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", value or "")
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_site_text(html_text: str, title: str) -> str:
    flat = _strip_html(html_text)
    idx = flat.lower().find(title.lower())
    if idx == -1:
        return flat[:4500]
    return flat[idx : idx + 4500].strip()


def _extract_tg_text(html_text: str) -> str:
    match = re.search(
        r'(?is)<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        html_text,
    )
    if not match:
        return _strip_html(html_text)[:3000]
    text = re.sub(r"(?i)<br\s*/?>", "\n", match.group(1))
    return _strip_html(text)


def _telegram_post_ref(url: str) -> tuple[str, str] | None:
    match = re.search(r"(?i)https?://t\.me/(?:s/)?([^/?#]+)/(\d+)", url or "")
    if not match:
        return None
    return match.group(1), match.group(2)


def _telegram_embed_url(url: str) -> str | None:
    ref = _telegram_post_ref(url)
    if not ref:
        return None
    channel, post_id = ref
    return f"https://t.me/{channel}/{post_id}?embed=1&mode=tme"


def _extract_tg_post_text(html_text: str, channel: str, post_id: str | int) -> str:
    target = f"{channel}/{post_id}".lower()
    message_open = re.compile(
        r'(?is)<div\b(?=[^>]*class="[^"]*\btgme_widget_message\b[^"]*")(?=[^>]*data-post="([^"]+)")[^>]*>'
    )
    matches = list(message_open.finditer(html_text or ""))
    selected: re.Match[str] | None = None
    for match in matches:
        if match.group(1).lower() == target:
            selected = match
            break
    if selected is None and len(matches) == 1:
        selected = matches[0]
    if selected is None:
        return ""
    next_match = next((item for item in matches if item.start() > selected.start()), None)
    chunk = html_text[selected.start() : next_match.start() if next_match else len(html_text)]
    return _extract_tg_text(chunk)


def _fetch_tg_text(url: str) -> str:
    embed_url = _telegram_embed_url(url)
    if not embed_url:
        return _extract_tg_text(_http_get(url))
    channel, post_id = _telegram_post_ref(url) or ("", "")
    exact_text = _extract_tg_post_text(_http_get(embed_url), channel, post_id)
    if exact_text:
        return exact_text
    page_html = _http_get(url)
    return _extract_tg_post_text(page_html, channel, post_id) or _extract_tg_text(page_html)


def _extract_vk_text(html_text: str) -> str:
    match = re.search(r'property="og:description"\s+content="([^"]+)"', html_text, re.I)
    if not match:
        return _strip_html(html_text)[:3000]
    return _strip_html(html.unescape(match.group(1)).replace("<br>", "\n").replace("<br/>", "\n"))


def _build_kalmania_fixture() -> BenchmarkFixture:
    title = "Кальмания"
    site_url = "https://muzteatr39.ru/spektakli/koncerty/kalmaniya/"
    tg_url = "https://t.me/s/muztear39/9421"
    vk_url = "https://vk.com/wall-131136967_21590"
    return BenchmarkFixture(
        fixture_id="KALMANIA-2026-04-03",
        title=title,
        event_type="концерт",
        date="2026-04-03",
        time=None,
        location_name="Калининградский областной музыкальный театр",
        location_address=None,
        city="Калининград",
        sources=[
            SourcePacket("site", "parser", site_url, _extract_site_text(_http_get(site_url), title)),
            SourcePacket("tg", "telegram", tg_url, _fetch_tg_text(tg_url)),
            SourcePacket("vk", "vk", vk_url, _extract_vk_text(_http_get(vk_url))),
        ],
    )


def _build_vivat_fixture() -> BenchmarkFixture:
    title = "Виват, Мюнхгаузен!"
    site_url = "https://muzteatr39.ru/spektakli/dlya-detej/vivat-myunxgauzen/"
    tg_url = "https://t.me/s/muztear39/9440"
    return BenchmarkFixture(
        fixture_id="VIVAT-MUNCHHAUSEN-9440",
        title=title,
        event_type="мюзикл",
        date=None,
        time=None,
        location_name="Калининградский областной музыкальный театр",
        location_address=None,
        city="Калининград",
        sources=[
            SourcePacket("site", "parser", site_url, _extract_site_text(_http_get(site_url), title)),
            SourcePacket("tg", "telegram", tg_url, _fetch_tg_text(tg_url)),
        ],
    )


def _single_source_fixture(
    *,
    fixture_id: str,
    title: str,
    event_type: str,
    date: str | None,
    time: str | None,
    location_name: str | None,
    location_address: str | None = None,
    city: str | None = "Калининград",
    source_id: str = "tg",
    source_type: str = "telegram",
    url: str,
    text: str,
) -> BenchmarkFixture:
    return BenchmarkFixture(
        fixture_id=fixture_id,
        title=title,
        event_type=event_type,
        date=date,
        time=time,
        location_name=location_name,
        location_address=location_address,
        city=city,
        sources=[SourcePacket(source_id, source_type, url, re.sub(r"\s+", " ", text).strip())],
    )


def _build_audio_walk_fixture() -> BenchmarkFixture:
    return _single_source_fixture(
        fixture_id="AUDIO-WALK-QUARTER-971",
        title="Аудиопутешествие «Четверть длиннее восьмой»",
        event_type="аудиопрогулка",
        date="2026-04-24",
        time="16:00-20:00",
        location_name="Бар Советов",
        location_address="проспект Мира, 118",
        url="https://t.me/barn_kaliningrad/971",
        text=(
            "Аудиопутешествие «Четверть длиннее восьмой» 24 и 26 апреля. "
            "Не самый короткий маршрут от «Бара Советов» до «Барна», во время которого будут звучать "
            "проза, поэзия и экспериментальная музыка. Арт-группа «Нежные бабы» и саунд-художник "
            "Денис Баенко придумали звуковой комментарий к непарадному пространству исторического района "
            "и предлагают останавливаться у заборов, фонарных столбов, опор теплотрасс, присаживаться "
            "на лавочки и заглядывать во дворы. Каждая точка — композиция, созданная художниками. "
            "Не торопитесь! Начать прогулку можно в любой промежуток с 16:00 до 20:00 в пятницу и "
            "воскресенье. Время и скорость движения вы выбираете сами. А понравившиеся треки не "
            "запрещается ставить на повтор. Возможно, для полного погружения стоит отправиться в это "
            "путешествие в одиночку. Как принять участие: записаться бесплатно по ссылке; с собой "
            "обязательно взять наушники и заряженный смартфон; полную инструкцию и карту вы получите "
            "в «Баре Советов» по адресу: проспект Мира, 118; нужно будет найти наклейки с QR-кодом "
            "и перейти на аудио. В финале ждём вас на видеоинсталляции «Доля огня. Хор» группы "
            "«Нежные Бабы». Билеты можно приобрести на месте и на сайте barnkaliningrad.ru. "
            "OCR: 24 И 26 АПРЕЛЯ, 16:00-20:00. АУДИОПУТЕШЕСТВИЕ "
            "\"ЧЕТВЕРТЬ ДЛИННЕЕ ВОСЬМОЙ\". КАШТАНОВАЯ АЛЛЕЯ, 1А."
        ),
    )


def _build_peter_fleet_lecture_fixture() -> BenchmarkFixture:
    return _single_source_fixture(
        fixture_id="PETER-FLEET-LECTURE-5600",
        title="Лекция о быте и нравах регулярного военного флота Петра Великого",
        event_type="лекция",
        date="2026-04-24",
        time="16:00",
        location_name="Музей янтаря",
        location_address="пл. Василевского, 1",
        url="https://t.me/ambermuseum/5600",
        text=(
            "Приглашаем на лекцию о быте и нравах регулярного военного флота Петра Великого.\n\n"
            "Лектор – Борис Мегорский, заведующий отделом эстампов и фотографий Российской "
            "национальной библиотеки, руководитель клуба исторической реконструкции "
            "«Лейб-гвардии Преображенский полк, 1709», историк-исследователь, автор книг "
            "и статей по истории Северной войны.\n\n"
            "⚓ Вы узнаете о поведении и нравах личного состава российского корабельного "
            "флота первой трети XVIII века. На основании служебных документов и источников "
            "личного происхождения станет понятнее дисциплина и поведение в экстремальных "
            "ситуациях, отношения офицеров и нижних чинов и девиантные поступки, которыми "
            "могли «грешить» моряки.\n\n"
            "Во время доклада лектор покажет реконструированные предметы обмундирования "
            "петровских матросов.\n\n"
            "Билеты продаются на сайте (https://clck.ru/3TD7H8) и в кассе музея. "
            "OCR: 24.04 в 16:00."
        ),
    )


def _build_sacred_lecture_fixture() -> BenchmarkFixture:
    return _single_source_fixture(
        fixture_id="SACRED-LECTURE-ZYGMONT-3170",
        title="В поисках абсолютно инакового",
        event_type="лекция",
        date="2026-04-23",
        time="18:30",
        location_name="Дом китобоя",
        location_address="проспект Мира, 9",
        url="https://t.me/domkitoboya/3170",
        text=(
            "Завтра, 23 апреля, в Доме китобоя стартует цикл лекций от религиоведа и историка "
            "философии Алексея Зыгмонта «В поисках \"абсолютно инакового\": краткая история "
            "сакрального». Что такое «сакральное»: туманное понятие архаичных религий, концепция "
            "в современной науке и философии, объективный вневременной феномен — или всё разом? "
            "Почему его так часто связывают с ужасом, безумием и насилием? Почему мы описываем "
            "«религиозное во все времена» при помощи термина, привезённого в Европу с островов "
            "Тонги в 1770-е годы? Наконец, как вышло, что «сакральное» столь полюбилось политикам — "
            "и что это говорит нам о современности? Трактовок у этого понятия множество. Объединяют "
            "их разве что известная мрачность — и убеждение, что бездны «сакрального» неким образом "
            "хранят ответы о глубочайшей сути человека, общества, жизни и смерти. На первой лекции "
            "разговор пойдёт об истоках понятия сакрального и в целом его интеллектуальных судьбах, "
            "пройдя путь от Древнего Рима до идеологов Французской революции, Шарля де Бросса и "
            "Фридриха Шлейермахера, а затем — до теоретиков XIX-XX веков. Автор — Алексей Зыгмонт, "
            "кандидат философских наук, религиовед, историк философии и переводчик. Начало в 18.30. "
            "16+. Вход — 300 р. Билеты — на сайте музея. Дом китобоя, пр-т Мира 9. "
            "OCR: краткая история сакрального; лекторий; Алексей Зыгмонт; "
            "САКРАЛЬНОЕ В ТЕОРИИ И В ИСТОРИИ; 23.04; 18:30; ДОМ Китобоя; 16+."
        ),
    )


def _build_world_hobbies_fixture() -> BenchmarkFixture:
    return _single_source_fixture(
        fixture_id="WORLD-HOBBIES-5505",
        title="Мир увлечений",
        event_type="выставка",
        date="2026-04-23",
        time="16:00",
        location_name="Калининградский историко-художественный музей",
        url="https://t.me/koihm/5505",
        text=(
            "МИР УВЛЕЧЕНИЙ || выставка ко Дню Земли. 23 апреля в 16:00 в Калининградском "
            "историко-художественном музее состоится открытие персональной выставки анималистической "
            "скульптуры художника по металлу, члена Союза художников России и Союза художников "
            "Советского Союза Геннадия Медера «Мир увлечений». Экспозиция посвящается празднованию "
            "международного Дня Земли. На выставке будут представлены художественные работы из металла, "
            "созданные в технике ковки и металлопластики. Об авторе: своё призвание Геннадий Борисович "
            "Медер нашёл в работе с металлом. Из всех видов художественной обработки он отдаёт "
            "предпочтение ковке. Диапазон работ Геннадия Медера широк — это и творческие композиции, "
            "достойные уровня музейной коллекции, и кованые ограды, решётки, балконы, которые прекрасно "
            "вписываются в любой дизайн, придавая ему неповторимый колорит. Есть у художника удивительные "
            "произведения, выполненные в технике металлопластики. В его произведениях воплотились "
            "художественные образы, созданные фантазией писателей. В творческой коллекции Геннадия "
            "Медера есть и живописные произведения, но металл — это самое главное и любимое. Мастер "
            "называет себя художником-кузнецом и гордится этим, так как имеет власть над металлом. "
            "22 апреля — Международный День Земли. В России его отмечают с 1990-х гг. Это день "
            "экологических акций, просветительских мероприятий и субботников, направленных на защиту "
            "окружающей среды. Часто мероприятия проходят в рамках «Дней защиты от экологической "
            "опасности». Самый массовый формат — уборка парков, дворов, берегов рек и озёр, а также "
            "высадка деревьев и кустарников. Выставка будет работать до 22 мая 2026 года."
        ),
    )


def _build_red_cosmos_fixture() -> BenchmarkFixture:
    return _single_source_fixture(
        fixture_id="RED-COSMOS-7902",
        title="Космос красного",
        event_type="выставка",
        date=None,
        time=None,
        location_name="Калининградский музей изобразительных искусств",
        url="https://t.me/kaliningradartmuseum/7902",
        text=(
            "В разделе «Красны девицы, добры молодцы» на выставке «Космос красного» можно увидеть "
            "произведения русского народного промысла, которые имеют свои неповторимые особенности. "
            "Здесь представлены жостовские подносы, каргопольская и дымковская игрушки. Жостовская "
            "роспись существует с 1825 года и отличается разнообразием и уникальностью: среди работ "
            "практически невозможно найти две одинаковые. Визитная карточка жостовских подносов — "
            "букет, а главный секрет — лак. Состав до сих пор держат в тайне. Каргопольскую игрушку "
            "можно назвать действительно народной, потому что её исполнение достаточно свободное. "
            "Сюжеты условно делятся на две категории: это архаичные типы и сюжетная игрушка, "
            "демонстрирующая сцены деревенской жизни. Дымковская игрушка связана с весенним праздником "
            "вятской свистуньи. Промысел возник среди женского населения слободы Дымково. Лепились "
            "свистульки из глины в виде коней, баранов, козлов, уток и других животных."
        ),
    )


def _fixture_by_name(name: str) -> BenchmarkFixture:
    normalized = (name or "").strip().lower()
    if normalized in {"kalmania", "kalmania-2885", "2885"}:
        return _build_kalmania_fixture()
    if normalized in {"vivat", "vivat-munchausen", "vivat-munchhausen", "9440"}:
        return _build_vivat_fixture()
    if normalized in {"audio_walk", "audio-walk", "quarter", "971"}:
        return _build_audio_walk_fixture()
    if normalized in {"peter_fleet_lecture", "peter-fleet-lecture", "fleet_lecture", "5600"}:
        return _build_peter_fleet_lecture_fixture()
    if normalized in {"sacred_lecture", "sacred-lecture", "zygmunt", "zygmont", "3170"}:
        return _build_sacred_lecture_fixture()
    if normalized in {"world_hobbies", "world-hobbies", "mir_uvlecheniy", "5505"}:
        return _build_world_hobbies_fixture()
    if normalized in {"red_cosmos", "red-cosmos", "cosmos_red", "7902"}:
        return _build_red_cosmos_fixture()
    raise ValueError(f"Unsupported fixture: {name}")


def _fixtures_from_cli(raw: str) -> list[BenchmarkFixture]:
    names = [item.strip() for item in (raw or "").split(",") if item.strip()]
    if not names:
        names = [item.strip() for item in DEFAULT_FIXTURES.split(",") if item.strip()]
    return [_fixture_by_name(name) for name in names]


def _variants_from_cli(raw: str) -> list[str]:
    allowed = {"baseline", "baseline_g4", "lollipop", "lollipop_g4", "lollipop_legacy"}
    variants = [item.strip() for item in (raw or DEFAULT_VARIANTS).split(",") if item.strip()]
    if not variants:
        variants = [item.strip() for item in DEFAULT_VARIANTS.split(",") if item.strip()]
    unknown = [item for item in variants if item not in allowed]
    if unknown:
        raise ValueError(f"Unsupported variants: {', '.join(unknown)}")
    if ("baseline_g4" in variants or "lollipop_legacy" in variants) and "baseline" not in variants:
        variants.insert(0, "baseline")
    return list(dict.fromkeys(variants))


def _fixture_from_artifact_row(row: dict[str, Any]) -> BenchmarkFixture | None:
    fixture = row.get("fixture") if isinstance(row.get("fixture"), dict) else None
    if not fixture:
        return None
    sources = []
    for source in list(fixture.get("sources") or []):
        if not isinstance(source, dict):
            continue
        sources.append(SourcePacket(**source))
    if not sources:
        return None
    return BenchmarkFixture(
        fixture_id=str(fixture.get("fixture_id") or "").strip(),
        title=str(fixture.get("title") or "").strip(),
        event_type=str(fixture.get("event_type") or "").strip(),
        date=fixture.get("date"),
        time=fixture.get("time"),
        location_name=fixture.get("location_name"),
        location_address=fixture.get("location_address"),
        city=fixture.get("city"),
        sources=sources,
    )


def _fixture_row_matches_name(row: dict[str, Any], name: str) -> bool:
    fixture = row.get("fixture") if isinstance(row.get("fixture"), dict) else {}
    normalized = (name or "").strip().lower()
    fixture_id = str(fixture.get("fixture_id") or "").strip().lower()
    title = str(fixture.get("title") or "").strip().lower()
    if normalized in {"kalmania", "kalmania-2885", "2885"}:
        return fixture_id.startswith("kalmania") or "кальмания" in title
    if normalized in {"vivat", "vivat-munchausen", "vivat-munchhausen", "9440"}:
        return fixture_id.startswith("vivat") or "мюнхгаузен" in title
    if normalized in {"audio_walk", "audio-walk", "quarter", "971"}:
        return fixture_id.startswith("audio-walk") or "четверть длиннее" in title
    if normalized in {"peter_fleet_lecture", "peter-fleet-lecture", "fleet_lecture", "5600"}:
        return fixture_id.startswith("peter-fleet") or "флота петра" in title
    if normalized in {"sacred_lecture", "sacred-lecture", "zygmunt", "zygmont", "3170"}:
        return fixture_id.startswith("sacred-lecture") or "инакового" in title
    if normalized in {"world_hobbies", "world-hobbies", "mir_uvlecheniy", "5505"}:
        return fixture_id.startswith("world-hobbies") or "мир увлечений" in title
    if normalized in {"red_cosmos", "red-cosmos", "cosmos_red", "7902"}:
        return fixture_id.startswith("red-cosmos") or "космос красного" in title
    return normalized in fixture_id or normalized in title


def _fixtures_from_artifact(path_value: str, raw: str) -> list[BenchmarkFixture]:
    path = Path(path_value)
    data = json.loads(path.read_text(encoding="utf-8"))
    results = list(data.get("results") or []) if isinstance(data, dict) else []
    if not results:
        raise RuntimeError(f"Fixture artifact {path} does not contain results[]")
    names = [item.strip() for item in (raw or "").split(",") if item.strip()] or [DEFAULT_FIXTURES]
    fixtures: list[BenchmarkFixture] = []
    for name in names:
        row = next((item for item in results if isinstance(item, dict) and _fixture_row_matches_name(item, name)), None)
        if row is None:
            raise RuntimeError(f"Fixture {name} not found in {path}")
        fixture = _fixture_from_artifact_row(row)
        if fixture is None:
            raise RuntimeError(f"Fixture {name} in {path} is missing source texts")
        fixtures.append(fixture)
    return fixtures


def _source_excerpt(sources: list[SourcePacket], *, limit: int = 2200) -> str:
    blocks: list[str] = []
    for source in sources:
        cleaned = re.sub(r"\s+", " ", source.text).strip()
        if cleaned:
            blocks.append(f"[{source.source_id}] {source.url}\n{cleaned[:limit]}")
    return "\n\n".join(blocks).strip()


def _build_candidate(fixture: BenchmarkFixture, source: SourcePacket) -> EventCandidate:
    return EventCandidate(
        source_type=source.source_type,
        source_url=source.url,
        source_text=source.text,
        raw_excerpt=source.text[:1000],
        title=fixture.title,
        date=fixture.date,
        time=fixture.time,
        location_name=fixture.location_name,
        location_address=fixture.location_address,
        city=fixture.city,
        event_type=fixture.event_type,
    )


async def _gemma_gap_sleep(gap_s: float) -> None:
    if gap_s > 0:
        await asyncio.sleep(gap_s)


def _set_gemma_model(model: str) -> None:
    su.SMART_UPDATE_MODEL = model


async def _run_baseline(
    fixture: BenchmarkFixture,
    *,
    gemma_model: str,
    gemma_call_gap_s: float,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    gemma_calls = 0
    sleep_sec = 0.0
    _set_gemma_model(gemma_model)
    extracted: list[str] = []
    per_source: dict[str, list[str]] = {}
    for source in fixture.sources:
        candidate = _build_candidate(fixture, source)
        facts = await su._llm_extract_candidate_facts(candidate, text_for_facts=source.text)
        gemma_calls += 1
        per_source[source.source_id] = facts
        extracted.extend(facts)
        await _gemma_gap_sleep(gemma_call_gap_s)
        sleep_sec += gemma_call_gap_s if gemma_call_gap_s > 0 else 0.0
    anchors = [fixture.date or "", fixture.time or "", fixture.city or "", fixture.location_name or "", fixture.location_address or ""]
    facts_text_clean = su._facts_text_clean_from_facts(extracted, anchors=anchors)
    budget_chars = su._estimate_fact_first_description_budget_chars(facts_text_clean)
    desc_max_tokens = su._estimate_fact_first_description_max_tokens(budget_chars=budget_chars, floor=1700)
    description = await su._ask_gemma_text(
        su._fact_first_description_prompt(
            title=fixture.title,
            event_type=fixture.event_type,
            facts_text_clean=facts_text_clean,
            epigraph_fact=su._pick_epigraph_fact(facts_text_clean),
        ),
        max_tokens=desc_max_tokens,
        label=re.sub(r"[^a-zA-Z0-9_-]+", "_", f"benchmark_{fixture.fixture_id}_baseline_first_pass").strip("_"),
        temperature=0.0,
    )
    gemma_calls += 1
    wall_clock_sec = round(time.perf_counter() - started_at, 6)
    return {
        "gemma_model": gemma_model,
        "baseline_mode": "prod_style_first_pass_proxy",
        "per_source_facts": per_source,
        "facts_text_clean": facts_text_clean,
        "description_md": description,
        "timings": {
            "wall_clock_sec": wall_clock_sec,
            "model_active_sec": round(max(0.0, wall_clock_sec - sleep_sec), 6),
            "sleep_sec": round(sleep_sec, 6),
            "gemma_calls": gemma_calls,
            "four_o_calls": 0,
        },
    }


def _baseline_g4_fact_schema() -> dict[str, Any]:
    fact_item = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "source_span": {"type": "STRING"},
            "kind": {"type": "STRING"},
        },
        "required": ["text", "source_span", "kind"],
    }
    return {
        "type": "OBJECT",
        "properties": {
            "public_facts": {"type": "ARRAY", "items": fact_item},
            "logistics_facts": {"type": "ARRAY", "items": fact_item},
            "lead_hooks": {"type": "ARRAY", "items": {"type": "STRING"}},
            "narrative_angles": {"type": "ARRAY", "items": {"type": "STRING"}},
            "warnings": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["public_facts", "logistics_facts", "lead_hooks", "narrative_angles", "warnings"],
    }


def _baseline_g4_writer_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "title": {"type": "STRING"},
            "description_md": {"type": "STRING"},
            "covered_public_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "used_logistics_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "warnings": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["title", "description_md", "covered_public_fact_indexes", "used_logistics_fact_indexes", "warnings"],
    }


def _baseline_g4_review_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "verdict": {"type": "STRING"},
            "errors": {"type": "ARRAY", "items": {"type": "STRING"}},
            "warnings": {"type": "ARRAY", "items": {"type": "STRING"}},
            "quality_notes": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["verdict", "errors", "warnings", "quality_notes"],
    }


def _baseline_g4_fact_prompt() -> str:
    return textwrap.dedent(
        """
        You are the Gemma 4 version of the existing Smart Update baseline fact extractor.
        Return only JSON matching the schema. No markdown. No analysis. No comments.

        Extract concise Russian facts from the source text for one event description.
        Keep the same semantic contract as the baseline:
        - public_facts: event format, title/program, topic, named people and roles,
          route/object/list, source-local atmosphere, explicit format texture, and source-grounded
          cultural handle for the writer;
        - logistics_facts: date, time, venue, address, tickets, price, age, URL, service instructions.

        Rules:
        - Use only the provided source text and event metadata.
        - Preserve named entities exactly.
        - Do not invent programme items, promises, locations, dates, prices, or cultural interpretation.
        - Each fact is one short Russian phrase; source_span is the shortest supporting source phrase.
        - kind is a short lowercase label: format, title, topic, person, route, object, texture,
          date, time, venue, address, tickets, price, age, url, service.
        - Prefer recall over brevity: return 4-10 public_facts when source supports them.
        - Do not collapse a short source to only "format + title": split format, route/place,
          instruction/map, source OCR route, and start mode when present.
        - Fact text must be a clean Russian phrase. Do not include field labels such as
          "text:", "kind:", JSON keys, or English helper words inside fact text.
        - Format mechanics are public facts when they define the experience: "start any time
          within an interval", "get full instruction and map", "self-guided route", "opening
          of an exhibition", "lecture cycle starts". They may also appear as logistics/service
          facts if needed, but do not hide them only in logistics.
        - If an OCR line names a street/place without service wording, classify it by source role:
          route/object if it looks like the route or object of the event, venue/address only if
          the source explicitly says it is the venue/address.
        - A street plus house number attached to a named venue is logistics/address, not route,
          unless the source explicitly says the route passes there. For this shape, "Бар Советов,
          проспект Мира, 118" is a pickup venue/address; a separate OCR place like
          "Каштановая аллея, 1А" may be the route/location marker.
        - lead_hooks are short grounded lead options, not slogans or ad CTA.
        - narrative_angles are source-grounded ways to write about the format/topic. They may
          unpack audio walk/lecture/exhibition as a public experience, but must not add new
          names, places, works, dates, prices, promises, or programme items.
        """
    ).strip()


def _baseline_g4_writer_prompt() -> str:
    return textwrap.dedent(
        """
        You are the Gemma 4 version of the existing Smart Update baseline public-description writer.
        Return one JSON object only. No markdown fences. No analysis.

        Write description_md: final Russian Markdown for Telegram.
        Ground truth: public_facts, logistics_facts, lead_hooks, narrative_angles, source_excerpt.

        Hard rules:
        - Single string output: description_md must contain prose only, never JSON/list fragments
          like "],", field names, braces, or schema words.
        - Cover every meaningful public_fact. Use logistics_facts only in one compact final
          "### Когда и где" block when date/time/place facts are useful.
        - Do not invent names, works, venues, route details, dates, prices, promises, or claims.
        - Keep two locations separate unless source explicitly says they are the same venue/address.
          Never say a route leads to/ends at a venue unless source_excerpt says leads/ends.
        - No URL, phone, age limit, OCR/poster labels, internal field names, or English prompt words.
        - Russian prose only. Do not use English words or mixed-language phrases unless the exact
          English name appears in source_excerpt.
        - No direct address or CTA: "вы", "вам", "для вас", "приглашаем", "приходите",
          "не упустите", "уникальная возможность", "вы сможете", "позволит вам".
        - Avoid dry formulas: "посвящено", "характеризуется", "программа состоит из",
          "представляет собой".

        Style and length:
        - min_description_chars is binding when public_facts are present; target
          target_description_chars, stay under max_description_chars. A shorter description is
          invalid output. Before returning, expand if description_md is below min_description_chars.
        - If min_description_chars is 650 or more, use at least three public paragraphs before
          the final logistics block.
        - Use 2-4 public paragraphs. Add ### headings only when they help; logistics heading
          must be "### Когда и где".
        - If public_facts contain a concrete object list, use one compact paragraph plus a short
          bullet list. Do not inflate each object into generic art-history prose.
        - Do not hard-wrap sentences. Each paragraph is one continuous line; use a blank line
          only between paragraphs, before/after a ### heading, and nowhere inside a sentence.
        - Open with a concrete grounded hook from a fact/hook/angle: route, object, topic,
          person, exact title phrase, or format texture. Never open with "Название — это...".
        - To reach the floor, expand only through grounded interpretation of the existing
          format/topic: audio walk = listening/route/city attention; lecture = unpacking topic;
          exhibition = looking at named objects. No new facts.
        - Prefer concrete source nouns over abstract filler. Avoid "исследование пространства",
          "инструменты познания", "полностью погрузиться", "атмосфера путешествия",
          "архитектура", "точки маршрута", and "аудиогид" unless those words are in source_excerpt.
        - For audio walks, prefer concrete grounded phrasing: карта, инструкция, маршрут,
          возможность начать в выбранный промежуток, Каштановая аллея, Бар Советов.
        - Each paragraph must add new information. Proofread: no duplicate adjacent words,
          broken phrases, doubled place/address fragments, invented compound words, or
          accidentally concatenated Russian words. If any syllable or letter cluster repeats
          more than three times in a row, rewrite before returning.

        Return title, description_md, covered_public_fact_indexes, used_logistics_fact_indexes, warnings.
        """
    ).strip()


def _baseline_g4_writer_text_prompt() -> str:
    return textwrap.dedent(
        """
        You are the Gemma 4 version of the existing Smart Update baseline public-description writer.
        Return only the final Russian Markdown description text. No JSON. No markdown fences.
        No analysis, comments, field names, schema words, or instruction echoes.

        Ground truth: public_facts, logistics_facts, lead_hooks, narrative_angles, source_excerpt.

        Hard rules:
        - Cover every meaningful public_fact.
        - Use logistics_facts only in one compact final "### Когда и где" block when useful.
        - Do not invent names, works, venues, route details, dates, prices, promises, or claims.
        - Keep locations separate. Never say a route leads to/ends at a venue unless source says so.
        - Russian prose only; no English words unless the exact English name appears in source.
        - No URL, phone, age limit, OCR/poster labels, direct address, or ad CTA.
        - Avoid: "вы", "вам", "для вас", "приглашаем", "приходите", "не упустите",
          "уникальная возможность", "вы сможете", "позволит вам", "представляет собой",
          "посвящено", "характеризуется", "программа состоит из".

        Shape:
        - Obey length_contract. Shorter output is invalid.
        - Use 2-4 public paragraphs and then the final logistics block.
        - Do not hard-wrap sentences. Each paragraph is one continuous line; blank lines only
          between paragraphs and around a ### heading.
        - Open with a concrete grounded hook: route, object, topic, person, exact title phrase,
          or format texture. Never open with "Название — это...".
        - Expand only through grounded interpretation of the existing format/topic:
          audio walk = listening/route/city attention; lecture = unpacking topic;
          exhibition = looking at named objects. No new facts.
        - Prefer concrete source nouns over abstract filler.
        - Proofread before returning: no duplicate words, broken grammar, invented compound words,
          or mixed-language phrases.
        """
    ).strip()


def _baseline_g4_object_list_writer_prompt() -> str:
    return textwrap.dedent(
        """
        Return one JSON object matching the schema. No markdown fences. No analysis.

        Write final Russian Markdown for a cultural digest from public_facts only.
        This fixture has a concrete object list. Use a compact structure:
        - opening paragraph that starts with concrete named objects, not with "Выставка...";
        - one flowing sentence that names the object list, not bullets;
        - one paragraph explaining the source-grounded common theme;
        - final "### Когда и где" block only if logistics_facts have place/date/time.

        Rules:
        - Russian only, no English words.
        - Do not invent artists, techniques, dates, venues, schools, eras, prices, or claims.
        - Preserve every named object from public_facts; do not expand beyond source.
        - Do not add regions, techniques, motifs, "роспись", "сюжеты", or art-history claims
          unless those exact ideas are in public_facts/source_excerpt.
        - No bullets. No catalog tone.
        - No CTA/direct address/report formulas: "представляем вашему вниманию", "данная",
          "в рамках мероприятия", "посвящена", "представлен", "представляет",
          "включает в себя", "демонстрирует", "позволяет", "уникальные",
          "приглашаем", "посетители".
        - Do not use generic filler or repeat syllables/clusters.
        - Finish every sentence. Do not return if the last sentence is incomplete.
        - Obey length_contract, but prefer compactness over padding.
        - description_md contains prose/Markdown only, no JSON fragments or field names.

        Return title, description_md, covered_public_fact_indexes, used_logistics_fact_indexes, warnings.
        """
    ).strip()


def _baseline_g4_review_prompt() -> str:
    return textwrap.dedent(
        """
        You are an LLM-first reviewer for the Gemma 4 baseline migration.
        Return only JSON matching the schema. No prose outside JSON.

        Decide whether candidate_description is acceptable as a replacement for
        baseline_description for the same event.

        Use provided candidate_chars, baseline_chars, and min_description_chars exactly.
        Do not estimate or recount character length by eye.

        Review against source_excerpt first, then against baseline quality:
        - factual grounding: no unsupported venue/address/date/price/person/work claims;
        - fact coverage: all meaningful public_facts must be reflected, with logistics only in
          a compact final block when useful;
        - public-copy completeness: not a noticeably poorer card if baseline has richer useful copy;
          however, do not penalize the candidate for omitting baseline decorative claims that are
          not supported by source_excerpt or public_facts;
        - baseline_description is a quality/length reference, not a source of extra facts. Never
          require candidate to preserve baseline epigraphs, bullets, thematic questions, atmosphere,
          or interpretation unless the same concrete details are supported by source_excerpt or
          public_facts;
        - length: reject only when candidate_chars is below min_description_chars unless public_facts are empty or
          the source genuinely cannot support a longer public text without invention;
        - Russian prose quality: no broken grammar, stray JSON/list artifacts, duplicate words,
          malformed phrases, or prompt/instruction leaks;
        - style: cultural digest, not ad CTA and not dry report;
        - logistics: compact and correctly attached to the right venue/location.

        Put hard blockers in errors. Put softer tradeoffs in warnings.
        verdict must be one of: accepted, no_worse_with_warnings, rejected.
        Do not repair the text.
        """
    ).strip()


def _normalize_baseline_g4_fact_items(payload: dict[str, Any]) -> dict[str, Any]:
    def _items(key: str) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        seen: set[str] = set()
        for raw in list(payload.get(key) or []):
            if not isinstance(raw, dict):
                continue
            text = re.sub(r"\s+", " ", str(raw.get("text") or "")).strip()
            text = re.sub(r"(?iu)^(?:text|kind|fact)\s*:\s*", "", text).strip()
            if not text:
                continue
            normalized = text.casefold()
            if normalized in seen:
                continue
            seen.add(normalized)
            result.append(
                {
                    "text": text[:420],
                    "source_span": re.sub(r"\s+", " ", str(raw.get("source_span") or "")).strip()[:220],
                    "kind": re.sub(r"\s+", " ", str(raw.get("kind") or "")).strip()[:40],
                }
            )
        return result

    def _strings(key: str, limit: int) -> list[str]:
        result: list[str] = []
        for raw in list(payload.get(key) or []):
            text = re.sub(r"\s+", " ", str(raw or "")).strip()
            if text and text not in result:
                result.append(text[:limit])
        return result

    return {
        "public_facts": _items("public_facts"),
        "logistics_facts": _items("logistics_facts"),
        "lead_hooks": _strings("lead_hooks", 220),
        "narrative_angles": _strings("narrative_angles", 260),
        "warnings": _strings("warnings", 220),
    }


def _merge_baseline_g4_fact_items(items: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        text = re.sub(r"\s+", " ", str(item.get("text") or "")).strip()
        if not text:
            continue
        normalized = text.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        merged.append(
            {
                "index": len(merged),
                "text": text[:420],
                "source_span": re.sub(r"\s+", " ", str(item.get("source_span") or "")).strip()[:220],
                "kind": re.sub(r"\s+", " ", str(item.get("kind") or "")).strip()[:40],
            }
        )
    return merged


def _baseline_g4_prompt_leaks(text: str) -> list[str]:
    leaks: list[str] = []
    patterns = (
        "facts_text_clean",
        "epigraph_fact",
        "Cultural journalist",
        "Self-Correction",
        "Wait,",
        "I need",
        "schema",
        "prompt",
    )
    for pattern in patterns:
        if pattern.lower() in str(text or "").lower():
            leaks.append(pattern)
    return leaks


async def _run_baseline_g4(
    fixture: BenchmarkFixture,
    *,
    baseline: dict[str, Any],
    gemma_model: str,
    gemma_call_gap_s: float,
) -> dict[str, Any]:
    print(f"[benchmark] {fixture.fixture_id} start baseline_g4 model={gemma_model}", file=sys.stderr, flush=True)
    started_at = time.perf_counter()
    gemma_calls = 0
    sleep_sec = 0.0
    source_excerpt = _source_excerpt(fixture.sources, limit=5000)
    per_source: dict[str, list[str]] = {}
    public_fact_items_raw: list[dict[str, str]] = []
    logistics_fact_items_raw: list[dict[str, str]] = []
    lead_hooks: list[str] = []
    narrative_angles: list[str] = []
    source_warnings: list[str] = []

    for source in fixture.sources:
        raw = await _ask_gemma_json_direct(
            model=gemma_model,
            system_prompt=_baseline_g4_fact_prompt(),
            user_payload={
                "title": fixture.title,
                "event_type": fixture.event_type,
                "date": fixture.date,
                "time": fixture.time,
                "location_name": fixture.location_name,
                "location_address": fixture.location_address,
                "city": fixture.city,
                "source_id": source.source_id,
                "source_type": source.source_type,
                "source_text": source.text[:5000],
            },
            max_tokens=900,
            response_schema=_baseline_g4_fact_schema(),
            timeout_sec=min(_gemma_direct_timeout_sec(), 35.0),
            allow_json_repair=False,
        )
        gemma_calls += 1
        normalized = _normalize_baseline_g4_fact_items(raw)
        public_items = list(normalized.get("public_facts") or [])
        logistics_items = list(normalized.get("logistics_facts") or [])
        per_source[source.source_id] = [item["text"] for item in public_items + logistics_items if item.get("text")]
        public_fact_items_raw.extend(public_items)
        logistics_fact_items_raw.extend(logistics_items)
        for key, target in (
            ("lead_hooks", lead_hooks),
            ("narrative_angles", narrative_angles),
            ("warnings", source_warnings),
        ):
            for item in list(normalized.get(key) or []):
                if item not in target:
                    target.append(item)
        await _gemma_gap_sleep(gemma_call_gap_s)
        sleep_sec += gemma_call_gap_s if gemma_call_gap_s > 0 else 0.0

    public_fact_items = _merge_baseline_g4_fact_items(public_fact_items_raw)
    logistics_fact_items = _merge_baseline_g4_fact_items(logistics_fact_items_raw)
    facts_text_clean = [str(item.get("text") or "") for item in public_fact_items if item.get("text")]
    baseline_len = len(str(baseline.get("description_md") or ""))
    object_fact_count = sum(
        1
        for item in public_fact_items
        if str(item.get("kind") or "").casefold() == "object"
    )
    if object_fact_count >= 3 and baseline_len >= 1000:
        length_floor_ratio = 0.52
        length_target_ratio = 0.62
    else:
        length_floor_ratio = 0.68 if baseline_len >= 1000 else 0.80
        length_target_ratio = 0.78 if baseline_len >= 1000 else 0.92
    min_description_chars = int((baseline_len or 650) * length_floor_ratio) if facts_text_clean else 0
    target_description_chars = int((baseline_len or 650) * length_target_ratio) if facts_text_clean else 0
    max_description_ratio = 0.75 if object_fact_count >= 3 and baseline_len >= 1000 else 1.0
    max_description_chars = int((baseline_len or 650) * max_description_ratio) if facts_text_clean else 0
    writer_payload = {
        "title": fixture.title,
        "event_type": fixture.event_type,
        "reference_description_chars": baseline_len,
        "min_description_chars": min_description_chars,
        "target_description_chars": target_description_chars,
        "max_description_chars": max_description_chars,
        "length_contract": (
            f"description_md must be {min_description_chars}-{max_description_chars} characters; "
            f"target about {target_description_chars}; shorter output is invalid"
        ),
        "public_facts": public_fact_items,
        "logistics_facts": logistics_fact_items[:10],
        "lead_hooks": lead_hooks[:8],
        "narrative_angles": narrative_angles[:8],
        "source_excerpt": source_excerpt[:2600],
    }
    writer_system_prompt = (
        _baseline_g4_object_list_writer_prompt()
        if object_fact_count >= 3
        else _baseline_g4_writer_prompt()
    )
    writer_max_tokens = 600 if object_fact_count >= 3 else 700
    writer_raw = await _ask_gemma_json_direct(
        model=gemma_model,
        system_prompt=writer_system_prompt,
        user_payload=writer_payload,
        max_tokens=writer_max_tokens,
        response_schema=_baseline_g4_writer_schema(),
        timeout_sec=min(_gemma_writer_timeout_sec(), 55.0),
        allow_json_repair=False,
    )
    gemma_calls += 1
    description = str(writer_raw.get("description_md") or "").strip()
    writer_raw["payload_contract"] = writer_payload
    quality_delta = legacy_writer_family.compare_to_baseline(
        baseline_description=str(baseline.get("description_md") or ""),
        candidate_description=description,
        source_excerpt=source_excerpt,
    )
    validation_errors: list[str] = []
    validation_warnings: list[str] = list(quality_delta.get("warnings") or [])
    leaks = _baseline_g4_prompt_leaks(description)
    for leak in leaks:
        validation_errors.append(f"prompt_leak:{leak}")
    if not description:
        validation_errors.append("description.empty")
    if len(description) > max(1400, int((len(str(baseline.get("description_md") or "")) or 650) * 1.25)):
        validation_errors.append(f"length.too_long:{len(description)}")
    if len(description) < 180:
        validation_errors.append(f"length.too_short:{len(description)}")
    if min_description_chars and len(description) < min_description_chars:
        validation_errors.append(f"length.below_min_description_chars:{len(description)}/{min_description_chars}")
    duplicate_word = re.search(r"(?iu)\b([а-яёa-z]{4,})\b[\s,;:—-]+\1\b", description)
    if duplicate_word:
        validation_errors.append(f"text.duplicate_word:{duplicate_word.group(1).lower()}")
    repeated_cluster = re.search(r"(?iu)([а-яё]{2,5})\1{3,}", description)
    if repeated_cluster:
        validation_errors.append(f"text.repeated_cluster:{repeated_cluster.group(1).lower()}")
    english_word = re.search(r"\b[A-Za-z]{3,}\b", description)
    if english_word:
        validation_errors.append(f"text.english_word:{english_word.group(0)}")
    if baseline_len and len(description) < min_description_chars:
        validation_errors.append(f"length.too_short_vs_baseline:{len(description)}/{baseline_len}")
    if re.search(r"(?iu)\b(?:вы сможете|для вас|позволит вам)\b", description):
        validation_errors.append("style.direct_address")
    review_raw = await _ask_gemma_json_direct(
        model=gemma_model,
        system_prompt=_baseline_g4_review_prompt(),
        user_payload={
            "title": fixture.title,
            "event_type": fixture.event_type,
            "source_excerpt": source_excerpt[:3000],
            "public_facts": public_fact_items,
            "logistics_facts": logistics_fact_items[:10],
            "min_description_chars": min_description_chars,
            "target_description_chars": target_description_chars,
            "candidate_chars": len(description),
            "baseline_chars": baseline_len,
            "baseline_description": str(baseline.get("description_md") or ""),
            "candidate_description": description,
        },
        max_tokens=900,
        response_schema=_baseline_g4_review_schema(),
        timeout_sec=min(_gemma_direct_timeout_sec(), 35.0),
        allow_json_repair=False,
    )
    gemma_calls += 1
    review_errors = [str(item).strip() for item in list(review_raw.get("errors") or []) if str(item).strip()]
    review_warnings = [str(item).strip() for item in list(review_raw.get("warnings") or []) if str(item).strip()]
    verdict = str(review_raw.get("verdict") or "").strip().lower()
    validation_errors.extend(f"llm_review:{item}" for item in review_errors)
    validation_warnings.extend(f"llm_review:{item}" for item in review_warnings)
    if verdict == "rejected" and not review_errors:
        validation_errors.append("llm_review:rejected")
    if quality_delta.get("regressions"):
        validation_errors.extend(str(item) for item in quality_delta.get("regressions") or [])

    wall_clock_sec = round(time.perf_counter() - started_at, 6)
    baseline_wall = None
    try:
        baseline_wall = float((baseline.get("timings") or {}).get("wall_clock_sec") or 0.0) or None
    except Exception:
        baseline_wall = None
    speed_ratio = None
    if baseline_wall:
        ratio = wall_clock_sec / baseline_wall
        speed_ratio = {
            "ratio": round(ratio, 4),
            "target": 3.0,
            "pass": ratio <= 3.0,
            "gate": "pass" if ratio <= 3.0 else "latency.3x_exceeded",
        }
        if ratio > 3.0:
            validation_errors.append("latency.3x_exceeded")

    return {
        "gemma_model": gemma_model,
        "variant_mode": "baseline_g4",
        "baseline_g4_mode": "optimized_fact_first_baseline_contract",
        "per_source_facts": per_source,
        "facts_text_clean": facts_text_clean,
        "public_fact_items": public_fact_items,
        "logistics_facts": [str(item.get("text") or "") for item in logistics_fact_items if item.get("text")],
        "logistics_fact_items": logistics_fact_items,
        "lead_hooks": lead_hooks,
        "narrative_angles": narrative_angles,
        "source_warnings": source_warnings,
        "writer_output": writer_raw,
        "llm_review": review_raw,
        "applied_output": {"title": fixture.title, "description_md": description},
        "description_md": description,
        "quality_delta_vs_baseline": quality_delta,
        "validation": {"errors": validation_errors, "warnings": validation_warnings},
        "quality_profile": _quality_profile(description),
        "metrics": _variant_metrics(description),
        "timings": {
            "wall_clock_sec": wall_clock_sec,
            "model_active_sec": round(max(0.0, wall_clock_sec - sleep_sec), 6),
            "sleep_sec": round(sleep_sec, 6),
            "gemma_calls": gemma_calls,
            "four_o_calls": 0,
            "baseline_stage_sec": baseline_wall,
        },
        "speed_ratio_vs_baseline": speed_ratio,
    }


def _extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = (text or "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", cleaned)
        cleaned = cleaned.replace("```", "").strip()
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        data = json.loads(cleaned[start : end + 1])
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None


def _extract_model_text(response: Any) -> str:
    try:
        text = getattr(response, "text", None)
        if isinstance(text, str) and text.strip():
            return text.strip()
    except Exception:
        pass
    parts: list[str] = []
    for candidate in list(getattr(response, "candidates", None) or []):
        content = getattr(candidate, "content", None)
        if content is None and isinstance(candidate, dict):
            content = candidate.get("content")
        if content is None:
            continue
        candidate_parts = getattr(content, "parts", None)
        if candidate_parts is None and isinstance(content, dict):
            candidate_parts = content.get("parts")
        for part in list(candidate_parts or []):
            if getattr(part, "thought", False):
                continue
            if isinstance(part, dict) and part.get("thought"):
                continue
            text = getattr(part, "text", None)
            if text is None and isinstance(part, dict):
                text = part.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts).strip()


def _gemma_model_name(model: str) -> str:
    raw = (model or "").strip()
    return raw if raw.startswith("models/") else f"models/{raw}"


def _gemma_direct_timeout_sec() -> float:
    raw = (os.getenv("LOLLIPOP_GEMMA_DIRECT_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            return max(10.0, float(raw))
        except Exception:
            pass
    return DEFAULT_GEMMA_DIRECT_TIMEOUT_SEC


def _gemma_writer_timeout_sec() -> float:
    raw = (os.getenv("LOLLIPOP_GEMMA_WRITER_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            return max(5.0, float(raw))
        except Exception:
            pass
    return DEFAULT_GEMMA_WRITER_TIMEOUT_SEC


def _quota_retry_delay_seconds(message: str) -> float | None:
    match = re.search(r"retry in ([0-9]+(?:\.[0-9]+)?)s", message, re.I)
    if match:
        return float(match.group(1)) + 1.0
    if "quota exceeded" in message.lower() or "resource_exhausted" in message.lower():
        return 45.0
    return None


async def _ask_gemma_json_direct(
    *,
    model: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    max_tokens: int,
    response_schema: dict[str, Any] | None = None,
    timeout_sec: float | None = None,
    allow_json_repair: bool = True,
) -> dict[str, Any]:
    api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY2") or "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is missing")
    genai = _legacy_genai()
    genai.configure(api_key=api_key)
    prompt_json = json.dumps(user_payload, ensure_ascii=False, indent=2)
    model_name = _gemma_model_name(model)
    timeout_sec = timeout_sec or _gemma_direct_timeout_sec()
    async def _invoke(system_text: str, user_text: str, *, override_max_tokens: int | None, use_system_instruction: bool) -> Any:
        generation_config = {
            "temperature": 0,
            "max_output_tokens": override_max_tokens or max_tokens,
            "response_mime_type": "application/json",
        }
        if response_schema is not None:
            generation_config["response_schema"] = response_schema
        if use_system_instruction:
            return await asyncio.wait_for(
                genai.GenerativeModel(
                    model_name=model_name,
                    system_instruction=system_text.strip(),
                ).generate_content_async(
                    user_text,
                    generation_config=generation_config,
                ),
                timeout=timeout_sec,
            )
        return await asyncio.wait_for(
            genai.GenerativeModel(model_name=model_name).generate_content_async(
                "SYSTEM:\n"
                + system_text.strip()
                + "\n\nUSER:\n"
                + user_text
                + "\n\nReturn only valid JSON.",
                generation_config=generation_config,
            ),
            timeout=timeout_sec,
        )

    async def _generate(system_text: str, user_text: str, *, override_max_tokens: int | None = None) -> str:
        use_system_instruction = True
        last_error: Exception | None = None
        for _attempt in range(3):
            try:
                response = await _invoke(
                    system_text,
                    user_text,
                    override_max_tokens=override_max_tokens,
                    use_system_instruction=use_system_instruction,
                )
                return _extract_model_text(response)
            except Exception as exc:
                last_error = exc
                lower = str(exc).lower()
                if use_system_instruction and (
                    "developer instruction is not enabled" in lower or "system instruction" in lower
                ):
                    use_system_instruction = False
                    continue
                retry_delay = _quota_retry_delay_seconds(str(exc))
                if retry_delay is None and (
                    "internal error encountered" in lower
                    or "statuscode.internal" in lower
                    or "500 internal" in lower
                ):
                    retry_delay = 5.0
                if retry_delay is not None:
                    await asyncio.sleep(retry_delay)
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Gemma call failed without explicit error for {model}")

    raw = await _generate(system_prompt, prompt_json)
    data = _extract_json_object(raw)
    if data is not None:
        return data
    if not allow_json_repair:
        raise RuntimeError(f"Invalid JSON from {model}: {raw[:1200]}")
    repair_payload = json.dumps(
        {
            "stage_contract": system_prompt,
            "previous_response": raw[:8000],
            "task": "Extract the intended structured answer and return one valid JSON object only. No prose. No bullets. No markdown fences.",
        },
        ensure_ascii=False,
        indent=2,
    )
    repaired_raw = await _generate(
        "JSON repair mode. Return one valid JSON object only.",
        repair_payload,
        override_max_tokens=max(max_tokens + 800, 2200),
    )
    repaired = _extract_json_object(repaired_raw)
    if repaired is None:
        raise RuntimeError(f"Invalid JSON from {model}: {raw[:1200]}")
    return repaired


async def _ask_gemma_text_direct(
    *,
    model: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    max_tokens: int,
    timeout_sec: float | None = None,
) -> str:
    api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY2") or "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is missing")
    genai = _legacy_genai()
    genai.configure(api_key=api_key)
    prompt_json = json.dumps(user_payload, ensure_ascii=False, indent=2)
    model_name = _gemma_model_name(model)
    timeout_sec = timeout_sec or _gemma_direct_timeout_sec()

    async def _invoke(system_text: str, user_text: str, *, use_system_instruction: bool) -> Any:
        generation_config = {
            "temperature": 0,
            "max_output_tokens": max_tokens,
        }
        if use_system_instruction:
            return await asyncio.wait_for(
                genai.GenerativeModel(
                    model_name=model_name,
                    system_instruction=system_text.strip(),
                ).generate_content_async(user_text, generation_config=generation_config),
                timeout=timeout_sec,
            )
        return await asyncio.wait_for(
            genai.GenerativeModel(model_name=model_name).generate_content_async(
                "SYSTEM:\n" + system_text.strip() + "\n\nUSER:\n" + user_text,
                generation_config=generation_config,
            ),
            timeout=timeout_sec,
        )

    use_system_instruction = True
    last_error: Exception | None = None
    for _attempt in range(3):
        try:
            response = await _invoke(system_prompt, prompt_json, use_system_instruction=use_system_instruction)
            return _extract_model_text(response).strip()
        except Exception as exc:
            last_error = exc
            lower = str(exc).lower()
            if use_system_instruction and (
                "developer instruction is not enabled" in lower or "system instruction" in lower
            ):
                use_system_instruction = False
                continue
            retry_delay = _quota_retry_delay_seconds(str(exc))
            if retry_delay is None and (
                "internal error encountered" in lower
                or "statuscode.internal" in lower
                or "500 internal" in lower
            ):
                retry_delay = 5.0
            if retry_delay is not None:
                await asyncio.sleep(retry_delay)
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Gemma text call failed without explicit error for {model}")


def _extract_prompt_system() -> str:
    return textwrap.dedent(
        """
        You are a fact extractor for a cultural event digest pipeline.
        You do one step: facts.extract.multi_source.v1.

        CONTRACT
        - Extract grounded facts from the provided source excerpts.
        - Every fact must trace back to at least one source_id.
        - Do not synthesize, bridge, or smooth conflicts across sources.
        - Preserve meaningful detail: repertoire, named performers with roles, concert framing, contextual lines.
        - If a fact appears in multiple sources with compatible wording, emit one fact with all relevant source_refs.
        - Literal repertoire items must be preserved in literal_items when the source gives an actual program list.
        - Do not emit logistics here: date, time, venue, address, tickets, prices, URLs.
        - Return all fact texts in Russian. Do not translate the sources into English.
        - Do not emit broken or truncated tail fragments. If a list item is cut mid-name, omit the broken tail.

        OUTPUT JSON
        {
          "facts": [
            {
              "bucket": "event_core|program_list|people_and_roles|support_context|forward_looking",
              "text": "string",
              "literal_items": ["string"],
              "source_refs": ["site|tg|vk"],
              "confidence": "high|medium|low"
            }
          ]
        }
        """
    ).strip()


def _prioritize_prompt_system() -> str:
    return textwrap.dedent(
        """
        You are a salience ranker for a cultural event digest pipeline.
        You do one step: facts.prioritize.v1.

        CONTRACT
        - Rank every input fact by editorial salience for a short public event description.
        - Select exactly one lead_fact_id.
        - Optionally select one lead_support_id.
        - Assign weight: high|medium|low.
        - Assign narrative_policy: include|suppress.
        - Lead must explain what happens at the event, not describe a project or person in isolation.
        - Bad openings: cast trivia, biography, background-only, "project represents...".
        - Good openings: event action plus the main substance of the evening.
        - For concert and repertoire-heavy cases, program_list facts usually remain include/high, but lead should still feel event-facing.
        - Suppress only generic filler or duplicate residue.
        - Do not rewrite facts. Do not generate prose.
        - Use every input fact exactly once in the facts array.

        OUTPUT JSON
        {
          "lead_fact_id": "fact_id",
          "lead_support_id": "fact_id|null",
          "facts": [
            {
              "fact_id": "fact_id",
              "weight": "high|medium|low",
              "narrative_policy": "include|suppress"
            }
          ]
        }
        """
    ).strip()


def _layout_prompt_system() -> str:
    return textwrap.dedent(
        """
        You are a structure planner for a cultural event digest pipeline.
        You do one step: editorial.layout.plan.v1.

        CONTRACT
        - Plan the block structure of the final text from the prioritized fact pack.
        - Do not write prose. Do not paraphrase facts.
        - Lead is always first.
        - Infoblock is always last and will contain deterministic logistics only.
        - Program block is separate when program facts exist.
        - Every fact_id from all_fact_ids must appear exactly once across blocks.
        - Use heading only on body/program blocks, only when short and factual, and only when allow_semantic_headings is true.
        - If non_logistics_total >= 4 and there is more than one thematic cluster, prefer at least one semantic split instead of one long body blob.
        - Avoid generic headings like "О событии", "Подробности", "Основная идея".

        OUTPUT JSON
        {
          "title_strategy": "keep|enhance",
          "title_hint_ref": "fact_id|null",
          "blocks": [
            {
              "role": "lead|body|program|infoblock",
              "fact_refs": ["fact_id"],
              "style": "narrative|list|structured",
              "heading": "string|null"
            }
          ]
        }
        """
    ).strip()


def _slug_prefix(bucket: str) -> str:
    return {
        "event_core": "EC",
        "program_list": "PL",
        "people_and_roles": "PR",
        "support_context": "SC",
        "forward_looking": "FL",
    }[bucket]


def _normalize_extracted_facts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    allowed_buckets = {"event_core", "program_list", "people_and_roles", "support_context", "forward_looking"}
    facts: list[dict[str, Any]] = []
    counters: dict[str, int] = {}
    for raw_item in list(payload.get("facts") or []):
        if not isinstance(raw_item, dict):
            continue
        bucket = str(raw_item.get("bucket") or "").strip()
        text = re.sub(r"\s+", " ", str(raw_item.get("text") or "")).strip()
        if bucket not in allowed_buckets or not text:
            continue
        if re.search(r"(?i)\b(the|concert|program|described|magical|atmosphere|light|love)\b", text):
            continue
        if text.endswith(",") or re.search(r"(?iu)[:,;]\s*[А-ЯA-Z][а-яa-z]{0,3}\.?$", text):
            continue
        counters[bucket] = counters.get(bucket, 0) + 1
        facts.append(
            {
                "fact_id": f"{_slug_prefix(bucket)}{counters[bucket]:02d}",
                "bucket": bucket,
                "text": text,
                "literal_items": [
                    re.sub(r"\s+", " ", str(item or "")).strip()
                    for item in list(raw_item.get("literal_items") or [])
                    if re.sub(r"\s+", " ", str(item or "")).strip()
                ],
                "source_refs": [
                    str(item).strip()
                    for item in list(raw_item.get("source_refs") or [])
                    if str(item).strip()
                ],
                "confidence": str(raw_item.get("confidence") or "medium").strip() or "medium",
            }
        )
    return facts


def _validate_extract(fixture: BenchmarkFixture, facts: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    source_hits: dict[str, int] = {source.source_id: 0 for source in fixture.sources}
    seen_ids: set[str] = set()
    for fact in facts:
        fact_id = fact["fact_id"]
        if fact_id in seen_ids:
            errors.append(f"duplicate_fact_id:{fact_id}")
        seen_ids.add(fact_id)
        for ref in fact.get("source_refs") or []:
            if ref in source_hits:
                source_hits[ref] += 1
    if not 4 <= len(facts) <= 20:
        errors.append(f"fact_count_out_of_band:{len(facts)}")
    missing_sources = sorted(source_id for source_id, hits in source_hits.items() if hits == 0)
    if missing_sources:
        errors.append(f"missing_source_coverage:{','.join(missing_sources)}")
    if any(re.search(r"(?i)\b(the|concert|program|described|magical|atmosphere|light|love)\b", fact["text"]) for fact in facts):
        errors.append("english_leak_detected")
    return errors


def _apply_priorities(
    extracted_facts: list[dict[str, Any]],
    prioritize_payload: dict[str, Any],
) -> dict[str, Any]:
    fact_map = {item["fact_id"]: dict(item) for item in extracted_facts}
    annotated: list[dict[str, Any]] = []
    used: set[str] = set()
    for item in list(prioritize_payload.get("facts") or []):
        if not isinstance(item, dict):
            continue
        fact_id = str(item.get("fact_id") or "").strip()
        if fact_id not in fact_map or fact_id in used:
            continue
        used.add(fact_id)
        annotated.append(
            dict(
                fact_map[fact_id],
                weight=str(item.get("weight") or "medium").strip() or "medium",
                narrative_policy=str(item.get("narrative_policy") or "include").strip() or "include",
            )
        )
    for fact in extracted_facts:
        if fact["fact_id"] not in used:
            annotated.append(dict(fact, weight="medium", narrative_policy="include"))
    lead_fact_id = str(prioritize_payload.get("lead_fact_id") or "").strip()
    lead_support_id = str(prioritize_payload.get("lead_support_id") or "").strip() or None
    if lead_fact_id not in {item["fact_id"] for item in annotated}:
        preferred = [item["fact_id"] for item in annotated if item["bucket"] in {"event_core", "forward_looking", "program_list"}]
        lead_fact_id = preferred[0] if preferred else (annotated[0]["fact_id"] if annotated else "")
    if lead_support_id == lead_fact_id:
        lead_support_id = None
    if lead_support_id and lead_support_id not in {item["fact_id"] for item in annotated}:
        lead_support_id = None
    fact_id_map = {item["fact_id"]: item for item in annotated}
    if (
        lead_support_id
        and lead_support_id in fact_id_map
        and fact_id_map[lead_support_id]["bucket"] == "program_list"
        and not fact_id_map[lead_support_id].get("literal_items")
        and any(item["bucket"] == "program_list" and item.get("literal_items") for item in annotated)
    ):
        replacement = next(
            (
                item["fact_id"]
                for item in annotated
                if item["fact_id"] != lead_fact_id and item["bucket"] == "event_core"
            ),
            None,
        )
        if replacement:
            lead_support_id = replacement
    return {
        "lead_fact_id": lead_fact_id,
        "lead_support_id": lead_support_id,
        "facts": annotated,
    }


def _precompute_layout(prioritized_facts: list[dict[str, Any]]) -> dict[str, Any]:
    included = [item for item in prioritized_facts if item.get("narrative_policy") != "suppress"]
    non_logistics_total = len(included)
    has_long_program = any(item["bucket"] == "program_list" and item.get("literal_items") for item in included)
    body_candidates = [item for item in included if item["bucket"] != "program_list"]
    body_cluster_count = len({item["bucket"] for item in body_candidates})
    return {
        "density": "rich" if non_logistics_total >= 6 else "standard" if non_logistics_total >= 4 else "minimal",
        "has_long_program": has_long_program,
        "non_logistics_total": non_logistics_total,
        "body_cluster_count": body_cluster_count,
        "body_block_floor": 2 if body_cluster_count >= 2 and non_logistics_total >= 5 else 1,
        "multi_body_split_recommended": body_cluster_count >= 2 and non_logistics_total >= 5,
        "title_is_bare": False,
        "title_needs_format_anchor": False,
        "allow_semantic_headings": non_logistics_total >= 4,
        "heading_guardrail_recommended": non_logistics_total >= 5,
        "all_fact_ids": [item["fact_id"] for item in included],
    }


def _clean_layout_heading(value: Any) -> str | None:
    heading = re.sub(r"\s+", " ", str(value or "")).strip()
    if not heading:
        return None
    if len(heading) < 3:
        return None
    if not re.search(r"[A-Za-zА-Яа-яЁё0-9]", heading):
        return None
    if heading.casefold() in {"о событии", "подробности", "основная идея", "что будет"}:
        return None
    return heading


def _clean_layout_plan(prioritized: dict[str, Any], precompute: dict[str, Any], raw_layout: dict[str, Any]) -> dict[str, Any]:
    included = [item for item in prioritized["facts"] if item.get("narrative_policy") != "suppress"]
    included_ids = [item["fact_id"] for item in included]
    fact_map = {item["fact_id"]: item for item in included}
    lead_ids = [prioritized["lead_fact_id"]]
    if prioritized.get("lead_support_id") and prioritized["lead_support_id"] in fact_map and prioritized["lead_support_id"] not in lead_ids:
        lead_ids.append(prioritized["lead_support_id"])

    used: set[str] = set(lead_ids)
    cleaned_blocks: list[dict[str, Any]] = [{"role": "lead", "fact_refs": lead_ids, "style": "narrative", "heading": None}]
    raw_blocks = [block for block in list(raw_layout.get("blocks") or []) if isinstance(block, dict)]

    def _take_ids(role: str, pool: list[str]) -> tuple[list[str], str | None]:
        heading: str | None = None
        picked: list[str] = []
        for block in raw_blocks:
            if str(block.get("role") or "").strip() != role:
                continue
            if role in {"lead", "infoblock"}:
                continue
            heading_value = _clean_layout_heading(block.get("heading"))
            if role in {"body", "program"} and not precompute["allow_semantic_headings"]:
                heading_value = None
            if heading is None:
                heading = heading_value
            for fact_id in list(block.get("fact_refs") or []):
                if fact_id in pool and fact_id not in used and fact_id not in picked:
                    picked.append(fact_id)
        return picked, heading

    program_pool = [fact_id for fact_id in included_ids if fact_map[fact_id]["bucket"] == "program_list" and fact_id not in used]
    body_pool = [fact_id for fact_id in included_ids if fact_id not in used and fact_id not in program_pool]

    if program_pool:
        program_ids, program_heading = _take_ids("program", program_pool)
        for fact_id in program_pool:
            if fact_id not in program_ids:
                program_ids.append(fact_id)
        used.update(program_ids)
        cleaned_blocks.append(
            {"role": "program", "fact_refs": program_ids, "style": "list", "heading": program_heading}
        )

    model_body_blocks = [block for block in raw_blocks if str(block.get("role") or "").strip() == "body"]
    temp_body_blocks: list[dict[str, Any]] = []
    for block in model_body_blocks:
        block_ids: list[str] = []
        for fact_id in list(block.get("fact_refs") or []):
            if fact_id in body_pool and fact_id not in used and fact_id not in block_ids:
                block_ids.append(fact_id)
        if block_ids:
            used.update(block_ids)
            temp_body_blocks.append(
                {
                    "role": "body",
                    "fact_refs": block_ids,
                    "style": "narrative",
                    "heading": _clean_layout_heading(block.get("heading")) if precompute["allow_semantic_headings"] else None,
                }
            )

    remaining_body = [fact_id for fact_id in body_pool if fact_id not in used]
    if remaining_body:
        temp_body_blocks.append({"role": "body", "fact_refs": remaining_body, "style": "narrative", "heading": None})

    # Deterministic carry: single-fact event_core body blocks are usually stronger as lead support
    # than as their own micro-section.
    if temp_body_blocks:
        first_body = temp_body_blocks[0]
        if (
            len(first_body["fact_refs"]) == 1
            and fact_map[first_body["fact_refs"][0]]["bucket"] == "event_core"
            and len(cleaned_blocks[0]["fact_refs"]) < 2
        ):
            cleaned_blocks[0]["fact_refs"].append(first_body["fact_refs"][0])
            temp_body_blocks = temp_body_blocks[1:]

    cleaned_blocks.extend(temp_body_blocks)
    cleaned_blocks.append({"role": "infoblock", "fact_refs": [item["fact_id"] for item in _compose_infoblock_rows()], "style": "structured", "heading": None})

    title_strategy = "enhance" if str(raw_layout.get("title_strategy") or "").strip() == "enhance" else "keep"
    title_hint_ref = str(raw_layout.get("title_hint_ref") or "").strip() or None
    if title_strategy == "keep":
        title_hint_ref = None
    if title_hint_ref and title_hint_ref not in included_ids:
        title_hint_ref = None
        title_strategy = "keep"
    return {"title_strategy": title_strategy, "title_hint_ref": title_hint_ref, "blocks": cleaned_blocks}


def _compose_infoblock_rows() -> list[dict[str, str]]:
    return [
        {"fact_id": "LG01", "label": "Дата"},
        {"fact_id": "LG02", "label": "Время"},
        {"fact_id": "LG03", "label": "Локация"},
        {"fact_id": "LG04", "label": "Город"},
    ]


def _compose_infoblock(fixture: BenchmarkFixture) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    rows = _compose_infoblock_rows()
    if fixture.date:
        items.append(dict(rows[0], value=fixture.date))
    if fixture.time:
        items.append(dict(rows[1], value=fixture.time))
    if fixture.location_name:
        items.append(dict(rows[2], value=fixture.location_name))
    if fixture.city and fixture.city != fixture.location_name:
        items.append(dict(rows[3], value=fixture.city))
    return items


def _fact_priority(weight: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(weight, 2)


def _compose_writer_pack(
    fixture: BenchmarkFixture,
    *,
    prioritized: dict[str, Any],
    layout_payload: dict[str, Any],
) -> dict[str, Any]:
    fact_catalog = {item["fact_id"]: item for item in prioritized["facts"] if item.get("narrative_policy") != "suppress"}
    sections: list[dict[str, Any]] = []
    for block in list(layout_payload.get("blocks") or []):
        role = str(block.get("role") or "").strip()
        if role == "infoblock":
            continue
        fact_ids = [fact_id for fact_id in list(block.get("fact_refs") or []) if fact_id in fact_catalog]
        if not fact_ids:
            continue
        facts = []
        literal_items: list[str] = []
        literal_item_source_fact_ids: list[str] = []
        coverage_plan: list[dict[str, str]] = []
        partial = False
        for fact_id in fact_ids:
            item = fact_catalog[fact_id]
            if item.get("literal_items"):
                literal_items.extend(list(item["literal_items"]))
                literal_item_source_fact_ids.append(fact_id)
                coverage_plan.append({"fact_id": fact_id, "mode": "literal_list" if role == "program" else "narrative_plus_literal_list"})
                if re.search(r"(?iu)\b(и другие|и др\.?|среди них|в том числе)\b", item["text"]):
                    partial = True
                if role != "program":
                    facts.append({"fact_id": fact_id, "text": item["text"], "priority": _fact_priority(item["weight"])})
                continue
            coverage_plan.append({"fact_id": fact_id, "mode": "narrative"})
            facts.append({"fact_id": fact_id, "text": item["text"], "priority": _fact_priority(item["weight"])})
        sections.append(
            {
                "role": role,
                "style": str(block.get("style") or "narrative"),
                "heading": block.get("heading"),
                "fact_ids": fact_ids,
                "facts": facts,
                "coverage_plan": coverage_plan,
                "literal_items": literal_items,
                "literal_item_source_fact_ids": literal_item_source_fact_ids,
                "literal_list_is_partial": partial,
            }
        )
    headings = [str(section["heading"]).strip() for section in sections if section.get("heading")]
    infoblock = _compose_infoblock(fixture)
    return {
        "event_type": fixture.event_type,
        "title_context": {
            "original_title": fixture.title,
            "strategy": str(layout_payload.get("title_strategy") or "keep"),
            "hint_fact_id": layout_payload.get("title_hint_ref"),
            "hint_fact_text": fact_catalog.get(str(layout_payload.get("title_hint_ref") or ""), {}).get("text"),
            "is_bare": False,
        },
        "sections": sections,
        "infoblock": infoblock,
        "constraints": {
            "must_cover_fact_ids": [item["fact_id"] for item in prioritized["facts"] if item.get("narrative_policy") != "suppress"],
            "infoblock_fact_ids": [item["fact_id"] for item in infoblock],
            "headings": headings,
            "list_required": any(section.get("literal_items") for section in sections),
            "no_logistics_in_narrative": True,
        },
    }


def _build_writer_prompt(pack: dict[str, Any]) -> str:
    structure_lines: list[str] = []
    for section in pack["sections"]:
        structure_lines.append(
            f"- role={section['role']}, style={section['style']}, heading={json.dumps(section.get('heading'), ensure_ascii=False)}"
        )
    return textwrap.dedent(
        f"""
        Ты — writer.final_4o.v1 для lollipop.

        Верни только JSON:
        {{
          "title": "string",
          "description_md": "string"
        }}

        Правила:
        - Иди по sections строго по порядку.
        - Первый абзац — короткий lead на 1-2 предложения.
        - На каждой границе section начинается новый абзац.
        - Заголовки разрешены только как exact `### ...` из pack, без новых headings.
        - literal_items выводи markdown bullets `- item` без перефразирования.
        - Если literal_list_is_partial=true, явно подай список как примеры, а не как полный перечень.
        - Не добавляй новых фактов, CTA, атмосферный filler и логистику из infoblock.
        - Не повторяй дату/время/локацию/город/адрес/цены/ссылки в description_md.
        - Плохие opening patterns запрещены: `Режиссёр фильма — ...`, `Проект представляет собой ...`, `В главных ролях ...`, если это не объясняет формат события.
        - Стиль: живой, сдержанный русский культурный дайджест, а не карточка.

        СТРУКТУРА:
        {chr(10).join(structure_lines)}

        PACK JSON:
        {json.dumps(pack, ensure_ascii=False, indent=2)}
        """
    ).strip()


def _four_o_token() -> str:
    token = (os.getenv("FOUR_4O_TOKEN") or os.getenv("FOUR_O_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("FOUR_4O_TOKEN is missing")
    return token


def _to_openai_schema(node: Any) -> Any:
    """Convert a Gemma-style response_schema (uppercase types) into the
    lowercase JSON-Schema shape OpenAI's structured-output API accepts."""
    if isinstance(node, dict):
        result: dict[str, Any] = {}
        for key, value in node.items():
            if key == "type" and isinstance(value, str):
                result[key] = value.lower()
            else:
                result[key] = _to_openai_schema(value)
        return result
    if isinstance(node, list):
        return [_to_openai_schema(item) for item in node]
    return node


def _openai_429_retry_delay(response: requests.Response, *, attempt: int) -> float:
    raw = (response.headers.get("retry-after") or "").strip()
    if raw:
        try:
            return max(0.0, min(90.0, float(raw)))
        except Exception:
            pass
    base = float(os.getenv("LOLLIPOP_4O_RETRY_BASE_SEC", "4") or "4")
    delay = base * (2 ** max(0, attempt - 1))
    return max(1.0, min(90.0, delay + random.uniform(0.0, 1.5)))


def _openai_error_summary(response: requests.Response) -> str:
    """Compact error details for benchmark artifacts.

    OpenAI 429 can mean RPM/TPM/RPD/TPD rate limits or account/project quota.
    The HTTP status alone is not enough for debugging, so preserve the structured
    body and rate-limit headers without exposing the API token.
    """
    status = int(getattr(response, "status_code", 0) or 0)
    err_type = ""
    err_code = ""
    err_message = ""
    try:
        payload = response.json()
        err = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(err, dict):
            err_type = str(err.get("type") or "").strip()
            err_code = str(err.get("code") or "").strip()
            err_message = re.sub(r"\s+", " ", str(err.get("message") or "")).strip()
    except Exception:
        err_message = re.sub(r"\s+", " ", str(response.text or "")).strip()
    header_names = [
        "retry-after",
        "x-ratelimit-limit-requests",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-reset-requests",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-reset-tokens",
    ]
    headers = {
        name: response.headers.get(name)
        for name in header_names
        if response.headers.get(name) is not None
    }
    parts = [f"status={status}"]
    if err_type:
        parts.append(f"type={err_type}")
    if err_code:
        parts.append(f"code={err_code}")
    if err_message:
        parts.append(f"message={err_message[:500]}")
    if headers:
        parts.append(f"headers={json.dumps(headers, ensure_ascii=False, sort_keys=True)}")
    return "openai_http_error:" + " ".join(parts)


async def _ask_4o_json(*, prompt: str, schema: dict[str, Any], model: str, system_prompt: str | None = None, max_tokens: int = 1600) -> dict[str, Any]:
    token = _four_o_token()
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": max_tokens,
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "LollipopFinalWriter", "schema": _to_openai_schema(schema)},
        },
        "messages": [
            {"role": "system", "content": system_prompt or "Return only valid JSON for the requested schema."},
            {"role": "user", "content": prompt},
        ],
    }
    max_retries = max(0, int(os.getenv("LOLLIPOP_4O_MAX_RETRIES", "2") or "2"))
    response: requests.Response | None = None
    for attempt in range(1, max_retries + 2):
        response = await asyncio.to_thread(
            requests.post,
            os.getenv("FOUR_O_URL", "https://api.openai.com/v1/chat/completions"),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        if response.status_code < 400:
            break
        summary = _openai_error_summary(response)
        if (
            response.status_code == 429
            and "insufficient_quota" not in summary
            and attempt <= max_retries
        ):
            await asyncio.sleep(_openai_429_retry_delay(response, attempt=attempt))
            continue
        raise RuntimeError(summary)
    if response is None:
        raise RuntimeError("openai_http_error:no_response")
    raw = response.json().get("choices", [{}])[0].get("message", {}).get("content") or "{}"
    parsed = _extract_json_object(raw)
    if parsed is None:
        raise RuntimeError(f"Invalid 4o JSON: {raw[:1200]}")
    return parsed


def _validate_writer_output(pack: dict[str, Any], output: dict[str, Any]) -> dict[str, list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    description = str(output.get("description_md") or "")
    title = str(output.get("title") or "").strip()
    if pack["title_context"]["strategy"] == "keep" and title != pack["title_context"]["original_title"]:
        warnings.append("title.keep_overridden_by_model")
    expected_headings = [heading for heading in pack["constraints"]["headings"] if heading]
    actual_headings = [match.group(1).strip() for match in re.finditer(r"(?m)^###\s+(.+?)\s*$", description)]
    for heading in actual_headings:
        if heading not in expected_headings:
            errors.append(f"heading.invented:{heading}")
    for item in expected_headings:
        if item not in actual_headings:
            warnings.append(f"heading.missing:{item}")
    for section in pack["sections"]:
        for item in section.get("literal_items") or []:
            if not re.search(rf"(?m)^\-\s+{re.escape(item)}\s*$", description):
                errors.append(f"literal.missing_or_mutated:{item}")
    for row in pack["infoblock"]:
        value = row["value"]
        if row["label"] in {"Дата", "Время", "Локация", "Город"} and value and value in description:
            errors.append(f"infoblock.leak:{row['fact_id']}")
    return {"errors": errors, "warnings": warnings}


def _variant_metrics(description: str | None) -> dict[str, Any]:
    text = str(description or "")
    return {
        "chars": len(text),
        "headings": len(re.findall(r"(?m)^###\s+\S", text)),
        "bullets": len(re.findall(r"(?m)^\-\s+\S", text)),
        "paragraphs": len([part for part in re.split(r"\n\s*\n", text) if part.strip()]),
    }


def _quality_profile(description: str | None) -> dict[str, Any]:
    return writer_final_family._describe_text_quality(str(description or ""))


def _timing_summary(payload: dict[str, Any]) -> dict[str, Any] | None:
    timings = payload.get("timings") if isinstance(payload, dict) else None
    if not isinstance(timings, dict) or not timings:
        return None
    family_sec = dict(timings.get("stage_family_sec") or {})
    top_families = sorted(family_sec.items(), key=lambda item: (-float(item[1]), item[0]))[:6]
    return {
        "wall_clock_sec": timings.get("wall_clock_sec"),
        "model_active_sec": timings.get("model_active_sec"),
        "sleep_sec": timings.get("sleep_sec"),
        "gemma_calls": timings.get("gemma_calls"),
        "four_o_calls": timings.get("four_o_calls"),
        "top_stage_families": top_families,
    }


def _load_reused_variant_payload(
    path_value: str | None,
    *,
    fixture_id: str,
    section: str,
) -> dict[str, Any] | None:
    if not path_value:
        return None
    path = Path(path_value)
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        for row in list(data.get("results") or []):
            if not isinstance(row, dict):
                continue
            fixture = row.get("fixture") if isinstance(row.get("fixture"), dict) else {}
            row_fixture_id = str(fixture.get("fixture_id") or "").strip()
            if row_fixture_id != fixture_id:
                continue
            payload = row.get(section)
            if isinstance(payload, dict):
                return payload
        raise RuntimeError(f"{section} fixture {fixture_id} not found in {path}")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unsupported reuse artifact shape in {path}")
    if section == "baseline" and "baseline_mode" in data:
        return data
    if section in {"lollipop", "lollipop_g4", "lollipop_legacy"} and "applied_output" in data:
        payload = dict(data)
        payload.setdefault("metrics", _variant_metrics(payload.get("applied_output", {}).get("description_md")))
        return payload
    raise RuntimeError(f"Unsupported {section} reuse artifact shape in {path}")


async def _run_lollipop_variant(
    fixture: BenchmarkFixture,
    *,
    gemma_model: str,
    four_o_model: str,
    gemma_call_gap_s: float,
) -> dict[str, Any]:
    print(f"[benchmark] {fixture.fixture_id} start full-cascade upstream={gemma_model}", file=sys.stderr, flush=True)
    result = await run_full_cascade_variant(
        fixture=fixture,
        gemma_model=gemma_model,
        gemma4="gemma-4" in gemma_model,
        gemma_json_call=_ask_gemma_json_direct,
        four_o_json_call=_ask_4o_json,
        sleep=_gemma_gap_sleep,
        four_o_model=four_o_model,
        gemma_call_gap_s=gemma_call_gap_s,
    )
    result["metrics"] = _variant_metrics(result["applied_output"].get("description_md"))
    return result


def _baseline_fact_list_for_review(baseline: dict[str, Any]) -> list[str]:
    """Flatten Gemma 3 baseline per-source facts for reviewer comparison only.

    The flat list is used in the benchmark fact-coverage reviewer payload, which
    is read-only. It must never be passed back into the legacy generation
    payloads (extractor / writer / 4o fallback).
    """
    facts: list[str] = []
    for _source_id, source_facts in dict(baseline.get("per_source_facts") or {}).items():
        for fact in list(source_facts or []):
            text = re.sub(r"\s+", " ", str(fact or "")).strip()
            if text and text not in facts:
                facts.append(text)
    return facts


def _baseline_fact_surfaces_for_review(
    baseline: dict[str, Any],
    fixture: BenchmarkFixture,
) -> dict[str, list[str]]:
    """Expose every baseline fact surface used by the benchmark.

    `raw_extracted_facts` are the Gemma 3 extractor output. `writer_facts_text_clean`
    is the filtered list actually passed to the baseline writer. Metadata/anchors
    are not extractor facts, but they are visible to the baseline benchmark path
    and must be shown so the report is auditable by eye.
    """

    raw_facts = _baseline_fact_list_for_review(baseline)
    writer_facts: list[str] = []
    for fact in list(baseline.get("facts_text_clean") or []):
        text = re.sub(r"\s+", " ", str(fact or "")).strip()
        if text and text not in writer_facts:
            writer_facts.append(text)
    writer_set = set(writer_facts)
    filtered_out = [fact for fact in raw_facts if fact not in writer_set]

    metadata_pairs = [
        ("title", fixture.title),
        ("event_type", fixture.event_type),
        ("date", fixture.date),
        ("time", fixture.time),
        ("location_name", fixture.location_name),
        ("location_address", fixture.location_address),
        ("city", fixture.city),
    ]
    metadata_facts = [
        f"{key}: {value}"
        for key, value in metadata_pairs
        if str(value or "").strip()
    ]
    return {
        "raw_extracted_facts": raw_facts,
        "writer_facts_text_clean": writer_facts,
        "filtered_out_before_writer": filtered_out,
        "metadata_anchors": metadata_facts,
    }


async def _run_fact_coverage_reviewer(
    *,
    fixture: BenchmarkFixture,
    baseline: dict[str, Any],
    public_facts: list[dict[str, Any]],
    logistics_facts: list[dict[str, Any]],
    source_excerpt: str,
    gemma_model: str,
) -> dict[str, Any]:
    """Run an LLM-first fact-coverage reviewer on (baseline, Gemma 4) facts.

    Benchmark-only stage. Baseline facts are allowed in this payload because the
    reviewer is read-only; the legacy generation payloads (extractor / writer /
    4o fallback) continue to receive no baseline text/facts.
    """
    baseline_surfaces = _baseline_fact_surfaces_for_review(baseline, fixture)
    baseline_facts = baseline_surfaces["raw_extracted_facts"]
    gemma_calls = 0
    errors: list[str] = []
    review_payload_user = legacy_writer_family.build_fact_coverage_payload(
        title=fixture.title,
        event_type=fixture.event_type,
        date=fixture.date,
        time=fixture.time,
        location_name=fixture.location_name,
        location_address=fixture.location_address,
        city=fixture.city,
        source_excerpt=source_excerpt,
        baseline_facts=baseline_facts,
        baseline_writer_facts=baseline_surfaces["writer_facts_text_clean"],
        baseline_metadata_facts=baseline_surfaces["metadata_anchors"],
        g4_public_facts=public_facts,
        g4_logistics_facts=logistics_facts,
    )
    baseline_count = len(review_payload_user["baseline_facts"])
    g4_count = len(review_payload_user["g4_facts"])
    if baseline_count == 0 and g4_count == 0:
        normalized = legacy_writer_family.normalize_fact_coverage_payload(
            {
                "baseline_facts_review": [],
                "g4_facts_review": [],
                "coverage_summary": {
                    "public_coverage_status": "unknown",
                    "logistics_coverage_status": "unknown",
                    "named_entity_coverage_status": "unknown",
                    "format_topic_program_coverage_status": "unknown",
                    "overall_verdict": "unknown",
                    "verdict_reason": "no facts on either side",
                },
            },
            baseline_count=0,
            g4_count=0,
            baseline_facts=review_payload_user["baseline_facts"],
            g4_facts=review_payload_user["g4_facts"],
        )
        summary = legacy_writer_family.summarize_fact_coverage(normalized)
        summary.update(
            {
                "baseline_raw_extracted_fact_count": len(baseline_surfaces["raw_extracted_facts"]),
                "baseline_writer_fact_count": len(baseline_surfaces["writer_facts_text_clean"]),
                "baseline_filtered_out_fact_count": len(baseline_surfaces["filtered_out_before_writer"]),
                "baseline_metadata_fact_count": len(baseline_surfaces["metadata_anchors"]),
                "g4_public_fact_count": len(public_facts),
                "g4_logistics_fact_count": len(logistics_facts),
            }
        )
        return {
            "input": review_payload_user,
            "baseline_fact_surfaces": baseline_surfaces,
            "review": normalized,
            "summary": summary,
            "errors": ["no_facts_on_either_side"],
            "gemma_calls": 0,
            "model": "none",
            "verdict": "unknown",
        }

    raw: dict[str, Any] = {}
    reviewer_model = gemma_model
    reviewer_four_o_calls = 0
    reviewer_notes: list[str] = []
    try:
        raw = await _ask_gemma_json_direct(
            model=gemma_model,
            system_prompt=legacy_writer_family.build_fact_coverage_system_prompt(),
            user_payload=review_payload_user,
            max_tokens=2400,
            response_schema=legacy_writer_family.fact_coverage_response_schema(),
            timeout_sec=max(_gemma_direct_timeout_sec(), 180.0),
            allow_json_repair=False,
        )
        gemma_calls += 1
    except asyncio.TimeoutError:
        gemma_calls += 1
        errors.append("reviewer.timeout")
    except Exception as exc:
        gemma_calls += 1
        errors.append(f"reviewer.error:{type(exc).__name__}")

    if errors and not raw:
        previous_errors = list(errors)
        try:
            raw = await _ask_4o_json(
                prompt=json.dumps(review_payload_user, ensure_ascii=False, indent=2),
                schema=legacy_writer_family.fact_coverage_response_schema(),
                model="gpt-4o",
                system_prompt=legacy_writer_family.build_fact_coverage_system_prompt(),
                max_tokens=3200,
            )
            reviewer_four_o_calls += 1
            reviewer_model = f"{gemma_model}->gpt-4o"
            reviewer_notes.extend(f"{item}:fallback_4o_ok" for item in previous_errors)
            errors = []
        except Exception as exc:
            reviewer_four_o_calls += 1
            errors.append(f"reviewer.fallback_4o_error:{type(exc).__name__}")

    normalized = legacy_writer_family.normalize_fact_coverage_payload(
        raw or {},
        baseline_count=baseline_count,
        g4_count=g4_count,
        baseline_facts=review_payload_user["baseline_facts"],
        g4_facts=review_payload_user["g4_facts"],
    )
    summary = legacy_writer_family.summarize_fact_coverage(normalized)
    summary.update(
        {
            "baseline_raw_extracted_fact_count": len(baseline_surfaces["raw_extracted_facts"]),
            "baseline_writer_fact_count": len(baseline_surfaces["writer_facts_text_clean"]),
            "baseline_filtered_out_fact_count": len(baseline_surfaces["filtered_out_before_writer"]),
            "baseline_metadata_fact_count": len(baseline_surfaces["metadata_anchors"]),
            "g4_public_fact_count": len(public_facts),
            "g4_logistics_fact_count": len(logistics_facts),
        }
    )
    if errors and not normalized.get("baseline_facts_review") and not normalized.get("g4_facts_review"):
        summary.update(
            {
                "baseline_fact_count": baseline_count,
                "g4_fact_count": g4_count,
                "grounded_baseline_fact_count": 0,
                "covered_grounded_baseline_fact_count": 0,
                "grounded_g4_fact_count": 0,
                "deterministic_verdict_floor": "unknown",
                "verdict": "unknown",
            }
        )
    if errors and summary.get("verdict") == "accepted":
        # Reviewer never returned a real answer — do not let the deterministic
        # floor over-promise an "accepted" verdict.
        summary["verdict"] = "unknown"
    return {
        "input": review_payload_user,
        "baseline_fact_surfaces": baseline_surfaces,
        "review": normalized,
        "summary": summary,
        "errors": errors,
        "warnings": reviewer_notes,
        "gemma_calls": gemma_calls,
        "four_o_calls": reviewer_four_o_calls,
        "model": reviewer_model if not errors else f"{reviewer_model}+errors",
        "verdict": summary.get("verdict") or "unknown",
    }


def _legacy_4o_writer_text_prompt(payload: dict[str, Any]) -> str:
    return textwrap.dedent(
        f"""
        Это финальный writer для lollipop_legacy.v13 (4o final fallback).
        Возвращай только JSON, соответствующий response_format. Без markdown-fence.

        Входные данные:
        {json.dumps(payload, ensure_ascii=False, indent=2)}

        Жёсткие правила:
        - description_md — это финальный публичный текст события на русском в Markdown.
        - Покрой каждый осмысленный public_fact. logistics_facts разрешены только в
          одном финальном блоке `### Когда и где`, если это полезно.
        - Не выдумывай ни одного факта/имени/даты/цены, которых нет в public_facts,
          logistics_facts или source_excerpt.
        - Соблюдай длину: между `min_description_chars` и `max_description_chars`,
          целься в `target_description_chars`. Текст короче минимума — невалидный ответ.
        - 2–4 публичных абзаца перед опциональным `### Когда и где`. Заголовки
          ставь только если они помогают читателю. Никаких других `###` заголовков.
        - Открывай первый абзац конкретной зацепкой: маршрут, объект, тема, имя,
          точная цитата названия, format texture, или событийное действие. Никаких
          стоковых нарраторских открытий вроде "Погружение в ...", "Знакомство с ...".
        - Никаких CTA, прямых обращений к читателю, "приглашаем", "вы сможете",
          "не упустите", "уникальная возможность".
        - Поля covered_public_fact_indexes и used_logistics_fact_indexes заполняй
          честно: какие индексы реально покрыты текстом.
        """
    ).strip()


async def _run_lollipop_legacy_variant(
    fixture: BenchmarkFixture,
    *,
    baseline: dict[str, Any],
    gemma_model: str,
    gemma_call_gap_s: float,
    legacy_g4_extract: bool = False,
) -> dict[str, Any]:
    """Baseline-equivalent Gemma 4 extract+write with 4o final fallback.

    Flow (lollipop_legacy.v13):
      1. Per-source Gemma 4 extraction -> public_facts, logistics_facts.
      2. Single Gemma 4 writer call from those facts + source_excerpt.
      3. If Gemma 4 writer times out / errors / returns empty,
         fall back once to gpt-4o with the same writer payload.

    No baseline text/facts in any generation payload. No repair pass. No
    deterministic regex fixers; the validator only reports issues.
    """
    print(
        f"[benchmark] {fixture.fixture_id} start lollipop_legacy model={gemma_model}",
        file=sys.stderr,
        flush=True,
    )
    started_at = time.perf_counter()
    gemma_calls = 0
    four_o_calls = 0
    sleep_sec = 0.0
    _set_gemma_model(gemma_model)

    source_excerpt = _source_excerpt(fixture.sources, limit=5000)
    baseline_description = str(baseline.get("description_md") or "")
    reference_chars = len(baseline_description)

    extract_warnings: list[str] = []
    public_items_raw: list[dict[str, str]] = []
    logistics_items_raw: list[dict[str, str]] = []
    per_source_facts: dict[str, list[str]] = {}

    extraction_schema = legacy_writer_family.extraction_response_schema()
    extraction_prompt = legacy_writer_family.build_extraction_system_prompt()

    for source in fixture.sources:
        try:
            raw = await _ask_gemma_json_direct(
                model=gemma_model,
                system_prompt=extraction_prompt,
                user_payload={
                    "title": fixture.title,
                    "event_type": fixture.event_type,
                    "date": fixture.date,
                    "time": fixture.time,
                    "location_name": fixture.location_name,
                    "location_address": fixture.location_address,
                    "city": fixture.city,
                    "source_id": source.source_id,
                    "source_type": source.source_type,
                    "source_text": source.text[:5000],
                },
                max_tokens=1400,
                response_schema=extraction_schema,
                timeout_sec=max(_gemma_direct_timeout_sec(), 75.0),
                allow_json_repair=False,
            )
            gemma_calls += 1
        except asyncio.TimeoutError:
            gemma_calls += 1
            extract_warnings.append(f"extraction.timeout:{source.source_id}")
            raw = {}
        except Exception as exc:
            gemma_calls += 1
            extract_warnings.append(
                f"extraction.error:{source.source_id}:{type(exc).__name__}"
            )
            raw = {}
        normalized = legacy_writer_family.normalize_extraction_payload(raw)
        public_items_raw.extend(normalized["public_facts"])
        logistics_items_raw.extend(normalized["logistics_facts"])
        per_source_facts[source.source_id] = [
            item["text"]
            for item in normalized["public_facts"] + normalized["logistics_facts"]
            if item.get("text")
        ]
        await _gemma_gap_sleep(gemma_call_gap_s)
        sleep_sec += gemma_call_gap_s if gemma_call_gap_s > 0 else 0.0

    public_facts = legacy_writer_family.merge_extraction_facts(public_items_raw)
    logistics_facts = legacy_writer_family.merge_extraction_facts(logistics_items_raw)

    writer_payload = legacy_writer_family.build_writer_payload(
        title=fixture.title,
        event_type=fixture.event_type,
        public_facts=public_facts,
        logistics_facts=logistics_facts[:10],
        source_excerpt=source_excerpt,
        reference_description_chars=reference_chars,
    )
    writer_schema = legacy_writer_family.writer_response_schema()
    writer_system_prompt = legacy_writer_family.build_writer_system_prompt()

    writer_model = "gemma-4"
    writer_raw: dict[str, Any] = {}
    writer_failure_reasons: list[str] = []
    if not public_facts:
        writer_failure_reasons.append("extraction.no_public_facts")
    else:
        try:
            writer_raw = await _ask_gemma_json_direct(
                model=gemma_model,
                system_prompt=writer_system_prompt,
                user_payload=writer_payload,
                max_tokens=1300,
                response_schema=writer_schema,
                timeout_sec=min(_gemma_writer_timeout_sec(), 55.0),
                allow_json_repair=False,
            )
            gemma_calls += 1
        except asyncio.TimeoutError:
            gemma_calls += 1
            writer_failure_reasons.append("writer.timeout:gemma4")
            writer_raw = {}
        except Exception as exc:
            gemma_calls += 1
            writer_failure_reasons.append(
                f"writer.error:gemma4:{type(exc).__name__}"
            )
            writer_raw = {}

    description = str((writer_raw or {}).get("description_md") or "").strip()
    if writer_raw and not description:
        writer_failure_reasons.append("writer.empty:gemma4")

    fallback_used = False
    if writer_failure_reasons and public_facts:
        try:
            writer_raw_4o = await _ask_4o_json(
                prompt=_legacy_4o_writer_text_prompt(writer_payload),
                schema=writer_schema,
                model="gpt-4o",
                system_prompt=writer_system_prompt,
                max_tokens=1500,
            )
            four_o_calls += 1
            description_4o = str(writer_raw_4o.get("description_md") or "").strip()
            if description_4o:
                writer_raw = writer_raw_4o
                description = description_4o
                writer_model = "4o"
                fallback_used = True
            else:
                writer_failure_reasons.append("writer.empty:4o")
        except Exception as exc:
            four_o_calls += 1
            writer_failure_reasons.append(
                f"writer.error:4o:{type(exc).__name__}"
            )

    applied_output = legacy_writer_family.apply_writer_output(
        title=fixture.title,
        output=writer_raw or {},
    )
    description = str(applied_output.get("description_md") or "")

    validation = legacy_writer_family.validate_writer_output(
        public_facts=public_facts,
        description_md=description,
        baseline_description=baseline_description,
    )
    if writer_failure_reasons and not description.strip():
        validation.errors.extend(writer_failure_reasons)
    elif writer_failure_reasons:
        validation.warnings.extend(writer_failure_reasons)
    validation.warnings.extend(extract_warnings)

    quality_delta = legacy_writer_family.compare_to_baseline(
        baseline_description=baseline_description,
        candidate_description=description,
        source_excerpt=source_excerpt,
    )
    validation.warnings.extend(quality_delta.get("warnings") or [])

    fact_coverage = await _run_fact_coverage_reviewer(
        fixture=fixture,
        baseline=baseline,
        public_facts=public_facts,
        logistics_facts=logistics_facts,
        source_excerpt=source_excerpt,
        gemma_model=gemma_model,
    )
    if fact_coverage.get("errors"):
        for err in fact_coverage["errors"]:
            validation.warnings.append(f"fact_coverage:{err}")

    wall_clock_sec = round(time.perf_counter() - started_at, 6)
    model_active_sec = round(max(0.0, wall_clock_sec - sleep_sec), 6)
    baseline_wall = None
    try:
        baseline_wall = float(
            (baseline.get("timings") or {}).get("wall_clock_sec") or 0.0
        ) or None
    except Exception:
        baseline_wall = None
    speed_ratio = None
    if baseline_wall:
        ratio = wall_clock_sec / baseline_wall
        speed_ratio = {
            "ratio": round(ratio, 4),
            "target": 3.0,
            "pass": ratio <= 3.0,
            "gate": "pass" if ratio <= 3.0 else "latency.3x_exceeded",
        }
        if ratio > 3.0:
            validation.warnings.append(f"latency.3x_exceeded:{ratio:.2f}")
    fact_calls = int(fact_coverage.get("gemma_calls") or 0)
    fact_four_o_calls = int(fact_coverage.get("four_o_calls") or 0)

    return {
        "gemma_model": gemma_model,
        "variant_mode": "lollipop_legacy",
        "contract_version": legacy_writer_family.LEGACY_CONTRACT_VERSION,
        "writer_model": writer_model,
        "generation_uses_baseline": False,
        "uses_baseline_fact_floor": False,
        "includes_baseline_stage": False,
        "baseline_assisted": False,
        "legacy_g4_extract_enabled": True,
        "legacy_g4_extract_flag_ignored": bool(legacy_g4_extract),
        "baseline_reference_chars": reference_chars,
        "legacy_public_fact_count": len(public_facts),
        "legacy_logistics_fact_count": len(logistics_facts),
        "legacy_extract_warnings": extract_warnings,
        "writer_fallback_to_4o": fallback_used,
        "writer_failure_reasons": writer_failure_reasons,
        "writer_fallback_to_baseline": False,
        "writer_retry_count": 0,
        "per_source_facts": per_source_facts,
        "legacy_public_facts": [item["text"] for item in public_facts],
        "legacy_logistics_facts": [item["text"] for item in logistics_facts],
        "writer_payload_contract": {
            "min_description_chars": writer_payload["min_description_chars"],
            "target_description_chars": writer_payload["target_description_chars"],
            "max_description_chars": writer_payload["max_description_chars"],
            "reference_description_chars": writer_payload["reference_description_chars"],
        },
        "writer_output": writer_raw,
        "applied_output": applied_output,
        "validation": {"errors": validation.errors, "warnings": validation.warnings},
        "quality_delta_vs_baseline": quality_delta,
        "quality_profile": _quality_profile(description),
        "metrics": _variant_metrics(description),
        "fact_coverage": fact_coverage,
        "timings": {
            "wall_clock_sec": wall_clock_sec,
            "model_active_sec": model_active_sec,
            "sleep_sec": round(sleep_sec, 6),
            "gemma_calls": gemma_calls + fact_calls,
            "four_o_calls": four_o_calls + fact_four_o_calls,
            "baseline_stage_sec": baseline_wall,
            "lollipop_legacy_only_sec": wall_clock_sec,
            "lollipop_legacy_only_gemma_calls": gemma_calls + fact_calls,
            "fact_coverage_gemma_calls": fact_calls,
            "fact_coverage_four_o_calls": fact_four_o_calls,
        },
        "speed_ratio_vs_baseline": speed_ratio,
    }

def _render_fact_coverage_section(fact_coverage: dict[str, Any]) -> list[str]:
    summary = dict(fact_coverage.get("summary") or {})
    inputs = dict(fact_coverage.get("input") or {})
    surfaces = dict(
        fact_coverage.get("baseline_fact_surfaces")
        or inputs.get("baseline_fact_surfaces")
        or {}
    )
    review = dict(fact_coverage.get("review") or {})
    coverage_summary = dict(summary.get("coverage_summary") or {})
    errors = list(fact_coverage.get("errors") or [])
    warnings = list(fact_coverage.get("warnings") or [])
    lines: list[str] = ["", "### Fact Extraction Coverage", ""]
    lines.append(f"- verdict: `{summary.get('verdict') or 'unknown'}`")
    lines.append(f"- llm_overall_verdict: `{summary.get('llm_overall_verdict') or 'unknown'}`")
    lines.append(
        f"- deterministic_verdict_floor: `{summary.get('deterministic_verdict_floor') or 'unknown'}`"
    )
    lines.append(
        "- baseline_fact_count: "
        f"`{summary.get('baseline_fact_count') or 0}` "
        f"(grounded `{summary.get('grounded_baseline_fact_count') or 0}`, "
        f"covered `{summary.get('covered_grounded_baseline_fact_count') or 0}`)"
    )
    lines.append(
        "- baseline surfaces: "
        f"raw `{summary.get('baseline_raw_extracted_fact_count') or 0}`, "
        f"writer facts_text_clean `{summary.get('baseline_writer_fact_count') or 0}`, "
        f"filtered before writer `{summary.get('baseline_filtered_out_fact_count') or 0}`, "
        f"metadata anchors `{summary.get('baseline_metadata_fact_count') or 0}`"
    )
    lines.append(
        "- g4_fact_count: "
        f"`{summary.get('g4_fact_count') or 0}` "
        f"(grounded `{summary.get('grounded_g4_fact_count') or 0}`, "
        f"public `{summary.get('g4_public_fact_count') or 0}`, "
        f"logistics `{summary.get('g4_logistics_fact_count') or 0}`)"
    )
    lines.append(f"- public_coverage: `{coverage_summary.get('public_coverage_status') or '-'}`")
    lines.append(
        f"- logistics_coverage: `{coverage_summary.get('logistics_coverage_status') or '-'}`"
    )
    lines.append(
        f"- named_entity_coverage: `{coverage_summary.get('named_entity_coverage_status') or '-'}`"
    )
    lines.append(
        f"- format_topic_program_coverage: "
        f"`{coverage_summary.get('format_topic_program_coverage_status') or '-'}`"
    )
    if coverage_summary.get("verdict_reason"):
        lines.append(f"- verdict_reason: `{coverage_summary['verdict_reason']}`")
    if errors:
        lines.append(f"- reviewer_errors: `{', '.join(errors)}`")
    if warnings:
        lines.append(f"- reviewer_warnings: `{', '.join(warnings)}`")

    def _render_input_list(title: str, items: list[object], *, field: str = "text") -> None:
        lines.extend(["", title])
        if not items:
            lines.append("- -")
            return
        for item in items:
            if isinstance(item, dict):
                idx = item.get("index")
                text = item.get(field) or item.get("text") or ""
                prefix = f"`{idx}` " if idx is not None else ""
                kind = item.get("kind") or item.get("category") or ""
                suffix = f" ({kind})" if kind else ""
                lines.append(f"- {prefix}{text}{suffix}")
            else:
                lines.append(f"- {item}")

    _render_input_list(
        "**Baseline raw extractor facts (`per_source_facts`):**",
        list(surfaces.get("raw_extracted_facts") or inputs.get("baseline_facts") or []),
    )
    _render_input_list(
        "**Baseline writer facts (`facts_text_clean`):**",
        list(surfaces.get("writer_facts_text_clean") or []),
    )
    _render_input_list(
        "**Baseline facts filtered out before writer:**",
        list(surfaces.get("filtered_out_before_writer") or []),
    )
    _render_input_list(
        "**Baseline metadata / anchors visible to writer:**",
        list(surfaces.get("metadata_anchors") or []),
    )
    g4_input_facts = list(inputs.get("g4_facts") or [])
    _render_input_list(
        "**Gemma 4 extracted facts (public + logistics, exact extractor output):**",
        g4_input_facts,
    )

    lost = list(summary.get("lost_baseline_facts") or [])
    if lost:
        lines.extend(["", "**Lost baseline facts:**"])
        for item in lost:
            severity = item.get("loss_severity") or "minor"
            lines.append(
                f"- [{severity}] {item.get('baseline_fact') or ''}"
                + (f" — {item.get('reason')}" if item.get("reason") else "")
            )
    added = list(summary.get("added_g4_facts") or [])
    if added:
        lines.extend(["", "**Useful added Gemma 4 facts:**"])
        for item in added:
            kind_hint = item.get("fact_kind") or item.get("category") or ""
            tag = f" ({kind_hint})" if kind_hint else ""
            lines.append(f"- {item.get('g4_fact') or ''}{tag}")
    suspicious = list(summary.get("suspicious_g4_facts") or [])
    if suspicious:
        lines.extend(["", "**Suspicious Gemma 4 facts:**"])
        for item in suspicious:
            reason = item.get("suspicious_reason") or item.get("grounded_in_source") or ""
            tag = f" — {reason}" if reason else ""
            lines.append(f"- {item.get('g4_fact') or ''}{tag}")

    baseline_review = list(review.get("baseline_facts_review") or [])
    if baseline_review:
        lines.extend(["", "**Baseline facts (reviewer view):**"])
        for item in baseline_review:
            covered = "✓" if item.get("covered_by_g4") else "✗"
            grounded = item.get("grounded_in_source") or "?"
            severity = item.get("loss_severity") or "none"
            lines.append(
                f"- {covered} grounded={grounded} severity={severity}: {item.get('baseline_fact') or ''}"
            )
    g4_review = list(review.get("g4_facts_review") or [])
    if g4_review:
        lines.extend(["", "**Gemma 4 facts (reviewer view):**"])
        for item in g4_review:
            grounded = item.get("grounded_in_source") or "?"
            useful = "✓" if item.get("useful_new_fact") else "·"
            category = item.get("category") or "?"
            kind = item.get("fact_kind") or ""
            tag = f" [{kind}]" if kind else ""
            note = (
                f" — {item.get('suspicious_reason')}"
                if item.get("suspicious_reason")
                else ""
            )
            lines.append(
                f"- {useful} grounded={grounded} cat={category}{tag}: "
                f"{item.get('g4_fact') or ''}{note}"
            )
    return lines


def _render_markdown_report(results: list[dict[str, Any]], output_json_path: Path) -> str:
    lines = [
        "# Lollipop Benchmark",
        "",
        f"- generated_at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- artifact_json: `{output_json_path}`",
        "",
    ]
    if any(isinstance(result.get("baseline_g4"), dict) for result in results):
        lines.extend(
            [
                "## Baseline vs Baseline G4 Summary",
                "",
                "| Fixture | Baseline chars | Baseline G4 chars | Length ratio | Quality | Errors | Warnings | Speed ratio | Gemma calls |",
                "| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for result in results:
            fixture = result["fixture"]
            baseline = result.get("baseline") if isinstance(result.get("baseline"), dict) else {}
            baseline_g4 = result.get("baseline_g4") if isinstance(result.get("baseline_g4"), dict) else None
            if not baseline_g4:
                continue
            baseline_chars = int((baseline.get("metrics") or _variant_metrics(baseline.get("description_md"))).get("chars") or 0)
            g4_chars = int((baseline_g4.get("metrics") or {}).get("chars") or 0)
            length_ratio = round(g4_chars / baseline_chars, 4) if baseline_chars else None
            quality_delta = baseline_g4.get("quality_delta_vs_baseline") if isinstance(baseline_g4.get("quality_delta_vs_baseline"), dict) else {}
            validation = baseline_g4.get("validation") if isinstance(baseline_g4.get("validation"), dict) else {}
            speed = baseline_g4.get("speed_ratio_vs_baseline") if isinstance(baseline_g4.get("speed_ratio_vs_baseline"), dict) else {}
            timings = baseline_g4.get("timings") if isinstance(baseline_g4.get("timings"), dict) else {}
            lines.append(
                "| "
                f"`{fixture['fixture_id']}` | {baseline_chars} | {g4_chars} | "
                f"{length_ratio if length_ratio is not None else '-'} | "
                f"{quality_delta.get('status') or '-'} | "
                f"{len(validation.get('errors') or [])} | {len(validation.get('warnings') or [])} | "
                f"{speed.get('ratio') if speed else '-'} | {timings.get('gemma_calls') or '-'} |"
            )
        lines.append("")
    for result in results:
        fixture = result["fixture"]
        lines.extend(
            [
                f"## {fixture['fixture_id']}",
                "",
                f"- title: `{fixture['title']}`",
                f"- event_type: `{fixture['event_type']}`",
                f"- date: `{fixture['date']}`",
                "- sources:",
            ]
        )
        for source in fixture["sources"]:
            lines.append(f"  - `{source['source_id']}`: {source['url']}")
        lines.extend(
            [
                "",
                "### Source Excerpts",
                "",
                "```text",
                _source_excerpt([SourcePacket(**source) for source in fixture["sources"]], limit=9000),
                "```",
                "",
            ]
        )
        variant_labels = [
            ("baseline", "Baseline"),
            ("baseline_g4", "Baseline G4"),
            ("lollipop", "Lollipop"),
            ("lollipop_g4", "Lollipop G4"),
            ("lollipop_legacy", "Lollipop Legacy"),
        ]
        for key, label in variant_labels:
            payload = result.get(key)
            if not isinstance(payload, dict):
                continue
            body = payload.get("description_md") if key == "baseline" else payload.get("applied_output", {}).get("description_md")
            metrics = _variant_metrics(body)
            quality = dict(payload.get("quality_profile") or _quality_profile(body))
            lines.extend(
                [
                    f"### {label}",
                    "",
                    f"- chars: `{metrics['chars']}`",
                    f"- headings: `{metrics['headings']}`",
                    f"- bullets: `{metrics['bullets']}`",
                    f"- lead_hook_signals: `{','.join(quality.get('lead_hook_signals') or []) or '-'}'",
                    f"- report_formula_hits: `{','.join(quality.get('report_formula_hits') or []) or '-'}'",
                    f"- promo_phrase_hits: `{','.join(quality.get('promo_phrase_hits') or []) or '-'}'",
                    f"- poster_leak: `{bool(quality.get('poster_leak'))}`",
                    f"- age_leak: `{bool(quality.get('age_leak'))}`",
                ]
            )
            timing_summary = _timing_summary(payload)
            if timing_summary is None and isinstance(payload.get("timings"), dict):
                timing_summary = {
                    "wall_clock_sec": payload["timings"].get("wall_clock_sec"),
                    "model_active_sec": payload["timings"].get("model_active_sec"),
                    "sleep_sec": payload["timings"].get("sleep_sec"),
                    "gemma_calls": payload["timings"].get("gemma_calls"),
                    "four_o_calls": payload["timings"].get("four_o_calls"),
                    "top_stage_families": [],
                }
            if timing_summary is not None:
                lines.append(f"- wall_clock_sec: `{timing_summary['wall_clock_sec']}`")
                lines.append(f"- model_active_sec: `{timing_summary['model_active_sec']}`")
                lines.append(f"- sleep_sec: `{timing_summary['sleep_sec']}`")
                lines.append(f"- gemma_calls: `{timing_summary['gemma_calls']}`")
                lines.append(f"- four_o_calls: `{timing_summary['four_o_calls']}`")
                top_families = ", ".join(f"{name}={value}" for name, value in timing_summary.get("top_stage_families") or [])
                if top_families:
                    lines.append(f"- slowest_stage_families: `{top_families}`")
            if key in {"lollipop", "lollipop_g4"}:
                lines.append(f"- selected_sources: `{','.join(payload['scope_select']['selected_source_ids'])}`")
                lines.append(f"- extract_records: `{len(payload['extract_records'])}`")
                lines.append(f"- kept_records_after_dedup: `{len([item for item in payload['extract_records'] if item['record_id'] not in {decision['record_id'] for decision in payload['dedup']['decisions'] if decision['keep'] == 'drop'}])}`")
                lines.append(f"- validation errors: `{len(payload['validation']['errors'])}`")
                lines.append(f"- validation warnings: `{len(payload['validation']['warnings'])}`")
            if key in {"baseline_g4", "lollipop_legacy"} and isinstance(payload.get("quality_delta_vs_baseline"), dict):
                quality_delta = payload.get("quality_delta_vs_baseline")
                lines.append(f"- quality_delta_status: `{quality_delta.get('status')}`")
                lines.append(f"- quality_score: `{quality_delta.get('baseline_score')} -> {quality_delta.get('candidate_score')}`")
                lines.append(f"- length_ratio_vs_baseline: `{quality_delta.get('length_ratio')}`")
                improvements = ", ".join(quality_delta.get("improvements") or [])
                regressions = ", ".join(quality_delta.get("regressions") or [])
                lines.append(f"- quality_improvements: `{improvements or '-'}`")
                lines.append(f"- quality_regressions: `{regressions or '-'}`")
                speed = payload.get("speed_ratio_vs_baseline") if isinstance(payload.get("speed_ratio_vs_baseline"), dict) else None
                if speed:
                    lines.append(f"- speed_ratio_vs_baseline: `{speed.get('ratio')}` (`{speed.get('gate')}`)")
                lines.append(f"- validation errors: `{len(payload.get('validation', {}).get('errors') or [])}`")
                lines.append(f"- validation warnings: `{len(payload.get('validation', {}).get('warnings') or [])}`")
                if payload.get("validation", {}).get("errors"):
                    lines.append(f"- validation error list: `{', '.join(payload['validation']['errors'])}`")
            if key == "lollipop_legacy":
                lines.append(f"- contract_version: `{payload.get('contract_version')}`")
                lines.append(f"- generation_uses_baseline: `{bool(payload.get('generation_uses_baseline'))}`")
                lines.append(f"- includes_baseline_stage: `{bool(payload.get('includes_baseline_stage'))}`")
                lines.append(f"- uses_baseline_fact_floor: `{bool(payload.get('uses_baseline_fact_floor'))}`")
                lines.append(f"- writer_model: `{payload.get('writer_model')}`")
                lines.append(f"- writer_fallback_to_4o: `{bool(payload.get('writer_fallback_to_4o'))}`")
                lines.append(f"- legacy_public_fact_count: `{payload.get('legacy_public_fact_count')}`")
                lines.append(f"- legacy_logistics_fact_count: `{payload.get('legacy_logistics_fact_count')}`")
                lines.append(f"- writer_retry_count: `{payload.get('writer_retry_count') or 0}`")
                lines.append(f"- writer_fallback_to_baseline: `{bool(payload.get('writer_fallback_to_baseline'))}`")
                fact_coverage = payload.get("fact_coverage") if isinstance(payload.get("fact_coverage"), dict) else None
                if fact_coverage:
                    lines.extend(_render_fact_coverage_section(fact_coverage))
            lines.extend(["", "```md", str(body or ""), "```", ""])
    return "\n".join(lines).strip() + "\n"


async def _run(args: argparse.Namespace) -> tuple[Path, Path]:
    _load_env_file()
    variants = _variants_from_cli(args.variants)
    fixtures = _fixtures_from_artifact(args.reuse_fixture_artifact, args.fixtures) if args.reuse_fixture_artifact else _fixtures_from_cli(args.fixtures)
    results: list[dict[str, Any]] = []
    for fixture in fixtures:
        baseline = _load_reused_variant_payload(
            args.reuse_baseline_artifact,
            fixture_id=fixture.fixture_id,
            section="baseline",
        )
        if baseline is None:
            baseline = await _run_baseline(fixture, gemma_model=args.g3_model, gemma_call_gap_s=args.gemma_call_gap_s)
            await _gemma_gap_sleep(args.gemma_call_gap_s)
        baseline["quality_profile"] = _quality_profile(baseline.get("description_md"))
        baseline["metrics"] = _variant_metrics(baseline.get("description_md"))

        row: dict[str, Any] = {
            "fixture": {
                "fixture_id": fixture.fixture_id,
                "title": fixture.title,
                "event_type": fixture.event_type,
                "date": fixture.date,
                "time": fixture.time,
                "location_name": fixture.location_name,
                "location_address": fixture.location_address,
                "city": fixture.city,
                "sources": [asdict(source) for source in fixture.sources],
            }
        }
        if "baseline" in variants:
            row["baseline"] = baseline

        if "baseline_g4" in variants:
            try:
                baseline_g4 = await _run_baseline_g4(
                    fixture,
                    baseline=baseline,
                    gemma_model=args.g4_model,
                    gemma_call_gap_s=args.gemma_call_gap_s,
                )
            except Exception as exc:
                baseline_g4 = {
                    "gemma_model": args.g4_model,
                    "variant_mode": "baseline_g4",
                    "baseline_g4_mode": "optimized_fact_first_baseline_contract",
                    "description_md": "",
                    "applied_output": {"title": fixture.title, "description_md": ""},
                    "validation": {"errors": [f"exception:{type(exc).__name__}:{str(exc)[:240]}"], "warnings": []},
                    "quality_delta_vs_baseline": {"status": "failed", "regressions": ["exception"], "warnings": []},
                    "quality_profile": _quality_profile(""),
                    "metrics": _variant_metrics(""),
                    "timings": {
                        "wall_clock_sec": None,
                        "model_active_sec": None,
                        "sleep_sec": 0.0,
                        "gemma_calls": None,
                        "four_o_calls": 0,
                        "baseline_stage_sec": (baseline.get("timings") or {}).get("wall_clock_sec"),
                    },
                    "speed_ratio_vs_baseline": None,
                }
            row["baseline_g4"] = baseline_g4

        if "lollipop" in variants:
            lollipop = _load_reused_variant_payload(
                args.reuse_lollipop_artifact,
                fixture_id=fixture.fixture_id,
                section="lollipop",
            )
            if lollipop is None:
                lollipop = await _run_lollipop_variant(
                    fixture,
                    gemma_model=args.g3_model,
                    four_o_model=args.four_o_model,
                    gemma_call_gap_s=args.gemma_call_gap_s,
                )
                await _gemma_gap_sleep(args.gemma_call_gap_s)
            lollipop["quality_profile"] = _quality_profile(lollipop["applied_output"].get("description_md"))
            lollipop["metrics"] = _variant_metrics(lollipop["applied_output"].get("description_md"))
            row["lollipop"] = lollipop

        if "lollipop_g4" in variants:
            lollipop_g4 = await _run_lollipop_variant(
                fixture,
                gemma_model=args.g4_model,
                four_o_model=args.four_o_model,
                gemma_call_gap_s=args.gemma_call_gap_s,
            )
            lollipop_g4["quality_profile"] = _quality_profile(lollipop_g4["applied_output"].get("description_md"))
            row["lollipop_g4"] = lollipop_g4

        if "lollipop_legacy" in variants:
            lollipop_legacy = await _run_lollipop_legacy_variant(
                fixture,
                baseline=baseline,
                gemma_model=args.g4_model,
                gemma_call_gap_s=args.gemma_call_gap_s,
                legacy_g4_extract=args.legacy_g4_extract,
            )
            row["lollipop_legacy"] = lollipop_legacy

        results.append(row)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_slug = f"lollipop_g4_benchmark_{timestamp}"
    ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)
    json_path = ARTIFACTS_ROOT / f"{run_slug}.json"
    md_path = ARTIFACTS_ROOT / f"{run_slug}.md"
    json_path.write_text(json.dumps({"variants": variants, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown_report(results, json_path), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run baseline/baseline g4/lollipop/lollipop g4/lollipop legacy benchmark.")
    parser.add_argument("--fixtures", default=DEFAULT_FIXTURES, help="Comma-separated fixture names")
    parser.add_argument("--variants", default=DEFAULT_VARIANTS, help="Comma-separated variants: baseline,baseline_g4,lollipop,lollipop_g4,lollipop_legacy")
    parser.add_argument("--g3-model", default=DEFAULT_G3_MODEL, help="Gemma 3 upstream/baseline model")
    parser.add_argument("--g4-model", default=DEFAULT_G4_MODEL, help="Gemma 4 upstream model")
    parser.add_argument("--four-o-model", default=DEFAULT_4O_MODEL, help="Final writer model")
    parser.add_argument("--gemma-call-gap-s", type=float, default=DEFAULT_GEMMA_CALL_GAP_S, help="Sleep between Gemma calls")
    parser.add_argument(
        "--legacy-g4-extract",
        action="store_true",
        help="Deprecated no-op for v7: lollipop_legacy now always uses Gemma 4 facts+writer stages and never baseline facts.",
    )
    parser.add_argument(
        "--reuse-baseline-artifact",
        help="Existing benchmark/debug JSON to reuse baseline for matching fixture_id",
    )
    parser.add_argument(
        "--reuse-lollipop-artifact",
        help="Existing benchmark/debug JSON to reuse lollipop for matching fixture_id",
    )
    parser.add_argument(
        "--reuse-fixture-artifact",
        help="Existing benchmark JSON to reuse fixture source texts for matching fixture names",
    )
    args = parser.parse_args()
    json_path, md_path = asyncio.run(_run(args))
    print(json.dumps({"ok": True, "json_path": str(json_path), "md_path": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
