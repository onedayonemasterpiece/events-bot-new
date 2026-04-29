#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import re
import sys
import textwrap
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import google.generativeai as genai
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import smart_event_update as su
from smart_event_update import EventCandidate
from smart_update_lollipop_lab.full_cascade import run_full_cascade_variant
from smart_update_lollipop_lab import writer_final_4o_family as writer_final_family
from smart_update_lollipop_lab import legacy_writer_family


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
            SourcePacket("tg", "telegram", tg_url, _extract_tg_text(_http_get(tg_url))),
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
            SourcePacket("tg", "telegram", tg_url, _extract_tg_text(_http_get(tg_url))),
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
            "Начать прогулку можно в любой промежуток с 16:00 до 20:00 в пятницу и воскресенье. "
            "Полную инструкцию и карту вы получите в «Баре Советов», проспект Мира, 118. "
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
            "Приглашаем на лекцию о быте и нравах регулярного военного флота Петра Великого. "
            "Лектор — Борис Мегорский, заведующий отделом Российской национальной библиотеки. "
            "Билеты продаются на сайте и в кассе музея. OCR: 24.04 в 16:00."
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
            "Завтра, 23 апреля, в Доме китобоя стартует цикл лекций от религиоведа Алексея Зыгмонта "
            "«В поисках абсолютно инакового». Начало в 18.30. Вход — 300 р. Билеты — на сайте музея. "
            "Дом китобоя, пр-т Мира 9. OCR: краткая история сакрального; лекторий; Алексей Зыгмонт; "
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
            "историко-художественном музее состоится открытие персональной выставки Геннадия Медера "
            "«Мир увлечений». Выставка будет работать до 22 мая 2026 года."
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
            "произведения русского народного промысла: жостовские подносы, каргопольская и дымковская игрушки."
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
    allowed = {"baseline", "lollipop", "lollipop_g4", "lollipop_legacy"}
    variants = [item.strip() for item in (raw or DEFAULT_VARIANTS).split(",") if item.strip()]
    if not variants:
        variants = [item.strip() for item in DEFAULT_VARIANTS.split(",") if item.strip()]
    unknown = [item for item in variants if item not in allowed]
    if unknown:
        raise ValueError(f"Unsupported variants: {', '.join(unknown)}")
    if "lollipop_legacy" in variants and "baseline" not in variants:
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
) -> dict[str, Any]:
    api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY2") or "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is missing")
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
            heading_value = str(block.get("heading") or "").strip() or None
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
                    "heading": (str(block.get("heading") or "").strip() or None) if precompute["allow_semantic_headings"] else None,
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


async def _ask_4o_json(*, prompt: str, schema: dict[str, Any], model: str) -> dict[str, Any]:
    token = os.getenv("FOUR_O_TOKEN")
    if not token:
        raise RuntimeError("FOUR_O_TOKEN is missing")
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": 1600,
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "LollipopFinalWriter", "schema": schema},
        },
        "messages": [
            {"role": "system", "content": "Return only valid JSON for the requested schema."},
            {"role": "user", "content": prompt},
        ],
    }
    response = await asyncio.to_thread(
        requests.post,
        os.getenv("FOUR_O_URL", "https://api.openai.com/v1/chat/completions"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    response.raise_for_status()
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


def _baseline_fact_list(baseline: dict[str, Any]) -> list[str]:
    facts: list[str] = []
    for _source_id, source_facts in dict(baseline.get("per_source_facts") or {}).items():
        for fact in list(source_facts or []):
            text = re.sub(r"\s+", " ", str(fact or "")).strip()
            if text and text not in facts:
                facts.append(text)
    return facts


def _legacy_event_fact_floor(facts: list[str]) -> list[str]:
    event_facts: list[str] = []
    logistics_re = re.compile(
        r"(?iu)\b(?:"
        r"\d{1,2}(?:[.:]\d{2})?|"
        r"апрел[ья]|ма[йяе]|июн[ья]|июл[ья]|август|сентябр|октябр|ноябр|декабр|"
        r"билет\w*|касс\w*|сайт\w*|стоимост\w*|руб(?:\.|л|лей)?|₽|"
        r"место\s+старта"
        r")\b"
    )
    for fact in facts:
        text = re.sub(r"\s+", " ", str(fact or "")).strip()
        if not text:
            continue
        if logistics_re.search(text):
            continue
        event_facts.append(text)
    return event_facts or facts


def _legacy_required_fact_floor(facts: list[str]) -> list[str]:
    """Full baseline fact floor required by lollipop_legacy.

    The legacy recovery contract is additive: it starts from the same baseline
    extracted facts, then runs lollipop enhancement/writer stages on top.
    """
    required: list[str] = []
    for fact in facts:
        text = re.sub(r"\s+", " ", str(fact or "")).strip()
        if text and text not in required:
            required.append(text)
    return required


async def _run_lollipop_legacy_variant(
    fixture: BenchmarkFixture,
    *,
    baseline: dict[str, Any],
    gemma_model: str,
    gemma_call_gap_s: float,
    legacy_g4_extract: bool = False,
) -> dict[str, Any]:
    print(f"[benchmark] {fixture.fixture_id} start lollipop_legacy model={gemma_model}", file=sys.stderr, flush=True)
    additional_started_at = time.perf_counter()
    gemma_calls = 0
    sleep_sec = 0.0
    _set_gemma_model(gemma_model)

    per_source: dict[str, list[str]] = {}
    extracted: list[str] = []
    if legacy_g4_extract:
        for source in fixture.sources:
            candidate = _build_candidate(fixture, source)
            facts = await su._llm_extract_candidate_facts(candidate, text_for_facts=source.text)
            gemma_calls += 1
            per_source[source.source_id] = facts
            extracted.extend(facts)
            await _gemma_gap_sleep(gemma_call_gap_s)
            sleep_sec += gemma_call_gap_s if gemma_call_gap_s > 0 else 0.0

    baseline_all_facts = _baseline_fact_list(baseline)
    baseline_event_facts = _legacy_event_fact_floor(baseline_all_facts)
    baseline_floor_facts = _legacy_required_fact_floor(baseline_all_facts)
    legacy_facts = [re.sub(r"\s+", " ", str(item or "")).strip() for item in extracted if str(item or "").strip()]
    fact_floor = baseline_floor_facts or legacy_facts
    for fact in legacy_facts:
        if fact and fact not in fact_floor:
            fact_floor.append(fact)

    source_excerpt = _source_excerpt(fixture.sources, limit=3200)
    enhancement_raw: dict[str, Any] = {"mode": "single_writer_pass", "extra_facts": []}
    enhancement = {
        "lead_hook_fact_indexes": [],
        "quote_candidates": [],
        "extra_facts": [],
        "writer_notes": [
            "Single bounded lollipop pass: preserve the baseline draft first, then improve only with grounded source texture."
        ],
    }

    fallback_to_baseline = False
    fallback_reasons: list[str] = []
    try:
        writer_raw = await _ask_gemma_json_direct(
            model=gemma_model,
            system_prompt=legacy_writer_family.build_writer_system_prompt(),
            user_payload=legacy_writer_family.build_writer_payload(
                title=fixture.title,
                event_type=fixture.event_type,
                baseline_description=str(baseline.get("description_md") or ""),
                baseline_facts=fact_floor,
                enhancement=enhancement,
                source_excerpt=source_excerpt,
            ),
            max_tokens=2200,
            response_schema=legacy_writer_family.writer_response_schema(),
            timeout_sec=_gemma_writer_timeout_sec(),
        )
        gemma_calls += 1
        applied_output = legacy_writer_family.apply_writer_output(title=fixture.title, output=writer_raw)
        validation = legacy_writer_family.validate_writer_output(
            baseline_facts=fact_floor,
            baseline_description=str(baseline.get("description_md") or ""),
            enhancement=enhancement,
            output=writer_raw,
        )
    except asyncio.TimeoutError:
        gemma_calls += 1
        writer_raw = {
            "title": fixture.title,
            "description_md": str(baseline.get("description_md") or ""),
            "covered_baseline_fact_indexes": list(range(len(fact_floor))),
            "used_extra_fact_indexes": [],
        }
        applied_output = legacy_writer_family.apply_writer_output(title=fixture.title, output=writer_raw)
        validation = legacy_writer_family.validate_writer_output(
            baseline_facts=fact_floor,
            baseline_description=str(baseline.get("description_md") or ""),
            enhancement=enhancement,
            output=writer_raw,
        )
        fallback_to_baseline = True
        fallback_reasons = ["writer.timeout"]
        validation.warnings.append("writer.fallback_to_baseline:writer.timeout")
    if validation.errors:
        fallback_to_baseline = True
        fallback_reasons = list(validation.errors)
        writer_raw = {
            "title": fixture.title,
            "description_md": str(baseline.get("description_md") or ""),
            "covered_baseline_fact_indexes": list(range(len(fact_floor))),
            "used_extra_fact_indexes": [],
        }
        applied_output = legacy_writer_family.apply_writer_output(title=fixture.title, output=writer_raw)
        validation = legacy_writer_family.validate_writer_output(
            baseline_facts=fact_floor,
            baseline_description=str(baseline.get("description_md") or ""),
            enhancement=enhancement,
            output=writer_raw,
        )
        validation.warnings.append("writer.fallback_to_baseline:" + ",".join(fallback_reasons))
    additional_wall_clock_sec = round(time.perf_counter() - additional_started_at, 6)
    baseline_timings = dict(baseline.get("timings") or {})
    baseline_wall = None
    baseline_active = 0.0
    baseline_sleep = 0.0
    baseline_gemma_calls = 0
    baseline_four_o_calls = 0
    try:
        baseline_wall = float(baseline_timings.get("wall_clock_sec") or 0.0) or None
    except Exception:
        baseline_wall = None
    try:
        baseline_active = float(baseline_timings.get("model_active_sec") or 0.0)
    except Exception:
        baseline_active = 0.0
    try:
        baseline_sleep = float(baseline_timings.get("sleep_sec") or 0.0)
    except Exception:
        baseline_sleep = 0.0
    try:
        baseline_gemma_calls = int(baseline_timings.get("gemma_calls") or 0)
    except Exception:
        baseline_gemma_calls = 0
    try:
        baseline_four_o_calls = int(baseline_timings.get("four_o_calls") or 0)
    except Exception:
        baseline_four_o_calls = 0

    additional_active_sec = round(max(0.0, additional_wall_clock_sec - sleep_sec), 6)
    wall_clock_sec = round((baseline_wall or 0.0) + additional_wall_clock_sec, 6)
    model_active_sec = round(baseline_active + additional_active_sec, 6)
    total_sleep_sec = round(baseline_sleep + sleep_sec, 6)
    total_gemma_calls = baseline_gemma_calls + gemma_calls
    total_four_o_calls = baseline_four_o_calls
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
            "min": 1.0,
            "pass": 1.0 < ratio <= 3.0,
            "gate": "pass" if 1.0 < ratio <= 3.0 else ("latency.baseline_stage_not_counted" if ratio <= 1.0 else "latency.3x_exceeded"),
        }
        if ratio <= 1.0:
            validation.errors.append("latency.baseline_stage_not_counted")
        if ratio > 3.0:
            validation.errors.append("latency.3x_exceeded")

    return {
        "gemma_model": gemma_model,
        "variant_mode": "lollipop_legacy",
        "contract_version": legacy_writer_family.LEGACY_CONTRACT_VERSION,
        "uses_baseline_fact_floor": True,
        "includes_baseline_stage": True,
        "legacy_g4_extract_enabled": legacy_g4_extract,
        "baseline_all_fact_count": len(baseline_all_facts),
        "baseline_floor_fact_count": len(fact_floor),
        "baseline_event_fact_count": len(baseline_event_facts),
        "baseline_logistics_fact_count": max(0, len(baseline_all_facts) - len(baseline_event_facts)),
        "legacy_extracted_fact_count": len(legacy_facts),
        "per_source_facts": per_source,
        "baseline_floor_facts": fact_floor,
        "legacy_extracted_facts": legacy_facts,
        "enhancement_raw": enhancement_raw,
        "enhancement": enhancement,
        "writer_output": writer_raw,
        "writer_retry_count": 0,
        "writer_fallback_to_baseline": fallback_to_baseline,
        "writer_fallback_reasons": fallback_reasons,
        "applied_output": applied_output,
        "validation": {"errors": validation.errors, "warnings": validation.warnings},
        "quality_profile": _quality_profile(applied_output.get("description_md")),
        "metrics": _variant_metrics(applied_output.get("description_md")),
        "timings": {
            "wall_clock_sec": wall_clock_sec,
            "model_active_sec": model_active_sec,
            "sleep_sec": total_sleep_sec,
            "gemma_calls": total_gemma_calls,
            "four_o_calls": total_four_o_calls,
            "baseline_stage_sec": baseline_wall,
            "lollipop_additional_sec": additional_wall_clock_sec,
            "lollipop_additional_gemma_calls": gemma_calls,
        },
        "speed_ratio_vs_baseline": speed_ratio,
    }


def _render_markdown_report(results: list[dict[str, Any]], output_json_path: Path) -> str:
    lines = [
        "# Lollipop Benchmark",
        "",
        f"- generated_at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- artifact_json: `{output_json_path}`",
        "",
    ]
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
        lines.extend(["", "### Source Excerpts", "", "```text", _source_excerpt([SourcePacket(**source) for source in fixture["sources"]]), "```", ""])
        variant_labels = [
            ("baseline", "Baseline"),
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
            if key == "lollipop_legacy":
                lines.append(f"- baseline_all_fact_count: `{payload.get('baseline_all_fact_count')}`")
                lines.append(f"- baseline_floor_fact_count: `{payload.get('baseline_floor_fact_count')}`")
                lines.append(f"- baseline_logistics_fact_count: `{payload.get('baseline_logistics_fact_count')}`")
                lines.append(f"- legacy_extracted_fact_count: `{payload.get('legacy_extracted_fact_count')}`")
                lines.append(f"- writer_retry_count: `{payload.get('writer_retry_count') or 0}`")
                lines.append(f"- writer_fallback_to_baseline: `{bool(payload.get('writer_fallback_to_baseline'))}`")
                speed = payload.get("speed_ratio_vs_baseline") if isinstance(payload.get("speed_ratio_vs_baseline"), dict) else None
                if speed:
                    lines.append(f"- speed_ratio_vs_baseline: `{speed.get('ratio')}` (`{speed.get('gate')}`)")
                lines.append(f"- validation errors: `{len(payload.get('validation', {}).get('errors') or [])}`")
                lines.append(f"- validation warnings: `{len(payload.get('validation', {}).get('warnings') or [])}`")
                if payload.get("validation", {}).get("errors"):
                    lines.append(f"- validation error list: `{', '.join(payload['validation']['errors'])}`")
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
    parser = argparse.ArgumentParser(description="Run baseline/lollipop/lollipop g4/lollipop legacy benchmark.")
    parser.add_argument("--fixtures", default=DEFAULT_FIXTURES, help="Comma-separated fixture names")
    parser.add_argument("--variants", default=DEFAULT_VARIANTS, help="Comma-separated variants: baseline,lollipop,lollipop_g4,lollipop_legacy")
    parser.add_argument("--g3-model", default=DEFAULT_G3_MODEL, help="Gemma 3 upstream/baseline model")
    parser.add_argument("--g4-model", default=DEFAULT_G4_MODEL, help="Gemma 4 upstream model")
    parser.add_argument("--four-o-model", default=DEFAULT_4O_MODEL, help="Final writer model")
    parser.add_argument("--gemma-call-gap-s", type=float, default=DEFAULT_GEMMA_CALL_GAP_S, help="Sleep between Gemma calls")
    parser.add_argument(
        "--legacy-g4-extract",
        action="store_true",
        help="Experimental: also rerun baseline-style fact extraction on Gemma 4 before lollipop_legacy writer. Off by default because it is slow on current Google path.",
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
