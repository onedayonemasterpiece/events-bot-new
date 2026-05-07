"""One-shot Gemma 4 upstream VK smoke for the harder PRODDB-4594 fixture.

Path: raw VK source_text -> build_event_drafts_from_vk (Gemma 4 parser) ->
EventCandidate -> smart_event_update (lollipop-light, Gemma 4 only) ->
persisted event row + Telegraph body. Writes a markdown + JSON artifact under
``artifacts/codex/`` so the run can be reviewed without re-spinning the bot.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


SOURCE_URL = "https://vk.com/wall-211997788_3002"
SOURCE_NAME = "ОКЦ Сигнал"
SOURCE_TEXT = """День идей и людей. Питчинг в Сигнале.

14 мая, 18:00. Леонова, 22.

⎯⎯⎯⎯

Страшно? Всегда страшно выйти из своей обыденности.

Цикличность, минимум живых контактов, нового опыта. Просмотр ленты не в счёт — это не ваш опыт, это вы смотрите чужой, безопасно, ничего не надо делать.

Как найти партнёра, друга? Что ты можешь сделать? На что способен? Для чего рождён?

Город стимулирует одиночество, анонимизирует связи, технологизирует даже рутину.

⎯⎯⎯⎯

В Калининграде и области для этого есть уникальные места — Сигнал (Леонова, 22), ОКЦ на Горького 116, Телеграф в Светлогорске, Теплосеть в Советске, Крупорушка в Озёрске. Эту инфраструктуру горожане создавали для горожан, именно для объединения. Места, где появляется репутация и возникают отношения.

⎯⎯⎯⎯

Чтобы было проще начать, есть подробная инструкция (но русский человек инструкции не читает)). Поэтому — Питчинг-день. Питч это быстрый рассказ про идею и обратная связь. Дежурный модератор помогает рассказать, запускает обсуждение, следит, чтобы вашу идею никто не высмеял и не назвал глупой.

⎯⎯⎯⎯

Что нужно?

Подумать свою идею. Погуглить, почертить мыслишки на листочках, посмотреть свои фотографии — отчего у вас горят глаза. Позвонить друзьям, рассказать свой план-капкан.

Ответить себе честно: сообщество — это партнёры по совместному опыту, или ресурс для карьеры? Что будет двигать вас собираться регулярно? Какую пользу принесёт лично вам, другим, городу? В какой роли вы готовы участвовать?

В начале вы инициатор — классная и сложная роль. Дальше — по возможностям: идеолог, продюсер, модератор, казначей, хранитель традиций и тп. В ОКЦ используем «ролевую модель управления» вместо «лидер и команда» (иначе лидер выгорает или тиранизируется, а команда простаивает и интригует). Сложнее на старте, устойчивее в кризисах.

⎯⎯⎯⎯

В городском исследовании — несколько тысяч анкет. В районе центров — 412 человек, 204 готовы участвовать. У многих годами своя тема в одиночку.

Строки из анкет:

«винил, музыка, плёночная фотография, поездки в исторические места области»

«генеалогия, вязание, чтение истории, изучение иностранных языков»

«театр, телесные практики, импровизация, писательство»

«нумизматика, военная посуда, антиквариат»

«переводчик, кинематограф и литература»

Чего не хватает:

«техно-рейвы в кофейнях по утрам для работающих людей»

«мероприятий по интересам — для читающих, любителей кофе»

«реквизит и спектакль в сквере, как летом 2025 с Гаричем»

«пешеходные прогулки-лекции на темы природоведения»

Если что-то из этого — про вас, или своё, такое же конкретное и давно лежащее в столе — приходите.

⎯⎯⎯⎯

Регулярно. Раз в неделю (или в две — зависит от количества желающих).

Можно прийти с инициативой.
Можно прийти поддержать других.
Можно просто прийти послушать.

Открыто и бесплатно. Сообщество поможет начать. Собираться будем в Сигнале.

⎯⎯⎯⎯

14 мая, 18:00. ОКЦ Сигнал, Леонова, 22.

Регистрация:
https://signalcommunity.timepad.ru/event/3963929/

методология: tg: @postsovieturban
© Институт прикладной урбанистики
"""


def _scrub_4o_env() -> dict[str, str | None]:
    saved: dict[str, str | None] = {}
    for key in (
        "FOUR_O_TOKEN",
        "FOUR_4O_TOKEN",
        "OPENAI_API_KEY",
        "OPENAI_TOKEN",
        "OPENAI_KEY",
    ):
        saved[key] = os.environ.get(key)
        if key in os.environ:
            del os.environ[key]
    return saved


async def main() -> int:
    saved_4o = _scrub_4o_env()
    os.environ.setdefault("DB_PATH", "")
    os.environ["EVENT_PARSE_GEMMA_MODEL"] = os.environ.get(
        "EVENT_PARSE_GEMMA_MODEL", "gemma-4-31b-it"
    )
    os.environ["SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE"] = "1"
    # Pin writer lane to gemma4 so the smoke exercises the lollipop-light Gemma 4
    # writer prompt and does not attempt OpenAI 4o at all.
    os.environ["SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE"] = "gemma4"
    os.environ.setdefault("SMART_UPDATE_GEMMA_NATIVE_SCHEMA", "1")
    os.environ.setdefault("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
    os.environ.pop("EVENT_PARSE_ENABLE_4O_FALLBACK", None)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifact_dir = ROOT / "artifacts" / "codex"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    sandbox_db = artifact_dir / f"smart_update_g4_upstream_vk_4594_sandbox_{ts}.sqlite"
    if sandbox_db.exists():
        sandbox_db.unlink()
    md_path = artifact_dir / f"smart_update_g4_upstream_vk_smoke_4594_{ts}.md"
    json_path = md_path.with_suffix(".json")

    os.environ["DB_PATH"] = str(sandbox_db)

    import db as db_module
    from db import Database
    import smart_event_update as su
    import vk_intake
    # vk_intake/smart_event_update look up callables on the running `main` module
    # via runtime.require_main_attr. Import main so attributes are reachable.
    import main as main_mod
    sys.modules["main"] = main_mod

    database = Database(str(sandbox_db))
    await database.init()

    from sqlalchemy import select
    from models import Event

    parse_started = time.monotonic()
    drafts, festival_payload = await vk_intake.build_event_drafts_from_vk(
        SOURCE_TEXT,
        source_name=SOURCE_NAME,
        location_hint="Калининград",
        parse_gemma_model=os.environ["EVENT_PARSE_GEMMA_MODEL"],
    )
    parse_sec = time.monotonic() - parse_started
    if not drafts:
        raise RuntimeError("VK parser returned 0 drafts")
    draft = drafts[0]

    candidate = su.EventCandidate(
        source_type="vk",
        source_url=SOURCE_URL,
        source_text=SOURCE_TEXT,
        title=draft.title,
        date=draft.date or None,
        time=draft.time or "",
        time_is_default=bool(getattr(draft, "time_is_default", False)),
        end_date=draft.end_date or None,
        festival=draft.festival or None,
        location_name=draft.venue or "",
        location_address=draft.location_address or None,
        city=draft.city or None,
        ticket_link=(draft.links[0] if draft.links else None),
        ticket_price_min=draft.ticket_price_min,
        ticket_price_max=draft.ticket_price_max,
        event_type=draft.event_type or None,
        emoji=draft.emoji or None,
        is_free=bool(draft.is_free),
        pushkin_card=bool(draft.pushkin_card),
        search_digest=draft.search_digest,
        raw_excerpt=draft.description or "",
    )

    su.reset_smart_update_llm_trace()
    update_started = time.monotonic()
    update_result = await su.smart_event_update(
        database,
        candidate,
        check_source_url=False,
    )
    update_sec = time.monotonic() - update_started
    trace = list(su.get_smart_update_llm_trace())

    persisted: dict[str, object] = {}
    facts_clean: list[str] = []
    facts_raw: list[str] = []
    body_text = ""
    short_description = ""
    search_digest = ""
    if update_result.event_id:
        async with database.get_session() as session:
            ev = await session.get(Event, update_result.event_id)
            if ev is not None:
                for col in (
                    "title",
                    "event_type",
                    "date",
                    "time",
                    "end_date",
                    "location_name",
                    "location_address",
                    "city",
                    "ticket_price_min",
                    "ticket_price_max",
                    "ticket_link",
                    "is_free",
                    "pushkin_card",
                    "lifecycle_status",
                    "short_description",
                    "search_digest",
                ):
                    persisted[col] = getattr(ev, col, None)
                body_text = getattr(ev, "description", "") or ""
                short_description = getattr(ev, "short_description", "") or ""
                search_digest = getattr(ev, "search_digest", "") or ""
                facts_raw = list(getattr(ev, "facts", []) or [])
                facts_clean = list(getattr(ev, "facts_text_clean", []) or []) if hasattr(ev, "facts_text_clean") else []

    await database.close()

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_url": SOURCE_URL,
        "model": os.environ["EVENT_PARSE_GEMMA_MODEL"],
        "four_o_disabled": True,
        "draft_fields": {
            "title": draft.title,
            "date": draft.date,
            "time": draft.time,
            "venue": draft.venue,
            "location_address": draft.location_address,
            "city": draft.city,
            "event_type": draft.event_type,
            "links": list(getattr(draft, "links", []) or []),
            "is_free": bool(draft.is_free),
            "pushkin_card": bool(draft.pushkin_card),
        },
        "smart_update_status": getattr(update_result, "status", None),
        "smart_update_event_id": getattr(update_result, "event_id", None),
        "smart_update_reason": getattr(update_result, "reason", None),
        "parse_sec": round(parse_sec, 3),
        "smart_update_sec": round(update_sec, 3),
        "trace": [
            {k: v for k, v in entry.items() if k in {"label", "kind", "duration_sec", "status", "provider_errors"}}
            for entry in trace
        ],
        "persisted": persisted,
        "facts_raw": facts_raw,
        "facts_text_clean": facts_clean,
        "body_text": body_text,
        "short_description": short_description,
        "search_digest": search_digest,
    }

    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines: list[str] = []
    md_lines.append("# Smart Update G4 Upstream VK Smoke (PRODDB-4594, harder fixture)\n")
    md_lines.append(f"- generated_at: `{summary['generated_at']}`")
    md_lines.append(f"- artifact_json: `{json_path}`")
    md_lines.append(f"- sandbox_db: `{sandbox_db}`")
    md_lines.append(f"- model: `{summary['model']}`")
    md_lines.append("- 4o: disabled/cleared; OPENAI/FOUR_O envs scrubbed before run")
    md_lines.append(f"- smart_update_status: `{summary['smart_update_status']}`")
    md_lines.append(f"- smart_update_event_id: `{summary['smart_update_event_id']}`")
    md_lines.append(f"- parse_sec: `{summary['parse_sec']}`")
    md_lines.append(f"- smart_update_sec: `{summary['smart_update_sec']}`")
    md_lines.append("\n## Source Text\n")
    md_lines.append("```text")
    md_lines.append(SOURCE_TEXT.strip())
    md_lines.append("```\n")
    md_lines.append("## Parsed Draft Fields\n")
    for key, value in summary["draft_fields"].items():
        md_lines.append(f"- {key}: `{value}`")
    md_lines.append("\n## Persisted Fields\n")
    for key, value in persisted.items():
        md_lines.append(f"- {key}: `{value}`")
    md_lines.append("\n## Smart Update LLM Trace\n")
    md_lines.append("| Label | Kind | Sec | Status | Provider errors |")
    md_lines.append("| --- | --- | ---: | --- | ---: |")
    for entry in trace:
        md_lines.append(
            "| `{label}` | `{kind}` | `{sec}` | `{status}` | `{errs}` |".format(
                label=entry.get("label"),
                kind=entry.get("kind"),
                sec=round(float(entry.get("duration_sec") or 0.0), 3),
                status=entry.get("status"),
                errs=entry.get("provider_errors"),
            )
        )
    md_lines.append("\n## Candidate Telegraph Body\n")
    md_lines.append("```md")
    md_lines.append(body_text.strip())
    md_lines.append("```\n")
    md_lines.append("## Derived Fields\n")
    md_lines.append(f"- short_description: `{short_description}`")
    md_lines.append(f"- search_digest: `{search_digest}`")
    md_lines.append("\n## Candidate Facts (raw)\n")
    for f in facts_raw:
        md_lines.append(f"- {f}")
    md_lines.append("\n## Candidate facts_text_clean\n")
    for f in facts_clean:
        md_lines.append(f"- {f}")

    md_path.write_text("\n".join(md_lines).rstrip() + "\n", encoding="utf-8")
    print(f"OK status={summary['smart_update_status']} event_id={summary['smart_update_event_id']}")
    print(f"artifact_md={md_path}")
    print(f"artifact_json={json_path}")

    # Restore 4o env vars (just in case the runner reuses the shell).
    for key, value in saved_4o.items():
        if value is not None:
            os.environ[key] = value
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
