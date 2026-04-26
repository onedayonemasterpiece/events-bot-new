from __future__ import annotations

import re
import textwrap
from typing import Any


FAST_EXTRACT_STAGE_ID = "fast.extract_per_source.v1"
FAST_MERGE_STAGE_ID = "fast.merge_pack.v1"
FAST_PLANNER_STAGE_ID = "fast.layout_planner.v1"

BUCKETS = [
    "event_core",
    "program_list",
    "people_and_roles",
    "forward_looking",
    "support_context",
    "uncertain",
    "infoblock_only",
    "drop",
]
SALIENCE_VALUES = ["must_keep", "support", "suppress", "uncertain"]
HOOK_TYPES = [
    "format_action",
    "rarity",
    "atmosphere",
    "quote",
    "local_context",
    "program_literal",
    "people_roles",
    "staging",
    "logistics",
    "other",
    "none",
]
ROLE_CLASSES = ["production_team", "cast", "ensemble", "none"]


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _clip_text(value: Any, *, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    clipped = text[:limit].rsplit(" ", 1)[0].strip()
    return clipped or text[:limit].strip()


def _event_relevant_excerpt(value: Any, *, title: str, limit: int) -> str:
    text = _clean_text(value)
    title_text = _clean_text(title)
    if title_text:
        lowered = text.lower()
        title_lowered = title_text.lower()
        positions: list[int] = []
        start = 0
        while True:
            idx = lowered.find(title_lowered, start)
            if idx < 0:
                break
            positions.append(idx)
            start = idx + len(title_lowered)
        if positions:
            content_idx = positions[1] if len(positions) > 1 else positions[0]
            if content_idx > 0:
                text = text[content_idx:]
    for marker in (
        "Добавить комментарий",
        "Ваш e-mail не будет опубликован",
        "Оценить качество работы",
        "Мы используем cookie",
        "Материалы антитеррористической направленности",
    ):
        idx = text.find(marker)
        if idx > 900:
            text = text[:idx]
    comment_match = re.search(r"(?iu)\b\d+\s+комментари", text)
    if comment_match and comment_match.start() > 900:
        text = text[: comment_match.start()]
    return _clip_text(text, limit=limit)


def build_fast_extract_system_prompt() -> str:
    return textwrap.dedent(
        """
        You do one bounded step: fast.extract_per_source.v1 for a cultural event digest.
        Return exactly one JSON object. No prose, no markdown fences, no analysis.

        CORE CONTRACT
        - This call sees one source only. Treat source-local uniqueness as valuable.
        - Extract compact grounded facts that can feed a later public Telegraph event description.
        - Do not write public prose and do not plan the article layout.
        - You own the semantic labels: bucket, salience, hook_type, literal_items, dedup_key.
        - Python will only merge and pack your labels, so do not leave meaningful choices implicit.
        - Do not emit date/time/venue/tickets/age/access notes as narrative facts; use bucket=infoblock_only or drop.

        QUALITY TARGET
        - Preserve facts that make the text livelier and more natural than a dry baseline: format/action, rarity, atmosphere, staging, named roles, explicit repertoire/program items.
        - Keep official-source atmosphere or rarity when it characterises the event experience, but strip poster/admin language from the fact text.
        - Anti-promo: never add CTA or hype. Avoid `уникальная возможность`, `приглашает вас`, `не упустите`, `для ценителей`, `не оставит равнодушным`.
        - Anti-report: do not rewrite into `событие посвящено`, `характеризуется`, `наполнена`, `представлены истории`, `программа состоит из`.
        - Prefer event-facing Russian grounded in the source: `звучат`, `собирает`, `на сцене`, `в программе`, `вечер держится на`.

        LITERAL FIDELITY
        - Preserve titles of works exactly as written in the source: quotes, spelling, order.
        - literal_items must contain only verbatim program/repertoire/work titles from an explicit source list.
        - Do not put event title, people names, venue names, date/time, rarity words, or ensemble categories into literal_items.
        - If the excerpt is cut mid-name or mid-title, suppress that named fact instead of emitting a partial token.
        - If the source lists works like «Баядера» and «Фиалки Монмартра», keep those exact strings in literal_items.

        DEDUP_KEY CONTRACT
        - dedup_key is a stable semantic key for exact/near duplicate merge across sources.
        - Use lowercase snake-ish Russian/Latin words, no source id, no date.
        - Same underlying fact across sources should get the same dedup_key.
        - Distinct facts must not share dedup_key: a rarity line, named repertoire list, and ensemble/staging line are separate keys.

        BUCKET GUIDE
        - event_core: what attendable event this is, its format/action, central material.
        - program_list: explicit repertoire/program/work-title list or program forms.
        - people_and_roles: named people/collectives with explicit roles or participation.
        - forward_looking: future-facing attendance-relevant hook, premiere/rare showing/one-off frame.
        - support_context: atmosphere, staging, theme, source-local colour that helps the description feel alive.
        - uncertain: grounded but weak/ambiguous; keep only if it may matter.
        - infoblock_only: logistics/admin/access note, not for narrative prose.
        - drop: generic promo, URL/ticket noise, duplicate boilerplate, unrelated event.

        SALIENCE GUIDE
        - must_keep: losing it would make the final text worse or less truthful than baseline.
        - support: useful supporting detail.
        - suppress: grounded but should usually stay out of public narrative.
        - uncertain: may be useful but needs cautious downstream handling.

        POSITIVE EXAMPLES
        - Source says: `Редкий гость в афише — Кальмания`.
          Emit text like: `«Кальмания» появляется в репертуаре редко.` bucket=support_context salience=must_keep hook_type=rarity dedup_key=rarity_kalmania.
          Do not emit public prose with the word `афиша`.
        - Source lists: `«Баядера», «Сильва», «Фиалки Монмартра»`.
          Emit one program_list fact, literal_items exactly [`«Баядера»`, `«Сильва»`, `«Фиалки Монмартра»`], hook_type=program_literal.
        - Source says: `романтические истории, атмосфера интриги и игры`.
          Emit a support_context fact; do not rewrite it as `вечер характеризуется романтическими историями`.

        NEGATIVE EXAMPLES
        - Bad: `Программа концерта наполнена ариями и маршами.` Use `В программе звучат арии, дуэты, терцеты и марши.`
        - Bad: `Событие посвящено опереттам Кальмана.` Use the grounded event action and material instead.
        - Bad: collapse `«Баядера»` and `«Фиалки Монмартра»` into `известные оперетты`.

        OUTPUT JSON
        {
          "facts": [
            {
              "text": "Russian grounded compact fact",
              "evidence": "short source quote or source-shaped evidence",
              "bucket": "event_core|program_list|people_and_roles|forward_looking|support_context|uncertain|infoblock_only|drop",
              "salience": "must_keep|support|suppress|uncertain",
              "hook_type": "format_action|rarity|atmosphere|quote|local_context|program_literal|people_roles|staging|logistics|other|none",
              "literal_items": ["verbatim title"],
              "dedup_key": "stable_key",
              "source_refs": ["source_id"]
            }
          ]
        }
        """
    ).strip()


def build_fast_extract_compact_system_prompt() -> str:
    return textwrap.dedent(
        """
        You do one bounded step: fast.extract_per_source.v1.
        Return exactly one JSON object. No prose, no markdown fences.

        This is a source-local Gemma 4 fast run. It is coverage-first, not summary-fast.
        Emit every distinct event-facing fact that must survive into a public Telegraph event description.
        There is no fixed fact cap: a rich source may legitimately emit 8-14 facts; a sparse source may emit 1-4.
        Never pad, but never omit a grounded fact only because the source is rich.

        Scope guard:
        - The target event_title is the event being described. If this source is primarily about a different event, do not extract facts about that other event.
        - For a different-event source, emit at most one drop fact explaining the mismatch, with bucket=drop, salience=suppress, hook_type=none.
        - Example: event_title=`Виват, Мюнхгаузен!`, source text is about `Кальмания` with Kálmán/opеретта/cast. Output only a drop mismatch fact; do not emit Kальмания facts.

        You own semantic labels: bucket, salience, hook_type, literal_items, dedup_key.
        Python will only merge/pack your labels; do not leave meaningful choices implicit.

        Keep:
        - event format/action and central material;
        - plot/character premise that explains why the event is interesting: protagonist traits, unusual setup, conflict, descendants/continuation frame, stage-world hook;
        - rarity/scarcity/frequency if grounded (`редкий`, `раз в сезоне`, `долгожданный`, `два вечера подряд`);
        - atmosphere/staging if it makes the event more alive and specific;
        - local/historical context when it helps explain why this event is specific to this city or venue;
        - short grounded quote-like official wording when it can become a vivid public hook without becoming promo;
        - named people/collectives with explicit roles;
        - named cast/team lists under headings like `Действующие лица и исполнители`, `Солисты`, `Артисты балета`, `Постановочная группа`;
        - explicit repertoire/program/work-title lists.

        Drop or mark infoblock_only:
        - facts about a different primary event than event_title;
        - date/time/venue/tickets/age/access/admin notes;
        - CTA, hashtags, subscription lines, prices, URLs;
        - generic promo with no grounded event detail.
        - Audience suitability like `для всей семьи`, `для детей`, `для взрослых и детей` is not event_core. Mark it as support_context/support at most, and suppress it when stronger event-action or plot/local-context facts exist.
        - If a source mixes exact dates with a useful rarity/frequency hook, keep only the hook, e.g. `два вечера подряд`, and omit calendar dates.

        Voice for fact text:
        - Russian, compact, grounded, event-facing.
        - Fact text must be a complete event-facing clause with an action/verb, not a noun-card fragment.
        - Bad event_core: `Концерт «Кальмания» с романтическими историями из оперетт`.
        - Good event_core: `Концерт «Кальмания» собирает романтические истории из оперетт Имре Кальмана.`
        - Bad event_core: `Мюзикл «Виват, Мюнхгаузен!» предназначен для всей семьи.`
        - Good event_core: `Мюзикл «Виват, Мюнхгаузен!» рассказывает о новых приключениях потомков барона Мюнхгаузена.`
        - Good support_context: `Барон Мюнхгаузен показан учёным, предприимчивым и весёлым мечтателем, который находит приключения в обычных ситуациях.`
        - Good support_context: `История делает Мюнхгаузена героем, на которого хотят походить взрослые и дети.`
        - Bad rarity: `Редкий и долгожданный концерт, который проходит раз в сезоне`.
        - Good rarity: `Редкий концерт «Кальмания» проходит раз в сезоне.`
        - Avoid report formulas in extracted fact text: `посвящено`, `характеризуется`, `наполнена`, `наполнен`, `наполняет`, `представлены`, `программа состоит из`.
        - If the source says `наполненный легкой музыкой`, rewrite the grounded meaning as `держится на легкой, волшебной музыке` or `в концерте звучит легкая, волшебная музыка`.
        - Avoid promo: `уникальная возможность`, `приглашает вас`, `не упустите`, `для ценителей`.
        - If source says `афиша` only as rarity/poster language, keep rarity meaning without the word `афиша`.
        - Stage smoke/allergy/safety warnings are not atmosphere. Mark them infoblock_only unless the source explicitly describes smoke as artistic staging rather than a visitor warning.
        - For theatre pages, public-facing creators/cast are useful; purely backstage service roles such as lighting operator, sound operator, assistant, or curator should be salience=suppress unless the source makes them artistically event-defining.

        Literal fidelity:
        - literal_items contains only verbatim work/program titles from an explicit list.
        - Preserve spelling/quotes exactly, e.g. `«Баядера»`, `«Фиалки Монмартра»`.
        - Put one title per array item. Never put a full sentence fragment or half sentence into literal_items.
        - Do not put people, event title, venue, dates, rarity words, or roles into literal_items.
        - If the excerpt is cut mid-name or mid-title, suppress that named fact instead of emitting a partial token.

        Title declension fidelity:
        - Copy work titles in the exact case/number/declension form they appear in the source.
        - Do not normalize `«Фиалки Монмартра»` to `«Фиалка Монмартра»`; if the source has the plural/genitive form, keep it.
        - Do not silently fix a source typo like `«Фиалки Монматра»` to `«Фиалки Монмартра»`; copy the cited source form.
        - If a title is cut mid-word in the excerpt, suppress that named fact entirely instead of guessing the full form.

        dedup_key:
        - stable semantic key, lowercase snake-ish, no source id/date.
        - Same fact across sources => same key.
        - Distinct rarity, atmosphere, title-list, people/staging facts => different keys.

        Buckets: event_core, program_list, people_and_roles, forward_looking, support_context, uncertain, infoblock_only, drop.
        Salience: must_keep, support, suppress, uncertain.
        hook_type: format_action, rarity, atmosphere, quote, local_context, program_literal, people_roles, staging, logistics, other, none.

        Plot/character coverage:
        - For musicals, plays and family stage titles, do not stop at `new adventures` when the source gives the story engine.
        - Preserve protagonist traits, stage-world premise and emotional angle as separate support_context/support or event_core/support facts when grounded.
        - Audience suitability alone is weak, but a grounded line like `герой, на которого хочется походить и взрослым и детям` is a character/audience-emotion fact; keep the character meaning without turning it into `для всей семьи`.

        Named-list fidelity:
        - If the source lists named solists, ballet artists, directors, conductors, designers, or other role-bearing participants, emit those names in people_and_roles facts.
        - Long named lists are not noise, but fast extraction must be compact: keep all names inside dense role-list facts instead of emitting one record per person/role.
        - Good cast fact: `В ролях: барон Мюнхгаузен — Антон Арнтгольц; Фридрих — Антон Топорков; Марта — Юлия Русакова.`
        - Bad cast extraction: three separate facts for Антон Арнтгольц, Антон Топорков and Юлия Русакова when they came from one cast list.
        - Split only by source heading / role_class: production team, cast, ensemble. Do not collapse to `солисты театра` when the names are present.
        - Do not emit `и другие` unless the source itself says the list is partial.
        - Keep main creative credits and cast: director, composer, libretto, choreographer, set/costume/visual artists, conductor, named roles/performers, named ballet/cast groups.
        - Suppress purely technical/backstage credits by default: lighting operator, sound operator, assistant director, curator, administrator.

        role_class:
        - For people_and_roles facts, label the kind of role list you are emitting.
        - production_team: director, composer, librettist, choreographer, conductor, designer, main creative credits.
        - cast: named characters and their performers.
        - ensemble: named solists, ballet artists, choir/orchestra groups, unnamed stage collectives.
        - none: any non-people_and_roles fact.
        - Do not mix production_team and cast in the same people_and_roles fact; split them.
        - A typical theatre page should emit at most one production_team fact, one cast fact, and one ensemble fact unless the source has clearly separate named groups.

        Output:
        {"facts":[{"text":"...","evidence":"short quote","bucket":"...","salience":"...","hook_type":"...","role_class":"none","literal_items":[],"dedup_key":"...","source_refs":["source_id"]}]}
        """
    ).strip()


def fast_extract_response_schema() -> dict[str, Any]:
    item_schema = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "evidence": {"type": "STRING"},
            "bucket": {"type": "STRING", "format": "enum", "enum": BUCKETS},
            "salience": {"type": "STRING", "format": "enum", "enum": SALIENCE_VALUES},
            "hook_type": {"type": "STRING", "format": "enum", "enum": HOOK_TYPES},
            "role_class": {"type": "STRING", "format": "enum", "enum": ROLE_CLASSES},
            "literal_items": {"type": "ARRAY", "items": {"type": "STRING"}},
            "dedup_key": {"type": "STRING"},
            "source_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": [
            "text",
            "evidence",
            "bucket",
            "salience",
            "hook_type",
            "role_class",
            "literal_items",
            "dedup_key",
            "source_refs",
        ],
    }
    return {
        "type": "OBJECT",
        "properties": {"facts": {"type": "ARRAY", "items": item_schema}},
        "required": ["facts"],
    }


def build_fast_merge_system_prompt() -> str:
    return textwrap.dedent(
        """
        You do one bounded step: fast.merge_pack.v1.
        Return exactly one JSON object. No prose, no markdown fences.

        Input records already come from source-local Gemma 4 extraction.
        Your job is semantic merge/dedup and canonical pack emission. Python will not repair duplicated public prose later.

        Coverage-first merge: emit every distinct meaningful fact that should be available to the final writer.
        There is no fixed final-fact cap: a rich multi-source event may legitimately emit 10-18 facts.
        Merge semantic duplicates even when keys or wording differ, but do not compress two different facts into one vague summary.
        Merge participant collective duplicates into one best people_and_roles fact.
        Preserve named participant lists: if input records contain solist/ballet/team names, output people_and_roles facts with all those names.
        Never replace a named list with only `солисты/хор/оркестр/балет` when the names are present upstream.
        Merge program title-list duplicates into one program_list fact.
        If rarity facts include both `раз в сезон` and `два вечера подряд`, keep both when they mean different public facts: frequency and scheduling intensity are not duplicates.

        Preserve: event format/action; grounded rarity/frequency; atmosphere/staging; local/historical context; explicit program titles; named people and roles.
        Do not emit exact dates, times, venue, tickets, access or age notes. If a source record mixes `3 и 4 апреля` with `два вечера подряд`, keep only `два вечера подряд`.

        Writer readiness: event_core/support_context facts must be complete event-facing clauses with verbs.
        Good: `Редкий концерт «Кальмания» проходит раз в сезоне.`
        Bad: `Концерт «Кальмания» с романтическими историями из оперетт.`
        Bad: `Концерт «Кальмания» наполнен легкой музыкой.`
        Bad: `Постановка состоит из дуэтов и арий.`
        Bad: `Концерт является долгожданным возвращением жанра.`
        Good: `В концерте «Кальмания» звучит легкая, волшебная музыка Имре Кальмана.`
        Good: `В программе звучат дуэты, арии, терцеты, марши и песенки из любимых оперетт.`
        Good: `После трехлетней паузы оперетта возвращается в театр концертом «Кальмания».`
        Stage smoke/allergy/safety warnings are not narrative atmosphere; do not emit them unless they were marked as a non-narrative source fact.

        Literal title handling:
        - literal_items is the clean canonical title list for the merged program fact.
        - Every literal_items value must be copied exactly from one input record literal_items value.
        - Never change one letter inside a title; no typo correction, no spelling normalization by memory.
        - Do not duplicate one work in inflected/nominative forms.
        - If spelling differs, keep both only when they are genuinely different titles.
        - Do not invent titles absent from sources.

        Style guard: no CTA/promo, no `настоящий праздник`, no `для ценителей`, no report formulas, no `наполнена/наполнен/наполняет`, no `состоит из`, no `является`, no public-facing `афиша`.

        Output facts with record_ids that justify each merged fact.
        Buckets: event_core, program_list, people_and_roles, forward_looking, support_context, uncertain.
        Salience: must_keep, support, suppress, uncertain.
        hook_type: format_action, rarity, atmosphere, quote, local_context, program_literal, people_roles, staging, logistics, other, none.

        Output:
        {"facts":[{"text":"...","bucket":"...","salience":"...","hook_type":"...","literal_items":[],"source_refs":["source_id"],"record_ids":["record_id"]}]}
        """
    ).strip()


def fast_merge_response_schema() -> dict[str, Any]:
    item_schema = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "bucket": {"type": "STRING", "format": "enum", "enum": ["event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "uncertain"]},
            "salience": {"type": "STRING", "format": "enum", "enum": SALIENCE_VALUES},
            "hook_type": {"type": "STRING", "format": "enum", "enum": HOOK_TYPES},
            "literal_items": {"type": "ARRAY", "items": {"type": "STRING"}},
            "source_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
            "record_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["text", "bucket", "salience", "hook_type", "literal_items", "source_refs", "record_ids"],
    }
    return {
        "type": "OBJECT",
        "properties": {"facts": {"type": "ARRAY", "items": item_schema}},
        "required": ["facts"],
    }


def build_fast_merge_payload(*, fixture: Any, records: list[dict[str, Any]]) -> dict[str, Any]:
    compact_records = []
    for record in records:
        bucket = str(record.get("bucket") or "")
        if bucket in {"drop", "infoblock_only"}:
            continue
        compact_records.append(
            {
                "record_id": record.get("record_id"),
                "text": record.get("text"),
                "bucket": bucket,
                "salience": record.get("salience"),
                "hook_type": record.get("hook_type"),
                "role_class": record.get("role_class") or "none",
                "literal_items": record.get("literal_items") or [],
                "dedup_key": record.get("dedup_key"),
                "source_refs": record.get("source_refs") or [],
            }
        )
    return {
        "event_title": fixture.title,
        "event_type": fixture.event_type,
        "records": compact_records,
    }


def _literal_allowed_by_extractors(literal: str, source_records: list[dict[str, Any]]) -> bool:
    if not literal:
        return False
    for record in source_records:
        if literal in [str(item) for item in list(record.get("literal_items") or [])]:
            return True
        if literal in f"{record.get('text') or ''} {record.get('evidence') or ''}":
            return True
    return False


def normalize_fast_merge_items(*, payload: dict[str, Any], source_records: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    counter = 1
    allowed_buckets = {"event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "uncertain"}
    source_records = list(source_records or [])
    for raw in list(payload.get("facts") or []):
        if not isinstance(raw, dict):
            continue
        text = _clean_text(raw.get("text"))
        if not text:
            continue
        bucket = str(raw.get("bucket") or "support_context").strip()
        if bucket not in allowed_buckets:
            bucket = "support_context"
        salience = str(raw.get("salience") or "support").strip()
        if salience not in SALIENCE_VALUES:
            salience = "support"
        hook_type = str(raw.get("hook_type") or "other").strip()
        if hook_type not in HOOK_TYPES:
            hook_type = "other"
        record_ids = [_clean_text(item) for item in list(raw.get("record_ids") or []) if _clean_text(item)]
        literal_items = [_clean_text(item) for item in list(raw.get("literal_items") or []) if _clean_text(item)]
        if source_records:
            literal_items = [item for item in literal_items if _literal_allowed_by_extractors(item, source_records)]
        literal_items = list(dict.fromkeys(literal_items))
        records.append(
            {
                "record_id": f"FMG_{counter:02d}",
                "stage_id": FAST_MERGE_STAGE_ID,
                "text": text,
                "evidence": "",
                "bucket": bucket,
                "bucket_hint": bucket,
                "salience": salience,
                "hook_type": hook_type,
                "literal_items": literal_items,
                "dedup_key": f"fast_merge_{counter:02d}",
                "source_refs": [_clean_text(item) for item in list(raw.get("source_refs") or []) if _clean_text(item)],
                "record_ids": record_ids,
            }
        )
        counter += 1
    return records


def build_fast_extract_payload(*, fixture: Any, source: Any, limit: int = 3600) -> dict[str, Any]:
    return {
        "event_title": fixture.title,
        "event_type": fixture.event_type,
        "known_logistics": {
            "date": fixture.date,
            "time": fixture.time,
            "location_name": fixture.location_name,
            "location_address": fixture.location_address,
            "city": fixture.city,
        },
        "source": {
            "source_id": source.source_id,
            "source_type": source.source_type,
            "url": source.url,
            "excerpt": _event_relevant_excerpt(getattr(source, "text", ""), title=fixture.title, limit=limit),
        },
    }


def normalize_fast_extract_items(*, payload: dict[str, Any], source_id: str, record_prefix: str, source_excerpt: str = "") -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    counter = 1
    source_excerpt = str(source_excerpt or "")
    for raw in list(payload.get("facts") or []):
        if not isinstance(raw, dict):
            continue
        text = _clean_text(raw.get("text"))
        if not text:
            continue
        bucket = str(raw.get("bucket") or "support_context").strip()
        if bucket not in BUCKETS:
            bucket = "support_context"
        salience = str(raw.get("salience") or "support").strip()
        if salience not in SALIENCE_VALUES:
            salience = "support"
        hook_type = str(raw.get("hook_type") or "other").strip()
        if hook_type not in HOOK_TYPES:
            hook_type = "other"
        role_class = str(raw.get("role_class") or "none").strip()
        if role_class not in ROLE_CLASSES:
            role_class = "none"
        if bucket != "people_and_roles":
            role_class = "none"
        dedup_key = _clean_text(raw.get("dedup_key")).lower()
        if not dedup_key:
            dedup_key = f"{bucket}:{text[:80].lower()}"
        source_refs = [
            _clean_text(item)
            for item in list(raw.get("source_refs") or [])
            if _clean_text(item)
        ]
        if source_id not in source_refs:
            source_refs.insert(0, source_id)
        raw_literal_items = [
            _clean_text(item)
            for item in list(raw.get("literal_items") or [])
            if _clean_text(item)
        ]
        literal_items = list(dict.fromkeys(raw_literal_items))
        if source_excerpt:
            literal_items = [item for item in literal_items if item in source_excerpt]
            if raw_literal_items and not literal_items and bucket == "program_list":
                continue
        records.append(
            {
                "record_id": f"{record_prefix}{counter:02d}",
                "stage_id": FAST_EXTRACT_STAGE_ID,
                "text": text,
                "evidence": _clean_text(raw.get("evidence")),
                "bucket": bucket,
                "bucket_hint": bucket,
                "salience": salience,
                "hook_type": hook_type,
                "role_class": role_class,
                "literal_items": literal_items,
                "dedup_key": dedup_key,
                "source_refs": list(dict.fromkeys(source_refs)),
            }
        )
        counter += 1
    return records


def build_fast_planner_system_prompt() -> str:
    return textwrap.dedent(
        """
        You do one optional step: fast.layout_planner.v1.
        Return exactly one JSON object. No prose, no markdown fences.
        You may improve lead/layout choice only; do not rewrite facts.
        Use fact_ids from all_fact_ids only.
        Lead must clarify the event format/action and prefer rarity/atmosphere support when grounded.
        Keep literal program facts out of the lead when they can be shown in a program block.
        Infoblock remains last.

        OUTPUT JSON
        {
          "lead_fact_id": "fact_id",
          "lead_support_id": "",
          "title_strategy": "keep|enhance",
          "title_hint_ref": "",
          "blocks": [
            {"role": "lead|body|program|infoblock", "fact_refs": ["fact_id"], "style": "narrative|list|structured", "heading": ""}
          ]
        }
        """
    ).strip()


def fast_planner_response_schema() -> dict[str, Any]:
    block_schema = {
        "type": "OBJECT",
        "properties": {
            "role": {"type": "STRING", "format": "enum", "enum": ["lead", "body", "program", "infoblock"]},
            "fact_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
            "style": {"type": "STRING", "format": "enum", "enum": ["narrative", "list", "structured"]},
            "heading": {"type": "STRING"},
        },
        "required": ["role", "fact_refs", "style", "heading"],
    }
    return {
        "type": "OBJECT",
        "properties": {
            "lead_fact_id": {"type": "STRING"},
            "lead_support_id": {"type": "STRING"},
            "title_strategy": {"type": "STRING", "format": "enum", "enum": ["keep", "enhance"]},
            "title_hint_ref": {"type": "STRING"},
            "blocks": {"type": "ARRAY", "items": block_schema},
        },
        "required": ["lead_fact_id", "lead_support_id", "title_strategy", "title_hint_ref", "blocks"],
    }
