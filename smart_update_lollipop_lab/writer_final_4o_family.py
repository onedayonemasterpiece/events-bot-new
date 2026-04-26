from __future__ import annotations

from dataclasses import dataclass
import re
import textwrap
from typing import Any


@dataclass(slots=True)
class ValidationResult:
    errors: list[str]
    warnings: list[str]


REPORT_PATTERNS: dict[str, str] = {
    "характеризуется": r"(?iu)\bхарактериз\w*",
    "event_dedicated": r"(?iu)\bсобыти\w+[^.!?\n]{0,24}\bпосвящ\w*",
    "program_filled": r"(?iu)(?:программ\w+|концерт|вечер)[^.!?\n]{0,40}\bнаполнен\w*",
    "stories_presented": r"(?iu)(?:истори\w+[^.!?\n]{0,24}представлен\w*|представлен\w*[^.!?\n]{0,24}истори\w+)",
    "program_consists": r"(?iu)программ\w+[^.!?\n]{0,32}\bсостоит\s+из\b",
}

PROMO_PATTERNS: dict[str, str] = {
    "cta": r"(?iu)не\s+упустите",
    "invites_you": r"(?iu)приглаш(?:ает|аем)\s+вас",
    "invites_viewers": r"(?iu)приглаш\w+\s+зрител",
    "invites": r"(?iu)\bприглаша\w*",
    "unique_opportunity": r"(?iu)уникальн\w+\s+возможност",
    "for_connoisseurs": r"(?iu)для\s+(?:всех\s+)?ценител",
    "wont_leave_indifferent": r"(?iu)не\s+оставит\s+равнодуш",
    "true_celebration": r"(?iu)настоящ\w+\s+праздник",
    "unforgettable": r"(?iu)незабыва\w+",
    "promises_to_be": r"(?iu)обеща\w+\s+стать",
    "promises": r"(?iu)\bобеща\w*",
    "offers_viewers": r"(?iu)\bпредлага\w+\s+зрител",
}

LEAD_HOOK_PATTERNS: dict[str, str] = {
    "rarity": r"(?iu)\bредк\w+|\bраз\s+в\s+сезон\w*|\bдолгождан\w+|\bдва\s+вечер\w+\s+подряд",
    "atmosphere": r"(?iu)\bатмосфер\w+|\bинтриг\w+|\bигр\w+|\bволшебн\w+|\bромантическ\w+",
    "program": r"(?iu)\bарии?\b|\bдуэт\w*|\bтерцет\w*|\bмарш\w*|\bпесенк\w*|\bмелоди\w+",
    "stage": r"(?iu)\bсценическ\w+\s+дым\b|\bхор\b|\bбалет\b|\bоркестр\b",
    "local_context": r"(?iu)\bкалининград\w*|\bвнучат\w+\s+мюнхгаузен\w*|\bнаши\s+края\b",
}


def _lead_needs_format_bridge(pack: dict[str, Any]) -> bool:
    event_type = str(pack.get("event_type") or "").strip().lower()
    return bool(pack.get("title_context", {}).get("is_bare")) and event_type in {"кинопоказ", "screening", "презентация", "presentation", "лекция", "lecture"}


def _section_opening_hint_fact_id(section: dict[str, Any]) -> str | None:
    facts = [item for item in list(section.get("facts") or []) if isinstance(item, dict)]
    if not facts:
        return None

    def _rank(item: dict[str, Any]) -> tuple[int, int, int]:
        fact_id = str(item.get("fact_id") or "").strip()
        text = str(item.get("text") or "")
        style_penalty = 0
        if re.search(r"(?iu)\b(посвящ\w*|характериз\w*|представлен\w*|наполнен\w*)\b", text):
            style_penalty += 1
        if re.search(r"(?iu)\b(афиш\w*|возрастн|\b(?:6|12|16|18)\+\b)\b", text):
            style_penalty += 2
        prefix_rank = {
            "SC": 0,
            "FL": 0,
            "EC": 1,
            "PR": 2,
            "PL": 3,
        }.get(fact_id[:2], 4)
        return (style_penalty, prefix_rank, -int(item.get("priority") or 0))

    best = min(facts, key=_rank)
    best_fact_id = str(best.get("fact_id") or "").strip()
    return best_fact_id or None


def _describe_text_quality(description_md: str) -> dict[str, Any]:
    text = str(description_md or "")
    lead_paragraph = re.split(r"\n\s*\n", text.strip())[0] if text.strip() else ""
    report_hits = [label for label, pattern in REPORT_PATTERNS.items() if re.search(pattern, text)]
    promo_hits = [label for label, pattern in PROMO_PATTERNS.items() if re.search(pattern, text)]
    lead_hook_hits = [label for label, pattern in LEAD_HOOK_PATTERNS.items() if re.search(pattern, lead_paragraph)]
    return {
        "report_formula_hits": report_hits,
        "promo_phrase_hits": promo_hits,
        "poster_leak": bool(re.search(r"(?iu)\bафиш\w*", text)),
        "age_leak": bool(re.search(r"(?iu)\b(?:6|12|16|18)\+\b|возрастн", text)),
        "lead_meta_opening": bool(
            re.search(r"(?iu)—\s*это\b", lead_paragraph)
        ),
        "lead_hook_signals": lead_hook_hits,
    }


def _build_prompt(pack: dict[str, Any]) -> str:
    meta = pack.get("meta") if isinstance(pack.get("meta"), dict) else {}
    fast_additions = ""
    if meta.get("variant") == "lollipop_g4_fast":
        profile = meta.get("writer_profile") if isinstance(meta.get("writer_profile"), dict) else {}
        profile_lines = [
            "FAST RUN CONTRACT:",
            "- ABSOLUTE FAIL CONDITIONS before returning JSON: no lead `— это`, no `это редкое событие`, no words with root `зрител`, no `смогут насладиться`, no `предлагает зрителям`, no words with root `приглаша`, no words with root `обещ`, no `и другие` when named-role facts are present, no words with root `наполн`, no `состоит из`, no `является`.",
            "- In fast output, the word root `зрител` is banned. Use event/music/stage as the grammatical subject instead.",
            "- Before returning JSON, scan description_md yourself. If it contains `зрител`, `обещ`, `приглаша`, `наслад`, `предлагает зрителям`, `погружая зрителей`, `является`, `состоит из`, or root `наполн`, rewrite that sentence before returning.",
            "- The phrase `погружая зрителей` is forbidden. Use `на сцене разворачивается`, `сюжет ведёт`, `история переносит действие`, or `мир фантазии раскрывается` instead.",
            "- meta.variant=lollipop_g4_fast means upstream is compact, source-local Gemma 4 extraction plus deterministic packing.",
            "- This is coverage-first fast, not summary-fast: every fact_id in must_cover_fact_ids must be semantically used unless it is explicitly covered by a literal list.",
            "- Do not shorten by omission. Compression is allowed only by connecting facts in better sentences.",
            "- The target is at least baseline fact coverage plus stronger hook, livelier natural voice, grounded quotes/atmosphere when available, and no promo/report cliches.",
            "- Fast has no semantic repair/rescue layer: returned JSON must already be the main-flow result.",
            f"- writer_profile: lead_strategy={profile.get('lead_strategy')!r}, rich_case={bool(profile.get('rich_case'))}, must_cover_fact_count={profile.get('must_cover_fact_count')}, narrative_fact_count={profile.get('narrative_fact_count')}, hook_types={profile.get('hook_types') or []}, buckets={profile.get('buckets') or []}.",
        ]
        if profile.get("rich_case"):
            profile_lines.append("- Rich case: prefer 2-4 real paragraphs/sections with editorial flow. Do not collapse the event into a mini-card.")
            profile_lines.append("- If named-role sections are present, write enough narrative before the first people/credits heading so the event does not read like only a cast sheet.")
            profile_lines.append("- Hard shape for rich people-heavy cases: before the first `###` heading, write at least two separate narrative paragraphs. Paragraph 1 = hook/format; paragraph 2 = plot/atmosphere/material. Do not solve this as one long paragraph.")
        if profile.get("has_rarity"):
            profile_lines.append("- Rarity is present: use it as a lead or early support hook unless format clarity would suffer.")
            profile_lines.append("- For rarity leads, the first sentence should start with `Раз в сезон` or `Редкий концерт/спектакль/показ`, then continue through event action. Do not start with `Концерт «...» — это`.")
        if profile.get("has_atmosphere"):
            profile_lines.append("- Atmosphere is present: weave it into the lead or first body opening, not as an afterthought.")
        if profile.get("has_quote_candidate"):
            profile_lines.append("- Quote-like grounded wording is present: you may use one short source-shaped phrase if it reads natural; do not invent quotes.")
        if profile.get("has_local_context"):
            profile_lines.append("- Local/historical context is present: use it as a distinguishing hook, especially for opaque/family stage titles.")
            profile_lines.append("- For local-context leads, good shape: `В Калининграде у барона Мюнхгаузена есть собственный след: ...`; bad shape: `Мюзикл приглашает зрителей ...`.")
            profile_lines.append("- For family/stage titles, open with local fact plus stage action. Preferred Vivat shape: `В Калининграде у барона Мюнхгаузена есть собственный след: мюзикл ... выводит на сцену новые приключения его потомков.`")
            profile_lines.append("- For local/family stage titles, use the second narrative paragraph for plot or character premise when the pack has it; do not jump straight from local hook to credits.")
        if profile.get("has_literal_program"):
            profile_lines.append("- Literal program is present: keep exact markdown bullets in the program section, but also explain what kind of evening those works create.")
            profile_lines.append("- Anti-repetition: if the lead already names ensemble units (солисты/хор/оркестр/балет) or program forms (дуэты/арии/терцеты/марши), do not restate the same set in the first body paragraph. Body must add a different angle: composer frame, atmosphere, rarity, staging, or local context.")
        if profile.get("has_named_roles"):
            profile_lines.append("- Named roles/participants are present: preserve role information instead of replacing it with generic `участники`.")
            profile_lines.append("- If a people/roles fact contains a comma-separated named list, every full name in that fact is mandatory. Do not write `и другие`, `включая ...` with only a subset, or `среди солистов` if it hides names.")
            profile_lines.append("- For long casts, use dense role-lines or compact sentences separated by semicolons; never solve length by dropping names.")
        profile_lines.extend(
            [
                "- If a fact came from poster/admin wording such as `афиша`, keep the underlying rarity/attendance meaning but never copy poster language into public prose.",
                "- Do not put calendar dates, exact times, venue names, ticket/access/age notes into narrative prose.",
                "- If frequency (`раз в сезон`) and scheduling intensity (`два вечера подряд`) both survived into the pack, treat them as distinct facts and use both naturally when possible.",
                "- Avoid audience-template phrases: `зрители смогут`, `смогут насладиться`, `зрителей ждут`, `подарит эмоции`. Describe what is on stage instead.",
                "- ЖЁСТКИЙ ЗАПРЕТ: в итоговом description_md не должно быть слов/подстрок `зрител`, `погружая зрителей`, `приглаш`, `наслад`, `обещ`, `предлагает`, `настоящим праздником`, `для любителей`, `уникаль`, `подарит эмоции`, `масса эмоций`.",
                "- If named-role facts are present, `и другие` / `и др.` is also banned in description_md.",
                "- FIRST SENTENCE CONTRACT: open through grounded event action, rarity, programme, atmosphere, quote-like hook, or stage movement. Bad shape: `Концерт «...» — это ...`.",
                "- Replace any draft phrase `приглашает зрителей` with an event-action construction: `выводит на сцену`, `рассказывает`, `собирает`, `держится на`, `звучит`, `разворачивает историю`.",
            ]
        )
        fast_additions = "\n".join(profile_lines)
    title_context = pack["title_context"]
    section_lines: list[str] = []
    for section in pack["sections"]:
        heading = section.get("heading")
        coverage_modes = [f"{item['fact_id']}:{item['mode']}" for item in list(section.get("coverage_plan") or [])]
        opening_hint = _section_opening_hint_fact_id(section)
        line = (
            f"- role={section['role']}, style={section['style']}, heading={heading!r}, fact_ids={section['fact_ids']}, "
            f"coverage={coverage_modes}, literal_items={section.get('literal_items') or []}, opening_hint_fact_id={opening_hint!r}"
        )
        section_lines.append(line)
    must_cover_count = len(pack["constraints"]["must_cover_fact_ids"])
    if must_cover_count >= 10:
        length_line = "По объёму ориентируйся примерно на 800-1400 знаков"
    elif must_cover_count >= 4:
        length_line = "По объёму ориентируйся примерно на 500-900 знаков"
    else:
        length_line = "По объёму ориентируйся примерно на 400-700 знаков"
    headings_source = list(pack["constraints"].get("headings") or [])
    section_headings = [
        str(section.get("heading")).strip()
        for section in pack["sections"]
        if str(section.get("heading") or "").strip()
    ]
    for heading in section_headings:
        if heading not in headings_source:
            headings_source.append(heading)
    headings = [f"use exact heading: ### {heading}" for heading in headings_source]
    return textwrap.dedent(
        f"""
        Ты — writer.final_4o.v1 для lollipop.

        Верни только JSON:
        {{
          "title": "string",
          "description_md": "string"
        }}

        {fast_additions}

        СТРУКТУРА (соблюдай порядок и exact headings):
        {chr(10).join(section_lines)}

        event_type: "{pack['event_type']}"
        title_needs_format_clarity: {str(bool(title_context.get('is_bare'))).lower()}
        lead_needs_format_bridge: {str(_lead_needs_format_bridge(pack)).lower()}
        must_cover_fact_ids: {pack['constraints']['must_cover_fact_ids']}
        первое предложение должно сразу прояснить формат события
        первое предложение обязано прямо назвать формат события через `event_type`
        На каждой границе `section` начинай новый абзац
        {length_line}
        Плохое открытие: `Режиссёр фильма — ...`
        REGISTER TARGET: живой культурный анонс для городского дайджеста, а не отчёт, карточка или реклама.
        POSITIVE VOICE EXEMPLARS:
        - `Раз в сезоне на сцене собираются хиты Кальмана — от «Сильвы» до «Фиалок Монмартра».`
        - `Два вечера подряд на сцене звучат хиты Кальмана — от «Сильвы» до «Фиалок Монмартра».`
        - `На сцене собираются солисты, хор, оркестр и балет театра в постановке Ильи Ильина.`
        - `Вечер держится на романтических историях, атмосфере интриги и игры и целой палитре жанров — от арий до маршей.`
        В lead и в начале каждого narrative section выноси самый сильный grounded hook раньше сухой справки.
        Не прячь самый интересный факт в конец абзаца, если он может открыть абзац естественно и честно.
        Для концертов и list-heavy cases lead должен звучать как обещание вечера, а не как каталогическое `событие посвящено ...`.
        Избегай неестественных report-style формул вроде `характеризуется`, `представлены истории`, `наполнена`, `посвящен/посвящена`, если ту же мысль можно выразить живее и естественнее без потери фактов.
        Если fact text сам звучит сухо или канцелярски, перепиши его в естественный event-facing русский, сохранив точный смысл и grounding.
        Предпочитай конкретное событие и действие: `в этот вечер звучат ...`, `концерт собирает ...`, `на сцене ...`, `в программе ...`, а не meta-фразы вроде `это уникальная возможность`, `этот вечер обещает стать`, `программа наполнена`, `в концерте представлены истории`.
        Если grounded rarity/support fact формулируется через `афишу`, сохрани смысл редкости, но не переноси poster-language буквально в public prose.
        Lead formula for concert/list-heavy cases: формат события + rarity/character hook + один конкретный material/detail fact.
        Если в pack есть grounded rarity fact, используй его как lead hook раньше secondary credit.
        Если в pack есть grounded atmosphere fact, вплетай его в первый абзац или первое narrative body opening, а не в хвост.
        Избегай copula-heavy meta-openings на `это`, `является`, `обещает стать`, если тот же смысл можно выразить через сценическое действие, звучание или устройство вечера.
        Не используй шаблон `X — это ...` в первом предложении, если можно сразу показать, что звучит, собирается или происходит на сцене.
        Первое предложение не должно содержать последовательность `— это`.
        Не переходи на зрительский шаблон `зрители смогут насладиться`; пусть subject остаётся у самого события, музыки, программы или сцены.
        Не открывай narrative section с generic ensemble-category line вроде `участвуют солисты, оркестр, хор и балет`, если в этом же section есть более vivid atmosphere/rarity/material hook.
        Если service/admin note не является главным содержанием события, не делай его финальной сильной нотой текста.
        Возрастные ограничения и прочие admin/access notes не должны попадать в public narrative prose.
        Если fact text содержит явный список имён или ролей, сохрани все явно названные имена и роли.
        Если в pack есть 3+ non-people narrative facts и затем people/credits sections, перед первым heading должно быть не менее двух содержательных narrative paragraphs или один lead плюс отдельный body paragraph.
        Для fast rich_case с people sections это жёсткое требование: два отдельных абзаца до первого `###`, не один длинный абзац.
        Не теряй plot/character premise ради списков участников: сначала объясни, что происходит в событии и почему это живо, затем переходи к ролям.
        Non-people fact coverage важнее компактности: если факт не logistics/admin и не duplicate, он должен быть явно использован в narrative prose.
        Не сокращай grounded named lists до `и другие`, если исходный fact не помечен как partial list.
        Для people-heavy structured section делай плотные role-lines, а не общий пересказ.
        Если section содержит literal_items или coverage mode = literal_list / narrative_plus_literal_list, обязателен реальный markdown bullet list в этой section.
        Простого упоминания literal_items в прозе недостаточно: literal list должен быть выведен отдельными строками `- ...`.
        Если есть отдельная program section с literal_items, не перечисляй исчерпывающий repertoire list уже в lead; сохрани lead компактным и вынеси полный список в program section.
        BANNED FORMULAS:
        - `приглашает вас` / `приглашаем вас`
        - `обещает стать настоящим праздником` / `настоящий праздник`
        - `для ценителей` / `для всех ценителей`
        - `уникальная возможность` / `не упустите шанс`
        - `X — это ...` как lead opening
        - `зрители смогут насладиться ...`
        - `программа состоит из`
        - `событие посвящено ...`
        - `характеризуется` / `концерт наполнен` / `вечер наполнен` / `программа наполнена` / `представлены истории`
        - `не оставит равнодушным`
        WHAT TO DO INSTEAD:
        - Subject of sentences = the event, the music, the stage, the programme.
        - Open with the strongest grounded hook from the pack.
        - Connect facts with natural editorial flow, not one-fact-per-sentence.
        {' '.join(headings)}

        PACK JSON:
        {pack}
        """
    ).strip()


def _build_retry_prompt(pack: dict[str, Any], validation: ValidationResult) -> str:
    lines = [
        _build_prompt(pack),
        "",
        "Исправь ошибки валидатора и верни заново только JSON той же схемы.",
        "Это correction pass: перепиши текст естественно, а не перечисляй коды ошибок.",
    ]
    errors = list(validation.errors)
    if "lead.cliche_posvyash" in errors:
        lines.append("- Перепиши lead без слов с корнем `посвящ...`; открой текст через формат события, редкость или атмосферу вечера.")
    if "lead.meta_opening" in errors:
        lines.append("- Убери lead-шаблон `X — это ...`; первое предложение начни через редкость, сценическое действие, музыку или устройство вечера.")
        lines.append("- Запрещён паттерн `Концерт/спектакль/событие «Название» — это ...`; замени его на действие: `Раз в сезон ... собирает/звучит/выходит на сцену ...`.")
    if "poster.leak" in errors:
        lines.append("- Убери любые упоминания `афиша` / `афиши`; замени на более естественное event-facing wording без poster language.")
    if "age.leak" in errors:
        lines.append("- Полностью убери возрастные ограничения и прочие admin/access notes из narrative prose.")
    if any(item.startswith("style.report_formula:") for item in errors):
        lines.append("- Убери report-style формулы и замени их на естественный язык культурного анонса.")
    if "style.report_formula:program_filled" in errors:
        lines.append("- Не пиши `концерт/вечер/программа наполнены ...`; тот же смысл передай через `на сцене звучат ...`, `вечер держится на ...` или `в программе ...`.")
    if any(item.startswith("style.promo_phrase:") for item in errors):
        lines.append("- Убери CTA и рекламные обещания вроде `уникальная возможность`, `не упустите шанс`, `не оставит равнодушным`; тон должен оставаться живым, но не промо-агитационным.")
    if any(item.startswith("style.audience_template:") for item in errors):
        lines.append("- Убери зрительские шаблоны вроде `зрители смогут насладиться`; тот же смысл передавай через музыку, сцену, программу или устройство вечера.")
    if any(
        item.startswith("style.report_formula:") or item.startswith("style.promo_phrase:")
        for item in errors
    ):
        lines.append("- Сохрани тот же фактологический hook, но перепиши его через конкретное событие, звучание, сценическое действие или устройство вечера, а не через meta-оценки.")
        lines.append("- Не меняй report-language на promo-language и наоборот; нужен третий регистр: живой сдержанный дайджест.")
    if "named_list.collapsed_to_and_others" in errors:
        lines.append("- Восстанови все явно перечисленные имена из grounded cast/team facts; не используй `и другие`.")
        explicit_lists = [
            fact["text"]
            for section in pack["sections"]
            for fact in list(section.get("facts") or [])
            if isinstance(fact, dict)
            and str(fact.get("text") or "").count(",") >= 2
            and re.search(r"(?u)[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)+", str(fact.get("text") or ""))
        ]
        if explicit_lists:
            lines.append("  Явные списки, которые нужно сохранить:")
            for item in explicit_lists:
                lines.append(f"  - {item}")
    remaining = [item for item in errors if item not in {"lead.cliche_posvyash", "poster.leak", "age.leak", "named_list.collapsed_to_and_others"} and not item.startswith("style.report_formula:")]
    if remaining:
        lines.append("Ошибки валидатора:")
        for item in remaining:
            lines.append(f"- {item}")
    return "\n".join(lines).strip()


def _apply_writer_output(pack: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    title = str(output.get("title") or "").strip()
    if pack["title_context"]["strategy"] == "keep":
        title = pack["title_context"]["original_title"]
    return {"title": title, "description_md": str(output.get("description_md") or "")}


def _validate_writer_output(pack: dict[str, Any], output: dict[str, Any]) -> ValidationResult:
    meta = pack.get("meta") if isinstance(pack.get("meta"), dict) else {}
    title = str(output.get("title") or "").strip()
    description_md = str(output.get("description_md") or "")
    errors: list[str] = []
    warnings: list[str] = []

    if pack["title_context"]["strategy"] == "keep" and title != pack["title_context"]["original_title"]:
        warnings.append("title.keep_overridden_by_model")
    if pack["title_context"]["strategy"] == "enhance" and title == pack["title_context"]["original_title"]:
        errors.append("title.enhance_unchanged")

    for row in list(pack.get("infoblock") or []):
        value = str(row.get("value") or "").strip()
        if value and value in description_md:
            errors.append(f"infoblock.leak:{row['fact_id']}")

    expected_headings = list(pack["constraints"].get("headings") or []) or [
        str(section.get("heading")).strip()
        for section in pack["sections"]
        if str(section.get("heading") or "").strip()
    ]
    actual_headings = [match.group(1).strip() for match in re.finditer(r"(?m)^###\s+(.+?)\s*$", description_md)]
    for heading in actual_headings:
        if heading not in expected_headings:
            errors.append(f"heading.invented:{heading}")
    for heading in expected_headings:
        if heading not in actual_headings:
            warnings.append(f"heading.missing:{heading}")

    for section in pack["sections"]:
        literal_items = list(section.get("literal_items") or [])
        if not literal_items:
            continue
        for literal in literal_items:
            if re.search(rf"(?m)^\-\s+{re.escape(literal)}\s*$", description_md):
                continue
            if re.search(rf"(?m)^\-\s+{re.escape(literal.replace('«', '').replace('»', ''))}\s*$", description_md):
                warnings.append(f"literal.format_mutation:{literal}")
                continue
            errors.append(f"literal.missing:{literal}")
        if section.get("literal_list_is_partial") and not re.search(r"(?iu)(среди|в программе также|например|в том числе)", description_md):
            errors.append("list.partial_intro_missing")

    lines = description_md.splitlines()
    bullet_line_index = next((idx for idx, line in enumerate(lines) if re.match(r"^\-\s+\S", line)), None)
    if bullet_line_index is not None:
        prev_nonempty = ""
        for idx in range(bullet_line_index - 1, -1, -1):
            if lines[idx].strip():
                prev_nonempty = lines[idx].strip()
                break
        has_narrative_plus_list = any(
            item["mode"] == "narrative_plus_literal_list"
            for section in pack["sections"]
            for item in list(section.get("coverage_plan") or [])
        )
        if not prev_nonempty.startswith("### "):
            if prev_nonempty.endswith(":"):
                pass
            elif has_narrative_plus_list and prev_nonempty.endswith("."):
                pass
            else:
                errors.append("list.unintroduced_block:3")

    lead_paragraph = re.split(r"\n\s*\n", description_md.strip())[0] if description_md.strip() else ""
    initials_safe = re.sub(r"\b([А-ЯA-ZЁ])\.", r"\1<prd>", lead_paragraph)
    lead_sentences = [
        item.replace("<prd>", ".")
        for item in re.split(r"(?<=[.!?])\s+", initials_safe)
        if item.strip()
    ]
    if len(lead_sentences) > 2:
        warnings.append("lead.too_long")
    profile = meta.get("writer_profile") if isinstance(meta.get("writer_profile"), dict) else {}
    people_headings = [
        str(section.get("heading") or "").strip()
        for section in list(pack.get("sections") or [])
        if any(
            isinstance(fact, dict)
            and str(fact.get("fact_id") or "").startswith("PR")
            for fact in list(section.get("facts") or [])
        )
    ]
    first_declared_heading = next(
        (
            str(section.get("heading") or "").strip()
            for section in list(pack.get("sections") or [])
            if str(section.get("heading") or "").strip()
        ),
        "",
    )
    if (
        meta.get("variant") == "lollipop_g4_fast"
        and profile.get("rich_case")
        and people_headings
        and first_declared_heading in set(people_headings)
    ):
        first_people_heading = next((heading for heading in people_headings if heading), "")
        marker = f"\n### {first_people_heading}" if first_people_heading else "\n### "
        before_heading = description_md.split(marker, 1)[0].strip() if marker in description_md else description_md.split("\n### ", 1)[0].strip()
        narrative_paragraphs = [
            item.strip()
            for item in re.split(r"\n\s*\n", before_heading)
            if item.strip() and not item.strip().startswith("#")
        ]
        if len(narrative_paragraphs) < 2:
            errors.append("body.missing_narrative_before_people")
    if _lead_needs_format_bridge(pack):
        event_type = str(pack["event_type"] or "").strip().lower()
        if event_type not in lead_paragraph.lower():
            warnings.append("lead.format_anchor_missing")

    if re.search(r"(?iu)\bпосвящ\w*", lead_paragraph):
        errors.append("lead.cliche_posvyash")
    if re.search(r"(?iu)—\s*это\b", lead_paragraph):
        errors.append("lead.meta_opening")
    if re.search(r"(?iu)\bафиш\w*", description_md):
        errors.append("poster.leak")
    if re.search(r"(?iu)\b(?:6|12|16|18)\+\b|возрастн", description_md):
        errors.append("age.leak")
    quality_profile = _describe_text_quality(description_md)
    for label in quality_profile["report_formula_hits"]:
        errors.append(f"style.report_formula:{label}")
    for label in quality_profile["promo_phrase_hits"]:
        errors.append(f"style.promo_phrase:{label}")
    if re.search(r"(?iu)зрител\w+[^.!?\n]{0,48}(?:смогут\s+)?наслад", description_md):
        errors.append("style.audience_template:will_enjoy")
    if meta.get("variant") == "lollipop_g4_fast" and re.search(r"(?iu)\bзрител\w*", description_md):
        errors.append("style.audience_template:viewers_root")

    has_explicit_named_people_list = any(
        str(section.get("role") or "").strip() != "program"
        and any(
            isinstance(fact, dict)
            and str(fact.get("text") or "").count(",") >= 2
            and re.search(r"(?u)[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)+", str(fact.get("text") or ""))
            for fact in list(section.get("facts") or [])
        )
        for section in pack["sections"]
    )
    if has_explicit_named_people_list and re.search(r"(?iu)\bи другие\b|\bи др\.?\b", description_md):
        errors.append("named_list.collapsed_to_and_others")

    return ValidationResult(errors=errors, warnings=warnings)
