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
    "stories_presented": r"(?iu)(?:истори\w+[^.!?\n]{0,24}представлен(?:а|о|ы|ный|ная|ное|ные|ным|ной|ными)?\b|представлен(?:а|о|ы|ный|ная|ное|ные|ным|ной|ными)?\b[^.!?\n]{0,24}истори\w+)",
    "program_consists": r"(?iu)программ\w+[^.!?\n]{0,32}\bсостоит\s+из\b",
}

PROMO_PATTERNS: dict[str, str] = {
    "cta": r"(?iu)не\s+упустите",
    "invites_you": r"(?iu)приглаш(?:ает|аем)\s+вас",
    "unique_opportunity": r"(?iu)уникальн\w+\s+возможност",
    "for_connoisseurs": r"(?iu)для\s+(?:всех\s+)?ценител",
    "wont_leave_indifferent": r"(?iu)не\s+оставит\s+равнодуш",
    "true_celebration": r"(?iu)настоящ\w+\s+праздник",
    "unforgettable": r"(?iu)незабыва\w+",
    "promises_to_be": r"(?iu)обеща\w+\s+стать",
}

LEAD_HOOK_PATTERNS: dict[str, str] = {
    "rarity": r"(?iu)\bредк\w+|\bраз\s+в\s+сезон\w*|\bдолгождан\w+|\bдва\s+вечер\w+\s+подряд",
    "atmosphere": r"(?iu)\bатмосфер\w+|\bинтриг\w+|\bигр\w+|\bволшебн\w+|\bромантическ\w+",
    "program": r"(?iu)\bарии?\b|\bдуэт\w*|\bтерцет\w*|\bмарш\w*|\bпесенк\w*|\bмелоди\w+",
    "stage": r"(?iu)\bсценическ\w+\s+дым\b|\bхор\b|\bбалет\b|\bоркестр\b",
    "format_texture": r"(?iu)\b(?:аудио\w*|звук\w+|звуков\w+|маршрут\w+|прогулк\w+|сакральн\w+|священн\w+|промысл\w+|народн\w+\s+искусств\w+)\b",
    "event_action": r"(?iu)\b(?:открыва\w+|старту\w+|разбер\w+|расскаж\w+|раскро\w+|погружа\w+|исслед\w+|прослеж\w+|становит\w+\s+проводник\w*|приглаша\w+\s+задуматься|в\s+центре\s+внимания|в\s+преддверии|повседневн\w+|суров\w+)\b",
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
    if re.search(r"(?iu)зрител\w+[^.!?\n]{0,32}смогут\s+наслад", description_md):
        errors.append("style.audience_template:will_enjoy")

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
