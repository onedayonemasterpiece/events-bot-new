from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from smart_update_lollipop_lab.fast_cascade import _fast_literal_gate_errors, _fast_merge_passthrough_records, run_fast_cascade_variant
from smart_update_lollipop_lab.fast_extract_family import build_fast_extract_compact_system_prompt, normalize_fast_extract_items, normalize_fast_merge_items
from smart_update_lollipop_lab.writer_final_4o_family import _validate_writer_output


@dataclass(slots=True)
class SourcePacket:
    source_id: str
    source_type: str
    url: str
    text: str


@dataclass(slots=True)
class Fixture:
    fixture_id: str
    title: str
    event_type: str
    date: str | None
    time: str | None
    location_name: str | None
    location_address: str | None
    city: str | None
    sources: list[SourcePacket]


@pytest.mark.asyncio
async def test_lollipop_g4_fast_can_use_llm_merge_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOLLIPOP_G4_FAST_LLM_MERGE", "1")
    fixture = Fixture(
        fixture_id="FAST-KALMANIA",
        title="Кальмания",
        event_type="концерт",
        date="2026-04-03",
        time=None,
        location_name="Музыкальный театр",
        location_address=None,
        city="Калининград",
        sources=[
            SourcePacket(
                "site",
                "parser",
                "https://example.test/site",
                "Редкий гость в афише: концерт Кальмания. В программе «Баядера» и «Фиалки Монмартра».",
            ),
            SourcePacket(
                "tg",
                "telegram",
                "https://example.test/tg",
                "На сцене солисты, хор, оркестр и балет театра. Вечер держится на атмосфере интриги и игры.",
            ),
        ],
    )
    gemma_stage_labels: list[str] = []
    four_o_prompts: list[str] = []

    async def fake_gemma_json_call(**kwargs: Any) -> dict[str, Any]:
        assert kwargs.get("response_schema")
        if "source" not in kwargs["user_payload"]:
            gemma_stage_labels.append("merge")
            return {
                "facts": [
                    {
                        "text": "Концерт «Кальмания» появляется в репертуаре редко.",
                        "bucket": "event_core",
                        "salience": "must_keep",
                        "hook_type": "rarity",
                        "literal_items": [],
                        "source_refs": ["site"],
                        "record_ids": ["FASSIT_01"],
                    },
                    {
                        "text": "Вечер держится на атмосфере интриги и игры.",
                        "bucket": "support_context",
                        "salience": "must_keep",
                        "hook_type": "atmosphere",
                        "literal_items": [],
                        "source_refs": ["tg"],
                        "record_ids": ["FASTG_02"],
                    },
                    {
                        "text": "В программе звучат номера из «Баядеры» и «Фиалок Монмартра».",
                        "bucket": "program_list",
                        "salience": "must_keep",
                        "hook_type": "program_literal",
                        "literal_items": ["«Баядера»", "«Фиалки Монмартра»"],
                        "source_refs": ["site"],
                        "record_ids": ["FASSIT_02"],
                    },
                    {
                        "text": "На сцене собираются солисты, хор, оркестр и балет театра.",
                        "bucket": "people_and_roles",
                        "salience": "support",
                        "hook_type": "staging",
                        "literal_items": [],
                        "source_refs": ["tg"],
                        "record_ids": ["FASTG_01"],
                    },
                ]
            }
        label = kwargs["user_payload"]["source"]["source_id"]
        gemma_stage_labels.append(label)
        if label == "site":
            return {
                "facts": [
                    {
                        "text": "Концерт «Кальмания» появляется в репертуаре редко.",
                        "evidence": "Редкий гость в афише",
                        "bucket": "event_core",
                        "salience": "must_keep",
                        "hook_type": "rarity",
                        "literal_items": [],
                        "dedup_key": "rarity_kalmania",
                        "source_refs": ["site"],
                    },
                    {
                        "text": "В программе звучат номера из «Баядеры» и «Фиалок Монмартра».",
                        "evidence": "«Баядера» и «Фиалки Монмартра»",
                        "bucket": "program_list",
                        "salience": "must_keep",
                        "hook_type": "program_literal",
                        "literal_items": ["«Баядера»", "«Фиалки Монмартра»"],
                        "dedup_key": "program_titles_kalmania",
                        "source_refs": ["site"],
                    },
                ]
            }
        return {
            "facts": [
                {
                    "text": "На сцене собираются солисты, хор, оркестр и балет театра.",
                    "evidence": "солисты, хор, оркестр и балет",
                    "bucket": "people_and_roles",
                    "salience": "support",
                    "hook_type": "staging",
                    "literal_items": [],
                    "dedup_key": "ensemble_stage",
                    "source_refs": ["tg"],
                },
                {
                    "text": "Вечер держится на атмосфере интриги и игры.",
                    "evidence": "атмосфере интриги и игры",
                    "bucket": "support_context",
                    "salience": "must_keep",
                    "hook_type": "atmosphere",
                    "literal_items": [],
                    "dedup_key": "atmosphere_intrigue_game",
                    "source_refs": ["tg"],
                },
            ]
        }

    async def fake_four_o_json_call(*, prompt: str, schema: dict[str, Any], model: str) -> dict[str, Any]:
        assert schema["required"] == ["title", "description_md"]
        assert model == "gpt-4o"
        assert "meta.variant=lollipop_g4_fast" in prompt
        assert "coverage-first fast" in prompt
        assert "FAST OUTPUT SHAPE" not in prompt
        assert "writer_profile: lead_strategy='rarity'" in prompt
        assert "The phrase `погружая зрителей` is forbidden" in prompt
        four_o_prompts.append(prompt)
        return {
            "title": "Кальмания",
            "description_md": (
                "Концерт «Кальмания» появляется в репертуаре редко.\n\n"
                "Вечер держится на атмосфере интриги и игры.\n\n"
                "### Программа\n"
                "В программе звучат номера из классических оперетт:\n"
                "- «Баядера»\n"
                "- «Фиалки Монмартра»\n\n"
                "### Участники\n"
                "На сцене собираются солисты, хор, оркестр и балет театра."
            ),
        }

    async def fake_sleep(_: float) -> None:
        return None

    result = await run_fast_cascade_variant(
        fixture=fixture,
        gemma_model="gemma-4-31b-it",
        gemma4=True,
        gemma_json_call=fake_gemma_json_call,
        four_o_json_call=fake_four_o_json_call,
        sleep=fake_sleep,
        four_o_model="gpt-4o",
        gemma_call_gap_s=0,
    )

    assert result["variant_mode"] == "lollipop_g4_fast"
    assert gemma_stage_labels == ["site", "tg", "merge"]
    assert len(four_o_prompts) == 1
    assert result["writer_pack"]["meta"]["variant"] == "lollipop_g4_fast"
    assert result["writer_pack"]["meta"]["contract_version"] == "lollipop_g4_fast.v2"
    assert result["writer_pack"]["meta"]["writer_profile"]["has_rarity"] is True
    assert result["writer_pack"]["meta"]["writer_profile"]["has_atmosphere"] is True
    assert result["writer_pack"]["meta"]["writer_profile"]["has_literal_program"] is True
    assert result["timings"]["gemma_calls"] == len(fixture.sources) + 1
    assert result["timings"]["four_o_calls"] == 1
    assert result["validation"]["errors"] == []
    assert result["fast_merge_stats"]["merge_owner"] == "fast.merge_pack.v1"
    assert result["fast_merge_stats"]["output_records"] == 4
    assert result["metrics"]["must_cover_fact_count"] == 4


@pytest.mark.asyncio
async def test_lollipop_g4_fast_default_multi_source_passthrough() -> None:
    fixture = Fixture(
        fixture_id="FAST-KALMANIA-PASSTHROUGH",
        title="Кальмания",
        event_type="концерт",
        date=None,
        time=None,
        location_name=None,
        location_address=None,
        city="Калининград",
        sources=[
            SourcePacket("site", "parser", "https://example.test/site", "Редкий концерт Кальмания."),
            SourcePacket("vk", "vk", "https://example.test/vk", "Кальмания проходит раз в сезоне."),
        ],
    )
    gemma_stage_labels: list[str] = []

    async def fake_gemma_json_call(**kwargs: Any) -> dict[str, Any]:
        assert "source" in kwargs["user_payload"]
        label = kwargs["user_payload"]["source"]["source_id"]
        gemma_stage_labels.append(label)
        return {
            "facts": [
                {
                    "text": "Редкий концерт «Кальмания» проходит раз в сезоне.",
                    "evidence": "раз в сезоне",
                    "bucket": "event_core",
                    "salience": "must_keep",
                    "hook_type": "rarity",
                    "literal_items": [],
                    "dedup_key": f"rarity_{label}",
                    "source_refs": [label],
                }
            ]
        }

    async def fake_four_o_json_call(*, prompt: str, schema: dict[str, Any], model: str) -> dict[str, Any]:
        return {
            "title": "Кальмания",
            "description_md": "Раз в сезон концерт «Кальмания» возвращает на сцену редкий театральный вечер.",
        }

    async def fake_sleep(_: float) -> None:
        return None

    result = await run_fast_cascade_variant(
        fixture=fixture,
        gemma_model="gemma-4-31b-it",
        gemma4=True,
        gemma_json_call=fake_gemma_json_call,
        four_o_json_call=fake_four_o_json_call,
        sleep=fake_sleep,
        four_o_model="gpt-4o",
        gemma_call_gap_s=0,
    )

    assert gemma_stage_labels == ["site", "vk"]
    assert result["timings"]["gemma_calls"] == len(fixture.sources)
    assert result["fast_merge_stats"]["merge_owner"] == "default_no_llm_merge_passthrough"


def test_lollipop_g4_fast_flags_merge_literal_mutation() -> None:
    errors = _fast_literal_gate_errors(
        extract_records=[
            {"record_id": "E01", "text": "В программе звучит «Цыган-премьера».", "literal_items": ["«Цыган-премьера»"]},
        ],
        merged_records=[
            {"record_id": "M01", "literal_items": ["«Сыган-премьера»"]},
        ],
    )

    assert errors == ["literal.title_mutation:M01:«Сыган-премьера»"]


def test_lollipop_g4_fast_accepts_merge_literal_from_extractor_text() -> None:
    errors = _fast_literal_gate_errors(
        extract_records=[
            {"record_id": "E01", "text": "В программе звучит «Марица».", "literal_items": []},
        ],
        merged_records=[
            {"record_id": "M01", "literal_items": ["«Марица»"]},
        ],
    )

    assert errors == []


def test_lollipop_g4_fast_merge_literal_allowlist_drops_provider_noise() -> None:
    records = normalize_fast_merge_items(
        payload={
            "facts": [
                {
                    "text": "В программе звучат названия.",
                    "bucket": "program_list",
                    "salience": "must_keep",
                    "hook_type": "program_literal",
                    "literal_items": ["«Сильва»", "« SSCПринцесса цирка»", "«Сильва»"],
                    "source_refs": ["site"],
                    "record_ids": ["E01"],
                }
            ]
        },
        source_records=[
            {"record_id": "E01", "text": "В программе звучит «Сильва».", "literal_items": ["«Сильва»"]},
        ],
    )

    assert records[0]["literal_items"] == ["«Сильва»"]


def test_lollipop_g4_fast_extract_literal_items_must_exist_in_source_excerpt() -> None:
    records = normalize_fast_extract_items(
        payload={
            "facts": [
                {
                    "text": "В программе звучит «Фиалка Монмартра».",
                    "evidence": "«Фиалки Монмартра»",
                    "bucket": "program_list",
                    "salience": "must_keep",
                    "hook_type": "program_literal",
                    "literal_items": ["«Фиалка Монмартра»"],
                    "dedup_key": "program_titles",
                    "source_refs": ["vk"],
                },
                {
                    "text": "В программе звучит «Фиалки Монмартра».",
                    "evidence": "«Фиалки Монмартра»",
                    "bucket": "program_list",
                    "salience": "must_keep",
                    "hook_type": "program_literal",
                    "literal_items": ["«Фиалки Монмартра»"],
                    "dedup_key": "program_titles_exact",
                    "source_refs": ["vk"],
                },
            ]
        },
        source_id="vk",
        record_prefix="FASVK_",
        source_excerpt="В программе звучит «Фиалки Монмартра».",
    )

    assert len(records) == 1
    assert records[0]["literal_items"] == ["«Фиалки Монмартра»"]


def test_lollipop_g4_fast_passthrough_dedups_by_llm_owned_key() -> None:
    records = _fast_merge_passthrough_records(
        [
            {
                "record_id": "FASSIT_01",
                "text": "Концерт «Кальмания» появляется в репертуаре редко.",
                "bucket": "event_core",
                "salience": "support",
                "hook_type": "rarity",
                "literal_items": [],
                "dedup_key": "rarity_kalmania",
                "source_refs": ["site"],
            },
            {
                "record_id": "FASVK_01",
                "text": "Редкий концерт «Кальмания» проходит раз в сезоне.",
                "bucket": "event_core",
                "salience": "must_keep",
                "hook_type": "rarity",
                "literal_items": [],
                "dedup_key": "rarity_kalmania",
                "source_refs": ["vk"],
            },
        ]
    )

    assert len(records) == 1
    assert records[0]["text"] == "Редкий концерт «Кальмания» проходит раз в сезоне."
    assert records[0]["record_ids"] == ["FASSIT_01", "FASVK_01"]
    assert records[0]["source_refs"] == ["site", "vk"]


def test_lollipop_g4_fast_extractor_prompt_groups_long_cast_lists() -> None:
    prompt = build_fast_extract_compact_system_prompt()

    assert "instead of emitting one record per person/role" in prompt
    assert "one production_team fact, one cast fact, and one ensemble fact" in prompt
    assert "Audience suitability like `для всей семьи`" in prompt
    assert "plot/character premise" in prompt
    assert "Барон Мюнхгаузен показан учёным" in prompt
    assert "new adventures" in prompt


def test_lollipop_g4_fast_validation_requires_body_before_people_sections() -> None:
    pack = {
        "event_type": "мюзикл",
        "title_context": {"original_title": "Виват, Мюнхгаузен!", "strategy": "keep", "is_bare": False},
        "sections": [
            {
                "role": "lead",
                "style": "narrative",
                "heading": None,
                "fact_ids": ["EC01", "SC01"],
                "facts": [
                    {"fact_id": "EC01", "text": "Мюзикл рассказывает о новых приключениях потомков барона.", "priority": 3},
                    {"fact_id": "SC01", "text": "Барон находит необычное в обычных ситуациях.", "priority": 2},
                ],
                "coverage_plan": [{"fact_id": "EC01", "mode": "narrative"}, {"fact_id": "SC01", "mode": "narrative"}],
                "literal_items": [],
            },
            {
                "role": "body",
                "style": "structured",
                "heading": "Действующие лица и исполнители",
                "fact_ids": ["PR01"],
                "facts": [{"fact_id": "PR01", "text": "В ролях: барон Мюнхгаузен — Антон Арнтгольц.", "priority": 3}],
                "coverage_plan": [{"fact_id": "PR01", "mode": "narrative"}],
                "literal_items": [],
            },
        ],
        "infoblock": [],
        "constraints": {"headings": ["Действующие лица и исполнители"], "must_cover_fact_ids": ["EC01", "SC01", "PR01"]},
        "meta": {
            "variant": "lollipop_g4_fast",
            "writer_profile": {"rich_case": True, "narrative_fact_count": 2, "has_named_roles": True},
        },
    }

    validation = _validate_writer_output(
        pack,
        {
            "title": "Виват, Мюнхгаузен!",
            "description_md": (
                "Мюзикл выводит на сцену потомков барона, а сам Мюнхгаузен находит необычное в обычных ситуациях.\n\n"
                "### Действующие лица и исполнители\n"
                "В ролях: барон Мюнхгаузен — Антон Арнтгольц."
            ),
        },
    )

    assert "body.missing_narrative_before_people" in validation.errors

    ok_validation = _validate_writer_output(
        pack,
        {
            "title": "Виват, Мюнхгаузен!",
            "description_md": (
                "Мюзикл выводит на сцену потомков барона.\n\n"
                "Сам Мюнхгаузен находит необычное в обычных ситуациях, и это держит историю живой.\n\n"
                "### Действующие лица и исполнители\n"
                "В ролях: барон Мюнхгаузен — Антон Арнтгольц."
            ),
        },
    )

    assert "body.missing_narrative_before_people" not in ok_validation.errors




@pytest.mark.asyncio
async def test_lollipop_g4_fast_skips_merge_for_single_relevant_source() -> None:
    fixture = Fixture(
        fixture_id="FAST-SINGLE-RELEVANT",
        title="Виват, Мюнхгаузен!",
        event_type="мюзикл",
        date=None,
        time=None,
        location_name="Музыкальный театр",
        location_address=None,
        city="Калининград",
        sources=[
            SourcePacket("site", "parser", "https://example.test/site", "Мюзикл Виват, Мюнхгаузен о новых приключениях потомков барона."),
            SourcePacket("tg", "telegram", "https://example.test/tg", "Второй вечер даем Кальманию с аншлагом."),
        ],
    )
    gemma_stage_labels: list[str] = []

    async def fake_gemma_json_call(**kwargs: Any) -> dict[str, Any]:
        assert "source" in kwargs["user_payload"]
        label = kwargs["user_payload"]["source"]["source_id"]
        gemma_stage_labels.append(label)
        if label == "tg":
            return {
                "facts": [
                    {
                        "text": "Источник говорит о другом событии: «Кальмания».",
                        "evidence": "Кальманию",
                        "bucket": "drop",
                        "salience": "suppress",
                        "hook_type": "none",
                        "literal_items": [],
                        "dedup_key": "source_mismatch_kalmania",
                        "source_refs": ["tg"],
                    }
                ]
            }
        return {
            "facts": [
                {
                    "text": "Мюзикл «Виват, Мюнхгаузен!» рассказывает о новых приключениях потомков барона Мюнхгаузена.",
                    "evidence": "о новых приключениях его потомков",
                    "bucket": "event_core",
                    "salience": "must_keep",
                    "hook_type": "format_action",
                    "literal_items": [],
                    "dedup_key": "vivat_new_adventures",
                    "source_refs": ["site"],
                }
            ]
        }

    async def fake_four_o_json_call(*, prompt: str, schema: dict[str, Any], model: str) -> dict[str, Any]:
        return {
            "title": "Виват, Мюнхгаузен!",
            "description_md": "Мюзикл «Виват, Мюнхгаузен!» рассказывает о новых приключениях потомков барона Мюнхгаузена.",
        }

    async def fake_sleep(_: float) -> None:
        return None

    result = await run_fast_cascade_variant(
        fixture=fixture,
        gemma_model="gemma-4-31b-it",
        gemma4=True,
        gemma_json_call=fake_gemma_json_call,
        four_o_json_call=fake_four_o_json_call,
        sleep=fake_sleep,
        four_o_model="gpt-4o",
        gemma_call_gap_s=0,
    )

    assert gemma_stage_labels == ["site", "tg"]
    assert result["timings"]["gemma_calls"] == len(fixture.sources)
    assert result["fast_merge_stats"]["merge_owner"] == "single_relevant_source_passthrough"
    assert result["fast_merge_stats"]["relevant_source_ids"] == ["site"]
    assert len(result["merged_records"]) == 1
