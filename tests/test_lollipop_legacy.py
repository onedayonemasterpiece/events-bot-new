from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

from smart_update_lollipop_lab import legacy_writer_family
from smart_update_lollipop_lab import writer_final_4o_family


def _load_benchmark_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "inspect" / "benchmark_lollipop_g4.py"
    spec = importlib.util.spec_from_file_location("benchmark_lollipop_g4", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# --- legacy_writer_family contract ------------------------------------------

def test_legacy_contract_version_is_v14() -> None:
    assert legacy_writer_family.LEGACY_CONTRACT_VERSION == "lollipop_legacy.v14"


def test_extraction_prompt_has_no_baseline_assumptions() -> None:
    prompt = legacy_writer_family.build_extraction_system_prompt()
    assert "baseline" not in prompt.lower()
    assert "public_facts" in prompt
    assert "logistics_facts" in prompt


def test_writer_prompt_has_no_baseline_or_repair_language() -> None:
    prompt = legacy_writer_family.build_writer_system_prompt()
    assert "baseline" in prompt.lower()
    assert "Baseline text/facts are not in the request" in prompt
    assert "repair" not in prompt.lower()
    assert "min_description_chars" in prompt


def test_writer_payload_has_no_baseline_keys() -> None:
    payload = legacy_writer_family.build_writer_payload(
        title="Лекция",
        event_type="лекция",
        public_facts=[{"index": 0, "text": "Лектор разберёт быт моряков.", "source_span": "", "kind": "topic"}],
        logistics_facts=[],
        source_excerpt="Лектор разберёт быт моряков.",
        reference_description_chars=800,
    )
    assert "baseline_description" not in payload
    assert "baseline_facts" not in payload
    assert payload["min_description_chars"] > 0
    assert payload["target_description_chars"] >= payload["min_description_chars"]
    assert payload["max_description_chars"] >= payload["target_description_chars"]


def test_extraction_normalization_dedups_and_strips_labels() -> None:
    payload = {
        "public_facts": [
            {"text": "  Лектор Борис Мегорский разбирает быт моряков.  ", "source_span": "Борис Мегорский", "kind": "person"},
            {"text": "kind: лектор Борис Мегорский разбирает быт моряков.", "source_span": "Борис Мегорский", "kind": "person"},
            {"text": "В программе — каргопольская и дымковская игрушки.", "source_span": "каргопольская и дымковская", "kind": "object"},
        ],
        "logistics_facts": [
            {"text": "Билеты по 300 руб.", "source_span": "300 р.", "kind": "tickets"},
        ],
        "warnings": ["short source"],
    }
    normalized = legacy_writer_family.normalize_extraction_payload(payload)
    public_texts = [item["text"] for item in normalized["public_facts"]]
    assert public_texts == [
        "Лектор Борис Мегорский разбирает быт моряков.",
        "В программе — каргопольская и дымковская игрушки.",
    ]
    assert [item["kind"] for item in normalized["logistics_facts"]] == ["tickets"]
    assert normalized["warnings"] == ["short source"]


def test_merge_extraction_facts_assigns_indexes() -> None:
    merged = legacy_writer_family.merge_extraction_facts(
        [
            {"text": "Лектор Борис Мегорский разбирает быт моряков.", "kind": "person"},
            {"text": "Лектор Борис Мегорский разбирает быт моряков.", "kind": "person"},
            {"text": "Дисциплина и морские традиции.", "kind": "topic"},
        ]
    )
    assert [item["index"] for item in merged] == [0, 1]
    assert [item["text"] for item in merged] == [
        "Лектор Борис Мегорский разбирает быт моряков.",
        "Дисциплина и морские традиции.",
    ]


def test_apply_writer_output_returns_clean_title_and_description() -> None:
    applied = legacy_writer_family.apply_writer_output(
        title="Лекция о флоте",
        output={"title": "  ", "description_md": "Текст лекции.\n\n### Когда и где\nКалининград."},
    )
    assert applied["title"] == "Лекция о флоте"
    assert applied["description_md"].startswith("Текст лекции.")


def test_validate_writer_output_flags_empty_description() -> None:
    result = legacy_writer_family.validate_writer_output(
        public_facts=[{"index": 0, "text": "Факт"}],
        description_md="",
        baseline_description="Baseline.",
    )
    assert "description.empty" in result.errors


def test_validate_writer_output_passes_clean_text() -> None:
    description = (
        "Борис Мегорский разбирает быт моряков и устои первого регулярного флота "
        "России. В фокусе — повседневная дисциплина, морские традиции, медицина, "
        "снабжение экипажей и важные особенности комплектования флота. Лекция "
        "опирается на архивные документы и исторические исследования и помогает "
        "восстановить картину быта моряков петровской эпохи."
    )
    result = legacy_writer_family.validate_writer_output(
        public_facts=[{"index": 0, "text": "Факт один"}, {"index": 1, "text": "Факт два"}],
        description_md=description,
        baseline_description="A" * 220,
    )
    assert result.errors == []


def test_validate_writer_output_flags_duplicate_word() -> None:
    description = "Лекция расскажет расскажет о флоте."
    result = legacy_writer_family.validate_writer_output(
        public_facts=[{"index": 0, "text": "Факт"}],
        description_md=description,
        baseline_description="A" * 200,
    )
    assert any(item.startswith("text.duplicate_word:") for item in result.errors)


# --- compare_to_baseline (read-only signal) ---------------------------------

def test_compare_to_baseline_rewards_compact_no_worse_rewrite() -> None:
    delta = legacy_writer_family.compare_to_baseline(
        baseline_description=(
            "Событие посвящено истории флота. Лекция расскажет о быте моряков "
            "и о повседневной дисциплине на кораблях."
        ),
        candidate_description=(
            "Лекция разбирает историю флота через быт моряков и повседневную "
            "дисциплину на кораблях."
        ),
    )
    assert delta["status"] == "improved"
    assert "quality.more_compact" in delta["improvements"]


def test_compare_to_baseline_warns_on_too_short_summary() -> None:
    delta = legacy_writer_family.compare_to_baseline(
        baseline_description=(
            "На выставке представлены жостовские подносы, каргопольская игрушка "
            "и дымковская игрушка в разделе «Красны девицы, добры молодцы»."
        ),
        candidate_description="На выставке покажут народные промыслы.",
    )
    assert delta["status"] != "regressed"
    assert any(item.startswith("quality.shorter_than_baseline:") for item in delta["warnings"])


def test_compare_to_baseline_flags_narrator_frame_regression() -> None:
    delta = legacy_writer_family.compare_to_baseline(
        baseline_description=(
            "В разделе «Красны девицы, добры молодцы» на выставке «Космос красного» "
            "собраны жостовские подносы, каргопольская и дымковская игрушки."
        ),
        candidate_description=(
            "Погружение в мир русского народного искусства через призму «космического» "
            "видения. Выставка «Космос красного» предлагает взглянуть на знакомые образы."
        ),
    )
    assert "quality.narrator_frame_opening" in delta["regressions"]
    assert delta["status"] == "regressed"


def test_compare_to_baseline_rewards_narrator_frame_avoidance() -> None:
    delta = legacy_writer_family.compare_to_baseline(
        baseline_description=(
            "Погружение в мир русского народного искусства через призму «космического» "
            "видения. Выставка «Космос красного» предлагает взглянуть на знакомые образы."
        ),
        candidate_description=(
            "«Космос красного» собирает жостовские подносы, каргопольскую и дымковскую "
            "игрушки в разделе «Красны девицы, добры молодцы»."
        ),
    )
    assert "quality.narrator_frame_avoided" in delta["improvements"]


def test_compare_to_baseline_rewards_source_fidelity() -> None:
    delta = legacy_writer_family.compare_to_baseline(
        source_excerpt="Лекция Бориса Мегорского о быте и нравах флота Петра Великого.",
        baseline_description=(
            "Борис Мегорский расскажет о флоте. В программе: снабжение, медицина, "
            "наказания и развлечения моряков."
        ),
        candidate_description="Борис Мегорский разберёт быт и нравы флота Петра Великого.",
    )
    assert (
        "quality.invented_named_tokens_reduced" in delta["improvements"]
        or "quality.source_named_tokens_improved" in delta["improvements"]
    )
    assert delta["status"] == "improved"


def test_quality_detector_does_not_flag_predstavleniy_false_positive() -> None:
    quality = writer_final_4o_family._describe_text_quality(
        "В центре внимания — эволюция представлений о священном от древности до современности."
    )
    assert "stories_presented" not in quality["report_formula_hits"]


# --- benchmark wiring -------------------------------------------------------

def test_benchmark_accepts_lollipop_legacy_variant_and_static_fixtures() -> None:
    benchmark = _load_benchmark_module()
    assert benchmark._variants_from_cli("lollipop_legacy") == ["baseline", "lollipop_legacy"]
    assert benchmark._variants_from_cli("baseline_g4") == ["baseline", "baseline_g4"]
    fixtures = benchmark._fixtures_from_cli(
        "audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos"
    )
    assert [fixture.fixture_id for fixture in fixtures] == [
        "AUDIO-WALK-QUARTER-971",
        "PETER-FLEET-LECTURE-5600",
        "SACRED-LECTURE-ZYGMONT-3170",
        "WORLD-HOBBIES-5505",
        "RED-COSMOS-7902",
    ]
    peter = fixtures[1]
    source_text = peter.sources[0].text
    assert "отделом эстампов и фотографий" in source_text
    assert "Лейб-гвардии Преображенский полк, 1709" in source_text
    assert "первой трети XVIII века" in source_text
    assert "реконструированные предметы обмундирования" in source_text


def test_lollipop_legacy_variant_does_not_send_baseline_to_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    benchmark = _load_benchmark_module()
    captured_payloads: list[dict[str, object]] = []
    captured_systems: list[str] = []

    async def fake_ask_gemma_json_direct(**kwargs):
        payload = kwargs["user_payload"]
        system_prompt = kwargs["system_prompt"]
        captured_payloads.append(payload)
        captured_systems.append(system_prompt)
        is_reviewer = "fact-coverage reviewer" in system_prompt
        # No baseline text or facts may leak into the legacy *generation* payload.
        # The fact-coverage reviewer is benchmark-only and is allowed to see
        # baseline facts; only the extractor/writer must stay clean.
        if not is_reviewer:
            assert "baseline_description" not in payload
            assert "baseline_facts" not in payload
            flat = repr(payload)
            assert "Gemma 3 baseline draft" not in flat
            assert "Gemma 3 baseline fact" not in flat
        if "fact extractor" in system_prompt:
            return {
                "public_facts": [
                    {"text": "Лектор Борис Мегорский разбирает быт моряков.", "source_span": "Борис Мегорский", "kind": "person"},
                    {"text": "В фокусе — дисциплина и морские традиции.", "source_span": "дисциплина и традиции", "kind": "topic"},
                ],
                "logistics_facts": [{"text": "Начало в 19:00.", "source_span": "19:00", "kind": "time"}],
                "warnings": [],
            }
        if is_reviewer:
            return {
                "baseline_facts_review": [
                    {
                        "baseline_index": 0,
                        "baseline_fact": "Gemma 3 baseline fact",
                        "grounded_in_source": "false",
                        "covered_by_g4": False,
                        "matched_g4_fact_indexes": [],
                        "loss_severity": "none",
                        "reason": "decorative ungrounded",
                    }
                ],
                "g4_facts_review": [
                    {
                        "g4_index": 0,
                        "g4_fact": "Лектор Борис Мегорский разбирает быт моряков.",
                        "fact_kind": "person",
                        "category": "public",
                        "grounded_in_source": "true",
                        "useful_new_fact": False,
                        "suspicious_reason": "",
                    }
                ],
                "coverage_summary": {
                    "public_coverage_status": "accepted",
                    "logistics_coverage_status": "accepted",
                    "named_entity_coverage_status": "accepted",
                    "format_topic_program_coverage_status": "accepted",
                    "overall_verdict": "accepted",
                    "verdict_reason": "ok",
                },
            }
        # writer call
        return {
            "title": "Флот Петра",
            "description_md": (
                "Борис Мегорский разбирает быт и нравы первого регулярного флота. "
                "В фокусе — морская дисциплина и повседневные традиции экипажа."
            ),
            "covered_public_fact_indexes": [0, 1],
            "used_logistics_fact_indexes": [],
            "warnings": [],
        }

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(benchmark, "_ask_gemma_json_direct", fake_ask_gemma_json_direct)
    monkeypatch.setattr(benchmark, "_gemma_gap_sleep", fake_sleep)

    fixture = benchmark.BenchmarkFixture(
        fixture_id="TEST",
        title="Флот Петра",
        event_type="лекция",
        date=None,
        time=None,
        location_name=None,
        location_address=None,
        city="Калининград",
        sources=[
            benchmark.SourcePacket(
                source_id="site",
                source_type="site",
                url="https://example.test",
                text="Борис Мегорский рассказывает о быте моряков, дисциплине и традициях.",
            )
        ],
    )
    result = asyncio.run(
        benchmark._run_lollipop_legacy_variant(
            fixture,
            baseline={
                "description_md": "Gemma 3 baseline draft.",
                "per_source_facts": {"site": ["Gemma 3 baseline fact"]},
                "timings": {"wall_clock_sec": 10.0},
            },
            gemma_model="gemma-4-31b-it",
            gemma_call_gap_s=0,
        )
    )

    # one extraction + one writer + one fact-coverage reviewer call
    assert len(captured_payloads) == 3
    assert any("fact extractor" in s for s in captured_systems)
    assert any("final writer" in s for s in captured_systems)
    assert any("fact-coverage reviewer" in s for s in captured_systems)
    assert result["generation_uses_baseline"] is False
    assert result["uses_baseline_fact_floor"] is False
    assert result["includes_baseline_stage"] is False
    assert result["writer_fallback_to_baseline"] is False
    assert result["writer_fallback_to_4o"] is False
    assert result["writer_model"] == "gemma-4"
    assert result["legacy_public_facts"] == [
        "Лектор Борис Мегорский разбирает быт моряков.",
        "В фокусе — дисциплина и морские традиции.",
    ]
    assert "Борис Мегорский" in result["applied_output"]["description_md"]


def test_lollipop_legacy_variant_falls_back_to_4o_on_writer_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    benchmark = _load_benchmark_module()
    four_o_calls: list[dict[str, object]] = []

    async def fake_ask_gemma_json_direct(**kwargs):
        system_prompt = kwargs["system_prompt"]
        if "fact extractor" in system_prompt:
            return {
                "public_facts": [
                    {"text": "Лектор разбирает быт моряков.", "source_span": "быт моряков", "kind": "topic"},
                    {"text": "В фокусе — морские традиции.", "source_span": "морские традиции", "kind": "topic"},
                ],
                "logistics_facts": [],
                "warnings": [],
            }
        if "fact-coverage reviewer" in system_prompt:
            return {
                "baseline_facts_review": [],
                "g4_facts_review": [],
                "coverage_summary": {
                    "public_coverage_status": "unknown",
                    "logistics_coverage_status": "unknown",
                    "named_entity_coverage_status": "unknown",
                    "format_topic_program_coverage_status": "unknown",
                    "overall_verdict": "unknown",
                    "verdict_reason": "no baseline facts",
                },
            }
        # Simulate Gemma 4 writer timeout
        raise asyncio.TimeoutError("simulated writer timeout")

    async def fake_ask_4o_json(**kwargs):
        four_o_calls.append(kwargs)
        return {
            "title": "Лекция о флоте",
            "description_md": (
                "Лектор разбирает быт моряков и морские традиции первого регулярного флота. "
                "Слушатели увидят дисциплину и повседневный уклад через архивные документы."
            ),
            "covered_public_fact_indexes": [0, 1],
            "used_logistics_fact_indexes": [],
            "warnings": [],
        }

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(benchmark, "_ask_gemma_json_direct", fake_ask_gemma_json_direct)
    monkeypatch.setattr(benchmark, "_ask_4o_json", fake_ask_4o_json)
    monkeypatch.setattr(benchmark, "_gemma_gap_sleep", fake_sleep)

    fixture = benchmark.BenchmarkFixture(
        fixture_id="TEST",
        title="Лекция о флоте",
        event_type="лекция",
        date=None,
        time=None,
        location_name=None,
        location_address=None,
        city="Калининград",
        sources=[
            benchmark.SourcePacket(
                source_id="site",
                source_type="site",
                url="https://example.test",
                text="Лектор разбирает быт моряков и морские традиции.",
            )
        ],
    )
    result = asyncio.run(
        benchmark._run_lollipop_legacy_variant(
            fixture,
            baseline={"description_md": "x" * 800, "per_source_facts": {}, "timings": {"wall_clock_sec": 10.0}},
            gemma_model="gemma-4-31b-it",
            gemma_call_gap_s=0,
        )
    )

    assert len(four_o_calls) == 1
    assert result["writer_fallback_to_4o"] is True
    assert result["writer_model"] == "4o"
    assert "writer.timeout:gemma4" in result["writer_failure_reasons"]
    assert "Лектор" in result["applied_output"]["description_md"]


def test_four_o_token_helper_prefers_FOUR_4O_TOKEN(monkeypatch: pytest.MonkeyPatch) -> None:
    benchmark = _load_benchmark_module()
    monkeypatch.delenv("FOUR_O_TOKEN", raising=False)
    monkeypatch.setenv("FOUR_4O_TOKEN", "token-4o")
    assert benchmark._four_o_token() == "token-4o"

    monkeypatch.delenv("FOUR_4O_TOKEN", raising=False)
    monkeypatch.setenv("FOUR_O_TOKEN", "token-legacy")
    assert benchmark._four_o_token() == "token-legacy"

    monkeypatch.delenv("FOUR_O_TOKEN", raising=False)
    monkeypatch.delenv("FOUR_4O_TOKEN", raising=False)
    with pytest.raises(RuntimeError):
        benchmark._four_o_token()


# --- fact-coverage reviewer (benchmark-only LLM judge) ----------------------

def test_fact_coverage_response_schema_has_required_top_level_keys() -> None:
    schema = legacy_writer_family.fact_coverage_response_schema()
    required = set(schema.get("required") or [])
    assert {"baseline_facts_review", "g4_facts_review", "coverage_summary"}.issubset(required)
    coverage_props = set(
        ((schema.get("properties") or {}).get("coverage_summary") or {}).get("properties") or {}
    )
    assert {
        "public_coverage_status",
        "logistics_coverage_status",
        "named_entity_coverage_status",
        "format_topic_program_coverage_status",
        "overall_verdict",
        "verdict_reason",
    }.issubset(coverage_props)


def test_fact_coverage_payload_carries_baseline_facts_for_reviewer_only() -> None:
    payload = legacy_writer_family.build_fact_coverage_payload(
        title="Лекция",
        event_type="лекция",
        date="2026-04-24",
        time="16:00",
        location_name="Музей",
        location_address="ул. Х, 1",
        city="Калининград",
        source_excerpt="Лектор Борис Мегорский о флоте.",
        baseline_facts=["Лектор Борис Мегорский", "Лекция о флоте"],
        g4_public_facts=[
            {"index": 0, "text": "Борис Мегорский", "kind": "person"},
            {"index": 1, "text": "лекция о флоте", "kind": "topic"},
        ],
        g4_logistics_facts=[
            {"index": 0, "text": "16:00", "kind": "time"},
        ],
    )
    assert [item["text"] for item in payload["baseline_facts"]] == [
        "Лектор Борис Мегорский",
        "Лекция о флоте",
    ]
    assert [item["category"] for item in payload["g4_facts"]] == ["public", "public", "logistics"]
    assert [item["index"] for item in payload["g4_facts"]] == [0, 1, 2]


def test_fact_coverage_payload_exposes_all_benchmark_fact_surfaces() -> None:
    payload = legacy_writer_family.build_fact_coverage_payload(
        title="Лекция",
        event_type="лекция",
        date="2026-04-24",
        time="16:00",
        location_name="Музей",
        location_address="ул. Х, 1",
        city="Калининград",
        source_excerpt="Лектор Борис Мегорский о флоте.",
        baseline_facts=["Лектор Борис Мегорский", "Билеты в кассе"],
        baseline_writer_facts=["Лектор Борис Мегорский"],
        baseline_metadata_facts=["date: 2026-04-24", "time: 16:00"],
        g4_public_facts=[{"text": "Борис Мегорский", "kind": "person"}],
        g4_logistics_facts=[{"text": "16:00", "kind": "time"}],
    )
    surfaces = payload["baseline_fact_surfaces"]
    assert [item["text"] for item in surfaces["raw_extracted_facts"]] == [
        "Лектор Борис Мегорский",
        "Билеты в кассе",
    ]
    assert [item["text"] for item in surfaces["writer_facts_text_clean"]] == [
        "Лектор Борис Мегорский"
    ]
    assert [item["text"] for item in surfaces["metadata_anchors"]] == [
        "date: 2026-04-24",
        "time: 16:00",
    ]


def test_normalize_fact_coverage_payload_clamps_invalid_indexes_and_enums() -> None:
    raw = {
        "baseline_facts_review": [
            {
                "baseline_index": 0,
                "baseline_fact": "  Лекция Мегорского  ",
                "grounded_in_source": "True",
                "covered_by_g4": "true",
                "matched_g4_fact_indexes": [0, 99, "1"],
                "loss_severity": "MAJOR",
                "reason": "decorative",
            },
            {
                "baseline_index": 99,  # out of range
                "baseline_fact": "ghost",
                "grounded_in_source": "false",
                "covered_by_g4": False,
                "matched_g4_fact_indexes": [],
                "loss_severity": "critical",
                "reason": "ignored because oor",
            },
        ],
        "g4_facts_review": [
            {
                "g4_index": 0,
                "g4_fact": "Борис Мегорский",
                "fact_kind": "person",
                "category": "public",
                "grounded_in_source": "true",
                "useful_new_fact": True,
                "suspicious_reason": "",
            }
        ],
        "coverage_summary": {
            "public_coverage_status": "ACCEPTED",
            "logistics_coverage_status": "partial",
            "named_entity_coverage_status": "wat",
            "format_topic_program_coverage_status": "rejected",
            "overall_verdict": "PARTIAL",
            "verdict_reason": "ok",
        },
    }
    normalized = legacy_writer_family.normalize_fact_coverage_payload(
        raw, baseline_count=1, g4_count=2
    )
    assert len(normalized["baseline_facts_review"]) == 1
    assert normalized["baseline_facts_review"][0]["matched_g4_fact_indexes"] == [0, 1]
    assert normalized["baseline_facts_review"][0]["loss_severity"] == "major"
    assert normalized["baseline_facts_review"][0]["covered_by_g4"] is True
    assert normalized["coverage_summary"]["public_coverage_status"] == "accepted"
    assert normalized["coverage_summary"]["named_entity_coverage_status"] == "unknown"
    assert normalized["coverage_summary"]["overall_verdict"] == "partial"


def test_normalize_fact_coverage_payload_uses_input_texts_not_reviewer_echo() -> None:
    raw = {
        "baseline_facts_review": [
            {
                "baseline_index": 0,
                "baseline_fact": "sЛектор Борис Мегорский",
                "grounded_in_source": "true",
                "covered_by_g4": True,
                "matched_g4_fact_indexes": [0],
                "loss_severity": "none",
                "reason": "match",
            }
        ],
        "g4_facts_review": [
            {
                "g4_index": 0,
                "g4_fact": "gБорис Мегорский",
                "fact_kind": "person",
                "category": "public",
                "grounded_in_source": "true",
                "useful_new_fact": False,
                "suspicious_reason": "",
            }
        ],
        "coverage_summary": {
            "public_coverage_status": "accepted",
            "logistics_coverage_status": "accepted",
            "named_entity_coverage_status": "accepted",
            "format_topic_program_coverage_status": "accepted",
            "overall_verdict": "accepted",
            "verdict_reason": "ok",
        },
    }
    normalized = legacy_writer_family.normalize_fact_coverage_payload(
        raw,
        baseline_count=1,
        g4_count=1,
        baseline_facts=[{"index": 0, "text": "Лектор Борис Мегорский"}],
        g4_facts=[{"index": 0, "text": "Борис Мегорский", "kind": "person", "category": "public"}],
    )
    assert normalized["baseline_facts_review"][0]["baseline_fact"] == "Лектор Борис Мегорский"
    assert normalized["g4_facts_review"][0]["g4_fact"] == "Борис Мегорский"


def test_summarize_fact_coverage_critical_loss_drives_rejected_verdict() -> None:
    normalized = {
        "baseline_facts_review": [
            {
                "baseline_index": 0,
                "baseline_fact": "Лекция Бориса Мегорского",
                "grounded_in_source": "true",
                "covered_by_g4": False,
                "matched_g4_fact_indexes": [],
                "loss_severity": "critical",
                "reason": "лектор потерян",
            },
            {
                "baseline_index": 1,
                "baseline_fact": "украшение из decorative прозы",
                "grounded_in_source": "false",
                "covered_by_g4": False,
                "matched_g4_fact_indexes": [],
                "loss_severity": "none",
                "reason": "decorative ungrounded",
            },
        ],
        "g4_facts_review": [
            {
                "g4_index": 0,
                "g4_fact": "лекция",
                "fact_kind": "format",
                "category": "public",
                "grounded_in_source": "true",
                "useful_new_fact": False,
                "suspicious_reason": "",
            }
        ],
        "coverage_summary": {
            "public_coverage_status": "partial",
            "logistics_coverage_status": "accepted",
            "named_entity_coverage_status": "rejected",
            "format_topic_program_coverage_status": "partial",
            "overall_verdict": "partial",
            "verdict_reason": "лектор потерян",
        },
    }
    summary = legacy_writer_family.summarize_fact_coverage(normalized)
    assert summary["verdict"] == "rejected"
    assert summary["deterministic_verdict_floor"] == "rejected"
    assert summary["llm_overall_verdict"] == "partial"
    assert summary["lost_baseline_facts"]
    assert summary["lost_baseline_facts"][0]["baseline_fact"] == "Лекция Бориса Мегорского"
    # baseline fact 1 is ungrounded — must NOT count as a loss
    assert all(
        item["baseline_index"] != 1 for item in summary["lost_baseline_facts"]
    )
    assert summary["covered_grounded_baseline_fact_count"] == 0
    assert summary["grounded_baseline_fact_count"] == 1


def test_summarize_fact_coverage_useful_added_fact_is_tracked() -> None:
    normalized = {
        "baseline_facts_review": [
            {
                "baseline_index": 0,
                "baseline_fact": "Лекция о флоте",
                "grounded_in_source": "true",
                "covered_by_g4": True,
                "matched_g4_fact_indexes": [0],
                "loss_severity": "none",
                "reason": "match",
            }
        ],
        "g4_facts_review": [
            {
                "g4_index": 0,
                "g4_fact": "Лекция о флоте Петра",
                "fact_kind": "topic",
                "category": "public",
                "grounded_in_source": "true",
                "useful_new_fact": False,
                "suspicious_reason": "",
            },
            {
                "g4_index": 1,
                "g4_fact": "16:00",
                "fact_kind": "time",
                "category": "logistics",
                "grounded_in_source": "true",
                "useful_new_fact": True,
                "suspicious_reason": "",
            },
        ],
        "coverage_summary": {
            "public_coverage_status": "accepted",
            "logistics_coverage_status": "accepted",
            "named_entity_coverage_status": "accepted",
            "format_topic_program_coverage_status": "accepted",
            "overall_verdict": "accepted",
            "verdict_reason": "good",
        },
    }
    summary = legacy_writer_family.summarize_fact_coverage(normalized)
    assert summary["verdict"] == "accepted"
    assert any(item["g4_fact"] == "16:00" for item in summary["added_g4_facts"])
    assert summary["covered_grounded_baseline_fact_count"] == 1


def test_summarize_fact_coverage_marks_ungrounded_g4_as_suspicious() -> None:
    normalized = {
        "baseline_facts_review": [
            {
                "baseline_index": 0,
                "baseline_fact": "Лекция о флоте",
                "grounded_in_source": "true",
                "covered_by_g4": True,
                "matched_g4_fact_indexes": [0],
                "loss_severity": "none",
                "reason": "match",
            }
        ],
        "g4_facts_review": [
            {
                "g4_index": 0,
                "g4_fact": "Лекция о флоте",
                "fact_kind": "topic",
                "category": "public",
                "grounded_in_source": "true",
                "useful_new_fact": False,
                "suspicious_reason": "",
            },
            {
                "g4_index": 1,
                "g4_fact": "знаковые места города",
                "fact_kind": "atmosphere",
                "category": "public",
                "grounded_in_source": "false",
                "useful_new_fact": False,
                "suspicious_reason": "не подтверждено в source",
            },
        ],
        "coverage_summary": {
            "public_coverage_status": "partial",
            "logistics_coverage_status": "accepted",
            "named_entity_coverage_status": "accepted",
            "format_topic_program_coverage_status": "accepted",
            "overall_verdict": "partial",
            "verdict_reason": "G4 invented atmosphere fact",
        },
    }
    summary = legacy_writer_family.summarize_fact_coverage(normalized)
    assert any(item["g4_fact"] == "знаковые места города" for item in summary["suspicious_g4_facts"])
    # Single ungrounded G4 fact moves deterministic floor to partial.
    assert summary["deterministic_verdict_floor"] == "partial"
    assert summary["verdict"] in {"partial", "rejected"}


def test_lollipop_legacy_reviewer_payload_uses_baseline_facts_but_generation_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _load_benchmark_module()
    captured_payloads: list[tuple[str, dict[str, object]]] = []

    async def fake_ask_gemma_json_direct(**kwargs):
        payload = kwargs["user_payload"]
        system_prompt = kwargs["system_prompt"]
        captured_payloads.append((system_prompt, payload))
        if "fact extractor" in system_prompt:
            return {
                "public_facts": [
                    {"text": "Лекция о флоте", "source_span": "Лекция о флоте", "kind": "topic"},
                    {"text": "Борис Мегорский", "source_span": "Борис Мегорский", "kind": "person"},
                ],
                "logistics_facts": [{"text": "16:00", "source_span": "16:00", "kind": "time"}],
                "warnings": [],
            }
        if "final writer" in system_prompt:
            return {
                "title": "Лекция о флоте",
                "description_md": (
                    "Борис Мегорский разбирает быт моряков и устои первого регулярного флота. "
                    "В фокусе — повседневная дисциплина и морские традиции экипажа."
                ),
                "covered_public_fact_indexes": [0, 1],
                "used_logistics_fact_indexes": [],
                "warnings": [],
            }
        if "fact-coverage reviewer" in system_prompt:
            return {
                "baseline_facts_review": [
                    {
                        "baseline_index": 0,
                        "baseline_fact": "Лекция о флоте",
                        "grounded_in_source": "true",
                        "covered_by_g4": True,
                        "matched_g4_fact_indexes": [0],
                        "loss_severity": "none",
                        "reason": "match",
                    }
                ],
                "g4_facts_review": [
                    {
                        "g4_index": 0,
                        "g4_fact": "Лекция о флоте",
                        "fact_kind": "topic",
                        "category": "public",
                        "grounded_in_source": "true",
                        "useful_new_fact": False,
                        "suspicious_reason": "",
                    }
                ],
                "coverage_summary": {
                    "public_coverage_status": "accepted",
                    "logistics_coverage_status": "accepted",
                    "named_entity_coverage_status": "accepted",
                    "format_topic_program_coverage_status": "accepted",
                    "overall_verdict": "accepted",
                    "verdict_reason": "ok",
                },
            }
        raise AssertionError(f"unexpected stage: {system_prompt[:60]}")

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(benchmark, "_ask_gemma_json_direct", fake_ask_gemma_json_direct)
    monkeypatch.setattr(benchmark, "_gemma_gap_sleep", fake_sleep)

    fixture = benchmark.BenchmarkFixture(
        fixture_id="TEST-LEAK",
        title="Лекция о флоте",
        event_type="лекция",
        date=None,
        time=None,
        location_name=None,
        location_address=None,
        city="Калининград",
        sources=[
            benchmark.SourcePacket(
                source_id="site",
                source_type="site",
                url="https://example.test",
                text="Лекция о флоте Бориса Мегорского.",
            )
        ],
    )
    result = asyncio.run(
        benchmark._run_lollipop_legacy_variant(
            fixture,
            baseline={
                "description_md": "Gemma 3 baseline draft.",
                "per_source_facts": {
                    "site": ["Лекция о флоте", "Gemma 3 invented decorative fact"]
                },
                "timings": {"wall_clock_sec": 10.0},
            },
            gemma_model="gemma-4-31b-it",
            gemma_call_gap_s=0,
        )
    )

    extractor_payloads = [p for s, p in captured_payloads if "fact extractor" in s]
    writer_payloads = [p for s, p in captured_payloads if "final writer" in s]
    reviewer_payloads = [p for s, p in captured_payloads if "fact-coverage reviewer" in s]
    assert extractor_payloads, "extractor stage was not called"
    assert writer_payloads, "writer stage was not called"
    assert reviewer_payloads, "fact-coverage reviewer was not called"

    # Generation (extractor + writer) must not see baseline facts/text.
    for payload in extractor_payloads + writer_payloads:
        flat = repr(payload)
        assert "Gemma 3 baseline draft" not in flat
        assert "Gemma 3 invented decorative fact" not in flat
        assert "baseline_facts" not in payload
        assert "baseline_description" not in payload

    # Reviewer payload, on the other hand, MUST carry baseline facts.
    reviewer_payload = reviewer_payloads[0]
    assert reviewer_payload.get("baseline_facts"), "reviewer payload missing baseline facts"
    baseline_texts = [item["text"] for item in reviewer_payload["baseline_facts"]]
    assert "Лекция о флоте" in baseline_texts
    assert "Gemma 3 invented decorative fact" in baseline_texts

    coverage = result.get("fact_coverage")
    assert isinstance(coverage, dict)
    assert coverage.get("verdict") == "accepted"
    assert coverage["summary"]["covered_grounded_baseline_fact_count"] >= 1


def test_render_fact_coverage_section_includes_raw_inputs() -> None:
    benchmark = _load_benchmark_module()
    lines = benchmark._render_fact_coverage_section(
        {
            "input": {
                "baseline_facts": [{"index": 0, "text": "Лекция о флоте"}],
                "g4_facts": [
                    {"index": 0, "text": "Лекция о флоте", "kind": "topic", "category": "public"}
                ],
            },
            "baseline_fact_surfaces": {
                "raw_extracted_facts": ["Лекция о флоте", "Билеты в кассе"],
                "writer_facts_text_clean": ["Лекция о флоте"],
                "filtered_out_before_writer": ["Билеты в кассе"],
                "metadata_anchors": ["date: 2026-04-24"],
            },
            "review": {"baseline_facts_review": [], "g4_facts_review": []},
            "summary": {
                "verdict": "accepted",
                "llm_overall_verdict": "accepted",
                "deterministic_verdict_floor": "accepted",
                "baseline_fact_count": 1,
                "baseline_raw_extracted_fact_count": 2,
                "baseline_writer_fact_count": 1,
                "baseline_filtered_out_fact_count": 1,
                "baseline_metadata_fact_count": 1,
                "g4_fact_count": 1,
                "g4_public_fact_count": 1,
                "g4_logistics_fact_count": 0,
                "coverage_summary": {},
            },
            "errors": [],
        }
    )
    rendered = "\n".join(lines)
    assert "Baseline raw extractor facts" in rendered
    assert "Baseline writer facts" in rendered
    assert "Baseline facts filtered out before writer" in rendered
    assert "Baseline metadata / anchors" in rendered
    assert "Gemma 4 extracted facts" in rendered
    assert "Билеты в кассе" in rendered
