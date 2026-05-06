from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _load_stage_benchmark():
    path = Path(__file__).resolve().parents[1] / "scripts" / "inspect" / "benchmark_smart_update_g4_stages.py"
    spec = importlib.util.spec_from_file_location("benchmark_smart_update_g4_stages", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stage_benchmark_loads_frozen_baseline_artifact(tmp_path: Path) -> None:
    stage = _load_stage_benchmark()
    artifact = {
        "results": [
            {
                "fixture": {"fixture_id": "CASE-1"},
                "baseline": {
                    "gemma_model": "gemma-3-27b-it",
                    "baseline_mode": "prod_style_first_pass_proxy",
                    "per_source_facts": {
                        "tg": ["Первый факт.", "Второй факт."],
                        "ocr": ["Первый факт.", "Третий факт."],
                    },
                    "facts_text_clean": ["Первый факт.", "Второй факт."],
                    "description_md": "### Раздел\n\nПервый факт. Второй факт.",
                    "timings": {"wall_clock_sec": 12.5, "gemma_calls": 2, "four_o_calls": 0},
                    "quality_profile": {"lead_hook_signals": ["object"]},
                },
            }
        ]
    }
    artifact_path = tmp_path / "baseline.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False), encoding="utf-8")
    fixture = stage.bench.BenchmarkFixture(
        fixture_id="CASE-1",
        title="Событие",
        event_type="лекция",
        date="2026-05-02",
        time="18:00",
        location_name="Музей",
        location_address="ул. Примерная, 1",
        city="Калининград",
        sources=[
            stage.bench.SourcePacket(
                source_id="tg",
                source_type="telegram",
                url="https://t.me/example/1",
                text="Первый факт. Второй факт. Третий факт.",
            )
        ],
    )

    baseline = stage._load_frozen_baseline(str(artifact_path), fixture)

    assert baseline["model"] == "gemma-3-27b-it"
    assert baseline["path"] == "frozen_current_smart_update_baseline"
    assert baseline["baseline_artifact_mode"] == "prod_style_first_pass_proxy"
    assert baseline["source_artifact"] == str(artifact_path)
    assert baseline["candidate_has_g3"] is True
    assert baseline["raw_facts"] == ["Первый факт.", "Второй факт.", "Третий факт."]
    assert baseline["facts_text_clean"] == ["Первый факт.", "Второй факт."]
    assert baseline["metrics"]["semantic_headings"] == 1
    assert baseline["timings"]["wall_clock_sec"] == 12.5
    assert baseline["timings"]["gemma_calls_observed"] == 2


def test_stage_benchmark_rejects_missing_frozen_baseline_fixture(tmp_path: Path) -> None:
    stage = _load_stage_benchmark()
    artifact_path = tmp_path / "baseline.json"
    artifact_path.write_text(
        json.dumps({"results": [{"fixture": {"fixture_id": "OTHER"}, "baseline": {}}]}),
        encoding="utf-8",
    )
    fixture = stage.bench.BenchmarkFixture(
        fixture_id="CASE-1",
        title="Событие",
        event_type="лекция",
        date=None,
        time=None,
        location_name=None,
        location_address=None,
        city="Калининград",
        sources=[
            stage.bench.SourcePacket(
                source_id="tg",
                source_type="telegram",
                url="https://t.me/example/1",
                text="Факт из staged baseline.",
            )
        ],
    )

    with pytest.raises(RuntimeError, match="does not contain fixture"):
        stage._load_frozen_baseline(str(artifact_path), fixture)


def test_stage_benchmark_loads_single_fixture_stage_artifact(tmp_path: Path) -> None:
    stage = _load_stage_benchmark()
    artifact_path = tmp_path / "stage-baseline.json"
    artifact_path.write_text(
        json.dumps(
            {
                "fixture": {"fixture_id": "CASE-1"},
                "baseline": {
                    "model": "gemma-3-27b-it",
                    "raw_facts": ["Факт из staged baseline."],
                    "facts_text_clean": ["Факт из staged baseline."],
                    "description_md": "Описание baseline.",
                    "short_description": "Коротко.",
                    "search_digest": "Поиск.",
                    "timings": {"wall_clock_sec": 9.0},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fixture = stage.bench.BenchmarkFixture(
        fixture_id="CASE-1",
        title="Событие",
        event_type="лекция",
        date=None,
        time=None,
        location_name=None,
        location_address=None,
        city="Калининград",
        sources=[
            stage.bench.SourcePacket(
                source_id="tg",
                source_type="telegram",
                url="https://t.me/example/1",
                text="Факт из staged baseline.",
            )
        ],
    )

    baseline = stage._load_frozen_baseline(str(artifact_path), fixture)

    assert baseline["raw_facts"] == ["Факт из staged baseline."]
    assert baseline["short_description"] == "Коротко."
    assert baseline["search_digest"] == "Поиск."


def test_prod_db_snapshot_baseline_uses_stored_facts_without_reextracting() -> None:
    stage = _load_stage_benchmark()
    fixture = stage.StageBenchmarkFixture(
        fixture_id="PRODDB-1",
        title="Событие",
        event_type="концерт",
        date="2026-05-08",
        time="18:00",
        location_name="Филармония",
        location_address="ул. Примерная, 1",
        city="Калининград",
        sources=[
            stage.bench.SourcePacket(
                source_id="tg",
                source_type="telegram",
                url="https://t.me/example/1",
                text="Исполнитель: Анна Юсупова. В программе произведения Баха.",
            )
        ],
        baseline_description_md="Prod Telegraph body.",
        baseline_short_description="Коротко.",
        baseline_search_digest="Дайджест.",
        baseline_raw_facts=[
            "Дата: 2026-05-08",
            "Время: 18:00",
            "Локация: Филармония, ул. Примерная, 1, Калининград",
            "Исполнитель: Анна Юсупова",
            "В программе произведения Баха",
        ],
        baseline_source_artifact="snapshot.sqlite",
    )

    baseline = stage._baseline_from_fixture_snapshot(fixture)

    assert baseline["baseline_fact_source"] == "prod_db_event_source_fact"
    assert baseline["raw_facts"] == fixture.baseline_raw_facts
    assert baseline["facts_text_clean"] == [
        "Исполнитель: Анна Юсупова",
        "В программе произведения Баха",
    ]
    assert "baseline_text_fact_extract" not in baseline


def test_stage_benchmark_uses_valid_bundle_derived_fields_before_extra_calls() -> None:
    stage = _load_stage_benchmark()

    search_digest, short_description = stage._bundle_derived_fields(
        {
            "search_digest": "Экспозиция соединяет жостовскую роспись, каргопольскую и дымковскую игрушку в рассказ о народных промыслах.",
            "short_description": "Экспозиция рассказывает о жостовской росписи, каргопольской и дымковской игрушке в народной традиции.",
        }
    )

    assert search_digest
    assert short_description


def test_stage_benchmark_rejects_invalid_bundle_short_description() -> None:
    stage = _load_stage_benchmark()

    _search_digest, short_description = stage._bundle_derived_fields(
        {
            "search_digest": "Короткий дайджест.",
            "short_description": "Слишком коротко.",
        }
    )

    assert short_description is None


def test_compact_gemma_writer_payload_keeps_only_writer_contract_fields() -> None:
    stage = _load_stage_benchmark()

    payload = stage._compact_gemma_writer_payload(
        {
            "event_type": "выставка",
            "title_context": {"original_title": "Космос красного"},
            "constraints": {
                "must_cover_fact_ids": ["EC01", "SC01"],
                "headings": ["Жостовская роспись"],
            },
            "sections": [
                {
                    "role": "body",
                    "style": "narrative",
                    "heading": "Жостовская роспись",
                    "fact_ids": ["EC01", "SC01"],
                    "facts": [
                        {"fact_id": "EC01", "text": "Представлены жостовские подносы"},
                        {"fact_id": "SC01", "text": "Жостовская роспись существует с 1825 года"},
                    ],
                    "coverage_plan": [{"fact_id": "EC01", "mode": "narrative"}],
                    "literal_items": [],
                }
            ],
        }
    )

    assert payload["title"] == "Космос красного"
    assert payload["must_cover_fact_ids"] == ["EC01", "SC01"]
    assert payload["required_headings"] == ["Жостовская роспись"]
    assert payload["sections"][0]["facts"] == [
        {"fact_id": "EC01", "text": "Представлены жостовские подносы"},
        {"fact_id": "SC01", "text": "Жостовская роспись существует с 1825 года"},
    ]
    assert "infoblock" not in payload


def test_telegraph_preview_includes_infoblock_search_and_description() -> None:
    stage = _load_stage_benchmark()
    fixture = stage.StageBenchmarkFixture(
        fixture_id="PRODDB-1",
        title="Кинопоказ",
        event_type="кинопоказ",
        date="2026-05-08",
        time="17:00",
        location_name="Остров Канта",
        location_address=None,
        city="Калининград",
        is_free=True,
        sources=[],
    )

    preview = stage._telegraph_preview_text(
        fixture,
        {
            "search_digest": "Подборка короткометражных фильмов калининградских режиссёров.",
            "description_md": "### Программа\n\n- Первый фильм",
        },
    )

    assert "🗓 8 мая в 17:00" in preview
    assert "📍 Остров Канта, Калининград" in preview
    assert "🆓 Бесплатно" in preview
    assert "Подборка короткометражных фильмов" in preview
    assert "### Программа" in preview


def test_stage_report_fences_generated_markdown_blocks(tmp_path: Path) -> None:
    stage = _load_stage_benchmark()
    data = {
        "generated_at": "2026-05-06T00:00:00+00:00",
        "fixture": {
            "fixture_id": "PRODDB-1",
            "title": "Событие",
            "event_type": "лекция",
            "date": "2026-05-08",
            "time": "18:00",
            "location_name": "Музей",
            "location_address": "ул. Примерная, 1",
            "city": "Калининград",
        },
        "baseline": {
            "model": "prod_db_gemma3_snapshot",
            "path": "prod_db_snapshot_current_smart_update_text",
            "source_artifact": "snapshot.sqlite",
            "facts_text_clean": ["Факт baseline."],
            "description_md": "### Baseline section\n\nТекст baseline.",
            "short_description": "Коротко baseline.",
            "search_digest": "Дайджест baseline.",
            "metrics": {"chars": 34},
            "timings": {"wall_clock_sec": None, "stage_sec": {}},
        },
        "candidate": {
            "model": "gemma-4-31b-it",
            "path": "smart_update_g4_variant2_lollipop_light_create_path",
            "writer_model": "gemma-4-31b-it",
            "candidate_has_g3": False,
            "facts_text_clean": ["Факт candidate."],
            "layout_payload": {"blocks": []},
            "layout_audit": {"flags": []},
            "writer_pack": {"payload": {"constraints": {"must_cover_fact_ids": [], "headings": []}}},
            "description_md": "### Candidate section\n\nТекст candidate.",
            "short_description": "Коротко candidate.",
            "search_digest": "Дайджест candidate.",
            "metrics": {"chars": 36},
            "timings": {"wall_clock_sec": 12.0, "stage_sec": {"writer.final_g4_primary": 1.0}},
            "stage_errors": [],
        },
        "fact_coverage": {
            "summary": {
                "verdict": "accepted",
                "covered_grounded_baseline_fact_count": 1,
                "grounded_baseline_fact_count": 1,
                "lost_baseline_facts": [],
                "added_g4_facts": [],
                "suspicious_g4_facts": [],
            }
        },
        "stage_summary": [],
    }

    report = stage._render_report(data, tmp_path / "report.json")

    assert "### Baseline Description\n\n```md\n### Baseline section" in report
    assert "### Candidate Description\n\n```md\n### Candidate section" in report
    assert "### Baseline Telegraph Preview\n\n```md\n🗓 8 мая в 18:00" in report
