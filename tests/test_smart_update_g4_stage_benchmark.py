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
