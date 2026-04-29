from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

from smart_update_lollipop_lab import legacy_writer_family


def _load_benchmark_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "inspect" / "benchmark_lollipop_g4.py"
    spec = importlib.util.spec_from_file_location("benchmark_lollipop_g4", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_lollipop_legacy_enhancement_normalization_filters_logistics() -> None:
    payload = {
        "lead_hook_fact_indexes": [2, "2", 9, "bad"],
        "quote_candidates": ["  редкая прогулка  ", "редкая прогулка"],
        "extra_facts": [
            {"text": "В основе прогулки — звуковой маршрут по городу.", "source_refs": ["tg"]},
            {"text": "Билеты стоят 300 руб.", "source_refs": ["tg"]},
        ],
        "writer_notes": [" держать аудио-формат в lead "],
    }

    normalized = legacy_writer_family.normalize_enhancement_payload(payload, baseline_fact_count=3)

    assert normalized["lead_hook_fact_indexes"] == [2]
    assert normalized["quote_candidates"] == ["редкая прогулка"]
    assert normalized["extra_facts"] == [
        {"text": "В основе прогулки — звуковой маршрут по городу.", "source_refs": ["tg"]}
    ]
    assert normalized["writer_notes"] == ["держать аудио-формат в lead"]


def test_lollipop_legacy_validation_requires_fact_floor_coverage() -> None:
    result = legacy_writer_family.validate_writer_output(
        baseline_facts=["Факт один", "Факт два"],
        baseline_description="Факт один. Факт два.",
        enhancement={"extra_facts": []},
        output={
            "description_md": "Факт один.",
            "covered_baseline_fact_indexes": [0],
            "used_extra_fact_indexes": [],
        },
    )

    assert "baseline_fact.missing:1" in result.errors


def test_lollipop_legacy_validation_rejects_bad_register() -> None:
    result = legacy_writer_family.validate_writer_output(
        baseline_facts=["Концерт проходит раз в сезон."],
        baseline_description="Концерт проходит раз в сезон.",
        enhancement={"extra_facts": []},
        output={
            "description_md": "Концерт — это уникальная возможность, которая не оставит равнодушным.",
            "covered_baseline_fact_indexes": [0],
            "used_extra_fact_indexes": [],
        },
    )

    assert "lead.meta_opening" in result.errors
    assert any(item.startswith("style.promo_phrase:") for item in result.errors)


def test_benchmark_accepts_lollipop_legacy_variant_and_static_fixtures() -> None:
    benchmark = _load_benchmark_module()

    assert benchmark._variants_from_cli("lollipop_legacy") == ["baseline", "lollipop_legacy"]

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


def test_lollipop_legacy_fact_floor_filters_logistics() -> None:
    benchmark = _load_benchmark_module()

    floor = benchmark._legacy_event_fact_floor(
        [
            "Лекция посвящена быту и нравам флота.",
            "Билеты доступны на сайте музея.",
            "Место старта: Бар Советов, проспект Мира, 118",
            "Лектор — Борис Мегорский.",
        ]
    )

    assert floor == [
        "Лекция посвящена быту и нравам флота.",
        "Лектор — Борис Мегорский.",
    ]
