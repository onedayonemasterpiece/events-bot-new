from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

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
    assert any(item.startswith("length.below_baseline_ratio:") for item in result.errors)


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


def test_lollipop_legacy_writer_payload_requires_baseline_volume() -> None:
    payload = legacy_writer_family.build_writer_payload(
        title="Событие",
        event_type="лекция",
        baseline_description="1234567890",
        baseline_facts=["Факт"],
        enhancement={"extra_facts": []},
    )

    assert payload["min_description_chars"] == 7
    assert payload["target_description_chars"] == 8
    assert payload["max_description_chars"] == 10
    assert payload["hard_validation_gates"]["minimum_description_chars"] == 6


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


def test_lollipop_legacy_required_floor_keeps_full_baseline_facts() -> None:
    benchmark = _load_benchmark_module()

    required = benchmark._legacy_required_fact_floor(
        [
            "Лекция посвящена быту и нравам флота.",
            "Билеты доступны на сайте музея.",
            "Лектор — Борис Мегорский.",
        ]
    )

    assert required == [
        "Лекция посвящена быту и нравам флота.",
        "Билеты доступны на сайте музея.",
        "Лектор — Борис Мегорский.",
    ]


def test_lollipop_legacy_quality_delta_rewards_compact_no_worse_rewrite() -> None:
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
    assert not delta["regressions"]


def test_lollipop_legacy_quality_delta_rejects_too_short_summary() -> None:
    delta = legacy_writer_family.compare_to_baseline(
        baseline_description=(
            "На выставке представлены жостовские подносы, каргопольская игрушка "
            "и дымковская игрушка в разделе «Красны девицы, добры молодцы»."
        ),
        candidate_description="На выставке покажут народные промыслы.",
    )

    assert delta["status"] == "regressed"
    assert any(item.startswith("quality.too_short_vs_baseline:") for item in delta["regressions"])


def test_lollipop_quality_detector_does_not_flag_predstavleniy_false_positive() -> None:
    quality = writer_final_4o_family._describe_text_quality(
        "В центре внимания — эволюция представлений о священном от древности до современности."
    )

    assert "stories_presented" not in quality["report_formula_hits"]


def test_lollipop_legacy_validation_rejects_duplicate_words() -> None:
    result = legacy_writer_family.validate_writer_output(
        baseline_facts=["Выставка открывается в музее."],
        baseline_description="Выставка открывается в музее.",
        enhancement={"extra_facts": []},
        output={
            "description_md": "Выставка открывается в музее и передает передает настроение автора.",
            "covered_baseline_fact_indexes": [0],
            "used_extra_fact_indexes": [],
        },
    )

    assert "text.duplicate_word:передает" in result.errors


def test_lollipop_legacy_validation_rejects_double_comma_artifact() -> None:
    result = legacy_writer_family.validate_writer_output(
        baseline_facts=["Лекция о флоте."],
        baseline_description="Лекция о флоте.",
        enhancement={"extra_facts": []},
        output={
            "description_md": "В центре внимания — состав моряков и то,, как была устроена жизнь на борту.",
            "covered_baseline_fact_indexes": [0],
            "used_extra_fact_indexes": [],
        },
    )

    assert "text.double_comma" in result.errors


def test_lollipop_legacy_quality_delta_flags_narrator_frame_regression() -> None:
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


def test_lollipop_legacy_quality_delta_rewards_narrator_frame_avoidance() -> None:
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
    assert "quality.narrator_frame_opening" not in delta["regressions"]


def test_lollipop_legacy_quality_delta_warns_on_lost_baseline_headings() -> None:
    baseline = (
        "Атмосфера лекции. Лектор расскажет о флоте Петра Великого.\n\n"
        "### Лектор и тема\nБорис Мегорский — заведующий отделом библиотеки.\n\n"
        "### Что будет на лекции\nПовседневная жизнь моряков, дисциплина, традиции.\n\n"
        "### Формат и детали\nОбстоятельный рассказ по архивным документам."
    )
    candidate = (
        "Атмосфера лекции. Борис Мегорский расскажет о флоте Петра Великого. "
        "Лектор разберёт повседневную жизнь моряков, дисциплину и традиции "
        "по архивным документам петровской эпохи."
    )

    delta = legacy_writer_family.compare_to_baseline(
        baseline_description=baseline,
        candidate_description=candidate,
    )

    assert any(item.startswith("quality.lost_baseline_headings:") for item in delta["warnings"])


def test_lollipop_legacy_validation_rejects_duplicate_tail_typos() -> None:
    result = legacy_writer_family.validate_writer_output(
        baseline_facts=["Лекция расскажет о быте моряков."],
        baseline_description="Лекция расскажет о быте моряков.",
        enhancement={"extra_facts": []},
        output={
            "description_md": "Лекция расскажет о быте моряков, их трудностиности и традициях.",
            "covered_baseline_fact_indexes": [0],
            "used_extra_fact_indexes": [],
        },
    )

    assert "text.duplicate_tail:ности" in result.errors
