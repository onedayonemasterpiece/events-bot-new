from __future__ import annotations

import json

from scripts import run_contour_svg_kaggle_sample as launcher
from scripts import run_contour_svg_neural_branch_kaggle as neural_launcher


def _set_limiter_env(monkeypatch) -> None:
    monkeypatch.setenv(
        "GOOGLE_AI_LIMITER_SUPABASE_URL", "https://limiter.supabase.co"
    )
    monkeypatch.setenv(
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", "limiter-service-key"
    )


def test_secret_payload_adds_limiter_overflow_envs(monkeypatch) -> None:
    for name in [
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
        "GOOGLE_API_KEY4",
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS",
    ]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "primary")
    monkeypatch.setenv("GOOGLE_API_KEY2", "secondary")
    monkeypatch.setenv("GOOGLE_API_KEY3", "tertiary")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "supabase-key")
    _set_limiter_env(monkeypatch)

    payload = json.loads(launcher.build_secret_payload())

    assert payload["GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"] == "GOOGLE_API_KEY2,GOOGLE_API_KEY3"


def test_secret_payload_respects_explicit_limiter_overflow_envs(monkeypatch) -> None:
    for name in [
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
        "GOOGLE_API_KEY4",
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS",
    ]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "primary")
    monkeypatch.setenv("GOOGLE_API_KEY2", "secondary")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "supabase-key")
    monkeypatch.setenv("GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS", "GOOGLE_API_KEY3")
    _set_limiter_env(monkeypatch)

    payload = json.loads(launcher.build_secret_payload())

    assert payload["GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"] == "GOOGLE_API_KEY3"


def test_secret_payload_derives_key4_limiter_overflow_env(monkeypatch) -> None:
    for name in [
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
        "GOOGLE_API_KEY4",
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS",
    ]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "primary")
    monkeypatch.setenv("GOOGLE_API_KEY4", "quaternary")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "supabase-key")
    _set_limiter_env(monkeypatch)

    payload = json.loads(launcher.build_secret_payload())

    assert payload["GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"] == "GOOGLE_API_KEY4"


def test_build_run_config_keeps_config_project_relative() -> None:
    config = launcher.build_run_config(
        run_id="contour-svg-tower-1",
        config_path=launcher.PROJECT_ROOT / "docs/features/countur_svg_generator/examples/tower_92_11_16.yaml",
        output_dir="/kaggle/working/contour_svg_tower_92_11_16",
    )

    assert config == {
        "run_id": "contour-svg-tower-1",
        "config_path": "docs/features/countur_svg_generator/examples/tower_92_11_16.yaml",
        "output_dir": "/kaggle/working/contour_svg_tower_92_11_16",
    }


def test_compact_unique_slug_preserves_run_suffix() -> None:
    slug = launcher.compact_unique_slug("contour-svg-tower-neural-20260614-210013-f972e0", max_len=32)

    assert len(slug) <= 32
    assert slug.endswith("210013-f972e0")


def test_neural_run_config_uses_payload_artifact_dir() -> None:
    config = neural_launcher.build_neural_run_config(
        run_id="contour-svg-tower-neural-1",
        artifact_dir=launcher.PROJECT_ROOT / "artifacts/codex/example",
        style_reference=launcher.PROJECT_ROOT
        / "docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp",
        variants="A1,C2",
        init_modes="line_init",
        seeds="92",
    )

    assert config == {
        "run_id": "contour-svg-tower-neural-1",
        "artifact_dir": "neural_artifacts",
        "style_reference": "docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp",
        "variants": "A1,C2",
        "init_modes": "line_init",
        "seeds": "92",
    }
