from __future__ import annotations

import json

from scripts import run_contour_svg_kaggle_sample as launcher


def test_secret_payload_adds_limiter_overflow_envs(monkeypatch) -> None:
    for name in [
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
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

    payload = json.loads(launcher.build_secret_payload())

    assert payload["GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"] == "GOOGLE_API_KEY2,GOOGLE_API_KEY3"


def test_secret_payload_respects_explicit_limiter_overflow_envs(monkeypatch) -> None:
    for name in [
        "GOOGLE_API_KEY",
        "GOOGLE_API_KEY2",
        "GOOGLE_API_KEY3",
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

    payload = json.loads(launcher.build_secret_payload())

    assert payload["GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS"] == "GOOGLE_API_KEY3"
