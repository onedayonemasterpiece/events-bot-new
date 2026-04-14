from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    ("module_name", "helper_name"),
    [
        ("scripts.render_cherryflash_full", "_scale_audio_volume"),
        ("scripts.render_mobilefeed_intro_scene1_approval", "scale_audio_volume"),
    ],
)
def test_scale_audio_volume_uses_effects_fallback(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    helper_name: str,
) -> None:
    module = importlib.import_module(module_name)
    helper = getattr(module, helper_name)

    class _DummyAudio:
        def with_effects(self, effects):  # noqa: ANN204, ANN001
            return effects

    monkeypatch.setattr(
        module,
        "MultiplyVolume",
        lambda factor: ("multiply", factor),
        raising=False,
    )
    monkeypatch.setattr(module, "volumex_fx", None, raising=False)

    scaled = helper(_DummyAudio(), 0.45)

    assert scaled == [("multiply", 0.45)]


@pytest.mark.parametrize(
    ("module_name", "helper_name"),
    [
        ("scripts.render_cherryflash_full", "_scale_audio_volume"),
        ("scripts.render_mobilefeed_intro_scene1_approval", "scale_audio_volume"),
    ],
)
def test_scale_audio_volume_keeps_original_when_no_api_available(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    helper_name: str,
) -> None:
    module = importlib.import_module(module_name)
    helper = getattr(module, helper_name)

    class _DummyAudio:
        pass

    monkeypatch.setattr(module, "MultiplyVolume", None, raising=False)
    monkeypatch.setattr(module, "volumex_fx", None, raising=False)

    audio = _DummyAudio()

    assert helper(audio, 0.45) is audio


@pytest.mark.parametrize(
    "module_name",
    [
        "scripts.render_cherryflash_full",
        "scripts.render_mobilefeed_intro_scene1_approval",
    ],
)
def test_cherryflash_encodes_audio_at_192k(module_name: str) -> None:
    module = importlib.import_module(module_name)

    assert getattr(module, "AUDIO_BITRATE") == "192k"
