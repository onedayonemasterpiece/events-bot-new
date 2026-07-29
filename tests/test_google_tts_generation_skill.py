from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


SKILL = Path(".codex/skills/google-tts-generation")
SCRIPT = SKILL / "scripts/generate_tts.py"


def test_skill_forbids_direct_provider_and_raw_key_escape_hatches():
    skill_text = (SKILL / "SKILL.md").read_text()
    script_text = SCRIPT.read_text()

    assert "Never call `google.genai.Client`" in skill_text
    assert "direct-key fallback" in skill_text
    assert "generativelanguage.googleapis.com" not in script_text
    assert "google.genai" not in script_text
    assert "--api-key" not in script_text
    assert "--retry" not in script_text


def test_cli_help_has_check_but_no_raw_key_or_retry_options():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "--check" in result.stdout
    assert "--key-envs" in result.stdout
    assert "--api-key" not in result.stdout
    assert "--retry" not in result.stdout


def test_transcript_path_cannot_escape_artifacts(tmp_path):
    spec = importlib.util.spec_from_file_location("google_tts_skill_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    outside = tmp_path / "secret.txt"
    outside.write_text("do not send", encoding="utf-8")
    try:
        module.transcript_path(str(outside))
    except SystemExit as exc:
        assert "artifacts/codex/google-tts" in str(exc)
    else:
        raise AssertionError("outside transcript path must be rejected")
