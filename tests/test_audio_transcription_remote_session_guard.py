from __future__ import annotations

import ast
from pathlib import Path


def test_audio_transcription_remote_job_type_is_guarded() -> None:
    tree = ast.parse(Path("remote_telegram_session.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        else:
            continue
        if not any(
            isinstance(target, ast.Name)
            and target.id == "REMOTE_TELEGRAM_KAGGLE_JOB_TYPES"
            for target in targets
        ):
            continue
        strings = {
            item.value
            for item in ast.walk(value)
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        }
        assert "audio_transcription" in strings
        return
    raise AssertionError("REMOTE_TELEGRAM_KAGGLE_JOB_TYPES assignment not found")
