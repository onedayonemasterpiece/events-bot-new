from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_runtime() -> None:
    matches = sorted(Path("/kaggle/input").rglob("audio-transcription-runtime.bundle"))
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one audio-transcription-runtime.bundle, found {len(matches)}"
        )
    sys.path.insert(0, str(matches[0]))


_bootstrap_runtime()
from audio_transcription.kaggle_worker import main  # noqa: E402


if __name__ == "__main__":
    main()
