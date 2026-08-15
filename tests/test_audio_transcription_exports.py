from datetime import datetime

from audio_transcription.contracts import (
    AnchorSource,
    AudioProbe,
    Precision,
    RecordingAnchor,
    TranscriptResult,
    TranscriptSegment,
)
from audio_transcription.exports import render_plain, render_srt, render_timeline, render_vtt


def _result():
    start = datetime.fromisoformat("2026-08-12T10:02:57+02:00")
    return TranscriptResult(
        job_ref="atr_example",
        source_sha256="a" * 64,
        source_name="lecture.m4a",
        probe=AudioProbe(120_000, "mov,mp4", "aac", 48_000, 2, {}),
        anchor=RecordingAnchor(start, AnchorSource.EXPLICIT, 0),
        precision=Precision.PHRASE,
        segments=(
            TranscriptSegment(
                id="seg_000001",
                source_start_ms=16_420,
                source_end_ms=21_980,
                text="Сегодня   мы поговорим.",
                absolute_start=datetime.fromisoformat("2026-08-12T10:03:13.420+02:00"),
                absolute_end=datetime.fromisoformat("2026-08-12T10:03:18.980+02:00"),
            ),
        ),
        created_at=datetime.fromisoformat("2026-08-12T12:00:00+00:00"),
    )


def test_exports_keep_readable_text_and_source_relative_subtitles():
    result = _result()
    assert render_plain(result) == "Сегодня мы поговорим.\n"
    assert render_timeline(result, timezone_name="Europe/Kaliningrad") == (
        "[12.08.26 10:03:13] Сегодня мы поговорим.\n"
    )
    assert "00:00:16,420 --> 00:00:21,980" in render_srt(result)
    assert "00:00:16.420 --> 00:00:21.980" in render_vtt(result)
