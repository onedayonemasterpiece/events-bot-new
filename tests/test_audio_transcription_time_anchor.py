from datetime import timedelta

from audio_transcription.contracts import AnchorSource
from audio_transcription.time_anchor import absolute_at, resolve_recording_anchor


def test_explicit_anchor_wins_and_preserves_offset():
    anchor = resolve_recording_anchor(
        explicit_started_at="2026-08-12T10:02:57+02:00",
        tags={"creation_time": "2026-08-11T09:00:00Z"},
        file_name="20260812_100000.m4a",
        timezone_name="Europe/Kaliningrad",
    )
    assert anchor.source is AnchorSource.EXPLICIT
    assert anchor.started_at.isoformat() == "2026-08-12T10:02:57+02:00"
    assert absolute_at(anchor, 16_420) == anchor.started_at + timedelta(milliseconds=16_420)


def test_quicktime_metadata_precedes_filename():
    anchor = resolve_recording_anchor(
        explicit_started_at=None,
        tags={"com.apple.quicktime.creationdate": "2026-08-12T10:02:57+0200"},
        file_name="20260811_100000.m4a",
        timezone_name="Europe/Kaliningrad",
    )
    assert anchor.source is AnchorSource.QUICKTIME_CREATION_TIME
    assert anchor.started_at.isoformat() == "2026-08-12T10:02:57+02:00"


def test_filename_anchor_and_missing_anchor():
    anchor = resolve_recording_anchor(
        explicit_started_at=None,
        tags={},
        file_name="Лекция 12.08.2026 10.02.57.m4a",
        timezone_name="Europe/Kaliningrad",
    )
    assert anchor.source is AnchorSource.FILENAME
    assert anchor.started_at.strftime("%Y-%m-%d %H:%M:%S %z") == "2026-08-12 10:02:57 +0200"

    missing = resolve_recording_anchor(
        explicit_started_at=None,
        tags={},
        file_name="recording.m4a",
        timezone_name="Europe/Kaliningrad",
    )
    assert missing.source is AnchorSource.MISSING
    assert missing.started_at is None


def test_stream_creation_time_preserves_stream_provenance():
    anchor = resolve_recording_anchor(
        explicit_started_at=None,
        tags={"stream.creation_time": "2026-08-12T08:02:57Z"},
        file_name="recording.m4a",
        timezone_name="Europe/Kaliningrad",
    )
    assert anchor.source is AnchorSource.STREAM_CREATION_TIME
    assert anchor.started_at.isoformat() == "2026-08-12T08:02:57+00:00"
