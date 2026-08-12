from audio_transcription.telegram_native import (
    flood_wait_seconds,
    is_voice_too_long_error,
    normalize_transcript_text,
)


class MsgVoiceTooLongError(Exception):
    pass


def test_telegram_error_and_text_normalization():
    assert is_voice_too_long_error(MsgVoiceTooLongError("x"))
    assert is_voice_too_long_error(RuntimeError("MSG_VOICE_TOO_LONG"))
    assert not is_voice_too_long_error(RuntimeError("other"))
    assert normalize_transcript_text("  Привет   мир\n") == "Привет мир"


class FloodWaitLike(Exception):
    seconds = 42


def test_flood_wait_seconds_is_bounded_integer_input():
    assert flood_wait_seconds(FloodWaitLike()) == 42
    assert flood_wait_seconds(RuntimeError("no wait")) is None


def test_cleanup_counters_start_at_zero():
    transcriber = __import__(
        "audio_transcription.telegram_native", fromlist=["TelegramNativeTranscriber"]
    ).TelegramNativeTranscriber(object(), object())
    assert transcriber.cleanup_attempts == 0
    assert transcriber.cleanup_succeeded == 0
    assert transcriber.cleanup_failed == 0
