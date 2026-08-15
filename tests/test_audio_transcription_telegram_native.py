import asyncio
from types import SimpleNamespace

import pytest

from audio_transcription.telegram_native import (
    NativeTranscriptionError,
    TelegramNativeTranscriber,
    flood_wait_seconds,
    is_transient_internal_error,
    is_voice_too_long_error,
    normalize_transcript_text,
    resolve_telegram_peer,
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


def test_private_numeric_peer_is_resolved_from_dialogs(monkeypatch):
    entity = SimpleNamespace(peer_id=-1001234567890)

    class Client:
        async def iter_dialogs(self):
            yield SimpleNamespace(entity=entity)

        async def get_input_entity(self, value):
            return value

    monkeypatch.setattr("telethon.utils.get_peer_id", lambda value: value.peer_id)

    async def exercise():
        resolved = await resolve_telegram_peer(Client(), "-1001234567890")
        assert resolved is entity

    asyncio.run(exercise())


class InterdcCallErrorError(Exception):
    pass


def test_telegram_internal_errors_are_retryable_without_resending_voice():
    assert is_transient_internal_error(
        InterdcCallErrorError("An error occurred while communicating with DC 102")
    )
    assert is_transient_internal_error(ValueError("Request was unsuccessful 6 time(s)"))
    assert not is_transient_internal_error(ValueError("other"))


def test_internal_dc_backoff_reuses_one_temporary_voice(tmp_path, monkeypatch):
    class Message:
        id = 123

    class Client:
        send_count = 0
        transcribe_count = 0
        delete_count = 0

        async def send_file(self, *_args, **_kwargs):
            self.send_count += 1
            return Message()

        def add_event_handler(self, *_args, **_kwargs):
            return None

        def remove_event_handler(self, *_args, **_kwargs):
            return None

        async def __call__(self, _request):
            self.transcribe_count += 1
            raise ValueError("Request was unsuccessful 6 time(s)")

        async def delete_messages(self, *_args, **_kwargs):
            self.delete_count += 1

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("audio_transcription.telegram_native.asyncio.sleep", no_sleep)
    voice = tmp_path / "voice.ogg"
    voice.write_bytes(b"OggSfixture")
    client = Client()
    transcriber = TelegramNativeTranscriber(client, object())

    async def exercise():
        with pytest.raises(NativeTranscriptionError) as caught:
            await transcriber.transcribe_voice_file(voice)
        assert caught.value.code == "TELEGRAM_INTERNAL_RETRY_EXHAUSTED"

    asyncio.run(exercise())
    assert client.send_count == 1
    assert client.transcribe_count == 3
    assert client.delete_count == 1
    assert transcriber.cleanup_attempts == 1
    assert transcriber.cleanup_succeeded == 1
