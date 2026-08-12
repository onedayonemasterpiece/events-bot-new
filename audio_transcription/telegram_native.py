from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class NativeTranscriptionError(RuntimeError):
    def __init__(self, code: str, message: str, *, retry_safe: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retry_safe = retry_safe


def is_voice_too_long_error(exc: BaseException) -> bool:
    name = type(exc).__name__.casefold()
    message = str(exc).casefold()
    return (
        "msgvoicetoolong" in name
        or "msg_voice_too_long" in message
        or "voice too long" in message
    )


def flood_wait_seconds(exc: BaseException) -> int | None:
    raw = getattr(exc, "seconds", None)
    if raw is None:
        return None
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return None


def normalize_transcript_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


@dataclass(frozen=True, slots=True)
class NativeTranscript:
    text: str
    transcription_id: int | None


class TelegramNativeTranscriber:
    """Upload one OGG/Opus voice note and ask Telegram for native transcription."""

    def __init__(
        self,
        client: Any,
        peer: Any,
        *,
        timeout_seconds: int = 180,
        cleanup_messages: bool = True,
        max_flood_wait_seconds: int = 300,
    ) -> None:
        self.client = client
        self.peer = peer
        self.timeout_seconds = max(10, int(timeout_seconds))
        self.cleanup_messages = bool(cleanup_messages)
        self.max_flood_wait_seconds = max(0, int(max_flood_wait_seconds))
        self.cleanup_attempts = 0
        self.cleanup_succeeded = 0
        self.cleanup_failed = 0

    async def _transcribe_message(self, message: Any) -> NativeTranscript:
        try:
            from telethon import events
            from telethon.tl import functions, types
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise NativeTranscriptionError(
                "TELETHON_NOT_AVAILABLE", "telethon is not installed", retry_safe=False
            ) from exc

        loop = asyncio.get_running_loop()
        completed: asyncio.Future[Any] = loop.create_future()

        async def on_raw(update: object) -> None:
            if not isinstance(update, types.UpdateTranscribedAudio):
                return
            if int(getattr(update, "msg_id", 0)) != int(message.id):
                return
            if not bool(getattr(update, "pending", False)) and not completed.done():
                completed.set_result(update)

        self.client.add_event_handler(on_raw, events.Raw)
        try:
            for attempt in range(3):
                try:
                    result = await self.client(
                        functions.messages.TranscribeAudioRequest(
                            peer=self.peer,
                            msg_id=int(message.id),
                        )
                    )
                except Exception as exc:
                    wait_seconds = flood_wait_seconds(exc)
                    if (
                        wait_seconds is not None
                        and wait_seconds <= self.max_flood_wait_seconds
                        and attempt < 2
                    ):
                        await asyncio.sleep(wait_seconds + 1)
                        continue
                    raise
                if not bool(getattr(result, "pending", False)):
                    text = normalize_transcript_text(getattr(result, "text", ""))
                    if not text:
                        raise NativeTranscriptionError(
                            "TELEGRAM_EMPTY_TRANSCRIPT",
                            "Telegram returned an empty transcript",
                            retry_safe=False,
                        )
                    return NativeTranscript(
                        text=text,
                        transcription_id=(
                            int(result.transcription_id)
                            if getattr(result, "transcription_id", None) is not None
                            else None
                        ),
                    )
                try:
                    update = await asyncio.wait_for(
                        asyncio.shield(completed), timeout=self.timeout_seconds
                    )
                except asyncio.TimeoutError:
                    if attempt < 2:
                        continue
                    raise NativeTranscriptionError(
                        "TELEGRAM_TRANSCRIPTION_TIMEOUT",
                        "Telegram transcription did not finish in time",
                        retry_safe=True,
                    )
                text = normalize_transcript_text(getattr(update, "text", ""))
                if not text:
                    raise NativeTranscriptionError(
                        "TELEGRAM_EMPTY_TRANSCRIPT",
                        "Telegram returned an empty transcript",
                        retry_safe=False,
                    )
                return NativeTranscript(
                    text=text,
                    transcription_id=(
                        int(update.transcription_id)
                        if getattr(update, "transcription_id", None) is not None
                        else None
                    ),
                )
            raise NativeTranscriptionError(
                "TELEGRAM_TRANSCRIPTION_TIMEOUT",
                "Telegram transcription did not finish in time",
                retry_safe=True,
            )
        finally:
            self.client.remove_event_handler(on_raw, events.Raw)

    async def transcribe_voice_file(
        self,
        path: str | Path,
        *,
        caption: str | None = None,
    ) -> NativeTranscript:
        voice_path = Path(path)
        if not voice_path.is_file() or voice_path.stat().st_size <= 0:
            raise NativeTranscriptionError(
                "VOICE_CHUNK_MISSING", "voice chunk does not exist", retry_safe=False
            )
        sent = None
        try:
            for attempt in range(3):
                try:
                    sent = await self.client.send_file(
                        self.peer,
                        str(voice_path),
                        caption=caption,
                        voice_note=True,
                        force_document=False,
                    )
                    break
                except Exception as exc:
                    if is_voice_too_long_error(exc):
                        raise NativeTranscriptionError(
                            "TELEGRAM_VOICE_TOO_LONG",
                            "Telegram rejected an overlong voice note",
                            retry_safe=True,
                        ) from exc
                    wait_seconds = flood_wait_seconds(exc)
                    if (
                        wait_seconds is not None
                        and wait_seconds <= self.max_flood_wait_seconds
                        and attempt < 2
                    ):
                        await asyncio.sleep(wait_seconds + 1)
                        continue
                    raise
            if sent is None:
                raise NativeTranscriptionError(
                    "TELEGRAM_SEND_FAILED",
                    "Telegram voice upload did not complete",
                    retry_safe=True,
                )
            return await self._transcribe_message(sent)
        except NativeTranscriptionError:
            raise
        except Exception as exc:
            if is_voice_too_long_error(exc):
                raise NativeTranscriptionError(
                    "TELEGRAM_VOICE_TOO_LONG",
                    "Telegram rejected an overlong voice note",
                    retry_safe=True,
                ) from exc
            wait_seconds = flood_wait_seconds(exc)
            if wait_seconds is not None:
                raise NativeTranscriptionError(
                    "TELEGRAM_FLOOD_WAIT",
                    "Telegram requested a wait longer than the configured bound",
                    retry_safe=True,
                ) from exc
            raise NativeTranscriptionError(
                "TELEGRAM_TRANSCRIPTION_FAILED",
                f"Telegram native transcription failed: {type(exc).__name__}",
                retry_safe=False,
            ) from exc
        finally:
            if sent is not None and self.cleanup_messages:
                self.cleanup_attempts += 1
                try:
                    await self.client.delete_messages(
                        self.peer, [int(sent.id)], revoke=True
                    )
                except Exception:
                    # Cleanup is best-effort; the transcription result must not be
                    # discarded merely because Telegram retained a temporary note.
                    self.cleanup_failed += 1
                else:
                    self.cleanup_succeeded += 1
