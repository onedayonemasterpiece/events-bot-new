from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .chunking import plan_chunks, reindex_chunks, split_chunk_near_middle
from .contracts import AudioChunk, Precision, SCHEMA_VERSION, TranscriptResult, TranscriptSegment
from .exports import write_exports
from .ffmpeg import (
    AudioProcessingError,
    detect_silence_end_points,
    probe_audio,
    transcode_chunk_to_voice,
)
from .telegram_native import NativeTranscriptionError, TelegramNativeTranscriber
from .time_anchor import absolute_at, resolve_recording_anchor

INPUT_ROOT = Path("/kaggle/input")
WORKING_ROOT = Path("/kaggle/working")
OUTPUT_ROOT = WORKING_ROOT / "audio_transcription_output"


def _find_unique(name: str) -> Path:
    matches = sorted(INPUT_ROOT.rglob(name))
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one {name}, found {len(matches)}")
    return matches[0]


def _ensure_runtime_dependencies() -> None:
    try:
        import telethon  # noqa: F401
        from cryptography.fernet import Fernet  # noqa: F401
    except ImportError:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-q",
                "telethon>=1.40,<2",
                "cryptography>=42,<46",
            ]
        )


def _load_request() -> dict[str, Any]:
    payload = json.loads(_find_unique("request.json").read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("request.json must contain an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("request.json schema version mismatch")
    required = {
        "job_ref",
        "source_file",
        "source_sha256",
        "source_name",
        "precision",
        "timezone",
        "telegram_peer",
    }
    missing = sorted(key for key in required if not payload.get(key))
    if missing:
        raise RuntimeError("request.json missing fields: " + ", ".join(missing))
    return payload


def _load_secrets() -> dict[str, str]:
    from cryptography.fernet import Fernet

    encrypted = _find_unique("secrets.enc").read_bytes()
    key = _find_unique("fernet.key").read_bytes().strip()
    decoded = Fernet(key).decrypt(encrypted)
    payload = json.loads(decoded.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("decrypted secrets must be an object")
    return {str(key): str(value) for key, value in payload.items() if value is not None}


def _decode_auth_bundle(value: str) -> dict[str, Any]:
    raw = value.strip()
    padding = "=" * ((4 - len(raw) % 4) % 4)
    payload = json.loads(base64.urlsafe_b64decode((raw + padding).encode("ascii")))
    if not isinstance(payload, dict) or not payload.get("session"):
        raise RuntimeError("Telegram auth bundle is invalid")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def _open_telegram(secrets: dict[str, str]):
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    bundle_env = secrets.get("AUDIO_TRANSCRIPTION_AUTH_BUNDLE_ENV") or ""
    if not bundle_env or not secrets.get(bundle_env):
        raise RuntimeError("dedicated Telegram auth bundle missing")
    bundle = _decode_auth_bundle(secrets[bundle_env])
    api_id = int(secrets.get("TG_API_ID") or bundle.get("api_id") or 0)
    api_hash = secrets.get("TG_API_HASH") or str(bundle.get("api_hash") or "")
    if not api_id or not api_hash:
        raise RuntimeError("Telegram API credentials missing")
    client = TelegramClient(
        StringSession(str(bundle["session"])),
        api_id,
        api_hash,
        device_model=str(bundle.get("device_model") or "events-bot audio worker"),
        system_version=str(bundle.get("system_version") or "Kaggle Linux"),
        app_version=str(bundle.get("app_version") or "events-bot-audio/1"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(
            bundle.get("system_lang_code") or bundle.get("lang_code") or "ru"
        ),
        flood_sleep_threshold=60,
    )
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Telegram transcription session is not authorized")
    return client


async def _transcribe_chunk(
    *,
    source: Path,
    chunk: AudioChunk,
    silence_points: tuple[int, ...],
    transcriber: TelegramNativeTranscriber,
    temporary_root: Path,
    depth: int = 0,
) -> list[tuple[AudioChunk, str, str]]:
    if depth > 8:
        raise RuntimeError("voice chunk split recursion exceeded")
    voice_path = temporary_root / f"chunk-{chunk.start_ms}-{chunk.end_ms}.ogg"
    transcode_chunk_to_voice(source, chunk, voice_path)
    digest = _sha256(voice_path)
    try:
        for attempt in range(3):
            try:
                native = await transcriber.transcribe_voice_file(
                    voice_path,
                    caption=f"audio transcription {chunk.index + 1}",
                )
                return [(chunk, native.text, digest)]
            except NativeTranscriptionError as exc:
                if exc.code == "TELEGRAM_EMPTY_TRANSCRIPT":
                    return []
                if exc.code == "TELEGRAM_VOICE_TOO_LONG":
                    left, right = split_chunk_near_middle(chunk, silence_points)
                    first = await _transcribe_chunk(
                        source=source,
                        chunk=left,
                        silence_points=silence_points,
                        transcriber=transcriber,
                        temporary_root=temporary_root,
                        depth=depth + 1,
                    )
                    second = await _transcribe_chunk(
                        source=source,
                        chunk=right,
                        silence_points=silence_points,
                        transcriber=transcriber,
                        temporary_root=temporary_root,
                        depth=depth + 1,
                    )
                    return [*first, *second]
                if exc.retry_safe and attempt < 2:
                    await asyncio.sleep(2**attempt)
                    continue
                raise
        raise RuntimeError("Telegram chunk retry loop exhausted")
    finally:
        voice_path.unlink(missing_ok=True)


async def run() -> None:
    _ensure_runtime_dependencies()
    request = _load_request()
    secrets = _load_secrets()
    source = _find_unique(str(request["source_file"]))
    if _sha256(source) != str(request["source_sha256"]):
        raise RuntimeError("source digest does not match handoff")
    precision = Precision(str(request["precision"]))
    probe = probe_audio(source)
    try:
        silence_points = detect_silence_end_points(source)
    except AudioProcessingError:
        silence_points = ()
    chunks = plan_chunks(
        probe.duration_ms,
        silence_points,
        precision=precision,
    )
    anchor = resolve_recording_anchor(
        explicit_started_at=(
            str(request["recording_started_at"])
            if request.get("recording_started_at")
            else None
        ),
        tags=probe.tags,
        file_name=str(request["source_name"]),
        timezone_name=str(request["timezone"]),
    )
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    OUTPUT_ROOT.mkdir(parents=True)
    temp_root = WORKING_ROOT / "audio_transcription_chunks"
    if temp_root.exists():
        shutil.rmtree(temp_root)
    temp_root.mkdir(parents=True)

    client = await _open_telegram(secrets)
    try:
        peer = await client.get_input_entity(str(request["telegram_peer"]))
        transcriber = TelegramNativeTranscriber(
            client,
            peer,
            timeout_seconds=int(request.get("transcription_timeout_seconds") or 180),
            cleanup_messages=bool(request.get("cleanup_messages", True)),
        )
        observed: list[tuple[AudioChunk, str, str]] = []
        for index, chunk in enumerate(chunks, start=1):
            print(
                json.dumps(
                    {
                        "event": "chunk_started",
                        "index": index,
                        "total": len(chunks),
                        "start_ms": chunk.start_ms,
                        "end_ms": chunk.end_ms,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            observed.extend(
                await _transcribe_chunk(
                    source=source,
                    chunk=chunk,
                    silence_points=silence_points,
                    transcriber=transcriber,
                    temporary_root=temp_root,
                )
            )
    finally:
        await client.disconnect()
        shutil.rmtree(temp_root, ignore_errors=True)

    normalized_chunks = reindex_chunks(item[0] for item in observed)
    indexed = {
        (chunk.start_ms, chunk.end_ms): (text, digest)
        for chunk, text, digest in observed
    }
    segments: list[TranscriptSegment] = []
    for index, chunk in enumerate(normalized_chunks, start=1):
        text, digest = indexed[(chunk.start_ms, chunk.end_ms)]
        segments.append(
            TranscriptSegment(
                id=f"seg_{index:06d}",
                source_start_ms=chunk.start_ms,
                source_end_ms=chunk.end_ms,
                text=text,
                absolute_start=absolute_at(anchor, chunk.start_ms),
                absolute_end=absolute_at(anchor, chunk.end_ms),
                chunk_sha256=digest,
            )
        )
    result = TranscriptResult(
        job_ref=str(request["job_ref"]),
        source_sha256=str(request["source_sha256"]),
        source_name=str(request["source_name"]),
        probe=probe,
        anchor=anchor,
        precision=precision,
        segments=tuple(segments),
        created_at=datetime.now(timezone.utc),
    )
    files = write_exports(
        result,
        OUTPUT_ROOT,
        timezone_name=str(request["timezone"]),
    )
    manifest = {
        "schema_version": "events-bot.audio-transcription-manifest.v1",
        "job_ref": result.job_ref,
        "source_sha256": result.source_sha256,
        "files": files,
        "telegram": {
            "native_transcriptions": len(segments),
            "temporary_messages": {
                "cleanup_enabled": transcriber.cleanup_messages,
                "cleanup_attempts": transcriber.cleanup_attempts,
                "cleanup_succeeded": transcriber.cleanup_succeeded,
                "cleanup_failed": transcriber.cleanup_failed,
            },
        },
    }
    (OUTPUT_ROOT / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "event": "transcription_complete",
                "segments": len(segments),
                "duration_ms": probe.duration_ms,
                "anchor_source": anchor.source.value,
                "temporary_message_cleanup": {
                    "attempts": transcriber.cleanup_attempts,
                    "succeeded": transcriber.cleanup_succeeded,
                    "failed": transcriber.cleanup_failed,
                },
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
