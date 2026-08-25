from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl import functions, types

CHAT_ID = -1004337049383
LINKED_MESSAGE_ID = 1169
MAX_CHAT_SCAN = 5000
MAX_THREAD_REPLIES = 2000
PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAxGSFE/tjUs8j+CZ6CTEG
mggNfwY3diXpg1B6pR5vAl6c5oW3qWFLwOW4bzS/Ukt6wMgrKnCm5Tl2dtn5mq6m
gzbfW3EUmlOXFQ54Ugbhr7g9kHFzWy5HprHW9qgAGl+xQSKXbEmX4572PKvbw3v+
A3mZE+0YJTNn8semqqoVU8yUcDJdz9i9NDvX+ZLorjXXAvw0sHXpRffbSEh6Gu7Y
JfPztUUI2dhu9S/zElY8rqxfOPdeUrkVXpzm90KDxRyjsN1CNxPRZXh/AKvZtCdw
l72iGqQXmTqsUpaNs3tkKcwXkSFgmI1NabWm1UEupgOTccXe1BVEPrBg3RpsZxpI
BlXJ5O0DpU2i9gStwkLo2enS7+zXmQ8eQPNt6SiiOLU0KpQnO4R+RAjrWfIeDy2v
1DT8oqCda3ZxdnxWp8yZW2WshpS6nhHJolzGcILzM3hNjdRGlGVoErPvW6nBj7CA
6Je7aPYdWue5JRRffofCPjnvxZblPD1EhZ7oLTRofjdDAgMBAAE=
-----END PUBLIC KEY-----
"""
AAD = b"telegram-thread-1169-v1"


def decode_bundle(value: str) -> dict[str, Any]:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("missing_auth_bundle")
    if raw.startswith("{"):
        data = json.loads(raw)
    else:
        padded = raw + ("=" * (-len(raw) % 4))
        decoded: bytes | None = None
        for decoder in (base64.urlsafe_b64decode, base64.b64decode):
            try:
                decoded = decoder(padded.encode("ascii"))
                break
            except Exception:
                continue
        if decoded is None:
            raise RuntimeError("invalid_auth_bundle_encoding")
        data = json.loads(decoded.decode("utf-8"))
    if not isinstance(data, dict) or not str(data.get("session") or "").strip():
        raise RuntimeError("invalid_auth_bundle")
    return data


def topic_id_of(message: Any) -> int | None:
    header = getattr(message, "reply_to", None)
    top_id = getattr(header, "reply_to_top_id", None)
    if top_id is not None:
        return int(top_id)
    if getattr(header, "forum_topic", False):
        reply_id = getattr(header, "reply_to_msg_id", None)
        if reply_id is not None:
            return int(reply_id)
    return None


def kind_of(message: Any) -> str:
    if bool(getattr(message, "voice", None)):
        return "voice"
    if bool(getattr(message, "video_note", None)):
        return "video_note"
    if bool(getattr(message, "audio", None)):
        return "audio"
    if bool(getattr(message, "photo", None)):
        return "photo"
    if bool(getattr(message, "document", None)):
        return "document"
    return "text"


def message_urls(message: Any) -> list[str]:
    text = str(getattr(message, "raw_text", "") or "")
    result: list[str] = []
    for entity in getattr(message, "entities", None) or []:
        value = ""
        if isinstance(entity, types.MessageEntityTextUrl):
            value = str(entity.url or "").strip()
        elif isinstance(entity, types.MessageEntityUrl):
            value = text[entity.offset : entity.offset + entity.length].strip()
        if value and value not in result:
            result.append(value)
    return result


def media_duration(message: Any) -> int | None:
    document = getattr(message, "document", None)
    for attribute in getattr(document, "attributes", None) or []:
        duration = getattr(attribute, "duration", None)
        if duration is not None:
            try:
                return int(duration)
            except (TypeError, ValueError):
                return None
    return None


async def native_transcript(
    client: TelegramClient,
    peer: Any,
    message: Any,
    *,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    completed: asyncio.Future[Any] = loop.create_future()

    async def on_raw(update: object) -> None:
        if not isinstance(update, types.UpdateTranscribedAudio):
            return
        if int(update.msg_id) != int(message.id):
            return
        if not bool(update.pending) and not completed.done():
            completed.set_result(update)

    client.add_event_handler(on_raw, events.Raw)
    try:
        result = await client(
            functions.messages.TranscribeAudioRequest(peer=peer, msg_id=int(message.id))
        )
        if not bool(result.pending):
            return {
                "engine": "telegram",
                "text": str(result.text or "").strip(),
                "pending": False,
            }
        update = await asyncio.wait_for(completed, timeout=timeout_seconds)
        return {
            "engine": "telegram",
            "text": str(update.text or "").strip(),
            "pending": False,
        }
    finally:
        client.remove_event_handler(on_raw, events.Raw)


def local_transcribe_file(path: Path, model_holder: list[Any]) -> dict[str, Any]:
    from faster_whisper import WhisperModel

    if not model_holder:
        model_holder.append(
            WhisperModel("small", device="cpu", compute_type="int8")
        )
    model = model_holder[0]
    segments, info = model.transcribe(
        str(path),
        language="ru",
        vad_filter=True,
        beam_size=5,
    )
    text = " ".join(
        segment.text.strip() for segment in segments if segment.text.strip()
    ).strip()
    return {
        "engine": "faster-whisper",
        "model": "small",
        "language": str(getattr(info, "language", None) or "ru"),
        "text": text,
    }


async def local_transcript(
    client: TelegramClient,
    message: Any,
    model_holder: list[Any],
    temp_dir: Path,
) -> dict[str, Any]:
    suffix = ".mp4" if bool(getattr(message, "video_note", None)) else ".ogg"
    target = temp_dir / f"{int(message.id)}{suffix}"
    downloaded = await client.download_media(message, file=str(target))
    if not downloaded or not target.is_file() or target.stat().st_size == 0:
        raise RuntimeError("media_download_failed")
    return await asyncio.to_thread(local_transcribe_file, target, model_holder)


async def sender_snapshot(message: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "sender_id": int(message.sender_id) if message.sender_id is not None else None,
        "out": bool(getattr(message, "out", False)),
    }
    try:
        sender = await message.get_sender()
    except Exception:
        sender = None
    if sender is not None:
        first = str(getattr(sender, "first_name", "") or "").strip()
        last = str(getattr(sender, "last_name", "") or "").strip()
        title = str(getattr(sender, "title", "") or "").strip()
        username = str(getattr(sender, "username", "") or "").strip()
        display = " ".join(part for part in (first, last) if part).strip() or title
        result["display_name"] = display or None
        result["username"] = username or None
    return result


async def collect_messages(
    client: TelegramClient,
    entity: Any,
    target: Any,
) -> tuple[list[Any], dict[str, Any]]:
    explicit_topic_id = topic_id_of(target)
    action = getattr(target, "action", None)
    is_topic_root = isinstance(action, types.MessageActionTopicCreate)
    root_id = explicit_topic_id or (int(target.id) if is_topic_root else None)

    collected: dict[int, Any] = {int(target.id): target}
    selection_mode = "linked_message_only"

    if root_id is not None:
        selection_mode = "forum_topic"
        root = await client.get_messages(entity, ids=root_id)
        if root:
            collected[int(root.id)] = root

        try:
            async for message in client.iter_messages(
                entity,
                reply_to=root_id,
                limit=MAX_THREAD_REPLIES,
                reverse=True,
            ):
                if getattr(message, "id", None):
                    collected[int(message.id)] = message
        except Exception:
            pass

        async for message in client.iter_messages(entity, limit=MAX_CHAT_SCAN):
            if not getattr(message, "id", None):
                continue
            if int(message.id) == root_id or topic_id_of(message) == root_id:
                collected[int(message.id)] = message
    else:
        try:
            async for message in client.iter_messages(
                entity,
                reply_to=int(target.id),
                limit=MAX_THREAD_REPLIES,
                reverse=True,
            ):
                if getattr(message, "id", None):
                    collected[int(message.id)] = message
        except Exception:
            pass
        if len(collected) > 1:
            selection_mode = "reply_thread"
            root_id = int(target.id)
        else:
            selection_mode = "message_range_from_link"
            async for message in client.iter_messages(
                entity,
                min_id=int(target.id) - 1,
                limit=1000,
                reverse=True,
            ):
                if getattr(message, "id", None):
                    collected[int(message.id)] = message

    ordered = [collected[key] for key in sorted(collected)]
    metadata = {
        "selection_mode": selection_mode,
        "topic_root_id": root_id,
        "linked_message_id": int(target.id),
        "message_count": len(ordered),
    }
    return ordered, metadata


async def capture() -> tuple[dict[str, Any], dict[str, Any]]:
    bundle = decode_bundle(os.environ.get("TELEGRAM_AUTH_BUNDLE_GH_ACTIONS", ""))
    session = str(bundle["session"]).strip()
    api_id = int(
        os.environ.get("TELEGRAM_GH_ACTIONS_API_ID", "").strip()
        or str(bundle.get("api_id") or "").strip()
    )
    api_hash = (
        os.environ.get("TELEGRAM_GH_ACTIONS_API_HASH", "").strip()
        or str(bundle.get("api_hash") or "").strip()
    )
    if not api_hash:
        raise RuntimeError("missing_api_hash")

    client = TelegramClient(
        StringSession(session),
        api_id,
        api_hash,
        device_model="GitHub Actions encrypted thread capture",
        system_version="Linux",
        app_version="thread-capture/1.0",
        flood_sleep_threshold=30,
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("telegram_session_not_authorized")
        entity = await client.get_input_entity(CHAT_ID)
        target = await client.get_messages(entity, ids=LINKED_MESSAGE_ID)
        if not target:
            raise RuntimeError("linked_message_not_found")

        messages, selection = await collect_messages(client, entity, target)
        if not messages:
            raise RuntimeError("empty_thread")

        model_holder: list[Any] = []
        normalized: list[dict[str, Any]] = []
        voice_like_count = 0
        native_count = 0
        local_count = 0
        effective_count = 0

        with tempfile.TemporaryDirectory(prefix="telegram-thread-1169-") as temp:
            temp_dir = Path(temp)
            for message in messages:
                kind = kind_of(message)
                raw_text = str(getattr(message, "raw_text", "") or "").strip()
                transcript: dict[str, Any] | None = None

                if kind in {"voice", "audio", "video_note"}:
                    voice_like_count += 1
                    native: dict[str, Any] | None = None
                    local: dict[str, Any] | None = None
                    native_error: str | None = None
                    local_error: str | None = None

                    try:
                        native = await native_transcript(client, entity, message)
                        if str(native.get("text") or "").strip():
                            native_count += 1
                    except Exception as exc:
                        native_error = type(exc).__name__

                    try:
                        local = await local_transcript(
                            client,
                            message,
                            model_holder,
                            temp_dir,
                        )
                        if str(local.get("text") or "").strip():
                            local_count += 1
                    except Exception as exc:
                        local_error = type(exc).__name__

                    effective = (
                        str((local or {}).get("text") or "").strip()
                        or str((native or {}).get("text") or "").strip()
                        or raw_text
                    )
                    if effective:
                        effective_count += 1
                    transcript = {
                        "native": native,
                        "native_error_type": native_error,
                        "local": local,
                        "local_error_type": local_error,
                        "effective_text": effective,
                    }

                date = getattr(message, "date", None)
                if date is None:
                    sent_at = None
                else:
                    if date.tzinfo is None:
                        date = date.replace(tzinfo=UTC)
                    sent_at = date.astimezone(UTC).isoformat().replace("+00:00", "Z")

                reply_header = getattr(message, "reply_to", None)
                reply_to_msg_id = getattr(reply_header, "reply_to_msg_id", None)
                item = {
                    "message_id": int(message.id),
                    "topic_id": topic_id_of(message),
                    "reply_to_message_id": (
                        int(reply_to_msg_id) if reply_to_msg_id is not None else None
                    ),
                    "sent_at": sent_at,
                    "kind": kind,
                    "text": raw_text,
                    "urls": message_urls(message),
                    "duration_seconds": media_duration(message),
                    "sender": await sender_snapshot(message),
                    "transcription": transcript,
                }
                normalized.append(item)

        payload = {
            "schema_version": 1,
            "captured_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "source": {
                "chat_id": CHAT_ID,
                **selection,
            },
            "messages": normalized,
        }
        status = {
            "schema_version": 1,
            "capture_complete": (
                len(normalized) > 0 and effective_count == voice_like_count
            ),
            "message_count": len(normalized),
            "voice_like_count": voice_like_count,
            "native_transcribed_count": native_count,
            "local_transcribed_count": local_count,
            "effective_transcribed_count": effective_count,
            "selection_mode": selection["selection_mode"],
        }
        return payload, status
    finally:
        await client.disconnect()


def encrypt_payload(payload: dict[str, Any]) -> dict[str, Any]:
    plaintext = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, AAD)
    public_key = serialization.load_pem_public_key(PUBLIC_KEY_PEM)
    wrapped_key = public_key.encrypt(
        key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return {
        "schema_version": 1,
        "algorithm": "RSA-OAEP-SHA256+A256GCM",
        "aad_b64": base64.b64encode(AAD).decode("ascii"),
        "wrapped_key_b64": base64.b64encode(wrapped_key).decode("ascii"),
        "nonce_b64": base64.b64encode(nonce).decode("ascii"),
        "ciphertext_b64": base64.b64encode(ciphertext).decode("ascii"),
        "plaintext_sha256": hashlib.sha256(plaintext).hexdigest(),
    }


def main() -> None:
    output_dir = Path("secure-capture")
    output_dir.mkdir(parents=True, exist_ok=True)
    status_path = output_dir / "status.json"
    try:
        payload, status = asyncio.run(capture())
        envelope = encrypt_payload(payload)
        (output_dir / "thread.enc.json").write_text(
            json.dumps(envelope, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        status["encrypted_payload_written"] = True
        status_path.write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        status = {
            "schema_version": 1,
            "capture_complete": False,
            "encrypted_payload_written": False,
            "error_type": type(exc).__name__,
        }
        status_path.write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
