#!/usr/bin/env python3
"""Capture one source-faithful ingestion packet for bounded shadow replay.

This is deliberately a manual, pure serialization tool.  It does not import
``Database`` or any ingestion/publication entry point.  Operators give it an
already-produced packet (or call the pure ``build_*`` helpers immediately
before the corresponding handler) and it writes one immutable JSON artifact.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import re
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, urlsplit


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "docs" / "review-data" / "static_collection_upstream_capture.schema.json"
SCHEMA_VERSION = "static-collection-upstream-capture-v1"
CANONICAL_SERIALIZATION = "json-utf8-sort-keys-compact-no-nan-v1"
HANDLERS = {
    "telegram": "source_parsing.telegram.handlers.process_telegram_results",
    "vk": "vk_intake.persist_event_and_pages",
    "parser": "source_parsing.handlers.process_source_events",
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPO_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_CREDENTIAL_KEYS = {
    "access_token",
    "api_key",
    "api_secret",
    "api_token",
    "auth_bundle",
    "authorization",
    "cookie",
    "cookies",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "session",
    "session_string",
    "telegram_session",
    "tg_session",
}
_DEPENDENCY_KEYS = {
    "linked_source_urls",
    "linked_sources",
    "poster_bridge",
    "poster_bridge_target",
    "bridge_target",
    "reply_to",
    "reply_to_message_id",
    "reply_to_msg_id",
    "reply_message_id",
    "quoted_message",
    "quoted_message_id",
    "quote_source",
    "sibling_message_ids",
    "album_message_ids",
    "group_message_ids",
}
_POSTER_OWNER_KEYS = {
    "owner_message_id",
    "source_message_id",
    "assigned_from_message_id",
    "poster_message_id",
}


class CaptureContractError(ValueError):
    """A packet cannot be captured without changing its ingestion meaning."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        raise CaptureContractError("raw binary is only supported as VK PosterMedia.data")
    raise CaptureContractError(f"unsupported JSON value type: {type(value).__name__}")


def _is_nonempty(value: Any) -> bool:
    return value not in (None, "", 0, False, [], {})


def _assert_no_credentials(value: Any, *, pointer: str = "$") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.strip().casefold()
            child_pointer = f"{pointer}.{key}"
            if normalized in _CREDENTIAL_KEYS and _is_nonempty(child):
                raise CaptureContractError(
                    f"credential-like field is forbidden: {child_pointer}"
                )
            _assert_no_credentials(child, pointer=child_pointer)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _assert_no_credentials(child, pointer=f"{pointer}[{index}]")
    elif isinstance(value, str) and value.startswith(("http://", "https://")):
        query_keys = {key.strip().casefold() for key, _item in parse_qsl(urlsplit(value).query)}
        forbidden = sorted(query_keys & _CREDENTIAL_KEYS)
        if forbidden:
            raise CaptureContractError(
                f"credential-like URL query is forbidden: {pointer} keys={forbidden}"
            )


def _positive_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise CaptureContractError(f"{label} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CaptureContractError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise CaptureContractError(f"{label} must be a positive integer")
    return parsed


def _normalize_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@").casefold()


def _assert_telegram_dependencies_closed(
    message: Mapping[str, Any], *, selected_message_id: int, all_messages: Sequence[Any]
) -> None:
    """Reject cross-message dependencies instead of guessing how to rebuild them."""

    grouped_id = message.get("grouped_id")
    if _is_nonempty(grouped_id):
        sibling_ids = {
            _positive_int(item.get("message_id"), label="Telegram sibling message_id")
            for item in all_messages
            if isinstance(item, Mapping)
            and item is not message
            and item.get("grouped_id") == grouped_id
        }
        if sibling_ids:
            raise CaptureContractError(
                "Telegram grouped/album packet still depends on sibling messages: "
                f"{sorted(sibling_ids)}"
            )

    def walk(value: Any, *, pointer: str, poster_context: bool = False) -> None:
        if isinstance(value, Mapping):
            for raw_key, child in value.items():
                key = str(raw_key)
                normalized = key.strip().casefold()
                child_pointer = f"{pointer}.{key}"
                child_poster_context = poster_context or "poster" in normalized or "assigned_media" in normalized
                if normalized in _DEPENDENCY_KEYS and _is_nonempty(child):
                    raise CaptureContractError(
                        f"Telegram cross-message dependency is unsupported: {child_pointer}"
                    )
                if (normalized.startswith("reply_") or normalized.startswith("quoted_")) and _is_nonempty(child):
                    raise CaptureContractError(
                        f"Telegram reply/quoted dependency is unsupported: {child_pointer}"
                    )
                if child_poster_context and normalized in _POSTER_OWNER_KEYS and _is_nonempty(child):
                    owner_id = _positive_int(child, label=child_pointer)
                    if owner_id != selected_message_id:
                        raise CaptureContractError(
                            f"Telegram poster owner {owner_id} differs from selected message "
                            f"{selected_message_id}: {child_pointer}"
                        )
                walk(child, pointer=child_pointer, poster_context=child_poster_context)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for index, child in enumerate(value):
                walk(child, pointer=f"{pointer}[{index}]", poster_context=poster_context)

    walk(message, pointer="$.messages[0]")


def _telegram_payload(
    raw: Mapping[str, Any], *, source_username: str, message_id: int
) -> tuple[dict[str, Any], str]:
    messages = raw.get("messages")
    if not isinstance(messages, list):
        raise CaptureContractError("Telegram input messages must be an array")
    username = _normalize_username(source_username)
    selected = [
        item
        for item in messages
        if isinstance(item, Mapping)
        and _normalize_username(item.get("source_username")) == username
        and item.get("message_id") == message_id
    ]
    if len(selected) != 1:
        raise CaptureContractError(
            "Telegram selector must match exactly one message; "
            f"matched={len(selected)} username={username!r} message_id={message_id}"
        )
    message = selected[0]
    events = message.get("events")
    if not isinstance(events, list) or not events:
        raise CaptureContractError("selected Telegram message must contain at least one extracted event")
    _assert_telegram_dependencies_closed(
        message, selected_message_id=message_id, all_messages=messages
    )

    sources_meta = raw.get("sources_meta")
    if not isinstance(sources_meta, list):
        raise CaptureContractError("Telegram input sources_meta must be an array")
    matching_meta = [
        item
        for item in sources_meta
        if isinstance(item, Mapping) and _normalize_username(item.get("username")) == username
    ]
    if len(matching_meta) != 1:
        raise CaptureContractError(
            "Telegram capture requires exactly one matching sources_meta row; "
            f"matched={len(matching_meta)} username={username!r}"
        )
    payload = {
        "schema_version": raw.get("schema_version"),
        "run_id": raw.get("run_id"),
        "generated_at": raw.get("generated_at"),
        "sources_meta": [_jsonable(matching_meta[0])],
        "messages": [_jsonable(message)],
        "stats": {
            "sources_total": 1,
            "messages_scanned": 1,
            "messages_with_events": 1,
            "events_extracted": len(events),
        },
    }
    source_url = str(message.get("source_link") or "").strip()
    if not source_url:
        source_url = f"https://t.me/{username}/{message_id}"
    return payload, source_url


def _read_vk_binary(raw: Mapping[str, Any], *, base_dir: Path | None) -> tuple[bytes | None, str, int | None, bool]:
    supplied = [key for key in ("data_base64", "data_path", "data_sha256") if _is_nonempty(raw.get(key))]
    if len(supplied) > 1:
        raise CaptureContractError(f"VK poster supplies ambiguous binary representations: {supplied}")
    if supplied == ["data_base64"]:
        try:
            data = base64.b64decode(str(raw["data_base64"]), validate=True)
        except Exception as exc:
            raise CaptureContractError("VK poster data_base64 is invalid") from exc
        return data, hashlib.sha256(data).hexdigest(), len(data), True
    if supplied == ["data_path"]:
        path = Path(str(raw["data_path"])).expanduser()
        if not path.is_absolute():
            if base_dir is None:
                raise CaptureContractError("relative VK poster data_path requires input base directory")
            path = base_dir / path
        if not path.is_file():
            raise CaptureContractError(f"VK poster data_path does not exist: {path}")
        data = path.read_bytes()
        return data, hashlib.sha256(data).hexdigest(), len(data), True
    if supplied == ["data_sha256"]:
        digest = str(raw["data_sha256"])
        if not _SHA256_RE.fullmatch(digest):
            raise CaptureContractError("VK poster data_sha256 must be lowercase SHA-256")
        byte_count = raw.get("data_byte_count")
        if byte_count is not None and (isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0):
            raise CaptureContractError("VK poster data_byte_count must be a non-negative integer or null")
        return None, digest, byte_count, False
    digest = str(raw.get("digest") or "")
    if not _SHA256_RE.fullmatch(digest):
        raise CaptureContractError(
            "VK PosterMedia requires data_base64, data_path, data_sha256, or a SHA-256 digest"
        )
    return None, digest, None, False


def _serialize_vk_poster(
    poster: Any, *, index: int, base_dir: Path | None
) -> tuple[dict[str, Any], dict[str, Any]]:
    if dataclasses.is_dataclass(poster):
        raw = {field.name: getattr(poster, field.name) for field in dataclasses.fields(poster)}
        data = raw.pop("data", b"")
        if not isinstance(data, bytes):
            raise CaptureContractError(f"VK poster #{index} data must be bytes")
        if data:
            raw["data_sha256"] = hashlib.sha256(data).hexdigest()
            raw["data_byte_count"] = len(data)
            available = True
        else:
            raw["data_sha256"] = raw.get("digest")
            raw["data_byte_count"] = None
            available = False
    elif isinstance(poster, Mapping):
        raw = dict(poster)
        if "data" in raw:
            raise CaptureContractError(
                f"VK poster #{index} raw data is forbidden in JSON; use data_base64 or data_path"
            )
        _data, digest, byte_count, available = _read_vk_binary(raw, base_dir=base_dir)
        raw["data_sha256"] = digest
        raw["data_byte_count"] = byte_count
    else:
        raise CaptureContractError(f"VK poster #{index} must be PosterMedia or an object")

    allowed = {
        "name",
        "catbox_url",
        "supabase_url",
        "digest",
        "phash",
        "ocr_text",
        "ocr_title",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "data_base64",
        "data_path",
        "data_sha256",
        "data_byte_count",
    }
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise CaptureContractError(f"VK poster #{index} has unsupported fields: {unknown}")
    digest = str(raw.get("data_sha256") or "")
    if not _SHA256_RE.fullmatch(digest):
        raise CaptureContractError(f"VK poster #{index} has invalid binary SHA-256")
    declared_digest = str(raw.get("digest") or "")
    if declared_digest and declared_digest != digest:
        raise CaptureContractError(
            f"VK poster #{index} digest does not match binary SHA-256"
        )
    metadata = {
        key: _jsonable(raw.get(key))
        for key in (
            "name",
            "catbox_url",
            "supabase_url",
            "digest",
            "phash",
            "ocr_text",
            "ocr_title",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
        )
    }
    metadata.update(
        {
            "data_omitted": True,
            "data_sha256": digest,
            "data_byte_count": raw.get("data_byte_count"),
            "binary_was_available": bool(available),
        }
    )
    omission = {
        "json_pointer": f"/payload/draft/poster_media/{index}/data",
        "sha256": digest,
        "byte_count": raw.get("data_byte_count"),
        "was_available": bool(available),
    }
    return metadata, omission


def _vk_payload(raw: Mapping[str, Any], *, base_dir: Path | None) -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
    source_url = str(raw.get("source_post_url") or "").strip()
    if not source_url:
        raise CaptureContractError("VK source_post_url is required")
    draft_value = raw.get("draft")
    if dataclasses.is_dataclass(draft_value):
        draft = {field.name: getattr(draft_value, field.name) for field in dataclasses.fields(draft_value)}
    elif isinstance(draft_value, Mapping):
        draft = dict(draft_value)
    else:
        raise CaptureContractError("VK draft must be EventDraft or an object")
    posters = draft.pop("poster_media", [])
    if not isinstance(posters, Sequence) or isinstance(posters, (str, bytes, bytearray)):
        raise CaptureContractError("VK draft.poster_media must be an array")
    serialized_posters: list[dict[str, Any]] = []
    omissions: list[dict[str, Any]] = []
    for index, poster in enumerate(posters):
        serialized, omission = _serialize_vk_poster(poster, index=index, base_dir=base_dir)
        serialized_posters.append(serialized)
        omissions.append(omission)
    draft["poster_media"] = serialized_posters
    photos = raw.get("photos") or []
    if not isinstance(photos, list) or any(not isinstance(item, str) for item in photos):
        raise CaptureContractError("VK photos must be an array of URL strings")
    return {
        "source_post_url": source_url,
        "draft": _jsonable(draft),
        "photos": list(photos),
    }, source_url, omissions


def _parser_payload(raw: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    source = str(raw.get("source") or "").strip()
    if not source:
        raise CaptureContractError("parser source is required")
    event_value = raw.get("event")
    if dataclasses.is_dataclass(event_value):
        event = {field.name: _jsonable(getattr(event_value, field.name)) for field in dataclasses.fields(event_value)}
    elif isinstance(event_value, Mapping):
        event = _jsonable(event_value)
    else:
        raise CaptureContractError("parser event must be TheatreEvent or an object")
    source_url = str(event.get("url") or "").strip()
    if not source_url:
        raise CaptureContractError("parser TheatreEvent.url is required")
    return {"source": source, "event": event}, source_url


def _captured_at(value: str | None = None) -> str:
    if value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise CaptureContractError("captured_at must include a UTC offset")
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _repo_sha(value: str | None = None) -> str:
    sha = str(value or "").strip().lower()
    if not sha:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
    if not _REPO_SHA_RE.fullmatch(sha):
        raise CaptureContractError("repo_sha must be a 40-character lowercase git SHA")
    return sha


def build_capture(
    *,
    adapter: str,
    raw: Mapping[str, Any],
    repo_sha: str | None = None,
    captured_at: str | None = None,
    source_username: str | None = None,
    message_id: int | None = None,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    """Build one closed capture artifact without any I/O side effects."""

    adapter = str(adapter).strip().casefold()
    omissions: list[dict[str, Any]] = []
    if adapter == "telegram":
        if not source_username or message_id is None:
            raise CaptureContractError("Telegram source_username and message_id are required")
        selected_id = _positive_int(message_id, label="Telegram message_id")
        payload, source_url = _telegram_payload(
            raw,
            source_username=source_username,
            message_id=selected_id,
        )
        source_type = "telegram"
    elif adapter == "vk":
        payload, source_url, omissions = _vk_payload(raw, base_dir=base_dir)
        source_type = "vk"
    elif adapter == "parser":
        payload, source_url = _parser_payload(raw)
        source_type = f"parser:{payload['source']}"
    else:
        raise CaptureContractError(f"unsupported adapter: {adapter!r}")
    _assert_no_credentials(payload)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "adapter": adapter,
        "production_handler": HANDLERS[adapter],
        "repo_sha": _repo_sha(repo_sha),
        "captured_at": _captured_at(captured_at),
        "source_url": source_url,
        "source_type": source_type,
        "canonical_serialization": CANONICAL_SERIALIZATION,
        "payload_sha256": canonical_sha256(payload),
        "packet_count": 1,
        "payload": payload,
        "sanitization": {
            "credentials_policy": "fail-closed-no-credential-fields",
            "binary_omissions": omissions,
        },
    }
    validate_capture(artifact)
    return artifact


def validate_capture(value: Mapping[str, Any]) -> None:
    import jsonschema

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(
        schema,
        format_checker=jsonschema.FormatChecker(),
    ).validate(dict(value))
    adapter = str(value.get("adapter") or "")
    if value.get("production_handler") != HANDLERS.get(adapter):
        raise CaptureContractError("production_handler does not match adapter")
    payload = value.get("payload")
    if value.get("payload_sha256") != canonical_sha256(payload):
        raise CaptureContractError("payload_sha256 does not match canonical sanitized payload")
    _assert_no_credentials(payload)
    if adapter == "telegram":
        telegram = dict(payload) if isinstance(payload, Mapping) else {}
        messages = telegram.get("messages")
        sources_meta = telegram.get("sources_meta")
        if not isinstance(messages, list) or len(messages) != 1 or not isinstance(messages[0], Mapping):
            raise CaptureContractError("Telegram capture must contain exactly one message object")
        message = messages[0]
        selected_id = _positive_int(message.get("message_id"), label="Telegram message_id")
        username = _normalize_username(message.get("source_username"))
        if not username:
            raise CaptureContractError("Telegram source_username is required")
        if not isinstance(sources_meta, list) or len(sources_meta) != 1 or not isinstance(sources_meta[0], Mapping):
            raise CaptureContractError("Telegram capture must contain exactly one sources_meta object")
        if _normalize_username(sources_meta[0].get("username")) != username:
            raise CaptureContractError("Telegram sources_meta does not match selected message")
        _assert_telegram_dependencies_closed(
            message,
            selected_message_id=selected_id,
            all_messages=messages,
        )
        expected_url = str(message.get("source_link") or f"https://t.me/{username}/{selected_id}").strip()
        if str(value.get("source_url")) != expected_url:
            raise CaptureContractError("Telegram source_url does not match selected message")
    elif adapter == "vk":
        vk_payload = dict(payload) if isinstance(payload, Mapping) else {}
        if str(value.get("source_url")) != str(vk_payload.get("source_post_url")):
            raise CaptureContractError("VK source_url does not match source_post_url")
        draft = vk_payload.get("draft")
        posters = draft.get("poster_media") if isinstance(draft, Mapping) else None
        omissions = (value.get("sanitization") or {}).get("binary_omissions")
        if not isinstance(posters, list) or not isinstance(omissions, list) or len(posters) != len(omissions):
            raise CaptureContractError("VK poster_media and binary_omissions must correspond one-to-one")
        for index, (poster, omission) in enumerate(zip(posters, omissions)):
            if not isinstance(poster, Mapping) or not isinstance(omission, Mapping):
                raise CaptureContractError(f"VK poster omission #{index} is invalid")
            if (
                omission.get("json_pointer") != f"/payload/draft/poster_media/{index}/data"
                or omission.get("sha256") != poster.get("data_sha256")
                or omission.get("byte_count") != poster.get("data_byte_count")
                or omission.get("was_available") != poster.get("binary_was_available")
            ):
                raise CaptureContractError(f"VK poster omission #{index} does not match metadata")
    elif adapter == "parser":
        parser_payload = dict(payload) if isinstance(payload, Mapping) else {}
        source = str(parser_payload.get("source") or "")
        event = parser_payload.get("event")
        if not isinstance(event, Mapping):
            raise CaptureContractError("parser event must be an object")
        if str(value.get("source_type")) != f"parser:{source}":
            raise CaptureContractError("parser source_type does not match source")
        if str(value.get("source_url")) != str(event.get("url") or ""):
            raise CaptureContractError("parser source_url does not match TheatreEvent.url")


def load_capture(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise CaptureContractError("capture artifact must be a JSON object")
    artifact = dict(value)
    validate_capture(artifact)
    return artifact


def write_capture(path: Path, artifact: Mapping[str, Any]) -> None:
    validate_capture(artifact)
    resolved = path.expanduser().resolve()
    data_root = Path("/data")
    if resolved == data_root or data_root in resolved.parents:
        raise CaptureContractError("capture output under /data is forbidden")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    try:
        with resolved.open("x", encoding="utf-8") as stream:
            json.dump(artifact, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
    except FileExistsError as exc:
        raise CaptureContractError(f"refusing to overwrite existing capture: {resolved}") from exc


def _load_input(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise CaptureContractError("input JSON must be an object")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("adapter", choices=sorted(HANDLERS))
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repo-sha")
    parser.add_argument("--source-username", help="required for Telegram")
    parser.add_argument("--message-id", type=int, help="required for Telegram")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        raw = _load_input(args.input)
        artifact = build_capture(
            adapter=args.adapter,
            raw=raw,
            repo_sha=args.repo_sha,
            source_username=args.source_username,
            message_id=args.message_id,
            base_dir=args.input.resolve().parent,
        )
        write_capture(args.output, artifact)
    except (CaptureContractError, json.JSONDecodeError, OSError, subprocess.CalledProcessError) as exc:
        print(f"capture refused: {exc}")
        return 2
    print(
        json.dumps(
            {
                "status": "captured",
                "adapter": artifact["adapter"],
                "output": str(args.output.resolve()),
                "payload_sha256": artifact["payload_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
