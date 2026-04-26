from __future__ import annotations

import base64
import json
import hashlib
import hmac
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet


WEBHOOK_ALLOWED_UPDATES: tuple[str, ...] = (
    "message",
    "callback_query",
    "my_chat_member",
    "channel_post",
    "edited_channel_post",
    "business_connection",
    "business_message",
    "edited_business_message",
)


def secure_short_hash(value: object, *, length: int = 12) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    salt = (
        os.getenv("TELEGRAM_BUSINESS_HASH_SALT")
        or os.getenv("TELEGRAM_BOT_TOKEN")
        or "events-bot-new"
    )
    digest = hmac.new(salt.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()[:length]


def business_connection_store_path() -> Path:
    raw = (os.getenv("TELEGRAM_BUSINESS_CONNECTIONS_FILE") or "").strip()
    if raw:
        return Path(raw)
    data_dir = Path(os.getenv("DATA_DIR") or "/data")
    if data_dir.exists():
        return data_dir / "telegram_business_connections.enc.json"
    return Path("artifacts/run/telegram_business_connections.enc.json")


def _fernet_from_env() -> Fernet:
    raw_key = (os.getenv("TELEGRAM_BUSINESS_FERNET_KEY") or "").strip()
    if raw_key:
        return Fernet(raw_key.encode("utf-8"))
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is required to encrypt Telegram Business connection cache"
        )
    key = base64.urlsafe_b64encode(hashlib.sha256(token.encode("utf-8")).digest())
    return Fernet(key)


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _visible_bool(obj: Any, name: str) -> bool:
    return bool(_field(obj, name, False))


def business_connection_summary(connection: Any) -> dict[str, Any]:
    user = _field(connection, "user")
    rights = _field(connection, "rights")
    connection_id = str(_field(connection, "id") or "").strip()
    user_id = _field(user, "id")
    username = str(_field(user, "username") or "").strip().lstrip("@")
    return {
        "connection_hash": secure_short_hash(connection_id),
        "user_hash": secure_short_hash(user_id),
        "username_hash": secure_short_hash(username.casefold()) if username else "",
        "is_enabled": bool(_field(connection, "is_enabled", False)),
        "can_manage_stories": _visible_bool(rights, "can_manage_stories"),
    }


def _load_store(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "connections": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": 1, "connections": []}
    if not isinstance(payload, dict):
        return {"version": 1, "connections": []}
    if not isinstance(payload.get("connections"), list):
        payload["connections"] = []
    payload.setdefault("version", 1)
    return payload


def _write_store(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        tmp_name = handle.name
    os.replace(tmp_name, path)


def cache_business_connection(connection: Any, *, path: Path | None = None) -> dict[str, Any]:
    summary = business_connection_summary(connection)
    connection_id = str(_field(connection, "id") or "").strip()
    if not connection_id:
        raise RuntimeError("business connection update has no id")

    user = _field(connection, "user")
    rights = _field(connection, "rights")
    encrypted_payload = {
        "connection_id": connection_id,
        "user_id": _field(user, "id"),
        "username": str(_field(user, "username") or "").strip().lstrip("@") or None,
        "user_chat_id": _field(connection, "user_chat_id"),
        "date": _field(connection, "date"),
        "is_enabled": summary["is_enabled"],
        "can_manage_stories": summary["can_manage_stories"],
        "rights": {
            "can_manage_stories": _visible_bool(rights, "can_manage_stories"),
            "can_reply": _visible_bool(connection, "can_reply"),
        },
    }

    fernet = _fernet_from_env()
    encrypted = fernet.encrypt(
        json.dumps(encrypted_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).decode("ascii")

    target = path or business_connection_store_path()
    store = _load_store(target)
    now = datetime.now(timezone.utc).isoformat()
    record = {
        **summary,
        "encrypted_payload": encrypted,
        "updated_at": now,
    }

    records = [
        item
        for item in store["connections"]
        if not isinstance(item, dict)
        or item.get("connection_hash") != summary["connection_hash"]
    ]
    records.append(record)
    store["connections"] = records
    _write_store(target, store)
    return {**summary, "path": str(target)}


def load_cached_business_connections(*, path: Path | None = None) -> list[dict[str, Any]]:
    target = path or business_connection_store_path()
    store = _load_store(target)
    fernet = _fernet_from_env()
    connections: list[dict[str, Any]] = []
    for item in store.get("connections", []):
        if not isinstance(item, dict):
            continue
        encrypted = str(item.get("encrypted_payload") or "").strip()
        if not encrypted:
            continue
        decrypted = json.loads(fernet.decrypt(encrypted.encode("ascii")).decode("utf-8"))
        if not isinstance(decrypted, dict):
            continue
        username = str(decrypted.get("username") or "").strip().lstrip("@")
        connections.append(
            {
                "connection_hash": item.get("connection_hash"),
                "user_hash": item.get("user_hash"),
                "username_hash": item.get("username_hash")
                or (secure_short_hash(username.casefold()) if username else ""),
                "updated_at": item.get("updated_at"),
                **decrypted,
            }
        )
    return connections


def _parse_business_target_selector(raw: str | None) -> list[str] | None:
    value = str(raw or "").strip()
    if not value:
        return []
    if value.casefold() == "all":
        return None
    if value.startswith("["):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS must be JSON "
                "or comma-separated hashes"
            ) from exc
        if not isinstance(payload, list):
            raise RuntimeError(
                "VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS JSON value must be a list"
            )
        return [str(item or "").strip() for item in payload if str(item or "").strip()]
    return [item.strip() for item in value.split(",") if item.strip()]


def _selector_match(item: dict[str, Any], allowed: set[str]) -> bool:
    username = str(item.get("username") or "").strip().lstrip("@").casefold()
    candidates = {
        str(item.get("connection_hash") or "").strip(),
        str(item.get("user_hash") or "").strip(),
        str(item.get("username_hash") or "").strip(),
    }
    if username:
        candidates.add(username)
        candidates.add(f"@{username}")
    return bool(candidates.intersection(allowed))


def load_business_story_targets(
    *,
    path: Path | None = None,
    selector_raw: str | None = None,
) -> list[dict[str, Any]]:
    """Return decrypted, story-capable Business connections selected for autopublish.

    `VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS=all` selects every cached enabled
    connection with story rights. Otherwise the value is a comma-separated or JSON
    list of connection/user/username hashes.
    """
    selector_source = (
        os.getenv("VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS")
        if selector_raw is None
        else selector_raw
    )
    selector = _parse_business_target_selector(selector_source)
    if selector == []:
        return []
    allowed = {item.strip().casefold() for item in (selector or []) if item.strip()}
    targets: list[dict[str, Any]] = []
    for item in load_cached_business_connections(path=path):
        if not bool(item.get("is_enabled")) or not bool(item.get("can_manage_stories")):
            continue
        if allowed and not _selector_match(item, allowed):
            continue
        connection_hash = str(item.get("connection_hash") or "").strip()
        if not connection_hash:
            continue
        targets.append(
            {
                "connection_hash": connection_hash,
                "user_hash": str(item.get("user_hash") or "").strip(),
                "username_hash": str(item.get("username_hash") or "").strip(),
                "connection_id": str(item.get("connection_id") or "").strip(),
                "user_chat_id": item.get("user_chat_id"),
                "updated_at": item.get("updated_at"),
                "is_enabled": bool(item.get("is_enabled")),
                "can_manage_stories": bool(item.get("can_manage_stories")),
            }
        )
    return targets
