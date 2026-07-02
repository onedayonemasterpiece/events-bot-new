#!/usr/bin/env python3
"""Read Telegram post links through the local Telethon human session.

This helper intentionally prints only message evidence, never session strings,
API hashes, or tokens.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from telethon import TelegramClient
from telethon.sessions import StringSession


@dataclass(slots=True)
class TelethonConfig:
    api_id: int
    api_hash: str
    session_string: str
    device_model: str | None = None
    system_version: str | None = None
    app_version: str | None = None
    lang_code: str | None = None
    system_lang_code: str | None = None


def _load_dotenv_if_present(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = value[1:-1]
        os.environ[key] = value


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _urlsafe_b64decode_text(value: str) -> str:
    padded = value + ("=" * (-len(value) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


def _load_config() -> TelethonConfig:
    _load_dotenv_if_present()
    api_id_raw = _env("TELEGRAM_API_ID") or _env("TG_API_ID")
    api_hash = _env("TELEGRAM_API_HASH") or _env("TG_API_HASH")
    if not api_id_raw or not api_hash:
        raise SystemExit("missing TELEGRAM_API_ID/TELEGRAM_API_HASH or TG_API_ID/TG_API_HASH")

    bundle_b64 = _env("TELEGRAM_AUTH_BUNDLE_E2E")
    if bundle_b64:
        bundle = json.loads(_urlsafe_b64decode_text(bundle_b64))
        session_string = str(bundle.get("session") or "").strip()
        if not session_string:
            raise SystemExit("invalid TELEGRAM_AUTH_BUNDLE_E2E: missing session")
        return TelethonConfig(
            api_id=int(api_id_raw),
            api_hash=api_hash,
            session_string=session_string,
            device_model=(bundle.get("device_model") or None),
            system_version=(bundle.get("system_version") or None),
            app_version=(bundle.get("app_version") or None),
            lang_code=(bundle.get("lang_code") or None),
            system_lang_code=(bundle.get("system_lang_code") or None),
        )

    session_string = _env("TELEGRAM_SESSION")
    if not session_string:
        raise SystemExit("missing TELEGRAM_AUTH_BUNDLE_E2E or TELEGRAM_SESSION")
    return TelethonConfig(
        api_id=int(api_id_raw),
        api_hash=api_hash,
        session_string=session_string,
    )


def _parse_tme_link(link: str) -> tuple[str | int, int]:
    raw = link.strip()
    if not re.match(r"^[a-z][a-z0-9+.-]*://", raw, flags=re.I):
        raw = "https://" + raw.lstrip("/")
    parsed = urlparse(raw)
    host = parsed.netloc.lower()
    if host not in {"t.me", "telegram.me"}:
        raise ValueError(f"not a Telegram t.me link: {link}")
    parts = [part for part in parsed.path.split("/") if part]
    if parts and parts[0] == "s":
        parts = parts[1:]
    if len(parts) >= 3 and parts[0] == "c":
        internal_id = parts[1]
        msg_id = int(parts[2])
        return int(f"-100{internal_id}"), msg_id
    if len(parts) < 2:
        raise ValueError(f"expected /<chat>/<message_id> link: {link}")
    return parts[0].lstrip("@"), int(parts[1])


def _message_to_dict(link: str, chat: Any, message: Any) -> dict[str, Any]:
    chat_id = getattr(chat, "id", None)
    date = getattr(message, "date", None)
    media = getattr(message, "media", None)
    fwd = getattr(message, "fwd_from", None)
    return {
        "input": link,
        "chat": {
            "id": chat_id,
            "title": getattr(chat, "title", None),
            "username": getattr(chat, "username", None),
        },
        "message": {
            "id": getattr(message, "id", None),
            "date": date.isoformat() if date else None,
            "text": getattr(message, "message", None) or "",
            "grouped_id": str(getattr(message, "grouped_id", None) or "") or None,
            "media_type": type(media).__name__ if media is not None else None,
            "views": getattr(message, "views", None),
            "forwards": getattr(message, "forwards", None),
            "edit_date": (
                message.edit_date.isoformat()
                if getattr(message, "edit_date", None)
                else None
            ),
        },
        "forward": {
            "from_name": getattr(fwd, "from_name", None) if fwd else None,
            "channel_post": getattr(fwd, "channel_post", None) if fwd else None,
            "date": fwd.date.isoformat() if fwd and getattr(fwd, "date", None) else None,
        }
        if fwd
        else None,
    }


async def _read_links(links: list[str]) -> list[dict[str, Any]]:
    cfg = _load_config()
    kwargs: dict[str, object] = {}
    for key in ("device_model", "system_version", "app_version", "lang_code", "system_lang_code"):
        value = getattr(cfg, key)
        if value:
            kwargs[key] = value
    async with TelegramClient(
        StringSession(cfg.session_string),
        cfg.api_id,
        cfg.api_hash,
        **kwargs,
    ) as client:
        out: list[dict[str, Any]] = []
        me = await client.get_me()
        for link in links:
            try:
                peer, msg_id = _parse_tme_link(link)
                chat = await client.get_entity(peer)
                message = await client.get_messages(chat, ids=msg_id)
                if not message:
                    out.append({"input": link, "error": "message_not_found"})
                    continue
                item = _message_to_dict(link, chat, message)
                item["inspector"] = {
                    "user_id": getattr(me, "id", None),
                    "username": getattr(me, "username", None),
                }
                out.append(item)
            except Exception as exc:
                out.append({"input": link, "error": f"{type(exc).__name__}: {exc}"})
        return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("links", nargs="+", help="Telegram t.me post links")
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON")
    args = parser.parse_args()
    result = asyncio.run(_read_links(args.links))
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
