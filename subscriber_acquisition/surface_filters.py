from __future__ import annotations

import re
from urllib.parse import urlparse


TG_SERVICE_HANDLES = {"c", "s", "joinchat", "share", "addstickers", "addemoji", "iv", "boost"}
TG_HANDLE_RE = re.compile(r"(?i)(?:https?://)?(?:t\.me|telegram\.me)/(?P<handle>[A-Za-z0-9_]{4,})")


def tg_handle_from_surface(*, url: str | None = None, handle: str | None = None, external_id: str | None = None) -> str:
    if handle:
        return str(handle).strip().strip("/")
    ext = str(external_id or "").strip()
    if ext.lower().startswith("tg:"):
        return ext.split(":", 1)[1].strip().strip("/")
    value = str(url or "").strip()
    if not value:
        return ""
    match = TG_HANDLE_RE.search(value)
    if match:
        return match.group("handle").strip().strip("/")
    parsed = urlparse(value if "://" in value else f"https://{value}")
    if parsed.netloc.lower() in {"t.me", "telegram.me"}:
        return parsed.path.strip("/").split("/", 1)[0]
    return ""


def is_tg_bot_or_service_surface(*, url: str | None = None, handle: str | None = None, external_id: str | None = None) -> bool:
    tg_handle = tg_handle_from_surface(url=url, handle=handle, external_id=external_id).lower()
    return bool(tg_handle and (tg_handle in TG_SERVICE_HANDLES or tg_handle.endswith("bot")))
