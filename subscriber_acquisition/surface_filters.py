from __future__ import annotations

import re
from urllib.parse import urlparse


VK_WALL_RE = re.compile(r"(?i)^wall(?P<owner_id>-?\d+)_\d+$")
VK_NON_COMMUNITY_PREFIXES = ("wall", "photo", "video", "topic", "im", "album", "app", "market", "away.php", "id")
VK_NON_DISCOVERY_PREFIXES = ("wall", "photo", "video", "topic", "im", "album", "app", "market", "away.php")
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


def vk_handle_from_surface(*, url: str | None = None, handle: str | None = None, external_id: str | None = None) -> str:
    if handle:
        raw = str(handle).strip().strip("/")
    else:
        ext = str(external_id or "").strip()
        if ext.lower().startswith("vk:"):
            raw = ext.split(":", 1)[1].strip().strip("/")
        else:
            value = str(url or "").strip()
            if not value:
                return ""
            parsed = urlparse(value if "://" in value else f"https://{value}")
            if parsed.netloc.lower() not in {"vk.com", "vk.ru", "m.vk.com"}:
                return ""
            raw = parsed.path.strip("/").split("/", 1)[0]
    raw = raw.rstrip(".,)")
    wall = VK_WALL_RE.match(raw)
    if wall:
        owner_id = int(wall.group("owner_id"))
        return f"club{abs(owner_id)}" if owner_id < 0 else f"id{owner_id}"
    return raw


def is_vk_community_surface(*, url: str | None = None, handle: str | None = None, external_id: str | None = None) -> bool:
    vk_handle = vk_handle_from_surface(url=url, handle=handle, external_id=external_id).lower()
    return bool(vk_handle and not vk_handle.startswith(VK_NON_COMMUNITY_PREFIXES))


def is_vk_profile_surface(*, url: str | None = None, handle: str | None = None, external_id: str | None = None) -> bool:
    vk_handle = vk_handle_from_surface(url=url, handle=handle, external_id=external_id).lower()
    return bool(re.match(r"^id\d+$", vk_handle))


def is_vk_discovery_surface(*, url: str | None = None, handle: str | None = None, external_id: str | None = None) -> bool:
    """VK surface that acquisition discovery may scan read-only.

    Communities are the default. Explicit personal profiles (`id123` or
    positive-owner `wall123_...`) are allowed only as profile-wall candidates;
    vanity personal names are intentionally not auto-classified as profiles.
    """
    vk_handle = vk_handle_from_surface(url=url, handle=handle, external_id=external_id).lower()
    return bool(vk_handle and not vk_handle.startswith(VK_NON_DISCOVERY_PREFIXES))
