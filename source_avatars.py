"""VK source portraits shared by guide cards and event medallions.

Signed wall IDs distinguish personal pages from communities. Downloads and CDN
hosting happen at the async collection boundary; renderers only read local files.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
import re
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite
from PIL import Image, ImageOps, ImageDraw, ImageChops

logger = logging.getLogger(__name__)
AVATAR_ROOT = Path(os.getenv("DB_PATH", "/data/db.sqlite")).parent / "source_avatars"


def vk_post_owners(urls: Any) -> list[int]:
    if isinstance(urls, str):
        urls = [urls]
    return list(dict.fromkeys(int(m[1]) for url in (urls or [])
        if (m := re.search(r"https?://(?:www\.)?vk\.(?:com|ru)/wall(-?\d+)_\d+", str(url)))))


def avatar_path(owner: int) -> Path | None:
    path = AVATAR_ROOT / f"{int(owner)}.webp"
    return path if path.is_file() else None


def avatar_metadata(owner: int) -> dict[str, Any]:
    path = AVATAR_ROOT / f"{int(owner)}.json"
    try:
        return json.loads(path.read_text()) if path.is_file() else {}
    except (OSError, ValueError):
        return {}


def round_avatar_image(image: Image.Image) -> Image.Image:
    image = ImageOps.fit(image.convert("RGBA"), (min(image.size),) * 2)
    mask = Image.new("L", image.size, 0)
    ImageDraw.Draw(mask).ellipse((0, 0, image.width - 1, image.height - 1), fill=255)
    image.putalpha(ImageChops.multiply(image.getchannel("A"), mask))
    return image


def _write(path: Path, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
        f.write(data)
        temporary = Path(f.name)
    temporary.replace(path)


async def ensure_vk_avatar(owner: int) -> Path | None:
    """Best effort: unavailable portraits must not stop source collection."""
    path = avatar_path(owner)
    if path and time.time() - path.stat().st_mtime < 86400:
        return path
    try:
        from main import get_http_session, vk_api
        from yandex_storage import upload_yandex_public_bytes

        method = "users.get" if owner > 0 else "groups.getById"
        params = {"user_ids" if owner > 0 else "group_ids": abs(owner), "fields": "photo_max_orig,photo_200"}
        result = await vk_api(method, **params)
        items = result if isinstance(result, list) else result.get("users" if owner > 0 else "groups", [])
        item = next(x for x in items if int(x["id"]) == abs(owner))
        url = item.get("photo_max_orig") or item.get("photo_200")
        if not url:
            logger.info("source_avatar unavailable owner=%s reason=no_photo", owner)
            return path
        async with get_http_session().get(url, timeout=20) as response:
            response.raise_for_status()
            data = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                data.extend(chunk)
                if len(data) > 5 * 1024 * 1024:
                    raise ValueError("Avatar exceeds 5 MiB")
        image = round_avatar_image(ImageOps.fit(Image.open(io.BytesIO(data)), (320, 320)))
        out = io.BytesIO()
        image.save(out, "WEBP", quality=90)
        encoded = out.getvalue()
        digest = hashlib.sha256(encoded).hexdigest()
        hosted = await asyncio.to_thread(upload_yandex_public_bytes, encoded,
            object_path=f"source-avatars/vk/{owner}/{digest}.webp", content_type="image/webp")
        name = item.get("name") or " ".join(str(item.get(k) or "") for k in ("first_name", "last_name")).strip()
        AVATAR_ROOT.mkdir(parents=True, exist_ok=True)
        target = AVATAR_ROOT / f"{owner}.webp"
        _write(target, encoded)
        meta = {"owner_id": owner, "name": name, "image_url": hosted, "source_url": f"https://vk.com/{'id' if owner > 0 else 'club'}{abs(owner)}"}
        _write(target.with_suffix(".json"), json.dumps(meta, ensure_ascii=False).encode())
        logger.info("source_avatar ready owner=%s cdn=%s", owner, bool(hosted))
        return target
    except Exception as exc:
        logger.warning("source_avatar failed owner=%s error_type=%s", owner, type(exc).__name__)
        return path


def _identity_names(profile: dict[str, Any]) -> set[str]:
    from guide_excursions.identity_policy import _candidate_anchor
    names = set()
    rollup = json.loads(profile.get("facts_rollup_json") or "{}")
    guide_name = _candidate_anchor(rollup.get("guide_line")) if profile.get("profile_kind") in {"person", "guide"} else ""
    for value in (profile.get("display_name"), profile.get("marketing_name"), guide_name):
        value = str(value or "").split(" — ")[0].strip()
        variants = [value, re.sub(r"\s*\([^)]*\)", "", value)]
        if match := re.fullmatch(r"\S+\s+\(([^)]+)\)(.*)", value):
            variants.append(match[1] + match[2])
        names.update(" ".join(x.casefold().replace("ё", "е").split()) for x in variants if x)
    return names


def _scheduled_guide_tags(row: dict[str, Any]) -> dict[str, set[int]]:
    """Personal VK tags explicitly credited as guides on the exact dated programme line."""
    from guide_excursions.parser import QUOTE_TITLE_RE, extract_date_iso, extract_time_text, normalize_title_key
    title = normalize_title_key(row.get("canonical_title")).replace("ё", "е")
    day = row.get("date")
    start = extract_time_text(str(row.get("time") or ""))
    slots: dict[str, set[int]] = {}
    if not title or not day or not start:
        return slots
    for post in row.get("linked_posts") or []:
        if not vk_post_owners(post.get("url") or post.get("source_url")):
            continue
        try:
            published = datetime.fromisoformat(str(post.get("post_date") or day))
        except ValueError:
            continue
        for line in str(post.get("text") or "").splitlines():
            if "[id" not in line or extract_time_text(line) != start or extract_date_iso(line, post_date=published) != day:
                continue
            titles = list(QUOTE_TITLE_RE.finditer(line))
            if len(titles) != 1 or normalize_title_key(titles[0][1]).replace("ё", "е") != title:
                continue
            credit = re.match(r"\s*[,;—–-]?\s*гид(?:ы)?\s*[:—–-]?\s*(\[id\d+\|[^\]]+\]"
                              r"(?:(?:\s*,\s*|\s+и\s+)\[id\d+\|[^\]]+\])*)", line[titles[0].end():], re.I)
            if not credit:
                continue
            for uid, label in re.findall(r"\[id([1-9]\d*)\|([^\]]+)\]", credit[1]):
                name = " ".join(label.split())
                if len(name.split()) >= 2:
                    slots.setdefault(name, set()).add(int(uid))
    return slots


def guide_participants(row: dict[str, Any], profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bind already extracted roles to explicit public profile identities, never surnames."""
    from guide_excursions.visual_digest import _AVATAR_DIR, _AVATAR_FILES
    result: dict[str, dict[str, Any]] = {}
    facts = row.get("fact_pack") or {}
    tags = _scheduled_guide_tags(row)
    tagged_owners: dict[str, set[int]] = {}
    for label, ids in tags.items():
        tagged_owners.setdefault(label.casefold().replace("ё", "е"), set()).update(ids)
    for field, role in (("guide_names", "Гид"), ("organizer_names", "Организатор")):
        # The latest extracted facts supersede older role metadata left by upsert.
        names = facts.get(field, row.get(field, []))
        if role == "Гид" and tags:
            names = list(dict.fromkeys([*(names if isinstance(names, list) else []), *tags]))
        for name in names if isinstance(names, list) else []:
            key = " ".join(str(name).casefold().replace("ё", "е").split())
            matches = [p for p in profiles if key in _identity_names(p)]
            aliases = set().union(*(_identity_names(p) for p in matches))
            matches = [p for p in profiles if aliases & _identity_names(p)]
            owners = set()
            paths = set()
            for profile in matches:
                rollup = json.loads(profile.get("facts_rollup_json") or "{}")
                if uid := rollup.get("vk_user_id"):
                    owners.add(int(uid))
                for url in json.loads(profile.get("source_links_json") or "[]"):
                    if not uid and (m := re.fullmatch(r"https?://(?:www\.)?vk\.(?:com|ru)/(id|club|public)(\d+)/?", url)):
                        owners.add(int(m[2]) * (1 if m[1] == "id" else -1))
                    if m := re.fullmatch(r"https?://t\.me/([\w]+)/?", url):
                        if filename := _AVATAR_FILES.get(m[1]):
                            paths.add(str(_AVATAR_DIR / filename))
            owner = next(iter(owners)) if len(owners) == 1 else None
            path = next(iter(paths)) if len(paths) == 1 else None
            conflicting_tags = False
            if role == "Гид" and key in tagged_owners:
                ids = tagged_owners[key]
                known_people = {uid for uid in owners if uid > 0}
                conflicting_tags = len(ids) != 1 or bool(known_people and ids.isdisjoint(known_people))
                owner = None if conflicting_tags else next(iter(ids))
                if owner not in owners:
                    path = None
            identity = f"vk:{owner}" if owner else key
            if identity in result:
                if role not in result[identity]["roles"]:
                    result[identity]["roles"].append(role)
                continue
            result[identity] = {"name": name, "roles": [role], "owner": owner, "path": path}
            if conflicting_tags:
                result[identity]["conflicting_guide_tags"] = True
    return list(result.values())


async def prepare_guide_avatars(rows: list[dict[str, Any]], db: Any = None) -> None:
    profiles = []
    if db is not None:
        async with db.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            profiles = [dict(p) for p in await (await conn.execute(
                "SELECT display_name,marketing_name,profile_kind,source_links_json,facts_rollup_json FROM guide_profile"
            )).fetchall()]
    for row in rows:
        participants = guide_participants(row, profiles)
        for person in participants:
            if person["owner"] and (path := await ensure_vk_avatar(person["owner"])):
                person["path"] = str(path)
        row["participant_medallions"] = participants
        if row.get("source_roles_checked") and not participants:
            continue
        if row.get("source_platform") != "vk":
            continue
        flags = json.loads(row.get("source_flags_json") or "{}")
        owner = int(flags.get("vk_id") or 0)
        if owner:
            owner = abs(owner) if flags.get("vk_personal_page") else -abs(owner)
        else:
            try:
                from main import vk_resolve_group
                uid, _, _, kind = await vk_resolve_group(row["source_username"])
                owner = uid if kind == "user" else -uid
            except Exception as exc:
                logger.warning("source_avatar resolve_failed source=%s error_type=%s", row.get("source_username"), type(exc).__name__)
                continue
        row["source_avatar_owner"] = owner
        if path := await ensure_vk_avatar(owner):
            row["source_avatar_path"] = str(path)
            public_name = avatar_metadata(owner).get("name") or ""
            source_names = _identity_names({"display_name": public_name})
            source_participant = None
            resolved = []
            for person in participants:
                if not person.get("owner") and not person.get("conflicting_guide_tags") and source_names & _identity_names({"display_name": person["name"]}):
                    person.update(owner=owner, path=str(path))
                if person["owner"] == owner:
                    if source_participant is not None:
                        for role in person["roles"]:
                            if role not in source_participant["roles"]:
                                source_participant["roles"].append(role)
                        if public_name:
                            source_participant["name"] = public_name
                        continue
                    source_participant = person
                resolved.append(person)
            if not resolved:
                resolved.append({"name": public_name or row.get("source_title"),
                    "roles": [], "owner": owner, "path": str(path)})
            row["participant_medallions"] = resolved


def source_medallions(urls: Any, *, index: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return [meta for owner in vk_post_owners(urls)
        if (meta := (index.get(str(owner), {}) if index is not None else avatar_metadata(owner))).get("image_url")]
