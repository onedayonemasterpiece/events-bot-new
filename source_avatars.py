"""VK source portraits shared by guide cards and event medallions.

Signed wall IDs distinguish personal pages from communities. Downloads and CDN
hosting happen at the async collection boundary; renderers only read local files.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import re
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from weakref import WeakValueDictionary
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


_AVATAR_TTL = 86400
_MAX_IMAGE = 5 * 1024 * 1024
_VK_LOCKS = WeakValueDictionary()


def _portrait_bytes(data: bytes) -> bytes:
    if not data or len(data) > _MAX_IMAGE:
        raise ValueError("Unsupported portrait file size")
    with Image.open(io.BytesIO(data)) as image:
        if image.width * image.height > 16_000_000:
            raise ValueError("Unsupported portrait dimensions")
        portrait = round_avatar_image(ImageOps.fit(image, (320, 320)))
    output = io.BytesIO()
    portrait.save(output, "WEBP", quality=90)
    return output.getvalue()


def telegram_portrait(state: Any) -> Path | None:
    if not isinstance(state, dict) or not isinstance(state.get("peer_id"), int) or not state.get("sha256"):
        return None
    digest = state["sha256"]
    if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
        return None
    path = AVATAR_ROOT / "telegram" / str(state["peer_id"]) / f"{digest}.webp"
    return path if path.is_file() else None


async def import_telegram_portrait(conn: Any, source_id: int, observation: Any) -> None:
    """The source import owns the SQLite transaction. Its confirmed profile keeps one current file."""
    if not isinstance(observation, dict):
        return
    peer = observation.get("peer_id")
    photo = observation.get("photo_id")
    if type(peer) is not int or not peer or not photo:
        return
    row = await (await conn.execute("SELECT flags_json FROM guide_source WHERE id=? AND platform='telegram'", (source_id,))).fetchone()
    if not row:
        return
    previous = (json.loads(row[0] or "{}").get("avatar") or {})
    if previous.get("peer_id") not in (None, peer):
        logger.warning("guide_avatar peer_changed source_id=%d", source_id)
        return
    try:
        observed_at = datetime.fromisoformat(str(observation["observed_at"]))
        if observed_at.tzinfo is None:
            return
        observed = observed_at.astimezone(timezone.utc).isoformat()
    except (KeyError, TypeError, ValueError):
        return
    if observed < str(previous.get("observed_at") or ""):
        return
    current = telegram_portrait(previous)
    if str(photo) == previous.get("photo_id") and current:
        state = dict(previous, observed_at=observed)
    else:
        data = observation.get("image_base64")
        if not isinstance(data, str) or len(data) > _MAX_IMAGE * 4 // 3 + 4:
            return
        try:
            encoded = _portrait_bytes(base64.b64decode(data, validate=True))
        except (OSError, ValueError, Image.DecompressionBombError):
            logger.warning("guide_avatar invalid_telegram_photo source_id=%d", source_id)
            return
        digest = hashlib.sha256(encoded).hexdigest()
        path = AVATAR_ROOT / "telegram" / str(peer) / f"{digest}.webp"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.is_file():
            _write(path, encoded)
        state = {"peer_id": peer, "photo_id": str(photo), "observed_at": observed, "sha256": digest}
    await conn.execute("UPDATE guide_source SET flags_json=json_set(COALESCE(flags_json,'{}'), '$.avatar', json(?)) WHERE id=?",
                       (json.dumps(state), source_id))


async def ensure_vk_avatar(owner: int) -> Path | None:
    """Refresh at collection time and retain the last good picture on an upstream failure."""
    key = (asyncio.get_running_loop(), int(owner))
    lock = _VK_LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        return await _refresh_vk_avatar(int(owner))


async def _refresh_vk_avatar(owner: int) -> Path | None:
    path = avatar_path(owner)
    meta = avatar_metadata(owner)
    now = time.time()
    if path and now - float(meta.get("checked_at") or path.stat().st_mtime) < _AVATAR_TTL:
        return path
    try:
        from main import get_http_session, vk_api
        from yandex_storage import upload_yandex_public_bytes

        method = "users.get" if owner > 0 else "groups.getById"
        params = {"user_ids" if owner > 0 else "group_ids": abs(owner), "fields": "photo_max_orig,photo_200,crop_photo,screen_name"}
        result = await vk_api(method, **params)
        items = result if isinstance(result, list) else result.get("users" if owner > 0 else "groups", [])
        item = next(x for x in items if int(x["id"]) == abs(owner))
        url = item.get("photo_max_orig") or item.get("photo_200")
        if not url or re.search(r"/(?:images/(?:community|camera)|[\w.-]*camera_[\d])", url, re.I):
            return path
        crop = item.get("crop_photo") or {}
        # VK rotates signed URLs even when the underlying photo and crop stay the same.
        photo_id = (crop.get("photo") or {}).get("id")
        marker = json.dumps([photo_id, crop.get("crop"), crop.get("rect")], sort_keys=True) if photo_id else None
        name = item.get("name") or " ".join(str(item.get(k) or "") for k in ("first_name", "last_name")).strip()
        info = dict(meta, owner_id=owner, name=name, screen_name=item.get("screen_name"), checked_at=now,
                    source_url=f"https://vk.com/{'id' if owner > 0 else 'club'}{abs(owner)}", photo_marker=marker)
        if not (path and marker and marker == meta.get("photo_marker")):
            async with get_http_session().get(url, timeout=20) as response:
                response.raise_for_status()
                data = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    data.extend(chunk)
                    if len(data) > _MAX_IMAGE:
                        raise ValueError("Avatar exceeds 5 MiB")
            encoded = _portrait_bytes(bytes(data))
            digest = hashlib.sha256(encoded).hexdigest()
            AVATAR_ROOT.mkdir(parents=True, exist_ok=True)
            target = AVATAR_ROOT / f"{owner}.webp"
            if not path or meta.get("sha256") != digest:
                _write(target, encoded)
            path = target
            info["sha256"] = digest
        if path and (info.get("sha256") != meta.get("hosted_sha256") or not meta.get("image_url")):
            try:
                hosted = await asyncio.to_thread(upload_yandex_public_bytes, path.read_bytes(),
                    object_path=f"source-avatars/vk/{owner}/current.webp", content_type="image/webp",
                    cache_control="public, max-age=86400")
                if hosted:
                    info.update(image_url=hosted, hosted_sha256=info.get("sha256"))
            except Exception as exc:
                logger.warning("source_avatar hosting_failed owner=%s error_type=%s", owner, type(exc).__name__)
        _write(path.with_suffix(".json"), json.dumps(info, ensure_ascii=False).encode())
        return path
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
        day_tags: dict[str, set[int]] = {}
        plain_credits = set()
        for line in str(post.get("text") or "").splitlines():
            clock = extract_time_text(line)
            if not clock or extract_date_iso(line, post_date=published) != day:
                continue
            titles = list(QUOTE_TITLE_RE.finditer(line))
            if len(titles) != 1:
                continue
            matches_slot = clock == start and normalize_title_key(titles[0][1]).replace("ё", "е") == title
            credit = re.match(r"\s*[,;—–-]?\s*гид(?:ы)?\b\s*[:—–-]?\s*(\S.*)", line[titles[0].end():], re.I)
            if not credit:
                continue
            names = credit[1].strip().rstrip(".!")
            if matches_slot and re.fullmatch(r"[^\W\d_]+(?:[- ][^\W\d_]+)+", names):
                plain_credits.add(names)
            tag_list = re.match(r"\[id\d+\|[^\]]+\](?:(?:\s*,\s*|\s+и\s+)\[id\d+\|[^\]]+\])*", names, re.I)
            for uid, label in re.findall(r"\[id([1-9]\d*)\|([^\]]+)\]", tag_list[0] if tag_list else ""):
                name = " ".join(label.split())
                if len(name.split()) >= 2:
                    day_tags.setdefault(name.casefold().replace("ё", "е"), set()).add(int(uid))
                    if matches_slot:
                        slots.setdefault(name, set()).add(int(uid))
        for name in plain_credits:
            # The same announcement explicitly credits this person again, without repeating the VK link.
            if ids := day_tags.get(name.casefold().replace("ё", "е")):
                slots.setdefault(name, set()).update(ids)
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
            # A venue, project or channel can organize a tour led by a different person.
            if role == "Гид":
                matches = [p for p in matches if p.get("profile_kind") not in _ORG_PROFILE_KINDS]
            owners = set()
            telegram = {}
            paths = set()
            for profile in matches:
                rollup = json.loads(profile.get("facts_rollup_json") or "{}")
                for state in profile.get("_telegram_portraits", []):
                    if path := telegram_portrait(state):
                        telegram[state["peer_id"]] = str(path)
                if uid := rollup.get("vk_user_id"):
                    owners.add(int(uid))
                for url in json.loads(profile.get("source_links_json") or "[]"):
                    if not uid and (m := re.fullmatch(r"https?://(?:www\.)?vk\.(?:com|ru)/(id|club|public)(\d+)/?", url)):
                        if m[1] == "id" or role != "Гид":
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
            tg_peer = None
            same_telegram = len(matches) == 1 or all(
                any(state.get("peer_id") in telegram for state in p.get("_telegram_portraits", [])) for p in matches
            )
            source_peer = None
            if row.get("source_platform") == "telegram":
                source_peer = (json.loads(row.get("source_flags_json") or "{}").get("avatar") or {}).get("peer_id")
            if (telegram and same_telegram and len(owners) <= 1 and not conflicting_tags
                    and not (key in tagged_owners and owner not in owners)):
                if source_peer in telegram:
                    tg_peer, path = source_peer, telegram[source_peer]
                elif len(telegram) == 1:
                    tg_peer, path = next(iter(telegram.items()))
            identity = f"vk:{owner}" if owner else f"tg:{tg_peer}" if tg_peer else key
            if identity in result:
                if role not in result[identity]["roles"]:
                    result[identity]["roles"].append(role)
                continue
            result[identity] = {"name": name, "roles": [role], "owner": owner, "path": path}
            if tg_peer is not None:
                result[identity].update(avatar_platform="telegram", avatar_peer_id=tg_peer)
            if conflicting_tags:
                result[identity]["conflicting_guide_tags"] = True
            if len(owners) > 1 and not owner:
                result[identity]["conflicting_profile_owners"] = True
    return list(result.values())


def _organizer_in_title(name: str, title: str) -> bool:
    """Match the full brand at a title boundary or inside a pair of quotes."""
    def normalized(value: str) -> str:
        return " ".join(value.casefold().replace("ё", "е").replace("‑", "-").split())

    brand, title = normalized(name), normalized(title)
    segments = re.split(r"\s*[|•·]\s*|\s+[–—-]\s+|[,:!]", title)
    quoted = re.findall(r'[«“"]([^»”"]+)[»”"]', title)
    return bool(brand and any(brand == part.strip() for part in [*segments, *quoted]))


_VK_COMMUNITY_LINK = r"https?://(?:www\.)?vk\.(?:com|ru)/([a-z0-9_][a-z0-9_.]*[a-z0-9_]|[a-z0-9_])/?"
_ORG_PROFILE_KINDS = {"operator", "organization", "project"}


def _declared_vk_community(about: str) -> str | None:
    # A source's redirect to a venue, a partner, or the booking contact is not its own page.
    if len(re.findall(r"https?://(?:www\.)?vk\.(?:com|ru)/", about, re.I)) != 1:
        return None
    label = r"(?:мы\s+|наш(?:а|е)\s+(?:группа|сообщество)\s+)(?:в\s*)?(?:вконтакте|контакте|вк|vk)"
    match = re.search(r"\b" + label + r"\s*[:—–-]?\s*" + _VK_COMMUNITY_LINK + r"(?=\s|$)", about, re.I)
    return match[1].casefold() if match else None


def _registered_organizer_groups(profiles, sources) -> dict[str, set[str]]:
    groups: dict[str, set[str]] = {}
    for profile in profiles:
        if profile.get("profile_kind") not in _ORG_PROFILE_KINDS:
            continue
        handles = {m[1].casefold() for url in json.loads(profile.get("source_links_json") or "[]")
                   if (m := re.fullmatch(_VK_COMMUNITY_LINK, url, re.I))}
        names = _identity_names(profile)
        for source in sources.get(profile["id"], []):
            if any(_organizer_in_title(name, source["title"] or "") for name in names):
                if handle := _declared_vk_community(source["about_text"] or ""):
                    handles.add(handle)
        for name in names:
            groups.setdefault(name, set()).update(handles)
    return groups


async def _vk_identity(handle, cache, refresh):
    """Only monitoring resolves handles. Digest lookups use the same persisted signed identity."""
    handle = str(handle or "").casefold()
    if not re.fullmatch(r"[a-z0-9_][a-z0-9_.]{0,79}", handle):
        return None
    if handle in cache:
        return cache[handle]
    path = AVATAR_ROOT / "vk-identities" / f"{handle}.json"
    try:
        info = json.loads(path.read_text())
    except (OSError, ValueError):
        info = {}
    if refresh and time.time() - float(info.get("at") or 0) >= _AVATAR_TTL:
        try:
            from main import vk_resolve_group
            uid, title, _, kind = await vk_resolve_group(handle)
            if not uid or kind not in {"user", "group"}:
                raise ValueError("Unexpected VK identity")
            owner = int(uid) if kind == "user" else -int(uid)
            if info.get("owner") not in (None, owner):
                logger.warning("source_avatar handle_reassigned handle=%s", handle)
                info = dict(info, conflict_owner=owner, at=time.time())
            else:
                info = {"owner": owner, "name": title, "at": time.time()}
            path.parent.mkdir(parents=True, exist_ok=True)
            _write(path, json.dumps(info, ensure_ascii=False).encode())
        except Exception as exc:
            logger.info("source_avatar resolution_failed handle=%s error_type=%s", handle, type(exc).__name__)
    cache[handle] = None if info.get("conflict_owner") else info or None
    return cache[handle]


async def _attach_organizer_communities(row, participants, registered, community_cache, *, refresh=False):
    for organizer in participants:
        if organizer["roles"] != ["Организатор"] or organizer.get("conflicting_profile_owners") or organizer["owner"]:
            continue
        name = organizer["name"]
        handles = set(registered.get(" ".join(name.casefold().replace("ё", "е").split()), ()))
        if row.get("source_platform") == "telegram" and _organizer_in_title(name, row.get("source_title") or ""):
            if handle := _declared_vk_community(row.get("source_about_text") or ""):
                handles.add(handle)
        if len(handles) == 1:
            identity = await _vk_identity(next(iter(handles)), community_cache, refresh)
            if identity and identity["owner"] < 0 and _organizer_in_title(name, identity["name"]):
                organizer["owner"] = identity["owner"]


async def _guide_registry(db):
    if db is None:
        return [], {}
    async with db.raw_conn() as conn:
        conn.row_factory = aiosqlite.Row
        profiles = [dict(p) for p in await (await conn.execute(
            "SELECT id,display_name,marketing_name,profile_kind,source_links_json,facts_rollup_json FROM guide_profile"
        )).fetchall()]
        sources = {}
        for source in await (await conn.execute(
            "SELECT primary_profile_id,title,about_text,flags_json FROM guide_source WHERE platform='telegram'"
        )).fetchall():
            sources.setdefault(source["primary_profile_id"], []).append(source)
    for profile in profiles:
        states = [json.loads(src["flags_json"] or "{}").get("avatar") for src in sources.get(profile["id"], [])]
        profile["_telegram_portraits"] = [state for state in states if telegram_portrait(state)]
    return profiles, _registered_organizer_groups(profiles, sources)


async def _prepare_guide_avatars(rows, db, *, refresh):
    profiles, registered = await _guide_registry(db)
    community_cache = {}
    photos = {}
    async def portrait(owner):
        if owner not in photos:
            photos[owner] = await ensure_vk_avatar(owner) if refresh else avatar_path(owner)
        return photos[owner]
    for row in rows:
        participants = guide_participants(row, profiles)
        await _attach_organizer_communities(row, participants, registered, community_cache, refresh=refresh)
        for person in participants:
            if person["owner"] and (path := await portrait(person["owner"])):
                if person.get("avatar_platform") != "telegram":
                    person.update(path=str(path), avatar_platform="vk")
        row["participant_medallions"] = participants
        if row.get("source_roles_checked") and not participants:
            continue
        flags = json.loads(row.get("source_flags_json") or "{}")
        if row.get("source_platform") == "telegram" and not participants:
            state = flags.get("avatar")
            if path := telegram_portrait(state):
                row["participant_medallions"] = [{"name": row.get("source_title") or row.get("source_username"),
                    "roles": [], "owner": None, "path": str(path), "avatar_platform":"telegram",
                    "avatar_peer_id": state["peer_id"]}]
        if row.get("source_platform") != "vk":
            continue
        owner = int(flags.get("vk_id") or 0)
        if owner:
            owner = abs(owner) if flags.get("vk_personal_page") else -abs(owner)
        else:
            entry = await _vk_identity(row.get("source_username"), community_cache, refresh)
            if not entry:
                continue
            owner = entry["owner"]
        row["source_avatar_owner"] = owner
        if path := await portrait(owner):
            row["source_avatar_path"] = str(path)
            public_name = avatar_metadata(owner).get("name") or ""
            source_names = _identity_names({"display_name": public_name})
            source_participant = None
            resolved = []
            for person in participants:
                matched = source_names & _identity_names({"display_name": person["name"]})
                if owner < 0 and person["roles"] == ["Гид"]:
                    matched = matched and any("Организатор" in p["roles"] and
                        source_names & _identity_names({"display_name": p["name"]}) for p in participants)
                if (not person.get("owner") and not person.get("conflicting_guide_tags")
                        and not person.get("conflicting_profile_owners") and matched):
                    person["owner"] = owner
                    if person.get("avatar_platform") != "telegram":
                        person.update(path=str(path), avatar_platform="vk")
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
                    "roles": [], "owner": owner, "path": str(path), "avatar_platform": "vk"})
            row["participant_medallions"] = resolved


async def prepare_guide_avatars(rows: list[dict[str, Any]], db: Any = None) -> None:
    """Assemble each digest from existing entity portraits without provider requests."""
    await _prepare_guide_avatars(rows, db, refresh=False)
    for row in rows:
        row["avatar_roles_resolved"] = True


async def refresh_guide_avatars(db, source_ids) -> int:
    """Refresh the imported sources and the credited people on their forthcoming departures."""
    ids = sorted({int(source_id) for source_id in source_ids})
    if not ids:
        return 0
    args = ",".join("?" for _ in ids)
    async with db.raw_conn() as conn:
        conn.row_factory = aiosqlite.Row
        sources = await (await conn.execute(f"""
            SELECT gs.id, gs.platform AS source_platform, gs.title AS source_title, gs.about_text AS source_about_text,
                   gs.username AS source_username, gs.flags_json AS source_flags_json, gp.display_name, gp.profile_kind
              FROM guide_source gs LEFT JOIN guide_profile gp ON gp.id=gs.primary_profile_id WHERE gs.id IN ({args})
        """, ids)).fetchall()
        rows = []
        for source in sources:
            row = dict(source)
            if row["display_name"]:
                role = "organizer_names" if row["profile_kind"] in _ORG_PROFILE_KINDS else "guide_names"
                row[role] = [row["display_name"]]
            rows.append(row)
        cur = await conn.execute(f"""
            SELECT go.id,go.canonical_title,go.date,go.time,go.guide_names_json,go.organizer_names_json,go.fact_pack_json,
                   mp.post_date,mp.source_url,mp.text
              FROM guide_occurrence go
              LEFT JOIN guide_occurrence_source gos ON gos.occurrence_id=go.id
              LEFT JOIN guide_monitor_post mp ON mp.id=gos.post_id
             WHERE go.date >= date('now','-1 day') AND COALESCE(go.status,'') != 'cancelled'
               AND (go.primary_source_id IN ({args}) OR EXISTS (
                     SELECT 1 FROM guide_occurrence_source o JOIN guide_monitor_post p ON p.id=o.post_id
                      WHERE o.occurrence_id=go.id AND p.source_id IN ({args})))
        """, [*ids,*ids])
        occurrences = {}
        for record in await cur.fetchall():
            if record["id"] not in occurrences:
                occurrences[record["id"]] = {k:record[k] for k in ("id","canonical_title","date","time")}
                occurrences[record["id"]].update(guide_names=json.loads(record["guide_names_json"] or "[]"),
                    organizer_names=json.loads(record["organizer_names_json"] or "[]"),
                    fact_pack=json.loads(record["fact_pack_json"] or "{}"), linked_posts=[])
            if record["source_url"]:
                occurrences[record["id"]]["linked_posts"].append({"url":record["source_url"],"text":record["text"],"post_date":record["post_date"]})
        # Serialize this short file cleanup with other imports. No provider I/O runs in this transaction.
        await conn.execute("BEGIN IMMEDIATE")

        active = {}
        for flags, in await (await conn.execute("SELECT flags_json FROM guide_source WHERE platform='telegram'")).fetchall():
            state = json.loads(flags or "{}").get("avatar")
            if path := telegram_portrait(state):
                active.setdefault(path.parent, set()).add(path)
        telegram_root = AVATAR_ROOT / "telegram"
        for directory in telegram_root.iterdir() if telegram_root.is_dir() else ():
            if not directory.is_dir() or directory.is_symlink() or not re.fullmatch(r"-?\d+", directory.name):
                continue
            retained = active.get(directory, set())
            archived = sorted((p for p in directory.glob("*.webp") if p not in retained and
                               re.fullmatch(r"[0-9a-f]{64}", p.stem)), key=lambda p: p.stat().st_mtime, reverse=True)
            for superseded in archived[1:] if retained else archived:
                if retained or superseded.stat().st_mtime < time.time() - 7 * _AVATAR_TTL:
                    superseded.unlink(missing_ok=True)
            if not any(directory.iterdir()):
                directory.rmdir()
        await conn.commit()
    rows.extend(occurrences.values())
    await _prepare_guide_avatars(rows, db, refresh=True)
    return len(sources)


def source_medallions(urls: Any, *, index: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return [meta for owner in vk_post_owners(urls)
        if (meta := (index.get(str(owner), {}) if index is not None else avatar_metadata(owner))).get("image_url")]
