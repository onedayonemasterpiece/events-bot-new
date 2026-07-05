from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import aiosqlite

from db import Database

LOCAL_TZ = ZoneInfo("Europe/Kaliningrad")
GUIDE_EXCURSION_PROMO_SCENE_VARIANT = "guide_excursion_promo"
GUIDE_EXCURSION_PROMO_KEY = "guide_excursion_promo"
GUIDE_EXCURSION_PROMO_MIN_POSITION = 2
GUIDE_EXCURSION_PROMO_MAX_POSITION = 6

_AVATAR_FILES = {
    "twometerguide": "twometerguide.jpg",
    "natakkaz": "natakkaz.jpg",
    "katimartihobby": "katimartihobby.jpg",
    "katya_kostyugova": "katya_kostyugova.jpg",
    "gid_zelenogradsk": "gid_zelenogradsk.jpg",
    "murnikovat": "murnikovat.jpg",
    "valeravezet": "valeravezet.jpg",
    "amber_fringilla": "amber_fringilla.jpg",
    "tatyana_udovenko_face": "tatyana_udovenko_face.jpg",
}

# These are the only existing visual-digest avatars that are suitable for a
# person-led promo scene. Project-like sources are allowed only when the avatar
# is still a human/personal asset; organization-only avatar keys are deliberately
# omitted.
_PERSONAL_AVATAR_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"татьяна удовенко|tanja_from_koenigsberg", "tatyana_udovenko_face"),
    (r"progulki_s_katey|katerinakostiugova|kostiugova|прогулки с катей|катя костюгова|екатерина костюгова", "katya_kostyugova"),
    (r"twometerguide|двухметров", "twometerguide"),
    (r"natakkaz|наталья казакова", "natakkaz"),
    (r"katimartihobby|катя марти|шаги кати", "katimartihobby"),
    (r"gid_zelenogradsk|кот[оа]ва наталья|наталья котова|зеленоградск", "gid_zelenogradsk"),
    (r"murnikovat|мурникова", "murnikovat"),
    (r"valeravezet|автобус валер", "valeravezet"),
    (r"amber_fringilla|amber fringilla|пруссии|юлия гришанова", "amber_fringilla"),
)

_ORG_SOURCE_KINDS = {"organization_with_tours", "excursion_operator", "aggregator"}
_BAD_AVAILABILITY_RE = re.compile(
    r"\bрезерв\b|нет\s+мест|мест\s+нет|sold\s*out|soldout|запись\s+закрыт|закрыт[аы]?\s+запись|"
    r"мест[а]?\s+законч|мест[а]?\s+заполн|группа\s+набран",
    re.IGNORECASE,
)
_PHONE_RE = re.compile(r"(?:\+?7|8)?[\s\-()]*(?:\d[\s\-()]*){10,}")
_TG_USERNAME_RE = re.compile(r"@([A-Za-z0-9_]{4,32})")

_PALETTES = ("prussian_cream", "deep_wine_ivory", "museum_green_ivory", "black_lime")
_MONTHS_GENITIVE_UPPER = (
    "",
    "ЯНВАРЯ",
    "ФЕВРАЛЯ",
    "МАРТА",
    "АПРЕЛЯ",
    "МАЯ",
    "ИЮНЯ",
    "ИЮЛЯ",
    "АВГУСТА",
    "СЕНТЯБРЯ",
    "ОКТЯБРЯ",
    "НОЯБРЯ",
    "ДЕКАБРЯ",
)


@dataclass(frozen=True)
class GuideExcursionPromoCandidate:
    occurrence_id: int
    title: str
    date_iso: str
    time: str
    status: str
    seats_count: int
    seats_text: str
    booking_text: str
    booking_url: str
    channel_url: str
    source_username: str
    source_title: str
    source_kind: str
    profile_kind: str
    avatar_keys: tuple[str, ...]

    def to_payload(self, *, insert_position: int, palette: str) -> dict[str, Any]:
        contact_label, contact_value = _contact_for_candidate(self)
        return {
            "occurrence_id": self.occurrence_id,
            "title": self.title,
            "date_iso": self.date_iso,
            "time": self.time,
            "seats_count": self.seats_count,
            "seats_text": self.seats_text,
            "booking_text": self.booking_text,
            "booking_url": self.booking_url,
            "channel_url": self.channel_url,
            "source_username": self.source_username,
            "source_title": self.source_title,
            "avatar_keys": list(self.avatar_keys),
            "avatar_images": [f"assets/guide_avatars/{_AVATAR_FILES[key]}" for key in self.avatar_keys if key in _AVATAR_FILES],
            "contact_label": contact_label,
            "contact": contact_value,
            "palette": palette,
            "icon_kind": _icon_kind(self.title),
            "insert_position": insert_position,
        }


def guide_avatar_bundle_files(project_root: Path) -> list[tuple[Path, str]]:
    base = project_root / "guide_excursions" / "assets" / "visual_digest_avatars"
    files: list[tuple[Path, str]] = []
    for key, filename in sorted(_AVATAR_FILES.items()):
        files.append((base / filename, f"assets/guide_avatars/{filename}"))
    return files


def build_guide_excursion_promo_scene(promo: Mapping[str, Any]) -> dict[str, Any]:
    images = [str(v).strip() for v in promo.get("avatar_images") or [] if str(v).strip()]
    return {
        "scene_variant": GUIDE_EXCURSION_PROMO_SCENE_VARIANT,
        "title": str(promo.get("title") or "").strip(),
        "date": _format_payload_date(str(promo.get("date_iso") or ""), str(promo.get("time") or "")),
        "date_iso": str(promo.get("date_iso") or "").strip(),
        "time": str(promo.get("time") or "").strip(),
        "images": images,
        "guide_excursion": dict(promo),
    }


def insert_guide_excursion_promo_scene(scenes: list[dict[str, Any]], promo: Mapping[str, Any]) -> list[dict[str, Any]]:
    if not promo:
        return scenes
    scene = build_guide_excursion_promo_scene(promo)
    try:
        position = int(promo.get("insert_position") or GUIDE_EXCURSION_PROMO_MIN_POSITION)
    except Exception:
        position = GUIDE_EXCURSION_PROMO_MIN_POSITION
    position = max(GUIDE_EXCURSION_PROMO_MIN_POSITION, min(GUIDE_EXCURSION_PROMO_MAX_POSITION, position))
    idx = max(1, min(len(scenes), position - 1)) if scenes else 0
    return [*scenes[:idx], scene, *scenes[idx:]]


def _format_payload_date(raw_date: str, raw_time: str) -> str:
    raw_time = raw_time.strip()
    try:
        day = date.fromisoformat(str(raw_date or "").strip())
        value = f"{day.day:02d}.{day.month:02d}"
    except Exception:
        value = str(raw_date or "").strip()
    if raw_time:
        return f"{value} {raw_time[:5]}" if value else raw_time[:5]
    return value


def _stable_rng(target_day: date) -> random.Random:
    digest = hashlib.sha256(f"guide-excursion-promo:{target_day.isoformat()}".encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


def _row_blob(row: Mapping[str, Any]) -> str:
    keys = (
        "source_username", "source_title", "source_kind", "profile_kind", "display_name", "marketing_name",
        "guide_names_json", "organizer_names_json", "participant_profiles_json", "canonical_title", "booking_text", "channel_url",
    )
    return " ".join(str(row.get(key) or "") for key in keys).lower()


def _avatar_keys(row: Mapping[str, Any]) -> tuple[str, ...]:
    blob = _row_blob(row)
    keys: list[str] = []
    for pattern, key in _PERSONAL_AVATAR_PATTERNS:
        if re.search(pattern, blob, flags=re.IGNORECASE) and key not in keys:
            keys.append(key)
    return tuple(keys[:3])


def seats_count(value: str | None) -> int | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    if _BAD_AVAILABILITY_RE.search(raw):
        return 0
    match = re.search(r"\d+", raw)
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def _available(row: Mapping[str, Any], *, now: datetime) -> tuple[bool, int | None]:
    status = str(row.get("status") or "").strip().lower()
    if status in {"cancelled", "canceled", "sold_out", "full", "unavailable", "rescheduled"}:
        return False, None
    text = " ".join(str(row.get(key) or "") for key in ("status", "seats_text", "booking_text"))
    if _BAD_AVAILABILITY_RE.search(text):
        return False, None
    date_raw = str(row.get("date") or "").strip()
    try:
        day = date.fromisoformat(date_raw)
    except Exception:
        return False, None
    time_raw = str(row.get("time") or "").strip()
    hhmm = time_raw[:5] if re.match(r"^\d{1,2}:\d{2}", time_raw) else "23:59"
    try:
        hour, minute = [int(part) for part in hhmm.split(":", 1)]
        starts_at = datetime.combine(day, time(hour, minute), tzinfo=LOCAL_TZ)
    except Exception:
        starts_at = datetime.combine(day, time(23, 59), tzinfo=LOCAL_TZ)
    if starts_at <= now.astimezone(LOCAL_TZ):
        return False, None
    free = seats_count(str(row.get("seats_text") or ""))
    if free is None or free <= 0:
        return False, free
    return True, free


def _candidate_from_row(row: Mapping[str, Any], *, now: datetime) -> GuideExcursionPromoCandidate | None:
    source_kind = str(row.get("source_kind") or "").strip()
    if source_kind in _ORG_SOURCE_KINDS:
        return None
    avatar_keys = _avatar_keys(row)
    if not avatar_keys:
        return None
    ok, free = _available(row, now=now)
    if not ok or free is None or free <= 0:
        return None
    title = " ".join(str(row.get("canonical_title") or "").split()).strip()
    if not title:
        return None
    return GuideExcursionPromoCandidate(
        occurrence_id=int(row["id"]),
        title=title,
        date_iso=str(row.get("date") or "").strip(),
        time=str(row.get("time") or "").strip(),
        status=str(row.get("status") or "").strip(),
        seats_count=int(free),
        seats_text=str(row.get("seats_text") or "").strip(),
        booking_text=str(row.get("booking_text") or "").strip(),
        booking_url=str(row.get("booking_url") or "").strip(),
        channel_url=str(row.get("channel_url") or "").strip(),
        source_username=str(row.get("source_username") or "").strip(),
        source_title=str(row.get("source_title") or "").strip(),
        source_kind=source_kind,
        profile_kind=str(row.get("profile_kind") or "").strip(),
        avatar_keys=avatar_keys,
    )


def _last_promoted_occurrence_ids(rows: list[Mapping[str, Any]]) -> set[int]:
    seen: set[int] = set()
    for row in rows:
        raw = row.get("selection_params")
        params: Any = raw
        if isinstance(raw, str):
            try:
                params = json.loads(raw)
            except Exception:
                params = None
        if not isinstance(params, Mapping):
            continue
        promo = params.get(GUIDE_EXCURSION_PROMO_KEY)
        if not isinstance(promo, Mapping):
            continue
        try:
            occurrence_id = int(promo.get("occurrence_id") or 0)
        except Exception:
            occurrence_id = 0
        if occurrence_id > 0:
            seen.add(occurrence_id)
    return seen


def _contact_for_candidate(candidate: GuideExcursionPromoCandidate) -> tuple[str, str]:
    combined = " ".join([candidate.booking_text, candidate.booking_url, candidate.channel_url]).strip()
    phone_match = _PHONE_RE.search(combined)
    if phone_match:
        return "запись", re.sub(r"\s+", " ", phone_match.group(0)).strip()
    for value in (candidate.booking_url, candidate.booking_text, candidate.channel_url):
        tg = _extract_telegram_username(value)
        if tg:
            return "запись", f"@{tg}"
    for value in (candidate.booking_url, candidate.channel_url):
        vk_label = _extract_vk_booking_label(value)
        if vk_label:
            return "запись", vk_label
    if candidate.source_username and not candidate.source_username.startswith("wall"):
        if candidate.channel_url.startswith("https://t.me/"):
            return "запись", f"@{candidate.source_username}"
        if re.match(r"^[A-Za-z0-9_.-]{3,64}$", candidate.source_username):
            return "запись", f"vk.com/{candidate.source_username}"
        return "запись", candidate.source_title or candidate.source_username
    host = _host_label(candidate.booking_url) or _host_label(candidate.channel_url)
    if host:
        return "бронь", host
    return "бронь", candidate.source_title or "Полюбить Калининград"


def _extract_telegram_username(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    match = _TG_USERNAME_RE.search(raw)
    if match:
        return match.group(1)
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    if parsed.netloc.lower() in {"t.me", "telegram.me"}:
        parts = [part for part in parsed.path.split("/") if part]
        if parts and re.match(r"^[A-Za-z0-9_]{4,32}$", parts[0]):
            return parts[0]
    return ""


def _extract_vk_booking_label(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = parsed.netloc.lower().removeprefix("www.")
    if host not in {"vk.com", "m.vk.com"}:
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return "vk.com"
    slug = parts[0].strip()
    if slug.startswith(("wall", "photo", "video", "album")) or slug in {"club", "public", "event"}:
        return "VK"
    if re.match(r"^[A-Za-z0-9_.-]{3,64}$", slug):
        return f"vk.com/{slug}"
    return "VK"


def _host_label(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = parsed.netloc.lower().removeprefix("www.")
    if host in {"t.me", "telegram.me"}:
        tg = _extract_telegram_username(raw)
        return f"@{tg}" if tg else "Telegram"
    if host in {"vk.com", "m.vk.com"}:
        return "VK"
    return host


def _icon_kind(title: str) -> str:
    text = title.lower()
    if any(word in text for word in ("библиотек", "здан", "дом", "фонд")):
        return "building"
    if any(word in text for word in ("сплав", "лод", "каяк", "рек", "анграп")):
        return "water"
    if any(word in text for word in ("поезд", "автобус", "путешествие", "район", "советск", "багратионовск")):
        return "route"
    return "walk"


async def choose_guide_excursion_promo(
    db: Database,
    *,
    now: datetime | None = None,
    profile_key: str = "popular_review",
) -> dict[str, Any] | None:
    if profile_key != "popular_review":
        return None
    now = now or datetime.now(timezone.utc)
    local_now = now.astimezone(LOCAL_TZ)
    lookback_start = (local_now.date() - timedelta(days=1)).isoformat()
    async with aiosqlite.connect(db.path) as conn:
        conn.row_factory = aiosqlite.Row
        last_rows = await conn.execute_fetchall(
            """
            SELECT selection_params
            FROM videoannounce_session
            WHERE profile_key = 'popular_review'
              AND date(created_at) >= date(?)
            ORDER BY id DESC
            LIMIT 8
            """,
            (lookback_start,),
        )
        repeated = _last_promoted_occurrence_ids([dict(row) for row in last_rows])
        rows = await conn.execute_fetchall(
            """
            SELECT go.id, go.canonical_title, go.date, go.time, go.status, go.seats_text,
                   go.booking_text, go.booking_url, go.channel_url,
                   go.guide_names_json, go.organizer_names_json, go.participant_profiles_json,
                   gs.username AS source_username, gs.title AS source_title, gs.source_kind,
                   gp.profile_kind, gp.display_name, gp.marketing_name
            FROM guide_occurrence go
            LEFT JOIN guide_source gs ON gs.id = go.primary_source_id
            LEFT JOIN guide_profile gp ON gp.id = gs.primary_profile_id
            WHERE go.date >= ?
            ORDER BY go.date ASC, COALESCE(go.time, '23:59') ASC, go.id ASC
            LIMIT 180
            """,
            (local_now.date().isoformat(),),
        )
    candidates = [
        c
        for row in rows
        if (c := _candidate_from_row(dict(row), now=now)) is not None
        and c.occurrence_id not in repeated
    ]
    if not candidates:
        return None
    rng = _stable_rng(local_now.date())
    # Prefer sooner excursions but shuffle within the first shelf so the daily
    # slot does not feel deterministic when several equally urgent promos exist.
    shelf = candidates[: min(8, len(candidates))]
    chosen = rng.choice(shelf)
    insert_position = rng.randint(GUIDE_EXCURSION_PROMO_MIN_POSITION, GUIDE_EXCURSION_PROMO_MAX_POSITION)
    palette = rng.choice(_PALETTES)
    return chosen.to_payload(insert_position=insert_position, palette=palette)
