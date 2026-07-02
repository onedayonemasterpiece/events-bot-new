from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import text

from geo_region import (
    KALININGRAD_OBLAST_REGION_CODE,
    _allowlist_norm,
    _db_get_cached,
    _normalize_city,
)


LOGGER = logging.getLogger(__name__)

# Conservative city-centre marker directory for Kaliningrad Oblast places that
# are common in event data. VK wall.post accepts lat/long for a post marker; we
# intentionally avoid city_id because wall.post has no such parameter.
# Values are approximate settlement centres, not venue-level coordinates.
_STATIC_CITY_MARKERS: dict[str, tuple[str, float, float]] = {
    "калининград": ("Калининград", 54.710426, 20.452214),
    "балтийск": ("Балтийск", 54.651412, 19.914191),
    "багратионовск": ("Багратионовск", 54.386822, 20.641850),
    "гвардейск": ("Гвардейск", 54.647742, 21.065137),
    "гурьевск": ("Гурьевск", 54.773232, 20.605217),
    "гусев": ("Гусев", 54.592221, 22.199716),
    "зеленоградск": ("Зеленоградск", 54.960022, 20.475327),
    "краснознаменск": ("Краснознаменск", 54.942213, 22.489719),
    "ладушкин": ("Ладушкин", 54.570012, 20.170172),
    "мамоново": ("Мамоново", 54.464703, 19.945690),
    "неман": ("Неман", 55.031110, 22.032760),
    "нестеров": ("Нестеров", 54.630604, 22.571395),
    "озёрск": ("Озёрск", 54.410580, 22.011590),
    "озерск": ("Озёрск", 54.410580, 22.011590),
    "пионерский": ("Пионерский", 54.951793, 20.227480),
    "полесск": ("Полесск", 54.862052, 21.102790),
    "правдинск": ("Правдинск", 54.443914, 21.017849),
    "приморск": ("Приморск", 54.731065, 19.945604),
    "светлогорск": ("Светлогорск", 54.943956, 20.151478),
    "славск": ("Славск", 55.043620, 21.674750),
    "советск": ("Советск", 55.083923, 21.878510),
    "черняховск": ("Черняховск", 54.631640, 21.815570),
    "янтарный": ("Янтарный", 54.871110, 19.938060),
    "янтарное": ("Янтарный", 54.871110, 19.938060),
    # Context-sensitive settlements. These are applied only when supporting
    # event location/address context points back to Kaliningrad Oblast.
    "донское": ("Донское", 54.940600, 19.972200),
    "куликово": ("Куликово", 54.941900, 20.336400),
    "лесной": ("Лесной", 55.012500, 20.614200),
    "малиновка": ("Малиновка", 54.933000, 20.282000),
    "морское": ("Морское", 55.226300, 20.917500),
    "некрасово": ("Некрасово", 54.845000, 20.554000),
    "отрадное": ("Отрадное", 54.936800, 20.187800),
    "переславское": ("Переславское", 54.756000, 20.218000),
    "приморье": ("Приморье", 54.912500, 20.079800),
    "рыбачий": ("Рыбачий", 55.154700, 20.853600),
    "сосновка": ("Сосновка", 54.900000, 20.650000),
    "ушаково": ("Ушаково", 54.627800, 20.300000),
}

_AMBIGUOUS_CITY_MARKERS = {
    "донское",
    "городок",
    "куликово",
    "лесной",
    "малиновка",
    "морское",
    "некрасово",
    "отрадное",
    "переславское",
    "приморье",
    "рыбачий",
    "сосновка",
    "ушаково",
}

_CONTEXT_TOKENS = {
    "калининград",
    "калининградская область",
    "кёнигсберг",
    "кенингсберг",
    "куршская",
    "куршской",
    "куршскую",
    "балтийск",
    "светлогорск",
    "зеленоградск",
    "гурьевск",
    "гвардейск",
    "черняховск",
    "советск",
    "гусев",
    "янтарный",
}

_LOCATION_PARAM_KEYS = {"lat", "long", "place_id"}


def location_marker_param_keys() -> set[str]:
    return set(_LOCATION_PARAM_KEYS)


@dataclass(frozen=True, slots=True)
class VkLocationMarkerDecision:
    status: str
    query_norm: str = ""
    query_display: str = ""
    display_title: str | None = None
    city: str | None = None
    is_kaliningrad_oblast: bool | None = None
    lat: float | None = None
    long: float | None = None
    place_id: str | None = None
    confidence: float = 0.0
    provenance: str | None = None
    details: dict[str, Any] | None = None

    @property
    def applied(self) -> bool:
        return self.status == "applied" and bool(self.payload)

    @property
    def payload(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if self.place_id:
            out["place_id"] = str(self.place_id)
        if self.lat is not None and self.long is not None:
            out["lat"] = f"{float(self.lat):.6f}"
            out["long"] = f"{float(self.long):.6f}"
        return out


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _negative_ttl_seconds() -> int:
    raw = os.getenv("VK_LOCATION_MARKER_NEGATIVE_TTL_SECONDS", str(7 * 24 * 3600))
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 7 * 24 * 3600


def _is_negative_cache_fresh(updated_at: Any) -> bool:
    ttl = _negative_ttl_seconds()
    if ttl <= 0:
        return False
    dt = _parse_dt(updated_at)
    if dt is None:
        return False
    age = (datetime.now(timezone.utc) - dt).total_seconds()
    return age <= ttl


async def ensure_vk_location_marker_cache_table(db: Any) -> None:
    if db is None:
        return
    async with db.get_session() as session:
        await session.execute(
            text(
                "CREATE TABLE IF NOT EXISTS vk_location_marker_cache("
                "query_norm TEXT PRIMARY KEY, "
                "query_display TEXT, "
                "display_title TEXT, "
                "city TEXT, "
                "is_kaliningrad_oblast BOOLEAN, "
                "lat REAL, "
                "long REAL, "
                "place_id TEXT, "
                "confidence REAL, "
                "provenance TEXT, "
                "status TEXT, "
                "details JSON, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
        )
        await session.commit()


async def _cache_get(db: Any, query_norm: str) -> VkLocationMarkerDecision | None:
    if db is None or not query_norm:
        return None
    await ensure_vk_location_marker_cache_table(db)
    async with db.get_session() as session:
        row = (
            await session.execute(
                text(
                    "SELECT query_norm, query_display, display_title, city, "
                    "is_kaliningrad_oblast, lat, long, place_id, confidence, "
                    "provenance, status, details, updated_at "
                    "FROM vk_location_marker_cache WHERE query_norm = :query_norm LIMIT 1"
                ),
                {"query_norm": query_norm},
            )
        ).first()
    if not row:
        return None
    status = str(row[10] or "").strip() or "skipped_low_confidence"
    if status != "applied" and not _is_negative_cache_fresh(row[12]):
        return None
    details: dict[str, Any] | None = None
    try:
        parsed = json.loads(row[11] or "{}")
        details = parsed if isinstance(parsed, dict) else None
    except Exception:
        details = None
    return VkLocationMarkerDecision(
        status=status,
        query_norm=str(row[0] or query_norm),
        query_display=str(row[1] or ""),
        display_title=str(row[2]) if row[2] is not None else None,
        city=str(row[3]) if row[3] is not None else None,
        is_kaliningrad_oblast=None if row[4] is None else bool(row[4]),
        lat=None if row[5] is None else float(row[5]),
        long=None if row[6] is None else float(row[6]),
        place_id=str(row[7]) if row[7] is not None and str(row[7]).strip() else None,
        confidence=float(row[8] or 0.0),
        provenance=str(row[9]) if row[9] is not None else None,
        details=details,
    )


async def _cache_put(db: Any, decision: VkLocationMarkerDecision) -> None:
    if db is None or not decision.query_norm:
        return
    await ensure_vk_location_marker_cache_table(db)
    now = _now_iso()
    async with db.get_session() as session:
        await session.execute(
            text(
                "INSERT INTO vk_location_marker_cache("
                "query_norm, query_display, display_title, city, is_kaliningrad_oblast, "
                "lat, long, place_id, confidence, provenance, status, details, created_at, updated_at"
                ") VALUES("
                ":query_norm, :query_display, :display_title, :city, :is_kaliningrad_oblast, "
                ":lat, :long, :place_id, :confidence, :provenance, :status, :details, :created_at, :updated_at"
                ") ON CONFLICT(query_norm) DO UPDATE SET "
                "query_display=excluded.query_display, "
                "display_title=excluded.display_title, "
                "city=excluded.city, "
                "is_kaliningrad_oblast=excluded.is_kaliningrad_oblast, "
                "lat=excluded.lat, "
                "long=excluded.long, "
                "place_id=excluded.place_id, "
                "confidence=excluded.confidence, "
                "provenance=excluded.provenance, "
                "status=excluded.status, "
                "details=excluded.details, "
                "updated_at=excluded.updated_at"
            ),
            {
                "query_norm": decision.query_norm,
                "query_display": decision.query_display,
                "display_title": decision.display_title,
                "city": decision.city,
                "is_kaliningrad_oblast": None
                if decision.is_kaliningrad_oblast is None
                else int(bool(decision.is_kaliningrad_oblast)),
                "lat": decision.lat,
                "long": decision.long,
                "place_id": decision.place_id,
                "confidence": float(decision.confidence or 0.0),
                "provenance": decision.provenance,
                "status": decision.status,
                "details": json.dumps(decision.details or {}, ensure_ascii=False),
                "created_at": now,
                "updated_at": now,
            },
        )
        await session.commit()


def _context_supports_kaliningrad_oblast(
    *,
    city_norm: str,
    location_name: str | None,
    location_address: str | None,
) -> bool:
    if city_norm not in _AMBIGUOUS_CITY_MARKERS:
        return True
    context = f"{location_name or ''} {location_address or ''}".casefold()
    context = re.sub(r"\s+", " ", context)
    return any(token in context for token in _CONTEXT_TOKENS)


async def _fast_region_allowed(db: Any, *, city_norm: str) -> tuple[bool | None, str | None, dict[str, Any]]:
    details: dict[str, Any] = {}
    if not city_norm:
        return None, "no_city", details
    if city_norm in _allowlist_norm():
        details["region_source"] = "allowlist"
        return True, "allowlist", details
    if db is not None:
        cached = await _db_get_cached(db, city_norm=city_norm)
        if cached is not None:
            details["region_source"] = cached.source
            details["region_code"] = cached.region_code
            details["region_name"] = cached.region_name
            return cached.allowed, cached.source or "geo_cache", details
    return None, "geo_cache_miss", details


def _decision(
    status: str,
    *,
    query_norm: str,
    query_display: str,
    is_kaliningrad_oblast: bool | None = None,
    provenance: str | None = None,
    confidence: float = 0.0,
    details: dict[str, Any] | None = None,
) -> VkLocationMarkerDecision:
    return VkLocationMarkerDecision(
        status=status,
        query_norm=query_norm,
        query_display=query_display,
        city=query_display or None,
        is_kaliningrad_oblast=is_kaliningrad_oblast,
        confidence=confidence,
        provenance=provenance,
        details=details or {},
    )


async def resolve_vk_location_marker_for_event(
    event: Any,
    db: Any,
) -> VkLocationMarkerDecision:
    """Resolve a safe optional VK wall.post location marker for an event.

    The resolver is deliberately conservative and fail-open: it never raises to
    the publisher, and it only returns an applied marker when structured
    ``event.city`` is present, Kaliningrad Oblast membership is already known
    from the allowlist/cache, and a static marker exists.
    """

    try:
        raw_city = str(getattr(event, "city", None) or "").strip()
        if not raw_city:
            return VkLocationMarkerDecision(status="skipped_no_city")
        city_norm = _normalize_city(raw_city)
        if not city_norm:
            return VkLocationMarkerDecision(status="skipped_no_city")

        location_name = str(getattr(event, "location_name", None) or "").strip()
        location_address = str(getattr(event, "location_address", None) or "").strip()

        if _context_supports_kaliningrad_oblast(
            city_norm=city_norm,
            location_name=location_name,
            location_address=location_address,
        ):
            cached = await _cache_get(db, city_norm)
            if cached is not None:
                LOGGER.info(
                    "vk.location_marker decision=%s city=%s source=cache confidence=%.2f",
                    cached.status,
                    city_norm,
                    cached.confidence,
                )
                return cached

        allowed, region_source, region_details = await _fast_region_allowed(db, city_norm=city_norm)
        if allowed is not True:
            status = "skipped_not_region" if allowed is False else "skipped_low_confidence"
            dec = _decision(
                status,
                query_norm=city_norm,
                query_display=raw_city,
                is_kaliningrad_oblast=allowed,
                provenance=region_source,
                details=region_details,
            )
            await _cache_put(db, dec)
            LOGGER.info(
                "vk.location_marker decision=%s city=%s region_source=%s",
                status,
                city_norm,
                region_source,
            )
            return dec

        if not _context_supports_kaliningrad_oblast(
            city_norm=city_norm,
            location_name=location_name,
            location_address=location_address,
        ):
            dec = _decision(
                "skipped_low_confidence",
                query_norm=city_norm,
                query_display=raw_city,
                is_kaliningrad_oblast=True,
                provenance="ambiguous_city_without_supporting_context",
                details={**region_details, "ambiguous_city": True},
            )
            LOGGER.info(
                "vk.location_marker decision=skipped_low_confidence city=%s reason=ambiguous_city",
                city_norm,
            )
            return dec

        marker = _STATIC_CITY_MARKERS.get(city_norm)
        if not marker:
            dec = _decision(
                "skipped_low_confidence",
                query_norm=city_norm,
                query_display=raw_city,
                is_kaliningrad_oblast=True,
                provenance="static_directory_miss",
                details=region_details,
            )
            await _cache_put(db, dec)
            LOGGER.info(
                "vk.location_marker decision=skipped_low_confidence city=%s reason=static_miss",
                city_norm,
            )
            return dec

        display_title, lat, long = marker
        dec = VkLocationMarkerDecision(
            status="applied",
            query_norm=city_norm,
            query_display=raw_city,
            display_title=display_title,
            city=display_title,
            is_kaliningrad_oblast=True,
            lat=lat,
            long=long,
            confidence=0.95 if city_norm not in _AMBIGUOUS_CITY_MARKERS else 0.90,
            provenance=f"static_directory+{region_source or 'region'}",
            details={**region_details, "region_code": KALININGRAD_OBLAST_REGION_CODE},
        )
        await _cache_put(db, dec)
        LOGGER.info(
            "vk.location_marker decision=applied city=%s title=%s confidence=%.2f provenance=%s",
            city_norm,
            display_title,
            dec.confidence,
            dec.provenance,
        )
        return dec
    except Exception as exc:
        LOGGER.warning("vk.location_marker decision=lookup_error err=%s", exc, exc_info=True)
        return VkLocationMarkerDecision(
            status="lookup_error",
            details={"error": type(exc).__name__},
        )


def sanitize_location_marker_payload(payload: Mapping[str, Any] | None) -> dict[str, str]:
    if not payload:
        return {}
    out: dict[str, str] = {}
    place_id = str(payload.get("place_id") or "").strip()
    if place_id:
        out["place_id"] = place_id
    lat = payload.get("lat")
    long = payload.get("long")
    if lat is not None and long is not None:
        try:
            lat_f = float(str(lat).strip().replace(",", "."))
            long_f = float(str(long).strip().replace(",", "."))
        except (TypeError, ValueError):
            lat_f = long_f = None  # type: ignore[assignment]
        if lat_f is not None and long_f is not None and -90 <= lat_f <= 90 and -180 <= long_f <= 180:
            out["lat"] = f"{lat_f:.6f}"
            out["long"] = f"{long_f:.6f}"
    return out
