#!/usr/bin/env python3
"""Demo only: one traced walk -> Yandex ruler/navigation links and route metadata.

No provider calls, secrets, DB, or publishing. A route is in (lon, lat) order.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
from decimal import Decimal
from urllib.parse import urlencode

EXAMPLE_RL = (
    "20.478906,54.720380~0.000089,-0.000524~0.000006,-0.000836"
    "~0.000329,-0.000022~0.000380,0.000161~0.000146,0.000154"
    "~0.000399,0.000183~0.000868,0.000334~0.000627,0.000249"
    "~0.000342,0.000143~0.000558,0.000062~0.000596,0.000000"
    "~0.000361,0.000011~0.000361,0.000088~0.000279,-0.000055"
    "~0.000139,-0.000165~0.000558,0.000110"
)
UNITS = 1_000_000
MODE = {"walk": ("pd", "pedestrian"), "bike": ("bc", "bicycle"), "car": ("auto", "car")}


def micro(value: str) -> int:
    return int(Decimal(value) * UNITS)


def fmt(value: int) -> str:
    return f"{value / UNITS:.6f}"


def read_rl(value: str) -> list[tuple[int, int]]:
    """Yandex rl: first absolute longitude,latitude; then signed deltas."""
    points: list[tuple[int, int]] = []
    for part in value.split("~"):
        a, b = part.split(",")
        x, y = micro(a), micro(b)
        if points:
            x += points[-1][0]
            y += points[-1][1]
        if not (-180 * UNITS <= x <= 180 * UNITS and -90 * UNITS <= y <= 90 * UNITS):
            raise ValueError("coordinate out of range")
        points.append((x, y))
    if len(points) < 2:
        raise ValueError("route needs at least two points")
    return points


def write_rl(points: list[tuple[int, int]]) -> str:
    out = []
    prev = (0, 0)
    for i, pt in enumerate(points):
        x, y = pt if i == 0 else (pt[0] - prev[0], pt[1] - prev[1])
        out.append(f"{fmt(x)},{fmt(y)}")
        prev = pt
    return "~".join(out)


def segment_m(a: tuple[int, int], b: tuple[int, int]) -> float:
    lon1, lat1 = (v / UNITS for v in a)
    lon2, lat2 = (v / UNITS for v in b)
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = p2 - p1, math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * 6371008.8 * math.asin(math.sqrt(h))


def length_m(points: list[tuple[int, int]]) -> float:
    return sum(segment_m(a, b) for a, b in zip(points, points[1:]))


def _distance_to_segment(p, a, b, latitude: float) -> float:
    mx = 111320 * math.cos(math.radians(latitude)) / UNITS
    my = 111320 / UNITS
    ax, ay = a[0] * mx, a[1] * my
    bx, by = b[0] * mx, b[1] * my
    px, py = p[0] * mx, p[1] * my
    dx, dy = bx - ax, by - ay
    t = max(0., min(1., ((px - ax) * dx + (py - ay) * dy) / max(dx * dx + dy * dy, 1e-12)))
    return math.hypot(px - ax - t * dx, py - ay - t * dy)


def simplify(points: list[tuple[int, int]], tolerance_m: float = 1.0,
             keep: tuple[int, ...] = ()) -> list[tuple[int, int]]:
    """RDP; preserve ordered start, finish, and all supplied significant stops."""
    if tolerance_m < 0:
        raise ValueError("tolerance must be nonnegative")
    saved = set((0, len(points) - 1, *keep))
    if any(i < 0 or i >= len(points) for i in saved):
        raise ValueError("invalid mandatory waypoint index")
    lat = sum(y for _, y in points) / (UNITS * len(points))

    def segment(lo: int, hi: int):
        if hi <= lo + 1:
            return
        i = max(range(lo + 1, hi),
                key=lambda j: _distance_to_segment(points[j], points[lo], points[hi], lat))
        if _distance_to_segment(points[i], points[lo], points[hi], lat) > tolerance_m:
            saved.add(i)
            segment(lo, i)
            segment(i, hi)

    boundaries = sorted(saved)
    for a, b in zip(boundaries, boundaries[1:]):
        segment(a, b)
    return [points[i] for i in sorted(saved)]


def ruler_url(points: list[tuple[int, int]], city: str = "22/kaliningrad",
              center=(20.481789, 54.719623), zoom=17.76,
              markers: list[tuple[int, int]] | None = None) -> str:
    q = {"ll": f"{center[0]:.6f},{center[1]:.6f}", "rl": write_rl(points), "z": str(zoom)}
    if markers:
        q["pt"] = "~".join(f"{fmt(x)},{fmt(y)}" for x, y in markers)
    return f"https://yandex.ru/maps/{city}/?" + urlencode(q, safe="~,.-")


def navigation_url(points: list[tuple[int, int]], mode: str = "walk",
                   city: str = "22/kaliningrad") -> str:
    if mode not in MODE:
        raise ValueError("unknown mode")
    if not 2 <= len(points) <= 10:  # at most eight intermediate waypoints
        raise ValueError("Yandex navigation needs 2 to 10 points")
    route = "~".join(f"{fmt(y)},{fmt(x)}" for x, y in points)  # LAT,LON
    return f"https://yandex.ru/maps/{city}/?" + urlencode(
        {"rtext": route, "rtt": MODE[mode][0]}, safe="~,.-")


def gis2_url(pois: list[tuple[int, int, str]], mode: str = "walk") -> str:
    """Only use verified 2GIS object IDs, never invent them."""
    if mode not in MODE or not 2 <= len(pois) <= 10:
        raise ValueError("unknown mode or wrong number of POIs")
    parts = []
    for lon, lat, object_id in pois:
        if not object_id.isdigit():
            raise ValueError("2GIS object ID required from a verified provider link")
        parts.append(f"{fmt(lon)},{fmt(lat)};{object_id}")
    return "https://2gis.ru/kaliningrad/directions/tab/" + MODE[mode][1] + "/points/" + "%7C".join(
        p.replace(";", "%3B").replace(",", "%2C") for p in parts)


def vk_short_from_api(response: dict) -> str:
    """Normalize existing utils.getShortLink API response. Does not call VK."""
    body = response.get("response", response)
    if not isinstance(body, dict):
        raise ValueError("invalid VK response")
    key = body.get("key")
    link = body.get("short_url")
    if not key and not link:
        raise ValueError("VK did not return short link")
    value = link or f"https://vk.cc/{key}"
    if not value.startswith("https://vk.cc/"):
        raise ValueError("unexpected short-link domain")
    return value


async def vk_shorten_with_existing_adapter(long_url: str, vk_api_fn, db=None, bot=None) -> str:
    """Reuse events-bot's existing vk_api_fn contract; no Event or database mutation."""
    result = await vk_api_fn("utils.getShortLink", {"url": long_url}, db, bot)
    return vk_short_from_api(result)


def self_test():
    pts = read_rl(EXAMPLE_RL)
    assert len(pts) == 17 and pts[0] == (20478906, 54720380)
    assert pts[-1] == (20484944, 54720273)
    assert write_rl(pts) == EXAMPLE_RL
    assert 598 < length_m(pts) < 600
    assert len(simplify(pts, 1.0)) == 14
    assert read_rl(write_rl(simplify(pts, 1.0))) == simplify(pts, 1.0)
    assert "rl=" in ruler_url(pts)
    assert "pt=" in ruler_url(pts, markers=[pts[0], pts[-1]])
    assert "rtt=pd" in navigation_url([pts[0], pts[-1]])
    assert "rtt=bc" in navigation_url([pts[0], pts[-1]], "bike")
    assert "rtt=auto" in navigation_url([pts[0], pts[-1]], "car")
    assert vk_short_from_api({"response": {"key": "abc123"}}) == "https://vk.cc/abc123"
    async def fake_vk_call(method, params, db, bot):
        assert method == "utils.getShortLink" and params == {"url": "https://yandex.ru/maps/"}
        assert db is None and bot is None
        return {"response": {"key": "abc123"}}
    assert asyncio.run(vk_shorten_with_existing_adapter("https://yandex.ru/maps/", fake_vk_call)) == "https://vk.cc/abc123"
    assert "tab/bicycle/points/" in gis2_url([(pts[0][0], pts[0][1], "123"),
                                               (pts[-1][0], pts[-1][1], "456")], "bike")
    print("PASS: rl roundtrip, 17 points, approx 599 m, 14-point RDP, 3 modes, VK/2GIS adapters")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rl", default=EXAMPLE_RL, help="Ruler parameter only; default owner's example")
    ap.add_argument("--mode", choices=MODE, default="walk")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return
    pts = read_rl(args.rl)
    reduced = simplify(pts, 1.0)
    nav_indices = [0, 2, 3, 6, 9, 13, 15, 16] if len(pts) == 17 else [0, len(pts) - 1]
    result = {
        "mode": args.mode,
        "ruler_points": len(pts),
        "length_m": round(length_m(pts), 1),
        "start_lon_lat": [float(fmt(v)) for v in pts[0]],
        "finish_lon_lat": [float(fmt(v)) for v in pts[-1]],
        "yandex_ruler": ruler_url(pts, markers=[pts[0], pts[-1]]),
        "yandex_ruler_simple_14": ruler_url(reduced),
        "ruler_simplified_points": len(reduced),
        "yandex_navigation_candidate_unverified": navigation_url([pts[i] for i in nav_indices], args.mode),
        "vk_short": None,  # Requires existing VK API; do not invent a vk.cc link
        "telegram_url": ruler_url(pts, markers=[pts[0], pts[-1]]),
        "points_lon_lat": [[float(fmt(x)), float(fmt(y))] for x, y in pts],
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()