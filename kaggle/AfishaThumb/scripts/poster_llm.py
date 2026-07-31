"""Gemini-lite poster region detector for AfishaThumb sticker placement.

Sends an event poster to `gemini-3.1-flash-lite` and asks for the
normalised bounding boxes of five info regions (`title`, `date`, `time`,
`location`, `price`). The placement engine uses these to decide:

  - whether a separate sticker is needed for that info (skipped when
    the LLM confirms the poster already carries it legibly),
  - which on-poster regions are off-limits for sticker overlap,
  - where the camera should LOOK during the relevant beat (focus point
    aligned with where the info actually sits on the poster).

Contract:
  Output JSON example
    {
      "title":    [0.04, 0.07, 0.92, 0.22],
      "date":     [0.62, 0.78, 0.96, 0.86],
      "time":     [0.62, 0.86, 0.96, 0.94],
      "location": null,
      "price":    null
    }
  Coordinates are normalised 0..1, origin top-left.

Fallback:
  If the API is offline / quota-exhausted / returns invalid JSON, the
  caller can use `fallback_regions_from_density(...)` which returns the
  same schema using the cv2 text-mask integral image.

Caching:
  Results are persisted to `slot_<event_id>/poster_regions.json` so
  re-prep does not re-burn LLM quota.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


REGION_KEYS = ("title", "date", "time", "location", "price")
# Use the canonical project Lite model name (per `CHANGELOG.md` entry
# "LLM / Smart Update facts+writer primary moved to gemini-3.1-flash-lite").
DEFAULT_MODEL = "gemini-3.1-flash-lite"
MAX_RETRIES = 3
RETRY_DELAYS_SEC = (0.6, 1.6, 4.0)


@dataclass
class PosterRegions:
    """Parsed LLM response. Each region is either a normalised bbox
    `(x0, y0, x1, y1)` or `None` when the LLM says the region is not
    visibly present on the poster."""
    title: Optional[tuple[float, float, float, float]] = None
    date: Optional[tuple[float, float, float, float]] = None
    time: Optional[tuple[float, float, float, float]] = None
    location: Optional[tuple[float, float, float, float]] = None
    price: Optional[tuple[float, float, float, float]] = None
    source: str = "llm"   # "llm" | "cache" | "cv2_fallback"
    model: str = ""
    raw_response: str = ""

    def to_dict(self) -> dict:
        def _box(b):
            if b is None:
                return None
            return [float(v) for v in b]
        return {
            "title": _box(self.title),
            "date": _box(self.date),
            "time": _box(self.time),
            "location": _box(self.location),
            "price": _box(self.price),
            "source": self.source,
            "model": self.model,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PosterRegions":
        def _box(b):
            if b is None:
                return None
            return tuple(float(v) for v in b)  # type: ignore[return-value]
        return cls(
            title=_box(d.get("title")),
            date=_box(d.get("date")),
            time=_box(d.get("time")),
            location=_box(d.get("location")),
            price=_box(d.get("price")),
            source=str(d.get("source", "cache")),
            model=str(d.get("model", "")),
        )

    def occupied_boxes(self) -> list[tuple[float, float, float, float]]:
        """List of all non-null regions — placements must avoid these."""
        return [b for b in (self.title, self.date, self.time, self.location, self.price) if b is not None]

    def has_legible(self, key: str, min_area: float = 0.012) -> bool:
        """Did the LLM find this info region with at least `min_area`
        normalised area? Tiny regions don't count — likely OCR noise."""
        b = getattr(self, key, None)
        if b is None:
            return False
        return (b[2] - b[0]) * (b[3] - b[1]) >= min_area


_PROMPT = """You are looking at a printed event poster. Locate the following information regions on the poster and return their bounding boxes in normalised coordinates (origin top-left, both x and y in 0..1).

Return ONLY a single JSON object with these exact keys and no other text:

- "title":    the main event name (largest / most prominent name text), or null
- "date":     the day + month (e.g. "15 МАЯ", "May 15", "31 декабря"), or null
- "time":     the start time (e.g. "19:00", "18:30"), or null
- "location": the venue name or street address shown on the poster, or null
- "price":    the ticket cost / "БЕСПЛАТНО" / "FREE" / "0 ₽", or null

Each value is either null (region is not visibly present) or an array of four numbers [x0, y0, x1, y1] in 0..1 normalised coordinates that tightly bound the visible region. Be precise — bounding boxes should hug the text, not include large empty space.

Strict rules:
- Output exactly one JSON object, no markdown fences, no commentary.
- Use null for regions that are not present on the poster.
- Coordinates must be 0..1 floats with x0 < x1 and y0 < y1.
"""


def _call_gemini(image_bytes: bytes, model: str) -> str:
    """Single physical attempt through the mandatory shared gateway."""

    async def _generate() -> str:
        from google_ai import GoogleAIClient, SecretsProvider
        from google_ai.limiter_supabase import (
            build_google_ai_limiter_supabase_client,
        )

        client = GoogleAIClient(
            supabase_client=build_google_ai_limiter_supabase_client(
                require_configured=True,
            ),
            secrets_provider=SecretsProvider(),
            consumer="afishathumb.poster",
            default_env_var_name="GOOGLE_API_KEY",
        )
        client.allow_reserve_fallback = False
        client.allow_local_limiter_fallback = False
        client.allow_local_limiter_on_reserve_error = False
        client.max_retries = 1
        client.fallback_models = []
        response_text, _usage = await client.generate_content_async(
            model=model,
            prompt=[
                {"inline_data": {"mime_type": "image/png", "data": image_bytes}},
                {"text": _PROMPT},
            ],
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.1,
                "max_output_tokens": 512,
            },
            max_output_tokens=512,
        )
        return response_text

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_generate())
    raise RuntimeError("poster_llm synchronous API cannot run inside an active event loop")


def _parse_regions(raw: str) -> PosterRegions:
    """Parse the model's JSON response. Tolerate fenced JSON; reject
    coordinates outside [0,1] or with inverted edges."""
    text = (raw or "").strip()
    if text.startswith("```"):
        # Strip markdown fence if the model added one despite the prompt.
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError(f"expected JSON object, got {type(obj).__name__}")

    def _box(v) -> Optional[tuple[float, float, float, float]]:
        if v is None:
            return None
        if not isinstance(v, (list, tuple)) or len(v) != 4:
            return None
        try:
            x0, y0, x1, y1 = (float(c) for c in v)
        except (TypeError, ValueError):
            return None
        # Gemini vision models return bbox coords in 0..1000 by default,
        # not 0..1 — even when the prompt explicitly asks for 0..1. Detect
        # the scale by the magnitude of the largest coord and normalise.
        max_coord = max(x0, y0, x1, y1)
        if max_coord > 1.5:
            scale = 1000.0 if max_coord <= 1000.5 else max_coord
            x0, y0, x1, y1 = x0 / scale, y0 / scale, x1 / scale, y1 / scale
        x0 = max(0.0, min(1.0, x0))
        x1 = max(0.0, min(1.0, x1))
        y0 = max(0.0, min(1.0, y0))
        y1 = max(0.0, min(1.0, y1))
        if x1 <= x0 or y1 <= y0:
            return None
        return (x0, y0, x1, y1)

    return PosterRegions(
        title=_box(obj.get("title")),
        date=_box(obj.get("date")),
        time=_box(obj.get("time")),
        location=_box(obj.get("location")),
        price=_box(obj.get("price")),
        source="llm",
        raw_response=text,
    )


def detect_regions(
    poster_path: Path,
    *,
    cache_path: Optional[Path] = None,
    model: str = DEFAULT_MODEL,
    force_refresh: bool = False,
) -> PosterRegions:
    """Detect poster info regions via Gemini-lite, with retries + cache.

    On total failure (3 retries exhausted), returns an empty PosterRegions
    with `source="cv2_fallback"`. The caller can then patch in the cv2
    density heuristic via `fallback_regions_from_density(...)`.
    """
    if cache_path is not None and not force_refresh and cache_path.exists():
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                cached = PosterRegions.from_dict(json.load(f))
            cached.source = "cache"
            return cached
        except Exception:
            pass  # malformed cache → re-fetch

    img_bytes = poster_path.read_bytes()

    last_err: Optional[BaseException] = None
    for attempt in range(MAX_RETRIES):
        try:
            raw = _call_gemini(img_bytes, model)
            regions = _parse_regions(raw)
            regions.model = model
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with cache_path.open("w", encoding="utf-8") as f:
                    json.dump(regions.to_dict(), f, ensure_ascii=False, indent=2)
            return regions
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            delay = RETRY_DELAYS_SEC[min(attempt, len(RETRY_DELAYS_SEC) - 1)]
            print(f"[poster_llm] attempt {attempt + 1}/{MAX_RETRIES} failed: {exc!r}; retry in {delay}s")
            time.sleep(delay)

    print(f"[poster_llm] all attempts failed; falling back to cv2 density. last err: {last_err!r}")
    return PosterRegions(source="cv2_fallback", model=model)


def fallback_regions_from_density(analysis) -> PosterRegions:
    """When the LLM is unreachable, synthesise rough regions from the cv2
    text-density mask. We only fill `title` (if the top 30% of the poster
    is dense) — everything else stays null, which means the placement
    engine treats those as "not on poster, sticker required"."""
    from poster_analysis import fill_ratio_at  # noqa: WPS433
    out = PosterRegions(source="cv2_fallback")
    if fill_ratio_at(analysis, 0.04, 0.04, 0.92, 0.28) > 0.20:
        out.title = (0.04, 0.04, 0.96, 0.32)
    return out


__all__ = [
    "PosterRegions",
    "REGION_KEYS",
    "detect_regions",
    "fallback_regions_from_density",
]
