"""Telegram custom-emoji medallion selection and HTML rendering."""
from __future__ import annotations

import html
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

ALT_DEFAULT = "🟧"
ROWS_DEFAULT = 4
COLS_DEFAULT = 4
SEP_DEFAULT = "\u200a"
MAX_MEDALLIONS_DEFAULT = 2
DISABLED_SLUGS = {
    "rostec-arena", "signal", "locostandup", "ruin-keepers",
    "meow-afisha", "kaliningrad-art-museum",
}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("ё", "е").lower()).strip()


_ALIAS_BOUNDARY_RE = re.compile(r"[0-9a-zа-я]", re.IGNORECASE)


def _alias_matches_haystack(alias: str, haystack: str) -> bool:
    """Return true only when alias appears as a standalone token/phrase.

    Telegram medallion aliases may be very short (`ММО`). Plain substring
    matching turns that into false positives inside ordinary words such as
    `программой`, `Эммой`, `фильмом`. Boundaries keep URL/domain aliases and
    multi-word venue names working while preventing short acronym drift.
    """

    if not alias or not haystack:
        return False
    start = haystack.find(alias)
    while start >= 0:
        end = start + len(alias)
        before = haystack[start - 1] if start > 0 else ""
        after = haystack[end] if end < len(haystack) else ""
        if not _ALIAS_BOUNDARY_RE.match(before) and not _ALIAS_BOUNDARY_RE.match(after):
            return True
        start = haystack.find(alias, start + 1)
    return False


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _load_raw_config() -> dict[str, Any]:
    path = os.getenv("TG_MEDALLION_CUSTOM_EMOJI_PATH", "").strip()
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    raw = os.getenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", "").strip()
    if raw:
        return json.loads(raw)
    return {}


@lru_cache(maxsize=1)
def medallion_config() -> dict[str, Any]:
    data = _load_raw_config()
    if not isinstance(data, dict):
        return {}
    items = data.get("items") or {}
    if isinstance(items, list):
        items = {str(item.get("slug") or ""): item for item in items if isinstance(item, dict)}
    rows = int(data.get("rows") or ROWS_DEFAULT)
    cols = int(data.get("cols") or COLS_DEFAULT)
    clean_items: dict[str, dict[str, Any]] = {}
    for slug, item in dict(items).items():
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or slug).strip()
        if not slug or slug in DISABLED_SLUGS:
            continue
        item_rows = int(item.get("rows") or rows)
        item_cols = int(item.get("cols") or cols)
        if item_rows <= 0 or item_cols <= 0:
            continue
        ids = item.get("emoji_ids") or item.get("document_ids") or []
        flat = [str(x) for row in ids for x in (row if isinstance(row, list) else [row])]
        if len(flat) < item_rows * item_cols:
            continue
        aliases = item.get("aliases") or []
        alias_values = [item.get("name"), item.get("label"), item.get("short_name"), *aliases]
        clean_items[slug] = {
            **item,
            "slug": slug,
            "rows": item_rows,
            "cols": item_cols,
            "emoji_ids_flat": flat[: item_rows * item_cols],
            "aliases_norm": sorted({_norm(a) for a in alias_values if _norm(a)}, key=len, reverse=True),
            "priority": int(item.get("priority") or 100),
        }
    return {
        "enabled": bool(data) and _truthy_env("TG_MEDALLIONS_ENABLED", True),
        "alt": str(data.get("alt") or ALT_DEFAULT),
        "rows": rows,
        "cols": cols,
        "separator": str(data.get("separator") or SEP_DEFAULT),
        "max_medallions": min(2, int(data.get("max_medallions") or MAX_MEDALLIONS_DEFAULT)),
        "items": clean_items,
    }


def reset_medallion_config_cache() -> None:
    medallion_config.cache_clear()


def _event_haystack(event: Any) -> str:
    parts: list[str] = []
    for attr in (
        "title", "description", "short_description", "search_digest", "festival",
        "location_name", "location_address", "city", "ticket_link", "source_post_url",
        "source_vk_post_url", "tg_source_author",
    ):
        value = getattr(event, attr, None)
        if value:
            parts.append(str(value))
    raw_source_texts = getattr(event, "source_texts", None)
    if isinstance(raw_source_texts, str):
        parts.append(raw_source_texts)
    elif isinstance(raw_source_texts, (list, tuple)):
        parts.extend(str(item or "") for item in raw_source_texts)
    return _norm(" | ".join(parts))


def _event_location_haystack(event: Any) -> str:
    parts: list[str] = []
    for attr in ("location_name", "location_address", "city"):
        value = getattr(event, attr, None)
        if value:
            parts.append(str(value))
    return _norm(" | ".join(parts))


def _event_identity_haystack(event: Any) -> str:
    parts: list[str] = []
    for attr in ("title", "festival", "source_post_url", "source_vk_post_url", "tg_source_author"):
        value = getattr(event, attr, None)
        if value:
            parts.append(str(value))
    return _norm(" | ".join(parts))


def resolve_event_medallions(event: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
    cfg = medallion_config()
    if not cfg.get("enabled"):
        return []
    items: dict[str, dict[str, Any]] = cfg.get("items") or {}
    if not items:
        return []
    max_items = min(2, int(limit or cfg.get("max_medallions") or MAX_MEDALLIONS_DEFAULT))
    haystack = _event_haystack(event)
    location_haystack = _event_location_haystack(event)
    identity_haystack = _event_identity_haystack(event)
    selected: dict[str, dict[str, Any]] = {}

    def add(slug: str, reason: str, priority_boost: int = 0) -> None:
        item = items.get(slug)
        if not item:
            return
        current = dict(item)
        current["reason"] = reason
        current["effective_priority"] = int(item.get("priority") or 100) + priority_boost
        selected[slug] = current

    if getattr(event, "pushkin_card", False):
        add("pushkin-card", "pushkin_card", -100)

    kgd80_signal = "80 истор" in haystack and "главн" in haystack
    suppress_alias_slugs: set[str] = set()
    if kgd80_signal:
        if "kgd80-znanie" in items:
            add("kgd80-znanie", "kgd80_znanie_curated_pair", -40)
            suppress_alias_slugs.update({"kgd80", "kgd80-80-stories", "znanie-russia"})
        else:
            add("kgd80", "kgd80_curated", -40)
            add("kgd80-80-stories", "kgd80_curated", -40)
            add("znanie-russia", "kgd80_curated_partner", -35)

    identity_alias_slugs = {"znanie-russia"}
    for slug, item in items.items():
        if slug in selected or slug in suppress_alias_slugs:
            continue
        aliases = item.get("aliases_norm") or []
        match_scope = str(item.get("match_scope") or ("identity" if slug in identity_alias_slugs else "location")).strip().lower()
        alias_haystack = identity_haystack if match_scope == "identity" else location_haystack
        if aliases and alias_haystack and any(_alias_matches_haystack(alias, alias_haystack) for alias in aliases):
            add(slug, f"{match_scope}_alias_match")

    ordered = sorted(
        selected.values(),
        key=lambda item: (int(item.get("effective_priority") or item.get("priority") or 100), str(item.get("slug") or "")),
    )
    return ordered[:max_items]


def render_medallion_html_block(medallions: list[dict[str, Any]] | None = None) -> str:
    cfg = medallion_config()
    if not cfg.get("enabled") or not medallions:
        return ""
    default_rows = int(cfg.get("rows") or ROWS_DEFAULT)
    default_cols = int(cfg.get("cols") or COLS_DEFAULT)
    alt = html.escape(str(cfg.get("alt") or ALT_DEFAULT))
    sep = html.escape(str(cfg.get("separator") or SEP_DEFAULT))
    # A three-wide 4x4 row wraps on narrow Telegram mobile clients. Do not
    # crop logos; keep each medallion full-size and split 3 medallions into
    # two visual rows: 2 medallions above, 1 medallion below. Two-medallion
    # posts keep the original one-row layout and separator.
    groups = [medallions]
    if len(medallions) >= 3:
        groups = [medallions[:2], medallions[2:]]
    lines: list[str] = []
    for group in groups:
        group_rows = max((int(med.get("rows") or default_rows) for med in group), default=default_rows)
        for row in range(group_rows):
            parts: list[str] = []
            for med in group:
                ids = med.get("emoji_ids_flat") or []
                rows = int(med.get("rows") or default_rows)
                cols = int(med.get("cols") or default_cols)
                cells: list[str] = []
                if row >= rows:
                    parts.append("")
                    continue
                for col in range(cols):
                    idx = row * cols + col
                    if idx >= len(ids):
                        continue
                    emoji_id = html.escape(str(ids[idx]), quote=True)
                    cells.append(f'<tg-emoji emoji-id="{emoji_id}">{alt}</tg-emoji>')
                parts.append("".join(cells))
            lines.append(sep.join(parts))
    return "\n".join(lines) + "\n⠀"


def event_medallion_html_block(event: Any, *, limit: int | None = None) -> str:
    return render_medallion_html_block(resolve_event_medallions(event, limit=limit))
