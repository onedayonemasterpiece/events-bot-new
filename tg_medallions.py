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
MAX_MEDALLIONS_DEFAULT = 3
DISABLED_SLUGS = {
    "rostec-arena", "signal", "locostandup", "ruin-keepers",
    "meow-afisha", "kaliningrad-art-museum",
}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("ё", "е").lower()).strip()


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
        ids = item.get("emoji_ids") or item.get("document_ids") or []
        flat = [str(x) for row in ids for x in (row if isinstance(row, list) else [row])]
        if len(flat) < rows * cols:
            continue
        aliases = item.get("aliases") or []
        alias_values = [item.get("name"), item.get("label"), item.get("short_name"), *aliases]
        clean_items[slug] = {
            **item,
            "slug": slug,
            "emoji_ids_flat": flat[: rows * cols],
            "aliases_norm": sorted({_norm(a) for a in alias_values if _norm(a)}, key=len, reverse=True),
            "priority": int(item.get("priority") or 100),
        }
    return {
        "enabled": bool(data) and _truthy_env("TG_MEDALLIONS_ENABLED", True),
        "alt": str(data.get("alt") or ALT_DEFAULT),
        "rows": rows,
        "cols": cols,
        "separator": str(data.get("separator") or SEP_DEFAULT),
        "max_medallions": int(data.get("max_medallions") or MAX_MEDALLIONS_DEFAULT),
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


def resolve_event_medallions(event: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
    cfg = medallion_config()
    if not cfg.get("enabled"):
        return []
    items: dict[str, dict[str, Any]] = cfg.get("items") or {}
    if not items:
        return []
    max_items = int(limit or cfg.get("max_medallions") or MAX_MEDALLIONS_DEFAULT)
    haystack = _event_haystack(event)
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
    if kgd80_signal:
        add("kgd80", "kgd80_curated", -40)
        add("kgd80-80-stories", "kgd80_curated", -40)
        add("znanie-russia", "kgd80_curated_partner", -35)

    for slug, item in items.items():
        if slug in selected:
            continue
        aliases = item.get("aliases_norm") or []
        if aliases and any(alias in haystack for alias in aliases):
            add(slug, "alias_match")

    ordered = sorted(
        selected.values(),
        key=lambda item: (int(item.get("effective_priority") or item.get("priority") or 100), str(item.get("slug") or "")),
    )
    return ordered[:max_items]


def render_medallion_html_block(medallions: list[dict[str, Any]] | None = None) -> str:
    cfg = medallion_config()
    if not cfg.get("enabled") or not medallions:
        return ""
    rows = int(cfg.get("rows") or ROWS_DEFAULT)
    cols = int(cfg.get("cols") or COLS_DEFAULT)
    alt = html.escape(str(cfg.get("alt") or ALT_DEFAULT))
    sep = html.escape(str(cfg.get("separator") or SEP_DEFAULT))
    lines: list[str] = []
    for row in range(rows):
        parts: list[str] = []
        for med in medallions:
            ids = med.get("emoji_ids_flat") or []
            cells: list[str] = []
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
