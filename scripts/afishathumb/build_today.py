"""Round-11 today-orchestrator.

Reads `artifacts/afishathumb/selection_today.json` (output of the
CherryFlash-style selector), downloads each event's posters, runs the
per-event bento composer, and writes per-event flat previews under
`artifacts/afishathumb/bento/<event_id>/preview.png`. Then it asks the
column composer (round-11 pkg-2.3) to lay all event-blocks plus the
excursion flythrough afishas onto the cylinder.

For round-11 pkg-2 first commit: only the per-event flat previews and a
combined contact-sheet view; the column LLM-C + flythrough hook lands
in the next commit on top of this orchestrator.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.request
from pathlib import Path
from typing import Optional

from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.afishathumb.bento_preview import render_bento_preview
from scripts.afishathumb.bento_slot import BentoSlot, compose_bento
from scripts.afishathumb.column_bento import (apply_placements, build_tiles,
                                               grid_summary, pack)
from scripts.afishathumb.flythrough import Flythrough, build_flythrough
from scripts.afishathumb.mercator_v2 import render_mercator

# Up to 6 EVENTS the camera dwells on; if CherryFlash returns more,
# the surplus become ambient flythrough afishas without stickers.
TARGET_COUNT_DEFAULT = 6

MONTHS_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _strip_leading_emoji(s: str) -> str:
    return re.sub(r"^[\U0001F000-\U0001FFFF☀-➿︎️‍\s]+",
                  "", s).strip()


def _parse_date(iso: str) -> tuple[str, str]:
    """Returns `(day_text, month_text)` for an ISO date like
    `2026-05-16`."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", iso or "")
    if not m:
        return "", ""
    day = str(int(m.group(3)))
    month = MONTHS_RU.get(int(m.group(2)), "")
    return day, month


def _format_price(ev: dict) -> str:
    pmin = ev.get("price_min")
    pmax = ev.get("price_max")
    if pmin is None and pmax is None:
        return ""
    if pmin and pmax and pmin != pmax:
        return f"{pmin}–{pmax} ₽"
    return f"{pmin or pmax} ₽"


def _download(url: str, dest: Path) -> Optional[Path]:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
        dest.write_bytes(data)
        return dest
    except Exception as exc:  # noqa: BLE001
        print(f"[build_today] download failed {url}: {exc!r}", file=sys.stderr)
        return None


def _image_aspect(path: Path) -> float:
    try:
        im = Image.open(path)
        return im.width / im.height
    except Exception:
        return 0.707  # safe portrait default


def _format_location(name: str) -> str:
    """Returns the location string with parenthesised qualifiers
    stripped but comma-separated parts preserved — operator round-11c
    wants venue + address + city all visible. Parts are rendered on
    separate lines downstream."""
    if not name:
        return ""
    cleaned = re.sub(r"\s*\(.*?\)\s*", " ", name).strip()
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    return ", ".join(parts)


def _truncate_digest(text: str, max_words: int = 22) -> str:
    if not text:
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "…"


def build_one(ev: dict, out_root: Path) -> tuple[dict, BentoSlot | None]:
    event_id = int(ev["id"])
    ev_dir = out_root / f"{event_id}"
    ev_dir.mkdir(parents=True, exist_ok=True)

    # download up to 5 photos: 1 primary + 4 extras (operator round-11c
    # wants the 4th poster on a next row, not off-canvas).
    poster_paths: list[Path] = []
    for i, url in enumerate(ev.get("photo_urls", [])[:5]):
        ext = Path(url.split("?")[0]).suffix.lower() or ".jpg"
        if ext not in (".jpg", ".jpeg", ".png", ".webp"):
            ext = ".jpg"
        dest = ev_dir / (f"poster{ext}" if i == 0 else f"extra_{i}{ext}")
        got = _download(url, dest)
        if got is not None:
            poster_paths.append(got)
    if not poster_paths:
        return {"event_id": event_id, "status": "no_poster"}, None

    primary_path = poster_paths[0]
    extras = [(str(p), _image_aspect(p)) for p in poster_paths[1:5]]

    day, month = _parse_date(ev.get("date", ""))
    time_text = ev.get("time", "")
    location_text = _format_location(ev.get("location_name", ""))
    title_text = _strip_leading_emoji(ev.get("title", ""))
    is_free = bool(ev.get("is_free"))
    price_text = "" if is_free else _format_price(ev)
    digest = _truncate_digest(ev.get("search_digest", ""))

    slot = compose_bento(
        event_id=event_id,
        primary_image_path=str(primary_path),
        primary_aspect=_image_aspect(primary_path),
        title_text=title_text,
        day_text=day,
        month_text=month,
        time_text=time_text,
        location_text=location_text,
        is_free=is_free,
        price_text=price_text,
        digest_text=digest,
        extra_image_paths=extras,
    )
    preview_path = ev_dir / "preview.png"
    render_bento_preview(slot, is_free_event=is_free, out_path=preview_path)

    return {
        "event_id": event_id,
        "title": title_text,
        "block_w_d": slot.block_w_d,
        "block_h_d": slot.block_h_d,
        "n_extras": len(extras),
        "preview": str(preview_path.relative_to(REPO_ROOT)),
    }, slot


def build_contact_sheet(per_event: list[dict], out_path: Path) -> None:
    """Stack the per-event flat previews in a grid so the operator can
    eyeball all 6 at once."""
    previews = []
    for r in per_event:
        if "preview" not in r:
            continue
        p = REPO_ROOT / r["preview"]
        if p.exists():
            previews.append((r, Image.open(p)))
    if not previews:
        return
    cols = 3
    rows = (len(previews) + cols - 1) // cols
    cell_w = max(img.width for _, img in previews)
    cell_h = max(img.height for _, img in previews)
    pad = 40
    W = cols * cell_w + (cols + 1) * pad
    H = rows * cell_h + (rows + 1) * pad
    sheet = Image.new("RGB", (W, H), (28, 30, 26))
    for i, (r, img) in enumerate(previews):
        gx = i % cols
        gy = i // cols
        x = pad + gx * (cell_w + pad) + (cell_w - img.width) // 2
        y = pad + gy * (cell_h + pad) + (cell_h - img.height) // 2
        sheet.paste(img, (x, y))
    sheet.save(out_path, "PNG")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection",
                        default="artifacts/afishathumb/selection_today.json")
    args = parser.parse_args()
    selection = json.load(open(args.selection, "r", encoding="utf-8"))
    out_root = REPO_ROOT / "artifacts" / "afishathumb" / "bento"
    out_root.mkdir(parents=True, exist_ok=True)

    per_event: list[dict] = []
    bento_slots: list[BentoSlot] = []
    flythrough_items: list[Flythrough] = []
    # All target-event extras (beyond what's already shown in bento)
    # become ambient flythrough afishas to fill the cylinder grid.
    ambient_sources: list[tuple[int, Path, float]] = []
    target_count = TARGET_COUNT_DEFAULT
    # First `target_count` events from CherryFlash get the full bento;
    # remaining events on the cylinder are flythrough afishas (poster
    # only, no stickers, no camera dwell).
    for idx, ev in enumerate(selection):
        if idx < target_count:
            r, slot = build_one(ev, out_root)
            per_event.append(r)
            if slot is not None:
                bento_slots.append(slot)
            if "block_w_d" in r:
                print(f"target {r['event_id']:5d}  block "
                      f"{r['block_w_d']:.2f}×{r['block_h_d']:.2f} D  "
                      f"extras={r['n_extras']}")
            else:
                print(f"target {r['event_id']:5d}  STATUS {r.get('status')}")
            # Collect target's extra-photos for AMBIENT use elsewhere
            # on the cylinder. These extras are already in the
            # target's bento at extras-size; we ALSO place them as
            # ambient afishas on free grid cells so the column reads
            # as a real Litfaßsäule with multiple posters per event.
            ev_dir = out_root / f"{ev['id']}"
            urls = ev.get("photo_urls", [])
            for i, url in enumerate(urls[1:5], start=1):
                ext = Path(url.split("?")[0]).suffix.lower() or ".jpg"
                if ext not in (".jpg", ".jpeg", ".png", ".webp"):
                    ext = ".jpg"
                candidate = ev_dir / f"extra_{i}{ext}"
                if candidate.exists():
                    ambient_sources.append((int(ev["id"]), candidate,
                                             _image_aspect(candidate)))
        else:
            # Non-target event → use ALL its photos, each one placed
            # MULTIPLE times around the cylinder as ambient flythrough
            # afishas. This gives the column real Litfaßsäule density.
            ev_dir = out_root / f"{ev['id']}_flythrough"
            ev_dir.mkdir(parents=True, exist_ok=True)
            urls = ev.get("photo_urls", [])
            local_paths: list[tuple[Path, float]] = []
            for i, url in enumerate(urls[:5]):
                ext = Path(url.split("?")[0]).suffix.lower() or ".jpg"
                if ext not in (".jpg", ".jpeg", ".png", ".webp"):
                    ext = ".jpg"
                dest = ev_dir / (f"poster{ext}" if i == 0 else f"extra_{i}{ext}")
                got = _download(url, dest)
                if got is None:
                    continue
                local_paths.append((got, _image_aspect(got)))
            # Each photo placed REPS times at slightly varied
            # (deterministic-seeded) angle/z. The column packer below
            # will scatter them around to fill gaps.
            REPS = 2
            for rep in range(REPS):
                for i, (path, aspect) in enumerate(local_paths):
                    ft = build_flythrough(int(ev["id"]), str(path), aspect,
                                           rng_seed=f"flythrough:{ev['id']}:{i}:r{rep}")
                    flythrough_items.append(ft)
            print(f"flythrough {ev['id']}: {len(local_paths)} photos × {REPS} reps "
                  f"= {len(local_paths)*REPS} ambient afishas")

    with (out_root / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(per_event, f, ensure_ascii=False, indent=2)
    build_contact_sheet(per_event, out_root / "contact_sheet.png")
    print(f"contact sheet: {out_root / 'contact_sheet.png'}")

    # Round-11 final: REAL global Bento grid packer.
    tiles = build_tiles(bento_slots, flythrough_items)
    placed_tiles, unplaced = pack(tiles)
    apply_placements(placed_tiles)
    print("\nbento grid:")
    print(grid_summary(placed_tiles))
    if unplaced:
        print(f"\nunplaced ({len(unplaced)}):")
        for t in unplaced:
            print(f"  {t.kind} {t.event_id}: footprint {t.cell_w}×{t.cell_h}")
    print(f"\nplaced {len(placed_tiles)} tiles "
          f"({sum(1 for t in placed_tiles if t.kind == 'target')} targets + "
          f"{sum(1 for t in placed_tiles if t.kind == 'flythrough')} flythrough)")
    placement_path = out_root / "column_placement.json"
    placement_path.write_text(json.dumps({
        "tiles": [
            {"kind": t.kind, "event_id": t.event_id,
             "grid_col": t.grid_col, "grid_row": t.grid_row,
             "cell_w": t.cell_w, "cell_h": t.cell_h,
             "angle_deg": (t.grid_col + t.cell_w / 2.0) * (360.0 / 6),
             "z": getattr(t.ref, "anchor_z", 0)}
            for t in placed_tiles
        ]
    }, ensure_ascii=False, indent=2))
    placed_flythrough = [t.ref for t in placed_tiles if t.kind == "flythrough"]

    # Mercator unwrap
    previews_by_id = {
        slot.event_id: out_root / f"{slot.event_id}" / "preview.png"
        for slot in bento_slots
    }
    mercator_path = REPO_ROOT / "artifacts" / "afishathumb" / "mercator_today.png"
    render_mercator(bento_slots, previews_by_id, mercator_path,
                    flythrough=placed_flythrough)
    print(f"mercator: {mercator_path}")


if __name__ == "__main__":
    main()
