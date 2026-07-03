#!/usr/bin/env python3
"""Generate, upload, send, and analyze Telegram custom-emoji calibration probes.

This tool is intentionally deterministic: the generated 100x100 assets encode
source pixel coordinates as colors, so a screenshot of several custom emoji rows
can be mapped back to the source pixels that Telegram actually displayed.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception as exc:  # pragma: no cover - dependency preflight
    Image = None  # type: ignore[assignment]
    ImageDraw = None  # type: ignore[assignment]
    ImageFont = None  # type: ignore[assignment]
    _PIL_IMPORT_ERROR = exc
else:
    _PIL_IMPORT_ERROR = None

FALLBACK = "🧩"
DEFAULT_SHORT_NAME = "kenigeventspack"
PROBE_SIZE = 100

Y_PALETTE: list[tuple[int, int, int]] = [
    # 100 high-saturation colors; deterministic and far enough for screenshot matching.
    ((37 * y + 23) % 256, (91 * y + 71) % 256, (53 * y + 151) % 256)
    for y in range(PROBE_SIZE)
]
X_PALETTE: list[tuple[int, int, int]] = [
    ((83 * x + 17) % 256, (47 * x + 113) % 256, (109 * x + 31) % 256)
    for x in range(PROBE_SIZE)
]


def _require_pillow() -> None:
    if _PIL_IMPORT_ERROR is not None:
        raise SystemExit(
            "Pillow is required. Install project requirements or run: "
            "python3 -m pip install Pillow\n"
            f"Import error: {_PIL_IMPORT_ERROR}"
        )


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _urlsafe_b64decode_text(value: str) -> str:
    padded = value + ("=" * (-len(value) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


def _telethon_config() -> tuple[int, str, str, dict[str, str], str]:
    api_id = int(os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "0")
    api_hash = os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or ""
    session = (os.getenv("TELEGRAM_SESSION") or "").strip()
    auth_scope = "TELEGRAM_SESSION" if session else ""
    kwargs: dict[str, str] = {}
    bundle_raw = (os.getenv("TELEGRAM_AUTH_BUNDLE_E2E") or "").strip()
    if bundle_raw:
        bundle = json.loads(_urlsafe_b64decode_text(bundle_raw))
        session = str(bundle.get("session") or "").strip()
        auth_scope = "TELEGRAM_AUTH_BUNDLE_E2E"
        for key in ("device_model", "system_version", "app_version", "lang_code", "system_lang_code"):
            if bundle.get(key):
                kwargs[key] = str(bundle[key])
    if not api_id or not api_hash or not session:
        raise SystemExit(
            "Missing Telegram human-client env: need TG_API_ID/TG_API_HASH or "
            "TELEGRAM_API_ID/TELEGRAM_API_HASH plus TELEGRAM_AUTH_BUNDLE_E2E or TELEGRAM_SESSION."
        )
    return api_id, api_hash, session, kwargs, auth_scope


def _nearest_index(color: tuple[int, int, int], palette: Sequence[tuple[int, int, int]]) -> tuple[int, float]:
    best_i = 0
    best_d = float("inf")
    r, g, b = color
    for i, (pr, pg, pb) in enumerate(palette):
        d = (r - pr) ** 2 + (g - pg) ** 2 + (b - pb) ** 2
        if d < best_d:
            best_i = i
            best_d = d
    return best_i, math.sqrt(best_d)


def _font(size: int = 12):
    assert ImageFont is not None
    for candidate in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ):
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size=size)
    return ImageFont.load_default()


def _write_image(im: "Image.Image", out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix.lower() == ".webp":
        im.save(out, format="WEBP", lossless=True, quality=100, method=6)
    else:
        im.save(out, format="PNG")


def _draw_crosshair(draw: "ImageDraw.ImageDraw") -> None:
    # Thin neutral grid; source colors remain visible in most pixels.
    for p in range(10, 100, 10):
        draw.line([(p, 0), (p, 99)], fill=(255, 255, 255, 150), width=1)
        draw.line([(0, p), (99, p)], fill=(0, 0, 0, 150), width=1)
    draw.rectangle([(0, 0), (99, 99)], outline=(255, 255, 255, 255), width=1)
    draw.rectangle([(1, 1), (98, 98)], outline=(0, 0, 0, 255), width=1)


def make_y_stripes() -> "Image.Image":
    assert Image is not None and ImageDraw is not None
    im = Image.new("RGBA", (100, 100), (0, 0, 0, 0))
    px = im.load()
    for y, color in enumerate(Y_PALETTE):
        for x in range(100):
            px[x, y] = (*color, 255)
    # Keep this probe pure: every source Y pixel maps to exactly one color row.
    # Use edge_probe/tile_* for human-readable borders and labels.
    return im


def make_x_stripes() -> "Image.Image":
    assert Image is not None and ImageDraw is not None
    im = Image.new("RGBA", (100, 100), (0, 0, 0, 0))
    px = im.load()
    for x, color in enumerate(X_PALETTE):
        for y in range(100):
            px[x, y] = (*color, 255)
    # Keep this probe pure: every source Y pixel maps to exactly one color row.
    # Use edge_probe/tile_* for human-readable borders and labels.
    return im


def make_edge_probe() -> "Image.Image":
    assert Image is not None and ImageDraw is not None
    im = Image.new("RGBA", (100, 100), (30, 30, 30, 255))
    draw = ImageDraw.Draw(im, "RGBA")
    for i in range(0, 100, 4):
        fill = (255, 255, 255, 255) if (i // 4) % 2 == 0 else (0, 0, 0, 255)
        draw.rectangle([(i, 10), (min(i + 3, 99), 89)], fill=fill)
    draw.rectangle([(0, 0), (99, 99)], outline=(255, 0, 255, 255), width=1)
    draw.rectangle([(1, 1), (98, 98)], outline=(0, 255, 255, 255), width=1)
    draw.rectangle([(2, 2), (97, 97)], outline=(255, 255, 0, 255), width=1)
    draw.line([(0, 0), (99, 0)], fill=(255, 0, 0, 255), width=4)
    draw.line([(0, 99), (99, 99)], fill=(0, 0, 255, 255), width=4)
    draw.line([(0, 0), (0, 99)], fill=(0, 255, 0, 255), width=4)
    draw.line([(99, 0), (99, 99)], fill=(255, 128, 0, 255), width=4)
    draw.text((21, 42), "EDGE", fill=(255, 255, 255, 255), font=_font(13), stroke_width=2, stroke_fill=(0, 0, 0, 255))
    return im


def make_tile(row: int, col: int) -> "Image.Image":
    assert Image is not None and ImageDraw is not None
    im = Image.new("RGBA", (100, 100), (0, 0, 0, 0))
    px = im.load()
    tile_shift = row * 29 + col * 47
    for y, base in enumerate(Y_PALETTE):
        # Mostly horizontal rows so vertical source mapping stays easy, but shifted per tile.
        color = tuple((channel + tile_shift) % 256 for channel in base)
        for x in range(100):
            if x % 10 == 0 or y % 10 == 0:
                px[x, y] = (255, 255, 255, 255) if ((x + y) // 10) % 2 else (0, 0, 0, 255)
            else:
                px[x, y] = (*color, 255)
    draw = ImageDraw.Draw(im, "RGBA")
    border_colors = [
        (255, 0, 0, 255), (0, 200, 255, 255), (255, 220, 0, 255),
        (0, 255, 80, 255), (255, 0, 200, 255), (255, 120, 0, 255),
        (120, 90, 255, 255), (0, 255, 220, 255), (255, 255, 255, 255),
    ]
    draw.rectangle([(0, 0), (99, 99)], outline=border_colors[row * 3 + col], width=3)
    label = f"{row + 1}{col + 1}"
    draw.text((34, 38), label, fill=(255, 255, 255, 255), font=_font(24), stroke_width=3, stroke_fill=(0, 0, 0, 255))
    return im


def generate(args: argparse.Namespace) -> None:
    _require_pillow()
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = ".webp" if args.format == "webp" else ".png"
    assets: list[dict[str, Any]] = []

    specs: list[tuple[str, str, Any]] = [
        ("y_stripes", "Y-axis 100 one-pixel horizontal stripes", make_y_stripes),
        ("x_stripes", "X-axis 100 one-pixel vertical stripes", make_x_stripes),
        ("edge_probe", "edge sentinel top/bottom/left/right borders", make_edge_probe),
    ]
    for name, description, maker in specs:
        path = out_dir / f"{args.prefix}_{name}{ext}"
        im = maker()
        _write_image(im, path)
        assets.append({"name": name, "description": description, "path": str(path), "alt": FALLBACK})

    for row in range(3):
        for col in range(3):
            name = f"tile_r{row + 1}c{col + 1}"
            path = out_dir / f"{args.prefix}_{name}{ext}"
            _write_image(make_tile(row, col), path)
            assets.append({
                "name": name,
                "description": f"3x3 mosaic tile row={row + 1} col={col + 1}",
                "path": str(path),
                "alt": FALLBACK,
                "row": row + 1,
                "col": col + 1,
            })

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "size": [100, 100],
        "format": args.format,
        "fallback": FALLBACK,
        "assets": assets,
        "telegram_docs": [
            "Official static custom emoji input is 100x100 PNG/WEBP: https://core.telegram.org/stickers",
            "Telegram does not document line-height/overlap rendering metrics; use screenshot calibration.",
        ],
    }
    manifest_path = out_dir / f"{args.prefix}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "manifest": str(manifest_path), "assets": len(assets)}, ensure_ascii=False, indent=2))


async def _upload_asset(client: Any, path: Path, alt: str, text_color: bool) -> tuple[Any, int]:
    from telethon import functions, types, utils

    uploaded = await client.upload_file(str(path))
    media = types.InputMediaUploadedDocument(
        file=uploaded,
        mime_type="image/webp" if path.suffix.lower() == ".webp" else "image/png",
        attributes=[
            types.DocumentAttributeFilename(file_name=path.name),
            types.DocumentAttributeCustomEmoji(
                alt=alt,
                stickerset=types.InputStickerSetEmpty(),
                text_color=True if text_color else None,
            ),
        ],
    )
    result = await client(functions.messages.UploadMediaRequest(peer=await client.get_input_entity("me"), media=media))
    return utils.get_input_document(result.document), int(result.document.id)


async def _upload(args: argparse.Namespace) -> None:
    from telethon import TelegramClient, functions, types
    from telethon.sessions import StringSession

    _load_env_file(args.env_file)
    api_id, api_hash, session, kwargs, auth_scope = _telethon_config()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    selected = set(args.only or [])
    assets = [a for a in manifest["assets"] if not selected or a["name"] in selected]
    if not assets:
        raise SystemExit("No assets selected for upload")
    client = TelegramClient(StringSession(session), api_id, api_hash, **kwargs)
    await client.connect()
    me = await client.get_me()
    uploaded_assets: list[dict[str, Any]] = []
    try:
        for asset in assets:
            path = Path(asset["path"])
            doc, doc_id = await _upload_asset(client, path, asset.get("alt") or FALLBACK, args.text_color)
            await client(functions.stickers.AddStickerToSetRequest(
                stickerset=types.InputStickerSetShortName(short_name=args.short_name),
                sticker=types.InputStickerSetItem(
                    document=doc,
                    emoji=asset.get("alt") or FALLBACK,
                    keywords=f"kenigevents calibration {asset['name']}",
                ),
            ))
            new_asset = dict(asset)
            new_asset["document_id"] = doc_id
            uploaded_assets.append(new_asset)
        verify = await client(functions.messages.GetStickerSetRequest(
            stickerset=types.InputStickerSetShortName(short_name=args.short_name), hash=0
        ))
    finally:
        await client.disconnect()
    # The temporary document id returned by messages.UploadMediaRequest is not
    # always the final custom-emoji document id accepted by the sticker set.
    # For sending MessageEntityCustomEmoji, use the ids reread from GetStickerSet.
    final_docs = list(verify.documents)[-len(uploaded_assets):] if uploaded_assets else []
    if len(final_docs) == len(uploaded_assets):
        for asset, doc in zip(uploaded_assets, final_docs):
            asset["uploaded_media_document_id"] = asset.get("document_id")
            asset["document_id"] = int(doc.id)

    receipt = {
        "ok": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "short_name": args.short_name,
        "url": f"https://t.me/addemoji/{args.short_name}",
        "pack_title": verify.set.title,
        "pack_count": verify.set.count,
        "pack_documents_len": len(verify.documents),
        "auth_scope": auth_scope,
        "account_id": int(me.id),
        "account_username": getattr(me, "username", None),
        "assets": uploaded_assets,
        "propagation_note": "Telegram clients may need up to ~1 hour before newly added emoji render everywhere.",
    }
    out_path = args.out or args.manifest.with_name(args.manifest.stem.replace("_manifest", "") + "_upload_receipt.json")
    out_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "receipt": str(out_path), "uploaded": len(uploaded_assets), "pack_count": verify.set.count}, ensure_ascii=False, indent=2))


def upload(args: argparse.Namespace) -> None:
    asyncio.run(_upload(args))


@dataclass
class _MessageBuilder:
    text: str
    entities: list[Any]

    def append(self, s: str) -> None:
        self.text += s

    def emoji(self, document_id: int, fallback: str = FALLBACK) -> None:
        from telethon.helpers import add_surrogate
        from telethon.tl.types import MessageEntityCustomEmoji

        offset = len(add_surrogate(self.text))
        length = len(add_surrogate(fallback))
        self.text += fallback
        self.entities.append(MessageEntityCustomEmoji(offset=offset, length=length, document_id=int(document_id)))


def _asset_by_name(receipt: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {asset["name"]: asset for asset in receipt["assets"]}


def _build_smoke_message(receipt: dict[str, Any]) -> _MessageBuilder:
    by_name = _asset_by_name(receipt)
    missing = [name for name in ["y_stripes", "x_stripes", "edge_probe"] + [f"tile_r{r}c{c}" for r in range(1, 4) for c in range(1, 4)] if name not in by_name]
    if missing:
        raise SystemExit(f"Receipt is missing required assets: {', '.join(missing)}")
    b = _MessageBuilder(text="", entities=[])
    b.append("TG_EMOJI_CALIBRATION_SMOKE\n")
    b.append("Crop/analyze the emoji blocks below, not this header.\n\n")
    b.append("Y_STRIPES_3_ROWS\n")
    for row in range(3):
        for _col in range(3):
            b.emoji(by_name["y_stripes"]["document_id"])
        b.append("\n")
    b.append("\nX_STRIPES_1_ROW\n")
    for _col in range(3):
        b.emoji(by_name["x_stripes"]["document_id"])
    b.append("\n\nEDGE_3_ROWS\n")
    for _row in range(3):
        for _col in range(3):
            b.emoji(by_name["edge_probe"]["document_id"])
        b.append("\n")
    b.append("\nTILE_3X3\n")
    for row in range(1, 4):
        for col in range(1, 4):
            b.emoji(by_name[f"tile_r{row}c{col}"]["document_id"])
        b.append("\n")
    b.append("\nPropagation note: if these still show as fallback 🧩, wait and reopen Telegram.")
    return b


async def _send(args: argparse.Namespace) -> None:
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    _load_env_file(args.env_file)
    api_id, api_hash, session, kwargs, auth_scope = _telethon_config()
    receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    b = _build_smoke_message(receipt)
    client = TelegramClient(StringSession(session), api_id, api_hash, **kwargs)
    await client.connect()
    me = await client.get_me()
    try:
        msg = await client.send_message(args.chat, b.text, formatting_entities=b.entities)
        # Verify by rereading a small window from the target chat without exposing secrets.
        recent = [m async for m in client.iter_messages(args.chat, limit=8)]
        found = any(int(getattr(m, "id", 0)) == int(msg.id) for m in recent)
    finally:
        await client.disconnect()
    out = {
        "ok": bool(found),
        "chat": args.chat,
        "sent_message_id": int(msg.id),
        "auth_scope": auth_scope,
        "account_id": int(me.id),
        "account_username": getattr(me, "username", None),
        "custom_emoji_entities": len(b.entities),
        "text_chars": len(b.text),
    }
    if args.out:
        args.out.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))


def send(args: argparse.Namespace) -> None:
    asyncio.run(_send(args))


def _parse_crop(raw: str | None) -> tuple[int, int, int, int] | None:
    if not raw:
        return None
    parts = [int(p.strip()) for p in raw.replace("x", ",").split(",") if p.strip()]
    if len(parts) != 4:
        raise SystemExit("--crop must be x,y,w,h")
    return parts[0], parts[1], parts[2], parts[3]


def _sample_visible_indices(cell: "Image.Image", axis: str, max_distance: float, sample_margin: float) -> dict[str, Any]:
    palette = Y_PALETTE if axis == "y" else X_PALETTE
    rgb = cell.convert("RGB")
    width, height = rgb.size
    counts: dict[int, int] = {}
    total = 0
    # Optionally ignore the outer part of the crop cell if the screenshot crop includes gutters.
    margin = max(0.0, min(0.45, float(sample_margin)))
    x0 = max(0, int(width * margin)); x1 = min(width, int(width * (1.0 - margin)))
    y0 = max(0, int(height * margin)); y1 = min(height, int(height * (1.0 - margin)))
    step = max(1, min(width, height) // 160)
    for yy in range(y0, y1, step):
        for xx in range(x0, x1, step):
            idx, dist = _nearest_index(rgb.getpixel((xx, yy)), palette)
            if dist <= max_distance:
                counts[idx] = counts.get(idx, 0) + 1
                total += 1
    if not counts:
        return {"matched_pixels": 0, "visible_min": None, "visible_max": None, "histogram": {}}
    # Drop tiny outliers from interpolation by requiring at least 0.25% of matched samples or 3 px.
    threshold = max(3, int(total * 0.0025))
    visible = [idx for idx, count in counts.items() if count >= threshold]
    if not visible:
        visible = list(counts)
    return {
        "matched_pixels": total,
        "visible_min": min(visible),
        "visible_max": max(visible),
        "coverage": len(visible),
        "top_counts": dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:12]),
    }


def analyze(args: argparse.Namespace) -> None:
    _require_pillow()
    assert Image is not None
    im = Image.open(args.screenshot).convert("RGBA")
    crop = _parse_crop(args.crop)
    if crop:
        x, y, w, h = crop
        im = im.crop((x, y, x + w, y + h))
    width, height = im.size
    rows, cols = args.rows, args.cols
    results: list[dict[str, Any]] = []
    for row in range(rows):
        for col in range(cols):
            left = round(col * width / cols)
            right = round((col + 1) * width / cols)
            top = round(row * height / rows)
            bottom = round((row + 1) * height / rows)
            cell = im.crop((left, top, right, bottom))
            axis = args.axis
            results.append({
                "row": row + 1,
                "col": col + 1,
                "box_in_crop": [left, top, right - left, bottom - top],
                **_sample_visible_indices(cell, axis, args.max_distance, args.sample_margin),
            })
    mins = [r["visible_min"] for r in results if r["visible_min"] is not None]
    maxs = [r["visible_max"] for r in results if r["visible_max"] is not None]
    safe = None
    if mins and maxs:
        # Conservative source-pixel range seen across every cell.
        safe = [max(mins), min(maxs)]
    report = {
        "ok": bool(results),
        "screenshot": str(args.screenshot),
        "analyzed_size": [width, height],
        "grid": [rows, cols],
        "axis": args.axis,
        "safe_source_pixel_range_intersection": safe,
        "interpretation": (
            "If safe range is e.g. [10, 89], source pixels outside that axis should be empty/background "
            "for a multi-line custom-emoji mosaic on this Telegram client/zoom."
        ),
        "cells": results,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    gen = sub.add_parser("generate", help="Generate 100x100 calibration PNG/WebP assets")
    gen.add_argument("--out-dir", type=Path, default=Path("artifacts/codex/tg-emoji-calibration/generated"))
    gen.add_argument("--prefix", default="tgcal")
    gen.add_argument("--format", choices=["png", "webp"], default="png")
    gen.set_defaults(func=generate)

    upl = sub.add_parser("upload", help="Add generated assets to an existing custom emoji pack")
    upl.add_argument("--env-file", type=Path, default=Path(".env"))
    upl.add_argument("--manifest", type=Path, required=True)
    upl.add_argument("--short-name", default=DEFAULT_SHORT_NAME)
    upl.add_argument("--text-color", action="store_true")
    upl.add_argument("--only", action="append", help="Asset name to upload; repeatable. Defaults to all.")
    upl.add_argument("--out", type=Path)
    upl.set_defaults(func=upload)

    snd = sub.add_parser("send-smoke", help="Send the calibration blocks as custom emoji entities")
    snd.add_argument("--env-file", type=Path, default=Path(".env"))
    snd.add_argument("--receipt", type=Path, required=True)
    snd.add_argument("--chat", default="me")
    snd.add_argument("--out", type=Path)
    snd.set_defaults(func=send)

    ana = sub.add_parser("analyze-screenshot", help="Analyze a cropped screenshot of a calibration block")
    ana.add_argument("screenshot", type=Path)
    ana.add_argument("--crop", help="Optional x,y,w,h crop before analysis")
    ana.add_argument("--rows", type=int, default=3)
    ana.add_argument("--cols", type=int, default=3)
    ana.add_argument("--axis", choices=["y", "x"], default="y")
    ana.add_argument("--max-distance", type=float, default=42.0)
    ana.add_argument("--sample-margin", type=float, default=0.0, help="Fraction of each grid cell edge to ignore before color matching, e.g. 0.08")
    ana.add_argument("--out", type=Path)
    ana.set_defaults(func=analyze)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
