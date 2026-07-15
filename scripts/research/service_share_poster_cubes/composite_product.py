#!/usr/bin/env python3
"""Composite the locked product/brand layer over a Blender base render."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    root = Path(args.bundle_root)
    image = Image.open(args.base).convert("RGBA")
    scale = image.width / 1024
    if image.width != image.height:
        raise RuntimeError("square base render required")

    def font(name: str, size: float):
        return ImageFont.truetype(
            str(root / "fonts" / name),
            max(8, round(size * scale)),
            layout_engine=ImageFont.Layout.RAQM,
        )

    def text(x, y, value, selected_font, fill=(38,31,27,255)):
        draw.text(
            (round(x*scale), round(y*scale)), value,
            font=selected_font, fill=fill, features=["kern"],
        )

    tag_path = root / "brand" / "desktop_tag_exact_960x352.png"
    if not tag_path.exists():
        tag_path = root / "brand" / "desktop_tag_exact_240x88.png"
    tag = Image.open(tag_path).convert("RGBA").resize((round(420*scale),round(154*scale)), Image.Resampling.LANCZOS)
    image.alpha_composite(tag, (round(32*scale),round(-2*scale)))
    # Create ImageDraw only after replacing the image buffer with alpha composites;
    # older Kaggle Pillow builds can otherwise draw through a stale pixel access.
    draw = ImageDraw.Draw(image)
    headline = font("Cygre-ExtraBold.ttf",46)
    text(48,205,"НАЙДИТЕ",headline)
    # Cygre ExtraBold's full-size comma has a very deep descender and looked
    # detached at social size. Keep the grammatically required comma, but use
    # an optically smaller Bold glyph positioned against the final Е.
    comma_x = 48 + draw.textlength("НАЙДИТЕ", font=headline, features=["kern"]) / scale + 7
    text(comma_x,220,",",font("Cygre-SemiBold.ttf",30))
    text(48,255,"КУДА ПОЙТИ",font("Cygre-ExtraBold.ttf",46))
    current_count = str(config.get("current_event_count", 274))
    new_count = str(config.get("new_event_count_7d", 75))
    text(46,326,current_count,font("Cygre-ExtraBold.ttf",118))
    text(266,355,"АКТУАЛЬНЫХ",font("Cygre-SemiBold.ttf",28))
    text(266,390,"СОБЫТИЯ",font("Cygre-SemiBold.ttf",28))
    text(50,462,f"+{new_count} НОВЫХ ЗА 7 ДНЕЙ",font("Cygre-SemiBold.ttf",25),fill=(152,64,31,255))
    x1,x2,y = 48,438,532
    draw.line((round(x1*scale),round(y*scale),round(x2*scale),round(y*scale)),fill=(85,73,65,160),width=max(1,round(scale)))
    text(48,551,"СЕГОДНЯ · ЗАВТРА · ВЫХОДНЫЕ",font("Cygre-SemiBold.ttf",21))
    text(48,590,"С ДЕТЬМИ · БЕСПЛАТНО · УМНЫЙ ПОИСК",font("Cygre-SemiBold.ttf",19),fill=(84,72,64,230))
    y=826
    draw.line((round(48*scale),round(y*scale),round(408*scale),round(y*scale)),fill=(152,64,31,255),width=max(1,round(2*scale)))
    text(48,846,"СМОТРЕТЬ СОБЫТИЯ",font("Cygre-Bold.ttf",20),fill=(76,61,52,255))
    text(48,876,"kenigevents.ru",font("Cygre-Bold.ttf",40))

    city_header = font("Cygre-SemiBold.ttf",17)
    city_font = font("Cygre-SemiBold.ttf",13)
    text(530,82,"15 ГОРОДОВ И ПОСЁЛКОВ",city_header,fill=(75,68,63,195))
    for city_y, city_line in zip(
        (119, 153, 187, 221),
        (
            "КАЛИНИНГРАД · СВЕТЛОГОРСК · ЗЕЛЕНОГРАДСК",
            "ГУСЕВ · РОМАНОВО · БАЛТИЙСК · ГВАРДЕЙСК",
            "СОВЕТСК · ЯНТАРНЫЙ · ГУРЬЕВСК · ПИОНЕРСКИЙ",
            "ПОЛЕССК · ЗНАМЕНСК · ВЕСЕЛОВКА · УШАКОВО",
        ),
    ):
        text(530,city_y,city_line,city_font,fill=(87,79,73,175))
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(args.output)
    print(json.dumps({"ok":True,"output":args.output,"global_date":False},ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
