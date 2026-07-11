#!/usr/bin/env python3
"""CherryFlash guide-excursion true3D v4 approved renderer.

Design contract:
- The approved v7/v4 PIL composition is the source of truth for layout,
  typography, date/contact format and SVG Repo icon.
- Blender is used for the depth/material/light layer: matte background discs,
  3D medallion(s), glass CTA substrate, camera-led parallax and moving light.
- This tracked file is the approved production source copied from the Telegram
  artifact run `run_video_20260711_130410`; do not reimplement "по мотивам".
- Crisp product UI is composited as high-resolution PIL overlay so text/icon are
  never degraded by perspective, DOF or tone mapping.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import bpy
import mathutils
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageStat

# ---------------------------------------------------------------------------
# CLI / constants
# ---------------------------------------------------------------------------

BASE_W, BASE_H = 720, 1280
DEFAULT_FPS = 30
DEFAULT_DURATION = 5.90
APPROVED_VERSION = "true3d-v4-approved-2026-07-11"

P0_END = 0.36
T_ENTRY = 0.45
T_HOLD = 1.20
T_MOVE = 0.75
T_INFO = 2.50
T_EXIT = 0.58
P1_END = P0_END + T_ENTRY + T_HOLD
P2_END = P1_END + T_MOVE
P3_START = P2_END + T_INFO

PALETTES = {
    "prussian_cream": {"bg":"#0E5B7B","cream":"#FFF1CF","accent":"#FF6833","ink":"#060708","pill":"#535A62","bubble":"#6F8FA0","line":"#A8C1CB","top":"#CB5D38"},
    "deep_wine_ivory": {"bg":"#2A1024","cream":"#FFF0D3","accent":"#F46D43","ink":"#070607","pill":"#594853","bubble":"#4A3C4B","line":"#85717E","top":"#F46D43"},
    "museum_green_ivory": {"bg":"#12352F","cream":"#FFF2D6","accent":"#F2A23A","ink":"#070807","pill":"#465D56","bubble":"#3C504C","line":"#789087","top":"#D9783D"},
    "black_lime": {"bg":"#111416","cream":"#F4F3DF","accent":"#D6FF3F","ink":"#080909","pill":"#3F4548","bubble":"#33393E","line":"#6F777B","top":"#D6FF3F"},
}

ICON_MASKS = {
    "walk": "walk.mask.png",
    "route": "route.mask.png",
    "water": "water.mask.png",
    "building": "building.mask.png",
}

CONTENT = {
    "title": "История в переплётах: экскурсия по библиотеке БФУ",
    "date_line": "10 июля 16:00",
    "contact": "@amber_fringilla",
    "contact_label": "ЗАПИСЬ В TELEGRAM",
    "footer": "kenigevents • guide promo",
    "icon_kind": "building",
    "palette": "prussian_cream",
    # Two guide avatars deliberately test face-safe overlap.  Final separation
    # is larger than the old v7 overlap so rims intersect but central faces stay
    # outside the occlusion zone.
    "avatars": [
        "guide_excursions/assets/visual_digest_avatars/amber_fringilla.jpg",
        "guide_excursions/assets/visual_digest_avatars/katya_kostyugova.jpg",
    ],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def ease_out_cubic(x: float) -> float:
    x = clamp01(x)
    return 1 - (1 - x) ** 3

def ease_in_cubic(x: float) -> float:
    x = clamp01(x)
    return x ** 3

def ease_in_out_cubic(x: float) -> float:
    x = clamp01(x)
    return 4 * x * x * x if x < 0.5 else 1 - ((-2*x + 2) ** 3) / 2

def smoothstep(x: float) -> float:
    x = clamp01(x)
    return x * x * (3 - 2 * x)

def lerp(a: float, b: float, q: float) -> float:
    return a + (b - a) * q

def hex_rgb(hex_color: str) -> tuple[int, int, int]:
    h = hex_color.strip().lstrip('#')
    return (int(h[:2], 16), int(h[2:4], 16), int(h[4:6], 16))

def hex_rgba(hex_color: str, alpha: float = 255) -> tuple[int, int, int, int]:
    r, g, b = hex_rgb(hex_color)
    return (r, g, b, int(max(0, min(255, alpha))))

def srgb_to_linear(v: float) -> float:
    v = max(0.0, min(1.0, v))
    if v <= 0.04045:
        return v / 12.92
    return ((v + 0.055) / 1.055) ** 2.4

def hex_blender(hex_color: str, alpha: float = 1.0) -> tuple[float, float, float, float]:
    r, g, b = hex_rgb(hex_color)
    return (srgb_to_linear(r/255), srgb_to_linear(g/255), srgb_to_linear(b/255), alpha)

def font(root: Path, rel: str, size: int) -> ImageFont.FreeTypeFont:
    p = root / rel
    if not p.exists():
        raise FileNotFoundError(p)
    return ImageFont.truetype(str(p), size)

def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def load_content(args: argparse.Namespace) -> dict:
    """Load renderer content without changing the approved visual contract."""
    content = dict(CONTENT)
    if getattr(args, "scene_json", None):
        scene_path = Path(args.scene_json).expanduser().resolve()
        override = json.loads(scene_path.read_text(encoding="utf-8"))
        if not isinstance(override, dict):
            raise TypeError(f"--scene-json must contain object, got {type(override).__name__}")
        content.update(override)
    content["palette"] = args.palette or content.get("palette") or "prussian_cream"
    if content["palette"] not in PALETTES:
        content["palette"] = "prussian_cream"
    avatars = content.get("avatars") or []
    if isinstance(avatars, str):
        avatars = [avatars]
    content["avatars"] = [str(item) for item in avatars if str(item).strip()]
    if not content["avatars"]:
        raise ValueError("Guide true3D renderer requires at least one avatar path")
    return content


def resolve_asset_path(root: Path, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return root / path

# ---------------------------------------------------------------------------
# PIL asset preparation and overlay
# ---------------------------------------------------------------------------

def crop_avatar_square(src_path: Path, dst_path: Path, size: int = 1200) -> None:
    with Image.open(src_path).convert("RGB") as src:
        w, h = src.size
        side = min(w, h)
        left = (w - side) // 2
        # Keep the crop close to approved v7, with a mild upward bias that keeps
        # faces inside the central safe circle for common portrait crops.
        top = max(0, min((h - side) // 2, int(h * 0.06)))
        crop = src.crop((left, top, left + side, top + side)).resize((size, size), Image.Resampling.LANCZOS)
        crop.save(dst_path, quality=96)

def build_icon(root: Path, kind: str, size: int, color: tuple[int,int,int]) -> Image.Image:
    mask_path = root / "video_announce" / "assets" / "cherryflash_icons" / ICON_MASKS.get(kind, ICON_MASKS["walk"])
    if not mask_path.exists():
        mask_path = root / "video_announce" / "assets" / "cherryflash_icons" / ICON_MASKS["walk"]
    mask = Image.open(mask_path).convert("L").resize((size, size), Image.Resampling.LANCZOS)
    icon = Image.new("RGBA", (size, size), (*color, 255))
    icon.putalpha(mask)
    return icon

def wrap_title(root: Path, title: str, max_width: int = 650, max_lines: int = 3) -> list[str]:
    text = " ".join(title.split()).upper()
    if not text:
        return ["СКОРО", "ЭКСКУРСИЯ"]
    words = text.split()
    f = font(root, "video_announce/assets/DrukCyr-Super.ttf", 74)
    d = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    lines: list[str] = []
    cur = ""
    for word in words:
        test = (cur + " " + word).strip()
        if not cur or d.textlength(test, font=f) <= max_width:
            cur = test
        else:
            lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    if len(lines) > max_lines:
        best = None
        for a in range(1, len(words)):
            for b in range(a + 1, len(words)):
                cand = [" ".join(words[:a]), " ".join(words[a:b]), " ".join(words[b:])]
                if not all(cand):
                    continue
                widths = [d.textlength(x, font=f) for x in cand]
                if all(w <= max_width for w in widths):
                    score = max(widths) - min(widths)
                    if best is None or score < best[0]:
                        best = (score, cand)
        if best:
            lines = best[1]
        else:
            lines = lines[:max_lines]
            while lines[-1] and d.textlength(lines[-1] + "…", font=f) > max_width:
                lines[-1] = lines[-1][:-1].rstrip()
            lines[-1] += "…"
    return lines[:max_lines]

def scene_times(t: float) -> dict[str, float]:
    # Camera/medallions start moving at encoded frame 1; P0 is a background
    # reveal running concurrently, not a static pre-roll.
    intro_fast_end = P0_END + T_ENTRY
    if t < intro_fast_end:
        p1 = 0.78 * ease_out_cubic(t / intro_fast_end)
    elif t < P1_END:
        p1 = 0.78 + 0.22 * smoothstep((t - intro_fast_end) / (P1_END - intro_fast_end))
    else:
        p1 = 1.0
    p2 = ease_in_out_cubic((t - P1_END) / T_MOVE)
    read = clamp01((t - P2_END) / T_INFO)
    p3 = ease_in_cubic((t - P3_START) / T_EXIT)
    return {"p1": p1, "p2": p2, "read": read, "p3": p3}

def overlay_ui(raw: Image.Image, *, root: Path, t: float, fps: int, content: dict) -> Image.Image:
    pal = PALETTES[content.get("palette", "prussian_cream")]
    # Blender may render the 3D layer above target resolution for cleaner object
    # edges; the product plate itself remains exactly the approved 720x1280.
    if raw.size != (BASE_W, BASE_H):
        raw = raw.resize((BASE_W, BASE_H), Image.Resampling.LANCZOS)
    im = raw.convert("RGBA")
    W, H = im.size
    scale = W / BASE_W
    S = 4  # supersampled overlay for clean edges
    RW, RH = int(W*S), int(H*S)
    layer = Image.new("RGBA", (RW, RH), (0,0,0,0))
    d = ImageDraw.Draw(layer, "RGBA")
    xy = lambda v: int(round(v * scale * S))
    rgba = hex_rgba
    cream = hex_rgb(pal["cream"])
    accent = hex_rgb(pal["accent"])
    ink = hex_rgb(pal["ink"])
    times = scene_times(t)
    p3 = times["p3"]
    read = times["read"]
    reveal = 0.28 + 0.72 * ease_out_cubic((t + 1/fps) / P0_END)

    # Fixed 2D overlay stripe: not part of 3D, never flies, no fake shadow.
    d.rectangle((0, 0, RW, xy(32)), fill=rgba(pal["top"], 255 * reveal * (1 - 0.25*p3)))

    def paste_trans(src: Image.Image, x: int, y: int, opacity: float = 1.0):
        if opacity <= 0:
            return
        src2 = src
        if opacity < 0.999:
            src2 = src.copy()
            src2.putalpha(src2.getchannel("A").point(lambda a: int(a * opacity)))
        layer.alpha_composite(src2, (x, y))

    def comp(sub: Image.Image | None, start: float, rise: float, exit_delay: float, exit_dist: float = 560):
        if sub is None or t < start:
            return
        ap = ease_out_cubic((t - start) / 0.64)
        yoff = rise * (1 - ap)
        # P2 has a tiny continuous upward drift; it must not freeze.
        if t >= P2_END:
            yoff += -17 * read
        op = min(1.0, (t - start) / 0.20)
        if t >= P3_START + exit_delay:
            ep = ease_in_cubic((t - (P3_START + exit_delay)) / T_EXIT)
            yoff += -exit_dist * ep
            op *= max(0.0, 1 - ep * 1.10)
        tmp = Image.new("RGBA", (RW, RH), (0,0,0,0))
        tmp.alpha_composite(sub, (0, xy(yoff/scale)))
        if op < .999:
            tmp.putalpha(tmp.getchannel("A").point(lambda a: int(a*op)))
        layer.alpha_composite(tmp)

    def label_icon() -> Image.Image:
        sub = Image.new("RGBA", (RW, RH), (0,0,0,0)); sd = ImageDraw.Draw(sub, "RGBA")
        x, y, w, h = 52, 57, 232, 30
        sd.rounded_rectangle((xy(x/scale),xy(y/scale),xy((x+w)/scale),xy((y+h)/scale)), radius=xy(15/scale), fill=rgba(pal["pill"],115), outline=rgba(pal["cream"],145), width=max(1, xy(1.5/scale)))
        f = font(root, "video_announce/assets/Akrobat-Bold.otf", int(round(25*scale*S)))
        text = "СКОРО ЭКСКУРСИЯ"; bb = sd.textbbox((0,0), text, font=f)
        sd.text((xy((x+14)/scale), xy((y+h/2)/scale) - (bb[3]-bb[1])//2 - bb[1]), text, font=f, fill=rgba(pal["cream"],255))
        ix, iy, iw, ih = 590, 50, 80, 80
        sd.rounded_rectangle((xy(ix/scale),xy(iy/scale),xy((ix+iw)/scale),xy((iy+ih)/scale)), radius=xy(18/scale), fill=rgba(pal["accent"],255))
        # SVG Repo mask path; cream icon on orange for strong visibility.
        icon = build_icon(root, content.get("icon_kind", "walk"), xy(52/scale), cream)
        sub.alpha_composite(icon, (xy((ix+(iw-52)/2)/scale), xy((iy+(ih-52)/2)/scale)))
        return sub

    def title_layer() -> Image.Image:
        sub = Image.new("RGBA", (RW, RH), (0,0,0,0)); sd = ImageDraw.Draw(sub, "RGBA")
        f = font(root, "video_announce/assets/DrukCyr-Super.ttf", int(round(74*scale*S)))
        y = 668
        for line in wrap_title(root, content["title"], max_width=int(650*scale), max_lines=3):
            bb = sd.textbbox((0,0), line, font=f)
            tw = bb[2] - bb[0]
            x = (RW - tw)//2 - bb[0]
            sd.text((x+xy(2/scale), xy(y/scale)+xy(2/scale)), line, font=f, fill=(0,0,0,74))
            sd.text((x, xy(y/scale)), line, font=f, fill=rgba(pal["cream"],255))
            y += 75
        return sub

    def date_layer() -> Image.Image:
        sub = Image.new("RGBA", (RW, RH), (0,0,0,0)); sd = ImageDraw.Draw(sub, "RGBA")
        f = font(root, "video_announce/assets/Akrobat-Black.otf", int(round(42*scale*S)))
        text = content["date_line"]
        bb = sd.textbbox((0,0), text, font=f); tw, th = bb[2]-bb[0], bb[3]-bb[1]
        x = (RW - tw)//2 - xy(24/scale); y = xy(940/scale); ww = tw + xy(48/scale); hh = xy(52/scale)
        sd.rounded_rectangle((x, y, x+ww, y+hh), radius=xy(18/scale), fill=rgba(pal["accent"],255))
        sd.text((x+xy(24/scale), y + hh//2 - th//2 - bb[1]), text, font=f, fill=rgba(pal["ink"],255))
        return sub

    def cta_layer() -> Image.Image:
        sub = Image.new("RGBA", (RW, RH), (0,0,0,0)); sd = ImageDraw.Draw(sub, "RGBA")
        x, y, w, h = 52, 1068, 616, 94
        # Crisp glass UI surface on top of actual 3D glass slab: subtle, not a fake flash.
        sd.rounded_rectangle((xy(x/scale),xy(y/scale),xy((x+w)/scale),xy((y+h)/scale)), radius=xy(20/scale), fill=rgba("#E8F4FF", 28), outline=rgba(pal["cream"],205), width=max(1, xy(1.55/scale)))
        sd.rounded_rectangle((xy((x+2)/scale),xy((y+2)/scale),xy((x+w-2)/scale),xy((y+h-2)/scale)), radius=xy(18/scale), outline=rgba("#FFFFFF", 64), width=max(1, xy(0.85/scale)))
        # Warm lower glass edge, so the block reads as a lit surface, not flat fill.
        sd.line((xy((x+22)/scale), xy((y+h-8)/scale), xy((x+w-22)/scale), xy((y+h-8)/scale)), fill=rgba(pal["accent"], 72), width=max(1, xy(1.2/scale)))
        contact = (content.get("contact") or "kenigevents").strip()
        label = (content.get("contact_label") or "ЗАПИСЬ").upper()
        if contact.startswith("@"):
            label = "ЗАПИСЬ В TELEGRAM"
        elif contact.startswith("+") or contact.startswith("8 "):
            label = "ЗАПИСЬ ПО ТЕЛЕФОНУ"
        elif contact.lower().startswith("vk.com/"):
            label = "ЗАПИСЬ В VK"
        lf = font(root, "video_announce/assets/Akrobat-Bold.otf", int(round(20*scale*S)))
        size = 52
        while size > 30:
            hf = font(root, "video_announce/assets/Akrobat-Black.otf", int(round(size*scale*S)))
            if sd.textlength(contact, font=hf) <= xy((w-60)/scale):
                break
            size -= 2
        bb = sd.textbbox((0,0), label, font=lf)
        sd.text((xy((x+30)/scale), xy((y+12)/scale)-bb[1]), label, font=lf, fill=rgba(pal["cream"],250))
        hb = sd.textbbox((0,0), contact, font=hf)
        sd.text((xy((x+30)/scale), xy((y+30)/scale)-hb[1]), contact, font=hf, fill=rgba(pal["cream"],255))
        ff = font(root, "video_announce/assets/Akrobat-Bold.otf", int(round(16*scale*S)))
        sd.text((xy(52/scale), xy(1218/scale)), content.get("footer", "kenigevents • guide promo"), font=ff, fill=rgba(pal["cream"],238))
        return sub

    def title_halo() -> Image.Image | None:
        # Guided light, not a rectangular border: broad warm bloom under title +
        # left-to-right sheen clipped to the title block area.
        start, dur = 2.72, 0.82
        p = clamp01((t - start) / dur)
        if p <= 0 or p >= 1:
            return None
        strength = math.sin(math.pi * p)
        sub = Image.new("RGBA", (RW, RH), (0,0,0,0))
        glow = Image.new("RGBA", (RW, RH), (0,0,0,0)); gd = ImageDraw.Draw(glow, "RGBA")
        cx = lerp(54, 666, p); cy = 770
        for k in range(7):
            a = 18 * strength * (1 - k/7)
            gd.ellipse((xy((cx-170-k*22)/scale), xy((cy-76-k*12)/scale), xy((cx+170+k*22)/scale), xy((cy+76+k*12)/scale)), fill=rgba(pal["accent"], a))
        sub.alpha_composite(glow.filter(ImageFilter.GaussianBlur(xy(14/scale))))
        sheen = Image.new("RGBA", (RW, RH), (0,0,0,0)); sd = ImageDraw.Draw(sheen, "RGBA")
        band_x = lerp(-120, 780, p)
        sd.polygon([
            (xy((band_x-50)/scale), xy(920/scale)),
            (xy((band_x+45)/scale), xy(920/scale)),
            (xy((band_x+125)/scale), xy(625/scale)),
            (xy((band_x+30)/scale), xy(625/scale)),
        ], fill=rgba("#FFFFFF", 38*strength))
        mask = Image.new("L", (RW, RH), 0); md = ImageDraw.Draw(mask)
        md.rounded_rectangle((xy(28/scale), xy(642/scale), xy(692/scale), xy(906/scale)), radius=xy(34/scale), fill=210)
        sheen.putalpha(Image.composite(sheen.getchannel("A"), Image.new("L", (RW,RH), 0), mask))
        sub.alpha_composite(sheen)
        return sub

    def cta_halo() -> Image.Image | None:
        start, dur = 3.45, 0.96
        p = clamp01((t - start) / dur)
        if p <= 0 or p >= 1:
            return None
        strength = math.sin(math.pi * p)
        sub = Image.new("RGBA", (RW, RH), (0,0,0,0)); x,y,w,h = 52,1068,616,94
        # Bottom/left glass glow plus moving edge highlight left -> right.
        glow = Image.new("RGBA", (RW,RH), (0,0,0,0)); gd = ImageDraw.Draw(glow,"RGBA")
        gd.rounded_rectangle((xy((x-14)/scale), xy((y+28)/scale), xy((x+w+14)/scale), xy((y+h+22)/scale)), radius=xy(30/scale), fill=rgba(pal["accent"], 72*strength))
        sub.alpha_composite(glow.filter(ImageFilter.GaussianBlur(xy(16/scale))))
        edge = Image.new("RGBA", (RW,RH), (0,0,0,0)); ed = ImageDraw.Draw(edge,"RGBA")
        cx = lerp(x-96, x+w+96, p); bw = 46
        ed.polygon([
            (xy((cx-bw)/scale), xy((y+h+8)/scale)),
            (xy((cx+bw)/scale), xy((y+h+8)/scale)),
            (xy((cx+bw+50)/scale), xy((y-8)/scale)),
            (xy((cx-bw+50)/scale), xy((y-8)/scale)),
        ], fill=rgba("#FFFFFF", 122*strength))
        mask = Image.new("L", (RW,RH), 0); md = ImageDraw.Draw(mask)
        md.rounded_rectangle((xy(x/scale), xy(y/scale), xy((x+w)/scale), xy((y+h)/scale)), radius=xy(20/scale), fill=255)
        edge.putalpha(Image.composite(edge.getchannel("A"), Image.new("L", (RW,RH), 0), mask))
        sub.alpha_composite(edge)
        return sub

    comp(label_icon(), 2.06, 44, 0.04, 560)
    comp(title_layer(), 2.42, 58, 0.09, 560)
    comp(title_halo(), 2.72, 30, 0.09, 560)
    comp(date_layer(), 2.64, 46, 0.14, 560)
    comp(cta_halo(), 3.08, 30, 0.19, 560)
    comp(cta_layer(), 3.08, 52, 0.19, 560)

    layer = layer.resize((W, H), Image.Resampling.LANCZOS)
    im.alpha_composite(layer)
    return im.convert("RGB")

# ---------------------------------------------------------------------------
# Blender scene creation
# ---------------------------------------------------------------------------

def clean_scene() -> None:
    bpy.ops.object.select_all(action='SELECT')
    bpy.ops.object.delete()


def set_input(node, names: Iterable[str], value) -> None:
    for name in names:
        if name in node.inputs:
            node.inputs[name].default_value = value
            return


def principled_material(name: str, color: tuple[float,float,float,float], *, roughness: float = 0.72, alpha: float = 1.0, metallic: float = 0.0, image: bpy.types.Image | None = None, emission_strength: float = 0.0) -> bpy.types.Material:
    mat = bpy.data.materials.new(name)
    mat.use_nodes = True
    bsdf = mat.node_tree.nodes.get("Principled BSDF")
    if bsdf:
        set_input(bsdf, ["Base Color"], color)
        set_input(bsdf, ["Alpha"], alpha)
        set_input(bsdf, ["Roughness"], roughness)
        set_input(bsdf, ["Metallic"], metallic)
        set_input(bsdf, ["Specular IOR Level", "Specular"], 0.35)
        set_input(bsdf, ["Coat Weight", "Clearcoat"], 0.12)
        set_input(bsdf, ["Coat Roughness", "Clearcoat Roughness"], 0.38)
        if image:
            tex = mat.node_tree.nodes.new("ShaderNodeTexImage")
            tex.image = image
            mat.node_tree.links.new(tex.outputs["Color"], bsdf.inputs["Base Color"])
            if emission_strength > 0:
                if "Emission Color" in bsdf.inputs:
                    mat.node_tree.links.new(tex.outputs["Color"], bsdf.inputs["Emission Color"])
                set_input(bsdf, ["Emission Strength"], emission_strength)
        elif emission_strength > 0:
            set_input(bsdf, ["Emission Color"], color)
            set_input(bsdf, ["Emission Strength"], emission_strength)
    mat.diffuse_color = (color[0], color[1], color[2], alpha)
    if alpha < 1.0:
        mat.use_screen_refraction = True
        mat.blend_method = 'BLEND'
        mat.show_transparent_back = True
        mat.alpha_threshold = 0.02
    return mat


def emission_material(name: str, color: tuple[float,float,float,float], strength: float = 1.0, alpha: float = 1.0) -> bpy.types.Material:
    mat = bpy.data.materials.new(name)
    mat.use_nodes = True
    tree = mat.node_tree
    for n in list(tree.nodes):
        tree.nodes.remove(n)
    out = tree.nodes.new("ShaderNodeOutputMaterial")
    if alpha < 0.999:
        transparent = tree.nodes.new("ShaderNodeBsdfTransparent")
        em = tree.nodes.new("ShaderNodeEmission")
        mix = tree.nodes.new("ShaderNodeMixShader")
        em.inputs["Color"].default_value = color
        em.inputs["Strength"].default_value = strength
        mix.inputs["Fac"].default_value = alpha
        tree.links.new(transparent.outputs["BSDF"], mix.inputs[1])
        tree.links.new(em.outputs["Emission"], mix.inputs[2])
        tree.links.new(mix.outputs["Shader"], out.inputs["Surface"])
        mat.blend_method = 'BLEND'
        mat.use_screen_refraction = False
        mat.show_transparent_back = True
    else:
        em = tree.nodes.new("ShaderNodeEmission")
        em.inputs["Color"].default_value = color
        em.inputs["Strength"].default_value = strength
        tree.links.new(em.outputs["Emission"], out.inputs["Surface"])
    mat.diffuse_color = (color[0], color[1], color[2], alpha)
    return mat


def look_at(obj: bpy.types.Object, target: tuple[float,float,float]) -> None:
    loc = obj.location
    direction = mathutils.Vector((target[0] - loc.x, target[1] - loc.y, target[2] - loc.z))
    obj.rotation_euler = direction.to_track_quat('-Z', 'Y').to_euler()


def create_circle_mesh(name: str, radius: float, mat: bpy.types.Material, *, segments: int = 96, location=(0,0,0), uv: bool = True) -> bpy.types.Object:
    verts = [(0.0, 0.0, 0.0)]
    for i in range(segments):
        a = 2 * math.pi * i / segments
        verts.append((radius * math.cos(a), 0.0, radius * math.sin(a)))
    faces = []
    for i in range(1, segments + 1):
        faces.append((0, i, 1 + (i % segments)))
    mesh = bpy.data.meshes.new(name + "Mesh")
    mesh.from_pydata(verts, [], faces)
    mesh.update()
    if uv:
        uv_layer = mesh.uv_layers.new(name="UVMap")
        for poly in mesh.polygons:
            for li in poly.loop_indices:
                vi = mesh.loops[li].vertex_index
                x, _y, z = mesh.vertices[vi].co
                uv_layer.data[li].uv = ((x / radius + 1) * 0.5, (z / radius + 1) * 0.5)
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.collection.objects.link(obj)
    obj.location = location
    obj.data.materials.append(mat)
    return obj


def create_medallion(name: str, avatar_img: Path, radius: float, loc: tuple[float,float,float], accent_mat: bpy.types.Material, cream_mat: bpy.types.Material, shadow_mat: bpy.types.Material) -> bpy.types.Object:
    parent = bpy.data.objects.new(name, None)
    bpy.context.collection.objects.link(parent)
    parent.location = loc

    img = bpy.data.images.load(str(avatar_img))
    photo_mat = principled_material(name + "_photo_mat", (1,1,1,1), roughness=0.66, image=img, emission_strength=0.20)

    # Fast physical medallion: photo disc + bevel backing + thin rim.  No
    # transparent bitmap shadow/cover, because those caused clipped fake shadows
    # and very slow software EEVEE renders.
    bpy.ops.mesh.primitive_cylinder_add(vertices=96, radius=radius*1.045, depth=0.055, location=(0, 0.006, 0), rotation=(math.radians(90), 0, 0))
    backing = bpy.context.object
    backing.name = name + "_bevel_backing"
    backing.parent = parent
    backing.data.materials.append(cream_mat)
    bevel = backing.modifiers.new(name="small_soft_bevel", type='BEVEL')
    bevel.width = 0.012
    bevel.segments = 5
    backing.modifiers.new(name="weighted_normals", type='WEIGHTED_NORMAL')

    photo = create_circle_mesh(name + "_photo_disc", radius * 0.942, photo_mat, segments=96, location=(0, -0.034, 0), uv=True)
    photo.parent = parent

    # Thin accent rim + inner cream rim.  The minor radius is intentionally small
    # so it reads as premium rim, not a thick pipe.
    bpy.ops.mesh.primitive_torus_add(major_radius=radius*0.985, minor_radius=0.012, major_segments=96, minor_segments=10, location=(0, -0.050, 0), rotation=(math.radians(90),0,0))
    rim = bpy.context.object
    rim.name = name + "_thin_accent_rim"
    rim.parent = parent
    rim.data.materials.append(accent_mat)
    rim.modifiers.new(name="weighted_normals", type='WEIGHTED_NORMAL')

    bpy.ops.mesh.primitive_torus_add(major_radius=radius*0.935, minor_radius=0.0045, major_segments=96, minor_segments=8, location=(0, -0.055, 0), rotation=(math.radians(90),0,0))
    inner = bpy.context.object
    inner.name = name + "_inner_highlight_rim"
    inner.parent = parent
    inner.data.materials.append(cream_mat)

    return parent


def create_rounded_rect_mesh(name: str, width: float, height: float, radius: float, mat: bpy.types.Material, *, location=(0,0,0), segments: int = 14) -> bpy.types.Object:
    verts: list[tuple[float,float,float]] = []
    # rectangle in XZ plane, normal along Y
    cx = [width/2 - radius, -width/2 + radius, -width/2 + radius, width/2 - radius]
    cz = [height/2 - radius, height/2 - radius, -height/2 + radius, -height/2 + radius]
    starts = [0, math.pi/2, math.pi, 3*math.pi/2]
    # Actually build clockwise around: top-right, top-left, bottom-left, bottom-right.
    centers = [(width/2-radius, height/2-radius), (-width/2+radius, height/2-radius), (-width/2+radius, -height/2+radius), (width/2-radius, -height/2+radius)]
    angle_ranges = [(0, math.pi/2), (math.pi/2, math.pi), (math.pi, 3*math.pi/2), (3*math.pi/2, 2*math.pi)]
    for (ccx, ccz), (a0, a1) in zip(centers, angle_ranges):
        for i in range(segments + 1):
            a = a0 + (a1 - a0) * i / segments
            verts.append((ccx + radius*math.cos(a), 0.0, ccz + radius*math.sin(a)))
    verts.insert(0, (0.0, 0.0, 0.0))
    faces = []
    n = len(verts) - 1
    for i in range(1, n+1):
        faces.append((0, i, 1 + (i % n)))
    mesh = bpy.data.meshes.new(name + "Mesh")
    mesh.from_pydata(verts, [], faces)
    mesh.update()
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.collection.objects.link(obj)
    obj.location = location
    obj.data.materials.append(mat)
    return obj


def create_disc(name: str, *, loc: tuple[float,float,float], radius: float, scale_x: float, scale_z: float, mat: bpy.types.Material, segments: int = 96) -> bpy.types.Object:
    # Real shallow cylinder in the XZ plane: matte face plus visible side edge,
    # so background depth does not read as a flat 2D ellipse.
    bpy.ops.mesh.primitive_cylinder_add(vertices=segments, radius=radius, depth=0.060, location=loc, rotation=(math.radians(90), 0, 0))
    obj = bpy.context.object
    obj.name = name
    obj.scale.x = scale_x
    obj.scale.z = scale_z
    obj.data.materials.append(mat)
    bevel = obj.modifiers.new(name="soft_disc_bevel", type='BEVEL')
    bevel.width = 0.010
    bevel.segments = 3
    obj.modifiers.new(name="weighted_normals", type='WEIGHTED_NORMAL')
    return obj


@dataclass
class Rig:
    camera: bpy.types.Object
    medallions: list[bpy.types.Object]
    bubbles: list[bpy.types.Object]
    cta: bpy.types.Object
    cta_glow: bpy.types.Object
    title_glow: bpy.types.Object
    key_light: bpy.types.Object
    halo_light: bpy.types.Object
    bg_plane: bpy.types.Object
    base_meds: list[tuple[float,float,float]]
    base_bubbles: list[tuple[float,float,float]]


def setup_blender_scene(root: Path, work: Path, width: int, height: int, fps: int, content: dict) -> Rig:
    clean_scene()
    pal = PALETTES[content.get("palette", "prussian_cream")]
    scene = bpy.context.scene
    scene.render.engine = 'BLENDER_EEVEE_NEXT'
    try:
        scene.eevee.taa_render_samples = int(scene.get("_guide_render_samples", 2))
        scene.eevee.use_gtao = False
        scene.eevee.gtao_distance = 3
        scene.eevee.gtao_factor = 0.0
        scene.eevee.use_bloom = True
        scene.eevee.bloom_threshold = 1.15
        scene.eevee.bloom_intensity = 0.055
        scene.eevee.bloom_radius = 5.5
    except Exception:
        pass
    scene.render.resolution_x = width
    scene.render.resolution_y = height
    scene.render.fps = fps
    scene.frame_start = 1
    scene.frame_end = int(math.ceil(DEFAULT_DURATION * fps))
    scene.view_settings.view_transform = 'Standard'
    scene.view_settings.look = 'None'
    scene.view_settings.exposure = 0.0
    scene.view_settings.gamma = 1.0
    scene.render.film_transparent = False
    scene.render.dither_intensity = 0.0
    # Color management: calibrated product colors; no AgX/Filmic wash.

    world = scene.world or bpy.data.worlds.new("World")
    scene.world = world
    world.color = tuple(c * 0.72 for c in hex_blender(pal["bg"], 1)[:3])

    bg_mat = principled_material("matte_deep_teal_wall", hex_blender(pal["bg"], 1), roughness=0.88, emission_strength=0.025)
    bubble_mat_1 = principled_material("matte_depth_disc_light", hex_blender("#A3BAC4", 1), roughness=0.82, emission_strength=0.020)
    bubble_mat_2 = principled_material("matte_depth_disc_mid", hex_blender("#7896A5", 1), roughness=0.84, emission_strength=0.014)
    bubble_mat_3 = principled_material("matte_depth_disc_dark", hex_blender("#456B80", 1), roughness=0.86, emission_strength=0.010)
    accent_mat = principled_material("soft_orange_accent_rim", hex_blender(pal["accent"], 1), roughness=0.52, emission_strength=0.06)
    cream_mat = principled_material("warm_matte_cream_edge", hex_blender(pal["cream"], 1), roughness=0.66, emission_strength=0.03)
    shadow_mat = principled_material("transparent_soft_depth_shadow", (0.0, 0.0, 0.0, 0.24), roughness=0.95, alpha=0.24)
    glass_mat = principled_material("real_glass_cta_slab", hex_blender("#6E8794", 1.0), roughness=0.30, alpha=1.0, emission_strength=0.020)
    glow_mat = principled_material("warm_translucent_glow_disabled", hex_blender(pal["accent"], 1.0), roughness=0.60, alpha=1.0, emission_strength=0.035)

    # Oversized background wall as a single XZ plane, not a visible blue box.
    # This removes the side-edge artifact the user saw in earlier Blender tries.
    bg_mesh = bpy.data.meshes.new("oversized_teal_wall_mesh")
    bg_mesh.from_pydata([(-5.4, 0.0, -8.4), (5.4, 0.0, -8.4), (5.4, 0.0, 8.4), (-5.4, 0.0, 8.4)], [], [(0, 1, 2, 3)])
    bg_mesh.update()
    bg = bpy.data.objects.new("oversized_deep_scene_wall_no_edges", bg_mesh)
    bpy.context.collection.objects.link(bg)
    bg.location = (0, 2.95, 0.0)
    bg.data.materials.append(bg_mat)

    # Depth discs / bubbles: independent Y planes with matte materials.  They are
    # behind the product card and respond to camera parallax; no heavy hard shadow.
    bubbles: list[bpy.types.Object] = []
    bubble_specs = [
        # Large matte depth forms placed on different Y planes; they stay behind
        # the UI but remain visible enough to read as a staged 3D scene.
        ("bubble_far_left_top", (-2.35, 2.20, 1.42), 1.22, 1.10, 0.62, bubble_mat_1),
        ("bubble_mid_right", (2.68, 1.56, 0.42), 0.78, 0.98, 0.72, bubble_mat_2),
        ("bubble_far_bottom_right", (2.35, 2.62, -1.62), 1.18, 1.05, 0.70, bubble_mat_2),
        ("bubble_dark_lower_left", (-2.72, 1.36, -1.05), 0.74, 1.05, 0.64, bubble_mat_3),
        ("bubble_tiny_depth", (1.15, 2.42, 2.08), 0.34, 1.0, 0.75, bubble_mat_3),
    ]
    for name, loc, rad, sx, sz, mat in bubble_specs:
        bubbles.append(create_disc(name, loc=loc, radius=rad, scale_x=sx, scale_z=sz, mat=mat))

    # CTA glass substrate: actual 3D frosted rounded slab behind crisp overlay text.
    cta = create_rounded_rect_mesh("cta_real_frosted_glass_slab", 3.90, 0.58, 0.105, glass_mat, location=(0, -1.12, -1.58), segments=18)
    cta.modifiers.new(name="soft_weighted_normals", type='WEIGHTED_NORMAL')
    cta_glow = create_rounded_rect_mesh("cta_bottom_left_real_halo_surface", 4.24, 0.50, 0.14, glow_mat, location=(-0.10, -1.20, -1.69), segments=18)
    title_glow = create_rounded_rect_mesh("title_soft_real_halo_plane", 4.20, 1.08, 0.25, glow_mat, location=(0.0, 0.10, -0.25), segments=18)

    # Avatars as true 3D medallions.  Low overlap protects face safe-zones.
    asset_dir = work / "prepared_assets"
    asset_dir.mkdir(parents=True, exist_ok=True)
    prepared: list[Path] = []
    for idx, rel in enumerate(content["avatars"][:2]):
        dst = asset_dir / f"avatar_{idx+1}.jpg"
        crop_avatar_square(resolve_asset_path(root, rel), dst, size=1300)
        prepared.append(dst)
    # Pixel-level design: roughly 320 px medallions, centers ~250 px apart.
    med_radius = 0.38
    med_x_sep = 0.76
    med_z = 0.60
    med_y = -1.28
    if len(prepared) == 1:
        # Same approved rig, but one centered hero medallion.  Two-avatar output
        # stays byte-for-byte structurally identical to the agreed candidate.
        medallions = [
            create_medallion("guide_medallion_single_centered", prepared[0], 0.46, (0.0, med_y - 0.010, med_z + 0.010), accent_mat, cream_mat, shadow_mat),
        ]
    else:
        medallions = [
            create_medallion("guide_medallion_left_facesafe", prepared[0], med_radius, (-med_x_sep/2, med_y + 0.020, med_z - 0.020), accent_mat, cream_mat, shadow_mat),
            create_medallion("guide_medallion_right_facesafe", prepared[1], med_radius, ( med_x_sep/2, med_y - 0.030, med_z + 0.012), accent_mat, cream_mat, shadow_mat),
        ]
    # Draw order/depth: right medallion slightly closer, but overlap only rim-safe.

    # Lights: /3di-inspired sun+area fill + animated halo sweep.  Warm light moves
    # across surfaces to create a real scene-level halo, not a 2D fake flash.
    bpy.ops.object.light_add(type='SUN', location=(-2.6, -4.6, 5.4))
    key = bpy.context.object
    key.name = "soft_key_sun"
    key.data.energy = 1.55
    key.data.angle = math.radians(8.0)
    key.data.use_shadow = False
    bpy.ops.object.light_add(type='AREA', location=(0.0, -4.8, 2.7))
    fill = bpy.context.object
    fill.name = "large_soft_front_fill"
    fill.data.energy = 560
    fill.data.size = 5.7
    fill.data.use_shadow = False
    bpy.ops.object.light_add(type='AREA', location=(-2.8, -3.2, 0.0))
    halo_light = bpy.context.object
    halo_light.name = "animated_left_to_right_warm_halo_light"
    halo_light.data.energy = 185
    halo_light.data.size = 3.4
    halo_light.data.color = (1.0, 0.58, 0.30)
    halo_light.data.use_shadow = False

    # Keep render clean: no stochastic shadow speckles.  Depth comes from
    # perspective, bevels, matte surfaces and moving light, not noisy shadows.
    for obj in bpy.context.scene.objects:
        try:
            obj.visible_shadow = False
        except Exception:
            pass

    # Long lens perspective camera: enough depth for parallax without product-card
    # warping.  Camera, not avatar-only motion, creates the P1/P2 upward drift.
    bpy.ops.object.camera_add(location=(0, -14.2, 0.52))
    cam = bpy.context.object
    cam.name = "camera_long_lens_product_parallax"
    cam.data.lens = 78
    cam.data.sensor_fit = 'VERTICAL'
    cam.data.dof.use_dof = False
    scene.camera = cam
    look_at(cam, (0, -0.15, 0.34))

    return Rig(
        camera=cam,
        medallions=medallions,
        bubbles=bubbles,
        cta=cta,
        cta_glow=cta_glow,
        title_glow=title_glow,
        key_light=key,
        halo_light=halo_light,
        bg_plane=bg,
        base_meds=[tuple(o.location) for o in medallions],
        base_bubbles=[tuple(o.location) for o in bubbles],
    )


def update_rig(rig: Rig, t: float) -> None:
    times = scene_times(t)
    p1, p2, read, p3 = times["p1"], times["p2"], times["read"], times["p3"]

    # Camera-led depth: no blank/static first frames; frame 1 already moved.
    cam_y = lerp(-17.2, -10.55, p1) + 0.10 * read + 0.36 * p3
    cam_z = lerp(0.52, 0.36, p1) + lerp(0.0, -0.24, p2) - 0.045 * read - 1.26 * p3
    cam_x = 0.018 * math.sin(0.9 + t * 0.72)
    rig.camera.location = (cam_x, cam_y, cam_z)
    target_z = lerp(0.34, 0.30, p1) + lerp(0.0, -0.12, p2) - 0.028 * read - 1.10 * p3
    look_at(rig.camera, (0.0, -0.23, target_z))

    # Medallion approach/turn: true 3D rotation from angled/back-ish side into
    # the photo side.  Vertical reading pose is camera-led, not object-only.
    for i, obj in enumerate(rig.medallions):
        base = rig.base_meds[i]
        # initial z/y still in stage, no upward diagonal flight; approach happens
        # through scale, yaw and camera dolly.
        sx = lerp(0.72, 1.0, p1)
        sy = sx
        sz = sx
        obj.scale = (sx, sy, sz)
        # Only a modest yaw: enough to feel a 3D medallion turn, not enough to
        # make the avatar group sweep sideways like a 2D slide.
        yaw0 = math.radians(26 if i == 0 else -26)
        yaw = lerp(yaw0, 0.0, ease_in_out_cubic(p1))
        # Very slight non-identical tilt gives two-medallion grouping depth but
        # avoids one face passing through the other.
        obj.rotation_euler = (math.radians(1.5 if i == 0 else -1.2), 0.0, yaw)
        exit_i = ease_in_cubic((t - (P3_START + i*0.045)) / T_EXIT)
        obj.location = (
            base[0] + (0.015 if i == 0 else -0.010) * math.sin(t*0.7),
            base[1] - 0.030 * exit_i,
            base[2] + 0.18 * exit_i,
        )

    # Matte background discs: mostly parallax from camera, plus tiny independent
    # read-phase drift so P2 never freezes.  Farther Y planes move less in P3.
    for i, obj in enumerate(rig.bubbles):
        bx, by, bz = rig.base_bubbles[i]
        depth_factor = clamp01((by - 1.2) / 1.8)  # farther -> closer to 1
        small = (1.0 - depth_factor) * 0.035 + 0.012
        obj.location = (bx + small * math.sin(t * (0.43 + i*0.05) + i), by, bz - (0.035 + 0.030*(1-depth_factor))*read - (0.18 + 0.20*(1-depth_factor))*p3)
        # P1 pseudo 3D approach: background participates in much smaller scale.
        s = lerp(0.90, 1.0, p1 * (0.34 - 0.10*depth_factor))
        obj.scale.x = obj.scale.x / max(obj.scale.x, 1e-6) * obj.scale.x  # keep original aspect; no-op guard
        # Store original scale in object custom props once.
        if "base_sx" not in obj:
            obj["base_sx"] = float(obj.scale.x)
            obj["base_sz"] = float(obj.scale.z)
        obj.scale.x = obj["base_sx"] * s
        obj.scale.z = obj["base_sz"] * s

    # Glass CTA and halo surfaces.  Halo is left-to-right illumination, not flash.
    cta_p = clamp01((t - 3.45) / 0.96)
    title_p = clamp01((t - 2.72) / 0.82)
    cta_strength = math.sin(math.pi * cta_p) if 0 < cta_p < 1 else 0.0
    title_strength = math.sin(math.pi * title_p) if 0 < title_p < 1 else 0.0
    # Physical CTA glass appears only when the booking block is introduced; it
    # must not be visible as a grey bar during the opening.
    rig.cta.hide_render = t < 2.70 or p3 > 0.98
    rig.cta.location.z = -1.58 - 0.020 * read - 0.92 * p3
    rig.cta.scale = (1.0, 1.0, 1.0)
    rig.cta_glow.hide_render = True
    rig.cta_glow.location.x = -10.0
    rig.cta_glow.location.z = -1.69 - 0.020 * read - 0.92 * p3
    rig.cta_glow.scale = (0.34 + 0.50*cta_strength, 1.0, 0.95)
    # Avoid a visible fake title rectangle.  Title attention is carried by the
    # moving light and the clipped PIL sheen, not a solid plane.
    rig.title_glow.hide_render = True
    rig.title_glow.location.x = -10.0

    rig.halo_light.location.x = lerp(-3.4, 3.2, clamp01((t - 2.42) / 1.85))
    rig.halo_light.location.z = lerp(1.25, -1.45, clamp01((t - 2.42) / 1.85))
    rig.halo_light.data.energy = 120 + 175 * (title_strength + 0.92*cta_strength)

    # Keep wall oversized; minute vertical move with camera prevents dead/static.
    rig.bg_plane.location.z = -0.020 * read - 0.16 * p3


def render_frames(args, frame_indices: list[int]) -> None:
    root = Path(args.project_root).resolve()
    out = Path(args.out_root).resolve()
    work = out / "work"
    raw_dir = work / "raw"
    final_dir = work / "frames"
    if getattr(args, "resume", False):
        raw_dir.mkdir(parents=True, exist_ok=True)
        final_dir.mkdir(parents=True, exist_ok=True)
    else:
        ensure_clean_dir(raw_dir)
        ensure_clean_dir(final_dir)
    work.mkdir(parents=True, exist_ok=True)

    content = load_content(args)
    # A small private setting consumed while the scene is initialized.
    bpy.context.scene["_guide_render_samples"] = max(1, int(args.samples))
    rig = setup_blender_scene(root, work, args.width, args.height, args.fps, content)
    scene = bpy.context.scene
    scene.render.image_settings.file_format = 'PNG'
    scene.render.image_settings.color_mode = 'RGBA'
    scene.frame_start = 1
    scene.frame_end = max(frame_indices)

    manifest = {
        "content": content,
        "approved_version": APPROVED_VERSION,
        "width": args.width,
        "height": args.height,
        "fps": args.fps,
        "duration": args.duration,
        "frames": frame_indices,
        "notes": [
            "Frame t=(index+1)/fps, so first encoded frame is already in motion.",
            "Top orange stripe and typography are crisp 2D v7 overlay; not a 3D object.",
            "P1/P2 avatar rise is camera-led; medallions do not slide upward independently.",
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    for idx in frame_indices:
        t = (idx + 1) / args.fps
        update_rig(rig, t)
        scene.frame_set(idx + 1)
        raw_path = raw_dir / f"raw_{idx+1:05d}.png"
        scene.render.filepath = str(raw_path)
        bpy.ops.render.render(write_still=True)
        with Image.open(raw_path).convert("RGB") as raw:
            final = overlay_ui(raw, root=root, t=t, fps=args.fps, content=content)
            final.save(final_dir / f"frame_{idx+1:05d}.png", quality=96)
        print(f"RENDERED frame={idx+1} t={t:.3f}", flush=True)


def make_silence(path: Path, duration: float, sample_rate: int = 44100) -> None:
    n = int(duration * sample_rate)
    with wave.open(str(path), 'wb') as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        # write in chunks to avoid large memory
        chunk = b'\x00\x00\x00\x00' * 4096
        full, rem = divmod(n, 4096)
        for _ in range(full):
            wf.writeframes(chunk)
        if rem:
            wf.writeframes(b'\x00\x00\x00\x00' * rem)


def encode_video(args, frame_count: int) -> Path:
    out = Path(args.out_root).resolve()
    frames_dir = out / "work" / "frames"
    wav_path = out / "silence.wav"
    make_silence(wav_path, args.duration)
    mp4 = out / "cherryflash_guide_true3d_v4_candidate.mp4"
    ffmpeg = args.ffmpeg or shutil.which("ffmpeg")
    if not ffmpeg:
        # Local project venv used by prior CherryFlash artifact renders.
        candidate = Path("/home/dev/projects/events-bot-new/artifacts/ffmpeg-venv/lib/python3.12/site-packages/imageio_ffmpeg/binaries/ffmpeg-linux-x86_64-v7.0.2")
        if candidate.exists():
            ffmpeg = str(candidate)
    if not ffmpeg:
        raise RuntimeError("ffmpeg not found; pass --ffmpeg")
    cmd = [
        ffmpeg, "-y",
        "-framerate", str(args.fps),
        "-start_number", "1",
        "-i", str(frames_dir / "frame_%05d.png"),
        "-i", str(wav_path),
        "-shortest",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-profile:v", "high",
        "-level", "4.1",
        "-crf", str(args.crf),
        "-preset", "slow",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        str(mp4),
    ]
    print("ENCODE", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)
    return mp4


def make_storyboard(args, selected: list[int]) -> Path:
    out = Path(args.out_root).resolve()
    frames_dir = out / "work" / "frames"
    thumbs = []
    for idx in selected:
        p = frames_dir / f"frame_{idx+1:05d}.png"
        if p.exists():
            im = Image.open(p).convert("RGB").resize((180, 320), Image.Resampling.LANCZOS)
            thumbs.append((idx, im))
    if not thumbs:
        raise RuntimeError("No frames for storyboard")
    cols = min(4, len(thumbs))
    rows = math.ceil(len(thumbs)/cols)
    sheet = Image.new("RGB", (cols*180, rows*352), (18,18,18))
    d = ImageDraw.Draw(sheet)
    for k, (idx, im) in enumerate(thumbs):
        x = (k % cols)*180
        y = (k // cols)*352
        sheet.paste(im, (x, y+24))
        d.text((x+6, y+5), f"f{idx+1} t={(idx+1)/args.fps:.2f}s", fill=(240,240,220))
    path = out / "storyboard_new_candidate.jpg"
    sheet.save(path, quality=92)
    return path


def qa_motion(args, frame_count: int) -> dict:
    out = Path(args.out_root).resolve()
    frames_dir = out / "work" / "frames"
    qa: dict = {"frame_count": frame_count, "checks": {}}
    def rms(a: Path, b: Path) -> float:
        im1 = Image.open(a).convert("RGB")
        im2 = Image.open(b).convert("RGB")
        diff = ImageChops.difference(im1, im2)
        stat = ImageStat.Stat(diff)
        return float(sum(stat.rms)/len(stat.rms))
    qa["checks"]["rms_f1_f2"] = rms(frames_dir / "frame_00001.png", frames_dir / "frame_00002.png") if (frames_dir / "frame_00002.png").exists() else None
    qa["checks"]["rms_f2_f3"] = rms(frames_dir / "frame_00002.png", frames_dir / "frame_00003.png") if (frames_dir / "frame_00003.png").exists() else None
    qa["checks"]["first_frame_non_static"] = (qa["checks"].get("rms_f1_f2") or 0) > 0.35 and (qa["checks"].get("rms_f2_f3") or 0) > 0.25
    qa_path = out / "qa_motion.json"
    qa_path.write_text(json.dumps(qa, ensure_ascii=False, indent=2), encoding="utf-8")
    return qa


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=os.getcwd())
    parser.add_argument("--out-root", default="artifacts/codex/cherryflash_true3d_v4/run")
    parser.add_argument("--width", type=int, default=720)
    parser.add_argument("--height", type=int, default=1280)
    parser.add_argument("--fps", type=int, default=DEFAULT_FPS)
    parser.add_argument("--duration", type=float, default=DEFAULT_DURATION)
    parser.add_argument("--palette", default=None, choices=sorted(PALETTES.keys()))
    parser.add_argument("--scene-json", default=None, help="Optional content JSON for production CherryFlash scene rendering")
    parser.add_argument("--mode", choices=["keyframes", "video"], default="keyframes")
    parser.add_argument("--frames", default="1,2,3,12,24,36,50,68,82,96,116,136,158,176")
    parser.add_argument("--ffmpeg", default=None)
    parser.add_argument("--crf", type=int, default=17)
    parser.add_argument("--samples", type=int, default=2)
    parser.add_argument("--skip-render", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--start-frame", type=int, default=1, help="1-based first frame for chunk/resume")
    parser.add_argument("--end-frame", type=int, default=0, help="1-based inclusive last frame for chunk/resume; 0 means full duration")
    parser.add_argument("--no-encode", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    argv = sys.argv[sys.argv.index("--") + 1:] if "--" in sys.argv else sys.argv[1:]
    args = parse_args(argv)
    out = Path(args.out_root).resolve()
    out.mkdir(parents=True, exist_ok=True)
    frame_count = int(math.ceil(args.duration * args.fps))
    if args.mode == "video":
        start = max(1, int(args.start_frame))
        end = int(args.end_frame) if int(args.end_frame) > 0 else frame_count
        end = max(start, min(frame_count, end))
        frame_indices = list(range(start - 1, end))
    else:
        # User-visible frame numbers in args are 1-based; store/render indices 0-based.
        vals = [int(x.strip()) for x in args.frames.split(',') if x.strip()]
        frame_indices = sorted({max(0, min(frame_count-1, v-1)) for v in vals})
        # Always include first three frames for static-start QA.
        frame_indices = sorted(set([0,1,2]) | set(frame_indices))
    if not args.skip_render:
        render_frames(args, frame_indices)
    storyboard = make_storyboard(args, frame_indices if args.mode == "keyframes" else [0,1,2,12,24,36,50,68,82,96,116,136,158,frame_count-1])
    qa = qa_motion(args, frame_count if args.mode == "video" else max(frame_indices)+1)
    result = {"storyboard": str(storyboard), "qa": qa}
    if args.mode == "video" and not args.no_encode:
        mp4 = encode_video(args, frame_count)
        result["video"] = str(mp4)
    (out / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print("RESULT_JSON", json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
