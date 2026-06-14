from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SvgValidation:
    valid: bool
    flags: list[str] = field(default_factory=list)
    path_count: int = 0
    stroke_colors: set[str] = field(default_factory=set)

    def to_dict(self) -> dict[str, object]:
        return {
            "valid": self.valid,
            "flags": self.flags,
            "path_count": self.path_count,
            "stroke_colors": sorted(self.stroke_colors),
        }


def validate_svg_text(svg_text: str, *, expected_stroke: str | None = None) -> SvgValidation:
    flags: list[str] = []
    try:
        root = ET.fromstring(svg_text)
    except ET.ParseError as exc:
        return SvgValidation(valid=False, flags=[f"xml_parse_error:{exc}"])

    if "viewBox" not in root.attrib:
        flags.append("missing_viewbox")
    if re.search(r"<image\b", svg_text, re.IGNORECASE):
        flags.append("embedded_image_tag")
    if "base64," in svg_text.lower() or "data:image" in svg_text.lower():
        flags.append("embedded_raster_data")
    if re.search(r"<rect\b[^>]*(class=|id=|fill=)", svg_text, re.IGNORECASE):
        if "transparent" not in svg_text.lower():
            flags.append("possible_background_rect")

    stroke_colors: set[str] = set()
    path_count = 0
    for elem in root.iter():
        tag = elem.tag.split("}", 1)[-1].lower()
        if tag in {"path", "line", "polyline"}:
            path_count += 1
            fill = (elem.attrib.get("fill") or "").strip().lower()
            if fill and fill != "none":
                flags.append("non_none_fill")
            stroke = elem.attrib.get("stroke")
            if stroke and not stroke.startswith("var("):
                stroke_colors.add(stroke.upper())
            style = elem.attrib.get("style") or ""
            if "stroke:" in style:
                match = re.search(r"stroke:\s*([^;]+)", style)
                if match:
                    stroke_colors.add(match.group(1).strip().upper())

    if path_count <= 0:
        flags.append("no_stroke_paths")
    if expected_stroke and stroke_colors:
        expected = expected_stroke.upper()
        if any(color != expected for color in stroke_colors):
            flags.append("unexpected_stroke_color")
    if 'stroke-linecap="round"' not in svg_text and "stroke-linecap: round" not in svg_text:
        flags.append("missing_round_linecap")
    if 'stroke-linejoin="round"' not in svg_text and "stroke-linejoin: round" not in svg_text:
        flags.append("missing_round_linejoin")
    return SvgValidation(valid=not flags, flags=flags, path_count=path_count, stroke_colors=stroke_colors)


def validate_svg_file(path: str | Path, *, expected_stroke: str | None = None) -> SvgValidation:
    return validate_svg_text(Path(path).read_text(encoding="utf-8"), expected_stroke=expected_stroke)
