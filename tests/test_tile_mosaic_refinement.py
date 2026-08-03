from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import tempfile
import unittest

from PIL import Image, ImageDraw

from tools.tile_mosaic.core import build_scene_plan, load_preset
from tools.tile_mosaic.pillow_renderer import render_pillow
from tools.tile_mosaic.refine_render import (
    _perspective_coefficients,
    load_refinement,
    render_refinement,
)
from tools.tile_mosaic.validate_render import validate_render

ROOT = Path(__file__).resolve().parents[1]
PRESET = ROOT / "tools" / "tile_mosaic" / "presets" / "kafel_classic_v1.json"
PROFILE = ROOT / "tools" / "tile_mosaic" / "refinements" / "reference_balanced_v1.json"


class TileMosaicRefinementTests(unittest.TestCase):
    def test_profile_contract(self) -> None:
        profile = load_refinement(PROFILE)
        self.assertEqual(profile["name"], "reference_balanced_v1")
        self.assertEqual(len(profile["profile_sha256"]), 64)
        self.assertGreater(profile["geometry"]["active_fraction"], 0)
        self.assertGreater(profile["blur"]["fraction"], 0)

    def test_perspective_identity_maps_corners(self) -> None:
        points = [(0.0, 0.0), (99.0, 0.0), (99.0, 99.0), (0.0, 99.0)]
        coefficients = _perspective_coefficients(points, points)
        expected = (1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0)
        for actual, wanted in zip(coefficients, expected):
            self.assertAlmostEqual(actual, wanted, places=6)

    def test_refinement_is_deterministic_and_structurally_valid(self) -> None:
        preset = load_preset(PRESET)
        compact = deepcopy(preset)
        compact["canvas"].update({"width": 640, "height": 360})
        compact["grid"].update({"columns": 4, "rows": 2, "texture_tile_px": 64})
        plan = build_scene_plan(compact, focal_x=0.55, focal_y=0.48, seed=29)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = Image.new("RGB", (256, 128), (30, 39, 48))
            draw = ImageDraw.Draw(source)
            draw.rectangle((42, 20, 220, 116), fill=(194, 79, 34))
            draw.ellipse((112, 26, 212, 126), fill=(230, 186, 120))
            baseline = root / "baseline.png"
            plan_path = root / "plan.json"
            plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            render_pillow(plan=plan, source_texture=source, output_path=baseline)
            first = root / "first.png"
            second = root / "second.png"
            render_refinement(base_render=baseline, base_plan=plan_path, profile_path=PROFILE, output_path=first)
            render_refinement(base_render=baseline, base_plan=plan_path, profile_path=PROFILE, output_path=second)
            self.assertEqual(
                sha256(first.read_bytes()).hexdigest(),
                sha256(second.read_bytes()).hexdigest(),
            )
            report = validate_render(first, plan_path)
            self.assertEqual(report["status"], "pass", report)


if __name__ == "__main__":
    unittest.main()
