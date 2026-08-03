from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import tempfile
import unittest

from PIL import Image, ImageDraw

from tools.tile_mosaic.core import (
    STATE_NAMES,
    build_scene_plan,
    canonical_json_bytes,
    load_preset,
    load_scene_plan,
)
from tools.tile_mosaic.pillow_renderer import render_pillow
from tools.tile_mosaic.prepare import cover_crop_box, load_image, prepare_textures

ROOT = Path(__file__).resolve().parents[1]
PRESET = ROOT / "tools" / "tile_mosaic" / "presets" / "kafel_classic_v1.json"
GOLDEN_PLAN = (
    ROOT
    / "docs"
    / "review-data"
    / "tile-mosaic-material-lab"
    / "v1"
    / "kafel-classic-v1.png.plan.json"
)


class TileMosaicCoreTests(unittest.TestCase):
    def test_cover_crop_matches_frozen_source_contract(self) -> None:
        self.assertEqual(
            cover_crop_box((1200, 799), (3072, 1536), focal_x=0.58, focal_y=0.44),
            (0, 52, 1200, 652),
        )

    def test_state_counts_and_plan_are_deterministic(self) -> None:
        preset = load_preset(PRESET)
        first = build_scene_plan(preset, focal_x=0.58, focal_y=0.44, seed=20260901)
        second = build_scene_plan(preset, focal_x=0.58, focal_y=0.44, seed=20260901)
        self.assertEqual(first, second)
        self.assertEqual(sum(first["state_counts"].values()), 72)
        self.assertEqual(set(first["state_counts"]), set(STATE_NAMES))
        self.assertEqual(first["renderer_contract"]["pillow_geometry_version"], 2)

    def test_frozen_plan_is_loadable_and_keeps_provenance(self) -> None:
        plan = load_scene_plan(GOLDEN_PLAN)
        self.assertEqual(len(plan["tiles"]), 72)
        self.assertEqual(plan["grid"]["columns"], 12)
        self.assertEqual(plan["grid"]["rows"], 6)
        self.assertIn("plan_sha256", plan)
        # Legacy hash remains available if the v2 validator canonicalises the
        # old payload differently.
        if "legacy_plan_sha256" in plan:
            self.assertEqual(len(plan["legacy_plan_sha256"]), 64)

    def test_prepare_texture_is_reproducible(self) -> None:
        preset = load_preset(PRESET)
        compact = deepcopy(preset)
        compact["canvas"].update({"width": 640, "height": 360})
        compact["grid"].update({"columns": 4, "rows": 2, "texture_tile_px": 64})
        # Largest-remainder allocation works for any grid size.
        plan = build_scene_plan(compact, focal_x=0.55, focal_y=0.45, seed=17)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = Image.new("RGB", (640, 480), (18, 24, 30))
            draw = ImageDraw.Draw(source)
            draw.rectangle((80, 70, 560, 410), fill=(176, 78, 38))
            draw.ellipse((260, 90, 500, 330), fill=(224, 190, 138))
            source_path = root / "source.png"
            source.save(source_path)
            first = prepare_textures(input_path=source_path, plan=plan, output_dir=root / "a")
            second = prepare_textures(input_path=source_path, plan=plan, output_dir=root / "b")
            self.assertEqual(first.crop_box, second.crop_box)
            self.assertEqual(
                sha256(first.base_path.read_bytes()).hexdigest(),
                sha256(second.base_path.read_bytes()).hexdigest(),
            )

    def test_pillow_render_is_byte_deterministic(self) -> None:
        preset = load_preset(PRESET)
        compact = deepcopy(preset)
        compact["canvas"].update({"width": 640, "height": 360})
        compact["grid"].update({"columns": 4, "rows": 2, "texture_tile_px": 64})
        plan = build_scene_plan(compact, focal_x=0.5, focal_y=0.5, seed=23)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = Image.new("RGB", (256, 128))
            draw = ImageDraw.Draw(source)
            for x in range(256):
                draw.line((x, 0, x, 127), fill=(x, 80, 255 - x))
            first = root / "first.png"
            second = root / "second.png"
            render_pillow(plan=plan, source_texture=source, output_path=first)
            render_pillow(plan=plan, source_texture=source, output_path=second)
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_canonical_json_is_stable(self) -> None:
        left = {"b": 2, "a": [3, 1]}
        right = {"a": [3, 1], "b": 2}
        self.assertEqual(canonical_json_bytes(left), canonical_json_bytes(right))


class TileMosaicEvidenceTests(unittest.TestCase):
    def test_manifest_normalization_is_portable_and_deterministic(self) -> None:
        import os
        from tempfile import TemporaryDirectory
        from tools.tile_mosaic.evidence import normalize_baseline_manifest

        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "generated_at": "volatile",
                        "input": {"reference": "/tmp/01.jpg", "sha256": "abc"},
                        "output": {"path": "/tmp/out.png", "sha256": "def"},
                        "backend_result": {"output": "/tmp/out.png"},
                        "plan": {"input_path": "/tmp/plan.json", "path": "/tmp/work/plan.json"},
                        "preset": {"path": "/tmp/presets/kafel_classic_v1.json"},
                    }
                ),
                encoding="utf-8",
            )
            previous = os.environ.get("SOURCE_DATE_EPOCH")
            os.environ["SOURCE_DATE_EPOCH"] = "1785783046"
            try:
                first = normalize_baseline_manifest(
                    path,
                    input_reference="golden-source.jpg",
                    output_name="kafel-classic-v1.png",
                    plan_name="kafel-classic-v1.png.plan.json",
                )
                first_bytes = path.read_bytes()
                second = normalize_baseline_manifest(
                    path,
                    input_reference="golden-source.jpg",
                    output_name="kafel-classic-v1.png",
                    plan_name="kafel-classic-v1.png.plan.json",
                )
            finally:
                if previous is None:
                    os.environ.pop("SOURCE_DATE_EPOCH", None)
                else:
                    os.environ["SOURCE_DATE_EPOCH"] = previous
            self.assertEqual(first, second)
            self.assertEqual(first_bytes, path.read_bytes())
            self.assertEqual(first["generated_at"], "2026-08-03T18:50:46Z")
            self.assertEqual(first["input"]["reference"], "golden-source.jpg")
            self.assertEqual(first["output"]["path"], "kafel-classic-v1.png")
            self.assertEqual(first["backend_result"]["output"], "kafel-classic-v1.png")
            self.assertEqual(first["plan"]["path"], "kafel-classic-v1.png.plan.json")
            self.assertEqual(first["preset"]["path"], "presets/kafel_classic_v1.json")


if __name__ == "__main__":
    unittest.main()
