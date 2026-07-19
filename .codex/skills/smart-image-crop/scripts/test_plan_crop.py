#!/usr/bin/env python3
from __future__ import annotations

import unittest

from plan_crop import Box, plan_crop


class CropPlanTest(unittest.TestCase):
    def test_visual_only_safe_crop_uses_cover(self) -> None:
        result = plan_crop(
            width=1200,
            height=1000,
            target_ratio=1.5,
            image_text_mode="visual_only",
            safe_crop=True,
            selection_reason="editorial-wide",
        )
        self.assertEqual(result["decision"], "cover")
        self.assertAlmostEqual(result["potential_crop_area_fraction"], 0.2)
        self.assertEqual(result["token_selection_reason"], "editorial-wide")

    def test_ocr_vertical_crop_is_bounded_and_box_safe(self) -> None:
        result = plan_crop(
            width=800,
            height=1200,
            target_ratio=0.8,
            image_text_mode="ocr_text",
            ocr_boxes=[Box(0.08, 0.12, 0.84, 0.68)],
        )
        self.assertEqual(result["decision"], "bounded-cover")
        self.assertLessEqual(result["potential_crop_area_fraction"], 0.2)
        self.assertAlmostEqual(result["crop"]["top"] + result["crop"]["bottom"], 1 / 6)

    def test_extreme_ocr_portrait_requires_card_fallback(self) -> None:
        result = plan_crop(
            width=600,
            height=1200,
            target_ratio=0.8,
            image_text_mode="ocr_text",
            ocr_boxes=[Box(0.05, 0.1, 0.9, 0.8)],
        )
        self.assertEqual(result["decision"], "fallback-required")

    def test_ocr_never_crops_left_and_right(self) -> None:
        result = plan_crop(
            width=1200,
            height=800,
            target_ratio=0.8,
            image_text_mode="ocr_text",
            ocr_boxes=[Box(0.1, 0.1, 0.8, 0.8)],
        )
        self.assertEqual(result["decision"], "fallback-required")

    def test_missing_ocr_boxes_fails_closed(self) -> None:
        result = plan_crop(
            width=800,
            height=1200,
            target_ratio=0.8,
            image_text_mode="ocr_text",
        )
        self.assertEqual(result["decision"], "fallback-required")

    def test_unknown_fails_closed(self) -> None:
        result = plan_crop(
            width=1000,
            height=1000,
            target_ratio=1.5,
            image_text_mode="unknown",
        )
        self.assertEqual(result["decision"], "fallback-required")

    def test_arbitrary_card_ratio_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "approved card token"):
            plan_crop(
                width=1200,
                height=800,
                target_ratio=1.35,
                image_text_mode="visual_only",
                safe_crop=True,
            )

    def test_natural_ratio_is_available_only_on_document_surface(self) -> None:
        result = plan_crop(
            width=700,
            height=1100,
            target_ratio=700 / 1100,
            image_text_mode="ocr_text",
            surface="document",
        )
        self.assertEqual(result["decision"], "exact-ratio")
        self.assertIsNone(result["target_token"])

    def test_exact_card_token_renders_source(self) -> None:
        result = plan_crop(
            width=800,
            height=1000,
            target_ratio=4 / 5,
            image_text_mode="ocr_text",
        )
        self.assertEqual(result["decision"], "exact-ratio")
        self.assertEqual(result["target_token"], "P")
        self.assertTrue(result["render_source_image"])


if __name__ == "__main__":
    unittest.main()
