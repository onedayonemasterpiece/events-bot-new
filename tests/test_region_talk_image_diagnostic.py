from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "kaggle" / "RegionTalkImageDiagnostic" / "region_talk_image_diagnostic.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_image_diagnostic", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkImageDiagnosticTests(unittest.TestCase):
    def test_video_download_is_terminal_unsupported_not_retry(self) -> None:
        old_output = os.environ.get("REGION_TALK_IMAGE_DIAG_OUTPUT_DIR")
        old_allow_missing = os.environ.get("REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT")
        with tempfile.TemporaryDirectory() as td:
            os.environ["REGION_TALK_IMAGE_DIAG_OUTPUT_DIR"] = td
            os.environ["REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT"] = "1"
            try:
                mod = load_module()
                row = {
                    "image_queue_id": "imgq_video",
                    "image_queue_status": "image_analysis_in_progress",
                    "actual_media_path": "/tmp/telegram_post.mp4",
                }
                self.assertIsNone(mod.validate_image(row))
                mod.finalize(row)
                mod.apply_image_queue_status(row)
                self.assertEqual(row["image_queue_status"], "not_reviewable_unsupported_media")
                self.assertEqual(row["media_acquisition_status"], "unsupported_media_or_decode_failed")
                self.assertEqual(row["final_visual_status"], "unsupported_media")
                self.assertEqual(row["next_action"], "skip_unsupported_media")
            finally:
                if old_output is None:
                    os.environ.pop("REGION_TALK_IMAGE_DIAG_OUTPUT_DIR", None)
                else:
                    os.environ["REGION_TALK_IMAGE_DIAG_OUTPUT_DIR"] = old_output
                if old_allow_missing is None:
                    os.environ.pop("REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT", None)
                else:
                    os.environ["REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT"] = old_allow_missing


if __name__ == "__main__":
    unittest.main()
