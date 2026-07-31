from __future__ import annotations

import unittest

from scripts import region_talk_state_maintenance as maintenance


class RegionTalkStateMaintenanceTests(unittest.TestCase):
    def test_missing_text_before_gemini_is_not_terminal_storage(self) -> None:
        for status in ["no_text_for_gemini", "text_restore_pending"]:
            self.assertFalse(maintenance.is_publication_terminal({
                "publication_status": status,
                "publication_candidate_status": "filtered_before_llm",
                "finalization_status": "terminal",
            }))

    def test_reopens_placeholder_html_decode_failure_for_telegram_fetch(self) -> None:
        row = {
            "image_queue_status": "not_reviewable_unsupported_media",
            "image_url_or_local_path": "https://t.me/example/123#media",
            "media_fetch_status": "decode_failed",
            "media_fetch_error": "UnidentifiedImageError: cannot identify image file public_url.jpg",
            "actual_media_path": "/tmp/public_url.jpg",
            "final_visual_status": "unsupported_media",
            "media_fetch_attempt_count": 1,
            "full_text": "transient text",
        }

        changed = maintenance.reopen_placeholder_media_row(row, "2026-07-12T14:00:00+00:00")

        self.assertTrue(changed)
        self.assertEqual(row["image_queue_status"], "needs_actual_image_fetch")
        self.assertEqual(row["media_fetch_status"], "needs_actual_image_fetch")
        self.assertEqual(row["media_fetch_attempt_count"], 0)
        self.assertNotIn("actual_media_path", row)
        self.assertNotIn("final_visual_status", row)

    def test_does_not_reopen_real_non_image_media(self) -> None:
        row = {
            "image_queue_status": "not_reviewable_unsupported_media",
            "image_url_or_local_path": "https://cdn.example/video.mp4",
            "media_fetch_status": "unsupported_media",
            "media_fetch_error": "telegram media is not an image: .mp4",
        }
        self.assertFalse(maintenance.reopen_placeholder_media_row(row, "2026-07-12T14:00:00+00:00"))


if __name__ == "__main__":
    unittest.main()
