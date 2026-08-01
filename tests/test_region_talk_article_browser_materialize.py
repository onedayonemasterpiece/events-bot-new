from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from unittest import mock

from scripts import region_talk_article_browser_materialize as materializer


PUBLIC_ANSWER = [(None, None, None, None, ("93.184.216.34", 443))]


class RegionTalkArticleBrowserMaterializerTests(unittest.TestCase):
    def test_public_http_url_rejects_private_mixed_dns_and_credentials(self) -> None:
        self.assertEqual(
            materializer.public_http_url(
                "https://publisher.example/article",
                resolver=lambda *args, **kwargs: PUBLIC_ANSWER,
            ),
            "https://publisher.example/article",
        )
        with self.assertRaisesRegex(ValueError, "non-public"):
            materializer.public_http_url(
                "https://publisher.example/article",
                resolver=lambda *args, **kwargs: [
                    *PUBLIC_ANSWER,
                    (None, None, None, None, ("127.0.0.1", 443)),
                ],
            )
        with self.assertRaisesRegex(ValueError, "embedded credentials"):
            materializer.public_http_url(
                "https://user:password@publisher.example/article",
                resolver=lambda *args, **kwargs: PUBLIC_ANSWER,
            )

    def test_rendered_candidates_keep_dom_evidence_and_filter_logo(self) -> None:
        raw = [
            {
                "url": "/media/kaliningrad-ocean.jpg",
                "role": "article_figure",
                "alt": "Фасад Планеты Океан в Калининграде",
                "caption": "Новый корпус музея",
                "width": 1600,
                "height": 1000,
                "selector": "article figure img",
                "dom_path": "main > article > figure > img",
            },
            {
                "url": "/assets/logo.png",
                "role": "article_main",
                "alt": "publisher logo",
                "width": 600,
                "height": 200,
            },
        ]
        with mock.patch.object(materializer, "public_http_url", side_effect=lambda value, **_: value):
            rows = materializer.normalize_rendered_candidates(
                raw,
                page_url="https://publisher.example/article",
                article_title="Планета Океан в Калининграде",
                article_summary="Архитектурный разбор нового корпуса музея.",
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["url"], "https://publisher.example/media/kaliningrad-ocean.jpg")
        self.assertEqual(rows[0]["role"], "article_figure")
        self.assertEqual(rows[0]["caption"], "Новый корпус музея")
        self.assertEqual(rows[0]["dom_path"], "main > article > figure > img")
        self.assertEqual(rows[0]["association_decision"], "accept")

    def test_row_due_respects_retry_and_unexpired_lease(self) -> None:
        now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
        base = {
            "post_url": "https://publisher.example/article",
            "image_queue_status": "needs_browser_materialization",
            "browser_materialization_attempt_count": 1,
        }
        self.assertTrue(materializer.row_due(base, now))
        self.assertFalse(materializer.row_due(
            {**base, "browser_materialization_next_attempt_after": (now + timedelta(hours=1)).isoformat()}, now
        ))
        self.assertFalse(materializer.row_due(
            {
                **base,
                "browser_materialization_lease_run_id": "other",
                "browser_materialization_lease_expires_at": (now + timedelta(minutes=5)).isoformat(),
            },
            now,
        ))
        self.assertTrue(materializer.row_due(
            {
                **base,
                "browser_materialization_lease_run_id": "stale",
                "browser_materialization_lease_expires_at": (now - timedelta(seconds=1)).isoformat(),
            },
            now,
        ))

    def test_success_materializes_refs_or_terminalizes_zero_association(self) -> None:
        now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
        row = {"browser_materialization_attempt_count": 0, "image_queue_status": "needs_browser_materialization"}
        candidate = {
            "url": "https://cdn.example/hero.jpg",
            "role": "article_figure",
            "association_decision": "accept",
        }
        success = materializer.apply_success(
            row,
            {"candidates": [candidate], "rendered_page_url": "https://publisher.example/article", "request_count": 12},
            run_id="run-1",
            now=now,
        )
        self.assertEqual(success["browser_materialization_status"], "materialized")
        self.assertEqual(success["image_queue_status"], "needs_actual_image_fetch")
        self.assertEqual(success["browser_materialized_image_urls"], ["https://cdn.example/hero.jpg"])
        self.assertEqual(success["next_action"], "region_talk_image_diagnostic_download_and_vlm_rank")

        empty = materializer.apply_success(
            row,
            {"candidates": [], "rendered_page_url": "https://publisher.example/article", "request_count": 4},
            run_id="run-1",
            now=now,
        )
        self.assertEqual(empty["browser_materialization_status"], "terminal_no_associated_images")
        self.assertEqual(empty["image_queue_status"], "not_reviewable_no_media")
        self.assertEqual(empty["presentation_recommendation"], "system_link_preview")

    def test_failures_back_off_six_then_twenty_four_hours_and_terminalize(self) -> None:
        now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
        first = materializer.apply_failure(
            {"browser_materialization_attempt_count": 0}, RuntimeError("timeout"), run_id="run", now=now
        )
        self.assertEqual(first["browser_materialization_status"], "retry_wait")
        self.assertEqual(
            materializer.parse_dt(first["browser_materialization_next_attempt_after"]),
            now + timedelta(hours=6),
        )

        second = materializer.apply_failure(first, RuntimeError("timeout"), run_id="run", now=now)
        self.assertEqual(
            materializer.parse_dt(second["browser_materialization_next_attempt_after"]),
            now + timedelta(hours=24),
        )

        third = materializer.apply_failure(second, RuntimeError("timeout"), run_id="run", now=now)
        self.assertEqual(third["browser_materialization_status"], "terminal_fetch_failed")
        self.assertEqual(third["image_queue_status"], "broken_media")
        self.assertEqual(third["presentation_recommendation"], "system_link_preview")

    def test_max_pages_is_hard_clamped_to_three(self) -> None:
        self.assertEqual(materializer.MAX_PAGES_HARD, 3)


if __name__ == "__main__":
    unittest.main()
