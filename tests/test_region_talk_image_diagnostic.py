from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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
    def _load_in_temp_output(self, td: str):
        os.environ["REGION_TALK_IMAGE_DIAG_OUTPUT_DIR"] = td
        os.environ["REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT"] = "1"
        os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = "publication-gate-test-v1"
        return load_module()

    def test_vk_fetch_uses_prefetched_public_url_before_vk_api(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                def fake_download(url, path):
                    self.assertEqual(url, "https://sun.example/vk-photo.jpg?quality=95")
                    Path(path).write_bytes(b"image")
                    return str(path)

                mod._download_http_image = fake_download
                with mock.patch.object(mod.requests, "get", side_effect=AssertionError("VK API must not be called")):
                    row = {
                        "image_queue_id": "vk-prefetched",
                        "post_url": "https://vk.com/wall-211445468_273",
                        "image_url_or_local_path": "https://sun.example/vk-photo.jpg?quality=95",
                    }
                    mod.fetch_vk(row)
                self.assertEqual(row["media_fetch_status"], "downloaded_public_url")
                self.assertEqual(row["media_fetch_error"], "")
                self.assertTrue(Path(row["actual_media_path"]).exists())
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_publication_eligibility_gate_fails_closed_and_blocks_local_source(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                accepted = {
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                    "publication_eligibility_reason": "all_p0_text_and_source_gates_passed",
                }
                self.assertEqual(mod.publication_eligibility_gate_reason(accepted), "")
                mod.apply_publication_eligibility_audit(accepted)
                self.assertEqual(accepted["image_eligibility_status"], "accepted")
                self.assertEqual(accepted["image_eligibility_gate_version"], "publication-gate-test-v1")
                self.assertEqual(accepted["image_eligibility_reason"], "all_p0_text_and_source_gates_passed")

                self.assertEqual(
                    mod.publication_eligibility_gate_reason({}),
                    "publication_eligibility_decision_missing",
                )
                self.assertEqual(
                    mod.publication_eligibility_gate_reason({
                        "publication_eligibility_decision": "unknown",
                        "publication_eligibility_gate_version": "publication-gate-test-v1",
                    }),
                    "publication_eligibility_decision_not_accept:unknown",
                )
                self.assertEqual(
                    mod.publication_eligibility_gate_reason({
                        "publication_eligibility_decision": "accept",
                    }),
                    "publication_eligibility_gate_version_missing",
                )
                self.assertEqual(
                    mod.publication_eligibility_gate_reason({
                        "publication_eligibility_decision": "accept",
                        "publication_eligibility_gate_version": "old-gate-v0",
                    }),
                    "publication_eligibility_gate_version_mismatch:expected=publication-gate-test-v1;actual=old-gate-v0",
                )
                local = {
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                    "source_scope": "local_region",
                }
                self.assertEqual(
                    mod.publication_eligibility_gate_reason(local),
                    "local_source_marker:source_scope=local_region",
                )
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_ydb_queue_leases_only_eligible_rows_and_persists_blocked_audit(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)

                def queue_row(queue_id: str, **updates):
                    row = {
                        "image_queue_id": queue_id,
                        "image_queue_order": len(queue_id),
                        "image_queue_status": "needs_actual_image_fetch",
                        "post_url": f"https://t.me/example/{len(queue_id)}",
                        "kaliningrad_oblast_only_scope": "true",
                        "kaliningrad_mention_role": "main_subject",
                        "publication_eligibility_decision": "accept",
                        "publication_eligibility_gate_version": "publication-gate-test-v1",
                    }
                    row.update(updates)
                    return row

                raw = [
                    queue_row("eligible"),
                    queue_row("attempted_same_run", last_image_diag_run_id=mod.RUN_ID),
                    queue_row("rejected", publication_eligibility_decision="reject"),
                    queue_row("legacy", publication_eligibility_decision="", publication_eligibility_gate_version=""),
                    queue_row("local", source_geo_class="kaliningrad_local"),
                ]
                writes: list[tuple[str, list[dict]]] = []
                mod.ydb_select_image_queue = lambda _limit: raw
                mod.ydb_upsert_image_rows = lambda batch, *, stage: writes.append((stage, [dict(row) for row in batch]))

                leased, total = mod.ydb_rows_for_diagnostic(10)

                self.assertEqual(total, 5)
                self.assertEqual([row["image_queue_id"] for row in leased], ["eligible"])
                self.assertEqual(leased[0]["image_queue_status"], "image_analysis_in_progress")
                self.assertEqual(mod.input_payload["publication_eligibility_pending_count"], 1)
                self.assertEqual(mod.input_payload["publication_eligibility_blocked_count"], 3)

                blocked_writes = [batch for stage, batch in writes if stage == "blocked_publication_eligibility"]
                self.assertEqual(len(blocked_writes), 1)
                blocked = blocked_writes[0]
                self.assertEqual({row["image_queue_id"] for row in blocked}, {"rejected", "legacy", "local"})
                self.assertTrue(all(row["image_queue_status"] == "rejected_publication_eligibility" for row in blocked))
                self.assertTrue(all(row["image_eligibility_status"] == "blocked" for row in blocked))
                self.assertTrue(all(row["image_eligibility_reason"] for row in blocked))
                self.assertTrue(all(row["image_eligibility_expected_gate_version"] == "publication-gate-test-v1" for row in blocked))
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_image_queue_scan_is_not_limited_to_five_batches(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_QUEUE_SCAN_LIMIT",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                os.environ["REGION_TALK_IMAGE_DIAG_QUEUE_SCAN_LIMIT"] = "5000"
                with mock.patch.object(mod, "ydb_select_kind", return_value=[]) as select:
                    mod.ydb_select_image_queue(10)
                select.assert_called_once_with("image_queue_item", 5000)
                self.assertIn(mod.IMAGE_TERMINAL_ELIGIBILITY_STATUS, mod.IMAGE_TERMINAL_SKIP_STATUSES)
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_public_telegram_html_fallback_is_explicit_opt_in(self) -> None:
        old = os.environ.get("REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK")
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                os.environ.pop("REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK", None)
                self.assertFalse(mod.public_tg_html_fallback_enabled())
                os.environ["REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK"] = "1"
                self.assertTrue(mod.public_tg_html_fallback_enabled())
            finally:
                if old is None:
                    os.environ.pop("REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK", None)
                else:
                    os.environ["REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK"] = old

    def test_process_batch_rejects_before_media_fetch_or_scoring(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                calls = {"telegram": 0, "vk": 0, "validate": 0}

                async def forbidden_telegram_fetch(_batch):
                    calls["telegram"] += 1

                def forbidden_vk_fetch(_row):
                    calls["vk"] += 1

                def forbidden_validate(_row):
                    calls["validate"] += 1
                    return None

                writes: list[tuple[str, list[dict]]] = []
                def capture_write(batch, *, stage):
                    if batch:
                        writes.append((stage, [dict(row) for row in batch]))

                mod.fetch_telegram = forbidden_telegram_fetch
                mod.fetch_vk = forbidden_vk_fetch
                mod.validate_image = forbidden_validate
                mod.ydb_upsert_image_rows = capture_write
                mod.ydb_update_source_visual_rollups = lambda: None
                blocked_input = [
                    {
                        "image_queue_id": "unsigned_tg",
                        "post_url": "https://t.me/example/1",
                        "image_queue_status": "needs_actual_image_fetch",
                    },
                    {
                        "image_queue_id": "local_vk",
                        "post_url": "https://vk.com/wall-1_2",
                        "image_queue_status": "needs_actual_image_fetch",
                        "publication_eligibility_decision": "accept",
                        "publication_eligibility_gate_version": "publication-gate-test-v1",
                        "source_quick_class": "local_region_source",
                    },
                ]

                result = mod.process_batch(blocked_input, 1)

                self.assertEqual(calls, {"telegram": 0, "vk": 0, "validate": 0})
                self.assertEqual(len(result), 2)
                self.assertTrue(all(row["image_queue_status"] == "rejected_publication_eligibility" for row in result))
                self.assertEqual(mod.input_payload["publication_eligibility_pending_count"], 0)
                self.assertEqual(mod.input_payload["publication_eligibility_blocked_count"], 2)
                self.assertEqual([stage for stage, _batch in writes], ["blocked_publication_eligibility"])
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

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
