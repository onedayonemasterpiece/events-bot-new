from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
import json
import shutil
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "kaggle" / "RegionTalkImageDiagnostic" / "region_talk_image_diagnostic.py"
EXECUTOR_PATH = ROOT / "kaggle" / "execute_region_talk_image_diagnostic.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_image_diagnostic", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_executor_module():
    spec = importlib.util.spec_from_file_location("execute_region_talk_image_diagnostic", EXECUTOR_PATH)
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

    def test_long_model_and_inference_stages_publish_business_heartbeats(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            heartbeat = mock.Mock()
            mod.write_region_talk_image_diag_heartbeat = heartbeat
            mod.log_event("model_load_started", phase="model_load", model="clip")
            mod.log_event("image_inference_current", phase="inference", index=1, total=1)
            self.assertEqual(heartbeat.call_count, 2)
            self.assertEqual(heartbeat.call_args_list[0].args[0]["event_name"], "model_load_started")
            self.assertEqual(heartbeat.call_args_list[1].args[0]["event_name"], "image_inference_current")
            self.assertIn("model", mod.IMAGE_DIAG_HEARTBEAT_FIELDS)
            self.assertIn("post_url", mod.IMAGE_DIAG_HEARTBEAT_FIELDS)
            self.assertIn("load_seconds", mod.IMAGE_DIAG_HEARTBEAT_FIELDS)
            self.assertIn("final_visual_score", mod.IMAGE_DIAG_HEARTBEAT_FIELDS)
            self.assertIn("model_origin", mod.IMAGE_DIAG_HEARTBEAT_FIELDS)
            self.assertIn("model_reference", mod.IMAGE_DIAG_HEARTBEAT_FIELDS)

    def test_clip_model_reference_prefers_complete_explicit_local_directory(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
            "REGION_TALK_CLIP_MODEL_LOCAL_PATH",
            "REGION_TALK_CLIP_REQUIRE_LOCAL_MODEL",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                model_dir = Path(td) / "clip"
                model_dir.mkdir()
                for name in ("config.json", "preprocessor_config.json", "tokenizer.json"):
                    (model_dir / name).write_text("{}", encoding="utf-8")
                (model_dir / "pytorch_model.bin").write_bytes(b"weights")
                os.environ["REGION_TALK_CLIP_MODEL_LOCAL_PATH"] = str(model_dir)
                os.environ["REGION_TALK_CLIP_REQUIRE_LOCAL_MODEL"] = "1"

                reference, origin = mod.clip_model_reference()

                self.assertEqual(reference, str(model_dir))
                self.assertEqual(origin, "local_model_path")
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_clip_model_reference_fails_fast_when_local_input_is_required(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
            "REGION_TALK_CLIP_MODEL_LOCAL_PATH",
            "REGION_TALK_CLIP_REQUIRE_LOCAL_MODEL",
            "REGION_TALK_KAGGLE_INPUT_ROOT",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                os.environ.pop("REGION_TALK_CLIP_MODEL_LOCAL_PATH", None)
                os.environ["REGION_TALK_CLIP_REQUIRE_LOCAL_MODEL"] = "1"
                os.environ["REGION_TALK_KAGGLE_INPUT_ROOT"] = str(Path(td) / "empty-input")

                with self.assertRaisesRegex(FileNotFoundError, "complete local CLIP model input"):
                    mod.clip_model_reference()
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_launcher_attaches_pinned_clip_model_source(self) -> None:
        keys = ("KAGGLE_USERNAME", "REGION_TALK_CLIP_KAGGLE_MODEL_SOURCE")
        old = {key: os.environ.get(key) for key in keys}
        staged = None
        try:
            os.environ["KAGGLE_USERNAME"] = "unit-user"
            os.environ.pop("REGION_TALK_CLIP_KAGGLE_MODEL_SOURCE", None)
            executor = load_executor_module()
            staged = executor.stage_kernel("unit-run", "unit-image-diagnostic")
            metadata = json.loads((staged / "kernel-metadata.json").read_text(encoding="utf-8"))
            self.assertIn(executor.DEFAULT_CLIP_KAGGLE_MODEL_SOURCE, metadata["model_sources"])
            self.assertFalse(metadata["enable_gpu"])
        finally:
            if staged is not None:
                shutil.rmtree(staged.parent, ignore_errors=True)
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

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

    def test_vk_fetch_downloads_every_photo_attachment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            old_token = os.environ.get("VK_USER_TOKEN")
            os.environ["VK_USER_TOKEN"] = "unit-token"

            class Response:
                def __init__(self, *, payload=None, content=b""):
                    self._payload = payload
                    self.content = content

                def json(self):
                    return self._payload

                def raise_for_status(self):
                    return None

            payload = {
                "response": [{
                    "attachments": [
                        {"type": "photo", "photo": {"sizes": [{"width": 100, "height": 100, "url": "https://cdn.example/1.jpg"}]}},
                        {"type": "photo", "photo": {"sizes": [{"width": 200, "height": 100, "url": "https://cdn.example/2.jpg"}]}},
                    ]
                }]
            }

            def fake_get(url, **_kwargs):
                if "wall.getById" in url:
                    return Response(payload=payload)
                return Response(content=url.encode("utf-8"))

            try:
                with mock.patch.object(mod.requests, "get", side_effect=fake_get):
                    row = {"image_queue_id": "vk-album", "post_url": "https://vk.com/wall-1_2"}
                    mod.fetch_vk(row)
                self.assertEqual(len(mod._actual_media_paths(row)), 2)
                self.assertEqual(row["expected_image_count"], 2)
                self.assertEqual(row["fetched_image_count"], 2)
                self.assertEqual(row["image_acquisition_status"], "complete")
            finally:
                if old_token is None:
                    os.environ.pop("VK_USER_TOKEN", None)
                else:
                    os.environ["VK_USER_TOKEN"] = old_token

    def test_image_specific_runtime_config_wins_over_generic_glob_order(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
            "REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                root = Path(td)
                generic = root / "generic"
                image = root / "image"
                generic.mkdir()
                image.mkdir()
                (generic / "region_talk_run_config.json").write_text(
                    '{"env":{"REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS":"600"}}',
                    encoding="utf-8",
                )
                (image / "region_talk_run_config.json").write_text(
                    '{"env":{"REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS":"0"}}',
                    encoding="utf-8",
                )
                # Deliberately put the generic config last: preferred_parent
                # must still make the image-specific zero win.
                mod.load_runtime_config(
                    preferred_parent=image,
                    config_paths=[image / "region_talk_run_config.json", generic / "region_talk_run_config.json"],
                )
                self.assertEqual(os.environ["REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS"], "0")
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

    def test_ydb_queue_leases_only_eligible_rows_and_separates_refresh_from_terminal_block(self) -> None:
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
                self.assertEqual(mod.input_payload["publication_eligibility_blocked_count"], 2)
                self.assertEqual(mod.input_payload["publication_eligibility_refresh_deferred_count"], 1)

                blocked_writes = [batch for stage, batch in writes if stage == "blocked_publication_eligibility"]
                self.assertEqual(len(blocked_writes), 1)
                blocked = blocked_writes[0]
                self.assertEqual({row["image_queue_id"] for row in blocked}, {"rejected", "legacy", "local"})
                by_id = {row["image_queue_id"]: row for row in blocked}
                self.assertEqual(by_id["legacy"]["image_queue_status"], "needs_actual_image_fetch")
                self.assertEqual(by_id["legacy"]["image_eligibility_status"], "deferred_refresh")
                self.assertEqual(by_id["legacy"]["next_action"], "recompute_publication_eligibility_before_image_analysis")
                self.assertEqual(by_id["rejected"]["image_queue_status"], "rejected_publication_eligibility")
                self.assertEqual(by_id["local"]["image_queue_status"], "rejected_publication_eligibility")
                self.assertEqual(by_id["rejected"]["image_eligibility_status"], "blocked")
                self.assertEqual(by_id["local"]["image_eligibility_status"], "blocked")
                self.assertTrue(all(row["image_eligibility_reason"] for row in blocked))
                self.assertTrue(all(row["image_eligibility_expected_gate_version"] == "publication-gate-test-v1" for row in blocked))
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_same_run_seen_key_is_not_released_from_stale_read(self) -> None:
        keys = (
            "REGION_TALK_IMAGE_DIAG_OUTPUT_DIR",
            "REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT",
            "REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                row = {
                    "image_queue_id": "already-processed",
                    "image_queue_order": 1,
                    "image_queue_status": "needs_actual_image_fetch",
                    "post_url": "https://t.me/example/1",
                    "kaliningrad_oblast_only_scope": "true",
                    "kaliningrad_mention_role": "main_subject",
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                }
                mod.PROCESSED_IMAGE_KEYS.add("already-processed")
                mod.ydb_select_image_queue = lambda _limit: [dict(row)]
                writes = []
                mod.ydb_upsert_image_rows = lambda batch, *, stage: writes.append((stage, batch))
                leased, total = mod.ydb_rows_for_diagnostic(10)
                self.assertEqual(total, 1)
                self.assertEqual(leased, [])
                self.assertFalse(any(stage == "leased_for_image_analysis" and batch for stage, batch in writes))
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

    def test_post_media_marker_is_not_treated_as_direct_image_url(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            self.assertEqual(mod.direct_image_url("https://t.me/example/123#media"), "")
            self.assertEqual(
                mod.direct_image_url("https://cdn.example/photo.jpg?size=large"),
                "https://cdn.example/photo.jpg?size=large",
            )

    def test_process_batch_blocks_or_defers_before_media_fetch_or_scoring(self) -> None:
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
                by_id = {row["image_queue_id"]: row for row in result}
                self.assertEqual(by_id["unsigned_tg"]["image_queue_status"], "needs_actual_image_fetch")
                self.assertEqual(by_id["unsigned_tg"]["image_eligibility_status"], "deferred_refresh")
                self.assertEqual(by_id["local_vk"]["image_queue_status"], "rejected_publication_eligibility")
                self.assertEqual(mod.input_payload["publication_eligibility_pending_count"], 0)
                self.assertEqual(mod.input_payload["publication_eligibility_blocked_count"], 1)
                self.assertEqual(mod.input_payload["publication_eligibility_refresh_deferred_count"], 1)
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

    def test_repeated_empty_media_fetch_becomes_nonterminal_visual_review_after_bounded_attempts(self) -> None:
        old_attempts = os.environ.get("REGION_TALK_IMAGE_MAX_MEDIA_FETCH_ATTEMPTS")
        os.environ["REGION_TALK_IMAGE_MAX_MEDIA_FETCH_ATTEMPTS"] = "3"
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                row = {
                    "image_queue_id": "imgq_empty",
                    "image_queue_status": "image_analysis_in_progress",
                    "media_fetch_attempt_count": 3,
                    "media_fetch_status": "needs_actual_image_fetch",
                    "media_fetch_error": "download_media returned empty path",
                    "actual_image_count": 0,
                }
                mod.apply_image_queue_status(row)
                self.assertEqual(row["image_queue_status"], "needs_visual_review")
                self.assertEqual(row["media_acquisition_status"], "media_fetch_exhausted_requires_nonterminal_review")
                self.assertEqual(row["image_quality_decision"], "needs_visual_review")
                self.assertEqual(row["image_quality_terminality"], "nonterminal")
                self.assertEqual(row["next_action"], "visual_review_or_acquisition_repair")
                self.assertEqual(row["media_fetch_retry_exhausted"], "true")
            finally:
                if old_attempts is None:
                    os.environ.pop("REGION_TALK_IMAGE_MAX_MEDIA_FETCH_ATTEMPTS", None)
                else:
                    os.environ["REGION_TALK_IMAGE_MAX_MEDIA_FETCH_ATTEMPTS"] = old_attempts

    def test_media_refs_and_manifest_are_plural_and_compact(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            first = Path(td) / "first.jpg"
            second = Path(td) / "second.jpg"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            row = {"image_queue_id": "album"}
            mod._apply_acquired_paths(
                row,
                [str(first), str(second)],
                media_ids=["tg:10", "tg:11"],
                expected=2,
                status="complete",
            )
            self.assertEqual(mod._actual_media_paths(row), [str(first), str(second)])
            self.assertEqual(row["expected_image_count"], 2)
            self.assertEqual(row["fetched_image_count"], 2)
            self.assertEqual(row["distinct_image_count"], 2)
            self.assertEqual(row["image_acquisition_status"], "complete")
            self.assertTrue(row["input_media_manifest_hash"])
            self.assertNotIn("path", row["media_manifest_items"][0])

    def test_album_quality_low_score_is_review_not_terminal_reject(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "expected_image_count": 2,
                "fetched_image_count": 2,
                "image_acquisition_status": "complete",
            }
            frames = [
                {
                    "frame_index": index,
                    "media_id": f"tg:{index}",
                    "final_visual_status": "scored_actual_image",
                    "final_visual_score": 0.50 + index / 100,
                    "cv_overall_media_score": 0.6,
                    "clip_postcardness_score": 0.4,
                    "laion_aesthetic_score": 0.5,
                    "nima_quality_score": 0.5,
                    "cv_technical_quality_score": 0.7,
                }
                for index in (1, 2)
            ]
            mod.apply_album_quality_decision(row, frames)
            mod.apply_image_queue_status(row)
            self.assertEqual(row["image_quality_decision"], "needs_visual_review")
            self.assertEqual(row["image_quality_terminality"], "nonterminal")
            self.assertEqual(row["image_queue_status"], "actual_scored")
            self.assertEqual(row["images_scored_actual_count"], 2)
            self.assertEqual(row["shadow_best_frame_index"], 2)

    def test_missing_required_component_routes_to_scoring_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {"expected_image_count": 1, "fetched_image_count": 1, "image_acquisition_status": "complete"}
            frame = {
                "frame_index": 1,
                "media_id": "tg:1",
                "final_visual_status": "scored_actual_image",
                "final_visual_score": 0.9,
                "cv_overall_media_score": 0.9,
                "clip_postcardness_score": 0.9,
                "laion_aesthetic_score": 0.9,
                # NIMA deliberately absent.
            }
            mod.apply_album_quality_decision(row, [frame])
            mod.apply_image_queue_status(row)
            self.assertEqual(row["image_quality_decision"], "scoring_retry")
            self.assertEqual(row["image_queue_status"], "scoring_retry")

    def test_legacy_low_score_row_is_reopened_once_for_v2_album_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            legacy = {
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
                "overall_media_score": 0.5,
                "image_decision_contract_version": "legacy-v1",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "region_talk_publication_eligibility_v4",
            }
            self.assertTrue(mod.image_row_needs_contract_rescore(legacy))
            legacy["image_decision_contract_version"] = mod.IMAGE_DECISION_CONTRACT_VERSION
            self.assertFalse(mod.image_row_needs_contract_rescore(legacy))

    def test_all_supported_legacy_gate_versions_can_enter_bounded_low_score_rescore(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            for version in sorted(mod.LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS):
                with self.subTest(version=version):
                    row = {
                        "image_queue_status": "actual_scored",
                        "image_model_input_type": "actual_image",
                        "overall_media_score": 0.5,
                        "image_decision_contract_version": "legacy-v1",
                        "publication_eligibility_decision": "accept",
                        "publication_eligibility_gate_version": version,
                    }
                    self.assertTrue(mod.image_row_needs_contract_rescore(row))

    def test_stale_high_actual_image_attestation_is_deferred_without_losing_score(self) -> None:
        old_expected = os.environ.get("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION")
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = (
                    "region_talk_publication_eligibility_v5"
                )
                row = {
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "images_scored_actual_count": 3,
                    "overall_media_score": 0.8,
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "region_talk_publication_eligibility_v4",
                }
                eligible, deferred = mod.partition_publication_eligible_rows([row])
                self.assertEqual(eligible, [])
                self.assertEqual(len(deferred), 1)
                self.assertEqual(row["image_queue_status"], "actual_scored")
                self.assertEqual(row["images_scored_actual_count"], 3)
                self.assertEqual(row["image_eligibility_status"], "deferred_refresh")
                # The YDB writer reapplies the audit immediately before the
                # UPSERT; that must not collapse refresh work back to blocked.
                mod.apply_publication_eligibility_audit(row)
                self.assertEqual(row["image_eligibility_status"], "deferred_refresh")
            finally:
                if old_expected is None:
                    os.environ.pop("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION", None)
                else:
                    os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = old_expected

    def test_previous_version_only_terminalization_is_restored_to_actual_scored(self) -> None:
        old_expected = os.environ.get("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION")
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = (
                    "region_talk_publication_eligibility_v5"
                )
                row = {
                    "image_queue_status": "rejected_publication_eligibility",
                    "image_model_input_type": "actual_image",
                    "images_scored_actual_count": 4,
                    "overall_media_score": 0.8,
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "region_talk_publication_eligibility_v4",
                }
                eligible, deferred = mod.partition_publication_eligible_rows([row])
                self.assertEqual(eligible, [])
                self.assertEqual(len(deferred), 1)
                self.assertEqual(row["image_queue_status"], "actual_scored")
                self.assertEqual(row["images_scored_actual_count"], 4)
                self.assertEqual(row["previous_image_queue_status"], "rejected_publication_eligibility")
                self.assertEqual(row["image_eligibility_status"], "deferred_refresh")
            finally:
                if old_expected is None:
                    os.environ.pop("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION", None)
                else:
                    os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = old_expected

    def test_v4_album_rescore_remains_authorized_after_lease_status_change(self) -> None:
        old_expected = os.environ.get("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION")
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = (
                    "region_talk_publication_eligibility_v5"
                )
                row = {
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "overall_media_score": 0.5,
                    "image_decision_contract_version": "legacy-v1",
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "region_talk_publication_eligibility_v4",
                }
                eligible, blocked = mod.partition_publication_eligible_rows([row])
                self.assertEqual(len(eligible), 1)
                self.assertEqual(blocked, [])

                row["image_queue_status"] = "image_analysis_in_progress"
                eligible, blocked = mod.partition_publication_eligible_rows([row])
                self.assertEqual(len(eligible), 1)
                self.assertEqual(blocked, [])
            finally:
                if old_expected is None:
                    os.environ.pop("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION", None)
                else:
                    os.environ["REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION"] = old_expected

    def test_failed_v4_to_v5_rescore_cycle_is_recoverable_but_semantic_reject_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_status": "rejected_publication_eligibility",
                "image_model_input_type": "actual_image",
                "overall_media_score": 0.5,
                "image_decision_contract_version": "legacy-v1",
                "publication_eligibility_decision": "reject",
                "publication_eligibility_gate_version": "region_talk_publication_eligibility_v5",
                "publication_eligibility_reason": "image_queue_not_actual_scored",
                "image_eligibility_decision": "accept",
                "image_eligibility_gate_version": "region_talk_publication_eligibility_v4",
                "image_eligibility_reason": (
                    "publication_eligibility_gate_version_mismatch:expected="
                    "region_talk_publication_eligibility_v5;actual=region_talk_publication_eligibility_v4"
                ),
            }
            self.assertTrue(mod.image_row_needs_contract_rescore(row))
            self.assertEqual(mod.publication_eligibility_gate_reason(row), "")

            row["publication_eligibility_reason"] = "source_local"
            self.assertFalse(mod.image_row_needs_contract_rescore(row))
            self.assertEqual(
                mod.publication_eligibility_gate_reason(row),
                "publication_eligibility_decision_not_accept:reject",
            )

    def test_telegram_album_selection_uses_exact_grouped_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)

            class Message:
                def __init__(self, message_id, grouped_id):
                    self.id = message_id
                    self.grouped_id = grouped_id

            class Client:
                async def get_messages(self, _handle, ids):
                    self.ids = ids
                    return [Message(102, 7), Message(100, 7), Message(101, 8), None]

            client = Client()
            anchor = Message(100, 7)
            selected = __import__("asyncio").run(mod._telegram_album_messages(client, "example", anchor, 100))
            self.assertEqual([message.id for message in selected], [100, 102])
            self.assertIn(100, client.ids)

    def test_locked_operator_positives_cannot_be_terminal_quality_rejects(self) -> None:
        fixture_path = ROOT / "tests" / "fixtures" / "region_talk_image_scoring_review_cases.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            for case in fixture["operator_confirmed_positive_regressions"]:
                row = {
                    "expected_image_count": case["album_image_count"],
                    "fetched_image_count": 1,
                    "image_acquisition_status": "partial",
                }
                frame = {
                    "frame_index": 1,
                    "media_id": "anchor",
                    "final_visual_status": "scored_actual_image",
                    "final_visual_score": case["overall_media_score"],
                    "cv_overall_media_score": case["cv_overall_media_score"],
                    "clip_postcardness_score": case["clip_postcardness_score"],
                    "laion_aesthetic_score": case["laion_aesthetic_score"],
                    "nima_quality_score": case["nima_quality_score"],
                    "cv_technical_quality_score": case["technical_quality_score"],
                }
                mod.apply_album_quality_decision(row, [frame])
                mod.apply_image_queue_status(row)
                self.assertTrue(case["must_not_be_terminal_quality_reject"], case["post_url"])
                self.assertEqual(row["image_quality_decision"], "needs_visual_review", case["post_url"])
                self.assertEqual(row["image_quality_terminality"], "nonterminal", case["post_url"])

    def test_image_rollup_repairs_legacy_raw_score_source_exclusion(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            images = [
                {
                    "post_url": f"https://t.me/visual/{index}",
                    "source_url": "https://t.me/visual",
                    "kaliningrad_oblast_only_scope": True,
                    "kaliningrad_mention_role": "main_subject",
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "overall_media_score": score,
                }
                for index, score in enumerate((0.2, 0.3, 0.4), 1)
            ]
            source = {
                "canonical_source_key": "telegram:visual",
                "source_url": "https://t.me/visual",
                "source_queue_status": "processed_found_ko_low_image_quality",
                "monitoring_exclusion_reason": "kaliningrad_posts_found_but_actual_images_systematically_low_score",
            }
            writes = []
            mod.ydb_select_kind = lambda kind, _limit: images if kind == "image_queue_item" else []
            mod.ydb_select_source_queue = lambda _limit: [source]
            mod.ydb_upsert_source_rows = lambda rows, *, stage: writes.extend(dict(row) for row in rows)
            mod.ydb_update_source_visual_rollups()
            self.assertEqual(len(writes), 1)
            self.assertEqual(writes[0]["source_queue_status"], "processed_found_ko_candidate")
            self.assertEqual(writes[0]["source_image_quality_status"], "unadjudicated_raw_score_low_observation")
            self.assertEqual(writes[0]["monitoring_exclusion_reason"], "")


if __name__ == "__main__":
    unittest.main()
