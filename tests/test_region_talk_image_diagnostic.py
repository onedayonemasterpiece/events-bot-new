from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
import json
import shutil
from pathlib import Path
from types import ModuleType
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

    def test_local_clip_load_does_not_disable_huggingface_for_later_nima(self) -> None:
        old_offline = os.environ.pop("HF_HUB_OFFLINE", None)
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                mod.CLIP = {"loaded": False, "error": None}
                mod.clip_model_reference = lambda: ("/pinned/clip", "kaggle_model_input")
                fake_torch = ModuleType("torch")
                fake_torch.cuda = mock.Mock()
                fake_torch.cuda.is_available.return_value = False
                fake_transformers = ModuleType("transformers")
                processor = mock.Mock()
                processor.from_pretrained.return_value = object()
                model = mock.Mock()
                model_instance = mock.Mock()
                model_instance.to.return_value = model_instance
                model.from_pretrained.return_value = model_instance
                fake_transformers.CLIPProcessor = processor
                fake_transformers.CLIPModel = model

                with mock.patch.dict(sys.modules, {"torch": fake_torch, "transformers": fake_transformers}):
                    self.assertTrue(mod.maybe_clip())

                self.assertNotIn("HF_HUB_OFFLINE", os.environ)
                processor.from_pretrained.assert_called_once_with("/pinned/clip", local_files_only=True)
                model.from_pretrained.assert_called_once_with("/pinned/clip", local_files_only=True)
            finally:
                if old_offline is not None:
                    os.environ["HF_HUB_OFFLINE"] = old_offline

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

    def test_web_publication_fetches_direct_image_without_platform_api(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)

            def fake_download(url, path):
                self.assertEqual(url, "https://cdn.example/publication.jpg")
                Path(path).write_bytes(b"image")
                return str(path)

            mod._download_http_image = fake_download
            row = {
                "image_queue_id": "web-publication",
                "post_url": "https://publisher.example/article",
                "image_url_or_local_path": "https://cdn.example/publication.jpg",
                "media_count": 1,
                "rights_policy": "link_only",
                "media_use_policy": "score_only_no_reuse",
            }

            mod.fetch_web_direct(row)

            self.assertEqual(row["media_fetch_status"], "downloaded_public_url")
            self.assertEqual(row["image_acquisition_status"], "complete")
            self.assertEqual(row["expected_image_count"], 1)
            self.assertEqual(row["fetched_image_count"], 1)
            self.assertTrue(Path(row["actual_media_path"]).exists())
            self.assertEqual(row["rights_policy"], "link_only")
            self.assertEqual(row["media_use_policy"], "score_only_no_reuse")

    def test_editorial_gallery_extractor_ignores_site_chrome_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            page = """
                <a href="/related/thumb.jpg"><img src="/related/thumb.jpg"></a>
                <a data-fancybox="slider" href="//cdn.example/full-1.jpg">one</a>
                <a data-fancybox="slider" href="/media/full-2.webp?quality=95">two</a>
                <a data-fancybox="slider" href="//cdn.example/full-1.jpg">duplicate</a>
                <a data-lightbox="article" href="https://cdn.example/full-3.png">three</a>
            """
            self.assertEqual(
                mod.extract_editorial_gallery_image_urls(
                    page, base_url="https://publisher.example/story"
                ),
                [
                    "https://cdn.example/full-1.jpg",
                    "https://publisher.example/media/full-2.webp?quality=95",
                    "https://cdn.example/full-3.png",
                ],
            )

    def test_http_article_image_extraction_keeps_evidence_and_rejects_logo_related_assets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            page = """
                <head>
                  <meta property="og:image" content="/assets/site-logo.jpg">
                  <meta name="twitter:image" content="/media/coast-twitter.jpg">
                  <script type="application/ld+json">
                    {"@type":"NewsArticle","image":{"url":"/media/planet-ocean.jpg","width":1200,"height":800,"caption":"Музей Планета Океан"}}
                  </script>
                </head>
                <main><article>
                  <figure><img src="/media/facade.jpg" width="1000" height="700" alt="Фасад Планеты Океан">
                    <figcaption>Новый корпус музея на набережной</figcaption>
                  </figure>
                  <img class="related recommendation" src="/related/other-story.jpg">
                  <img src="/pixel.gif" width="1" height="1">
                </article></main>
            """

            candidates = mod.extract_external_publication_image_candidates(
                page,
                base_url="https://publisher.example/story",
                article_title="Планета Океан: новый корпус музея",
                article_summary="Архитектурный разбор фасада и пространства на набережной.",
            )

            self.assertEqual(
                [item["url"] for item in candidates],
                [
                    "https://publisher.example/media/planet-ocean.jpg",
                    "https://publisher.example/media/facade.jpg",
                    "https://publisher.example/media/coast-twitter.jpg",
                ],
            )
            self.assertEqual(candidates[0]["role"], "jsonld_article_image")
            self.assertEqual(candidates[1]["role"], "article_figure")
            self.assertIn("набережной", candidates[1]["caption"])
            self.assertEqual(candidates[1]["referrer"], "https://publisher.example/story")
            self.assertEqual(candidates[1]["association_decision"], "accept")

    def test_media_first_contract_falls_back_to_preview_without_selected_media(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {"content_origin_type": "editorial_publication"}

            mod.apply_media_first_presentation_contract(row)

            self.assertEqual(row["presentation_recommendation"], "system_link_preview")
            self.assertEqual(row["visual_asset_rights_status"], "not_independently_verified")
            self.assertEqual(row["source_attribution_required"], "true")
            self.assertEqual(row["presentation_max_assets"], 0)

            selected = {
                "content_origin_type": "editorial_publication",
                "image_quality_decision": "vlm_visual_accept",
                "image_vlm_article_association_supported": "true",
                "selected_primary_media_id": "web_direct:2",
                "fetched_image_count": 3,
            }
            mod.apply_media_first_presentation_contract(selected)
            self.assertEqual(selected["presentation_recommendation"], "article_single_source_image")
            self.assertEqual(selected["presentation_max_assets"], 1)

            social_album = {"image_quality_decision": "vlm_visual_accept", "fetched_image_count": 14}
            mod.apply_media_first_presentation_contract(social_album)
            self.assertEqual(social_album["presentation_recommendation"], "source_media_carousel")
            self.assertEqual(social_album["presentation_max_assets"], 6)

    def test_external_publication_fetches_intentional_article_gallery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)

            class Headers:
                def get(self, key, default=None):
                    return "text/html; charset=utf-8" if key == "Content-Type" else default

                def get_content_charset(self):
                    return "utf-8"

            class Response:
                headers = Headers()

                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return None

                def read(self, _limit):
                    return (
                        '<a href="/chrome.jpg">chrome</a>'
                        '<a data-fancybox="slider" href="/gallery/1.jpg">one</a>'
                        '<a data-fancybox="slider" href="/gallery/2.jpg">two</a>'
                    ).encode("utf-8")

            downloaded: list[str] = []

            def fake_download(url, path):
                downloaded.append(url)
                Path(path).write_bytes(url.encode("utf-8"))
                return str(path)

            mod._download_http_image = fake_download
            row = {
                "image_queue_id": "editorial-gallery",
                "post_url": "https://publisher.example/article",
                "content_origin_type": "editorial_publication",
                "publication_content_type": "architecture_criticism",
                "media_acquisition_target_type": "external_article_page",
                "media_acquisition_target_url": "https://publisher.example/article",
                "has_media": "false",
                "media_count": 0,
                "rights_policy": "link_only",
                "media_use_policy": "score_only_no_reuse",
            }
            with mock.patch.object(mod, "_public_urlopen", return_value=Response()):
                mod.fetch_web_direct(row)

            self.assertEqual(
                downloaded,
                [
                    "https://publisher.example/gallery/1.jpg",
                    "https://publisher.example/gallery/2.jpg",
                ],
            )
            self.assertEqual(row["web_gallery_discovered_count"], 2)
            self.assertEqual(row["web_gallery_used_count"], 2)
            self.assertEqual(row["expected_image_count"], 2)
            self.assertEqual(row["fetched_image_count"], 2)
            self.assertEqual(row["image_acquisition_status"], "complete")
            self.assertEqual(row["rights_policy"], "link_only")
            self.assertEqual(row["media_use_policy"], "score_only_no_reuse")
            materialization = json.loads(row["media_materialization_items_json"])
            self.assertEqual(materialization[0]["source_ref"], "https://publisher.example/gallery/1.jpg")
            self.assertEqual(materialization[0]["refetch_locator"]["canonical_page_url"], "https://publisher.example/article")
            self.assertEqual(materialization[0]["refetch_locator"]["method"], "article_page_image_evidence")
            self.assertEqual(materialization[0]["refetch_locator"]["dom_role"], "article_lightbox")
            self.assertTrue(materialization[0]["reviewed_content_sha256"])
            self.assertTrue(materialization[0]["materialization_fingerprint"])

    def test_js_only_article_requests_bounded_browser_materialization_instead_of_silent_preview(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)

            class Headers:
                def get(self, key, default=None):
                    return "text/html; charset=utf-8" if key == "Content-Type" else default
                def get_content_charset(self):
                    return "utf-8"

            class Response:
                headers = Headers()
                def __enter__(self): return self
                def __exit__(self, *_args): return None
                def read(self, _limit):
                    return b'<main id="app"></main><script>renderArticleImages()</script>'

            row = {
                "image_queue_id": "js-only-article",
                "post_url": "https://publisher.example/js-story",
                "content_origin_type": "editorial_publication",
            }
            with mock.patch.object(mod, "_public_urlopen", return_value=Response()):
                mod.fetch_web_direct(row)
            mod.apply_image_queue_status(row)

            self.assertEqual(row["media_fetch_status"], "needs_browser_materialization")
            self.assertEqual(row["image_queue_status"], "needs_browser_materialization")
            self.assertEqual(row["presentation_recommendation"], "browser_materialization_pending")
            request = json.loads(row["browser_materialization_request_json"])
            self.assertEqual(request["canonical_page_url"], "https://publisher.example/js-story")
            self.assertEqual(request["max_pages"], 1)
            self.assertLessEqual(request["max_assets"], 20)
            self.assertIn("article figure img", request["selectors"])

    def test_architecture_track_uses_editorial_prompts_not_scenic_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "content_origin_type": "editorial_publication",
                "publication_content_type": "architecture_criticism",
            }
            track = mod.visual_content_track(row)
            positive, negative = mod.clip_prompt_bank(track)
            prompt = mod._visual_adjudication_prompt(row, 5)

            self.assertEqual(track, "architecture_interior_editorial")
            self.assertIn("professional architectural photography", positive)
            self.assertIn("screenshot", negative)
            self.assertIn("не требуй", prompt)
            self.assertIn("профессиональная архитектурная", prompt)
            self.assertIn("editorial_suitability_score", prompt)

    def test_browser_materialized_refs_feed_existing_image_download_and_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            evidence = [{
                "url": "https://cdn.example/rendered-hero.jpg",
                "source_url": "https://cdn.example/rendered-hero.jpg",
                "role": "article_figure",
                "caption": "Фасад музея",
                "association_decision": "accept",
                "association_reason": "publisher_declared_role_with_textual_match",
                "evidence_version": "region_talk_rendered_article_image_evidence_v1",
            }]
            row = {
                "image_queue_id": "browser-ready",
                "post_url": "https://publisher.example/js-story",
                "content_origin_type": "editorial_publication",
                "browser_materialization_status": "materialized",
                "browser_materialized_image_urls": ["https://cdn.example/rendered-hero.jpg"],
                "browser_materialization_evidence_json": json.dumps(evidence),
            }

            def fake_download(_url, path, **_kwargs):
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_bytes(b"rendered image bytes")
                return str(path)

            with (
                mock.patch.object(mod, "_public_urlopen", side_effect=RuntimeError("static page blocked")),
                mock.patch.object(mod, "_download_http_image", side_effect=fake_download),
            ):
                mod.fetch_web_direct(row)

            self.assertEqual(row["media_fetch_status"], "downloaded_public_url")
            used = json.loads(row["web_image_used_evidence_json"])
            self.assertEqual(used[0]["role"], "article_figure")
            materialization = json.loads(row["media_materialization_items_json"])
            self.assertEqual(materialization[0]["source_ref"], "https://cdn.example/rendered-hero.jpg")
            self.assertEqual(materialization[0]["refetch_locator"]["dom_role"], "article_figure")

    def test_public_http_guard_rejects_private_or_mixed_dns_answers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            public = [(None, None, None, None, ("93.184.216.34", 443))]
            self.assertEqual(
                mod._public_http_url("https://publisher.example/article", resolver=lambda *_a, **_k: public),
                "https://publisher.example/article",
            )
            mixed = public + [(None, None, None, None, ("10.0.0.2", 443))]
            with self.assertRaisesRegex(ValueError, "non-public"):
                mod._public_http_url("https://publisher.example/article", resolver=lambda *_a, **_k: mixed)

    def test_complete_external_gallery_enters_selective_vlm_review(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "content_origin_type": "editorial_publication",
                "publication_content_type": "architecture_criticism",
                "image_quality_decision": "needs_visual_review",
                "image_quality_reason": "uncalibrated_legacy_low_score_requires_visual_review",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "image_model_input_type": "actual_image",
                "image_acquisition_status": "complete",
                "expected_image_count": 8,
                "fetched_image_count": 8,
                "image_component_bundle_complete": "true",
                "input_media_manifest_hash": "gallery-hash",
                "overall_media_score": 0.52,
                "shadow_best_frame_score": 0.55,
            }
            self.assertTrue(mod.image_row_needs_vlm_review(row))

    def test_legacy_positive_external_article_still_gets_bounded_association_review(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "content_origin_type": "editorial_publication",
                "image_quality_decision": "legacy_auto_accept",
                "image_quality_reason": "complete_album_anchor_passed_existing_quality_contract",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "image_model_input_type": "actual_image",
                "image_acquisition_status": "complete",
                "expected_image_count": 1,
                "fetched_image_count": 1,
                "image_component_bundle_complete": "true",
                "input_media_manifest_hash": "article-image",
                "overall_media_score": 0.78,
                "shadow_best_frame_score": 0.78,
            }
            self.assertTrue(mod.image_row_needs_vlm_review(row))

    def test_vk_read_prefers_service_token_for_remote_wall_fetch(self) -> None:
        keys = (
            "VK_SERVICE_TOKEN",
            "VK_SERVICE_KEY",
            "VK_TOKEN",
            "VK_USER_TOKEN",
            "VK_ACCESS_TOKEN4",
            "VK_ACCESS_TOKEN5",
            "VK_ACCESS_TOKEN",
            "REGION_TALK_VK_READ_SERVICE_FIRST",
        )
        old = {key: os.environ.get(key) for key in keys}
        with tempfile.TemporaryDirectory() as td:
            try:
                mod = self._load_in_temp_output(td)
                for key in keys:
                    os.environ.pop(key, None)
                os.environ["VK_SERVICE_TOKEN"] = "service-token"
                os.environ["VK_ACCESS_TOKEN"] = "ip-bound-user-token"
                os.environ["REGION_TALK_VK_READ_SERVICE_FIRST"] = "1"
                self.assertEqual(mod._vk_read_token(), ("service-token", "VK_SERVICE_TOKEN"))
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

    def test_ydb_queue_prioritizes_candidate_report_selected_batch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)

            def row(queue_id: str, order: int, selected: bool) -> dict:
                return {
                    "image_queue_id": queue_id,
                    "image_queue_order": order,
                    "image_queue_status": "needs_actual_image_fetch",
                    "post_url": f"https://example.test/{queue_id}",
                    "kaliningrad_oblast_only_scope": "true",
                    "kaliningrad_mention_role": "main_subject",
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                    "selected_for_next_image_batch": str(selected).lower(),
                }

            mod.ydb_select_image_queue = lambda _limit: [
                row("older-unselected", 1, False),
                row("candidate-report-selected", 140, True),
            ]
            mod.ydb_upsert_image_rows = lambda _batch, *, stage: None

            leased, total = mod.ydb_rows_for_diagnostic(1)

            self.assertEqual(total, 2)
            self.assertEqual([item["image_queue_id"] for item in leased], ["candidate-report-selected"])

    def test_unchanged_historical_eligibility_row_is_not_rewritten(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_id": "already-blocked",
                "image_queue_status": "rejected_publication_eligibility",
                "publication_eligibility_decision": "reject",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "publication_eligibility_reason": "source_local",
                "image_eligibility_decision": "reject",
                "image_eligibility_gate_version": "publication-gate-test-v1",
                "image_eligibility_expected_gate_version": "publication-gate-test-v1",
                "image_eligibility_reason": "publication_eligibility_decision_not_accept:reject",
                "image_eligibility_status": "blocked",
                "next_action": "skip_publication_eligibility_rejected",
                "last_image_diag_run_id": "older-run",
                "last_image_diag_stage": "blocked_publication_eligibility",
                "last_image_diag_at": "2026-07-16T00:00:00+00:00",
                "queue_item_updated_at": "2026-07-16T00:00:00+00:00",
            }

            eligible, blocked = mod.partition_publication_eligible_rows([row])

            self.assertEqual(eligible, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["_image_diag_material_change"], "false")
            self.assertEqual(
                mod.image_rows_for_stage_persist(
                    blocked, stage="blocked_publication_eligibility"
                ),
                [],
            )
            with mock.patch.object(mod, "ydb_connect") as connect:
                mod.ydb_upsert_image_rows(
                    blocked, stage="blocked_publication_eligibility"
                )
            connect.assert_not_called()

    def test_real_eligibility_transition_is_selected_for_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_id": "newly-blocked",
                "image_queue_status": "needs_actual_image_fetch",
                "publication_eligibility_decision": "reject",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "publication_eligibility_reason": "source_local",
            }

            _eligible, blocked = mod.partition_publication_eligible_rows([row])

            self.assertEqual(blocked[0]["_image_diag_material_change"], "true")
            selected = mod.image_rows_for_stage_persist(
                blocked, stage="blocked_publication_eligibility"
            )
            self.assertEqual([item["image_queue_id"] for item in selected], ["newly-blocked"])

    def test_vlm_backlog_heartbeat_counts_unique_rows_across_repolls(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "post_url": "https://t.me/travelcase/album",
                "image_queue_id": "travelcase-album",
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
                "image_quality_decision": "needs_visual_review",
                "image_quality_reason": "uncalibrated_legacy_low_score_requires_visual_review",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "kaliningrad_oblast_only_scope": "true",
                "kaliningrad_mention_role": "main_subject",
                "image_acquisition_status": "complete",
                "expected_image_count": 3,
                "fetched_image_count": 3,
                "image_component_bundle_complete": "true",
                "input_media_manifest_hash": "album-hash",
                "overall_media_score": 0.61,
                "postcardness_score": 0.93,
                "shadow_best_frame_score": 0.68,
            }
            mod.ydb_select_image_queue = lambda _limit: [dict(row)]
            mod.ydb_upsert_image_rows = lambda _batch, *, stage: None

            mod.ydb_rows_for_diagnostic(10)
            mod.ydb_rows_for_diagnostic(10)

            self.assertEqual(mod.IMAGE_VLM_STATS["backlog_seen"], 1)
            self.assertEqual(mod.IMAGE_VLM_BACKLOG_KEYS, {"travelcase-album"})

    def test_partial_album_visual_review_is_reopened_for_acquisition_repair(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_id": "partial-vk-album",
                "image_queue_order": 1,
                # Scored partial albums retain actual_scored as their queue
                # status; the nonterminal quality decision carries review.
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
                "post_url": "https://vk.com/wall-1_2",
                "kaliningrad_oblast_only_scope": "true",
                "kaliningrad_mention_role": "main_subject",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "image_quality_decision": "needs_visual_review",
                "image_quality_terminality": "nonterminal",
                "image_quality_reason": "incomplete_album_never_terminal_quality_reject",
                "image_acquisition_status": "partial",
                "media_fetch_attempt_count": 1,
            }
            writes = []
            mod.ydb_select_image_queue = lambda _limit: [row]
            mod.ydb_upsert_image_rows = lambda batch, *, stage: writes.append((stage, batch))

            leased, total = mod.ydb_rows_for_diagnostic(10)

            self.assertEqual(total, 1)
            self.assertEqual(len(leased), 1)
            self.assertEqual(leased[0]["image_queue_status"], "image_analysis_in_progress")
            self.assertEqual(leased[0]["actual_image_retry_reason"], "partial_album_requires_acquisition_repair")
            self.assertTrue(any(stage == "leased_for_image_analysis" for stage, _batch in writes))

    def test_complete_strict_dual_visual_review_enters_bounded_vlm_lane(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "post_url": "https://t.me/travelcase/album",
                "image_queue_id": "travelcase-album",
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
                "image_quality_decision": "needs_visual_review",
                "image_quality_reason": "uncalibrated_legacy_low_score_requires_visual_review",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "kaliningrad_oblast_only_scope": "true",
                "kaliningrad_mention_role": "main_subject",
                "image_acquisition_status": "complete",
                "expected_image_count": 3,
                "fetched_image_count": 3,
                "image_component_bundle_complete": "true",
                "input_media_manifest_hash": "album-hash",
                "overall_media_score": 0.61,
                "postcardness_score": 0.93,
                "shadow_best_frame_score": 0.68,
            }
            self.assertTrue(mod.image_row_needs_vlm_review(row))
            fingerprint = mod.image_vlm_request_fingerprint(row)
            accepted = mod.apply_image_vlm_result(
                dict(row),
                {
                    "vlm_gate_status": "ok",
                    "vlm_decision": "accept",
                    "strong_publishable_image": True,
                    "best_image_ordinal": 2,
                    "postcardness_score": 0.9,
                    "technical_quality_score": 0.8,
                    "reason": "сильный атмосферный кадр",
                },
                fingerprint=fingerprint,
            )
            self.assertEqual(accepted["image_quality_decision"], "vlm_visual_accept")
            self.assertEqual(accepted["next_action"], "publication_verification")
            self.assertTrue(mod.image_vlm_current_verdict(accepted))
            self.assertFalse(mod.image_row_needs_vlm_review(accepted))

            rejected = mod.apply_image_vlm_result(
                dict(row),
                {
                    "vlm_gate_status": "ok",
                    "vlm_decision": "reject",
                    "strong_publishable_image": False,
                    "best_image_ordinal": 1,
                    "reason": "все кадры — скриншоты",
                },
                fingerprint=fingerprint,
            )
            self.assertEqual(rejected["image_quality_decision"], "needs_visual_review")
            self.assertEqual(rejected["image_quality_terminality"], "nonterminal")

    def test_external_article_vlm_requires_association_and_honors_ranked_best_ordinal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            base = {
                "content_origin_type": "editorial_publication",
                "post_url": "https://publisher.example/article",
                "fetched_image_count": 3,
                "input_media_manifest_hash": "article-images",
                "media_manifest_items": [
                    {"ordinal": 1, "media_id": "og:cover"},
                    {"ordinal": 2, "media_id": "figure:facade"},
                    {"ordinal": 3, "media_id": "figure:interior"},
                ],
                "media_materialization_items": [
                    {"media_id": "og:cover", "materialization_fingerprint": "fp1", "refetch_locator": {"method": "article_page_image_evidence"}},
                    {"media_id": "figure:facade", "materialization_fingerprint": "fp2", "refetch_locator": {"method": "article_page_image_evidence"}},
                    {"media_id": "figure:interior", "materialization_fingerprint": "fp3", "refetch_locator": {"method": "article_page_image_evidence"}},
                ],
                "selected_media_ids": '["og:cover","figure:interior"]',
            }
            fingerprint = mod.image_vlm_request_fingerprint(base)
            missing_association = mod.apply_image_vlm_result(
                dict(base),
                {
                    "vlm_gate_status": "ok", "vlm_decision": "accept",
                    "strong_publishable_image": True, "best_image_ordinal": 2,
                    "ranked_image_ordinals": [2, 3, 1],
                },
                fingerprint=fingerprint,
            )
            self.assertEqual(missing_association["image_quality_decision"], "needs_visual_review")

            accepted = mod.apply_image_vlm_result(
                dict(base),
                {
                    "vlm_gate_status": "ok", "vlm_decision": "accept",
                    "strong_publishable_image": True, "best_image_ordinal": 2,
                    "ranked_image_ordinals": [2, 3, 1],
                    "article_association_supported": True,
                    "article_association_reason": "Фасад соответствует архитектурному разбору.",
                },
                fingerprint=fingerprint,
            )
            self.assertEqual(accepted["image_quality_decision"], "vlm_visual_accept")
            self.assertEqual(accepted["selected_primary_image_ordinal"], 2)
            self.assertEqual(accepted["selected_primary_media_id"], "figure:facade")
            self.assertEqual(json.loads(accepted["selected_media_ids"]), ["figure:facade", "figure:interior", "og:cover"])
            selected_materialization = json.loads(accepted["selected_media_materialization_json"])
            self.assertEqual([item["media_id"] for item in selected_materialization], ["figure:facade", "figure:interior", "og:cover"])
            self.assertTrue(accepted["selected_media_materialization_fingerprint"])

    def test_vlm_lane_excludes_partial_nondual_and_weak_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            base = {
                "post_url": "https://t.me/travelcase/album",
                "image_quality_decision": "needs_visual_review",
                "image_quality_reason": "uncalibrated_legacy_low_score_requires_visual_review",
                "publication_eligibility_decision": "accept",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "kaliningrad_oblast_only_scope": "true",
                "kaliningrad_mention_role": "main_subject",
                "image_model_input_type": "actual_image",
                "image_acquisition_status": "complete",
                "expected_image_count": 2,
                "fetched_image_count": 2,
                "image_component_bundle_complete": "true",
                "input_media_manifest_hash": "hash",
                "overall_media_score": 0.60,
            }
            self.assertFalse(mod.image_row_needs_vlm_review({**base, "image_acquisition_status": "partial"}))
            self.assertFalse(mod.image_row_needs_vlm_review({**base, "text_vector_fusion_status": "missing_bge_m3_enrichment"}))
            self.assertFalse(mod.image_row_needs_vlm_review({**base, "overall_media_score": 0.4}))

    def test_launcher_ships_shared_llm_runtime_for_kaggle_vlm(self) -> None:
        source = EXECUTOR_PATH.read_text(encoding="utf-8")
        self.assertIn('PROJECT_ROOT / "region_talk_llm_runtime.py"', source)
        self.assertIn('"REGION_TALK_IMAGE_VLM_MAX_CALLS_PER_RUN"', source)
        self.assertIn('"region_talk_llm_runtime.py"]', source)

    def test_exhausted_ip_bound_vk_album_gets_one_service_token_retry_reset(self) -> None:
        old_service = os.environ.get("VK_SERVICE_KEY")
        os.environ["VK_SERVICE_KEY"] = "test-service-token"
        try:
            with tempfile.TemporaryDirectory() as td:
                mod = self._load_in_temp_output(td)
                row = {
                    "image_queue_id": "exhausted-vk-album",
                    "image_queue_order": 1,
                    "image_queue_status": "needs_visual_review",
                    "image_model_input_type": "actual_image",
                    "post_url": "https://vk.com/wall-1_3",
                    "kaliningrad_oblast_only_scope": "true",
                    "kaliningrad_mention_role": "main_subject",
                    "publication_eligibility_decision": "accept",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                    "image_quality_decision": "needs_visual_review",
                    "image_quality_terminality": "nonterminal",
                    "image_quality_reason": "media_acquisition_exhausted_without_complete_album",
                    "image_acquisition_status": "partial",
                    "media_fetch_attempt_count": 3,
                    "media_fetch_retry_exhausted": "true",
                    "media_fetch_error": "error_subcode': 1130; access_token was given to another ip address",
                }
                writes = []
                mod.ydb_select_image_queue = lambda _limit: [row]
                mod.ydb_upsert_image_rows = lambda batch, *, stage: writes.append((stage, batch))

                leased, total = mod.ydb_rows_for_diagnostic(10)

                self.assertEqual(total, 1)
                self.assertEqual(len(leased), 1)
                self.assertEqual(leased[0]["image_queue_status"], "image_analysis_in_progress")
                self.assertEqual(leased[0]["media_fetch_attempt_count"], 0)
                self.assertEqual(leased[0]["media_fetch_attempt_count_before_strategy_reset"], 3)
                self.assertEqual(leased[0]["media_fetch_retry_reset_version"], mod.IMAGE_AUTH_RETRY_RESET_VERSION)
                self.assertFalse(any(stage == "media_fetch_retry_exhausted" for stage, _batch in writes))

                # The durable marker makes the reset one-shot even if the new
                # service-token attempt later fails.
                replay = dict(leased[0])
                replay["image_queue_status"] = "needs_visual_review"
                replay["image_acquisition_status"] = "partial"
                replay["media_fetch_attempt_count"] = 3
                self.assertFalse(mod.image_row_needs_auth_strategy_retry_reset(replay))
        finally:
            if old_service is None:
                os.environ.pop("VK_SERVICE_KEY", None)
            else:
                os.environ["VK_SERVICE_KEY"] = old_service

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

    def test_soft_source_review_restores_actual_score_instead_of_terminalizing_it(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_status": "rejected_publication_eligibility",
                "image_model_input_type": "actual_image",
                "images_scored_actual_count": 0,
                "actual_image_count": 3,
                "frame_scores_available_count": 3,
                "overall_media_score": 0.71,
                "final_visual_status": "blocked_publication_eligibility",
                "media_acquisition_status": "blocked_publication_eligibility",
                "image_acquisition_status": "complete",
                "publication_eligibility_decision": "needs_source_review",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
            }

            eligible, deferred = mod.partition_publication_eligible_rows([row])

            self.assertEqual(eligible, [])
            self.assertEqual(len(deferred), 1)
            self.assertEqual(row["image_queue_status"], "actual_scored")
            self.assertEqual(row["images_scored_actual_count"], 3)
            self.assertEqual(row["overall_media_score"], 0.71)
            self.assertEqual(row["final_visual_status"], "scored_actual_image")
            self.assertEqual(row["media_acquisition_status"], "actual_album_downloaded_and_scored")
            self.assertEqual(row["image_eligibility_status"], "deferred_soft_gate")
            self.assertEqual(row["next_action"], "wait_for_source_or_text_gate_without_rescoring_image")

    def test_soft_text_review_without_image_evidence_uses_nonterminal_deferred_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_status": "rejected_publication_eligibility",
                "publication_eligibility_decision": "needs_text_review",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
            }

            eligible, deferred = mod.partition_publication_eligible_rows([row])

            self.assertEqual(eligible, [])
            self.assertEqual(len(deferred), 1)
            self.assertEqual(row["image_queue_status"], "deferred_text_gate")
            self.assertEqual(row["image_eligibility_status"], "deferred_soft_gate")
            self.assertEqual(row["next_action"], "complete_dual_text_gate")

    def test_soft_visual_review_preserves_unsupported_video_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_status": "not_reviewable_unsupported_media",
                "image_model_input_type": "unsupported_media",
                "media_acquisition_status": "unsupported_media_or_decode_failed",
                "publication_eligibility_decision": "needs_visual_review",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
            }

            eligible, deferred = mod.partition_publication_eligible_rows([row])

            self.assertEqual(eligible, [])
            self.assertEqual(len(deferred), 1)
            self.assertEqual(row["image_queue_status"], "not_reviewable_unsupported_media")
            self.assertEqual(row["image_model_input_type"], "unsupported_media")
            self.assertEqual(row["image_eligibility_status"], "deferred_soft_gate")

    def test_hard_publication_reject_closes_queue_but_preserves_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            row = {
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
                "images_scored_actual_count": 0,
                "actual_image_count": 4,
                "frame_scores_available_count": 4,
                "overall_media_score": 0.81,
                "final_visual_status": "blocked_publication_eligibility",
                "media_acquisition_status": "blocked_publication_eligibility",
                "image_acquisition_status": "complete",
                "publication_eligibility_decision": "reject",
                "publication_eligibility_gate_version": "publication-gate-test-v1",
                "publication_eligibility_reason": "source_local",
            }

            eligible, blocked = mod.partition_publication_eligible_rows([row])

            self.assertEqual(eligible, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(row["image_queue_status"], "rejected_publication_eligibility")
            self.assertEqual(row["image_eligibility_status"], "blocked")
            self.assertEqual(row["images_scored_actual_count"], 4)
            self.assertEqual(row["overall_media_score"], 0.81)
            self.assertEqual(row["final_visual_status"], "scored_actual_image")
            self.assertEqual(row["media_acquisition_status"], "actual_album_downloaded_and_scored")

    def test_ydb_poll_counts_soft_defer_separately_from_hard_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod = self._load_in_temp_output(td)
            rows = [
                {
                    "image_queue_id": "soft",
                    "image_queue_status": "needs_actual_image_fetch",
                    "publication_eligibility_decision": "needs_source_review",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                },
                {
                    "image_queue_id": "hard",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "images_scored_actual_count": 2,
                    "publication_eligibility_decision": "reject",
                    "publication_eligibility_gate_version": "publication-gate-test-v1",
                },
            ]
            writes: list[tuple[str, list[dict]]] = []
            mod.ydb_select_image_queue = lambda _limit: rows
            mod.ydb_upsert_image_rows = lambda batch, *, stage: writes.append(
                (stage, [dict(row) for row in batch])
            )

            leased, total = mod.ydb_rows_for_diagnostic(10)

            self.assertEqual(leased, [])
            self.assertEqual(total, 2)
            self.assertEqual(mod.input_payload["publication_eligibility_blocked_count"], 1)
            self.assertEqual(mod.input_payload["publication_eligibility_soft_deferred_count"], 1)
            self.assertEqual(mod.input_payload["publication_eligibility_refresh_deferred_count"], 0)
            persisted = [row for stage, batch in writes if stage == "blocked_publication_eligibility" for row in batch]
            self.assertEqual({row["image_queue_id"] for row in persisted}, {"soft", "hard"})
            self.assertEqual(next(row for row in persisted if row["image_queue_id"] == "soft")["image_queue_status"], "deferred_text_gate")
            self.assertEqual(next(row for row in persisted if row["image_queue_id"] == "hard")["images_scored_actual_count"], 2)

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
