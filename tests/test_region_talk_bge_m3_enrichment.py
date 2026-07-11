from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
BGE_MODULE_PATH = ROOT / "kaggle" / "RegionTalkBgeM3Enrichment" / "region_talk_bge_m3_enrichment.py"
BGE_RUNNER_PATH = ROOT / "kaggle" / "execute_region_talk_bge_m3_enrichment.py"


def load_bge_module():
    spec = importlib.util.spec_from_file_location("region_talk_bge_m3_enrichment", BGE_MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_bge_runner_module():
    spec = importlib.util.spec_from_file_location("execute_region_talk_bge_m3_enrichment", BGE_RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkBgeM3EnrichmentTests(unittest.TestCase):
    def test_secret_names_do_not_package_telegram_sessions(self) -> None:
        mod = load_bge_runner_module()
        names = mod.bge_secret_names()
        self.assertIn("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY1", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY2", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_E2E", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_S22", names)
        self.assertNotIn("TELEGRAM_SESSION", names)

    def test_collect_text_rows_prefers_publication_and_dedupes_when_legacy_inputs_enabled(self) -> None:
        mod = load_bge_module()
        old = os.environ.get("REGION_TALK_BGE_E5_ONLY")
        os.environ["REGION_TALK_BGE_E5_ONLY"] = "0"
        items = {
            "candidate_memory_item": {
                "candidate_memory_item:a": {"post_id": "p1", "post_url": "https://t.me/a/1", "short_summary": "Калининград и Куршская коса: личные впечатления от поездки."},
            },
            "publication_candidate_item": {
                "publication_candidate_item:b": {"post_id": "p2", "post_url": "https://t.me/b/2", "short_summary": "Зеленоградск, море и прогулка: что запомнилось автору."},
            },
            "image_queue_item": {
                "image_queue_item:dup": {"post_id": "p1", "post_url": "https://t.me/a/1", "text_excerpt": "Дубликат того же поста."},
            },
        }
        try:
            rows = mod.collect_text_rows(items, existing_pks=set(), limit=10)
        finally:
            if old is None:
                os.environ.pop("REGION_TALK_BGE_E5_ONLY", None)
            else:
                os.environ["REGION_TALK_BGE_E5_ONLY"] = old
        self.assertEqual([row["post_id"] for row in rows], ["p2", "p1"])
        self.assertTrue(all(row.get("_enrichment_pk", "").startswith("text_vector_enrichment_item:") for row in rows))
        self.assertEqual(len(rows), 2)

    def test_collect_text_rows_uses_e5_text_vector_items_as_bge_input(self) -> None:
        mod = load_bge_module()
        text = "Личный отзыв о поездке в Калининградскую область: море, дюны и Куршская коса."
        sha = mod.text_hash(text)
        e5_row = {
            "post_id": "p-e5",
            "post_url": "https://t.me/e5/1",
            "model_id": "intfloat/multilingual-e5-base",
            "model_short": "e5",
            "text_hash": sha,
            "text_excerpt": text,
        }
        rows = mod.collect_text_rows({"text_vector_enrichment_item": {"text_vector_enrichment_item:p-e5:e5": e5_row}}, existing_pks=set(), limit=5)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["_source_kind"], "text_vector_enrichment_item")
        self.assertEqual(rows[0]["_embedding_text_hash"], sha)
        self.assertEqual(rows[0]["_paired_e5_text_hash"], sha)
        self.assertIn(":bge_m3:", rows[0]["_enrichment_pk"])

    def test_collect_text_rows_skips_bge_vector_rows_by_default(self) -> None:
        mod = load_bge_module()
        text = "BGE result should not be re-embedded as BGE input."
        sha = mod.text_hash(text)
        bge_row = {
            "post_id": "p-bge",
            "post_url": "https://t.me/bge/1",
            "model_id": "BAAI/bge-m3",
            "model_short": "bge_m3",
            "text_hash": sha,
            "text_excerpt": text,
        }
        rows = mod.collect_text_rows({"text_vector_enrichment_item": {"text_vector_enrichment_item:p-bge:bge": bge_row}}, existing_pks=set(), limit=5)
        self.assertEqual(rows, [])

    def test_collect_text_rows_reserves_priority_and_fifo_capacity(self) -> None:
        mod = load_bge_module()
        items = {"text_vector_enrichment_item": {}}
        for index in range(10):
            text = f"Пост {index} о поездке в Калининградскую область и личных впечатлениях."
            row = {
                "post_id": f"p{index}",
                "post_url": f"https://t.me/src/{index}",
                "model_id": "intfloat/multilingual-e5-base",
                "model_short": "e5",
                "text_hash": mod.text_hash(text),
                "text_excerpt": text,
                "post_date": f"2026-07-{index + 1:02d}T00:00:00+00:00",
            }
            if index < 8:
                row.update({
                    "discovery_method": "exact_post_link_queue",
                    "post_link_priority": 0,
                    "priority_reason": "global_keyword_search_exact_post",
                })
            items["text_vector_enrichment_item"][f"e5:{index}"] = row
        with mock.patch.dict(os.environ, {"REGION_TALK_BGE_PRIORITY_SHARE_PERCENT": "80"}):
            rows = mod.collect_text_rows(items, existing_pks=set(), limit=5)
        self.assertEqual(len(rows), 5)
        # Four product-priority rows (fresh-first) plus one oldest FIFO row.
        self.assertEqual([row["post_id"] for row in rows[:4]], ["p7", "p6", "p5", "p4"])
        self.assertEqual(rows[4]["post_id"], "p8")

    def test_build_enrichment_payload_contains_geo_and_antivector_fields(self) -> None:
        mod = load_bge_module()
        row = {
            "post_id": "p1",
            "post_url": "https://t.me/a/1",
            "source_title": "source",
            "_embedding_text": "Калининград, Куршская коса и море — личный отзыв.",
            "_embedding_text_hash": mod.text_hash("Калининград, Куршская коса и море — личный отзыв."),
            "_embedding_text_fields": ["short_summary"],
        }
        payload = mod.build_enrichment_payload(
            row,
            {"ko_visit_impression": 0.72, "ad_or_promo": 0.21, "other_region_travel": 0.18},
            {"ko_geo:Куршская коса": 0.81, "external_ru_geo:Сочи": 0.17},
            [0.1, 0.2, 0.3],
            run_id="unit",
            semantic_bank_version="semantic_bank_v1",
            semantic_bank_hash="abcdef0123456789",
            geo_bank_version="geo_discriminator_bank_v1",
            geo_bank_hash="1234567890abcdef",
            embedding_dim=3,
            row_index=1,
        )
        self.assertEqual(payload["model_id"], "BAAI/bge-m3")
        self.assertEqual(payload["encoder_contract"], "bge_m3_flagembedding_dense_v1")
        self.assertEqual(payload["bge_m3_positive_class"], "ko_visit_impression")
        self.assertEqual(payload["bge_m3_ko_geo_top"], "Куршская коса")
        self.assertEqual(payload["bge_m3_external_geo_top"], "Сочи")
        self.assertIn("embedding_vector", payload)


if __name__ == "__main__":
    unittest.main()
