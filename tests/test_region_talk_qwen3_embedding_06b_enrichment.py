from __future__ import annotations

import argparse
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QWEN_MODULE_PATH = ROOT / "kaggle" / "RegionTalkQwen3Embedding06BEnrichment" / "region_talk_qwen3_embedding_06b_enrichment.py"
QWEN_RUNNER_PATH = ROOT / "kaggle" / "execute_region_talk_qwen3_embedding_06b_enrichment.py"


def load_qwen_module():
    spec = importlib.util.spec_from_file_location("region_talk_qwen3_embedding_06b_enrichment", QWEN_MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_qwen_runner_module():
    spec = importlib.util.spec_from_file_location("execute_region_talk_qwen3_embedding_06b_enrichment", QWEN_RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkQwen3Embedding06BEnrichmentTests(unittest.TestCase):
    def test_secret_names_do_not_package_telegram_sessions(self) -> None:
        mod = load_qwen_runner_module()
        names = mod.qwen3_secret_names()
        self.assertIn("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY1", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY2", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_E2E", names)
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_S22", names)
        self.assertNotIn("TELEGRAM_SESSION", names)

    def test_embeddinggemma_runner_spec_is_research_only_cpu_candidate(self) -> None:
        mod = load_qwen_runner_module()
        spec = mod.model_spec(argparse.Namespace(model_size="embeddinggemma"))
        self.assertEqual(spec["model_id"], "google/embeddinggemma-300m")
        self.assertEqual(spec["model_short"], "embeddinggemma_300m")
        self.assertEqual(spec["encoder_contract"], "embeddinggemma_300m_sentence_transformers_dense_768_v1")
        self.assertEqual(spec["kaggle_source"], "google/embeddinggemma/Transformers/embeddinggemma-300m/1")
        self.assertEqual(spec["kernel_slug"], "rt-embeddinggemma-300m-enrichment")
        self.assertLessEqual(len(spec["title"]), 50)

    def test_collect_text_rows_uses_research_kind_and_dedupes(self) -> None:
        mod = load_qwen_module()
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
        rows = mod.collect_text_rows(items, existing_pks=set(), limit=10)
        self.assertEqual([row["post_id"] for row in rows], ["p2", "p1"])
        self.assertTrue(all(row.get("_enrichment_pk", "").startswith("qwen3_embedding_0_6b_enrichment_item:") for row in rows))
        self.assertEqual(len(rows), 2)

    def test_build_enrichment_payload_contains_qwen_research_fields(self) -> None:
        mod = load_qwen_module()
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
        self.assertEqual(payload["model_id"], "Qwen/Qwen3-Embedding-0.6B")
        self.assertEqual(payload["encoder_contract"], "qwen3_embedding_0_6b_sentence_transformers_dense_1024_v1")
        self.assertIn("qwen3_embedding_0_6b_enrichment_id", payload)
        self.assertNotIn("text_vector_enrichment_id", payload)
        self.assertEqual(payload["qwen3_embedding_0_6b_positive_class"], "ko_visit_impression")
        self.assertEqual(payload["qwen3_embedding_0_6b_ko_geo_top"], "Куршская коса")
        self.assertEqual(payload["qwen3_embedding_0_6b_external_geo_top"], "Сочи")
        self.assertEqual(payload["vector_gate_status_qwen3_embedding_0_6b"], "qwen3_embedding_0_6b_accept_candidate")
        self.assertIn("embedding_vector", payload)


if __name__ == "__main__":
    unittest.main()
