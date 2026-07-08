from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "region_talk_embedding_quality_compare.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_embedding_quality_compare", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkEmbeddingQualityCompareTests(unittest.TestCase):
    def test_compare_rows_reports_agreement_and_delta(self) -> None:
        mod = load_module()
        bge = {
            "url:https://t.me/a/1": {
                "post_url": "https://t.me/a/1",
                "bge_m3_positive_score": 0.8,
                "bge_m3_negative_score": 0.2,
                "bge_m3_margin_positive_vs_negative": 0.6,
                "bge_m3_ko_vs_external_geo_margin": 0.3,
                "bge_m3_top_class": "ko_visit_impression",
                "vector_gate_status_bge_m3": "bge_m3_accept_candidate",
            }
        }
        qwen = {
            "url:https://t.me/a/1": {
                "post_url": "https://t.me/a/1",
                "qwen3_embedding_0_6b_positive_score": 0.7,
                "qwen3_embedding_0_6b_negative_score": 0.3,
                "qwen3_embedding_0_6b_margin_positive_vs_negative": 0.4,
                "qwen3_embedding_0_6b_ko_vs_external_geo_margin": 0.1,
                "qwen3_embedding_0_6b_top_class": "ko_visit_impression",
                "vector_gate_status_qwen3_embedding_0_6b": "qwen3_embedding_0_6b_accept_candidate",
            }
        }
        labels = {"url:https://t.me/a/1": {"label": "positive_final", "label_source_kind": "publication_candidate_item", "label_status": "sent_to_chat", "confidence": 100}}
        pairs, summary = mod.compare_rows(bge, qwen, labels)
        self.assertEqual(len(pairs), 1)
        self.assertTrue(pairs[0]["gate_agree"])
        self.assertEqual(summary["positive_pairs"], 1)
        self.assertLess(pairs[0]["quality_axis_delta_qwen_minus_bge"], 0)

    def test_label_index_prefers_final_publication_label(self) -> None:
        mod = load_module()
        labels = mod.build_label_index({
            "candidate_memory_item": {"a": {"post_url": "https://t.me/a/1"}},
            "publication_candidate_item": {"b": {"post_url": "https://t.me/a/1", "publication_candidate_status": "sent_to_chat"}},
        })
        self.assertEqual(labels["url:https://t.me/a/1"]["label"], "positive_final")


if __name__ == "__main__":
    unittest.main()
