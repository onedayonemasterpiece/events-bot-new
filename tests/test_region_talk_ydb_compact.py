from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "region_talk_ydb_compact.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_ydb_compact", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class RegionTalkYdbCompactTests(unittest.TestCase):
    def test_transform_drops_duplicates_research_and_slins_snapshot(self) -> None:
        mod = load_module()
        rows = [
            {"pk": "latest_state", "kind": "state_snapshot", "updated_at": "2026-07-11T10:00:00Z", "payload": {
                "run_id": "r1", "unified_source_queue": {"a": {"source": "large"}},
                "processed_posts": {"p": {"text": "large"}}, "all_time_metrics": {"posts": 1},
            }},
            {"pk": "processed_post_item:tg:a:1", "kind": "processed_post_item", "updated_at": "2026-07-11T10:00:00Z", "payload": {"post_url": "https://t.me/a/1"}},
            {"pk": "post_live_item:tg:a:1", "kind": "post_live_item", "updated_at": "2026-07-11T10:00:00Z", "payload": {"post_url": "https://t.me/a/1"}},
            {"pk": "qwen3_embedding_0_6b_enrichment_item:x", "kind": "qwen3_embedding_0_6b_enrichment_item", "updated_at": "2026-07-11T10:00:00Z", "payload": {"embedding_vector": [1.0]}},
        ]
        target, meta = mod.transform_rows(rows)
        self.assertEqual({row["kind"] for row in target}, {"state_snapshot", "processed_post_item"})
        checkpoint = next(row["payload"] for row in target if row["kind"] == "state_snapshot")
        self.assertEqual(checkpoint["state_schema_version"], "region-talk-ydb-checkpoint-v4")
        self.assertNotIn("processed_posts", checkpoint)
        self.assertEqual(meta["dropped_rows_by_reason"]["duplicate_post_live_item"], 1)
        self.assertEqual(meta["dropped_rows_by_reason"]["completed_embedding_research"], 1)

    def test_vector_is_float16_base64_and_keeps_scores(self) -> None:
        mod = load_module()
        payload, saved = mod.transform_vector({
            "model_id": "BAAI/bge-m3",
            "model_short": "bge_m3",
            "embedding_dim": 3,
            "embedding_vector": [0.1, -0.2, 0.3],
            "text_excerpt": "Калининград",
            "semantic_scores_by_class": {"ko_visit_impression": 0.8},
        })
        self.assertNotIn("embedding_vector", payload)
        self.assertNotIn("text_excerpt", payload)
        self.assertEqual(payload["embedding_vector_encoding"], "f16_le_base64")
        self.assertEqual(payload["semantic_scores_by_class"]["ko_visit_impression"], 0.8)
        self.assertGreater(saved, 0)

    def test_legacy_only_post_is_promoted_to_processed_projection(self) -> None:
        mod = load_module()
        rows = [
            {"pk": "latest_state", "kind": "state_snapshot", "updated_at": "2026-07-11T10:00:00Z", "payload": {"run_id": "r1"}},
            {"pk": "post_live_item:legacy", "kind": "post_live_item", "updated_at": "2026-07-11T10:00:00Z", "payload": {
                "post_url": "https://t.me/travel/42", "platform_post_key": "tg:travel:42", "text_excerpt": "rich",
            }},
            {"pk": "processed_post_item:tg:travel:43", "kind": "processed_post_item", "updated_at": "2026-07-11T10:00:00Z", "payload": {
                "post_url": "https://t.me/travel/43", "platform_post_key": "tg:travel:43",
            }},
        ]
        target, meta = mod.transform_rows(rows)
        posts = {row["pk"]: row for row in target if row["kind"] == "processed_post_item"}
        self.assertEqual(set(posts), {"processed_post_item:tg:travel:42", "processed_post_item:tg:travel:43"})
        self.assertNotIn("text_excerpt", posts["processed_post_item:tg:travel:42"]["payload"])
        self.assertEqual(meta["post_projection_source_rows"], 2)
        self.assertEqual(meta["post_projection_canonical_rows"], 2)
        self.assertTrue(mod.validate(rows, target)["ok"])

    def test_existing_target_replacement_requires_explicit_bootstrap_ack(self) -> None:
        mod = load_module()
        with self.assertRaises(RuntimeError):
            mod.validate_target_replacement(exists=True, replace=False, bootstrap_ack=False, table_path="/db/target")
        with self.assertRaises(RuntimeError):
            mod.validate_target_replacement(exists=True, replace=True, bootstrap_ack=False, table_path="/db/target")
        mod.validate_target_replacement(exists=True, replace=True, bootstrap_ack=True, table_path="/db/target")


if __name__ == "__main__":
    unittest.main()
