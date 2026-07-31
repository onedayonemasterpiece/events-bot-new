from __future__ import annotations

import unittest

from scripts import region_talk_post_row_normalize as mod


class RegionTalkPostRowNormalizeTests(unittest.TestCase):
    def test_plan_merges_fetch_path_duplicates_under_platform_key(self) -> None:
        rows = [
            {
                "_ydb_pk": "processed_post_item:post_hash_a",
                "_ydb_updated_at": "2026-07-10T00:00:00Z",
                "post_id": "post_hash_a",
                "platform_post_key": "tg:Example:42",
                "post_url": "https://t.me/example/42",
                "source_title": "Example",
            },
            {
                "_ydb_pk": "processed_post_item:post_hash_b",
                "_ydb_updated_at": "2026-07-11T00:00:00Z",
                "post_id": "post_hash_b",
                "platform_post_key": "tg:example:42",
                "post_url": "https://t.me/example/42",
                "current_stage": "semantic_candidate",
            },
        ]
        plan = mod.normalize_plan(rows)
        self.assertEqual(plan["migration_groups_selected"], 1)
        self.assertEqual(plan["duplicate_groups_selected"], 1)
        operation = plan["operations"][0]
        self.assertEqual(operation["canonical_pk"], "processed_post_item:tg:example:42")
        self.assertEqual(len(operation["delete_pks"]), 2)
        self.assertEqual(operation["merged_payload"]["source_title"], "Example")
        self.assertEqual(operation["merged_payload"]["current_stage"], "semantic_candidate")
        self.assertNotIn("_ydb_pk", operation["merged_payload"])

    def test_plan_is_idempotent_for_canonical_single_row(self) -> None:
        plan = mod.normalize_plan([{
            "_ydb_pk": "processed_post_item:tg:example:42",
            "platform_post_key": "tg:example:42",
            "post_url": "https://t.me/example/42",
        }])
        self.assertEqual(plan["duplicate_groups_selected"], 0)
        self.assertEqual(plan["migration_groups_selected"], 0)

    def test_plan_migrates_legacy_singleton_before_it_can_duplicate(self) -> None:
        plan = mod.normalize_plan([{
            "_ydb_pk": "processed_post_item:post_old_fetch_hash",
            "post_id": "post_old_fetch_hash",
            "platform_post_key": "tg:example:43",
            "post_url": "https://t.me/example/43",
        }])
        self.assertEqual(plan["migration_groups_selected"], 1)
        self.assertEqual(plan["duplicate_groups_selected"], 0)
        self.assertEqual(plan["legacy_singleton_groups_selected"], 1)
        self.assertEqual(plan["operations"][0]["canonical_pk"], "processed_post_item:tg:example:43")
        self.assertEqual(plan["operations"][0]["delete_pks"], ["processed_post_item:post_old_fetch_hash"])


if __name__ == "__main__":
    unittest.main()
