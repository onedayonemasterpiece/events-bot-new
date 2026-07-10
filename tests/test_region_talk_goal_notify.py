from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_goal_notify.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_goal_notify", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkGoalNotifyTests(unittest.TestCase):
    def test_only_current_eligibility_attested_confirmed_rows_are_sendable(self) -> None:
        mod = load_module()
        base = {
            "publication_candidate_status": "llm_confirmed",
            "publication_status": "gemini_accept",
            "sent_to_chat": "false",
        }
        signed = {
            **base,
            "publication_eligibility_verdict": "eligible",
            "publication_eligibility_gate_version": mod.PUBLICATION_ELIGIBILITY_GATE_VERSION,
            "authoritative_source_fingerprint_version": mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "authoritative_source_fingerprint": "current-source-fingerprint",
            "_live_authoritative_source_fingerprint": "current-source-fingerprint",
        }
        self.assertTrue(mod.is_confirmed_publication(signed))
        self.assertTrue(mod.is_unsent_confirmed_publication(signed))
        self.assertFalse(mod.is_confirmed_publication(base))
        self.assertFalse(mod.is_confirmed_publication({**signed, "publication_eligibility_verdict": "review"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "publication_revoked": "true"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "_live_authoritative_source_fingerprint": "source-became-local"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "_live_authoritative_source_fingerprint": ""}))
        self.assertFalse(mod.is_unsent_confirmed_publication({**signed, "sent_to_chat": "true"}))

    def test_source_fingerprint_changes_when_source_classification_changes(self) -> None:
        mod = load_module()
        external = {
            "canonical_source_key": "telegram:travelcase",
            "source_queue_status": "processed_found_ko_candidate",
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "posts_scanned": 1,
            "ko_posts_found": 1,
            "candidate_posts_found": 1,
            "queue_item_updated_at": "2026-07-10T00:00:00+00:00",
        }
        local = {
            **external,
            "source_queue_status": "rejected_local_region_source",
            "source_scope": "local_region",
            "source_geo_class": "kaliningrad_local",
            "queue_item_updated_at": "2026-07-10T01:00:00+00:00",
        }
        self.assertNotEqual(
            mod.authoritative_source_fingerprint(external),
            mod.authoritative_source_fingerprint(local),
        )
        self.assertEqual(
            mod.authoritative_source_fingerprint(external),
            mod.authoritative_source_fingerprint({**external, "queue_item_updated_at": "2026-07-11T00:00:00+00:00"}),
        )
        self.assertNotEqual(
            mod.authoritative_source_fingerprint(external),
            mod.authoritative_source_fingerprint({**external, "posts_scanned": 9, "ko_posts_found": 2}),
        )

    def test_delivery_identity_is_stable_per_canonical_post_and_chat(self) -> None:
        mod = load_module()
        first = {"post_url": "https://telegram.me/TravelCase/10?single=1"}
        second = {"post_url": "https://t.me/travelcase/10/"}
        key1 = mod.publication_delivery_key(first, "-100123")
        key2 = mod.publication_delivery_key(second, "-100123")
        self.assertEqual(key1, key2)
        self.assertEqual(mod.delivery_random_id(key1), mod.delivery_random_id(key1))
        self.assertNotEqual(key1, mod.publication_delivery_key(second, "-100999"))


if __name__ == "__main__":
    unittest.main()
