import importlib.util
import json
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "docs/reference/data/artist_registry_batch_001.canonical.json"
CONVERTER = ROOT / "scripts/convert_artist_registry_xlsx.py"

spec = importlib.util.spec_from_file_location("artist_registry_converter", CONVERTER)
assert spec and spec.loader
converter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(converter)


class ArtistRegistryConverterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))

    def test_canonical_snapshot_is_fail_closed_for_locality(self) -> None:
        self.assertFalse(
            self.snapshot["safety_contract"]["list_membership_proves_non_locality"]
        )
        self.assertFalse(self.snapshot["safety_contract"]["absence_proves_non_locality"])
        self.assertTrue(
            self.snapshot["safety_contract"]["locality_requires_row_level_evidence"]
        )
        self.assertTrue(
            all(entity["locality"]["status"] == "unknown" for entity in self.snapshot["entities"])
        )

    def test_profile_matches_entities(self) -> None:
        entities = self.snapshot["entities"]
        profile = self.snapshot["profile"]
        self.assertEqual(profile["entity_count"], 1235)
        self.assertEqual(len(entities), profile["entity_count"])
        self.assertEqual(
            len({entity["registry_id"] for entity in entities}),
            profile["unique_registry_id_count"],
        )
        match_key_counts = Counter(entity["matching"]["match_key"] for entity in entities)
        self.assertEqual(len(match_key_counts), profile["unique_match_key_count"])
        self.assertEqual(
            sum(count > 1 for count in match_key_counts.values()),
            profile["duplicate_match_key_group_count"],
        )
        self.assertEqual(profile["row_level_wikidata_qid_count"], 0)
        self.assertEqual(profile["active_confirmed_count"], 0)
        self.assertEqual(profile["last_verified_at_count"], 0)

    def test_duplicate_match_keys_are_flagged(self) -> None:
        entities = self.snapshot["entities"]
        match_key_counts = Counter(entity["matching"]["match_key"] for entity in entities)
        for entity in entities:
            is_duplicate = match_key_counts[entity["matching"]["match_key"]] > 1
            self.assertEqual(entity["matching"]["duplicate_match_key"], is_duplicate)
            if is_duplicate:
                self.assertIn(
                    "duplicate_match_key_review",
                    entity["matching"]["ambiguity_flags"],
                )

    def test_standard_library_helpers(self) -> None:
        self.assertEqual(converter._column_index("A1"), 0)
        self.assertEqual(converter._column_index("AA17"), 26)
        self.assertEqual(converter._split_semicolon("Баста; Basta; "), ["Баста", "Basta"])
        self.assertIsNone(converter._nullable_bool(""))
        self.assertTrue(converter._nullable_bool("да"))
        self.assertFalse(converter._nullable_bool("0"))


if __name__ == "__main__":
    unittest.main()
