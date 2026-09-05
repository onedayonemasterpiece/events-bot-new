from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("occurrence_relation_preview_exporter", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


def event(event_id: int, event_type: str, start: str, end: str, links: list[int]) -> dict:
    return {
        "id": event_id,
        "event_type": event_type,
        "start_date": start,
        "end_date": end,
        "starts_at": f"{start}T19:00:00+02:00",
        "related_event_ids": links.copy(),
        "other_date_ids": links.copy(),
    }


class StaticExportOccurrenceRelationsTest(unittest.TestCase):
    def test_exhibition_tour_relation_stays_related_but_never_becomes_another_date(self) -> None:
        exhibition = event(1, "выставка", "2026-09-01", "2026-09-30", [2, 5])
        tour = event(2, "экскурсия", "2026-09-12", "2026-09-12", [1])
        repeat_a = event(3, "концерт", "2026-09-13", "2026-09-13", [4])
        repeat_b = event(4, "концерт", "2026-09-14", "2026-09-14", [3])
        # id 5 is past/ineligible and intentionally absent from this catalog.
        EXPORTER.normalize_linked_occurrences([exhibition, tour, repeat_a, repeat_b])

        self.assertEqual(exhibition["related_event_ids"], [2, 5])
        self.assertEqual(tour["related_event_ids"], [1])
        self.assertEqual(exhibition["other_date_ids"], [])
        self.assertEqual(tour["other_date_ids"], [])
        self.assertEqual(repeat_a["other_date_ids"], [4])
        self.assertEqual(repeat_b["other_date_ids"], [3])
        self.assertEqual(exhibition["starts_at"], "2026-09-01T19:00:00+02:00")

    def test_same_type_is_not_invented_as_a_series_without_an_explicit_mutual_link(self) -> None:
        first = event(6, "концерт", "2026-09-13", "2026-09-13", [7])
        second = event(7, "концерт", "2026-09-14", "2026-09-14", [])
        EXPORTER.normalize_linked_occurrences([first, second])
        self.assertEqual(first["other_date_ids"], [])
        self.assertEqual(second["other_date_ids"], [])
        self.assertEqual(first["related_event_ids"], [7])


if __name__ == "__main__":
    unittest.main()
