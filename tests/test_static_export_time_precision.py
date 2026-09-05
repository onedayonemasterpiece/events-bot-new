from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("time_precision_preview_exporter", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


class StaticExportTimePrecisionTest(unittest.TestCase):
    def test_default_source_time_never_becomes_a_public_exact_time(self) -> None:
        self.assertEqual(
            EXPORTER.public_event_time_projection("19:09", time_is_default=True),
            (None, None, None),
        )

    def test_confirmed_source_time_retains_exact_public_projection(self) -> None:
        self.assertEqual(
            EXPORTER.public_event_time_projection("19:09–21:00", time_is_default=False),
            ("19:09", "21:00", "19:09–21:00"),
        )

    def test_default_time_flag_is_explicit_and_old_snapshots_keep_existing_precision(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"time_is_default": time_is_default', source)
        self.assertIs(EXPORTER.source_time_is_default({"time_is_default": 1}), True)
        self.assertIs(EXPORTER.source_time_is_default({"time_is_default": 0}), False)
        self.assertIs(EXPORTER.source_time_is_default({}), False)


if __name__ == "__main__":
    unittest.main()
