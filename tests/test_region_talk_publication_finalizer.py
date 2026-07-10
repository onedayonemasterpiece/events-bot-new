from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_publication_finalizer.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_publication_finalizer", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkPublicationFinalizerTests(unittest.TestCase):
    def test_source_class_guess_uses_region_talk_local_source_filter(self) -> None:
        mod = load_module()
        self.assertEqual(
            mod.source_class_guess("Дом китобоя", "https://t.me/domkitoboya", {}),
            "local_region_source",
        )
        self.assertEqual(
            mod.source_class_guess("Travel notes", "https://t.me/example_travel", {}),
            "nonlocal_travel_or_general_source",
        )


if __name__ == "__main__":
    unittest.main()
