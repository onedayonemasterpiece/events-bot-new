from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_vk_media_prefetch.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_vk_media_prefetch", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkVkMediaPrefetchTests(unittest.TestCase):
    def test_parse_vk_post_and_choose_largest_photo(self) -> None:
        mod = load_module()
        self.assertEqual(mod.parse_vk_post("https://vk.com/wall-211445468_273"), (-211445468, 273))
        self.assertIsNone(mod.parse_vk_post("https://t.me/example/1"))
        urls = mod.photo_urls_from_post({
            "attachments": [{
                "type": "photo",
                "photo": {"sizes": [
                    {"url": "https://cdn.test/s.jpg", "width": 320, "height": 200},
                    {"url": "https://cdn.test/l.jpg", "width": 1080, "height": 1410},
                ]},
            }, {"type": "video"}],
        })
        self.assertEqual(urls, ["https://cdn.test/l.jpg"])


if __name__ == "__main__":
    unittest.main()
