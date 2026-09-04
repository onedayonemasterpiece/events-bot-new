from __future__ import annotations

import base64
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "telegram_read.py"
SPEC = importlib.util.spec_from_file_location("telegram_read", MODULE_PATH)
assert SPEC and SPEC.loader
telegram_read = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = telegram_read
SPEC.loader.exec_module(telegram_read)


class AuthBundleTests(unittest.TestCase):
    def test_raw_json_bundle(self) -> None:
        raw = json.dumps({"api_id": 123, "api_hash": "hash", "session": "session"})
        with patch.dict(os.environ, {"ROLE_BUNDLE": raw}, clear=True):
            config = telegram_read.load_auth_config("ROLE_BUNDLE")
        self.assertEqual(config.api_id, 123)
        self.assertEqual(config.api_hash, "hash")
        self.assertEqual(config.session, "session")

    def test_urlsafe_base64_bundle_and_device_metadata(self) -> None:
        payload = {
            "api_id": "456",
            "api_hash": "hash2",
            "session_string": "session2",
            "device_model": "CI reader",
            "lang_code": "ru",
        }
        encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        with patch.dict(os.environ, {"ROLE_BUNDLE": encoded}, clear=True):
            config = telegram_read.load_auth_config("ROLE_BUNDLE")
        self.assertEqual(config.api_id, 456)
        self.assertEqual(config.device_model, "CI reader")
        self.assertEqual(config.lang_code, "ru")

    def test_bundle_does_not_fall_back_to_other_session_roles(self) -> None:
        raw = json.dumps({"api_id": 123, "api_hash": "hash"})
        with patch.dict(
            os.environ,
            {"ROLE_BUNDLE": raw, "TELEGRAM_SESSION": "must-not-be-used"},
            clear=True,
        ):
            with self.assertRaises(telegram_read.TelegramReadError):
                telegram_read.load_auth_config("ROLE_BUNDLE")


class RequestTests(unittest.TestCase):
    def test_latest_request(self) -> None:
        request = telegram_read.parse_request(
            {"mode": "latest", "targets": ["https://t.me/lovekenig"], "limit": 2}
        )
        self.assertEqual(request.mode, "latest")
        self.assertEqual(request.limit, 2)

    def test_total_message_cap(self) -> None:
        with self.assertRaises(telegram_read.TelegramReadError):
            telegram_read.parse_request(
                {
                    "mode": "latest",
                    "targets": [f"channel{i:02d}" for i in range(10)],
                    "limit": 11,
                }
            )


class TargetParsingTests(unittest.TestCase):
    def test_normalizes_public_channel(self) -> None:
        self.assertEqual(
            telegram_read.normalize_latest_target("https://t.me/lovekenig"),
            "lovekenig",
        )
        self.assertEqual(telegram_read.normalize_latest_target("@lovekenig"), "lovekenig")

    def test_rejects_post_link_in_latest_mode(self) -> None:
        with self.assertRaises(telegram_read.TelegramReadError):
            telegram_read.normalize_latest_target("https://t.me/lovekenig/123")

    def test_parses_public_exact_message_link(self) -> None:
        peer, message_id, canonical = telegram_read.parse_exact_message_link(
            "https://t.me/lovekenig/123"
        )
        self.assertEqual(peer, "lovekenig")
        self.assertEqual(message_id, 123)
        self.assertEqual(canonical, "https://t.me/lovekenig/123")

    def test_parses_private_exact_message_link(self) -> None:
        peer, message_id, canonical = telegram_read.parse_exact_message_link(
            "https://t.me/c/1234567890/42"
        )
        self.assertEqual(peer, -1001234567890)
        self.assertEqual(message_id, 42)
        self.assertEqual(canonical, "https://t.me/c/1234567890/42")


class PacingTests(unittest.TestCase):
    def test_repository_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            pacer = telegram_read.HumanPacer()
        self.assertEqual(pacer.startup_range, (4.0, 12.0))
        self.assertEqual(pacer.request_range, (2.0, 5.0))
        self.assertEqual(pacer.target_range, (5.0, 15.0))

    def test_range_is_normalized(self) -> None:
        with patch.dict(os.environ, {"RANGE": "5,2"}, clear=True):
            self.assertEqual(telegram_read.parse_seconds_range("RANGE", (0, 0)), (2.0, 5.0))


if __name__ == "__main__":
    unittest.main()
