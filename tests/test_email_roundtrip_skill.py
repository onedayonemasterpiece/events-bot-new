from __future__ import annotations

import importlib.util
import unittest
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".codex/skills/kenigevents-email-roundtrip/scripts/yandex_mail_trigger.py"
SPEC = importlib.util.spec_from_file_location("yandex_mail_trigger", SCRIPT)
assert SPEC and SPEC.loader
mail = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mail)


def envelope(body: str = "Ваш код: 123456") -> dict:
    return {
        "received_at": "2026-08-02T10:00:01Z",
        "headers": [
            {"name": "From", "values": ["KenigEvents <auth@example.test>"]},
            {"name": "To", "values": ["trigger@serverless.yandexcloud.net"]},
            {"name": "Subject", "values": ["Код для входа"]},
            {"name": "Message-ID", "values": ["<fixture@example.test>"]},
        ],
        "trigger_body": {"value": body},
    }


class EmailRoundtripSkillTests(unittest.TestCase):
    def test_extracts_one_six_digit_otp(self) -> None:
        self.assertEqual(mail.extract_otp(envelope()), "123456")

    def test_rejects_zero_or_multiple_codes(self) -> None:
        with self.assertRaisesRegex(mail.RoundtripError, "otp_not_found"):
            mail.extract_otp(envelope("No code"))
        with self.assertRaisesRegex(mail.RoundtripError, "otp_ambiguous"):
            mail.extract_otp(envelope("123456 then 654321"))

    def test_matches_checkpoint_sender_subject_and_recipient(self) -> None:
        self.assertTrue(mail.envelope_matches(
            envelope(),
            since=datetime(2026, 8, 2, 10, 0, tzinfo=UTC),
            from_pattern=mail.re.compile("kenigevents", mail.re.I),
            subject_pattern=mail.re.compile("код", mail.re.I),
            recipient="trigger@serverless.yandexcloud.net",
        ))
        self.assertFalse(mail.envelope_matches(
            envelope(),
            since=datetime(2026, 8, 2, 10, 1, tzinfo=UTC),
            from_pattern=mail.re.compile("kenigevents", mail.re.I),
            subject_pattern=mail.re.compile("код", mail.re.I),
            recipient=None,
        ))

    def test_date_prefixes_cross_midnight(self) -> None:
        self.assertEqual(mail.date_prefixes(
            datetime(2026, 8, 1, 23, 59, tzinfo=UTC),
            datetime(2026, 8, 2, 0, 1, tzinfo=UTC),
        ), ["messages/2026/08/01/", "messages/2026/08/02/"])

    def test_trigger_and_private_bucket_discovery_fail_closed(self) -> None:
        trigger = {
            "name": mail.DEFAULT_TRIGGER_NAME,
            "status": "ACTIVE",
            "rule": {"mail": {"email": "fixture@serverless.yandexcloud.net"}},
        }
        self.assertEqual(mail.mail_trigger([trigger], mail.DEFAULT_TRIGGER_NAME), trigger)
        bucket = {
            "name": f"{mail.DEFAULT_BUCKET_PREFIX}fixture",
            "anonymous_access_flags": {"read": False, "list": False, "config_read": False},
        }
        self.assertEqual(mail.private_bucket([bucket], mail.DEFAULT_BUCKET_PREFIX), bucket)
        bucket["anonymous_access_flags"]["read"] = True
        with self.assertRaisesRegex(mail.RoundtripError, "bucket_not_private"):
            mail.private_bucket([bucket], mail.DEFAULT_BUCKET_PREFIX)


if __name__ == "__main__":
    unittest.main()
