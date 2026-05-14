from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from video_announce.poller import _extract_partner_story_metadata


def test_extract_partner_story_finds_business_target_with_story_id():
    report = {
        "ok": True,
        "targets": [
            {
                "ok": True,
                "transport": "telethon",
                "story_id": 42,
                "peer": "@kenigevents",
            },
            {
                "ok": True,
                "transport": "telegram_business",
                "connection_hash": "abcd1234",
                "story_id": 9001,
            },
        ],
    }
    story_id, conn_hash = _extract_partner_story_metadata(report)
    assert story_id == "9001"
    assert conn_hash == "abcd1234"


def test_extract_partner_story_returns_none_when_business_failed():
    report = {
        "targets": [
            {
                "ok": False,
                "transport": "telegram_business",
                "connection_hash": "abcd1234",
                "story_id": 9001,
            }
        ]
    }
    assert _extract_partner_story_metadata(report) == (None, None)


def test_extract_partner_story_returns_none_when_no_business_target():
    report = {
        "targets": [
            {
                "ok": True,
                "transport": "telethon",
                "story_id": 42,
                "peer": "@kenigevents",
            }
        ]
    }
    assert _extract_partner_story_metadata(report) == (None, None)


def test_extract_partner_story_uses_business_connection_hash_field():
    report = {
        "targets": [
            {
                "ok": True,
                "transport": "telegram_business",
                "business_connection_hash": "hash_xyz",
                "story_id": 7,
            }
        ]
    }
    story_id, conn_hash = _extract_partner_story_metadata(report)
    assert story_id == "7"
    assert conn_hash == "hash_xyz"


def test_extract_partner_story_empty_or_invalid_report():
    assert _extract_partner_story_metadata(None) == (None, None)
    assert _extract_partner_story_metadata({}) == (None, None)
    assert _extract_partner_story_metadata({"targets": []}) == (None, None)
