from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_publication_draft_backfill.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_publication_draft_backfill", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_backfill_source_never_mentions_local_e2e_sessions() -> None:
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "TELEGRAM_AUTH_BUNDLE_E2E" not in source
    assert "TELEGRAM_SESSION" not in source


def test_telegram_post_ref_is_exact_and_public() -> None:
    mod = load_module()
    assert mod.telegram_post_ref("https://t.me/s/TravelCase/123?single=1") == ("travelcase", 123)
    assert mod.telegram_post_ref("https://t.me/travelcase") is None
    assert mod.telegram_post_ref("https://vk.com/wall-1_2") is None
    assert mod.social_post_surface("https://vk.com/wall-1_2") == "vk"


def test_selection_skips_ready_terminal_and_future_retry_rows() -> None:
    mod = load_module()
    now = datetime(2026, 7, 31, 20, 0, tzinfo=timezone.utc)
    rows = [
        {"post_url": "https://t.me/source/1", "publication_rank": 2},
        {
            "post_url": "https://t.me/source/2",
            "publication_rank": 1,
            "publication_draft_backfill_status": "llm_not_accepted",
        },
        {
            "post_url": "https://t.me/source/3",
            "publication_rank": 3,
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_next_attempt_after": (now + timedelta(hours=1)).isoformat(),
        },
    ]
    with (
        mock.patch.object(mod.notify, "is_confirmed_publication", return_value=True),
        mock.patch.object(mod.notify, "is_publication_draft_ready", return_value=False),
    ):
        selected = mod.select_rows(rows, limit=10, now=now, surface="telegram")
    assert [row["post_url"] for row in selected] == ["https://t.me/source/1"]


def test_request_fingerprint_changes_with_exact_source_text() -> None:
    mod = load_module()
    row = {"post_url": "https://t.me/source/1", "llm_decision": "accept"}
    first = mod.draft_request_fingerprint(
        row,
        "Первый исходный текст",
        model="gemini-3.1-flash-lite",
    )
    second = mod.draft_request_fingerprint(
        row,
        "Другой исходный текст",
        model="gemini-3.1-flash-lite",
    )
    assert first != second


def test_vk_fetch_requires_the_exact_wall_identity() -> None:
    mod = load_module()
    row = {"post_url": "https://vk.com/wall-10_20"}
    text, fields = mod.fetch_vk_text(
        row,
        {"-10_20": {"owner_id": -10, "id": 20, "text": "Исходный текст VK", "date": 1785530000}},
        "",
    )
    assert text == "Исходный текст VK"
    assert fields["platform"] == "vk"
    try:
        mod.fetch_vk_text(
            row,
            {"-10_20": {"owner_id": -10, "id": 21, "text": "Другой пост"}},
            "",
        )
    except RuntimeError as exc:
        assert "different post" in str(exc)
    else:
        raise AssertionError("mismatched VK post must fail closed")
