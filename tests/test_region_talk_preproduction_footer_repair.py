from __future__ import annotations

from types import SimpleNamespace

from scripts import region_talk_goal_notify as notify
from scripts import region_talk_preproduction_footer_repair as repair


def _row() -> dict[str, object]:
    row: dict[str, object] = {
        "_ydb_pk": "publication_candidate_item:https://t.me/a/1",
        "post_url": "https://t.me/a/1",
        "sent_message_id": "42",
        "publication_draft_telegram_text": (
            "Первый смысловой абзац.\n\nВторой смысловой абзац.\n\n"
            "Источник: Канал\nОригинал: https://t.me/a/1"
        ),
        "publication_draft_vk_text": (
            "Первый смысловой абзац.\n\nВторой смысловой абзац.\n\n"
            "Источник: Канал\nОригинал: https://t.me/a/1"
        ),
        "operator_review_decision": "pending",
        "operator_review_rewrite_status": "clean",
        "operator_review_positive": False,
        "operator_review_negative": False,
        "operator_review_rewrite_requested": False,
    }
    fingerprint = notify.publication_operator_review_fingerprint(row)
    row["operator_review_fingerprint"] = fingerprint
    row["sent_operator_review_fingerprint"] = fingerprint
    return row


def test_pending_selector_requires_exact_current_undecided_revision(monkeypatch) -> None:
    monkeypatch.setattr(repair.notify, "is_confirmed_publication", lambda row: True)
    monkeypatch.setattr(repair.notify, "is_publication_draft_ready", lambda row: True)
    row = _row()
    assert repair.pending_current_review(row) is True
    assert repair.pending_current_review({**row, "operator_review_decision": "approved"}) is False
    assert repair.pending_current_review({**row, "operator_review_rewrite_requested": True}) is False
    assert repair.pending_current_review({**row, "sent_operator_review_fingerprint": "old"}) is False


def test_repaired_candidate_rotates_review_identity_and_removes_duplicate_source() -> None:
    row = _row()
    old_fingerprint = str(row["operator_review_fingerprint"])
    updated = repair.repaired_candidate(row, chat_id="-100")

    draft = str(updated["publication_draft_telegram_text"])
    assert draft.count("https://t.me/a/1") == 1
    assert "Источник: Канал" not in draft
    assert "Оригинал" not in draft
    assert "\n\nО Калининграде говорят: https://t.me/kalinigrad_visit" in draft
    assert updated["operator_review_fingerprint"] != old_fingerprint
    assert updated["sent_operator_review_fingerprint"] == updated["operator_review_fingerprint"]
    assert updated["operator_review_decision"] == "pending"


def test_message_verifier_requires_exact_two_footer_links() -> None:
    updated = repair.repaired_candidate(_row(), chat_id="-100")
    p1, p2 = notify._draft_two_paragraphs(updated)
    TextUrl = type("MessageEntityTextUrl", (), {})
    original_entity = TextUrl()
    original_entity.url = "https://t.me/a/1"
    channel_entity = TextUrl()
    channel_entity.url = "https://t.me/kalinigrad_visit"
    message = SimpleNamespace(
        message=notify.public_caption_visible_text(p1, p2),
        entities=[original_entity, channel_entity],
    )
    assert repair.message_matches(message, updated) is True
    message.entities.append(type("MessageEntityTextUrl", (), {"url": "https://example.org"})())
    assert repair.message_matches(message, updated) is False
