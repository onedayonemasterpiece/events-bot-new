from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from scripts import region_talk_goal_notify as notify
from scripts import region_talk_publication_plan as plan
from scripts import region_talk_reaction_sync as sync


def review_row() -> dict:
    return {
        "post_url": "https://t.me/external/7",
        "publication_draft_status": "ready_for_operator_review",
        "publication_draft_title": "Берег",
        "publication_draft_source_attribution": "Авторский канал",
        "publication_draft_telegram_text": "Первый абзац.\n\nВторой абзац.",
        "publication_draft_vk_text": "Первый абзац.\n\nВторой абзац.",
        "publication_draft_fact_points_json": '[{"claim":"Факт","support_excerpt":"Опора"}]',
        "publication_draft_prompt_version": "writer-v1",
        "media_manifest_items": [{"id": "photo-a"}, {"id": "photo-b"}],
        "publication_presentation_mode": "carousel",
    }


def test_emoji_aliases_and_truth_table() -> None:
    assert sync.normalize_reaction("❤") == "positive_heart"
    assert sync.normalize_reaction("❤️") == "positive_heart"
    assert sync.normalize_reaction("👍") == "positive_like"
    assert sync.normalize_reaction("👎") == "negative_dislike"
    assert sync.normalize_reaction("✍️") == "rewrite_pen"
    assert sync.classify_operator_reactions({"1": ["positive_like"]}) == {
        "operator_review_decision": "approved",
        "operator_review_rewrite_status": "clean",
        "operator_review_positive": True,
        "operator_review_negative": False,
        "operator_review_rewrite_requested": False,
    }
    assert sync.classify_operator_reactions({"1": ["rewrite_pen"]})["operator_review_decision"] == "pending"
    approved_rewrite = sync.classify_operator_reactions({"1": ["positive_heart", "rewrite_pen"]})
    assert approved_rewrite["operator_review_decision"] == "approved"
    assert approved_rewrite["operator_review_rewrite_status"] == "rewrite_requested"
    assert sync.classify_operator_reactions({"1": ["negative_dislike", "rewrite_pen"]})["operator_review_decision"] == "rejected"
    assert sync.classify_operator_reactions({"1": ["positive_like"], "2": ["negative_dislike"]})["operator_review_decision"] == "conflict"


def test_reviewer_allowlist_is_exact_and_required() -> None:
    assert sync.parse_reviewer_ids("123, 456") == {"123", "456"}
    with pytest.raises(RuntimeError):
        sync.parse_reviewer_ids("")
    with pytest.raises(RuntimeError):
        sync.parse_reviewer_ids("@editor")


def test_exact_pagination_ignores_nonbinding_reactors(monkeypatch) -> None:
    pages = [
        SimpleNamespace(
            count=3,
            next_offset="next",
            reactions=[SimpleNamespace(peer_id=SimpleNamespace(user_id=10), reaction=SimpleNamespace(emoticon="❤️"))],
        ),
        SimpleNamespace(
            count=3,
            next_offset=None,
            reactions=[
                SimpleNamespace(peer_id=SimpleNamespace(user_id=99), reaction=SimpleNamespace(emoticon="👎")),
                SimpleNamespace(peer_id=SimpleNamespace(user_id=10), reaction=SimpleNamespace(emoticon="✍️")),
            ],
        ),
    ]

    class Client:
        async def __call__(self, _request):
            return pages.pop(0)

    result = asyncio.run(sync.fetch_exact_reactions(Client(), object(), 7, {"10"}, page_limit=1))
    assert result["reactions_by_reviewer"] == {"10": ["positive_heart", "rewrite_pen"]}
    assert result["operator_review_decision"] == "approved"
    assert result["operator_review_rewrite_status"] == "rewrite_requested"
    assert result["ignored_reaction_count"] == 1


def test_pagination_failure_and_incomplete_count_fail_closed() -> None:
    class FailedSecondPage:
        calls = 0

        async def __call__(self, _request):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(count=2, next_offset="next", reactions=[
                    SimpleNamespace(peer_id=SimpleNamespace(user_id=10), reaction="👍")
                ])
            raise TimeoutError("page lost")

    with pytest.raises(TimeoutError):
        asyncio.run(sync.fetch_exact_reactions(FailedSecondPage(), object(), 7, {"10"}))

    class Truncated:
        async def __call__(self, _request):
            return SimpleNamespace(count=2, next_offset=None, reactions=[
                SimpleNamespace(peer_id=SimpleNamespace(user_id=10), reaction="👍")
            ])

    with pytest.raises(RuntimeError, match="incomplete exact reaction observation"):
        asyncio.run(sync.fetch_exact_reactions(Truncated(), object(), 7, {"10"}))


def test_revision_is_idempotent_and_reaction_removal_is_a_new_revision() -> None:
    current = {
        "operator_review_fingerprint": "fp",
        "message_id": "7",
        "chat_id": "-1",
        "reactions_by_reviewer": {"10": ["positive_like"]},
    }
    previous = {"observation_hash": sync.observation_hash(current)}
    assert sync.reaction_revision_changed(previous, current) is False
    removed = {**current, "reactions_by_reviewer": {}}
    assert sync.reaction_revision_changed(previous, removed) is True
    added = {**current, "reactions_by_reviewer": {"10": ["positive_like", "rewrite_pen"]}}
    assert sync.reaction_revision_changed(previous, added) is True
    initial_to_positive = sync.reaction_event_id("", previous["observation_hash"])
    positive_to_empty = sync.reaction_event_id(
        previous["observation_hash"], sync.observation_hash(removed)
    )
    assert initial_to_positive != positive_to_empty


def test_review_fingerprint_invalidates_on_copy_media_or_order_change() -> None:
    row = review_row()
    original = notify.publication_operator_review_fingerprint(row)
    assert original != notify.publication_operator_review_fingerprint({
        **row, "publication_draft_telegram_text": "Новый текст"
    })
    assert original != notify.publication_operator_review_fingerprint({
        **row, "media_manifest_items": [{"id": "photo-b"}, {"id": "photo-a"}]
    })
    assert original != notify.publication_operator_review_fingerprint({
        **row, "publication_presentation_mode": "single"
    })


def test_old_fingerprint_cannot_approve_current_revision() -> None:
    row = review_row()
    row.update({
        "operator_review_fingerprint": notify.publication_operator_review_fingerprint(row),
        "operator_review_decision": "approved",
        "operator_review_rewrite_status": "clean",
    })
    assert plan.operator_review_approved_clean(row) is True
    changed = {**row, "publication_draft_telegram_text": "Исправленная подводка"}
    assert plan.operator_review_approved_clean(changed) is False


def test_delivery_payload_is_explicit_and_message_has_reaction_legend() -> None:
    row = review_row()
    fields = notify.publication_delivery_review_fields(row)
    assert fields["operator_review_payload_version"] == notify.OPERATOR_REVIEW_PAYLOAD_VERSION
    assert fields["operator_review_fingerprint"] == notify.publication_operator_review_fingerprint(row)
    assert "photo-a" in fields["operator_review_media_manifest_json"]
    message = notify.candidate_message(row)
    assert "❤️ или 👍 — одобрить" in message
    assert "👎 — отклонить" in message
    assert "✍️ — нужен новый текст" in message


def test_reaction_sync_source_is_session_boundary_safe() -> None:
    source = (sync.ROOT / "scripts" / "region_talk_reaction_sync.py").read_text(encoding="utf-8")
    assert 'TRANSPORT = "telethon_discovery2"' in source
    assert "TELEGRAM_AUTH_BUNDLE_E2E" not in source
    assert "TELEGRAM_SESSION" not in source
