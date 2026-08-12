from __future__ import annotations

import copy

import pytest

from vk_source_envelope import (
    build_vk_source_envelope,
    vk_source_envelope_replayability,
    vk_source_packet_hashes,
    vk_source_semantic_projection,
)


def _photo(url: str, *, photo_id: int) -> dict:
    return {
        "type": "photo",
        "photo": {
            "owner_id": -1,
            "id": photo_id,
            "access_key": f"key-{photo_id}",
            "sizes": [{"width": 100, "height": 100, "url": url}],
        },
    }


def test_builder_preserves_outer_sibling_and_nested_copy_evidence() -> None:
    raw = {
        "id": 10,
        "owner_id": -1,
        "date": 100,
        "edited": 101,
        "text": "Концерт 12 августа",
        "attachments": [_photo("https://img/outer", photo_id=1)],
        "copy_history": [
            {
                "id": 20,
                "text": "Общий текст",
                "attachments": [],
                "copy_history": [
                    {
                        "id": 30,
                        "text": "Детали на афише",
                        "attachments": [_photo("https://img/nested", photo_id=2)],
                    }
                ],
            },
            {"id": 40, "text": "Второй источник", "attachments": []},
        ],
    }

    envelope = build_vk_source_envelope(raw, owner_id=1, media_limit=1)

    assert envelope["raw_item"]["text"] == "Концерт 12 августа"
    assert [segment["path"] for segment in envelope["text_segments"]] == [
        "$",
        "$.copy_history[0]",
        "$.copy_history[0].copy_history[0]",
        "$.copy_history[1]",
    ]
    assert "Концерт 12 августа" in envelope["text"]
    assert "Детали на афише" in envelope["text"]
    assert envelope["counts"] == {
        "attachment_inventory_count": 2,
        "visual_candidate_count": 2,
        "selected_media_count": 1,
        "omitted_media_count": 1,
        "unavailable_visual_count": 0,
        "text_segment_count": 4,
    }
    assert len(envelope["attachment_inventory"]) == 2
    assert len(envelope["all_media_candidates"]) == 2
    assert envelope["photos"] == ["https://img/outer"]
    assert envelope["omitted_media_candidates"][0]["url"] == "https://img/nested"
    assert envelope["raw_item"]["attachments"][0]["photo"]["access_key"] == "key-1"
    assert vk_source_envelope_replayability(envelope) == "replayable_lossless"


def test_builder_inventory_keeps_semantic_nonvisual_attachments_and_previews() -> None:
    raw = {
        "id": 11,
        "date": 100,
        "text": "",
        "attachments": [
            {
                "type": "link",
                "link": {
                    "url": "https://tickets.test/event",
                    "title": "Билеты на концерт",
                    "description": "12 августа в 19:00",
                    "photo": {"sizes": [{"width": 2, "height": 2, "url": "https://img/link"}]},
                },
            },
            {
                "type": "doc",
                "doc": {
                    "owner_id": -1,
                    "id": 2,
                    "title": "program.pdf",
                    "url": "https://docs.test/program.pdf",
                    "preview": {"photo": {"sizes": [{"width": 3, "height": 3, "url": "https://img/doc"}]}},
                },
            },
            {
                "type": "video",
                "video": {
                    "owner_id": -1,
                    "id": 3,
                    "title": "Анонс",
                    "description": "Начало в 20:00",
                    "image": [{"width": 4, "height": 4, "url": "https://img/video"}],
                },
            },
            {"type": "poll", "poll": {"id": 4, "question": "Придёте?"}},
        ],
    }

    envelope = build_vk_source_envelope(raw, owner_id=1)

    assert [item["type"] for item in envelope["attachment_inventory"]] == [
        "link", "doc", "video", "poll"
    ]
    assert envelope["photos"] == ["https://img/link", "https://img/doc", "https://img/video"]
    assert envelope["counts"]["visual_candidate_count"] == 3
    assert envelope["counts"]["attachment_inventory_count"] == 4
    assert envelope["attachment_inventory"][0]["semantic"]["content"]["url"] == "https://tickets.test/event"
    assert envelope["attachment_inventory"][3]["semantic"]["content"]["question"] == "Придёте?"


def test_semantic_revision_hash_matrix_ignores_counters_and_key_order() -> None:
    raw = {
        "id": 12,
        "date": 100,
        "edited": 101,
        "text": "outer",
        "views": {"count": 1},
        "attachments": [
            {"type": "link", "link": {"url": "https://a", "title": "A"}}
        ],
        "copy_history": [{"id": 2, "text": "copy", "attachments": []}],
    }
    base = build_vk_source_envelope(raw, owner_id=1)
    counter = copy.deepcopy(raw)
    counter["views"]["count"] = 999
    reordered = {key: raw[key] for key in reversed(list(raw))}
    changed_outer = {**raw, "text": "outer changed"}
    changed_copy = copy.deepcopy(raw)
    changed_copy["copy_history"][0]["text"] = "copy changed"
    changed_link = copy.deepcopy(raw)
    changed_link["attachments"][0]["link"]["title"] = "B"
    changed_edit = {**raw, "edited": 102}

    payload_hash, revision_hash = vk_source_packet_hashes(base)
    assert payload_hash != vk_source_packet_hashes(build_vk_source_envelope(counter, owner_id=1))[0]
    assert revision_hash == vk_source_packet_hashes(build_vk_source_envelope(counter, owner_id=1))[1]
    assert revision_hash == vk_source_packet_hashes(build_vk_source_envelope(reordered, owner_id=1))[1]
    for changed in (changed_outer, changed_copy, changed_link, changed_edit):
        assert revision_hash != vk_source_packet_hashes(
            build_vk_source_envelope(changed, owner_id=1)
        )[1]


def test_security_denylist_is_recursive_but_attachment_access_key_is_retained() -> None:
    raw = {
        "id": 13,
        "date": 100,
        "access_token": "provider-secret",
        "Authorization": "Bearer secret",
        "captcha_sid": "captcha",
        "error_params": [{"key": "access_token", "value": "nested-secret"}],
        "source_link": "https://example.test/path?access_token=url-secret&keep=yes#fragment",
        "copy_history": [
            {
                "id": 2,
                "token": "copy-secret",
                "attachments": [_photo("https://img/1", photo_id=1)],
            }
        ],
    }
    envelope = build_vk_source_envelope(raw, owner_id=1)
    encoded = str(envelope)
    assert "provider-secret" not in encoded
    assert "nested-secret" not in encoded
    assert "copy-secret" not in encoded
    assert "Bearer secret" not in encoded
    assert "url-secret" not in encoded
    assert envelope["raw_item"]["source_link"] == "https://example.test/path?keep=yes"
    assert envelope["raw_item"]["copy_history"][0]["attachments"][0]["photo"]["access_key"] == "key-1"


@pytest.mark.asyncio
async def test_crawl_fresh_and_legacy_facades_share_recursive_derivation(monkeypatch) -> None:
    import main
    import vk_auto_queue

    raw = {
        "id": 21,
        "owner_id": -1,
        "date": 100,
        "text": "outer event 12 August",
        "attachments": [_photo("https://img/outer", photo_id=1)],
        "copy_history": [
            {
                "id": 22,
                "text": "nested generic",
                "attachments": [
                    {
                        "type": "link",
                        "link": {
                            "title": "Ticket details",
                            "url": "https://tickets.test/21",
                            "photo": {"sizes": [{"width": 2, "height": 2, "url": "https://img/link"}]},
                        },
                    }
                ],
            }
        ],
    }
    calls: list[tuple[str, dict]] = []

    async def fake_api(method, **params):
        calls.append((method, params))
        return {"items": [copy.deepcopy(raw)]}

    monkeypatch.setattr(main, "vk_api", fake_api)
    crawled = (await main.vk_wall_since(1, 0))[0]
    text, photos, _published, _metrics, status = (
        await vk_auto_queue.fetch_vk_post_text_and_photos(1, 21)
    )
    legacy_photos = main._vk_extract_photo_urls([raw])

    assert len(calls) == 2
    assert [method for method, _params in calls] == ["wall.get", "wall.getById"]
    assert text == crawled["text"]
    assert photos == crawled["photos"] == legacy_photos
    assert "outer event" in text and "nested generic" in text and "Ticket details" in text
    assert vk_source_semantic_projection(crawled) == vk_source_semantic_projection(
        status.source_envelope or {}
    )


@pytest.mark.asyncio
async def test_user_wall_owner_type_is_positive_and_canonical_across_crawl_and_fresh(monkeypatch) -> None:
    import main
    import vk_auto_queue

    raw = {"id": 7, "owner_id": 42, "date": 100, "text": "personal event", "attachments": []}
    calls: list[tuple[str, dict]] = []

    async def fake_api(method, **params):
        calls.append((method, params))
        return {"items": [copy.deepcopy(raw)]}

    monkeypatch.setattr(main, "vk_api", fake_api)
    crawled = (await main.vk_wall_since(42, 0, owner_type="user"))[0]
    _text, _photos, _published, _metrics, status = (
        await vk_auto_queue.fetch_vk_post_text_and_photos(
            42, 7, owner_type="user"
        )
    )
    assert calls[0][1]["owner_id"] == 42
    assert calls[1][1]["posts"] == "42_7"
    assert crawled["source_url"] == "https://vk.com/wall42_7"
    assert (status.source_envelope or {})["source_url"] == "https://vk.com/wall42_7"
