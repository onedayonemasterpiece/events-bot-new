from __future__ import annotations

import hashlib
import io

import pytest
from PIL import Image

import event_media
from media_dedup import (
    build_event_thumbnail_object_path,
    prepare_image_for_supabase,
)


def _jpeg_bytes(width: int = 1600, height: int = 1000) -> bytes:
    image = Image.new("RGB", (width, height), (34, 91, 146))
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=90)
    return out.getvalue()


def _role_decision(*, role: str, confidence: float = 0.97, contract: bool = True):
    return {
        "media_role": role,
        "image_text_mode": "ocr_text",
        "primary_purpose": "identifies the event",
        "confidence": confidence,
        "reason_code": "event_identity_matches",
        "poster_contract": {
            "primary_event_promotion": contract,
            "event_identity_grounded": contract,
            "not_utility_document": contract,
        },
        "focal_point": {"x": 0.45, "y": 0.4},
        "safe_crop": False,
        "evidence": ["title and date match"],
    }


def test_prepared_event_media_has_small_immutable_derivatives() -> None:
    prepared = prepare_image_for_supabase(_jpeg_bytes())

    assert prepared is not None
    assert (prepared.width, prepared.height) == (1600, 1000)
    assert prepared.encoded_sha256 == hashlib.sha256(prepared.webp_bytes).hexdigest()
    assert [item.longest_edge for item in prepared.thumbnails] == [256, 512]
    assert [(item.width, item.height) for item in prepared.thumbnails] == [
        (256, 160),
        (512, 320),
    ]
    assert all(item.webp_bytes for item in prepared.thumbnails)


def test_thumbnail_object_path_is_recipe_and_content_addressed() -> None:
    digest = "ab" * 32

    assert build_event_thumbnail_object_path(digest, longest_edge=256) == (
        f"p/thumb/v1/ab/{digest}/256.webp"
    )
    with pytest.raises(ValueError, match="unsupported event thumbnail edge"):
        build_event_thumbnail_object_path(digest, longest_edge=320)


def test_identity_poster_contract_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("EVENT_MEDIA_ROLE_POSTER_CONFIDENCE", "0.88")

    accepted = event_media._validated_media_role(
        _role_decision(role="event_identity_poster")
    )
    assert accepted is not None
    assert accepted["media_role"] == "event_identity_poster"

    assert event_media._validated_media_role(
        _role_decision(role="event_identity_poster", confidence=0.87)
    ) is None
    assert event_media._validated_media_role(
        _role_decision(role="event_identity_poster", contract=False)
    ) is None


def test_utility_document_is_never_promoted_by_deterministic_validation() -> None:
    value = _role_decision(role="attendee_information", contract=False)
    value["primary_purpose"] = "services and visitor rules"
    value["reason_code"] = "utility_services_card"

    accepted = event_media._validated_media_role(value)

    assert accepted is not None
    assert accepted["media_role"] == "attendee_information"
    assert accepted["poster_contract"]["primary_event_promotion"] is False
