from __future__ import annotations

from PIL import Image

from scripts.research.prepare_service_share_faces import CROP_REASONS, frame_square


def _wide_source() -> Image.Image:
    """A wide photo with side regions that a centered square cover removes."""

    image = Image.new("RGB", (1200, 600), (20, 180, 80))
    for x in range(300):
        for y in range(600):
            image.putpixel((x, y), (240, 20, 80))
            image.putpixel((1199 - x, y), (20, 80, 240))
    return image


def test_visual_only_photo_uses_cover_even_when_safe_crop_is_false() -> None:
    framed, crop, mode = frame_square(
        _wide_source(),
        {"image_text_mode": "visual_only", "safe_crop": False},
    )

    assert framed.size == (1024, 1024)
    assert crop == [300, 0, 900, 600]
    assert mode == "non_ocr_photo_center_cover"
    assert CROP_REASONS[mode] == "non_ocr_photo_with_renderer_title_and_date"
    # The full square is sourced from the central photo area: no generated
    # contain/letterbox field remains above or below the event image.
    assert framed.getpixel((0, 0)) == (20, 180, 80)
    assert framed.getpixel((1023, 1023)) == (20, 180, 80)


def test_ocr_poster_preserves_full_image_contain_when_crop_is_unsafe() -> None:
    source = _wide_source()
    framed, crop, mode = frame_square(
        source,
        {"image_text_mode": "ocr_text", "safe_crop": False},
    )

    assert framed.size == (1024, 1024)
    assert crop == [0, 0, 1200, 600]
    assert mode == "full_image_contain"
    assert CROP_REASONS[mode] == "protect_ocr_or_unclassified_document_edges"
    # Existing document behavior remains: both edge regions survive inside
    # the contained foreground instead of being cropped as a photo.
    assert framed.getpixel((47, 512)) == (240, 20, 80)
    assert framed.getpixel((976, 512)) == (20, 80, 240)


def test_null_mode_without_ocr_uses_legacy_photo_cover() -> None:
    framed, crop, mode = frame_square(
        _wide_source(),
        {"image_text_mode": None, "image_has_ocr_text": False, "safe_crop": False},
    )

    assert framed.size == (1024, 1024)
    assert crop == [300, 0, 900, 600]
    assert mode == "classification_gap_landscape_cover"
    assert CROP_REASONS[mode] == "null_mode_landscape_with_renderer_title_and_date"


def test_null_mode_landscape_with_incidental_ocr_uses_photo_cover() -> None:
    framed, crop, mode = frame_square(
        _wide_source(),
        {"image_text_mode": None, "image_has_ocr_text": True, "safe_crop": False},
    )

    assert framed.size == (1024, 1024)
    assert crop == [300, 0, 900, 600]
    assert mode == "classification_gap_landscape_cover"


def test_null_mode_portrait_or_square_with_ocr_keeps_full_document() -> None:
    for size in ((800, 1000), (900, 900)):
        source = Image.new("RGB", size, (80, 100, 120))
        framed, crop, mode = frame_square(
            source,
            {"image_text_mode": None, "image_has_ocr_text": True, "safe_crop": False},
        )

        assert framed.size == (1024, 1024)
        assert crop == [0, 0, *size]
        assert mode == "full_image_contain"


def test_ocr_poster_still_honours_explicit_safe_crop() -> None:
    framed, crop, mode = frame_square(
        _wide_source(),
        {"image_text_mode": "ocr_text", "safe_crop": True},
    )

    assert framed.size == (1024, 1024)
    assert crop == [300, 0, 900, 600]
    assert mode == "safe_center_cover"
