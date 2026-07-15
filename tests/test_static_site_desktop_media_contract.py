from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _between(source: str, start: str, end: str) -> str:
    return source.split(start, 1)[1].split(end, 1)[0]


def test_desktop_related_documents_keep_the_complete_intrinsic_frame() -> None:
    layout = (ROOT / "site/src/layouts/EventLayout.astro").read_text(encoding="utf-8")
    document_css = _between(
        layout,
        "#discovery-feed .event-card__media-shell--document {",
        "#discovery-feed .event-card__media-shell--document .event-card__media {",
    )
    document_image_css = _between(
        layout,
        "#discovery-feed .event-card__media-shell--document .event-card__media {",
        "#discovery-feed .event-card--split-actions .event-card__body",
    )

    assert "aspect-ratio: auto" in document_css
    assert "aspect-ratio: 1 / 1.04" not in document_css
    assert "width: 100%" in document_image_css
    assert "height: auto" in document_image_css
    assert "max-height: none" in document_image_css
    assert "transform: none" in document_image_css
    assert "object-fit: cover" not in document_image_css
    assert "object-position:" not in document_image_css
    assert "position: absolute" not in document_image_css

    document_link_css = _between(
        layout,
        "#discovery-feed .event-card--split-actions .event-card__media-link--document {",
        "#discovery-feed .event-card__media-shell--cover {",
    )
    assert "border-radius: 0" in document_link_css
    assert "overflow: visible" in document_link_css


def test_only_explicit_event_photo_unlocks_related_card_cover() -> None:
    component = (ROOT / "site/src/components/EventCard.astro").read_text(encoding="utf-8")
    layout = (ROOT / "site/src/layouts/EventLayout.astro").read_text(encoding="utf-8")

    assert "const documentMedia = imageMediaRole !== 'event_photo'" in component
    assert "const documentMedia = data.image_media_role !== 'event_photo'" in layout


def test_only_explicit_event_photo_unlocks_hero_and_gallery_cover() -> None:
    hero = (ROOT / "site/src/components/EventHero.astro").read_text(encoding="utf-8")
    page = (ROOT / "site/src/pages/sobytiya/[slug].astro").read_text(encoding="utf-8")
    layout = (ROOT / "site/src/layouts/EventLayout.astro").read_text(encoding="utf-8")

    assert "const primaryIsEventPhoto = primaryImageMediaRole === 'event_photo'" in hero
    assert "event.image_text_mode === 'visual_only' ? 'photo-cover'" not in hero
    assert "const primaryIsPhoto = primaryImageMediaRole === 'event_photo'" in page
    assert "['event_photo', 'unknown_visual']" not in page
    assert '.hero-gallery__image[data-media-role="event_photo"]' in layout
    assert '.hero-gallery__image[data-image-text-mode="visual_only"]' not in layout
    assert "querySelector('.hero-gallery__image[data-media-role=\"event_photo\"]')" in layout


def test_related_payloads_carry_intrinsic_dimensions_for_cls() -> None:
    events = (ROOT / "site/src/lib/events.ts").read_text(encoding="utf-8")
    types = (ROOT / "site/src/lib/types.ts").read_text(encoding="utf-8")
    layout = (ROOT / "site/src/layouts/EventLayout.astro").read_text(encoding="utf-8")

    assert events.count("image_width: imageAsset?.width ?? null") == 2
    assert events.count("image_height: imageAsset?.height ?? null") == 2
    assert "image_width?: number | null" in types
    assert "image_height?: number | null" in types
    assert 'width="${escapeHtml(data.image_width)}"' in layout
    assert 'height="${escapeHtml(data.image_height)}"' in layout
