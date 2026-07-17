from pathlib import Path


DESKTOP_EVENT_PAGE = Path("site/src/components/DesktopEventPage.astro")


def _fullscreen_gallery_source() -> str:
    source = DESKTOP_EVENT_PAGE.read_text(encoding="utf-8")
    start = source.index("{photoCount > 0 && (")
    end = source.index("\n\n  <script>", start)
    return source[start:end]


def test_desktop_fullscreen_gallery_appends_mobile_parity_recommendation_slide() -> None:
    source = DESKTOP_EVENT_PAGE.read_text(encoding="utf-8")
    gallery = _fullscreen_gallery_source()

    assert "const viewerRecommendation = related[0];" in source
    assert "const gallerySlideCount = photoCount + (viewerRecommendation ? 1 : 0);" in source
    assert gallery.index("{galleryImages.map((image, index) => (") < gallery.index("{viewerRecommendation && (")
    assert (
        '<article class="hero-gallery__slide hero-gallery__slide--cta" '
        'data-hero-gallery-slide data-gallery-slide-kind="cta" '
        'data-gallery-index={galleryImages.length} aria-hidden="true">'
    ) in gallery
    assert 'class="hero-gallery__cta-image" data-gallery-src={viewerRecommendationImage}' in gallery
    assert '<span>Дальше можно похожее</span>' in gallery
    assert "<h2>{viewerRecommendation.title}</h2>" in gallery
    assert "{viewerRecommendationMeta && <p>{viewerRecommendationMeta}</p>}" in gallery
    assert '<a class="cta-button" href={eventHref(viewerRecommendation)}>Смотреть похожее</a>' in gallery


def test_desktop_gallery_counts_cta_but_keeps_image_and_efficient_stops_distinct() -> None:
    source = DESKTOP_EVENT_PAGE.read_text(encoding="utf-8")
    gallery = _fullscreen_gallery_source()

    assert 'data-gallery-slide-kind="image"' in gallery
    assert 'data-gallery-slide-kind="cta"' in gallery
    assert "{gallerySlideCount > 1 &&" in gallery
    assert "{`1 / ${gallerySlideCount}`}" in gallery
    assert 'data-efficient-viewer-item' not in gallery
    assert 'data-efficient-viewer-recommendation' not in gallery
    assert "const efficientStops = [...efficientItems, ...(efficientRecommendation ? [efficientRecommendation] : [])];" in source
