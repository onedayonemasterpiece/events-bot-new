from io import BytesIO
from types import SimpleNamespace

import pytest
from PIL import Image

from media_dedup import prepare_consumer_jpeg


@pytest.mark.parametrize(('kind', 'size', 'expected'), [
    ('WEBP', (9449, 4017), (1600, 680)),
    ('JPEG', (6000, 4000), (1600, 1067)),
])
def test_large_consumer_copy_is_bounded_without_changing_source(kind, size, expected):
    source = BytesIO()
    with Image.new('RGB', size, '#204060') as original:
        original.save(source, kind)
    raw = source.getvalue()
    from main import _vk_carousel_cover

    with Image.open(BytesIO(_vk_carousel_cover(raw))) as cover:
        assert cover.width == 1600 and cover.height > expected[1]
    with Image.open(BytesIO(prepare_consumer_jpeg(raw))) as image:
        assert image.size == expected and image.format == 'JPEG'
        assert image.layer[0][1:3] == (1, 1)  # No chroma subsampling.
    with Image.open(BytesIO(raw)) as unchanged:
        assert unchanged.size == size and unchanged.format == kind


def test_consumer_rejects_oversized_nondraft_source_before_load(monkeypatch):
    class HugeImage:
        width, height, size = 8000, 6000, (8000, 6000)
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def draft(self, *args): pass
        def thumbnail(self, *args): pytest.fail('Oversized raster must not load')

    monkeypatch.setattr(Image, 'open', lambda *_: HugeImage())
    with pytest.raises(ValueError, match='40 MP'):
        prepare_consumer_jpeg(b'oversized header')


def test_review_reuses_original_identity_before_preparing_working_copy(monkeypatch):
    import event_media
    poster = SimpleNamespace(raw_sha256='raw', pixel_sha256='pixels', phash='dhash',
                             perceptual_hash='phash', width=9449, height=4017,
                             mime_type='image/webp')
    monkeypatch.setattr(event_media, 'compute_image_fingerprints',
                        lambda *_: pytest.fail('Canonical identity already exists'))
    fp = event_media._review_fingerprints(poster, b'canonical bytes')
    assert fp.raw_sha256 == 'raw' and fp.pixel_sha256 == 'pixels'
    assert (fp.width, fp.height) == (9449, 4017)
