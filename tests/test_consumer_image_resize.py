from io import BytesIO
from types import SimpleNamespace

import pytest
from PIL import Image

from media_dedup import prepare_consumer_jpeg


@pytest.mark.parametrize(('kind', 'size', 'expected'), [
    ('WEBP', (9449, 4017), (1600, 680)),
    ('JPEG', (6000, 4000), (1600, 1067)),
    ('JPEG', (8000, 6000), (1600, 1200)),
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
        format = "PNG"
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


@pytest.mark.parametrize("max_side", [1200, 1600])
def test_webp_above_40mp_is_scaled_during_native_decode(max_side):
    source = BytesIO()
    with Image.new("RGB", (11216, 4000), "#204060") as original:
        original.save(source, "WEBP")
    raw = source.getvalue()
    with Image.open(BytesIO(prepare_consumer_jpeg(raw, max_side=max_side))) as image:
        assert image.format == "JPEG" and image.width == max_side
        assert image.height == int(4000 * max_side / 11216)
        assert image.layer[0][1:3] == (1, 1)
    with Image.open(BytesIO(raw)) as original:
        assert original.size == (11216, 4000) and original.format == "WEBP"


def test_consumer_preserves_small_rgba_quality():
    source = BytesIO()
    with Image.new("RGBA", (64, 48), (32, 64, 96, 0)) as original:
        original.paste((200, 50, 80, 255), (16, 16, 48, 32))
        original.save(source, "PNG")
    with Image.open(BytesIO(prepare_consumer_jpeg(source.getvalue()))) as image:
        assert image.size == (64, 48)
        assert image.getpixel((0, 0)) == (255, 255, 255)
        assert max(abs(a - b) for a, b in zip(image.getpixel((32, 24)), (200, 50, 80))) < 10


def test_native_webp_preserves_exif_orientation_and_alpha():
    source = BytesIO()
    exif = Image.Exif()
    exif[274] = 6
    with Image.new("RGBA", (11216, 4000), (32, 64, 96, 0)) as original:
        original.save(source, "WEBP", exif=exif)
    with Image.open(BytesIO(prepare_consumer_jpeg(source.getvalue(), max_side=1600))) as image:
        assert image.size == (570, 1600)
        assert image.getpixel((0, 0)) == (255, 255, 255)


def test_consumer_keeps_pillow_bomb_and_malformed_rejections(monkeypatch):
    source = BytesIO()
    Image.new("RGB", (20, 20)).save(source, "PNG")
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 100)
    with pytest.raises(Image.DecompressionBombError):
        prepare_consumer_jpeg(source.getvalue())
    with pytest.raises(OSError):
        prepare_consumer_jpeg(b"not an image")


def test_animated_webp_retains_existing_first_frame_behavior():
    source = BytesIO()
    with Image.new("RGB", (80, 40), "red") as first, Image.new("RGB", (80, 40), "blue") as second:
        first.save(source, "WEBP", save_all=True, append_images=[second], duration=100)
    with Image.open(BytesIO(prepare_consumer_jpeg(source.getvalue(), max_side=40))) as image:
        assert image.size == (40, 20)
        assert image.getpixel((0, 0))[0] > 240
