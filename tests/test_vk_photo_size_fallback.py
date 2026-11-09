"""A real VK photo must not become missing evidence solely due to its rendition."""
import asyncio
import sys
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import vk_intake

URL = 'https://sun9-9.userapi.com/photo.jpg?as=640x853%2C1280x1707%2C1920x2560&cs=1920x0'


@pytest.mark.parametrize('chunked', [False, True])
@pytest.mark.parametrize('case', ['smaller_available', 'still_oversize', 'http_error'])
@pytest.mark.asyncio
async def test_source_photo_uses_one_bounded_smaller_rendition(monkeypatch, chunked, case):
    calls = []

    class Content:
        async def iter_chunked(self, _size):
            yield b'0123456789'

    class Response:
        headers = {}
        content = Content()
        content_length = None if chunked else 10

        def __init__(self, small):
            if small and case == 'smaller_available':
                self.content_length = 3
                self.headers = {'Content-Length': '3'}
                async def chunks(_size):
                    yield b'jpg'
                self.content = SimpleNamespace(iter_chunked=chunks)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def raise_for_status(self):
            if case == 'http_error':
                raise ValueError('HTTP 404')

    class Session:
        def get(self, url, **_kwargs):
            calls.append(url)
            small = parse_qs(urlsplit(url).query)['cs'] == ['1280x0']
            return Response(small)

    fake_main = SimpleNamespace(get_http_session=lambda: Session(),
        HTTP_SEMAPHORE=asyncio.Semaphore(1), HTTP_TIMEOUT=1, MAX_DOWNLOAD_SIZE=5,
        ensure_jpeg=lambda data, name: (data, name), detect_image_type=lambda _data: 'jpeg',
        validate_jpeg_markers=lambda _data: None)
    monkeypatch.setitem(sys.modules, 'main', fake_main)
    result = await vk_intake._download_photo_media([URL])
    assert result == ([(b'jpg', 'vk_poster_1.jpg')] if case == 'smaller_available' else [])
    assert len(calls) == (1 if case == 'http_error' else 2)
    if len(calls) == 2:
        assert parse_qs(urlsplit(calls[1]).query)['cs'] == ['1280x0']


def test_non_vk_and_missing_advertised_sizes_do_not_get_fabricated_renditions():
    assert vk_intake._smaller_vk_photo_url(URL.replace('userapi.com', 'example.org')) is None
    assert vk_intake._smaller_vk_photo_url('https://sun9-9.userapi.com/photo.jpg?cs=1920x0') is None
