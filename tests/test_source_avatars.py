import io
import json
from types import SimpleNamespace

import pytest
from PIL import Image, ImageDraw

import source_avatars as avatars


@pytest.mark.asyncio
@pytest.mark.parametrize('owner,method', [(123, 'users.get'), (-123, 'groups.getById')])
async def test_vk_portrait_reaches_both_consumers_and_reuses_download(tmp_path, monkeypatch, owner, method):
    import main
    import yandex_storage
    from guide_excursions.visual_digest import _draw_avatar, PALETTES
    from tg_graphic_medallions import resolve_event_graphic_medallions

    monkeypatch.setattr(avatars, 'AVATAR_ROOT', tmp_path)
    calls = []
    async def api(name, **params):
        calls.append((name, params))
        return [{'id': 123, 'first_name': 'Иван', 'last_name': 'Гид', 'name': 'Экскурсии', 'photo_200': 'https://vk.example/photo'}]
    data = io.BytesIO()
    Image.new('RGB', (400, 300), 'red').save(data, 'PNG')
    class Response:
        content = None
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        def raise_for_status(self): pass
        async def iter_chunked(self, _): yield data.getvalue()
    response = Response(); response.content = response
    monkeypatch.setattr(main, 'vk_api', api)
    monkeypatch.setattr(main, 'get_http_session', lambda: SimpleNamespace(get=lambda *a, **k: response))
    monkeypatch.setattr(yandex_storage, 'upload_yandex_public_bytes', lambda *a, **k: 'https://cdn.example/avatar.webp')
    path = await avatars.ensure_vk_avatar(owner)
    assert await avatars.ensure_vk_avatar(owner) == path
    assert len(calls) == 1 and calls[0][0] == method
    url = f'https://vk.com/wall{owner}_1'
    assert avatars.source_medallions([url])[0]['owner_id'] == owner
    assert not avatars.source_medallions([f'https://vk.com/wall{-owner}_1'])
    event = SimpleNamespace(source_vk_post_url=url)
    assert any(x.get('source_owner') == owner for x in resolve_event_graphic_medallions(event))
    image = Image.new('RGBA', (180, 180))
    _draw_avatar(image, {'source_avatar_path': str(path)}, 10, 10, 140, PALETTES[0], '#000000')
    pixel = image.getpixel((80, 80))
    assert pixel[0] > 240 and max(pixel[1:3]) < 10


def test_card_uses_existing_description_without_generation(monkeypatch):
    from guide_excursions.visual_digest import render_visual_digest_card
    texts = []
    original = ImageDraw.ImageDraw.text
    def text(self, xy, text, *args, **kwargs):
        texts.append(text)
        return original(self, xy, text, *args, **kwargs)
    monkeypatch.setattr(ImageDraw.ImageDraw, 'text', text)
    render_visual_digest_card([{'canonical_title': 'Поездка', 'date': '2026-11-29', 'city': 'Город', 'digest_blurb': 'Увидим лес и старые кирхи.'}], issue_id=1)
    assert any('Увидим лес' in str(x) for x in texts)
