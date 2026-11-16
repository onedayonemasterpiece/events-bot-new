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


def test_two_guides_and_organizer_keep_roles_and_explicit_identities():
    profiles = [
        {'display_name': 'Шахноза (Ноза) Усманова', 'facts_rollup_json': '{"vk_user_id":408521495}'},
        {'display_name': 'Татьяна Удовенко — авторские маршруты', 'facts_rollup_json': '{"vk_user_id":99185890}'},
        {'display_name': 'Проект', 'source_links_json': '["https://t.me/ruin_keepers"]'},
    ]
    people = avatars.guide_participants({'guide_names': ['Ноза Усманова', 'Татьяна Удовенко'],
        'organizer_names': ['Проект']}, profiles)
    assert [x['owner'] for x in people] == [408521495, 99185890, None]
    assert [x['roles'] for x in people] == [['Гид'], ['Гид'], ['Организатор']]
    assert people[-1]['path'].endswith('ruin_keepers.jpg')
    assert avatars.guide_participants({'guide_names': ['Другая Усманова']}, profiles)[0]['owner'] is None


def test_latest_roles_clear_stale_organizer_and_same_person_keeps_both_roles():
    profile = {'display_name': 'Мария Морозова', 'facts_rollup_json': '{"vk_user_id":21093288}'}
    row = {'guide_names': ['Мария Морозова'], 'organizer_names': ['Посторонний'],
        'fact_pack': {'organizer_names': []}}
    people = avatars.guide_participants(row, [profile])
    assert len(people) == 1 and people[0]['roles'] == ['Гид']
    row['fact_pack']['organizer_names'] = ['Мария Морозова']
    people = avatars.guide_participants(row, [profile])
    assert len(people) == 1 and people[0]['roles'] == ['Гид', 'Организатор']


def test_card_renders_every_participant_without_using_source_photo(monkeypatch):
    from guide_excursions import visual_digest as visual
    drawn = []
    monkeypatch.setattr(visual, '_draw_avatar', lambda img, row, *args: drawn.append(row))
    people = [{'name': 'Ноза Усманова', 'roles': ['Гид'], 'path': '/nosa.webp'},
        {'name': 'Другой гид', 'roles': ['Гид'], 'path': None},
        {'name': 'Проект', 'roles': ['Организатор'], 'path': '/project.webp'}]
    visual.render_visual_digest_card([{'canonical_title': 'Прогулка', 'date': '2026-11-29',
        'source_avatar_path': '/source.webp', 'participant_medallions': people}], issue_id=1)
    assert [r['source_avatar_path'] for r in drawn] == ['/nosa.webp', '', '/project.webp']


@pytest.mark.asyncio
async def test_source_matching_an_explicit_organizer_gets_that_role_once(tmp_path, monkeypatch):
    monkeypatch.setattr(avatars, 'AVATAR_ROOT', tmp_path)
    photo = tmp_path / '-237389053.webp'
    Image.new('RGB', (10, 10)).save(photo, 'WEBP')
    photo.with_suffix('.json').write_text(json.dumps({'name': '80 ЛЕТ ЕДИНСТВА'}))
    async def ensure(owner): return photo
    monkeypatch.setattr(avatars, 'ensure_vk_avatar', ensure)
    row = {'source_platform': 'vk', 'source_flags_json': '{"vk_id":237389053}',
        'guide_names': ['Ноза Усманова'], 'organizer_names': ['80 ЛЕТ ЕДИНСТВА']}
    await avatars.prepare_guide_avatars([row])
    people = row['participant_medallions']
    assert len(people) == 2
    assert people[1]['owner'] == -237389053 and people[1]['roles'] == ['Организатор']
