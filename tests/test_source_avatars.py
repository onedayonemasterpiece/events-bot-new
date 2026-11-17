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
    portrait = Image.open(path).convert('RGBA')
    assert portrait.getpixel((0, 0))[3] == 0
    assert portrait.getpixel((160, 160))[3] == 255
    url = f'https://vk.com/wall{owner}_1'
    assert avatars.source_medallions([url])[0]['owner_id'] == owner
    assert not avatars.source_medallions([f'https://vk.com/wall{-owner}_1'])
    Image.new('RGB', (320, 320), 'blue').save(tmp_path / '-999.webp')
    event = SimpleNamespace(source_post_url=url, source_vk_post_url='https://vk.com/wall-999_1')
    assert [x.get('source_owner') for x in resolve_event_graphic_medallions(event)] == [owner]
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
    render_visual_digest_card([{'canonical_title': 'Поездка', 'date': '2026-11-29', 'city': 'Город', 'fact_pack': {'main_hook': 'Увидим лес и старые кирхи.'}}], issue_id=1)
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


def test_card_renders_pictured_participants_without_using_source_photo(monkeypatch, tmp_path):
    # Owner rule 2026-11-16: a row shows only people with a picture; initials appear once and
    # only when nobody in the row has one.
    from guide_excursions import visual_digest as visual
    drawn = []
    monkeypatch.setattr(visual, '_draw_avatar', lambda img, row, *args: drawn.append(row))
    nosa, project = str(tmp_path / 'nosa.webp'), str(tmp_path / 'project.webp')
    for path in (nosa, project):
        Image.new('RGB', (10, 10)).save(path, 'WEBP')
    people = [{'name': 'Ноза Усманова', 'roles': ['Гид'], 'path': nosa},
        {'name': 'Другой гид', 'roles': ['Гид'], 'path': None},
        {'name': 'Проект', 'roles': ['Организатор'], 'path': project}]
    row = {'canonical_title': 'Прогулка', 'date': '2026-11-29', 'source_avatar_path': '/source.webp',
        'participant_medallions': people}
    visual.render_visual_digest_card([row], issue_id=1)
    assert [r['source_avatar_path'] for r in drawn] == [nosa, project]
    drawn.clear()
    visual.render_visual_digest_card([dict(row, participant_medallions=[
        {'name': 'Проект', 'roles': ['Организатор'], 'path': None},
        {'name': 'Другой гид', 'roles': ['Гид'], 'path': None}])], issue_id=1)
    assert [r['guide_names'] for r in drawn] == [['Другой гид']]


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


@pytest.mark.asyncio
async def test_two_source_aliases_merge_after_vk_resolution_and_keep_other_people(tmp_path, monkeypatch):
    source = -237389053
    guide_owner = 123
    photo = tmp_path / 'public.webp'
    async def ensure(owner):
        return photo
    monkeypatch.setattr(avatars, 'ensure_vk_avatar', ensure)
    monkeypatch.setattr(avatars, 'avatar_metadata', lambda owner: {'name': '80 лет единства'})
    profiles = [{'display_name': 'Анна Орлова', 'facts_rollup_json': '{"vk_user_id":123}'}]
    participants = avatars.guide_participants
    monkeypatch.setattr(avatars, 'guide_participants', lambda row, _: participants(row, profiles))
    row = {'source_platform': 'vk', 'source_flags_json': '{"vk_id":237389053}',
           'guide_names': ['Анна Орлова', '80 лет единства — экскурсионный контур', 'Мария Орлова'],
           'organizer_names': ['80 лет единства']}
    await avatars.prepare_guide_avatars([row])
    assert [(p['name'], p['owner'], p['roles']) for p in row['participant_medallions']] == [
        ('Анна Орлова', guide_owner, ['Гид']),
        ('80 лет единства', source, ['Гид', 'Организатор']),
        ('Мария Орлова', None, ['Гид']),
    ]


def test_old_card_features_and_full_size_horizontal_participants_are_preserved(monkeypatch, tmp_path):
    from guide_excursions import visual_digest as visual
    avatars_drawn, badges, chips, texts = [], [], [], []
    monkeypatch.setattr(visual, '_draw_avatar', lambda img, row, x, y, size, *args: avatars_drawn.append((x,y,size)))
    original_badges, original_chip, original_text = visual._draw_icon_badges, visual._draw_chip, ImageDraw.ImageDraw.text
    def badge(img, modes, *args): badges.extend(modes); return original_badges(img,modes,*args)
    def chip(draw, text, *args): chips.append(text); return original_chip(draw,text,*args)
    def text(draw, xy, value, *args, **kwargs): texts.append(value); return original_text(draw,xy,value,*args,**kwargs)
    monkeypatch.setattr(visual, '_draw_icon_badges', badge)
    monkeypatch.setattr(visual, '_draw_chip', chip)
    monkeypatch.setattr(ImageDraw.ImageDraw, 'text', text)
    row = {'canonical_title': 'Амалиенау', 'date': '2026-11-20', 'seats_text':'10 мест',
        'fact_pack': {'group_format':'walking_tour', 'route_summary':'Амалиенау',
                      'main_hook':'архитектура · виллы · детали'},
        'participant_medallions': [ {'name':'Гид Один','roles':['Гид'],'path':str(tmp_path/'a.webp')},
            {'name':'Проект','roles':['Организатор'],'path':str(tmp_path/'b.webp')} ]}
    for name in ('a.webp', 'b.webp'):
        Image.new('RGB', (10, 10)).save(tmp_path / name, 'WEBP')
    visual.render_visual_digest_card([row], issue_id=309)
    assert [a[2] for a in avatars_drawn] == [140,140]
    assert avatars_drawn[0][1] == avatars_drawn[1][1]
    assert avatars_drawn[1][0]-avatars_drawn[0][0] == 112
    assert 'walk' in badges and chips == ['10 мест']
    assert 'архитектура · виллы · детали' in texts
    assert not set(texts) & {'Гид','Организатор','Источник','Источник ВКонтакте'}
    assert visual._modes_for({'fact_pack':{'group_format':'bus_tour'}}) == ['bus']
    assert visual._seats_text({'seats_text':'7+', 'fact_pack':{'seats_text':None}}) == ''


def test_route_title_is_not_repeated_as_the_place_for_real_new_tour():
    from guide_excursions.visual_digest import _place_line
    assert _place_line({'canonical_title':'Зелёные истории', 'city':'Калининград',
        'fact_pack':{'route_summary':'Зелёные истории'}}) == 'Калининград'


def test_existing_person_profile_alias_keeps_one_portrait_for_both_roles():
    profiles = [
        {'display_name':'Amber Fringilla','profile_kind':'person',
         'facts_rollup_json':'{"guide_line":"Юлия Гришанова, орнитолог и гид"}'},
        {'display_name':'Юлия Гришанова — экопрогулки','profile_kind':'guide',
         'facts_rollup_json':'{"vk_user_id":51545052}'},
    ]
    people = avatars.guide_participants({'guide_names':['Юлия Гришанова'],
        'organizer_names':['Amber Fringilla']},profiles)
    assert len(people) == 1 and people[0]['owner'] == 51545052
    assert people[0]['roles'] == ['Гид','Организатор']
    profiles[0]['profile_kind']='project'
    assert len(avatars.guide_participants({'guide_names':['Юлия Гришанова'],
        'organizer_names':['Amber Fringilla']},profiles)) == 2


def test_card_description_keeps_the_entire_hook():
    from guide_excursions.visual_digest import _card_description
    row = {'canonical_title': 'Зелёные истории', 'guide_names': ['Ольга Рубцова'],
           'organizer_names': ['80 ЛЕТ ЕДИНСТВА'],
           'fact_pack': {'main_hook': 'экскурсия проекта 80 ЛЕТ ЕДИНСТВА с музыкой, играми и квестами'},
           'digest_blurb': 'Приглашаем на экскурсию «Зелёные истории» 17 ноября в 14:00. Гид — Ольга Рубцова.'}
    assert _card_description(row) == row['fact_pack']['main_hook']
    row = {'canonical_title': 'Секреты чёрного золота: экскурсия на Осетровую ферму',
           'fact_pack': {'main_hook': 'экскурсия на Осетровую ферму с секретами чёрного золота'},
           'digest_blurb': 'Увлекательная экскурсия на Осетровую ферму с дегустацией эксклюзивных закусок'}
    assert _card_description(row) == row['fact_pack']['main_hook']
    assert _card_description({'canonical_title': 'Поездка', 'digest_blurb': 'Поездка'}) == ''


def test_card_does_not_repeat_named_venue_below_title(monkeypatch):
    from guide_excursions.visual_digest import render_visual_digest_card
    texts = []
    original = ImageDraw.ImageDraw.text
    def capture(self, xy, text, *args, **kwargs):
        texts.append(text)
        return original(self, xy, text, *args, **kwargs)
    monkeypatch.setattr(ImageDraw.ImageDraw, 'text', capture)
    render_visual_digest_card([{'canonical_title': 'Экскурсия на Осетровую ферму',
        'route_summary': 'Осетровую ферму', 'date': '2026-11-22'}], issue_id=1)
    assert 'Осетровую ферму' not in texts


@pytest.mark.parametrize('original_cached', [False, True])
def test_publisher_avatar_is_not_an_original_source_fallback(tmp_path, monkeypatch, original_cached):
    from tg_graphic_medallions import resolve_event_graphic_medallions
    portrait = tmp_path / 'avatar.webp'
    Image.new('RGB', (80, 80), 'red').save(portrait)
    monkeypatch.setattr(avatars, 'avatar_path', lambda owner: portrait if owner == -999 or (owner == -123 and original_cached) else None)
    event = SimpleNamespace(source_post_url='https://vk.com/wall-123_1', source_vk_post_url='https://vk.com/wall-999_2')
    assert [item['source_owner'] for item in resolve_event_graphic_medallions(event)] == ([-123] if original_cached else [])
    assert avatars.round_avatar_image(Image.new('RGBA', (80, 80), (255, 0, 0, 100))).getpixel((40, 40))[3] == 100


@pytest.mark.asyncio
async def test_site_medallion_uses_original_while_source_links_keep_publication(tmp_path, monkeypatch):
    import importlib.util
    import sqlite3
    from pathlib import Path
    import main
    spec = importlib.util.spec_from_file_location('avatar_site_export', Path(__file__).resolve().parents[1] / 'site/scripts/export-production-preview-data.py')
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    monkeypatch.setattr(exporter, '_SOURCE_AVATARS', {'-123': {'owner_id': -123, 'image_url': '/original.webp'}, '-999': {'owner_id': -999, 'image_url': '/publisher.webp'}})
    db = main.Database(str(tmp_path / 'site.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            session.add(main.Event(id=1, title='Викторина', date='2099-01-01', time='19:00', location_name='', description='', source_text='', source_post_url='https://vk.com/wall-123_1', source_vk_post_url='https://vk.com/wall-999_2'))
            await session.commit()
        with sqlite3.connect(db.path) as con:
            con.row_factory = sqlite3.Row
            event = exporter.build_event(con, con.execute('select * from event where id=1').fetchone(), '2098-12-01')
        assert event['source_urls'][:2] == ['https://vk.com/wall-123_1', 'https://vk.com/wall-999_2']
        assert [item['owner_id'] for item in event['source_medallions']] == [-123]
    finally:
        await db.close()


def test_dated_guide_credit_supplies_writer_identity_without_a_profile():
    title = 'Время первых: через тернии к звёздам'
    line = f'28.11 (сб) 11:00 — «{title}», гид [id123|Варвара Лалэко]'
    post = {'url': 'https://vk.com/wall-10_20', 'post_date': '2026-11-10', 'text': line}
    row = {'id': 749, 'canonical_title': title, 'date': '2026-11-28', 'time': '11:00',
           'linked_posts': [post], 'source_roles_checked': True, 'fact_pack': {'guide_names': []}}
    row['participant_medallions'] = avatars.guide_participants(row, [])
    from guide_excursions.visual_digest import _vk_narrative_payload
    payload, identities = _vk_narrative_payload([row])
    assert list(identities.values()) == [(123, 'Варвара Лалэко')]
    assert payload['excursions'][0]['guides'] == ['Варвара Лалэко']

    for different in (dict(date='2026-11-29'), dict(time='12:00'), dict(canonical_title='Другой маршрут')):
        assert not avatars.guide_participants(dict(row, **different), [])
    for wrong in (dict(url='https://t.me/example/20'), dict(text=line.replace('гид', 'организатор')),
                  dict(text=line.replace('[id123', '[club123'))):
        assert not avatars.guide_participants(dict(row, linked_posts=[dict(post, **wrong)]), [])
    other = dict(post, text=line.replace('[id123', '[id456'))
    conflict, = avatars.guide_participants(dict(row, linked_posts=[post, other]), [])
    assert conflict['name'] == 'Варвара Лалэко' and conflict['owner'] is None
    stale = {'display_name': 'Варвара Лалэко', 'facts_rollup_json': '{"vk_user_id":456}'}
    conflict, = avatars.guide_participants(row, [stale])
    assert conflict['owner'] is None and conflict['path'] is None


def test_same_announced_guide_on_two_departures_keeps_one_writer_identity():
    from guide_excursions.visual_digest import _vk_narrative_payload

    linked = '29.11 (вс) 11:00 — «Созидание: люди и судьбы», гид [id90171626|Татьяна Медведева]'
    plain = '29.11 (вс) 14:00 — «От Победы до Победы», гид Татьяна Медведева'
    post = dict(url='https://vk.com/wall-211429559_3892', post_date='2026-11-15', text=linked+'\n'+plain)
    first = dict(id=751, canonical_title='Созидание: люди и судьбы', date='2026-11-29', time='11:00', linked_posts=[post])
    second = dict(first, id=752, canonical_title='От Победы до Победы', time='14:00')
    for row in (first, second):
        row['participant_medallions'] = avatars.guide_participants(row, [])
        assert row['participant_medallions'][0]['owner'] == 90171626
    payload, people = _vk_narrative_payload([first, second])
    assert people == {1: (90171626, 'Татьяна Медведева')}
    assert payload['people'][0]['excursions'] == [751, 752]

    # Another post, date, role, or namesake cannot identify the second departure's guide.
    for text in (plain, linked.replace('29.11', '28.11')+'\n'+plain,
                 linked.replace('гид ', 'запись у ')+'\n'+plain,
                 linked.replace('Татьяна Медведева', 'Тамара Медведева')+'\n'+plain):
        people = avatars.guide_participants(dict(second, linked_posts=[dict(post, text=text)]), [])
        assert not any(person.get('owner') for person in people)
    conflicting = linked.replace('11:00', '10:00').replace('id90171626', 'id999')
    people = avatars.guide_participants(dict(second, linked_posts=[dict(post, text='\n'.join([linked, conflicting, plain]))]), [])
    assert people[0]['owner'] is None


@pytest.mark.asyncio
async def test_first_party_operator_community_works_from_telegram_and_a_third_party_wall(tmp_path, monkeypatch):
    import aiosqlite
    import main

    db_path = tmp_path / 'registry.sqlite'
    db = SimpleNamespace(raw_conn=lambda: aiosqlite.connect(db_path))
    async with db.raw_conn() as conn:
        await conn.executescript('''
            CREATE TABLE guide_profile (id, display_name, marketing_name, profile_kind, source_links_json, facts_rollup_json);
            CREATE TABLE guide_source (primary_profile_id, title, about_text, platform);
            INSERT INTO guide_profile VALUES (1, 'Профи-тур', '', 'operator', '[]', '{}');
            INSERT INTO guide_source VALUES (1, 'Экскурсии от «Профи-тур»', 'Мы ВКонтакте: https://vk.com/profi_tur39', 'telegram');
            INSERT INTO guide_profile VALUES (2, 'Клуб Прогулок', '', 'project', '["https://vk.com/club_walks"]', '{}');
        ''')
    image = tmp_path / 'portrait.webp'
    Image.new('RGB', (64,64), 'red').save(image)
    handles = []
    async def resolve(handle):
        handles.append(handle)
        if handle == 'club_walks': return 46, 'Клуб прогулок', handle, 'group'
        return 12286984, 'ПУТЕШЕСТВИЯ "ПРОФИ-ТУР"', handle, 'group'
    async def ensure(owner): return image if owner in {-12286984, -46} else None
    monkeypatch.setattr(main, 'vk_resolve_group', resolve)
    monkeypatch.setattr(avatars, 'ensure_vk_avatar', ensure)
    primary = dict(source_platform='telegram', source_title='Экскурсии от «Профи-тур»',
                   source_about_text='Мы ВКонтакте: https://vk.com/profi_tur39', organizer_names=['Профи-тур'])
    secondary = dict(source_platform='vk', source_title='Завод OSQ', source_flags_json='{"vk_id":123}',
                     organizer_names=['Профи-тур', 'Клуб прогулок'], guide_names=['Татьяна'])
    await avatars.prepare_guide_avatars([primary, secondary, dict(primary)], db)
    assert set(handles) == {'profi_tur39', 'club_walks'} and len(handles) == 2
    assert primary['participant_medallions'] == [{'name':'Профи-тур','roles':['Организатор'],'owner':-12286984,'path':str(image)}]
    assert [p['owner'] for p in secondary['participant_medallions']] == [None,-12286984,-46]


@pytest.mark.asyncio
@pytest.mark.parametrize(('about', 'source', 'title', 'kind'), [
    ('Контакт: https://vk.com/example', 'Экскурсии от «Бюро»', 'Бюро', 'group'),
    ('Мы ВКонтакте: https://vk.com/example', 'Другой издатель', 'Бюро', 'group'),
    ('Мы ВКонтакте: https://vk.com/example', 'Бюро', 'Друзья бюро', 'group'),
    ('Мы ВКонтакте: https://vk.com/example', 'Бюро', 'Бюро', 'user'),
    ('Мы ВКонтакте: https://vk.com/first и https://vk.com/second', 'Бюро', 'Бюро', 'group'),
    ('Мы ВКонтакте: https://vk.com/example/posts', 'Бюро', 'Бюро', 'group'),
])
async def test_organizer_portrait_needs_an_unambiguous_first_party_group(monkeypatch, about, source, title, kind):
    import main
    async def resolve(handle): return 123, title, handle, kind
    async def no_portrait(owner): pytest.fail(f'Unexpected portrait request for {owner}')
    monkeypatch.setattr(main, 'vk_resolve_group', resolve)
    monkeypatch.setattr(avatars, 'ensure_vk_avatar', no_portrait)
    row = dict(source_platform='telegram', source_title=source, source_about_text=about,
               organizer_names=['Бюро'], guide_names=['Инга Петрова'])
    await avatars.prepare_guide_avatars([row])
    assert [p['owner'] for p in row['participant_medallions']] == [None, None]


@pytest.mark.asyncio
async def test_competing_registered_and_declared_communities_remain_unassigned(monkeypatch):
    import main
    async def unexpected(handle): pytest.fail(f'Unexpected VK lookup for {handle}')
    monkeypatch.setattr(main, 'vk_resolve_group', unexpected)
    row = dict(source_platform='telegram', source_title='Бюро', source_about_text='Мы ВКонтакте: https://vk.com/second')
    organizer = {'name':'Бюро', 'roles':['Организатор'], 'owner':None}
    await avatars._attach_organizer_communities(row, [organizer], {'бюро':{'first'}}, {})
    assert organizer['owner'] is None
