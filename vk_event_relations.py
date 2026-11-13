"""VK presentation of the shared semantic graph and canonical linked occurrences."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from io import BytesIO
import json
import logging
from pathlib import Path
import textwrap

from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import select
from models import Event, Setting

ROOT = Path(__file__).parent


def graph_path(db):
    return Path(db.path).with_name('static_site_event_related.json')


async def presentation(db, event):
    import main
    path = graph_path(db)
    graph = json.loads(path.read_text())['related'] if path.is_file() else {}
    # `similar` can contain broad discovery when pure matches are absent.
    similar_ids = graph.get(str(event.id), {}).get('pure_related', [])
    linked_ids = list(event.linked_event_ids or [])
    if not linked_ids and not similar_ids:
        return {'dates': [], 'related': []}
    async with db.get_session() as session:
        rows = (await session.execute(select(Event).where(
            Event.id.in_(set(linked_ids + similar_ids)),
            Event.lifecycle_status == 'active', Event.silent.is_(False),
            Event.identity_status != 'merged', Event.merged_into_event_id.is_(None),
        ))).scalars().all()
    rows = {row.id: row for row in rows if row.id != event.id
            and not main._event_has_ended_before_today(row)
            and not main._event_start_has_passed_for_publication(row)}
    dates = sorted((rows[i] for i in linked_ids if i in rows),
                   key=lambda row: (row.date, row.time or '', row.id))
    related, seen_urls = [], {event.source_vk_post_url}
    for i in similar_ids:
        row = rows.get(i)
        if not row or i in linked_ids or not row.source_vk_post_url or row.source_vk_post_url in seen_urls:
            continue
        seen_urls.add(row.source_vk_post_url)
        related.append(row)
        if len(related) == 5:
            break
    from tg_graphic_medallions import resolve_event_graphic_medallions
    def item(row):
        date = main.parse_iso_date(row.date)
        return {'id': row.id, 'date': date.strftime('%d.%m') if date else row.date,
                'time': row.time or '', 'title': row.title,
                'place': row.location_name or '', 'url': row.source_vk_post_url or '',
                'medallions': [str(Path(m['asset_path']).relative_to(ROOT))
                               for m in resolve_event_graphic_medallions(row, limit=2)],
                'is_free': bool(row.is_free)}
    return {'dates': [item(row) for row in dates[:6]],
            'related': [item(row) for row in related]}


def listing(items):
    return '\n'.join(f"{r['date']} {r['time']} {r['title']}".strip() for r in items)


def append_list(message, data):
    if not data['related']:
        return message
    return message + '\n\nПОХОЖИЕ СОБЫТИЯ\n' + listing(data['related'])


def render_card(items, *, schedule=False):
    image = Image.new('RGB', (1080, 1350), '#F7F5EF')
    draw = ImageDraw.Draw(image)
    draw.ellipse((-260, -300, 510, 360), fill='#E9EDE1')
    draw.ellipse((800, -300, 1400, 330), fill='#F2E3D8')
    def font(size, bold=False):
        return ImageFont.truetype(str(ROOT / 'assets/fonts' / ('Cygre-Bold.ttf' if bold else 'Cygre-SemiBold.ttf')), size)
    tag = Image.open(ROOT / 'site/public/assets/ui/mobile-head-skinny-leather-3x.webp').convert('RGBA')
    tag.thumbnail((288, 202))
    image.paste(tag, (48, 0), tag)
    for y, line, size in [(45, 'Полюбить', 30), (83, 'Калининград', 30), (126, 'АНОНСЫ', 25)]:
        draw.text((73, y), line, font=font(size, True), fill='#FFF5E8')
    heading = 'Другие даты' if schedule else 'Похожие события'
    draw.text((48, 235), heading, font=font(64, True), fill='#24382D')
    y = 350
    for row in items:
        draw.rounded_rectangle((48, y, 1032, y + 144), 20, fill='#E9EDE1')
        draw.text((72, y + 16), f"{row['date']}  {row['time']}", font=font(32, True), fill='#315848')
        medallions = list(row.get('medallions', []))
        if row.get('is_free'):
            medallions = medallions[:1]
            medallions.append('site/public/assets/badges/free-listing-medallion.png')
        medallions = medallions[:2]
        for index, asset in enumerate(medallions):
            medal = Image.open(ROOT / asset).convert('RGBA')
            medal.thumbnail((112, 112))
            image.paste(medal, (1008 - (len(medallions) - index) * 120, y + 16), medal)
        label = row['place'] if schedule else row['title']
        width = 924 - len(medallions) * 120
        lines = textwrap.wrap(label, max(25, width // 18))[:2]
        for n, line in enumerate(lines):
            if n == 1 and len(label) > 94:
                line = line.rstrip(' .') + '…'
            draw.text((72, y + 48 + n * 30), line, font=font(29), fill='#24382D')
        if row.get('place') and not schedule:
            draw.text((72, y + 111), textwrap.shorten(row['place'], width=max(24, width // 15), placeholder='…'),
                      font=font(24), fill='#315848')
        y += 156
    if not schedule:
        draw.text((48, 1210), 'Ссылки на события', font=font(38, True), fill='#315848')
        draw.text((48, 1264), 'в комментарии ↓', font=font(38, True), fill='#315848')
    out = BytesIO()
    image.save(out, 'PNG')
    return out.getvalue()


async def cards(data, slots):
    # Never displace an original source image at VK's ten-attachment limit.
    result = []
    for key in ('dates', 'related'):
        if data[key] and len(result) < slots:
            result.append(await asyncio.to_thread(render_card, data[key], schedule=key == 'dates'))
    return result


async def enqueue_refresh(db):
    import main
    async with db.get_session() as session:
        events = (await session.execute(select(Event).where(
            Event.source_vk_post_url.is_not(None), Event.lifecycle_status == 'active',
            Event.silent.is_(False), Event.identity_status != 'merged',
        ))).scalars().all()
    owner = -abs(int(main.VK_EVENTS_GROUP_ID or main.VK_AFISHA_GROUP_ID))
    postponed = await main._fetch_vk_postponed_items_for_owner(
        owner, main._vk_wall_get_actors(owner), db, None)
    public = await main._vk_api('wall.get', {'owner_id': owner, 'filter': 'owner', 'count': 10},
        db, None, token=main.VK_SERVICE_TOKEN, token_kind='service', skip_captcha=True)
    # The owner's bounded backfill: postponed posts and up to ten recent public posts.
    target_ids = {int(row.get(key) or 0) for row in postponed + main._vk_postponed_items(public)
                  for key in ('id', 'postponed_id')}
    count = 0
    for event in events:
        if (not main._event_has_managed_vk_post(event)
                or main._event_has_ended_before_today(event)
                or main._event_start_has_passed_for_publication(event)):
            continue
        ids = main._vk_owner_and_post_id(event.source_vk_post_url)
        if int(ids[1]) not in target_ids:
            continue
        await main.enqueue_job(db, event.id, main.JobTask.vk_sync, requeue_done=True)
        count += 1
    logging.info('vk.relations_refresh queued=%s', count)


async def ensure_comment(db, event, post_url, data, bot=None):
    """One editable group comment; postponed posts use the existing delayed outbox."""
    import main
    owner, post = map(int, main._vk_owner_and_post_id(post_url))
    token = main._vk_user_token()
    async def api(method, **params):
        response = await main._vk_api(method, params, db, bot, token=token, token_kind='user')
        if 'response' not in response:
            raise RuntimeError(f'vk_relations_method_failed:{method}')
        return response['response']
    key = f'vk_related_comment:{owner}:{post}'
    async with db.get_session() as session:
        saved = await session.get(Setting, key)
        state = json.loads(saved.value) if saved else {}
    if not data['related']:
        if state.get('comment_id'):
            await api('wall.deleteComment', owner_id=owner, comment_id=state['comment_id'])
            async with db.get_session() as session:
                saved = await session.get(Setting, key)
                await session.delete(saved)
                await session.commit()
        return False
    response = await api('wall.getById', posts=f'{owner}_{post}')
    items = main._vk_wall_get_by_id_items({'response': response})
    if not items:
        raise RuntimeError('vk_relations_post_missing')
    if 'ПОХОЖИЕ СОБЫТИЯ\n' not in str(items[0].get('text') or ''):
        return False
    publish_date = int(items[0].get('date') or 0)
    if publish_date > datetime.now(timezone.utc).timestamp():
        await main.enqueue_job(db, event.id, main.JobTask.vk_calendar_comment,
            payload={'post_url': post_url, 'relations': True},
            coalesce_key=f'vk_related_comment:{owner}:{post}', requeue_done=True,
            next_run_at=datetime.fromtimestamp(publish_date + 60, timezone.utc))
        return False
    message = 'ПОХОЖИЕ СОБЫТИЯ\n\n' + '\n\n'.join(
        f"{listing([row])}\n{row['url']}" for row in data['related'])
    if state.get('text') == message:
        return False
    if not state.get('comment_id'):
        comments = await api('wall.getComments', owner_id=owner, post_id=post, count=100)
        existing = next((row for row in comments.get('items', [])
                         if row.get('from_id') == owner and row.get('text', '').startswith('ПОХОЖИЕ СОБЫТИЯ\n')), None)
        if existing:
            state['comment_id'] = existing['id']
    if state.get('comment_id'):
        await api('wall.editComment', owner_id=owner, comment_id=state['comment_id'], message=message)
    else:
        result = await api('wall.createComment', owner_id=owner, post_id=post, from_group=abs(owner),
                           message=message, guid=f'related:{owner}:{post}')
        state['comment_id'] = result['comment_id']
    state['text'] = message
    async with db.get_session() as session:
        saved = await session.get(Setting, key) or Setting(key=key, value='')
        saved.value = json.dumps(state, ensure_ascii=False)
        session.add(saved)
        await session.commit()
    logging.info('vk.relations_comment event_id=%s post_id=%s comment_id=%s', event.id, post, state['comment_id'])
    return True
