"""VK presentation of the shared semantic graph and canonical linked occurrences."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from io import BytesIO
import json
import logging
import random
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import select
from models import Event, Setting, JobOutbox, JobStatus

ROOT = Path(__file__).parent


def graph_path(db):
    return Path(db.path).with_name('static_site_event_related.json')


async def presentation(db, event):
    import main
    if not event.photo_urls:
        return {'dates': [], 'related': []}
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
    peer_ids = {row.id: main._vk_owner_and_post_id(row.source_vk_post_url or '')
                for row in rows.values() if row.id in similar_ids and row.id not in linked_ids}
    posts = ','.join('_'.join(ids) for ids in peer_ids.values() if ids)
    public_ids = set()
    if posts:
        response = await main._vk_api('wall.getById', {'posts': posts}, db, None,
            token=main.VK_SERVICE_TOKEN, token_kind='service', skip_captcha=True)
        public_ids = {(str(row['owner_id']), str(row['id']))
                      for row in main._vk_wall_get_by_id_items(response)
                      if row.get('post_type') == 'post' and not row.get('is_deleted')}
        logging.info('vk_related_public_peers event_id=%s candidates=%s visible=%s',
                     event.id, len(peer_ids), len(public_ids))
    related, seen_urls = [], {event.source_vk_post_url}
    for i in similar_ids:
        row = rows.get(i)
        if not row or peer_ids.get(i) not in public_ids or row.source_vk_post_url in seen_urls:
            continue
        seen_urls.add(row.source_vk_post_url)
        related.append(row)
        if len(related) == 5:
            break
    from tg_graphic_medallions import resolve_event_graphic_medallions
    def item(row):
        start, _, span_end = row.date.partition('..')
        date = main.parse_iso_date(start)
        end = main.parse_iso_date(span_end or (row.end_date if not row.end_date_is_inferred else '') or '')
        label = date.strftime('%d.%m') if date else row.date
        if date and end and end > date:
            label += '–' + end.strftime('%d.%m')
        return {'id': row.id, 'date': label, 'date_iso': start, 'end_date_iso': end.isoformat() if end else '',
                'time': '' if row.time_is_default else (row.time or ''), 'title': row.title,
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
    from guide_excursions.hook_cards import _strip_emoji
    from guide_excursions.visual_digest import PALETTES, _draw_date_tile, _wrap_by_width
    image = Image.new('RGBA', (1080, 1350), '#F7F5EF')
    draw = ImageDraw.Draw(image)
    palette = PALETTES[0]
    draw.ellipse((-260, -300, 510, 360), fill='#E9EDE1')
    draw.ellipse((800, -300, 1400, 330), fill='#F2E3D8')
    def font(size, bold=False):
        return ImageFont.truetype(str(ROOT / 'assets/fonts' / ('Cygre-Bold.ttf' if bold else 'Cygre-SemiBold.ttf')), size)
    tag = Image.open(ROOT / 'site/public/assets/ui/mobile-head-skinny-leather-3x.webp').convert('RGBA')
    tag.thumbnail((288, 202))
    image.alpha_composite(tag, (48, 0))
    # Mobile Reference4 lockup at 2.4x, including its original wide-o SVG raster.
    for y, line in [(80, 'ПОЛЮБИТЬ'), (100, 'КАЛИНИНГРАД')]:
        x = 80
        for char in line:
            draw.text((x, y), char, font=font(18, True), fill='#FFFAF4')
            x += draw.textlength(char, font=font(18, True)) + 1.35
    wordmark = Image.open(ROOT / 'site/public/brand/announcements-wordmark-ui-white.png').convert('RGBA')
    wordmark.thumbnail((230, 45))
    image.alpha_composite(wordmark, (82, 128))
    draw.text((48, 235), 'Другие даты' if schedule else 'Похожие события', font=font(64, True), fill=palette.text)
    for i, row in enumerate(items):
        accent = palette.accents[i % len(palette.accents)]
        layer = Image.new('RGBA', (996, 174))
        rd = ImageDraw.Draw(layer)
        rd.rounded_rectangle((0, 0, 995, 173), 30, fill=palette.card, outline=palette.card_stroke)
        rd.rounded_rectangle((0, 0, 10, 173), 5, fill=accent)
        date_iso = row.get('date_iso', '')
        _draw_date_tile(rd, {'date': date_iso, 'time': row['time']}, 174, accent, palette)
        medallions = list(row.get('medallions', []))
        if row.get('is_free'):
            medallions = medallions[:1] + ['site/public/assets/badges/free-listing-medallion.png']
        medallions = medallions[:2]
        for index, asset in enumerate(medallions):
            medal = Image.open(ROOT / asset).convert('RGBA')
            medal.thumbnail((100, 100))
            layer.alpha_composite(medal, (974 - (len(medallions) - index) * 108, 36))
        width = 738 - len(medallions) * 108
        title = _strip_emoji(row['title']).strip()
        for n, line in enumerate(_wrap_by_width(rd, title, font(32, True), width, 2)):
            rd.text((238, 24 + n * 34), line, font=font(32, True), fill=palette.text)
        details = row.get('place', '')
        if '–' in row['date']:
            details = row['date'] + (' · ' + details if details else '')
        for n, line in enumerate(_wrap_by_width(rd, details, font(23), width, 2)):
            rd.text((238, 102 + n * 26), line, font=font(23), fill=palette.route)
        height = 148 if len(items) > 5 else 174
        if height != 174:
            layer = layer.resize((996, height), Image.Resampling.LANCZOS)
        image.alpha_composite(layer, (42, 314 + i * (height + 12)))
    if not schedule:
        draw.text((58, 1290), 'Ссылки на события — в комментарии ↓', font=font(30, True), fill=palette.text)
    out = BytesIO()
    image.convert('RGB').save(out, 'PNG')
    return out.getvalue()


async def cards(data, slots):
    # Never displace an original source image at VK's ten-attachment limit.
    result = []
    for key in ('dates', 'related'):
        if data[key] and len(result) < slots:
            result.append(await asyncio.to_thread(render_card, data[key], schedule=key == 'dates'))
    return result


async def enqueue_refresh(db):
    """Wake initial announcements when their shared computation is ready."""
    import main
    path = graph_path(db)
    graph = json.loads(path.read_text())['related'] if path.is_file() else {}
    async with db.get_session() as session:
        events = (await session.execute(select(Event).where(
            Event.id.in_([int(i) for i in graph]), Event.vk_source_hash.is_(None),
            Event.lifecycle_status == 'active', Event.silent.is_(False),
            Event.identity_status != 'merged',
        ))).scalars().all()
    for event in events:
        if (not main._event_has_ended_before_today(event)
                and not main._event_start_has_passed_for_publication(event)):
            await main.enqueue_job(db, event.id, main.JobTask.vk_sync, requeue_done=True)
    logging.info('vk.relations_initial_ready candidates=%s', len(events))


async def ensure_comment(db, event, post_url, data, bot=None, *, deferred=False):
    """Create one group comment; existing comments are never edited or deleted."""
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
    message = 'ПОХОЖИЕ СОБЫТИЯ\n\n' + '\n\n'.join(
        f"{row['date']} {row['time']}".strip() + f" [{row['url']}|{row['title']}]" for row in data['related']) if data['related'] else ''
    if state.get('comment_id') or not message:
        return False
    response = await api('wall.getById', posts=f'{owner}_{post}')
    items = main._vk_wall_get_by_id_items({'response': response})
    if not items:
        raise RuntimeError('vk_relations_post_missing')
    if message and 'ПОХОЖИЕ СОБЫТИЯ\n' not in str(items[0].get('text') or ''):
        return False
    publish_date = int(items[0].get('date') or 0)
    now = datetime.now(timezone.utc).timestamp()
    if not deferred or publish_date + 180 > now:
        if not deferred:
            async with db.get_session() as session:
                pending = (await session.execute(select(JobOutbox).where(
                    JobOutbox.coalesce_key == key,
                    JobOutbox.status.in_([JobStatus.pending, JobStatus.running]),
                ))).first()
            if pending:
                return False
        await main.enqueue_job(db, event.id, main.JobTask.vk_calendar_comment,
            payload={'post_url': post_url, 'relations': True},
            coalesce_key=key, requeue_done=True,
            next_run_at=datetime.fromtimestamp(max(publish_date, now) + random.randint(180, 1200), timezone.utc))
        return False
    comments = await api('wall.getComments', owner_id=owner, post_id=post, count=100)
    existing = next((row for row in comments.get('items', [])
                     if row.get('from_id') == owner and row.get('text', '').startswith('ПОХОЖИЕ СОБЫТИЯ\n')), None)
    if existing:
        state['comment_id'] = existing['id']
        message = existing['text']
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
    return not existing
