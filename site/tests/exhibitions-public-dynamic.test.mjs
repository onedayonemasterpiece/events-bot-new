import { Script } from 'node:vm';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  isExhibitionProjectionCandidate,
  normalizeExhibitionTitle,
  projectExhibitionsPersonal,
} from '../src/lib/exhibitionsPersonal.ts';

function event(id, title, startDate, endDate, extra = {}) {
  return {
    id,
    title,
    slug: `exhibition-${id}`,
    event_type: 'выставка',
    festival: null,
    status_label: '',
    lifecycle_status: 'active',
    starts_at: `${startDate}T12:00:00+02:00`,
    start_date: startDate,
    start_time: null,
    end_date: endDate,
    end_at: null,
    time_range_end: null,
    timezone: 'Europe/Kaliningrad',
    display_date: '',
    display_time: null,
    city: 'Калининград',
    venue_name: 'Музей',
    address: null,
    map_query: null,
    ticket: { kind: 'status', label: '', href: null, status: null, is_free: false, price_label: null },
    age_restriction: null,
    age_restriction_status: 'unknown',
    age_restriction_provenance: null,
    age_restriction_decision_version: null,
    age_recommendation: null,
    age_recommendation_label: null,
    source_url: null,
    telegraph_url: null,
    image_url: null,
    image_alt: '',
    image_text_mode: 'unknown',
    summary: '',
    meta_description: '',
    description_html: '',
    topics: ['EXHIBITIONS'],
    likes_count: 0,
    source_likes_count: 0,
    service_likes_count: 0,
    shares_count: 0,
    popularity_reason_codes: [],
    pushkin_card: false,
    other_date_ids: [],
    source_prod_id: id,
    data_quality_notes: [],
    updated_at: null,
    ...extra,
  };
}

test('projection derives lifecycle buckets and Russian copy from the supplied as-of date', () => {
  const projection = projectExhibitionsPersonal([
    event(101, 'Открытие сегодня', '2031-02-10', '2031-03-10'),
    event(102, 'Скоро откроется', '2031-02-15', '2031-03-15'),
    event(103, 'Закрывается', '2030-12-01', '2031-02-12'),
    event(104, 'Публичный интерес', '2030-12-01', '2031-06-01', {
      shares_count: 7,
      popularity_reason_codes: ['frequently_shared'],
    }),
    event(105, 'Старая экспозиция', '2029-01-01', '2032-01-01'),
    event(106, 'Анонс на будущее', '2031-04-10', '2031-05-10'),
  ], '2031-02-10');

  assert.deepEqual(projection.newItems.map((item) => item.id), [102, 101]);
  assert.equal(projection.newItems.find((item) => item.id === 101)?.status, 'Открылась сегодня');
  assert.equal(projection.newItems.find((item) => item.id === 102)?.status, 'Откроется через 5 дней');
  assert.equal(projection.priorityItems.find((item) => item.id === 103)?.lifecycle, 'ending');
  assert.equal(projection.priorityItems.find((item) => item.id === 103)?.status, 'Закроется через 2 дня');
  assert.equal(projection.priorityItems.find((item) => item.id === 103)?.dateLabel, 'до 12 февраля');
  assert.equal(projection.priorityItems.find((item) => item.id === 104)?.lifecycle, 'popular');
  assert.equal(projection.priorityItems.find((item) => item.id === 106)?.lifecycle, 'upcoming');
  assert.equal(projection.priorityItems.find((item) => item.id === 106)?.reasons[0], 'Откроется позже');
  assert.deepEqual(projection.tailItems.map((item) => item.id), [105]);
});

test('projection fails closed on invalid/inactive/non-exhibition rows and removes exact public duplicates', () => {
  const projection = projectExhibitionsPersonal([
    event(201, 'Выставка «Линия»', '2031-01-01', '2031-05-01'),
    event(202, 'Линия', '2031-01-02', '2031-05-02'),
    event(201, 'Повтор идентификатора', '2031-01-03', '2031-05-03'),
    event(203, 'Отменённая', '2031-01-01', '2031-05-01', { lifecycle_status: 'cancelled' }),
    event(204, 'Сломанная дата', 'not-a-date', '2031-05-01'),
    event(205, 'Уже закончилась', '2030-01-01', '2031-02-09'),
    event(206, 'Концерт', '2031-02-10', '2031-02-10', { event_type: 'концерт', topics: ['CONCERTS'] }),
  ], '2031-02-10');

  assert.deepEqual(projection.items.map((item) => item.id), [201]);
  assert.deepEqual(projection.suppressed, {
    invalid: 2,
    inactive: 1,
    nonExhibition: 1,
    duplicateId: 1,
    duplicateTitle: 1,
  });
  assert.equal(normalizeExhibitionTitle('Выставка «Линия»'), normalizeExhibitionTitle('Линия'));
});

test('current committed preview projects only current real rows with unique ids and titles', async () => {
  const preview = JSON.parse(await readFile(new URL('../src/data/preview-events.json', import.meta.url), 'utf8'));
  const currentDate = preview.build.current_date;
  const ongoing = preview.events.filter((candidate) => {
    const endDate = candidate.end_date || candidate.start_date;
    return candidate.start_date <= '9999-12-31'
      && endDate >= currentDate
      && isExhibitionProjectionCandidate(candidate);
  });
  const projection = projectExhibitionsPersonal(ongoing, currentDate);
  const previewIds = new Set(preview.events.map((candidate) => candidate.id));
  const projectedIds = projection.items.map((item) => item.id);
  const titleKeys = projection.items.map((item) => normalizeExhibitionTitle(item.event.title));

  assert.ok(projection.items.length > 0, 'fresh preview must provide at least one current exhibition');
  assert.ok(projection.newItems.length > 0, 'current preview should exercise the new-inbox presentation');
  assert.ok(projection.priorityItems.length > 0, 'current preview should exercise priority ranking');
  assert.ok(projection.tailItems.length > 0, 'current preview should exercise progressive tail disclosure');
  assert.ok(projectedIds.every((id) => previewIds.has(id)), 'projection may not resurrect donor-only ids');
  assert.equal(new Set(projectedIds).size, projectedIds.length);
  assert.equal(new Set(titleKeys).size, titleKeys.length);
});

test('public route keeps the donor CSS/interaction contract while replacing fixture specs with the public read path', async () => {
  const [publicSource, surfaceSource, eventsSource] = await Promise.all([
    readFile(new URL('../src/pages/vystavki/index.astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/components/ExhibitionsPersonalSurface.astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/lib/events.ts', import.meta.url), 'utf8'),
  ]);
  assert.match(surfaceSource, /data-gallery\s+data-app-lower-surface="media"/u);
  assert.match(surfaceSource, /data-exhibitions-theme="graphite"/u);
  assert.match(surfaceSource, /new URLSearchParams\(window\.location\.search\).*theme.*light/u);
  assert.match(surfaceSource, /toastHost\.KenigEventsToast\?\.show\) toastHost\.KenigEventsToast\.show\(detail\)/u);
  assert.match(surfaceSource, /action:undo \? \{ label:'Отменить', callback:undo \} : null/u);
  assert.doesNotMatch(surfaceSource, /data-live-message|data-live-undo|liveUndoAction|liveTimer/u);
  assert.match(surfaceSource, /data-deck-live role="status" aria-live="polite" aria-atomic="true"/u);
  for (const invariant of ['data-mode-switch', 'data-category-filter', 'data-gallery-prev', 'data-gallery-next', 'data-mark-new-seen', 'data-row-focus', 'data-exhibition-row']) {
    assert.match(surfaceSource, new RegExp(invariant, 'u'));
  }
  assert.match(publicSource, /projectExhibitionsPersonal\(getOngoingExhibitionEvents\(\), currentDate\)/u);
  assert.match(publicSource, /ExhibitionsPersonalSurface/u);
  assert.match(surfaceSource, /ExhibitionPrototypeRow/u);
  assert.match(publicSource, /headerCurrent="exhibitions"/u);
  assert.doesNotMatch(publicSource, /featuredSpecs|curatedTailSpecs|canonicalUrl = absoluteUrl\('\/lab\//u);
  assert.match(eventsSource, /getOngoingExhibitionEvents[\s\S]*collapseLinkedSessionEvents\(/u);
});


test('product inline runtime is executable JavaScript, not untranspiled TypeScript', async () => {
  const source = await readFile(new URL('../src/components/ExhibitionsPersonalSurface.astro', import.meta.url), 'utf8');
  const scripts = [...source.matchAll(/<script\s+is:inline[^>]*>([\s\S]*?)<\/script>/gu)];
  assert.ok(scripts.length > 0);
  for (const [, script] of scripts) assert.doesNotThrow(() => new Script(script));
});
