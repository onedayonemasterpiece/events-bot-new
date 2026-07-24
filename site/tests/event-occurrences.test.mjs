import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  collapseOccurrenceCards,
  formatOccurrencePresentation,
  isPopularEligible,
  resolveOccurrenceFamily,
} from '../src/lib/eventOccurrences.ts';

function event(id, date, time, linked = [], extra = {}) {
  return {
    id,
    title: extra.title || 'Одна программа',
    slug: `event-${id}`,
    event_type: 'спектакль',
    venue_name: 'Один театр',
    city: 'Калининград',
    start_date: date,
    end_date: date,
    start_time: time,
    display_time: time,
    lifecycle_status: 'active',
    other_date_ids: linked,
    ...extra,
  };
}

function presentation(current, catalog, currentDate = '2026-07-21') {
  const family = resolveOccurrenceFamily(current, catalog, { currentDate });
  return formatOccurrencePresentation(family, currentDate);
}

test('same time across dates uses one compact month label and accessible conjunction', () => {
  const first = event(1, '2026-11-02', '19:00', [2]);
  const second = event(2, '2026-11-09', '19:00', [1]);
  const result = presentation(first, [first, second]);
  assert.equal(result.compactLabel, '2, 9 ноября 19:00');
  assert.equal(result.railDateLine, '2, 9 ноя');
  assert.equal(result.railTimeLine, '19:00');
  assert.equal(result.ariaLabel, '2 и 9 ноября в 19:00');
  assert.equal(result.isComplexSchedule, false);
});

test('same date with several times uses one date and comma-separated times', () => {
  const early = event(3, '2026-11-04', '17:00', [4]);
  const late = event(4, '2026-11-04', '19:00', [3]);
  const result = presentation(early, [early, late]);
  assert.equal(result.compactLabel, '4 ноября 17:00, 19:00');
  assert.equal(result.railDateLine, '4 ноя');
  assert.equal(result.railTimeLine, '17:00, 19:00');
  assert.equal(result.ariaLabel, '4 ноября в 17:00 и 19:00');
});

test('same time across months remains compact without hiding month boundaries', () => {
  const november = event(5, '2026-11-30', '19:00', [6]);
  const december = event(6, '2026-12-02', '19:00', [5]);
  const result = presentation(november, [november, december]);
  assert.equal(result.compactLabel, '30 ноября, 2 декабря 19:00');
  assert.equal(result.railDateLine, '30 ноя, 2 дек');
});

test('same time groups every stable year-month and joins groups accessibly', () => {
  const julyFirst = event(50, '2026-07-24', '19:00', [51, 52]);
  const julySecond = event(51, '2026-07-25', '19:00', [50, 52]);
  const september = event(52, '2026-09-27', '19:00', [50, 51]);
  const result = presentation(julyFirst, [julyFirst, julySecond, september], '2026-07-23');
  assert.equal(result.compactLabel, '24, 25 июля, 27 сентября 19:00');
  assert.equal(result.railDateLine, '24, 25 июл, 27 сен');
  assert.equal(result.railTimeLine, '19:00');
  assert.equal(result.ariaLabel, '24 и 25 июля и 27 сентября в 19:00');
});

test('Popular eligibility rejects non-public lifecycle states', () => {
  const reference = { currentDate: '2026-07-24', referenceIso: '2026-07-24T12:00:00+02:00' };
  for (const lifecycle_status of ['cancelled', 'postponed', 'duplicate', 'merged', 'deleted', 'inactive']) {
    assert.equal(isPopularEligible(event(60, '2026-07-25', '19:00', [], {
      lifecycle_status,
      timezone: 'Europe/Kaliningrad',
      starts_at: '2026-07-25T19:00:00+02:00',
    }), reference), false, lifecycle_status);
  }
});

test('Popular eligibility keeps ranges through end_date before considering end_at', () => {
  const reference = { currentDate: '2026-07-24', referenceIso: '2026-07-24T12:00:00+02:00' };
  const continuing = event(61, '2026-07-20', '10:00', [], {
    end_date: '2026-07-24',
    end_at: '2026-07-20T18:00:00+02:00',
    starts_at: '2026-07-20T10:00:00+02:00',
    timezone: 'Europe/Kaliningrad',
  });
  const ended = event(62, '2026-07-20', '10:00', [], {
    end_date: '2026-07-23',
    end_at: '2026-07-20T18:00:00+02:00',
    starts_at: '2026-07-20T10:00:00+02:00',
    timezone: 'Europe/Kaliningrad',
  });
  assert.equal(isPopularEligible(continuing, reference), true);
  assert.equal(isPopularEligible(ended, reference), false);
});

test('Popular eligibility accepts future one-offs and gates same-day events by start instant', () => {
  const reference = { currentDate: '2026-07-24', referenceIso: '2026-07-24T12:00:30+02:00' };
  const future = event(63, '2026-07-25', '09:00', [], {
    starts_at: '2026-07-24T08:00:00+02:00',
    timezone: 'Europe/Kaliningrad',
  });
  const elapsed = event(64, '2026-07-24', '11:59', [], {
    starts_at: '2026-07-24T11:59:00+02:00',
    timezone: 'Europe/Kaliningrad',
  });
  const upcoming = event(65, '2026-07-24', '12:01', [], {
    starts_at: null,
    timezone: 'Europe/Kaliningrad',
  });
  const unknownTime = event(66, '2026-07-24', null, [], {
    starts_at: null,
    timezone: 'Europe/Kaliningrad',
  });
  assert.equal(isPopularEligible(future, reference), true);
  assert.equal(isPopularEligible(elapsed, reference), false);
  assert.equal(isPopularEligible(upcoming, reference), true);
  assert.equal(isPopularEligible(unknownTime, reference), false);
});

test('mixed date/time matrix falls back to explicit semicolon groups', () => {
  const first = event(7, '2026-11-02', '19:00', [8, 9]);
  const second = event(8, '2026-11-03', '17:00', [7, 9]);
  const third = event(9, '2026-11-03', '20:00', [7, 8]);
  const result = presentation(first, [first, second, third]);
  assert.equal(result.compactLabel, '2 ноября 19:00; 3 ноября 17:00, 20:00');
  assert.equal(result.railDateLine, '2, 3 ноя');
  assert.equal(result.railTimeLine, 'разное время');
  assert.equal(result.isComplexSchedule, true);
});

test('matching copy without explicit reciprocal links never creates a family', () => {
  const first = event(10, '2026-11-02', '19:00');
  const lookalike = event(11, '2026-11-09', '19:00');
  const result = presentation(first, [first, lookalike]);
  assert.deepEqual(result.family.memberIds, [10]);
  assert.equal(result.family.hasAlternatives, false);
  assert.equal(result.compactLabel, '2 ноября 19:00');
});

test('explicit cross-venue Romeo family collapses to the required 6408 card label', () => {
  const second = event(6318, '2026-11-02', '19:00', [6586], {
    title:'Спектакль «Ромео и Джульетта»',
    venue_name:'Драматический театр',
  });
  const third = event(6586, '2026-11-03', '19:00', [6318], {
    title:'Спектакль «Ромео и Джульетта»',
    venue_name:'Музыкальный театр',
  });
  const result = presentation(third, [second, third]);
  assert.deepEqual(collapseOccurrenceCards([third, second], 'per-family').map((item) => item.id), [6586]);
  assert.deepEqual(result.family.memberIds, [6318, 6586]);
  assert.equal(result.compactLabel, '2, 3 ноября 19:00');
  assert.equal(result.ariaLabel, '2 и 3 ноября в 19:00');
});

test('resolver rejects asymmetric, dangling, inactive, past and range members', () => {
  const current = event(20, '2026-11-02', '19:00', [21, 22, 23, 24, 25]);
  const asymmetric = event(21, '2026-11-03', '19:00', []);
  const inactive = event(23, '2026-11-04', '19:00', [20], { lifecycle_status: 'cancelled' });
  const past = event(24, '2026-07-20', '19:00', [20]);
  const range = event(25, '2026-11-05', '19:00', [20], { end_date: '2026-11-06' });
  const family = resolveOccurrenceFamily(current, [current, asymmetric, inactive, past, range], { currentDate: '2026-07-21' });
  assert.deepEqual(family.memberIds, [20]);
  assert.deepEqual(new Set(family.issues.map((issue) => issue.code)), new Set([
    'asymmetric_link', 'dangling_link', 'inactive_member', 'past_member', 'range_member',
  ]));
});

test('duplicate exact slots stay safe in UI and remain visible as an issue', () => {
  const current = event(30, '2026-11-04', '17:00', [31]);
  const duplicate = event(31, '2026-11-04', '17:00', [30]);
  const family = resolveOccurrenceFamily(current, [current, duplicate], { currentDate: '2026-07-21' });
  assert.deepEqual(family.memberIds, [30]);
  assert.equal(family.issues.some((issue) => issue.code === 'duplicate_slot'), true);
});

test('card collapse policy is per-date for temporal lists and per-family for entity lists', () => {
  const first = event(40, '2026-11-02', '17:00', [41, 42]);
  const secondTime = event(41, '2026-11-02', '19:00', [40, 42]);
  const nextDate = event(42, '2026-11-09', '19:00', [40, 41]);
  const input = [first, secondTime, nextDate];
  assert.deepEqual(collapseOccurrenceCards(input, 'none').map((item) => item.id), [40, 41, 42]);
  assert.deepEqual(collapseOccurrenceCards(input, 'per-date').map((item) => item.id), [40, 42]);
  assert.deepEqual(collapseOccurrenceCards(input, 'per-family').map((item) => item.id), [40]);
});

test('production surfaces and live search producer wire per-date, per-family and selector policies explicitly', async () => {
  const [eventsSource, searchSource, edgeSearchSource, edgeFamilySource, vectorSyncSource, layoutSource, detailSource, listItemSource] = await Promise.all([
    readFile(new URL('../src/lib/events.ts', import.meta.url), 'utf8'),
    readFile(new URL('../src/components/AuthorizedEventSearch.astro', import.meta.url), 'utf8'),
    readFile(new URL('../../supabase/functions/event-search/index.ts', import.meta.url), 'utf8'),
    readFile(new URL('../../supabase/functions/event-search/occurrence-families.ts', import.meta.url), 'utf8'),
    readFile(new URL('../../scripts/sync_event_search_vectors_to_supabase.py', import.meta.url), 'utf8'),
    readFile(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/pages/sobytiya/[slug].astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/components/EventListItem.astro', import.meta.url), 'utf8'),
  ]);
  assert.match(eventsSource, /collapseLinkedSessionEvents[\s\S]*collapseOccurrenceCards\(events, 'per-date'\)/u);
  assert.match(eventsSource, /getPopularEvents[\s\S]*?filter\(\(event\) => isPopularEligible\(event, eligibilityReference\)\)[\s\S]*?collapseOccurrenceCards\(ranked, 'per-family'\)/u);
  assert.match(eventsSource, /getPopularDesktopEvents[\s\S]*?filter\(\(event\) => isPopularEligible\(event, eligibilityReference\)\)/u);
  assert.match(eventsSource, /getStaticRelatedCandidates[\s\S]*collapseOccurrenceCards\([\s\S]*'per-family'/u);
  assert.match(searchSource, /collapseSearchOccurrenceFamilies\(rawItems, seenFamilies\)/u);
  assert.match(edgeSearchSource, /paginateOccurrenceFamilies\(\s*rankedCandidates,/u);
  assert.match(edgeSearchSource, /collapseOccurrenceFamilies\(\s*llmResult\.used/u);
  assert.match(edgeFamilySource, /memberIds\.includes\(eventId\)/u);
  assert.match(vectorSyncSource, /build_occurrence_projections[\s\S]*reciprocal explicit occurrence families/u);
  assert.match(layoutSource, /collapseRankedOccurrenceFamilies\(rankedBeforeOccurrenceCollapse\)/u);
  assert.match(detailSource, /occurrencePresentation=\{occurrencePresentation\}/u);
  assert.doesNotMatch(detailSource, /mobile-event-production__other-dates/u);
  assert.match(listItemSource, /variant="rail-date-first"/u);
});
