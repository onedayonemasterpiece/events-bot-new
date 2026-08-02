import assert from 'node:assert/strict';
import test from 'node:test';

import {
  deriveDeploymentBase,
  extractEventLinks,
  parseFirstVevent,
  parseIcsDate,
  selectCurrentCandidate,
  unfoldIcs,
} from '../../e2e/event-reminders/resolve-current-event.mjs';

test('preview deployment base and event links remain inside the exact prefix', () => {
  const target = new URL('https://kenigevents.ru/preview-calendar-deadbeef/zavtra/');
  const deployment = deriveDeploymentBase(target);
  assert.equal(deployment.basePath, '/preview-calendar-deadbeef/');
  assert.equal(deployment.baseUrl.href, 'https://kenigevents.ru/preview-calendar-deadbeef/');

  const html = `
    <a href="/preview-calendar-deadbeef/sobytiya/a/">A</a>
    <a href="../sobytiya/b/">B</a>
    <a href="https://evil.test/preview-calendar-deadbeef/sobytiya/c/">C</a>
    <a href="/sobytiya/root-leak/">Root</a>
  `;
  assert.deepEqual(extractEventLinks(html, target, deployment.basePath), [
    'https://kenigevents.ru/preview-calendar-deadbeef/sobytiya/a/',
    'https://kenigevents.ru/preview-calendar-deadbeef/sobytiya/b/',
  ]);
});

test('ICS unfolding and parsing preserves event fields and Kaliningrad time', () => {
  const ics = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'BEGIN:VEVENT',
    'UID:event-42@kenigevents.ru',
    'DTSTART;TZID=Europe/Kaliningrad:20260815T170000',
    'DTEND;TZID=Europe/Kaliningrad:20260815T190000',
    'SUMMARY:Лабораторное событие',
    'LOCATION:Калининград\\, Остров Канта',
    'DESCRIPTION:Строка 1\\nСтрока 2 и длинное ',
    ' продолжение',
    'URL:https://kenigevents.ru/sobytiya/lab/',
    'STATUS:CONFIRMED',
    'END:VEVENT',
    'END:VCALENDAR',
  ].join('\r\n');
  assert.ok(unfoldIcs(ics).includes('DESCRIPTION:Строка 1\\nСтрока 2 и длинное продолжение'));
  const event = parseFirstVevent(ics);
  assert.equal(event.uid, 'event-42@kenigevents.ru');
  assert.equal(event.summary, 'Лабораторное событие');
  assert.equal(event.location, 'Калининград, Остров Канта');
  assert.equal(event.description, 'Строка 1\nСтрока 2 и длинное продолжение');
  assert.equal(event.startsAt.toISOString(), '2026-08-15T15:00:00.000Z');
  assert.equal(event.endsAt.toISOString(), '2026-08-15T17:00:00.000Z');
  assert.equal(event.allDay, false);
});

test('date parser distinguishes UTC, explicit offset and all-day values', () => {
  assert.equal(parseIcsDate('20260815T150000Z').date.toISOString(), '2026-08-15T15:00:00.000Z');
  assert.equal(parseIcsDate('20260815T170000+0200').date.toISOString(), '2026-08-15T15:00:00.000Z');
  assert.equal(parseIcsDate('20260815').allDay, true);
  assert.equal(parseIcsDate('20260815T170000', {}), null);
});

test('selector chooses the earliest complete future timed event and rejects cancelled/all-day', () => {
  const now = new Date('2026-08-02T10:00:00Z');
  const candidate = (overrides = {}) => ({
    uid: 'uid',
    summary: 'Title',
    location: 'Place',
    startsAt: new Date('2026-08-02T14:00:00Z'),
    endsAt: new Date('2026-08-02T15:00:00Z'),
    allDay: false,
    status: 'CONFIRMED',
    eventUrl: 'https://kenigevents.ru/preview/sobytiya/a/',
    ...overrides,
  });
  const chosen = selectCurrentCandidate([
    candidate({ eventUrl: 'https://kenigevents.ru/preview/sobytiya/late/', startsAt: new Date('2026-08-03T12:00:00Z'), endsAt: new Date('2026-08-03T13:00:00Z') }),
    candidate({ eventUrl: 'https://kenigevents.ru/preview/sobytiya/cancelled/', status: 'CANCELLED' }),
    candidate({ eventUrl: 'https://kenigevents.ru/preview/sobytiya/all-day/', allDay: true }),
    candidate({ eventUrl: 'https://kenigevents.ru/preview/sobytiya/earliest/' }),
  ], now, { minLeadMinutes: 90, maxLeadDays: 30 });
  assert.equal(chosen.eventUrl, 'https://kenigevents.ru/preview/sobytiya/earliest/');
});
