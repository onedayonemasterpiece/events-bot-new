import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { createDemoCards, createGoogleCalendarUrl, createIcs } from '../src/lib/pwaCapabilitiesLab.js';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('PWA capabilities lab keeps the promised isolated browser contract', async () => {
  const event = {
    title: 'Тест, календарь; lab',
    description: 'Строка 1\nСтрока 2',
    location: 'Калининград',
    url: 'https://kenigevents.ru/lab/pwa-capabilities/',
    timezone: 'Europe/Kaliningrad',
    start: new Date('2026-08-03T10:00:00Z'),
    end: new Date('2026-08-03T11:30:00Z'),
  };
  const ics = createIcs(event, 'stable@example.test', new Date('2026-08-02T10:00:00Z'));
  assert.match(ics, /\r\n/u);
  assert.match(ics, /UID:stable@example\.test/u);
  assert.match(ics, /SUMMARY:Тест\\, календарь\\; lab/u);
  assert.match(ics, /DESCRIPTION:Строка 1\\nСтрока 2/u);
  assert.equal(createDemoCards().length, 30);

  const calendar = new URL(createGoogleCalendarUrl(event));
  assert.equal(calendar.hostname, 'calendar.google.com');
  assert.equal(calendar.searchParams.get('dates'), '20260803T100000Z/20260803T113000Z');
  assert.equal(calendar.searchParams.get('ctz'), 'Europe/Kaliningrad');

  const [page, worker] = await Promise.all([
    read('src/pages/lab/pwa-capabilities/index.astro'),
    read('src/pages/lab/pwa-capabilities/sw.js.ts'),
  ]);
  assert.match(page, /noindex,nofollow,noarchive/u);
  assert.match(worker, /Service-Worker-Allowed': '\/lab\/pwa-capabilities\//u);
  assert.doesNotMatch(worker, /Service-Worker-Allowed': '\/'[,;]/u);
});
