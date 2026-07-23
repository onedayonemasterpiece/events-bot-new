import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('free one-day events use one actionable calendar primary', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const examples = JSON.parse(await read('src/data/desktop-event-examples.json'));
  const freeFixture = examples.events.find((event) => event.id === 6901);
  assert.ok(freeFixture, 'event 6901 must remain frozen for CTA acceptance');

  const freeWithSource = {
    ...freeFixture,
    id: 6959,
    ticket: { ...freeFixture.ticket, href: 'https://actop.us/performances/zvyozdy-v-chyornoy-dyre' },
  };
  for (const event of [freeFixture, freeWithSource]) {
    assert.equal(event.ticket.kind, 'free');
    assert.ok(!event.end_date || event.end_date === event.start_date);
  }
  assert.equal(freeFixture.ticket.href, null);
  assert.match(freeWithSource.ticket.href, /^https:\/\//u);

  assert.match(panel, /const calendarPrimary = !soldOut\s*&& event\.ticket\.kind === 'free'\s*&& isCalendarEligible\(event\)/u);
  assert.doesNotMatch(panel, /event\.ticket\.kind === 'free'\s*&& !ctaHref/u);
  assert.match(panel, /calendarPrimary \? \(\s*<CalendarLink event=\{event\} className="desktop-prototype__primary-action" \/>/u);
  assert.match(panel, /\{!calendarPrimary && <CalendarLink event=\{event\} className="desktop-prototype__icon-action" compact \/>\}/u);
});

test('Split CTA is the tactile stacked ticket card rather than the legacy inline bar', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const page = await read('src/components/DesktopEventPage.astro');
  assert.match(panel, /data-action-layout="stacked"/u);
  assert.match(panel, /data-action-treatment=\{family === 'split' \? 'ticket-card' : 'editorial'\}/u);
  assert.match(panel, /data-feedback-scope[\s\S]*data-event-id=\{event\.id\}[\s\S]*data-event-title=\{event\.title\}/u);
  assert.match(panel, /data-action-treatment="ticket-card"[^}]*width: min\(100%, 340px\)/su);
  assert.match(panel, /data-action-treatment="ticket-card"[^}]*grid-template-columns: minmax\(0, 1fr\) !important/su);
  assert.match(panel, /data-action-treatment="ticket-card"[^}]*linear-gradient\(180deg, #ca5a2c 0%, #a8431d 100%\)/su);
  assert.match(panel, /data-action-treatment="ticket-card"[^}]*grid-template-columns: repeat\(auto-fit, minmax\(0, 1fr\)\)/su);
  assert.match(panel, /desktop-prototype__primary-action[^}]*white-space: nowrap/su);
  assert.match(page, /className="desktop-prototype__action--ticket-card" family="split"/u);
  assert.doesNotMatch(page, /const measureInlineActionPanel/u);
  assert.match(page, /panel\.dataset\.actionFit = panel\.dataset\.actionFamily === 'split' \? 'ticket-card' : 'stacked'/u);
});

test('registration and free-calendar semantic cases are frozen as Split specimens', async () => {
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');
  const browserGate = await read('scripts/check-desktop-cta-geometry-playwright.sh');
  const examples = JSON.parse(await read('src/data/desktop-event-examples.json'));
  const registration = examples.events.find((event) => event.id === 6811);
  const free = examples.events.find((event) => event.id === 6901);

  assert.equal(registration?.ticket.kind, 'registration');
  assert.equal(registration?.ticket.label, 'Зарегистрироваться');
  assert.equal(free?.ticket.kind, 'free');
  assert.equal(free?.ticket.href, null);
  assert.match(lab, /slug: 'cta-registration-invariant', eventId: 6811, candidate: 'split'/u);
  assert.match(lab, /slug: 'cta-free-calendar-invariant', eventId: 6901, candidate: 'split'/u);
  assert.match(browserGate, /cta-registration-invariant" split "Зарегистрироваться" 3/u);
  assert.match(browserGate, /cta-free-calendar-invariant" split "В календарь" 2 calendar-primary/u);
});

test('Editorial hierarchy and production role-first media boundaries stay intact', async () => {
  const page = await read('src/components/DesktopEventPage.astro');
  const presentation = await read('src/lib/desktopEventPresentation.ts');

  assert.match(page, /family="editorial"/u);
  assert.match(page, /family="split"/u);
  assert.match(presentation, /isTechnicallyStrongEventMedia/u);
  assert.match(presentation, /media_semantic_status === 'classified'/u);
  assert.match(presentation, /media_role === 'event_photo'/u);
});
