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

test('Split CTA collapses utilities, remeasures, and only then stacks', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const page = await read('src/components/DesktopEventPage.astro');
  const fit = page.slice(page.indexOf('const fitActionPanel'));

  const comfortable = fit.indexOf("panel.dataset.actionDensity = 'comfortable'");
  const firstMeasure = fit.indexOf('if (measureInlineActionPanel(panel))', comfortable);
  const compact = fit.indexOf("panel.dataset.actionDensity = 'compact'", firstMeasure);
  const secondMeasure = fit.indexOf('if (measureInlineActionPanel(panel))', compact);
  const stacked = fit.indexOf("panel.dataset.actionLayout = 'stacked'", secondMeasure);
  assert.ok(comfortable >= 0 && comfortable < firstMeasure);
  assert.ok(firstMeasure < compact && compact < secondMeasure && secondMeasure < stacked);

  assert.match(page, /primaryLabelDoesNotFit/u);
  assert.match(page, /const wrapped = labelRect\.height > lineHeight \* 1\.35/u);
  assert.match(page, /primaryLabel\.scrollWidth > primaryLabel\.clientWidth \+ 1/u);
  assert.match(page, /panel\.dataset\.actionFit = 'icons'/u);
  assert.match(panel, /data-action-density="compact"[^}]*\[data-calendar-label\][^}]*width:0; max-width:0; opacity:0/su);
  assert.match(panel, /desktop-prototype__primary-action[^}]*white-space: nowrap/su);
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
  assert.match(browserGate, /cta-registration-invariant" split "Зарегистрироваться" 3 icons/u);
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
