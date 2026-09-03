import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(relativePath, import.meta.url), 'utf8');
const readBytes = (relativePath) => readFile(new URL(relativePath, import.meta.url));

test('clubs data is a fresh policy-current SQLite projection, not a donor fixture', async () => {
  const projection = JSON.parse(await read('../src/data/interest-clubs.json'));

  assert.equal(projection.schema_version, 'interest-clubs-static-v1');
  assert.equal(projection.projection_version, 1);
  assert.equal(projection.source, 'sqlite-interest-clubs-v1');
  assert.equal(projection.current_date, '2026-07-23');
  assert.deepEqual(
    projection.clubs.map((club) => club.slug),
    ['game-vibes', 'neural-researchers', 'technology-researchers'],
  );
  assert.ok(projection.clubs.every((club) => club.activity.distinct_date_count >= 2));
  assert.ok(projection.clubs.every((club) => (
    Date.parse(`${projection.current_date}T00:00:00Z`)
      - Date.parse(`${club.activity.last_observed_date}T00:00:00Z`)
  ) <= 90 * 24 * 60 * 60 * 1000));
  assert.ok(!projection.clubs.some((club) => (
    ['review-fixture-v1', 'disabled-by-build-gate'].includes(projection.source)
    || ['s-toboi-ok', 'autoretroclub', 'cinemango'].includes(club.slug)
  )));
});

test('future-meeting actions fail closed unless their exact event is materialized inside the prefix', async () => {
  const projection = JSON.parse(await read('../src/data/interest-clubs.json'));
  const preview = JSON.parse(await read('../src/data/preview-events.json'));
  const eventSlugById = new Map(preview.events.map((event) => [event.id, event.slug]));
  const meetings = projection.clubs.flatMap((club) => club.future_meetings);

  assert.equal(meetings.length, 1, '2026-07-23 snapshot has one policy-approved future meeting');
  for (const meeting of meetings) {
    const materializedSlug = eventSlugById.get(meeting.event_id);
    if (meeting.event_path) {
      assert.ok(materializedSlug, `linked event ${meeting.event_id} must be present in preview-events.json`);
      assert.equal(meeting.event_path, `/sobytiya/${materializedSlug}/`);
      assert.match(meeting.event_path, /^\/sobytiya\/[a-z0-9]+(?:-[a-z0-9]+)*\/$/u);
      continue;
    }
    assert.equal(meeting.source_url, null, 'an unmaterialized meeting must not fall back to an external action');
  }
  assert.equal(meetings[0].event_id, 6990);
  assert.equal(eventSlugById.has(6990), true);
  assert.equal(meetings[0].event_path, `/sobytiya/${eventSlugById.get(6990)}/`);
});

test('ICAE is the deterministic sixth partner and uses one byte-faithful local official SVG', async () => {
  const manifest = await read('../src/data/info-partners.ts');
  const sourceLogo = await readBytes('../src/assets/partners/source/icae-kaliningrad.logo-footer-h.svg');
  const runtimeLogo = await readBytes('../public/assets/partners/icae-kaliningrad.svg');
  const expectedSha = 'e59541c9ffa5c4865d87c1273068b2440ebf89bc794de6d5d18387cc9a0f3797';

  assert.equal((manifest.match(/^\s+id: '/gmu) || []).length, 6);
  assert.match(manifest, /id: 'icae-kaliningrad'[\s\S]*?href: 'https:\/\/klgd\.myatom\.ru\/'/u);
  assert.match(manifest, /id: 'icae-kaliningrad'[\s\S]*?logoUrl: '\/assets\/partners\/icae-kaliningrad\.svg'/u);
  assert.match(manifest, /id: 'icae-kaliningrad'[\s\S]*?gridColumnStart: 1,[\s\S]*?gridColumnSpan: 8,[\s\S]*?gridRowStart: 3/u);
  assert.match(manifest, /id: 'icae-kaliningrad'[\s\S]*?mobileColumnStart: 1,[\s\S]*?mobileColumnSpan: 4,[\s\S]*?mobileRowStart: 4/u);
  assert.deepEqual(runtimeLogo, sourceLogo);
  assert.equal(sourceLogo.length, 13523);
  assert.equal(createHash('sha256').update(sourceLogo).digest('hex'), expectedSha);
});

test('partners route keeps its bespoke official-logo board but owns one normalized route composition', async () => {
  const page = await read('../src/pages/partners/index.astro');

  assert.match(page, /import '\.\.\/\.\.\/components\/design-system\/product-contour-foundations\.css'/u);
  assert.match(page, /data-ds-family="PartnersRouteComposition"/u);
  assert.match(page, /data-ds-version="1"/u);
  assert.match(page, /data-ds-variant="official-logo-board"/u);
  assert.match(page, /data-ds-state=\{INFO_PARTNERS\.length \? 'populated' : 'empty'\}/u);
  assert.match(page, /data-partners-route/u);
  assert.match(page, /INFO_PARTNERS\.map/u);
  assert.doesNotMatch(page, /AdaptiveEventCardGrid|EventCard/u,
    'the partner logo board is a distinct composition, not an event-card lookalike');
  assert.match(page, /--ke-color-text-primary/u);
  assert.match(page, /--ke-color-text-muted/u);
});

test('preview hub no longer describes Clubs as empty and advertises the six-logo board', async () => {
  const hub = await read('../src/pages/[preview]/index.astro');

  assert.match(hub, /Свежая policy-current проекция подтверждённых клубов/u);
  assert.match(hub, /Шесть локальных официальных логотипов/u);
  assert.doesNotMatch(hub, /Честное пустое состояние в текущем срезе данных/u);
});
