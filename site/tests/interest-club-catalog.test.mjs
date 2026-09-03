import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  adjacentVisualClubCard,
  visualClubCardRows,
} from '../src/components/clubCatalogNavigation.mjs';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('club keyboard rows and movement follow rendered geometry, not DOM order', () => {
  const cards = ['dom-a', 'dom-b', 'dom-c', 'dom-d', 'dom-e'];
  const rectangles = new Map([
    ['dom-a', { top: 220, left: 410, width: 300 }],
    ['dom-b', { top: 100, left: 710, width: 300 }],
    ['dom-c', { top: 100, left: 90, width: 300 }],
    ['dom-d', { top: 220, left: 90, width: 300 }],
    ['dom-e', { top: 100, left: 400, width: 300 }],
  ]);
  const options = { rectFor: (card) => rectangles.get(card) };
  const rows = visualClubCardRows(cards, options);

  assert.deepEqual(rows.map((row) => row.cards.map(({ card }) => card)), [
    ['dom-c', 'dom-e', 'dom-b'],
    ['dom-d', 'dom-a'],
  ]);
  assert.equal(adjacentVisualClubCard(cards, 'dom-c', 'ArrowRight', options), 'dom-e');
  assert.equal(adjacentVisualClubCard(cards, 'dom-e', 'ArrowDown', options), 'dom-a');
  assert.equal(adjacentVisualClubCard(cards, 'dom-b', 'ArrowDown', options), 'dom-a');
  assert.equal(adjacentVisualClubCard(cards, 'dom-a', 'ArrowUp', options), 'dom-e');
  assert.equal(adjacentVisualClubCard(cards, 'dom-a', 'Home', options), 'dom-c');
  assert.equal(adjacentVisualClubCard(cards, 'dom-c', 'End', options), 'dom-a');
});

test('club cover registry is source-grounded and generated source art stays on fallback', async () => {
  const [registry, component, metadataText, image] = await Promise.all([
    read('src/data/interest-club-covers.ts'),
    read('src/components/InterestClubCard.astro'),
    read('src/assets/clubs/source/game-vibes-event-2897.metadata.json'),
    readFile(path.join(siteRoot, 'src/assets/clubs/source/game-vibes-event-2897.webp')),
  ]);
  const metadata = JSON.parse(metadataText);

  assert.equal(metadata.club_slug, 'game-vibes');
  assert.equal(metadata.source_event_id, 2897);
  assert.equal(metadata.source_post_url, 'https://t.me/signalkld/9929');
  assert.equal(createHash('sha256').update(image).digest('hex'), metadata.sha256);
  assert.match(metadata.audit_note, /Existing approved event-source photograph/u);
  assert.match(registry, /klub-issledovateley-neyronok[\s\S]*generated[\s\S]*fallback/u);
  assert.doesNotMatch(registry, /imagegen|gpt-image/u);
  assert.match(component, /data-cover-state=\{cover \? 'ready' : 'fallback'\}/u);
  assert.match(component, /data-club-cover/u);
  assert.match(component, /data-club-primary-action/u);
});

test('catalog markup keeps shortcut hints focus-scoped and desktop columns count-aware', async () => {
  const [page, card, controller] = await Promise.all([
    read('src/pages/kluby-po-interesam/index.astro'),
    read('src/components/InterestClubCard.astro'),
    read('src/components/clubCatalogNavigation.mjs'),
  ]);

  assert.match(page, /data-club-columns=\{Math\.min\(clubs\.length, 3\)\}/u);
  assert.match(page, /grid-template-columns:repeat\(var\(--club-columns\),minmax\(0,1fr\)\)/u);
  assert.match(page, /@media \(max-width: 760px\)[\s\S]*grid-template-columns:1fr/u);
  assert.match(page, /data-club-mobile-shelf/u);
  assert.match(page, /\.club-mobile-shelf \{[\s\S]*position:sticky;[\s\S]*top:var\(--ke-clubs-mobile-shelf-top\);/u);
  assert.match(page, /<strong>Клубы по интересам<\/strong>/u);
  assert.doesNotMatch(page, /class="crumbs"/u);
  assert.match(card, /\.club-card__keyboard-hint[\s\S]*visibility:hidden;[\s\S]*opacity:0;/u);
  assert.match(card, /\.club-card:focus-visible \.club-card__keyboard-hint,[\s\S]*visibility:visible; opacity:\.72;/u);
  assert.match(controller, /if \(target !== card\)[\s\S]*return;/u);
  assert.match(controller, /const primary = card\.querySelector\('\[data-club-primary-action\]'\)/u);
  assert.match(controller, /matchMedia\('\(min-width: 1024px\)'\)/u);
});

test('mobile club cards reuse the accepted desktop overlay and luminous corner badge', async () => {
  const card = await read('src/components/InterestClubCard.astro');

  assert.match(card, /class="club-card__future club-card__future--desktop"[\s\S]*data-club-future-badge="desktop"/u);
  assert.match(card, /class="club-card__future club-card__future--mobile"[\s\S]*data-club-future-badge="mobile"/u);
  assert.match(card, /\.club-card__future--desktop \{[\s\S]*position:absolute;[\s\S]*top:1\.2rem;[\s\S]*right:1\.2rem;/u);
  assert.match(card, /\.club-card__future--desktop \{[\s\S]*box-shadow:var\(--ke-elevation-club-future\);/u);
  assert.match(card, /\.club-card__future--desktop::after \{[\s\S]*top:calc\(100% - \.12rem\);[\s\S]*radial-gradient\(ellipse at top,var\(--ke-color-club-card-future-glow-start\),var\(--ke-color-club-card-future-glow-middle\) 34%,var\(--ke-color-club-card-future-glow-end\) 62%,transparent 80%\)[\s\S]*mix-blend-mode:screen;/u);
  assert.match(card, /\.club-card__future--mobile \{ display:none; \}/u);
  assert.doesNotMatch(card, /@media \(max-width: 760px\) \{[\s\S]*\.club-card__future--desktop \{ display:none; \}/u);
  assert.match(card, /@media \(max-width: 760px\) \{[\s\S]*\.club-card \{[\s\S]*min-height:28rem;/u);
  assert.match(card, /@media \(max-width: 430px\) \{[\s\S]*\.club-card__facts \{ grid-template-columns:repeat\(2,minmax\(0,1fr\)\); \}/u);
  assert.match(card, /\.club-card__media,[\s\S]*position:absolute; inset:0; width:100%; height:100%;/u);
  assert.match(card, /\.club-card__veil \{[\s\S]*var\(--ke-color-club-card-veil-bottom\) 100%/u);
});
