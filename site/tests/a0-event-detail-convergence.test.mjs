import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const occurrences = (source, needle) => source.split(needle).length - 1;

test('desktop event detail delegates both rail anatomies to canonical EventMediaRail', async () => {
  const source = await read('src/components/DesktopEventPage.astro');

  assert.match(source, /import EventMediaRail, \{ type EventMediaRailItem \} from '\.\/EventMediaRail\.astro'/u);
  assert.match(source, /data-ds-family="DesktopEventDetailSurface"/u);
  assert.match(source, /data-ds-version="1"/u);
  assert.equal(occurrences(source, '<EventMediaRail'), 2);
  assert.match(source, /variant="hero-selector"/u);
  assert.match(source, /variant="poster-strip"/u);
  assert.doesNotMatch(source, /<div class="desktop-prototype__media-rail/u);
  assert.doesNotMatch(source, /<button[\s\S]{0,300}data-responsive-(?:split-)?item/u,
    'the event-detail consumer must not keep a second rail item anatomy');
});

test('EventHero uses a non-interactive inner MediaFrame while its opener remains the interaction owner', async () => {
  const source = await read('src/components/EventHero.astro');
  const primaryFrame = source.match(/<span\s+class="event-hero__media-frame"[\s\S]*?<\/span>/u)?.[0] || '';

  assert.match(source, /import '\.\/media-frame\.css';/u);
  assert.match(source, /data-ds-family="EventHero"/u);
  assert.match(source, /<button type="button" class="event-hero__visual"[\s\S]*?<span[\s\S]*?data-media-frame/u);
  for (const marker of [
    'data-media-frame-contract="v1"',
    'data-media-frame-style-owner="media-frame.css"',
    'data-media-frame-surface="event-hero"',
    'data-media-frame-fit=',
    'data-media-frame-object-position=',
    'data-media-frame-focal-position=',
    'data-media-frame-crop-reason=',
    'data-media-frame-interaction-owner="caller"',
    'data-media-frame-image',
  ]) assert.ok(source.includes(marker), `missing EventHero MediaFrame marker: ${marker}`);
  assert.ok(primaryFrame, 'primary opener MediaFrame must be present');
  assert.doesNotMatch(primaryFrame, /object-fit\s*:/u);
  assert.doesNotMatch(primaryFrame, /style=\{\[/u);
});

test('EventLayout names its shell and leaves canonical MediaFrame in charge of fit and clipping', async () => {
  const [layout, mobile] = await Promise.all([
    read('src/layouts/EventLayout.astro'),
    read('src/components/MobileEventProductionStyles.astro'),
  ]);

  assert.match(layout, /data-ds-family="EventLayout" data-ds-version="1"/u);
  assert.doesNotMatch(layout, /\.event-card__media-shell[^}]*overflow:\s*hidden/u);
  assert.doesNotMatch(layout, /\.event-card__media-shell[^}]*\.event-card__media\s*\{[^}]*object-fit/u);
  assert.doesNotMatch(layout, /\.event-hero[^}]*\.event-hero__image\s*\{[^}]*object-fit/u);
  assert.doesNotMatch(layout, /\.event-card--split-actions \.event-card__utility-row \.feedback-button--negative\s*\{[^}]*min-height:\s*36px/u);
  assert.match(layout, /\.feedback-button--negative\s*\{[^}]*min-height:\s*44px/u);
  assert.doesNotMatch(mobile, /\.event-hero__actions \.icon\s*\{[^}]*\b(?:width|height)\s*:/u);
  assert.doesNotMatch(mobile, /\.event-card__[^}]*\{[^}]*object-(?:fit|position)/u);
});
