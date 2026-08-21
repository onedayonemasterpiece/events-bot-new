import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const read = (path) => readFileSync(resolve(siteDir, path), 'utf8');

const css = read('src/styles/design-system.css');
const eventTokens = read('src/components/EventTokenMedallions.astro');
const eventLayout = read('src/layouts/EventLayout.astro');
const mobileEvent = read('src/components/MobileEventProductionStyles.astro');
const desktopEvent = read('src/components/DesktopEventPage.astro');
const listingCard = read('src/components/listings/ListingEventCard.astro');
const mobileRail = read('src/components/listings/MobileListingRailSurface.astro');
const exhibition = read('src/components/ExhibitionsPersonalSurface.astro');
const exhibitionLab = read('src/pages/lab/exhibitions-personal/index.astro');
const catalog = read('src/pages/lab/design-system/index.astro');

test('the design system owns exactly three identity-medallion size tiers', () => {
  assert.match(css, /--ke-medallion-size-compact:\s*44px/);
  assert.match(css, /--ke-medallion-size-standard:\s*60px/);
  assert.match(css, /--ke-medallion-size-feature:\s*88px/);
});

test('EventTokenMedallions v2 defaults to feature while retaining a catalog-only v1 comparison', () => {
  assert.match(eventTokens, /sizeContract\?:\s*'legacy'\s*\|\s*'normalized'/);
  assert.match(eventTokens, /sizeContract\s*=\s*'normalized'/);
  assert.match(eventTokens, /data-medallion-size-contract=\{sizeContract\}/);
  assert.match(eventTokens, /data-ds-version=\{sizeContract === 'normalized' \? '2' : '1'\}/);
  assert.match(eventLayout, /--token-size:\s*var\(--ke-medallion-size-feature\)/);
  assert.match(mobileEvent, /data-medallion-size-contract="legacy"/);
  assert.match(desktopEvent, /data-medallion-size-contract="legacy"/);
  assert.match(catalog, /<EventTokenMedallions[^>]*sizeContract="legacy"/);
  assert.match(catalog, /<EventTokenMedallions[^>]*sizeContract="normalized"/);
  assert.match(catalog, /data-ds-component="EventTokenMedallions" data-ds-version="1" data-ds-replaced-by="EventTokenMedallions@2"/);
  assert.match(catalog, /data-ds-component="EventTokenMedallions" data-ds-version="2"/);
});

test('listing, rail and exhibition consumers bind to standard, feature and compact tiers', () => {
  assert.match(listingCard, /data-medallion-size-contract="normalized-v2"/);
  assert.match(listingCard, /data-listing-medallion-tier="standard-desktop-compact-mobile"/);
  assert.match(css, /\.ke-listing-card__identity-medallion,[\s\S]*?width:\s*var\(--ke-medallion-size-standard\)/);
  assert.match(css, /\.ke-listing-card__medallion\s*\{[\s\S]*?width:\s*var\(--ke-medallion-size-standard\)/);
  assert.match(css, /mobile-adaptive[^}]*\.ke-listing-card__medallion\s*\{[\s\S]*?width:\s*var\(--ke-medallion-size-compact\)/);
  assert.match(mobileRail, /data-medallion-size-contract="normalized-v2"/);
  assert.match(mobileRail, /\.event-medallion-slot picture\{display:block;width:var\(--ke-medallion-size-feature\);height:var\(--ke-medallion-size-feature\)\}/);
  assert.match(exhibition, /width:var\(--ke-medallion-size-compact\);\s*height:var\(--ke-medallion-size-compact\)/);
  assert.match(exhibitionLab, /width:var\(--ke-medallion-size-compact\);\s*height:var\(--ke-medallion-size-compact\)/);
});

test('the normalized active rules do not reintroduce the retired identity sizes', () => {
  const activeListing = css.slice(css.indexOf('/* Quiet recognition:'));
  for (const retired of ['width: 64px', 'width: 56px', 'width: 51px', 'width: 46px', 'width: 40px']) {
    assert.equal(activeListing.includes(retired), false, `retired listing identity size remains: ${retired}`);
  }
  assert.equal(mobileRail.includes('picture{display:block;width:86px;height:86px}'), false);
});
