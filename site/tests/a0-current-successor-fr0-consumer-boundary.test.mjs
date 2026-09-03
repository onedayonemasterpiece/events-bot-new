import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

const styleSource = (source) => [...source.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)]
  .map((match) => match[1])
  .join('\n');

const exhibitionPrivateThemeAliases = [
  '--ex-bg',
  '--ex-surface',
  '--ex-raised',
  '--ex-border',
  '--ex-text',
  '--ex-muted',
  '--ex-blue',
  '--ex-orange',
  '--ex-red',
  '--ex-gray',
  '--ex-yellow',
  '--ex-purple',
  '--ex-green',
  '--ex-ease-cinematic',
  '--ex-ease-emphasis',
  '--ex-motion-fast',
  '--ex-motion-base',
];

const exhibitionRuntimeLayoutVariables = [
  '--ex-media-column',
  '--ex-row-gap',
  '--ex-surface-start',
  '--ex-rail-color',
];

test('current successor already contains the bounded A0 consumer closure', async () => {
  const [exhibitions, festivals, clubDetail, clubCard, focusCollection, closedHub] = await Promise.all([
    read('src/components/ExhibitionsPersonalSurface.astro'),
    read('src/pages/festivali/index.astro'),
    read('src/pages/kluby-po-interesam/[slug]/index.astro'),
    read('src/components/InterestClubCard.astro'),
    read('src/pages/fokus-gruppa/kollektsiya/index.astro'),
    read('src/pages/zakrytaya-afisha/index.astro'),
  ]);

  assert.match(exhibitions, /data-ds-family="ExhibitionsPersonalSurface"/u);
  assert.match(exhibitions, /data-ke-foundation-consumer="exhibitions-personal-surface"/u);
  assert.match(exhibitions, /product-contour-foundations\.css/u);
  assert.match(exhibitions, /<SemanticIcon name="arrow-left" role="control" \/>/u);
  assert.match(exhibitions, /<SemanticIcon name="arrow-right" role="control" \/>/u);
  for (const alias of exhibitionPrivateThemeAliases) {
    assert.ok(!exhibitions.includes(`${alias}:`), `exhibitions retains private declaration ${alias}`);
    assert.ok(!exhibitions.includes(`var(${alias})`), `exhibitions retains private reference ${alias}`);
  }
  for (const variable of exhibitionRuntimeLayoutVariables) {
    assert.match(exhibitions, new RegExp(variable, 'u'), `exhibitions lost runtime layout variable ${variable}`);
  }

  assert.match(festivals, /data-ds-family="FestivalsTimelineRouteComposition"/u);
  assert.match(festivals, /data-ke-foundation-consumer="festival-route"/u);
  assert.match(festivals, /<SemanticIcon name="heart" role="control" \/>/u);
  assert.match(festivals, /<SemanticIcon name="link" role="control" \/>/u);
  assert.match(festivals, /<SemanticIcon name="calendar" role="control" \/>/u);
  assert.match(festivals, /background:\s*var\(--ke-color-festival-guide-like-surface\)/u);
  assert.match(festivals, /background:\s*var\(--ke-color-festival-category-surface\)/u);
  assert.match(festivals, /target=\{item\.isExternal \? '_blank' : undefined\}/u);
  assert.match(festivals, /rel=\{item\.isExternal \? 'noopener noreferrer' : undefined\}/u);

  assert.match(clubDetail, /data-ds-family="InterestClubDetailRouteComposition"/u);
  assert.match(clubDetail, /product-contour-foundations\.css/u);
  assert.match(clubDetail, /<SemanticIcon name="arrow-left" role="inline" \/>/u);
  assert.match(clubDetail, /var\(--ke-color-club-detail-hero-start\)/u);
  assert.match(clubDetail, /var\(--ke-color-club-detail-note-surface\)/u);
  assert.match(clubDetail, /rel=\{isExternal \? 'noopener noreferrer nofollow' : undefined\}/u);

  for (const token of [
    '--ke-color-club-card-fallback-orbit-ring-inner',
    '--ke-color-club-card-fallback-orbit-ring-outer',
    '--ke-color-club-card-fallback-orbit-line',
    '--ke-color-club-card-fact-divider',
  ]) assert.match(clubCard, new RegExp(token, 'u'), `club card misses ${token}`);

  assert.match(focusCollection, /data-ds-family="FocusEggCollectionRouteComposition"/u);
  assert.match(focusCollection, /data-ds-variant="collection-prototype"/u);
  assert.match(focusCollection, /data-ds-state=\{`found-\$\{collectionProgress\.found\}-of-\$\{collectionProgress\.eligible\}`\}/u);
  assert.match(focusCollection, /root\.dataset\.dsState = `found-\$\{found\}-of-\$\{eligible\}`;/u);

  assert.match(closedHub, /data-ds-family="ClosedFocusHubRouteComposition"/u);
  assert.match(closedHub, /data-ds-variant="participant-hub"/u);
  assert.match(closedHub, /data-ds-state="checking"/u);
  assert.match(closedHub, /root\.dataset\.dsState = marker\?\.status === 'active' \? 'available' : 'locked';/u);
  assert.match(closedHub, /readFocusParticipationMarker/u);
  assert.match(closedHub, /clearFocusParticipationMarker/u);
});

test('FR0 remains the effective exhibitions fit, focal, clip and resource-state owner', async () => {
  const [surface, row, bridge, mediaFrame] = await Promise.all([
    read('src/components/ExhibitionsPersonalSurface.astro'),
    read('src/components/ExhibitionPrototypeRow.astro'),
    read('src/components/exhibitionsMediaFrameBridge.mjs'),
    read('src/components/media-frame.css'),
  ]);
  const localStyles = styleSource(surface);

  for (const surfaceName of ['exhibitions-deck', 'exhibitions-gallery', 'exhibitions-medallion']) {
    assert.match(mediaFrame, new RegExp(`data-media-frame-surface="${surfaceName}"`, 'u'));
  }
  assert.match(
    mediaFrame,
    /\[data-media-frame\]\[data-media-frame-contract="v1"\]\s*>\s*\[data-media-frame-image\]\s*\{[^}]*object-position:\s*var\(--media-frame-object-position/u,
  );
  assert.match(mediaFrame, /data-media-frame-fit="cover"[^{}]*\{[^}]*object-fit:\s*cover;/su);
  assert.match(mediaFrame, /data-media-frame-fit="contain"[^{}]*\{[^}]*object-fit:\s*contain;/su);
  assert.match(mediaFrame, /\[data-media-frame\]\[data-media-frame-contract="v1"\]\s*\{[^}]*overflow:\s*hidden;/su);
  assert.match(mediaFrame, /data-media-frame-resource-state="broken"[^{}]+data-media-frame-fallback/su);

  assert.match(row, /data-media-frame-style-owner="media-frame\.css"/u);
  assert.match(row, /data-media-frame-surface="exhibitions-deck"/u);
  assert.match(row, /data-media-frame-surface="exhibitions-medallion"/u);
  assert.match(bridge, /const FRAME_STYLE_OWNER = 'media-frame\.css';/u);
  assert.match(bridge, /publishFrame\(media, 'exhibitions-gallery'/u);
  assert.match(bridge, /frame\.style\.setProperty\('--media-frame-object-position', decision\.objectPosition\)/u);

  assert.doesNotMatch(
    localStyles,
    /\[data-media-frame[^{}]*\{[^}]*(?:object-fit|object-position|overflow|clip-path|border-radius)\s*:/iu,
    'A0 consumer must not create an attribute-scoped competing MediaFrame style owner',
  );
  assert.doesNotMatch(
    localStyles,
    /(?:object-fit|object-position|overflow|clip-path|border-radius)\s*:[^;}]*!important/iu,
    'legacy A0 donor declarations must remain subordinate to the canonical FR0 selectors',
  );
});

test('the bounded batch does not recreate M0 grid or component diagnostics', async () => {
  const consumers = await Promise.all([
    read('src/components/ExhibitionsPersonalSurface.astro'),
    read('src/pages/festivali/index.astro'),
    read('src/pages/kluby-po-interesam/[slug]/index.astro'),
    read('src/components/InterestClubCard.astro'),
    read('src/pages/fokus-gruppa/kollektsiya/index.astro'),
    read('src/pages/zakrytaya-afisha/index.astro'),
  ]);
  const combined = consumers.join('\n');
  for (const forbidden of [
    'packRelatedCardRows',
    'data-adaptive-grid-diagnostic-owner',
    'data-adaptive-grid-rendered-order',
    'data-adaptive-grid-remainder-variant',
    'data-media-frame-style-owner="ExhibitionsPersonalSurface.astro"',
  ]) assert.ok(!combined.includes(forbidden), `A0 consumer batch recreates canonical owner ${forbidden}`);
  assert.doesNotMatch(combined, /\.ke-icon-role[^{}]*\{[^}]*(?:width|height)\s*:/iu);
});
