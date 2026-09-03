import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('EventHero primary opener delegates fit and focal application to a non-interactive MediaFrame', async () => {
  const source = await read('src/components/EventHero.astro');

  assert.match(source, /import '\.\/media-frame\.css';/u);
  assert.match(source, /data-ds-family="EventHero"/u);
  assert.match(source, /data-ds-version="1"/u);
  assert.match(source, /data-ds-variant=\{heroComposition\}/u);
  assert.match(source, /data-ds-state=\{`\$\{heroMode\} \$\{hasGallery \? 'gallery' : 'fallback'\}`\}/u);

  const primaryFrame = /<span\s+[\s\S]*?class="event-hero__media-frame"[\s\S]*?<\/span>/u.exec(source)?.[0] || '';
  assert.match(primaryFrame, /data-media-frame/u);
  assert.match(primaryFrame, /data-media-frame-contract="v1"/u);
  assert.match(primaryFrame, /data-media-frame-style-owner="media-frame\.css"/u);
  assert.match(primaryFrame, /data-media-frame-surface="event-hero"/u);
  assert.match(primaryFrame, /data-media-frame-fit=\{primaryMediaFrameFit\}/u);
  assert.match(primaryFrame, /data-media-frame-object-position=\{primaryMediaFramePosition\}/u);
  assert.match(primaryFrame, /data-media-frame-interaction-owner="caller"/u);
  assert.match(primaryFrame, /data-media-frame-image/u);
  assert.doesNotMatch(primaryFrame, /object-fit:|object-position:/u);
  assert.doesNotMatch(primaryFrame, /<(?:a|button)\b[^>]*data-media-frame/u,
    'MediaFrame must remain non-interactive; the existing EventHero opener owns the action');
  assert.match(source, /<button type="button" class="event-hero__visual"[\s\S]*?data-hero-gallery-open/u);
});

test('the full-screen gallery remains named EventHero viewer anatomy, not a second primary-frame owner', async () => {
  const source = await read('src/components/EventHero.astro');

  assert.match(source, /class="hero-gallery"[\s\S]*data-hero-gallery[\s\S]*role="dialog"/u);
  assert.match(source, /data-gallery-slide-kind="image"/u);
  assert.match(source, /data-protected-crop-fit=\{image\.protected_crop_fit\}/u);
  assert.match(source, /data-protected-crop-reason=\{image\.protected_crop_reason\}/u);
  assert.match(source, /object-fit:\$\{image\.low_resolution_portrait \? 'contain' : image\.protected_crop_fit \|\| 'contain'\}/u);
  assert.equal((source.match(/class="event-hero__media-frame"/gu) || []).length, 1,
    'the primary EventHero frame has one canonical MediaFrame root');
});
