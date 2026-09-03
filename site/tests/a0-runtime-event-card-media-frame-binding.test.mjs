import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const layout = await readFile(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8');

function functionSource(name, nextName) {
  const start = layout.indexOf(`function ${name}(`);
  const end = nextName ? layout.indexOf(`function ${nextName}(`, start + 1) : -1;
  assert.ok(start >= 0, `missing ${name}`);
  assert.ok(end > start, `missing ${nextName} after ${name}`);
  return layout.slice(start, end);
}

test('one reusable local helper fully rebinds runtime EventCard MediaFrame evidence', () => {
  const binder = functionSource('bindRuntimeEventCardMediaFrame', 'applyRuntimeRelatedLayout');

  for (const marker of [
    "shell.setAttribute('data-media-frame', '')",
    "shell.dataset.mediaFrameContract = 'v1'",
    "shell.dataset.mediaFrameStyleOwner = 'media-frame.css'",
    "shell.dataset.mediaFrameSurface = 'event-card'",
    "shell.dataset.mediaFrameInteractionOwner = 'caller'",
    'shell.dataset.mediaFrameRole = role',
    'shell.dataset.mediaFrameKind = kind',
    'shell.dataset.mediaFrameFit = fit',
    'shell.dataset.mediaFrameCropPermission = cropPermission',
    'shell.dataset.mediaFrameCropReason = cropReason',
    'shell.dataset.mediaFrameRatio = frameRatio.toFixed(5)',
    'shell.dataset.mediaFrameObjectPosition = objectPosition',
    'shell.dataset.mediaFrameFocalPosition = objectPosition',
    "shell.dataset.mediaFrameClip = 'frame'",
    "shell.dataset.mediaFrameRadius = 'surface'",
    "shell.dataset.mediaFrameFill = 'true'",
    "shell.dataset.mediaFrameLoading = 'lazy'",
    "shell.style.setProperty('--media-frame-ratio', frameRatio.toFixed(5))",
    "shell.style.setProperty('--media-frame-object-position', objectPosition)",
    "image.style.removeProperty('object-fit')",
    "image.style.removeProperty('object-position')",
  ]) assert.ok(binder.includes(marker), `missing authoritative runtime binding: ${marker}`);

  assert.match(binder, /for \(const key of RUNTIME_EVENT_CARD_MEDIA_DATASET_KEYS\) delete shell\.dataset\[key\]/u);
  assert.match(binder, /for \(const property of RUNTIME_EVENT_CARD_MEDIA_STYLE_PROPERTIES\) shell\.style\.removeProperty\(property\)/u);
  assert.match(binder, /delete image\.dataset\.cardMediaReview/u);
});

test('runtime binding implements pending, loaded, fallback and terminal broken transitions', () => {
  const binder = functionSource('bindRuntimeEventCardMediaFrame', 'applyRuntimeRelatedLayout');

  assert.match(binder, /const resourceState = imageUrl \? 'pending' : 'fallback'/u);
  assert.match(binder, /const cropReason = imageUrl[\s\S]*'runtime_event_card_fallback'/u);
  assert.match(binder, /shell\.dataset\.mediaFrameResourceState = resourceState/u);
  assert.match(binder, /shell\.setAttribute\('data-media-frame-fallback', ''\)/u);
  assert.match(binder, /fallback\.hidden = false/u);

  for (const token of [
    "shell.dataset.mediaFrameResourceState='loaded'",
    "shell.removeAttribute('data-media-frame-fallback')",
    "shell.dataset.mediaFrameResourceState='broken'",
    "shell.dataset.mediaFrameKind='fallback'",
    "shell.dataset.mediaFrameFit='contain'",
    "shell.dataset.mediaFrameCropPermission='forbidden'",
    "shell.dataset.mediaFrameCropReason='resource_load_error'",
    "image.removeAttribute('src')",
    "image.removeAttribute('srcset')",
    "image.removeAttribute('sizes')",
    'fallback.hidden=false',
  ]) assert.ok(binder.includes(token), `missing resource transition token: ${token}`);

  assert.match(binder, /image\.decode\(\)\.then\(completeLoaded\)\.catch\(failBroken\)/u);
  assert.match(binder, /if\(shell\.dataset\.mediaFrameResourceState==='broken'\)return/u);
  assert.match(binder, /if\(!\(image\.naturalWidth>0&&image\.naturalHeight>0\)\)\{failBroken\(\);return;\}/u);
});

test('both runtime entrypoints consume the same rebinding and no inline fit owner remains', () => {
  const create = functionSource('createEventCardElement', 'appendEventCard');
  const layoutBinding = functionSource('applyRuntimeRelatedLayout', 'createEventCardElement');

  assert.match(create, /applyRuntimeRelatedLayout\(card,[\s\S]*\);\s*bindRuntimeEventCardMediaFrame\(card, data, relatedLayout, imageUrl, documentMedia\);/u);
  assert.doesNotMatch(layout, /image\.style\.(?:objectFit|objectPosition)\s*=/u);
  assert.doesNotMatch(layoutBinding, /image\.style\.(?:objectFit|objectPosition)\s*=/u);
  assert.match(layoutBinding, /mediaShell\.dataset\.mediaFrameFit = authoritativeFit/u);
  assert.match(layoutBinding, /mediaShell\.dataset\.mediaFrameObjectPosition = objectPosition/u);
  assert.match(layoutBinding, /mediaShell\.style\.setProperty\('--media-frame-object-position', objectPosition\)/u);

  assert.match(layout, /window\.KenigEventsCreateEventCard = createEventCardElement/u);
  assert.match(layout, /window\.KenigEventsRenderEventCard = \(\.\.\.args\) => createEventCardElement\(\.\.\.args\)\?\.outerHTML \|\| ''/u);
});

test('EventCard anatomy, actions and ranking entrypoints remain in the existing factory', () => {
  const create = functionSource('createEventCardElement', 'appendEventCard');
  for (const marker of [
    "sourceCard.cloneNode(true)",
    "[data-card-media-link]",
    "[data-card-title]",
    "[data-card-meta]",
    "[data-card-status]",
    "[data-card-place]",
    "[data-feedback-action=\"not_interested\"]",
    "[data-feedback-action=\"like\"]",
    "[data-native-share]",
    "[data-calendar-action]",
  ]) assert.ok(create.includes(marker), `runtime factory lost ${marker}`);

  for (const marker of [
    'rankEventDetailRelated',
    'rankPersonalFeedCandidates',
    'rankPopularFallbackCandidates',
    'rankAdjacentContinuationCandidates',
    'KenigEventsSelectEventContinuation',
    'appendPersonalFeedChunk',
    'syncFeedWithManifest',
  ]) assert.ok(layout.includes(marker), `layout lost ranking/order entrypoint ${marker}`);
});
