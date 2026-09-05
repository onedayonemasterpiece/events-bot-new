import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const layout = await readFile(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8');
const eventCard = await readFile(new URL('../src/components/EventCard.astro', import.meta.url), 'utf8');
const adaptiveGrid = await readFile(new URL('../src/components/AdaptiveEventCardGrid.astro', import.meta.url), 'utf8');
const eventsSource = await readFile(new URL('../src/lib/events.ts', import.meta.url), 'utf8');

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
    "shell.dataset.mediaFrameRole = imageUrl ? role : 'fallback'",
    "shell.dataset.mediaFrameKind = imageUrl ? kind : 'fallback'",
    "shell.dataset.mediaFrameFit = imageUrl ? fit : 'contain'",
    "shell.dataset.mediaFrameCropPermission = imageUrl ? cropPermission : 'forbidden'",
    'shell.dataset.mediaFrameCropReason = cropReason',
    'shell.dataset.mediaFrameRatio = frameRatio.toFixed(5)',
    'shell.dataset.mediaFrameObjectPosition = effectiveObjectPosition',
    'shell.dataset.mediaFrameFocalPosition = effectiveObjectPosition',
    "shell.dataset.mediaFrameClip = 'frame'",
    "shell.dataset.mediaFrameRadius = 'surface'",
    "shell.dataset.mediaFrameFill = 'true'",
    "shell.dataset.mediaFrameLoading = 'lazy'",
    "shell.style.setProperty('--media-frame-ratio', frameRatio.toFixed(5))",
    "shell.style.setProperty('--media-frame-object-position', effectiveObjectPosition)",
    "image.style.removeProperty('object-fit')",
    "image.style.removeProperty('object-position')",
  ]) assert.ok(binder.includes(marker), `missing authoritative runtime binding: ${marker}`);

  assert.match(binder, /for \(const key of RUNTIME_EVENT_CARD_MEDIA_DATASET_KEYS\) delete shell\.dataset\[key\]/u);
  assert.match(binder, /for \(const property of RUNTIME_EVENT_CARD_MEDIA_STYLE_PROPERTIES\) shell\.style\.removeProperty\(property\)/u);
  assert.match(binder, /for \(const attribute of \['src', 'srcset', 'sizes', 'width', 'height', 'onload', 'onerror'\]\)/u);
  assert.match(binder, /for \(const key of RUNTIME_EVENT_CARD_IMAGE_DATASET_KEYS\) delete image\.dataset\[key\]/u);
  assert.match(binder, /shell\.removeAttribute\('data-media-frame-fallback'\)/u);
  assert.match(binder, /shell\.className = 'event-card__media-shell event-card__media-shell--dynamic'/u);
  for (const property of ['--media-frame-ratio', '--media-frame-object-position', '--dynamic-media-ratio', '--card-focal-y', '--card-focal-offset']) {
    assert.ok(layout.includes(`'${property}'`), `template CSS cleanup omits ${property}`);
  }
});

test('runtime binding keeps unknown semantics while honoring an explicit planned fallback cover', () => {
  const binder = functionSource('bindRuntimeEventCardMediaFrame', 'applyRuntimeRelatedLayout');

  assert.match(binder, /const kind = imageTextMode === 'visual_only'\s*\? 'visual'\s*:\s*imageTextMode === 'ocr_text' \? 'document' : 'unknown'/u);
  assert.match(binder, /shell\.dataset\.mediaFrameKind = imageUrl \? kind : 'fallback'/u);
  assert.match(binder, /const fit = relatedLayout \? requestedFit : kind === 'unknown' \? 'contain' : requestedFit/u);
  assert.doesNotMatch(binder, /imageTextMode\s*!==\s*'visual_only'\s*\?\s*'document'/u);
});

test('source dimensions and ratio are published only behind one finite positive pair guard', () => {
  const binder = functionSource('bindRuntimeEventCardMediaFrame', 'applyRuntimeRelatedLayout');

  assert.match(binder, /const hasFactualSourceDimensions = Number\.isFinite\(sourceWidth\)\s*&& sourceWidth > 0\s*&& Number\.isFinite\(sourceHeight\)\s*&& sourceHeight > 0/u);
  assert.match(binder, /if \(hasFactualSourceDimensions\) \{\s*shell\.dataset\.mediaFrameSourceWidth = String\(sourceWidth\);\s*shell\.dataset\.mediaFrameSourceHeight = String\(sourceHeight\);\s*shell\.dataset\.mediaFrameSourceRatio = sourceRatio\.toFixed\(5\);\s*\}/u);
  assert.match(binder, /if \(hasFactualSourceDimensions\) \{\s*image\.setAttribute\('width', String\(sourceWidth\)\);\s*image\.setAttribute\('height', String\(sourceHeight\)\);\s*\}/u);
  for (const key of ['mediaFrameSourceWidth', 'mediaFrameSourceHeight', 'mediaFrameSourceRatio']) {
    assert.ok(layout.includes(`'${key}'`), `source cleanup omits ${key}`);
  }
  assert.doesNotMatch(binder, /mediaFrameSource(?:Width|Height|Ratio)\s*=\s*''/u);
});

test('runtime binding implements pending, loaded, fallback and terminal broken transitions', () => {
  const binder = functionSource('bindRuntimeEventCardMediaFrame', 'applyRuntimeRelatedLayout');

  assert.match(binder, /const resourceState = imageUrl \? 'pending' : 'fallback'/u);
  assert.match(binder, /const cropReason = imageUrl[\s\S]*'runtime_event_card_fallback'/u);
  assert.match(binder, /const effectiveObjectPosition = imageUrl \? objectPosition : '50% 50%'/u);
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
    "image.removeAttribute('width')",
    "image.removeAttribute('height')",
    'fallback.hidden=false',
  ]) assert.ok(binder.includes(token), `missing resource transition token: ${token}`);

  assert.match(binder, /image\.decode\(\)\.then\(completeLoaded\)\.catch\(failBroken\)/u);
  assert.match(binder, /if\(shell\?\.dataset\.mediaFrameResourceState==='broken'\)return/u);
  assert.match(binder, /if\(!\(image\.naturalWidth>0&&image\.naturalHeight>0\)\)\{failBroken\(\);return;\}/u);
  assert.match(binder, /image\.setAttribute\('onerror', `const image=this;\$\{failBroken\}`\)/u);
  for (const attribute of ['data-media-frame-source-width', 'data-media-frame-source-height', 'data-media-frame-source-ratio']) {
    assert.ok(binder.includes(`shell.removeAttribute('${attribute}')`), `broken state retains ${attribute}`);
  }
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
  assert.match(layoutBinding, /if \(resourceState === 'broken' \|\| resourceState === 'fallback'\) return/u);
  assert.doesNotMatch(layoutBinding, /mediaShell\.dataset\.mediaFrameKind\s*=/u);
  assert.match(layoutBinding, /const authoritativeFit = relatedLayout\.fit === 'cover' \? 'cover' : 'contain'/u);
  assert.match(layoutBinding, /mediaShell\.dataset\.mediaFrameRatio = rowRatio\.toFixed\(5\)/u);
  assert.match(layoutBinding, /mediaShell\.style\.setProperty\('--media-frame-ratio', rowRatio\.toFixed\(5\)\)/u);
  assert.match(layoutBinding, /setRuntimeCardDataset\(card, 'labCropPermission', window\.KenigEventsRelatedCardMediaFrameBinding\(relatedLayout\)\.cropPermission\)/u);

  // An explicit shared plan is authoritative even when semantic classification
  // is unknown; the binding still publishes fallback-minimal rather than
  // misrepresenting that crop as reviewed.
  assert.match(layoutBinding, /window\.KenigEventsRelatedCardMediaFrameBinding\(\{[\s\S]*mediaTreatment:relatedLayout\.mediaTreatment,[\s\S]*\}\)\.cropPermission/u);

  assert.match(layout, /window\.KenigEventsCreateEventCard = createEventCardElement/u);
  assert.match(layout, /window\.KenigEventsRenderEventCard = \(\.\.\.args\) => createEventCardElement\(\.\.\.args\)\?\.outerHTML \|\| ''/u);
  assert.doesNotMatch(create, /mediaShell\.(?:className|style|dataset)|image\.style\.(?:objectFit|objectPosition)/u);
});

test('server and hydrated cards carry one source-bound protected framing contract', () => {
  assert.match(eventCard, /relatedCardMediaFrameBinding\(cardCrop\)/u);
  assert.match(eventCard, /const relatedMediaDecision = desktopRelatedCrop\s*\? \(desktopRelatedLayout \|\| resolveRelatedCardMediaTreatment\(event, cardTargetAspect\)\)/u);
  assert.match(eventCard, /mediaTreatment: 'document-safe-cover' \| 'document-protected-cover'/u);
  assert.match(eventCard, /data-lab-crop-permission=\{desktopRelatedLayout \? cardFrameBinding\.cropPermission : undefined\}/u);
  assert.match(adaptiveGrid, /desktopRelatedLayout=\{layout\}/u);
  assert.match(adaptiveGrid, /dataset\.labFramingStatus === 'satisfied'/u);
  assert.equal((eventsSource.match(/\.\.\.relatedCardCropProofPayload\(primaryAsset\)/gu) || []).length, 2);
  assert.match(layout, /import \{[^}]*relatedCardMediaFrameBinding[^}]*\} from '\.\.\/lib\/relatedCardLayout\.mjs'/u);
  assert.match(layout, /KenigEventsRelatedCardMediaFrameBinding: relatedCardMediaFrameBinding/u);
  assert.equal((layout.match(/window\.KenigEventsRelatedCardMediaFrameBinding\(\{/gu) || []).length, 2);
  assert.match(layout, /window\.KenigEventsPlanRelatedCardRows\(ranked,/u);
  assert.match(layout, /window\.KenigEventsPackRelatedCardRows\(ranked,/u);
  assert.match(layout, /const presentation = feed\?\.dataset\.adaptiveGridMode === 'flow' \? 'flow' : 'packed'/u);
  assert.match(layout, /store\.ranked = composeRankedForFraming\(semanticallyRanked, feed, \{ preserveOrder:alreadyComposed \}\)/u);
  assert.match(layout, /applyFeedbackState\(\{ skipDiscoveryHydration:true \}\)/u);
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
