import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('EventCard publishes the canonical MediaFrame resource-state channel', async () => {
  const source = await read('src/components/EventCard.astro');

  assert.equal((source.match(/<article\b/gu) || []).length, 1, 'EventCard must keep one component root');
  assert.match(source, /data-ds-family="EventCard"/u);
  assert.match(source, /data-media-frame-resource-state="pending"/u);
  assert.match(source, /data-media-frame-resource-state="fallback"/u);
  assert.match(source, /data-media-frame-source-ratio=\{fixedLayoutMetric\(mediaFrameSourceRatio, 5\)\}/u);
  assert.doesNotMatch(source, /data-media-frame-state|dataset\.mediaFrameState/u);
});

test('EventCard load preserves crop evidence and broken resources fail closed', async () => {
  const source = await read('src/components/EventCard.astro');

  assert.match(source, /dataset\.mediaFrameResourceState='loaded'/u);
  assert.match(source, /removeAttribute\('data-media-frame-fallback'\)/u);
  assert.match(source, /dataset\.mediaFrameResourceState='broken'/u);
  assert.match(source, /dataset\.mediaFrameKind='fallback'/u);
  assert.match(source, /dataset\.mediaFrameFit='contain'/u);
  assert.match(source, /dataset\.mediaFrameCropPermission='forbidden'/u);
  assert.match(source, /dataset\.mediaFrameCropReason='resource_load_error'/u);
  assert.match(source, /dataset\.mediaFrameObjectPosition='50% 50%'/u);
  assert.match(source, /dataset\.mediaFrameFocalPosition='50% 50%'/u);
  assert.match(source, /setAttribute\('data-media-frame-fallback',''\)/u);
  assert.match(source, /style\.setProperty\('--media-frame-object-position','50% 50%'\)/u);
  assert.match(source, /removeAttribute\('src'\)/u);
  assert.match(source, /removeAttribute\('srcset'\)/u);
  assert.match(source, /classList\.add\('is-image-missing'\)/u);
  assert.match(source, /if\(!\(this\.naturalWidth>0&&this\.naturalHeight>0\)\)/u);
  assert.doesNotMatch(source, /object-fit:/u, 'EventCard must not reimplement canonical framing paint');
  assert.doesNotMatch(source, /object-position:/u, 'EventCard must not reimplement canonical framing paint');
});

test('EventCard keeps fallback, natural-aspect, action and metadata anatomy', async () => {
  const source = await read('src/components/EventCard.astro');

  assert.match(source, /data-card-image-fallback data-media-frame-fallback/u);
  assert.match(source, /dataset\.naturalAspectReconciled='true'/u);
  assert.match(source, /class="event-card__meta-row"/u);
  assert.match(source, /data-card-type/u);
  assert.match(source, /<EventOccurrenceLabel presentation=\{occurrencePresentation\} \/>/u);
  assert.match(source, /data-card-status/u);
  assert.match(source, /data-feedback-action="not_interested"/u);
  assert.match(source, /<SemanticIcon name="dislike" role="action" \/>/u);
  assert.match(source, /data-calendar-action/u);
  assert.match(source, /data-native-share/u);
  assert.match(source, /data-feedback-action="like"/u);
});
