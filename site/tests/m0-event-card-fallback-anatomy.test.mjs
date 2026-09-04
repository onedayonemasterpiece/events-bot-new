import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../src/components/EventCard.astro', import.meta.url), 'utf8');

function noUrlFallbackBranch() {
  const startMarker = '    ) : (\n      <span\n        class="event-card__media-shell';
  const start = source.indexOf(startMarker);
  const end = source.indexOf('    )}\n  </a>', start);
  assert.ok(start >= 0, 'missing EventCard no-URL fallback branch');
  assert.ok(end > start, 'missing EventCard no-URL fallback branch end');
  return source.slice(start, end);
}

test('no-URL EventCard keeps one card root and one direct fallback anatomy child', () => {
  const fallback = noUrlFallbackBranch();

  assert.match(fallback, /class="event-card__media-shell event-card__media-shell--cover event-card__fallback is-image-missing"/u);
  assert.match(
    fallback,
    />\s*<span class="event-card__image-fallback" data-card-image-fallback data-media-frame-fallback aria-hidden="true">/u,
  );
  assert.equal((fallback.match(/data-card-image-fallback/gu) || []).length, 1,
    'fallback branch must keep one canonical direct fallback child');
  assert.doesNotMatch(fallback, /<img\b/u,
    'no-source fallback anatomy must not retain an inert image resource');
  assert.equal((source.match(/<article\b/gu) || []).length, 1,
    'fallback anatomy must not introduce a second EventCard root');
});

test('EventCard diagnostics publish source geometry only from finite positive image evidence', () => {
  assert.match(source, /const mediaFrameSourceWidth = Number\(primaryImageAsset\?\.width\);/u);
  assert.match(source, /const mediaFrameSourceHeight = Number\(primaryImageAsset\?\.height\);/u);
  assert.match(source, /Number\.isFinite\(mediaFrameSourceWidth\)[\s\S]*mediaFrameSourceWidth > 0/u);
  assert.match(source, /Number\.isFinite\(mediaFrameSourceHeight\)[\s\S]*mediaFrameSourceHeight > 0/u);
  assert.match(source, /const mediaFrameSourceRatio = hasMediaFrameSourceDimensions[\s\S]*: undefined;/u);
  assert.doesNotMatch(
    source,
    /const mediaFrameSourceRatio =[\s\S]{0,220}desktopRelatedLayout\?\.mediaRatio/u,
    'row/frame geometry is not factual source-geometry evidence',
  );
  assert.match(source, /data-media-frame-source-width=\{hasMediaFrameSourceDimensions \? mediaFrameSourceWidth : undefined\}/u);
  assert.match(source, /data-media-frame-source-height=\{hasMediaFrameSourceDimensions \? mediaFrameSourceHeight : undefined\}/u);
  assert.match(source, /width=\{hasMediaFrameSourceDimensions \? mediaFrameSourceWidth : undefined\}/u);
  assert.match(source, /height=\{hasMediaFrameSourceDimensions \? mediaFrameSourceHeight : undefined\}/u);
});

test('the bounded anatomy correction preserves EventCard actions and metadata', () => {
  for (const marker of [
    'class="event-card__meta-row"',
    'data-card-type',
    '<EventOccurrenceLabel presentation={occurrencePresentation} />',
    'data-card-status',
    'data-feedback-action="not_interested"',
    'data-calendar-action',
    'data-native-share',
    'data-feedback-action="like"',
  ]) assert.ok(source.includes(marker), `EventCard lost ${marker}`);

  assert.doesNotMatch(source, /<(?:a|button)\b[^>]*data-media-frame/u,
    'MediaFrame root must remain noninteractive inside the caller-owned media link');
});
