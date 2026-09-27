import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('Live search page is a messenger conversation with a large mic and canonical responsive cards', async () => {
  const [page, surface, controller] = await Promise.all([
    read('../src/pages/poisk/index.astro'),
    read('../src/components/ConversationalEventSearch.astro'),
    read('../src/lib/liveEventSearch.js'),
  ]);

  assert.match(page, /PUBLIC_STATIC_SITE_LIVE_SEARCH_URL/u);
  assert.match(page, /<ConversationalEventSearch/u);

  assert.match(surface, /data-conversation-feed/u);
  assert.match(surface, /conversation-search__user-message/u);
  assert.match(surface, /conversation-search__assistant-message/u);
  assert.match(surface, /data-conversation-mic/u);
  assert.match(surface, /width:88px;\s*height:88px/u);
  assert.match(surface, /conversation-search__mic\.is-active[\s\S]*conversation-mic-pulse/u);
  assert.match(surface, /conversation-search__skeleton-cards[\s\S]*repeat\(3,minmax\(0,1fr\)\)/u);
  assert.match(surface, /import OptimizedEventCardGrid/u);
  assert.match(surface, /<OptimizedEventCardGrid[\s\S]*responsiveMobile/u);
  assert.match(surface, /data-conversation-card-grid-template/u);
  assert.match(surface, /gridTemplate\.content\.firstElementChild\?\.cloneNode\(true\)/u);
  assert.match(surface, /KenigEventsCreateEventCard/u);
  assert.match(surface, /packRelatedCardRows/u);
  assert.doesNotMatch(surface, /\.conversation-search__cards\s*\{/u);
  assert.match(surface, /onTranscript:\s*appendTranscript/u);
  assert.match(surface, /scrollIntoView/u);

  assert.match(controller, /input_transcript/u);
  assert.match(controller, /output_transcript/u);
  assert.match(controller, /onTurnComplete/u);
  assert.match(controller, /function mute\(\)/u);
});

test('conversation cards remain the shared EventCard renderer instead of a bespoke search card', async () => {
  const surface = await read('../src/components/ConversationalEventSearch.astro');
  assert.doesNotMatch(surface, /<article[^>]+conversation-search__event-card/u);
  assert.match(surface, /createCard\(item, 'split-actions', layout\)/u);
});
