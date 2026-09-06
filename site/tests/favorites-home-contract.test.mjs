import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  joinFutureSavedEvents,
  mergeSavedEventRefs,
  readLocalSavedEventInputs,
} from '../src/lib/favorites.mjs';
import {
  homeProfileSignalCount,
  rankHomeFeedItems,
} from '../src/lib/homeFeed.mjs';
import {
  buildSavedEventReconciliationPlan,
  localSavedEventState,
} from '../src/lib/savedEventRuntimeCore.mjs';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('favorites merge is stable, deduplicated, calendar-first and future-only', () => {
  const local = readLocalSavedEventInputs({
    getItem(key) {
      if (key === 'ke_calendar_saved_v1') return JSON.stringify({ v: 1, e: { 11: 99999, 10: 99999 } });
      if (key === 'ke_personalization_profile') return JSON.stringify({
        consent_ok: true,
        liked_event_ids: [10, 12, 13],
      });
      return null;
    },
  });
  const refs = mergeSavedEventRefs({
    remoteRows: [
      { event_id: 12, favorite_saved: true, favorite_added_at: '2026-07-27T12:00:00Z' },
      { event_id: 11, calendar_saved: true, calendar_added_at: '2026-07-27T13:00:00Z' },
    ],
    calendarIds: local.calendarIds,
    likedEventIds: local.likedEventIds,
  });
  assert.deepEqual(refs.map((item) => item.eventId), ['11', '10', '12', '13']);
  assert.equal(refs.filter((item) => item.eventId === '11').length, 1);
  assert.equal(refs.find((item) => item.eventId === '10')?.source, 'calendar');

  const joined = joinFutureSavedEvents({
    related_static: [
      { event_id: 10, date: '2026-07-28' },
      { event_id: 11, date: '2026-07-26' },
      { event_id: 12, date: '2026-07-27' },
      { event_id: 13, date: '2026-08-01' },
    ],
  }, refs, '2026-07-27');
  assert.deepEqual(joined.map(({ saved }) => saved.eventId), ['10', '12', '13']);
});

test('home stays static without a compatible profile and progressively reranks local signals', () => {
  const items = [
    { id: 1, baseRank: 0, category: 'music', tags: ['music'] },
    { id: 2, baseRank: 1, category: 'lecture', tags: ['lecture'] },
    { id: 3, baseRank: 2, category: 'music', tags: ['music'] },
  ];
  assert.deepEqual(rankHomeFeedItems(items, null).map((item) => item.id), [1, 2, 3]);
  const profile = {
    consent_ok: true,
    profile_version: 'anon-profile-v1',
    feature_schema_version: 'event-detail-related-v1',
    taxonomy_version: 'event-taxonomy-v1',
    anon_id: '11111111-1111-4111-8111-111111111111',
    session_id: '22222222-2222-4222-8222-222222222222',
    liked_event_ids: [3,4,5],
    not_interested_event_ids: [1],
    hidden_event_ids: [],
    positive_tags: { music: 1.2 },
    negative_interest_tags: {},
    share_counts: {},
  };
  assert.equal(homeProfileSignalCount(profile), 4);
  assert.deepEqual(rankHomeFeedItems(items, profile).map((item) => item.id), [3, 2]);
});

test('post-action local state drives durable like/unlike and calendar add/remove reconciliation', () => {
  const state = {
    calendar: { v: 1, e: {} },
    profile: { consent_ok: true, liked_event_ids: [] },
  };
  const storage = {
    getItem(key) {
      if (key === 'ke_calendar_saved_v1') return JSON.stringify(state.calendar);
      if (key === 'ke_personalization_profile') return JSON.stringify(state.profile);
      return null;
    },
  };

  assert.equal(localSavedEventState(storage, 'favorite', 41), false);
  state.profile.liked_event_ids.push(41);
  assert.equal(localSavedEventState(storage, 'favorite', 41), true);
  state.profile.liked_event_ids = [];
  assert.equal(localSavedEventState(storage, 'favorite', 41), false);

  assert.equal(localSavedEventState(storage, 'calendar', 42), false);
  state.calendar.e[42] = 99999;
  assert.equal(localSavedEventState(storage, 'calendar', 42), true);
  delete state.calendar.e[42];
  assert.equal(localSavedEventState(storage, 'calendar', 42), false);

  state.calendar.e = { 44: 99999, 43: 99999 };
  state.profile.liked_event_ids = [43, 45];
  assert.deepEqual(buildSavedEventReconciliationPlan(storage), [
    { eventId: 44, source: 'calendar', saved: true },
    { eventId: 43, source: 'calendar', saved: true },
    { eventId: 45, source: 'favorite', saved: true },
    { eventId: 43, source: 'favorite', saved: true },
  ]);
});

test('favorites and home source contracts use canonical adaptive/card runtimes and never browser secrets', async () => {
  const [page, surface, home, homeFeed, authRuntime, savedRuntime, migration] = await Promise.all([
    read('src/pages/izbrannoe/index.astro'),
    read('src/components/FavoritesSurface.astro'),
    read('src/pages/index.astro'),
    read('src/components/HomeColdStartFeed.astro'),
    read('src/components/auth/StaticSiteAuthRuntime.astro'),
    read('src/lib/savedEventRuntime.ts'),
    read('../supabase/migrations/20260727141820_durable_saved_events_v1.sql'),
  ]);
  assert.match(page, /<EventLayout[\s\S]*\bnoindex\b/u);
  assert.match(page, /data-favorites-page/u);
  assert.match(surface, /data-ds-family="FavoritesSurface"/u);
  assert.match(surface, /data-favorites-skeleton/u);
  assert.match(surface, /<AdaptiveEventCardGrid[\s\S]*mode="flow"[\s\S]*rowSize=\{3\}[\s\S]*responsive="progressive"/u);
  assert.match(surface, /'data-favorites-grid'\s*:\s*''/u);
  assert.match(surface, /'data-favorites-runtime-host'\s*:\s*'adaptive-v1'/u);
  assert.doesNotMatch(surface, /syncAdaptiveDiagnostics/u,
    'AdaptiveEventCardGrid is the only diagnostics writer for Favorites');
  assert.match(surface, /KenigEventsCreateEventCard/u);
  assert.match(surface, /joinFutureSavedEvents/u);
  assert.doesNotMatch(surface, /\.favorites-surface__grid\s*\{[\s\S]*grid-template-columns/u,
    'FavoritesSurface must not own a second event-card grid');
  assert.match(authRuntime, /PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY/u);
  assert.match(savedRuntime, /setDurableSavedEvent/u);
  assert.match(savedRuntime, /settleAfterLocalCommit/u);
  assert.match(savedRuntime, /reconcileLocalState/u);
  assert.match(savedRuntime, /data-feedback-action="like"/u);
  assert.doesNotMatch([surface, authRuntime, savedRuntime].join('\n'), /PERSONALIZATION_SUPABASE_SECRET_KEY|service_role|sb_secret_/u);

  assert.match(home, /data-home-static-fallback-count/u);
  assert.match(homeFeed, /<AdaptiveEventCardGrid/u);
  assert.match(homeFeed, /mode="flow"/u);
  assert.match(homeFeed, /limit=\{30\}/u);
  assert.match(homeFeed, /'data-home-feed-limit'\s*:\s*'30'/u);
  assert.match(homeFeed, /itemRoots=\{itemRoots\}/u);
  assert.doesNotMatch(homeFeed, /<EventCard\b/u);
  assert.doesNotMatch(homeFeed, /\bfetch\s*\(/u);
  assert.doesNotMatch(homeFeed, /\brpc\s*\(|LLM|provider/iu);

  assert.match(migration, /alter table public\.user_saved_event enable row level security/u);
  assert.match(migration, /with \(security_invoker = true\)/u);
  assert.match(migration, /security invoker/u);
  assert.match(migration, /to authenticated/u);
  assert.doesNotMatch(migration, /auth\.role\(\)/u);
});


test('Home supplies the complete eligible pool to shared ranking before its finite budget', async () => {
  const [home, feed] = await Promise.all([read('src/pages/index.astro'), read('src/components/HomeColdStartFeed.astro')]);
  assert.doesNotMatch(home, /if\s*\(feed\.length\s*>=\s*30\)\s*break/u);
  assert.match(home, /data-home-eligible-count=\{feed\.length\}/u);
  assert.doesNotMatch(feed, /events\.slice\(0,\s*30\)/u);
  assert.match(feed, /rankHomeCandidates\(candidates\)/u);
  assert.match(feed, /events=\{eligibleEvents\}/u);
  assert.match(feed, /limit=\{30\}/u);
  assert.match(feed, /bindHomeFeeds/u);
});
