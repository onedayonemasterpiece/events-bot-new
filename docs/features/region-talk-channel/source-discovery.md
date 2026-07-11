# Source discovery — Region Talk Channel

Status: design for MVP-1. This contour discovers external Russian-language sources that may publish strong, useful posts about Kaliningrad Oblast.

## Inputs / seed sources

Canonical seed files for MVP-1:

- human-readable: [`seed-sources-v1.md`](seed-sources-v1.md);
- machine-readable: [`seed-sources-v1.csv`](seed-sources-v1.csv).

Other discovery inputs:

- Telega.in travel/tourism catalogs;
- TLGRM travel category;
- TGStat travel category;
- VK Video travel selections;
- VK Места / travel hubs;
- links, mentions, reposts and forwards found in monitored posts;
- web lists of Russian travel channels/bloggers.

Personal profiles are not a primary MVP source. They may appear as `candidate` only if public, author-like, clearly non-private and manually accepted later.

## Seed list + graph expansion

Pipeline:

1. Load manual/catalog seed list.
2. Normalize source identity:
   - `platform`;
   - `handle`;
   - `canonical_url`;
   - `title`;
   - `avatar`;
   - `description`.
3. Create `region_talk_source_candidate` rows in YDB.
4. Do **not** enable monitoring automatically for all seeds. Discovery sources and monitored sources are different concepts.
5. For accepted / monitor candidates:
   - inspect recent `N` posts;
   - extract links, mentions, forwards, reposts;
   - extract source attribution from post text;
   - extract VK/TG/website/social links from descriptions;
   - create `region_talk_source_graph_edge` rows.
6. New sources from graph remain `candidate` until scored/reviewed.
7. Score each new source with `externality_score`, `source_type_score`, `travel_relevance_score`, `media_quality_prior`, `originality_score`, `newsiness_penalty`, `trash_penalty`, `ad_penalty`, `rights_risk_penalty`.
8. Export source discovery to XLSX sheets: `11_sources_seed`, `12_sources_discovered`, `source_graph_edges` or graph columns in `12_sources_discovered`, and `source_review_queue` / `04_review_queue` source rows.

Catalog entries can be used for discovery even if they are not monitored for posts.

## New source discovery method

### 1. Catalog crawl

Use catalogs as discovery sources, not as automatic allowlists:

- Telega.in travel/tourism;
- TLGRM travel;
- TGStat travel;
- VK Video travel selections;
- VK Места / travel hubs.

### 2. Source description links

Extract Telegram/VK/YouTube/Rutube/Dzen/web/MAX links from source profiles/descriptions where available.

### 3. Post link graph

Extract:

- `t.me` links;
- `vk.com` links;
- repost attribution;
- `forwarded_from` metadata;
- mentions in text;
- “source/photo author” lines.

Telegram rich-text targets are source evidence even when the visible caption
contains only an unprefixed label. CandidateReport reads public
`MessageEntityTextUrl.url` values (and public HTML anchor targets in the
disabled fallback implementation), stores a bounded `embedded_links_json`, and
emits `edge_type=media_attribution` for `Фото:`, `Видео:` and `Источник:`
credits. This prevents a credit such as
`Видео: [moresvobod](https://t.me/moresvobod)` from degrading to the
non-discoverable plain word `moresvobod`. Only public URLs/labels are persisted;
Telegram private entity identifiers remain in the separate private cache.
The canonical queue preserves `added_from`, `discovery_types` and
`edge_types_all` across later monitored-source overlays; otherwise the next
run would erase the attribution priority before an uncached source could reach
the single controlled resolve lane.

### 4. Cross-platform identity

Link likely same source identity across platforms using same title/handle and explicit description links such as “YouTube, VK, RUTUBE, MAX”. Keep links as evidence; do not merge destructively without confidence.

### 5. Regional mention expansion

When a monitored source posts about Kaliningrad, inspect linked/mentioned authors and photo credits as possible new source candidates.

### 6. Catalog-neighbor expansion

From a good source, discover nearby/related catalog entries, but keep them `candidate` until scored.

### 6a. Keyword / hashtag search expansion

Telegram global search is a source-discovery signal, not a post acceptance
signal. Query banks are generated from
`kaliningrad-place-lexicon-v1.csv`, not from four hand-written place names:

- global keyword search gets a small rotating slice of travel-intent phrases
  and safe core/tourist/important toponyms;
- global hashtag search gets a separate small rotating slice from the lexicon
  hashtag bank (`#Калининград`, `#Балтийск`, `#Черняховск`,
  `#Куршскаякоса`, `#Рыбнаядеревня`, `#Виштынецкоеозеро`, ...);
- source-local preflight search may use the much broader lexicon bank because
  it is scoped to one already known channel.

A source found this way must be either:

- admitted once into the canonical ledger with immutable `queue_seq` and given
  a high-priority due reason when it is a plausible external source, so the
  next short run selects it before generic tail backlog without renumbering the
  whole queue;
- terminally routed to the local-source list when title/handle clearly says it
  is a Kaliningrad-local public (`Калининград`/`kaliningrad`, `Кёниг`/`kenig`,
  `kgd`/`kld`/`klgd`, `39`, regional towns/resorts);
- terminally routed to the spam-source list when the title/excerpt matches
  repeated hashtag-spam/commercial bait (`ты не сможешь...`, `VPN`,
  `промокод`, crypto/trading, betting/casino/bonus, cheap-flight feeds) or
  repeated spoiler/hidden-text posts.

The live 2026-07-09 local E2E probe over
`#Калининград|#калининградскаяобласть|#Зеленоградск|#Светлогорск|#КуршскаяКоса|#Балтийск|#Янтарный`
returned 210 raw results but only 7 unique channels for that account: 178
local-region results, 31 spam/commercial-like results and 1 plausible external
candidate (`Кот с рюкзаком`). A follow-up channel scan hit a large Telegram
`FloodWait` on username resolves. That follow-up was an ad-hoc local research
script without the production request governor; production code must route
global search, resolves and source-local preflight through the shared
`TelegramRequestGovernor`, entity cache and human-like pacing, do the cheap
surface classification before resolving/scanning channels, and must not spend
history budget on obvious local/spam hashtag hits.

### 6b. Source-local fast-check-KO preflight search

When a new source enters YDB from a catalog, similar-channel edge, keyword hit
or hashtag hit, the next cheap step is not a deep history crawl. CandidateReport
therefore has a bounded `fast-check-KO` pass over the existing
`unified_source_queue` backlog after the current cursor. This is **not** a
second source queue: it reads rows from the same YDB `source_queue_item`, writes
`fast_check_*` evidence back to the same row, and uses the normal queue selector
to promote KO hits in the same ledger without creating a second source queue.

Per run, the orchestrator keeps this intentionally conservative:

- `REGION_TALK_FAST_CHECK_KO_ENABLED=1`;
- `REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN=5` in iterative debug runs;
- `REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE=2`;
- `REGION_TALK_FAST_CHECK_KO_RESULTS_PER_QUERY=2`;
- all resolves/searches go through `TelegramRequestGovernor` plus human-like
  pacing.

Algorithm:

1. surface-filter title/handle/recent evidence for local-region and spam
   terminal statuses;
2. resolve the channel through the shared governor/cache only if needed;
3. search the channel for `Калининград` first, then one rotating
   town/POI/toponym from the broader lexicon (`Зеленоградск`, `Куршская коса`,
   `Светлогорск`, `Балтийск`, ...);
4. stop early on the first fresh hit (`post_date >= now-365d`);
5. persist `matched_query`, `keyword_hit_post_url`, `post_date`,
   `preflight_hit_age_days`, `keyword_evidence_excerpt` and `fast_check_status`.

If a fresh hit is found, the source receives an immediate priority/due marker
in the canonical ledger and the exact post URL must be written to
a known-post queue (`post_link_queue_item` / generalized candidate-link fetch)
so that the post itself is scored next instead of being only a source hint. If
no hit is found, `fast_check_status=no_hit` is not terminal: the source remains
eligible for normal history scanning later, but is not rechecked by fast-check
and is deprioritized behind un-preflighted backlog. If the hit is older than one
year, keep it as evidence and lower-priority backlog; do not jump it immediately
ahead of fresh sources.

Implementation invariants:

- hashtag rows use `discovery_type=edge_type=telegram_hashtag_search` and stay
  source/context evidence only;
- exact post-link rows are not allowed to bypass source hygiene: before writing
  or fetching a `post_link_queue_item`, the same local/spam surface classifier is
  applied. Obvious Kaliningrad-local or spam sources are written as terminal
  post-link rows (`terminal_source_rejected`) instead of `pending_fetch`, so
  they remain auditable but do not consume Telethon exact-fetch or entity-cache
  budget;
- local/spam terminal rows remain visible in `source_queue_item` with
  `rejected_local_region_source` or `rejected_spam_source`, but are not selected
  for history scans;
- local-region is a terminal source-level decision, not just an annotation: if
  a row already has KO/candidate counters but later gets
  `source_scope=local_region`, `source_geo_class=kaliningrad_local` or
  `source_quick_class=local_region_source`, the queue status must still become
  `rejected_local_region_source`. YDB merge must not let older
  `processed_found_ko_candidate` status rows overwrite that terminal local
  decision.
- uncertain rows are kept for the normal semantic/vector gate rather than
  rejected by regex;
- `fast_check_status=ko_hit` is the only source-local preflight state that gets
  keyword priority; `no_hit`/`error` must not be promoted just because the
  internal stage name contains “keyword”;
- a keyword/preflight post hit is not a final candidate, but its URL must not be
  lost: CandidateReport writes it as `post_link_queue_item`, and the next normal
  CandidateReport run first refetches a bounded batch of these exact links
  (`REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST=1`) before spending history-scan
  budget; the baseline is three links, but the orchestrator raises the batch to
  at most eight when the actionable queue already has cached
  `channel_id/access_hash`, without raising the one-username-resolve budget;
  fetched links then pass the normal E5+BGE/text/image/LLM funnel;
- permanent exact-link identity failures (`ChannelInvalid`, invalid/unoccupied
  username/peer) become `terminal_invalid_public_post_source` instead of
  retrying forever at the head of the high-priority lane; FloodWait and
  transient RPC failures remain retryable;
- every keyword/hashtag/fast-check priority change is persisted as
  `priority_lane=ko_keyword_or_fast_check` in the same YDB source ledger.
  Neither immutable `queue_seq` nor legacy `queue_order` is rewritten merely
  to consume that source before generic backlog.

Debug-run budget after the 2026-07-09 long-run incident:

- CandidateReport orchestrator launches use `--max-sources 6`,
  `REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS=1200`, 20 posts/source, three exact
  post-link refetches by default (adaptive cached-only ceiling eight), four
  global keyword/hashtag queries and three
  similar-channel seeds;
- the run should reach exact post-link fetch, fast-check-KO, a small history
  scan, E5 write and source-queue handoff, then stop before heavy report tail;
- YDB online discovery writes are capped per run so source-frontier assembly
  cannot block the next orchestration cycle on hundreds of row upserts.
- Public `t.me/s` scraping is disabled in orchestrator debug/product cycles:
  if Telethon cannot resolve/fetch or returns `FloodWait`, the run records the
  concrete Telethon reason/cooldown and defers later Telegram phases instead of
  masking the blocker with a slow web fallback.
- Exact post-link fetch and source-local fast-check must use the private
  Telegram entity cache first. `channel_id/access_hash` are saved only in
  private state/YDB payloads when observed through successful resolve, Telegram
  similar-channel results or global keyword search chat metadata; public XLSX
  rows may expose only `private_state_key`. If a source has no cached entity,
  username resolve is a scarce operation and must be budgeted separately.
- Orchestrated CandidateReport cycles run in `REGION_TALK_TG_CACHED_ENTITY_ONLY=1`
  with `REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN=0`: exact post-link fetch
  and fast-check skip cache-miss rows as `retry_wait_entity_cache` /
  `skipped_cached_entity_only_no_private_entity` instead of calling
  `client.get_entity(handle)`. Any non-zero network resolve count in this mode is
  a run-contract violation.
- A `get_entity` FloodWait from exact post-link fetch is treated as a username
  resolve cooldown for source-local fast-check/history too. Cached
  `InputPeerChannel(channel_id, access_hash)` reads are still allowed during
  this cooldown, so the backlog selector should prefer sources with cached
  entities instead of blocking all Telethon work on uncached post-link rows.
- These limits must be present in the launcher-written
  `region_talk_run_config.json`, not only in the local orchestrator dry-run
  action. Otherwise Kaggle silently falls back to notebook defaults and the live
  run no longer matches the orchestrator plan.

### 7. Public travel blogger catalog import

The operator-provided workbook `public_travel_blogger_channel_links.xlsx` is a discovery input, not an allowlist and not a monitored-source switch. The Kaggle launcher copies it into the private input dataset when present, and the runner can also read it from `REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE`. Rows are normalized from columns such as `Platform`, `Handle`, `URL`, `Type`, `Category`, `Source`, `Source page`, `Collected on`, `Notes` and exported as source-frontier candidates with `edge_type=public_travel_blogger_catalog`.

Rules:

- import Telegram/VK/VKVideo/web links into the source frontier only;
- do not fetch posts from catalog rows in the same run unless the source is also selected by seed/monitoring policy;
- keep the original public catalog evidence (`source_page`, `catalog_source`, notes) but never add private Telegram ids/access hashes to XLSX;
- de-dupe catalog rows against seeds, graph edges and similar-channel recommendations in `12a_source_frontier_unique`.

### 8. Telegram Similar Channels expansion

For already resolved Telegram seed/monitor sources, the runner may call Telegram's similar/recommended channel API (`channels.getChannelRecommendations`) through Telethon when the installed Telethon version supports it. Similar-channel results are source-discovery edges only:

- `edge_type=telegram_similar_channel`;
- `discovery_type=telegram_similar_channels`;
- `frontier_action=add_to_source_frontier`;
- no auto-subscribe, no auto-join, no participant scraping, no private/invite-only channels;
- recommendation results become `candidate` frontier rows and require later scoring/manual acceptance before monitoring.

MVP-1.z6 adds recursive but bounded seed accumulation: every resolved/fetched Telegram channel can be written to `12d_similar_seed_queue` and used as a seed in later recommendation passes. Frontier rows from previous state may be promoted to dynamic shallow probes (`REGION_TALK_MAX_NEW_SOURCE_PROBES`) and the per-source cursor evidence is visible in `13b_source_delta_scan`.

If Telethon does not expose the request class, the run must report `telegram_similar_channels_status=not_supported_by_telethon_version` instead of failing or emulating it with scraping.

Every discovered source must include:

- `discovered_from`;
- `evidence_url`;
- `evidence_text`;
- `confidence`;
- `why_candidate`;
- `risk_flags`.

## Priority order

1. Channels/bloggers over personal profiles.
2. Author/travel channels over news channels.
3. External non-Kaliningrad sources over local/regional sources.
4. Sources with original photos/texts over repost farms.
5. Sources with diverse geography and occasional Kaliningrad posts over sources that only publish local regional material.

## Source candidate scoring

Suggested fields, stored in `region_talk_source_candidate`:

```text
source_quality_score =
  0.18 * source_type_score
+ 0.18 * externality_score
+ 0.16 * originality_score
+ 0.16 * travel_relevance_score
+ 0.12 * image_quality_prior
+ 0.08 * engagement_health_score
+ 0.06 * language_fit_score
+ 0.06 * graph_trust_score
- newsiness_penalty
- trash_penalty
- ad_penalty
- rights_risk_penalty
```

Scoring is evidence-driven. A high subscriber count alone is not enough.

## Accept / reject policy

Accept/monitor when:

- language is Russian or mostly Russian;
- source scope is `external` or `mixed`, not primarily Kaliningrad local news;
- topics include travel, architecture, history, nature, city walks, weekend routes, food or photogenic places;
- source posts original or well-attributed content;
- media style likely supports strong photos;
- rights policy can be inferred enough for report/link-only or later publication.

Reject or block when:

- regional Kaliningrad news, incident channels, police/crime/trash, politics/war/military agenda;
- federal news wire or repost-only aggregators;
- pure ads/catalogs/coupon feeds;
- low-quality memes/screenshots;
- questionable rights behavior or blocked source.

## Discovery graph

Store every link/mention/repost/catalog-neighbor relationship in `region_talk_source_graph_edge` with evidence URL/text and confidence. New sources discovered from graph edges remain `candidate` until reviewed.

## Safety and rate limits

Telegram discovery must be human-like in the P0 sense: conservative, cache-first and rate-aware. This means avoiding wasteful API calls and respecting Telegram limits; it does **not** mean evasion.

- Crawl only allowlisted catalogs/seeds and accepted candidates.
- Keep `last_checked_at`, `next_fetch_after`, `consecutive_errors` and `status`.
- Broken/private/forbidden source does not fail the run.
- Do not use role-scoped Telegram auth bundles outside their intended context.
- Do not borrow E2E/human-session auth for Kaggle discovery unless the operator explicitly overrides the session plan for that run.
- Resolve Telegram entities through the shared request governor/cache first; network username resolves are capped (`REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN`, default `8`, orchestrator live-YDB cycles set `0`). During a resolve cooldown, cache hits must be attempted before the cooldown gate because they do not call `ResolveUsernameRequest`; in cached-entity-only mode cache misses are deferred rather than resolved.
- Telethon network calls are paced by default (`REGION_TALK_TG_HUMANLIKE_PACING_ENABLED=1`): username resolves, exact post-link refetches from `post_link_queue_item`, global keyword/hashtag search and similar-channel recommendation calls sleep before the call, history queries/media downloads/source-to-source scans use bounded pauses, and the call is deferred instead of skipping the pause when the runtime reserve is nearly exhausted. Public `t.me/s` fallback reads do not consume the Telethon user session and are not part of this pacing; they are disabled for orchestrated live-YDB cycles and may only be used as an explicit diagnostic fallback.
- Cap history sources and media downloads (`REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN`, default `40`; `REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN`, default `60`).
- In short live-YDB handoff runs, fetched posts are sorted before vector scoring by Kaliningrad place evidence/media presence (`REGION_TALK_PRIORITIZE_REGION_TEXT_BEFORE_VECTOR=1`) so the limited embedding budget is spent on likely region posts first.
- Large `FloodWait` values (default threshold `300` seconds) are not slept through: record a cooldown/degraded mode, skip later resolves/history that would hit the same method/source, and still write the XLSX report.
- Keep the private entity cache/cooldown ledger in state/artifacts only; public XLSX sheets may contain `private_state_key`, but never raw `channel_id`, `access_hash`, session strings or tokens.
- Forbidden: multi-account/proxy rotation, automatic joins/subscriptions, participant scraping, private/invite-only source traversal, or any publication from this MVP report.

Default live/Kaggle Telegram discovery limits for z7 growth runs:

```bash
REGION_TALK_TG_GOVERNOR_ENABLED=1
REGION_TALK_DISCOVERY_MODE=mixed
REGION_TALK_HISTORY_SCAN_MODE=primary_and_delta
REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN=800
REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN=8
REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN=100
REGION_TALK_HISTORY_SOURCES_TARGET=100
REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE=25
REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN=120
REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN=100
REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN=100
REGION_TALK_MAX_NEW_SOURCE_PROBES=30
REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS=300
REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS=1800
REGION_TALK_TG_HUMANLIKE_PACING_ENABLED=1
REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS=20
REGION_TALK_TG_RESOLVE_DELAY_MAX_SECONDS=45
REGION_TALK_TG_KEYWORD_QUERY_DELAY_MIN_SECONDS=6
REGION_TALK_TG_KEYWORD_QUERY_DELAY_MAX_SECONDS=14
REGION_TALK_TG_SIMILAR_DELAY_MIN_SECONDS=20
REGION_TALK_TG_SIMILAR_DELAY_MAX_SECONDS=45
REGION_TALK_TG_HISTORY_QUERY_DELAY_MIN_SECONDS=2
REGION_TALK_TG_HISTORY_QUERY_DELAY_MAX_SECONDS=6
REGION_TALK_TG_MEDIA_DELAY_MIN_SECONDS=1
REGION_TALK_TG_MEDIA_DELAY_MAX_SECONDS=4
REGION_TALK_TG_SOURCE_PAUSE_MIN_SECONDS=4
REGION_TALK_TG_SOURCE_PAUSE_MAX_SECONDS=12
REGION_TALK_TG_SIMILAR_ENABLED=1
REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN=100
REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED=10
REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN=1000
REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY=1
REGION_TALK_TELEGRAM_QUERY_SOURCE=place_lexicon
REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES=30
REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES=18
REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN=12
REGION_TALK_TELEGRAM_QUERY_ROTATE=1
REGION_TALK_MEDIA_SCORING_MODE=retry_queue_first
REGION_TALK_ACTUAL_IMAGE_TARGET=30
```

Keyword discovery is source discovery only. It may record `matched_query`, public channel title/username/url and a source-candidate edge, but it must not store raw personal/comment data or treat the matched post as a content candidate outside the normal Region Talk funnel.

## z8 product-acceleration discovery defaults

The authoritative frontier is `region_talk_sources` / `source_frontier_unique` keyed by `canonical_source_key`, not separate raw catalog/similar/keyword lists. Catalog imports, Telegram similar recommendations, Telegram keyword-discovered channels, post-text links and forwarded/repost origins upsert into the same deduped source record and accumulate evidence.

Similar Channels remain non-recursive inside one run: select a round-robin/cooldown seed queue, call Telegram recommendations for the next bounded batch, reject self-loops/duplicates, update `similar_seed_*` cursor fields, then let later runs scan the newly found frontier. z8 target config is 200 seeds/run, 30 recommendations/seed, 2000 new frontier cap and 250 recommendation calls.

VK public/group wall sources are now first-class read-only scan candidates when a VK token is configured. Unsupported or missing-token VK rows stay visible in backlog metrics instead of disappearing from the catalog/frontier.


## MVP-1.x forwarded/repost/comment discovery hardening

Link-to-link discovery now treats forwarded/reposted origin as a high-value source graph edge:

- Telegram: `forwarded_from`, `forward_origin`, `fwd_from`, accessible original channel/post link, author/source attribution, links in forwarded text.
- VK: `copy_history`, repost source, original wall post link, attachment source links and photo credits.

Edge types: `forward_origin`, `repost_origin`, `copy_history_origin`, `post_text_link`, `comment_link`, `photo_credit`, `profile_link`, `catalog_neighbor`.

Discovered originals are added to source frontier/review, not monitored automatically. A source profile probe is required first.

Comments are never content candidates. They may be scanned only for source discovery with redacted evidence fields:

- `from_comment_id_hash`;
- `comment_text_redacted`;
- `extracted_url`;
- `extracted_handle`;
- `evidence_context_short`;
- `author_hash_optional`;
- `privacy_status=redacted`.

No raw comment bodies or personal ids should appear in public artifacts.

Depth/budget defaults for MVP-1.x:

```bash
REGION_TALK_MAX_DISCOVERED_LINKS_PER_RUN=3000
REGION_TALK_MAX_NEW_SOURCE_CANDIDATES_PER_RUN=800
REGION_TALK_MAX_COMMENTS_PER_POST_FOR_LINKS=50
REGION_TALK_MAX_SOURCE_PROFILE_FETCHES=300
REGION_TALK_MAX_SOURCE_PROBES=120
REGION_TALK_MAX_DISCOVERY_DEPTH_PER_RUN=2
```
