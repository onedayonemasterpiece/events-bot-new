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

### 4. Cross-platform identity

Link likely same source identity across platforms using same title/handle and explicit description links such as “YouTube, VK, RUTUBE, MAX”. Keep links as evidence; do not merge destructively without confidence.

### 5. Regional mention expansion

When a monitored source posts about Kaliningrad, inspect linked/mentioned authors and photo credits as possible new source candidates.

### 6. Catalog-neighbor expansion

From a good source, discover nearby/related catalog entries, but keep them `candidate` until scored.

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

- Crawl only allowlisted catalogs/seeds and accepted candidates.
- Keep `last_checked_at`, `next_fetch_after`, `consecutive_errors` and `status`.
- Broken/private/forbidden source does not fail the run.
- Do not use role-scoped Telegram auth bundles outside their intended context.
- Do not borrow E2E/human-session auth for Kaggle discovery.
