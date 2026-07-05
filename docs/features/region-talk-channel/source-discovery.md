# Source discovery — Region Talk Channel

Status: design for MVP-1. This contour discovers external Russian-language sources that may publish strong, useful posts about Kaliningrad Oblast.

## Inputs / seed sources

- Manual seed Telegram/VK/web URLs curated by the operator.
- Telega.in travel/tourism catalogs and TGStat travel/blogger categories.
- VK travel, architecture, history, nature and city-life groups.
- Links, mentions, reposts and forwards found in monitored posts.
- Web lists of Russian travel channels/bloggers.

Personal profiles are not a primary MVP source. They may appear as `candidate` only if public, author-like, clearly non-private and manually accepted later.

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
