# Event reaction counters — source likes, service likes and static pages

> **Status:** architecture decision for the next implementation slice. Preview `v13` shows total counters from a committed fixture, hides zero like/share counts in UI, and still treats production scanner-to-Supabase sync plus counter manifests as not implemented yet.
> **Related DB:** separate personalization Supabase/Postgres project, not the core Fly SQLite database.

## Product rule

Cards show one simple public number: total likes. Users must not see technical wording such as “source likes” or “service likes”.

Internally the total is:

```text
likes_count = source_likes_count + service_likes_count
```

- `source_likes_count` — likes/reactions collected from the original Telegram/VK/source posts.
- `service_likes_count` — first-party likes made on `kenigevents.ru`.
- `likes_count` — generated/public total shown on cards and available to public Data API/manifest readers.

## Current status

Implemented in the preview:

- Supabase table `public.personalization_event_reaction_counter` exists and is seeded for the preview events.
- RLS/grants expose only public total columns: `event_id`, `likes_count`, `not_interested_count`, `share_count`, `updated_at`.
- Static preview cards render only the total number.

Not implemented yet:

- main bot scanners do **not** currently write source metrics into the new personalization DB;
- first-party site likes are local/preview-only and are not persisted to Supabase yet;
- static pages do not yet fetch a small live counter manifest after first paint.

## Recommended freshness architecture

Do **not** rebuild every static event page just because a like counter changed. Counter updates are high-frequency, small data; event pages are low-frequency, heavy artifacts.

Recommended hybrid:

1. **Core import/scanners stay authoritative for source metrics.** Existing Telegram/VK monitoring keeps writing post metrics into Fly SQLite (`telegram_post_metric`, `vk_post_metric`) and event-source mappings into canonical event tables.
2. **Non-blocking source-counter sync job** reads Fly SQLite, aggregates per `event_id`, and upserts only source fields into Supabase:
   - `source_likes_count`, `source_views_count`, `source_engagement_sources_count`, `source_refreshed_at`;
   - it must preserve `service_likes_count`.
3. **First-party reaction ingest** writes compact strong actions/state to Supabase through the selected safe write path, then updates `service_likes_count` aggregate.
4. **Small same-origin counter manifest** is generated frequently, e.g. every 5–15 minutes:

   ```text
   /data/reaction-counters/current.json
   /data/reaction-counters/YYYYMMDD-HHMM.json  (optional immutable history/debug)
   ```

   Shape:

   ```json
   {
     "generated_at": "2026-06-27T17:00:00Z",
     "version": 1,
     "counters": {
       "5878": {"likes": 9, "shares": 0, "not_interested": 0, "updated_at": "..."}
     }
   }
   ```

5. **Static HTML remains SEO/GEO-safe baseline.** Pages render build-time counters from the last known snapshot. After first paint, browser fetches the same-origin manifest and patches only counters if the manifest is newer.
6. **Full static rebuild** remains for content lifecycle changes: new/changed/cancelled events, image/SEO/schema changes, sitemap/robots/listing changes. Suggested cadence: on event import batch + 2–4 scheduled rebuilds/day as fallback, not on every counter change.

## Why not direct Supabase read from every page?

Direct Supabase Data API reads are acceptable as a debug/fallback path but should not be the default product path:

- every page view would depend on Supabase latency/availability;
- public free-tier traffic can grow faster than the tiny counter payload itself;
- same-origin manifests are easier to cache through the static bucket/CDN;
- manifest-first keeps static pages fast and resilient while still allowing counters to become fresh within minutes.

## Source-counter sync details

The sync job should run outside scanner hot paths. A Supabase outage must not break Telegram/VK monitoring or event import.

Aggregation contract:

- For each source post, use the latest/highest collected value across age buckets (`MAX(likes)`, `MAX(views)`) so repeated scans do not double-count.
- Then sum distinct source posts attached to the same `event_id`.
- Telegram mapping should prefer canonical `event_source.source_chat_username/source_message_id` or canonical `source_url` to join source metrics.
- VK mapping should use `vk_inbox_import_event` + `vk_inbox` + `vk_post_metric`, or a parsed canonical `event_source.source_url` fallback when the inbox mapping is absent.
- Upsert source fields only; never overwrite service fields.

Pseudo-upsert:

```sql
insert into public.personalization_event_reaction_counter(
  event_id,
  source_likes_count,
  source_views_count,
  source_engagement_sources_count,
  source_refreshed_at,
  updated_at
)
values (...)
on conflict (event_id) do update set
  source_likes_count = excluded.source_likes_count,
  source_views_count = excluded.source_views_count,
  source_engagement_sources_count = excluded.source_engagement_sources_count,
  source_refreshed_at = excluded.source_refreshed_at,
  updated_at = now();
```

## Free-tier storage guard

The long-lived table must be aggregates/state, not an infinite raw log.

Recommended tables:

1. `personalization_event_reaction_counter` — one row per event, long-lived aggregate.
2. `personalization_event_reaction_state` — one row per `(anon_id, event_id)`, current explicit state: liked/not_interested/share counters and timestamps.
3. `personalization_event_reaction_log` — append-only audit log for strong explicit actions only, short retention.
4. `personalization_visitor_reaction_daily` — compact per-anonymous-profile/day rollup for reports.

Retention/limits:

- raw explicit reaction log: 30 days by default, 90 days maximum only if DB size stays safe;
- state rows: keep while visitor profile is active, then delete/anonymize after profile retention window;
- daily rollups/counters: keep longer because they are compact;
- no weak impressions/scroll events in raw tables by default;
- dedupe by `client_event_id`, `(anon_id, event_id, action state)` and time bucket;
- per-anon quota: cap likes/shares/not-interested per minute/hour/day;
- emergency guard: if `pg_database_size` exceeds a configured threshold (e.g. 430–450 MiB on the 500 MB free tier), disable raw log writes first, keep only aggregate/state updates; if it approaches hard limit, fail closed to local-only reactions until cleanup succeeds.

This still allows the report “how many likes a concrete anonymous visitor made and when” from recent raw log + daily rollup, without retaining every toggle forever.

## UX rule for optimistic updates

The current card updates immediately in local state after a like/share. The card must not disappear or move because of that action. Server/manifest freshness affects later page loads or cards below the current anchor only.
