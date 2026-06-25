# Smart Update Contract for Personalization

> **Status:** design decision  
> **Decision:** personalization does **not** change Smart Update matching/extraction semantics in MVP.

## Summary

Personalization is a presentation/ranking layer over already accepted canonical events. It can consume Smart Update outputs, but it must not feed anonymous behavior back into Smart Update decisions.

## What personalization may consume

From Fly SQLite / Smart Update accepted state:

- `event.id`, `title`, `date`, `time`, `city`, `location_name`, `location_address`;
- `event_type`, `topics`, `tourist_label`, `is_free`, `ticket_status`;
- `short_description`, `search_digest`, public image/Telegraph/static URL;
- `linked_event_ids` for related dates;
- lifecycle/status fields: cancelled/postponed/sold out/active;
- content hash / update timestamp to rebuild feature snapshots.

These fields become `event_feature_snapshot` and same-origin static recommendation manifests.

## What personalization must not do

Personalization telemetry must not influence:

- dedup/match/create decisions;
- date/time/location/title/source repairs;
- source trust or default venue choice;
- event lifecycle cancellation/postponement decisions;
- LLM-first semantic extraction in Smart Update;
- public event descriptions except through the normal Smart Update pipeline.

Reason: clicks and hides are user preference signals, not factual evidence. Using them inside Smart Update would create feedback loops: popular but wrong events could become harder to repair, while niche but correct events could be suppressed.

## Requirements that personalization adds to Smart Update

MVP adds only export/observability requirements, not extraction logic requirements:

1. **Stable feature export trigger.** After Smart Update commits a canonical event, schedule/update `static_event_export` and `event_feature_snapshot` if fields relevant for ranking changed.
2. **Stable slug/source hash.** Personalization snapshots must include `event_id`, stable slug, `source_hash/content_hash`, and `built_at` so stale recommendations can be invalidated.
3. **Quality warnings are downstream-only.** Offline LLM enrichment may emit `quality_warnings` such as `type_description_mismatch`, but those warnings do not repair the Event row. They can open an admin review or Smart Update replay task.
4. **Lifecycle propagation.** Cancelled/postponed/sold-out changes must remove or down-rank events in manifests without waiting for personalization aggregation.
5. **No raw telemetry in core DB.** Fly SQLite remains clean from visitor/session/profile tables.

## Optional future feedback loop

A future product loop may surface aggregated, privacy-safe insights to admins, for example:

- many users hide an event because it is sold out;
- high ticket-click failure rate for one venue/source;
- frequent quick-skip on events with misleading title/image.

Even then, the loop must create a review signal only. A Smart Update/LLM-first repair still needs source evidence and regression checks.

## Regression checks

Before shipping personalization alongside Smart Update:

- run Smart Update fixture tests unchanged;
- verify `event_feature_snapshot` export does not mutate `event` rows;
- verify a telemetry flood cannot change dedup/match outcome;
- verify cancelled/postponed event disappears from active personalized feed after static rebuild;
- verify feature warnings are visible in debug but do not auto-edit title/date/venue.
