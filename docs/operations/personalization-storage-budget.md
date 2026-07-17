# Personalization Supabase storage budget

> Status: **foundation measured; production tables not activated** (2026-07-17).
> Limit used for release planning: 500 MB decimal database size.

## Measured baseline

The redacted dual-DB probe against the personalization project on 2026-07-17 reported:

- current database: **37,948,563 bytes (~36 MB)**;
- headroom to 500,000,000 bytes: **462,051,437 bytes (~462 MB)**;
- largest relation: `public.event_embeddings`, ~17 MB;
- `email_control` is already present; `site_identity`/`saved_events` are not yet live.

Reproduce without printing secrets:

```bash
python3 .codex/skills/events-bot-dual-db/scripts/check_personalization_db.py --env .env
```

## Compactness and growth model

The new model stores references and state, not event text/media. Budget with indexes,
TOAST/page slack and audit rows (conservative planning numbers, not exact row sizes):

| Unit | Budget |
|---|---:|
| profile + active identity link + one device | 1.5 KiB/user |
| saved occurrence including indexes | 0.8 KiB/save |
| optional like/not-interested signal | 0.5 KiB/signal |
| reminder subscription + one terminal delivery | 1.2 KiB/opt-in save |
| merge/purge audit amortization | 0.3 KiB/user/year |

Examples above the 36 MB baseline:

- 10,000 users × 10 saves, 20% reminders, 30% signals: about **112 MB** total;
- 25,000 users × 10 saves under the same mix: about **226 MB** total;
- 50,000 users × 10 saves: about **416 MB** total and therefore above the safe
  release operating envelope once unrelated growth/vacuum slack is included.

## Retention

Service-only `personalization_retention_cleanup_v1` enforces:

- device proof: 180-day expiry, then delete (durable profile/saves remain);
- soft-removed saves and inactive signals: 30 days;
- merge audit and reminder delivery/idempotency evidence: 400 days;
- completed purge requests: 90 days after completion;
- active saves, current identity links, active consents and suppressions: until user
  removal or their separate canonical retention policy requires deletion.

Run cleanup daily from the service scheduler and record only aggregate deleted counts.
Never run it from the browser.

## Gates and alerts

- warn at 250 MB; investigate top relations and weekly growth;
- freeze bulk personalization backfills at 325 MB;
- release blocker at 350 MB (70% of plan limit) unless capacity is increased;
- emergency stop at 425 MB: disable materialization/new durable saves, keep static
  pages and ICS working, run approved retention/vacuum work, and do not delete active
  user state merely to recover space.

Measure `pg_database_size`, top relation/index sizes, dead tuples and 7/30-day growth
weekly. Recompute these projections after the first 10k real saves.

## Rollback

Before production use, rollback is `drop schema saved_events cascade; drop schema
site_identity cascade;` plus dropping the public `personalization_*_v1` functions,
only after confirming there is no user data to preserve. After activation, rollback
means disable Edge/scheduler producers and RPC grants, export/preserve user state,
then run a reviewed down migration; never drop live schemas directly.
