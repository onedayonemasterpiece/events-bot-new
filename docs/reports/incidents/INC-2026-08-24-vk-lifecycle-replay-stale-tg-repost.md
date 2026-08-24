# INC-2026-08-24 VK lifecycle replay caused a stale Telegram repost

Status: mitigated
Severity: sev1
Service: VK lifecycle ingestion, canonical events, promo Telegram reposts
Opened: 2026-08-24
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-05-07-vk-time-reschedule-wrong-match`, `INC-2026-05-17-vk-retrospective-reschedule-wrong-postponement`, `INC-2026-06-13-poll-repost-wrong-date-and-copy`, `INC-2026-06-29-tg-promo-compensation-repeat`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/features/promo-campaigns/README.md`

## Summary

On 2026-08-24 the daily popular-event campaign forwarded `@kldevents/3716`
to `@kenigevents/4813`, although the source announcement was for 2026-08-23.
The promo selector believed the event was scheduled for 2026-09-05 because a
replayed VK lifecycle notice had reassigned an unrelated zoo event by location
alone. The same replay also cancelled a second unrelated zoo event.

## User / Business Impact

- Subscribers of the announcements channel received an expired event as a
  current recommendation.
- Event `8258` and its Telegraph/ICS projections exposed a false 2026-09-05
  date; event `8257` was falsely marked cancelled.
- One of the campaign's current-day publication opportunities was consumed by
  invalid content and therefore requires a controlled catch-up after the fix.

## Detection

- The incident was reported by a user with the public URL
  `https://t.me/kenigevents/4813`.
- `/healthz` remained green because service availability and schedulers were
  healthy; no semantic freshness alert compared an immutable source post date
  with the mutable canonical event date.
- Authenticated Telegram UI inspection confirmed that message `4813`, sent at
  2026-08-24 12:14 Europe/Kaliningrad, said `23 августа 10:00`.

## Timeline

- 2026-08-20: VK notice `wall-48383763_41891` was originally handled; its
  structured lifecycle actions correctly targeted the cosplay festival,
  yoga, and music evening.
- 2026-08-22 22:05 Europe/Kaliningrad: source event post
  `@kldevents/3716` was published for 2026-08-23 10:00.
- 2026-08-24 12:05 Europe/Kaliningrad: replay of the VK notice changed event
  `8258` from 2026-08-23 to 2026-09-05 and cancelled event `8257`.
- 2026-08-24 12:14 Europe/Kaliningrad: promo exposure `1015` forwarded the
  stale source post as `@kenigevents/4813`.
- 2026-08-24: user report opened this incident; investigation identified both
  data corruptions and the missing promo source-consistency guard.
- 2026-08-24 13:11 Europe/Kaliningrad: promo activity `31` was paused after an
  online SQLite backup and incident-scoped table backups were created.
- 2026-08-24 13:12 Europe/Kaliningrad: authenticated Telegram deletion removed
  message `4813`.
- 2026-08-24 13:14 Europe/Kaliningrad: Events `8257`/`8258` and their source
  ownership were repaired; the outbox rebuilt Telegraph, ICS, and both calendar
  posts by 13:14:41.

## Root Cause

1. The VK structured parser produced the correct typed targets, but lifecycle
   matching considered only active events on the original date.
2. During replay, the correct targets had already moved to the new date or
   reached their terminal lifecycle state, so they were absent from the
   candidate set.
3. `_cancel_matching_event_from_notice` allowed an exact location score of four
   to satisfy a request that also carried a title hint. It therefore selected
   unrelated zoo events with zero title overlap.
4. Promo Telegram repost selection trusted the mutable `event.date` and did not
   require the stored source-post publication snapshot to match the current
   event snapshot. It also recorded the selection time as
   `source_published_at` for stored Telegram URLs, obscuring source age.

## Contributing Factors

- Lifecycle replay was not idempotent after a target had already changed date
  or state.
- Exact venue identity was treated as sufficient semantic identity even when a
  typed lifecycle action supplied a non-matching title.
- Availability health checks do not detect semantic date drift across DB,
  Telegram, Telegraph, and ICS projections.

## Automation Contract

### Treat as regression guard when

- changing VK lifecycle candidate selection, scoring, replay, or terminal-state
  handling;
- changing promo `tg_repost` eligibility, source snapshots, or exposure
  accounting;
- repairing event dates/statuses that already have Telegram, Telegraph, or ICS
  projections.

### Affected surfaces

- `vk_auto_queue.py::_cancel_matching_event_from_notice`;
- `promo.py` Telegram repost candidate discovery and exposure metadata;
- production events `8257` and `8258`, lifecycle source/fact rows, exposure
  `1015`;
- `@kenigevents`, `@kldevents`, Telegraph, Supabase ICS storage, and managed VK
  event posts.

### Mandatory checks before closure or deploy

- replay a reschedule whose correct title target is already on the new date and
  prove an unrelated same-location event is untouched;
- replay a cancellation whose correct title target is already cancelled and
  prove an unrelated same-location active event is untouched;
- prove a stale Telegram source snapshot is not eligible for promo repost;
- run targeted VK auto-import and promo regression tests;
- verify repaired DB date/status, Telegraph, ICS, original Telegram/VK posts,
  deletion of `@kenigevents/4813`, and a valid same-day compensating promo;
- verify clean `origin/main` reachability, deployed SHA, `/healthz`, and targeted
  runtime logs.

### Required evidence

- deployed SHA reachable from `origin/main`;
- test commands/results and production health response;
- authenticated Telegram/VK inspection plus Telegraph/ICS evidence;
- production backup/repair receipts and current-day catch-up exposure.

## Immediate Mitigation

- Paused promo activity `31` before its next due run.
- Preserved `/data/inc-2026-08-24-before-stale-tg-repost.sqlite` plus
  `codex_backup_inc20260824_*` table snapshots; `PRAGMA quick_check` returned
  `ok`.
- Deleted `@kenigevents/4813` and marked exposure `1015` as
  `REMOVED_INCIDENT`, so the invalid delivery no longer consumes the public
  daily cap.
- Restored Event `8258` to 2026-08-23 and Event `8257` to `active`; removed
  false lifecycle source/fact rows `12303587`/`166134` and
  `12303588`/`166135`.
- Rebuilt the affected Telegraph, ICS, and Telegram calendar projections.

## Corrective Actions

- Lifecycle replay is title-grounded and idempotent across new-date and
  terminal-state targets.
- Promo Telegram reposts reject a generated source post whose immutable source
  snapshot date differs from the current Event date and preserve the earliest
  matching source observation time.
- Regression tests cover the exact already-rescheduled/already-cancelled replay
  shapes and the stale 23-August source versus 5-September Event shape.

## Follow-up Actions

- [ ] Add semantic monitoring for source-post date versus canonical event date.
- [ ] Reconcile older lifecycle rows for other location-only false matches.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Pending implementation and regression evidence.
