# INC-2026-07-17 MEOW source medallion leaked into Telegram Afisha posts

Status: open
Severity: sev2
Service: Telegram event publishing (`@kldevents`)
Opened: 2026-07-17
Closed: —
Owners: events-bot
Related incidents: `INC-2026-07-15-tg-rich-medallion-rendering-gaps`
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/static-site-pages/event-token-medallions.md`, `docs/operations/incident-management.md`

## Summary

The shared graphical-medallion resolver copied the static event-page
`MEOW Афиша` provenance badge into Telegram `@kldevents` RichMessage
strips. A source/aggregator identity is not an event attribute and must remain a
static event-detail-page provenance aid only.

## User / Business Impact

- Public Afisha announcements displayed `MEOW Афиша` branding as if it
  described the event itself.
- Production audit found four affected RichMessages: events `6867`, `6911`,
  `6931` and `6932`, originally mapped to message ids `2496`, `2520`, `2556`
  and `2548` respectively.
- Event content, poster, links and calendar buttons remained available; impact
  was incorrect public attribution/presentation rather than publication loss.

## Detection

- The operator reported the source medallion in the Afisha channel on
  2026-07-17.
- Read-only production SQLite, runtime-mirror and Telethon UI inspection tied
  the shared `1300×330` source strip to the affected events/posts.
- The public web embed cannot render Bot API RichMessage content and returned
  `Please open Telegram to view this post`; authenticated local E2E Telethon
  inspection was therefore used as the UI evidence path.

## Timeline

- 2026-07-15 21:28Z — graphical RichMessage publisher reached production as
  Fly release `v1684`.
- 2026-07-16 05:43Z — event `6867` published as RichMessage `2496` with the
  source-only strip.
- 2026-07-16 11:01Z — event `6911` published as RichMessage `2520` with the
  source-only strip.
- 2026-07-17 05:41Z and 06:44Z — events `6932` and `6931` published as
  RichMessages `2548` and `2556` with the source-only strip.
- 2026-07-17 — operator report opened this incident; production audit and
  prevention/repair work started.

## Root Cause

1. `tg_graphic_medallions.resolve_event_graphic_medallions` explicitly created
   a `manifest_kind="source"` item when an event referenced
   `t.me/meowafisha`.
2. The Telegram RichMessage rollout reused the static-site medallion inventory
   without defining a surface boundary that excludes provenance-only assets.
3. The rollout regression suite covered required positive identities but had
   no negative test proving that a source/aggregator avatar never reaches
   Telegram.

## Contributing Factors

- Static event pages and Telegram announcements share local assets but have
  different product semantics.
- The prior incident contract described source identities as approved Telegram
  strip inputs, so the incorrect policy was encoded as intentional behavior.
- RichMessage web embeds are unsupported, making ordinary public-HTML checks
  insufficient for this visual block.

## Automation Contract

### Treat as regression guard when

- Touching `tg_graphic_medallions.py`, Telegram RichMessage selection or hashes.
- Adding a static-site source/provenance medallion or reusing static medallion
  assets in another channel.
- Repairing or migrating `event.tg_event_post_mode='rich_message'` rows.

### Affected surfaces

- `tg_graphic_medallions.py::resolve_event_graphic_medallions`
- `main_part2.py::publish_tg_event_announcement`
- `site/public/assets/sources/` and static event-page source medallions
- production SQLite `event.tg_event_post_*` / `joboutbox`
- public `@kldevents` RichMessages and calendar buttons

### Mandatory checks before closure or deploy

- Unit: a MEOW-only event resolves no Telegram graphical medallions.
- Negative control: the same MEOW source at a curated venue still resolves the
  legitimate venue/organizer medallion, without `meow-afisha`.
- Regression: event `6811` still resolves exactly KОНБ + KGD80 + Znanie;
  graphical strip dimensions, footer spacing, multipart serialization and
  no-Premium-editor contracts from `INC-2026-07-15` remain green.
- Production audit: enumerate every current RichMessage whose strip contains
  the source badge; do not repair only the reported post.
- Repair: use send-first/delete-after-success for source-only RichMessages,
  persist the replacement `tg_event_post_*` mapping and leave no stale outbox
  job capable of recreating the badge.
- Post-deploy: `/healthz` ready, SQLite quick check `ok`, no fresh outbox loop
  failure, and Telethon UI verifies poster/text/calendar-button preservation
  plus absence of the source strip on every repaired post.

### Required evidence

- deployed SHA reachable from `origin/main`, Fly release/version and clean
  deploy worktree;
- focused pytest output and `git diff --check`;
- pre/post production event/outbox rows for `6867`, `6911`, `6931`, `6932`;
- authenticated Telethon block/media/button evidence for old and replacement
  message ids;
- runtime-mirror publication/repair lines and post-deploy health output.

## Immediate Mitigation

- Prevention and public cleanup are in progress. The source asset remains on
  static pages; only Telegram resolver use is being removed.

## Corrective Actions

- Remove every `manifest_kind="source"` branch from the Telegram graphical
  resolver.
- Add positive/negative regression controls around MEOW source handling.
- Document that source-channel medallions are static event-detail-page only.
- Repair all four public RichMessages and persist their clean replacement ids.

## Follow-up Actions

- [ ] Add an operator audit command that reports Telegram medallion slugs per
  current RichMessage without requiring a bespoke production script.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Shared asset storage no longer implies shared surface eligibility. Telegram
has an explicit allow-list by semantic kind, while source-channel provenance
badges remain a static-page-only concern.
