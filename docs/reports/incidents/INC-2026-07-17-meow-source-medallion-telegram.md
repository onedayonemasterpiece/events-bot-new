# INC-2026-07-17 MEOW source medallion leaked into Telegram Afisha posts

Status: closed
Severity: sev2
Service: Telegram event publishing (`@kldevents`)
Opened: 2026-07-17
Closed: 2026-07-17
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
- 2026-07-17 20:47Z — merge commit `eeb165a6cbe360cb9e1eef70e094901f499b2c93`
  was deployed from a clean `origin/main` worktree as Fly release `v1691`.
- 2026-07-17 20:50Z — the first canonical repair replay failed closed before
  sending because the Lite writer returned invalid grounded copy and the
  strict `gpt-4o` daily fallback budget was exhausted. No public mapping was
  changed by that attempt.
- 2026-07-17 20:54Z — all four posts were rebuilt through the canonical
  send-first/delete-after-success publisher while preserving their existing
  reviewed narrative. New photo-caption message ids are `2575`, `2576`,
  `2577` and `2578` for events `6867`, `6911`, `6931` and `6932`.
- 2026-07-17 20:55Z — event mappings and `joboutbox.last_result` were
  reconciled to the replacements; SQLite `PRAGMA quick_check` returned `ok`.
- 2026-07-17 20:56Z — authenticated Telethon UI verification found all four
  replacements with poster/caption and expected calendar buttons, no
  RichMessage/source strip, and all four old message ids absent. `/healthz`
  remained ready with no reported issues.

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

- The Telegram resolver no longer returns the MEOW source badge. The source
  asset remains available to the static event-detail page, where it represents
  provenance rather than an event attribute.
- All four affected RichMessages were replaced after the prevention release.
  The repair reused each post's already-reviewed narrative, so exhausted LLM
  fallback budget could not turn a visual cleanup into public-copy drift.

## Corrective Actions

- [x] Remove every `manifest_kind="source"` branch from the Telegram graphical
  resolver.
- [x] Add positive/negative regression controls around MEOW source handling.
- [x] Document that source-channel medallions are static event-detail-page
  only.
- [x] Repair all four public RichMessages and persist their clean replacement
  ids.

## Follow-up Actions

- [ ] Add an operator audit command that reports Telegram medallion slugs per
  current RichMessage without requiring a bespoke production script.

## Release And Closure Evidence

- Deployed SHA: `eeb165a6cbe360cb9e1eef70e094901f499b2c93`, reachable
  from `origin/main`; implementation commit
  `2d61bf7b8541626b4247aaa7e4abc4d9824ab803` was merged by PR
  [#66](https://github.com/onedayonemasterpiece/events-bot-new/pull/66).
- Deploy path: clean detached `origin/main` worktree, Fly release `v1691`,
  machine version `1691`, image
  `deployment-01KXRX9FE53KCPKTHABDSX2WPE`; Fly reported one passing check.
- Regression checks: focused MEOW/RichMessage suite `11 passed`; related
  outbox/ICS/public-fanout checks `4 passed`; GitHub Actions passed; `git diff
  --check` and `py_compile` passed. The complete
  `tests/test_tg_event_publish.py` run was `90 passed, 8 failed`; all eight
  failures are the pre-existing date-relative June 2026 fixtures that are past
  relative to the 2026-07-17 test date, not changed medallion behavior.
- Reversible data safety: backups
  `codex_backup_inc_20260717_meow_medallion_event` and
  `codex_backup_inc_20260717_meow_medallion_joboutbox` contain the four
  pre-repair rows.
- Post-repair mappings:
  - event `6867`: `2496` -> `2575` (`photo_caption`);
  - event `6911`: `2520` -> `2576` (`photo_caption`), calendar post `7506`;
  - event `6931`: `2556` -> `2577` (`photo_caption`), calendar post `7526`;
  - event `6932`: `2548` -> `2578` (`photo_caption`), calendar post `7527`.
- Authenticated Telethon UI verification: each new id is a normal single-photo
  message with non-empty caption and no `rich_message`; each old id is absent.
  The three events with calendar posts preserve the corresponding inline
  button URL. Event `6867` correctly has no calendar button.
- Production verification: `PRAGMA quick_check=ok`; `joboutbox` rows are
  `done`, have no error and point to the four new URLs; runtime mirror records
  four successful `photo_caption` publications; `/healthz` returned
  `ok=true`, `ready=true`, `job_outbox_worker_loop=ok`, `issues=[]`, with
  900 MB free on `/data`.

## Prevention

Shared asset storage no longer implies shared surface eligibility. Telegram
has an explicit allow-list by semantic kind, while source-channel provenance
badges remain a static-page-only concern.
