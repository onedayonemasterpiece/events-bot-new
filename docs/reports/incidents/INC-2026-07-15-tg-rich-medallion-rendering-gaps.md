# INC-2026-07-15 Telegram RichMessage medallion rendering gaps

Status: open
Severity: sev2
Service: Telegram event publishing (`@kldevents`)
Opened: 2026-07-15
Closed: —
Owners: events-bot
Related incidents: `INC-2026-07-05-tg-medallion-description-alias-drift`, `INC-2026-07-05-tg-afisha-edit-spacing-premium-medallions`, `INC-2026-06-25-outbox-unknown-jobtask-publication-outage`
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/static-site-pages/event-token-medallions.md`, `docs/operations/incident-management.md`

## Summary

The first manual RichMessage medallion rollout did not yet have a corresponding
normal production publisher path. Event `6811` (19 July, 18:30) was rendered
with only KОНБ and KGD80 although curated policy requires a third, distinct
Znanie medallion. RichMessage HTML also collapsed the deliberate footer gap
between the semantically separate `Подробнее` and `Max` links. Meanwhile the
ordinary `tg_event_publish` completion path still enqueued the legacy Premium
custom-emoji editor, so subsequent event posts could regress to the old mosaic
instead of getting a standalone graphical strip.

## User / Business Impact

- Event `6811` publicly omitted the source-grounded Znanie partner identity.
- The footer visually merged separate navigation destinations, reducing scanability.
- The one-off repaired posts did not prove that newly imported/updated events would receive the same graphical-medallion treatment.
- The legacy emoji job remained a competing post-publication mutation path.

## Detection

- The operator reported the missing Znanie mark on the 19 July 18:30 event.
- The operator visually noticed that the additional spaces before `Max` had disappeared.
- Code audit found `publish_tg_event_announcement` still using caption/media-group modes and `job_publish_tg_event_post` still enqueuing `tg_premium_emoji_edit` after every post.
- Production file-log evidence confirmed event `6811` was followed by a `tg_premium_emoji_edit` enqueue; all 407 existing premium jobs were already terminal `done`, so there was no pending queue to cancel.

## Timeline

- 2026-07-15 — about ten existing `@kldevents` posts were manually migrated to RichMessage with a bottom graphical strip.
- 2026-07-15 — operator reported that event `6811` had two rather than three medallions and that RichMessage footer spacing collapsed.
- 2026-07-15 21:09Z — implementation work established a manifest-backed production resolver/renderer, RichMessage send/edit path and regression tests; deployment and production catch-up remained pending.

## Root Cause

1. The manual rollout ledger specified a fixed two-item list for event `6811` and did not run the complete curated KGD80 partnership resolver.
2. RichMessage HTML used ordinary repeated spaces; HTML whitespace collapsing removed the intended 12-space visual separation.
3. The normal production publisher still implemented `sendPhoto`/`sendMediaGroup` plus delayed custom-emoji medallion placement and had not adopted `sendRichMessage`.
4. The legacy selector capped visual units and carried custom-emoji pack constraints that do not apply to a graphical strip.

## Contributing Factors

- The initial rollout was an operational migration, not a durable publisher implementation.
- Static-site asset semantics and Telegram custom-emoji configuration had diverged.
- The old incident contract explicitly required Premium editor evidence, which was valid for the old mechanism but became a regression after the transport decision changed.

## Automation Contract

### Treat as regression guard when

- Touching `publish_tg_event_announcement`, `job_publish_tg_event_post`, `tg_premium_emoji_edit`, Telegram footer composition or RichMessage transport.
- Touching organizer/festival/source/program/Pushkin manifests, aliases or medallion assets.
- Reformatting event post HTML/media blocks or changing `tg_event_post_mode`/source hashes.

### Affected surfaces

- `main_part2.py::publish_tg_event_announcement`
- `main.py::job_publish_tg_event_post` / `job_edit_tg_event_premium_emoji`
- `tg_graphic_medallions.py`
- `site/src/data/{organizerMedallions,festivalMedallions}.json` and local runtime assets
- production SQLite `event.tg_event_post_*` and `joboutbox`
- Bot API `sendRichMessage` / `editMessageText(rich_message=...)`
- public `@kldevents` posts and calendar buttons

### Mandatory checks before closure or deploy

- Unit: event `6811` semantics resolve exactly KОНБ + KGD80 + Znanie, without duplicate Znanie when the source explicitly names it.
- Unit: rendered strip is `1300×330`, contains every selected asset and stays separate from event poster blocks.
- Unit/session serialization: every approved event image plus the strip becomes a multipart attachment referenced by `tg://photo?id=...`.
- Unit: RichMessage footer keeps exactly 12 non-collapsing spaces between `Подробнее` and `Max`, remains one row and preserves `Max · Вконтакте` links.
- Unit: new RichMessages send through the canonical publisher; existing RichMessages edit in place; a single-message legacy-to-rich transition deletes the old post only after successful send; legacy media groups remain intact until a complete audited message-id ledger exists.
- Unit: `rich_message` completion does not enqueue or invoke Premium/custom-emoji medallion placement; legacy editor calls use `medallion_html_block=None`; stale editor jobs skip when the event's current mode is `rich_message`.
- Regression: mandatory checks from `INC-2026-06-25-outbox-unknown-jobtask-publication-outage.md` remain green for unknown outbox tasks, worker-loop health, Storage upload and ticket-site fanout.
- Production smoke: `/healthz` ready; no fresh outbox loop crash; a source-grounded canary reaches `done`, has the expected bottom strip/buttons/footer, stores `tg_event_post_mode='rich_message'`, and has no follow-up medallion emoji job.
- Catch-up: repair/requeue event `6811` and any eligible event whose old hash/mode predates graphical medallions; verify public post and DB mapping.

### Required evidence

- deployed SHA reachable from `origin/main` and Fly release image/version;
- focused pytest output including aiogram multipart serialization;
- pre/post production DB rows for canary and event `6811`;
- Telegram UI/Telethon or Bot API evidence for block order, three medallions, buttons and footer links;
- runtime mirror grep showing successful `tg_event_publish` and no new medallion custom-emoji enqueue.

## Immediate Mitigation

- Existing RichMessage posts were kept available while the durable implementation was prepared.
- Production queue audit found no pending Premium jobs requiring cancellation.
- Integrator owns the immediate event `6811` public correction and final mapping verification.

## Corrective Actions

- Added a shared manifest-backed resolver covering organizer/venue, festival/program, curated source and Pushkin assets.
- Added deterministic local Pillow strip rendering on brand graphite with no OpenAI image generation.
- Added canonical RichMessage send/edit behavior preserving all approved event images, buttons and footer links.
- Encoded footer separation as non-collapsing spaces in RichMessage HTML.
- Removed graphical medallion input from the Premium/custom-emoji editor and suppressed that job for RichMessages.
- Added a stale-job RichMessage guard before invoking Telethon and a fail-safe legacy-album guard so automatic migration cannot orphan unknown media-group items.
- Versioned source hashes with medallion slugs, renderer contract and asset digests.

## Follow-up Actions

- [ ] Integrator: deploy from a clean `origin/main`-reachable SHA and record release evidence below.
- [ ] Integrator: correct/reconcile event `6811` and run a normal production canary/catch-up.
- [ ] Events-bot: close or supersede old incident wording that requires custom-emoji medallion enrichment once production verification is complete.

## Release And Closure Evidence

- deployed SHA: pending integrator
- deploy path: pending integrator
- regression checks: implementation lane focused tests recorded in its `RESULTS.md`; production checks pending integrator
- post-deploy verification: pending integrator

## Prevention

Telegram event medallions now derive from the curated asset inventory at publish
time, participate in content identity, and travel as a real RichMessage media
block. An explicit event-6811 regression fixture, multipart serialization test,
footer whitespace assertion and no-emoji scheduling guard prevent the operational
one-off and durable publisher from diverging again.
