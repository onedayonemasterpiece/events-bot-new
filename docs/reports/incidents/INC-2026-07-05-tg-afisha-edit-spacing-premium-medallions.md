# INC-2026-07-05 Telegram Afisha edit jobs throttled new posts and delayed premium medallions

Status: monitoring
Severity: sev2
Service: Telegram event publishing (`tg_event_publish`) / `@kldevents`
Opened: 2026-07-05
Closed: —
Owners: Codex / events-bot operations
Related incidents: `INC-2026-07-05-tg-afisha-vk-dependency-backlog.md`, `INC-2026-06-29-tg-event-publish-fresh-import-starvation.md`, `INC-2026-06-29-tg-promo-compensation-repeat.md`
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/static-site-pages/event-token-medallions.md`, `docs/features/tg-premium-emojis-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-07-05 the operator reported that Telegram Афиша did not look active enough and that some posts appeared without Telegram Premium emoji / custom-emoji medallions. Production evidence showed the worker was alive, but `tg_event_publish` was spending the scarce 10-minute event-post lane on edits/reconciliation of already-existing Telegram messages. Those edit jobs updated old message ids and then became the latest spacing anchor, so fresh no-post announcements waited behind old edit work. Premium emoji and medallion enrichment was also scheduled as a delayed background task after the job was marked done, leaving a visible window where public posts had ordinary emoji/no medallion mosaic.

## User / Business Impact

- `@kldevents` looked stale even though the outbox worker was running.
- New event announcements competed with old message edits for the same 10-minute lane.
- Operators could see fresh DB `tg_event_publish` completions, but some were edits of older message IDs rather than new top-of-channel posts.
- Premium emoji / medallion rendering could be absent for minutes after publication, making the public channel look inconsistent.

## Detection

- Detected manually by the operator on 2026-07-05.
- Evidence sources: production SQLite `/data/db.sqlite`, runtime file mirror `/data/runtime_logs/events-bot.log*`, code inspection.
- Artifact directory: `artifacts/codex/tg-afisha-edit-spacing-20260705/` (not committed).

## Timeline

- 2026-07-05 05:00–09:34 UTC — normal worker published or edited `@kldevents` messages roughly every 10 minutes.
- 2026-07-05 08:22–09:34 UTC — fresh Smart Update events `6676`–`6679` produced new posts `1887`, `1891`, `1895`, `1899`, `1904`.
- 2026-07-05 09:44–10:25 UTC — subsequent `tg_event_publish` completions were mostly edits of older message ids (`1577`, `1584`, `1585`, `1586`, `1587`) rather than new channel-top posts.
- 2026-07-05 10:31 UTC — production DB showed `140` pending `tg_event_publish` jobs; many due at `10:35:52 UTC`, including no-post active/future events such as `6670`, `6664`, `6652`, `6651`.
- 2026-07-05 10:31 UTC — runtime logs confirmed repeated `reason=tg_spacing` deferrals and delayed premium editor completions such as message `1587` at `10:28:40 UTC`.
- 2026-07-05 10:39 UTC — regression tests added for no-post announcements outranking existing-message edit jobs and for synchronous premium editor invocation from the canonical event publisher.

## Root Cause

1. `_run_due_jobs_once_locked()` sorted due `tg_event_publish` rows by freshness/id but did not distinguish new announcements from jobs that already had `event.tg_event_post_id` and would only edit an existing message.
2. `_defer_tg_event_publish_if_spacing_blocked()` used recent done/running `tg_event_publish.updated_at` as the spacing anchor regardless of whether that job created a new channel-top post or edited an old one.
3. `next_tg_event_publish_run_at()` also treated pending existing-message edit rows as future publication anchors.
4. `publish_tg_event_announcement()` scheduled the Premium/medallion Telethon editor via `asyncio.create_task()` with the default delay/jitter, so the outbox job could finish before the public post had final custom emoji/medallions.

## Contributing Factors

- Earlier backlog mitigation rearmed many existing Telegram posts for repair/edit work at the same time as no-post announcements.
- Queue health counts did not distinguish new-post work from edit/reconciliation work.
- Existing incident checks verified that Telegram resumed, but not whether the resumed slots were new top-of-channel posts versus edits.

## Automation Contract

### Treat as regression guard when

- changing `_run_due_jobs_once_locked`, `_defer_tg_event_publish_if_spacing_blocked`, `next_tg_event_publish_run_at`, `schedule_event_update_tasks`, `publish_tg_event_announcement`, `_schedule_tg_premium_emoji_editor`, `_edit_tg_premium_emoji_now`, `tg_medallions`, or `tg_premium_emojis`;
- running bulk Telegram event post repair/rearm;
- changing `TG_EVENT_PUBLISH_INTERVAL_MINUTES`, `TG_EVENT_PUBLISH_FRESH_QUEUE_HOURS`, or Telegram premium emoji/medallion env.

### Affected surfaces

- `main.py` JobOutbox due ordering and Telegram spacing anchors;
- `main_part2.py` Telegram event publisher;
- `tg_premium_emojis.py` Telethon editor path;
- production DB `event.tg_event_post_id`, `event.tg_event_post_url`, `joboutbox`;
- public channel `@kldevents`.

### Mandatory checks before closure or deploy

- Unit tests: no-post `tg_event_publish` jobs outrank existing-post edit jobs; existing-post edits do not consume the new-post spacing anchor.
- Unit test: canonical event publisher awaits/runs the premium emoji editor path for event posts.
- Production DB check after deploy: due no-post active/future `tg_event_publish` rows drain ahead of existing-message edit rows; `depends_on` still excludes `vk_sync` for active today/future rows.
- Runtime log check: `tg_premium_emoji.edit_done` appears for newly sent/edited event posts without a delayed multi-minute public gap.
- Public smoke: verify a new `@kldevents` top-of-channel post after deploy and confirm premium emoji/medallion editor evidence.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted test output.
- Pre/post production DB counts for no-post pending rows and existing-post edit rows.
- Runtime log lines for `RUN [E...] done` and `tg_premium_emoji.edit_done`.
- `/healthz` after deploy.

## Immediate Mitigation

- Code change makes no-post `tg_event_publish` rows outrank existing-message edit jobs.
- Existing Telegram posts are no longer treated as future publication anchors in initial scheduling.
- Existing-message edit completions are ignored as new-post spacing anchors unless they are the current top/latest post.
- The canonical event publisher now awaits the Premium/medallion editor immediately for event posts instead of scheduling it as a delayed detached task.

## Corrective Actions

- Separate new channel-top announcements from edit/reconciliation work in due-job ordering and spacing.
- Keep Telegram's product-level daytime window and 10-minute cadence for real new posts, but do not let old edits spend that cadence.
- Make premium emoji / medallion enrichment part of the event publish transaction path for `tg_event_publish` posts.

## Follow-up Actions

- [ ] Add an operator queue-health report that splits `tg_event_publish` into `new_post_pending` and `existing_post_edit_pending`.
- [ ] Audit old existing-post pending rows after the hotfix drains fresh announcements and pause/expire obsolete edit rows with row-level evidence.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Regression tests guard against edit jobs consuming the announcement lane.
- Incident record must be raised for any future changes to Telegram spacing, fresh-lane ordering, or premium/medallion editor scheduling.
- Production closure requires proving that the next slots are real new top-of-channel posts when no-post events are waiting, not just any `tg_event_publish done` row.
