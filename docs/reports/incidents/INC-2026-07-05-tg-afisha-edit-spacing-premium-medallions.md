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
- 2026-07-05 11:09 UTC — production audit found false Музей Мирового океана medallion selection from the short `ММО` alias matching inside words (`программой`, `Эммой`, `фильмом`).
- 2026-07-05 11:13 UTC — alias-boundary fix `6f2166c1` was deployed as Fly image `deployment-01KWRZM8WGSCZ8X5HZ1EQR13YP`, machine version `1597`.
- 2026-07-05 11:17–11:22 UTC — compensating Telegram repair edited affected messages `1907`, `1908`, `1911`, `1916`, `1919`, `1921`, `1922`, `1830`, `1825`, `1822`, `1699`, `1770`, `1788`, `1780`, `1781`, `1799` with 18–28s spacing and no FloodWait.

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
- No-post announcements are sorted ahead of existing-message edit rows.
- Execution spacing remains mandatory for every `tg_event_publish`, including existing-message jobs, because they may send a new message on mode change.
- Premium/medallion editor remains delayed/rate-limited to avoid Telegram FloodWait.

## Corrective Actions

- Separate new channel-top announcements from edit/reconciliation work in due-job ordering and spacing.
- Keep Telegram's product-level daytime window and 10-minute cadence for real new posts, but do not let old edits spend that cadence.
- Keep premium emoji / medallion enrichment delayed and rate-limited; do not run it synchronously inside bulk publication catch-up.

## Follow-up Actions

- [ ] Add an operator queue-health report that splits `tg_event_publish` into `new_post_pending` and `existing_post_edit_pending`.
- [ ] Audit old existing-post pending rows after the hotfix drains fresh announcements and pause/expire obsolete edit rows with row-level evidence.

## Release And Closure Evidence

- initial deployed SHA: `85ea298ae4eae46b3dbeaa605357a163344b0246` (`origin/main`, Fly image `deployment-01KWRY0VEJ2FGEF1E95NDMQ2GV`, machine version `1592`). This mitigation caused a burst regression (see section below).
- corrected deployed SHA: `bf1675b54a6a8901dbb60b4fb579e1ff39f17ee9` (`origin/main`, Fly image `deployment-01KWRYFSEGKSZEH3QXW90P7M80`, machine version `1593`).
- medallion alias fix SHA: `6f2166c1da9b7e34f10f0e357206012a8d6cee06` (`origin/main`, Fly image `deployment-01KWRZM8WGSCZ8X5HZ1EQR13YP`, machine version `1597`).
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean branch `hotfix/tg-afisha-edit-spacing`; the same SHAs were pushed to `origin/main` before deploy.
- regression checks:
  - `python3 -m py_compile main.py main_part2.py tg_premium_emojis.py` passed.
  - initial mitigation tests: `uv run --with-requirements requirements.txt --with pytest pytest -q tests/test_job_due_filter.py::test_due_tg_event_publish_backlog_is_spaced_at_execution tests/test_job_due_filter.py::test_tg_event_publish_new_posts_outrank_existing_post_edits tests/test_job_due_filter.py::test_fresh_tg_event_publish_is_not_starved_by_old_backlog tests/test_tg_event_publish.py::test_tg_event_publish_runs_premium_editor_after_send tests/test_tg_event_publish.py::test_tg_event_announcement_places_medallions_before_details_footer tests/test_tg_event_publish.py::test_tg_promo_medallion_block_uses_custom_emoji_entities` → `6 passed`.
  - burst-regression hotfix tests: `uv run --with-requirements requirements.txt --with pytest pytest -q tests/test_job_due_filter.py::test_due_tg_event_publish_backlog_is_spaced_at_execution tests/test_job_due_filter.py::test_tg_event_publish_new_posts_outrank_existing_post_edits tests/test_tg_event_publish.py::test_tg_event_publish_schedules_premium_editor_after_send` → `3 passed`.
  - medallion alias tests: `uv run --with-requirements requirements.txt --with pytest pytest -q tests/test_tg_event_publish.py::test_tg_event_announcement_places_medallions_before_details_footer tests/test_tg_event_publish.py::test_tg_promo_medallion_block_uses_custom_emoji_entities tests/test_tg_event_publish.py::test_tg_medallions_do_not_match_short_acronym_inside_words tests/test_tg_event_publish.py::test_tg_medallions_match_short_acronym_as_standalone_token tests/test_tg_event_publish.py::test_tg_event_publish_schedules_premium_editor_after_send tests/test_job_due_filter.py::test_due_tg_event_publish_backlog_is_spaced_at_execution tests/test_job_due_filter.py::test_tg_event_publish_new_posts_outrank_existing_post_edits` → `7 passed`.
  - `git diff --check` passed before commit.
- post-deploy verification:
  - `/healthz` OK at `2026-07-05 10:44 UTC`, Fly machine version `1592`; corrected hotfix `/healthz` OK at `2026-07-05 10:53 UTC`, Fly machine version `1593`.
  - Runtime file mirror was enabled: `ENABLE_RUNTIME_FILE_LOGGING=1`, active `/data/runtime_logs/events-bot.log` present.
  - Production DB after deploy: new no-post event `6670` published at `2026-07-05 10:46:38 UTC` as `https://t.me/c/3954607218/1906`; public embed `https://t.me/s/kldevents` contained message `1906` and title `Стендап: «Шоу историй со зрителями»`.
  - Initial runtime log showed immediate enrichment before job done for `1906`, but this synchronous editor path was reverted after it caused Telegram `FloodWait` during catch-up.
  - Corrected hotfix verification: at `2026-07-05 10:56:38–10:56:39 UTC`, due `tg_event_publish` rows were skipped with `reason=tg_spacing next_run_at=2026-07-05T10:58:59.300683+00:00 latest_anchor=2026-07-05T10:48:59.300683+00:00`; no new `tg_event_publish done` rows appeared after `10:49:16` before the next allowed slot.
  - Backlog dependency mitigation removed remaining active/future `vk_sync:*` tokens from `13` pending/error `tg_event_publish` rows; backup table `codex_backup_20260705_tg_edit_spacing_vk_deps_20260705_104732`; remaining active/future `tg_event_publish.depends_on LIKE '%vk_sync:%'` = `0`.
  - Alias-boundary production probe after `6f2166c1`: event `6530` (`Показ фильма «Переселенцы. История Первых»), `6660` (`День КА-ПИ-БА-РЫ!`) and `6656` (`Louisa Voice of the Soul`) returned no medallions; event `6534` returned `yantar-hall` + `simfoniya-vetra`, not `world-ocean-museum`.
  - Compensating repair post-check: Telegram dry-run for messages `1907`, `1908`, `1911`, `1916`, `1919`, `1921`, `1922`, `1830`, `1825`, `1822`, `1699`, `1770`, `1788`, `1780`, `1781`, `1799` returned `dry_replacements=0`; message `1907` no longer had a visual medallion block or `world-ocean-museum` entities, message `1908` had `yantar-hall=16` and `simfoniya-vetra=16` entities only.
  - Runtime log after deploy showed the normal delayed editor path succeeding for a fresh post: `2026-07-05 11:22:39 UTC tg_premium_emoji.edit_done context=tg_event_publish_send message_id=1924 edited=True replacements=3 error=None`.
  - `/healthz` OK at `2026-07-05 11:23 UTC`, Fly machine version `1597`, no health issues.
  - Evidence artifacts saved locally under `artifacts/codex/tg-afisha-edit-spacing-20260705/` (not committed).


## Regression During Mitigation — 2026-07-05 10:47 UTC

The first mitigation in SHA `85ea298a` incorrectly let any event with an existing `tg_event_post_id` bypass execution spacing. That was unsafe: an existing-post job can still send a new public message when the desired mode changes (`text`/`photo_caption`/`album_caption`). Production then burst new `@kldevents` message ids `1906`, `1907`, `1908`, `1911`, `1916`, `1919` within minutes and the synchronous premium editor hit Telegram `FloodWait`. Follow-up hotfix restores the hard execution gate for every `tg_event_publish`; no-post rows still outrank edit rows in sorting, but no job may bypass the 10-minute public-send cadence.

## Medallion Alias Regression — 2026-07-05 11:09 UTC

Production audit after the operator report found that the Музей Мирового океана Telegram medallion was selected for unrelated recent posts because the short alias `ММО` was matched as a raw substring. Examples: event `6534` matched `ммо` inside `программой`, event `6530` inside `Эммой`, event `6660` inside `фильмом`, event `6656` inside `фильмом`. This made the channel show stray `ММО` medallions and made it look as if the medallion had been applied to multiple unrelated posts. The corrective code requires medallion aliases to match as standalone tokens/phrases, preserving explicit full aliases/venue/source URL matches while blocking acronym-in-word false positives.

Compensating repair was required for already edited public messages whose wrong medallion block was inserted before this fix; delayed premium-editor retries alone were insufficient because the editor's normal idempotency path only inserts missing expected blocks and does not remove wrong historical blocks. The repair removed the wrong `ММО` block from `1907`, replaced the wrong second medallion on `1908`, and reran premium/medallion enrichment for messages skipped by the FloodWait/restart window.

## Prevention

- Regression tests guard against edit jobs consuming the announcement lane.
- Regression tests guard that short Telegram medallion acronyms such as `ММО` do not match inside ordinary words and still match when used as standalone aliases.
- Medallion insertion now consumes the old blank separator before the `Подробнее` footer and replaces it with a single newline after the medallion block, preventing a visible empty row under medallions while leaving non-medallion posts unchanged.
- Incident record must be raised for any future changes to Telegram spacing, fresh-lane ordering, or premium/medallion editor scheduling.
- Production closure requires proving that the next slots are real new top-of-channel posts when no-post events are waiting, not just any `tg_event_publish done` row.
