# INC-2026-06-07 TG Event Publishing Media, Calendar Link, And Poster Dedup

Status: monitoring
Severity: sev2
Service: Telegram event publishing / Smart Update media storage
Opened: 2026-06-07
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-29-genai-response-repr-leak`
Related docs: `docs/features/tg-publishing/README.md`, `docs/backlog/features/tg-publishing/requirements.md`

## Summary

The first production Telegram event announcement in `@kldevents` was published as a separate text message followed by media messages, used the raw `.ics` Supabase file URL for the calendar button, omitted the Telegraph `Подробнее` link, and exposed two visually identical images. The same Smart Update run also failed the two-surface acceptance contract: DB had a managed VK URL for event `5776`, but VK API returned no wall item for it, so the event was visible in Telegram but not actually visible in `klgdevents`.

## User / Business Impact

- The new production channel received a post format that did not match the feature requirements.
- Calendar click-through went to the file asset instead of the existing Telegram calendar post surface.
- Duplicate images made the channel look broken and revealed a Smart Update media dedup gap.
- A stale/deleted managed VK URL made `vk_sync` look successful in DB while there was no visible VK post.

## Detection

- Detected by operator review of the first live production post after the feature deploy.
- Existing unit tests covered basic text/button/media fanout, but did not enforce captioned media format, `ics_post_url`, Telegraph `Подробнее`, or persisted `eventposter` phash backfill/prune.

## Timeline

- 2026-06-07 20:14 UTC: live VK auto-import E2E published event `5776` to `@kldevents` as text `id=3` plus media `id=4/5`.
- 2026-06-07 20:31 UTC: operator reported separate media, duplicate images, missing price/Telegraph link, and wrong calendar target.
- 2026-06-07 20:40 UTC: production DB confirmed event `5776` had one managed storage poster with `phash` and one raw VK CDN poster without `phash`.
- 2026-06-07 20:45 UTC: immediate mitigation replaced the production post with one `photo_caption` message `id=6`, deleted old messages `3/4/5`, removed duplicate eventposter row `10460`, and reduced `event.photo_urls` to one URL.
- 2026-06-07 20:50 UTC: production DB showed `source_vk_post_url=https://vk.com/wall-231920894_2375`, but VK API `wall.getById` returned `items=[]`. A manual publish created `wall-231920894_2389`; this is recorded as mitigation only, not acceptance evidence for the feature.
- 2026-06-07 21:03 UTC: normal production VK crawl + auto-import created event `5779` from `https://vk.com/wall-30777579_15383`, enqueued both `tg_event_publish` and `vk_sync`, and published Telegram post `https://t.me/c/3954607218/7`; `vk_sync` then failed with `vk_sync_missing_media_for_telegram_event`, proving the two-surface contract was still broken before the follow-up fix.
- 2026-06-07 21:15 UTC: after the `_event_has_telegram_origin` fix deployed, the production worker retried `vk_sync:5779` and created managed VK post `https://vk.com/wall-231920894_2391`; VK later exposed the scheduled item as public wall item `https://vk.com/wall-231920894_2392`, and production DB/job evidence was normalized to that public URL.
- 2026-06-07 21:22 UTC: post-deploy `/healthz` returned 503 because `job_outbox_worker` task had ended with `LookupError` while the app, DB, bot session, and schedulers were otherwise alive.

## Root Cause

1. The new Telegram event publisher was implemented as text-plus-follow-up-media to preserve an inline calendar button, but this was not in the original requirement and produced the wrong channel format.
2. The calendar button used `event.ics_url` (raw Supabase `.ics`) instead of the existing project contract `event.ics_post_url` pointing to the Telegram calendar channel post.
3. Smart Update near-duplicate poster pruning relied on `phash`. A raw VK CDN `EventPoster` row without `phash` survived next to the managed storage row for the same image, so `event.photo_urls` contained both URLs.
4. `job_sync_vk_source_post` skipped publishing when `vk_source_hash` matched and `source_vk_post_url` looked like a managed `klgdevents` URL, without verifying that `wall.getById` still returned a real VK item.
5. `_event_has_telegram_origin` treated non-null `source_chat_id` / `source_message_id` as Telegram-origin. VK auto-import stores VK group/post ids in the same columns, so a VK-origin event could be blocked by the Telegram text-only media guard before managed VK publication.
6. The background `job_outbox_worker` protected job execution errors, but its periodic stats heartbeat ran outside that protection; a diagnostic `LookupError` could terminate the worker task and make health fail even though the main app remained alive.

## Contributing Factors

- `tg_event_publish` did not depend on `tg_ics_post`, so the calendar post URL could race with event publication.
- Tests did not assert captioned media, calendar channel URL, or dedup of persisted `EventPoster` rows with missing `phash`.
- Documentation was updated to describe the implementation workaround rather than preserving the original media-post expectation.

## Automation Contract

### Treat as regression guard when

- Changing `tg_event_publish`, `build_tg_event_announcement`, `build_tg_event_reply_markup`, `schedule_event_update_tasks`, `smart_event_update._apply_posters`, or event media import paths.
- Changing calendar/ICS publication fields (`ics_url`, `ics_post_url`, `tg_ics_post`).

### Affected surfaces

- `main_part2.py` Telegram event publisher and post formatting.
- `main.py` job dependency scheduling for `tg_event_publish`.
- `main.py::job_sync_vk_source_post` idempotency guard for managed VK posts.
- `smart_event_update.py` poster dedup and persisted event media state.
- Production Telegram channels `@kldevents` and `kenigeventscalendar`, plus VK `klgdevents`.

### Mandatory checks before closure or deploy

- `tests/test_tg_event_publish.py`
- `tests/test_genai_dump_and_poster_dedup.py`
- Live Telegram UI check of event `5776` or equivalent E2E: one captioned media post, no duplicate media, calendar button points to `ics_post_url`, caption includes `Подробнее`, and no placeholder price when price/free status is unknown.
- Live VK API/UI check of the same Smart Update path: `vk_sync` must produce a managed `klgdevents` URL and `wall.getById` must return a real item with media.
- Production DB check: event `photo_urls` and `eventposter` rows are deduped; `tg_event_post_mode` is `photo_caption` for a one-image event.
- Release governance: deployed SHA reachable from `origin/main`, `/healthz` ready.

### Required evidence

- Deployed SHA.
- Test command output.
- Telegram UI inspection output for the fixed post.
- Production DB query output for event `5776`.

## Immediate Mitigation

- Replaced the bad production post with message `https://t.me/c/3954607218/6`.
- Deleted old messages `3`, `4`, and `5`.
- Removed duplicate `eventposter.id=10460` and kept only the managed storage URL for event `5776`.
- Manual VK mitigation created `https://vk.com/wall-231920894_2389` after the stale URL was found. This does not satisfy acceptance by itself; closure still requires a normal Smart Update/live E2E pass.

## Corrective Actions

- Publish one-image events as `sendPhoto` with caption and inline calendar button.
- Publish multi-image events as `sendMediaGroup` with caption on the first media item and calendar link in caption, because Telegram Bot API does not support inline buttons on media groups.
- Use `ics_post_url` for calendar actions; make `tg_event_publish` depend on `tg_ics_post` when available.
- Add Telegraph `Подробнее` to the footer.
- Backfill missing poster `phash` values and prune persisted near-duplicate `EventPoster` rows.
- Verify managed VK URL existence before treating a matching `vk_source_hash` as terminal; if `wall.getById` returns no item, republish via the normal VK sync path.
- Detect Telegram-origin events by `t.me` source URL or explicit `EventSource.source_type in ('telegram', 'tg')`, not by numeric source ids that VK imports also populate.
- Keep `job_outbox_worker` alive when stats/diagnostic logging fails, logging the stats failure without terminating the task.

## Follow-up Actions

- [ ] Add a replay fixture for `vk_inbox.id=8307` if live E2E exposes another gap outside unit coverage.

## Release And Closure Evidence

- deployed SHA: `303ad64cbe17ab67f5c49573ca312573b547c9b0` (`origin/main` contains it). The VK-origin source-id fix is `846103369b7c14f4b78b5ce015d28481fb6c4651`, the footer-link fix is `823a9b43ff4433f73844fc608d95fe6f43c2d600`, and the original dual-surface hardening is `d8134cba62b9aaa7c3f342c5b3998e8fad943621`.
- deploy path: clean linked worktree `codex/tg-publishing-smart-update`, pushed to `origin/main`, deployed to Fly app `events-bot-new-wngqia` as `deployment-01KTHZW627ENPJGJV76NE34JSN` after VK fix deploy `deployment-01KTHYH91220D4EJAA2Y3XF6PP` and footer deploy `deployment-01KTHXG6D75N3T24GYJHXWQNA9`; `/healthz` returned `ok=true`, `ready=true`.
- regression checks:
  - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main_part2.py tests/test_vk_source.py`
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py tests/test_genai_dump_and_poster_dedup.py tests/test_vk_source.py::test_sync_vk_source_post_blocks_text_only_telegram_event tests/test_vk_source.py::test_sync_vk_source_post_allows_vk_origin_with_source_ids tests/test_vk_source.py::test_job_sync_vk_source_post_republishes_missing_managed_post tests/test_vk_source.py::test_job_sync_vk_source_post_resyncs_title_only_change tests/test_vk_source.py::test_managed_klgdevents_event_skips_vk_sync` (`27 passed in 1.92s`).
  - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py tests/test_job_worker.py`
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_job_worker.py::test_job_outbox_worker_survives_stats_failure tests/test_tg_event_publish.py tests/test_vk_source.py::test_sync_vk_source_post_allows_vk_origin_with_source_ids` (`8 passed in 0.71s`).
- post-deploy verification:
  - event `5776` mitigation post: `https://t.me/c/3954607218/6`; one captioned photo, calendar button points to `https://t.me/c/2807919036/6502`, no `Стоимость не указана`, footer has Telegraph `Подробнее`, VK, and `Подписаться` -> `https://t.me/+MrSeuZSHv3VjMThi`.
  - live acceptance event `5779`: source `https://vk.com/wall-30777579_15383`, Telegram post `https://t.me/c/3954607218/7`, managed VK post `https://vk.com/wall-231920894_2392`; Telegram UI inspection confirmed media, Telegraph details, invite footer, VK footer, no placeholder price, and calendar button `📅 8 июня 18:30 · Добавить в календарь` -> `https://t.me/c/2807919036/6503`. VK `wall.getById` returned one public item with the title, calendar text, free-price text, and one photo attachment.
  - health: Fly machine `48e42d5b714228` version `1226` reported `1 passing` check; repeated `/healthz` calls through boot age `97.9s` returned HTTP 200 with `job_outbox_worker=ok`, `db=ok`, `bot_session_closed=false`, and `issues=[]`; runtime logs showed `WORKER_STATE` at `21:31:22`, `21:31:53`, and `21:32:23` UTC after deploy.
  - Existing `vk_sync_missing_media_for_telegram_event` rows after deploy were inspected as Telegram-origin/no-media guard cases, not the fixed VK-origin false-positive class.

## Prevention

- Regression tests now cover captioned media publishing, calendar post button URL, scheduling dependency on `tg_ics_post`, persisted poster dedup with missing `phash`, stale managed VK URLs whose `wall.getById` item is missing, and `job_outbox_worker` resilience when stats logging fails.
