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
- 2026-06-07 21:57 UTC: follow-up Smart Update publishing polish deployed: unified `Посты: VK, TG` report line, Telegram quiet-hour spacing, city/type hashtags, and VK coauthor proposal parameters with fallback.
- 2026-06-08 09:10 UTC: operator reported follow-up `@kldevents` issues: album posts still showed visually duplicate pictures, album posts had a text `Добавить в календарь` link instead of an inline button, and `https://t.me/kldevents/40` was text-only.
- 2026-06-08 10:02 UTC: Telethon E2E inspection confirmed the calendar behavior is split by Telegram format: single-photo posts `34/35` have inline buttons to `ics_post_url`; album post `29` has the expected caption text link because Bot API media groups cannot carry inline keyboards; date-only posts such as `32/36/38/40` have no calendar action.
- 2026-06-08 10:06 UTC: Telethon media download/dHash check confirmed active near-duplicate albums in `@kldevents`: `29/30` hamming `4`, `32/33` hamming `2`, `38/39` hamming `6`; `36/37` hamming `10` remains a less certain two-image case. Direct Telegraph checks for the same pages showed one rendered image for `Vystavka-Biletyory-20`, `Karta-treh-muzeev`, and `Tvorcheskij-chellendzh`, so the TG publisher was still seeing a richer/less-pruned `event.photo_urls` list than the Telegraph renderer.
- 2026-06-08 10:09 UTC: `@kldevents/40` Telethon inspection confirmed `media=None`, no buttons, Telegraph `https://telegra.ph/Sobache-serdce-06-07-2` has `0` figures/images. This specific post is evidence of an event/media-intake gap or image-less source, not proof that TG publishing dropped an already attached image.
- 2026-06-08 09:28 UTC: near-duplicate TG media safety-net deployed as Fly release `v1234` / image `deployment-01KTK8Z0JY5GBNT0RPSWPN2JER`.
- 2026-06-08 09:34 UTC: compensating cleanup deleted obvious duplicate-only media messages `22`, `25`, `27`, `30`, `33`, and `39` from `@kldevents` via the production bot token. Telethon verification confirmed those message IDs are no longer visible; caption messages and distinct album images remain.
- 2026-06-08 09:39 UTC: operator reported additional duplicate albums `https://t.me/kldevents/8`, `/11`, `/18`, and `/36`, plus event `Зойкина квартира` at `/42` visible in Telegram without a picture and not visible in VK.
- 2026-06-08 09:43 UTC: Telethon/dHash inspection confirmed duplicate media groups `8/9/10` (hamming `2/2/4`), `11/12` (hamming `4`), `18/19` (hamming `3`), and `36/37` (hamming `10`). Production cleanup deleted duplicate-only message IDs `9`, `10`, `12`, `19`, and `37`.
- 2026-06-08 09:44 UTC: production DB/log investigation mapped `Зойкина квартира` to event `5755`: source `https://t.me/dramteatr39/4361` had Telegram photo media, but `event.photo_urls=[]`; `tg_event_publish` had already published text-only `/42`, and `vk_sync` had failed with `vk_sync_missing_media_for_telegram_event`.
- 2026-06-08 09:45 UTC: root cause found in `_rehydrate_missing_event_source_posters_for_telegraph`: it returned early when `len(source_rows) <= 1`, so single-source Telegram events with zero stored media never rehydrated source photos before Telegraph/TG/VK fanout. `build_tg_event_source_hash` also ignored media URLs, so a later media repair would not invalidate the old text-only Telegram post.
- 2026-06-08 09:45 UTC: single-source media rehydrate and media-aware TG idempotency deployed as Fly release `v1235` / image `deployment-01KTK9NSN3FDT6NW8CNEV0ZVTY`; `/healthz` returned ready.
- 2026-06-08 09:45 UTC: compensating catch-up for event `5755` rehydrated three source photos, rebuilt `https://telegra.ph/Zojkina-kvartira-06-07`, created VK postponed post `https://vk.com/wall-231920894_2412`, republished Telegram as album `https://t.me/c/3954607218/44`, and deleted old text-only `/42`. At the scheduled public slot VK exposed the post as `https://vk.com/wall-231920894_2413`, and production DB/job evidence was normalized to the public URL.
- 2026-06-08 10:18 UTC: operator requested a title-order diff between `@kldevents` and VK `klgdevents`. Telethon/VK API comparison showed Telegram top sequence `Заключительный гала-концерт` -> `Английский разговорный клуб` -> `Зойкина квартира` -> `Мастер и Маргарита` while public VK top was `Зойкина квартира` -> older VK queue items. Production DB confirmed some fresh Telegram posts pointed at much older managed VK posts, and several text-only Telegram posts had `vk_sync_missing_media_for_telegram_event`.
- 2026-06-08 10:36-10:44 UTC: live Telegram UI E2E on production bot `@events_love39_bot` ran `/vk_auto_import 1`; the first attempt showed an empty queue, `/vk_crawl_now` filled pending rows, and the next `/vk_auto_import 1` created event `5785` (`Лекция «О природе исторического знания»`). Verification found `vk_sync`, `telegraph_build`, `ics_publish`, and `tg_ics_post` done, but `tg_event_publish` was still pending with `next_run_at=2036-06-05`. Root cause: the previous cleanup placed malformed old `tg_event_publish` rows in far-future error state, and `next_tg_event_publish_run_at()` treated those rows as spacing anchors for new posts.
- 2026-06-08 10:50-10:54 UTC: far-future spacing fix deployed as Fly `v1238` / image `deployment-01KTKDN6GN14ND55NV03Z7D997`; event `5785`'s already-created Telegram job was manually rearmed to current time. Job runner published `@kldevents` post `66` as `photo_caption` with media and inline calendar button, while managed VK post `https://vk.com/wall-231920894_2423` remained visible by VK API. The temporary E2E operator DB row for `8336351413/@The_day_of_kk` was removed after verification.
- 2026-06-08 10:24 UTC: scheduling root cause found: `tg_event_publish` was enqueued before `vk_sync` and depended only on `telegraph_build`/`tg_ics_post`; scheduler code also used enqueue action strings (`new`/`requeued`/`skipped`) as dependency values instead of real outbox keys, and the job runner ignored `depends_on` during execution. As a result, an `error`/backoff `vk_sync` did not prevent Telegram publication.
- 2026-06-08 21:17 UTC: live production VK auto-import on `@events_love39_bot` updated event `5464` and correctly produced Telegraph, calendar, and VK artifacts, but `tg_event_publish` was deferred to `2026-06-09 16:30 UTC` (`18:30` Kaliningrad). Production queue inspection showed no morning `tg_event_publish` slots for 2026-06-09 and a stale evening cluster at `16:00-16:30 UTC`, including rows that already had `tg_event_post_url` values.
- 2026-06-08 21:35 UTC: root cause expanded: the previous next-day-anchor guard only ignored tomorrow anchors while the current-day publish window was still open. After `23:00` local time, the candidate normalized to tomorrow `07:00`, so stale same-next-day evening anchors again became spacing anchors and pushed fresh imports out of the morning.
- 2026-06-08 22:40 UTC: late-next-day spacing hotfix was deployed, stale already-published `tg_event_publish` anchors were marked done, and the 2026-06-09 morning Telegram queue was repaired to 10-minute slots. The live VK E2E event `5464` was moved to `2026-06-09 05:30 UTC` (`07:30` Kaliningrad), with Telegraph, calendar, and managed VK dependencies done.

## Root Cause

1. The new Telegram event publisher was implemented as text-plus-follow-up-media to preserve an inline calendar button, but this was not in the original requirement and produced the wrong channel format.
2. The calendar button used `event.ics_url` (raw Supabase `.ics`) instead of the existing project contract `event.ics_post_url` pointing to the Telegram calendar channel post.
3. Smart Update near-duplicate poster pruning relied on `phash`. A raw VK CDN `EventPoster` row without `phash` survived next to the managed storage row for the same image, so `event.photo_urls` contained both URLs.
4. `job_sync_vk_source_post` skipped publishing when `vk_source_hash` matched and `source_vk_post_url` looked like a managed `klgdevents` URL, without verifying that `wall.getById` still returned a real VK item.
5. `_event_has_telegram_origin` treated non-null `source_chat_id` / `source_message_id` as Telegram-origin. VK auto-import stores VK group/post ids in the same columns, so a VK-origin event could be blocked by the Telegram text-only media guard before managed VK publication.
6. The background `job_outbox_worker` protected job execution errors, but its periodic stats heartbeat ran outside that protection; a diagnostic `LookupError` could terminate the worker task and make health fail even though the main app remained alive.
7. The follow-up TG publisher still deduped media URLs only by exact string. Telegraph rendering could select/prune near-duplicate poster assets, while `publish_tg_event_announcement` still chose `album_caption` from multiple Supabase `p/dh16/...` URLs whose perceptual hashes differed by only a few bits.
8. The Telegraph source-media rehydrate helper only repaired multi-source events. A single Telegram source row with zero persisted posters was treated as "nothing to aggregate", so a valid source photo could be left out of Telegraph, Telegram event publishing, and VK sync.
9. Telegram event post idempotency hashed text fields but not media state. After a source-media repair, an already-published text-only post could still be treated as current unless the media signature participates in `tg_event_source_hash`.
10. The two-surface fanout contract was not encoded in the outbox graph. `tg_event_publish` could run without a successful managed `vk_sync`; dependency fields contained enqueue actions instead of real job keys; and the runner did not treat failed dependencies as blockers, so Telegram and VK could diverge in both set and order.
11. The quiet-window spacing guard treated stale next-day anchors differently before and after local window close. Once the candidate rolled to tomorrow morning, old evening backlog rows were considered legitimate anchors, leaving the next morning empty.

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
- Add a TG-side safety-net that collapses near-duplicate Supabase `p/dh16/...` media URLs before choosing single-photo vs album publishing mode.
- Rehydrate source media for single-source Telegram/VK events when the event has no stored media before Telegraph/TG/VK fanout.
- Include the deduped media signature in Telegram event idempotency, and republish/delete stale text-only messages when repaired media changes the Telegram post mode.
- Make `tg_event_publish` depend on managed `vk_sync` for publish-eligible events, and make the runner block pending/running/error dependency rows until they reach `done`.
- Ignore `error` `tg_event_publish` jobs and far-future pending/running anchors beyond `TG_EVENT_PUBLISH_SPACING_HORIZON_HOURS` when spacing new Telegram event posts, so cleanup/cancel markers cannot freeze fresh fanout.
- After the local publish window closes, ignore same-next-day pending/running backlog anchors that are far beyond the next morning start, so fresh imports schedule from the morning rhythm instead of inheriting an old evening cluster.
- Enforce superadmin access on `/vk_auto_import`, matching the documented command contract.
- Verify managed VK URL existence before treating a matching `vk_source_hash` as terminal; if `wall.getById` returns no item, republish via the normal VK sync path.
- Detect Telegram-origin events by `t.me` source URL or explicit `EventSource.source_type in ('telegram', 'tg')`, not by numeric source ids that VK imports also populate.
- Keep `job_outbox_worker` alive when stats/diagnostic logging fails, logging the stats failure without terminating the task.

## Follow-up Actions

- [ ] Add a replay fixture for `vk_inbox.id=8307` if live E2E exposes another gap outside unit coverage.
- [x] After the 2026-06-08 media safety-net deploy, run/repair production Telegram-source events with near-duplicate Supabase poster URLs and confirm duplicate-only messages are absent from `@kldevents`.

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
- follow-up deploy: SHA `7fffb2281ed00ab149e0b6b28537a7632524fc00`, Fly image `deployment-01KTJ1D15P1W4FX9NB4CM2YHTV`, machine `48e42d5b714228` version `1227`; `/healthz` returned HTTP 200 with `ok=true`, `ready=true`, `db=ok`, `job_outbox_worker=ok`, and `issues=[]`.
- near-duplicate media safety-net deploy: SHA `dcd9dc080f1455bf3d3344cbb3e591366940a752`, pushed to `origin/main` from clean linked worktree `hotfix/tg-event-media-dedup-20260608`; Fly release `v1234`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KTK8Z0JY5GBNT0RPSWPN2JER`, machine `48e42d5b714228`, check `1 total, 1 passing`. Repeated `/healthz` after deploy returned HTTP 200 with `ok=true`, `ready=true`, `db=ok`, `job_outbox_worker=ok`, `add_event_worker=ok`, and `issues=[]`. Runtime file mirror was enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs/events-bot.log` present).
- near-duplicate media regression checks:
  - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py main_part2.py tests/test_tg_event_publish.py`
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py` (`8 passed in 0.83s`)
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_genai_dump_and_poster_dedup.py tests/test_tg_event_publish.py` (`24 passed in 1.55s`)
- 2026-06-08 compensation evidence:
  - Telethon dHash before cleanup: `21/22` hamming `2`, `24/25` hamming `2`, `26/27` hamming `4`, `29/30` hamming `4`, `32/33` hamming `2`, `38/39` hamming `6`; `36/37` hamming `10` was left untouched as not an obvious duplicate under the deployed threshold.
  - Production bot token cleanup deleted messages `22`, `25`, `27`, `30`, `33`, `39`; local `.env` bot token returned `403`, so the action was retried from the Fly runtime env without exposing the secret.
  - Telethon verification after cleanup showed visible post IDs `21`, `23`, `24`, `26`, `28`, `29`, `31`, `32`, `34`, `35`, `36`, `37`, `38`, `40`, `41`; deleted duplicate-only IDs are absent. Existing message `24` remains a former media-group message, so Telegram did not expose an inline button even after duplicate cleanup; it still has the caption text calendar link. Future one-image posts follow the fixed `photo_caption` path and get inline buttons.
  - Additional operator-reported duplicates were cleaned after threshold widening: Telethon verification showed `/8`, `/11`, `/18`, `/36` visible while duplicate-only IDs `/9`, `/10`, `/12`, `/19`, and `/37` were absent.
  - Event `5755` (`Зойкина квартира`) catch-up after release `v1235`: production DB final state had `photo_count=3`, `telegraph_url=https://telegra.ph/Zojkina-kvartira-06-07`, `source_vk_post_url=https://vk.com/wall-231920894_2413`, `tg_event_post_url=https://t.me/c/3954607218/44`, `tg_event_post_mode=album_caption`, and media-aware `tg_event_source_hash=3583f214f4876fd32c51e601bf4c3acf362fb184358452df381931fd8ace5bc8`.
  - Telethon verification for `@kldevents` confirmed old text-only `/42` is absent and new `/44` is visible as an album caption with photo media; latest album continuation messages are `/45` and `/46`.
  - VK verification confirmed public `wall.getById` sees `https://vk.com/wall-231920894_2413` with photo attachments. Production DB/job evidence was normalized from postponed `2412` to public `2413`; `vk_sync` and `tg_event_publish` job rows for event `5755` are `done`.
  - Post-catch-up `/healthz` returned HTTP 200 with `ok=true`, `ready=true`, `job_outbox_worker=ok`, `add_event_worker=ok`, and `issues=[]`.
  - Channel title-order diff before the ordering fix showed `@kldevents` contained Telegram-only/far-old-VK events (`Мастер и Маргарита`, `Женитьба`, `Собачье сердце`, `№ 13`) while public VK did not; this is the regression that the `tg_event_publish -> vk_sync` dependency now guards.
  - Ordering regression checks after the code change:
    - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py tests/test_tg_event_publish.py tests/test_job_outbox_depends.py`
    - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py tests/test_job_outbox_depends.py tests/test_job_due_filter.py` (`15 passed in 2.22s`)
  - follow-up regression checks:
    - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py main_part2.py vk_hashtags.py vk_coauthors.py source_parsing/handlers.py source_parsing/smart_update_report.py tests/test_tg_event_publish.py tests/test_vk_source.py tests/test_vk_hashtags.py tests/test_smart_update_report_posts.py`
    - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m pytest tests/test_tg_event_publish.py tests/test_vk_source.py tests/test_vk_hashtags.py tests/test_smart_update_report_posts.py -q` (`48 passed in 6.52s`; pytest process was manually killed after emitting the success summary because the local shell session stayed open).
  - Local VK API smoke used the production user token to create a postponed `wall.post` with `copyright=https://vk.com/konb39`, `coauthors=-30777579`, and `coauthor_ids=-30777579`; VK returned `post_id=2397`, and immediate `wall.delete` returned `1`.
  - 2036-anchor hotfix: SHA `4453dbb8896752117c1541e24ceeb003b6daa43a`, pushed to `origin/main`, deployed to Fly as `v1238` image `deployment-01KTKDN6GN14ND55NV03Z7D997`; `/healthz` returned `ok=true`, `ready=true`, `issues=[]`.
  - Live Telegram UI E2E: `/vk_auto_import 1` on production bot initially returned an empty queue; `/vk_crawl_now` via the same UI checked `119` communities, scanned `42` posts, matched `15`, and added `15`; the next `/vk_auto_import 1` created event `5785`, with UI summary `inbox imported: 1`, `events created: 1`, `took_sec: 102.0`.
  - Final dual-surface verification for event `5785`: DB has `source_vk_post_url=https://vk.com/wall-231920894_2423`, `tg_event_post_url=https://t.me/c/3954607218/66`, `tg_event_post_mode=photo_caption`, `ics_post_url=https://t.me/c/2807919036/6510`; `tg_event_publish` job is `done` with `last_result=https://t.me/c/3954607218/66`; Telethon inspected `@kldevents/66` with media and inline button `📅 9 июня 18:30 · Добавить в календарь`; VK API `wall.getById` returned public `wall-231920894_2423` with the same title text.
  - Late-next-day spacing hotfix: SHA `27b930ce7058b93de196ed24ab41861dbf7aeb3f`, pushed to `origin/main`, deployed to Fly image `registry.fly.io/events-bot-new-wngqia:deployment-01KTMJDDEE93MN6YJD6RSXWECV`; `/healthz` after deploy returned `ok=true`, `ready=true`, `scheduler=ok`, `job_outbox_worker=ok`, and `issues=[]`.
  - Late-next-day regression checks:
    - `PYTHONDONTWRITEBYTECODE=1 /home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py vk_dynamic_cover.py tests/test_tg_event_publish.py`
    - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py` (`22 passed in 2.93s`)
  - Final production VK auto-import E2E for event `5464`: command `/vk_auto_import --limit=1` on `@events_love39_bot` processed inbox `8344` from `https://vk.com/wall-86702629_7739`, updated `Матч сборной России против сборной Тринидада и Тобаго`, and reported `queue processed: 1/1`, `inbox imported: 1`, `events updated: 1`.
  - Event `5464` data verification: date/time `2026-06-09 19:00`, venue `Ростех Арена`, address `Солнечный бульвар 25`, city `Калининград`, ticket link `https://matchday.rfs.ru`, `is_free=0`, and no concrete minimum price because the source post did not provide one. Telegraph `https://telegra.ph/Match-sbornoj-Rossii-protiv-sbornoj-Trinidada-i-Tobago-05-29` contains the title/date/venue/address/ticket link and 3 images.
  - Event `5464` publication verification: `tg_event_publish` is pending for `2026-06-09 05:30 UTC` (`07:30` Kaliningrad) with dependencies `telegraph_build:5464,tg_ics_post:5464,vk_sync:5464`; VK was re-synced after the stale managed URL `2494` was missing, and VK API confirmed postponed `https://vk.com/wall-231920894_2496` at `2026-06-09 04:00 UTC` (`06:00` Kaliningrad) with 2 photo attachments and the same title/location/ticket text.
  - 2026-06-09 Telegram morning catch-up queue was repaired to real external-source future events, not test fixtures: `5680`, `5396`, `5494`, `5464`, `5673`, `5517`, `5681`, `5518`, `5693`, `5573`, `5682`, and `5333` are scheduled every 10 minutes from `05:00` to `06:50 UTC`; each has a non-test `event_source` from Telegram/VK plus a managed VK URL and at least one poster. Only `5464` is the current VK auto-import E2E event; the rest is production catch-up backlog. Events `4060` and `5105` were excluded from the 2026-06-09 catch-up and their `tg_event_publish` jobs (`22595`, `22578`) were paused because their only substantive external sources were imported on `2026-04-20` and `2026-05-19`.

## Prevention

- Regression tests now cover captioned media publishing, calendar post button URL, scheduling dependency on `tg_ics_post`, persisted poster dedup with missing `phash`, stale managed VK URLs whose `wall.getById` item is missing, and `job_outbox_worker` resilience when stats logging fails.
- Regression tests now also cover single-source source-media rehydrate, media URL participation in Telegram event idempotency, borderline near-dHash duplicate collapse, and stale text-post replacement when media appears later.
- Regression tests now also cover the fanout order contract: Telegram event publishing depends on managed VK sync, and dependency errors/backoff block downstream publication instead of letting Telegram run ahead.
- Regression tests now also cover the 2036 cleanup-anchor regression and the late-next-day backlog regression: far-future cancelled/backlog rows and stale evening anchors after local window close do not push fresh Telegram event posts outside the normal morning rhythm.
