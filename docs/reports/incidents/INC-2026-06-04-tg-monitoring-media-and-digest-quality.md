# INC-2026-06-04 Telegram Monitoring Media And Prose-Location Quality

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / VK Publishing / Smart Update
Opened: 2026-06-04
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-04-tg-monitoring-vk-fanout-llm-quota-storm`, `INC-2026-06-02-vk-captcha-text-only-posts`, `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-07-vk-auto-import-merge-regression-gemma4`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-publishing/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/prompts.md`

## Summary

Three Telegram-origin events from `https://t.me/k_mira101/424` were created without media and then published as managed `klgdevents` VK posts with `attachments=0`. The same rows were repeatedly skipped by CherryFlash/video announce because no renderable posters existed. A separate Telegram-origin row `event_id=5569` from `https://t.me/molod_kld/3709` was also materialized from a one-line post (`🏠 Дайджест, мы его очень ждали`): the parser kept `title=Дайджест`, inferred `date=2026-06-07` from poster OCR (`1-7 июня`), and incorrectly saved `location_name=мы его очень ждали`.

## User / Business Impact

- Telegram Monitoring could create legitimate event rows that looked complete enough for `vk_sync`, but public VK posts appeared without illustrations.
- Promo `vk_publication` could bypass the `vk_sync` no-media guard and create a text-only promoted VK post for a Telegram-origin event. Missing promo media must be treated as a media-ingestion incident with recovery/catch-up, not as a final reason to delete or silently replace the original promoted event.
- Events without renderable posters were invisible to video announce selection and therefore could miss promo surfaces.
- Reaction/prose text could survive as `location_name`, letting a weak one-line post become a public event row and later a managed VK post.

## Detection

- Operator noticed Telegram Monitoring publications in VK without pictures and asked for production investigation.
- Production DB rows and runtime file logs confirmed the issue:
  - `event_id=5640`, `5641`, `5642` have `photo_urls=[]`, no poster rows, source `https://t.me/k_mira101/424`.
  - `post_to_vk ok ... post_id=1983/1984/1985 ... attachments=0`.
  - `video_announce.popular_review: skipped event without renderable posters event_id=5640/5641/5642`.
  - `event_id=5569` has `title=Дайджест`, `time=''`, `location_name=мы его очень ждали`, source `https://t.me/molod_kld/3709`, poster OCR `1-7 июня`, and managed VK URL `https://vk.com/wall-231920894_2000`.

## Timeline

- 2026-06-04 08:32:39 UTC: Smart Update created `event_id=5640`, `added_posters=0`.
- 2026-06-04 08:32:54 UTC: Smart Update created `event_id=5641`, `added_posters=0`.
- 2026-06-04 08:33:05 UTC: Smart Update created `event_id=5642`, `added_posters=0`.
- 2026-06-04 10:32-12:28 UTC: video announce repeatedly skipped all three rows because they had no renderable posters.
- 2026-06-04 14:04:43 UTC: VK created `wall-231920894_1983` for `event_id=5642` with `attachments=0`.
- 2026-06-04 14:04:50 UTC: VK created `wall-231920894_1984` for `event_id=5641` with `attachments=0`.
- 2026-06-04 14:04:58 UTC: VK created `wall-231920894_1985` for `event_id=5640` with `attachments=0`.
- 2026-06-04 14:07:48 UTC: VK created `wall-231920894_2000` for `event_id=5569` with `attachments=0`.
- 2026-06-04 20:00-21:00 UTC: initial investigation confirmed media evidence but added an over-broad digest-title guard.
- 2026-06-04 21:00 UTC: follow-up investigation found the exact `event_id=5569` row and replaced the broad digest-title guard with a prose-location regression.
- 2026-06-05 05:42/06:42 UTC: runtime file logs show organic `vk_sync:4417` failed closed with `vk_sync_missing_media_for_telegram_event`; `event_id=4417` source is `https://t.me/kraftmarket39/199`.
- 2026-06-05 06:40 UTC: promo `vk_publication` created a text-only `80 историй о главном` post for Telegram-origin `event_id=4417` even though organic `vk_sync` for the same event had failed closed.
- 2026-06-05 08:41 UTC: local Telethon source check using the E2E session confirmed `kraftmarket39/199` has `MessageMediaPhoto`; the media loss was between Telegram source fetch/import and persisted `event.photo_urls`, not in the source post.
- 2026-06-05 08:50 UTC: production repair rehydrated `event_id=4417` media from the source photo into managed storage and published replacement `https://vk.com/wall-231920894_2182` with `attachments_count=1`.

## Root Cause

1. `sync_vk_source_post` treated empty `event.photo_urls` / empty Telegraph fallback as acceptable for new Telegram-origin managed VK posts.
2. Telegram multi-event text posts can legitimately produce several event rows without media, but the publication boundary did not distinguish "DB row without media" from "public VK post without media".
3. The Telegram candidate location prose detector did not treat `мы его очень ждали` as prose, so a reaction phrase survived as `location_name`. Because a location was present, Smart Update did not fail closed on `missing_location`.

## Contributing Factors

- Existing captcha fail-closed coverage only handled media upload failures after photos existed; it did not cover the no-media-from-source case.
- Video announce correctly rejected rows without renderable posters, but this signal did not block VK publication.
- The first follow-up search looked for the wrong exact phrase (`мы давно его ждали`) and missed the actual row (`мы его очень ждали`), producing an over-broad first fix that was rolled back.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring result import, media/poster ingestion, `event.photo_urls`, `eventposter`, `sync_vk_source_post`, `post_to_vk`, Telegram candidate location grounding, Smart Update prose-location guards, or digest/multi-event prompt rules.
- Changing video announce media eligibility for Telegram-origin events.

### Affected surfaces

- `main_part2.py::sync_vk_source_post`
- `promo.py::_build_promo_vk_source_post`
- `source_parsing/telegram/handlers.py::_build_candidate`
- `smart_event_update.py::_smart_event_update_impl`
- `docs/llm/prompts.md`
- `JobOutbox(vk_sync)` and managed VK community `klgdevents`
- CherryFlash/video announce media selection

### Mandatory checks before closure or deploy

- Unit coverage that Telegram-origin `vk_sync` raises `vk_sync_missing_media_for_telegram_event` before `wall.post` when no attachment is available.
- Unit coverage that promo `vk_publication` attempts Telegraph media recovery for no-media Telegram candidates, records `FAILED_NO_MEDIA` audit evidence when recovery is still empty, ignores failed/invalidated exposures in rolling-window counts, and applies the same fail-closed media gate/logs `photo_urls_count` / `attachments_count` before `wall.post`.
- Existing VK captcha text-only regression still passes.
- Telegram candidate builder and Smart Update tests for `location_name=мы его очень ждали` as prose, plus a real digest negative control where time and venue/room are present.
- Production evidence collected from `/data/runtime_logs` and `/data/db.sqlite`.
- No compensating rerun/requeue/data repair unless the operator explicitly asks for it.

### Required evidence

- deployed SHA: `e76cec2c`
- tests: `24 passed` (`test_vk_source` media/captcha subset, `test_tg_candidate_location_grounding.py`, Smart Update prose-location regressions)
- production DB/log evidence: `event_id=5569` row from `https://t.me/molod_kld/3709`; `post_to_vk ok ... post_id=2000 ... attachments=0`; `event_id=5640/5641/5642` rows from `https://t.me/k_mira101/424`; `post_id=1983/1984/1985 ... attachments=0`
- confirmation that fix is reachable from `origin/main`: `e76cec2c` pushed to `origin/main`

## Immediate Mitigation

- Added a fail-closed VK publication guard for Telegram-origin events without any renderable `photo...` attachment. Default is enabled; emergency override is `VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS=0`.
- Rolled back the over-broad generic digest-title guard.
- Tightened Telegram and Smart Update prose-location detection so reaction text such as `мы его очень ждали` cannot remain a venue.

## Corrective Actions

- `main_part2.py`: new Telegram-origin media requirement before creating a managed VK event post.
- `promo.py`: promo `vk_publication` tries Telegraph media recovery for Telegram-origin candidates with empty `event.photo_urls`, records `FAILED_NO_MEDIA` audit rows with source URL/media counts/recovery action when recovery is still empty, ignores failed/invalidated exposure rows when evaluating rolling-window delivery, logs per-event media evidence, and raises `vk_sync_missing_media_for_telegram_event` before `post_to_vk` for Telegram-origin promoted events with no uploaded VK photo attachments.
- `source_parsing/telegram/handlers.py`: `location_name` prose detector now drops `мы его очень ждали`-style reaction text.
- `smart_event_update.py`: direct candidates with the same prose `location_name` fail closed as `invalid:prose_location`.
- Docs and changelog updated; replay fixture added in `tests/replays/INC-2026-06-04-tg-monitoring-media-and-digest-quality/`.

## Follow-up Actions

- [ ] Add a broader future-active quality audit for giveaway-derived and charity-collection rows such as `event_id=5629` and `event_id=5657` without performing automatic repair during this incident.
- [ ] Consider an operator alert for repeated `video_announce.popular_review: skipped event without renderable posters` on newly-created Telegram-origin rows.

## Release And Closure Evidence

- deployed SHA: `e76cec2c`
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean worktree, commit reachable from `origin/main`
- regression checks: `24 passed` target set covering Telegram media fail-closed, VK captcha no-text-only regression, Telegram location grounding, and Smart Update `prose_location`
- post-deploy verification: Fly machine `48e42d5b714228` version `1189` started with `1 total, 1 passing`; `/healthz` returned `ok=true`, `ready=true`, `db=ok`, worker/scheduler tasks ok; container check confirmed `generic_guard_absent=True`, `reaction_prose_fix=True`, `vk_guard=True`

### 2026-06-05 Promo VK Follow-Up

- deployed SHA: `fa49b73095046ff468895e324c5809aabd62badb`
- deploy image: `events-bot-new-wngqia:deployment-01KTBD1N9H2HWMXPWV4M2J9ARD`
- Fly evidence: machine `48e42d5b714228` version `1194`, checks `1 total, 1 passing`; `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`; container probe confirmed `PUBLIC_PROMO_EXPOSURE_STATUSES` and promo missing-media candidate guard in `/app/promo.py`.
- regression checks: `28 passed` (`tests/test_promo.py`, two VK source no-text-only regressions, location aliases, Telegram Monitor prompt contract, master prompt historical-date contract) plus `py_compile promo.py kaggle/TelegramMonitor/telegram_monitor.py` and `git diff --check`.
- runtime logging evidence: production container has `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, `RUNTIME_LOG_BASENAME=events-bot.log`, `RUNTIME_LOG_RETENTION_HOURS=24`; active file `/data/runtime_logs/events-bot.log` present.
- compensating catch-up (initial containment): text-only promo exposure `45` for `event_id=4417` was invalidated as `FAILED_NO_MEDIA`; public text-only post `https://vk.com/wall-231920894_2157` was deleted via `wall.delete` (`response=1`); replacement exposure `46` scheduled `event_id=4446` with media at `https://vk.com/wall-231920894_2166`, `vk_post_date=2026-06-05T15:40:00+00:00`.

### 2026-06-05 Promo Media Recovery Correction

- Operator feedback corrected the containment decision: deleting or replacing a no-media promo is not sufficient when the source had a picture and the campaign needs reach. Missing promo media is now a recovery incident.
- Source evidence: local Telethon check of `https://t.me/kraftmarket39/199` confirmed `has_media=true`, `media_type=MessageMediaPhoto`, source text for `Калининградский морской торговый порт...`; downloaded investigation artifact stored under `artifacts/codex/vk-incidents-20260605/`.
- Runtime log evidence: `/data/runtime_logs/events-bot.log.2026-06-05_05` and `_06` contain `RUN pick key=vk_sync:4417`, `source_post_url=https://t.me/kraftmarket39/199`, and `sync_vk_source_post blocked text-only telegram source post event_id=4417`.
- Production data repair: `event_id=4417` now has `photo_count=1`, `photo_urls_count=1`, managed media URL `https://storage.yandexcloud.net/kenigevents/p/dh16/0c/0c400d881266892109a14c65597554f264cf41dc51d32e1dae6cb4e6583e465e.webp`.
- Public repair: replacement promo `https://vk.com/wall-231920894_2182` is scheduled for `2026-06-05T17:20:00+00:00`, text starts `Калининградский морской торговый порт: история и современность`, and VK API verification shows `attachments_count=1`.
- Audit repair: `promo_exposure.id=45` remains `FAILED_NO_MEDIA` with `replacement_url=https://vk.com/wall-231920894_2182` and `recovered_media_url=...`; new `promo_exposure.id=47` is `VK_SCHEDULED`, `placement_kind=manual_incident_media_recovery`, `event_id=4417`, target `https://vk.com/wall-231920894_2182`.
- Regression checks for the corrected behavior: `tests/test_promo.py::test_promo_vk_publication_recovers_telegraph_media_before_posting` and `tests/test_promo.py::test_promo_vk_publication_records_no_media_candidate_as_failed` plus existing promo media tests passed locally (`5 passed` targeted subset).
- Deployed correction SHA: `1c5a53ab313f80536236c918c842beea54354b0f`, pushed to `origin/main`.
- Deploy evidence: Fly image `events-bot-new-wngqia:deployment-01KTBG2ZYWTMYCJA8Z0EMHABW5`, machine `48e42d5b714228` version `1195`, checks `1 total, 1 passing`; `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`.
- Container probe after deploy confirmed `/app/promo.py` contains Telegraph recovery (`_ensure_promo_vk_photo_urls` + `extract_telegraph_image_urls`), `FAILED_NO_MEDIA` audit action `investigate_source_media_and_rehydrate_before_publication`, and `recent_failed_no_media_event_ids` dedupe.
- Release checks: `23 passed` (`tests/test_promo.py` plus two VK source no-text-only/captcha regressions), `py_compile promo.py`, `git diff --check`.

## Prevention

- Telegram-origin VK publication now fails closed when the media set is empty, so a missing-media import issue remains visible as a `vk_sync`/promo audit failure instead of becoming a text-only public VK post.
- Promo no-media containment must be followed by source-media recovery and replacement publication for the same promoted event when source media exists; deleting a text-only promo alone is not closure.
- Prose-location tests prevent reaction text from satisfying the mandatory venue/location anchor for one-line Telegram candidates.
