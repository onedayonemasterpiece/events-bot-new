# INC-2026-06-06 Guide Monitoring Missed Slot And VK Festival Hashtag

Status: open
Severity: sev1
Service: Guide excursions scheduled monitoring; VK event publishing
Opened: 2026-06-06
Closed: —
Owners: automation / VK publishing / guide monitoring
Related incidents: `INC-2026-04-21-guide-gemma4-partial-monitoring`, `INC-2026-04-23-guide-digest-extraction-loss`, `INC-2026-05-29-guide-vk-digest-missing-media`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`

## Summary

6 июня 2026 пользователь сообщил две production-деградации:

- VK event post `https://vk.com/wall-231920894_2314` получил фестивальный hashtag `#Кантаты` вместо canonical search tag `#Кантата`.
- Scheduled guide excursions monitoring did not produce the expected production run/digest after the previous guide VK card-generation changes, so the daily critical guide slot was effectively missed.

Это sev1, потому что дефект был user-visible в публичной VK-ленте и одновременно затронул critical daily scheduled guide surface.

## User / Business Impact

- Поиск по фестивалю в VK был испорчен для опубликованного поста: canonical hashtag `#Кантата` отсутствовал.
- Автоматический мониторинг экскурсий не дал ожидаемый daily output, поэтому свежие экскурсионные находки могли не попасть в digest/VK fanout.
- `/healthz` на момент первичной проверки был green, но не показывал guide scheduler slots; это скрывало деградацию.

## Detection

- Обнаружено вручную пользователем по публичному VK URL и отсутствию ожидаемой guide monitoring/digest активности.
- Подтверждено через VK API с production user token: post `wall-231920894_2314` содержал `#Кантаты`, не содержал `#Кантата`, `can_edit=1`, attachments=2.
- `/healthz` был reachable/green, но scheduler payload не раскрывал `guide_excursions_light` / `guide_excursions_full`.
- Fly runtime logs / production DB `ops_run` evidence were not fully available during first triage because local Fly auth was missing; this is an observability/release-tooling gap, not proof of absence.

## Timeline

- 2026-06-06, before triage: user reports wrong VK festival hashtag and missed guide monitoring production run.
- 2026-06-06T18:53Z: incident worktree created from `origin/main` at `f6bb27be`; code triage identifies raw `event.festival` hashtag path and missing guide critical watchdog implementation on `origin/main`.
- 2026-06-06T18:55Z: VK API evidence confirms `#Кантаты` in `wall-231920894_2314`; post is editable and has 2 attachments.
- 2026-06-06T18:56Z: immediate mitigation edits the public VK post to replace `#Кантаты` with `#Кантата`, preserving attachments; re-fetch verifies `#Кантата` present and `#Кантаты` absent.
- 2026-06-06: corrective code changes prepared for canonical festival hashtag resolution, guide scheduler health visibility, and guide critical catch-up watchdog.
- 2026-06-06T18:59Z: fix `a6e0915d` pushed to `origin/main`; agent incorrectly attempted GitHub Actions deploy because stale release-governance instructions still allowed it and `.github/workflows/fly-deploy.yml` existed.
- 2026-06-06T19:00Z: first invalid Actions deploy attempt failed while Docker Hub timed out resolving `python:3.12-slim`.
- 2026-06-06T19:02Z: rerun built and pushed GHCR image `ghcr.io/onedayonemasterpiece/events-bot-new/events-bot:a6e0915d317f43d658193d25c98052b2ce9622ce`, then failed at `flyctl deploy` because Actions `FLY_API_TOKEN` was empty. This was not a valid project release path; production deploy still required manual local `flyctl deploy`.
- 2026-06-06T19:07Z: follow-up record commit `2edb67eb` also built and pushed GHCR image `ghcr.io/onedayonemasterpiece/events-bot-new/events-bot:2edb67ebacc5f8c6718aa5ce120535da6c012eb3`, then failed at the same invalid Actions `flyctl deploy` step.
- 2026-06-06T19:47Z: local Fly release auth restored without user secret handoff by recovering a valid token from Codex session history, storing it in user-level `/home/dev/.config/fly/release.env` (`0600`) for all agents/projects on this devserver, and verifying `flyctl auth whoami` as `md.nikiforov@gmail.com`.
- 2026-06-06T19:59Z: manual Fly deploy from clean `origin/main` completed with image `registry.fly.io/events-bot-new-wngqia:deployment-01KTF87YGAVTBVTN2EY7FAVH65`; `/healthz` showed `guide_excursions_light=ok` and `guide_excursions_full=ok`.
- 2026-06-06T19:59Z-20:04Z: guide critical watchdog dispatched same-day `full` catch-up, but stale `/data/kaggle_jobs.json` entry `run_id=eb390776814f` first blocked new runs as `remote_telegram_session_busy`; after removing that stale registry entry, fresh catch-up `run_id=6c0eaf799628` pushed Kaggle kernel version `277` and then failed because `GetKernelSessionStatus` HTTP 500 was treated as fatal.
- 2026-06-06T20:11Z: hotfix `b8c6c050` was deployed manually through Fly image `registry.fly.io/events-bot-new-wngqia:deployment-01KTF8YJD89JH9WZY2FEFF9MZT`; `/healthz` stayed ok/ready with guide scheduler slots visible.
- 2026-06-06T20:13Z-20:18Z: after the stale registry entry for `run_id=6c0eaf799628` was manually removed, watchdog launched a fresh `full` catch-up `ops_run_id=1976`, `run_id=5273f7f8a26f`, kernel version `278`. This exposed an additional session-boundary incident: removing a `guide_monitoring` registry entry while Kaggle status is `UNKNOWN` / `GetKernelSessionStatus` 5xx can allow a second Kaggle kernel to run concurrently with the same `TELEGRAM_AUTH_BUNDLE_S22` and invalidate the Telethon auth key. From this point forward, `UNKNOWN` registry entries are treated as active until terminal evidence or explicit user-approved auth replacement.
- 2026-06-06T20:26Z-20:33Z: fresh Kaggle output for `run_id=5273f7f8a26f` was downloaded and imported server-side without launching another Telethon/Kaggle scan. Recovery import `ops_run_id=1977` completed as partial: `sources_scanned=20`, `posts_scanned=42`, `posts_prefiltered=19`, `occurrences_created=2`, `occurrences_updated=11`, `llm_ok=19`, with `AuthKeyDuplicatedError` on sources still affected by the duplicated S22 session.
- 2026-06-06T20:33Z: same-day `new_occurrences` digest issue `92` was published to Telegram targets `@wheretogo39` (`message_id=150`) and `@youwillsee39` (`message_id=168`).
- 2026-06-06T20:33Z: VK fanout for issue `92` failed with `RuntimeError: Guide VK digest requires materialized media assets`; the new production carousel path was attempted only after afisha upload, so hook-only cards could not publish when `media_items_json=[]`.
- 2026-06-06T20:46Z: issue `92` was published to VK postponed post `https://vk.com/wall-238875824_33` with 2 rendered hook-only carousel attachments. Follow-up inspection found the source post `https://vk.com/wall-99453147_1475` had a public photo attachment, but imported `guide_monitor_post.id=920` had `media_refs_json=[]`, `media_assets_json=[]`; the image was lost in the VK guide scanner before digest materialization.

## Root Cause

1. The VK event hashtag builder used raw `event.festival` when formatting the final hashtag line. If the event row carried an inflected label such as `Кантаты`, the public hashtag inherited that form.
2. `sync_vk_source_post` resolved `Festival` by exact `Festival.name == event.festival` only. Canonical `Festival(name="Кантата")` could not be found from `event.festival="Кантаты"`, so even the existing canonical festival object was not available to the hashtag path.
3. Guide scheduler code had tests/contract for `maybe_dispatch_critical_scheduler_watchdog`, but runtime code did not implement the watchdog on `origin/main`; guide full daily slot was not covered by the independent live watchdog loop.
4. Guide APScheduler jobs used a very short misfire grace window for a critical daily slot, making deploy/startup lag more likely to drop a run instead of recovering it.
5. `/healthz` did not expose guide job state, so a green health check did not mean guide monitoring was scheduled.
6. Guide Kaggle polling treated `GetKernelSessionStatus` HTTP 5xx as fatal instead of transient, so a Kaggle API outage aborted the catch-up immediately after the stale registry was cleared.
7. During incident handling, a `guide_monitoring` registry entry was manually removed while Kaggle status was `UNKNOWN` because `GetKernelSessionStatus` returned HTTP 5xx. That bypassed the existing `remote_telegram_session_busy` guard and could start two Kaggle kernels with the same `TELEGRAM_AUTH_BUNDLE_S22`, risking Telegram `AuthKeyDuplicatedError`.
8. Guide VK fanout required materialized afisha assets before attempting the new carousel renderer. A digest issue with no usable source media could therefore fail before generating hook-only carousel cards, even though the production format supports text-derived card images plus CTA.
9. The guide Kaggle VK scanner parsed `attachments` from `wall.get`, but `_vk_post_to_scanned_post()` returned `media_refs=[]` / `media_assets=[]` for VK posts and `process_source()` materialized media only for Telegram. VK source photos were therefore dropped during the normal scan, not during card rendering.

## Contributing Factors

- The first assistant response treated a code-path inference as a factual production-data claim. Production evidence must be explicitly separated from hypotheses.
- Runtime file mirror is normally disabled on production volume, so scheduler forensics depend on Fly auth, DB evidence, or Kaggle artifacts unless temporary file logging is enabled.
- Release-auth instructions were incomplete: they pointed at `~/.fly/config.yml access_token`, but did not cover the observed 2026-06-06 failure mode where Fly rewrote `~/.fly/config.yml` to WireGuard-only state without usable auth, nor did they route agents to the shared devserver token file or Codex/Claude session-history recovery before asking the user.
- The incident runbook did not explicitly forbid manual `kaggle_registry` removal for `UNKNOWN` guide jobs; the code path treated `UNKNOWN` as busy, but the manual recovery action bypassed that protection.

## Automation Contract

### Treat as regression guard when

- Changing `vk_hashtags.py`, `build_vk_source_message`, `sync_vk_source_post`, festival matching, or `Festival`/`event.festival` semantics for VK posts.
- Changing guide monitoring scheduler registration, `/healthz`, watchdog loops, heavy-operation gating, guide digest scheduled publish, or guide VK digest/card fanout.
- Touching `kaggle_registry`, `remote_telegram_session.py`, guide Kaggle recovery, or any manual production recovery step that can remove/replace a `guide_monitoring` registry entry.
- Any incident involving missed daily scheduled guide runs, guide digest publication, or public VK hashtag/search quality.

### Affected surfaces

- `vk_hashtags.py`
- `main_part2.py::build_vk_source_message`
- `main_part2.py::sync_vk_source_post`
- `main_part2.py::_resolve_event_festival`
- `scheduling.py` guide job registration and critical watchdog
- `main_part2.py` runtime health report and watchdog loop
- VK API `wall.getById` / `wall.edit`
- Fly `/healthz`
- `ops_run(kind='guide_monitoring')`
- `kaggle_registry` / `/data/kaggle_jobs.json`
- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py` VK media materialization
- `TELEGRAM_AUTH_BUNDLE_S22` remote-session boundary

### Mandatory checks before closure or deploy

- `python -m pytest -q tests/test_vk_hashtags.py tests/test_vk_source.py`
- Guide critical watchdog tests in `tests/test_scheduling.py`:
  - `test_critical_scheduler_watchdog_dispatches_guide_full_after_light_run_only`
  - `test_critical_scheduler_watchdog_skips_guide_when_full_run_exists`
  - `test_critical_scheduler_watchdog_defers_guide_after_remote_busy_skip`
- `python -m py_compile scheduling.py main.py main_part2.py vk_hashtags.py`
- `/home/dev/projects/events-bot-new/.venv/bin/pytest -q tests/test_guide_vk_digest.py`
- VK API verification for `wall-231920894_2314`: hashtag line must contain `#Кантата` and must not contain `#Кантаты`.
- Post-deploy `/healthz` must expose `guide_excursions_light` and `guide_excursions_full` statuses, not only generic scheduler status.
- Because the incident touched a daily scheduled production task, closure requires same-day guide `full` catch-up evidence: a successful/partial `ops_run(kind='guide_monitoring', details.mode='full')`, a published/empty-candidate scheduled digest evidence, or a clearly documented blocker.
- Never remove a `guide_monitoring` `kaggle_registry` entry while Kaggle status is `UNKNOWN` or `GetKernelSessionStatus` returns HTTP 5xx/network errors. That state is a live-session lock, not stale evidence. Cleanup is allowed only after terminal Kaggle status, fresh output import, or explicit user approval to abandon the old auth bundle after replacement.
- If a matching Kaggle output bundle can be downloaded and its `guide_excursions_results.json` has the registry `run_id`, that output is terminal evidence: recovery must import it and clear the `guide_monitoring` registry entry instead of keeping a false `remote_telegram_session_busy` lock.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Release path: manual `flyctl deploy` from clean, pushed main.
- VK post verification output for `wall-231920894_2314`.
- `/healthz` after deploy with guide scheduler fields.
- Guide full catch-up/digest evidence or blocker.

## Immediate Mitigation

- Edited `https://vk.com/wall-231920894_2314` through VK API using the production VK user token.
- Preserved both existing photo attachments and verified the final hashtag line:
  `#анонс #анонс39 #кудапойтиКалининград #афишаКалининград #Калининград #12июня #12_июня #Кантата`.

## Corrective Actions

- Make event VK hashtags prefer canonical `Festival.name` when a festival can be resolved.
- Resolve event festivals not only by exact name but also by normalized aliases and narrow Russian genitive variants needed for canonical festival names such as `Кантата` -> `Кантаты`.
- Include guide `light` and `full` job states in runtime health.
- Raise guide monitoring misfire grace to the documented critical-slot default.
- Run guide critical watchdog from the independent watchdog loop and dispatch missed `full` runs through the same scheduled guide path with heavy gate `wait`.
- Treat Kaggle status HTTP 5xx as transient while polling guide monitoring kernels.
- Treat `guide_monitoring` registry entries with `UNKNOWN` Kaggle status / Kaggle status API 5xx as active remote sessions; do not clear them or start a second guide Kaggle run without terminal evidence or explicit user-approved auth replacement.
- When status lookup is `UNKNOWN`/HTTP 5xx but matching output is already downloadable, treat output as terminal evidence, import it, and clear the registry without starting another remote Telegram session.
- Build/upload guide VK carousel slides before requiring materialized afisha assets; if hook-only carousel upload succeeds, publish it as normal VK photo attachments, and fail closed only when neither carousel nor afisha-grid attachments can be uploaded.
- Materialize VK guide source `photo` attachments during the normal Kaggle scan (`media_refs/media_assets`) and keep server-side VK media recovery only as repair insurance for already-imported rows that predate the scanner fix.
- Defer critical guide watchdog retries after `remote_telegram_session_busy` by `GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS` instead of retrying every minute while the same `UNKNOWN` Kaggle/session lock is still present.

## Follow-up Actions

- [x] Restore and document local Fly auth bootstrap: `/home/dev/.config/fly/release.env` is the shared devserver token file, `flyctl auth whoami` verifies `md.nikiforov@gmail.com`, and release-governance now requires session-history recovery if Fly config loses usable auth.
- [x] Add session-boundary guard to project instructions and guide runbook: never manually clear `guide_monitoring` registry on `UNKNOWN`/Kaggle status 5xx; this state remains a live lock for `TELEGRAM_AUTH_BUNDLE_S22`.
- [ ] Consider temporary runtime file logging during future scheduled-job incident windows, with explicit disk budget and retention.

## Release And Closure Evidence

- corrective code SHA: `a6e0915d317f43d658193d25c98052b2ce9622ce`, reachable from `origin/main`; later doc-only incident evidence commits may advance the latest `origin/main` SHA without changing the fix
- deploy: manual `flyctl deploy --config fly.toml --app events-bot-new-wngqia --remote-only` from clean `main` at `c4b34b75`; Fly image `deployment-01KTF87YGAVTBVTN2EY7FAVH65`; machine `48e42d5b714228` version `1206`, `1/1` checks passing
- invalid Actions attempts: `27071080197` and `27071216727`, caused by stale docs/workflow drift; these do not count as project deploy evidence
- release-auth recovery: shared devserver token file `/home/dev/.config/fly/release.env` created with `0600`; `flyctl auth whoami` verified `md.nikiforov@gmail.com`
- `/healthz`: ok/ready true, `guide_excursions_light=ok`, `guide_excursions_full=ok`, next full run `2026-06-07T18:10:00+00:00`
- catch-up evidence before Kaggle 5xx poll hotfix: watchdog dispatched; stale registry `guide_monitoring:zigomaro/guide-excursions-monitor` from `2026-06-06T07:00:23Z` was removed; fresh catch-up `ops_run_id=1968`, `run_id=6c0eaf799628`, pushed kernel version `277`, then failed on `GetKernelSessionStatus` HTTP 500
- hotfix deploy: manual `flyctl deploy --config fly.toml --app events-bot-new-wngqia --remote-only` from clean `main` at `b8c6c050`; Fly image `deployment-01KTF8YJD89JH9WZY2FEFF9MZT`; machine `48e42d5b714228` version `1207`, `1/1` checks passing
- catch-up evidence after Kaggle 5xx poll hotfix: `ops_run_id=1976`, `run_id=5273f7f8a26f`, `mode=full`, `status=running`; logs show repeated `guide_monitor.kernel_poll_transient_error` for `GetKernelSessionStatus` HTTP 500 without aborting; Kaggle output probe still returned no `guide_excursions_results.json` by 2026-06-06T20:18Z
- recovery import evidence: fresh Kaggle output for `run_id=5273f7f8a26f` was downloaded to `/data/guide_monitoring_results/guide-excursions-5273f7f8a26f/guide_excursions_results.json`; server-side recovery import `ops_run_id=1977` completed partial with `occurrences_created=2`, `occurrences_updated=11`, `llm_ok=19`, and `AuthKeyDuplicatedError` errors on sources affected by the duplicated S22 session
- digest evidence: guide digest issue `92` published to Telegram (`@wheretogo39` message `150`, `@youwillsee39` message `168`); initial VK fanout failed because hook-only carousel generation was gated behind materialized media upload
- regression checks:
  - `python -m pytest -q tests/test_vk_hashtags.py tests/test_vk_source.py tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_guide_full_after_light_run_only tests/test_scheduling.py::test_critical_scheduler_watchdog_skips_guide_when_full_run_exists tests/test_scheduling.py::test_critical_scheduler_watchdog_retries_guide_after_remote_busy_skip` printed `38 passed`; process then had to be stopped because imported runtime threads kept pytest alive after summary
  - `python -m pytest -q tests/test_scheduling.py::test_runtime_health_status_reports_guide_jobs` -> `1 passed`
  - `python -m py_compile scheduling.py main.py main_part2.py vk_hashtags.py` -> passed
  - `/home/dev/projects/events-bot-new/.venv/bin/pytest -q tests/test_guide_kaggle_service.py` -> `2 passed`
  - `/home/dev/projects/events-bot-new/.venv/bin/pytest -q tests/test_guide_vk_digest.py` -> `4 passed`
- VK mitigation verification: `wall-231920894_2314` re-fetch shows `#Кантата` present and `#Кантаты` absent, with 2 attachments preserved
- post-deploy verification: `/healthz` shows guide scheduler slots and Fly machine is healthy
- guide same-day catch-up/digest evidence: recovery import `ops_run_id=1977` completed partial from fresh Kaggle output and digest issue `92` published to Telegram; VK postponed post `https://vk.com/wall-238875824_33` published but initially used hook-only cards because VK source media was not materialized

## Prevention

- Canonical festival hashtag tests cover raw inflected event labels and `Festival.name` preference.
- Guide critical watchdog tests cover missed-full catch-up, full-run idempotence, and cooldown deferral after remote-session-busy skipped runs.
- `/healthz` guide job visibility prevents a green health check from hiding missing guide scheduler slots.
- Guide/Kaggle recovery instructions now make `UNKNOWN` status a session lock: agents must not manually delete the registry entry or trigger another guide run while status lookup is failing.
