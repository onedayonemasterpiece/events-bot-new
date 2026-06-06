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

## Root Cause

1. The VK event hashtag builder used raw `event.festival` when formatting the final hashtag line. If the event row carried an inflected label such as `Кантаты`, the public hashtag inherited that form.
2. `sync_vk_source_post` resolved `Festival` by exact `Festival.name == event.festival` only. Canonical `Festival(name="Кантата")` could not be found from `event.festival="Кантаты"`, so even the existing canonical festival object was not available to the hashtag path.
3. Guide scheduler code had tests/contract for `maybe_dispatch_critical_scheduler_watchdog`, but runtime code did not implement the watchdog on `origin/main`; guide full daily slot was not covered by the independent live watchdog loop.
4. Guide APScheduler jobs used a very short misfire grace window for a critical daily slot, making deploy/startup lag more likely to drop a run instead of recovering it.
5. `/healthz` did not expose guide job state, so a green health check did not mean guide monitoring was scheduled.

## Contributing Factors

- The first assistant response treated a code-path inference as a factual production-data claim. Production evidence must be explicitly separated from hypotheses.
- Runtime file mirror is normally disabled on production volume, so scheduler forensics depend on Fly auth, DB evidence, or Kaggle artifacts unless temporary file logging is enabled.
- Release-auth instructions were incomplete: they pointed at `~/.fly/config.yml access_token`, but did not cover the observed 2026-06-06 failure mode where Fly rewrote `~/.fly/config.yml` to WireGuard-only state without usable auth, nor did they route agents to the shared devserver token file or Codex/Claude session-history recovery before asking the user.

## Automation Contract

### Treat as regression guard when

- Changing `vk_hashtags.py`, `build_vk_source_message`, `sync_vk_source_post`, festival matching, or `Festival`/`event.festival` semantics for VK posts.
- Changing guide monitoring scheduler registration, `/healthz`, watchdog loops, heavy-operation gating, guide digest scheduled publish, or guide VK digest/card fanout.
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

### Mandatory checks before closure or deploy

- `python -m pytest -q tests/test_vk_hashtags.py tests/test_vk_source.py`
- Guide critical watchdog tests in `tests/test_scheduling.py`:
  - `test_critical_scheduler_watchdog_dispatches_guide_full_after_light_run_only`
  - `test_critical_scheduler_watchdog_skips_guide_when_full_run_exists`
  - `test_critical_scheduler_watchdog_retries_guide_after_remote_busy_skip`
- `python -m py_compile scheduling.py main.py main_part2.py vk_hashtags.py`
- VK API verification for `wall-231920894_2314`: hashtag line must contain `#Кантата` and must not contain `#Кантаты`.
- Post-deploy `/healthz` must expose `guide_excursions_light` and `guide_excursions_full` statuses, not only generic scheduler status.
- Because the incident touched a daily scheduled production task, closure requires same-day guide `full` catch-up evidence: a successful/partial `ops_run(kind='guide_monitoring', details.mode='full')`, a published/empty-candidate scheduled digest evidence, or a clearly documented blocker.

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

## Follow-up Actions

- [x] Restore and document local Fly auth bootstrap: `/home/dev/.config/fly/release.env` is the shared devserver token file, `flyctl auth whoami` verifies `md.nikiforov@gmail.com`, and release-governance now requires session-history recovery if Fly config loses usable auth.
- [ ] Consider temporary runtime file logging during future scheduled-job incident windows, with explicit disk budget and retention.

## Release And Closure Evidence

- corrective code SHA: `a6e0915d317f43d658193d25c98052b2ce9622ce`, reachable from `origin/main`; later doc-only incident evidence commits may advance the latest `origin/main` SHA without changing the fix
- deploy path: pending; must be manual `flyctl deploy` from clean `origin/main`
- invalid Actions attempts: `27071080197` and `27071216727`, caused by stale docs/workflow drift; these do not count as project deploy evidence
- release-auth recovery: shared devserver token file `/home/dev/.config/fly/release.env` created with `0600`; `flyctl auth whoami` verified `md.nikiforov@gmail.com`
- deploy status: not deployed to production yet; manual `flyctl deploy` pending after release-governance instruction commit is pushed
- regression checks:
  - `python -m pytest -q tests/test_vk_hashtags.py tests/test_vk_source.py tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_guide_full_after_light_run_only tests/test_scheduling.py::test_critical_scheduler_watchdog_skips_guide_when_full_run_exists tests/test_scheduling.py::test_critical_scheduler_watchdog_retries_guide_after_remote_busy_skip` printed `38 passed`; process then had to be stopped because imported runtime threads kept pytest alive after summary
  - `python -m pytest -q tests/test_scheduling.py::test_runtime_health_status_reports_guide_jobs` -> `1 passed`
  - `python -m py_compile scheduling.py main.py main_part2.py vk_hashtags.py` -> passed
- VK mitigation verification: `wall-231920894_2314` re-fetch shows `#Кантата` present and `#Кантаты` absent, with 2 attachments preserved
- post-deploy verification: pending until Fly deploy token is restored and deploy completes
- guide same-day catch-up/digest evidence: pending until production deploy and production runtime/DB evidence are available

## Prevention

- Canonical festival hashtag tests cover raw inflected event labels and `Festival.name` preference.
- Guide critical watchdog tests cover missed-full catch-up, full-run idempotence, and retry after remote-session-busy skipped runs.
- `/healthz` guide job visibility prevents a green health check from hiding missing guide scheduler slots.
