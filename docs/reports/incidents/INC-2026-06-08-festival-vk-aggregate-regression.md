# INC-2026-06-08 Festival VK Aggregate Regression

Status: investigating
Severity: sev2
Service: `/start` add-event, festival queue, VK festival aggregate publishing, Telegram Monitoring containment
Opened: 2026-06-08
Closed: —
Owners: engineering
Related incidents: `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel`, `INC-2026-06-05-vk-story-forward-wall-first`
Related docs: `docs/features/festivals/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/backlog/features/festival-monitoring-debt/README.md`, `docs/operations/release-governance.md`

## Summary

Operator-added event flow produced unexpected postponed VK output: an obsolete `kenigeventsofficial` VK community received a festival-like aggregate post, and `kldevents` received postponed content with weak/broken links around `80 историй о главном`. This exposed that `/start` -> `Добавить событие` and festival aggregate publishing were not aligned with the Smart Update contracts used by VK auto import and Telegram Monitoring.

## User / Business Impact

- Operators cannot trust that add-event/festival flows publish only to current target communities.
- Festival aggregate VK posts may mix programme-level content and event-level registration links.
- Broken or over-broad registration links can send users to a festival landing page instead of the concrete registration URL.

## Detection

- Detected manually by the operator on 2026-06-08 after checking postponed VK posts in both VK communities.
- The operator postponed both visible VK posts by one day to preserve evidence for investigation.

## Timeline

- 2026-06-08 UTC: operator reports incorrect postponed VK posts after adding an event through `/start` -> `Добавить событие`.
- 2026-06-08 UTC: incident record opened; containment chosen: add single-source `@kraftmarket39` Telegram Monitoring button and disable festival VK aggregate publishing by default.
- 2026-06-08 UTC: code/docs/tests updated in `hotfix/kraftmarket-single-source-20260608`.
- 2026-06-08 14:16 UTC: deployed `db47175cc3993abf754c5e2518cfe899c7122975` to Fly machine version `1239`.
- 2026-06-08 14:38 UTC: production UI E2E in `@events_love39_bot` confirmed `/tg` button `Только @kraftmarket39` and launched run `06addc951b7f4673a967855ccbb3bda7`; Kaggle produced output, but status polling failed on `GetKernelSessionStatus` HTTP 500.
- 2026-06-08 14:44 UTC: deployed `9e04bc70d7e9b1564704da11209eef1f1095b09a` to Fly machine version `1240`, adding transient handling for Kaggle status HTTP `429`/`5xx`.
- 2026-06-08 14:44-14:48 UTC: recovered/imported the `@kraftmarket39` output for run `06addc951b7f4673a967855ccbb3bda7`; `ops_run` recorded success with `messages_processed=2`, `messages_with_events=2`, `events_merged=1`, `errors_count=0`.
- 2026-06-08 15:03 UTC: initial acceptance failed: VK `wall-231920894_2432` was an old/problem evidence post and not a valid current-import result; VK had only one current imported postponed post, Telegram had only one visible `@kldevents` post, and the no-time event was blocked by `tg_ics_post: bad time`.
- 2026-06-08 UTC: root cause expanded: Telegram Monitoring preserved hidden/entity links in `messages[].links`, but broad LLM `ticket_link=https://kgd80.ru` was not refined to the concrete registration entity URL; no-time events incorrectly depended on `tg_ics_post`.

## Root Cause

1. Festival aggregate VK publishing (`sync_festival_vk_post`) could create/edit a whole-festival VK post independently of the normal Smart Update event fanout.
2. `/start` add-event and festival surfaces had drifted from the common Smart Update publication path, so link/media/source handling was not consistently shared.
3. Festival monitoring lacked a single routed technical-debt document and acceptance gate for full queue/E2E validation.

## Contributing Factors

- VK festival aggregates were enabled implicitly instead of guarded behind an explicit feature flag.
- Festival Queue/Universal Festival Parser had partial docs and tests, but no current production readiness gate for VK festival publishing.
- There was no fast operator button for a one-source `@kraftmarket39` Telegram Monitoring containment run through the normal Smart Update pipeline.

## Automation Contract

### Treat as regression guard when

- changing `/start` -> `Добавить событие`;
- changing Telegram Monitoring source scoping or `/tg` buttons;
- changing Festival Queue, Universal Festival Parser, `sync_festival_vk_post`, festival nav/index rebuilds;
- changing VK/TG event fanout for festival-tagged events.

### Affected surfaces

- `source_parsing/telegram/commands.py`
- `source_parsing/telegram/service.py`
- `scripts/run_tg_monitor.py`
- `main_part2.py::sync_festival_vk_post`
- festival docs/backlog/routes
- production Fly deploy path
- external systems: Telegram, Kaggle, VK

### Mandatory checks before closure or deploy

- Unit test proving Telegram Monitoring config can be scoped to `@kraftmarket39`.
- Unit test proving festival VK aggregate sync is disabled by default before reading VK settings.
- Live UI smoke: `/tg` shows `Только @kraftmarket39` button and can launch one-source monitoring.
- Release governance: deploy from a clean branch, push branch, record deployed SHA and prove fix is reachable from `origin/main`.
- Production verification: no new festival aggregate VK post is created while `ENABLE_FESTIVAL_VK_POSTS` is unset.

### Required evidence

- test command output;
- deployed SHA and branch;
- Fly deploy evidence;
- post-deploy `/healthz`;
- Telegram UI result for `@kraftmarket39` single-source run;
- VK postponed/created-post check after deploy.

## Immediate Mitigation

- Add `/tg` button `Только @kraftmarket39` that uses the existing Telegram Monitoring + Smart Update path with `source_usernames=["kraftmarket39"]`.
- Disable `sync_festival_vk_post` by default behind `ENABLE_FESTIVAL_VK_POSTS=1`.
- Do not count the failed `06addc951b7f4673a967855ccbb3bda7` import as successful E2E. Acceptance requires a fresh import after the fix and externally verified VK/TG posts.

## Corrective Actions

- Route festival-monitoring tech debt to `docs/backlog/features/festival-monitoring-debt/README.md`.
- Document that VK festival aggregate publishing is off by default until the debt is closed.
- Add tests for single-source Telegram Monitoring scope and disabled festival VK aggregate publishing.
- Refine Telegram hidden/entity registration links over broad landing-page `ticket_link` values.
- Do not enqueue `tg_ics_post` or depend on it for events without a concrete start time.

## Follow-up Actions

- [ ] Unify `/start` add-event publication with the same Smart Update components used by VK auto import and Telegram Monitoring.
- [ ] Add full Festival Queue E2E for Telegram, VK, and external URL sources.
- [ ] Add simplified Festival Queue E2E for `@kraftmarket39` and the `80 историй о главном` programme.
- [ ] Rebuild VK festival aggregate publishing as a separately reviewed feature with concrete registration URL guarantees and no obsolete-community target.

## Release And Closure Evidence

- deployed SHAs:
  - `db47175cc3993abf754c5e2518cfe899c7122975` (`fix(tg): add kraftmarket single-source monitor`)
  - `9e04bc70d7e9b1564704da11209eef1f1095b09a` (`fix(tg): tolerate kaggle status 5xx during monitor`)
- deploy path:
  - branch `hotfix/kraftmarket-single-source-20260608`
  - both commits pushed to the branch and fast-forwarded to `origin/main`
  - manual `flyctl deploy --remote-only --app events-bot-new-wngqia`
  - deployed image `registry.fly.io/events-bot-new-wngqia:deployment-01KTKV04P00A0SNQGM45D51Q1H`
  - Fly machine `48e42d5b714228`, version `1240`, region `iad`
- regression checks:
  - `.venv/bin/python -m pytest tests/test_telegram_monitor_service.py tests/test_bot.py::test_festival_vk_sync_disabled_by_default -q` -> `6 passed`
  - `.venv/bin/python -m compileall source_parsing/telegram/service.py tests/test_telegram_monitor_service.py`
  - production UI E2E: `@events_love39_bot` `/tg` showed `Только @kraftmarket39`; click launched run `06addc951b7f4673a967855ccbb3bda7`
  - production log evidence: `tg_monitor.config ... sources=1`, `tg_monitor.sources sample=['kraftmarket39']`
  - production recovery/import evidence: `ops_run` success for `run_id=06addc951b7f4673a967855ccbb3bda7`, `messages_processed=2`, `messages_with_events=2`, `events_merged=1`, `errors_count=0`; this is diagnostic evidence only, not acceptance, because external VK/TG verification failed.
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `job_outbox_worker=ok`, `issues=[]`
  - `ENABLE_FESTIVAL_VK_POSTS` unset in production, so whole-festival aggregate VK publishing remains disabled by default
  - `/data/kaggle_jobs.json` contains no active jobs after recovery
  - temporary E2E production `superadmin` grant for `user_id=8336351413` remains permitted for the active E2E debugging session and must not be revoked until external acceptance passes.

Closure remains blocked until a fresh `@kraftmarket39` production E2E import creates externally verified VK/TG posts through the normal process.

## Prevention

- Keep festival aggregate VK publishing explicit opt-in until there is passing E2E and review evidence.
- Treat this incident as a regression contract for every future festival/VK/add-event change.
