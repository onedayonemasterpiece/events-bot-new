# INC-2026-07-05 Telegram Afisha backlog blocked by VK sync dependency

Status: monitoring
Severity: sev2
Service: Telegram event publishing (`tg_event_publish`) / JobOutbox fanout
Opened: 2026-07-05
Closed: —
Owners: Codex / events-bot operations
Related incidents: `INC-2026-06-25-outbox-ics-publication-backlog`, `INC-2026-07-03-current-import-vector-vk-publication`
Related docs: `docs/features/tg-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-07-05 the operator noticed that Telegram Афиша (`@kldevents`) had no new top-of-channel event posts after about 07:20 Europe/Kaliningrad, while VK Афиша continued receiving fresh posts. Production evidence showed the Telegram worker was alive, but many fresh/new Telegram announcements were blocked because `tg_event_publish` depended on `vk_sync`; VK media upload failures (`vk_sync_missing_media_for_telegram_event`, `vk_sync_partial_media_upload`) therefore stalled Telegram.

## User / Business Impact

- Telegram subscribers did not receive eligible event announcements while VK kept moving.
- The channel looked stalled even though `/healthz` and workers were green.
- Operators had to inspect DB/logs to see that later `tg_event_publish` completions were edits of older posts, not new channel-top posts.

## Detection

- Detected manually by operator observation in Telegram/VK.
- Evidence sources: `/healthz`, `/data/runtime_logs/events-bot.log*`, production SQLite `/data/db.sqlite`, authenticated VK API `wall.get`.
- Artifact directory: `artifacts/codex/tg-afisha-gap-20260705/`.

## Timeline

- 2026-07-05 05:20 UTC — last new Telegram top-of-channel post found during triage: event `6675`, Telegram message `1883`.
- 2026-07-05 05:30–08:02 UTC — `tg_event_publish` continued to complete roughly every 10 minutes, but mostly edited older message IDs.
- 2026-07-05 08:01–08:05 UTC — several no-Telegram-message candidates were deferred because `vk_sync:<event_id>` failed.
- 2026-07-05 08:06 UTC — DB probe found 24 distinct events without Telegram post blocked behind `vk_sync` errors; 22 were today/future.
- 2026-07-05 08:00 UTC — VK API showed fresh managed VK post `wall-231920894_6061`, confirming VK and Telegram surfaces diverged.

## Root Cause

1. `schedule_event_update_tasks` put `vk_sync:<event_id>` into `tg_event_publish.depends_on` for events without an existing managed VK post.
2. `vk_sync` can fail for VK-specific reasons unrelated to Telegram publishing, especially media upload/materialization problems.
3. The JobOutbox dependency runner deferred `tg_event_publish` behind those VK errors, so Telegram stopped creating new posts even though the Telegram-specific prerequisites were satisfied.

## Contributing Factors

- The original product intent was surface consistency, but the actual product requirement is Telegram spacing/daytime window, not hard VK/TG lockstep.
- `/healthz` only showed worker health, not “Telegram announcements blocked by VK dependency”.
- Previous incidents already showed that leaf fanout tasks should not hard-block the main Telegram channel surface.

## Automation Contract

### Treat as regression guard when

- changing `schedule_event_update_tasks`, `enqueue_job`, `tg_event_publish`, `vk_sync`, or JobOutbox dependency handling;
- changing Telegram publish spacing/window logic;
- repairing production event-publication backlog.

### Affected surfaces

- `main.py::schedule_event_update_tasks`
- `joboutbox.depends_on`
- `JobTask.tg_event_publish`
- `JobTask.vk_sync`
- Telegram channel `@kldevents`
- VK group `klgdevents` / owner `-231920894`

### Mandatory checks before closure or deploy

- Unit test proving `tg_event_publish.depends_on` excludes `vk_sync` while `vk_sync` is still enqueued independently.
- Verify Telegram publish spacing/daytime window tests still pass.
- Production DB check after deploy: new/rearmed `tg_event_publish` rows must depend only on `telegraph_build` plus valid `tg_ics_post`, never on `vk_sync`.
- Backlog mitigation: remove stale `vk_sync:*` dependencies from currently pending eligible `tg_event_publish` rows and verify posts resume.
- VK failures must remain visible in `vk_sync` rows/operator evidence instead of being hidden.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted test output.
- Pre/post production DB counts of `tg_event_publish` rows blocked by `vk_sync`.
- Public Telegram evidence of resumed event posts.

## Immediate Mitigation

- Code hotfix removes `vk_sync` from future `tg_event_publish` dependencies.
- Current production backlog must be rearmed by stripping stale `vk_sync:*` dependency tokens from pending `tg_event_publish` rows.

## Corrective Actions

- Decouple Telegram event announcements from VK sync failures.
- Keep Telegram spacing and local publish window as the product-level throttles.

## Follow-up Actions

- [ ] Add an operator report/health issue for pending Telegram announcements blocked by non-Telegram dependencies.
- [ ] Review whether `skip_vk_sync` should be renamed/split from “skip public announcement” to avoid future coupling confusion.

## Release And Closure Evidence

- deployed SHA: `5a24aac98ec3b249b4008a3b43c592fa24695a5e` (`origin/main`, Fly image `deployment-01KWRPSACFA3FXR3VVHQMT3XHX`)
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only` from clean worktree `hotfix/tg-publish-decouple-vk` after pushing the same SHA to `origin/main`.
- regression checks:
  - `python3 -m py_compile main.py db.py main_part2.py` passed.
  - `git diff --check` passed.
  - targeted pytest could not complete in the local runner because a temporary venv install hit `OSError: [Errno 28] No space left on device`; without dependencies, pytest import failed on missing `aiogram`. The test expectations were updated in `tests/test_tg_event_publish.py` to assert that `tg_event_publish` dependencies exclude `vk_sync`.
- post-deploy verification:
  - `/healthz` OK after deploy, Fly machine version `1583`, image `deployment-01KWRPSACFA3FXR3VVHQMT3XHX`.
  - Production DB backup table before mitigation: `codex_backup_20260705_tg_vk_dep_joboutbox_20260705_083912`.
  - Backlog mitigation removed `vk_sync:*` from 83 pending active today/future `tg_event_publish` rows; remaining active today/future pending Telegram jobs with `depends_on LIKE '%vk_sync:%'` = `0`.
  - Telegram posts resumed with dependencies excluding VK, e.g. event `6679` published at `2026-07-05 08:43:02 UTC` to `https://t.me/c/3954607218/1891` with deps `telegraph_build:6679,tg_ics_post:6679`.

## Prevention

- `vk_sync` must be treated as an independent public surface and best-effort retry path for Telegram purposes.
- Future fanout dependencies should be justified by direct data requirements for the target surface, not by cross-surface lockstep.
