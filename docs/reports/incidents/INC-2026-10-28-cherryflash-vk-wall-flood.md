# INC-2026-10-28 CherryFlash VK wall publication stopped

Status: open — credential rotation deployed; awaiting fresh daily public wall acceptance
Severity: sev2
Service: CherryFlash publication to `vk.com/club231828790` on RunCoveer
Opened: 2026-10-28
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-09-24-vk-afisha-publication-recurrence`, `INC-2026-09-27-vk-auto-video-reader-publisher-token`, `INC-2026-10-14-scheduler-disk-exhaustion`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/runtime-logs.md`

## Summary

CherryFlash videos continued to upload to VK, but video announcements stopped appearing on the `Полюбить Калининград Анонсы` wall after 4 October. The Kaggle per-target report exposes VK `wall.post` code 9 (`Flood control`); the target is optional for overall Telegram delivery, so the enclosing report still says `ok=true`. Separately, RunCoveer fails to download recent Kaggle output after excessive output-list pagination reaches HTTP 429.

The transport fix and replacement credentials are deployed. Earlier credentials failed `video.save` with code 18 (`User was deleted or banned`); the owner subsequently replaced the shared user credentials. The new production upload identity now passes `video.save` from RunCoveer. A fresh daily public wall video and per-target report remain required before closure.

## User / Business Impact

- Subscribers have not received daily CherryFlash wall posts since 4 October, despite upload notifications.
- Recent sessions, including 1518 on 27 October, also ended locally with `kernel output download failed`; the Kaggle handoff `ops_run` success is not a publication receipt.
- Old announcements must not be replayed as current event recommendations.

## Detection

- Owner reported uploaded videos absent from the community wall.
- Authenticated VK wall search found the last `Видеоанонс` on 4 October; RunCoveer `/healthz` was ready and did not cover per-target delivery.

## Timeline

- 2026-10-04: last observed CherryFlash `Видеоанонс` wall post.
- 2026-10-27: Kaggle kernel version 522, session 1518, reported `wall.post` code 9 for `club231828790`; Telegram targets succeeded and overall report was `ok=true`.
- 2026-10-27 09:44:57 UTC: RunCoveer output download for session 1518 failed after `ListKernelSessionOutput` HTTP 429.
- 2026-10-28: confirmed `VK_ACCESS_TOKEN4` through `7` receive code 9 on `users.get`; `VK_TOKEN` resolves to `club231828790`, has wall permission and remains usable. `VK_ACCESS_TOKEN5` still accepts `video.save`; the group token cannot call `video.save` (code 5).
- 2026-10-29: compared the 4 October source and target configuration with current source; the upload/wall helper is unchanged since May and `VK_ACCESS_TOKEN5` is equal across the 3 October, 5 October and current production snapshots. The first request that caused VK's restriction remains unknown.
- 2026-10-29 08:28 UTC: deployed merged-main commit `b156fc067749e920383e7559e9e7efca804a1461` on RunCoveer. Session 1529 had already started with the old dataset/auth bundle.
- 2026-10-29 09:47:41 UTC: session 1529 ended `PUBLISHED_TEST`, without a wall post. This is not an end-to-end test of the new bundle.
- 2026-10-29: verified the running app generates `vk_wall_tokens.231828790` from its configured community token. CherryFlash loads the helper from each new dataset's `kaggle_common/story_publish.py`.
- 2026-10-29: actual `wall.post` with the generated community token and an existing video attachment succeeded as postponed diagnostic post 2255. `wall.delete` is unavailable for group auth (code 27); all available user credentials failed cleanup with code 5. The diagnostic post must be removed before its scheduled publication, approximately 31 October 11:05 UTC.
- 2026-10-29 11:08–11:10 UTC: `VK_USER_TOKEN8`, read from the shared credential source and tested from RunCoveer, fails `video.save` with code 18 and `wall.get` with code 5 (`user is blocked`). Existing production `VK_ACCESS_TOKEN5` now also fails `video.save` with code 18. This supersedes the earlier successful upload probe. Resolvable shared user tokens identify the same account; the saved owner browser session redirects to `vk.ru/login?act=blocked`.

## Root Cause

1. CherryFlash used one restricted user token (`VK_ACCESS_TOKEN5`) for both video upload and `wall.post`. The retained Kaggle report proves wall publication fails with code 9. By 29 October the upload identity also fails `video.save` with code 18. Newly supplied token 8 belongs to the same restricted account and has the same upload failure. The precise initial VK request that caused restriction is unknown; source/history does not show removal of the wall publication mechanism.
2. The RunCoveer Kaggle client used the SDK's default 20-item output listing page. The observed kernel had 1,532 files, requiring about 77 list calls and hitting HTTP 429. Kaggle accepts 500 items per page, reducing that listing to four calls.

## Contributing Factors

- VK wall is a best-effort fanout target; overall `ok=true` means required Telegram targets succeeded, not that VK received a post.
- RunCoveer health reports scheduler/process readiness, not successful VK wall delivery.

## Automation Contract

### Treat as regression guard when

- Changing CherryFlash VK token routing, wall/story fanout, Kaggle output listing or scheduled publication.

### Affected surfaces

- `video_announce/story_publish.py`, `video_announce/poller.py`, `video_announce/kaggle_client.py`, `kaggle/CrumpleVideo/story_publish.py`, embedded CrumpleVideo notebook helper, production `VK_ACCESS_TOKEN5` and `VK_TOKEN`.

### Mandatory checks before closure or deploy

- Verify the encrypted story bundle maps the `club231828790` wall call to its community token while `video.save` retains the user token.
- Verify the Kaggle client requests a supported 500-item output page, retains the file filter and downloads a real output.
- Check a new day's CherryFlash video appears on the public community wall, with one post and the expected video attachment; check session report, DB and RunCoveer logs.
- Prove the deployed commit belongs to `origin/main` and the current running release is its ancestor; check `/healthz` after RunCoveer deploy.

### Required evidence

- Kaggle version 522 `story_publish_report.json`, RunCoveer log at 2026-10-27 09:44:57 UTC, focused tests, public VK post URL, session/report result, deployed image and SHA.

## Immediate Mitigation

- Deployed group-token wall routing and larger output-list pages in PR #753. The live community-token `wall.post` boundary succeeds. The initial replacement token 8 was not installed because its exact upload method failed. After the owner replaced the shared credentials again, tested active identities were installed and the exact upload boundary succeeded.

## Corrective Actions

- Route only `club231828790` `wall.post` through its already configured community token; retain user-token video upload.
- Request 500 output entries per Kaggle list page, within the provider's accepted limit.

## Follow-up Actions

- [x] Configure tested replacement identities; verify `video.save` from the restarted production container.
- [x] Confirm diagnostic post `-231828790_2255` is absent: authenticated `wall.getById` returned no item and the postponed queue was empty after credential replacement.
- [ ] Verify a fresh daily run, its public wall post/video attachment and per-target report. A one-off repair or old in-flight run is not daily automation acceptance.
- [ ] Review whether optional VK fanout needs a delivery alert; do not change Telegram success semantics as part of this transport repair.

## Release And Closure Evidence

- deployed SHA: `b156fc067749e920383e7559e9e7efca804a1461`, merged in PR #753 and contained in `origin/main`; prior running release was its ancestor.
- deploy path: RunCoveer `deployment-events-1`, image `runcoveer/events:cherryflash-wall-b156fc067`, image ID `sha256:c32cb3516741094bcbe54d9fbfb8c30ea7d65f442a596ad3079a80726e4ad44b`.
- regression checks: all four PR CI jobs passed; focused checks covered 21 Kaggle client/notebook tests, 17 helper tests and one story bundle test. Full tracked-source verification covered 4,423 files.
- post-deploy verification: process healthy, `/healthz` ready and DB OK; generated group token map and actual postponed wall video post verified. Daily public video remains unverified; the replacement upload identity now passes the exact provider method.
- parallel work: production env was preserved during deploy; live `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL=gemini-3.1-flash-lite` and `EVENT_PARSE_ENABLE_4O_FALLBACK=0` remain effective.
- retained evidence: `/home/dev/artifacts/events-bot/20261028T063100Z-cherryflash-vk-20261028/`, including `release/source-receipt.json`, `wall-token-live-check-20261029.jsonl` and `user-token-capability-check-20261029.jsonl`.

## Prevention

- The group-token route and focused pagination/token-routing tests guard this failure mode.

## Replacement credential rollout — 2026-10-29

The owner replaced `/home/dev/.env:VK_USER_TOKEN` through `VK_USER_TOKEN8`
and explicitly requested propagation to every active consumer. Inventory covered
24 unique repositories, active local unit environment files and RunCoveer
containers. Active consumers were Events and VibePublish; the env-recovery utility
is not a publishing consumer and was not run to recover older secrets.

Both hosts rejected new tokens 2, 6 and 7 with the provider's other-IP binding
error. Token 4 had no photo/video/wall/group permissions. Production uses tested
identities through its existing aliases:

| Events destination | Shared credential source |
| --- | --- |
| `VK_USER_TOKEN` | `VK_USER_TOKEN` |
| `VK_ACCESS_TOKEN4`, `VK_ACCESS_TOKEN6` | `VK_USER_TOKEN3` |
| `VK_ACCESS_TOKEN5` | `VK_USER_TOKEN5` |
| `VK_ACCESS_TOKEN7` | `VK_USER_TOKEN8` |
| Private MCP messenger and dialog reader | `VK_USER_TOKEN1` |
| Private MCP notification, analytics, community, story reader/editor | `VK_USER_TOKEN3` |
| Private MCP media editor | `VK_USER_TOKEN8` |

All 13 consumed assignments were updated in the canonical RunCoveer env file;
community/service credentials and unrelated configuration were preserved. The
Events container was recreated at 12:56:46 UTC with the same image ID listed
above. It is healthy, ready and DB OK. In-container checks passed wall read,
photo upload-server discovery, `video.save`, message conversations, notifications,
statistics, stories and video-story upload-server discovery. No video bytes were
uploaded and no public diagnostic post was created during this rotation.
Future CherryFlash bundles use the refreshed upload token and existing community
wall token. The parallel Gemini Flash Lite/4o fallback settings remain unchanged.

The restart interrupted scheduled source-parser run 11225 while processing
Qtickets; six source summaries were already committed. The retained per-run log
identifies Qtickets as the unfinished source (Philharmonia had failed earlier,
before the restart). The existing durable recovery helper queued Qtickets as
pending at 13:09 UTC with reason `credential_reload_interrupted_ops_run_11225`.
No full catalog replay or CherryFlash rerender was started.

VibePublish's owner explicitly selected `VK_USER_TOKEN3` instead of IP-bound
token 2. Only its existing worker credential-key argument changed; the server and
worker source release `c1053811c2c46c2e56633b8b4fec040f03cfdb78` and other provider
identities were preserved. Worker PID 64689 started at 12:50:05 UTC; actual bound
Afisha feed read completed as verified operation
`op_2c17be263e0d45b5a510c0f8233789c4`.

Retained redacted rollout evidence:
`/home/dev/artifacts/shared-services/20261029T123537Z-vk-token-rollout-20261029/`.
The incident remains open until a fresh scheduled daily public wall video and its
per-target report are verified.
