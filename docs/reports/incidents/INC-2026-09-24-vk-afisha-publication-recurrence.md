# INC-2026-09-24 VK Afisha publication recurrence

Status: open
Severity: sev1
Service: managed event publication to `vk.com/klgdevents`
Opened: 2026-09-24
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-09-13-guide-vk-monitoring-user-token-flood`, `INC-2026-06-02-vk-captcha-text-only-posts`, `INC-2026-09-01-yandex-storage-cdn-media-outage`
Related docs: `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Managed event announcements in VK Afisha stopped after 2026-09-23 04:00 UTC. The blocking API response is code 9, but the actionable system cause is shared use of the publishing user token by an autonomous, every-30-minute Kaggle social-metrics reader that has no shared per-token budget or code-9 circuit with Fly. Reader failures are recorded as generic `RuntimeError`; the run still finishes `done`, and the same targets are retried in the next slot. In parallel, Fly `vk_sync` probes the same restricted user actor hourly after its local circuit expires. Some earlier text-only posts are separate: the inspected source images describe 26 September events and cannot safely illustrate 3/17 October children.

## User / Business Impact

- The latest ordinary managed event announcement visible through authenticated `wall.get(filter=owner)` was `wall-231920894_11194`, dated 2026-09-23 04:00 UTC; no 24 September event announcement was visible at investigation time.
- New events continue to enter production: 13 event rows were added on 24 September by 07:30 UTC, but none had a managed VK URL. The eight events created at 03:02–03:15 UTC had one canonical photo each and their `vk_sync` jobs were deferred by flood control.
- 92 `vk_sync` jobs had `vk_flood_wait` as their current error by 07:30 UTC. This is an observed current backlog, not an estimate of all missed eligible publications.
- Several 21–23 September managed wall posts had no photo, including `11194`, `11193`, `11189`, `11187`, and `11186`.

## Detection

- Operator reported the sequence: some text-only VK posts, followed by no fresh posts for days.
- `/healthz` at 2026-09-24 07:30 UTC reported `ready=true`, scheduler jobs `ok`, and `/data` free space 573 MiB. This liveness check did not detect channel delivery failure.
- Production file logs and the outbox expose VK code 9 and delayed jobs. Authenticated VK readback exposes the public gap.

## Timeline

- 2026-07-17: social-metrics Kaggle reader was introduced with `VK_USER_TOKEN` as its first credential choice for read-only VK methods (`faaaa65996`). That routing remained in place when the September Guide reader was moved to `VK_SERVICE_TOKEN`.
- 2026-09-19: prior incident rotated `VK_USER_TOKEN` and contained 155 old flood rows. Its required first fresh managed VK publication check remained pending in the prior record.
- 2026-09-19 06:38 through 2026-09-21 14:29 UTC: 107 successful half-hourly social-metrics runs planned 2,274 target observations with zero imported errors. The remote reader selected `VK_USER_TOKEN` over the available service token for VK reads.
- 2026-09-21 14:29 UTC: first subsequent social-metrics run with imported errors (3). Error counts then grew and recurred in every half-hourly slot; the collector retained only the exception class `RuntimeError`, so this does not identify the first provider error code.
- 2026-09-21 17:57 UTC: child event `9229` from `wall-48845044_25348` retained one photo. A later child from the same source, event `9234`, had no `photo_urls` or `EventPoster`. Inspection of both source images showed dates of **26 September**, while `9234` is dated **17 October**; attaching either image would publish misleading event details.
- 2026-09-23 04:00 UTC: latest observed ordinary managed event post, `11194`, was text-only.
- 2026-09-23 11:14 UTC: earliest code-9 `photos.getWallUploadServer` entry in the retained 48-hour runtime mirror. The same provider error recurred roughly hourly through 2026-09-24 07:19 UTC, each time opening a 3600-second user-actor circuit.
- 2026-09-24 06:27–07:30 UTC: 92 `vk_sync` jobs ended with current `vk_flood_wait`; many old jobs had dozens of attempts, and new event jobs were also affected.
- 2026-09-21 14:29 through 2026-09-24 08:30 UTC: 126 social-metrics runs planned 8,852 target observations and recorded 6,503 imported errors. By 24 September the VK subset repeatedly produced 75–79 errors per run while the ledger still recorded each run as `done`.

## Root Cause

1. **Confirmed design defect:** `social_metrics_kaggle.py::_secret_payload()` prefers `VK_USER_TOKEN` for read-only metrics even though a service-read token exists. Production enables this collector every 30 minutes. Its Kaggle calls do not share Fly's `_vk_throttle` or actor circuit. The reader continues across publisher groups after a VK request fails, records only `RuntimeError` as the observation error code, and marks the enclosing run `done` after import.
2. **Confirmed retry amplification:** `social_metrics_batch.py::_load_terminal_buckets()` excludes errors, so failed targets return in the next 30-minute run. The prior token rotation left this reader routing unchanged. Once the publishing actor is restricted, the remote reader keeps exercising it while Fly's `vk_sync` also retries after each one-hour circuit. Thus replacing the token alone restores the same failure-producing workload and cannot be a durable fix.
3. **Confirmed publication blocker:** VK rejects the same user actor on `photos.getWallUploadServer` with code 9. `vk_sync` fails closed rather than publishing an image-bearing event as text.
4. **Text-only symptom audit:** the inspected multi-event source has images, but both explicitly advertise 26 September. Its later October children correctly have no matching media. Other text-only posts need per-event review before being classified as a loss.

The **initial provider decision** that restricted the rotated token cannot be attributed to one exact request: the remote collector discarded the VK provider code and the production runtime mirror covers only the later window. The shared-credential and retry design is directly verified; its contribution to the onset is strongly supported by the timeline and workload, but the exact VK threshold and first rejected method remain unknown. The observed code 9 is not evidence of captcha or bucket exhaustion.

## Contributing Factors

- Error jobs retry indefinitely and consume queue cycles; observed attempts reach 61 for `vk_sync` and over 1000 for some `event_media_review` rows. Whether this additional load triggered the initial VK restriction is unproven.
- The remote collector's `done` means result import completed, even when most VK observations failed. No VK-lane failure gate stops subsequent slots or protects the publisher credential.
- `ready=true` means the process and scheduler are running, not that a fresh VK event reached the wall.
- The prior incident's fresh managed post readback remained a follow-up, allowing this regression to pass without closure evidence.

## Automation Contract

### Treat as regression guard when

- changing VK user-actor selection, social-metrics token routing and retry, photo upload, flood control, outbox retry, managed post scheduling, or production VK credential configuration.

### Affected surfaces

- `social_metrics_kaggle.py::_secret_payload`, `kaggle/SocialMetricsCollector/social_metrics_collector.py::_vk_request/_collect_vk/_resolve_vk`, `social_metrics_batch.py::_load_terminal_buckets`; `main.py::upload_vk_photo`, `_vk_api`, `_vk_mark_actor_flood`, job worker; `main_part2.py::sync_vk_source_post`; `JobOutbox.vk_sync`; `VK_USER_TOKEN`; managed group `231920894`.

### Mandatory checks before closure or deploy

- Prove the chosen publishing actor can complete `photos.getWallUploadServer` after cooldown without code 9, without repeatedly probing a restricted actor.
- Prove read-only social metrics use the service credential, preserve bounded VK provider error codes, stop retrying a restricted actor across groups and runs, and surface a failed VK lane even if Telegram metrics import succeeds.
- Regression test actor-level flood propagation, durable staggered retry, and no text-only fallback when canonical media exists.
- For any proposed text-only repair, verify the source image's title/date against that exact child; do not attach the 26 September Kaup posters to October events.
- Verify one genuinely new managed event post in VK has the expected photo attachment and its DB/outbox state matches the provider readback. Reconcile missed eligible events without duplicate or past-event posts.
- Check fresh logs for renewed code 9, `vk_sync` backlog direction, `/healthz`, and exact deployed SHA reachable from `origin/main` if code is released.

### Required evidence

- Redacted provider errors, outbox summary, authenticated wall readback, selected event/media rows, focused tests, recovery and catch-up receipt, and deployment SHA/image if applicable.

## Immediate Mitigation

- A freshly issued replacement publishing token passed non-mutating `account.getAppPermissions`, `photos.getWallUploadServer` and postponed-wall checks for the managed group. It has not yet been installed in Fly.
- The prepared hotfix routes scheduled metrics and poll popularity reads through `VK_SERVICE_TOKEN`, stops a Kaggle VK batch at the first code 9, and persists a one-hour cooldown across scheduler slots. Production deployment and credential rotation remain pending.

## Corrective Actions

- Implemented locally: separation of social-metrics and poll-popularity read traffic from the publishing actor, typed VK provider errors, stop-on-code-9, and cross-run cooldown.
- Pending safe publishing-actor recovery after removal of the repeated read load.
- Pending audit of other recent text-only posts for genuinely missing, event-matching media.
- Pending delivery-health alert and post-rotation fresh-publication gate.

## Follow-up Actions

- [x] Route the half-hourly Kaggle social-metrics VK reads to the service token; add a typed provider code-9 result and stop the VK lane after the first flood response. Pending production verification.
- [ ] Audit the remaining lower-volume publisher-token readers (`poll_to_forward_popularity`, promo and dynamic-cover paths) and enforce one per-credential budget across Fly and remote consumers.
- [ ] Restore actor capability safely, then reconcile eligible missed `vk_sync` rows and verify public photo-bearing readback.
- [ ] Audit other recent text-only posts and repair only those with exact, date-consistent source media.
- [ ] Alert on prolonged absence of fresh managed VK posts plus increasing `vk_flood_wait`, even when `/healthz` is ready.

## Release And Closure Evidence

- deployed SHA at investigation time: not established by image metadata; local checkout `887d755468699230c536a62a49da432125f80f63` is not claimed as deployed.
- Fly machine: `48e419df93e078`, version `2076`, image `deployment-01M2WE2R4474VRMEQJ0X7TJPJ6`.
- Regression checks: read-only production log, SQLite and authenticated VK API inspection completed. Local focused release suite: 104 passed (`test_social_metrics_kaggle`, `test_vk_actor`, `test_job_captcha_pause`, `test_social_metrics_batch`, `test_poll_to_forward`, `test_poll_to_forward_popularity`). This also fixes a pre-existing poll test that fetched a cursor after its database context closed. Initial PR CI passed the VK-related Python job and static browser gate; its unrelated Smart Update gate found two date-sensitive fixture failures (20 September was past on 24 September). Both failures reproduced locally; the fixture dates were advanced to a fixed future year and all five selected cases passed locally before rerun.
- Redacted evidence: `/home/dev/artifacts/events-bot-new/20260924T072844Z-vk-publishing-incident-20260924/evidence.md` (retained while the incident is open).
- Post-deploy verification: pending.

## Prevention

Open until a photo-bearing new managed event is visible after recovery, missed eligible events are reconciled, and any genuine text-only media loss is separated from correctly unillustrated children.
