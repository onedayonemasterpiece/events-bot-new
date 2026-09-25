# INC-2026-09-24 VK Afisha publication recurrence

Status: mitigated
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
- 2026-09-24 13:10–13:11 UTC: token-routing hotfix deployed from merged `main` SHA `6df3aa0484bf306bd16dd9f5059792677bb5887d`; Fly image `deployment-01M39RD6WGEJJWXSYSAC3V4Q4K`. `VK_USER_TOKEN` rotated from local `VK_USER_TOKEN4`; remote/local SHA-256 prefixes matched (`8fcbb5e487d8`), health remained ready.
- 2026-09-24 13:21 UTC: a guarded manual run of eligible control `vk_sync` job `87256` (event `9278`, 26 September, exact matching image) passed `photos.getWallUploadServer` but the upload server returned HTTP 200 with an empty `photo`. The code retried `photos.saveWallPhoto` five times with an empty `photo`, received code 100, and failed closed before `wall.post`.
- 2026-09-24 13:10–15:29 UTC: the token-routing, photo-encoding, source-date, media-identity, rejected-attachment and exact-object reimport hotfixes were merged through CI and deployed from exact `origin/main`. New photo-bearing posts appeared on the public wall; a targeted replay of event 9229's `vk_sync` finished `done` without restoring its rejected photo.
- Local, non-wall VK upload probes with that same new token: the control WebP (802×930, 47,808 bytes) converted by the existing JPEG default produced 75,899 bytes and empty `photo`; quality 95 with default subsampling produced 171,346 bytes and empty `photo`; quality 75 with subsampling 0 produced 86,403 bytes and empty `photo`; quality 95 with subsampling 0 produced 184,591 bytes and a nonempty `photo`. A different known-published JPEG also uploaded successfully. This establishes an image-encoding-specific rejection, not another token code 9. The exact provider acceptance rule is unknown.

## Root Cause

1. **Confirmed design defect:** `social_metrics_kaggle.py::_secret_payload()` prefers `VK_USER_TOKEN` for read-only metrics even though a service-read token exists. Production enables this collector every 30 minutes. Its Kaggle calls do not share Fly's `_vk_throttle` or actor circuit. The reader continues across publisher groups after a VK request fails, records only `RuntimeError` as the observation error code, and marks the enclosing run `done` after import.
2. **Confirmed retry amplification:** `social_metrics_batch.py::_load_terminal_buckets()` excludes errors, so failed targets return in the next 30-minute run. The prior token rotation left this reader routing unchanged. Once the publishing actor is restricted, the remote reader keeps exercising it while Fly's `vk_sync` also retries after each one-hour circuit. Thus replacing the token alone restores the same failure-producing workload and cannot be a durable fix.
3. **Confirmed publication blocker:** VK rejects the same user actor on `photos.getWallUploadServer` with code 9. `vk_sync` fails closed rather than publishing an image-bearing event as text.
4. **Text-only symptom audit:** the inspected multi-event source has images, but both explicitly advertise 26 September. Its later October children correctly have no matching media. Other text-only posts need per-event review before being classified as a loss.
5. **Separate confirmed media transport defect after token recovery:** the URL-based WebP→JPEG path used Pillow defaults that VK silently rejected for an otherwise valid, date-matching 26 September image (`photo=""` despite HTTP 200). The uploader did not validate this response and made five doomed `photos.saveWallPhoto` calls. It then correctly prevented a text-only `wall.post`. A higher-quality 4:4:4 JPEG of the exact same source was accepted in a controlled upload probe.
6. **Catch-up date-span defect:** event 9231's source explicitly says 21 September–13 December 2026. Its stored start date was 7 September and was corrected to the source-backed range. The VK header formatter then dropped the end of every explicit range; two incorrect postponed posts (11204, 11219) were removed before public release. The formatter fix deployed, and replacement postponed post 11258 has the full range and one photo.
7. **Unnecessary publisher-token calls during catch-up:** event 9208's `tel:` ticket link triggered five provider-rejected `utils.getShortLink` calls (code 100) because the short-link helper accepted every nonempty scheme. The helper now rejects non-web URLs before calling VK. This is a load reduction, not the proven initial cause of code 9.
8. **Separate systemic media-identity failure:** linked child event 9229 is on 3 October, but its first poster was auto-approved as `first_event_media_seed` before vision role classification. The later VLM response explicitly described a different show and 26 September, yet marked `event_identity_grounded=true`; the gallery kept the image and managed VK post 11193 displayed it. A second 26 September image was correctly rejected. The new guard holds approved media for linked children until the LLM decision and rejects images whose visibly advertised event dates all conflict with that child date. This is distinct from the VK code-9 cause.
9. **Rejected-media edit defect:** after rejecting event 9229's wrong poster and clearing its canonical gallery, a manual `wall.edit(attachments="")` removed the image from post 11193. The next `vk_sync` passed `attachments=None`, preserving/restoring the old VK photo even though the ledger had only rejected candidates. A further hotfix makes this explicit-rejection case pass an empty attachment list and disables stale Telegraph image fallback. Transient empty media with approved/pending candidates still preserves existing attachments.
10. **Rejected-object reimport defect:** a later source reconciliation created poster rows 28000 and 28001 for the exact same immutable hosted objects as rejected rows 27864 and 27865, but with new candidate hashes. One was auto-approved as a first seed and remained unclassified because the daily model budget was exhausted. Its public gallery was held empty, but the VK edit guard treated that transient approval as safe to preserve old attachments. The new guard carries a semantic rejection across exact object identity, excludes reincarnated rows from the public gallery, and clears old VK attachments for linked children with no eligible classified image.

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

- A freshly issued replacement publishing token passed non-mutating `account.getAppPermissions`, `photos.getWallUploadServer` and postponed-wall checks for the managed group, and is installed in Fly. Only its fingerprint was recorded.
- The deployed hotfix routes scheduled metrics and poll popularity reads through `VK_SERVICE_TOKEN`, stops a Kaggle VK batch at the first code 9, and persists a one-hour cooldown across scheduler slots.

## Corrective Actions

- Deployed: separation of social-metrics and poll-popularity read traffic from the publishing actor, typed VK provider errors, stop-on-code-9, and cross-run cooldown.
- Publishing actor recovered: merged hotfix deployed; rotated token fingerprint and photo-upload-server access verified. Event 9278 reached the public wall as post 11200 with its exact date-matching photo; the DB URL was reconciled after VK changed the postponed id.
- Second release deployed: VK-specific JPEG quality/subsampling profile and empty-upload-response guard.
- Corrected event 9231's source-backed date span, removed two malformed postponed posts, and verified replacement postponed post 11258 with the full date range and a matching photo. Its Telegram and Telegraph material was corrected.
- Deployed linked-child media classification and exact-object rejection guards; rejected event 9229's wrong 26 September images, cleared its public VK post 11193, removed duplicate 11187, and replayed `vk_sync` on the final image without reattachment.
- Recent public wall readback at 15:35 UTC showed eight consecutive photo-bearing event posts through 15:14 UTC and a later text-only ballet post whose event rows have no canonical photo. Whether that source has usable, date-matching media remains unverified; the wider text-only audit remains open.
- Pending delivery-health alert and post-rotation fresh-publication gate.

## Follow-up Actions

### Postponed-to-live URL continuity, 2026-09-25

- A scheduled image-bearing announcement for event 8944 became public at
  20:54 UTC as `wall-231920894_11409`, with one photo. VK replaced its
  postponed ID 11401; the completed `vk_sync` job left the stored event URL
  pointing at 11401. Previously, URL recovery ran only if the event later
  triggered another `vk_sync` job.
- The outbox worker now scans the newest 100 public Afisha posts every five
  minutes using `VK_SERVICE_TOKEN`. It selects only a unique exact title/date
  match for an active future event with a managed URL, then invokes the
  existing fail-closed live-ID recovery. Routine scanning does not use the
  publishing credential. If the old post still exists, the match is
  ambiguous, or the provider is unavailable, the URL is left untouched.
- Regression check: a public post with a changed ID and unique title/date
  recovers the event and `EventSource` URL automatically; duplicate headers
  cause no rewrite; one service-token scan does not turn into a per-event
  publisher-token poll. Verify this against the 8944 production transition
  after release, along with `/healthz` and absence of renewed VK code 9.

### Stability review, 2026-09-25

- Production remained ready on the 24 September release image. The retained
  runtime log window since 24 September 15:26 UTC had zero VK code-9 errors,
  zero actor flood blocks and zero `vk_flood_wait` entries. Recent half-hourly
  social-metrics imports completed with zero VK error snapshots, and an
  authenticated wall readback found new photo-bearing event posts through
  25 September 18:44 UTC. This supports recovery of the publishing actor;
  it does not prove future provider availability.
- A separate media-review scheduling defect threatened six future linked
  events (8794, 8915, 8930, 8932, 8934 and 9293). Each had an approved legacy
  or pending image, no eligible classified gallery image, and a repeating
  `vk_sync_missing_materialized_media` error. The fail-closed guard prevented
  an inaccurate text-only or wrong-image announcement. Their next media
  review was queued for 26 September 00:05 UTC.
- The semantic-role allowance was exhausted at 150/150 on both 24 and 25
  September. Of 130 role classifications completed on 25 September, 120 were
  for already past events, five for events within seven days and five for later
  events. The outbox sorted media reviews by job id ahead of VK publication,
  regardless of event date or blocked delivery. A second condition selected
  legacy prompt-version rows before their scheduled daily-budget retry.
- Corrective code reserves first place for due media reviews with a matching
  blocked VK job on an active future event, then processes due VK jobs, then
  ordinary media reviews ordered by future event date before past events.
  It also respects the pending role retry time across prompt versions. The
  existing 150-call daily allowance and the source-bound vision identity gate
  remain in force. A manual dynamic-cover metadata read now selects the
  service credential; the user credential remains for the required cover
  upload and publication methods.
- PR #664 merged with all applicable CI checks green; 121 focused local tests
  passed. The fix was deployed from clean exact `origin/main` SHA
  `dee74ccf6320e9273c8844afb4e524a20b25bf98` via
  `scripts/deploy_fly_main.sh --remote-only` as image
  `deployment-01M3CZDCS721V3N74MGAYXAWBG`. Production SSH readback matched
  the embedded SHA, `/healthz` remained ready with no issues, and the retained
  runtime log scan still had zero VK code-9 entries after the prior recovery.
  All six media reviews remained due for 26 September 00:05 UTC with matching
  blocked VK jobs. Release evidence is retained at
  `/home/dev/artifacts/events-bot-new/20260925T184705Z-vk-stability-followup-20260925/evidence.md`.
- Follow-up gate: after the 26 September allowance resets, verify that the
  six blocked events enter review before historical rows, their eligible
  images pass the vision date/identity guard, and resulting VK jobs either
  publish exact media or retain a source-specific blocked reason. Alerting on
  prolonged absence of fresh managed VK posts remains open.

### Fresh-delivery correction, 2026-09-25

- A product-impact audit separated the six future legacy-media jobs from
  fresh publication delivery. Of 46 active future events added after
  24 September 15:26 UTC, 43 had managed Afisha VK URLs. The three exceptions
  were one future event still waiting for v2 image classification (9293) and
  **two fresh events with source-matched canonical photos whose VK jobs had
  expired**: 9302, «Конкур» on 3 October, and 9305, «Пивной вечер с Понарт»
  on 30 September. Both source posters were visually inspected against the
  exact event/date. This is an actual missed-announcement defect, unlike the
  five distinct future announcements represented by the six legacy-media jobs.
- At 04:46–04:47 UTC, both fresh jobs reached VK
  `photos.getWallUploadServer`, but the HTTP-200 upload-server response had
  `photo=""` on all three retries of the identical JPEG. The fail-closed guard
  prevented text-only posts. The worker then let the **error** rows expire
  after the one-hour job TTL; its active-event catch-up covered pending rows
  only. The jobs were stranded despite future event dates and later canonical
  photos. Thus the product failure is a combination of image-specific upload
  rejection and retryable-error expiration, not another VK code-9 restriction.
- Controlled upload-server probes (no `photos.saveWallPhoto` or `wall.post`)
  showed a 1600-pixel 95-quality 4:4:4 JPEG accepted for event 9302 and an
  800-pixel variant accepted for event 9305. Smaller 85-quality variants were
  still rejected. Re-encoding the already converted JPEG changed the payload
  and failed for 9302, so fallback variants must be resized directly from the
  original source bytes. The provider's exact acceptance rule remains unknown.
- Corrective code retains due `vk_sync` errors for active future events and
  tries at most three smaller JPEG variants from the original source bytes on
  an empty upload response. It changes bytes only after provider rejection;
  a failed final attempt still
  blocks text-only publication. Product acceptance requires replay of both
  exact events and authenticated VK readback with matching photos, followed
  by a fresh-event delivery audit.
- Post-release replay created postponed VK posts 11395 (9302) and 11396
  (9305); authenticated `wall.get(filter=postponed)` showed one photo on each.
  A separate near-term replay, event 8922, succeeded only on its 800-pixel
  fallback and created post 11397. Event 8943 still failed with empty `photo`
  at original, 800-pixel and 600-pixel sizes. A bounded upload-server-only
  probe accepted its 400-pixel JPEG while PNG remained rejected. This release
  adds the 400-pixel candidate and holds 8943's retry until it is deployed.
- The 400-pixel release recovered event 8943 as postponed post 11399 with one
  photo. Five further legacy expired events with visually checked relevant
  media (8938, 8944, 8936, 8937, 8935) were requeued and gained photo-bearing
  postponed posts 11400–11405 (excluding 11402, the live id of event 9302).
  Event 8923 was held: its only approved image is a news card about an
  unrelated skating tournament. The stored structured vision result itself
  says both `primary_event_promotion=false` and
  `event_identity_grounded=false`, yet a legacy seed left it approved as
  `event_photo`. The gallery now excludes this contradictory classification
  and future role review rejects it explicitly.
- VK publication previously read `Event.photo_urls` directly. Because that
  field is a cache, a source reimport could briefly restore a rejected URL
  before the media-review projection refreshed it. The publication job now
  filters URLs with explicit semantic rejection evidence before hashing or
  uploading. A read-only production audit found 41 linked future events with
  cached images awaiting current semantic classification, so the preflight
  deliberately retains those pending images and legacy pre-ledger images.
  This closes the observed path for the rejected news card to enter a VK post
  without causing a broad removal of existing photos.

- [x] Route the half-hourly Kaggle social-metrics VK reads to the service token; add a typed provider code-9 result and stop the VK lane after the first flood response. Deployed and credential separation verified.
- [ ] Audit the remaining lower-volume publisher-token readers (`poll_to_forward_popularity`, promo and dynamic-cover paths) and enforce one per-credential budget across Fly and remote consumers.
- [x] Finish the second photo-encoding hotfix and verify public photo-bearing readback for event 9278.
- [x] Reconcile the incident's eligible backlog and corrected event 9231 span header. All eight initially blocked new events 9266–9273 have managed VK URLs. No current `vk_flood_wait` rows remain. The 92 `expired` errors were last updated no later than 12 September, before this incident, and are excluded from this catch-up.
- [ ] Audit other recent text-only posts and repair only those with exact, date-consistent source media.
- [ ] Alert on prolonged absence of fresh managed VK posts plus increasing `vk_flood_wait`, even when `/healthz` is ready.

## Release And Closure Evidence

- Deployed first hotfix SHA: `6df3aa0484bf306bd16dd9f5059792677bb5887d`, merged via PR #655; Fly machine `48e419df93e078`, version `2078` after secret rotation, image `deployment-01M39RD6WGEJJWXSYSAC3V4Q4K`.
- Regression checks: read-only production log, SQLite and authenticated VK API inspection completed. Local focused release suite: 104 passed (`test_social_metrics_kaggle`, `test_vk_actor`, `test_job_captcha_pause`, `test_social_metrics_batch`, `test_poll_to_forward`, `test_poll_to_forward_popularity`). This also fixes a pre-existing poll test that fetched a cursor after its database context closed. Initial PR CI passed the VK-related Python job and static browser gate; its unrelated Smart Update gate found two date-sensitive fixture failures (20 September was past on 24 September). Both failures reproduced locally; the fixture dates were advanced to a fixed future year and all five selected cases passed locally before rerun.
- Redacted evidence: `/home/dev/artifacts/events-bot-new/20260924T072844Z-vk-publishing-incident-20260924/evidence.md` (retained while the incident is open).
- Post-first-deploy verification: `/healthz` HTTP 200/ready; remote image SHA exact, service token present, new publishing token fingerprint matched. Control job exposed the separate media encoding defect; no new wall post yet.
- Second hotfix SHA `0fed2c96e675c5600e08653bd7818f6644fe6381`, image `deployment-01M39SZR5061C9J7RNQ0Y1C194`; 91 focused tests passed. Exact problematic image's 95-quality, 4:4:4 diagnostic upload produced a nonempty VK `photo` field. Public post `https://vk.com/wall-231920894_11200` had one matching photo; `JobOutbox` 87256 is done and the DB URL was reconciled.
- At 13:41 UTC, `vk_sync` done increased from 4512 to 4531 while `vk_flood_wait` decreased from 91 to 76. Continue monitoring as the staggered queue drains.
- Third hotfix SHA `ad8fa55cf2d1a4b2660fb02214ae9fced3a74ed4`, image `deployment-01M39VNPS8FZ6TFZVM8XWB1TVD`, fixed explicit date-range headers and invalid `tel:` short-link calls. Postponed post `11258` was read back with one photo and `📅 21 сентября — 13 декабря`, scheduled for 2026-09-24 21:44 UTC; the two malformed postponed posts were deleted.
- Media classification hotfix SHA `5ed3a02704eed922d3f803c43e922bcfe08c9211`, image `deployment-01M39Y3SCH0FY73MTFQ1ZGQP1T`, held linked-child media until vision classification and rejected images whose visible dates conflict with that child. Event 9229's wrong poster was removed from its canonical gallery, VK, Telegram and Telegraph.
- Rejected-attachment hotfix SHA `14e530f942c616c537a93a612ff812076a63aa2c`, image `deployment-01M39Z4BB5ANCPVBBZ7PZMDTY3`, makes an empty rejected gallery explicitly clear existing VK attachments. Later production reconciliation recreated the same hosted images under new source hashes; one was auto-approved before classification. This additional reimport path was captured and fixed next.
- Exact-object reimport hotfix SHA `63901215976d7727d68a7a21e0a180f1062ceefd`, image `deployment-01M3A07R6RZJ4CGEMW1PY8KCRD`, deployed with all PR checks green and 124 focused local tests passed. At deployment, event 9229 still had two new approved/pending rows for the rejected exact images, proving the production guard was exercised. Targeted `vk_sync` job 85604 completed `done`; authenticated `wall.getById` showed post 11193 still had zero photos. The two reincarnated rows were then marked rejected with `media_role_visible_date_conflict`; canonical `photo_urls=[]`.
- At 15:30 UTC, `vk_sync` had `done=4606`, `error=93`, `paused=199`; current `vk_flood_wait=0` versus 92 at incident start. The 93 errors grouped as 92 `expired` and one `stale`. `/healthz` was HTTP 200 and `ready=true`, and the running image's embedded SHA matched exact merged `origin/main`. Public `wall.get(filter=owner)` showed multiple fresh photo-bearing event announcements, including posts 11271, 11270, 11267, 11262, 11261, 11260, 11259 and 11256.
- Production readback confirmed all eight new event rows 9266–9273, originally deferred by code 9, now have managed VK URLs. `expired` outbox rows were last updated between September 2025 and 12 September 2026, predating the observed incident onset; they are historical issues and were not blindly replayed.

## Prevention

The production publication blocker is mitigated and the system causes identified in this record have deployed regression guards. Keep the incident open for the remaining text-only source audit, lower-volume publisher-token consumers, and a delivery-health alert that detects a public-wall gap even when `/healthz` is ready. Historical `expired`/`stale` rows belong to separate pre-existing backlog work; do not replay them without event date, source-media and duplicate checks.
