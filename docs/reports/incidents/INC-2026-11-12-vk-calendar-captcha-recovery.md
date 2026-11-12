# Calendar comment CAPTCHA did not enter normal outbox recovery

Status: open. Registry: `inc_3d23e0008f860b634306a59b`, observation112.
Surface: managed Afisha calendar comments. Mechanical transport integration, not event semantics.

On November12 the owner requested a two-press instruction and readable calendar filenames. PR897 delivered exact main `abbbc6f4dbb8e34155cb6cb551dcd6aaf84390c8`. In-place edits of nine current comments were independently verified; the next `wall.editComment` returned VK code14 and the manual batch stopped. No challenge was bypassed and no post/comment was recreated. Ordinary post-release reconciliation also delivered comments for newly imported10848 and10849 with readable downloadable calendars and unchanged canonical schedule/UID.

`ensure_calendar_comment` called `_vk_api` without the current DB/bot context. A real code14 cached the global challenge but could not notify the existing administrator flow. The ordinary live-ID reconciliation caught the exception and only logged it; no `captcha_wait:*` marker existed. The normal outbox worker saw the cached challenge and excluded VK tasks before they could enter its ordinary CAPTCHA exception handler. Without a marker, the existing bounded harmless-read recovery had no durable cohort to inspect.

The existing mechanism from [July18](INC-2026-07-18-vk-captcha-publication-cadence-gap.md) was used, without broad unpause or historical replay. First operator registration failed with SQLITE_BUSY and rolled back; only after checking no marker was present was that failed attempt resumed. The existing helper registered192 current VK jobs under `captcha_wait:calendar-comment-20261112`, preserving the normal600-second cooldown and paced recovery. No provider calls, forced resume, changed attempts or budget bypass were used. Historical already-paused rows remain outside this marker. Recovery success is still a live criterion.

The prevention change passes the current DB/bot to the existing VK transport and routes code14 from the ordinary calendar scan into the existing `vk_captcha_pause_outbox`. No new state, scheduler, worker, retry policy, limit, provider actor or challenge solver is introduced.

Mandatory checks: delivery passes its exact DB/bot context; ordinary scan registers the existing CAPTCHA pause on code14 and does not pause on success; existing marker-scoped resume excludes historical pauses and preserves pacing. Run `tests/test_vk_calendar_comments.py`, `tests/test_job_captcha_pause.py` and `tests/test_vk_captcha.py`. Qualification is not live acceptance: verify the ordinary worker's marker-scoped recovery and a fresh managed comment after cooldown, without duplicate publication.

Public proof: [new workshop](https://vk.com/wall-231920894_14106?reply=14108), [new exhibition](https://vk.com/wall-231920894_14098?reply=14099). Retained evidence: `/home/dev/artifacts/events-bot-new/20261112T135027Z-calendar-friendly-file-20261112/`.

Pre-delivery multi-event review of PR900 found that a shared post could hit the cached challenge twice in one scan. Stop the existing scan after registering the first CAPTCHA cohort, so it does not replace the resume callback with an empty second cohort. The initialized two-event contrast fails before this one-line change;25 calendar/captcha tests pass afterwards. No prevention image was built or deployed before this correction.

The same repeated action also occurred across scans. At 15:07:14.089 UTC a
calendar error reached `main.py:3988`, the cached CAPTCHA early raise before
provider HTTP; a new cohort was recorded at 15:07:14.092. Earlier cohorts were
created at 14:45:33, 14:52:31 and 14:58:53. Since automatic recovery uses the
newest paused timestamp, these cached errors renewed its 600-second cooldown.
An initialized two-scan regression reproduces two pause calls without provider
HTTP. The existing calendar action now runs only when `_vk_captcha_needed`
is false. Service-token live-ID reconciliation and its existing recovery limit
remain active. First real CAPTCHA still uses the original pause/break path;
there is no new timer, marker migration, forced resume or provider bypass.
Verify ordinary marker-scoped recovery and public photo delivery after release;
passing the regression does not establish live CAPTCHA clearance.

## Owner correction: no cosmetic comment rewrites, November 12

The owner rejected regular comment edits without a real content change and
prefers deleting the previous comment and creating a replacement when needed.
Deferred first delivery is already deployed and remains unchanged. A later
read-only log audit found 39 calendar deliveries on 26 posts, including repeated
document uploads after filename/template releases. Full report and bounded
traffic comparisons are retained at
`/home/dev/artifacts/events-bot-new/20261112T183333Z-vk-request-load-retrospective-20261112/report.md`.
This supports an aggravating-load hypothesis; it does not establish VK's
internal reason for the account block, and CAPTCHA existed before this rollout.

Prevention uses calendar-byte hashes and accepts the previous filename-inclusive
receipts, migrating them locally without any VK calls. Template membership no
longer triggers replacement. A real calendar change deletes the previous
comment and creates a new content-version GUID through the existing deferred
scan. The existing comment lookup adopts a document-matching accepted creation
after a lost response, without deleting or creating it again. No new persisted
fields, queue, timer, actor rotation or challenge bypass are introduced.

The calendar lifecycle regression must show zero VK calls for unchanged content
and legacy receipt migration, delete/create on an actual time change, and
no duplicate write after a lost creation response. Existing CAPTCHA and deferred
publication checks remain mandatory. Live replacement and ordinary multiphoto
acceptance remain open while the owner prepares replacement credentials.
