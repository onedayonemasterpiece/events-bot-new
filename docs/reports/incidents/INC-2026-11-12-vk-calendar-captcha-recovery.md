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

## Containment after credential replacement, 19:07 UTC

Exact main `3facbcacd947f054c2e1ebf1308f973dedd96a16` delivered the delayed,
content-only comment changes with a replacement primary credential. Source
verification, isolated preflight and live health passed. The actual process
loaded the new credential; its authorization had previously succeeded from the
production host. No broad token matrix or manual publication was performed.

The ordinary scan received upload-document HTTP 504 for events 10872 and 10871,
then real `docs.save` code 14 for event 10535 at 19:07:21 UTC. The first two
failures continued to another calendar candidate because the previous pacing
stopped only on success or CAPTCHA. No comments were delivered; normal photo
jobs 154623/154705 paused under the existing global challenge handling.

Calendar and photo upload use the same user credential. The last challenged
method does not prove that VK's restriction is method-scoped; do not bypass the
cache or rotate actors. Temporary containment therefore subtracts only the
automatic calendar invocation from live-ID reconciliation. Service-token
postponed-ID recovery, existing challenge cooldown/probe/resume, media publishing
and calendar receipts remain intact. Automatic calendar comments are unavailable
until a legitimate working document route is verified. This is a partial
mitigation, not full feature or multiphoto acceptance.

Mandatory regression: exact published-ID recovery must not invoke the optional
calendar writer or add a new challenge pause. Existing marker-scoped recovery
and cached-CAPTCHA guards must still pass; ordinary photo delivery is verified
only after that existing recovery, never by ignoring a live challenge. Retained
implementation tests continue to verify delayed delivery, content-only receipts
and replacement safety. Live acceptance still requires real photo attachments.


## Owner correction: one scheduled action, no recurring own-wall scan

The owner explicitly rejected wall reconciliation and historical catch-up for
calendar delivery. The exact public timestamp is known when a new announcement
is placed in postponed posts. This supersedes the scan/content-replacement
regressions above: existing delivered comments are no longer updated by this
feature, even when an event later changes.

Historical source comparison found the five-minute live-ID scan already in
September's PR670, before this incident. Calendar integration on November12
converted it into a repeating writer. Source baseline `c7ee91e61b` and the
measured traffic analysis are retained in
`/home/dev/artifacts/events-bot-new/20261112T193558Z-vk-before-after-simplification-20261112/report.md`.

The replacement removes that scan from the worker and deletes its broad
matching loop. Successful creation schedules one task in the existing
JobOutbox at publish_date plus nonuniform 5–15 minutes. On execution a single
service read resolves that announcement; existing ICS transport uploads one
document and creates one comment. The saved receipt is reused after a lost
response. No old-public-post bootstrap, new queue, worker, CAPTCHA bypass,
actor rotation, content hash migration or automatic comment replacement.

Mandatory distinguishing regression:
`test_new_post_schedules_one_comment_after_public_time_without_wall_polling`
asserts no calls before the due time, exact new public URL, one upload/create,
no writes to the historical post, shared-session URL propagation and no repeat
requests after completion. Lost-response recovery must reuse the document
without another create. Existing marker-scoped pause/recovery remains required.
A release and real ordinary post/comment receipt are still needed for live
acceptance; this source change does not establish CAPTCHA clearance.


## Isolated scheduled document failure and repeated import UI, 20:40 UTC

PR918 actual `cc0edabccc1b26f0222c89e1ef2add731a0d70b0` passed live
health. At20:40:07 UTC job155301/event10888 performed one document upload,
then `docs.save` returned genuine code14 on the replacement primary actor.
There was no calendar history scan or clustered write in this invocation.
The harmless `wall.get` recovery probe cannot prove `docs.save` is usable.
Automatic calendar task creation is therefore temporarily disconnected again;
the existing affected job alone is contained, preserving its receipt. This
is not calendar feature acceptance. Photo jobs and the shared CAPTCHA guard
remain intact; no challenge bypass or actor rotation is introduced.

The owner also received the same68-post Telegram progress repeatedly. The
saved run `tg-new-account-bootstrap-20261112` has102 raw carriers,68 with
events; successive releases interrupted its server import. Existing scan
receipts correctly avoid repeat Smart Update, but the progress callback
created a new message even for `metrics_only` carriers. Start now reports
that already-computed status and the UI suppresses both start and done for
it. Metrics, aggregate summaries and incomplete/new-carrier progress remain.
The initialized distinguishing test imports the same completed carrier twice
with fresh real callbacks: no per-post send/edit, no Smart Update, both reports
retain metrics_only. It fails before the two-line fix. Focused recovery,
media, queue and calendar tests:76 passed in33.00seconds.

The owner's filename/slug hypothesis matches the earlier incident timeline:
`6b3b394fa` replaced `event-<id>.ics` and added filename to document_hash;
the first subsequent bulk rewrite at16:15 local preceded code14 at16:16.
This proves a re-upload trigger, not that Cyrillic itself causes a CAPTCHA.
Successful later readable-name receipts are counterexamples to that stronger
claim. No new provider request is needed to establish the repeated-upload bug.
Evidence: `/home/dev/artifacts/events-bot-new/20261112T204450Z-vk-no-repeated-metrics-or-docs/`
and the retained load retrospective. Calendar and initial full-gallery live
acceptance remain open.
