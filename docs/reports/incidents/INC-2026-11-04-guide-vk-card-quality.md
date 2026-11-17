# INC-2026-11-04 Guide VK schedule card reduced to 320×400

Status: open
Severity: sev3
Service: guide visual digest / VK photo upload
Opened: 2026-11-04
Closed: —
Owners: Codex
Registry: inc_f5d0a79f3a7ffa6073158f3c
Related incidents: inc_9b10802ccbe05795372dd983 (promo spacing)

## Summary and impact

Readers of edition 328 saw blurred schedule text in VK. The renderer produced
1080×1350 JPEG (181054 bytes), but the published photo's largest size was
320×400. This is a mechanical media transport problem, not event semantics.

## Evidence and timeline (UTC)

- 07:05: guide light run 11617 skipped because the heavy gate was occupied.
- 08:50–08:51: a separate visual catch-up built edition 328 from saved
  occurrences 711 and 699. It did not scan the newly registered sources.
- 08:50:58 and 08:51:01: VK returned empty photo results for the original
  JPEG and first reduced variant. The 44952-byte second variant succeeded.
- 09:00:38: published wall post became https://vk.com/wall-238875824_378;
  photo-238875824_457239682 exposes maximum size 320×400.
- Investigation: the same 1080×1350 pixels re-encoded as full JPEG 4:4:4
  failed, while PNG (505256 bytes) uploaded successfully through the normal
  bytes uploader. Asset receipt: photo868977531_457264569.

Retained evidence:
`/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104/`
contains original-render.jpg, vk-published.jpg, vk-photo-evidence.json,
upload-probe-receipt.json and the exact bounded inspection/probe scripts.

## Root cause and correction

The existing upload fallback reduced pixel dimensions until VK accepted the
photo, sacrificing schedule legibility. Full-resolution PNG is accepted for
this exact card. Convert VK schedule payloads to PNG before the existing
uploader, and make its multipart MIME/extension match the attempt's format.
No new retry, queue, flag, mode, provider or schedule is needed.

## Automation contract

### Treat as regression guard when

Changing guide visual VK media serialization or the shared bytes uploader.

### Affected surfaces

`guide_excursions/visual_digest.py`, `main.upload_vk_photo_bytes`, VK wall photos.

### Mandatory checks before closure or deploy

- Distinguishing test: the actual VK publication boundary receives a PNG
  card at 1080×1350 and attaches its accepted asset to the wall request.
- Shared VK upload retry/source-image tests still pass.
- Live provider readback confirms the accepted PNG asset retains full size.
- Required CI and merged-main release/ancestry checks pass before deployment.
- Read back the affected normal publication path after deployment; a unit
  test or accepted asset alone does not prove deployed future behavior.

### Required evidence

Test results, immutable deployed SHA/image, full-size VK photo readback and
the original wall-post identity. Preserve existing publication receipts.

## Related observations and follow-up

New sources 23–30 have not been scanned yet. The next normal light slot is
13:20 Kaliningrad; owner-requested source-by-source import control is armed
for 14:20. Missing new guides in edition 328 does not mean no future tours.
Afisha repost is prepared for 13:26 local. Announcements activity has a
three-day cadence and last posted November 3; November 4 is not eligible.
Do not duplicate posts or collapse these independent schedules.


## 2026-11-15: returned VK wall owner differs from requested wall

- Incident registry: `inc_f5d0a79f3a7ffa6073158f3c`. Status: open; new-source acceptance and source-link repair are ongoing.
- Live `wall.get(owner_id=-237389053)` returned post `3892` with `owner_id=-211429559`, and personal posts owned by `41501067` and `408521495`. The scanner constructed URLs using the requested wall, so `wall-237389053_3892` did not resolve; `wall-211429559_3892` resolved with matching text through ordinary service API readback.
- This is a mechanical source identity error, not a semantic/event classification decision. Use the returned signed item owner when constructing source URLs; retain the requested owner only for older payloads without that field. No extra crawler, retry, state, source-date change or semantic bypass is needed.
- Regression: the actual cross-community case and a personal-owner case fail before the fix; both pass afterwards, alongside unchanged group/user fallback controls. Eight focused AST/runtime contract tests pass. These tests intentionally avoid notebook bootstrap and external providers.
- Evidence: retained `new-guide-source-post-links-check-20261115.log`, `new-guide-wall-owner-check-20261115.log` and `guide-recover-exact-1d1d54f10511-20261115.log` in `/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104`.
- Release and product verification remain outstanding at this entry: merge with successful required CI, preserve actual runtime ancestry, deliver the corrected source used to generate the next notebook, then verify canonical source links and occurrences through the ordinary new-registry scan. Do not publish the previously frozen old-only issue 346 or count source variants as independent tours.

### Owner-link fix release, 15 November 2026

PR 992 passed all four required CI checks and merged as
`2fb79197ff45920efe03d55d2805a27b3934b515`. Events-only cutover deployed
`runcoveer/events:guide-vk-owner-20261115`, immutable image
`sha256:8ba755d32a58be1a48c090c158fab2a419976b6716b657b55e1a8e59041767e2`.
The prior actual runtime `944f34175003cabc34edc90e2b6b37a182cf76eb` is an
ancestor; Dockerfile and dependency inputs are unchanged. All 4,497 tracked
files matched exact main. Network-isolated startup with a fresh separate
database returned HTTP 200, `ok=true`, `issues=[]`; production readiness
then returned `ready=true`, `db=ok`, with scheduler jobs healthy. Effective
environment, data mounts and Kotopogoda container identity were preserved.
Rollback is immutable image
`sha256:a30316ba3d73ea336f7c86280acc1586b01bc3c264ee13195c42ef7711a9f1a1`;
original Compose and cutover receipt remain in the exact release directory.

Retained evidence: `guide-owner-release-build-20261115.log`,
`guide-owner-preflight-20261115.log`, `guide-owner-cutover-20261115.log`,
`guide-owner-live-health-20261115.json` in the incident artifact directory.
The completed preflight container/database and original fix worktree were
removed. The ordinary full-registry catch-up has started locally with
81 enabled sources and automatic publication disabled. Remote completion,
source-level acceptance and corrected product publication are separate
outstanding actions; this release entry does not close them. The first
one-off helper exited before submission due to Python import path; the
corrected invocation uses the existing `/app` module path.


### 15 November: alias and schedule cards selected twice

Normal version 12 completed all 81 sources and 135 posts; local run 12271
was interrupted by another runtime release. Its exact completed output
was recovered by authenticated SDK version-number download (session
369966876) and imported by the existing saved-results handler as 12281:
6 created, 37 updated, 13 past skipped, 60 LLM OK and one provider error.
Source receipts are 76 OK, four access-denied private walls and one partial
source (unonatravel 503). These errors do not mean no events. The scheduled
20:10 run 12280/version 13 independently includes all then-current 87
sources. Later additions remain outside its frozen config.

Visual preview 347 selected both 745 “Зелёные истории” and 753 “Зелёный
наряд города” for 17 November 14:00. The candidate router did not offer
this pair to the LLM because title/route token overlap was zero. Source
texts were 3,214 and 2,099 characters; the existing 1,400-character clipping
removed the tail schedule, and guide names were absent from judge input.
Removing clipping alone was tested and still yielded DISTINCT: it is not
a sufficient correction. Route same-source/same-time candidates to the
existing judge, preserve full source and guide names, and instruct it to
check whether a narrative title aliases one specific scheduled outing.
Date, contact and organiser alone still cannot merge different outings.

Two contract tests fail before the change and pass afterwards. A live
read-only shadow of the ordinary dedup flow reduced 745/753 to 745 with
LLM confidence 0.95; a same-organiser/date/time negative control with
different routes and guides remained two rows (DISTINCT, confidence 1).
No occurrence IDs, dates, priority, roles or stored source payloads were
manually edited. No second dedup mechanism, retry or queue is introduced.

Expert review attempts failed: DevCoveer Opus returned provider_error;
Antigravity Opus was ineligible in this location; Gemini 3.1 Pro reservation
failed model_not_found in the central limiter. The live Flash-Lite results
are semantic acceptance probes, not a completed external consultant review.
Retained files: guide-745-753-dedup-probe-20261115.log,
guide-alias-dedup-shadow-20261115.log and guide-dedup-pro-review-20261115.log
in the existing incident artifact directory. Release and final VK
publication are not yet performed at this entry; preview 347 is unpublished.


### 15 November: canonical variant and four-row rendering acceptance

PR 995 was delivered as main 97f3a9988, image sha256:96d348e4c715179bcda6792dadf7c277323f20b658f6e689b54c75aa8dc53360, with all four CI, 4499-file proof, isolated preflight and live readiness. Preview 348 merged the alias pair but selected stale 753 instead of 745. Its city came from the Dostoevsky biography, organisation names replaced the actual guide, and booking_url retained a wrong-owner source fallback absent from the text. Normal source user-token API reads do return personal post 41501067_3447; the initial service-token omission was not proof of an inaccessible wall. The source explicitly names Olga Rubtsova and a Google registration form.

The existing pair judge now receives city and source URL and chooses canonical_side by facts grounded in the specific outing's full text, rather than favouring extra details borrowed from narrative context. No manual Event/occurrence edit or second selection mechanism is used. The updated context contract fails before and passes after adding these missing fields. Read-only semantic acceptance uses the actual pair plus the different-route negative control; artifacts are guide-canonical-grounding-shadow-20261115.log in the retained incident directory. External consultant review remains unavailable for the previously recorded provider reasons; these probes are not expert review.

Visual inspection of preview 348 also exposed not_specified in place text, long organiser text behind the portrait, and fourth-row/footer overlap. Extend the existing empty-value sanitizer, apply the existing pixel wrapper to row text with room before portraits, and reduce expanded row height from 236 to 228. No new renderer, fallback, persisted state or retry. Preview 348 was not published. A fresh normal builder and actual VK readback remain required after delivery.


### 15 November: public VK digest restored; later source rollout continues

PR 995 delivered main 97f3a9988 with a complete 4499-file proof. Its normal preview 348 exposed the canonical-quality and rendering follow-up above. PR 997 initially passed CI but met a changelog conflict after the concurrent OCR release. Merging fresh main retained both release notes. Updated exact head b857e24c1e749959e71c51b9fcf209372ea83377 passed all four CI jobs and merged as 59079296778805cf4906115fe90ddece2f8c2210. Running 885e9334 was its verified ancestor, preserving the concurrent OCR fix.

Image runcoveer/events:guide-canonical-20261115, immutable sha256:3aa80ec9c50eecfb540fa51def5fd9f98dd62146ac9ad3229c434f849ac20e49, passed the full 4501-file proof, isolated network-none preflight and live ready/db health. Events-only cutover preserved effective environment, data mounts and Kotopogoda. Rollback image sha256:1468c6f9d199c37851cc3b4355d00f9f10f6216d5cb99bacec2ef3a1a66794c4 and compose.before.yaml remain in its exact release directory. A later independent release, main 369ecfd82857699bd5617d2f24a6dc1a922d81fd, already includes our delivered fix. Its current repository image pin is preserved; this report does not switch runtime again.

The fresh ordinary builder created issue 349 with occurrences 745, 746, 747 and 763: walks on 17, 20 and 21 November, and Sturgeon Farm on 22 November. All four have new primary sources 47 or 52. Public source posts wall-211429559_3892 and wall1746008_13815 were independently read through VK API. Canonical 745 carries Olga Rubtsova and the actual Google registration form. The actual card was inspected before publication: no duplicate alias, service sentinel or portrait/footer overlap.

One guarded VK send first returned draft wall-238875824_409. Independent user readback showed post_type postpone for 21:50 Kaliningrad, rather than a live post; no second wall.post was attempted. After that time, wall.get independently returned exactly one issue 349 as live post_type post, actual ID 410, date 1794772200. Verified public URL: https://vk.com/wall-238875824_410. Attached photo -238875824_457239722 has maximum resolution 1080×1350. The existing issue publication-state writer reconciles the canonical issue URL/message IDs to 410 while the original immutable transport receipt remains audit evidence for 409. This is receipt reconciliation only, not a resend or occurrence edit.

The unchanged Uhty stories flag also produced story-238875824_456239101 during the normal send. Afisha stories were not changed. Frozen issue 346 and previews 347/348 were not resumed; Telegram posts 531/550 were not replayed. Public VK wall acceptance is complete for today's issue 349; this does not claim all later registered sources were already imported.

Scheduled version 13 exact recovery completed as ops 12285 PARTIAL with all 87 source reports: 80 OK, five API 15 private-wall errors, and two provider-analysis partial sources (club219302938 post 14251; unonatravel post 6388). Errors do not mean no-event. New occurrences 765 and 766 belong to previous sources, not new-registry value. Full source reports are retained in guide-run13-acceptance-12285-20261115.json.

One ordinary FULL, ops 12288 / run 34c50f791e83 / version 14, was submitted for all then-current 122 sources, including IDs 88–122. Its config was independently downloaded and compared. Local ops crashed on the deliberate runtime cutover, while the exact version 14 registry survived and remote processing continued. At 21:55 it completed 16 of 122 sources and read 55 posts. Do not repeat SaveKernel or start a competing scan; reconcile and import that exact result when complete. The current registry has 131 enabled rows; IDs 123–131 were added after config freeze and remain next-pass work. Raw rows are not a unique-wall count because canonical aliases exist.

Evidence in the existing managed retained incident directory: guide-canonical-release-build-20261115.log, guide-canonical-preflight-health-20261115.log, guide-canonical-cutover-20261115.log, guide-canonical-live-health-20261115.json, preview-guide-canonical-four-selection-20261115.json, visual-preview-349.jpg, publish-guide-visual349-vk-20261115.log, guide-visual349-user-provider-readback-20261115.log, guide-visual349-live-wall-20261115.log and guide-visual349-canonical-receipt-20261115.log. Provider transport acceptance, public publication and later registry coverage are separate verified stages.


### 2026-11-16: missing source portraits and unused descriptions

Owner observed missing avatars in visual issue 349 after publication. Source
inspection found only a curated `_AVATAR_FILES` inventory, so newly registered
VK sources always fell back to initials. Event medallions likewise read only
curated manifests. Existing guide rows already contain `digest_blurb` and
`summary_one_liner`; the renderer did not consume them.

Correction uses one shared signed-owner VK portrait cache at existing async
collection/render boundaries, existing reader credentials and CDN storage.
Event portraits are labelled sources, without manufacturing organizer or
venue bindings. Static-site staging carries the public source-avatar index
into the Kaggle source bundle. Cards fit the existing blurb in spare space;
no extra semantic classification or description generation is introduced.

Mandatory acceptance: new group and personal-page portraits reach guide and
event renderers; equal numeric group/user IDs remain distinct; repeated reads
reuse the downloaded file; missing images retain the current fallback; card
geometry and source identity stay correct; no unsolicited public post is made.
Focused tests and production delivery evidence are recorded in the task state.
Deployment and live visual acceptance have not yet run for this change.

### 2026-11-16 — Preserve co-guide and organizer medallions

Read the actual historical VK cards 301/339 and 319/365. Their captions retain
multiple names while the universal schedule renderer paints one portrait. The
existing promo renderer already accepts several avatars; this fix restores the
same participant distinction in schedule cards rather than reducing a tour to
its source page. Owner explicitly identified this as existing behavior.

The shared portrait preparation binds extracted names to exact registered public
identities, including explicit parenthesized name variants, and retains legacy
Telegram portrait assets. Co-guides and organizers receive separate medallions;
the same public VK identity in both roles receives one portrait and both labels.
A page without an extracted participant role remains labelled source. Empty
latest fact-pack roles override older occurrence metadata left by an upsert.
Existing main_hook provides the optional description before older short blurbs.

The existing Tier-1 schema now describes the two role fields. A before/after
probe on real source posts found the old extractor omitted the explicitly named
80 ЛЕТ ЕДИНСТВА project; the revised schema returned it for all seven schedule
blocks and retained both guides on joint tours. An initial probe also invented a
farm guide from the source title; the guide description was tightened to forbid
that inference, and the repeated real-source negative case returned no invented guide or organizer for all three farm dates. No new LLM
step, credential, model, database table, worker or publisher was introduced.

23 focused portrait/card/schema checks passed. Real portraits for Olga Rubtsova,
Noza Usmanova, Yulia Grishanova and Tatyana Udovenko were fetched through the
normal VK reader; local rendered roles were inspected. Retained probe logs and
images are in the managed 20261104T093210Z-guide-digest-vk-quality-20261104
directory. This entry records implementation evidence; deployment and public
product acceptance are recorded separately after they happen.


Normal-import regression reproduced stale organizer metadata surviving a newer
explicit empty role list. The occurrence upsert now replaces a role list when
that role was authoritatively supplied by the new fact pack, including an empty
list; sparse status updates still preserve roles. The distinguishing test uses
the ordinary results importer twice, fails before the change with the old
organizer still present, and passes after it. This fixes the stored data path,
not only its portrait rendering.


### 2026-11-16: durable guide recovery after a container restart

The owner requested a permanent fix for a remote scan whose local poller dies
at deployment. Live run8a5351821148/version17 continued while ops12367 became
crashed. Production has ENABLE_KAGGLE_RECOVERY=0 intentionally; consequently
neither its broad periodic sweep nor startup sweep handled guide results.
Do not enable the other product recoveries to repair this narrow failure.

The existing recovery interval now selects the guide-only handler when guide
scheduling is enabled and the broad switch remains off. It uses the existing
run lock instead of a PID comparison that fails after Docker reuses PID1.
Downloaded exact results are recorded in existing registry metadata before
import. Failed materialization retains that job for a later ordinary tick.
An accepted ops_run import_summary receipt is reused rather than materialized
again if the process exits before registry settlement. The common importer
reconciles the exact completed Kaggle ledger and its owned session lease.
No new table, scheduler, model, scan, queue or general recovery switch.

Baseline regressions failed for missing periodic registration, reused PID and
repeated accepted import. Focused guide/scheduler checks pass. Full relevant
scheduler/guide/video/session regression run is recorded in task evidence.
Production deployment and automatic acceptance of17 are separate live checks;
this entry does not claim they have completed. Visual rendering is unchanged.

Expanded local regression: 218 tests PASS, including scheduler, guide, Kaggle status, video poller/pipeline and partner scheduling. Initial five video fixture failures were due to missing tracked CherryFlash assets in the sparse checkout; restoring those tracked paths resolved all five. Runtime release remains a separate live acceptance step.

### 16 November 2026 — Transport evidence on schedule cards

The old card selected walking from street names and missed the extracted value
`walking_tour`. Replacing an occurrence fact pack on a sparse update could also
discard transport. The normal import now keeps post-linked, scoped transport
claims. Card selection uses direct evidence, a named route template, or an
explicitly linked monthly VK programme. Group size remains a participation fact.

Retained comparison: `transport-d07db37d/` in the incident artifact directory.
Nine real primary inputs produced 26 Tier-1/template rows on main and 30 on the
candidate. On a disposable import, the 20 and 28 November “Рассвет над Балтикой”
cards showed walking with both the schedule (wall-211429559_3892) and programme
(wall6939421_6285) URLs. The 22 November farm card had no transport. The source
poster and both canonical posts were already inspected; neither has a repost
that explains these cases. This change has no VK ingestion or publisher delta.
Release, live import and public card verification belong to coordinator acceptance.

Coordinator review reproduced 13 legacy-icon regressions from actual production
claim spellings (including `walking tour`, «пешеходная», «автобусная»). Exact
alias normalization now reads only the existing type field; title/geography
guesses stay removed. The distinguishing regression changed from 13 FAIL / 2 PASS
to all PASS. Independent integrated transport/import/push/recovery tests: 89 PASS.
Avatar layout PR1026 stays separate; this entry does not claim a live typed import.

### 16 November 2026 — Coordinator release and automatic scan acceptance

PR 1031 merged after four successful CI checks. Independent integration tests: 89
PASS; legacy regression before correction: 13 FAIL / 2 PASS. Events-only release
2397c970d988333f8e1e6ec61de4b052f344919d uses immutable image
24dad7d68dcc2ec068e8420d4fab5966e077cd2abf719bbabacc317a900050e8
(`guide-transport-20261116`). All 4522 tracked files passed verification. Isolated
preflight exercised legacy walking/bus/mixed types and the negative geographic
control. Live readiness, database, scheduler and tasks are healthy. Existing
environment, mounts, data and Kotopogoda were preserved. Rollback is the
previous `guide-result-push-20261116` image a204fc391382. This release does not
activate the separate avatar layout or the prototype narrative writer.

The ordinary recovery scheduler accepted LIGHT 18 / run 495d0491c019 in ops 12385
SUCCESS at 14:15 Kaliningrad, without manual resume. All 192 source reports are
ok, including late IDs 190–196; no enabled source is missing. 173 posts, 51 LLM
OK, zero deferred/errors, 3 created and 22 updated. These are raw row counts:
784 is undated/ineligible;785 is a variant of 781 from the same original
post 23490;783 offers a December 15 visit for an organized school group. Do not
report these as three new unique public tours.

The first real direct-push run on this release and old programme grounding
remain separate live acceptance steps. The November 2 programme post 6285 is
absent from the imported posts and outside a normal five-day full scan; a
subsequent successful full alone cannot establish that specific claim. No
manual transport, source dates or event rows were assigned. Retained receipts:
`guide18-auto-import-acceptance-20261116.json`,
`guide18-new-occurrences-identity-20261116.json`,
`guide-transport-live-acceptance-20261116.log` in the canonical incident artifact
directory. Completed preflight fixtures can be removed; keep release rollback
and canonical source results.


### 16 November 2026 — Preserving explicit source exclusions

PR 1047 removes field-density promotion of an explicit negative extraction
result. The original OSQ announcement (Telegram excursions_profitour/982)
offers a November 19 visit for an organized school group. FULL 19 correctly
returned digest_eligible=false with that explanation, but the server promoted
it to true because logistics fields were populated. Net production change:
-14 lines; existing status/date checks stay in place. No second classifier,
LLM call, queue, retry, flag or model/quota change was introduced.

The normal-import regression reproduced two failures before the change;
the corrected focused suite passed 18 tests, and the exact candidate image
passed all four eligibility cases. All four required GitHub checks passed.
Merged main: 193578780cd25b991b5238c8d1d44caec7dbd46b. The exact image
runcoveer/events:guide-exclusion-20261116 has immutable ID
sha256:f4f1fa984ddb581380569fddad656b298839fe43b3fcb500a12dd6ffa87e71b2;
all 4524 tracked files matched Git. Network-isolated preflight boot passed
with scheduler and webhook disabled; production data was not mounted.

FULL 19 was accepted through the existing saved-input recovery in ops 12405:
192 reports, 187 ok / 5 partial, 309 posts, 89 LLM ok / 3 deferred / 2 errors.
Its registry is settled. The last resume created undated/ineligible row 788
and updated 61 rows. The real PUSH failed with three URLError responses;
fallback acceptance is not direct-PUSH acceptance. The observed 355 MiB media
bundle exceeded the old 128 MiB receiver capacity; 512 MiB is now active.
That mismatch alone does not prove the cause of the unclassified URLError.

One ordinary FULL 20 (run 0f1ee3ff3b88, ops 12414, version 20) is reading all
201 sources, including late IDs 197–205. Its frozen config was independently
compared with the enabled registry: no source missing. Do not submit it again.
The separate CLI container uses the previous immutable image, so the app
release does not repair its already-loaded importer. OSQ row 786 still needs
normal source-level acceptance on the corrected code after that run settles;
no category was manually changed. School-group rows 765/783 have a different
semantic extraction defect (raw positive eligibility), not fixed by PR 1047.
The old Noza programme 6285 is outside the five-day scan window and still
needs the existing targeted source path. These limits remain explicit.

Evidence is retained in the canonical incident artifact directory:
guide19-terminal12405-and-links-20261116.json,
guide-version20-frozen-coverage-20261116.json,
guide-exclusion-build-20261116.log,
guide-exclusion-candidate-test-20261116.log and
guide-exclusion-cutover-20261116.log. No accepted import, public digest,
Stories or prior Slack success message was repeated.

Events-only cutover completed: effective environment and mounts were unchanged,
Kotopogoda container identity was unchanged, live health returned ok=true,
ready=true, issues=[], and receiver capacity remained 512 MiB. Rollback image
is sha256:3790fe576507360d35bb3711026492e3daf7408fea0bfb511238a8ca72c23063.
The FULL 20 one-off remained running after cutover. Live receipt:
guide-exclusion-live-and-guide20-20261116.json. Production OSQ source
acceptance has not been executed while that run owns the guide lock.


### 16 November 2026 — Real Kaggle push and FULL 20 acceptance

FULL 20, run 0f1ee3ff3b88 / version 20, was materialized in ops 12414
PARTIAL at 18:44:41 UTC (20:44 Kaliningrad). The real Kaggle sender produced
a durable guide_result_saved receipt at 18:36:40 UTC. The saved package has
280 media files, its pending marker is removed, and its JSON SHA-256 equals
the normal importer's input: c2a67b8848c77edff21c612333fc686d562f8f3e2f5bb2d7e2556a825fde1c62.
The exact registry is settled. No explicit recovery or duplicate scan/import
was submitted. The standalone launcher remained alive, so this run proves
real push delivery and materialization, not a dead-listener scenario by itself.

All 201 frozen sources have reports: 161 ok / 40 partial; 322 posts,
15 LLM ok / 76 quota-deferred / 5 provider errors. Those deferred/error posts
remain unresolved; PARTIAL does not mean no tours. Late sources 197–205 all
completed without source errors: six posts on four walls failed the announcement
prefilter and five walls had no posts within five days. Older future announcements
are not excluded by that window. The registry now has 202 enabled sources;
late source 206, club212760444, has last_scan_at NULL and needs the next pass.

Created occurrence 789 is the free city tour in Slаvsk on November 25 at 12:00,
source 50 / https://vk.com/wall-240021059_82. A fresh wall.getById read verified
the signed owner, original text and registration https://vk.cc/d3Z4bO.
The canonical row and source anchor exist, digest_eligible=1, and no visual
issue has published it. Updated 775 is the existing Samland trip, not a new tour.

After FULL 20 settled, one normal saved-source catch-up began for unchanged
OSQ post 982 through run_guide_import_from_results and its existing OS lock.
It does not replay all of accepted FULL 19. Ops 12419 remains running in
optional provider enrichment retries; the importer already committed row 786
with digest_eligible=0 and the original school-group exclusion explanation.
No category was changed with SQL. Read its terminal receipt after completion.
Raw-positive school rows 765/783 and old Noza programme 6285 remain separate work.

Evidence in the canonical retained artifact directory:
guide20-current-receipts-20261116.json,
guide20-occurrences-live-before-osq-20261116.json,
guide20-push-saved-and-imported-identity-20261116.json,
guide20-slavsk-provider-read-20261116.log,
osq982-standard-source-acceptance-20261116.log.
PR 1048 passed all four CI and merged as bdba118d3e4872ccf62cb9e1d21bb407d125c964;
no documentation-only runtime deployment was performed.
