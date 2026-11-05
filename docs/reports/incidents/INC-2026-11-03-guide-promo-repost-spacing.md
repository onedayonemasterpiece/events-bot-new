# INC-2026-11-03 Guide promo catch-up collapsed independent repost delays

Status: open
Severity: sev3
Service: Events Bot / promo_vk / guide-digest reposts
Opened: 2026-11-03
Closed: —
Owners: Codex
Registry: inc_9b10802ccbe05795372dd983
Related incidents: INC-2026-06-04-80-stories-promo-vk-scheduler-gap, INC-2026-11-02-intake-quota-vk-media
Related docs: docs/features/promo-campaigns/README.md

## Summary and user impact

The owner requested independently delayed excursion-digest reposts into two VK
communities. Both actual posts were sent together by the late catch-up callback.
Presence and idempotency were verified, but temporal acceptance was missed.
This is a mechanical scheduling defect, not event semantics.

## Detection and timeline

- Edition326 source wall-238875824_376 published at08:40:05 UTC.
- Campaign14 froze separate due times: activity48 at11:10:05 UTC, activity49
  at10:14:05 UTC (13:10 and12:14 Europe/Kaliningrad).
- A late invocation at13:12 UTC sent both expired actions in one pass:
  wall-231920894_13296 and wall-231828790_2304. Public copies and durable receipts
  are real; that does not satisfy spacing.
- Owner reported simultaneous delivery. Earlier acceptance statements are
  corrected: today's content delivery passed; requested timing did not.
- Qualified source135540579 is insufficient final prevention and is superseded
  as a deployment candidate. Actual runtime remainsfd4da1b8 with active parser11581.

## Root cause and contributing factors

Random due timestamps alone did not constrain actual sends once both were overdue.
The serial runner executed every due activity. Independently sampled timestamps
could also fall within one five-minute scheduler tick. Tests checked duplicate
prevention and cadence, but allowed two sends in the same pass.

## Immediate mitigation

Keep today's two correct public posts; do not delete or repost to simulate a gap.
Preserve their published receipts. Do not deploy the known-insufficient candidate
as final prevention. Continue original import and cinema delivery work.

## Corrective action

Use the same per-edition ledger to choose due times at least35 minutes apart
(30 minutes plus the existing five-minute tick). Enforce at least30 minutes
between actual sibling sends, including overdue catch-up. Retain attempt time
in the same receipt when a provider reply is lost; it spaces the other target
without an indefinite cross-target block. Observation uses VK's actual post date,
not the later observation time. No new table, queue, timer, flag or retry worker.

## Automation contract

### Treat as regression guard when

Changing promo_guide_digest.py, promo runner timing, or guide_visual_delivery receipt handling.

### Affected surfaces

Campaign14 activities48/49, source guide edition receipts, actual VK reposts,
existing promo_vk interval and per-edition delivery ledger.

### Mandatory checks before deploy or closure

- Distinguishing overdue test fails on135540579 and passes after the fix.
- Normal due-time dispatch and overdue dispatch never bunch the destinations.
- Lost reply/readback preserves spacing and never resends an accepted repost.
- Cadence, daily caps, pause and existing promo/delivery regressions pass.
- Merged-main exact source/image qualification, unchanged cinema replay function,
  Events-only delivery, health and unchanged Kotopogoda.
- Actual normal promo tick after delivery; verify the next eligible normal pair
  by provider timestamps. Today's simultaneous pair cannot prove this criterion.

### Required evidence

Retained promo-guide-delivery-ledger.json, promo-guide-public-verify.log,
promo-spacing-before.log, promo-spacing-after.log and promo-spacing-final.log;
release SHA/image, scheduler logs and future actual post timestamps.

## Release and closure evidence

Fix is being tested; not deployed. Original source import must not be interrupted.
Keep the incident open until delivery and temporal verification are recorded.

## Follow-ups

Inspect normal VK13:30 UTC source/child receipts, original14:30 and TG21:40 UTC
checks alongside this fix. Next ordinary two-destination eligibility is November6
if today's successful Анонсы repost is the cadence anchor. Extend verification
rather than sending unauthorized duplicate public canaries.


## Combined release delivered, 2026-11-03 13:58 UTC

Actual RunCoveer Events runtime is now `7d03b15853d3548299aa334418a9a907ae415312`,
image `intake-guide-promo-spacing-20261103`, immutable
`sha256:96896a2b00717740524948c4b49caedc4b6f2a96e6dba4bf23c5482789ade566`.
PRs #777 and #778 passed all required CI before merge. Source parser completed
261/261 before cutover; the final guard found no active ops. Events alone was
recreated. Kotopogoda container ID remained unchanged. Explicit runtime/model
settings checks, SQLite quick_check, effective cinema reference and identity
function hash passed. Live health reports ready=true, database/scheduler ok,
no issues. The existing promo interval is five minutes.

Evidence is retained in the intake recovery directory under
`promo-spacing-cutover.log`, `promo-spacing-live-verification.log`,
`promo-spacing-live-health.json` and `promo-spacing-startup.log`.
This supersedes earlier not-deployed candidate status; it does not close the
incident or substitute for actual scheduled source receipts.


## Preserved through receipt-codec release, 2026-11-03 16:24 UTC

Actual runtime215c6a219edec24551e2d38215cda8ccc396091c, immutable
sha256:fa97c6e09aa6634ceda9575569abd9a6568802f7ab73e7bdaf3d1b40a0f03a72.
PR783 passed all required CI; exact source and isolated initialized preflight
passed before safe Events-only cutover. Live model settings, cinema reference
and qualified identity/adjudicator hashes are unchanged; promo interval5,
readiness/database/scheduler ok and Kotopogoda unchanged. Existing public
receipts remain authoritative. Future normal promo timing on November4/6 and
normal scheduled imports still need acceptance; incident remains open.

## Preserved through verifier release, 2026-11-03 17:33 UTC

Actual runtime is 78282649624fdcea714c75d2b89ec915ea235fb5, immutable
sha256:d3ec7e74bd1480a23853342e039e1c92373e92338d0a34e551abcd35acb90415.
Events-only delivery preserved the five-minute promo interval and original
campaign. Normal ticks before delivery returned today's original URLs without
duplicate sends. Actual future spacing remains unaccepted until the normal
November 6 pair; November 4 daily Afisha also needs observation. Today's posts
remain untouched. No public canary, cadence reset or second scheduler was added.


## Preserved through parser-reserve release, 2026-11-03 17:55 UTC

Actual runtime 292334719d86fe14a06ce3a6cc8cb8fc970e81c4, immutable sha256:6a0555e01949e189278ce0b8d285512495099eeaca6f3771b732f60ac6ea109e. Required CI, full source and isolated preflight passed; Events-only delivery preserved cinema/identity rules, model settings, promo interval five, healthy DB/scheduler and unchanged Kotopogoda. Existing public corrections and today's two reposts remain intact. Ordinary TG and actual future promo timing still require observation; incident remains open.


## Preserved through SQL alias delivery, 2026-11-03 19:12 UTC

Actual runtime is 9f14c503f977b467ed6ad963e2de59008bc0a435, immutable sha256:8b8875452f667264b279f0a10459b92e3556d57b6ec2a02ccebf72590ef51e24. Events-only delivery followed terminal ordinary guide ops 11605. Explicit runtime/model/promo-five/cinema/hash/SQLite checks and live readiness passed; Kotopogoda unchanged. The ordinary guide digest issue 327 is empty, with no new provider publication or duplicate repost. Existing public corrections and today's two repost URLs remain intact. Ordinary TG and actual November 4/6 promo observations remain outstanding; incident stays open.


## Preserved through permanent-edit outcome release, 2026-11-03 20:07 UTC

Production now runs 3bb038338dce53f1e1a74be0441b85fa11dc2be8, immutable sha256:1b9971bb3137fa6053cb8e535861039582278a4a5f01fec80ab69ef30b9c3171. Required CI and full source/isolated/live checks passed; cinema reference, identity/adjudicator hashes, models and promo-five preserved, readiness/DB/scheduler healthy and Kotopogoda unchanged. A background media review crossed the restart and was verified recovered by the existing stale handling; source imports were idle. Existing public corrections and today's promo URLs were not changed. Normal TG and November 4/6 promo timing still require acceptance. All incidents remain open.

## 2026-11-04 08:40 UTC — source digest skipped behind ordinary VK

The normal 08:30 visual guide slot was submitted, then skipped `heavy_busy`
while VK11620 was active. VK finished at 08:32:51; the daily slot was already
lost. Campaign14's normal ticks returned `source_unavailable`, so no November4
repost is claimed. This is upstream source scheduling, not a failed repost.
Existing story/delivery ticks service older editions but do not create the
missing current daily edition. Do not repeat yesterday's successful pair.

Minimal prevention selects the existing `heavy_guard_mode="wait"` for this one
registered daily digest, as for ordinary VK. No new timer, retry, state or
queue. A parameterized real scheduler-registration test exercises both VK and
guide against the actual shared semaphore: before the fix the guide variant
terminalizes as skipped; VK control passes. Qualification and delivery remain
required. A bounded same-day recovery must use the original daily entrypoint,
its current-edition idempotency and existing campaign runner only after active
heavy imports finish; it is not normal-slot acceptance. Verify actual source
VK and later campaign receipts/provider timestamps.

Evidence retained in the existing intake recovery directory:
`vk-and-guide-0840.log`, `normal-slot-0840.json`, `guide-wait-before.log`,
`guide-wait-after.log` and `guide-wait-plan.md`. Production remains3bb;
qualified457 is not delivered. Two cutover attempts made no mutations:
first host Python alias was absent; the explicit python3 guard then found a
running media-review job. Background review keeps making progress; no claim
was reset or interrupted.


### 2026-11-04 09:11 UTC — combined scheduling and diagnostic release delivered

Production now runs merged main `53a7791360ca8308458ce11df313d23c5c7bfd03`,
image `intake-guide-wait-20261104`, immutable
`sha256:b55e338530de0414ad4e1ab4b20c7710e61a2d25c57008d2e677556ce929b65a`.
This includes PR #796 rejection diagnostics, #798 submitted-slot accounting and
#801 daily guide waiting on the existing heavy semaphore. All required CI,
exact 4,436-file source qualification, initialized isolated adapter/scheduler
controls and wrong-SHA control passed. Preflight intentionally disables startup;
its `ready=false` is isolation evidence, not production readiness.

Events-only cutover followed terminal normal imports and idle MCP/background
work. BGE job 144315 was already done at 08:55:13; its generic ledger remained
created and must not override the actual job result. Remote video session 1559
continues rendering `zigomaro/cherryflash` with a durable dataset and notification
target. Existing enabled popular-review watchdog calls `resume_rendering_sessions`
every ten minutes, independently of disabled global Kaggle recovery. That resumes
the same poller without launching another kernel. Release preserved the remote
run; actual resumption after the 09:20 watchdog still needs verification. No
registry reset, recovery flag change or duplicate remote launch was performed.

Explicit live source, SQLite, model settings, cinema reference and semantic
hash checks passed. Health has `ready=true`, DB/scheduler healthy and no issues;
Kotopogoda container and source mount unchanged. No old source operation,
programme replay, 37-row plan or exact false-receipt reconciliation was repeated.

Retained evidence: `guide-wait-release-receipt.json`,
`guide-wait-cutover-0909.log`, `guide-wait-live-verification-0910.log`,
`guide-wait-health-0911.json`, `guide-wait-video-deploy-decision-0909.md` under
the existing intake recovery artifact directory. Incidents remain open: actual
normal import acceptance and residual technical failures are separate criteria.

Today's bounded guide catchup ran exactly once through the original daily
entrypoint, trigger manual: run `INC-2026-11-04-guide-digest-catchup`, ops 11622,
terminal partial at 08:51:16, issue 328 with two future excursions. Do not repeat
it. Live Telegram posts 504 and 523 have the correct body and photo. Actual
public VK source is https://vk.com/wall-238875824_378, provider timestamp
09:00:38 UTC, original postponed ID 377. One read-only wall scan verified title,
two excursions and photo. MAX delivery remains failed `max_prepare_unavailable`;
that independent surface is not accepted and no replacement was requested.

A normal campaign tick resolved the live source and froze Afisha due_at
11:26:38 UTC, 146 minutes after actual source publication. The Anons prepared row
is refused by its existing three-day cadence until November 6. A prepared row
is not a sent repost. Today's Afisha send and future paired spacing remain
unverified; yesterday's URLs were preserved without duplicate sends. Manual
source catchup does not establish acceptance of the next normal daily guide slot.
Evidence: `guide328-public-0907.json`, `guide328-telegram-live.json`,
`guide-digest-catchup-apply.log` and `guide-jobs-0906.json`.


## 2026-11-04 11:35 UTC — normal campaign repost publicly verified

Today's normal campaign tick published the Afisha repost without an operator
callback: https://vk.com/wall-231920894_13435. The existing per-edition receipt
is published, retaining frozen due_at 11:26:38 UTC and source
https://vk.com/wall-238875824_378. A read-only live wall scan confirms one exact
matching copy_history/source identity in the latest 100 Afisha posts and none
in Anons. The public copy includes the two-excursion text and photo. No provider
write, manual resend, canary or cadence reset was used for verification.

Provider timestamp is 11:30:20 UTC (13:30:20 Kaliningrad), source timestamp
09:00:38 UTC: actual delay 149 minutes 42 seconds, within the owner's 30–180
minute requirement. Ledger published_at 11:30:00.635858 is the callback's
recorded send time, not the later VK provider timestamp; do not conflate them.
Anons is correctly cadence-skipped after its November 3 send until November 6.

This accepts today's daily Afisha delay and normal campaign execution. It does
not accept actual separation of the next paired destination sends, which remains
a November 6 observation, or the next normal daily source digest slot. Existing
November 3 posts remain unchanged. Incident stays open for those criteria.

Retained evidence: promo-guide-state-1134.jsonl, promo328-live-repost-1136.json
and promo328-acceptance-receipt.json in the existing intake recovery directory.
The independent 11:20 ordinary guide monitor ops 11627 is still scanning remote
source ruin_keepers; its final import result is not accepted from heartbeat alone.


## November 5 08:49 UTC — normal source accepted, delayed repost pending

Normal visual issue330 ran through scheduled ops11652 after waiting for VK11651. Public source https://vk.com/wall-238875824_382 is confirmed with provider timestamp08:43:59 UTC, postponed ID381. Campaign14 ordinary tick froze Afisha activity48 due10:50:59 UTC,127 minutes after the source; expected actual send is the first subsequent ordinary tick. No manual callback or canary. Anons activity49 has a prepared delivery but remains subject to existing three-day cadence until November6: prepared is not sent. Both payloads reference actual live source382. Real Afisha delivery and November6 paired spacing still need ledger and public copy_history acceptance. Retained promo330-ledger-0849.json and guide330-public-0848.json.


## November 5 11:10 UTC — today's ordinary Afisha repost accepted

Campaign14/issue330 published through the normal promo tick at ledger time10:55:51.912325 UTC, preserving frozen due10:50:59 and actual source382. Public VKread confirms https://vk.com/wall-231920894_13544 has copy_history pointing exactly to source wall-238875824_382, same text and photo. Actual provider timestamp10:56:03 minus source08:43:59 is7924 seconds,132 minutes4 seconds, inside30–180 minutes. Latest100 target posts contain exactly one matching Afisha copy and zero Anons copies. No manual callback, canary, reset or resend.

Anons remains correctly cadence-skipped untilNovember6 despite its prepared ledger row. Today's single-target delivery is accepted; actual next paired spacing is still a separate November6 check. Existing posts remain unchanged. Evidence promo330-ledger-1053.json (actual capture11:08:25), promo330-live-public-1109.json and promo330-acceptance-receipt.json in retained incident evidence. Filename timestamps are operator labels; JSON capture/provider timestamps are authoritative.


## November 5 19:02 UTC — second real digest coverage defect

Ordinary full guide11666 produced new_occurrences issue331 with a VK receipt, while campaign14 only selects visual_schedule target21 and caps Afisha once per day. The actual per-edition guide_visual_delivery ledger has no issue331 delivery. A separate cadence condition also denies another same-day issue for every_days1 even without an explicit cap. This contradicts the original after-each-digest requirement; morning repost13544 remains accepted and must not repeat. Public source identity/time for331 still needs the existing live resolver; a rejected social-reader call is not acceptance.

The narrow change applies multi-day cadence only when every_days is greater than one. Existing per-issue idempotency, explicit daily caps, frozen random delay, real-send spacing, three-day Anons cadence and lost-reply recovery remain. Net production change: one condition and one explanatory comment. The existing full scheduler test now contrasts capped and uncapped second editions, including a second family target. Before: four uncapped failures and four passing capped controls. After: all eight pass, including lost replies and overdue spacing. No provider, Event, Job or campaign writes.

Deployment and exact campaign target/cap correction are pending. Reuse existing family targets for visual_schedule, new_occurrences and last_call, and remove only the campaign/Afisha caps that prevent each edition; preserve Anons cap1/every_days3. No new scheduler, states, retries, exposure mechanism or public canary. Retained promo331-scope-read.json, promo-all-editions-plan.md and promo-editions-family.log.
