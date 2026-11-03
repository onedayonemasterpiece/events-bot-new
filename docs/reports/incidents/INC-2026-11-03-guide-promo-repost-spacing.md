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
