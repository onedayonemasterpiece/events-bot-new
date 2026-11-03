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
