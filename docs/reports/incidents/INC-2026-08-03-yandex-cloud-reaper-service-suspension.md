# INC-2026-08-03 Yandex Cloud reaper service suspension after account transfer

Status: mitigated; closure pending release of the regression guard
Severity: sev1
Service: Supabase relay, focus Auth/email gateways, inbound email and Postbox feedback
Opened: 2026-08-03
Closed: —
Owners: events-bot production owner / Yandex Cloud operations
Related incidents: `INC-2026-08-03-ydb-request-unit-billing`, `INC-2026-07-30-focus-email-otp-false-success`
Related docs: `docs/operations/email-delivery.md`, `docs/features/unsigned-personalization/production-integration.md`, `docs/operations/release-governance.md`

## Summary

During the Yandex Cloud account/organization transfer window, the system actor
`yc.iam.reaper` stopped every KenigEvents API Gateway and paused every trigger in
the isolated email folder. The dedicated Postbox feedback YDB also became
`STOPPED`. Core Fly bot health remained ready, but mobile Supabase fallback,
focus Auth/email hook routing, protected OTP E2E receipt and automated inbound /
Postbox feedback processing no longer matched their accepted live desired state.

## User / Business Impact

- Phones that need `kenigevents-supabase-relay` lost the accepted fallback route
  for safe Supabase Auth/Data access.
- The focus Auth email hook and protected OTP E2E gateway were unavailable.
- Automated inbound email collection/processing and Postbox feedback ingestion
  were paused. The retained SpaceWeb mailbox remained the durable human-mail
  fallback.
- Application transactional/recommendation sending remained disabled by its
  independent Supabase gates, so there was no uncontrolled outbound replay.
- The Region Talk database and scheduler are explicitly outside this recovery
  scope and must not be enabled or queried by the incident fix.

## Detection

- A post-migration YDB dependency audit found `kenigevents-email-events` in
  `STOPPED` and all five email-folder triggers in `PAUSED`.
- Trigger and API Gateway operation histories identify `yc.iam.reaper` as the
  actor and show synchronized disable operations at 03:49–03:50 UTC.
- Existing health checks covered Fly workers but not the external gateways,
  triggers or YDB control-plane status.

## Timeline

- 2026-08-03 03:49:12–03:49:13 UTC — `yc.iam.reaper` stops all four KenigEvents
  API Gateways across `default` and `kenigevents-email-prod`.
- 2026-08-03 03:50:02 UTC — `yc.iam.reaper` pauses all five triggers in
  `kenigevents-email-prod`.
- 2026-08-03 16:18 UTC — control-plane audit detects the stopped/paused state.
- 2026-08-03 16:25 UTC — per-resource operation history confirms the common
  system actor and transfer-window timing.
- 2026-08-03 16:27 UTC — outbound safety audit confirms all three authoritative
  Supabase switches disabled and `dry_run_only=true`; the retained Postbox DLQ
  predates the transfer and is not replayed.
- 2026-08-03 16:28–16:30 UTC — only the accepted KenigEvents email database,
  four gateways and five email triggers are resumed. The IMAP collector completes
  two fresh invocations without error.
- 2026-08-03 16:39 UTC — repeated public acceptance passes: Supabase relay
  `10/10`, focus YDB control `5/5`, and Fly `/healthz` HTTP 200/ready.

## Root Cause

1. Yandex Cloud's `yc.iam.reaper` system actor suspended the traffic-bearing
   resources during the account/organization transfer window. The exact
   upstream billing/IAM condition that instructed the reaper remains a provider
   support follow-up; timing correlation alone is not treated as that answer.
2. Rebinding/access restoration did not automatically resume API Gateways,
   triggers or the Postbox feedback YDB.
3. The migration audit focused on the Region Talk database and did not inventory
   unrelated production serverless resources in both target-cloud folders.

## Contributing Factors

- The inbound desired-state reconciler treated an existing trigger as `ready`
  without checking whether its runtime status was `ACTIVE`.
- Fly `/healthz` cannot observe external API Gateway or trigger control-plane
  state.
- No post-transfer checklist required a complete target-cloud service inventory.

## Automation Contract

### Treat as regression guard when

- moving a cloud between organizations or billing accounts;
- changing Yandex Cloud ownership, IAM identities or billing bindings;
- changing the resilient Supabase relay, focus email gateways, inbound email or
  Postbox feedback infrastructure;
- changing the inbound desired-state reconciler.

### Affected surfaces

- API Gateways `kenigevents-supabase-relay`, `kenigevents-focus-connectivity`,
  `kenigevents-focus-auth-email-hook`, `kenigevents-focus-otp-mail`;
- email triggers, Cloud Functions, Postbox feedback YDB/YDS and YMQ DLQs;
- `infra/yandex/email-inbound/reconcile.py` and desired-state manifests;
- focus OTP/relay smoke paths and Fly email worker/monitor health.

### Mandatory checks before closure or deploy

- complete control-plane inventory of every folder in `cloud-art-koder` and
  classify each `STOPPED`/`PAUSED` resource before changing it;
- restore only resources whose committed desired state is live; do not start the
  obsolete KGD80 Postbox database or alter Region Talk;
- prove all four accepted API Gateways and all five intended triggers are active;
- prove `kenigevents-email-events` is running with zero provisioned RCU and its
  hard 10 RCU/s ceiling;
- run safe HTTP health/route checks for the Supabase relay and focus connectivity
  gateway without issuing OTP or sending mail;
- verify Fly health and email worker/monitor health, Supabase outbound gates off,
  and empty/acceptable queue/DLQ state;
- make the inbound reconciler fail/report drift when a required trigger is not
  `ACTIVE`, with regression coverage;
- prove the released guard is reachable from `origin/main`.

### Required evidence

- per-resource `yc.iam.reaper` operation IDs and timestamps;
- before/after control-plane inventory and safe route probes;
- queue/DLQ and application-gate evidence;
- reconciler tests and deployed/released SHA reachable from `origin/main`.

## Immediate Mitigation

- Audited the full target cloud and classified non-active resources before any
  mutation.
- Verified application outbound gates first: `global`, `transactional` and
  `recommendation` are disabled and dry-run-only in Supabase.
- Started only `kenigevents-email-events`, then resumed the four committed
  KenigEvents gateways and five intended email triggers.
- Left the obsolete KGD80 `postbox-events` database stopped. Region Talk remained
  disabled and was neither queried nor changed by recovery.
- Preserved the Postbox DLQ: 162 visible messages are a pre-existing focus OTP
  correlation backlog, not transfer catch-up work.

## Corrective Actions

- Hardened `infra/yandex/email-inbound/reconcile.py`: an existing Function or
  Trigger is no longer `ready` unless its runtime status is exactly `ACTIVE`.
  Non-active state is fail-closed `drift` and never produces an automatic resume
  command.
- Added regression tests for `PAUSED` Trigger and `STOPPED` Function inventory.
- Added the complete post-account-transfer inventory/backlog safety checklist to
  the canonical email operations documentation.

## Follow-up Actions

- [ ] Add a post-account-transfer inventory checklist for all cloud folders.
- [ ] Add external status monitoring for critical API Gateways and email triggers.
- [ ] Ask Yandex Cloud support to confirm the exact reaper trigger/billing event.

## Release And Closure Evidence

- live recovery: direct Yandex Cloud control-plane operations, recorded in
  ignored artifact
  `artifacts/codex/INC-2026-08-03-yandex-cloud-reaper-service-suspension/restore-operations.txt`;
  no Fly application deploy was required.
- release SHA: pending merge to `origin/main`.
- regression checks:
  - email YDB `RUNNING`, deletion protection enabled, provisioned RCU `0`, hard
    serverless throttle `10 RCU/s`;
  - four accepted API Gateways `ACTIVE`, five intended triggers `ACTIVE`, six
    email Functions `ACTIVE`;
  - inbound desired-state plan reports `ready=21`, `operator=3`, no drift after
    recovery;
  - targeted email infrastructure/runtime suite: 45 passed;
  - Fly SQLite `PRAGMA quick_check=ok`; no fresh `joboutbox` failures after the
    reaper timestamp.
- post-recovery verification:
  - relay safe read HTTP 200 in 10/10 consecutive normal-DNS probes;
  - focus-connectivity YDB read HTTP 200 with `status=ready` in 5/5 probes;
  - IMAP collector invocations at 16:28 and 16:30 completed without error;
  - Fly `/healthz` HTTP 200 with `ready=true`, DB/schedulers/email workers `ok`,
    `issues=[]`, and Region Talk explicitly `disabled`.

## Prevention

Closure requires desired-state status checks, not resource-existence checks only.
