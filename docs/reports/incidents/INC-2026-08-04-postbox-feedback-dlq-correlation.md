# INC-2026-08-04 Postbox feedback DLQ correlation backlog

Status: open  
Severity: sev2  
Service: Yandex Postbox feedback / focus Auth email  
Opened: 2026-08-04  
Closed: —  
Owners: email delivery / focus Auth / production operations  
Related incidents: `INC-2026-07-30-focus-email-otp-false-success`, `INC-2026-08-03-yandex-cloud-reaper-service-suspension`  
Runbook: `docs/operations/postbox-feedback-dlq-recovery.md`

## Summary

The production monitor repeatedly reported `postbox_dlq_nonempty` with a stable DLQ count of 162. The backlog is not evidence of 162 outbound-send failures. It contains Postbox feedback events that the deployed consumer could not correlate to direct focus Auth sends.

The direct Auth hook stores accepted Postbox `MessageId` values in `personalization.focus_auth_delivery_attempt`. The feedback RPC correlated only against `email_control.email_outbox`, returned `correlation_pending`, and allowed the YDS trigger to exhaust its bounded retries into YMQ DLQ.

## Observed notifications

On 4 August 2026 the bot reported the same count at 12:42, 13:02, 13:22, 16:22 and 16:42 local message time:

```text
codes=postbox_dlq_nonempty
dlq=162 unknown=0 submitted_oldest_s=0
```

The `unknown` and `submitted` counters covered only the transactional outbox and therefore did not describe direct Auth sends. The in-memory 15-minute alert cooldown also treated the unchanged backlog as a fresh page after every cooldown window.

## Impact

- OTP sending is not blocked merely because feedback entered DLQ.
- Delivery projection for affected Auth mail is incomplete.
- Hard-bounce, complaint and rendering-failure handling may be delayed.
- Operational messages combine an Auth-capable DLQ count with outbox-only health counters.
- Repeated unchanged alerts obscure actual queue growth and recovery.
- Retained evidence can expire if the queue is not classified and replayed before its retention boundary.

## Root cause

1. Postbox outbound acceptance and Postbox feedback used two different persistence ledgers.
2. The provider-event RPC required an `email_control.email_outbox` row for every `MessageId`.
3. Direct focus Auth mail never created that outbox row.
4. Unit tests validated each half independently but did not cover direct Auth send → provider event → applied feedback → empty DLQ.
5. Monitor state was process-local and keyed only by alarm codes, not persisted queue count/delta.

## Corrective implementation

Branch `agent/postbox-auth-feedback-correlation-20260804` adds:

- one PII-free receipt registry shared by outbox and Auth;
- automatic registration when either ledger persists a Postbox receipt;
- authenticated one-time HMAC binding for Auth and audited legacy receipts;
- `email_record_postbox_event_v3` with a v2 compatibility wrapper;
- Auth feedback state and unified health counters;
- an explicit service-only legacy-registration boundary;
- persisted DLQ alert state, delta reporting, six-hour static reminders and recovery notification;
- rollback-only SQL and Python regression contracts;
- a production recovery runbook.

## Immediate mitigation

- Keep the DLQ intact.
- Do not bulk replay before the migration and legacy classification.
- Treat unchanged `dlq=162` notifications as one open incident, not repeated new incidents.
- Escalate immediately if the count grows, a complaint/bounce subset is identified, or retention evidence approaches expiry.

## Closure gates

- reviewed migration applied after backup and rollback-only contracts;
- exact reviewed Fly release deployed;
- post-ledger Auth and outbox canaries reach `delivered` without DLQ growth;
- all 162 retained messages classified by exact identity, event type and outcome;
- independently supported legacy receipts registered from a sanitized manifest;
- bounded replay applies/de-duplicates messages before deletion;
- DLQ zero or every retained blocker explicitly owned;
- `postbox_missing_correlation_count=0`;
- no unexplained unbound correlation;
- one recovery notification and no unchanged 15–20 minute alert storm;
- production evidence and immutable identities recorded here before status changes to closed.
