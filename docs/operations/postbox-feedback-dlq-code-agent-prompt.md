# Code-agent prompt: production closeout for Postbox feedback DLQ

Use this prompt only after the implementation PR from branch `agent/postbox-auth-feedback-correlation-20260804` is reviewed and its CI is green.

---

You are closing `INC-2026-08-04-postbox-feedback-dlq-correlation` in `onedayonemasterpiece/events-bot-new`.

Read first:

- `docs/operations/postbox-feedback-dlq-recovery.md`
- `docs/reports/incidents/INC-2026-08-04-postbox-feedback-dlq-correlation.md`
- `supabase/migrations/20260804190000_postbox_auth_feedback_correlation_v1.sql`
- `supabase/tests/email_postbox_auth_feedback_contract.sql`
- `serverless/email_postbox_events/index.py`
- `email_control/scheduler.py`
- `docs/operations/email-delivery.md`
- `docs/reports/incidents/INC-2026-07-30-focus-email-otp-false-success.md`
- `docs/README.md`
- `docs/routes.yml`
- `docs/reports/incidents/README.md`

## Non-negotiable safety boundaries

- Do not purge the Postbox DLQ.
- Do not bulk-delete or blindly replay messages.
- Do not log or commit recipient email, OTP, body, subject, token, provider credentials, raw Supabase keys or full DLQ payloads.
- Do not register a legacy receipt merely because it appears in an incoming event. Build independent evidence and hash the sanitized manifest first.
- Do not retry an ambiguous provider dispatch through another provider.
- Do not enable global, transactional or recommendation outbound switches unless the existing product release plan separately authorizes it.
- Do not touch Region Talk, the obsolete KGD80 Postbox database, unrelated Yandex folders or unrelated queues.
- All production mutations require exact before/after evidence and an explicit rollback command.
- Do not claim that a stored suppression is enforced by the direct Auth hook unless a pre-network admission test proves it.
- Do not approximate suppression by `user_id` when the protected identity is an email HMAC; account deletion, email change and profile switching make that unsafe.

## Required work

### A. Review and validate the implementation

1. Rebase/refresh the branch non-force from current `origin/main` without dropping changes.
2. Inspect the migration for:
   - correct trigger ordering;
   - unique cross-source `MessageId` behavior;
   - no plaintext recipient persistence;
   - correct Auth/outbox/legacy state transitions;
   - exact duplicate and conflict semantics;
   - suppression scopes;
   - service-role-only grants.
3. Run the full email Python contracts and CI.
4. Parse the migration with a PostgreSQL parser.
5. In an isolated PostgreSQL/Supabase-compatible test database, apply the prerequisite migrations plus the new migration and execute `supabase/tests/email_postbox_auth_feedback_contract.sql` in rollback mode.
6. Fix any defect found; do not weaken assertions to obtain green checks.
7. Complete canonical documentation routing before merge:
   - add the runbook under `sections.operations` in `docs/routes.yml`;
   - add the new incident under `sections.reports` in `docs/routes.yml`;
   - add the incident to `docs/reports/incidents/README.md` as an active regression contract;
   - link the runbook and incident from the relevant Postbox section of `docs/operations/email-delivery.md` without duplicating the runbook;
   - keep the `docs/README.md` route added by the implementation branch;
   - parse `docs/routes.yml` and verify every changed relative Markdown link.
8. Audit the suppression boundary separately. The implementation records authenticated `hard_bounce`, `complaint` and `unsubscribe` suppressions. That alone does not prove the direct focus Auth hook checks them before a future provider network call. Either:
   - implement an explicit PII-free, versioned recipient-HMAC admission boundary for direct Auth and cover first-send/repeat/email-change cases without exposing the HMAC key unnecessarily; or
   - record a named unresolved blocker and keep the incident open. Never infer email suppression from user ID alone.

### B. Prepare production migration evidence

1. Capture exact repository SHA and target migration list.
2. Take and verify a restorable logical backup of the personalization Supabase project.
3. Use the session pooler on port 5432; do not use the transaction pooler for migrations.
4. Record pre-migration results for:
   - accepted Postbox outbox rows by state;
   - accepted Postbox Auth attempts by state and age;
   - duplicate/non-null provider `MessageId` checks;
   - current runtime switches;
   - current DLQ visible/in-flight count;
   - current Postbox destination, YDS trigger, Function and YDB status.
5. Dry-run the migration and SQL contract. Apply only if all checks are consistent.
6. After apply, prove:
   - every post-ledger accepted Postbox send has exactly one correlation;
   - `postbox_missing_correlation_count=0`;
   - no cross-source receipt collision;
   - browser roles cannot execute v3 or legacy registration.

### C. Deploy the reviewed Fly monitor change

1. Deploy the exact reviewed SHA to `events-bot-new-wngqia`.
2. Record Fly release and in-container SHA.
3. Verify `/healthz`, scheduler presence and the `email_outbox_monitor` job.
4. Verify `/data/email-postbox-monitor-state.json` is PII-free and mode `0600` where supported.
5. Prove an unchanged test backlog does not notify every 15–20 minutes, a count/code change notifies immediately, and clearing the test alarm emits one recovery message.
6. Do not change queue retention or Postbox destination configuration in this step.

### D. Audit the exact 162-message DLQ backlog

The current count is approximate until messages are inventoried. Build a sanitized exact inventory without deleting messages.

1. Determine the exact Yandex YMQ DLQ envelope schema from one controlled message. Redact before saving evidence.
2. Implement or use a bounded reader that accounts for YMQ visibility semantics and deduplicates by queue message ID plus Postbox `eventId`/`messageId`.
3. Produce a sanitized manifest containing only:
   - stable hashes/aliases;
   - event type;
   - provider event time bucket/exact UTC time where safe;
   - source classification;
   - correlation result;
   - stable error code;
   - evidence SHA-256.
4. Report exact totals for:
   - queue messages;
   - unique Postbox event IDs;
   - unique Postbox message IDs;
   - each event type;
   - outbox-correlated;
   - post-ledger Auth-correlated;
   - independently provable legacy Auth;
   - malformed/conflicting/unsupported/unproven.
5. The classification totals must reconcile exactly to the inventory. Preserve raw payloads only in the existing private provider boundary; never upload them to GitHub Actions artifacts.

### E. Register legacy receipts safely

1. Use retained Supabase/Auth/Postbox evidence to build the legacy mapping. Timing alone is insufficient if multiple receipts could match.
2. Hash the final sanitized manifest.
3. For each proven legacy receipt, call `email_register_legacy_postbox_auth_v1(message_id, manifest_sha256, sent_at)`.
4. Verify idempotency by repeating a safe sample registration.
5. Leave unproven receipts unregistered and explicitly owned; do not guess.

### F. Replay in bounded batches

1. Start with at most ten DLQ messages.
2. Reconstruct the exact YDS Function input; do not bypass its schema/identity/config/from-domain/HMAC validation.
3. Delete a queue receipt only after:
   - RPC returned `applied`, or
   - RPC returned `duplicate` and stored event fields match exactly.
4. Leave `correlation_pending`, conflict, validation or transport errors in DLQ and stop the current batch.
5. After each batch record:
   - queue delta;
   - provider-event applied/duplicate delta;
   - correlation bound/unbound delta;
   - Auth/outbox state delta;
   - suppression delta by reason;
   - sanitized error counts.
6. Require two clean ten-message batches before increasing the batch size.
7. Never use queue purge.

### G. Terminal canaries and closure

Run controlled production canaries:

- new first-time Auth routed to Postbox: one provider acceptance, authenticated `Send`, authenticated `Delivery`, Auth feedback `delivered`, no DLQ growth;
- ordinary transactional outbox: submitted → delivered;
- exact duplicate event: `duplicate`, no second transition;
- controlled hard-bounce/complaint fixtures: correct suppression, followed by the established fixture cleanup procedure;
- where direct Auth suppression enforcement is implemented, a repeated send to the suppressed exact email identity is rejected before any provider network call, while an explicit legitimate email change is not blocked by unrelated user-ID history;
- unchanged monitor state: no alert storm;
- alarm clear: one recovery notification.

Close the incident only when:

- the exact retained inventory is fully reconciled;
- DLQ is zero, or every remaining item has an explicit evidence blocker and owner before retention expiry;
- `postbox_missing_correlation_count=0`;
- no unexplained unbound correlation remains;
- direct Auth suppression is either proven pre-network or retained as an explicit blocker with the incident open;
- all production identities, migration versions, Function version, Fly release, test commands and evidence hashes are committed to the incident document;
- the final branch is merged to `main` and the deployed SHA is reachable from `main`.

## Required final response

Return:

- root cause confirmed or corrected;
- files changed and commits;
- PR and merge SHA;
- Supabase migration/backup evidence;
- Fly release and deployed SHA;
- exact DLQ inventory and event-type histogram;
- legacy registration totals and manifest hash;
- replay totals: applied, duplicate, pending, conflict/error, remaining;
- suppression changes by reason and direct-Auth pre-network enforcement status;
- canary results;
- monitor delta/recovery evidence;
- documentation routing/link checks;
- remaining blockers, with no claim of closure unless every closure gate passes.

---