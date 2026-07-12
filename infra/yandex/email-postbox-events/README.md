# Yandex Postbox event infrastructure

Canonical desired state is in [`desired-state.json`](desired-state.json). This
lane is isolated from inbound mail resources and uses:

- serverless YDB with zero provisioned RCU and a request-unit YDS topic;
- a dedicated trigger SA and a separate runtime SA;
- two KMS-backed, deletion-protected Lockbox secrets;
- a Python 3.12 Function pinned by the `prod` tag;
- a YDS trigger with five bounded retries and a private 14-day YMQ DLQ;
- a Postbox configuration destination created disabled and enabled only after the
  RPC, function, duplicate, failure, DLQ and replay checks pass.

The full reviewed provisioning/rollback command record is
`.codex/lanes/postbox-events-release/infra-audit/RESULTS.md`. It is evidence, not
an executable one-shot script: resource IDs, version IDs and current CLI contracts
must be verified at every step. Never create the destination before the migration
and consumer are live.

## Provider event-type gate

The current Postbox notification schema documents `Complaint` and
`Rendering Failure`, but the current create-destination API reference lists only
`SEND`, `BOUNCE`, `SUBSCRIPTION`, `DELIVERY`, `DELIVERY_DELAY`, `OPEN` and `CLICK`.
Production transactional sending must stay disabled if a disabled destination
probe rejects `COMPLAINT` or `RENDERING_FAILURE`; do not silently deploy a reduced
subscription and claim complete suppression coverage.

## Safe order

1. Merge tested code to `origin/main`; take a live Supabase backup.
2. Dry-run and apply the V2 RPC migration; execute SQL contracts.
3. Provision serverless YDB/YDS, isolated SAs, secrets, log group and DLQ.
4. Build the deterministic ZIP and create an untagged Function version.
5. Invoke valid, duplicate, mismatch and forced-failure fixtures; move `prod` only
   after success.
6. Create the YDS trigger and exercise retry -> DLQ -> replay.
7. Create the Postbox destination disabled with all required event types. Enable
   it only if the provider accepts the complete safety set.
8. Send one controlled canary, verify Send + Delivery, empty DLQ and DB evidence;
   immediately test destination-disable rollback.

Application switches remain `global=false`, `transactional=false`,
`dry_run_only=true` throughout this infrastructure release. NotiSend is unrelated
and remains disabled.
