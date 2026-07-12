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

The 2026-07-12 live disabled-destination probe succeeded. A subsequent GET returned
all nine event types (`SEND`, `DELIVERY`, `BOUNCE`, `SUBSCRIPTION`, `OPEN`, `CLICK`,
`DELIVERY_DELAY`, `COMPLAINT`, `RENDERING_FAILURE`), so this provider gate is
closed despite the incomplete create-method enumeration.

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

## Live release evidence (2026-07-12)

- Supabase migration `20260712072912` is applied after a verified 3.1 MB custom
  backup; both rollback-only email SQL contracts pass and V1 is no longer executable
  by `service_role`.
- YDB `etn97gnogglv3fgntadv` and topic `kenigevents-postbox-events` match the desired
  zero-provisioned/request-unit limits. Use the database's returned Kinesis endpoint;
  its internal path component is not the resource folder ID and must not be guessed.
- Function `d4enjcfg3h6nep4ij4fh` tag `prod` points to the Python 3.12 version built
  from `origin/main@140c9e15`; trigger `a1svvdcbe8pdoc8cv74a` is active with batch
  size 1 byte and five retries at 30 seconds. Both dedicated SAs have zero static keys.
- One real canary produced authenticated/verified `accepted` + `delivered` provider
  evidence. Exact duplicates were no-ops; the controlled retry test produced six
  invocations, one DLQ record, successful replay, duplicate replay and an empty DLQ.
- Structured Cloud Logging contains only request/version IDs, event hashes, bounded
  outcomes and stable error codes. The temporary user/outboxes and synthetic
  suppressions/events were removed; only the real Send/Delivery evidence remains.
- Destination disable/enable rollback passed and the nine-type destination is enabled.
  Fly release `1627` deploys `origin/main@ca2b24f9` with the transactional-only
  worker and the PII-free Supabase/YMQ monitor. The worker signs short-lived IAM
  tokens with the dedicated sender key; the monitor uses a separate `ymq.reader`
  key. Both credentials are deletion-protected in Lockbox and deployed as Fly
  secrets.
- A worker-owned canary was accepted once by Postbox and finished `delivered` after
  authenticated/verified `Send` and `Delivery` events. An initially incorrect HMAC
  on the temporary canary identity exercised five-item DLQ detection; after fixing
  the fixture HMAC, controlled replay applied once, duplicates were no-ops, the
  automatic YDS trigger probe applied, and the DLQ returned to zero.
- Application switches remain off/dry-run-only. Event-specific enqueue producers
  are still pending, and the fresh-domain/self-domain SpaceWeb canary was initially
  filed as Spam even though SPF, both DKIM signatures and DMARC passed. It was moved
  to Inbox as an explicit mailbox-training action; this is not evidence of broad
  inbox placement.
