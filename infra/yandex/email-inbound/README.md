# Yandex inbound email desired state

This directory describes the isolated infrastructure for the automated copy of
`info@kenigevents.ru`. It does not change SpaceWeb mailbox purpose, DNS, Postbox,
NotiSend, Supabase schema, or Fly.

## Safety model

- Use a dedicated Yandex folder `kenigevents-email-prod`; do not place a YMQ
  trigger worker with broad documented permissions beside the existing site/CDN.
- The reconciler is plan-only unless both `--apply` and the exact confirmation
  token are supplied.
- Apply is staged and non-destructive. It can create only the folder, service
  account shells, KMS key, private bucket shell and empty function shells.
- It never creates/deletes/rotates static access keys, puts Lockbox payloads,
  changes IAM, creates queues, creates function versions, activates triggers or
  deletes resources.
- Secretful and traffic-bearing steps remain explicit operator gates.
- Every command uses an explicit cloud/folder ID; the active CLI default folder is
  never a source of truth.

## Plan

```bash
export EMAIL_INBOUND_YC_CLOUD_ID='<cloud id>'
export EMAIL_INBOUND_BUCKET_NAME='<globally unique private bucket>'
python3 infra/yandex/email-inbound/reconcile.py
```

The plan uses `/home/dev/yandex-cloud/bin/yc` by default and only performs list/get
calls. Override with `YC_CLI` or `--yc`. To review a saved redacted inventory
without accessing Yandex:

```bash
python3 infra/yandex/email-inbound/reconcile.py \
  --inventory-file artifacts/codex/email-inbound/inventory.json \
  --json
```

The installed YC CLI has no YMQ data-plane group. Queue inventory in an offline
file is an array of `{ "name": "..." }` rows. Without it, queues remain
`operator` instead of being guessed absent.

## Apply safe shells

Review the plan and current `git status`, then:

```bash
python3 infra/yandex/email-inbound/reconcile.py \
  --apply \
  --confirm APPLY-kenigevents-email-prod
```

If the folder is absent, the first apply creates only the folder. Re-run plan and
apply to create safe resource shells. An existing Function or Trigger is `ready`
only when its runtime status is `ACTIVE`; `PAUSED`, `STOPPED` and unknown states
are reported as `drift`. Recovery remains an explicit operator action because a
blind resume can release a retained backlog. Other existing resource names remain
idempotent. The reconciler never resumes, updates or deletes an existing resource.

After an organization/billing-account transfer, run the plan even when no
infrastructure commit changed. The transfer acceptance must inventory every
KenigEvents folder, classify every non-active resource, verify outbound Supabase
send gates before resuming email automation, and then exercise safe read-only
gateway probes. Do not use this reconciler to start Region Talk or the obsolete
KGD80 Postbox database.

## Operator-gated completion order

1. Configure the bucket with all anonymous access disabled, KMS encryption and a
   30-day expiration lifecycle. Verify the flags via control plane and an
   unauthenticated GET/list denial.
2. Create the three **standard** queues:
   - `kenigevents-email-processing`, retention 14 days, visibility timeout greater
     than delivery timeout, redrive after five receives;
   - `kenigevents-email-intake-dlq`, retention 14 days;
   - `kenigevents-email-processing-dlq`, retention 14 days.
3. Add resource-specific IAM bindings described in
   `docs/operations/email-delivery.md`. First prove `ymq.reader` plus
   resource-level `functions.functionInvoker` for the queue trigger. If Yandex
   still enforces primitive `editor`, grant it only in this isolated folder.
4. Create exactly one static access key for `email-intake-runtime`, record its ID,
   put it directly into Lockbox without terminal output, verify the secret, then
   delete the `0600` temporary file. A status/preflight command must never create
   a key.
5. Create separate random idempotency and adapter-signing secrets. Do not reuse
   mailbox, AWS, Postbox or NotiSend secrets.
6. Build the function ZIPs and create untagged `python312` versions. Intake uses
   256 MB / 15 seconds; delivery and adapter use 128 MB; the IMAP collector uses
   256 MB / 30 seconds. Inject Lockbox values
   with `--secret`, never plaintext `--environment`.
7. Invoke each version directly using `tests/fixtures/email_inbound/`.
8. Move the `prod` tag to the tested versions.
9. Create the YMQ trigger with batch size `1`, cutoff `1s`, invoking delivery tag
   `prod`.
10. Create the Mail Trigger with batch size `1`, cutoff `1s`, five retries at 30s,
    the private attachment bucket, intake DLQ, and intake tag `prod`.
11. Send a direct canary from `info@kgd80.ru` to the generated trigger address.
12. Keep the SpaceWeb `info@` purpose as `Mail`: the panel's `Forwarding` purpose is
    mutually exclusive and would violate retention. Bootstrap the read-only IMAP
    collector at the current UID, then enable its two-minute timer.
13. Send a real canary from `info@kgd80.ru` to `info@kenigevents.ru`; prove the
    SpaceWeb UID remains present and unseen while the metadata receipt reaches
    Supabase and all processing/DLQ depths return to zero.

Do not reuse one DLQ for both triggers: Mail Trigger failure payloads and processing
pointer payloads have different replay contracts.

## Rollback

1. Suspend the IMAP timer (there is no SpaceWeb forwarding to undo).
2. Suspend the Mail Trigger.
3. Suspend the YMQ trigger.
4. Remove public invocation from the HMAC adapter if its key is suspect.
5. Move function `prod` tags back if needed.
6. Keep the SpaceWeb mailbox and queued/private retained data intact.

Resource deletion, queue purge and access-key deletion are never rollback steps.
