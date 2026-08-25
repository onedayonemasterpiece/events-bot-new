# Google AI interaction-start accounting v3 — 2026-08-25

## Purpose

Migration `20260825120000_google_ai_interaction_started_v3.sql` adds early
technical evidence for streamed Gemini Interactions API calls without changing
the existing quota model or the already-applied v2 terminal finalizer.

The expected sequence is:

```text
reserve
-> google_ai_mark_sent
-> physical Gemini POST
-> interaction.created
-> google_ai_mark_interaction_started_v1
-> interaction.completed/error/disconnect
-> google_ai_finalize_interaction_v2
```

The new RPC stores the provider interaction ID while the request is still
non-terminal. This allows operators to identify an accepted provider request if
the SSE connection later drops.

## Safety properties

`google_ai_mark_interaction_started_v1`:

- accepts only an exact existing request/attempt;
- requires the attempt to have been marked sent;
- rejects terminal attempts;
- accepts only `created` or `in_progress` as the early status;
- bounds and validates the interaction ID;
- is idempotent for the same interaction ID;
- raises a reconciliation conflict for a different interaction ID;
- changes no RPM, TPM, RPD, usage, reserve, terminal state, or completion time;
- stores no API-key secret.

The capability readback becomes:

```text
interaction_accounting=google_ai_interaction_usage_v3
interaction_started_supported=true
interaction_started_rpc=google_ai_mark_interaction_started_v1
```

All existing contract, bucket, quota-scope, and unsent-release markers remain
unchanged.

## Pre-apply readback

Run against the dedicated shared limiter ledger only. Do not print credentials
or raw key metadata.

```sql
select google_ai_limiter_capabilities();

select model, rpm, tpm, rpd, tpm_reserve_extra
from google_ai_model_limits
where model in ('gemini-3.6-flash', 'gemini-3.7-flash')
order by model;
```

Expected before v3:

```text
interaction_accounting=google_ai_interaction_usage_v2
interaction_started_supported absent
```

## Apply

Apply the exact migration file from the reviewed CI-green commit. The migration
is transactional and idempotent.

## Post-apply readback

```sql
select google_ai_limiter_capabilities();

select routine_name
from information_schema.routines
where routine_schema = 'public'
  and routine_name = 'google_ai_mark_interaction_started_v1';
```

Expected:

```text
limiter_contract=google_ai_project_model_atomic_v1
bucket_strategy=rolling_60s_pacific_day_v2
quota_dimension=quota_scope/model
lock_dimension=quota_scope/model
quota_scope_enforced=true
interaction_accounting=google_ai_interaction_usage_v3
unsent_release_supported=true
interaction_started_supported=true
interaction_started_rpc=google_ai_mark_interaction_started_v1
```

## Transactional dry-run

A dry-run may be performed in a transaction against a disposable synthetic sent
attempt. Roll back the transaction after verifying:

1. the first call stores the early ID;
2. the same call returns `idempotent=true`;
3. a different ID raises
   `interaction_started_id_conflict_reconciliation_required`;
4. usage counters and reserved TPM do not change;
5. `finalized_at` and `completed_at` remain null.

Do not use a production request UID for this proof.

## Rollback

Operational rollback of the YouTube feature does not require schema rollback.
Disable the feature and remove its OAuth scope while retaining the RPC and audit
evidence.

A schema rollback, if explicitly required before any v3 client is deployed, is:

```sql
begin;
revoke all on function google_ai_mark_interaction_started_v1(uuid, int, text, text)
  from public, service_role;
drop function if exists google_ai_mark_interaction_started_v1(uuid, int, text, text);
-- Restore the exact v2 google_ai_limiter_capabilities() body from the reviewed
-- v2 migration. Keep the additive columns unless a separate retention decision
-- explicitly authorizes their removal.
commit;
```

Never delete sent attempts, interaction IDs, or terminal audit rows as part of
feature rollback.
