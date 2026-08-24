# Google AI model-limit and Interactions accounting rollout — 2026-08-24

Canonical owner: `events-bot-new` dedicated Google AI quota ledger.

Migration: `supabase/migrations/20260824170000_google_ai_youtube_interaction_v2.sql`.

This is additive and idempotent. It adds thought-token fields, `google_ai_finalize_interaction_v2`, a provably-unsent release RPC, exact v2 capability markers, and positive finite owner-supplied model limits. It does not contain secrets and does not edit an already-applied bootstrap migration.

The owner label “Gemini Embedding 2” is canonicalized to the current official Gemini API endpoint ID `gemini-embedding-2-preview`. The display-like alias `gemini-embedding-2` is deliberately not inserted as a second row.

## Scope-ownership blocker

`google_ai_model_limits` is global by model. Before apply, enumerate the distinct `quota_scope` values for every candidate key in `GOOGLE_AI_NORMAL_KEY_ENVS` and establish that the owner-supplied matrix applies to all of them. Several ENV keys in one `quota_scope` are one project quota, not several quotas.

If scopes have different tiers, do not apply/use a false global value and do not send production traffic. The minimal follow-up is a scoped override table keyed by `(quota_scope, model)` with global rows used only as an explicit fallback.

## Mandatory dry-run/readback

Run with the existing service-role-safe database mechanism. Do not print service keys or ENV values.

```sql
BEGIN;

SELECT google_ai_limiter_capabilities() AS capabilities_before;

SELECT provider, quota_scope, count(*) AS registered_keys
FROM google_ai_api_keys
WHERE is_active
GROUP BY provider, quota_scope
ORDER BY provider, quota_scope;

SELECT model, rpm, tpm, rpd, tpm_reserve_extra
FROM google_ai_model_limits
WHERE model IN (
  'gemini-3.1-flash-lite', 'gemini-3.5-flash-lite',
  'gemma-4-31b-it', 'gemini-2.5-flash', 'gemini-2.5-flash-lite',
  'gemini-2.5-flash-preview-tts', 'gemini-3-flash-preview',
  'gemini-3.1-flash-tts-preview', 'gemini-3.5-flash',
  'gemini-3.6-flash', 'gemini-3.7-flash',
  'gemini-embedding-001', 'gemini-embedding-2-preview',
  'gemini-robotics-er-1.6-preview', 'gemini-robotics-er-2-preview',
  'gemma-4-26b-a4b-it'
)
ORDER BY model;

ROLLBACK;
```

Before apply, save the model-limit query result as redacted operational evidence. It is the rollback snapshot. Do not save key metadata beyond redacted aliases/scopes.

## Apply and exact readback

Apply only the canonical migration file from the reviewed commit. Do not recreate SQL manually on the server.

Then verify:

```sql
SELECT google_ai_limiter_capabilities();

SELECT model, rpm, tpm, rpd, tpm_reserve_extra
FROM google_ai_model_limits
WHERE model IN ('gemini-3.6-flash', 'gemini-3.7-flash')
ORDER BY model;
```

Both critical rows must be exactly `RPM=5`, `TPM=250000`, `RPD=20`; `tpm_reserve_extra` is not asserted to a new value because the migration preserves it.

The capability object must include:

- `limiter_contract=google_ai_project_model_atomic_v1`;
- `bucket_strategy=rolling_60s_pacific_day_v2`;
- `quota_dimension=quota_scope/model`;
- `lock_dimension=quota_scope/model`;
- `quota_scope_enforced=true`;
- `interaction_accounting=google_ai_interaction_usage_v2`;
- `unsent_release_supported=true`.

## Expected matrix

| model | RPM | TPM | RPD |
|---|---:|---:|---:|
| gemini-3.1-flash-lite | 15 | 250000 | 500 |
| gemini-3.5-flash-lite | 15 | 250000 | 500 |
| gemma-4-31b-it | 30 | 16000 | 14400 |
| gemini-2.5-flash | 5 | 250000 | 20 |
| gemini-2.5-flash-lite | 10 | 250000 | 20 |
| gemini-2.5-flash-preview-tts | 3 | 10000 | 10 |
| gemini-3-flash-preview | 5 | 250000 | 20 |
| gemini-3.1-flash-tts-preview | 3 | 10000 | 10 |
| gemini-3.5-flash | 5 | 250000 | 20 |
| gemini-3.6-flash | 5 | 250000 | 20 |
| gemini-3.7-flash | 5 | 250000 | 20 |
| gemini-embedding-001 | 100 | 30000 | 1000 |
| gemini-embedding-2-preview | 100 | 30000 | 1000 |
| gemini-robotics-er-1.6-preview | 5 | 250000 | 20 |
| gemini-robotics-er-2-preview | 5 | 250000 | 20 |
| gemma-4-26b-a4b-it | 30 | 16000 | 14400 |

`antigravity-preview-05-2026` is intentionally absent: it is an Interactions agent, not a canonical model-limit row in this update. Live API, grounding, video-hours, and other tool-specific quotas are also absent. No zero-valued rows are inserted.

## Rollback

1. Disable every new YouTube caller first. Do not send new provider POSTs while changing ledger capability/accounting.
2. Restore only the touched model rows from the mandatory pre-apply snapshot. Delete a touched row only when the snapshot proves it did not previously exist.
3. Do not change `tpm_reserve_extra` during rollback unless the snapshot contains its exact prior value.
4. Do not drop `usage_thought_tokens` columns or v2 functions while any deployed caller advertises/uses `google_ai_interaction_usage_v2`.
5. Restore the prior capability function only after every v2 caller is disabled or rolled back.
6. Never delete request/attempt audit rows for a provider attempt that was marked sent.

A capability mismatch is intentionally fail-closed: it is safer for callers to stop than to send unaccounted traffic.
