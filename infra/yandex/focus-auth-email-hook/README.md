# Focus Auth Send Email Hook

Serverless HTTP boundary for the Supabase **Send Email Hook**. Supabase remains
the only OTP/session issuer. The function verifies the raw Standard Webhooks
signature, correlates one opaque `attempt_id`, selects one mail provider, stores
the provider receipt, and returns `200 {}` only after accepted delivery.

## Routing policy

Routing is deterministic before network dispatch:

- every Auth recipient is checked by the exact versioned email HMAC against active
  `all`/`transactional` suppressions inside the same service-only RPC that reserves
  the attempt; an exact match stops before either provider request, while a real
  email change produces a different HMAC and is not blocked by the old user ID;
- a genuinely new user's first send → Yandex Cloud Postbox;
- an existing user, the second/later send, an allowlisted fixed test user, or an
  allowlisted fixed test mailbox → NotiSend with `payment=subscriber`;
- NotiSend admission is counted by unique Supabase user **within the current
  provider billing period** across Auth and recommendation sends; an already
  admitted user in that period keeps using NotiSend, while a new user above the
  shared 200-recipient ceiling is routed to Postbox before any provider request;
- until the operator reconciles the provider's real used-recipient counter and
  period end, or after that period expires, every new NotiSend candidate is
  routed to Postbox;
- no provider switch after a timeout/ambiguous result;
- the same accepted `attempt_id` returns success without another provider call;
- a same-id `started`, rejected or ambiguous attempt is never automatically
  resent. A user-initiated resend creates a new attempt after the Auth cooldown.

This is a narrow Auth-repeat/test exception to the general stream split. It
prevents fixed GitHub E2E identities and frequent operator retests from consuming
Postbox sends or creating unlimited Auth users. Normal recommendation sends keep
their existing NotiSend contract and consent/admission rules.

## Data boundary

Private Supabase tables:

- `personalization.focus_auth_delivery_attempt`: provider acceptance and
  direct/relay issue/verify outcomes;
- `personalization.focus_auth_method_attempt`: actual email/Yandex attempt and
  completion counts.
- `email_control.notisend_recipient_admission`: one PII-free row per unique
  NotiSend recipient and provider billing period, shared with recommendation
  sends. `recommendation_capacity.provider_used_count` is the latest real
  provider counter; admissions after that snapshot are added atomically.
- `email_control.suppression`: exact pseudonymous versioned email-HMAC evidence used
  by the direct Auth admission gate. The HMAC key never enters Supabase or logs.

Public receipt lookup returns only `accepted`, `pending_or_ambiguous`, `rejected`
or no row for a recent opaque UUID. It never returns email, OTP, token/hash, JWT,
provider message ID, IP or User-Agent. Function logs have the same PII-free rule.

## Build

```bash
infra/yandex/focus-auth-email-hook/build-function.sh
python -m pytest -q tests/test_focus_auth_email_hook.py
```

The deterministic ZIP is written under ignored `artifacts/codex/`.

## Provisioning contract

1. Apply `supabase/migrations/20260801222242_focus_auth_delivery_attempt_v1.sql`
   and `supabase/migrations/20260804190000_postbox_auth_feedback_correlation_v1.sql`
   before deploying a hook version that calls the batch admission/completion
   RPCs. The same atomic migration revokes the suppression-free v1 admission;
   apply it only after the production hook-disabled precondition below is
   reverified.
2. Create the `kenigevents-focus-auth-mailer` service account with only
   `postbox.sender`; attach it to the Function so `context.token` supplies the
   short-lived IAM token. In the Python runtime `context.token` is the
   authentication object (`access_token`, `expires_in`, `token_type`), not the
   token string itself. The provider request must use only its
   `access_token` value in `X-YaCloud-SubjectToken`; stringifying the complete
   object produces an invalid Postbox credential and a definitive provider
   rejection.
3. Provide secrets from Lockbox as Function secret environment variables:
   `SEND_EMAIL_HOOK_SECRET`, `EMAIL_ADDRESS_HMAC_KEY`,
   `PERSONALIZATION_SUPABASE_SECRET_KEY`,
   `NOTISEND_API_TOKEN`, and optional `FOCUS_AUTH_NOTISEND_EMAILS`.
4. Mount both `EMAIL_ADDRESS_HMAC_KEY` and
   `EMAIL_ADDRESS_HMAC_KEY_VERSION` from the exact Lockbox secret/version and
   keys recorded in `desired-state.json`; those references are shared with the
   feedback consumer. Set the remaining non-secret environment values listed
   there.
5. Build and deploy the Python 3.12 Function with a five-second timeout.
6. Render the exact Function/invoker IDs into `openapi.yaml` and deploy the
   one-path API Gateway.
7. Reconcile the real NotiSend billing-period counter as described below.
8. In Supabase Authentication → Hooks, enable **Send Email** HTTP Hook with that
   exact HTTPS endpoint and the same Standard Webhooks secret.
9. Run the Standard Webhooks signed fixture, provider seed canaries and the real
   external-mailbox GitHub Action before any wider onboarding rollout.

### Current production staging state (2026-08-02)

- migration `20260801222242` is present in the hosted migration history;
- Function `d4euk47p8gv7qmgrtib4` and API Gateway
  `d5d17smc4tutrt316fjo` are active, with request logging disabled and an
  invalid-signature smoke returning `401`;
- the Supabase Send Email Hook is intentionally **not enabled** yet, so the
  deployed boundary cannot receive Auth traffic;
- NotiSend capacity is `routing_ready=false` until the current provider-period
  used-recipient count and period end are reconciled. `provider_reported`,
  `occupied` and `available` are `null` before that first reconciliation; do not
  infer `0` from the empty local admission set;
- GitHub Environment `external-e2e` exists, requires review and allows only
  `main` in steady state. Its dedicated no-persistence Yandex Mail Trigger,
  WebSocket URL and fixed recipient secrets are configured. Until this hook is
  enabled, that external E2E intentionally verifies the existing Supabase
  custom SMTP/Postbox route rather than claiming NotiSend coverage.

This is an inactive staging state, not rollout evidence. Do not enable the Auth
hook merely because the Function and Gateway exist.

Before the canary, read from the NotiSend account the current tariff-period end
and the **actual number of unique recipients already used in that period**. Apply
that aggregate through the service-role-only RPC (example values only):

```sql
select public.focus_auth_reconcile_notisend_capacity_v1(
  'notisend-period-2026-08',
  '2026-09-01T00:00:00Z'::timestamptz,
  17
);
```

Do not guess zero. During the same period a later provider value must be at least
the previous provider baseline plus all local admissions after it; a stale/lower
dashboard value is rejected. Starting a new period is allowed only after the
recorded old period ends, and a period key cannot be reused. Between
reconciliations, Supabase adds each newly
admitted user once. `focus_auth_operator_summary_v1` reports
`provider_reported + admitted_after_reconcile = occupied`, `available`, period
end, and `routing_ready`. This keeps provider-list/dashboard work off the
five-second Auth path while preserving the real 200-user limit.

An explicit provider rejection/configuration failure before acceptance releases
only the new local reservation created by that attempt. A timeout or a 2xx
response without a verifiable provider receipt remains ambiguous and keeps the
slot reserved until the next provider-counter reconciliation; it is never
retried through Postbox.

For a PII-free operational bundle with actual email/Yandex outcomes,
direct/relay issue and verification routes, provider acceptance and the shared
NotiSend capacity:

```bash
set -a; source .env; set +a
python3 scripts/report_focus_auth_telemetry.py --hours 24
```

The ignored artifact contains `summary.json` and `CHATGPT_PROMPT.txt`; it has no
addresses, OTPs, tokens, IP/User-Agent values or provider message ids.

The hook replaces SMTP while enabled. Do not enable it until both provider paths,
ledger RPCs and the external mailbox test pass. Rollback is disabling the hook;
the prior custom SMTP configuration remains the fallback configuration, not a
runtime per-message retry.

Do not rotate `EMAIL_ADDRESS_HMAC_KEY` or its positive version while active
suppression/correlation rows or retained feedback still use the current pair.
Rotation requires a separately reviewed overlap migration and matching feedback
consumer keyring; a version mismatch intentionally fails closed.

## Fixed E2E identities

Use one stable mailbox by default. Put that mailbox in the secret
`FOCUS_AUTH_NOTISEND_EMAILS` and keep GitHub Actions in `fixed` mode. Use a tiny
pre-created persona set only when personalization scenarios need distinct stable
profiles. Unique addresses are reserved for an explicit fresh-user test and must
have their disposable Auth identity cleaned by an operator; they are not the
routine CI mode. Their PII-free NotiSend admission remains because deleting an
Auth row cannot restore provider capacity already spent in that billing period.

The exact fixed mailbox configured as `E2E_RECIPIENT_TEMPLATE` in the protected
GitHub Environment must be included in `FOCUS_AUTH_NOTISEND_EMAILS`. Routine
live runs consequently reuse one NotiSend recipient admission. A `{run_id}`
recipient template is an explicit capacity-consuming fresh-user test. In a new
billing period the stable mailbox occupies one slot again on its first send,
which matches the provider's accounting.
