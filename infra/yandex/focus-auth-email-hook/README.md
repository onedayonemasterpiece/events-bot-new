# Focus Auth Send Email Hook

Serverless HTTP boundary for the Supabase **Send Email Hook**. Supabase remains
the only OTP/session issuer. The function verifies the raw Standard Webhooks
signature, correlates one opaque `attempt_id`, selects one mail provider, stores
the provider receipt, and returns `200 {}` only after accepted delivery.

## Routing policy

Routing is deterministic before network dispatch:

- a genuinely new user's first send → Yandex Cloud Postbox;
- an existing user, the second/later send, an allowlisted fixed test user, or an
  allowlisted fixed test mailbox → paid NotiSend with `payment=subscriber`;
- NotiSend admission is counted by unique Supabase user across Auth and
  recommendation sends; an already admitted user keeps using NotiSend, while a
  new user above the shared 200-recipient ceiling is routed to Postbox before
  any provider request;
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
  NotiSend recipient, shared with recommendation admission. The existing
  `email_control.recommendation_capacity.external_reserved_count` value reserves
  provider contacts not represented by a Supabase user.

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

1. Apply `supabase/migrations/20260801222242_focus_auth_delivery_attempt_v1.sql`.
2. Create the `kenigevents-focus-auth-mailer` service account with only
   `postbox.sender`; attach it to the Function so `context.token` supplies the
   short-lived IAM token.
3. Provide secrets from Lockbox as Function secret environment variables:
   `SEND_EMAIL_HOOK_SECRET`, `PERSONALIZATION_SUPABASE_SECRET_KEY`,
   `NOTISEND_API_TOKEN`, and optional `FOCUS_AUTH_NOTISEND_EMAILS`.
4. Set non-secret environment values listed in `desired-state.json`.
5. Build and deploy the Python 3.12 Function with a five-second timeout.
6. Render the exact Function/invoker IDs into `openapi.yaml` and deploy the
   one-path API Gateway.
7. In Supabase Authentication → Hooks, enable **Send Email** HTTP Hook with that
   exact HTTPS endpoint and the same Standard Webhooks secret.
8. Run the Standard Webhooks signed fixture, provider seed canaries and the real
   external-mailbox GitHub Action before any wider onboarding rollout.

Before the canary, reconcile the provider contact total with:

```sql
select
  a.admitted_count + rc.external_reserved_count as occupied,
  rc.capacity,
  rc.capacity - a.admitted_count - rc.external_reserved_count as available
from email_control.recommendation_capacity rc
cross join (
  select count(*)::integer as admitted_count
  from email_control.notisend_recipient_admission
) a
where rc.capacity_key = 'launch'
;
```

Set `external_reserved_count` to the number of seed/service contacts present in
NotiSend but absent from the admission table. This keeps the effective count
conservative without putting provider-list calls on the five-second Auth path.

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

## Fixed E2E identities

Use one stable mailbox by default. Put that mailbox in the secret
`FOCUS_AUTH_NOTISEND_EMAILS` and keep GitHub Actions in `fixed` mode. Use a tiny
pre-created persona set only when personalization scenarios need distinct stable
profiles. Unique addresses are reserved for an explicit fresh-user test and must
have their disposable Auth identity cleaned by an operator; they are not the
routine CI mode. Their PII-free NotiSend admission remains because deleting an
Auth row cannot restore provider capacity already spent.

The exact fixed mailbox configured as `E2E_RECIPIENT_TEMPLATE` in the protected
GitHub Environment must be included in `FOCUS_AUTH_NOTISEND_EMAILS`. Routine
live runs consequently reuse one NotiSend recipient admission. A `{run_id}`
recipient template is an explicit capacity-consuming fresh-user test.
