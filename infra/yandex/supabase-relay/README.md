# Stateless Supabase transport relay

This API Gateway is a fixed-upstream transport alternative for the static
KenigEvents browser. Supabase remains the sole owner of Auth, sessions, RLS,
private feedback files and durable product data. The gateway has no service
account, database, secret key, request/body logging or application logic.

The browser first performs safe health reads over the direct and relay routes,
then sends a non-idempotent operation exactly once over the selected route.
Only safe reads may retry once on the alternate route. Supabase's own Auth
limits and per-user RPC quotas remain authoritative.

## Fail-closed route contract

There are no `/auth/v1/{path+}`, `/rest/v1/{path+}` or
`/functions/v1/{path+}` wildcards. The specification lists every production
method/path pair used by the static client:

- Auth health/settings, OAuth authorize/callback, PKCE or refresh token
  exchange, OTP send/verify/resend, logout, current user and identity linking;
- two narrow REST reads and thirteen named RPCs, including the side-effect-free
  nonce capability probe, participant registration and idempotent focus feedback;
- `POST /functions/v1/event-search` plus its separate lightweight nonce probe;
- authenticated upload/delete only inside private bucket `focus-feedback`.

All other Auth endpoints (especially `/auth/v1/admin/**`), unknown tables/RPCs,
unknown Edge Functions, Realtime and every other Storage bucket fail at API
Gateway before reaching Supabase. The feedback Storage exception forwards the
browser JWT; bucket RLS remains the authorization boundary. No public object
read is exposed through the relay.

The integration strips `Cookie`, `Host`, `Forwarded`, `X-Forwarded-*` and
`X-Real-IP` before the fixed upstream request. It also replaces the incoming
`Origin` with `https://kenigevents.ru`: Supabase reflects that header, so
forwarding an arbitrary browser origin would accidentally make the otherwise
narrow relay readable cross-origin. This prevents a caller from spoofing an
address, host or readable browser origin through the relay. `apikey` and
`Authorization` are forwarded because Supabase publishable-key checks and
JWT/RLS enforcement need them.

## Rate limiting and request size

Yandex's `x-yc-apigateway-rate-limit` extension is deprecated and discontinued
in favor of Smart Web Security. No reviewed KenigEvents Smart Web Security
profile currently exists in the target folder, so the desired spec deliberately
does **not** attach an invented profile or a low gateway-global limit. A global
limit would group users behind the relay and let one attacker starve legitimate
traffic.

Current Supabase Auth settings observed read-only on 2026-07-31 are retained:
OTP `200/hour`, email send `200/hour`, verify `30/hour`, refresh `150/hour`, OTP
expiry `600s`; CAPTCHA is not enabled. Before public growth, stage a Smart Web
Security Advanced Rate Limiter profile whose key does not collapse every relay
user into one shared upstream address, then validate browser and attack cases
before attaching its `securityProfileId`. CAPTCHA additionally requires a
provider credential and the corresponding client challenge UX; it is not safe
to turn on as an isolated control-plane toggle.

API Gateway does not provide a safe per-operation body-size cap in this fixed
HTTP relay contract. The expensive `event-search` Edge Function therefore
rejects bodies above 16 KiB before parsing. Database RPCs retain typed payload,
array, metadata, per-user and retention caps. Feedback screenshot file size/type
limits and private-bucket RLS are downstream Storage contracts, not a reason to
add a stateful proxy.

Official contracts used:

- Yandex API Gateway Smart Web Security extension:
  <https://yandex.cloud/en/docs/api-gateway/concepts/extensions/sws>
- deprecated API Gateway rate-limit extension:
  <https://yandex.cloud/en/docs/api-gateway/concepts/extensions/rate-limit>
- Supabase Edge Function authentication:
  <https://supabase.com/docs/guides/functions/auth>
- Supabase RLS:
  <https://supabase.com/docs/guides/database/postgres/row-level-security>

## Deploy (integrator only)

This lane changes desired state and tests only; it does not update the live
gateway or apply Supabase migrations.

```bash
export PATH=/home/dev/yandex-cloud/bin:$PATH
FOLDER=b1g5tck18cgqtjb7rn3s
yc serverless api-gateway update d5di5tr5vvck7ld8vbcn \
  --folder-id "$FOLDER" \
  --spec infra/yandex/supabase-relay/openapi.yaml \
  --no-logging
```

Before any update, the integrator must review the diff, confirm the gateway has
no service account, and apply the database/Edge Function changes in the same
release so the narrowed RPC surface does not precede its internal replacements.

## Acceptance

From an actual `https://kenigevents.ru` page verify:

- health, OAuth callback/PKCE exchange, refresh, OTP link/code, logout and
  identity linking;
- allowed REST reads/RPCs and `event-search`;
- authenticated upload/delete inside `focus-feedback`;
- unrelated Origin is rejected by CORS;
- unknown RPC/function, `/auth/v1/admin/**`, Realtime, another Storage bucket,
  unsupported method and public feedback-object read are rejected by Gateway;
- gateway logging remains disabled and no service account/secret is attached.
