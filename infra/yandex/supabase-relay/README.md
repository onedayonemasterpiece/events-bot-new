# Stateless Supabase transport relay

This API Gateway is a fixed-upstream transport alternative for the static
KenigEvents browser. Supabase remains the sole owner of Auth, sessions, RLS and
durable product data. The gateway has no service account, database, secret key
or application logic.

The browser first performs safe health reads over the direct and relay routes,
then sends a non-idempotent Auth operation exactly once over the selected route.
Only GET/HEAD operations may retry once on the alternate route.

## Deploy

```bash
export PATH=/home/dev/yandex-cloud/bin:$PATH
FOLDER=b1g5tck18cgqtjb7rn3s
yc serverless api-gateway create kenigevents-supabase-relay \
  --folder-id "$FOLDER" \
  --description "Stateless browser transport relay; Supabase keeps Auth and RLS" \
  --spec infra/yandex/supabase-relay/openapi.yaml \
  --no-logging
```

For an existing gateway, use `yc serverless api-gateway update ... --spec ...
--no-logging`. Put the returned public domain into
`PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL` for the static build. Do not use the
relay URL in server-side static rebuilds or bulk exports.

## Acceptance

From an actual `https://kenigevents.ru` page verify:

- `GET /auth/v1/health` returns 200;
- a tiny RLS-protected `GET /rest/v1/...` returns 200;
- invalid OTP verify and refresh reach Supabase once and return their expected
  Auth errors rather than a gateway error;
- CORS does not allow an unrelated Origin;
- gateway logging is disabled;
- no service account or secret key is attached.
