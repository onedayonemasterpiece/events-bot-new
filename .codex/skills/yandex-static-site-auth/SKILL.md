---
name: yandex-static-site-auth
description: Use in events-bot-new when implementing, debugging, testing, or configuring Yandex login for the static KenigEvents site through Supabase Auth. Triggers include Yandex OAuth, custom:yandex, Supabase custom OAuth/OIDC provider, static Astro auth callback, PKCE, localhost redirect, Error getting user email from external provider, yandex-userinfo Edge Function, AuthorizedEventSearch, /poisk/, event-search Edge Function auth, browser session issues, and OAuth redirect allow-list problems.
---

# Yandex Static Site Auth

## Scope

Use this skill for the KenigEvents static site auth path:

```text
static Astro page on https://kenigevents.ru/*
  -> Supabase Auth custom OAuth provider custom:yandex
  -> Yandex OAuth app
  -> Supabase callback
  -> static page receives ?code=...
  -> browser JS exchanges PKCE code into a Supabase session
  -> authenticated browser calls Edge Functions with Bearer session token
```

This is for the **personalization Supabase project**, not the legacy Supabase project. Use only `PERSONALIZATION_SUPABASE_*` / `STATIC_SITE_PUBLIC_*` / `PUBLIC_*` envs for this path.

## Start checklist

1. Open canonical docs before changing behavior:
   - `docs/features/unsigned-personalization/authorized-event-search.md`
   - `docs/features/unsigned-personalization/semantic-vector-retrieval.md` when search/vector retrieval is involved.
2. Inspect current code, not memory:
   - `site/src/components/AuthorizedEventSearch.astro`
   - `supabase/functions/yandex-userinfo/index.ts`
   - `supabase/functions/event-search/index.ts`
   - `scripts/check_authorized_search_readiness.py`
   - `scripts/smoke_authorized_search_ui.py`
3. Never print secrets. Check only presence of env values.
4. Browser/static build may expose only:
   - `PUBLIC_PERSONALIZATION_SUPABASE_URL` or `STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_URL`
   - `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` or `STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`
   - `PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex`
5. Never expose `PERSONALIZATION_SUPABASE_SECRET_KEY`, service-role keys, direct DB URLs, Yandex client secret, or Supabase access token in static HTML/JS.

## Required provider configuration

Supabase Auth custom provider:

- Identifier: `custom:yandex`.
- Type: OAuth2, unless the project deliberately migrates to OIDC discovery.
- Authorization URL: `https://oauth.yandex.ru/authorize`.
- Token URL: `https://oauth.yandex.ru/token`.
- UserInfo URL: `https://<PERSONALIZATION_SUPABASE_PROJECT_REF>.supabase.co/functions/v1/yandex-userinfo`.
- Scopes: `login:email login:info`.
- `email_optional=true`.

Yandex OAuth app:

- Redirect URI must include the Supabase callback URL shown by the Supabase custom provider form, normally `https://<project-ref>.supabase.co/auth/v1/callback`.
- If Yandex asks for a site/hostname field, include scheme: `https://kenigevents.ru`; plain `kenigevents.ru` was rejected by the Yandex UI.
- Do not try to make Yandex redirect directly to a preview page; Supabase Auth is the OAuth callback endpoint and then returns the browser to `redirectTo`.

Supabase Auth URL Configuration:

- `site_url=https://kenigevents.ru`.
- Redirect allow-list must contain at least:
  - `https://kenigevents.ru/**`
  - `https://www.kenigevents.ru/**`
- A `localhost:3000` Site URL or missing allow-list causes mobile users to return to `localhost` after Yandex consent.

## Yandex userinfo adapter is mandatory

Do **not** configure Supabase custom OAuth userinfo directly to `https://login.yandex.ru/info?format=json` for this flow. Yandex returns non-OIDC keys such as `id` and `default_email`; Supabase's generic OAuth2 provider expects claims like `sub`, `email`, `name`.

Use `supabase/functions/yandex-userinfo/index.ts` as a public server-to-server adapter:

- accepts `Authorization: Bearer <token>` or `Authorization: OAuth <token>` from Supabase Auth;
- calls Yandex using `Authorization: OAuth <token>` with Bearer fallback;
- maps `id -> sub`, `default_email`/`emails[0] -> email`, `real_name`/`display_name`/`login -> name`, plus username/avatar fields;
- returns `Cache-Control: no-store`;
- rejects missing tokens with `401 {"error":"missing_yandex_token"}`.

Deploy it with JWT verification disabled because Supabase Auth calls it using the Yandex OAuth token, not a Supabase JWT:

```bash
set -a; source .env; set +a
SUPABASE_ACCESS_TOKEN="$PERSONALIZATION_SUPABASE_ACCESS_TOKEN" \
  npx -y supabase@latest functions deploy yandex-userinfo \
  --project-ref "$PERSONALIZATION_SUPABASE_PROJECT_REF" \
  --no-verify-jwt --use-api
```

## Static PKCE implementation contract

Static Astro pages have no server callback handler. The browser JS must complete the OAuth flow.

Use `@supabase/supabase-js` with PKCE:

```ts
const supabase = createClient(url, publishableKey, {
  auth: {
    flowType: 'pkce',
    detectSessionInUrl: false,
    persistSession: true,
    autoRefreshToken: true,
    storage: authStorage,
  },
})
```

Required behavior:

1. On login, call `signInWithOAuth({ provider: 'custom:yandex', options: { redirectTo } })`.
2. `redirectTo` must be the **cleaned current URL**, preserving the page the user started from and removing stale `code`, `state`, `error`, `error_code`, `error_description`, `sb` params/fragments.
3. On return with `?code=...`, mark callback-in-progress before ordinary auth-state rendering.
4. Explicitly call `supabase.auth.exchangeCodeForSession(code)`.
5. After success, call `setSession` with returned access/refresh tokens as a hardening step.
6. Clean the visible URL with `history.replaceState` before rendering the authorized UI.
7. On callback errors, remove stale callback params and show product copy; do not silently show the anonymous button.
8. Show who is signed in using `preferred_username`, `name`, `full_name`, email, or a short user id.
9. Persist a small auth-intent marker such as `ke_yandex_auth_intent_v1` when the user clicks login, and update it on callback/signed-in/failure states. Use it only as a UX hint; the Supabase session remains the real auth source.
10. On new static/preview links under the same `kenigevents.ru` origin, restore the saved Supabase session before showing the login CTA. A new preview path must not log the user out.
11. Bound callback exchange with a timeout and always clean stale `code/error/sb` params so the UI cannot stay forever at “Завершаю вход…”.
12. Do not await Supabase calls inside `onAuthStateChange`; render from the callback `session` payload and defer follow-up RPCs with `setTimeout`.

PKCE verifier hardening:

- Supabase stores a short-lived `*-code-verifier` in browser storage.
- Mirror only this verifier into a `Secure; SameSite=Lax; Path=/; Max-Age=900` cookie as fallback for mobile OAuth round-trips.
- Do **not** mirror access/refresh tokens into custom cookies.

## Authenticated Edge Function calls from static pages

For authenticated features, the static page should call Edge Functions with the Supabase session token:

```ts
fetch(`${supabaseUrl}/functions/v1/event-search`, {
  method: 'POST',
  headers: {
    apikey: publishableKey,
    Authorization: `Bearer ${session.access_token}`,
    'Content-Type': 'application/json',
    Accept: 'application/x-ndjson',
  },
  body: JSON.stringify(payload),
})
```

For `event-search`, prefer direct `fetch` over `supabase.functions.invoke` when streaming backend progress is needed. The current contract uses NDJSON progress events: `accepted`, `auth`, `validate`, `quota`, `embedding`, `vector_search`, `llm_verify`, `fallback`, `finalize`, then `result` or `error`. The frontend should render immediately after `result` and cancel/stop reading the stream; do not wait for EOF before unblocking the search UI.

Even if an Edge Function is deployed with `--no-verify-jwt` for CORS/manual auth reasons, it must manually require and validate the Bearer token via `supabase.auth.getUser(accessToken)` before doing user-specific work.


Quota rule: ordinary search quota and optional LLM-rerank quota are different. Use `reserve_event_search_quota_v2`; if it returns `llm_reserved=false` while search quota is reserved, the Edge Function must still return pgvector results and mark the response/audit as `llm_status=llm_quota_exhausted`. Do not turn exhausted LLM verifier quota into a user-facing “search limit ended” error.

## Input validation and UX recovery

Search/query inputs need both client-side and Edge Function validation. Client validation is for UX only; server validation is authoritative.

Current search contract:

- normalize control characters and whitespace;
- length: 3..180 chars;
- reject obvious HTML/script tags, `javascript:`, SQL comment markers, broad SQL command patterns, template markers, and direct prompt-injection phrases;
- pass user query to LLM prompts as data, e.g. `JSON.stringify(query)`, not as an instruction.

UI must always recover:

- non-2xx / streamed `error` / timeout should show product copy;
- input and submit button must be re-enabled in `finally`;
- if the session expired, sign out locally and ask the user to log in again;
- do not show stale result cards before the first real query on `/poisk/`;
- signed-in account actions should use a compact avatar/account menu. Avoid a large always-visible logout button near the query input; it is too easy to hit accidentally on mobile. Prefer Yandex HTTPS avatar URL, then first initial, then a neutral SVG fallback, and keep logout inside the popover.

## Debugging playbook

- **After Yandex consent the browser opens `localhost:3000`**: Supabase Auth URL Configuration is wrong. Fix `site_url` and redirect allow-list; run readiness probe with `--probe-auth-config`.
- **Callback contains `Error getting user email from external provider`**: direct Yandex userinfo is incompatible or email is required. Configure the `yandex-userinfo` adapter and `email_optional=true`; run `--probe-yandex-userinfo-adapter`.
- **Browser returns to `/poisk/?code=...` but still shows login**: static page did not exchange the PKCE code or lost the code verifier. Check `detectSessionInUrl:false`, explicit `exchangeCodeForSession`, custom storage/cookie fallback, and URL cleanup.
- **Auth UI appears but search hangs or returns no cards**: check whether `event_search_requests` rows were created. No row means the browser request did not reach the Edge Function; inspect fetch headers, CORS, network errors, and NDJSON parser.
- **Raw `Edge Function returned a non-2xx status code` leaks into UI**: map backend errors to product text and re-enable controls.
- **Readiness passes but live mobile still fails**: capture final URL, visible status text, approximate UTC time, and then compare Supabase Auth users/sessions plus `event_search_requests`/Edge logs for that interval.

## Readiness and smoke commands

Run the redacted readiness probe before claiming the gate:

```bash
python3 scripts/check_authorized_search_readiness.py \
  --env-file .env \
  --probe-auth-config \
  --probe-yandex-provider \
  --probe-yandex-userinfo-adapter \
  --probe-edge \
  --strict
```

Build a static preview with browser-safe envs only:

```bash
set -a; source .env; set +a
PREVIEW_BUILD_ID=preview-YYYYMMDD-auth-test \
PUBLIC_PERSONALIZATION_SUPABASE_URL="$PERSONALIZATION_SUPABASE_URL" \
PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY="$PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY" \
PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex \
npm --prefix site run build:preview
npm --prefix site run check:preview
```

Run mocked browser UI smoke:

```bash
python3 scripts/smoke_authorized_search_ui.py \
  --dist site/dist/<preview-build-id> \
  --supabase-url "$PERSONALIZATION_SUPABASE_URL"
```

Run opt-in browser smoke against the real deployed Edge Function while mocking only the static PKCE callback/session handoff. This consumes live quota and may create a temporary Supabase Auth smoke user:

```bash
python3 scripts/smoke_authorized_search_ui.py \
  --real-edge \
  --dist site/dist/<preview-build-id> \
  --supabase-url "$PERSONALIZATION_SUPABASE_URL" \
  --supabase-publishable-key "$PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY" \
  --supabase-secret-key "$PERSONALIZATION_SUPABASE_SECRET_KEY" \
  --real-edge-query "джаз на выходных"
```

Deploy relevant Edge Functions:

```bash
set -a; source .env; set +a
SUPABASE_ACCESS_TOKEN="$PERSONALIZATION_SUPABASE_ACCESS_TOKEN" \
  npx -y supabase@latest functions deploy event-search \
  --project-ref "$PERSONALIZATION_SUPABASE_PROJECT_REF" \
  --no-verify-jwt --use-api
```

## File ownership map

- Auth/search UI: `site/src/components/AuthorizedEventSearch.astro`.
- Dedicated search page: `site/src/pages/poisk/index.astro`.
- Static build handoff args: `scripts/run_static_site_builder_kaggle.py`, `kaggle/StaticSiteBuilder/static_site_builder.py`.
- Userinfo adapter: `supabase/functions/yandex-userinfo/index.ts`.
- Authenticated search function: `supabase/functions/event-search/index.ts`.
- Readiness: `scripts/check_authorized_search_readiness.py`.
- UI smoke: `scripts/smoke_authorized_search_ui.py`.
- Canonical docs: `docs/features/unsigned-personalization/authorized-event-search.md`.

## Release hygiene

When changing this flow:

- update `docs/features/unsigned-personalization/authorized-event-search.md`;
- update `CHANGELOG.md`;
- run the readiness probe and at least the mocked UI smoke;
- if Edge Functions changed, deploy them before handing a preview link to the user;
- if static UI changed, build/check/deploy a preview and provide the exact `/poisk/` URL.
