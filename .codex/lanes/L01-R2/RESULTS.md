# L01-R2 — Authorized Search

## Status

Done. The noindex preview build can now receive the existing browser-safe
Supabase search configuration reproducibly, the signed-out Search form accepts
typing, and a submitted draft survives the Yandex PKCE round-trip and is
automatically executed through the real `event-search` contract after sign-in.

## Root cause

`site/scripts/build-preview.mjs` inherited only the caller process environment.
The integration preview was built from a linked worktree without exported
`PUBLIC_PERSONALIZATION_SUPABASE_*` values, even though the primary checkout
had the corresponding browser-safe personalization URL and publishable key.
Astro therefore generated `data-search-enabled="false"`, `readonly`, and a
disabled submit button.

## Changes

- Added `site/scripts/preview-public-env.mjs`:
  - resolves an explicit `STATIC_SITE_PREVIEW_ENV_FILE`, the checkout `.env`,
    or the primary checkout `.env` for linked worktrees;
  - maps `PUBLIC_*`, then `STATIC_SITE_PUBLIC_*`, then the known
    `PERSONALIZATION_SUPABASE_URL/PUBLISHABLE_KEY` aliases;
  - forwards only the public project URL, publishable key, and provider name;
    secret/service keys, OAuth client secrets, access tokens, and database URLs
    are never returned to Astro;
  - supports `PREVIEW_REQUIRE_AUTHORIZED_SEARCH=1` as a fail-closed publication
    gate.
- `preview-build.json` records only
  `authorizedSearchConfigured: true|false`; it does not record credentials.
- The configured Search form is visible and editable while signed out.
- Submitting a valid signed-out query stores a 30-minute local draft and starts
  the existing Yandex PKCE login.
- The callback restores, consumes, and automatically executes the exact draft.
  Invalid/expired drafts are discarded.
- Updated the browser smoke to cover signed-out typing, PKCE redirect, callback
  draft execution, real canonical event cards, and draft cleanup.
- The loading skeleton remains hidden before submit.

## Verification

### Public configuration/readiness

Redacted readiness probe (all checks `OK`):

```bash
python scripts/check_authorized_search_readiness.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --probe-auth-config --probe-yandex-provider \
  --probe-yandex-userinfo-adapter --probe-edge --strict
```

Verified: browser-safe build env, Yandex credentials/provider redirect,
Supabase deployment/runtime/vector-sync readiness, Edge Function OPTIONS,
redirect allow-list, and userinfo adapter.

### Tests

```bash
node --test \
  site/tests/preview-search-env.test.mjs \
  site/tests/search-initial-state.test.mjs \
  site/tests/search-learning.test.mjs
```

Result: **17/17 passed**.

```bash
python -m py_compile scripts/smoke_authorized_search_ui.py
git diff --check
```

Result: passed.

### Strict preview build

```bash
PREVIEW_BUILD_ID=preview-20260723-r2-search-lane \
PREVIEW_REQUIRE_AUTHORIZED_SEARCH=1 \
npm --prefix site run build:preview
```

Result: **389 pages built**. Generated `/poisk/` evidence:

- `data-search-enabled="true"`;
- Supabase project URL and publishable-key attributes present;
- textarea editable;
- submit enabled;
- skeleton and result containers hidden before a query;
- `preview-build.json.authorizedSearchConfigured=true`.

```bash
PREVIEW_BUILD_ID=preview-20260723-r2-search-lane \
npm --prefix site run check:preview
```

Result: passed, 288 events.

### Browser and live contract

Mocked PKCE/search browser smoke:

```bash
python scripts/smoke_authorized_search_ui.py \
  --dist site/dist/preview-20260723-r2-search-lane \
  --supabase-url '<redacted>' \
  --screenshot-dir artifacts/codex/static-unified-r2-search/mocked
```

Result: `authorized_search_ui_smoke=ok`; pre-auth draft was submitted, restored
after callback, automatically searched, and rendered canonical event cards.

Desktop 1440×900 signed-out smoke: `desktop_signed_out_search_input_smoke=ok`;
typing worked while skeleton/results stayed hidden.

Real Supabase Auth session + real deployed `event-search` Edge Function:

```bash
python scripts/smoke_authorized_search_ui.py --real-edge \
  --dist site/dist/preview-20260723-r2-search-lane \
  --supabase-url '<redacted>' \
  --supabase-publishable-key '<redacted>' \
  --supabase-secret-key '<redacted>' \
  --real-edge-query 'джаз на выходных'
```

Result:

```text
authorized_search_real_edge_smoke=ok
cards=2 first_event=6889 scrolled_event=5375
```

This consumed one live smoke query. No preview was deployed from this lane.

## Integration notes / risks

- Integration build should use `PREVIEW_REQUIRE_AUTHORIZED_SEARCH=1`; a missing
  config will then fail before a dead Search preview can be published.
- Final live acceptance still needs a human Yandex consent round-trip. The
  automated real-Edge gate uses a temporary real Supabase session while mocking
  only the Yandex/PKCE handoff.
- Public URL and publishable key are intentionally browser-visible Supabase
  credentials; privileged keys remain backend-only.
