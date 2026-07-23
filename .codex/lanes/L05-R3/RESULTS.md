# L05 Results — R05 controlled desktop Search smoke

## Lane contract

- **Lane:** L05
- **Requirement:** R05
- **Base SHA:** `68576d5b70f57164c00386b05cff126586c3f700`
- **Implementation head SHA:** `f27909a89c5d92c3f43adffd8e189501fd08a58e`
- **Final record commit:** the commit containing this file (reported to the
  integrator after commit)
- **Writable scope used:** Search smoke runner and this result record only
- **Forbidden scope preserved:** no edits to `site/src/pages/poisk`, production
  auth/runtime code, canonical `docs/`, or `CHANGELOG.md`

## Outcome

Added `scripts/smoke_authorized_search_desktop.cjs`, a reusable 1440×1000
Playwright acceptance runner with two explicitly separated modes:

1. **Default deterministic browser mode**
   - opens a fresh signed-out browser context;
   - proves the desktop Search input is visible, editable, and at least 320 px
     wide;
   - types the query with real keyboard events while signed out;
   - proves signed-out typing does not call `event-search`;
   - submits through the application's normal `signInWithOAuth` / PKCE path;
   - mocks only the Supabase network boundary in Playwright;
   - returns to the normal OAuth callback with `code=...`;
   - proves the pending typed query is restored and consumed only after a
     session is established;
   - proves the normal application sends a bearer-authenticated
     `event-search` request with `use_llm_verifier=true`;
   - proves the returned card renders and the loading skeleton clears.

2. **Opt-in `--real-edge` mode**
   - creates a short-lived Supabase Auth password user through the admin API;
   - obtains a legitimate Supabase access token;
   - mocks only the browser PKCE callback response, never `event-search`;
   - sends the real token to the real deployed Edge Function;
   - runs the three known Search incident queries;
   - deletes the temporary Auth user in `finally`.

The runner stores only a boolean indicating that a bearer header was present.
It never prints or retains the token in its result output. Secret values are
read only from environment variables and error output is deliberately
content-free.

## Accepted target

The target was built from an archive of accepted Search commit
`a4681d422183ab0da0345dc277d78f299436ede8`
(`b35a5780` is the equivalent worker commit) using build id
`preview-r3-search-accepted-a4681d42`.

Build evidence:

```text
389 page(s) built
Preview build ready: dist/preview-r3-search-accepted-a4681d42/
Authorized Search: configured with browser-safe public values
preview-build.json: authorizedSearchConfigured=true
```

No production source was copied into or changed by this lane. The temporary
accepted build remained outside the repository and is not committed.

## Commands and evidence

### Static checks

```bash
node --check scripts/smoke_authorized_search_desktop.cjs
NODE_PATH="$(npm root -g)" node scripts/smoke_authorized_search_desktop.cjs --help
git diff --cached --check
```

Result: passed.

### Accepted Search build

The source archive was produced with `git archive a4681d42` in `/tmp`, linked
to the existing shared `site/node_modules`, and built with:

```bash
PREVIEW_REQUIRE_AUTHORIZED_SEARCH=1 \
PREVIEW_BUILD_ID=preview-r3-search-accepted-a4681d42 \
npm --prefix "$TARGET/site" run build:preview
```

Public Supabase URL/publishable-key values came from the existing protected
environment. Their values were not printed.

### Deterministic desktop browser smoke

```bash
NODE_PATH="$(npm root -g)" \
node scripts/smoke_authorized_search_desktop.cjs \
  --dist /tmp/r3-search-accepted-a4681d42.4fGT0o/site/dist/preview-r3-search-accepted-a4681d42 \
  --supabase-url "$PERSONALIZATION_SUPABASE_URL"
```

Final output:

```json
{"status":"ok","target":"preview-r3-search-accepted-a4681d42","mode":"mocked-browser","viewport":"1440x1000","typedQueryPreserved":true,"authBoundary":"mocked-pkce-session-browser-context-only","requestCalls":1,"cards":1,"firstEventId":"6310"}
```

### Legitimate Auth token + real Edge Function smoke

```bash
NODE_PATH="$(npm root -g)" \
node scripts/smoke_authorized_search_desktop.cjs \
  --dist /tmp/r3-search-accepted-a4681d42.4fGT0o/site/dist/preview-r3-search-accepted-a4681d42 \
  --real-edge \
  --timeout-ms 90000
```

Final output:

```json
{"status":"ok","target":"preview-r3-search-accepted-a4681d42","mode":"real-edge","viewport":"1440x1000","authBoundary":"mocked-pkce-callback-with-legitimate-supabase-test-session","realEdgeValidatedToken":true,"queries":[{"query":"На природу с детьми","browserMs":2008,"cards":5},{"query":"искусство у моря","browserMs":767,"cards":3},{"query":"в пятницу бесплатно","browserMs":833,"cards":4}]}
```

The successful process exit also proves the temporary test user cleanup
returned success.

## Incident regression relevance

`INC-2026-07-02-static-search-92-percent-no-cards` was treated as the relevant
regression contract. This lane provides desktop browser evidence that all
three named queries reached real `event-search` and rendered cards.

This is **not** incident closure evidence by itself. The incident still calls
for mobile production-URL E2E, screenshots/artifact JSON, backend audit rows
with stage/model attempts, production entrypoint verification, and deployed
SHA reachability from `origin/main`. Those surfaces were outside L05.

## Operational notes and risks

- The first launch found the global Playwright 1.58 package did not have its
  matching Chromium revision. The runner now rejects unusable packages and
  reuses an already-installed shared Playwright/Chromium instead of installing
  another browser or changing dependencies.
- One later real-mode retry saw `Page crashed` while the filesystem had only
  59 MB free (`/dev/vda2` at 100%). Removing unneeded files from the temporary
  accepted-source archive restored 694 MB free; the unchanged runner then
  passed all three real queries. This was host capacity, not an application or
  auth failure.
- Real mode consumes Search quota and creates a temporary Auth user, so it is
  intentionally opt-in. Cleanup is mandatory and failures make the smoke fail.
- Real mode validates a legitimate Supabase password-auth token at the Edge
  Function, not the Yandex identity-provider redirect itself. This is
  deliberate: the acceptance target must not require or reuse the user's
  Yandex session.
- Supabase changelog/auth and Edge Function updates were checked before
  implementation; no relevant breaking contract was found for this
  browser-only PKCE interception/test-user flow.

## Changed files

- `scripts/smoke_authorized_search_desktop.cjs`
- `.codex/lanes/L05/RESULTS.md`
