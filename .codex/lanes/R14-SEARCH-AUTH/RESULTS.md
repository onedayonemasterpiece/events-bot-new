# R14 Search/Auth lane results

## Delivered

- Moved static-site Supabase PKCE ownership into one browser singleton (`staticSiteAuth.ts`).
- Added a global auth runtime component that binds login, logout, identity, and status surfaces without putting session tokens in DOM events or markup.
- Wired Search to the shared auth controller and retained pending-query resume after authentication.
- Added direct Yandex sign-in/account surfaces to the mobile menu and `/dlya-menya/`.
- Made unmodified, non-IME Enter submit Search through `form.requestSubmit()` and exposed `enterkeyhint="search"`.
- Added one bounded JSON rescue when the initial NDJSON request stalls before response headers; the existing stream-idle rescue remains bounded.
- Expanded source-contract, Node Playwright, and Python smoke coverage for the changed behavior.

## Verification

- PASS: 33 focused Node source tests.
- PASS: Node Playwright Search recovery test against `site/dist`, including header-stall rescue, IME guard, Enter submission, stream-idle rescue, overall timeout, and recovery after failure.
- PASS: direct TypeScript check of `site/src/lib/staticSiteAuth.ts`.
- PASS: `python3 -m py_compile scripts/smoke_authorized_search_ui.py`.
- PASS: `git diff --check`.
- PARTIAL: Astro/Vite compiled the changed browser entrypoints during a build, then the long static generation was manually stopped after the affected Search output had been emitted.
- BLOCKED (environment only): the Python browser smoke could not run because the Python `playwright` package is not installed. The equivalent focused Node Playwright browser test passed.

## Integration required

`StaticSiteAuthRuntime.astro` must be mounted exactly once by the integration owner in `site/src/layouts/EventLayout.astro` so callback/session handling and menu/personal auth bindings run on every static page:

```astro
import StaticSiteAuthRuntime from '../components/auth/StaticSiteAuthRuntime.astro';
```

Render `<StaticSiteAuthRuntime />` once in the layout (after the shared header/menu and before the page slot is suitable). This lane intentionally did not edit `EventLayout.astro` because it was outside lane ownership.

`Reference4MobileMenu.astro` is shared with the Collections lane. Preserve both its collection-link changes and this lane's `data-static-auth-*` account controls during integration.

## Material risks

- OAuth callback completion on non-Search pages depends on the required global runtime mount above.
- Public Supabase URL/publishable key/provider must be present at build time; missing config disables login controls fail closed.
- The controller singleton rejects conflicting public auth configuration on the same page.
