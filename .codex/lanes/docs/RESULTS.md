# Docs lane results — launch tile mosaic

## Revisions

- Base: `f44f7fc66ce6f833b7412796de1fb36f53cacec0`
  (`origin/feature/static-launch-tile-mosaic-20260803` at lane start).
- Head: commit containing this file on
  `agent/static-launch-tile-mosaic-20260803/docs`; resolve with
  `git rev-parse agent/static-launch-tile-mosaic-20260803/docs` after commit.

## Files owned and changed

- `docs/features/static-site-pages/launch-tile-mosaic-placeholder.md`
- `docs/features/static-site-pages/prompts/tile-mosaic-launch-coding-agent.md`
- `.codex/lanes/docs/RESULTS.md`

No files under `site/**`, `supabase/**`, or `CHANGELOG.md` were changed in this
lane.

## Result

- Added the canonical isolated `/lab/launch/tile-mosaic/` product/engineering
  contract: layers, physical 72-tile state model, seeded sparse motion,
  desktop/mobile compositions, image prop/query/event API and URL validation,
  personalization-Supabase table/RPC/env/transport boundary, SEO/GEO/noindex,
  accessibility/reduced motion, QA matrix and secret-preview vs production
  boundary.
- Added a durable coding-agent brief that preserves the branch, root-page,
  parallel-prototype, merge, image slicing, credential, direct-write,
  indexing, test, screenshot, draft-PR and secret-candidate prohibitions/gates
  without representing pending execution as complete.
- Linked official MDN engineering references for `backdrop-filter`,
  `mix-blend-mode`, and `prefers-reduced-motion`, plus official Supabase
  Database Functions, JavaScript `rpc()`, and RLS documentation.
- Aligned the browser path with the repository's dual-DB contract:
  `PUBLIC_PERSONALIZATION_SUPABASE_URL`, publishable key, optional relay,
  shared `getResilientDataClient`, cataloged `selected-once`, and backend-only
  `PERSONALIZATION_DIRECT_CONNECTION_STRING`.

## Commands and validation

Executed in `/dev/shm/static-launch-mosaic-docs`:

- read `docs/README.md`, `docs/routes.yml`,
  `docs/architecture/personalization-data-ownership.md`, and the applicable
  dual-DB/Supabase skills;
- fetched and reviewed the current Supabase changelog; no breaking change found
  that alters this RPC/RLS contract;
- opened current official MDN and Supabase reference pages;
- checked required environment-key presence without printing values;
- ran a Python contract-presence check: canonical doc 25/25 required markers,
  coding brief 19/19 required markers;
- compared source candidates and served copy with `sha256sum`; expected
  SHA-256 is
  `7015488739e0296f6c5b04935a16769804aa8bf128436450e8a60eef32ec07dd`;
- ran `git diff --check` successfully;
- reviewed the final diff and `git status` before commit.

No Astro build, browser, database, or live-preview test belongs to this
read/write-isolated documentation lane. The canonical QA matrix makes those
integration/release gates explicit.

## Risks and required integration checks

1. At the lane base, the user-designated canonical asset path under
   `auto-present/scenario-assets/` is absent from Git even though an identical
   tracked `docs/reference/PWA-icon.png` exists. The integrator has accepted
   ownership to add the designated path and must prove it is byte-identical to
   `site/public/assets/launch/PWA-icon.png` with the SHA above.
2. UI integration must add `subscribe_site_launch_v1` to the backend operation
   catalog as `selected-once`; otherwise the resilient client rejects it as an
   unclassified RPC.
3. The public anonymous RPC's honeypot and idempotency are not a server rate
   limit. A reviewed gateway rate-limit/abuse-monitoring policy remains a
   blocker for a generally public/indexable rollout, though not for the bounded
   secret lab candidate.
4. Cross-origin HTTPS projection images may still be blocked by the deployed
   CSP. URL validation must not weaken CSP as a workaround.
5. Docs describe the target integrated behavior. Integrator must reconcile any
   implementation drift in env names, RPC payload/result handling, URL rules,
   tile caps, reduced-motion behavior, route noindex, and migration grants
   before release evidence is recorded.

## Merge notes

- Merge this commit with the UI and DB lane commits; no file overlap is
  expected.
- Integrator owns the designated canonical source asset and `CHANGELOG.md`,
  which were explicitly forbidden in this lane.
- Keep the page at `/lab/launch/tile-mosaic/`; do not edit production root or
  merge/promote merely because documentation and a local build exist.
- After integration, run the full canonical QA matrix and record actual command,
  screenshot, SHA, secret-preview URL and Telegram delivery evidence outside
  this lane result.
