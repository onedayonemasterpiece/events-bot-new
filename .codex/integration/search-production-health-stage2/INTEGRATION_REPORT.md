# Search production-health Stage 2 integration report

## Base and safety

- Integration base: `origin/main@1f449af361e586da509d0199cfe059d620fb42d6`;
  it contains merged PR #441 at
  `dd5ffc2eb5327cb52eb62e232e1e927dbe4c9c66`.
- Branch/PR: `integration/search-production-health-stage2-20260809` / #451.
- Live Search workflows so far: `0 / 2` permitted.
- Production migration/deploy/session/Search calls during implementation: `0`.
- PR #436: untouched and not a dependency.

## Integrated lanes

| Lane | Implementation | Receipt | Result |
|---|---|---|---|
| workflow/release marker | `d7eb39e97` | `3a55b8995` | two schedules, manual/runtime marker, no generation triggers |
| journey/evidence | `0938baed8` | `967c74c4e` | one query/POST, target pin, bytes, strict evidence |
| mobile transport | `583ea5d0a` | `c27b983c8` | real Appium preflight/scroll/card open/cleanup |
| broker | `a22f01b62` | `1e8188427` | platform identity, typed admission, migration |
| reporter | `70754d633`, `ec02e0fc2` | `a557a6d0a` | platform disposition and REST mutation |
| integration hardening | current branch | this report | legacy/full qualification compatibility, Appium auth/RLS, bounded release wait, reporter history/labels/aggregate, docs |

## Deterministic acceptance

Required before live:

- Search production-health aggregate suite: **104/104 PASS**;
- legacy Search harness: **30/30 PASS**;
- broker Python/security **44/44 PASS**, Edge contract **5/5 PASS** and Auth
  Node **16/16 PASS**;
- static source-binding/release regression: **87/87 PASS**;
- workflow YAML and shell/node syntax;
- broker migration plus its canonical SQL replay/expiry contract on ephemeral
  PostgreSQL 17, and diff check;
- independent checklist audit and fresh GitHub Actions.

## Production activation gate

1. Merge #451 into current `main` after green checks.
2. Apply migration `20260809143602_static_site_auth_broker_platform_claims.sql`.
3. Add both `search-production-health.yml@refs/heads/main` and
   `search-release-qualification.yml@refs/heads/main` to the broker workflow allowlist.
4. Deploy exact merged `origin/main` through `scripts/deploy_fly_main.sh` with validation profile `none`.
5. Run manual `browser_android`, then manual `browser_ios`; no retry after side effects.
6. Only after both are terminal HEALTHY/PASS set `SEARCH_PRODUCTION_HEALTH_ENABLED=true`.
7. Update the Search incident regression record and issue #431 with exact run IDs.

Current disposition: `STAGE2_IMPLEMENTED_LIVE_ACCEPTANCE_PENDING / PRODUCT_HEALTH_UNCONFIRMED`.

The current hardening pass additionally proves one physical POST is metered
once, accepts real cache-hit execution, rejects real skeleton/placeholder UI,
aligns the Appium preflight/diagnostic receipt, preserves failed-journey bytes,
gates cleanup, rereads pointer on failure, suppresses superseded issue mutation,
keeps pre-runner UNKNOWN streaks, refuses missing-artifact BROKEN proof, pins
Appium drivers, makes full qualification synchronous, and provides a bounded
encrypted durable broker idempotency replay window. It also verifies the
active Edge contract with a side-effect-free HEAD before Auth/Search, enables
the iOS Safari console bucket, rejects mobile redirect chains, and keeps an
adapter-level physical Search observer alive through final event-page
diagnostics so the complete journey proves exactly one Search POST. Unknown
pre-runner cells retain the exact sanitized summary schema with explicit
closed null/zero values. Mobile protocol receipts also ingest CDP
`redirectResponse` for the document chain and correlate terminal
`loadingFinished.encodedDataLength` when Content-Length is absent. These are deterministic results;
live acceptance remains `0 / 2` and no production state has been changed.
