# Search production-health Stage 2 integration report

## Base and safety

- Integration base: `origin/main@1f449af36987ad63657da4387e909513c36cd995`;
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

- Search production-health aggregate suite: **80/80 PASS**;
- legacy Search harness: **30/30 PASS**;
- broker Python/security **41/41 PASS** and Auth Node **16/16 PASS**;
- static source-binding/release regression: **87/87 PASS**;
- workflow YAML and shell/node syntax;
- migration contract and diff check;
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
