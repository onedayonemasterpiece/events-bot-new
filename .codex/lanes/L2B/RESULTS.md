# L2B — zero-mail transport fault matrix

## Scope

- Lane: `L2B`
- Requirement IDs: `R5`, `R6`, `R7`
- Base SHA: `a2cf9315cd166b5e591b0a4ab4ebf746d61074dd`
- Head SHA (validated implementation): `ccccae053850ebc6f203148de5382cfc30c88348`
- Branch: `agent/static-unified/l2b-transport-fault-matrix`

## Result

Implemented the deterministic, zero-remote-write fault matrix for Auth, Search,
personalization and focus feedback. The existing receipt schema remains
`static_site_no_mail_fault_matrix_receipt.v1`; new fields are additive.

Executable profiles:

1. `normal`
2. `client_supabase_direct_unreachable`
3. `client_yandex_relay_unreachable`
4. `both_client_routes_unreachable`
5. `supabase_upstream_unavailable`
6. `selected_once_response_body_ambiguous`
7. `recovery_after_reload`

Proved invariants:

- selected-once product dispatch is `<= 1`;
- both-client-routes-down product dispatch is `0`;
- ambiguous selected-once response/body is not replayed;
- idempotent focus replay converges to exactly one logical effect;
- shared upstream failure is not classified as relay recovery;
- serialized pending intent survives a new transport instance and recovery;
- the second recovery flush performs no dispatch and creates no duplicate effect;
- every non-normal profile emits sanitized fault-activation codes;
- receipt omits target URLs, request paths/bodies, action IDs and auth material;
- product OTP issue / external mail send / external mail receipt are `0/0/0`.

## Evidence and commands

- Baseline before implementation:
  - `node --experimental-strip-types --test site/tests/no-mail-fault-matrix.test.ts`
  - PASS `2/2`.
- Test-first reproduction after expanding acceptance:
  - same command;
  - expected FAIL: only four profiles existed and effect/recovery receipt fields
    were absent.
- Final transport regression:
  - `node --experimental-strip-types --test site/src/lib/resilientSupabaseTransport.test.ts site/tests/no-mail-fault-matrix.test.ts`
  - PASS `24/24` (`16` transport + `8` matrix).
- Auth fixture regression using the integration worktree's existing dependency
  tree (temporary ignored symlink removed afterwards):
  - `node --experimental-strip-types --test site/tests/auth-session-fixture.test.mjs`
  - PASS `5/5`.
- Registry lint:
  - `node site/e2e/auth-session-fixture/registry-lint.mjs`
  - PASS.
- YAML parse using pinned `yaml@2.9.0` from the existing integration install:
  - PASS, seven profiles.
- Patch hygiene:
  - `git diff --check`
  - PASS.

Two preliminary aggregate checks failed only because this isolated worktree has
no `site/node_modules` and because a root-level ESM eval resolves bare packages
from the importing module rather than `site/node_modules`. No install was run.
The existing integration dependency tree was reused from the `site` package
location and all temporary links were removed.

## Changed files

- `site/e2e/auth-session-fixture/noMailFaultMatrix.ts`
- `site/tests/no-mail-fault-matrix.test.ts`
- `docs/testing/static-site-autotest-scenarios.v1.yml`
- `docs/operations/static-site-autotest-strategy.md`
- `docs/operations/yandex-dependency-resilience.md`
- `.codex/lanes/L2B/RESULTS.md`

## Risks / honest boundaries

- This is a deterministic L0 transport-policy proof. It does not claim live
  hosted Supabase, browser, Android or iOS acceptance.
- Reload uses an in-memory serialized journal to prove the stable-action
  contract; it does not prove the product runtime's actual localStorage/outbox.
- Shared-upstream and body ambiguity are injected locally; there are no external
  calls, remote writes or OTP/mail side effects.
- Registry statuses for both-down and shared-upstream are intentionally
  `partial_*_live_surface_acceptance_pending`; unrelated provider, YDB, inbound,
  OAuth and product-outbox scenarios remain open.
