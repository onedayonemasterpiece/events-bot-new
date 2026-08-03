# Tile mosaic v2 integration report

Date: 2026-08-03

Integration branch: `integration/static-launch-tile-mosaic-20260803`

Publication boundary: immutable `noindex` preview only; no production-root or `main` change

## Lane reconciliation

| Lane | Result | Integration evidence |
|---|---|---|
| `ui-v2` | Done | Worker `9df68f741623716c5f86cabe71611a2bbadf0a1f`, cherry-picked as `d6ec69954`; v2 component/route implementation merged. |
| `qa-v2` | Done | Worker `0a202244e43b3dd6563a48389a32d58f78c0f17b`, cherry-picked as `1f1069dbb`; deterministic L1 runner merged. |
| `docs-v2` | Done | Read-only review completed; integrator applied the canonical documentation corrections. The unused docs branch was superseded without changes. |
| `integration-v2` | In progress | Integration fixes, docs, changelog, exact-SHA preview publication and Telegram handoff are owned serially here. |

The workers used disjoint scopes. Both writable worker worktrees were clean,
committed, reconciled and removed. The dirty primary checkout was not used.

## Requirement status before immutable publication

| Requirements | Status | Evidence |
|---|---|---|
| R01–R16 | Done | Component and route implementation plus Chromium measurements in `l1-local-r3/report.json`. |
| R17–R22 | Done | L1 report: 17 passed, 0 failed, 1 optional live-email hook skipped; all required screenshots captured. |
| R23 | Done | Canonical feature contract, coding-agent prompt and `[Unreleased]` changelog entry updated. |
| R24 | In progress | Requires exact-SHA build/check/deploy, public verification and verified Telegram reply. |

## Validation evidence

- `node --check site/tests/tile-mosaic-launch.test.mjs`: passed.
- `bash -n site/scripts/check-tile-mosaic-launch-playwright.sh`: passed.
- `node --experimental-strip-types --test site/src/lib/resilientSupabaseTransport.test.ts`: 16/16 passed.
- L1 Chromium report:
  `artifacts/codex/static-launch-tile-mosaic-v2-20260803/l1-local-r3/report.json`.
- Required desktop: `1366×768`, `1440×900`, `1536×864`, `1672×941`, `1920×1080` — passed.
- Required mobile: `320×700`, `360×800`, `390×844`, `430×932` — passed without horizontal overflow.
- Motion frames `0/5/10`, reduced motion, default brand projection, arbitrary-photo projection, exact copy and mocked form outcomes — passed.
- Manual visual inspection covered `1672×941`, `1920×1080`, `390×844` and the arbitrary-photo fixture. A thin source-PNG edge found in the first pass was removed before the final `r3` run.

The L1 `503` console finding is intentional evidence from the mocked network-error
form scenario, not an unavailable application asset or endpoint.

Two full-build attempts before integration closure were prevented by shared-host
resource contention: first `/dev/shm` exhaustion while another Astro build was
writing, then an OOM kill while three Astro builds and a concurrent `git fsck`
were resident. Targeted Astro dev-server/L1 execution remained green. The
immutable exact-SHA build is intentionally deferred until those unrelated jobs
release memory; it remains a blocking R24 gate.

## External review evidence

- Antigravity Opus was unavailable with provider error: `Eligibility check
  failed: ... not currently available in your location`.
- Claude Code project agent `Opus` was installed but not authenticated (`Not
  logged in · Please run /login`).
- No lower-class model was substituted. Final visual acceptance therefore uses
  the explicit reference-transfer contract, measured L1 gates and direct
  screenshot inspection.

Native `mobile.keyboard_inputs` and `mobile.page_family_specimens` remain
planned L2 registry scenarios. They are not represented as completed by the
desktop Chromium emulation and do not block this isolated review preview.
