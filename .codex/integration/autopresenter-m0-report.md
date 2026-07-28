# Autopresenter M0 integration report

Base: `981aebd9`; integration branch: `integration/autopresenter-m0`.

## Requirement closure

| ID | Status | Integrated evidence |
|---|---|---|
| R01 | Done (implementation) | `run-plan.js` fixes 10 fresh + 10 reused-persistent cycles; `run-suite.js` creates a distinct portable Node worker and managed browser per cycle. |
| R02 | Done (implementation) | Loopback 20/20 and live 5/5 are separate; live is blocked unless local is exactly 20/20. |
| R03 | Done (packaging spec) | Exact Node 22.12.0 plus Playwright 1.61.1/rev 1228 and 1.54.2/rev 1181 bundles are pinned across the 1.57 boundary. |
| R04 | Done (implementation) | Exact candidate/Node/lock/browser path and hashes are verified; channel, cache, runtime download and system browser fallback fail closed. |
| R05 | Done (implementation) | Browser-root process identity snapshots, bounded graceful close, worker exit and orphan checks are retained per run. |
| R06 | Done (implementation) | Schemas, system collector, per-run artifacts, candidate preparer, combined manifest/checksums and strict directory aggregator are packaged. |
| R07 | Done (implementation) | Offline headed self-test covers portable layout, hashes, loopback click, writable files, trace/screenshot and cleanup. |
| R08 | Done (implementation) | Windows build/launcher, interactive double-click flow and automatic plain/spaces/Cyrillic path matrix are present. |
| R09 | Done (implementation) | `19/20` and `4/5` fail; exact stacks/inventory/artifacts are cross-checked before winner/no-go selection. |
| R10 | Done | No M1–M3 stage, relay, phone, overlay or recording implementation was added. |
| R11 | Done | Canonical docs, unique route, runbook and `[Unreleased]` changelog are synchronized. |
| R12 | Done | All docs and runtime output keep target execution pending; Linux validation cannot issue M0 PASS. |

## Validation

- Runtime unit tests: 9/9 pass, including independent target
  machine/account/Windows-build provenance mismatch rejection.
- Evidence/report tests: 12/12 pass, including provenance and fail-closed
  adversarial cases.
- Packaging static verifier: pass.
- All M0 JavaScript: `node --check` pass.
- Candidate JSON and schemas parse; route points to the M0 operator runbook.
- `git diff --check`: pass.
- Windows ZIP-to-folder binding is implemented by extracting the supplied ZIP
  during target evidence preparation and comparing candidate/version/manifest
  checksums before crediting runs.
- Forbidden-scope audit: no M1–M3 implementation.

## Remaining empirical gate

Windows 10 x64 packaging, CMD/PowerShell execution and both exact target-laptop
test matrices have not been run in this Linux environment. Therefore:

- `M0 = target execution pending`;
- `M1–M3 = blocked`;
- `PUBLIC_DEMO = NO-GO`.
