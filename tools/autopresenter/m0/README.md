# Autopresenter M0 compatibility spike

This directory implements **M0 only**: an empirical compatibility test for two
exact portable Node + Playwright + Playwright-managed-browser bundles on the
target Windows 10 x64 laptop.

The canonical product and acceptance contract is
[`docs/features/static-site-pages/auto-present/README.md`](../../../docs/features/static-site-pages/auto-present/README.md).
Candidate manifests under `candidates/` are the only machine-readable source
for exact versions, revisions, paths and hashes.

## Scope gate

Allowed in M0:

- two hermetic candidate bundles across the Playwright 1.57 packaging boundary;
- portable launcher and offline `self-test.cmd`;
- deterministic local fixture served only on loopback;
- one real Playwright `locator.click()` scenario;
- automatic cold-cycle runner, live `/zavtra/` smoke and evidence collection;
- strict candidate comparison/report.

Forbidden in M0:

- **all M1–M3 implementation**;
- iframe presentation stage, decorative pointer/tap overlays and recording;
- phone control UI, relay API, long-poll, remote auth or production service;
- final portable release/backup-video pipeline;
- desktop/typing/QR/infographic scenarios, multiple scenarios or a DSL;
- Electron, a combined `.exe`, system-browser fallback or on-target install.

Do not begin another milestone automatically after a successful result.

## Candidate integrity

Every `candidates/*.json` manifest must resolve, without placeholders, the exact:

- Windows 10/x64 target;
- Node version, ZIP filename and SHA-256;
- `playwright` version and `package-lock.json` SHA-256;
- browser product, revision/build, relative executable and SHA-256;
- headed launch arguments, `browserChannel: null`,
  bundle-local `PLAYWRIGHT_BROWSERS_PATH`;
- supported profile modes (`fresh`, `persistent`).

The runner must fail closed when any value differs. It must never use Edge,
installed Chrome, a browser channel, `%LOCALAPPDATA%\ms-playwright`, a global or
build-machine cache, `npx playwright install`, or any runtime download.

## Target run

M0 is not complete on Linux, CI or a substitute Windows machine. Copy the
prebuilt candidate folders to the **target laptop** and run them as the same
non-admin Windows account intended for the presentation. No Node, browser,
package, Visual C++ runtime or other system component may be installed during
acceptance.

Run `self-test.cmd` offline first. Then execute the M0 launcher from each path
required by the canonical contract, including a path with spaces and a
Cyrillic path.

For each candidate the runner must perform, in order:

1. **20/20 local compatibility cycles**
   - runs 001–010: a fresh profile directory per run;
   - runs 011–020: one persistent profile reused between runs.
2. **5/5 live-site smoke cycles**, only after compatibility is 20/20.

Every cycle starts a new portable Node process and a new headed browser process,
uses the intended profile, performs the real click/assertion, closes context and
browser, ends Node, records the result, and verifies no child processes remain.
Repeating navigation inside a retained process is not a cold cycle.

The local target is the loopback fixture:

```html
<a data-presenter-id="nav-tomorrow" href="./tomorrow.html">Завтра</a>
<h1 data-presenter-id="tomorrow-ready">Завтра</h1>
```

The live target is the exact immutable `/zavtra/` URL supplied to the run.
Compatibility and live smoke remain separate metrics.

## Acceptance

A candidate passes only with all of:

- local compatibility `20/20`;
- live-site smoke `5/5`;
- successful offline self-test and path matrix;
- exact manifest/package/browser hashes;
- genuine strict `locator.click()`;
- zero installs, admin prompts and runtime downloads;
- zero system-browser/cache fallback;
- zero orphan Node/browser child processes.

`19/20`, `4/5`, one crash, a locked profile needing manual removal, an admin/dev
shell requirement or a missing system dependency is **FAIL**. If neither
candidate passes, report `PLAYWRIGHT_ON_TARGET_WIN10_NO_GO` and stop before M1.
If both pass, the newer candidate wins only when stability is equal and it adds
no system requirement.

## Evidence handoff

The output package must contain:

```text
M0-REPORT.md
M0-REPORT.json
VERSIONS.json
RELEASE-MANIFEST.json
SHA256SUMS.txt
SYSTEM-INFO.json
runs/<candidate>/compatibility/run-001.json ... run-020.json
runs/<candidate>/live/run-001.json ... run-005.json
screenshots/
traces-on-failure/
logs/
```

Each run record includes target/profile, timestamps and exit codes, relative
browser executable, locator/action and marker assertion, download/fallback
flags, cleanup snapshot and result. Keep a log for every run and capture a
trace, screenshot and process snapshot on every failure.

Generated evidence belongs under `artifacts/codex/autopresenter/<m0-run-id>/`
and is not committed. The report must say `targetExecutionPending`/non-pass
until the complete evidence package is produced on the target laptop. A green
Linux/CI run is implementation evidence only and never a Windows M0 PASS.
