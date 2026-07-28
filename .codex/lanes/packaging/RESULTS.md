# Lane packaging Results

## Status

committed

## Requirement IDs

- R03
- R04
- R08

## Branch

`agent/autopresenter-m0/packaging`

## Worktree

`/home/dev/projects/events-bot-new-autopresenter-m0-packaging`

## Base SHA

`981aebd9d9179b3985e5fc10055ea96251997ec3`

## Head SHA

Implementation commit: `d1adf4d56a5794f1c8fb37a96d1686c490b9ef03`

The separate commit containing this results record is intentionally not
self-referenced; use `git rev-parse agent/autopresenter-m0/packaging` after
handoff for the lane tip.

## Files changed

- `.codex/lanes/packaging/RESULTS.md`
- `tools/autopresenter/m0/candidates/current-control/candidate.json`
- `tools/autopresenter/m0/candidates/current-control/package.json`
- `tools/autopresenter/m0/candidates/current-control/package-lock.json`
- `tools/autopresenter/m0/candidates/pre-cft-compat/candidate.json`
- `tools/autopresenter/m0/candidates/pre-cft-compat/package.json`
- `tools/autopresenter/m0/candidates/pre-cft-compat/package-lock.json`
- `tools/autopresenter/m0/release-m0/.gitignore`
- `tools/autopresenter/m0/release-m0/templates/start.cmd.in`
- `tools/autopresenter/m0/release-m0/templates/self-test.cmd.in`
- `tools/autopresenter/m0/scripts/build-all-candidates.ps1`
- `tools/autopresenter/m0/scripts/build-candidate.ps1`
- `tools/autopresenter/m0/scripts/verify-packaging.mjs`

No Node archive, browser, `node_modules`, ZIP, or other binary/generated
release artifact was committed.

## Evidence

- Candidate `current-control` is independently locked to portable Node
  `22.12.0` x64, Playwright / `playwright-core` `1.61.1`, managed browser
  revision `1228`, browser `149.0.7827.55`, and
  `browsers/chromium-1228/chrome-win64/chrome.exe`.
- Candidate `pre-cft-compat` is independently locked to portable Node
  `22.12.0` x64, Playwright / `playwright-core` `1.54.2`, managed browser
  revision `1181`, browser `139.0.7258.5`, and
  `browsers/chromium-1181/chrome-win/chrome.exe`.
- Both manifests pin the official Node win-x64 archive SHA-256
  `2b8f2256382f97ad51e29ff71f702961af466c4616393f767455501e6aece9b8`
  and the exact npm package integrities.
- The builder uses the pinned portable `npm.cmd` only during packaging, with
  browser download suppressed during `npm ci`; it then invokes the pinned
  `playwright-core/cli.js` directly to install only the managed headed
  Chromium into the release-local `browsers` path.
- Generated launchers use only packaged `runtime/node.exe`, source
  entrypoints below packaged `app/src`, fixed release-relative browser
  paths, and an explicit absolute `PLAYWRIGHT_BROWSERS_PATH`.
- Launchers set `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1`, preflight the exact
  browser executable, and exit before Node with code 3 if it is absent.
  They contain no npm/npx, browser channel, system browser, PowerShell,
  elevation, or execution-policy path.
- The builder emits `VERSIONS.json`, `RELEASE-MANIFEST.json`,
  `SHA256SUMS.txt`, candidate ZIP SHA-256, portable Node executable SHA-256,
  browser executable SHA-256, package-lock SHA-256, and per-file release
  hashes. It refuses dirty source or pre-existing candidate output.

## Commands run

```text
git status --short --branch
git rev-parse HEAD
sed -n ... docs/README.md docs/routes.yml
sed -n ... docs/features/static-site-pages/auto-present/README.md
sed -n ... .codex/integration/autopresenter-m0-plan.md
npm view playwright@{1.61.1,1.54.2} version dist.integrity dist.tarball --json
npm view playwright-core@{1.61.1,1.54.2} version dist.integrity dist.tarball --json
curl -fsSL https://nodejs.org/download/release/v22.12.0/SHASUMS256.txt
npm install --package-lock-only --ignore-scripts --no-audit --no-fund --prefix <each-candidate>
node tools/autopresenter/m0/scripts/verify-packaging.mjs
JSON.parse(...) for every candidate JSON/lock file
grep ... forbidden runtime launcher tokens
git diff --check
git diff --cached --check
git diff --cached --numstat
git commit -m "Add hermetic Autopresenter M0 Windows candidates"
```

## Tests / verification

- `node tools/autopresenter/m0/scripts/verify-packaging.mjs` — PASS.
  It verifies the two-candidate matrix, exact package-lock versions and
  integrities, the 1.57 boundary metadata, browser paths, fail-closed policy,
  launcher/runtime CLI contract, build-time browser isolation, and the
  absence of binary outputs under `release-m0`.
- Independent JSON parsing of all six candidate JSON files — PASS.
- Candidate lock refresh/validation with `--package-lock-only` — PASS for
  both candidates. The host reported the expected engine warning because
  validation ran on Node `22.22.3`, while target runtime is intentionally
  exact Node `22.12.0`.
- Forbidden launcher token scan (`npm`, `npx`, channel/Edge, elevation,
  execution-policy changes) — PASS.
- `git diff --check` and `git diff --cached --check` — PASS.
- Staged binary check — PASS; all staged paths were text files in lane scope.

## Risks

- No `pwsh`/Windows runtime is installed in the Linux worker, so the
  PowerShell script was statically inspected but not parsed or executed here.
- The actual portable Node/browser download, Windows PowerShell 5.1 build,
  CMD quoting path matrix, browser launch, and Windows 10 compatibility run
  remain required on the target Windows x64 environment. Linux validation
  is not M0 PASS evidence.
- The builder depends on runtime-lane entrypoints
  `app/src/run-suite.js` and `app/src/self-test.js` and their coordinated CLI
  contract. Integration must run the combined static tests after
  cherry-picking both lanes.

## Merge notes

- Cherry-pick the implementation commit and the following results-record
  commit.
- Merge after the runtime lane so `tools/autopresenter/m0/src/run-suite.js`
  and `self-test.js` are present for a real Windows package build.
- The docs/CHANGELOG synchronization is intentionally owned by the docs lane;
  this worker did not edit forbidden shared documentation.
- Do not claim M0 PASS until both exact ZIPs are built and the selected
  candidate passes the canonical target Windows 10 test contract.
