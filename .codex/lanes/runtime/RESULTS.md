# Lane runtime Results

## Status

committed

## Requirement IDs

- R01
- R02
- R05
- R07

## Branch

`agent/autopresenter-m0/runtime`

## Worktree

`/home/dev/projects/events-bot-new-autopresenter-m0-runtime`

## Base SHA

`981aebd9d9179b3985e5fc10055ea96251997ec3`

## Head SHA

Runtime implementation commit: `486a24efaca022cf881f99c848ba705cdd427e26`.
The final branch also contains the subsequent results-only commit that adds this file.

## Files changed

- `tools/autopresenter/m0/package.json`
- `tools/autopresenter/m0/package-lock.json`
- `tools/autopresenter/m0/fixture/index.html`
- `tools/autopresenter/m0/fixture/zavtra/index.html`
- `tools/autopresenter/m0/src/cli-options.js`
- `tools/autopresenter/m0/src/cycle-worker.js`
- `tools/autopresenter/m0/src/errors.js`
- `tools/autopresenter/m0/src/evidence-record.test.js`
- `tools/autopresenter/m0/src/fixture-server.js`
- `tools/autopresenter/m0/src/json-file.js`
- `tools/autopresenter/m0/src/portable-contract.js`
- `tools/autopresenter/m0/src/process-inspector.js`
- `tools/autopresenter/m0/src/run-cycle.js`
- `tools/autopresenter/m0/src/run-plan.js`
- `tools/autopresenter/m0/src/run-suite.js`
- `tools/autopresenter/m0/src/runtime-contract.test.js`
- `tools/autopresenter/m0/src/self-test.js`
- `.codex/lanes/runtime/RESULTS.md`

## Requirement evidence

- **R01:** `run-plan.js` fixes the credited matrix at 10 fresh local profiles,
  10 runs sharing one persistent local profile, and a distinct fresh Node child
  plus `launchPersistentContext` managed-browser process per cycle.
- **R02:** `fixture-server.js` binds only `127.0.0.1` and serves the committed
  `nav-tomorrow` to `tomorrow-ready` navigation. `run-suite.js` reports local
  20/20 and live 5/5 as separate metrics; live input must be immutable HTTPS
  `/_review/<build>/zavtra/` with exact unique selectors.
- **R05:** process discovery is executable-path and process-identity scoped
  beneath the candidate's portable `browsers/` directory. Each cycle records
  fresh browser discovery, bounded graceful close, path-scoped forced cleanup
  if needed, remaining browser orphans, fresh Node exit code, and worker
  termination. It never enumerates or kills browser processes by generic
  product name.
- **R07:** `self-test.js` is an offline loopback test covering portable Windows
  10 x64 evidence, portable Node location, exact Node/browser manifest hashes,
  data/log probe-file create/read/delete, managed headed launch from
  `about:blank`, strict fixture `locator.click()`, trace/screenshot, and clean
  browser shutdown. It records that target site, relay, npm, downloads, system
  browser, install, and admin access were not used.
- Runtime resolution is fail-closed: `process.execPath` must be beneath
  `runtime/`, `playwright-core` beneath `app/`, and the real browser executable
  beneath the release `browsers/` directory. No `channel`, browser install, or
  system/global fallback exists.
- Per-run `run.json` projection matches the evidence lane's strict flat field
  contract, including nullable local `liveRouteSuccess` /
  `liveContentSuccess`.

## Commands run

From `tools/autopresenter/m0`:

```text
npm test
npm run check
for f in src/*.js; do node --check "$f"; done
node src/run-suite.js --help
node src/self-test.js --help
git diff --check
```

## Tests / verification

- `npm test`: **7 passed, 0 failed**.
- Fixture test verified IPv4 loopback binding, `Cache-Control: no-store`,
  `data-presenter-id="nav-tomorrow"`, and
  `data-presenter-id="tomorrow-ready"`.
- Unit coverage verified the exact 10 fresh / 10 persistent / 5 live plan,
  immutable live URL rejection rules, managed-browser path containment,
  Windows 10 vs Windows 11/Linux gate behavior, strict per-run evidence keys,
  and independent local/live thresholds.
- All runtime JavaScript passed `node --check`.
- Both CLI help paths exited successfully without requiring cwd, Playwright, or
  a browser.
- `git diff --check` passed.

## Risks

- No target Windows 10 laptop or prepared portable candidate was available in
  this lane, so no real browser cold-cycle, live-site 5/5, self-test PASS, or
  Windows 10 compatibility PASS is claimed.
- The integrated packaging script observed during coordination copies
  `tools/autopresenter/m0/src/` into `app/src/`, but the runtime expects the
  committed fixture at `app/fixture/`. The integrator must make the candidate
  builder also copy `tools/autopresenter/m0/fixture/` to `app/fixture/`; without
  that packaging integration change local compatibility and self-test fail
  closed with `FIXTURE_INCOMPLETE`.
- Live selectors remain release inputs because the current review build has no
  `data-presenter-*` hooks. The optional marker defaults to the exact click
  selector, while click and post-click success selectors remain mandatory.
- Windows process evidence uses normal-user PowerShell
  `Get-CimInstance Win32_Process`; target execution must confirm executable
  paths are visible to the same user. Probe failure is a hard run failure.

## Merge notes

- Cherry-pick `486a24efaca022cf881f99c848ba705cdd427e26`, then the
  results-only commit.
- Packaging must copy `fixture/` into release `app/fixture/`.
- The runtime entrypoints agreed with packaging are
  `app/src/run-suite.js` and `app/src/self-test.js`; both require an absolute
  `--portable-root`. The existing optional fourth live marker launcher argument
  is compatible: when omitted, the runtime uses the exact click selector as
  the pre-click marker.
- Do not interpret Linux unit success or `runtimeChecksPassed` as M0 acceptance;
  the suite report always states
  `REQUIRES_TARGET_WINDOWS_10_EVIDENCE_AGGREGATION`.
