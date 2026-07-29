# INC-2026-07-29 Autopresenter Windows bootstrap startup failure

Status: monitoring
Severity: sev1
Service: Autopresenter owner-only Internet first test
Opened: 2026-07-29
Closed: —
Owners: Autopresenter implementation / release
Related incidents: —
Related docs: `docs/features/static-site-pages/auto-present/README.md`,
`docs/operations/release-governance.md`

## Summary

The freshly published Windows first-test ZIP failed before dependency bootstrap:
Windows PowerShell could compile the `FirstTestConsoleMode` C# type, but could
not invoke its non-public `GetStdHandle` method. The owner therefore could not
start the demonstrator.

## User / Business Impact

- the owner-facing first test was blocked at launch;
- the advertised automatic install/fullscreen path was not reached;
- repeated debug ZIPs would also have stored their runtime inside each
  extraction, causing unnecessary repeated downloads.

## Detection

The owner supplied a Windows console screenshot showing:
`[FirstTestConsoleMode] does not contain a method named 'GetStdHandle'`.
The current static bootstrap tests did not exercise PowerShell member
visibility and therefore missed the defect.

## Timeline

- 2026-07-29 09:00 UTC — corrected first-test ZIP deployed.
- 2026-07-29 09:28 UTC — owner reported immediate Windows startup failure.
- 2026-07-29 09:34 UTC — root cause localized to non-public P/Invoke methods;
  corrective implementation started.
- 2026-07-29 09:39 UTC — corrected ZIP deployed; public health and package
  inspection passed.
- 2026-07-29 09:40 UTC — exact-source public Run → Completed and
  Reset/Run/Stop lifecycle passed; incident moved to monitoring pending the
  target-Windows retry.
- 2026-07-29 09:57 UTC — owner confirmed bootstrap and shared-cache install
  succeeded on Windows; scenario then stopped at the event rail when Node
  emitted `MaxListenersExceededWarning`.
- 2026-07-29 10:05 UTC — listener leak and PowerShell native-stderr promotion
  were localized; incident reopened.
- 2026-07-29 10:07 UTC — listener cleanup, native exit-code handling and UTF-8
  fix deployed in a refreshed ZIP.
- 2026-07-29 10:09 UTC — exact-source public scenario completed through the
  event rail and detail description with zero bytes on agent stderr; separate
  Reset/Run/Stop lifecycle passed. Incident returned to monitoring for the
  target-Windows retry.

## Root Cause

1. The C# methods created with `Add-Type` were declared `internal`.
2. Windows PowerShell's static member invocation surface exposes public methods,
   so `[FirstTestConsoleMode]::GetStdHandle(...)` failed even though compilation
   succeeded.
3. The regression test checked that the method name existed in source but did
   not require public visibility or a best-effort console-mode fallback.
4. `abortableDelay()` attached an abort listener for each delay but removed it
   only on abort, not after a normal timeout; the eleventh listener caused
   Node's `MaxListenersExceededWarning`.
5. Windows PowerShell ran with `$ErrorActionPreference = "Stop"` and merged
   native stderr into its pipeline, promoting that warning into a terminating
   bootstrap error even though the Node process had not returned a failing exit
   code.

## Contributing Factors

- no target-Windows execution was available before owner handoff;
- runtime/dependency storage was scoped to the extracted ZIP rather than to a
  stable per-user cache.
- public Linux E2E asserted final state and process exit, but did not fail on
  stderr warnings and did not reproduce Windows PowerShell native-error
  semantics;
- Windows console code page was not explicitly UTF-8, producing mojibake in
  Russian event-title diagnostics.

## Automation Contract

### Treat as regression guard when

- changing the Windows first-test CMD/PowerShell bootstrap;
- changing portable Node, npm, Playwright or managed-browser packaging/cache;
- publishing a new Autopresenter owner-test Windows ZIP.

### Affected surfaces

- `tools/autopresenter/prototype/first-test/bootstrap.ps1`;
- dynamic `/api/download/windows-test.zip` packaging;
- `tools/autopresenter/agent/agent.mjs` dependency loading;
- Windows console mode, `%LOCALAPPDATA%` shared cache and first-test launch.

### Mandatory checks before closure or deploy

- bootstrap contract tests require public console methods, best-effort fallback
  and the stable versioned cache; native stderr alone must not own launcher
  failure;
- abort utility tests execute at least 20 sequential delays on one signal and
  prove zero retained abort listeners after every normal completion;
- agent and relay suites pass;
- refreshed ZIP contains the corrected bootstrap and dependency-aware agent;
- public exact-HEAD Run → Completed and Reset/Run/Stop lifecycle pass;
- `tools/autopresenter/m0/**` remains unchanged;
- owner Windows smoke starts a freshly extracted ZIP without the reported
  method error, then confirms a later compatible ZIP reuses cached Node,
  dependencies and browser without downloading them again.

### Required evidence

- source SHA and Fly image;
- bootstrap/agent/relay test outputs;
- refreshed ZIP SHA-256 and entry inspection;
- public relay E2E completion detail;
- owner Windows `latest.log` or screenshot from first fixed start and cache
  reuse start;
- main reachability before incident closure.

## Immediate Mitigation

- make all three console P/Invoke methods public;
- make console-mode adjustment best-effort so a console API limitation cannot
  block the demonstrator.
- remove abort listeners after both normal delay completion and cancellation;
- make the native Node exit code, not warning text on stderr, own launcher
  failure.

## Corrective Actions

- move versioned Node, lockfile-keyed npm dependencies and managed browsers to
  `%LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1`;
- teach the agent to resolve Playwright from that explicit shared dependency
  root;
- add regression assertions for member visibility, fallback and cache reuse.
- force UTF-8 for CMD/PowerShell native output and omit the unused Chromium
  headless shell from new headed-browser installs.

## Follow-up Actions

- [ ] Owner: run the newly published ZIP on the target Windows laptop and
  return the successful first-start/cache-reuse evidence.
- [ ] Release owner: merge the delivered source into `origin/main`; incident
  closure is blocked until the deployed fix is reachable from main.

## Release And Closure Evidence

- deployed SHA: `613fc30a27d8af31bf9cc4c1b75a2b02f394fc21`
- deploy path: manual Fly deploy from clean Autopresenter integration worktree
- Fly image:
  `kenigevents-autopresenter:deployment-01KYPNF3FNFVAHXVNXD1ZFRCWA`
- refreshed ZIP SHA-256:
  `7a56fdfd7549ff790f5cc78e35863dbea2e4db76f08bbe1d3c90fb2631ce65ac`
- regression checks: bootstrap 4/4, agent 14/14 (including 25 sequential
  same-signal delays and abort cleanup), relay 8/8; syntax checks and
  `git diff --check` passed; no `tools/autopresenter/m0/**` diff
- post-deploy verification: Fly machine `2879209fd9e998` version 7 is started
  with 1/1 checks; downloaded 11-entry ZIP contains abort cleanup, UTF-8
  launcher, native exit-code handling and shared dependency loader; public E2E
  completed through the event rail on event `5296`, agent stderr remained
  empty, and the separate stop lifecycle ended at
  `idle / agent confirmed stopped`
- remaining closure blockers: target-Windows successful start/cache reuse
  evidence and reachability from `origin/main`

## Prevention

The Windows bootstrap contract now fails if P/Invoke members become non-public,
if console setup becomes fatal again, or if runtime paths drift back into each
ZIP extraction. Target-Windows empirical smoke remains mandatory before
changing this incident from `open`.
