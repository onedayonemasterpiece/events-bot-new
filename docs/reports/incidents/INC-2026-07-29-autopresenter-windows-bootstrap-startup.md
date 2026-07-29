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

## Root Cause

1. The C# methods created with `Add-Type` were declared `internal`.
2. Windows PowerShell's static member invocation surface exposes public methods,
   so `[FirstTestConsoleMode]::GetStdHandle(...)` failed even though compilation
   succeeded.
3. The regression test checked that the method name existed in source but did
   not require public visibility or a best-effort console-mode fallback.

## Contributing Factors

- no target-Windows execution was available before owner handoff;
- runtime/dependency storage was scoped to the extracted ZIP rather than to a
  stable per-user cache.

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
  and the stable versioned cache;
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

## Corrective Actions

- move versioned Node, lockfile-keyed npm dependencies and managed browsers to
  `%LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1`;
- teach the agent to resolve Playwright from that explicit shared dependency
  root;
- add regression assertions for member visibility, fallback and cache reuse.

## Follow-up Actions

- [ ] Owner: run the newly published ZIP on the target Windows laptop and
  return the successful first-start/cache-reuse evidence.
- [ ] Release owner: merge the delivered source into `origin/main`; incident
  closure is blocked until the deployed fix is reachable from main.

## Release And Closure Evidence

- deployed SHA: `c1810fa33a930718106ca55a7f0b138b1f8054e8`
- deploy path: manual Fly deploy from clean Autopresenter integration worktree
- Fly image:
  `kenigevents-autopresenter:deployment-01KYPKTH8CZBE7NCV6DB9F0K60`
- refreshed ZIP SHA-256:
  `e5365c71f3b295585f6d0812231b07576fb0096d588aa1e8271c1dbeb652c322`
- regression checks: bootstrap 4/4, agent 12/12, relay 8/8; syntax checks and
  `git diff --check` passed; no `tools/autopresenter/m0/**` diff
- post-deploy verification: Fly machine `2879209fd9e998` version 6 is started
  with 1/1 checks; downloaded ZIP contains public P/Invoke methods, shared-cache
  bootstrap and shared dependency loader; public E2E completed on event `5296`
  and the separate stop lifecycle ended at `idle / agent confirmed stopped`
- remaining closure blockers: target-Windows successful start/cache reuse
  evidence and reachability from `origin/main`

## Prevention

The Windows bootstrap contract now fails if P/Invoke members become non-public,
if console setup becomes fatal again, or if runtime paths drift back into each
ZIP extraction. Target-Windows empirical smoke remains mandatory before
changing this incident from `open`.
