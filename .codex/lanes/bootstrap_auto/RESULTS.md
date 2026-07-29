# Lane `bootstrap_auto` results

## Status

- **Requirement R01: Done (implementation and deterministic contract verification).**
- Branch: `agent/autopresenter-first-test-ux/bootstrap-auto`
- Worktree: `/home/dev/projects/events-bot-new-autopresenter-bootstrap-auto`
- Base SHA: `e634403db817b3ac4c7fed4e5781f0a13ad0de2b`
- Implementation head SHA: `a9067b546cb71ec38b41a05879049ce57d87a8d2`

## Delivered

- Diagnosed the `Выбрать ...` console title as Windows Console QuickEdit/selection mode pausing the foreground download process.
- Added a Windows `GetConsoleMode` / `SetConsoleMode` guard that clears `ENABLE_QUICK_EDIT_MODE` and sets `ENABLE_EXTENDED_FLAGS` for the current console input buffer only.
- Restores the original console mode after the agent exits or bootstrap failure; no registry, admin, or persistent console setting mutation is used.
- Starts Windows PowerShell with `-NonInteractive` and supplies CI/non-interactive npm settings before `npm ci` and Playwright browser installation.
- Preserved ordered automatic launch of `agent.mjs`, error-to-`latest.log` diagnostics, launcher failure messaging, and `FIRST_TEST_NOT_M3` checks.
- Updated the bundled first-test README to state that first install proceeds without keyboard confirmation and that the console setting change is temporary/current-window-only.

## Changed files

- `tools/autopresenter/prototype/first-test/bootstrap.ps1`
- `tools/autopresenter/prototype/first-test/START-DEMONSTRATOR.cmd`
- `tools/autopresenter/prototype/first-test/README-FIRST-TEST.txt`
- `tools/autopresenter/prototype/first-test/tests/test_bootstrap_contract.py`
- `.codex/lanes/bootstrap_auto/RESULTS.md`

## Verification evidence

Passed:

```text
python3 -m unittest discover -s tools/autopresenter/prototype/first-test/tests -v
Ran 3 tests in 0.001s — OK

git diff --check
PASS
```

The tests deterministically verify that current-console mode handling precedes installation, the QuickEdit flag is removed while extended flags remain set, the original mode is restored, registry mutation is absent, install ordering remains npm -> Playwright -> agent, non-interactive settings are applied before npm, and release/failure contracts remain present.

Additional packaging-suite probe:

```text
python3 -m unittest tools.autopresenter.relay.tests.test_server -v
BLOCKED in this worktree: ModuleNotFoundError: No module named 'aiohttp'
```

This was an environment dependency absence, not a test assertion failure. No second speculative retry was made.

Reference contract checked against Microsoft `SetConsoleMode` documentation: disabling QuickEdit requires `ENABLE_EXTENDED_FLAGS` without `ENABLE_QUICK_EDIT_MODE`. Playwright's documented `install chromium` flow and existing pinned CLI invocation remain unchanged.

## Risks / remaining validation

- The lane ran on Linux, so the Windows Console Host behavior and the full network download/GUI launch were not live-executed here. A clean Windows x64 first-install smoke test should confirm no selection-mode pause and automatic browser launch.
- If the bootstrap is invoked without a real console input handle (outside the supported double-click launcher path), it now fails explicitly with a logged diagnostic rather than continuing without pause protection.

## Merge notes

- Merge/cherry-pick implementation commit `a9067b54` plus the subsequent results commit.
- No changes were made outside the assigned writable scope.
- No push was performed.
