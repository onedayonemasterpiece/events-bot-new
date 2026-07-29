# Lane results: persistent_runtime

## Scope

- Lane: `persistent_runtime`
- Requirements: `R02`, `R03`
- Base SHA: `49115f6c`
- Implementation SHA: `ad7bab77862c58714b6251eca98ad00d78ce7eb8`
- Branch: `agent/autopresenter-continuous-outro/persistent-runtime`

## Outcome

- Browser, BrowserContext, page, and presentation window are now created once and retained across normal Run, Stop, Reset, and scenario-switch operations. Context closure remains only in terminal shutdown/finalization.
- Removed `freshContext` and normal-scenario context recreation. Every explicit scenario now reloads the existing stage page, clears embedded `localStorage`/`sessionStorage`, and reloads the same iframe for deterministic state.
- A Run received while a scenario is active now publishes a switching state, cooperatively aborts, waits up to the bounded hard-stop deadline, performs same-page recovery if needed, and starts the requested scenario in the same context generation/page. Dispatches are serialized to preserve command order.
- Stop and Reset remain non-terminal; Shutdown/SIGINT/SIGTERM remain terminal.
- Replaced the global 30-second cap with an explicit per-scenario timeout policy: `tomorrow-mobile=30s`, `tomorrow-rail-like=120s`, `weekend-amber-artifact=120s`, with validated capacity for a future explicit one-hour scene.
- Addressed the Windows regression evidence reported for sequence 10 (`tomorrow-rail-like exceeded 30000ms`) by extending the gesture-heavy scenario's explicit bound to 120 seconds.
- Removed the hardcoded scenario-01 primary highlight from the control UI. Highlight/`aria-pressed` now follow `state.current_command.scenario`; Stop/Reset/Shutdown clear the scenario selection because their current command is not Run.

## Changed files

- `tools/autopresenter/agent/README.md`
- `tools/autopresenter/agent/agent.mjs`
- `tools/autopresenter/agent/pacing.mjs`
- `tools/autopresenter/agent/scenario-contract.mjs`
- `tools/autopresenter/agent/test/persistent-runtime.test.mjs`
- `tools/autopresenter/agent/test/static-contract.test.mjs`
- `tools/autopresenter/relay/control/index.html`
- `tools/autopresenter/relay/tests/test_server.py`
- `.codex/lanes/persistent_runtime/RESULTS.md`

## Verification evidence

All required suites passed from the lane worktree:

1. `cd tools/autopresenter/agent && npm test`
   - 21/21 Node tests passed.
   - Includes focused same-context-generation/page sequential Run switch regression and static lifecycle/timeout contracts.
2. `/home/dev/.codex/venvs/events-bot-new/bin/python -m unittest discover -s tools/autopresenter/relay/tests -p 'test_*.py'`
   - 13/13 relay/server tests passed.
3. `node --test tools/autopresenter/relay/tests/*.test.mjs`
   - 2/2 control-auth tests passed.
4. `/home/dev/.codex/venvs/events-bot-new/bin/python -m unittest discover -s tools/autopresenter/prototype/first-test/tests -p 'test_*.py'`
   - 4/4 Windows bootstrap contract tests passed.
5. `git diff --check`
   - Passed.
6. `node --check tools/autopresenter/agent/agent.mjs`
   - Passed.

Initial `python`/system `python3` attempts could not run relay tests because the shell had no `python` command and system Python lacked `aiohttp`; the canonical project venv above was then used successfully.

## Risks / integration notes

- No headed Windows live run was available in this lane. Integration must re-run exact scenario 02 on the Windows agent and verify it completes under the new 120-second bound without changing context generation/window.
- Same-page recovery intentionally does not recreate a wedged BrowserContext, to preserve the persistent-window contract. If Chromium itself is irrecoverably broken, the terminal shutdown/restart path remains the recovery boundary.
- This lane deliberately does not add or bridge the QR outro scene; the integrator owns that work.
- Canonical `docs/` and `CHANGELOG.md` updates are forbidden to this lane and remain integration-owner work.
