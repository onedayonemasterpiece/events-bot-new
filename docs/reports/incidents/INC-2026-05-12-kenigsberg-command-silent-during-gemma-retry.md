# INC-2026-05-12-kenigsberg-command-silent-during-gemma-retry

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality`, `INC-2026-05-12-kenigsberg-winter-dataset-not-mounted`, `INC-2026-05-12-kenigsberg-notebook-escaped-newlines`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Production `/kenigsberg` appeared silent to the operator because the command handler did slow pre-launch work before sending any Telegram acknowledgement. The selected thought was sent to Gemma 4, the provider returned `503 UNAVAILABLE`, the client retried, and the update was only marked handled after 142 seconds.

## User / Business Impact

- The operator could not tell whether `/kenigsberg` was accepted.
- A manual MVP path looked broken even though the handler eventually launched Kaggle.
- Existing public event surfaces stayed available; impact was limited to the new Kenigsberg manual generation command.

## Detection

- The operator reported: "бот молчит на эту команду".
- Runtime file mirror was enabled and available at `/data/runtime_logs`.
- Production logs showed `google_ai.call_error` for `consumer=kenigsberg_stories` followed by a retry and `Update id=178986356 is handled. Duration 142160 ms`.

## Timeline

- 2026-05-12 12:55:59 UTC — `/kenigsberg` handler reserved Gemma capacity for `kenigsberg_stories`.
- 2026-05-12 12:56:06 UTC — Gemma 4 returned provider `503 UNAVAILABLE`; retry started.
- 2026-05-12 12:57:52 UTC — retry succeeded and text rewrite completed.
- 2026-05-12 12:58:20 UTC — Telegram update finished after 142 seconds and Kaggle was launched.
- 2026-05-12 13:00 UTC — incident triage started from production runtime logs.

## Root Cause

1. `_launch_kaggle_generation` called `_rewrite_thought_for_story` before sending any operator-visible message.
2. `_rewrite_thought_for_story` delegated to the shared Google AI client without a Kenigsberg-specific hard wall-clock cap.
3. A retryable provider overload made the command look unhandled for more than two minutes.

## Contributing Factors

- The first Kenigsberg MVP prioritized Kaggle handoff and did not pin the UX contract that every manual command must acknowledge before slow external work.
- Runtime logs contained enough Google AI evidence, but there was no Kenigsberg-specific `launch accepted` log before the LLM step.

## Automation Contract

### Treat as regression guard when

- Changing `/kenigsberg` command flow.
- Changing Kenigsberg text rewrite, model choice, or fallback policy.
- Moving slow Kaggle/dataset/LLM work earlier in the handler.

### Affected surfaces

- `handlers/kenigsberg_stories_cmd.py`
- `docs/features/kenigsberg-stories/README.md`
- production runtime logs under `/data/runtime_logs`
- manual Telegram smoke path for `/kenigsberg`

### Mandatory checks before closure or deploy

- `/kenigsberg` sends an acknowledgement before LLM/Kaggle slow work.
- Text rewrite has a hard timeout and fail-closed behavior.
- Regression test covers timeout-to-fail-closed behavior.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.
- Runtime logs after deploy show the new code is present and file logging is enabled.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Test output.
- `/healthz` response after deploy.
- Production file/log evidence for runtime logging and deployed handler code.

## Immediate Mitigation

- Send an immediate Telegram acknowledgement after reserving the issue/thought and before calling the rewrite LLM.
- Add a Kenigsberg rewrite timeout; follow-up incident `INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality` removes deterministic fallback.
- Add a launch-accepted log with issue, thought, chat and user ids.

## Corrective Actions

- Patch `_launch_kaggle_generation` to acknowledge command acceptance before text rewrite.
- Wrap the rewrite LLM call with `asyncio.wait_for(..., timeout=45.0)`.
- Add a unit test that simulates a slow Google AI client and asserts fail-closed behavior.

## Follow-up Actions

- [ ] Consider adding a lightweight operator metric for command acknowledgement latency across long-running admin commands.
- [ ] After the MVP stabilizes, decide whether text rewrite should run in a background task before session creation or remain inline after the immediate ack.

## Release And Closure Evidence

- deployed SHA: `748ca42cff63d7eb4c1de23fe4c9db3531d15049`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1067`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRE4WWNCQ8EXSBAJSK1F6PX6`
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py`
  - `.venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` -> `20 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1067`, service check passing.
  - Production code contains `TEXT_REWRITE_TIMEOUT_SECONDS = 45.0` and `kenigsberg: launch accepted`.
  - Runtime file logging remains enabled: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`.

## Prevention

- Manual operator commands that may call external providers must acknowledge receipt before the first slow network call.
- Kenigsberg LLM quality improvements must preserve fail-closed behavior for unavailable or invalid story text.
