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

On 2026-05-13 the same operator-facing symptom reappeared after the LLM split migration: commands sent around 14:34 Europe/Kaliningrad did not create new `kenigsberg_story` rows and did not produce visible operator status. The immediate fix keeps the current text path intact but changes the launch contract: `/kenigsberg` acknowledges immediately before admin/DB preflight and runs the long preflight/Kaggle handoff in a background task after authorization.

Later the same day, issue `#15` showed that the Kenigsberg text split used the shared GoogleAI fallback chain: Gemini lite returned `503`, the global fallback tried Gemma 4, and both incidents were shown to the operator even though a later Gemini-lite retry produced valid `scene_lines` and the story published. Kenigsberg text splitting is quality-sensitive and should not silently fall back to Gemma 4; it should retry Gemini lite with bounded Kenigsberg-specific delays and fail before Kaggle if no valid split is produced.

On 2026-05-13 16:51-16:52 Europe/Kaliningrad, the fail-closed path became too brittle for manual MVP testing: issues `#17` and `#18` both acknowledged the command but stopped before Kaggle because Gemini lite did not produce a validated safe semantic split. The revised contract keeps Gemini lite as primary, still forbids Gemma 4 for this path, but allows one explicit `gpt-4o` fallback through the existing OpenAI `ask_4o` path before failing closed.

## User / Business Impact

- The operator could not tell whether `/kenigsberg` was accepted.
- A manual MVP path looked broken even though the handler eventually launched Kaggle.
- Existing public event surfaces stayed available; impact was limited to the new Kenigsberg manual generation command.
- The operator could not test the current rendered-story iteration because no new generation session was created after the 2026-05-13 14:34 Europe/Kaliningrad command window.
- The operator saw `kenigsberg_stories` LLM incidents for both Gemini lite and Gemma 4 and could not tell whether generation proceeded with or without a valid text split.
- Two consecutive manual launches stopped before Kaggle when Gemini lite did not return a validated split, making the MVP hard to test despite the rest of the render path being available.

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
- 2026-05-13 12:34 UTC — operator sent `/kenigsberg`; no new `kenigsberg_story` session was created after this timestamp.
- 2026-05-13 13:01-13:26 UTC — operator saw many unrelated LLM incident warnings from `event_parse`, `event_topics`, and `smart_update`, making it unclear whether Kenigsberg had started.
- 2026-05-13 UTC — production DB check confirmed no `kenigsberg_story` sessions after `2026-05-13 12:00:00 UTC`; only stale `default` `CREATED` video rows were present, which must not block Kenigsberg.
- 2026-05-13 13:55 UTC — issue `#15`, session `#271`, accepted. The first text-split call emitted `provider_error_fallback` from Gemini lite to Gemma 4, then `provider_error` from Gemma 4.
- 2026-05-13 13:58 UTC — session `#271` completed as `PUBLISHED_TEST`; DB payload contained valid `text_source=thoughts_md_llm_split` and five `scene_lines`, confirming a later Gemini-lite attempt succeeded and Kaggle did not run without a split.

## Root Cause

1. `_launch_kaggle_generation` called `_rewrite_thought_for_story` before sending any operator-visible message.
2. `_rewrite_thought_for_story` delegated to the shared Google AI client without a Kenigsberg-specific hard wall-clock cap.
3. A retryable provider overload made the command look unhandled for more than two minutes.
4. The later LLM split fix still left the manual command handler awaiting the whole launch preflight/handoff path inside the Telegram update handler after the first acknowledgement point, so any blocking before that point remained operator-visible as silence.
5. The Kenigsberg text-split code instantiated the shared `GoogleAIClient` without disabling `GOOGLE_AI_FALLBACK_MODELS`, so a Gemini-lite outage could automatically call Gemma 4 despite the product requirement to avoid Gemma 4 for this text path.
6. The subsequent Gemini-only fail-closed mitigation protected text quality but had no high-quality fallback, so transient Gemini-lite failures or invalid splits blocked manual testing entirely.

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
- `/kenigsberg` returns from the Telegram update quickly after scheduling the launch background task.
- The background launch path catches pre-Kaggle exceptions and reports them to the operator.
- A concurrent Kenigsberg handoff reports an explicit “previous launch is in preflight/handoff” message instead of going silent.
- CherryFlash/CrumpleVideo/default video sessions do not block `/kenigsberg`; only `profile_key="kenigsberg_story"` active sessions are considered.
- Kenigsberg text split does not use shared GoogleAI fallback models; `client.fallback_models` is empty for `consumer=kenigsberg_stories`.
- Kenigsberg text split does not emit global Telegram LLM incidents on intermediate provider failures; the command reports its own final fail-closed status if all retries fail.
- Kenigsberg text split retries Gemini lite according to `KENIGSBERG_STORIES_TEXT_SPLIT_ATTEMPTS` and `KENIGSBERG_STORIES_TEXT_SPLIT_RETRY_DELAYS_SEC`.
- If Gemini lite does not return a validated split, Kenigsberg may make one explicit `gpt-4o` fallback call through `ask_4o`; it must not use Gemma 4 or the shared `GOOGLE_AI_FALLBACK_MODELS` chain for this path.
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
- Move the long manual launch preflight/Kaggle handoff into a background task after an immediate acknowledgement.
- Disable the shared model fallback chain and global LLM incident notifier for Kenigsberg text splitting; rely on Kenigsberg-specific retries and final operator status.
- Add an explicit `gpt-4o` fallback for text-boundary splitting after Gemini-lite output fails provider or validation checks; keep validation identical and fail before Kaggle if both paths fail.

## Corrective Actions

- Patch `_launch_kaggle_generation` to acknowledge command acceptance before text rewrite.
- Wrap the rewrite LLM call with `asyncio.wait_for(..., timeout=45.0)`.
- Add a unit test that simulates a slow Google AI client and asserts fail-closed behavior.

## Follow-up Actions

- [ ] Consider adding a lightweight operator metric for command acknowledgement latency across long-running admin commands.
- [ ] After the MVP stabilizes, decide whether text rewrite should run in a background task before session creation or remain inline after the immediate ack.

## Release And Closure Evidence

### 2026-05-13 background launch handoff deploy

- deployed SHA: `9fb954134063a2f895429cb07a193322a9f2c43e`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-deploy-9fb95413` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly image: `registry.fly.io/events-bot-new-wngqia:deployment-01KRGSSWT538D9HDXGXDE27MK2`, machine `48e42d5b714228`, Fly version `1076`, checks `1/1` passing.
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py tests/test_kenigsberg_stories.py`
  - `timeout 90 .venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py tests/test_video_announce_story_publish.py` -> `50 passed in 1.26s`
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues.
  - Production `/app/handlers/kenigsberg_stories_cmd.py` contains `_run_launch_in_background`, `_KENIGSBERG_LAUNCH_LOCK`, and the immediate `Kenigsberg: команду получил...` acknowledgement.
  - Production DB evidence before the fix showed no `kenigsberg_story` sessions after `2026-05-13 12:00:00 UTC`; only stale `default` `CREATED` video rows were present, confirming the failure was before Kenigsberg session creation.
- live smoke:
  - Not run by Codex to avoid publishing a new `@keniggpt` story without an explicit operator `/kenigsberg` command.

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

Follow-up hardening deployed for `INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality`:

- deployed SHA: `c38432b3d4d21b35f773b40a0e657c300c0d8748`
- Fly release: `v1068`
- behavior update: rewrite now uses `gemini-3.1-flash-lite` and fails closed instead of deterministic fallback.

## Prevention

- Manual operator commands that may call external providers must acknowledge receipt before the first slow network call.
- Kenigsberg LLM quality improvements must preserve fail-closed behavior for unavailable or invalid story text.
