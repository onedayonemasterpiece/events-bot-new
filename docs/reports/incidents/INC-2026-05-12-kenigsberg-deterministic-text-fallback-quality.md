# INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-command-silent-during-gemma-retry`, `INC-2026-05-12-kenigsberg-music-range-overrun-into-vocals`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/release-governance.md`

## Summary

Kenigsberg issue `#3` launched with `text=fallback` after the LLM rewrite timed out. The deterministic text splitter produced visibly broken semantic cuts, which violated the feature requirement that the thought be rewritten and paced by an LLM before publication.

## User / Business Impact

- The test story in `@keniggpt` carried awkward, meaning-breaking text screens.
- The operator could see `text=fallback`, but that fallback still launched Kaggle and published a poor result.
- Production `@mostvkenig` auto-publishing was not enabled, so impact stayed in the test channel.

## Detection

- The operator reported that `text=fallback` produced terrible semantic splitting and that deterministic slicing is harmful.
- Runtime logs for 2026-05-12 show issue `#3` accepted, text rewrite warning after timeout, and Kaggle session `#257` completing anyway.

## Timeline

- 2026-05-12 13:13:57 UTC — `/kenigsberg` accepted issue `#3` / thought `17`.
- 2026-05-12 13:14:42 UTC — text rewrite fell back after timeout.
- 2026-05-12 13:17:27 UTC — Kaggle session `#257` completed.
- 2026-05-12 13:17:29 UTC — issue manifest registered for issue `#3`.
- 2026-05-12 13:20 UTC — operator reported the poor fallback text quality.

## Root Cause

1. The previous mitigation for silent commands added a deterministic fallback after Gemma 4 timeout.
2. The fallback split by words/short chunks rather than preserving semantic units.
3. The renderer also retained a deterministic `payload_scene_lines` fallback if the server payload did not contain LLM scene lines.
4. Gemma 4 was a poor fit for this small rewrite step because provider retries/timeouts made fallback likely.

## Contributing Factors

- The first MVP treated fallback as availability protection instead of a quality risk.
- Tests pinned timeout-to-fallback behavior, which was the wrong product contract.

## Automation Contract

### Treat as regression guard when

- Changing Kenigsberg text rewrite model selection.
- Changing `/kenigsberg` launch flow around text preparation.
- Changing renderer `payload_scene_lines` or any text fallback logic.

### Affected surfaces

- `handlers/kenigsberg_stories_cmd.py`
- `scripts/render_kenigsberg_story.py`
- `tests/test_kenigsberg_stories.py`
- `docs/features/kenigsberg-stories/README.md`

### Mandatory checks before closure or deploy

- Text rewrite uses `gemini-3.1-flash-lite` as the direct primary model.
- No Gemma 4 call is made by the Kenigsberg text rewrite step.
- LLM result must include a hook and at least two scene lines.
- If LLM text preparation fails, `/kenigsberg` must answer the operator and not launch Kaggle.
- Renderer must fail if `scene_lines` are absent instead of splitting thought text deterministically.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Test output.
- Post-deploy `/healthz`.
- Fresh `/kenigsberg` smoke evidence showing `text=llm_gemini_lite` or a fail-closed operator message, never `text=fallback`.

## Immediate Mitigation

- Remove deterministic story text fallback from the launch path.
- Switch the rewrite model from Gemma 4 to `gemini-3.1-flash-lite`.
- Make the renderer require LLM-provided `scene_lines`.

## Corrective Actions

- Add tests that assert the rewrite calls only `gemini-3.1-flash-lite`.
- Add tests that assert timeout fails closed.
- Add tests that assert missing renderer `scene_lines` raises.

## Follow-up Actions

- [ ] Review the next successful generated story for text pacing before enabling scheduled production.
- [ ] Consider adding a stored `text_review` field to the manifest if manual curation becomes necessary.

## Release And Closure Evidence

- deployed SHA: `c38432b3d4d21b35f773b40a0e657c300c0d8748`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1068`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRE6J59N9ZAW0QAJ5THMGSHB`
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py`
  - `.venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` -> `22 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1068`, service check passing.
  - Production handler contains `TEXT_REWRITE_MODEL = "gemini-3.1-flash-lite"` and fail-closed rewrite logging.
  - Production renderer raises `LLM scene_lines are required; deterministic text splitting is disabled`.
  - Runtime file logging remains enabled: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`.
  - Fresh `/kenigsberg` smoke still required before closure: expected `text=llm_gemini_lite` or fail-closed operator message, never `text=fallback`.

## Prevention

- Fallbacks that degrade semantic quality must fail closed for public/test story generation rather than publishing worse content.
