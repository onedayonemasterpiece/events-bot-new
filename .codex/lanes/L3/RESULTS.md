# Lane L3 Results

## Status
committed

## Requirement IDs
- R01 authoritative source join by canonical source key
- R02 normalized post URLs
- R03 fail-closed publication eligibility and Gemini bypass
- R04 eligibility verdict/evidence/gate-version persistence
- R05 tombstone/revoke transitions
- R06 URL-level never-finalized/retry-due triggering
- R07 terminal/retryable Gemini state and retry metadata
- R08 terminal `no_text` budget protection
- R09 opt-in-only public `t.me/s` fallback

## Branch
`agent/region-talk/L3`

## Worktree
`/home/dev/projects/events-bot-new-region-talk-l3`

## Base SHA
`7c8fdc5bd2c1db590dec773f0563bbc8fc2647e8`

## Head SHA
Implementation commit: `a5d07cd33ea8bc5b4b28bfa6eb5aaa4fd4a6e7e0`

## Files changed
- `scripts/region_talk_publication_finalizer.py`
- `tests/test_region_talk_publication_finalizer.py`

This `RESULTS.md` is required lane metadata and is not part of the owned runtime/test change set.

## Commands run
- `git status --short --branch`
- `git rev-parse HEAD`
- read-only inspection of the integration worktree status and committed Region Talk helper surfaces
- `python3 -m unittest -v tests.test_region_talk_publication_finalizer`
- `python3 -m py_compile scripts/region_talk_publication_finalizer.py tests/test_region_talk_publication_finalizer.py`
- `git diff --check`
- `python3 -m pytest -q tests/test_region_talk_publication_finalizer.py` (not runnable: system Python has no `pytest` module)

## Tests / verification
- PASS: 8 focused `unittest` tests.
- PASS: Python bytecode compilation for both owned files.
- PASS: `git diff --check`.
- Covered canonical source-key joins, Telegram URL normalization, URL-level terminal/retry selection, eligibility helper arguments and persisted gate fields, unknown/local/spam Gemini bypass, revoke/tombstone durability, attempt/retry metadata, terminal `no_text`, and fallback opt-in.

## Risks
- The base SHA does not yet define the concurrently agreed `rt.publication_eligibility(row, authoritative_source)` helper. L3 calls that API directly; missing/raising helpers fail closed to review and never call Gemini. Integration must include the helper-owning lane before release.
- No live YDB or Gemini call was made; verification is focused and mocked by design.
- Public fallback is available only with `REGION_TALK_ALLOW_PUBLIC_TME_S_FALLBACK=true`; default behavior relies on canonical YDB/Telethon exact-fetch text.

## Merge notes
- Cherry-pick implementation commit `a5d07cd33ea8bc5b4b28bfa6eb5aaa4fd4a6e7e0`.
- Merge after the CandidateReport lane providing `publication_eligibility` or resolve that dependency in the serial integrator.
- The integration worktree `/home/dev/projects/events-bot-new-region-talk-bge-m3-test` was inspected read-only and its pre-existing CandidateReport draft was not edited.
