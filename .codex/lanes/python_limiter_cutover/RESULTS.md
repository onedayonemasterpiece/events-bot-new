# Lane python_limiter_cutover Results

## Status
committed

## Requirement IDs
- R04

## Branch
`agent/python-limiter-cutover`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/python-limiter-cutover`

## Base SHA
`86a0a8382f0dd9cbb644cd02540bf503e012332c`

## Head SHA
Implementation commit: `69c24209f06b9b79dd53a0cbe681fafd178b1b29`

The lane has one additional results-only commit containing this report; its SHA is
reported in the handoff because a commit cannot contain its own SHA.

## Files changed
- `google_ai/limiter_supabase.py`
- `tests/test_google_ai_limiter_supabase.py`
- `contour_svg/llm_gateway.py`
- `event_media.py`
- `festival_web_research/runtime.py`
- `guide_excursions/dedup.py`
- `guide_excursions/digest_writer.py`
- `guide_excursions/enrich.py`
- `handlers/admin_assist_cmd.py`
- `handlers/kenigsberg_stories_cmd.py`
- `interest_clubs.py`
- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `kaggle/TelegramMonitor/telegram_monitor.py`
- `main.py`
- `main_part2.py`
- `poll_to_forward.py`
- `region_talk_llm_runtime.py`
- `scripts/run_festival_web_research.py`
- `scripts/sync_event_search_vectors_to_supabase.py`
- `site/scripts/export-production-preview-data.py`
- `smart_event_update.py`
- `video_announce/poster_overlay.py`
- `video_announce/scenario.py`
- `.codex/lanes/python_limiter_cutover/RESULTS.md`

## Commands run
- `git worktree add -b agent/python-limiter-cutover ... 86a0a838`
- `rg` inventories for every Python `GoogleAIClient(...)` construction and Supabase builder.
- `python3 -m py_compile ...` for every changed Python file.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest ...` focused test commands listed below.
- `git diff --check`
- Forbidden-file audit against `google_ai/client.py`, migrations/Edge Function, `docs/`, `fly.toml`, and `CHANGELOG.md`.

No network, Google provider, Supabase, deployment, key-reading, or push command was run.

## Tests / verification
- PASS: `tests/test_google_ai_limiter_supabase.py` — 12 passed.
- PASS: `tests/test_event_vector_sync.py` — 16 passed (27 total with the then-11-case helper suite).
- PASS: directly affected Kenigsberg, Smart Update, and video partner construction compatibility tests — 3 passed.
- PASS: `tests/test_google_ai_client.py` — 34 passed.
- PASS: `tests/test_tg_event_publish.py::test_tg_event_hook_rewrite_keeps_useful_non_question` — 1 passed.
- PASS: `python3 -m py_compile` for all 24 changed Python files.
- PASS: `git diff --check`.
- The broader exploratory selection reached one pre-existing, date-sensitive failure:
  `tests/test_kenigsberg_stories.py::test_recent_music_exclusions_uses_issue_manifest_history` expects a 2026-05-13 fixture to be within 30 days, which is false on the current 2026-07-31 clock. The directly affected Kenigsberg gateway test passes.

## Behavior / evidence
- `GOOGLE_AI_LIMITER_SUPABASE_URL` and
  `GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY` are treated as one atomic pair.
- The dedicated pair is resolved before any explicit legacy fallback factory.
- Partial configuration, invalid origin, client import/construction failure, and
  a client factory returning `None` fail closed without exposing the key.
- When both dedicated variables are absent, only a caller-supplied legacy
  factory may reuse the former limiter client during rollout/dev.
- General Supabase storage, personalization, Auth, and other non-limiter clients
  were not changed.

## Remaining consumers
- All Python production/Kaggle `GoogleAIClient(...)` construction sites found by
  the repository inventory now resolve their limiter backend through the helper.
- `scripts/inspect/audit_future_event_vectors.py` remains on direct legacy
  `SUPABASE_*`; it is an inspection-only audit utility, not a production/Kaggle
  construction site.
- The usage example inside `google_ai/client.py` remains unchanged because that
  file was explicitly forbidden.

## Risks
- `google_ai/client.py::_reserve_via_direct_rest()` still reads legacy
  `SUPABASE_URL` / `SUPABASE_SERVICE_KEY`. The file was explicitly forbidden in
  R04. The integrator must ensure this legacy direct-REST fallback is disabled
  or retargeted before dedicated-project release so an RPC-missing retry cannot
  cross back to the core project.
- Rollout correctness depends on production/Kaggle secret propagation of the
  dedicated pair. The explicit legacy fallback remains for dev/transition; the
  separately owned capability gate must prevent production from silently using
  it.

## Merge notes
Cherry-pick the implementation commit and the immediately following results-only
commit. No forbidden files or unrelated Supabase consumers are included.
