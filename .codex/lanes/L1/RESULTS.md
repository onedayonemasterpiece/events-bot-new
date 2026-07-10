# Lane L1 Results

## Scope

- Lane: `L1`
- Branch: `agent/region-talk/l1-candidate-report`
- Base SHA: `7c8fdc5bd2c1db590dec773f0563bbc8fc2647e8`
- Assigned requirements: durable batched Telegram entity cache and metrics; cached-first controlled uncached resolve lane; exact-post retry ordering; stable `queue_seq` repair; keyword evidence isolation; pure fail-closed publication eligibility.

## Implementation evidence

- Inspected the uncommitted owned-file diff in `/home/dev/projects/events-bot-new-region-talk-bge-m3-test` read-only.
- Added row-level `telegram_entity_cache_item` persistence/load, dirty batching/final flush, and cache write/resolve-lane metrics.
- Added cached-first source selection and a one-item controlled uncached queue lane; quota is consumed only after seed validation and deduplication.
- Persisted and honored exact-post `next_attempt_after`; exact-post work is ordered fresh/cache-resolvable first.
- Added immutable `queue_seq` admission metadata, stable missing/duplicate repair, full-read/truncation refusal, and full durable repair-write safeguards without changing queue-order values during admission repair.
- Fixed per-source keyword evidence capture so loop state cannot leak between sources.
- Added `publication_eligibility(row, authoritative_source=None)` with the exact five-key result contract and tri-state source verdict; unknown/local/spam fail closed, confirmed nonlocal/mixed-external may pass existing strict source/text/vector/image gates.
- Added focused unit coverage for all above behavior.

## Commands and checks

- `git -C /home/dev/projects/events-bot-new-region-talk-bge-m3-test diff -- kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py tests/test_region_talk_candidate_report.py` — reference inspected only.
- `python3 -m py_compile kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py tests/test_region_talk_candidate_report.py` — PASS.
- `git diff --check` — PASS.
- Focused new/regression unittest selection: 9 tests — PASS.
- Additional cache/queue/publication regression selection: 12 tests — PASS.
- Full `unittest` attempt: 131 tests ran; not clean (3 failures, 4 errors). Environment lacked `openpyxl` and blocked auto-install under PEP 668; remaining failures included pre-existing broad report/LLM expectations. The previously exposed queue-handoff regression was corrected and passed in the focused rerun.
- `pytest` was unavailable in the system Python (`No module named pytest`).

## Risks

- No live YDB or Telegram calls were made; row-level cache and full-read safeguards are unit/static validated only.
- The full test module remains non-green in this host environment as recorded above; focused tests covering lane changes pass.
- Canonical docs and changelog were intentionally not edited because this worker lane forbids them; integrator must synchronize documentation.

## Changed files

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `tests/test_region_talk_candidate_report.py`
- `.codex/lanes/L1/RESULTS.md`

## Final SHA

- Implementation head SHA: `fd1a60fa870815df711645affd609a6b40d31edb`
