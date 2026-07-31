# Telegram Video Quality + CDN Integration

Original fanout base: `origin/main@c128bde4fe7a7b289c3ed4a64a4fe56d33124ad9`.
The branch was synchronized through `0d6369912767f8e91c6401c0829ab950008ecd81`;
the final release synchronization with current `origin/main` is owned by the
integrator.

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| video-research | R04 | agent/telegram-video-quality/video-research | merged | `b7dd350c` | `b2671830` | `.codex/lanes/video-research/RESULTS.md` |
| video-producer | R01-R05 | agent/telegram-video-quality/video-producer | merged + hardened | `3fb4da23` | `3bb84aac`, `209fe8cd` | `.codex/lanes/video-producer/RESULTS.md` |
| video-persistence | R04,R06,R07 | agent/telegram-video-quality/video-persistence | merged + hardened | `832cd44f` | `d2cdb2cb`, `05af18c5` | `.codex/lanes/video-persistence/RESULTS.md` |
| video-export | R08 | agent/telegram-video-quality/video-export | merged | `0b882925` | `b96a009f`, `8ceab2bc`, `f3368e30`, `22c169c0` | `.codex/lanes/video-export/RESULTS.md` |
| integrator | R01-R08 | integration/telegram-video-quality-cdn | in progress | current branch | owner | full regression/release/live evidence below |

No worker patch was dropped. Integration hardening added an explicit source
republication allowlist, Fernet-encrypted permanent sidecars, a hard six-call
ceiling and two-key minimum, updating relation upserts, canonical `0..1`
confidence validation, and a 24-hour orphan grace/relink contract.

## Validation in progress

- producer/service/persistence/incident focused suites: `76 passed` before the
  last relation-test correction; the isolated persistence + incident replay
  rerun then passed `18/18`;
- earlier integrated feature suite: `70/70`;
- limiter/status/recovery regression: `27/27`, `24/24`; scheduling `38/38`;
- `test_supabase_storage.py`: green `10/10`, with the repository's pre-existing
  non-daemon `aiosqlite` interpreter-shutdown hang recorded separately per the
  Kaggle status runbook;
- notebook regenerated from the canonical `.py`; equality check passed;
- `py_compile` and `git diff --check`: passed.

Final full tests, static build, checklist re-review, main-reachable release and
live DB/CDN/limiter evidence remain pending at this checkpoint.
