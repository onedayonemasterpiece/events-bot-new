# Telegram Video Quality + CDN Integration

Original fanout base: `origin/main@c128bde4fe7a7b289c3ed4a64a4fe56d33124ad9`.
The integrated branch was synchronized with
`origin/main@897dd56631808d1d65a82498312418f35412609f` before final acceptance.

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| video-research | R04 | agent/telegram-video-quality/video-research | merged | `b7dd350c` | `b2671830` | `.codex/lanes/video-research/RESULTS.md` |
| video-producer | R01-R05 | agent/telegram-video-quality/video-producer | merged + hardened | `3fb4da23` | `3bb84aac`, `209fe8cd` | `.codex/lanes/video-producer/RESULTS.md` |
| video-persistence | R04,R06,R07 | agent/telegram-video-quality/video-persistence | merged + hardened | `832cd44f` | `d2cdb2cb`, `05af18c5` | `.codex/lanes/video-persistence/RESULTS.md` |
| video-export | R08 | agent/telegram-video-quality/video-export | merged | `0b882925` | `b96a009f`, `8ceab2bc`, `f3368e30`, `22c169c0` | `.codex/lanes/video-export/RESULTS.md` |
| integrator | R01-R08 | integration/telegram-video-quality-cdn | implementation acceptance passed | current branch | owner | integrated test/build evidence below |

No worker patch was dropped. Integration hardening added an explicit source
republication allowlist, Fernet-encrypted permanent sidecars, a hard six-call
physical-provider-send ceiling with retries/429 rotation disabled, a two-key
minimum, updating relation upserts, canonical `0..1` confidence validation,
and a minimum 24-hour production orphan grace/relink contract. The canonical
research document now distinguishes active v1 score semantics from the future
`ffprobe`/human-calibration plan.

## Integrated validation

- final feature/incident batch after acceptance hardening:
  `86 passed in 12.25s`;
- final limiter/storage/Kaggle-status/recovery/scheduling batch:
  `102 passed in 9.70s`;
- incident regression batch covering Kaggle status, Guide service and Telegram
  service: `35 passed in 7.13s`;
- focused physical-send/orphan/documentation hardening batch:
  `91 passed in 7.89s`;
- the repository's pre-existing non-daemon `aiosqlite` interpreter-shutdown
  hang was recorded separately per the Kaggle status runbook; both final
  batches returned `0` through a wrapper which exits after `pytest.main`;
- notebook regenerated from the canonical `.py`; equality check passed;
- `py_compile` and `git diff --check`: passed.
- full preview build: `461` pages, completed in `1m 26s`;
- `check:preview`: passed with `288` events and a backward-compatible empty
  video collection where the source snapshot has no video tables.
- independent final checklist re-review: R01-R08 `Done`; reviewer rerun
  `106 passed in 11.27s`, `py_compile` and `git diff --check` green, with no
  dropped worker patch or material unrelated change.

Main-reachable release and live DB/CDN/limiter evidence are intentionally kept
as release-phase gates after the final checklist re-review.
