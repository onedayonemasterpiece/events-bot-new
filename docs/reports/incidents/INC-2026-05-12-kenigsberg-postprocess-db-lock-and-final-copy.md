# INC-2026-05-12-kenigsberg-postprocess-db-lock-and-final-copy

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality`, `INC-2026-05-12-kenigsberg-assist-ban-routing-and-dominant-range`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

During manual testing, `/kenigsberg` reported that session `#265` was still rendering even though Kaggle had already reached `COMPLETE`. The completed output eventually published, but the operator-facing state was misleading while heavy VK auto-import work held SQLite locks. The same test window also showed `/a kenigsberg покажи список банов` being routed as `/kenigsberg покажи список банов` instead of `/kenigsberg bans`, confirmed that story text should now be treated as final curated copy from `thoughts.md`, not rewritten by an LLM, and exposed visible scene freezes in Kenigsberg `#10`. A later run, session `#266`, reached Kaggle dataset/kernel binding but failed to persist the real Kaggle metadata because SQLite was locked, leaving the row as `RENDERING + local:KoenigsbergStories`. Subsequent visual review showed the same source fragment could reappear later in the same generated story because per-run segment selection avoided only recent files, not already selected source-time ranges. Session `#267` then failed before rendering because the story-ready notebook imported the shared story helper during a normal test run without `story_publish.json`; the helper imports Telethon, which was not installed in the Kenigsberg Kaggle notebook environment.

## User / Business Impact

- The operator could not confidently start the next Kenigsberg test run.
- `/kenigsberg` used the generic “рендерится” wording while the real state was Kaggle complete plus local download/publish finalization.
- A natural `/a` request for the ban list fell through to command help.
- Text in generated stories could still differ from the locally edited `thoughts.md` because the server rewrite step transformed the selected thought before Kaggle.
- Some generated scenes visibly paused near their beginning/end because the renderer filled fixed `30fps` output tails with repeated frames when source clips had lower or variable frame rates.
- Session `#266` blocked the next manual `/kenigsberg` until startup recovery failed it after the local handoff grace window; the operator had no explicit command to clear the stuck pre-handoff row.
- The operator saw repeated scene material inside one issue, making different generations feel less random and cheaper than intended.
- Session `#267` did not render or publish because a disabled production-story path still executed its helper import in Kaggle.

## Detection

- Operator supplied the Telegram transcript for 2026-05-12 19:17-19:34 Europe/Kaliningrad.
- Production DB showed session `#265` eventually reached `PUBLISHED_TEST`.
- Runtime logs showed first post-processing pass for session `#265` hit SQLite lock-related failures around manifest/status persistence, then a later recovery poller finished the same session.

## Timeline

- 2026-05-12 17:26 UTC — Kenigsberg `#10`, session `#265`, launched.
- 2026-05-12 17:29 UTC — first output download/post-processing attempted for session `#265`; manifest/status writes hit SQLite lock symptoms while VK auto-import was active.
- 2026-05-12 17:31 UTC — operator retried `/kenigsberg`; bot reported session `#265` as still rendering.
- 2026-05-12 17:34 UTC — recovery poller downloaded output, registered manifest, and published test video successfully.
- 2026-05-12 17:40 UTC — incident fix started.
- 2026-05-12 18:27 UTC — Kenigsberg `#11`, session `#266`, created dataset `zigomaro/kenigsberg-session-266-1778610442` and bound `zigomaro/koenigsberg-stories`, then hit SQLite lock while writing `kaggle_dataset`/`kaggle_kernel_ref`.
- 2026-05-12 18:37 UTC — operator retried `/kenigsberg`; local row still said `RENDERING + local:KoenigsbergStories`.
- 2026-05-12 18:41 UTC — startup/recovery path marked session `#266` `FAILED` as a stale local handoff.
- 2026-05-12 19:23 UTC — Kenigsberg `#12`, session `#267`, failed in Kaggle cell 2.
- 2026-05-13 UTC — direct Kaggle logs for `zigomaro/koenigsberg-stories` showed `ModuleNotFoundError: No module named 'telethon'` from `/kaggle/working/story_publish.py` before `render_kenigsberg_story.py` started.

## Root Cause

1. Kenigsberg launch blocking checked only for local `RENDERING` sessions and returned a generic message without inspecting the current Kaggle kernel state.
2. `video_announce.poller._update_status` did not retry transient SQLite locks, so a completed Kaggle output could fail local terminal status persistence during concurrent heavy import work.
3. Kenigsberg state writes used a single attempt, so manifest registration/bans were also exposed to short SQLite locks.
4. `/a` direct-command parsing won over the Kenigsberg heuristic and did not canonicalize natural ban-list phrases.
5. The text contract had drifted: the implementation still rewrote curated `thoughts.md` entries, while the product now wants `thoughts.md` to contain final publication wording.
6. The renderer read source videos sequentially with OpenCV while writing a fixed `30fps` output. For lower-fps/VFR sources this could exhaust the selected source frames before the target scene duration and then repeat `last_frame`, creating visible freezes.
7. Kenigsberg launch handoff writes still used single-attempt SQLAlchemy commits for `RENDERING`, `kaggle_dataset`, `kaggle_kernel_ref`, and the fail-close path. A lock after successful Kaggle binding therefore stranded a local pseudo-ref row that neither the poller nor operator could use.
8. `pick_video_segments` prevented only immediate file repetition. It did not mark source intervals already selected in the current generation, so the same source video could later be chosen with an overlapping or near-adjacent offset.
9. The story-ready notebook copied and imported `kaggle_common/story_publish.py` unconditionally. Test runs do not include `story_publish.json`, but the import still required Telethon and failed before the renderer could start.

## Contributing Factors

- VK auto-import can run long LLM/Smart Update work in the same bot process and increases SQLite write contention.
- The status word `RENDERING` covers Kaggle execution plus local download/send/log persistence, which is too coarse for operator feedback.
- Earlier fixes optimized against fallback text quality, but the desired product direction changed to fully curated copy.

## Automation Contract

### Treat as regression guard when

- Changing `/kenigsberg` launch gating or active-session checks.
- Changing `video_announce` session status persistence around Kaggle completion.
- Changing Kenigsberg `thoughts.md` text preparation or `/a` Kenigsberg command routing.
- Changing Kenigsberg source-video decoding, frame-rate conversion, or transition assembly.
- Changing Kenigsberg source-video selection, randomization, or source-range exclusion logic.
- Changing Kenigsberg local-to-Kaggle handoff persistence or manual stuck-session controls.
- Changing Kenigsberg notebook production-story helper import/preflight behavior.

### Affected surfaces

- `handlers/kenigsberg_stories_cmd.py`
- `handlers/admin_assist_cmd.py`
- `kenigsberg_stories/state.py`
- `video_announce/poller.py`
- `scripts/render_kenigsberg_story.py`
- `docs/features/kenigsberg-stories/thoughts.md`
- `docs/features/kenigsberg-stories/README.md`

### Mandatory checks before closure or deploy

- `/kenigsberg` active-session gate is scoped to `profile_key="kenigsberg_story"`.
- If an active Kenigsberg session has Kaggle `COMPLETE`, operator feedback says it is finalizing/downloading/publishing, not simply “rendering”.
- Session status updates and Kenigsberg state writes retry transient SQLite lock errors.
- `/a kenigsberg покажи список банов` resolves to `/kenigsberg bans`.
- New story text payloads use `text_source=thoughts_md`.
- Kaggle renderer still requires explicit `scene_lines` in `payload.json`.
- Source scenes are extracted as constant `30fps` frames before overlays and must not rely on repeating OpenCV `last_frame` to reach scene duration.
- Source segment selection avoids persistent bans, recent source exclusions, and overlapping/near-adjacent source intervals already chosen in the same generated issue.
- `kenigsberg_issue_manifest.json` / `kenigsberg_render_log.json` include the seed and rhythm slots used for the run.
- Kenigsberg launch handoff writes retry SQLite locks, and stale `local:KoenigsbergStories` rows can be cleared by `/kenigsberg unlock` or auto-failed on the next `/kenigsberg` after the handoff grace window.
- Kenigsberg notebook imports/preflights `story_publish.py` only when `story_publish.json` exists; normal manual test renders without production story config must not import the helper.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py tests/test_video_announce_story_publish.py` passes.
- Production `/healthz` is green after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Test output.
- Production DB/status evidence for latest Kenigsberg sessions.
- Confirmation that local `thoughts.md` changes were included in the deployed bundle source.

## Immediate Mitigation

- Session `#265` already recovered and published through the existing recovery poller.
- Session `#266` was already failed by startup recovery after the local-handoff grace window; no publishable output can be downloaded from that row because the real Kaggle metadata was not persisted.
- Code fix narrows and improves active-session reporting, removes the LLM rewrite from Kenigsberg story text, and adds SQLite-lock retries around the fragile writes.
- Scene decoding now uses ffmpeg CFR `30fps` extraction per source segment before text/watermark/transitions are applied.
- Kenigsberg now exposes `/kenigsberg unlock` for local pre-handoff rows and auto-fails stale local handoffs on the next `/kenigsberg`.
- Scene selection now keeps a per-run in-memory ban map with a small margin around each chosen source interval, so a source file can be reused only on a genuinely different available interval.
- Notebook story helper import is now gated by the actual presence of `story_publish.json`; Telethon/requests/cryptography are installed for the future production-story path but are not required for disabled story publishing.

## Corrective Actions

- Add tests for natural ban-list routing and final-copy text preparation.
- Add a test for CFR segment-frame extraction and short-decode padding metadata.
- Add a test for stale local handoff detection.
- Add tests for per-run unused-file preference and overlapping source-range avoidance.
- Add a notebook guardrail test that `story_publish_requested` is computed before helper import and that the helper is imported only behind that gate.
- Update Kenigsberg documentation to make `thoughts.md` the final editorial source.
- Commit and deploy the current local `thoughts.md` changes with the code fix.

## Follow-up Actions

- [ ] Add a dedicated “finalizing” status enum or note for video sessions that have Kaggle `COMPLETE` but are still downloading/publishing.
- [ ] Review whether VK auto-import should lower concurrency or avoid long write transactions during manual video generation windows.

## Release And Closure Evidence

### 2026-05-12 source anti-repeat + story-readiness deploy

- deployed SHA: `89325c8b5e1e9e3e6f07e6f75b5a9f16f4411b93`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-new-deploy-89325c8b` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly release: `v1072`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRESXGQ6NJYWNYSP8J2B6HR4`, machine `48e42d5b714228` started with `1/1` checks passing.
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py video_announce/story_publish.py tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_story_publish.py tests/test_telegram_business.py main.py main_part2.py`
  - `timeout 60 .venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_story_publish.py tests/test_video_announce_poller.py tests/test_telegram_business.py` -> `50 passed in 1.45s`
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues.
  - Production `/app/scripts/render_kenigsberg_story.py` contains `reason: current_generation`, confirming the per-run source-range exclusion code is deployed.
  - Production `/app/handlers/kenigsberg_stories_cmd.py` contains `KENIGSBERG_STORIES_PRODUCTION_ENABLED` and production story status reporting.
  - Production `/app/kaggle/KoenigsbergStories/koenigsberg_stories.ipynb` contains notebook version `v3-mvp-heuristic-render-story-ready`.
  - Production read-only story readiness check: `KENIGSBERG_STORIES_PRODUCTION_ENABLED` is false; `VIDEO_ANNOUNCE_STORY_ENABLED` is true; webhook has Business updates and `pending_update_count=0`; encrypted Business cache has selected story-capable targets; no Kenigsberg session is currently `RENDERING`.
  - `@keniggpt` exists in the channel table; `@mostvkenig` is not required by the default production story path because Kenigsberg uses an explicit peer override instead of `main_chat_id` DB resolution.
- live smoke:
  - Not run by Codex to avoid publishing a new `@keniggpt` story or `@mostvkenig` story without an explicit operator command.

### 2026-05-12 final-copy + postprocess deploy

- deployed SHA: `37311d3c0e767abd368ece2ea929d6041de56886`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-new-deploy-f37afd1c` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly release: `v1071`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KREPDH8NVKNNFAYVQMETAXP5`, machine `48e42d5b714228` started with `1/1` checks passing.
- regression checks:
  - `python3 -m py_compile scripts/render_kenigsberg_story.py handlers/kenigsberg_stories_cmd.py handlers/admin_assist_cmd.py kenigsberg_stories/state.py video_announce/poller.py tests/test_kenigsberg_stories.py tests/test_video_announce_poller.py`
  - `timeout 30 .venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py` -> `31 passed in 1.48s`
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues.
  - Production `/app/handlers/kenigsberg_stories_cmd.py` contains `thoughts_md` and no `TEXT_REWRITE_MODEL` / `_rewrite_thought_for_story`.
  - Production `/app/scripts/render_kenigsberg_story.py` contains `ffmpeg_cfr_30fps` and `Payload scene_lines are required`.
  - Production `/app/docs/features/kenigsberg-stories/thoughts.md` contains the local edited entries for Кнайпхоф and Девау.
  - Production DB `/data/db.sqlite` latest Kenigsberg sessions `#265`, `#264`, `#263` are `PUBLISHED_TEST`; no stale latest `RENDERING` Kenigsberg session was present at verification time.
  - Kenigsberg state after deploy: `next_issue=11`, `bans=3`, `used_thought_ids=10`.
- live smoke:
  - Not run by Codex to avoid publishing a new `@keniggpt` story without an explicit operator `/kenigsberg` command after deploy.

## Prevention

- Treat Kaggle `COMPLETE` plus local `RENDERING` as an operator-visible finalization state.
- Keep publication copy deterministic and auditable by shipping exactly the curated `thoughts.md` entry plus non-semantic screen splitting.
