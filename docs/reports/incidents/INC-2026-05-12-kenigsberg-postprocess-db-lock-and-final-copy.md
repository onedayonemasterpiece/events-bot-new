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

During manual testing, `/kenigsberg` reported that session `#265` was still rendering even though Kaggle had already reached `COMPLETE`. The completed output eventually published, but the operator-facing state was misleading while heavy VK auto-import work held SQLite locks. The same test window also showed `/a kenigsberg покажи список банов` being routed as `/kenigsberg покажи список банов` instead of `/kenigsberg bans`, confirmed that story text should now be treated as final curated copy from `thoughts.md`, not rewritten by an LLM, and exposed visible scene freezes in Kenigsberg `#10`. A later run, session `#266`, reached Kaggle dataset/kernel binding but failed to persist the real Kaggle metadata because SQLite was locked, leaving the row as `RENDERING + local:KoenigsbergStories`. Subsequent visual review showed the same source fragment could reappear later in the same generated story because per-run segment selection avoided only recent files, not already selected source-time ranges. Session `#267` then failed before rendering because the story-ready notebook imported the shared story helper during a normal test run without `story_publish.json`; the helper imports Telethon, which was not installed in the Kenigsberg Kaggle notebook environment. Kenigsberg `#13` exposed a final-copy splitting regression: the launch path stopped using an LLM for semantic screen boundaries, sent a long one-sentence thought as one `scene_lines` item, and only the beginning stayed visible while the semantic tail never appeared as separate screens. Visual review also showed scene changes did not reliably land on music beats because the renderer used a seeded pseudo-rhythm grid instead of analyzing the selected audio.

On 2026-05-13, while a CrumpleVideo render was running, Kenigsberg issues `#19` and `#20` reached text preparation but failed before Kaggle with `sqlite3.IntegrityError: UNIQUE constraint failed: videoannounce_session.status` when their sessions were moved from `SELECTED` to `RENDERING`. The table still had a global partial unique index on `status='RENDERING'`, even though Kenigsberg launch gating had already been changed to be profile-scoped.

## User / Business Impact

- The operator could not confidently start the next Kenigsberg test run.
- `/kenigsberg` used the generic “рендерится” wording while the real state was Kaggle complete plus local download/publish finalization.
- A natural `/a` request for the ban list fell through to command help.
- Text in generated stories could still differ from the locally edited `thoughts.md` because the server rewrite step transformed the selected thought before Kaggle.
- Some generated scenes visibly paused near their beginning/end because the renderer filled fixed `30fps` output tails with repeated frames when source clips had lower or variable frame rates.
- Session `#266` blocked the next manual `/kenigsberg` until startup recovery failed it after the local handoff grace window; the operator had no explicit command to clear the stuck pre-handoff row.
- The operator saw repeated scene material inside one issue, making different generations feel less random and cheaper than intended.
- Session `#267` did not render or publish because a disabled production-story path still executed its helper import in Kaggle.
- The failed `#267` run selected `thought=1`; failed renders must not consume thoughts from the no-repeat pool.
- Kenigsberg `#13` published with a long thought cut mid-sense: the first fragment stayed on screen and the remaining words were not shown as readable follow-up screens.
- Scene cuts could look detached from the music because timing was random-ish rather than derived from detected strong beats.
- Some generated clips could still show a thin grey/light-text source strip along the lower edge after the existing bottom crop.
- Kenigsberg could not launch concurrently with an unrelated CrumpleVideo render, despite the desired profile-scoped launch contract.

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
- 2026-05-13 UTC — operator reviewed Kenigsberg `#13` and reported thought `25` stopped after “даже самым”, with the rest of the sentence absent from the visible story text.
- 2026-05-13 UTC — code review confirmed `beat_slots` used `rng.uniform(1.62, 2.32)` and random spans instead of `librosa` beat/onset analysis.

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
10. `choose_next_thought` wrote `used_thought_ids` before successful render completion, so any failed run could remove a thought from the publication shuffle-bag even though it was never actually published.
11. The text pipeline regressed from LLM semantic splitting to deterministic/sentence splitting. This violated the earlier no-fallback contract and allowed a long one-sentence thought to reach Kaggle as one screen.
12. `payload_scene_lines` trusted server-provided lines and did not reject overlong unsplit screens.
13. `beat_slots` was still an MVP pseudo-grid (`rng.uniform` base interval + random spans), not a `librosa`-derived grid anchored to strong musical beats.
14. SQLite schema still enforced `ux_videoannounce_session_rendering ON videoannounce_session(status) WHERE status='RENDERING'`, a global one-render limit that contradicted the later Kenigsberg profile-scoped gate.

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
- Changing Kenigsberg thought-pool reservation/consumption semantics.
- Changing Kenigsberg final-copy screen splitting or renderer `payload_scene_lines`.
- Changing Kenigsberg rhythm-grid construction, music analysis, or scene timeline slotting.
- Changing Kenigsberg bottom crop or lower-edge masking.

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
- The SQLite unique rendering guard is scoped by `COALESCE(profile_key, 'default')`, so Kenigsberg can render while CrumpleVideo/default is rendering, but two Kenigsberg renders cannot run at once.
- If an active Kenigsberg session has Kaggle `COMPLETE`, operator feedback says it is finalizing/downloading/publishing, not simply “rendering”.
- Session status updates and Kenigsberg state writes retry transient SQLite lock errors.
- `/a kenigsberg покажи список банов` resolves to `/kenigsberg bans`.
- New story text payloads use `text_source=thoughts_md_llm_split`.
- Long one-sentence `thoughts.md` entries are split by the text LLM into readable screens without rewriting or losing words; Gemini lite is primary, `gpt-4o` may be used as the explicit validated fallback, and invalid/missing splits after both paths fail generation before Kaggle.
- Kaggle renderer still requires explicit `scene_lines` in `payload.json`.
- Kaggle renderer rejects overlong unsplit `scene_lines` instead of slicing them locally.
- Scene timeline slots are derived from detected strong-beat anchors in the selected audio: first slot may be partial to the first strong beat; subsequent slots are random `1x` or `2x` strong-beat spans.
- If beat detection works and reaches a reasonable story length, the main montage ends on a detected strong-beat anchor before outro.
- If detected strong beats stop too early or beat detection fails, the renderer preserves an acceptable story duration with an explicit logged fallback instead of publishing a sharply shortened clip or blocking the whole run.
- `kenigsberg_issue_manifest.json` / `kenigsberg_render_log.json` include raw `beat_times`, selected `strong_beat_times`, `rhythm_end_mode`, and any `fallback_reason` for rhythm audit against the actual selected music segment.
- The lower source edge is masked after composition with `KENIGSBERG_STORIES_BOTTOM_MASK_PX` so thin grey/light-text source strips are hidden before watermark/export.
- Source scenes are extracted as constant `30fps` frames before overlays and must not rely on repeating OpenCV `last_frame` to reach scene duration.
- Source segment selection avoids persistent bans, recent source exclusions, and overlapping/near-adjacent source intervals already chosen in the same generated issue.
- `kenigsberg_issue_manifest.json` / `kenigsberg_render_log.json` include the seed and rhythm slots used for the run.
- Kenigsberg launch handoff writes retry SQLite locks, and stale `local:KoenigsbergStories` rows can be cleared by `/kenigsberg unlock` or auto-failed on the next `/kenigsberg` after the handoff grace window.
- Kenigsberg notebook imports/preflights `story_publish.py` only when `story_publish.json` exists; normal manual test renders without production story config must not import the helper.
- Selected thoughts are marked used only after a successful issue manifest is registered; failed Kaggle runs must not consume `used_thought_ids`.
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
- Thought shuffle-bag consumption moved from pre-render selection to successful manifest registration.
- Rhythm slotting now prefers detected strong-beat boundaries, preserves story length when the detected grid is too short, and stores raw/selected beat anchors plus fallback metadata in the render metadata.

## Corrective Actions

- Add tests for natural ban-list routing and final-copy text preparation.
- Add a test for CFR segment-frame extraction and short-decode padding metadata.
- Add a test for stale local handoff detection.
- Add tests for per-run unused-file preference and overlapping source-range avoidance.
- Add a notebook guardrail test that `story_publish_requested` is computed before helper import and that the helper is imported only behind that gate.
- Add a state test proving thought selection alone does not mutate `used_thought_ids`, while successful manifest registration does.
- Add regression tests for LLM-only long single-sentence final-copy splitting, invalid LLM tail loss, overlong payload rejection in the renderer, and strong-beat rhythm slot construction.
- Add a regression test for lower-edge source strip masking.
- Update Kenigsberg documentation to make `thoughts.md` the final editorial source.
- Commit and deploy the current local `thoughts.md` changes with the code fix.

## Follow-up Actions

- [ ] Add a dedicated “finalizing” status enum or note for video sessions that have Kaggle `COMPLETE` but are still downloading/publishing.
- [ ] Review whether VK auto-import should lower concurrency or avoid long write transactions during manual video generation windows.

## Release And Closure Evidence

### 2026-05-13 rhythm resilience deploy

- deployed SHA: `45614329d4f17cc202a5e8c7646ef0cd2b1d237c`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-deploy-45614329` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly image: `registry.fly.io/events-bot-new-wngqia:deployment-01KRGVETEM3CKMVTGVFGQP4WEW`, machine `48e42d5b714228`, Fly version `1080`, checks `1/1` passing.
- regression checks:
  - `python3 -m py_compile scripts/render_kenigsberg_story.py tests/test_kenigsberg_stories.py`
  - `timeout 90 .venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py tests/test_video_announce_story_publish.py` -> `54 passed in 1.25s`
- rhythm behavior:
  - Strong-beat endings remain preferred when detected anchors reach a normal story length.
  - If strong-beat anchors stop too early, the renderer keeps the established target duration through `rhythm_end_mode=target_duration_fallback` instead of publishing a sharply shortened story.
  - If beat detection itself fails, the renderer uses `rhythm_end_mode=approximate_fallback` and records `fallback_reason`.
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues.
  - Production `/app/scripts/render_kenigsberg_story.py` contains `rhythm_end_mode`, `approximate_rhythm_slots`, and `MIN_STRONG_MAIN_DURATION`.

### 2026-05-13 LLM split + beat-sync + lower-edge mask deploy

- deployed SHA: `4e23f2836eaa41d1549354d60b056a0d59816afc`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-deploy-4e23f283` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly image: `registry.fly.io/events-bot-new-wngqia:deployment-01KRG3ZR30X6VHHQ7H07009XN2`, machine `48e42d5b714228`, Fly status `started`, checks `1/1` passing.
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py guide_excursions/digest_writer.py guide_excursions/kaggle_service.py kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`
  - `timeout 90 .venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py tests/test_video_announce_story_publish.py` -> `49 passed in 1.34s`
  - local real-LLM smoke against all `26` `thoughts.md` entries with `gemini-3.1-flash-lite` -> `ALL_OK`; saved under `artifacts/codex/kenigsberg-text-split-smoke/results-bounded.json` (not committed).
- long-copy validation:
  - Thought `25` now splits into four semantic screens and preserves the full source text, including the Bessel / observatory tail.
  - Invalid or missing LLM splits now fail before Kaggle instead of falling back to deterministic slicing.
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues.
  - Production `/app/handlers/kenigsberg_stories_cmd.py` contains `thoughts_md_llm_split`.
  - Production `/app/scripts/render_kenigsberg_story.py` contains `detect_strong_beats` and `mask_bottom_source_strip`.
  - Production env contains `KENIGSBERG_STORIES_TEXT_SPLIT_MODEL=gemini-3.1-flash-lite`, `KENIGSBERG_STORIES_BOTTOM_MASK_PX=34`, `GUIDE_MONITORING_EXTRACT_MODEL=models/gemini-3.1-flash-lite`, and `GUIDE_DIGEST_WRITER_MODEL=gemini-3.1-flash-lite`.
  - Production guide monitoring/digest code contains Gemini-lite defaults, carrying the parallel Opus guide-monitoring update into the deployed image.
- live smoke:
  - Not run by Codex to avoid publishing a new `@keniggpt` story without an explicit operator `/kenigsberg` command.

### 2026-05-13 story-helper import gate deploy

- deployed SHA: `39386e29f0dd4e0e1a63fe6cf3d643d562c6304c`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-new-deploy-39386e29` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly release: `v1074`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRFXX99MS6DBWC6KMH2FGSXD`, machine `48e42d5b714228` started with `1/1` checks passing.
- regression checks:
  - `python3 -m py_compile kenigsberg_stories/state.py tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py video_announce/story_publish.py`
  - `timeout 60 .venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_story_publish.py tests/test_video_announce_poller.py` -> `46 passed in 1.27s`
- Kaggle failure evidence:
  - Direct Kaggle logs for `zigomaro/koenigsberg-stories` / session `#267` showed `ModuleNotFoundError: No module named 'telethon'` at `from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle` before `render_kenigsberg_story.py` started.
  - Saved minimal evidence locally under `artifacts/codex/kenigsberg-session-267/` (not committed).
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, no issues.
  - Production notebook contains `story_publish_ready = ensure_story_publish_helper(work) if story_publish_requested else False`.
  - Production notebook installs `telethon`, `requests`, and `cryptography` for the future production-story path.
  - Production `kenigsberg_stories/state.py` contains `return available[index]`, confirming thought selection no longer writes `used_thought_ids` before render success.
  - Production `docs/features/kenigsberg-stories/thoughts.md` contains operator-added entries `19..26`.
  - Production state was corrected for the failed `#267`: unpublished `thought_id=1` was removed from `used_thought_ids`; issue `#12` is absent because no successful manifest was registered.
- live smoke:
  - Not run by Codex to avoid publishing a new `@keniggpt` story without an explicit operator `/kenigsberg` command.

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
- Keep publication copy auditable by shipping exactly the curated `thoughts.md` entry plus LLM-only semantic screen splitting that is validated against the original text before Kaggle.
