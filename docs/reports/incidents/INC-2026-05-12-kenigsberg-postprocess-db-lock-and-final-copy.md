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

During manual testing, `/kenigsberg` reported that session `#265` was still rendering even though Kaggle had already reached `COMPLETE`. The completed output eventually published, but the operator-facing state was misleading while heavy VK auto-import work held SQLite locks. The same test window also showed `/a kenigsberg покажи список банов` being routed as `/kenigsberg покажи список банов` instead of `/kenigsberg bans`, confirmed that story text should now be treated as final curated copy from `thoughts.md`, not rewritten by an LLM, and exposed visible scene freezes in Kenigsberg `#10`.

## User / Business Impact

- The operator could not confidently start the next Kenigsberg test run.
- `/kenigsberg` used the generic “рендерится” wording while the real state was Kaggle complete plus local download/publish finalization.
- A natural `/a` request for the ban list fell through to command help.
- Text in generated stories could still differ from the locally edited `thoughts.md` because the server rewrite step transformed the selected thought before Kaggle.
- Some generated scenes visibly paused near their beginning/end because the renderer filled fixed `30fps` output tails with repeated frames when source clips had lower or variable frame rates.

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

## Root Cause

1. Kenigsberg launch blocking checked only for local `RENDERING` sessions and returned a generic message without inspecting the current Kaggle kernel state.
2. `video_announce.poller._update_status` did not retry transient SQLite locks, so a completed Kaggle output could fail local terminal status persistence during concurrent heavy import work.
3. Kenigsberg state writes used a single attempt, so manifest registration/bans were also exposed to short SQLite locks.
4. `/a` direct-command parsing won over the Kenigsberg heuristic and did not canonicalize natural ban-list phrases.
5. The text contract had drifted: the implementation still rewrote curated `thoughts.md` entries, while the product now wants `thoughts.md` to contain final publication wording.
6. The renderer read source videos sequentially with OpenCV while writing a fixed `30fps` output. For lower-fps/VFR sources this could exhaust the selected source frames before the target scene duration and then repeat `last_frame`, creating visible freezes.

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
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py` passes.
- Production `/healthz` is green after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Test output.
- Production DB/status evidence for latest Kenigsberg sessions.
- Confirmation that local `thoughts.md` changes were included in the deployed bundle source.

## Immediate Mitigation

- Session `#265` already recovered and published through the existing recovery poller.
- Code fix narrows and improves active-session reporting, removes the LLM rewrite from Kenigsberg story text, and adds SQLite-lock retries around the fragile writes.
- Scene decoding now uses ffmpeg CFR `30fps` extraction per source segment before text/watermark/transitions are applied.

## Corrective Actions

- Add tests for natural ban-list routing and final-copy text preparation.
- Add a test for CFR segment-frame extraction and short-decode padding metadata.
- Update Kenigsberg documentation to make `thoughts.md` the final editorial source.
- Commit and deploy the current local `thoughts.md` changes with the code fix.

## Follow-up Actions

- [ ] Add a dedicated “finalizing” status enum or note for video sessions that have Kaggle `COMPLETE` but are still downloading/publishing.
- [ ] Review whether VK auto-import should lower concurrency or avoid long write transactions during manual video generation windows.

## Release And Closure Evidence

- deployed SHA: `f37afd1c666a62662c07d4d0e0b2d3266eb10058`
- deploy path: `origin/main` -> clean detached worktree at `/tmp/events-bot-new-deploy-f37afd1c` -> `flyctl deploy --remote-only -a events-bot-new-wngqia --config fly.toml`
- Fly release: `v1070`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KREP0GFHWRCMXNJ3QNR1NF47`, machine `48e42d5b714228` started with `1/1` checks passing.
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
