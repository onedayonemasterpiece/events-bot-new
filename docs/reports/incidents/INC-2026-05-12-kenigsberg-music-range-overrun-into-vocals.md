# INC-2026-05-12-kenigsberg-music-range-overrun-into-vocals

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-command-silent-during-gemma-retry`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/release-governance.md`

## Summary

The test publication at `https://t.me/keniggpt/1944` contained vocals because the renderer allowed the audio extraction to run past the configured instrumental whitelist range. Later test sessions `#276` / `#277` stayed inside the whitelist but repeated the same overlapping `The Promise` window back-to-back and hit a vocalized fragment inside the allowed range. The whitelist remains the hard safety boundary, and recent-window avoidance plus best-effort voice-risk scoring are required so test renders do not keep selecting the same problematic musical phrase.

## User / Business Impact

- A visible test story in `@keniggpt` violated the content contract and used an undesired vocal fragment.
- The MVP became unsafe to rerun until audio range enforcement was fixed.
- Repeated same-track/overlapping music in adjacent test issues made the MVP feel repetitive even when the range was technically allowed.
- Production `@mostvkenig` auto-publishing was not enabled, so the impact stayed in the test channel.

## Detection

- The operator reviewed `https://t.me/keniggpt/1944` and reported that the selected audio was outside the allowed no-vocal range.
- Code review found that `choose_music` checked only `MAIN_DURATION` but `encode_video` extracted `MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION`.

## Timeline

- 2026-05-12 13:00 UTC — session `#256` completed and registered Kenigsberg issue `#2`.
- 2026-05-12 13:00 UTC — story was published to `@keniggpt`.
- 2026-05-12 13:10 UTC — operator reported vocals outside the allowed range.
- 2026-05-12 13:15 UTC — root cause identified in `scripts/render_kenigsberg_story.py::choose_music`.
- 2026-05-13 15:51 UTC — operator reported sessions `#276` / `#277` using the same audio track and overlapping vocalized range in adjacent renders.

## Root Cause

1. `choose_music` required `usable_end - start >= MAIN_DURATION` and picked `music_start <= allowed_end - MAIN_DURATION`.
2. `encode_video` then extracted `total_duration = MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION`, so the final outro audio could extend beyond `allowed_end`.
3. If a track name did not match `MUSIC_RANGES`, the renderer fell back to `(0.0, None)`, which was unsafe for a dataset containing tracks with vocals.
4. After the first strict-range fix, `kenigsberg_stories_state.recent_music` was not populated and the next payload did not include historical issue music, so the renderer had no memory that `The Promise` had just been used.
5. The configured no-word whitelist can still include non-lexical voice/vocalization; MVP had no scoring signal to prefer instrumental-sounding subwindows within the same allowed range.

## Contributing Factors

- The first MVP manifest logged only `music_start` and total duration, not `music_end` or the matched allowed range.
- The whitelist was documented but not pinned by tests as a hard full-story constraint.
- Music history was present in issue manifests, but not converted into the next run's selection payload.

## Automation Contract

### Treat as regression guard when

- Changing `MUSIC_RANGES`.
- Changing story duration or outro duration.
- Changing `choose_music` or `encode_video`.
- Changing `register_issue_manifest`, `recent_music_exclusions`, or payload fields used by the renderer.
- Adding new tracks to `zigomaro/koenigsberg-music`.

### Affected surfaces

- `scripts/render_kenigsberg_story.py`
- `tests/test_kenigsberg_stories.py`
- `docs/features/kenigsberg-stories/README.md`
- Kaggle output manifest/render-log shape

### Mandatory checks before closure or deploy

- Unit test proves `music_start + total_duration <= allowed_end`.
- Unit test proves unlisted tracks are skipped/fail closed.
- Unit test proves too-short whitelisted ranges are rejected for the full story.
- Unit test proves recent issue music history is exported as `recent_music`.
- Unit test proves the renderer avoids a recent overlapping same-track window when another whitelisted track is available.
- Unit test proves lower `voice_risk` candidates are preferred when other hard constraints are equal.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.
- Fresh `/kenigsberg` smoke manifest includes `music_end` inside `music_allowed_range` plus `music_voice_risk` / repeat flags.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Test output.
- Post-deploy `/healthz`.
- Fresh Kaggle manifest/log evidence for selected music start/end/range before wider production use.

## Immediate Mitigation

- Enforce the configured instrumental range against the full encoded story duration.
- Remove whole-track fallback for unlisted music.
- Add music start/end/allowed-range fields to `kenigsberg_issue_manifest.json` and `kenigsberg_render_log.json`.
- Persist selected music windows into Kenigsberg state and pass recent music into each Kaggle payload.
- Score candidate windows with recent-track/recent-overlap penalties and a best-effort voice-risk analyzer.
- Remove the problematic first `The Promise` whitelist range (`3:44-4:26`) after repeated production/test selections with vocalized audio; keep only the later configured `The Promise` range unless a future manual audit approves another instrumental window.

## Corrective Actions

- Update `choose_music` to require `MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION` inside the allowed range.
- Add robust normalized name matching so `The Promise.flac` still maps to the configured `the promise` range.
- Add regression tests for allowed range containment, unlisted tracks, and short ranges.
- Add regression tests for recent music history export, repeat avoidance, and voice-risk preference.
- Prefer low-voice non-overlapping candidates before falling back to high-voice fresh-track candidates, so "new track" status cannot beat an obviously vocalized window when a cleaner already-used track exists.

## Follow-up Actions

- [ ] Review `zigomaro/koenigsberg-music` filenames and expand `MUSIC_RANGES` with any newly added tracks before enabling them.
- [ ] Consider moving music range configuration to a versioned manifest file in the music dataset once the library grows.
- [ ] After the next successful test issue, inspect `kenigsberg_render_log.json.selected_music` for `voice_risk`, `recent_same_track`, and `overlaps_recent`.

## Release And Closure Evidence

### 2026-05-13 repeat / voice-risk follow-up

- deployed SHA: `e4978a83e9b4c384cad9879f211563f51ef49d2e`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1083`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRH348YJ0DM9232JBSB054VV`
- regression checks:
  - `python3 -m py_compile kenigsberg_stories/state.py handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py tests/test_kenigsberg_stories.py`
  - `git diff --check`
  - `.venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_video_announce_poller.py tests/test_video_announce_story_publish.py` -> `58 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1083`, service check passing.
  - Production code contains `recent_music_exclusions` and renderer manifest field `music_voice_risk`.
  - Production `thoughts.md` contains the latest operator-provided entries through `#36`.
- fresh smoke still required before closing: the next `/kenigsberg` issue should show `kenigsberg_render_log.json.selected_music.voice_risk`, `recent_same_track`, and `overlaps_recent`; adjacent runs should not repeat an overlapping same-track segment when alternatives are available.

### 2026-05-13 production Promise vocalise follow-up

- deployed SHA: `f6f387284665c4c69a5ee1dcf2401a725d1ea705`
- regression checks:
  - `.venv/bin/python -m compileall -q handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py kenigsberg_stories/state.py scheduling.py`
  - `.venv/bin/pytest tests/test_kenigsberg_stories.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_notebook.py -q` -> `61 passed`
- corrective evidence:
  - `MUSIC_RANGES["the promise"]` no longer includes the repeated `3:44-4:26` window.
  - Production state keeps issue `#44` `The Promise` window in effective `recent_music` after text/source usage reset.
  - Compensation issue `#45` did not repeat `The Promise`; it selected `05 - Save Me.flac`.

### 2026-05-12 strict whitelist fix

- deployed SHA: `748ca42cff63d7eb4c1de23fe4c9db3531d15049`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1067`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRE4WWNCQ8EXSBAJSK1F6PX6`
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py`
  - `.venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` -> `20 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1067`, service check passing.
  - Production renderer contains the strict `choose_music(...) -> tuple[Path, float, float, dict[str, Any]]` path, the fail-closed `"No audio track has an allowed instrumental range..."` error, and manifest fields `music_end` / `music_allowed_range`.
  - Fresh `/kenigsberg` smoke still required before closing: the next manifest must show `music_start <= music_end <= music_allowed_range.end`.

## Prevention

- Audio whitelist ranges are hard constraints, not scoring hints.
- New tracks must not enter random selection until their no-vocal ranges are configured and tested.
