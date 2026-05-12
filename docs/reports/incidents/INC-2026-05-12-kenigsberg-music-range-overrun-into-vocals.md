# INC-2026-05-12-kenigsberg-music-range-overrun-into-vocals

Status: open
Severity: sev2
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-command-silent-during-gemma-retry`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/release-governance.md`

## Summary

The test publication at `https://t.me/keniggpt/1944` contained vocals because the renderer allowed the audio extraction to run past the configured instrumental whitelist range. The whitelist was intended to cover only no-vocal fragments and must be enforced for the full generated story, including outro.

## User / Business Impact

- A visible test story in `@keniggpt` violated the content contract and used an undesired vocal fragment.
- The MVP became unsafe to rerun until audio range enforcement was fixed.
- Production `@mostvkenig` auto-publishing was not enabled, so the impact stayed in the test channel.

## Detection

- The operator reviewed `https://t.me/keniggpt/1944` and reported that the selected audio was outside the allowed no-vocal range.
- Code review found that `choose_music` checked only `MAIN_DURATION` but `encode_video` extracted `MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION`.

## Timeline

- 2026-05-12 13:00 UTC — session `#256` completed and registered Kenigsberg issue `#2`.
- 2026-05-12 13:00 UTC — story was published to `@keniggpt`.
- 2026-05-12 13:10 UTC — operator reported vocals outside the allowed range.
- 2026-05-12 13:15 UTC — root cause identified in `scripts/render_kenigsberg_story.py::choose_music`.

## Root Cause

1. `choose_music` required `usable_end - start >= MAIN_DURATION` and picked `music_start <= allowed_end - MAIN_DURATION`.
2. `encode_video` then extracted `total_duration = MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION`, so the final outro audio could extend beyond `allowed_end`.
3. If a track name did not match `MUSIC_RANGES`, the renderer fell back to `(0.0, None)`, which was unsafe for a dataset containing tracks with vocals.

## Contributing Factors

- The first MVP manifest logged only `music_start` and total duration, not `music_end` or the matched allowed range.
- The whitelist was documented but not pinned by tests as a hard full-story constraint.

## Automation Contract

### Treat as regression guard when

- Changing `MUSIC_RANGES`.
- Changing story duration or outro duration.
- Changing `choose_music` or `encode_video`.
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
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.
- Fresh `/kenigsberg` smoke manifest includes `music_end` inside `music_allowed_range`.

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

## Corrective Actions

- Update `choose_music` to require `MAIN_DURATION + 2 * OUTRO_SCREEN_DURATION` inside the allowed range.
- Add robust normalized name matching so `The Promise.flac` still maps to the configured `the promise` range.
- Add regression tests for allowed range containment, unlisted tracks, and short ranges.

## Follow-up Actions

- [ ] Review `zigomaro/koenigsberg-music` filenames and expand `MUSIC_RANGES` with any newly added tracks before enabling them.
- [ ] Consider moving music range configuration to a versioned manifest file in the music dataset once the library grows.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Audio whitelist ranges are hard constraints, not scoring hints.
- New tracks must not enter random selection until their no-vocal ranges are configured and tested.
