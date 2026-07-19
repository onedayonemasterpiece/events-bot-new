# R11-video-cleanup results

## Status

Done and committed.

## Requirement ID

- R11-video-cleanup

## Lane

- Branch: `agent/keyboard-navigation-production/R11-video-cleanup`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/keyboard-nav-prod-r11-video`
- Base SHA: `bd661f84eab63cbe423a00c976fb9c4c322fc8cf`
- Final lane SHA: reported in the worker handoff because embedding it here would
  change the commit SHA.

## Outcome

- Added end-of-poller cleanup for the exact local Kaggle output tree
  `<temp_root>/videoannounce-<session_id>`.
- Cleanup reloads the durable session immediately before deletion and requires
  a fully persisted publication: `PUBLISHED_MAIN`, or `PUBLISHED_TEST` with no
  configured main target, plus `finished_at`, `published_at`, and `video_url`.
- `RENDERING`, `DONE`, `FAILED`, and `PUBLISH_BLOCKED` remain preserved because
  they are live, transitional, failed, blocked, or recovery-capable states.
  `PUBLISHED_TEST` with a configured main target is also preserved until main
  publication succeeds.
- Bulk deletion refuses mismatched session IDs, sibling/unknown basenames,
  missing trees, and symlink targets. Cleanup errors fail closed and never alter
  the durable publication state.
- Repeated successful terminal poller runs no longer leave downloaded bulk
  output trees behind.

## Changed files

- `video_announce/poller.py`
- `tests/test_video_announce_poller.py`
- `.codex/lanes/R11-video-cleanup/RESULTS.md`

No site, canonical documentation, `CHANGELOG.md`, `main.py`, static release, or
production state was changed. Canonical docs and changelog remain integration
owner work per the lane contract.

## Validation

```text
uv run --with-requirements requirements.txt pytest -q \
  tests/test_video_announce_poller.py \
  tests/test_video_announce_v_pipeline.py
```

Result: `62 passed`.

Additional checks:

```text
git diff --check
uv run --with-requirements requirements.txt python -m compileall -q \
  video_announce/poller.py tests/test_video_announce_poller.py
```

Both passed.

## Regression coverage

- Removes nested output trees for fully persisted `PUBLISHED_TEST` and
  `PUBLISHED_MAIN` sessions.
- Exercises the real successful poller path and verifies its output directory
  is gone after terminal persistence.
- Preserves output for `RENDERING`, `DONE`, `FAILED`, `PUBLISH_BLOCKED`, pending
  main publication, and incomplete durable publication evidence.
- Refuses a mismatched `videoannounce-*` session directory and an exact-name
  symlink without deleting their contents.
- Existing post-render bot-delivery failure coverage now asserts that the
  `PUBLISH_BLOCKED` mp4 remains available for narrow recovery.

## Risks and merge notes

- `DONE` is intentionally treated as publish-recoverable, not terminal cleanup
  evidence. This avoids inventing a new discard policy for interrupted delivery.
- Failed and blocked terminal artifacts can still consume disk until an
  explicit recovery/supersede policy resolves them; this is the required
  fail-safe behavior for known recoverable sessions.
- Cherry-pick the single lane commit reported in the worker handoff. Run the
  incident-specific integration checks and add canonical docs/`CHANGELOG.md`
  only in the integration lane.
