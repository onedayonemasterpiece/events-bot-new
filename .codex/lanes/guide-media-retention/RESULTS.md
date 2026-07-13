# Lane guide-media-retention Results

## Status
committed

## Requirement IDs
- R-guide-media-retention

## Branch
agent/incident-20260713/guide-media-retention

## Worktree
`/home/dev/projects/events-bot-new/.worktrees/incident-20260713-guide-media-retention`

## Base SHA
`ef84a152`

## Head SHA
`7354de69` (implementation commit; this results record is a follow-up metadata commit)

## Files changed
- `guide_excursions/media_retention.py`
- `guide_excursions/service.py`
- `scripts/prune_guide_media_store.py`
- `tests/test_guide_media_retention.py`
- `docs/features/guide-excursions-monitoring/README.md`

## Commands run
- `python3 -m py_compile guide_excursions/media_retention.py guide_excursions/service.py scripts/prune_guide_media_store.py tests/test_guide_media_retention.py`
- `/home/dev/projects/events-bot-new/.worktrees/incident-20260711-vector-sync-e2e/.venv/bin/python -m pytest -q tests/test_guide_media_retention.py tests/test_guide_vk_digest.py tests/test_guide_digest_publish.py`
- `/home/dev/projects/events-bot-new/.worktrees/incident-20260711-vector-sync-e2e/.venv/bin/python scripts/prune_guide_media_store.py --help`
- `git diff --check`

## Tests / verification
- 13 focused/adjacent tests passed.
- Compile passed for implementation, wiring, CLI, and tests.
- Dry-run/apply coverage verifies live occurrence, recent post, current digest, young file, outside path, and symlink protection.
- Apply coverage verifies oldest bounded deletion, stale/missing DB path healing, aligned media-ref removal, carousel-specific retention, and finally-after-import execution on import failure.

## Risks
- Filesystem unlink and SQLite update cannot be one atomic transaction. The DB repair is transactional and retryable: after a DB failure the next pass proves the path missing and heals it; errors remain explicit in the result/log.
- `max_total` and `min_free` never override the age/protected safety floor. If safe candidates cannot reach targets, the report returns `policy_satisfied=false` instead of deleting recent/live media.
- Normal import retention is fail-open so an operational cleanup failure cannot discard a downloaded guide scan; the failure is logged and added to import summary on a successful import.

## Merge notes
- Cherry-pick implementation commit `7354de69`, then this metadata commit.
- Parent integrator owns `CHANGELOG.md`, incident record, `fly.toml`, runtime logging configuration, deployment, production dry-run/apply, and post-cleanup verification.
- Production operator flow: run CLI without `--apply`, inspect protected/candidate/DB-repair counts and samples, then repeat with `--apply` and a named reason.
