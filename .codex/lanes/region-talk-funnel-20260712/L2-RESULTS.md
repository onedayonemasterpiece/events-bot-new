# Lane L2 Results

## Status
merged by serial integrator

## Requirement IDs
- R03, R04, R07, R08

## Evidence
- Unified publication storage identity on normalized URL.
- Added terminal working-text pruning and historical state maintenance.
- Changed the last Qwen legacy namespace defaults to `region_talk_compact`.
- Exported, checksummed and removed the 644 MB legacy table; final Region Talk table is 46.2 MB.

## Risks
- Rollback dump is an uncommitted operational artifact and must remain outside git.
