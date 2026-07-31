# INC-2026-07-31-poster-candidate-url Smart Update persist crash

Status: open
Severity: sev1
Service: production VK/source import and Smart Update persistence
Opened: 2026-07-31
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-07-31-false-kgd80-festival-link`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

The KGD80 grounding guard added a provenance payload containing
`poster.url`, but `PosterCandidate` exposes only `supabase_url` and
`catbox_url`. Every Smart Update candidate carrying a poster therefore raised
`AttributeError` before persistence.

## User / Business Impact

- Production source imports with posters failed before event persistence.
- The file mirror shows widespread failures from 2026-07-31 12:42 UTC,
  including the operator-observed VK batch `ops_run_id=4952`.
- Candidates without posters could still proceed, so degradation was partial
  but affected a core ingestion path and requires a same-day catch-up.

## Detection

- Operator notifications reported repeated `ошибка сохранения (persist)` with
  `'PosterCandidate' object has no attribute 'url'`.
- Production `/data/runtime_logs/events-bot.log*` confirmed the first retained
  occurrence and the wider blast radius.

## Timeline

- 2026-07-31 12:42 UTC — first retained production failure after deployment of
  the KGD80 grounding change.
- 2026-07-31 17:01–17:09 UTC — repeated failures in VK auto-import run 4952.
- 2026-07-31 17:12 UTC — operator escalated the bot notifications.
- 2026-07-31 17:14 UTC — root cause reproduced in deployed source and a hotfix
  was started from current `origin/main`.

## Root Cause

1. The new grounding code assumed a generic poster URL attribute that does not
   exist in the slotted dataclass.
2. Existing incident tests covered festival grounding semantics but did not
   pass a poster-bearing candidate through the Smart Update entry boundary.

## Contributing Factors

- The failure occurred before persistence, so one bad provenance projection
  blocked the entire otherwise valid candidate.
- Notification summaries exposed individual rows but not the accumulated
  blast radius; file-mirror investigation was required.

## Automation Contract

### Treat as regression guard when

- Changing `PosterCandidate`, KGD80/festival grounding evidence, or the source
  import → Smart Update boundary.

### Affected surfaces

- `smart_event_update.py`
- VK/source parser persistence
- scheduler catch-up and production release path

### Mandatory checks before closure or deploy

- Replay a poster-bearing VK-shaped fixture through the exact pre-persistence
  Smart Update boundary.
- Verify managed URL precedence, source URL fallback, and a poster without a
  URL as a negative control.
- Deploy a SHA reachable from `origin/main`, check `/healthz`, and confirm no
  fresh `PosterCandidate.url` traceback in the file mirror.
- Run the compensating VK/source catch-up and verify the failed current-day
  rows no longer terminate with this exception.

### Required evidence

- `tests/replays/INC-2026-07-31-poster-candidate-url/source.json`
- targeted pytest output
- deployed SHA and `origin/main` ancestry
- post-deploy log excerpt and catch-up result

## Immediate Mitigation

- Replace the nonexistent attribute access with a narrow provenance helper:
  canonical `supabase_url`, then `catbox_url`, otherwise `null`.

## Corrective Actions

- Add boundary replay coverage with both positive and negative poster cases.
- Complete same-day catch-up after deployment.

## Follow-up Actions

- [ ] Add aggregate alerting for repeated per-row persist exceptions in one
  import run.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending manual Fly deploy from clean main-reachable hotfix
- regression checks: pending
- post-deploy verification: pending

## Prevention

The replay locks the dataclass/provenance contract at the same boundary that
failed in production instead of testing the grounding function in isolation.
