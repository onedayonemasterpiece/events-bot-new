# R04 evidence lane result

- Base: `origin/main` `0e1dad424811c2c2eedda2707b92263f8df1551b`
- Branch: `codex/r04-evidence-lane`
- Scope: existing seven read-only MCP tools only; no social adapter, runtime, server,
  configuration, documentation, changelog, provider, or deployment changes.

## Delivered

- `events_search(post_url=...)` strictly parses exact VK/Telegram post URLs and
  returns every event within the fail-closed row budget. Evidence relations cover
  EventSource, `event_publication`, managed/legacy event fields, `vk_inbox`, and
  `vk_inbox_import_event`; multi-event ambiguity and provenance are explicit.
- `event_get` exposes role-labelled, canonical, deduplicated evidence links plus
  bounded publication, VK inbox/import, and identity-decision evidence. Existing
  recursive redaction and untrusted-external-data contracts apply to all rows.
- `incidents_search` accepts exact event/source/post/run/job/error-class and UTC
  time filters, includes all-status bounded `ops_run`/`joboutbox` evidence, and
  keeps `incident_get` as the bounded dereference path.
- Runtime file mirror expansion is deliberately not implemented in the SQLite
  repository. Search metadata marks it as an explicit fixed-path adapter
  integration dependency; there is no arbitrary path, shell, SQL, log stream,
  database download, or provider request surface.
- Added ЮНОСТЬ-style fixtures for one managed VK post mapped to three events,
  original-source grouping, exact Telegram mapping, prefix-collision rejection,
  structured incident expansion, PII/credential redaction, read-only digest, and
  exact seven-tool discovery.

## Validation

- `PYTHONPATH=. ... pytest -q tests/test_private_events_mcp_repository.py tests/test_private_events_mcp_protocol.py`
  - `12 passed`
- `PYTHONPATH=. ... pytest -q tests/test_private_events_mcp_*.py`
  - `213 passed`, 3 pre-existing aiohttp `NotAppKeyWarning` warnings
- `python3 -m compileall -q private_events_mcp tests/test_private_events_mcp_repository.py tests/test_private_events_mcp_protocol.py`
  - passed
- `git diff --check`
  - passed
- No live provider calls or secrets used.
