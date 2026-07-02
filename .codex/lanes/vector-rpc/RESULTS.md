# Lane vector-rpc Results

## Status
committed

## Requirement IDs
- R02
- R03
- R11

## Branch
agent/smart-update-vector-identity-gate/vector-rpc

## Worktree
/home/dev/.codex/worktrees/events-bot-new/vector-rpc

## Base SHA
f44a3f3db3112e03e2cbf6ba4e24fff44cd1afc8

## Head SHA
Pending until commit creation; final response reports the committed lane HEAD SHA.

## Files changed
- CHANGELOG.md
- docs/features/smart-event-update/README.md
- event_identity.py
- supabase/migrations/20260702131500_event_identity_candidates_by_embedding_v1.sql
- tests/test_event_identity.py
- .codex/lanes/vector-rpc/RESULTS.md

## Commands run
- `cat /home/dev/.agents/skills/feature-fanout/SKILL.md`
- `cat /home/dev/.codex/skills/events-bot-event-investigation/SKILL.md`
- `git status --short --branch && git rev-parse HEAD && git branch --show-current`
- `sed -n '1,220p' AGENTS.md`
- `sed -n '1,260p' docs/features/smart-event-update/README.md`
- `rg -n "event_search_documents|event_embeddings|embedding_doc_kind|identity_candidate|candidate.*embedding|event_identity" --glob '!*.ipynb' --glob '!*.sqlite' --glob '!*.db'`
- `python3 -m pytest -q tests/test_event_identity.py` (failed: system Python has no pytest)
- `python3 -m compileall -q event_identity.py tests/test_event_identity.py`
- `python3 -m pip install --user pytest==8.1.1 pytest-asyncio==0.23.6` (failed: externally-managed environment)
- `python3 -m venv /tmp/vector-rpc-pytest-venv && /tmp/vector-rpc-pytest-venv/bin/pip install pytest==8.1.1 pytest-asyncio==0.23.6` (pytest then failed on repo conftest dependency `aiogram`)
- `/tmp/vector-rpc-pytest-venv/bin/pip install -r requirements.txt`
- `/tmp/vector-rpc-pytest-venv/bin/pytest -q tests/test_event_identity.py`

## Tests / verification
- PASS: `/tmp/vector-rpc-pytest-venv/bin/pytest -q tests/test_event_identity.py` → `5 passed`.
- PASS: `python3 -m compileall -q event_identity.py tests/test_event_identity.py`.
- Verified `git status --short --branch` baseline was clean on branch `agent/smart-update-vector-identity-gate/vector-rpc` at base SHA.

## Risks
- The Supabase RPC migration was inspected and covered by text-level tests but was not applied to a live Supabase project in this lane.
- The RPC dynamically adapts to available columns on `event_search_documents` / `event_embeddings`; integration should validate against the final schema lane before production migration.
- Vector recall is intentionally not wired into Smart Update yet, per lane scope.

## Merge notes
- No secrets were read or committed.
- No public/browser API was added; the RPC revokes public/anon/authenticated execution and grants only `service_role`.
- This lane did not push.
