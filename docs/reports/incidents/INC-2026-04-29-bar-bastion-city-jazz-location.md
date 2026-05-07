# INC-2026-04-29 Bar Bastion City Jazz Location Drift

Status: mitigated
Severity: sev2
Service: VK auto-import / event location reference
Opened: 2026-04-29
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-04-26-daily-location-fragments`
Related docs: `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/runtime-logs.md`

## Summary

VK auto-import created the public event `4360` (`Поэтический вечер Анны Грозовской`, 2026-05-03 18:00) from `vk.com/wall-149955604_22874` with `location_name=Калининград Сити Джаз Клуб` and `location_address=Мира 33-35`. The source post says only `в Бастионе` and does not include an address. Organizers reported that the event is in Bar Bastion at the Ponart cluster, not at City Jazz.

## User / Business Impact

- Organizers saw a public venue/address mismatch on an upcoming event.
- Readers could be sent to `Мира 33-35` instead of `Судостроительная 6/1`.
- Other recent `bar_bastion` imports with no explicit address had the same wrong City Jazz location, so the issue could recur on future posts.

## Detection

- Detected by organizer report via user request on 2026-04-29.
- Production DB evidence came from a fresh Fly snapshot saved as `artifacts/db/incident-bastion-2026-04-29.sqlite` (not committed).
- Runtime file mirror was checked on Fly: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory present but empty. Fly buffered logs did not contain useful import traces for the affected run.

## Timeline

- 2026-04-28 11:46:51 UTC — VK post `wall-149955604_22874` published.
- 2026-04-28 15:19:11 UTC — post entered `vk_inbox` as row `6320`.
- 2026-04-29 04:15:00 UTC — scheduled `ops_run` `945` (`vk_auto_import`, run_id `dbbd785ba25142278cd2e1b633b78a93`) started.
- 2026-04-29 04:43:10 UTC — event `4360` persisted with City Jazz location/address.
- 2026-04-29 — organizer complaint triggered incident investigation.
- 2026-04-29 20:13 UTC — production DB corrected: event `4360` now points to `Бар Бастион, Судостроительная 6/1`, `vk_source.group_id=149955604` now has the Bar Bastion default, and Telegraph rebuild jobs were requeued.
- 2026-04-29 20:25 UTC — Telegraph page for event `4360` rebuilt and verified to show `Бар Бастион, Судостроительная 6/1, Калининград`.

## Root Cause

1. `docs/reference/locations.md` contained `Понарт, Судостроительная 6, Калининград` and `Калининград Сити Джаз Клуб, Мира 33-35, Калининград`, but did not contain `Бар Бастион` as its own venue at `Судостроительная 6/1`.
2. `vk_source` for group `149955604` (`bar_bastion`, `БАСТИОН. Калининград`) had `location=NULL`, so VK auto-import had no source-level default for addressless posts.
3. The event-parse LLM is instructed to fill `location_name` and copy known venues when possible. With only the ambiguous source phrase `в Бастионе` and no Bar Bastion reference, it hallucinated/selected a wrong known venue (`Калининград Сити Джаз Клуб`).

## Contributing Factors

- Bar Bastion recently moved from the old `Дзержинского` location to Ponart, but the canonical reference layer was not updated.
- The parser requires a venue for event creation; addressless single-venue VK groups need a configured default to avoid model guessing.
- Runtime file logging is intentionally disabled in production, so detailed LLM prompt/response traces were unavailable after the fact.

## Automation Contract

### Treat as regression guard when

- changing `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `location_reference.py`, `main.py` event parsing venue normalization, `vk_auto_queue.py` source default handling, or `db.py` `vk_source` seeding;
- importing or reprocessing events from `vk.com/bar_bastion` / group `149955604`;
- changing VK auto-import prompt/context behavior for addressless source posts.

### Affected surfaces

- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- `db.py` `vk_source` default seeding
- `main.py::_normalise_event_location_from_reference`
- `location_reference.py`
- production `event`, `event_source`, `vk_source`, `vk_inbox`, `ops_run`
- Telegraph event page rebuild for corrected rows

### Mandatory checks before closure or deploy

- `pytest tests/test_location_reference_bastion.py`
- `python3 -m py_compile db.py location_reference.py main.py vk_auto_queue.py vk_intake.py`
- Production DB check that `vk_source.group_id=149955604` has `location='Бар Бастион, Судостроительная 6/1, Калининград'`.
- Production DB check that event `4360` has `location_name='Бар Бастион'`, `location_address='Судостроительная 6/1'`, `city='Калининград'`.
- Confirm runtime-log availability or fallback evidence per `docs/operations/runtime-logs.md`.
- If deployed, confirm deployed SHA is reachable from `origin/main`.

### Required evidence

- Fresh production DB snapshot/query output for the affected event and source defaults.
- External address evidence for Bar Bastion at `Судостроительная 6/1`.
- Test and py_compile output.
- Release/deploy evidence if code is deployed.

## Immediate Mitigation

- Add Bar Bastion to the canonical reference layer.
- Add aliases for common Bar Bastion mentions (`бастион`, `бар бастион`, `в бастионе`, `бастионник`).
- Seed `vk_source.group_id=149955604` with the current Bar Bastion default location when empty/stale.
- Corrected the affected production event row and source default.
- Corrected future active ticket-site Bar Bastion rows with missing/noncanonical address and requeued Telegraph rebuild jobs for changed rows: `3388`, `3802`, `3938`, `4096`, `4360`.

## Corrective Actions

- Added `Бар Бастион, Судостроительная 6/1, Калининград` to `docs/reference/locations.md`.
- Added data-driven aliases in `docs/reference/location-aliases.md`.
- Added `db.py` seed for `bar_bastion` source default location.
- Added a regression test for both `location_reference` and the event-parse reference normalizer.

## Follow-up Actions

- [ ] Add a lightweight VK-source audit that flags active single-venue sources with `location=NULL` when multiple recent imports infer inconsistent venues.
- [ ] Consider storing compact parse output diagnostics for VK auto-import addressless posts without retaining full prompts.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks:
  - `pytest tests/test_location_reference_bastion.py` → passed (`2 passed`)
  - `python3 -m py_compile db.py location_reference.py main.py vk_auto_queue.py vk_intake.py` → passed
  - runtime mirror checked: `ENABLE_RUNTIME_FILE_LOGGING=0`, `/data/runtime_logs` present but empty
  - production DB check: event `4360` has `Бар Бастион`, `Судостроительная 6/1`, `Калининград`
  - production DB check: `vk_source.group_id=149955604` has `Бар Бастион, Судостроительная 6/1, Калининград`
  - Telegraph page check: `https://telegra.ph/Poehticheskij-vecher-Anny-Grozovskoj-04-29` shows `Бар Бастион, Судостроительная 6/1, Калининград`
- post-deploy verification: pending code deploy; production data mitigation applied directly

## Prevention

Bar Bastion is now represented as a first-class venue instead of relying on the broader `Понарт` cluster or LLM guesses. Future addressless `bar_bastion` posts should receive a deterministic source default and reference normalization before Smart Update persistence.
