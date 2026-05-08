# INC-2026-05-08 VK Quality False Skips

Status: monitoring
Severity: sev1
Service: VK auto-import / Smart Update / Telegraph event pages
Opened: 2026-05-08
Closed: —
Owners: Codex / production operator
Related incidents: `INC-2026-04-28-vk-smart-update-false-skips`, `INC-2026-05-05-smart-update-gemma3-fallback-hallucination`, `INC-2026-05-07-vk-auto-import-merge-regression-gemma4`, `INC-2026-05-08-vk-tg-prompt-and-dup-probe`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`

## Summary

8 May 2026 production VK auto-import / Smart Update lost or damaged four user-visible event surfaces:

- event `4717` (`https://telegra.ph/Koncert-EHtot-Den-Pobedy-05-08`) published a performer surname typo: `Симюран` instead of source-grounded `Симуран`;
- `https://vk.com/wall-48383763_40430` was skipped as `skipped_non_event:non_event_notice`, although it is a paid zoo excursion with date, time, venue and booking link;
- `https://vk.com/wall-78248807_6006` was rejected as `rejected_out_of_region:unknown_region`, although Fort 11 Doenhoff is a Kaliningrad venue;
- `https://vk.com/wall-138053522_2532` was skipped as `skipped_non_event:online_event`, although the post describes an offline art market and only mentions a stream inside the programme.

## User / Business Impact

- Real Kaliningrad events did not reach the public event catalog from production VK auto-import.
- A public Telegraph event page exposed a wrong performer surname.
- Operator trust in automated LLM-first import degraded: the failing posts contained normal event anchors and should not have needed manual recovery.

## Detection

- Detected by operator report during production VK auto-import review on 2026-05-08.
- Runtime file mirror was available on Fly: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, active `/data/runtime_logs/events-bot.log`, 24h retention.
- Production DB snapshot was synced for replay into `artifacts/db/INC-2026-05-08-vk-quality-prod-snapshot.sqlite`.

## Timeline

- 2026-05-08T18:33Z: incident record opened after reproducing failures on a production snapshot.
- 2026-05-08T18:33Z: pre-fix replay confirmed the three VK sources reached LLM draft extraction but failed in Smart Update guards (`non_event_notice`, `unknown_region`, `online_event`).
- 2026-05-08T18:33Z: targeted LLM-first code fixes and docs/tests prepared.
- Deploy/closure evidence: pending in this record until production deployment and remediation complete.

## Root Cause

1. The `non_event_notice` deterministic guard treated discount/ticket wording as non-event notice even when the LLM draft had strong event anchors (`date`, `time`, `venue`, paid booking).
2. Fort 11 Doenhoff was missing from the canonical location/alias layer, so a real Kaliningrad venue with empty extracted city failed the deterministic region gate.
3. The `online_event` guard treated any stream/broadcast wording as event-wide online-only evidence, even when the LLM candidate had physical place/date/title anchors.
4. Public writer output could alter a rare proper surname, and there was no source-grounded known-spelling safety net for this class of high-salience typo.

## Contributing Factors

- Gemma 4 provider 500 instability made full Smart Update writer replay slow and flaky during the incident.
- A complex art-market programme made the upstream VK parser over-split into several child drafts before prompt tightening.

## Automation Contract

### Treat as regression guard when

- Changing `vk_intake.build_event_drafts_from_vk` prompts/schema for VK post extraction.
- Changing Smart Update non-event guards, especially `non_event_notice` or `online_event`.
- Changing geo/region filtering, location reference files or alias normalization.
- Changing writer/fact-first output sanitation for person names and other source-grounded proper nouns.

### Affected surfaces

- `vk_intake.py`
- `smart_event_update.py`
- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- production `event.id=4717` / Telegraph page rebuild path
- production `vk_inbox` rows for `wall-48383763_40430`, `wall-78248807_6006`, `wall-138053522_2532`

### Mandatory checks before closure or deploy

- Replay `tests/replays/INC-2026-05-08-vk-quality-false-skips/sources.json` through live VK fetch / Gemma 4 draft extraction and Smart Update on a prod snapshot or shadow DB.
- Targeted unit tests for Smart Update non-event guards, online guard, spelling guard and Fort 11 alias resolution.
- Verify event `4717` no longer contains `Симюран` in `description`, `short_description` or `search_digest`, and contains source-grounded `Симуран` where the performer is mentioned.
- Verify the Fort 11 candidate resolves to `city=Калининград`.
- Verify offline events with stream/broadcast mentions are not skipped as online-only when they have physical event anchors.
- Verify the fix does not fail-open obvious online-only events or pure non-event notices.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted test output.
- Pre/post replay artifacts under `artifacts/codex/INC-2026-05-08-vk-quality-replay/`.
- Production DB snapshot path and source replay fixture.
- Production remediation evidence for event `4717` and the three VK sources.

## Immediate Mitigation

- No broad regex semantic extraction was added. Fixes stayed in LLM-first flow: upstream VK prompt tightened, Smart Update deterministic guards were narrowed only around LLM-grounded candidates, and location reference data was extended.

## Corrective Actions

- Added source-grounded known-spelling restoration for `Симюран` -> `Симуран` when the source contains the correct surname.
- Narrowed `non_event_notice` so ticket/discount wording does not override a concrete excursion invitation.
- Added a physical-anchor exception to the `online_event` skip guard.
- Added Fort 11 Doenhoff to canonical locations and aliases.
- Tightened VK art-market/fair/programme extraction prompt to prefer one umbrella event when the source describes one place/day programme.
- Added regression fixture and targeted tests.

## Follow-up Actions

- [ ] Stabilize full Smart Update writer replay under Gemma 4 provider 500 storms so incident replay does not require `SMART_UPDATE_LLM=off` for the fast confirmation path.
- [ ] Add a scheduled/offline replay harness for source-quality incident fixtures so new VK prompt changes can batch-check known false skips without operator-driven scripts.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: `.venv/bin/pytest tests/test_smart_event_update_non_event_guards.py tests/test_smart_event_update_location_aliases.py -q` -> `19 passed`
- post-deploy verification: pending

## Prevention

- This incident is now an active regression contract in `docs/reports/incidents/README.md`.
- Replay source fixture is committed under `tests/replays/INC-2026-05-08-vk-quality-false-skips/sources.json`.
- Canonical feature docs describe the narrowed Smart Update guard behavior and the VK umbrella-event prompt contract.
