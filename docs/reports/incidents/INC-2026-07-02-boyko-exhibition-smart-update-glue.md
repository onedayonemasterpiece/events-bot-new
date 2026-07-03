# INC-2026-07-02 Boyko exhibition Smart Update glue

Status: monitoring
Severity: sev2
Service: events-bot Smart Update event identity / public `@kldevents` event correctness
Opened: 2026-07-02
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-06-28-opening-exhibition-range-duplicate`
Related docs: `docs/features/smart-event-update/README.md`, `docs/operations/incident-management.md`

## Summary

A source about the exhibition `Калининградская область. История любви` was incorrectly glued into the existing single-slot lecture/event `Калининград и область как кинодекорация` with Андрей Бойко. The affected public Telegram post was reported as `https://t.me/kldevents/1734?single`; Андрей Бойко stated he was not related to the exhibition, which indicates a false Smart Update merge/update side effect rather than a valid same-event update.

The incident predates the current vector-search/create identity gate. Vector recall helps the create path, but this failure family is a merge-path side-effect problem: Smart Update had already selected an existing row as `match_event` and then allowed source/poster/fact/text mutation of that row.

## User / Business Impact

- A public event card could attribute exhibition context to a person/event that did not belong to it.
- Telegraph/Telegram/VK surfaces could inherit wrong description/source/poster facts after a false merge.
- Operator trust in Smart Update match/merge safety is reduced for festival/exhibition sibling contexts.

## Detection

- User report: public post `https://t.me/kldevents/1734?single`; Андрей Бойко said he had no relation to the exhibition.
- Investigation found this as a likely erroneous merge/update path, not a duplicate-create path.

## Timeline

- 2026-07-02: user reported the public post correctness issue.
- 2026-07-02: incident diagnosis recorded: likely Smart Update glue between related-but-distinct exhibition and lecture contexts.
- 2026-07-03: merge-path identity gate implemented behind `SMART_UPDATE_MERGE_IDENTITY_GATE=off|shadow|enforce`, with Boyko regression tests.

## Root Cause

1. Smart Update had create-path identity protections and widened duplicate recall, but no dedicated LLM-first side-effect gate after a candidate was matched to an existing event.
2. Festival/exhibition sibling context (same venue/date/theme/campaign) could be treated as enough evidence for merge/update even when the candidate and existing row represented different real-world event identities.
3. Existing vector identity gate was create-path-only: it could prevent duplicate row creation, but it did not guard source/poster/fact mutation on an already selected match.

## Contributing Factors

- Shared venue/date/context are common for exhibitions plus related lectures/talks.
- Title overlap such as `Калининград и область...` can be a weak bridge between distinct exhibition and lecture entities.
- Before this fix, decision logging for identity gates did not explicitly distinguish merge-path side-effect decisions.

## Automation Contract

### Treat as regression guard when

- Smart Update matching, merge, source logging, poster merge, fact merge, vector identity, dedup adjudication, or event identity decision logging changes.
- Any incident mentions false merge/glue between exhibition/festival parent context and atomic talks/lectures/opening events.

### Affected surfaces

- `smart_event_update.py` match/merge orchestration.
- `smart_update_identity.py` identity verdict helpers.
- `event_identity_decision_log` decision evidence.
- Public event correctness for Telegram/VK/Telegraph/static pages after Smart Update imports.

### Mandatory checks before closure or deploy

- Unit tests for merge identity verdict behavior: off, shadow, enforce skip, and positive same-event allow.
- Smart Update replay/regression for the Boyko-style case: a long-running exhibition candidate matched to a single-slot lecture must return `skipped_identity_gate` in enforce mode and must not mutate `event`, `event_source`, posters, facts, or jobs.
- Positive control: a true same-event lecture source update must not be blocked.
- Confirm create-path identity gate tests still pass.

### Required evidence

- Test output for `tests/test_smart_update_merge_identity_gate.py`.
- Test output for existing create-path identity gate tests.
- Release SHA and production config state if deployed/enabled.

## Immediate Mitigation

- Public/data repair was handled separately during the incident investigation. This record focuses on preventing the failure family in code.
- No production backfill or bulk old-page/source rewrite is required for this prevention change.

## Corrective Actions

- Added `SMART_UPDATE_MERGE_IDENTITY_GATE=off|shadow|enforce`.
- Added an LLM-first decision-only merge identity stage before merge side effects. It classifies `same_event`, `source_update`, `related_but_distinct`, `festival_context_sibling`, or `unsafe_to_merge`.
- In `enforce`, unsafe/review verdicts return `skipped_identity_gate` before changing event fields, sources, posters, facts, or scheduling jobs.
- Added narrow deterministic fail-closed rails only for structural contradictions, not broad regex semantics.
- Persisted merge-gate decisions in `event_identity_decision_log` with `decision_payload.stage=merge_identity_gate`.
- Added regression and positive-control tests in `tests/test_smart_update_merge_identity_gate.py`.

## Follow-up Actions

- [ ] Enable `SMART_UPDATE_MERGE_IDENTITY_GATE=shadow` in production first and inspect decision-log precision on real Smart Update traffic.
- [ ] Promote to `enforce` after shadow evidence confirms no valid same-event updates are blocked.
- [ ] If production still has stale public surfaces from the original incident, repair them through the standard event repair workflow with row backups and public Telegram/VK/Telegraph verification.

## Release And Closure Evidence

- deployed SHA: `672c2e34c3559f71f6a3e674bd8e816146fabce2` (reachable from `origin/main`).
- deploy path: manual `flyctl deploy --config fly.toml --remote-only` from clean worktree; Fly image `events-bot-new-wngqia:deployment-01KWK8AETKNW6NB3NY3PC6E6H8`; machine version `1570`.
- rollout config: `SMART_UPDATE_MERGE_IDENTITY_GATE=shadow` set on Fly after deploy, so merge-gate decisions are logged without blocking live imports yet.
- regression checks:
  - `python3 -m py_compile smart_update_identity.py smart_event_update.py` — passed locally.
  - `.venv/bin/pytest -q tests/test_smart_update_merge_identity_gate.py` — 6 passed locally.
  - `.venv/bin/pytest -q tests/test_smart_update_identity_gate.py tests/test_smart_update_identity_persistence.py tests/test_smart_update_identity_incident_replay.py tests/test_smart_update_merge_identity_gate.py` — 26 passed locally.
  - `.venv/bin/pytest -q tests/test_smart_event_update_non_event_guards.py` — 27 passed locally.
- post-deploy verification: `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, DB/scheduler/tasks OK and no issues after the Fly secret restart.

## Prevention

The prevention mechanism is a merge-path identity gate, separate from create-path vector identity. It is intentionally LLM-first and decision-only: it does not try to rewrite/repair the matched row, and it does not create replacement events. In enforce mode it stops unsafe side effects before they can pollute an existing event.
