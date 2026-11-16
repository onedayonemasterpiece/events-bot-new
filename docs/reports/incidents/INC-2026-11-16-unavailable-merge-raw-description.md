# INC-2026-11-16-unavailable-merge-raw-description

Status: open
Severity: sev2
Service: Smart Update / Qtickets / VK related cards
Opened: 2026-11-16
Owners: KenigEvents (codex)
Related incidents: INC-2026-06-29-qtickets-structured-facts-lost
Related docs: docs/features/smart-event-update/README.md

## Summary and impact

Event 10424 description contains the parser instruction «OCR афиши используй…».
A new VK related card also shows malformed historical search_digest text. The
response that created that digest was not retained; its cause is not established.
Existing VK posts and comments must not be rewritten, deleted or recreated.

## Detection and timeline

- 2026-11-16 00:15 UTC: actual uploaded related card inspected.
- Historical source ledger 218919 (2026-11-13 05:11:47 UTC) records deterministic
  description update while LLM unavailable.
- 2026-11-16: local replay of saved Qtickets candidate 25902 through the actual
  parser wrapper, durable facade and merge transaction reproduces the append.

## Root cause

The normal parser wrapper uses schedule_tasks=False. The unavailable-merge
branch interpreted this scheduling parameter as permission to append raw source
sentences without LLM rewriting. The stored parser source includes instructions.
This is removal of an unsafe public-text fallback, not keyword-based semantic
classification. Raw source remains evidence; existing LLM text paths remain.

## Corrective action

Remove the sole-use raw append helper and its branch. Reuse the existing safe
unavailable-merge body for both scheduling settings, retaining topic eligibility.
The existing foreign-schedule cleanup remains unchanged. Preserving a short old
text can activate an existing full-rewrite helper (observed local count 0→1);
this is explicitly not a promise of unchanged model-call counts.

## Automation Contract

### Treat as regression guard when

Changing unavailable merge behavior, parser scheduling options, raw-source
storage or public description fallback.

### Affected surfaces

smart_event_update.py, Qtickets parser wrapper, source/fact receipts, descriptions.
No provider contract, schema, queue, setting or VK write path changes.

### Mandatory checks before closure or deploy

- Same normal wrapper/facade replay: baseline appends instruction; fixed source
  preserves before-description and structural fields with MERGED receipt.
- Source and fact receipts persist; topics and scheduling remain disabled for
  the isolated replay. Socket guard records no external network attempts.
- Existing Qtickets structured facts, parser attach and narrative controls;
  distinguish existing baseline failures from regressions.
- Independent AST review verifies old safe body and surrounding behavior.
- Mandatory CI, merged main release, actual ancestry and live readiness.

### Required evidence

Retained task directory:
/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check
Reports: qtickets10424-normal-unavailable-review-20261116.md and
qtickets-offline-description-branch-simplicity-review-20261116.md.
Raw fixture: tests/replays/INC-2026-11-16-unavailable-merge-raw-description/qtickets10424.json.
Before-description is a reconstructed current prefix, not a saved historical row.

## Release And Closure Evidence

Local code changed; release not started. Repository regression and CI results
will be recorded in the PR. Production receipt and ordinary next import remain
required before closure. No historical row or VK repair was performed.

## Follow-up Actions

- Observe an ordinary unavailable-merge import after release without manual replay.
- Investigate malformed historical search_digest separately if recurring; this
  fix does not establish or repair its cause.
