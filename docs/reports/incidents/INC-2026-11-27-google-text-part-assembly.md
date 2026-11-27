# INC-2026-11-27-google-text-part-assembly Visible response boundaries change text

Status: open
Severity: sev3
Service: shared Google AI gateway / generated event text
Opened: 2026-11-27
Closed: —
Owners: KenigEvents
Related incidents: DevCoveer `inc_af96f6510a55a8ccc1d713dc`, `INC-2026-08-01-telegram-monitor-google-ai-package-closure`, `INC-2026-08-01-guide-google-ai-package-closure`
Related docs: `docs/llm/request-guide.md`, `deploy/runcoveer/README.md`

## Summary and impact

The adapter trimmed every visible response part and inserted a newline between
parts. Valid provider fragments could therefore split a word or JSON string,
replace a space, or collapse paragraph boundaries. The installed SDK instead
concatenates visible text without inventing separators. This is a mechanical
transport defect; prompts, semantic scores and event meaning are unchanged.

The observed canonical examples `Узна йте` (11334) and `Тиффа\nни` (11564)
remain separate historical investigations: their original response parts were
not retained, so this reproduction does not establish their individual cause.

## Detection and root cause

On 27 November 2026, six offline adapter/installed-SDK fixtures exposed four
differences. A further standalone-whitespace-part control rejected the first
proposed fix. The full `_call_provider` fake new SDK regression then failed
five cases before the corrected patch and passed all eight afterwards.

## Automation Contract

Treat this record as a regression guard when response extraction, thought
filtering, empty-response handling or embedded Google gateway packaging changes.
Affected surfaces are `google_ai/client.py` and its existing Telegram/Guide
notebook snapshots; no production schema or provider configuration changes.

Mandatory checks before deploy/closure: full client tests, isolated generated
notebook package imports, provider path audit with zero unapproved/debt paths,
required CI, exact-main release, readiness and the next ordinary generated text.
Word fragments, separate whitespace, paragraphs and JSON must retain visible
content; thought-only/blank responses and the legacy fallback keep their rules.

## Corrective Actions

Concatenate untrimmed visible string parts, trim once after assembly and keep
the existing fallback when assembly is blank. Regenerate only the embedded
gateway mappings with the existing notebook builders; preserve other notebook
content. No new state, retry, prompt rule, model call or repair mechanism.

## Release And Closure Evidence

Before: 5 failed / 3 passed. After: 85 client tests passed locally; 114 client
and Telegram/Guide package tests passed in an isolated existing image with
network disabled and no production DB/env mounts. Retained evidence:
`/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check/text-assembly-*.log`.
Code is not deployed at the time of this record; root owns exact-main release
and ordinary output verification. Historical VK edits/replay are excluded.

## Follow-up and prevention

Keep the parameterized fake-provider regression and shared package audit. Root
retains the separate source-quality investigations and verifies new imports;
old malformed text is not silently rewritten or declared repaired by this fix.
