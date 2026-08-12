# VERIFICATION-NOEVENT lane results

Base: `e13c71b858a26e5ca1fea30dcd11b6ae0e1a7b0e`

Implementation: `193d705e6`

Branch: `agent/smart-update-final-code/verification-noevent`

## Outcome

Implemented one dependency-light pure collector,
`derive_source_contradiction_facts(source_text, all_ocr_blocks,
source_metadata, primary_decision, evidence_manifest)`, in
`source_contradiction_facts.py`. It has no provider, ORM, database, or runtime
I/O and produces verifier facts only. It never assigns a product verdict or a
no-event reason.

Both application ingestion and Telegram Monitor execute that exact module.
The Telegram notebook builder embeds `source_parse_contract.py` and
`source_contradiction_facts.py` into its deterministic import closure before
executing the runner; the generated-notebook test executes the import and calls
the collector.

Main/VK/direct parsing now derives facts automatically from the untouched
carrier, all OCR blocks, explicit `today` metadata, the primary decision, and
the manifest. VK prompt overlays are deliberately excluded from semantic fact
evidence. A clean carrier performs one primary provider call. A non-empty
closed fact set performs at most one existing verifier call. Primary retries
are returned without recursive verification; verifier failures preserve
positive siblings in a typed retry.

`SourceNoEventReason` is mandatory iff the disposition is
`CONFIRMED_NO_EVENT`. Missing, empty, unknown, or misplaced reasons become
`RETRY_REQUIRED/SCHEMA_MISMATCH`. The seven closed values survive core, VK,
TG, receipts, and album aggregation. Legacy reasonless receipts invalidate and
reparse. Defensive direct, forwarded, VK, and TG zero-event terminal gates
require a closed reason.

VK attempt durability now includes `vk_source_packet_attempt.no_event_reason`.
Confirmed no-event attempts cannot be marked structured-valid without a valid
reason and a valid typed receipt. Terminal observability uses
`CONFIRMED_NO_EVENT:<ENUM>`.

## V1-V11

| ID | Status | Evidence |
|---|---|---|
| V1 | Done | Complete no-event plus invitation, event type, attendance, and date/time signal classes derives `NO_EVENT_WITH_STRONG_SIGNALS`; VK and TG production-wiring tests invoke one verifier. |
| V2 | Done | One unambiguous source/OCR date conflicting with one parsed child date derives `EVENT_DATE_CONFLICT`; main and TG wiring tests cover OCR conflict. |
| V3 | Done | Distinct dates, explicit three-session times, typed occurrence anchors, and same-slot concrete OCR cards derive `MULTIPLE_OCCURRENCES_COLLAPSED`, excluding range/recurrence carriers. |
| V4 | Done | Empty/placeholder/bare type-or-type–venue titles derive `GENERIC_UNGROUNDED_TITLE`; existing main defender plus shared collector deduplicate into one verifier call. |
| V5 | Done | Lifecycle-only plus an independently invited, dated future event derives `LIFECYCLE_MIXED_CONTENT_CONFLICT`; main and TG wiring tests cover it. |
| V6 | Done | Impossible calendar dates/times, reversed ranges, price bounds, and typed booleans derive `IMPOSSIBLE_SCHEMA_VALUE`; `31.02` and `28:90` are covered dynamically. |
| V7 | Done | Absent/incomplete/truncated/cardinality-invalid manifests derive `INCOMPLETE_EVIDENCE`; positive children remain present and enrichment/retry continues. |
| V8 | Done | Clean carrier has zero facts and exactly one primary call. |
| V9 | Done | Facts deduplicate in closed enum order; overlapping generic-title producers still invoke only one verifier. |
| V10 | Done | Verifier timeout becomes durable `VERIFICATION_TECHNICAL_ERROR`; positive sibling and attempt reason are preserved. |
| V11 | Done | Unknown/malformed verifier output is bounded to one verifier, becomes typed uncertainty/technical retry, cannot recursively verify, and retains verification reasons/full source/OCR evidence. |

## N1-N9

| ID | Status | Evidence |
|---|---|---|
| N1 | Done | Reasonless complete `CONFIRMED_NO_EVENT` becomes schema retry in core, TG, VK receipts, and consumers. |
| N2 | Done | All seven `SourceNoEventReason` values round-trip through core and TG; VK A-M includes all seven semantic outcomes. |
| N3 | Done | Unknown/empty reasons become schema retry and emit `source_parse_schema_alert`. |
| N4 | Done | A reason on events, mixed, lifecycle, or retry dispositions becomes schema retry. |
| N5 | Done | A valid reason cannot terminalize incomplete evidence; result is `EVIDENCE_INCOMPLETE`. |
| N6 | Done | Direct and forwarded zero-event boundaries require evidence completeness and a closed reason. |
| N7 | Done | VK receipt replay preserves the reason; old reasonless receipts are invalid; attempt schema/model/recording and terminal values are covered. `mark_rejected` requires a closed reason. |
| N8 | Done | TG fresh validation, album aggregation, and consumer terminal checks require/preserve a reason. Unanimous albums preserve it; mixed valid reasons use carrier umbrella `NO_ATTENDABLE_EVENT`. |
| N9 | Done | Master, main, VK, and TG live prompts require the reason and closed enum. Static mutations catch legacy empty instructions, missing mandatory reason, enum drift, and TG schema drift. Dynamic VK A-M covers generic, giveaway, teaser, referral, service/rental, recap, out-of-scope, lifecycle, location hint, and ticket hint with no retry loop. |

## Validation receipt

Focused suite:

```text
PYTHONPATH=. /home/dev/.venvs/events-bot-region-talk/bin/pytest -q \
  tests/test_source_parse_contract.py \
  tests/test_event_parse_llm_first_contract.py \
  tests/test_vk_raw_first_llm_contract.py \
  tests/test_vk_auto_queue_import.py \
  tests/test_vk_review.py \
  tests/test_telegram_monitor_llm_first_producer_contract.py \
  tests/test_telegram_monitor_service.py \
  tests/test_tg_monitor_gemma4_contract.py \
  tests/test_tg_monitor_reprocess_incomplete_scan.py \
  tests/test_ingestion_caller_retry_contract.py \
  tests/test_add_events_from_text_source_decision.py \
  tests/test_source_parse_prompt_contract.py

318 passed, 8 warnings in 36.18s
```

The eight warnings are the existing Python 3.12 `sqlite3` default datetime
adapter deprecation emitted by the official-parser recovery test.

Additional gates:

```text
python3 scripts/inspect/audit_source_parse_prompt_contract.py --root .
source-parse prompt contract OK (4 live surfaces; shared enum parity; legacy TG extractor unreachable)

python3 -m py_compile <all changed production Python files>
PASS

git diff --check
PASS
```

No provider/model/call-count expansion, semantic product regex classifier,
Smart Update edit, recovery-script edit, production mutation, push, or deploy
was performed.
