# P0 final review remediation — integration report

Date: 2026-08-11 UTC
Repository / PR: `onedayonemasterpiece/events-bot-new`, Draft PR #494
Reviewed base: `f72dce8164c5b77a22865032dbbedbc4fd0817d9`
Start delta: none; the local and remote PR head both matched the reviewed base.
Final exact HEAD: the pushed PR ref and the final PR comment are authoritative
(the report commit cannot contain its own SHA).

No merge, deploy, production write, recovery apply, new PR, or issue was
performed. The incident remains open and this report is not a deploy-readiness
claim.

## Four focused blockers

| Blocker | Old path / defect | New path | Dynamic/static proof | Status |
|---|---|---|---|---|
| VK crawl continuation | `_schedule_vk_crawl_continuation` could create a pending row which no production consumer claimed; capped pages could stay beyond raw-first forever. | `vk_intake.py::_claim_vk_crawl_continuation` and `process_vk_crawl_continuations` atomically lease and drain stored pages; `scheduling.py` registers the default-on worker. | `tests/test_vk_crawl_continuation.py` covers deep incremental/backfill, partial failure, stale lease, concurrency, restart/idempotence and scheduler registration; `tests/test_scheduling.py` covers the actual job. | Done |
| Legacy source verdict | Missing decisions, bare `[]`/`None`, malformed mappings and old receipts could acquire `CONFIRMED_NO_EVENT` authority. | `source_parse_contract.py::decision_from_provider_payload`, `vk_intake.DraftParseResult`, `vk_auto_queue._adapt_vk_draft_result` and TG normalization use one fail-closed typed contract. | Source-contract, VK receipt/adapter, TG producer/consumer and caller tests cover explicit no-event versus every invalid compatibility shape. | Done |
| Attachment evidence | `attachment_count > OCR blocks` and omitted cards could still look complete; direct facades could hide unknown media. | Central `EvidenceManifest` canonicalizes cardinality; direct main/VK/TG builders pass attachment counts, omissions and unavailability; enrichment survives receipt replay. | Evidence truth-table tests, direct poster tests and positive-incomplete carrier tests cover missing, partial, complete and omitted OCR. | Done |
| Legacy prompt `[]` | Live master/VK/TG source prompts contained carrier-empty semantics that conflicted with the typed schema and could cause fail-open negatives/retry loops. | All three live surfaces require the closed disposition/retry/no-event-reason contract; `SourceNoEventReason.GIVEAWAY_ONLY` is preserved through durable receipts. | `audit_source_parse_prompt_contract.py`, mutation tests and dynamic A–I cases cover giveaway-only/+event, incomplete cards/+positive child, teaser, lifecycle, location and ticket hints. | Done |

## Final requirement audit

| Original section | Status | Closure evidence |
|---|---|---|
| 1. Continuation consumer, contract 1–12 and tests A–G | Done | Atomic `BEGIN IMMEDIATE` claim, explicit lease/run identity, frozen scan boundary, page-level raw-before-progress, five terminal rails, typed capped retry, stale recovery, concurrency exclusion, exact replay/idempotence, default-on scheduler, and separate incremental/backfill tests. |
| 2. Fail-open legacy source verdict, items 1–6 and tests A–F | Done | One central adapter; missing/empty/null/malformed/unknown shapes retry; non-empty legacy positives require minimum event schema; old/missing-manifest receipts invalidate and reparse; only explicit structured complete no-event is semantic terminal. |
| 3. Attachment evidence, items 1–6 and tests A–E | Done | Central cardinality invariant and direct/VK/TG facade propagation; missing/omitted/truncated media is incomplete; negative becomes retry; positive children persist with durable enrichment due. |
| 4. Typed source prompts, policy 1–7, static gate and tests A–G | Done | Master, VK overlay and live TG producer use the closed object schema; static extraction/mutation gate blocks legacy carrier-empty markers and fails if the historical TG extractor becomes reachable; dynamic A–I cases execute one primary call. |
| 5. README/architecture and model cleanup | Done | Canonical docs describe raw-first automatic typed ingestion and diagnostic-only manual UI; the duplicate `VKInbox.provider_retry_after` declaration was removed and packet ORM parity retained. |
| 6. No scope expansion | Done | No new model/provider, always-on verifier, operator gate, semantic regex classifier or TPM evidence filter; Smart Update/recovery core files are unchanged from the reviewed base. |
| 7. Mandatory static/dynamic gates 1–8 | Done | The exact updated focused CI command passes locally (321 tests), the broader relevant aggregate passes (620), prompt and provider path audits pass, and continuation/evidence/adapter mutations are covered. |
| 8. Release evidence 1–12 | Done for review handoff | This report, the incident update, retained local hashes, strict read-only production receipt and the exact pushed PR ref/comment provide the evidence. Release readiness itself remains blocked by the four explicitly excluded external proofs below. |

No requested implementation or test item is Missing. External consultant review
is Blocked by permitted Opus availability, and release readiness remains Blocked
by the four independent production/provider proofs; neither blocker changes the
Done status of the four focused code remediations.

## Continuation state machine

`pending|retry` due, or expired `running`
→ `BEGIN IMMEDIATE` claim
→ `running` with `attempts+1`, `locked_at`, `lease_owner/locked_by`, `run_id`,
`lease_expires_at`
→ fetch the stored `owner_id/owner_type/scan_mode/page_size/since_ts/offset/`
`horizon_ts/original_cursor_*`
→ persist every fetched revision through `_persist_vk_source_packet`
→ only then CAS-advance the continuation offset.

Terminal rails are empty page, short page, backfill horizon, original
incremental cursor overlap, or exact repeated page fingerprint. A technical
fetch/persist failure clears the lease and writes typed reason,
`next_attempt_at`, and capped exponential/provider backoff. A bounded worker
invocation yields the same row back to `pending` without losing offset. The
canonical `vk_crawl_cursor` is intentionally owned only by the primary crawl;
the continuation is bound to its frozen original cursor boundary.

The scheduler evidence is executable, not documentary:
`scheduling.py` registers `vk_crawl_continuation_worker` by default with bounded
interval, batch, pages/job and lease settings, while the env switch can disable
it explicitly.

## Typed legacy-adapter matrix

| Input | Typed outcome | Semantic terminal allowed? |
|---|---|---|
| Explicit structured `CONFIRMED_NO_EVENT`, matching empty children/actions, valid manifest, `evidence_complete=true` | `CONFIRMED_NO_EVENT` | Yes |
| Legacy `[]`, `None`, missing decision, malformed or unknown shape | `RETRY_REQUIRED/SCHEMA_MISMATCH` | No |
| Non-empty legacy event list/object with nonblank title | validated rolling `EVENTS_FOUND` adapter | Positive children only |
| Receipt without typed decision or manifest | invalidated and reparsed | No |
| Unknown disposition, retry reason or no-event reason | schema-mismatch retry + warning | No |
| `no_event_reason` on a non-no-event disposition | schema-mismatch retry | No |
| Typed positive with incomplete evidence | `EVENTS_FOUND|MIXED`, `enrichment_required=true` | Children persist; carrier remains due |

## Evidence completeness truth table

| Attachments | OCR available | OCR included | Other state | Complete? | Negative verdict |
|---:|---:|---:|---|---|---|
| 0 | 0 | 0 | full source/provider response | yes | explicit typed no-event allowed |
| 1 | 0 | 0 | OCR missing/unavailable | no | retry |
| 2 | 1 | 1 | one attachment unavailable | no | retry |
| 2 | 2 | 1 | one available block omitted | no | retry |
| 2 | 2 | 2 | no omission/truncation | yes | explicit typed no-event allowed |
| any | any | any | source/provider truncation | no | retry |
| any incomplete row | any | any | positive child exists | no | child persists; carrier needs enrichment |

`SourceParseDecision.to_payload()` now keeps `enrichment_required`, and exact
VK receipt replay cross-checks the decision/draft child count and normalized
titles before reuse.

## Source prompt contract and static inventory

The production surfaces are exactly:

1. the fenced `MASTER-PROMPT` read by `main.parse_event_via_llm`;
2. the VK overlay built by `vk_intake.build_event_drafts_from_vk`;
3. Telegram Monitor `_source_parse_prompt`.

They require `EVENTS_FOUND`, `CONFIRMED_NO_EVENT`, `LIFECYCLE_ONLY`, `MIXED`,
`RETRY_REQUIRED`, evidence completeness, closed retry reasons, and the optional
closed no-event reasons. They do not instruct the model to return a legacy
empty array. The large historical Telegram `extract_events` body remains only
for rollback archaeology; the AST gate fails if any production call makes it
reachable and then requires that surface to enter the audit.

The CI workflow runs the prompt audit plus the source/prompt/VK/TG contract
files. No model/provider or always-on second call was added; the normal carrier
still has one primary parse, with only the already-designed closed conditional
verification path.

The semantic prompt diff is deliberately narrow:

| Legacy instruction | Typed replacement |
|---|---|
| return/emit `[]` for a carrier | return one `SourceParseDecision` object with a closed `disposition` |
| empty result for a proven non-event | `CONFIRMED_NO_EVENT` with complete evidence and an optional closed `no_event_reason` |
| empty result when cards/OCR are missing | `RETRY_REQUIRED/EVIDENCE_INCOMPLETE` |
| discard positive children when evidence is partial | retain `EVENTS_FOUND`/`MIXED` children and set enrichment required |
| cancellation/update as an empty event list | `LIFECYCLE_ONLY` with typed actions |
| giveaway-only versus giveaway containing an event | closed `GIVEAWAY_ONLY` no-event reason versus retained event children |

## Caller/static inventory

- VK: raw packet → typed parse/receipt → child Smart Update/lifecycle → accepted
  side effects or durable retry.
- Telegram producer/server consumer: structured decision + manifest, event-array
  consistency and incomplete-evidence enrichment.
- Direct/manual/forwarded facade: actual poster/media attachment count reaches
  `_event_parse_evidence_manifest`; technical errors return a typed retry rather
  than an empty result.
- Official parser/festival/ticket and Smart Update accepted/diagnostic boundaries
  are unchanged by this focused remediation.
- Static gates: live prompt inventory, no legacy carrier-empty markers, dead TG
  extractor call graph, raw-before-semantic VK path, no technical terminal
  failure, accepted-only downstream event IDs.

## Verification receipts

- Continuation lane: 66 passed.
- Typed-evidence lane: 95 passed, 8 warnings.
- Prompt/VK lane: 80 + 100 + 54 focused passes; final owned VK subset 31 passed.
- Exact updated GitHub focused job command: **321 passed, 9 warnings in 48.71s**; retained log SHA-256 `ef1487af5821a6d38006ed91ca4cb63fcf0b6598f250e004f3e2538c8c3c603f`.
- Final relevant local aggregate: **620 passed, 17 warnings in 75.26s**; retained log SHA-256 `684ccab28897e3a919666fd195ee70bcb4e28174654c60c435ebf90a2931decc`.
- Prompt static audit: PASS, three live surfaces, legacy TG extractor unreachable.
- `py_compile` and `git diff --check`: PASS; log SHA-256 `cd3cde0db442f8cf142a8a3b3b7f62a30a7fd29f81a108189a274092c62014f3`.
- Google provider-path audit: PASS, 1,139 files, 0 unapproved, 0 unreadable; log SHA-256 `a51585a63d3eb060dcc4b94e0109d63d5c9efa8c35151d48bc26dd0573606c15`.
- GitHub Actions on the exact pushed SHA: **pending push**.

The raw-first and scheduling test modules now dispose their per-test SQLite
engines. This fixes assertion-complete pytest processes waiting at interpreter
shutdown on unclosed `aiosqlite` worker threads; it does not change production
behavior.

## Strict read-only production recovery repeat

Current local recovery source was executed in memory on Fly without copying or
writing a remote script. Window:
`[2026-08-04T00:00:00+00:00, 2026-08-12T00:00:00+00:00)`; flags are equivalent
to `--read-only --batch-size 10000 --include-discovery-misses`.

- final-source recovery SHA-256:
  `d18d7565f87484e284b95a2482f0e470c5e5db8e579cdecb31c0457f518a5c54`;
- census SHA-256:
  `dc5de3015fdbeaeedd8893156b9eba9e552bb133a5320c5f1a3312861af6d49a`;
- selected/would-change/changed queue carriers: `125 / 125 / 0`;
  Telegram `6`, VK `119`, ticket `0`, festival `0`;
- parser evidence identifies four affected sources, but the pre-migration
  production DB has no durable parser recovery-request table, so parser carrier
  selection is reported unavailable rather than invented;
- A–T census unit `carrier_revision`: `676` observed carriers. Its `256`
  extracted event occurrences are a separate measure, not carrier/recovery
  counts;
- SQLite `mode=ro`, `query_only=1`, `quick_check=ok`, write probe rejected,
  connection `total_changes=0`, file stat unchanged, DB SHA-256 before=after
  `57b6116edd97455baed4f83fc7b148a9211c81dd8a99f054ba7718ebfb427aea`.

Ignored detailed receipt:
`artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/p0-final-remediation/production-read-only/`.
No refetch, provider/model call or recovery apply occurred. The replay remains a
plan; model-derived recovered events/actions/true-no-event totals remain
unavailable.

## Repeated review and release status

The four focused blockers are ready for final local/GitHub CI evidence review.
They are **not deploy-ready** and the incident is not closed. The following
remain external release blockers exactly as requested:

1. real provider quota/tier entitlement;
2. an atomic production DB snapshot rehearsal;
3. an approved disposition/repair decision for existing FK orphans;
4. model-derived recovery replay through the deployed typed pipeline.

The Antigravity Opus review endpoint required interactive re-authentication and
timed out; the permitted Claude Code `Opus` fallback produced no response for
more than eleven minutes and was terminated. Both attempts are recorded in the
ignored external-review artifacts. This availability blocker was not replaced
by a lower-class model review.
