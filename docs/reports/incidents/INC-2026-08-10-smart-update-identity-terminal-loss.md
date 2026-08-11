# INC-2026-08-10 — LLM-first ingestion recall / identity terminal loss

Status: **open**
Priority / severity: **P0 / SEV-1**
Service: configured-source ingestion (VK, Telegram Monitoring, official parsers,
ticket/festival queues) and Smart Update
Opened: 2026-08-10
Closed: —
Related: `INC-2026-08-04-smart-update-identity-source-replay-corruption`

## Summary

The product promises a maximally complete regional event catalogue. Automatic
ingestion fell from a normal 30+ new events/day to roughly 2–5/day. This is a
recall incident, not an operator-review workflow: the product has no review
operator and may not strand candidates in `review_required`, `skipped`, generic
`failed`, or a deterministic semantic reject.

Two defect families overlapped:

1. the August Smart Update identity/caller boundary converted uncertainty into
   terminal/repeated downstream loss instead of automatic merge, distinct
   create, exact no-op, or durable retry;
2. older VK/TG discovery, evidence-budget, cancellation and post-LLM guards
   could prevent LLM coverage, omit OCR, or discard a positive child.

The count “109” from the first recovery pass meant **carrier rows** (5 Telegram
+ 104 VK), not 109 events. One carrier can contain zero, one or many event
occurrences and lifecycle actions.

## Impact and detection

- Canonical/public events were missing; some imported carriers regressed to a
  failed/deferred status and were processed again.
- A diagnostic Event ID could be mistaken for accepted persistence by callers.
- One VK/TG carrier URL could not safely bind multiple independent occurrences.
- Technical/provider/quota failures could become terminal or hold a worker
  inline; pre/post-LLM semantic shortcuts produced chronic false negatives.

The original identity audit froze
`[2026-08-04T00:00:00Z, 2026-08-10T07:17:05Z)`, opened Fly SQLite with
`mode=ro`, `query_only=1`, and got `quick_check=ok`. Both production identity
gates were `enforce`. The identity ledger had 413 rows (164 allow-create, 218
allow-merge, 13 review-required, 16 skip-side-effects, one veto), but recorded
zero `source_binding_conflict` rows while VK/parser runtime evidence did contain
those conflicts. The terminal and identity ledgers were not balanced one-to-one.

## Proven timeline

- Chronic discovery hint filters originate in September–October 2025. The first
  reachable snapshot containing current auto-import preclassification,
  cancellation bypass and free-form `reject_reason` is `bce860abd`
  (2026-04-11); this proves presence by that date, not the original deploy.
- The fixed VK Gemma 4 route existed from 2026-04-26. Long-caption
  logistics-only OCR budgeting appeared 2026-07-02.
- Shared quota cutover and 14.5k reservation/fit changes landed on 2026-07-31.
- Identity repair `5082a50` (2026-08-04 16:26Z) was followed by Fly releases
  v1907/v1908. Caller outcome boundary `86c4a62` (2026-08-05 09:18Z) was
  followed by v1910+. This is the plausible acute identity cliff route; older
  semantic filters explain chronic loss, not an August-only invention.
- Exact current deployed runtime files matched repository commit
  `f66330f8af81d4b898d137d83356e77914dce90a`; Fly release v1969 used image
  digest `sha256:32cc…6563`. Historical images have no trustworthy revision
  label, so old image-to-commit mapping is not claimed.
- Supabase crawl snapshots covered 121 sources on August 1–4, 33 on August 5,
  two on August 6, zero on August 7–9, and 121 again on August 10. Snapshot
  count is an operational signal, not an event/carrier universe.

The required `git log -S` inventory, deploy/env chronology and AS-IS control
flows are preserved in
`.codex/integration/smart-update-llm-first-recall/TIMELINE_AND_CONTROL_FLOW.md`
and the ignored forensic timeline artifact.

## Read-only all-source census

The strict read-only evidence pack assigned 1,634 **observed, non-overlapping
evidence rows** to exactly one A–T class. It is not a complete fetched-carrier
universe and not an event count: 748 rows are incomplete sampled VK discovery
observations and 250 are success/exact-replay evidence.

Key acute evidence:

- VK inbox: P=13 Smart Update identity losses, Q=114 technical terminals,
  S=129 success/replay evidence, T=232 unavailable/unknown evidence.
- Telegram: 398 carriers, 526 extracted occurrences, 147 imported occurrences.
  The observed child gap is 379 occurrences across 202 carriers; this is an
  extraction/import ledger gap, not yet a model-derived recovery result.
- Official parsers: 2,326 processed, 123 failed and 19 skipped repeated
  observations; exactly four sources had failures. Repeated observations cannot
  honestly be converted into unique carriers with the retained schema.
- Ticket queue: 302 active rows with zero attempts and its scheduler flag unset.
  Festival queue: 1,382 pending rows with zero attempts.
- Historical sampled VK evidence is retained only from 2026-06-12. February–May
  raw packets and exact configured-source history are unavailable, so the
  February–July false-negative rate cannot be measured. Missing evidence is T,
  never extrapolated by multiplying sampled misses.

The production DB had no new `vk_source_packet`, Smart Update candidate-state,
or attempt tables at audit time. Therefore exact global never-LLM count,
incomplete-evidence count, and **model-derived** recovered events/actions/
true-no-event/would-create/would-merge totals remain unavailable. They must not
be inferred from 109, parser source counts, or the 10% miss sample.

Production evidence was read-only: SQLite `mode=ro`, `query_only=1`,
`quick_check=ok`, `total_changes=0`, observer changed rows=0, and the core DB
SHA remained unchanged. Supabase access was GET-only. No refetch/model replay or
recovery apply was performed.

## Root cause and contributing factors

1. Smart Update exposed free-form/coarse outcomes and conflated accepted and
   diagnostic IDs.
2. Identity gates could veto/review without a durable automatic state machine;
   canonical source URL was treated as a single-event identity.
3. Producer ordinal could outrank source-native/structured occurrence identity,
   so sibling reorder/insertion could rebind events.
4. VK discovery crossed posts with semantic hint filters before a durable raw
   packet. Cursor/cap handling did not guarantee replayable continuation.
5. OCR/evidence budgeting could omit blocks; empty/invalid LLM results and
   incomplete evidence were not structurally distinct from no-event.
6. Regex cancellation and post-LLM `reject_reason`/date/title/venue guards could
   terminate a carrier or delete one child without a typed LLM verdict.
7. Quota admission over-reserved, callers slept/retried inline, and technical
   states could become terminal instead of remaining due.
8. Telegram/parser/VK/ticket/festival/manual callers translated results
   independently; attempt and child balances were not durable.

## Corrective implementation in Draft PR #494

The unmerged branch `integration/smart-update-identity-state-machine` now
contains the following reviewable blocks:

- raw-first `vk_source_packet` revisions, append-only physical-attempt receipts,
  continuation jobs, cursor-after-durability and changed-revision replay;
- complete source/OCR `EvidenceManifest`, one typed `SourceParseDecision`, mixed
  lifecycle+events, and conditional (not always-on) verification;
- removal of VK pre-LLM semantic prefilter/cancellation bypass/post-LLM child
  veto; incomplete evidence permits positive children but forbids no-event;
- project/model-scoped fail-fast gateway metadata, provider `countTokens`,
  model/consumer/prompt-version p99 calibration and finish/truncation receipts;
- durable quota/backpressure: lease release, `next_attempt_at`, scope/reason/
  retry-after, fairness, exact successful parse replay and no terminal technical
  failure;
- Smart Update closed reasons, accepted/diagnostic ID separation, candidate/
  attempt ledger, `UPSERT_EVENT`/`ATTACH_CONTEXT`, occurrence-scoped bindings,
  incoherent-merge rollback to distinct create and technical retry;
- stable source-native/vendor/structured occurrence identity and accepted-only
  caller side effects across VK, Telegram, parsers, ticket/festival/manual paths;
- A–T read-only census, all-source recovery planner, historical sampler,
  observability metrics/static gates and snapshot migration rehearsal.

No new provider/model was added. Normal carrier path remains one primary parse;
verification reuses an existing configured model only for a closed
contradiction class.

## Capacity evidence

For successful acute attempts:

- Gemma n=502: input p50/p95/p99/max 5,958/6,382/6,741/7,124;
  output 323/1,208/3,891/5,689; actual total p99/max 11,015/11,695; reserved
  14,500 throughout.
- Flash Lite n=411: input p99/max 8,473/14,410; output p99/max 407/4,209;
  actual total p99/max 8,849/18,619 while reservation p99/max was
  31,229/45,987.
- No separate thought field existed historically; derived thought was zero for
  these plain GenerateContent receipts. New code persists it explicitly.
- Audited p99 demand was six requests/minute. Deterministic local replay at
  1.5× (nine carriers) placed every rate-limited carrier into durable retry and
  drained the backlog to zero after quota recovery, with no loss.

Provider billing tier, active provider-side RPM/TPM/RPD and spend limit remain
unverified. Six configured redacted scopes do not by themselves prove six
independent provider projects. App limits are not provider entitlement proof.
No production-like live semantic/model replay was run.

## Production-snapshot migration rehearsal

A current Fly SQLite bundle was fetched read-only into ignored artifacts. The
main DB and WAL were unchanged by inspection; `quick_check=ok`. A clone-only
rehearsal ran full `Database.init()` twice, index/conflict inventory, acute
census, read-only recovery plan, compatibility rollback probe and final
`quick_check`; status was `passed`.

The snapshot already contained 195 foreign-key orphans:
45 `event_source_fact→event`, 111 `event_source_fact→event_source`, 37
`eventposter→event`, and two `videoannounce_item→event`. Migration introduced
zero new FK violations and zero candidate/occurrence identity conflicts. A
hash-bound deterministic repair plan was emitted but **not executed**, because
production writes are forbidden. Any new migration conflict still fails the
gate closed.

Rehearsal report SHA-256:
`965bf0b2996c8d24fabe21e1450e5a81d0c4e7a5d688d9b294f3a13596334a7e`.
The direct DB/WAL download is not an atomic Fly snapshot, although the copied
bundle passed quick-check; the rehearsal itself hashes its immutable input
bundle before/after.

## Recovery status and limits

The recovery command supports half-open `--since/--until`, source and A–T loss
class filters, `--include-discovery-misses`, bounded batch, `--read-only`, and
idempotent plan hashing. It feature-detects raw/durable and all legacy
Telegram/VK/parser/ticket/festival surfaces. It does not insert Events directly.

The current task permits only strict read-only planning. Historical raw payload,
attachments and complete OCR do not exist for many legacy carriers, so a true
raw→LLM→Smart Update model-derived dry-run cannot be completed for them and is
reported unavailable. Production recovery apply is forbidden until merge,
deploy, health/migration gates and separate explicit approval.

## Mandatory regression contract

Before review/release, require:

- all requested T01–T76 discovery/evidence/verdict/verification/queue/Smart
  Update/recovery/static cases;
- provider path audit with zero unapproved paths;
- carrier and child balance, no pre-LLM semantic terminal, no deterministic
  post-LLM veto, no incomplete-evidence no-event, no technical terminal failed;
- fresh/legacy SQLite init, init×2, uniqueness/conflict probes, quick-check and
  rollback rehearsal;
- exact/reordered/inserted sibling identity and accepted-only side effects;
- strict read-only census/recovery receipts and production DB unchanged proof;
- exact `origin/main` release SHA and compensating current-day catch-up only
  after separately approved merge/deploy.

## Evidence

Ignored artifact pack:
`artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/p0-llm-first-audit/`.

Final forensic hashes:

- report: `b56de401694fbd840dd2ea382998b06c05a40cb3a5341bc4653be76a7eaa4e9b`;
- full census: `bbdd6a28200c771dccd1eca0b8c9338f9cc1d47fc6577dd36e9996db22306b82`;
- capacity report: `7222debc01bed6f4f8505427cf54ee9524146c59028a63860a0f1890351140af`;
- capacity JSON: `62720a27814a6d7c8a2214a2e6337cadce13bba6e3990fadbd9a3d18e4e964db`;
- frozen timeline: `7361713debce0431f967757ccec43bc8ac4e7b827f1782dcdbb1015e519185c3`;
- manifest: `8c0550f7f57ad16f871c153c14e2c0d5f58b5c1a015301cf3cfa0bc0a919edad`;
- post-rebase focused suite: 467 passed, 17 warnings; log SHA
  `405dccdbb077146f4228584a7473067f51622cb407dd5ef574dec65afec4cf02`;
- final relevant aggregate: 489 passed in 70.82s; log SHA
  `07092f11d064f942d54ecd060ff6c9380a019e0d4953584499c05795111c1e0f`;
- final provider path audit: PASS, 1,138 files, zero unapproved/unreadable;
  log SHA `16009d8e7922c173cf4ba8fcdcab4e63c40655658918d286648068b557660436`.

Current-source offline production-bundle recovery dry-run was repeated twice
with the half-open window `[2026-08-04, 2026-08-12)`: both reports are
byte-identical (SHA
`3ad6810a48720448141e0d003fae5885db31fc8bd2f895b1a2404fb916433699`),
selected 5 Telegram + 119 VK rows, reported `would_change=124`, `changed=0`,
and left the main DB hash
`d0bd007994eb71e8587039da04249bf30af6175b03e2716b94d43076f29a44c1`
unchanged. It is a requeue plan; it does not manufacture model-derived event
counts from missing historical raw evidence.

The final exact branch HEAD is authoritative in the PR ref/final comment; the
integrated test/static receipts are kept in
`.codex/integration/smart-update-llm-first-recall/INTEGRATION_REPORT.md`.

## Release, rollback and open blockers

This task explicitly forbids merge, deploy, production write and recovery
apply. Production is unchanged and the incident remains open. Review readiness
is blocked/partial where exact historical evidence, provider-tier verification,
live model routing benchmark or model-derived recovery counts are unavailable;
green CI alone does not close the incident.

Rollback is code-first. Keep additive packet/candidate/attempt tables and
nullable EventSource keys; do not restore global one-URL/one-event uniqueness
after multi-occurrence data exists. Destructive SQLite rollback requires a table
rebuild and is not the default. Release/catch-up commands and stop conditions
are canonical in `docs/operations/release-smoke-smart-update.md`; production
audit rules are in `docs/operations/smart-update-prod-audit.md`.
