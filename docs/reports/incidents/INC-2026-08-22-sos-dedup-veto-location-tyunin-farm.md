# INC-2026-08-22 August Smart Update deduplication regression

Status: investigating
Severity: sev1
Service: Smart Update identity / Telegram Monitoring / public projections
Opened: 2026-08-22
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-07-11-event-vector-sidecar-sync-stalled`, `INC-2026-08-04-smart-update-identity-source-replay-corruption`, `INC-2026-08-10-smart-update-identity-terminal-loss`, `INC-2026-08-22-tyunin-farm-location-drift`
Related docs: `docs/features/smart-event-update/README.md`, `docs/features/smart-event-update/identity-state-machine.md`, `docs/features/unsigned-personalization/semantic-vector-retrieval.md`, `docs/operations/smart-update-prod-audit.md`

## Summary

An August control-flow regression allowed ordinary Event creation after the
existing identity gate found an owner and returned `VETO_CREATE`. The widened
vector recall and existing LLM dedup adjudicator both ran, but the caller
treated any non-empty adjudicator response as resolved. When merge acceptance
rejected `no_merge`, retry was bypassed and ordinary CREATE stayed reachable.

The production smoking gun is the Barn/SOS packet from
`https://t.me/kldevents/3619`: owner `8117` was found, a
`deterministic_same_ticket_slot` veto was persisted, `no_merge` followed, and
event `8242` was inserted about twenty seconds later. The same defect class is
present in qTickets refreshes `7580 → 7753` and `7603 → 7705`.

This is the systemic P0/SEV-1 record. The Tyunin Farm location defect reported
at the same time is a separate linked incident, not a root cause.

## User / Business Impact

- Duplicate active/upcoming rows can fan out to Telegram, VK, Telegraph, ICS,
  static pages, vectors and digests.
- A persisted veto did not describe the committed result, making the identity
  ledger incomplete.
- Reminder updates and vendor-range refreshes could create a new Event after
  recalling an existing owner.
- Broad repair is dangerous: one source can legitimately contain many children,
  so source URL and heuristic similarity are recall signals, not merge proof.

## Detection

Operator review found five public rows around the 22 August 21:00 Barn/SOS
programme. Production runtime-file logs supplied the end-to-end trace. An exact
checkpoint matrix and control-flow diff localized the first bad commit.

## Timeline

- `2026-07-02..11` — the separate vector-sidecar incident leaves `217/334`
  eligible documents projected. Post-Smart-Update enqueue and an independent
  three-hour reconciliation restore `334/334` for both kinds.
- `2026-07-31` — late-July replay remains fail-closed after unresolved veto.
- `2026-08-10 23:42:36Z` (committed `2026-08-11 19:35:52Z`) —
  `12e4e7c36c25ac250e9cd5c03972420c782dced7` first
  leaves CREATE reachable. Parent `04823317c8cafd438db01bb519cbf96d63f7bf00`
  is last good.
- `2026-08-12 08:05:42Z` — PR #494 merge `69ec40342` brings the regression to `main`
  alongside the required occurrence-scoped candidate/attempt work.
- `2026-08-22 01:51:35Z` — runtime records owner `8117`, `VETO_CREATE`
  (`deterministic_same_ticket_slot`, confidence `0.92`) and one existing dedup
  LLM call.
- `2026-08-22 01:51:38Z` — adjudicator returns `no_merge/llm_create`, confidence
  `0.90`, without source-grounded distinct proof.
- `2026-08-22 01:51:58Z` — ordinary create commits event `8242`.
- `2026-08-22 13:54:39Z` — pre-fix vector reconciliation `ops_run=6937`
  finishes successfully.
- `2026-08-22 16:49:52Z` — during rollout, four minutes before the first
  prevention exact-main deploy, the existing merge identity LLM correctly
  calls `7907/8280` a `source_update`, but a date conflict rewrites the
  intermediate relation to `unsafe_to_merge`; legacy merge-path application
  then creates `8280`.
- `2026-08-22 16:53:55Z` — prevention PR #554 merges to `origin/main` as
  `c0103be2c7ddc760486bd79e19825a69ceae4165` and deploys as Fly version
  `2016` / image digest
  `sha256:7a6f7cea54294a5262fae9a14344856340409d0fed909cc229c4ff4619aab8b2`.
- `2026-08-22 17:01:50Z` — the reviewed 21-merge / 3-keep-distinct manifest
  passes dry-run. Its first production apply reaches the `number-13` poster
  graph and hits `ux_eventposter_event_raw_sha256`: different derived
  `poster_hash` values represented identical raw bytes. SQLite rolls the
  transaction back; `quick_check=ok`, receipt count stays zero and none of the
  selected Events is marked merged.
- `2026-08-22 17:25:11Z` — a repost-profile venue default makes the merge LLM
  misclassify reminder `8284` against owner `3216` as
  `related_but_distinct`; legacy merge-path application creates it. The
  post-repair census detects both recurrences, so the incident remains open.
- `2026-08-22 18:19:42Z` — PR #556 is deployed from exact
  `origin/main@2c2a10c1688f1b1a58521da44898dbe0905f0936` as Fly version
  `2018`. The older merge path now uses the same terminal
  `FINAL_MATCH / FINAL_DISTINCT / FINAL_RETRY` contract.
- `2026-08-22 18:44:36Z` — final census manifest v4 freezes 13 true
  duplicate clusters / 18 obsolete Events and five reviewed
  `KEEP_DISTINCT` pairs. Apply succeeds, verify is clean and the second
  apply is a zero-change no-op.
- `2026-08-22 19:14:25Z` — the final untruncated census contains 661
  active/upcoming canonical Events. Its only three hard-signal pairs are
  source-reviewed legitimate occurrences; unresolved hard duplicates are
  zero.
- `2026-08-22 19:12..19:14Z` — post-fix backend ingestion proves both
  terminal safety directions: `FINAL_MATCH` updates owner `7895` with no
  create, `FINAL_RETRY` creates nothing, and a separate product rejection
  creates nothing.
- `2026-08-22 19:24..19:38Z` — the final static catch-up repeatedly stops
  before Kaggle with `vector_barrier_catalog_revision_pending`. A read-only
  production comparison proves all Event public revisions are unchanged, but
  the barrier's in-process exporter performs best-effort remote image probes
  while the canonical vector owner intentionally uses `--skip-image-probes`.
  The same immutable catalog therefore hashes differently according to media
  probe results. The comparator is corrected to use the vector owner's
  deterministic probe-free projection; the barrier itself remains mandatory.
- `2026-08-22 20:00..20:10Z` — a captured vector snapshot proves its receipt,
  captured snapshot and live full-DB catalog revisions are identical. The
  next compact-snapshot attempt still drifts. Full-versus-compact payload
  comparison localizes all 270 differing Events to `source_urls` ordering:
  the production DB and allowlisted projection have different indexes, while
  `collect_source_urls` used an unordered `event_source` scan. Ordering by the
  durable source id restores byte-identical payloads without suppressing the
  catalog barrier or changing event identity.

## Root Cause

1. First-bad commit `12e4e7c36` introduced `identity_gate_adjudicated` and
   set it for every non-null adjudicator response before establishing a merge
   or evidence-grounded distinct result.
2. `_dedup_adjudicator_accept_merge(...)` could reject that response and
   continue as `no_merge`.
3. The later fail-closed guard ran only when the boolean was false.
4. Ordinary CREATE therefore remained reachable after an owner and enforced
   create veto.
5. The first prevention patch closed the widened create-path adjudicator but
   left an older merge-path terminal mapping unchanged: every non-null
   `identity_distinct_reason`, including synthetic `unsafe_to_merge`, still
   invoked `_create_from_prepared_candidate`. A merge veto was therefore still
   conflated with positive distinct proof.

| checkpoint | unresolved owner/veto result |
| --- | --- |
| late-July `6775815de` | fail-closed |
| `5082a50` | fail-closed |
| `86c4a62` | fail-closed |
| last good `04823317c` | fail-closed |
| first bad `12e4e7c36` | ordinary CREATE reachable |
| PR #494 `69ec40342` | bad path reaches `main` |
| pre-fix main `ddd75b741` | bad path present |

Artifacts: `artifacts/codex/INC-2026-08-22-dedup-regression/first-bad-checkpoint-matrix.txt`,
`first-bad-control-flow.diff`, `first-bad-annotated-trace.txt`,
`runtime-sos-control-flow.txt`, and `runtime-sos-excerpts.txt`.

## Contributing Factors

- `no_merge` conflated explicit distinct, abstention and provider/schema
  uncertainty; it required no source-grounded occurrence/event difference.
- The RPC returned a small ranked list per vector kind, but Smart Update kept
  only rank one. Known true owners occur at ranks 2–3.
- Exact city/type filters hide owners after canonical-field drift (SOS and
  Baltic Odyssey probes).
- Documentation had temporarily replaced durable retry with inline distinct
  creation, contradicting the fail-closed contract.
- The final adjudicator evidence/conflicts were not persisted with the attempt.

## Vector Recall Evidence

Vectors are recall only. `related_v1` and `search_v3` widen discovery; SQLite
ownership plus the existing LLM adjudicator decide identity. Similarity alone
never authorizes merge.

| measure | fresh pre-fix production result |
| --- | --- |
| eligible documents | 703 |
| `related_v1` | 703/703; zero missing/extra/stale-hash |
| `search_v3` | 703/703; zero missing/extra/stale-hash |
| 2026-07-15..31 | 122/122 each kind |
| 2026-08-01..11 | 83/83 each kind |
| 2026-08-12..22 | 431/431 each kind |
| last reconciliation | `ops_run=6937`; 14 changed, 1392 unchanged, no cap remainder |
| current provider/RPC failures | no terminal failures; recovered RPM waits only |

The post-Smart-Update coalesced enqueue and independent 180-minute owner are
present. Event `7997` is intentionally export-ineligible because its ranged
date is malformed.

A stored-vector proxy over seven positive families used no provider call. The
true owner was top-1 for 2/7 and top-5 for 5/7; three owners ranked 2–3. Exact
type/city filtering removed SOS and Baltic Odyssey. Prevention must therefore
preserve a small top-k and omit drift-prone city/type filters in the same
existing RPC/embedding pass, without a fallback RPC or extra LLM call.

## Regression Corpus

### Required positives (`FINAL_MATCH`)

| family | anchors |
| --- | --- |
| Barn/SOS | `8117 → 8242`, `t.me/kldevents/3619` |
| qTickets 247858 | `7580`, `7601`, `7753` |
| qTickets 251796 | `7603`, `7705`, `7996`, `8183` |
| Baltic Odyssey | `8055`, `8108` |
| «Великие учителя» reminder | `3216`, `8244` |
| Dürer reminder | `5703`, `7690` |
| «Живая нить традиций» | `7609`, `7749` |

### Required hard negatives (`FINAL_DISTINCT`)

- exhibition versus its excursion;
- exhibition versus a separate lecture/activity/closing;
- different same-day sessions;
- different dates in a recurring series;
- festival parent versus independently attendable child;
- multiple child Events in one source post;
- different exhibitions at one venue.

The gate also requires exact replay twice, genuine concurrent two-worker
replay, zero hard-negative false merges and unchanged normal-path LLM counters.

The prevention branch executes all 14 sanitized named cases through Smart
Update: positives reuse one owner and all seven hard negatives create a separate
Event (`14/14`, 100%). Exact Telegram/VK packet replay is invoked twice after
the initial owner and adds zero Events/provider calls. Two SQLite handles run
the same candidate concurrently and leave one Event. Provider counters remain
at the historical budget: zero adjudicator calls when deterministic SOS proof
already resolves the owner, otherwise exactly one existing adjudicator call;
the widened vector test records one embedding and one recall pass. These tests
do not substitute for the post-deploy live top-1/top-5 measurement.

## Production Census And Cause Classification

The frozen read-only pre-deploy census covered all 704 active/upcoming public
rows: 1,837 sources, 15,079 facts, 4,443 poster/audit rows, 18,813 jobs, 658
publications and 2,071 identity decisions. `PRAGMA quick_check=ok`. The
recall-biased all-pairs pass emitted 1,742 pairs:

| overlapping signal / preliminary class | count |
| --- | ---: |
| shared poster | 887 |
| shared identity source | 1,116 |
| related title | 338 |
| same ticket | 211 |
| same title | 123 |
| same-slot related | 113 |
| legitimate/multi-child hint | 967 |
| needs adjudication | 601 |
| veto candidate | 130 |
| reminder candidate | 6 |
| hard-duplicate candidate | 38 |

Signals overlap and are deliberately **not** merge decisions. Closure needs a
post-prevention untruncated census classified as recall miss, LLM error, veto
fall-through, legitimate occurrence, reminder update, replay race or heuristic
false positive. No bulk heuristic merge is allowed. Artifacts include
`prod-active-upcoming-census-raw.json.gz`,
`prod-active-upcoming-census-candidates.json`, and
`strong-cluster-source-evidence.md`.

### Final post-repair census

The final source-complete snapshot was captured at
`2026-08-22T19:14:25.650403Z`, has SHA-256
`5e083181783bc37670329ef7a5c1bfc359f7dc8686c5e512c66c5d034846cecf`,
and passed `PRAGMA quick_check=ok`.

| measure | final result |
| --- | ---: |
| active/upcoming canonical Events | 661 |
| attached sources / facts / posters | 1,805 / 15,209 / 4,381 |
| recall-biased candidate pairs | 1,313 |
| hard-duplicate signal pairs | 3 |
| unresolved hard duplicates | **0** |

The three hard-signal pairs are all reviewed `KEEP_DISTINCT` occurrence
families: `7020/7021` (programme range versus 27 August performance),
`7764/7765` (series versus 29 August occurrence), and `7793/7794`
(series versus 26 August occurrence). Additional source-grounded negatives
include different exhibitions `6995/7488` and the 12:00 versus 16:00
Flag Day actions `8149/8285`. Event `8285` was repaired in place to the
source-grounded title `Акция «Три цвета Родины»` and 16:00 start.

The applied repair generations were:

- manifest v2: 21 merge clusters / 35 obsolete Events / three
  `KEEP_DISTINCT`;
- manifest v3 SHA-256
  `9af991ca89f864475c2388b75f7a567e0ea32a5bd9a16097bfd9608979e7bf6f`:
  rollout recurrences `8280 → 7907` and `8284 → 3216`;
- manifest v4 SHA-256
  `9a637f36271e0a75424a00270c5ec9ae37c2197946ceaeb3c2fc84473076d34e`:
  13 merge clusters / 18 obsolete Events / five `KEEP_DISTINCT`.

Every generation used the incident-safe manifest/hash/CAS flow, preserved
Event shells, sources, facts, poster audit and identity decisions, cancelled
obsolete pending work, kept the pre-existing foreign-key baseline at
`195 → 195`, passed `quick_check`, verified separately, and produced a
zero-change second apply.

Cause classification for the repaired clusters was dominated by the two
control-flow defects: original owner/veto fall-through and the older
merge-path unsafe/distinct conflation. The remainder comprised reminder
updates, recurring/rolling occurrence refreshes, self-publication replays and
programme-carrier contamination. Shared-source/shared-poster false positives
were retained as legitimate multi-child or occurrence pairs rather than bulk
merged.

## Automation Contract

### Treat as regression guard when

- create/merge gate, adjudicator, final create, candidate/attempt, occurrence,
  fingerprint or retry code changes;
- vector identity RPC/filter/top-k or sync ownership changes;
- Event merge/repair and obsolete-job handling changes;
- a named positive or hard-negative family is touched.

### Affected surfaces

- `smart_event_update.py`, `smart_update_identity.py`, `smart_update_state.py`;
- Fly SQLite identity/Event ledgers and personalization Supabase vector recall;
- Telegram/VK intake and accepted-only public fanout;
- incident-safe repair and separate social-cleanup handoff.

### Mandatory checks before closure or deploy

1. Named fixtures pass; exact replay twice adds zero Events; concurrent replay
   leaves one owner.
2. Owner/`VETO_CREATE` ends only as `FINAL_MATCH`, grounded
   `FINAL_DISTINCT`, or durable `FINAL_RETRY`; ordinary CREATE is unreachable.
3. Normal-path provider/LLM counters are unchanged.
4. Both vector kinds have 100% eligible coverage/current hashes; top-1/top-5
   recall is reported.
5. Manifest repair uses row hashes, preserves sources/facts/posters/audit,
   treats both derived poster hash and non-empty raw-byte SHA-256 as media
   identity, cancels only obsolete pending work, and second apply changes zero.
6. Telegraph, ICS, static, vector/search and digest are rebuilt; Telegram/VK
   cleanup uses a separate mapping.
7. SHA is on `origin/main`; deploy is clean exact-main; `/healthz`, quick-check,
   logs and two full ingestion iterations pass.

### Required evidence

- first-bad matrix/diff/runtime trace;
- tests, replay counts, LLM counters and corpus metrics;
- vector coverage/lag/rank/error receipts;
- classified census and reviewed manifest;
- backup/hash, apply/verify/second-apply, projection and social receipts;
- exact-main SHA/image, health, quick-check and two ingestion run IDs.

## Mitigation And Production Repair

- No heuristic bulk merge or generic row-deleting duplicate utility was used.
- The first manifest attempt hit the production raw-byte poster uniqueness
  index and fully rolled back. PR #555
  (`b662a4bef81efa431ae4642ceb62caa80e07973c`, Fly version `2017`)
  made the repair tool honor both derived `poster_hash` and non-empty
  `raw_sha256` identities.
- PR #556
  (`2c2a10c1688f1b1a58521da44898dbe0905f0936`, Fly version `2018`)
  closed the remaining legacy merge-path create reachability. No additional
  normal-path LLM operation was added.
- The combined repair merged 55 obsolete Event shells across the reviewed
  manifests. Social cleanup archived/redirected their Telegraph pages, cleared
  ICS projections, removed only unique obsolete managed VK/Telegram posts and
  preserved public references shared with a canonical owner.
- Event `3216` was restored from official exhibition evidence (including the
  6 September end date), its Telegram projection was edited in place, and its
  corrected VK post published with ten attachments. VK changed postponed id
  `10005` to live id `10007`; the Event and publication ledger were
  reconciled to `wall-231920894_10007`.
- Programme-carrier contamination was removed from canonicals `7915` and
  `8083` with an audited backup. Event `8285` was repaired as a distinct
  source-grounded 16:00 action. The operation removed 18 unrelated source
  bindings and 79 facts without increasing the foreign-key baseline.

## Corrective Actions

- [x] Replace the boolean with `FINAL_MATCH`, `FINAL_DISTINCT`, `FINAL_RETRY`.
- [x] Require grounded distinct proof for CREATE after identity concern; bare
  `no_merge`, abstention, provider/schema errors become durable retry.
- [x] Persist owner, action, relation, confidence, evidence/conflicts and
  attempt ID in the existing ledger.
- [x] Preserve exact occurrence/fingerprint no-op and avoid
  `UNIQUE(source_url)`.
- [x] Preserve small vector top-k in one widened RPC without extra calls.
- [x] Apply the typed final verdict to `merge_identity_gate`; exact source
  evidence is mandatory for distinct, `unsafe_to_merge` retries, and
  overlapping same-title/same-ticket long-event repost drift fails closed.
- [x] Deploy prevention from exact `origin/main`; verify health and SQLite.
- [x] Deploy raw-byte-safe repair hotfix, apply reviewed true duplicates and
  rebuild projections.

## Acceptance Criteria

- vector coverage both kinds 100%; critical fixtures 100%; duplicate recall
  at least 99%; distinct preservation at least 99%; hard-negative false merges
  zero;
- owner/veto to ordinary CREATE zero; exact replay zero new Events; concurrent
  replay one owner; normal-path LLM count unchanged;
- no unresolved hard duplicates after full census/repair;
- SHA reachable from `origin/main`; deploy, health, quick-check and two full
  ingestion iterations pass.

## Release And Closure Evidence

- prevention implementation: PR #554
  (`c0103be2c7ddc760486bd79e19825a69ceae4165`, Fly `2016`) and
  PR #556 (`2c2a10c1688f1b1a58521da44898dbe0905f0936`, Fly `2018`);
- repair safety: PR #555
  (`b662a4bef81efa431ae4642ceb62caa80e07973c`, Fly `2017`);
- tests: 121 focused merge-path tests pass, including exact production-shaped
  `3216/8284` and `7907/8280`; the full named positive/negative corpus,
  replay ×2 and two-worker replay pass with no added normal-path call;
- Fly deployment `2018` runs exact `origin/main@2c2a10c16`; machine
  `48e419df93e078`, image
  `deployment-01M0NB137BFTE9NSBK769YDA9S`; `/healthz` is ready and SQLite
  `quick_check=ok`;
- manifest v4 apply/verify/second-apply: `applied / verified / noop`;
  candidate/source/poster violations all zero, foreign-key baseline
  `195 → 195`;
- social cleanup v4: 18/18 merged, 18/18 ICS cleared and 18/18 Telegraph
  redirects verified; three unique VK posts were deleted and four were already
  absent; five older Telegram posts were removed through the approved local
  E2E human session and verified absent;
- vector/search: `ops_run=6986` removed 18 stale Events and embedded two
  changed documents; `6987`, `6988`, `6995` and `6997` completed with
  660/660 documents in each kind, 1,320 unchanged embeddings and zero provider
  calls on the convergence passes. Final coverage is 100% for
  `related_v1` and `search_v3`;
- final backend ingestion: `ops_run=6992` reached `FINAL_MATCH` and updated
  existing owner `7895` with zero creates; `ops_run=6993` completed a
  product rejection with zero creates. The adjacent `ops_run=6991` exercised
  `FINAL_RETRY/distinct_not_grounded` and also created nothing;
- final production census SHA-256
  `5e083181783bc37670329ef7a5c1bfc359f7dc8686c5e512c66c5d034846cecf`
  has zero unresolved hard duplicates.

Static-site completion evidence is recorded below when the final vector
barrier-protected operator build completes.

## Prevention

Once identity concern exists, the existing adjudication operation must produce
one explicit final outcome. Only grounded distinct authorizes CREATE; match
uses the owner; uncertainty is durable retry. Vectors remain recall evidence
and accepted-only side effects remain unchanged.
