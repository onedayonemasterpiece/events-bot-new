# Region Talk reuse audit and skill-first gate

> Status: **mandatory pre-implementation gate for F14 Event Comment Feedback**. Production collection, YDB schema/migrations, Kaggle runner port, static export and Astro UI must not start until this gate is accepted.

## Why this gate exists

Region Talk has already accumulated practical experience with YDB sidecars, compact online state, source/post identities, cursor-driven crawling, queue orchestration, vector-first selection, LLM-late verification, Kaggle worker separation, product-funnel metrics, Telegram rate limits and storage compaction. Rebuilding those mechanisms independently for comment feedback would repeat solved failures.

At the same time, Region Talk is not a library ready to import:

- its current product branch `origin/agent/region-talk/bge-m3-enrichment-test` is a large divergent side branch; the planning snapshot `b4c3c999` is `752` commits behind and `295` commits ahead of current `origin/main`;
- the current Event Comment Feedback probe branch `origin/agent/event-comment-feedback-kaggle-runner@7068510c` is `752` behind and `10` ahead;
- Region Talk discovers external regional posts and may prepare publication candidates, while F14 starts from already linked `EventSource` posts and publishes only privacy-safe aggregate comment signals into static event pages;
- Region Talk skills and implementation documents are branch evidence, not canonical main contracts.

Therefore the rule is **audit → classify → consolidate → create/validate skills → implement**, never merge/copy a whole stale branch.

## Source inventory for the formal audit

The audit must bind every conclusion to an exact SHA and path. At minimum inspect:

| Region Talk area | Evidence to inspect | Question for F14 |
|---|---|---|
| Compact YDB state | `region_talk_compact_state_kv`, row kinds, checkpoint v4, LZ4/compaction/TTL/maintenance contracts | Which stable-key, online-write, retention and compact-checkpoint patterns can be adapted without creating a second event source of truth? |
| Queue-driven orchestration | CandidateReport, BGE worker, ImageDiagnostic, orchestrator/finalizer and live funnel metrics | How should comment fetch, embedding, grouping, verifier and static export advance independently without a monolithic notebook or false-green heartbeat? |
| Source/post identity | canonical source/post keys, pre-network dedup, cursor/terminal/due states, exact-link fetch | Which primitives should anchor comments to `event_source` and avoid duplicate platform work? |
| Telegram operations | branch skill `region-talk-telethon-ops`, entity cache, role-scoped bundles, FloodWait/cooldown ledger | What is reusable for reading public discussion threads, and which Region Talk session roles must remain strictly separate? |
| Vector selection | branch skill `region-talk-vector-selection`, semantic banks, hard negatives, model/version/hash evidence and LLM-late gate | Which vector-first and false-positive audit patterns apply to a fixed feedback phrase bank? Is a dual-model path justified by an F14 probe rather than copied by default? |
| Data minimization | terminal text deletion, compact row projections, redacted evidence, bounded history | How must comment PII, author hashes, raw text retention and public export be stricter than post-candidate state? |
| Observability | product-funnel deltas, queue backlog, terminal/retry status, delivery vs raw throughput | Which F14 metrics prove new comments became safe public state and a checked static build, rather than only being fetched/embedded? |
| Tests and reports | Region Talk unit tests, golden false positives, XLSX/report contracts, system audit and integration reports | Which tests can be ported as generic fixtures and which remain Region Talk-specific evidence only? |

Also audit the existing F14 branch runner/kernel/tests against current main contracts. Its code is evidence for behavior and failure cases, not the integration base.

## Required adoption matrix

The formal report classifies every inspected pattern as `reuse`, `adapt`, `reject` or `defer`, with source SHA/path, reason, target owner and target test. Default expectations:

| Pattern | Default | Boundary |
|---|---|---|
| stable platform/source/post keys; hash/version idempotency | **adapt** | comments add their own stable key and event-source relation; core event identity remains Fly SQLite |
| compact row-level YDB state, bounded checkpoint, TTL/compaction | **adapt** | separate F14 namespace/kinds and size budget; no Region Talk table coupling or raw payload copy |
| queue-driven independent stages, terminal/retry states and product-funnel metrics | **reuse/adapt** | F14 stages are fetch → normalize → embed → phrase group → verify → static export/build |
| Kaggle status/lease/immutable config/artifact discipline | **reuse** | use current main generic framework, not a copied branch-local status client when main already owns the pattern |
| vector-first positives + hard negatives + LLM only after aggregation | **adapt** | F14 verifies event/phrase groups, never raw comments; model count is probe-driven |
| Telegram entity cache, role separation, FloodWait stop/cooldown | **adapt** | create an F14-specific approved session/credential lane; never borrow Region Talk `DISCOVERY1/2`, E2E or S22 roles |
| Region Talk source frontier, nonlocal/local blogger discovery | **reject** | F14 source set is derived from current `EventSource`, not an independent public-source frontier |
| image/postcardness and publication queue | **reject** | F14 exports a static aggregate manifest and does not republish source media/posts |
| Region Talk final post writer or generated public prose | **reject** | F14 public sentences remain an approved fixed phrase bank; verifier cannot invent copy |
| long-lived post text/debug evidence | **reject/adapt strictly** | comment text/PII has shorter retention and never enters public artifacts |

No Region Talk module is considered reusable merely because names or data shapes look similar. Reuse requires a current-main compatibility check, target tests and an explicit owner.

## Audit deliverables

Before any F14 implementation branch is opened from current `origin/main`, produce:

1. `docs/reports/event-comment-feedback-region-talk-reuse-audit-<date>.md` with exact Region Talk/F14/main SHAs and branch divergence;
2. the adoption matrix with `reuse|adapt|reject|defer`, target path and test mapping;
3. a data-boundary diff covering Fly SQLite, F14 YDB, Region Talk YDB, static manifests and forbidden PII/raw payloads;
4. a stage/queue mapping and metric dictionary from expected input through **successful static delivery**;
5. a branch consolidation decision: clean-port list, superseded commits and items intentionally left as evidence;
6. a minimum golden corpus of transferable failures: duplicate identity, stale overlay, cursor retry, false-green run, vector false positive, provider/session cooldown, raw-text leak and static-export leak;
7. the skill specifications and forward-test prompts described below.

## Skills that must precede implementation

Create at least the first two project skills on clean main-based branches before MVP-1 is ported. Use the repository `skill-creator` process: concise `SKILL.md`, precise trigger metadata, optional one-level `references/`, deterministic scripts only when repeated execution justifies them, generated `agents/openai.yaml`, `quick_validate.py` and forward tests on realistic prompts.

### 1. `region-talk-ydb-funnel-audit`

Purpose: make the proven Region Talk operational knowledge reusable for read-only inspection and architectural review.

Required contents:

- how to bind evidence to branch/SHA and identify current compact table/schema versions;
- stable row kinds/keys, queue/cursor/terminal/due semantics and online state vs heartbeat evidence;
- funnel metrics that separate raw throughput from product delivery;
- compactness/TTL/maintenance and PII/secret-safe inspection;
- an adoption checklist for another YDB sidecar;
- explicit non-goals: no source onboarding, no production mutation, no borrowed credentials, no claim that Region Talk branch code is already in main.

This skill complements rather than duplicates `region-talk-source-onboarding`, whose job is only to seed normal-priority public sources.

### 2. `event-comment-feedback-pipeline`

Purpose: route implementation, debugging and release acceptance for F14.

Required contents:

- mandatory read order for this feature home, data ownership, phrase bank, YDB, verifier, probe, static contract and this audit;
- EventSource-derived source manifest, platform-post/comment identities and incremental fetch rules;
- privacy/retention/redaction rules and public export allowlist;
- vector-first/group-verifier/no-generated-copy invariants;
- Region Talk adoption matrix lookup and prohibition on copying rejected stages;
- static-build handoff, no-runtime-provider fallback and release evidence checklist;
- exact stop conditions for PII, raw quotes, per-comment LLM, factual overclaim and stale/partial public state.

### 3. Conditional `social-comment-collection-ops`

Create this only if the Telegram and VK collection spike proves a repeated cross-feature operational workflow that does not fit the F14 skill cleanly. It may consolidate entity/post/thread resolution, cursors, rate limits, FloodWait/cooldown, role-scoped sessions, VK pagination and redaction. Do not create it speculatively or copy Region Talk session names.

## Skill acceptance

Each required skill must:

- trigger on three representative requests and stay silent on two adjacent non-goal requests;
- direct an agent to the canonical current-main documents before branch evidence;
- forbid secret/session output and production mutation during audits;
- distinguish heartbeat/run success from accepted product/static delivery;
- pass validation and a fresh forward test without relying on hidden conversation context;
- be linked from this feature home and `docs/routes.yml` only after it actually exists;
- remain small: move detailed schemas/adoption matrices into one-level references instead of duplicating feature documentation.

## Implementation sequence after the gate

1. **F14-0A — formal Region Talk audit:** inventory, failure corpus, adoption matrix and data/metric boundaries.
2. **F14-0B — skill creation and validation:** ship the two required skills; add the conditional collection skill only with evidence.
3. **F14-1 — clean probe port:** reimplement or cherry-pick only audited probe behavior onto current `origin/main`; do not rebase/merge the stale branch wholesale.
4. **F14-2 — YDB/incremental collection:** use adopted stable-key/queue/retention patterns in an isolated F14 namespace.
5. **F14-3 — phrase aggregation/static export:** prove compact current public state and changed-hash handoff.
6. **F14-4 — cached group verifier:** medium/high-risk groups only; fixed phrase bank.
7. **F14-5 — Astro UI and release:** static-only block, accessibility, privacy, stale/failure and canary evidence.

## Gate acceptance checklist

- [ ] Audit report names exact main, Region Talk and F14 probe SHAs.
- [ ] Every candidate pattern is classified with target tests; no whole-branch merge is proposed.
- [ ] Region Talk-only source discovery, image and publication stages are explicitly excluded.
- [ ] F14 YDB namespace, retention and compactness remain independent and within the approved budget.
- [ ] `region-talk-ydb-funnel-audit` and `event-comment-feedback-pipeline` exist, validate and pass forward tests.
- [ ] Any generic collection skill has concrete repeated-work evidence and role-scoped credential rules.
- [ ] Clean main-based F14 implementation starts only after this checklist is accepted.
