# Smart Update automatic identity state machine — integration report

## Scope and constraints

- Base: `origin/main` at `d7731ab4235b325e9ca52d13c45fba83eaf5de0b`.
- Integration branch: `integration/smart-update-identity-state-machine`.
- Incident contracts:
  `INC-2026-08-04-smart-update-identity-source-replay-corruption` and
  `INC-2026-08-10-smart-update-identity-terminal-loss`.
- Product invariant: no operator/manual Event review. Public outcomes are
  `CREATED`, `MERGED`, `NOOP_EXACT_REPLAY`, `REJECTED_PRODUCT_POLICY`, and
  `RETRY_SCHEDULED` only.
- No new model/provider/LLM stage or physical adjudicator call was introduced.
  Existing occurrence scope, match/create bundle, widened dedup adjudicator,
  and create/merge gates are reused.
- Explicitly absent: PR #338 continuation, operator UI/queue, publication
  redesign, production mutation, recovery apply, deploy, or PR merge.

## Lane integration

| Lane | Primary requirement IDs | Mode / effort | Branch / worktree | Status | Head / integration commit | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| D1 caller map | PRE-1, PRE-2, ART-8 | read-only / medium | read-only latest-main checkout | merged as documentation | no write head | `docs/features/smart-event-update/caller-inventory.md`; ignored D1 artifact |
| D2 production baseline | PRE-3, ART-3 | read-only production / maximum-risk | no writable branch | accepted | no repo commit | D2 report SHA `6c61dd6c56649df15dfb5b75727dfbd1249948be8e041e565dae0e75f49d6617` |
| D3 architecture | IMPL-1–10 design, ART-4–5 | read-only / maximum-risk | no writable branch | accepted | no repo commit | ignored `d3/architecture.md` |
| CORE-IMPL | IMPL-1–6, IMPL-9, TEST-1–10, TEST-13 | worktree worker / high | `agent/smart-update-identity/core-impl`; `/home/dev/.codex/worktrees/events-bot-new/smart-update-core-impl` | merged | worker `8118b0085`; cherry-pick `2d04b2812` | typed state, schema, keys, leases, retries, exact replay |
| CALLERS-IMPL | IMPL-7–8, TEST-11–12 | worktree worker / high | `agent/smart-update-identity/smart-update-callers-impl`; `/home/dev/.codex/worktrees/events-bot-new/smart-update-callers-impl` | merged | worker `13c588699`; cherry-pick `5d30891f4` | all six production boundaries + AST contract |
| RECOVERY-CLI | IMPL-10, TEST-14, ART-7 | worktree worker / high | `lane/recovery-smart-update-identity`; `/home/dev/.codex/worktrees/events-bot-new/recovery-smart-update-identity` | merged + reconciled | worker `c63cc37d3`; cherry-pick `66c310599` | durable + Telegram/VK/parser/ticket/festival recovery; final production read-only D4c dry-run |
| Integrator | all integration, ART-1–9, constraints | serial integrator / maximum-risk | this branch/worktree | in review | final SHA pending | parser occurrence keys, immediate known-distinct create, all-source recovery, caller dominance proof, final validation/CI |
| Checklist reviewer | all requirements | read-only reviewer / high | pending final diff | pending | pending | final closure table below will be reconciled to reviewer findings |

No worker change was rejected or abandoned. Worker worktrees are clean and their
implementation commits are represented on the integration branch; lane-only
metadata commits were not required for runtime delivery.

## Requirement execution matrix

| ID | Original requirement | Status before final review | Evidence |
| --- | --- | --- | --- |
| PRE-1 | AST inventory on latest main | Done | caller inventory, exact base SHA |
| PRE-2 | enumerate forbidden return/ID/failed paths | Done | caller inventory pre-fix terminal leaks |
| PRE-3 | read-only production audit since 2026-08-04 | Done | incident baseline + D2 report; gates both `enforce`; no mutation |
| IMPL-1 | one typed terminal result; accepted/diagnostic IDs separate | Done | `SmartUpdateTerminalOutcome`, `SmartUpdateResult`, DB triggers |
| IMPL-2 | remove VETO_CREATE terminal; reuse adjudicator | Done | one physical widened-adjudicator call asserted; typed same/distinct/retry |
| IMPL-3 | close merge decisions and bounded automatic convergence | Done | known distinct creates in the same attempt; only UNKNOWN/provider/schema uncertainty uses bounded durable retry |
| IMPL-4 | authoritative final duplicate probe in same operation | Done | reload/revalidate/attach/`MERGED` without second LLM pass; race test |
| IMPL-5 | carrier-child candidate/occurrence identity | Done | stable structured/ordinal keys and indexes; official parser same-page slot regression |
| IMPL-6 | split UPSERT_EVENT and ATTACH_CONTEXT | Done | typed intent facade and context-only target attachment |
| IMPL-7 | one facade and every discovered production caller migrated | Done | parser/TG/VK/ticket/festival/main + AST contract |
| IMPL-8 | queue semantics close on accepted/retry/product reject | Done | caller adapters and focused TG/VK/ticket tests |
| IMPL-9 | structured balanced funnel | Done | current candidate + attempt metrics and schema-adaptive prod audit |
| IMPL-10 | idempotent since/dry-run/batch-size recovery | Done | durable + Telegram/VK/parser/ticket/festival selectors; parser request worker; D4c dry-run |
| TEST-1–14 | mandatory regression matrix | Done | hermetic fixtures include provider/schema/vector/DB retention, official parser same-page sessions, immediate distinct create, all-source recovery, and AST acceptance dominance; 216-test aggregate passes |
| ART-1 | one focused PR from latest main | Pending | branch ready; draft PR created only after final reviewer |
| ART-2 | incident document with AS-IS cause | Done | canonical incident record remains open pending release/recovery |
| ART-3 | production reason baseline | Done | daily/reason tables in incident + D2 report |
| ART-4 | TO-BE state machine | Done | canonical identity-state-machine doc |
| ART-5 | migration and rollback | Done | state-machine and SQLite init docs |
| ART-6 | hermetic and full relevant CI results | Partial | local aggregate 216 passed; GitHub CI pending draft PR |
| ART-7 | dry-run recovery report | Done | final-source D4c all-source read-only aggregate and manifest |
| ART-8 | caller inventory | Done | canonical caller inventory |
| ART-9 | changed terminal outcomes | Done | state-machine old→new table |
| CONSTRAINT-1–8 | no issue/operator/new LLM/publication refactor/deploy/merge/prod mutation | Done | scoped diff; read-only evidence; final PR remains draft/unmerged |

## Integration-specific corrections

- The create gate now folds its concrete matched Event into the one existing
  widened dedup-adjudicator call rather than returning a veto.
- The final transaction probe reloads and revalidates the authoritative Event,
  attaches the keyed packet, and returns `MERGED` in the same facade operation;
  a disproved stale match proceeds with distinct `CREATED`, and neither path
  adds an LLM call.
- Accepted Event/EventSource writes remain accepted if only the short durable
  attempt acknowledgement is interrupted. Candidate state was projected to
  retry before work; exact replay closes the interrupted ledger row and
  converges to `NOOP_EXACT_REPLAY`, preventing imported-pointer/failed-state
  regression.
- Pure technical failure remains durably due. Only identity uncertainty uses
  the bounded final create-distinct rule.
- Positive distinct relations and deterministic ticket/structural conflicts
  create separately in the same facade attempt; they do not depend on the
  retry-worker interval.
- Official parsers derive a stable child key from source type, canonical page,
  structured date/end/time and producer ordinal. Legacy parser losses are
  materialized as source-level requests consumed by the existing scheduled
  full-catalogue parser.
- Caller acceptance tests structurally inspect all direct production
  boundaries: any non-observability `SmartUpdateResult.event_id` read must be
  dominated by `is_accepted` or an accepted terminal enum branch.
- Production audit metrics feature-detect the new tables and separately expose
  candidate balance, accepted-ID violations, retry due/exhausted, attempt
  balance, and expired-lease orphan rows.
- Relevant tests now close their aiosqlite workers, so the dedicated CI job
  exits normally rather than hanging after a passing summary.

## Verification snapshot

- Production baseline: strict read-only; `PRAGMA quick_check=ok`; D2 checksum
  recorded above.
- Production recovery: final exact-source D4c, strict read-only; Telegram 5 +
  VK 104 selected/would-requeue, four parser sources explicitly deployment-
  gated, zero changed; source SHA
  `2cc2d19bd7024e12201fbb45574aa43c39cd6260c88e8dda50ab24e7ed485b78`,
  manifest SHA
  `cb75b598960968244353a8ff091436e21b5c624e8ccad128a54f5fd20e4adbf3`.
- Local relevant suite: `216 passed in 144.12s`; ignored log SHA-256
  `4620444cf8666cd503840e0a9d4f1f073043b718164235b8c016fe856451e673`.
- Production boundary compile: passed for core, schema, every caller, recovery,
  and audit script.
- `git diff --check`: passed.
- GitHub relevant CI: pending draft PR.

## Residual release boundary

The implementation request forbids deploy and production mutation. Therefore
incident status remains open. A later approved release must merge to
`origin/main`, deploy that exact SHA, re-run snapshot/read-only readiness,
perform an explicitly approved recovery apply/catch-up, and verify current-day
canonical/public results under both incident regression contracts.
