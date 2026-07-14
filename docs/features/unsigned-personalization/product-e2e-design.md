# Product E2E-driven personalization: Phase A design

> **Status:** preliminary research/design; not an accepted architecture, KPI policy, release gate, or executable longitudinal suite.
>
> **Phase boundary:** this document completes only the pre-consultation Phase A from [the research brief](product-e2e-research-brief.md). Final KPI thresholds, maturity rules, persona behavior distributions, production schema, ranking changes, and a release canary remain blocked on two eligible external reviews and their accept/adapt/reject synthesis.
>
> **Executable assets:** the only current browser contract is [`tests/playwright/static_personalization_contract.spec.ts`](../../../tests/playwright/static_personalization_contract.spec.ts). Scenario names below are a **draft catalogue**, not executable Gherkin and not entries in `docs/operations/e2e-scenarios.md`.

## 1. Decision frame

The product-development loop is:

```text
measure -> diagnose -> change product/system -> deterministic replay
        -> compare against the same catalog/persona evidence -> accept or reject
```

The provisional product objective is preserved, not relaxed:

- primary target under investigation: `cards_to_first_relevant <= 20`;
- investigated hard ceiling: `30`;
- eligible population: mature persona sessions with independently proven relevant supply;
- `no_relevant_supply` is a separate catalog/candidate-generation outcome and never ranking success;
- the final percentile/SLO, maturity definition, relevance endpoint, and acceptance aggregation wait for consultant synthesis.

Phase A changes no production weight, database schema, release scope, or live data. A desired profile must emerge from UI actions. Directly injecting the expected final profile may be used only by a narrow compatibility unit/contract test, never as longitudinal E2E evidence.

## 2. Evidence reviewed and status vocabulary

The audit checked the canonical personalization documents named in the research brief, the pending PR #26 draft on remote branch `agent/static-release/checklist-cdn-social`, the current Gherkin and Playwright assets, the static reference client, Astro entrypoints, preview checks, Supabase migrations, and the `event-search` Edge Function.

Status labels have strict meanings:

| Status | Meaning |
|---|---|
| **Implemented** | Executable code/migration exists and the stated narrow behavior has test or build-check evidence. |
| **Partial** | Some executable path exists, but a required boundary or trustworthy end-to-end assertion is absent. |
| **Design-only** | Only a contract/draft exists; it is not runtime evidence. |
| **Missing** | No implementation or test seam was found. |
| **Contradictory/stale** | A document or check implies more than the checked runtime/migrations provide. |

The PR #26 `e2e-acceptance.md` draft is useful input but is not in `main` and is not copied here. Its stable-ID principle, selected `PERS-*` concepts, correlated evidence bundle, secret separation, and cleanup model are retained as design principles. Its unrelated identity/favorites release matrix is outside this task, while its fixed maturity counts and final-looking `<=20` release gate remain provisional rather than accepted.

## 3. Current-state audit

| Surface/component | Code owner | Data owner | Tests/evidence checked | Actual status | Missing seam | Release implication |
|---|---|---|---|---|---|---|
| Static event and listing fallback | Astro pages/components; `site/src/layouts/EventLayout.astro` | Fly SQLite export/static manifests | `site/scripts/check-preview.mjs:144-180,340-360` | **Implemented** for generated preview contracts | No longitudinal quality assertion | Safe fallback can be a harness baseline, not proof of personalization quality. |
| Consent and compatible local profile | `EventLayout.astro:1891-2115`; reference `static_site/personalization/personalization.js` | Browser `localStorage` | Playwright lines `52-64`, `125-209` | **Implemented** in the deterministic reference fixture; **Partial** for actual Astro journey coverage | Current Playwright test routes a demo HTML/JS fixture, not a built Astro page | A release E2E must exercise the built site and verify no-consent/no-storage behavior there. |
| Local strong actions and local profile mutation | `EventLayout.astro:2896-3246` | Browser profile and bounded debug log | Preview string checks; reference-client Playwright covers click/hide only | **Partial** | No actual-Astro browser test for like/unlike/share/calendar/ticket/detail/back; local fixed deltas are not a durable rollup | Useful for immediate UI feedback, but not evidence of the intended server profile. |
| Valid impression, dwell, quick-skip, read/detail/back | No personalization implementation found | Intended browser telemetry | No executable assertion | **Missing** | Visibility threshold, dwell clock, dedupe, navigation correlation, and valid-inspection definition | `cards_to_first_relevant` cannot be measured honestly until this collection seam exists. |
| Local related rerank and exclusions | `EventLayout.astro:2307-2439`; reference client `:180-335` | Same-origin related manifest + browser profile | Nine Playwright reference tests, all passing in this audit | **Implemented** for the reference fixture; **Partial** on built Astro | Reference and inline Astro implementations can drift; no shared module/actual-site contract | Keep as a deterministic contract layer; do not call it longitudinal E2E. |
| Local served-list evidence | `EventLayout.astro:2710-2758,3145-3154`; reference client `:372-438` | Browser memory/debug log | Reference Playwright verifies id/hash/context and resize dedupe | **Partial** | Astro summary is stored in memory for context but not remotely accepted; no persisted candidate-pool/build/profile revision | Ranking outcomes cannot be joined to authoritative exposure or DB state. |
| Remote telemetry ingest, validation, dedupe, quarantine | Draft SQL in `database.md`; no matching applied migration/RPC | Intended separate personalization Supabase project | No SQL/integration contract found | **Design-only / Missing** | Typed ingest, grants, quotas, consent validation, dedupe, synthetic marker, cleanup | Browser-to-DB and exactly-once claims are release blockers. |
| Durable profile rollup and multi-horizon snapshot | Draft in `database.md`/`neural-flow.md` | Intended Supabase profile authority | No migration, function, worker, or integration test found | **Design-only / Missing** | Rollup trigger, versioned algorithm, lag evidence, short/mid/long decay | Maturity and profile correctness cannot currently be asserted. |
| Listing personal-feed UI | `PersonalFeedSlot.astro`; listing pages; `EventLayout.astro:2480-2669` | Browser profile/cache plus intended Supabase RPC | `check-preview.mjs:354-360` checks strings/slots | **Partial / Contradictory-stale** | Client calls `get_listing_personal_feed_v1`, but no matching migration/function exists in the repository | UI preparation must not be represented as an operational personalized feed. |
| Candidate generation and profile application | Local related manifest and client rerank; pgvector related/search RPCs | Fly event facts; Supabase vector sidecar; browser profile | Vector migrations and static canaries; no profile-aware candidate integration test | **Partial** | No implemented server candidate API applying a durable profile and recording candidate/served lists | A later feed cannot yet prove profile-to-next-feed application. |
| Semantic `search_v3` / `related_v1` retrieval | Supabase migrations `20260628...` through `20260630...`, vector sync/build paths | Separate Supabase vector sidecar; Fly remains event authority | Existing vector/search tests and documented canary evidence | **Implemented** for narrow vector retrieval; not personalized longitudinal ranking | Ground truth and user-profile application remain separate | Reuse candidate evidence, but do not treat semantic similarity as relevance truth. |
| Public reaction counters | `20260627090000_event_reaction_counter.sql` | Supabase aggregate projected to static cards | Migration/RLS shape; downstream preview checks | **Implemented** for aggregate counters | Counters are not per-person behavior evidence | Never substitute aggregate likes/views for persona profile formation. |
| Authorized search served context | `supabase/functions/event-search/index.ts`; `AuthorizedEventSearch.astro` | Authenticated search service | Existing search smoke/contracts | **Partial** for this goal | Search is rare in the behavior model and is not joined to longitudinal rollup | Cover as a rare cross-surface action, not the primary training path. |
| Failure fallback | Reference client and inline Astro local fallback | Static manifest/browser | Reference Playwright timeout/no-storage cases | **Implemented** narrowly; **Partial** across remote ingest/rollup recovery | No queued retry/dedupe/recovery E2E through DB | Static usability is proven only in the fixture; exactly-once recovery is not. |
| Current Gherkin | `tests/e2e/features/static_site_personalization.feature` | Documentation-style scenario text | File is tagged `@draft`; no matching steps found | **Design-only** | No executable environment/step mapping | Keep out of executable scenario index until implemented. |
| Current Playwright contract | `tests/playwright/static_personalization_contract.spec.ts` | Routed demo fixture | `9 passed` locally with Playwright `1.58.2` | **Implemented** deterministic reference contract | No catalog timeline, UI-generated mature profile, DB/profile assertions, or production build | Preserve as fast regression coverage; add new layers rather than inflating it into the product harness. |

Additional implementation constraints found during the audit:

- actions on cards outside `[data-discovery-feed]` can toggle local event IDs but have no candidate features, so they cannot build the requested cross-surface semantic profile (`EventLayout.astro:2976-2986,3177-3210`);
- authorized search returns server-side `served_list_id/hash`, but the shared card-action handler only looks for context beneath `[data-discovery-feed]` and does not recover the search container (`EventLayout.astro:2976-2986`; `AuthorizedEventSearch.astro:525-534`); the documented exact strong-action correlation is therefore not implemented;
- the consent banner says likes and “not interested” stay only in this browser, while the prepared personal-feed RPC sends a compact local profile when configured (`EventLayout.astro:2094-2103,2584-2618`); enabling that path requires reconciled consent copy and ownership, not only an environment variable;
- the accepted data-ownership ADR requires same-origin, device-credentialed view/action intake and says `anon_id` is not ownership proof (`docs/architecture/personalization-data-ownership.md:61-67,117-121`). Older drafts that allow a direct anonymous Supabase ingest RPC are **Contradictory/stale** until explicitly reconciled. Phase B therefore defaults to isolated same-origin credentialed intake.

### Contradictory/stale documentation found

These findings are recorded rather than silently normalized into implementation claims:

1. `event-detail-related.md:334-354` still describes work “before Astro implementation,” while the same document and actual Astro page prove a preview implementation slice.
2. `personal-feed-architecture.md:3` says implementation is pending beyond local preview. The accurate status is **Partial**: client/slot preparation exists, but the server RPC does not.
3. `production-integration.md:3` broadly blocks canary/production, while semantic vector/search canaries exist. The block remains correct for anonymous telemetry/profile/personal-feed E2E, not for every personalization-family capability.
4. `database.md:121` treats pgvector as a future choice despite the applied search/related migrations; the full telemetry/profile SQL in the same document remains design-only.
5. Draft `database.md:609-617` stores a ready `local_profile` in `e2e_persona`; that cannot be used as longitudinal proof because this design requires action-emergent profiles.
6. Existing exploration values/weights in docs and `EventLayout.astro:1912-1922` are baselines under test, not accepted persona probabilities or Phase A targets.
7. The controlled taxonomy is design-only, while the preview uses deterministic feature mapping and currently emits empty `audience_exclusion_tags`; neither may be treated as independent ground truth.
8. The historical `8 passed` evidence was stale; the current routed Playwright suite has nine tests, passed `9/9` in this audit, and the two canonical references were synchronized.
9. Personalization project configuration has legacy Supabase fallback paths in other runtime code. The E2E preflight must require an explicit isolated personalization project ref/URL and reject a legacy or production fallback.

### What can be verified now

- no-consent static order and zero local profile/debug telemetry in the routed fixture;
- local profile schema compatibility and fail-closed behavior for corrupt/legacy IDs;
- local related rerank, hidden-event removal, negative-interest separation, local served-list context, and resize dedupe;
- mobile/desktop related-module presentation context;
- local fallback when the simulated telemetry backend is unavailable;
- static build presence of listing switches, personal-feed slots, local profile code, and the configured RPC name;
- pgvector search/related schemas separately from personalization profile application.

### What requires implementation before it can be verified

- valid impressions/dwell/quick-skip and cross-navigation action correlation;
- trusted remote telemetry acceptance, dedupe, quarantine, and retry recovery;
- a durable, versioned rollup and maturity decision;
- profile-aware candidate generation and the repository-missing `get_listing_personal_feed_v1` contract;
- durable served/candidate-list evidence carrying run, catalog, build, profile, and algorithm versions;
- two-week deterministic catalog replay and action-driven profile formation;
- independent relevance/adjacency/novelty labels and metric calculator;
- isolated Supabase setup/teardown and synthetic-data exclusion;
- a built-Astro Playwright journey and controlled canary policy.

## 4. Stable scenario and traceability model

IDs are immutable once an executable scenario ships. A semantic rewrite creates a new ID/version; wording changes do not. The same ID must appear in the future scenario catalogue, Playwright title/annotation, fixture manifest, DB assertion output, metric row, and artifact manifest.

### Draft scenario catalogue — not executable

| ID | Draft behavior | Primary purpose |
|---|---|---|
| `PERS-LONG-001` | A cold mobile persona forms a mature profile only through scheduled UI actions over at least 14 virtual catalog days | Complete longitudinal chain |
| `PERS-COLLECT-001` | Consent, valid/invalid impressions, dwell and strong actions are distinguished and accepted once | Collection and dedupe |
| `PERS-SUPPLY-001` | A mature persona has no relevant active candidate supply for a bounded period | Honest no-supply semantics |
| `PERS-LIFECYCLE-001` | New, updated, holdout, cancelled and ended events follow the immutable daily timeline | Catalog/replay integrity |
| `PERS-ROLLUP-001` | Accepted evidence produces expected positive, negative and time-horizon facets | Rollup correctness/lag |
| `PERS-APPLY-001` | The next eligible feed uses the new profile and records the exact served list | Profile application |
| `PERS-QUALITY-001` | A mature composer/classical persona encounters an activated relevant holdout within provisional 20, while 30 is reported as ceiling research | Primary outcome |
| `PERS-NOVEL-001` | An independently labelled adjacent/novel holdout is usefully encountered without displacing core relevance | Exploration quality |
| `PERS-NEG-001` | Negative interests and explicit hidden events remain excluded across sessions/builds | Safety guardrail |
| `PERS-DRIFT-001` | New actions can change the profile while stale interests decay | Adaptability |
| `PERS-FATIGUE-001` | Repeated event/category/venue exposure is bounded | Fatigue/concentration |
| `PERS-FAIL-001` | Ingest/rollup/feed failures recover without duplicates or static-site breakage | Failure recovery |
| `PERS-DESKTOP-001` | A small desktop grid/list suite preserves signal semantics and profile application | Desktop parity |

### Traceability matrix

“DB assertion” always runs in a service-side fixture; a service key never enters the browser.

| Product requirement | Draft scenario | Future Playwright journey | `localStorage` assertion | Network assertion | DB assertion | Profile assertion | Served-list assertion | KPI/guardrail | Required artifact |
|---|---|---|---|---|---|---|---|---|---|
| Consent/privacy | `PERS-COLLECT-001` | Decline, browse, then consent and act | No profile before consent; compatible version after consent | Zero trusted emission before consent | Zero accepted pre-consent rows | No remote mutation pre-consent | Static list marked unpersonalized | no-consent violation count | Redacted storage diff + HAR summary |
| Valid collection/dedupe | `PERS-COLLECT-001` | Scroll past, dwell, open/back, retry one action | Bounded local action/session state | Stable client event/run/session IDs; one retry | One accepted row; explicit drop/dedupe reason | Only eligible signals affect rollup | Action joins exact list/rank | collection success | Trace + request ledger + DB assertion JSON |
| UI-driven maturation | `PERS-LONG-001` | Execute scheduled sessions across catalog days | No injected final profile | Correlated action batches | Evidence belongs only to synthetic identity/run | Maturity reason references observed evidence | Each session has candidate/served evidence | eligible mature session | Daily checkpoint manifest |
| Rollup | `PERS-ROLLUP-001` | Finish session, trigger isolated rollup, start next session | Compatible cached/local view | Rollup request/job correlation | One current versioned snapshot | Expected facets/exclusions, no forbidden facet | Next list references snapshot revision | rollup lag | Before/after profile JSON, redacted |
| Profile application | `PERS-APPLY-001` | Reload/new session and open feed | Restored compatible state | Request carries only allowed compact context | Served record references profile revision | Applied revision equals current eligible revision | Candidate pool and visible order captured | application rate, MRR/precision | Screenshot + served/candidate JSON |
| Relevant discovery | `PERS-QUALITY-001` | Activate labelled holdout, inspect cards naturally | Inspections/actions append, no direct profile write | All inspections correlate | Ground-truth supply present and active | Persona mature before feed request | Distinct valid inspections and first relevant rank | cards/time to first relevant, @20/@30 | Metric JSON + trace/video |
| No supply | `PERS-SUPPLY-001` | Run session on a day with zero labelled relevant eligible candidates | Normal profile preserved | Candidate request succeeds | Supply assertion proves zero | No maturity downgrade caused by supply | Honest non-relevant list/fallback | `no_relevant_supply`, excluded from ranking denominator | Catalog/supply proof |
| Catalog lifecycle | `PERS-LIFECYCLE-001` | Cross activation/update/cancellation/end checkpoints | No stale event state cached as current | Request catalog/build hashes advance exactly once | Snapshot/hash/lifecycle rows match fixture | Profile remains intact across catalog change | New holdout enters; cancelled/ended IDs leave candidate and served lists | supply/lifecycle violations | Timeline validation + before/after served lists |
| Useful novelty | `PERS-NOVEL-001` | Encounter adjacent holdout and take policy-consistent useful action | Novel exposure state recorded | Exploration reason is explicit | Independent adjacency label available | Profile is allowed to learn after outcome, not before | Core relevant slots remain present | exploration share/success, useful novelty | Independent label + order comparison |
| Negatives/hidden | `PERS-NEG-001` | Hide/reject, reload, catalog update | Hidden and negative axes remain separate | Action is idempotent | No duplicate/contradictory active row | Expected exclusion persists | Hidden ID absent from later lists | hidden/negative violations | Cross-session diff |
| Drift/decay | `PERS-DRIFT-001` | Shift behavior in later virtual days | Local action history remains bounded | New evidence timestamps are virtual-date correct | Snapshot revisions remain ordered | New facet grows; stale facet decays per versioned rule | Later feed changes for a stated reason | drift response, stale-interest exposure | Profile-revision timeline |
| Failure/recovery | `PERS-FAIL-001` | Interrupt session; fail/retry ingest/rollup/feed; return | Bounded queue/cache; usable static state | Retry IDs stable, no storm | Exactly-once or explicit terminal drop | Last good profile is not corrupted | Static/last-good fallback labelled | recovery, duplicate, fallback rates | Failure trace + cleanup proof |
| Desktop parity | `PERS-DESKTOP-001` | Repeat selected collection/application cases in grid/list/new-tab | Same semantic state | Same action context despite layout | Same accepted semantic kind | Same revision applied | `layout_mode` changes, signal meaning does not | parity violations | Side-by-side trace summaries |

## 5. Golden-persona and ground-truth contract

The schema is a Phase A contract (`golden-persona/v0.1-draft`), not a final panel or probability table. Fixture data must be wholly synthetic. A persona’s latent truth is test oracle data, not browser profile state.

Machine-readable draft: [`schemas/product-e2e/golden-persona-v0.schema.json`](schemas/product-e2e/golden-persona-v0.schema.json).

The following compact instance validates against the draft schema; it contains bands and policy references, not final weights or probabilities.

```yaml
schema_version: golden-persona/v0.1-draft
persona_id: classical_composer_01
persona_version: 1
label_revision: independent-labels/2026-xx-xx.1
synthetic_fixture: true
description: synthetic fixture only

latent_interests:
  positive:
    - facet_id: classical_music
      strength_band: strong        # band, not final numeric ranker weight
      evidence_basis: independent_label
  negative:
    - facet_id: nightlife
      strength_band: strong
      evidence_basis: independent_label
constraints:
  cities: [kaliningrad]
  date_windows: []
  price: { currency: RUB, max: null, free_preference: null }
  accessibility: []
unknown_exploration_zone:
  - facet_id: ballet
    relationship: adjacent_unknown

maturity_rule:
  policy_ref: maturity-policy/pending-consultation
  required_evidence_classes: [valid_impression, strong_positive, explicit_negative]
  status_at_start: cold
session_schedule_ref: sessions/classical_composer_01/v1
behavior_policy_ref: behavior-policy/classical_composer_01/v1-pending-calibration

ground_truth_ref: ground-truth/catalog-timeline-01/labels-v1
holdout_events:
  - fixture_event_id: evt_holdout_tchaikovsky_01
    activation_day: 7
    label: relevant
    label_source: independent_panel
expected_profile_facets:
  required: [classical_music]
  allowed_adjacent: [ballet]
  forbidden: [nightlife]
expected_exclusions:
  event_ids: []
  facets: [nightlife]
no_relevant_supply:
  scheduled_days: [5]
  expected_state: separate_supply_failure
anti_bubble_expectations:
  useful_adjacent_event_ids: [evt_holdout_ballet_01]
  forbidden_return_event_ids: []
  max_concentration_policy_ref: pending-consultation
  stale_interest_decay_policy_ref: pending-consultation
```

Ground truth is a separate, versioned table keyed by `(persona_id, persona_version, fixture_event_id, catalog_revision)`. It carries `relevant | adjacent | irrelevant | prohibited`, confidence/disagreement, reason codes, independent reviewer provenance, and validity dates. It must not be derived from the ranker score under test. Candidate pools should include labelled holdouts, random eligible items, and hard negatives so evaluation does not only judge already-recommended items.

Maturity is evaluated from accepted action evidence. The harness may query a maturity decision, but it must not write `mature=true` or inject the desired profile snapshot.

## 6. Two-week catalog timeline contract

The minimum timeline is 14 daily snapshots advanced by virtual time. A snapshot is immutable and content-addressed; a later correction creates a new timeline revision.

Machine-readable draft: [`schemas/product-e2e/catalog-timeline-v0.schema.json`](schemas/product-e2e/catalog-timeline-v0.schema.json).

The following is a **single-day excerpt**, not a complete validating timeline instance: a complete fixture must contain at least 14 consecutive entries and pass the semantic invariants below. Hashes are illustrative values with schema-valid format; the loader recomputes them.

```yaml
schema_version: catalog-timeline/v0.1-draft
timeline_id: kaliningrad-14d-panel-01
timeline_version: 1
synthetic_fixture: true
seed: 184467
timezone: Europe/Kaliningrad
start_virtual_date: 2026-09-01
days:
  - day_index: 1
    virtual_date: 2026-09-01
    build_id: fixture-build-001
    catalog_id: fixture-catalog-001
    catalog_hash: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    previous_catalog_hash: null
    active_event_ids: [evt_001, evt_002]
    new_event_ids: [evt_001, evt_002]
    updated_events: []
    cancelled_event_ids: []
    ended_event_ids: []
    holdout_activations: []
    expected_candidate_supply:
      classical_composer_01:
        eligible_active_count: 2
        relevant_count: 0
        adjacent_count: 1
        no_relevant_supply: true
    lifecycle_transitions: []
    source_hashes:
      evt_001: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      evt_002: sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    content_hashes:
      evt_001: sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
      evt_002: sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
    replay_fingerprint: sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
```

Every day records active/new/updated/cancelled/ended sets, source and normalized-content hashes, holdout activation, persona-specific expected supply, and lifecycle transitions such as `scheduled -> active -> cancelled|ended`. Candidate eligibility is recomputed at the day watermark; an ended/cancelled event cannot remain “supply.” Replay fails closed when a build/catalog/hash differs from the fixture manifest.

The loader performs semantic validation that JSON Schema alone cannot express:

1. `day_index` is unique and strictly consecutive from `1`; `virtual_date` is unique and advances exactly one local calendar day.
2. Day 1 has `previous_catalog_hash: null`; every later value exactly equals the previous day’s `catalog_hash`.
3. `active`, `cancelled`, and `ended` membership is lifecycle-consistent: cancelled/ended events are not active or counted as eligible supply; transitions explain every removal.
4. Every `updated_events` previous/current hash matches the adjacent day and current `content_hashes` maps; all active/new/updated IDs have the required source/content hashes.
5. Holdout event, persona, activation day, and label-revision references exist and activation happens exactly on the declared day.
6. For every persona/day, `no_relevant_supply` is exactly `(relevant_count == 0)`, and relevant/adjacent counts cannot exceed eligible active count.
7. The replay fingerprint is recomputed from the canonicalized snapshot and seed; a mismatch aborts replay.

Calendar days advance through the harness clock. Only short interaction delays run as UI time. There are no week-long sleeps and no dependency on wall-clock passage.

## 7. Behavior simulator interfaces

The simulator is an orchestration layer over real UI actions, not a second product implementation. Ranking and KPI mathematics remain outside Playwright.

```ts
interface VirtualClock {
  install(start: Date): Promise<void>;
  setCatalogDay(day: CatalogDay): Promise<void>;
  runUiDelay(kind: 'scan' | 'dwell' | 'read' | 'return', seededMs: number): Promise<void>;
  checkpoint(): ClockEvidence;
}

interface HumanScroller {
  revealNextCard(): Promise<ScrollEvidence>;
  waitForValidImpression(card: CardInspector): Promise<ImpressionEvidence>;
  quickSkip(card: CardInspector): Promise<ImpressionEvidence>;
}

interface CardInspector {
  eventId(): Promise<string>;
  surfaceContext(): Promise<ServedContext>;
  dwell(): Promise<void>;
  openDetail(): Promise<void>;
  back(): Promise<void>;
  like(): Promise<void>;
  unlike(): Promise<void>;
  notInterested(): Promise<void>;
  save(): Promise<void>;
  addToCalendar(): Promise<void>;
  openTicket(): Promise<void>;
  shareIfScheduled(): Promise<void>;
  openRelated(): Promise<void>;
}

interface ActionPolicy {
  readonly policyId: string;
  decide(input: PolicyObservation, rng: SeededRng): PlannedAction;
  applyFatigue(state: PersonaState): PolicyState;
  injectNoise(state: PersonaState): PlannedAction | null;
}

interface HumanSession {
  run(persona: PersonaState, day: CatalogDay, policy: ActionPolicy): Promise<SessionTrace>;
  searchIfScheduled(queryFixtureId: string): Promise<void>;
  interrupt(reason: string): Promise<void>;
  resume(checkpoint: SessionCheckpoint): Promise<SessionTrace>;
}

interface EvidenceCollector {
  captureStorage(label: string): Promise<RedactedArtifact>;
  captureNetwork(): Promise<RedactedArtifact>;
  assertDatabase(runId: string): Promise<DatabaseEvidence>;
  captureProfileRevision(): Promise<ProfileEvidence>;
  captureServedList(): Promise<ServedListEvidence>;
  finalize(): Promise<ArtifactManifest>;
}
```

`CatalogDay` exposes the immutable snapshot and expected supply. `PersonaState` contains only simulator oracle/schedule state; it cannot write the application’s final profile. `ActionPolicy` references versioned behavior bands until empirical calibration exists; it supports seeded randomness, fatigue, accidental/contradictory actions, undo, rare share, extremely rare search, interruption, and return.

Valid impression is distinct from “scroll command executed”: the future product collector and test oracle must agree on visibility fraction, dwell threshold, foreground tab, dedupe window, and stable card/event identity. Those constants remain pending consultation. Hover/focus alone is never a strong action.

Playwright’s clock should be installed before application scripts, with a pinned Playwright version. `page.clock.install()` controls `Date`, timers, animation frames, and `performance`; the harness must separately assert application timestamps derived from those sources. Day changes use explicit checkpoints, while short seeded UI delays preserve observable ordering. Seeded `Math.random` may be installed once before navigation, but multiple init scripts must not rely on evaluation order.

## 8. KPI dictionary — provisional

All metric rows carry `e2e_run_id`, persona/version, catalog/build, virtual date, maturity policy, profile revision, algorithm, taxonomy, surface, viewport/layout, seed, and relevant-supply state.

| Metric | Provisional definition / denominator | Segmentation | No-supply semantics | Anti-gaming caveat | Required data |
|---|---|---|---|---|---|
| `cards_to_first_relevant` | Distinct valid card inspections from eligible feed start through first independently labelled relevant card. Denominator: eligible mature sessions with relevant supply. Whether meaningful action is part of the primary endpoint or a paired secondary endpoint awaits review. | Persona, surface, algorithm, maturity, catalog, viewport | Exclude from ranking denominator; report supply failure | Deduplicate rerenders; no title regex; do not count non-visible cards; report survival/censoring | Candidate pool, served order, valid impressions, ground truth |
| `time_to_first_relevant` | Virtual UI elapsed time to the same endpoint | Same plus session interruption | Separate | Never “improve” by eliminating human-shaped delays from one variant | Clock checkpoints, valid impressions |
| `eligible_mature_session` | Session beginning with a current compatible profile that passes a versioned evidence-based maturity policy | Persona/maturity-policy/profile version | Independent of supply; eligibility and supply are two flags | Harness cannot write maturity directly | Accepted evidence counts/times, profile revision, policy result |
| `relevant_supply` | Count of active, eligible, reachable, independently relevant events in the candidate universe and then candidate pool | Persona/day/constraints/candidate stage | `0` is an explicit coverage outcome | Do not infer supply from returned top-K or score | Catalog snapshot, constraints, labels, candidate stages |
| `MRR`, `precision@20/30`, `recall@20/30` | Standard offline ranking metrics over labelled eligible candidates; denominators/versioned label pool stated per run | Persona/day/algorithm/supply band | Report not applicable at zero relevant labels | Time-aware split; no same-score ground truth; expose incomplete labels | Full scored candidate output + labels |
| `collection_success` | Unique accepted eligible consented client event IDs / unique emitted eligible consented client event IDs; transport attempts and duplicate retries are separate diagnostics | Event kind, actor/trust, browser, failure mode | Not applicable | A lower emitter cannot appear better; report emitted/dropped/quarantined/deduped | Browser ledger + ingest result + DB rows |
| `rollup_lag` | Virtual/processing duration from accepted session cutoff to eligible profile revision | Job/version/failure mode | Not applicable | Separate queue wait and compute; no manual backdating | Accepted timestamps, job/run, snapshot timestamp |
| `profile_to_next_feed_application` | Next eligible feed requests referencing the latest eligible profile revision / eligible next feed requests | Surface/algorithm/fallback | Still measurable at no supply | Storage success alone is not application; exact revision required | Profile and served-list revisions |
| `hidden_event_violation` | Later served lists containing an explicitly hidden eligible ID / later served lists after hide acceptance | Surface/build/profile | Independent | Include linked/merged canonical IDs; do not erase history | Accepted hide, identity resolution, served lists |
| `negative_interest_violation` | Prohibited/strong-negative labelled exposures in guarded slots / guarded exposures | Persona/facet/algorithm | Independent | Separate exploration from prohibited; weak negatives are not hard filters | Labels, profile, served reasons |
| `exploration_share` | Explicit exploration slots / eligible served slots | Persona/maturity/algorithm | Report even without relevant supply, separately | Reason must be server-authored/versioned, not inferred from low score | Served reason/mode |
| `exploration_success` | Independently adjacent/novel exploration encounters with versioned useful outcome / exploration encounters | Persona/novelty class | Separate | Do not use ranker score as novelty or success truth | Independent labels + actions/follow-up |
| `useful_novel_event_encounter` | Sessions encountering a previously unseen, independently adjacent/novel event with a declared useful outcome / eligible sessions | Persona/day/algorithm | Separate | Must remain new to persona and useful by oracle/outcome, not merely clicked | Exposure history, independent labels, meaningful outcome |
| `diversity` | Distributional category/venue/format measures over eligible top-K and inspected cards | Persona/surface/K | Report with supply ceiling | Compare to available supply; do not add irrelevant diversity | Candidate/served facets and supply |
| `concentration` | Top-category/top-venue share and inequality statistic over served/inspected windows | Persona/time horizon | Report with supply | One category cannot win by duplicated aliases/events | Canonical category/venue/event IDs |
| `fatigue` | Repeated event/category/venue exposures within versioned windows and disengagement after repeats | Persona/window/algorithm | Independent | Deduplicate rerenders and linked occurrences | Exposure history + canonical links + actions |

Only the provisional `<=20` primary target and investigated `30` hard ceiling are recorded. Final percentiles, sample/panel aggregation, confidence intervals, minimum relevant supply, maturity thresholds, action endpoint, diversity gates, and canary SLOs are open.

## 9. Anti-bubble acceptance design

Every mature-persona evaluation must pair the primary relevance result with these independent guardrails:

1. At least one scheduled adjacent/novel holdout can be encountered and judged with independent labels.
2. Exploration does not remove all core-relevant opportunity from the guarded top range.
3. Category and venue concentration are evaluated against available supply, not a universal quota.
4. Strong negative/prohibited interests are respected.
5. Explicitly hidden canonical events and their resolved aliases do not return.
6. Later evidence can change the profile; the test does not freeze the initial persona forever.
7. Stale interests decay under a versioned policy.
8. Repeated exposure and fatigue are bounded across sessions.

“Useful novelty” cannot be defined by the same score or reason code being evaluated. It requires an independent adjacency label plus a declared useful outcome such as qualified dwell/detail/save/calendar/ticket under the persona policy; the final outcome set awaits review.

## 10. Test layers and ownership

| Layer | Owns | Does not own | Phase A status |
|---|---|---|---|
| Deterministic unit/contract | Schema validation, seeded RNG, clock, profile compatibility, dedupe, metric math, eligibility and lifecycle | Browser/UI integration | Existing narrow reference contract; new work pending |
| Offline ranker evaluation | Full candidate outputs, time-aware labels, MRR/precision/recall, diversity/concentration, counterfactual comparison | Collection/UI fidelity | Missing for golden timeline |
| Longitudinal simulator | Session schedule, persona policy, catalog progression, action trace, fatigue/noise/interruption | DOM/browser contract | Design-only |
| Playwright browser E2E | Real mobile UI actions, localStorage, network, navigation, visible order/fallback, trace/video/screenshot | Bulk metric computation | Current routed MVP contract only |
| Isolated Supabase integration | Ingest validation/dedupe/RLS, rollup, profile revision, served-list persistence, cleanup | Production data | Missing for telemetry/profile |
| Shadow/replay | Compare algorithm variants on immutable accepted evidence without user-visible mutation | Causal proof by itself | Future, after privacy/data contracts |
| Release canary | Real CDN/config/auth/network path with dedicated synthetic identities and kill switch | Arbitrary production mutation or final offline quality proof | Out of Phase A |

Playwright runs only a compact representative persona/day set through the real UI. Larger seeds/persona panels and ranking mathematics run offline, then a small number of browser journeys validate that the product emits the evidence used by those calculations.

## 11. Environment, isolation, and evidence

### Browser matrix

- Primary longitudinal project: pinned Playwright Chromium with the `Pixel 7` device descriptor (current audited descriptor at Playwright `1.58.2`: viewport `412x839`, touch/mobile enabled). Pin the Playwright version; do not duplicate its user agent by hand.
- Desktop parity: Chromium `1440x900`, only the semantic matrix below.
- Browser-engine expansion is a later compatibility decision, not a multiplier on the two-week persona matrix.

| Desktop parity case | Required assertion |
|---|---|
| Same event in grid and list | Impression/action keeps the same event, surface, served-list and semantic action kind; only layout/presentation changes. |
| Profile application | Same eligible profile revision is used and exclusions remain equivalent. |
| Hover/focus | Neither becomes a strong interest signal. |
| Back/new tab | Served/action context survives or fails explicitly; it is never silently attached to another list. |

### Clock and replay

- Install the browser clock before navigation/application timers.
- Use `Europe/Kaliningrad` as catalog/business timezone and store UTC instants plus virtual local date.
- Keep one master seed and derived per-persona/session/action seeds in the artifact manifest.
- Advance days only after evidence flush/checkpoint; replay rejects a mismatched timeline fingerprint.

### Isolated database

Preferred order:

1. ephemeral/local Supabase stack or dedicated non-production project;
2. otherwise a dedicated test namespace/identity range with hard environment allowlist, `synthetic=true`, mandatory `e2e_run_id`, retention TTL, and service-side cleanup;
3. release canary later uses only dedicated test identities and rows excluded from rollup/training of ordinary profiles.

The browser receives only the intended public/publishable credential. Service-role assertions and cleanup run in a separate fixture process. A production URL/project ref is a fail-closed preflight error for simulator/integration runs. No real user data appears in fixtures.

### Correlation envelope

Every browser event, ingest result, DB row, profile snapshot, candidate list, served list, metric and artifact manifest must be joinable on:

```text
e2e_run_id + persona_id/version + session_id + virtual_date
+ build_id/catalog_id/hash + profile_revision + algorithm_id/version
+ taxonomy/feature schema + served_list_id/hash
```

### Artifact bundle

Write ignored artifacts to `artifacts/codex/personalization-e2e/<e2e_run_id>/`:

- `manifest.json` with versions, seeds, scenario IDs, checksums and cleanup status;
- Playwright trace, selected video/screenshots, and console summary;
- redacted before/after localStorage snapshots;
- redacted request/response ledger (no token, secret, raw email, IP, or real user ID);
- catalog/supply/candidate/served-list evidence;
- service-side DB and profile assertions;
- metric outputs and comparison decision;
- cleanup proof and remaining-row count.

Redaction runs before artifact write, not only before upload. Browser storage state and HAR files are denied by default because they can contain auth material. Synthetic IDs must still be treated as identifiers in artifacts.

## 12. Missing seams as implementation backlog for Phase B

Priority follows dependency order, not desired UI polish:

1. Extract/shared-test the actual Astro collection/profile module so fixture and product cannot drift.
2. Define and implement valid impression/dwell/detail/back/action context.
3. Add typed, consent-aware isolated ingest with dedupe/quarantine/quota and SQL security tests.
4. Implement versioned rollup/maturity and profile-revision evidence.
5. Implement/commit the profile-aware candidate/feed RPC contract currently referenced but absent.
6. Persist candidate and served-list evidence with full correlation envelope.
7. Implement schema validators, timeline loader, independent ground-truth loader, and offline metric calculator.
8. Implement the seeded simulator and only then targeted actual-Astro Playwright journeys.
9. Add shadow/replay comparison and, after governance approval, a synthetic-only canary.

None of these is implemented by this Phase A document.

## 13. Awaiting external consultation

### Decisions that cannot close yet

- primary discovery endpoint: first valid ground-truth inspection versus first meaningful action, and paired metrics;
- mature-profile evidence rule and reactivation/drift semantics;
- final persona panel, session cadence, action probability distributions, fatigue/noise model, and calibration source;
- independent label protocol, disagreement handling, and holdout/sample design;
- percentile/SLO formulation around provisional `20` and investigated ceiling `30`;
- statistical power/uncertainty and comparison acceptance rule;
- exploration strategy and final diversity/concentration/fatigue gates;
- decay windows and profile rollback/versioning;
- counterfactual/shadow method and simulator-to-reality calibration;
- valid impression/dwell thresholds and accelerated-time fidelity;
- canary scope, synthetic traffic classification, and production exclusion controls.

### Questions for both eligible consultants

1. Which endpoint and percentile formulation best preserves the “find within 20–30 cards” product promise without coupling ranking quality to a simulated click policy?
2. What evidence-based maturity rule is stable across narrow, mixed, drifting, sparse-supply, and reactivated personas?
3. How should relevant/adjacent/usefully novel labels be created independently, sampled, versioned, and adjudicated?
4. Which minimal persona panel and catalog perturbations expose candidate-generation, ranking, profile, and anti-bubble failures without overfitting fixtures?
5. How should action policies be calibrated, and what sensitivity analysis is required before synthetic behavior can influence a release decision?
6. Which time-aware split, replay/counterfactual method, uncertainty interval, and acceptance test should compare variants?
7. Which anti-bubble measures and supply-aware gates are decision-useful for time-sensitive local events?
8. Which clock/impression semantics preserve browser realism while allowing a 14-day replay in minutes?
9. What is the smallest trustworthy Playwright subset versus offline/integration coverage?
10. Which data-isolation and contamination controls are mandatory before shadow or canary use?

### Review artifact and synthesis protocol

Future eligible reviews should be saved under:

```text
docs/features/unsigned-personalization/reviews/product-e2e/
  consultant-a-<provider-model>-<date>.md
  consultant-b-<provider-model>-<date>.md
  synthesis.md
```

Raw provider/CLI captures remain ignored under `artifacts/codex/personalization-product-e2e-consultants/<provider>/<date>/`. The committed review files above contain only sanitized, durable evidence and decisions. Each records provider, exact eligible model/class, date, prompt/brief version, reviewed commit, source links, limitations, and recommendations. An unavailable or lower-class model response is marked `supplementary probe material`, never a completed consultant review.

The user-supplied “Executive Conclusion” memo accompanying this task is useful supplementary intake (hybrid layers, virtual time, independent labels, no-supply isolation, survival-style reporting), but it lacks repository-recorded provider/model/provenance and does not satisfy the two-review gate by itself.

Synthesis uses this table:

| Decision ID | Consultant A | Consultant B | Cross-critique | Project evidence | Resolution | Required diff/test |
|---|---|---|---|---|---|---|
| `E2E-D01` | recommendation + rationale | recommendation + rationale | agree/disagree/risk | code/data anchor | accept/adapt/reject/defer | exact follow-up |

Mutual critique should focus on the 20/30 SLO formulation, maturity, ground-truth independence, simulator calibration, counterfactual bias, anti-bubble gates, valid-impression clock semantics, and synthetic-data contamination. Phase B starts only after every blocking decision has a recorded resolution.

## 14. Phase A acceptance record

| Requirement | Phase A status | Evidence |
|---|---|---|
| Current-state audit | **Done** | Section 3, with code/data/test owners and honest statuses |
| Traceability model and stable IDs | **Done (design-only)** | Section 4; no fake executable feature added |
| Golden-persona schema | **Done (draft)** | Section 5; no final probabilities/maturity threshold |
| Catalog timeline schema | **Done (draft)** | Section 6; deterministic 14-day minimum |
| Behavior simulator interface | **Done (draft)** | Section 7 |
| KPI draft | **Done (provisional)** | Section 8; only 20/30 carried as provisional/under investigation |
| Test layers | **Done** | Section 10 |
| Environment/evidence/isolation | **Done (design)** | Section 11 |
| Consultant intake | **Done; reviews missing by design** | Section 13 |
| Final implementation/live proof | **Missing / out of Phase A** | Explicit Phase B gate |

## References

- [Product E2E research brief](product-e2e-research-brief.md)
- [Personalization family status](README.md)
- [Event-detail related contract](event-detail-related.md)
- [Personal feed architecture](personal-feed-architecture.md)
- [Production integration gates](production-integration.md)
- [Database design](database.md)
- [Playwright Clock](https://playwright.dev/docs/clock)
- [Playwright `page.addInitScript`](https://playwright.dev/docs/api/class-page#page-add-init-script)
- [Supabase local development](https://supabase.com/docs/guides/local-development/overview)
