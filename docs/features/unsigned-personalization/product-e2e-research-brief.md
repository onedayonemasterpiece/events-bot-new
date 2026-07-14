# Product E2E-driven personalization research brief

> Status: **separate research/design task seed; external reviews pending; no final implementation or KPI calibration yet**
>
> Working branch: [`feature/personalization-product-e2e-design`](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/personalization-product-e2e-design)
>
> Pending release-plan input: [PR #26 E2E acceptance draft](https://github.com/onedayonemasterpiece/events-bot-new/blob/agent/static-release/checklist-cdn-social/docs/features/unsigned-personalization/e2e-acceptance.md)
>
> Phase A output: [preliminary product E2E design](product-e2e-design.md), [concrete golden-persona panel with strict real-event protocol](golden-personas-real-data-v0.md), [database sustainability gate](database-sustainability-e2e.md) and [external consultant handoff](external-consultant-review-pack.md). These remain design outputs and do not start Phase B.

## Product objective

Use longitudinal product E2E as a design and tuning loop, not merely as regression automation. After several weeks of short, human-like sessions, a mature user profile should make a genuinely relevant active event discoverable within a small number of valid card inspections while preserving useful exploration and avoiding a rigid filter bubble.

The current provisional release target is `cards_to_first_relevant <= 20` for eligible mature golden personas with relevant supply. The owner has framed `20–30` cards as the product range to investigate. Research must recommend whether `20` remains the primary target with `30` as a hard ceiling or whether another evidence-based formulation is better; it must not silently weaken the existing provisional gate.

## Current system (`as-is`)

- Static Astro pages and same-origin manifests remain useful without personalization, JavaScript or Supabase.
- The first implemented/reference surface is event-detail related recommendations; listing/personal feed architecture is designed but not complete end to end.
- The browser keeps a compact versioned local profile in `localStorage` only after consent and can locally filter/rerank static candidates.
- Signals include valid impressions/dwell, detail opens, likes, explicit `not interested`, calendar/favorite/ticket actions, successful shares and compact served-list context. Weak/noisy observations must not gain strong weight.
- Supabase/Postgres is intended to store compact accepted/deduped telemetry and profile snapshots; Fly SQLite remains the canonical event source. Remote ingest, durable rollup and profile-to-next-feed application are partial/design-stage rather than proven as one release E2E.
- Semantic `search_v3`/`related_v1` vectors and strict related-event canaries exist, but no LLM/vector call belongs in the page-view hot path.
- Current browser/Gherkin assets are deterministic MVP contracts, not a longitudinal multi-session product-quality simulator.

Canonical context:

- [Personalization family](README.md)
- [Product requirements](requirements.md)
- [Personal feed architecture](personal-feed-architecture.md)
- [Event-detail related contract](event-detail-related.md)
- [Semantic vector retrieval](semantic-vector-retrieval.md)
- [Production integration](production-integration.md)
- [Taxonomy](taxonomy.md)
- [Smart Update boundary](smart-update-contract.md)
- [Bots and automation](bots-and-automation.md)
- [Database design](database.md)
- [Golden personas + real-data protocol](golden-personas-real-data-v0.md)
- [Database sustainability gate](database-sustainability-e2e.md)
- [External consultant review pack](external-consultant-review-pack.md)
- [Current Gherkin](../../../tests/e2e/features/static_site_personalization.feature)
- [Current Playwright contract](../../../tests/playwright/static_personalization_contract.spec.ts)

## Target system (`to-be`)

A reproducible Playwright-based longitudinal harness drives controlled non-PII golden personas across content-addressed real production catalog snapshots and accelerated virtual time. Personas acquire their profile through visible UI actions rather than direct injection of the desired final profile. The same correlated run proves:

1. browser/localStorage collection;
2. accepted and deduplicated server evidence;
3. profile rollup with expected positive, negative and time-horizon facets;
4. application to a later served list;
5. product outcome and anti-bubble guardrails.

Mobile is the primary full-fidelity journey. Desktop gets a smaller parity suite proving the same semantic telemetry/profile effects under grid/list presentation, without trying to duplicate the entire longitudinal mobile matrix.

## Research questions

1. How should golden-persona interests and ground-truth relevant/adjacent/irrelevant events be labelled without circularly copying the ranker's own features?
2. Which persona families, catalogue conditions and behavior patterns give enough coverage: narrow/broad interests, strong negatives, changing interests, sparse supply, multi-city/date/price constraints, cold/mature/reactivated profiles?
3. How should two or more weeks of short sessions be accelerated through deterministic virtual time while preserving realistic dwell, scroll, return cadence, interruptions and action rarity?
4. Which actions should be simulated and at what calibrated distributions: scroll/quick skip, valid impression, dwell/read, detail/back, like/unlike, save/calendar, ticket, rare share, explicit hide, related navigation and rare search?
5. How do we prevent scripted personas from gaming the system or merely restating its scoring formula?
6. Which primary KPIs, diagnostics and guardrails are decision-useful, statistically defensible and hard to improve misleadingly?
7. How should exploration, novelty, serendipity, diversity, fatigue and negative-interest violations be evaluated so relevance gains do not produce a stale filter bubble?
8. What belongs in deterministic contract tests, isolated Supabase integration E2E, shadow/replay evaluation and a tightly controlled release canary?
9. Which architecture/test seams are missing and should become product backlog rather than being hidden inside the test harness?

## Required scenario dimensions

- Multiple seeded golden personas, including composer/classical-music affinity such as «Чайковский», theatre, family, exhibitions, excursions, mixed interests, strong exclusions and drifting interests.
- At least two weeks of `as_of` progression over frozen real event records. New/update/cancel transitions are used only when evidenced by captured daily snapshots; one-snapshot forward projection may only age/filter known records. Real holdouts, lifecycle expiry and low/no supply are reported without invented events or facts.
- Short sessions with seeded but human-shaped action timing; real-time waiting for weeks is forbidden.
- Meaningful action rarity: shares and search must remain uncommon; noisy/accidental behavior and occasional contradictory actions are represented.
- Cross-surface paths: listings/feed, event detail, related events, favorites/calendar, ticket CTA and occasional authorized search.
- Identity/consent variants, local-only fallback, remote ingest/rollup success and bounded failure/recovery.
- Mobile longitudinal coverage plus desktop collection/application parity.

## Provisional KPI frame to challenge and refine

- Primary outcome: `cards_to_first_relevant`, segmented by mature/cold, relevant supply, persona, surface and algorithm version.
- Reliability: collection acceptance/dedupe, profile-rollup lag and profile-to-next-feed application rate.
- Ranking diagnostics: MRR, precision/recall at 20/30, relevant-event coverage and repeated exposure/fatigue.
- Anti-bubble guardrails: useful novel-event encounter rate, category/venue/format diversity, exploration share and quality, negative-interest violations, hidden-event recurrence and top-K concentration.
- UX/reliability guardrails: quick-skip/not-interested rate, CTA latency, fallback rate, no-consent privacy and corrupted/offline recovery.

`No relevant supply` is a separate catalogue/coverage result, never a failed ranking session hidden inside the average. A “relevant” result must be determined from versioned persona/event ground truth and meaningful outcomes, not title regex or the same score being tested.

## Work phases

### Phase A — allowed before consultant synthesis

- map current code/docs/tests and identify trustworthy versus aspirational paths;
- draft persona, catalogue-day, behavior-policy, ground-truth and evidence schemas;
- define scenario IDs and a traceability matrix;
- design deterministic seeded virtual clock/session playback and Playwright page-object/human-behavior interfaces;
- specify isolated controlled test traffic, Supabase/YDB assertions and cleanup/TTL, per-store 30/90/365-day growth projections, artifacts and metric calculation;
- record open decisions and implementation seams without choosing hidden defaults.

### Phase B — only after external reviews are supplied and reconciled

- compare consultant recommendations and document accept/adapt/reject decisions;
- finalize KPI definitions/targets, persona panel, action distributions and anti-bubble gates;
- implement the executable longitudinal harness and Gherkin/Playwright scenarios;
- run the E2E-driven tuning loop, fix product/system gaps, and repeat until outcome and guardrails pass;
- produce release evidence from deterministic, integration and controlled canary layers.

## Guardrails

- Do not mutate production data or train ordinary profiles with controlled test activity. All event/catalog facts must come from frozen real production records; do not invent events, dates, prices, cancellations or lifecycle states.
- Do not inject the final desired profile as a substitute for interaction-driven formation.
- Do not call paid LLM/image/provider APIs merely to scaffold tests.
- Do not use week-long wall-clock sleeps; use deterministic virtual time plus short seeded UI delays.
- Do not optimize only CTR or only the happy persona; report supply, exclusions, diversity and failure paths.
- Do not change production ranking weights, DB schema or final thresholds in Phase A without the later consultant-synthesis decision.

## Expected durable outputs

- research synthesis and decision log;
- versioned golden-persona and ground-truth contract;
- longitudinal behavior/session simulator specification;
- Gherkin scenario catalogue and Playwright architecture;
- KPI dictionary with denominators, targets, segments and anti-gaming caveats;
- test-layer/environment/data-cleanup plan plus Supabase/YDB sustainability evidence;
- prioritized product/architecture gaps discovered by the E2E design;
- implementation and evidence plan for the subsequent E2E-driven tuning work.
