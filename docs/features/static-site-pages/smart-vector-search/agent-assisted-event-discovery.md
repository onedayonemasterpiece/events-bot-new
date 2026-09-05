# Agent-assisted event discovery — product, architecture and experiment plan

> **Status:** planned product implementation; the owner correction below is accepted product direction, not evidence of runtime delivery or authorization to enable a provider. Existing Auth, provider eligibility, privacy and release gates remain in force.  
> **Updated:** 5 September 2026, after the owner's clarification of voice limits, Floating Island composition and a scrollable answer timeline.  
> **Depends on:** the canonical Smart Search contract in [`README.md`](README.md), [shared shell](../mobile-shell.md), [personalization ownership](../../../architecture/personalization-data-ownership.md), [autotest strategy](../../../operations/static-site-autotest-strategy.md) and [location directory](../../location-directory/README.md).

## 0. Owner correction and authority

This file owns the planned conversational Search behavior. The [5 September analysis](../../../reports/voice-assistant-product-technical-vision-20260905.md) remains historical reasoning, not a second normative contract. The following correction supersedes any restrictive interpretation of its proposed budgets, single-widget placement or replacement-only result presentation:

| Earlier interpretation | Owner direction adopted here |
|---|---|
| Voice inherits the ordinary Search allowance or an arbitrarily small fixed turn cap | Voice may have a separate, load-adaptive product allowance, while every actual provider attempt still uses the one shared quota ledger. Preserve useful functionality when capacity permits. |
| Avoid another Floating Island | There is a managed system: partial header, eligible single shelf, bottom navigation, and voice/composer; answer-section headings participate through the same contextual role. Do not impose a one-island-per-page rule. |
| A successful refinement replaces the previous result list | A committed answer remains a scrollable section. A subsequent answer appends a new section with its own title, question, short answer and results. |
| Assistant prose is always a fixed tiny sentence | Give a short useful response; expand detail when the question needs it. Address/transport answers are legitimate when grounded in existing data. Compact presentation must not erase the answer. |

Only these product directions are accepted by this correction. Numerical TTLs, geometry, queue sizes and adaptive thresholds remain implementation candidates until tested. Auth-only admission is not changed by permission to adjust voice usage limits. No runtime, current normalization foundations, deployed quota configuration or STATUS is changed by this document.

The current [UI normalization issue #621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621) remains the integration owner for shared component families and the single published Kaggle Preview path. Historical mobile-shell lab restrictions describe their own baseline; they are not a prohibition on the owner's future multi-island system. Do not silently apply a new layout policy to the current AS-IS review.

## 1. Product hypothesis

A conversational assistant helps users describe a wish without knowing which filters, categories or events to choose. It is an optional discovery mode over the same trusted Event corpus, not a general-purpose chatbot or a replacement for useful classic listings.

```text
classic Search/listing
+ voice or text expression of intent
+ progressive refinements
+ scrollable, separately titled answer sections
+ the same canonical Event facts and actions
```

Expected value: natural multi-constraint requests; refinements such as «только бесплатно», «на побережье», «не концерт»; short grounded explanations; comparison; recovery after unsuccessful Search; and factual place/transport answers where data exists.

Risks to measure: exhausting clarification, generated false confidence, quota spent without better discovery, an ever-growing DOM/history, and floating controls obscuring the catalogue. The remedy is shared presentation and bounded execution, not arbitrary suppression of conversational functionality.

## 2. Initial rollout decision: rescue first, voice is an input mode

Do not replace the default Search surface without evidence. Initial assistant entry remains zero results, explicit «Нет, не нашёл», repeated reformulation or «Помочь подобрать». Optional voice input in Search need not wait for a user to fail repeatedly; input modality and assistant rollout eligibility are separate policies.

Both modes search the same corpus. Keep an immediate classic-list/filter route. Later opt-in or randomized primary presentation is an experiment, not implied deployment permission.

## 3. Architecture decision

The website calls first-party backend/domain operations directly; MCP is an adapter for external clients, not a dependency in the website's critical path.

```text
browser / existing Auth and operation transport
→ voice/text interpretation through allowed Lite policy and shared limiter
→ validated intent state and ordered utterance receipts
→ existing embedding/pgvector retrieval and hard filters
→ grounded optional presenter, or deterministic summary from trusted facts
→ typed answer section rendered with canonical EventCards
```

This is bounded host-owned orchestration, not an open-ended agent. Do not create a second Search index, user profile, quota ledger or result-card renderer. A Devstand deployment means an isolated ordinary service, not a coding-agent call per user query. Fly stays within the existing thin-runtime boundary.

Shared domain operations may later have direct API, Gemini function-declaration and external MCP adapters. Read-only candidates remain `events.search`, `events.get`, `events.compare`, `locations.search`, `locations.get`, `locations.resolve_for_event`.

Feedback/save/calendar/profile/subscription changes are separate typed product commands with existing ownership, consent, authentication and idempotency rules. Public discovery does not expose owner-only Smart Update/admin tools.

## 4. Model role and elastic resource policy

Use the owner-approved Lite family through the existing model-policy registry. Do not hardcode an unverified model ID from an old plan or silently escalate to Flash/Pro/Gemma. Interpret the input, return structured constraints, select allowed IDs when candidates are available, and produce short grounded explanations. Avoid a default chain of independent ASR, planner, verifier and prose-writer calls.

One Lite call per ordinary turn remains an efficiency target, **not a product cap or an unconditional release requirement**. Query embedding is separately accounted when not cached. If audio interpretation must precede retrieval, a grounded model answer requiring retrieved facts may need a subsequent bounded Lite call. A pre-retrieval model cannot truthfully describe unseen results. Reuse the existing verifier/presenter where possible rather than invoking two equivalent stages. A title alone must not trigger a separate model call.

### 4.1 Three distinct boundaries

1. Provider/project/model hard limits and approved overall spend: the existing authoritative limiter.
2. Product allowance for `voice_search`: separate from ordinary typed Search and adjustable under load, not necessarily stricter. The project owner may configure it more generously.
3. Technical safety: bounded audio bytes/duration, queue size and execution deadline. These protect resources; transport chunks are not independent user questions.

Every provider call from website, CI, notebooks or batch work goes through `reserve → mark_sent → provider → finalize`. A product allowance never creates another provider pool, changes project identity or bypasses this ledger. Provider limits are project-level, not per key; see [official rate limits](https://ai.google.dev/gemini-api/docs/rate-limits).

### 4.2 Adaptive behavior without choking the product

Start with a small versioned server-owned policy, not a second scheduling platform. Inputs are real shared RPM/TPM/RPD headroom, approved spend headroom, queue wait and active demand. Use smoothed signals and hysteresis so a single slow call does not alternate available/unavailable states.

- With spare capacity, allow generous bursts and borrow unused capacity within approved ceilings; do not reserve a permanently idle rigid voice slice.
- Under pressure, defer eligible bulk/test work at safe admission boundaries, reuse caches, coalesce not-yet-dispatched additions and avoid redundant generation before refusing interactive input.
- Keep a fair bounded interactive queue; neither a long existing conversation nor one user may starve all new users. New sessions must remain possible.
- Distinguish meaningful new input from technical retries/duplicate fragments. Their product accounting must be explicit, while actual provider attempts are always accounted individually.
- Lowering a policy does not retroactively discard accepted utterances, reset context or remove earlier answers. Existing leased work follows its accepted terms; genuinely unsent work can be rescheduled visibly. Resource/safety failure still terminates honestly.
- At a real ceiling, explain the temporary state and preserve the question. Show `retry_after` only when supported. Do not promise unlimited recording or display a false listening/accepted state after bounded capture capacity is exhausted.

Keep `policy_version`, admitted/queued/rejected outcome, coarse reason and actual usage in operational evidence, without raw speech in analytics. Existing per-user quota infrastructure is extended; no parallel allowance table/service is added without demonstrating an actual gap.

Availability manifests contain public service state, not personal allowance. A signed-in operation reads its effective personal policy from an authenticated response. Neither stale manifests nor client-advertised remaining capacity authorize a provider call. Missing authoritative limiter metadata fails closed for providers, not for static browsing.

## 5. Search-first, clarify-second interaction

Normally search known constraints immediately and show useful candidates before asking another question. A small initial page of canonical cards is a presentation default, not a total-result or turn limit. «Показать ещё» belongs to that answer section.

Avoid mandatory questionnaires about date, budget, companions, genre and city before results. Ask only when ambiguity materially changes the result. Hard constraints must not be silently relaxed; alternatives have their own explicit label. Unknown price is not free. Search-chip query insertion and links to a materialized collection must be visibly distinct; login does not auto-submit a cost-bearing query.

## 6. Compact conversation state and answer history

Maintain a bounded structured intent: dates with original temporal anchor/timezone, locality/location IDs, budget/currency, formats/themes, audience, exclusions, hard/soft status, provenance, shown/rejected IDs and a small recent utterance tail. Temporary «с ребёнком сегодня» is not automatically a permanent preference.

Use the existing Supabase identity/profile owner through authorized server/RPC operations; do not create a competing YDB profile. Client IDs alone do not prove ownership. Privileged routes must enforce ownership too. Browser storage stays within the shared budget; audio is not inserted into the tiny generic text outbox.

Active-context and resumable-history retention are distinct concerns. The analytical proposal of 30-minute inactive context / 7-day explicit resume is not an accepted hard product limit. Forgetting active implicit conditions must not silently erase a still-visible answer section. Showing an old section does not silently reactivate expired conditions: explicit refinement/resume checks dates, ownership and catalogue freshness. Deletion/logout invalidate late replies and clear the accessible local projection according to the existing identity rules.

### 6.1 Ordered additions and committed sections

Capture can remain active while processing. Preserve accepted utterance order and deduplicate by stable ID/payload; apply intent changes atomically against the right revision. Cancellation suppresses obsolete work/results, not the user's accepted speech. An ambiguous sent timeout is resolved through the existing operation receipt, not a second blind cost-bearing POST or alternate transport.

There is at most one visible **draft answer section for the current query revision chain**. U1 «бесплатно», U2 «на побережье», U3 «вечером» arriving before that answer is committed may refine that draft. Several not-yet-dispatched additions can be coalesced without losing their identity/order. Do not create a screenful of half-complete sections for each audio chunk.

After an answer is committed and shown, a new query/refinement creates another section. The preceding title, question, model answer, effective constraints and result membership are not overwritten. A duplicate completion cannot append a duplicate section; a stale completion cannot rewrite a committed newer section.

### 6.2 Minimum logical section record

Illustrative fields, not a mandate for separate tables:

```text
section_id, conversation_id, parent_section_id
state: draft | ready | empty | error
kind: results | explanation | mixed
user_query, effective_intent, title, answer_blocks
result_set_id, event_ids, result_count, section_cursor
catalog_revision, policy_version, created_at
```

Reuse existing utterance/operation IDs, epoch and context revision. No event binary/media copies or full profile vectors in history. Local and server history have byte/retention limits and deliberate pagination; no unbounded DOM or repeated complete-history transfer. A bounded private rendered snapshot can reuse existing versioned card projections so that historical results remain intelligible. Current cancellation/availability is a separately labelled freshness overlay, and actions revalidate current facts. Never represent a historical card as a current ticket guarantee.

### 6.3 Viewed section is not automatically the query target

Keep three concepts separate: section currently being read, selected base for the next refinement, and in-flight draft. Scrolling alone cannot change the base of «а какие из них…».

Default refinement base is the latest committed answer. The composer visibly names it, for example «Уточняем: Бесплатные события на побережье». An older section offers «Уточнить эту подборку»; selecting it sets `parent_section_id`, while the new answer still appends chronologically. No tree UI is needed. Show a compact «Уточнение к…» link when the parent is not the immediately previous section.

«Второе событие» resolves against the explicitly referenced/presented `result_set_id`, not whichever list finished loading most recently. «Из них на побережье» filters the logical complete parent selection, not only its initially rendered cards; preserve its constraints, catalogue revision and exclusion semantics. A broader fresh search is a distinct, visibly labelled action. Expired parent snapshots cannot silently change the meaning of «из них».

## 7. Typed response and grounded content

Return closed semantic blocks, not arbitrary HTML, executable Markdown, model-authored URLs or generated factual card fields. The host owns titles, dates, prices, images, addresses, medallions and navigation through trusted resolvers. Existing block types remain text, event group, event annotation, location/map actions and suggested replies.

An answer section also has a selection title and original user question. Derive the title from the effective validated constraints where possible: «Бесплатные события» → «Бесплатные события на побережье». Optional model wording cannot introduce an unsupported promise. Do not name a paid-alternative section «Бесплатные события» or include unknown-price records under that claim.

The user's wording and the interpreted constraints remain distinct and readable. Server validation checks every Event/location/action ID against the trusted retrieval/reference context. Explanations cannot invent popularity, endorsements, accessibility, age suitability, opening hours or transport times. Malformed generation falls back to the valid ordinary result set plus an honest status, not a fabricated answer.

Limited formatting such as paragraphs, emphasis and short lists is acceptable through the existing safe text renderer. No arbitrary HTML or model-authored action links. Facts expressed in prose require the same evidence as card facts.

## 8. UI composition: scrollable answers and a coordinated island system

The conversation is a vertical feed of useful answer sections, not a wall of chat bubbles and not one replaceable result list.

```text
SECTION 1
  Бесплатные события                 ← contextual heading / island
  Вы спросили: «Куда сходить бесплатно?»
  Short model answer grounded in results
  Existing EventCards + section-scoped «Показать ещё»

SECTION 2
  Бесплатные события на побережье     ← new heading; section 1 remains above
  Вы спросили: «А какие из них на побережье?»
  Short explanation / advice
  Existing EventCards + section-scoped «Показать ещё»

SECTION 3 may be explanation-only
  Как добраться до выбранной площадки?
  Question + factual answer / map actions; no dummy empty EventCard grid
```

A normal recommendation starts with roughly 2–4 useful sentences; use a collapsed preview with «Подробнее» when detail is relevant, rather than tiny inaccessible type or blindly chopping a sentence. This is a writing default, not a hard cap. Important uncertainty, an unavailable transport fact or a qualification such as registration requirement must not be hidden solely to make the answer short. Long user questions are expandable, not silently truncated. Cards remain scannable and the full selection remains reachable.

### 8.1 Sticky behavior and scroll

Each section has one semantic heading in the document flow. As it reaches the usable upper edge, it assumes the registered compact sticky Floating Island state within that section. The next section replaces it at its boundary; scrolling back restores the previous section's heading. Do not stack every historical heading, clone an extra accessible heading or leave the wrong heading over another result set. Only the compact title/essential context is pinned, not the entire question and model answer.

On an explicit new query, reveal the **boundary after the previous answer and the new section's heading**, not the end of the newly appended cards. Allocate the draft region first to avoid repeated scroll jumps as text/images arrive. Perform this deliberate transition once; a late response does not repeatedly force scrolling. If the user has subsequently scrolled upward to inspect history, keep their reading position and offer «Новый ответ ↓». Opening a card and navigating back restores its section and position. Merely loading more results for an old section must preserve the current reading anchor.

Use the existing shared keyboard/navigation behavior; do not introduce a search-only ArrowUp/ArrowDown interception that breaks text editing. Respect reduced motion, keyboard focus, screen-reader announcements and scroll offsets beneath effective chrome.

### 8.2 One layout policy, several islands

Floating Island is a surface/behavior system, not a component count limit. Its initial role inventory is:

| Role | Responsibility |
|---|---|
| Partial site header | Brand, top-level page context and existing menu |
| Single-shelf / answer-section context | Eligible shelf title/controls or the currently read answer title |
| Bottom navigation | Existing top-level navigation |
| Voice/composer | Recording, text entry, visible processing and current refinement base |

They may coexist where space allows. A section heading uses the same contextual family, not a new ad-hoc floating layer for each answer. Floating surface appearance is distinct from positioning: `in_flow`, `sticky` and `fixed` are explicit states, not synonymous with rounded corners.

Extend the existing shell/layout owner with role, placement, measured occupied rectangle/height, compact/expanded state and layer priority. Prefer a small shared layout module and semantic CSS variables, not a generic window manager, drag system, event-bus platform or second shell. Concrete exports/names are implementation choices after reading active #621 family APIs.

Required composition rules:

- Compute effective top and bottom occupied space, gaps, safe areas and keyboard viewport once. Do not let each consumer invent `top`, `bottom` or `z-index: 9999`.
- Reserve a top contextual slot below the actual header footprint; single-shelf/answer heading variants share that role rather than accumulating competing bars. On desktop, side-by-side placement is possible where measured geometry permits.
- Bottom navigation and the voice island are distinct roles. They can appear as neighbouring islands on desktop or a coordinated stack/compact accessory on mobile. Do not overlap touch targets or cover the last card's actions. This is not a prohibition on the fourth island.
- At small effective heights, compact secondary context and use the existing keyboard/drawer/modal policy. The active input, a visible stop-recording control and status remain reachable. Any replacement of dock chrome while editing must be an explicit shared-shell state with a return path, not an accidental disappearance under the keyboard.
- Expanded menus/dialogs have documented precedence and focus behavior. A lower layer must not become clickable through them. Reconcile notifications with existing toast policy; avoid duplicate announcements.
- Transitions must not move the recording control out from under a finger, keep invisible duplicate controls focusable or create a layout/scroll loop. No screen-sized invisible hit plane around a small island.

Implementation preference: section-contained CSS sticky with shared offsets; minimal observation for active heading/compact state and measured shell sizes. [CSS Positioned Layout](https://www.w3.org/TR/css-position-3/) constrains sticky positioning by the nearest scrollport and containing block. Nested overflow and transformed ancestors therefore belong in integration tests. Do not require experimental positioning APIs just to ship this behavior.

### 8.3 Progress, availability and A=S=P

Capture and processing are independent. Listening animation reflects real input; pending network work has a different status. «Ищу события; можно дополнить» is valid while recording another utterance. Keep committed sections visible throughout processing, quota delays and errors. JSON with honest indeterminate progress remains valid; do not simulate percent or invent backend phases.

The expiring CDN availability manifest is a public hint, not personal admission. Avoid per-tab direct backend polling. Expired status is unknown; an active composer's context/control does not disappear during an outage. Local failure does not publish a global service failure.

Register these states through the existing SoT component families and the [design-system work](https://github.com/onedayonemasterpiece/events-bot-new/issues/621). Astro and native Penpot must use the same frozen Event Corpus, UI-state/viewport fixture, component versions and semantic assets. Test flow/sticky headings, multiple islands, expanded/compact composer, dogon, errors and keyboard-effective-height cases. A static Penpot board proves a declared state, not network timing or hardware recording. This task does not claim Penpot materialization or normalization completion.

## 9. Location, address and transport answers

Support factual questions as first-class `explanation` or `mixed` answer sections. A useful answer does not require a new EventCard grid. Reuse trustworthy event/venue fields and existing transport descriptions with provenance when available; do not block a simple known address behind completion of every planned geodata feature.

The planned location directory remains the owner of canonical location identities, coordinates and generated map actions. An interim trusted event-address projection must not become a competing location database. Link actions are produced by approved resolvers, never by the model.

Distinguish named-place distance, straight-line distance, route distance and walking/driving/transit duration. Answer a known transport description without inventing live service information. Missing timetable or route data is stated clearly; «рядом со мной» requires its separate permission/privacy flow. The assistant does not perform unrestricted internet research during Search.

## 10. Social proof, medallions and source recommendations

Keep the initial cards and trusted facts comparable to classic Search; do not change social-proof semantics at the same time as the conversation experiment. Compact cards, organizer/venue medallions, editorial proof, aggregate behavior and richer map previews can be tested separately.

«Что рекомендует Культурная чайка?» is a future evidence-backed source filter, not an impersonation or invented taste profile. Mention, advertisement and endorsement differ. Reuse canonical source/medallion evidence; absent recommendations are reported as absent. No model-generated popularity or endorsement.

## 11. Product experiment sequence

Experiment A measures rescue of unsuccessful ordinary Search, preserving a classic fallback, comparable corpus/revisions, explicit found/not-found outcome and the shared dynamic resource policy. Do not introduce an arbitrary tiny turn cap in the name of the experiment.

Experiment B tests opt-in hybrid discovery and whether users understand both modes search the same catalogue. Experiment C may compare primary presentation while keeping retrieval, card facts and downstream actions comparable. Experiment D separately evaluates richer location/identity blocks.

The owner's answer timeline is the intended conversational presentation; these experiments measure its value rather than treating every new input as a success metric.

## 12. Measurement

Reuse `event_value_reached_rate`, `event_intent_action_rate`, `cards_to_first_event_value`, `time_to_first_event_value`, explicit `matched`/`missed` and abandonment.

Add only useful bounded metrics: rescue success, turns to value, clarification without result, refinement acceptance, return-to-older-section/refine action, assistant-to-classic switch, stale/duplicate answer applications, lost utterances, unintended scroll jumps, schema/factual fallback, calls/tokens/audio seconds per successful discovery, queue wait and p50/p95 result latency, and aggregate egress.

Adaptive policy evidence distinguishes provider exhaustion, product allowance and temporary concurrency pressure. Track useful capacity left idle while eligible users are blocked, quota oscillation and new-user starvation, without logging raw questions, voice or personal IDs in public analytics. Do not optimize chat count or synthetically low provider usage at the expense of successful discovery.

## 13. Acceptance thresholds

Choose numeric latency, quality and load thresholds after baseline measurement; do not invent observed SLAs. Show materially higher discovery/intent success, comparable success with less time/scanning, or meaningful rescue of failed sessions, while meeting factuality, privacy, quota and accessibility guards.

Hard constraints have zero tolerated fabricated Event/location/map actions or silently applied stale results in acceptance. A large synthetic corpus alone does not prove human usability. Skipped live testing because of budget or policy is not quality PASS.

## 14. MCP and storage boundaries

External MCP adapters use the same bounded domain services and canonical IDs/revisions; no arbitrary SQL/network/provider tools. Durable actions use separate permissions and existing idempotency/consent paths. Neither my-data-hub availability nor a cached manifest substitutes for an authoritative provider admission.

Do not create another history vector index, event catalogue copy, user-profile owner, email/subscription system or quota ledger. First-party Search does not call a coding agent. Shared transport rules for safe reads, selected-once cost-bearing calls and proven idempotent replay remain applicable.

## 15. Explicit non-goals

No general-purpose chatbot, autonomous purchase/registration, permanent raw conversation history by default, continuous background microphone guarantee, unrestricted internet research, long multi-agent planning loop, silent provider escalation, invented route times or popularity. These exclusions do not prohibit a useful grounded textual answer, multiple coordinated islands, a generously budgeted conversation or browsing previous answers within retention limits.

## 16. Delivery and executable-test plan

This is planned coverage, not implemented/passed tests. Extend the [existing scenario registry](../../../testing/static-site-autotest-scenarios.v1.yml), Search harness and `.github/workflows/ci.yaml` / `search-production-health.yml`; do not make a second executable registry out of this table.

| Required case | Deterministic expectation | Evidence lane |
|---|---|---|
| Distinct allowances | A valid voice operation uses its voice policy, not a accidentally inherited smaller typed-search cap; both share provider accounting | Unit + real test-DB integration |
| Spare capacity and pressure | Elastic allowance can increase within ceilings; pressure reduces redundant/batch work before unnecessary rejection; recovery restores allowance without oscillation | Fake-clock/load-policy unit + integration |
| Fairness and policy change | No new-user starvation, no post-admission utterance loss or retroactive product charge; authoritative missing limiter prevents provider dispatch | Integration |
| Provider attempt accounting | Duplicates and audio chunks are not duplicate user operations; every actual sent attempt remains accounted after cancel/timeout | Existing limiter tests + integration |
| Completed refinement | S1 remains unchanged; S2 appends once with correct parent, title, query, answer and event set | Reducer + browser |
| Dogon before completion | U1/U2/U3 preserve ordered meaning in one current draft; late completion cannot append obsolete/duplicate sections | Race/fault matrix |
| Logical parent selection | «Из них» includes eligible results beyond the first rendered page, never silently broadens the parent or applies a different catalogue snapshot | Retrieval fixture + integration |
| Old-section refinement | Scroll alone changes no query base; explicit «Уточнить» uses S1 and appends S3 with a visible parent link | Reducer + browser |
| Referenced event | «Второе» binds to the correct result_set_id even when a new list finishes | Integration + browser |
| Sticky succession | Down/up scrolling activates only the appropriate section title below measured chrome; no accumulating headers or duplicated accessible heading | Browser geometry + screenshots |
| New-answer scroll | Explicit submit reveals the new section heading once; manual reading cancels auto-follow; late image/text loads and old-section pagination preserve anchor | Browser + slow-network fixtures |
| Island composition | Header/context/nav/voice coexist without overlaps, full-screen invisible hit planes or inaccessible stop/CTA controls | 1440×900 and 390×844 + smaller/zoom/keyboard cases |
| Keyboard/dialog/motion | Existing key editing/navigation, focus, safe areas, modal precedence and reduced motion remain correct | Browser + relevant mobile adapters |
| Grounded short answer | An address/transport answer can have zero cards without falsely reporting no results; unknown facts are explicit; long content expands accessibly | Schema/fixtures + live quality subset |
| History, identity and freshness | Bounded storage/DOM, parent cursor isolation, TTL/resume, logout/delete, current action validation and no cross-user data leak | Real test DB + browser |
| Same-corpus A=S=P | Declared flow/sticky/composer/error variants share actual SoT sources; visual evidence does not claim runtime-network proof | Existing projection + owner-review gates |

### Implementation ownership

**ChatGPT / GitHub:** finalize schemas and copy; build pure intent/timeline/admission-decision modules and deterministic fixtures/tests; extend existing registry; prepare Qwen fixture/oracle data through the existing CPU baseline and allowed resource admission. This does not require a coding agent to repeat the analysis.

**Coding agent:** integrate the real endpoint, existing quota operations and SQL ownership/CAS/retention; wire MediaRecorder/VAD and operation recovery; connect the actual shared shell/SoT APIs; run test-DB/browser/race/geometry and relevant native-mobile checks; stage deployment and bounded live quality. No duplicate Search/limiter/shell or early rewrite of shared normalization foundations.

**Current UI owners in #621:** incorporate the system-level island roles and answer-heading/composer states through existing family ownership; preserve common card/grid/media behavior. Published review builds still use the single Kaggle StaticSiteBuilder; local focused builds get no owner-review or A=S=P completion credit.

Qwen audio is generated once per versioned fixture batch and replayed in CI, not synthesized on every PR. Use dialogue fixtures covering «бесплатно» → «из них на побережье», an old-section branch and a transport question, with independently defined expected meaning. Add actual-browser fake-microphone capture to provider-stub PR tests; only the protected allowed real-provider lane measures ASR quality. Keep the existing human/real-device holdout and shared limiter. No new Qwen or live provider run is authorized or claimed by this document.

## 17. Closure criterion

The assistant is ready for a bounded user experiment when it preserves classic Search, uses canonical facts/actions and existing identity/transport/limiter ownership, accepts ordered additions without lost speech, retains navigable committed answer sections, makes the refinement target explicit, coordinates island geometry through the shared system, provides grounded short or expandable answers, and passes the applicable automated and live gates. Resource economy is measured per successful discovery; an arbitrary tiny conversation limit or artificially one-call-only path is not a substitute for a useful product.
