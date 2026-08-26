# Agent-assisted event discovery — product, architecture and experiment plan

> **Status:** planned product experiment; not an authorization to replace `/poisk/`, expose unrestricted MCP tools, mutate production UI or enable a new provider caller.  
> **Updated:** 26 August 2026.  
> **Depends on:** the canonical Smart Search contract in [`README.md`](README.md) and the planned [location directory, coordinates and map links](../../location-directory/README.md).

## 1. Product hypothesis

A conversational assistant may help users who can describe a wish but do not know which filters, categories or events to choose.

The hypothesis is **not** «chat is always better than a feed». The planned product is a second discovery mode over the same trusted Event corpus:

```text
classic Search/listing
+ optional conversational refinement
+ the same canonical Event facts and actions
```

Expected advantages:

- natural multi-constraint requests;
- progressive refinements such as «только бесплатно», «ближе к центру», «не концерт»;
- short explanations of why an event matches;
- comparison of a few alternatives and trade-offs;
- recovery after a zero-result or explicitly unsuccessful search;
- safe answers to «где это?» using canonical location facts and map actions.

Expected risks:

- a chat may hide catalogue breadth and slow down visual scanning;
- clarification may become an exhausting questionnaire;
- generated prose may create false confidence;
- extra model turns may consume quota without improving discovery;
- a new visual language may fragment the existing Search and EventCard system.

The likely winning form is a hybrid: a short assistant answer, a small set of canonical event cards, refinement chips and an always-available switch to the full listing.

## 2. Initial rollout decision: rescue first

The first production experiment should not make the assistant the default Search surface.

Initial entry points:

```text
zero exact results
OR explicit «Нет, не нашёл»
OR repeated query reformulation
OR explicit «Помочь подобрать» action
→ agent-assisted refinement
```

This isolates incremental value: can the assistant rescue a session in which ordinary Search has already underperformed?

Only after rescue value is proven should the product test an opt-in assistant tab or randomized primary presentation.

## 3. Architecture decision

### First-party website

The website should call its own backend/domain services directly. MCP is not required in the first-party critical path.

```text
browser
→ authenticated bounded Search operation
→ existing embedding/pgvector retrieval
→ bounded Gemini Flash-Lite verifier/presenter
→ typed UI response
→ canonical EventCard/location rendering
```

The preferred first version is not an open-ended autonomous agent. It is a bounded orchestration in which the host owns retrieval and tool execution.

### External agents

The same domain operations may later be exposed through MCP:

```text
shared domain service
├── direct first-party API/operation
├── Gemini function declaration adapter
└── MCP adapter for ChatGPT/Claude/Gemini/other clients
```

This prevents a second Search implementation and a second location database.

Candidate read-only operations:

```text
events.search
events.get
events.compare
locations.search
locations.get
locations.resolve_for_event
```

Feedback/save/calendar actions remain separate typed product commands with existing authentication, idempotency and approval rules.

## 4. Model role and quota economy

`gemini-3.5-flash-lite` is the planned first candidate because the task is bounded:

1. interpret the user's current intent and prior refinement state;
2. inspect a server-provided candidate set;
3. select only allowed Event IDs;
4. produce short grounded explanations;
5. propose a few refinement actions;
6. return one closed structured response.

Target budget:

```text
one user turn
≈ one Lite model call
```

Do not create a default chain of separate intent-parser, tool-selector, verifier and prose-writer calls. The assistant should replace/extend the current verifier/presenter stage where possible, not duplicate it.

A stronger Flash model may be reserved for a separately approved complex-planning scenario, for example a multi-event day plan with time and transport constraints. Ordinary event selection must not silently escalate.

## 5. Search-first, clarify-second interaction

The assistant should normally show useful candidates before asking another question.

Preferred flow:

```text
interpret known constraints
→ search immediately
→ show 3–5 candidates
→ offer optional refinements
→ ask one direct clarification only when materially necessary
```

Avoid a sequence of mandatory questions about date, budget, companions, genre and location before any result appears.

A clarification is justified when different interpretations would produce substantially different candidate sets, for example the child's age range or which coastal town the user means.

## 6. Compact conversation state

Do not depend on a permanently stored raw transcript. Maintain a small first-party intent state:

```yaml
query_summary: bounded text
when:
  date_range: null
  weekdays: []
  time_of_day: []
where:
  locality_ids: []
  location_ids: []
  area_labels: []
with_whom: []
audience: []
budget: null
formats: []
themes: []
moods: []
accessibility_needs: []
exclusions: []
shown_event_ids: []
rejected_event_ids: []
turn_no: integer
```

The exact schema is an implementation candidate. Required properties are bounded size, explicit provenance from user input/typed actions, and no silent persistence of precise user location.

## 7. Typed response contract

The model must not return arbitrary HTML, model-authored URLs or factual card fields. It returns closed semantic blocks; the host renders them from trusted snapshots.

Illustrative response:

```yaml
answer_blocks:
  - type: text
    text: "Нашёл три спокойных варианта на субботний вечер."
  - type: event_group
    title: "Лучше всего подходят"
    event_ids: [event_123, event_456, event_789]
  - type: event_annotation
    event_id: event_123
    match_reason_codes: [time_match, mood_match, location_match]
    explanation: "Начинается вечером и проходит в центре."
  - type: location
    location_id: location_42
    show_address: true
    map_action_ids: [map_action_yandex, map_action_2gis]
  - type: suggested_replies
    actions:
      - id: refine_free
        label: "Только бесплатно"
        intent_patch: {budget: free}
      - id: refine_center
        label: "Ближе к центру"
        intent_patch: {area: kaliningrad_center}
      - id: show_unusual
        label: "Более необычное"
        intent_patch: {novelty: higher}
```

Server validation:

- every Event ID must belong to the retrieved allowlist;
- every location/map action must belong to the trusted location projection;
- title, date, price, address, image, URL, social proof and medallions come only from canonical snapshots/resolvers;
- explanations may be rejected when they contradict facts or use unsupported claims;
- malformed output falls back to ordinary Search results;
- no model-generated Markdown link is treated as a navigation action.

## 8. UI composition

The assistant response is not a long ChatGPT-like transcript with events hidden in paragraphs.

Recommended hierarchy:

```text
short assistant summary
→ interpreted-constraint chips
→ 3–5 canonical or approved compact EventCards
→ optional grounded match reasons
→ location/map block when relevant
→ suggested refinement chips
→ «Показать всю выдачу» / «Продолжить обычным поиском»
```

Planned semantic UI resources:

- conversation/turn container;
- user intent message;
- assistant summary block;
- interpreted-constraint chip;
- event reference inside text;
- compact agent-result EventCard variant or canonical full EventCard reuse;
- result-group heading;
- grounded match-reason row;
- suggested-reply/refinement control;
- location/address/map-actions block;
- loading/searching state;
- clarification state;
- zero/degraded/error/quota/auth states;
- explicit switch to the classic listing.

A dedicated design-system/product checklist is maintained in `lovekgd-design-system`; no component or pattern is promoted merely because this plan exists.

## 9. Location and map behavior

The assistant can answer «где это?» only through the canonical location directory.

Permitted response:

```text
Событие проходит в Калининградском областном историко-художественном музее,
Клиническая, 21.
[Открыть в Яндекс Картах] [Открыть в 2ГИС]
```

The displayed name/address and map actions are server-provided typed data.

Future distance queries require explicit semantics:

- distance from a named canonical place;
- straight-line distance;
- route distance;
- public-transport/walking/driving time.

These values are not interchangeable. No travel time is generated without an approved routing/data service. «Рядом со мной» requires a separate explicit permission and privacy flow.

## 10. Social proof and medallions

Do not change conversation mode, card composition, social proof and medallions in one initial experiment. Otherwise the source of any improvement is unknowable.

First experiment:

- same Event corpus;
- same trusted card facts and actions;
- as close as possible to the same card design;
- only the conversational refinement/explanation layer differs.

Later isolated experiments may test:

- a compact agent-result card;
- one structured organizer/venue medallion;
- editorial proof;
- aggregated behavioral social proof;
- map/location preview.

Every proof signal must be server-grounded and typed. The model must not invent popularity, editorial endorsement, accessibility, audience suitability or organizer identity.

## 11. Product experiment sequence

### Experiment A — rescue assistant

Control: ordinary Search recovery/fallback.  
Treatment: after zero/missed, offer conversational refinement over the same corpus.

- [ ] define eligibility and deterministic assignment;
- [ ] preserve an immediate classic-list fallback;
- [ ] cap turns/model calls;
- [ ] use the same Event revision and retrieval evidence;
- [ ] record explicit found/not-found verdict;
- [ ] stop rollout on factual or navigation errors.

### Experiment B — opt-in hybrid

```text
Search
├── Лента
└── Помочь подобрать
```

Measure discoverability, voluntary adoption, successful return to the feed and whether users understand that both modes search the same catalogue.

### Experiment C — randomized primary presentation

Control:

```text
natural-language query
→ ordinary EventCard listing
```

Treatment:

```text
natural-language query
→ short assistant summary
→ top 3–5 cards
→ refinement chips
→ full listing available
```

Card facts, retrieval pool, catalogue revision and downstream event actions must remain comparable.

### Experiment D — richer location/identity blocks

Only after conversational value is demonstrated, separately test map actions, compact card variants, medallions and social proof.

## 12. Measurement

Primary outcomes reuse the common site analytics contract:

- `event_value_reached_rate`;
- `event_intent_action_rate`;
- `cards_to_first_event_value`;
- `time_to_first_event_value`;
- explicit `matched` / `missed` verdict;
- Search abandonment.

Assistant-specific measures:

- `assistant_rescue_success_rate`;
- turns to first useful event/action;
- suggested-reply acceptance;
- free-text refinement rate;
- clarification-without-result rate;
- assistant→classic switch rate;
- event-card open/save/calendar/share rate by mode;
- model calls and tokens per successful discovery;
- malformed-schema/fallback rate;
- unsupported factual-claim rate;
- invented Event/location/map-action count, target `0`;
- p50/p95 first-result and turn latency.

Guardrails:

- no degradation of catalogue diversity;
- no increase in forced authentication beyond the accepted Search contract;
- no raw query/transcript in general analytics;
- no persistent precise user-location collection;
- no silent provider retry or unaccounted quota use;
- no false success when retrieval/model/location data are degraded.

## 13. Acceptance thresholds

Exact numeric thresholds should be fixed after measuring the current Search baseline. A treatment should not become default merely because users send messages.

The product must demonstrate at least one material improvement:

- higher successful event discovery/intent action; or
- comparable success with materially fewer cards or less time;
- rescue of a meaningful share of previously failed sessions.

It must also stay within agreed latency, quota, factuality and abandonment guardrails.

## 14. MCP boundary

MCP is useful for external agent access to the same data and actions, but it does not decide the first-party UI architecture.

Requirements:

- reuse canonical Event and location services;
- exact read-only tool allowlist for discovery;
- typed bounded results with canonical IDs and revisions;
- no arbitrary SQL/provider/network tool exposed to the model;
- no model-authored URLs;
- separate authenticated commands for durable actions;
- idempotency and approval for writes;
- source/revision/degraded-mode evidence in results;
- consistent behavior whether the consumer is the site, Gemini function calling or MCP.

## 15. Explicit non-goals for MVP

- no general-purpose chatbot;
- no unrestricted internet research during user Search;
- no autonomous purchase/registration;
- no long multi-agent planning loop;
- no permanent raw conversation history by default;
- no second Event index or second quota ledger;
- no dependency on Remote MCP for first-party availability;
- no new medallion/social-proof semantics in the same first experiment;
- no inferred coordinates, route times or popularity.

## 16. Delivery checklist

### Product/data

- [ ] approve rescue-first hypothesis and experiment population;
- [ ] define intent-state and typed-response schemas;
- [ ] define model call/turn/latency budgets;
- [ ] link the canonical location directory and coverage gate;
- [ ] define fallback and factual-claim validation;
- [ ] prepare comparable fixtures and baseline metrics.

### Architecture

- [ ] reuse existing Search/auth/transport/limiter/audit boundaries;
- [ ] decide direct orchestration vs function calling without putting MCP in the site critical path;
- [ ] expose only bounded domain operations;
- [ ] enforce one Lite call per ordinary turn where possible;
- [ ] include catalogue/corpus/location revisions in evidence;
- [ ] prove ordinary Search remains available on model failure.

### Product design

- [ ] map full journey and all states;
- [ ] prototype rescue entry and hybrid mode switch;
- [ ] design text/event/chip/location blocks using existing visual language;
- [ ] prove mobile/desktop/keyboard/screen-reader behavior;
- [ ] keep full listing and downstream Event actions reachable;
- [ ] run owner review on real event/location fixtures.

### Validation

- [ ] schema/property tests reject foreign Event/location/action IDs;
- [ ] golden conversations cover ambiguous, zero, degraded and location questions;
- [ ] browser tests cover auth, quota, cancel, fallback and repeated refinement;
- [ ] factual audit proves zero invented Event/location/map facts;
- [ ] analytics distinguish exposure, use, rescue and outcome;
- [ ] staged rollout and rollback are defined.

## 17. Closure criterion

The agent-assisted mode is ready for a bounded user experiment only when it uses the existing Search and canonical location data, returns closed typed UI blocks, preserves an immediate classic-list fallback, demonstrates zero invented Event/location/map actions in acceptance, has a one-call-per-turn economic path, covers all critical interaction states, and measures incremental event discovery rather than chat activity.