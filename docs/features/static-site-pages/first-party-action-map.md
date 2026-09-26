# First-party карта действий: component-aware diagnostic contract

> **Статус:** принятый TO-BE product/architecture contract; active capture не включён.  
> **Дата исходного решения:** 9 августа 2026 года.  
> **Актуализация Product Atlas delivery:** 25 августа 2026 года.  
> **Scope:** static site, first-party product analytics, personalization context, reviewed product evidence and UI SoT.

## 1. Решение

Карта действий — временный диагностический слой. Она связывает presentation context и фактическое действие внутри versioned UI identity, но не становится автоматическим источником пользовательского профиля.

```text
presentation receipt
→ что, где, в какой позиции и какой версией было показано

component-aware diagnostic capture
→ что было реально exposed и какое semantic action произошло

bounded aggregation
→ факты, denominators, technical context и limitations

reviewed evidence
→ Product Atlas Git finding / decision
→ при необходимости отдельное изменение UI или signal policy
```

Запрещённый путь:

```text
coordinate / no click / hover / retry
→ automatic profile mutation or UI decision
```

## 2. Инварианты

1. `ACTION_MAP_BUILD=off` означает нулевую добавочную browser-cost: нет chunk, import, bootstrap, listeners, observers, timers, config request, IndexedDB work, payload fields или action-map-only HTML attributes.
2. Включение/выключение — static build and publish decision, не постоянный remote loader.
3. Карта читает только безопасный presentation context; raw profile, embeddings, private history и score breakdown недоступны.
4. Диагностические observations не активируют персонализацию, не создают facets и не меняют ranking.
5. Like, hide, favorite, calendar, share и CTA считаются из authoritative product state; карта не создаёт второй факт.
6. Raw page coordinates, trajectories, mouse trails, scroll stream, DOM snapshots, form values и Search text не собираются.
7. Геометрия разрешена только как coarse local bin внутри versioned component/semantic zone и только для зарегистрированной кампании.
8. Raw summaries имеют короткий TTL; долгоживущим становится reviewed aggregate evidence package без user identity.
9. Causal uplift допускается только для зарегистрированного randomized/holdout contract.
10. Product Atlas и Penpot не получают raw stream.

## 3. Связанные контуры

| Контур | Authority | Связь с картой |
|---|---|---|
| Product operation | authoritative action/current state | карта ссылается на receipt, но не дублирует факт |
| Personalization | activation, profile, model/rule versions, served list | read-only presentation context |
| Product analytics | consent, compact facts, TTL, aggregation, cost | принимает bounded summaries |
| Product evidence | measurement question, finding, decision | получает reviewed immutable package |
| UI SoT | archetype/region/component/state identity | даёт stable semantic context |

Односторонняя граница:

```text
personalization presentation → action map: allowed
action map → current profile/ranking: forbidden
```

Новый map-derived signal может появиться только через:

```text
reviewed finding
→ signal-policy proposal
→ privacy/meaning review
→ offline benchmark
→ randomized experiment
→ versioned core event/model contract
```

## 4. Telemetry layers

### Layer A — authoritative actions

Существуют независимо от action map:

- like / unlike;
- not interested / undo;
- favorite/save;
- calendar add;
- share/copy;
- CTA intent и terminal outcome;
- подтверждённая регистрация/attendance при доверенном source.

### Layer B — compact core analytics

По отдельному `product_analytics` consent:

- page/view context;
- semantic exposure;
- semantic action/outcome;
- sampled performance;
- bounded session summary.

### Layer C — temporary diagnostic capture

Только при:

```text
active build
AND eligible route
AND approved unexpired campaign
AND product_analytics consent
AND deterministic sample hit
AND known component/schema binding
```

Дополнительные поля:

- component/zone-local coarse bin;
- mapped/unmapped action classification;
- repeat bucket;
- expected/observed effect;
- optional latency/layout context;
- render/presentation reference;
- quality counters.

### Layer D — reviewed evidence

После кампании остаются:

- aggregates;
- sanitized representative render;
- page/component maps;
- denominator and coverage;
- facts, limitations and competing explanations;
- reviewed finding;
- owner decision;
- follow-up question.

## 5. Zero-cost OFF

OFF-build не содержит:

- action-map entry/chunk;
- static/dynamic import reference or modulepreload;
- inline bootstrap;
- config fetch;
- listeners/observers/timers/background tasks;
- action-map storage work;
- requests or payload fields;
- action-map-only DOM attributes.

Remote toggle не заменяет OFF, потому что loader/config check сам создаёт bytes, execution и failure path.

### Activate

```text
approved campaign manifest in Git
→ route-scoped active build
→ static publish
→ browser consent/sample/expiry gates
→ conditional capture module import
```

### Stop

```text
expiry or owner stop
→ ingest rejects new packets
→ module stops in cached/open active build
→ OFF build published
→ next navigation has no action-map assets
```

Мгновенный remote kill для любой открытой вкладки и абсолютный zero-cost OFF одновременно недостижимы. Принято: embedded expiry + server rejection + следующая OFF-публикация.

## 6. Component and action identity

```text
page_archetype_id
→ layout_contract_id
→ component_id + contract_version + state_key
→ component_instance_id
→ semantic_zone_id
→ semantic_action_id
```

CSS class, DOM path, visible text, Astro filename и Penpot layer name не являются analytics identity.

Component observability contract:

```yaml
component_id: announcements.event-card
contract_version: 5.0.0
state_key: portrait.compact.default
observability:
  action_map_eligible: true
  zones:
    media:
      role: content_open_target
      allowed_actions: [open_event]
    favorite:
      role: explicit_action
      allowed_actions: [favorite_toggle]
  actions:
    open_event:
      interaction_mode: single_shot
      expected_effect: route_change
    favorite_toggle:
      interaction_mode: toggle
      expected_effect: authoritative_state_ack
```

Unknown component/version/zone fails closed. Visual similarity alone never merges identities.

## 7. Runtime capture

Use:

- one delegated pointer/click listener per campaign root;
- `pointerType` as mouse/touch/pen;
- keyboard activation as semantic action without geometry;
- explicit action hooks for expected effect;
- one shared IntersectionObserver only where core exposure registry does not provide denominator;
- Page Visibility for bounded exposure completion;
- existing sampled performance collector where available.

Do not use by default:

- pointermove/mousemove/touchmove;
- scroll event stream;
- global MutationObserver;
- persistent hover/dwell tracker;
- full interaction sequence;
- polling;
- per-component listeners.

Observed effect vocabulary:

```text
effect_observed
effect_not_observed
effect_unknown
command_rejected
navigation_interrupted
technical_error
```

`dead click` and `rage` are human review interpretations, not primary facts.

Repeat fact:

```text
repeat_attempt_count_bucket = 1 | 2 | 3-4 | 5+
```

Interpretation depends on `interaction_mode`; repeatable controls are not evaluated with a universal rage threshold.

## 8. Local geometry

For an allowed pointer action:

```text
u = (clientX - zone.left) / zone.width
v = (clientY - zone.top) / zone.height
bin_x = floor(clamp(u, 0, 0.999) × 8)
bin_y = floor(clamp(v, 0, 0.999) × 8)
```

Server receives only the bin, never exact coordinates or trajectory. Default precision is `8×8`; any increase requires a new approved campaign and decision use.

## 9. Render context

Static context may include:

- release/build SHA;
- page family/archetype and revision;
- content/layout contract revision;
- viewport class;
- relevant state enums.

Dynamic/search/personalized context uses one opaque `render_context_id` and optional `presentation_receipt_id`. Sanitized manifest may include canonical object IDs, order/slot/rank, revisions, component/state IDs, image/crop contract, allowed filter IDs, presentation mode and experiment variant.

Forbidden: raw Search text, DOM/HTML/textContent, form values, arbitrary response JSON, raw profile/vector, tokens and full score breakdown.

## 10. Summary schema

```json
{
  "schema_version": 1,
  "campaign_id": "am-mobile-search-001",
  "view_id": "campaign-scoped-idempotency-id",
  "render_context_id": "rc_...",
  "presentation_receipt_id": "pr_...",
  "release_sha": "...",
  "page_family": "search",
  "page_revision": "...",
  "layout_id": "mobile-list-v3",
  "device_class": "mobile",
  "presentation_mode": "personalized",
  "exposures": [],
  "actions": [],
  "quality": {
    "dropped_observations": 0,
    "unmapped_observations": 0,
    "truncated": false
  }
}
```

Browser aggregates equal observations, removes exact timestamps/points after bucketing and destroys the short correlation buffer after summary creation.

## 11. Resource budgets and retention

| Resource | Initial limit |
|---|---:|
| OFF incremental transfer/execute/storage | `0` |
| default sample | `5%` eligible views |
| without new approval | `≤10%` |
| default duration | `72h` |
| hard campaign duration | `7d` |
| map batches | `≤2/session`, inside global `3` |
| map bytes | target `<4 KiB`, hard `<8 KiB/session` |
| observations before aggregation | `≤64/view` |
| geometry | `8×8` |
| exact raw point retention | `0` |
| local unsent TTL | `24h` or expiry |
| raw YDB summary TTL | `7d` |
| long-lived result | aggregate evidence package only |

Action map consumes the global weak-telemetry budget and is dropped before authoritative/core facts under pressure. Supabase raw map rows remain `0`; browser direct YDB writes remain forbidden.

## 12. Consent and privacy

Personalization activation and product analytics consent are separate.

- No analytics consent: product actions and allowed personalization still work.
- Analytics consent does not activate personalization.
- Before personalization activation, de-identified analytics may use only its own consent and cannot mutate a profile.

Forbidden fields include personal identity, auth material, full IP/User-Agent/referrer, raw Search query, DOM/text, form values, exact coordinates, trajectories, key sequences, raw profile/embeddings/facets and screenshots in the raw stream.

Sensitive-topic slices are aggregated only above an approved minimum. Initial visual publication minimum is `20` eligible exposed views per rendered slice; below it the status is `INSUFFICIENT_DATA` or the slice is withheld.

## 13. Campaign and measurement contract

One campaign has:

- one `MeasurementQuestion`;
- one `decision_use`;
- at most six primary metrics;
- fixed denominators/slices/stop conditions;
- immutable manifest.

Default metrics:

- eligible exposure;
- action-given-exposure;
- activation success;
- repeat-without-effect;
- technical-confounded share;
- unmapped-action rate.

Never infer automatically:

```text
hot = liked
cold = disliked
hover/dwell = attention
retry = anger
no click = negative preference
higher clicks = causal uplift
```

Required finding format:

```text
fact
→ plausible interpretation
→ competing explanations
→ limitations
→ decision or additional evidence
```

## 14. Personalization evaluation

The map may answer whether high-ranked cards reached viewport, whether affordances worked given exposure, whether favorite conflicts with card open, whether exploration/rescue items were visible and whether retries were technically confounded.

It cannot by itself answer why the user chose an event, satisfaction, attendance, stable taste or long-term uplift.

Position bias requires algorithmic rank, visual slot, viewport exposure, editorial pinning and exploration/rescue placement. Rank bands are registered before results are viewed.

Active capture must also prove instrumentation parity for INP/LCP/CLS, bundle/CPU/network and behavior. A performance regression stops the campaign.

## 15. Product Atlas and UI SoT delivery

Long-lived action-map output is a canonical analysis record in `events-bot-new`:

```text
MeasurementQuestion
→ ProductAnalyticsEvidencePackage
→ reviewed finding
→ owner decision
→ follow-up
```

The Product Atlas Git SoT stores the product IDs, finding, decision and immutable evidence refs. The design-system repository stores only foreign keys and exact archetype/region/component/state context.

After the Product Atlas Penpot entry gate, a separate explicit Penpot MCP task may materialize reviewed evidence into the separate Product Atlas file. MCP does not read raw summaries or production DB, does not interpret metrics and cannot create a finding from an overlay. Completion requires exact read-back and a Git receipt. Unknown native bindings remain `binding_pending`; UUIDs are never invented.

## 16. Campaign manifest

```yaml
campaign_id: am-mobile-search-001
status: approved
measurement_question: ...
decision_use: ...
build:
  enabled: true
  starts_at: ...
  expires_at: ...
  schema_sha256: ...
scope:
  routes: [/poisk/]
  page_archetypes: [archetype.search]
  layouts: [mobile-list-v3]
  components: [announcements.event-card@5]
  zones: [media, body, favorite]
  devices: [mobile]
sampling:
  deterministic_key: campaign_view
  rate: 0.05
signals:
  exposure: true
  semantic_action: true
  local_bin: 8x8
  expected_effect: true
  repeat_bucket: true
  hover_dwell: false
  mouse_trail: false
  raw_scroll: false
limits:
  max_actions_per_view: 64
  target_session_bytes: 4096
  hard_session_bytes: 8192
  max_batches_per_session: 2
retention:
  local_ttl_hours: 24
  raw_ydb_ttl_days: 7
stop_conditions:
  - expiry
  - owner_stop
  - privacy_violation
  - performance_regression
  - budget_shedding
  - schema_mismatch
```

## 17. Release gates

### OFF proof — mandatory first

- no chunk/entry/import/modulepreload;
- no action-map-only attributes;
- zero requests/payload fields;
- zero listeners/observers/timers;
- zero action-map storage;
- zero incremental bytes except explained generic build metadata;
- ordinary navigation works with analytics route blocked.

### Active proof

- consent/route/sample/expiry fail closed;
- unknown identity fails closed;
- authoritative action is not duplicated;
- zero profile/rank mutation;
- prohibited-field scanner passes;
- budgets/TTL/idempotency pass;
- YDB outage does not break product action;
- active/control performance parity passes;
- low-sample slices are hidden;
- cached active build stops after embedded expiry;
- new navigation after OFF publication receives no map assets.

## 18. Rollout

```text
AM-0 contracts
→ AM-1 zero-cost OFF proof
→ AM-2 bounded static pilot
→ AM-3 presentation receipt / registered evaluation
→ AM-4 reviewed evidence, decision and OFF republish
```

Active capture is optional and does not block public static-site release. Without a registered question and decision use, the correct state is OFF / `not_applicable`.

## 19. Final boundary

The minimum correct system is:

```text
authoritative semantic actions
+ read-only presentation context
+ temporary component-local diagnostics
+ client aggregation
+ short raw TTL
+ reviewed evidence package
+ explicit owner decision
```

For the static site, OFF is implemented by absence of the collector from the build, not by a condition inside a permanently loaded collector.