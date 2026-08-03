# Шаблон сводного отчёта о тестировании персонализации

> **Статус:** предварительный формат отчёта для longitudinal E2E, offline evaluator и factor-ablation прогонов.  
> **Связанный сценарный пакет:** [`longitudinal-e2e-personalization.md`](longitudinal-e2e-personalization.md).  
> **Назначение:** показывать, как модель персонализации должна была работать, как она фактически сработала и что нужно исправить в следующей итерации.

## 1. Executive verdict

```text
Run: <workflow_run_id>
Repo SHA: <sha>
Catalog: <catalog_snapshot>
Model: <model_version>
Surface policy: <surface_policy_version>
Persona set: <persona_set_version>
Verdict: PASS | WARN | FAIL | BLOCKED
Decision: ship_to_shadow | tune_model | fix_contract | block_release
```

Короткий ответ:

```text
Модель проходит hard invariants, но не закрывает primary metric для
family_weekend_curator и price_sensitive_free_seeker. Следующая итерация:
перекалибровать price/distance constraints и exploration pool; rollout не
расширять.
```

## 2. Главный scorecard

| Metric | Target | Actual | Status |
|---|---:|---:|---|
| `first_relevant_within_30_rate` | `>= 0.95` | `<value>` | PASS/WARN/FAIL |
| `cards_to_first_relevant_p95` | `<= 30` | `<value>` | PASS/WARN/FAIL |
| `NDCG@10` vs baseline | `>= baseline - tolerance` | `<value>` | PASS/WARN/FAIL |
| `diversity@30` | `>= threshold` | `<value>` | PASS/WARN/FAIL |
| `hard_invariant_violations` | `0` | `<value>` | PASS/FAIL |
| `ordinary_local_rerank_network_requests` | `0` | `<value>` | PASS/FAIL |
| `moved_visible_cards` | `0` | `<value>` | PASS/FAIL |
| `sensitive_facet_materialized` | `0` | `<value>` | PASS/FAIL |

Primary chart:

```text
Persona                 within_30   p95_cards   verdict
explorer_omnivore          0.97        22        PASS
family_weekend_curator     0.91        38        WARN
science_learning_local     0.98        18        PASS
...
```

## 3. Persona scorecards

Каждая persona получает отдельную карточку:

```yaml
persona: family_weekend_curator
verdict: WARN
surfaces:
  for_me:
    first_relevant_within_30_rate: 0.91
    cards_to_first_relevant_p95: 38
    ndcg_at_10: 0.62
    baseline_ndcg_at_10: 0.58
  thematic_children:
    first_relevant_within_30_rate: 0.96
    cards_to_first_relevant_p95: 24
primary_failure:
  - price_distance_constraint_over_penalized_events_near_city_edge
next_iteration:
  - split distance constraint by transport context
  - add family-compatible exploration pool
  - reduce venue fatigue for weekend repeat venues
```

Required fields:

- `persona key`;
- expected interest graph;
- top positive facets before/after;
- top constraints before/after;
- first relevant rank timeline;
- hard invariant status;
- surprising failures;
- nearest fix hypothesis.

## 4. Profile timeline

Показывать развитие профиля по визитам:

| Visit | Time bucket | Strong actions | Projection revision | Top short facets | Top mid facets | Top long facets | Notes |
|---:|---|---:|---:|---|---|---|---|
| 0 | cold | 0 | none | unknown | unknown | unknown | static baseline |
| 1 | session | 3 | 0 | science, lecture | unknown | unknown | local overlay only |
| 3 | day 7 | 8 | 2 | science, museum | science | unknown | short→mid emerging |
| 6 | day 30 | 15 | 4 | science, jazz | science, jazz | unknown | stable mixed interests |
| 9 | month 7 | 24 | 9 | science | science | science | long evidence only from repeated trusted signals |

Графики:

- line chart: `cards_to_first_relevant` по визитам;
- stacked area: session/short/mid/long facet contribution;
- timeline: projection revision and materialization jobs;
- markers: reset/link/fault/materializer runs.

## 5. Static → personalized transformation

Для каждой surface:

```yaml
surface: for_me
static_top_10: [8120, 8122, 8110, ...]
after_local_overlay_top_10: [8122, 8101, 8120, ...]
after_projection_top_10: [8101, 8122, 8118, ...]
frozen_prefix_ids: [8120, 8122, 8110]
moved_visible_count: 0
moved_tail_ids: [8101, 8118, 8099]
served_dom_matches_evidence: true
```

Обязательные визуальные блоки отчёта:

1. static order vs actual personalized order;
2. rank deltas для relevant и irrelevant событий;
3. highlighted first relevant event;
4. frozen prefix proof;
5. screenshot before/after только если state стабилен и безопасен.

Calendar primary report:

```text
calendar_primary: identity order preserved; exact_hidden_removed=2; reranked=0
```

## 6. Factor ablation

Показывать вклад факторов не как «магическую оценку», а как counterfactual:

| Factor disabled | Δ NDCG@10 | Δ first relevant rank | Regression | Interpretation |
|---|---:|---:|---|---|
| session_overlay | -0.08 | +4 | no | session помогает свежему intent |
| short_horizon | -0.12 | +7 | no | short отвечает за быстрый learning |
| mid_horizon | -0.05 | +2 | no | mid стабилизирует повторные интересы |
| long_horizon | -0.01 | 0 | no | long ещё не накоплен |
| price_constraint | +0.02 | -1 | yes | constraint слишком агрессивен |
| exploration | -0.03 | +1 | diversity fail | exploration нужен для anti-bubble |

Каждая строка должна иметь:

- affected personas;
- affected surfaces;
- reason codes;
- whether expected;
- proposed model/config change.

## 7. Hard invariant ledger

```text
exact_hide_resurrection: 0
calendar_chronology_mutations: 0
eligibility_violations: 0
visible_card_moves: 0
served_dom_mismatch: 0
pre_activation_server_writes: 0
sensitive_facets_materialized: 0
campaign_noise_trained_organic: 0
cross_context_leaks: 0
```

Любое значение выше нуля делает итог `FAIL`, даже если relevance-метрики выросли.

## 8. Storage and transport health

| Metric | Target | Actual | Verdict |
|---|---:|---:|---|
| localStorage p95 bytes | `<= 24 KiB target / <= 48 KiB hard` | `<value>` | PASS/WARN/FAIL |
| outbox max depth | `<= 16` | `<value>` | PASS/WARN/FAIL |
| expired outbox actions | `0` normal, explained in fault tests | `<value>` | PASS/WARN/FAIL |
| projection payload p95 | `<= 8 KiB` | `<value>` | PASS/WARN/FAIL |
| refresh requests per ordinary page view | `0` | `<value>` | PASS/FAIL |
| materialization jobs per subject/day | bounded/coalesced | `<value>` | PASS/WARN/FAIL |
| direct/relay false ACK | `0` | `<value>` | FAIL if > 0 |

## 9. Failure taxonomy

Failure classes:

- `MODEL_RELEVANCE`: first relevant too deep, low NDCG/MRR.
- `MODEL_OVERFIT`: diversity/coverage collapse, persona dominance.
- `MODEL_SEMANTIC`: wrong interpretation of hide/constraint/share/CTA.
- `SURFACE_POLICY`: calendar/search/popular/related main meaning violated.
- `PRESENTER`: visible/focused card moved, DOM/evidence mismatch.
- `MATERIALIZER`: wrong horizon, wrong provenance, recompute storm.
- `TRANSPORT`: lost/duplicated/ambiguous action handling.
- `STORAGE`: budget exceeded, bad eviction, cross-tab leak.
- `LEGAL_PRIVACY`: pre-activation write, sensitive facet, raw data leak.
- `TEST_DATA`: bad judgement, insufficient supply, stale catalog.

Report должен указывать первый слой расхождения, а не сразу предлагать менять
weights.

## 10. Next-iteration plan

Каждый WARN/FAIL завершается конкретным планом:

```yaml
next_iteration:
  owner: personalization-model
  target_pr: P13N-05-model-calibration-v2
  affected_personas: [family_weekend_curator, price_sensitive_free_seeker]
  affected_surfaces: [for_me, thematic_children]
  change_type: model_config | feature_extraction | surface_policy | fixture_fix
  hypothesis: distance penalty слишком общий и должен учитывать транспорт/город
  expected_metric_delta:
    first_relevant_within_30_rate: +0.04
    diversity_at_30: no regression
  guardrails:
    - calendar_chronology_mutations == 0
    - exact_hide_resurrection == 0
    - constraint_precision improves
  rollback_condition: NDCG@10 falls below baseline - tolerance for 2 personas
```

## 11. Minimum machine-readable summary

`metrics.json` должен содержать:

```json
{
  "schema_version": "p13n-longitudinal-report-v1",
  "verdict": "WARN",
  "run": {
    "repo_sha": "...",
    "workflow_run_id": 0,
    "catalog_snapshot": "...",
    "model_version": "...",
    "surface_policy_version": "...",
    "persona_set_version": "..."
  },
  "primary": {
    "first_relevant_within_30_rate": 0.94,
    "cards_to_first_relevant_p95": 34
  },
  "personas": [],
  "hard_invariants": {},
  "storage": {},
  "transport": {},
  "factor_ablation": {},
  "next_iteration": []
}
```

## 12. Human-readable report layout

Рекомендуемый `report.html`:

1. one-screen verdict;
2. primary metric strip;
3. persona heatmap;
4. profile timeline chart;
5. DOM transformation panel;
6. factor ablation chart;
7. hard invariant table;
8. storage/transport health;
9. failures and next iteration backlog;
10. artifact links and reproducibility manifest.

Markdown-версия должна быть достаточно полной для PR review без открытия HTML.
