# Product analysis records

> **Статус:** рабочий контракт для целевых продуктовых анализов.  
> **Источник аналитики:** проверяемые запросы к БД/экспортам, документации и production evidence.  
> **Не является:** automatic dashboard, raw-data store или заменой статистического runtime.

## Назначение

Продуктовый анализ выполняется по конкретному вопросу владельца и сохраняется как versioned Markdown record:

```text
вопрос
→ источники и data cutoff
→ запрос / выборка / evidence
→ анализ
→ findings и неопределённость
→ варианты решения
→ owner decision
→ Product Atlas Git model/projection
```

Product Atlas использует только сохранённые и проверяемые analysis records. Ответ в чате без Git record и provenance не становится product truth.

## Record contract

```yaml
analysis_id:
title:
status: draft | reviewed | accepted | superseded
created_at:
analyst: human | chatgpt | mixed
question:
scope:
data_cutoff:
source_refs: []
query_or_export_refs: []
release_identity:
method:
linked_job_ids: []
linked_outcome_ids: []
linked_journey_ids: []
linked_capability_ids: []
linked_scenario_ids: []
findings: []
uncertainties: []
product_problem_ids: []
options: []
decision_refs: []
```

Обязательные разделы:

1. вопрос и decision use;
2. источники и точный срез;
3. метод и определения;
4. факты;
5. интерпретация;
6. ограничения и недостающие данные;
7. влияние на Jobs/outcomes/capabilities;
8. варианты;
9. решение или `decision_required`;
10. follow-up evidence.

## Правила достоверности

1. Факт, интерпретация и гипотеза записываются отдельно.
2. Raw count без denominator не используется для продуктового вывода.
3. Отсутствие данных — `unknown` или `insufficient_data`, а не PASS.
4. Before/after не называется причинным эффектом без соответствующего design.
5. Release, environment, population и exclusions указываются явно.
6. Для export сохраняются manifest/hash/run ID, но не raw sensitive data.
7. Противоречащие findings не усредняются молча.
8. Более поздний analysis supersedes ранний только по явно указанным вопросам.
9. UI, код, release и runtime health не доказывают user/owner outcome автоматически.

## Action-map evidence package

Для `action_map_diagnostic` canonical analysis record является единственной точкой принятия результата. Допустим один immutable reviewed `ProductAnalyticsEvidencePackage`, но не raw stream или live dashboard.

```text
MeasurementQuestion
→ evidence
→ finding
→ decision
→ follow-up
```

Минимальный contract:

```yaml
evidence_id: pae-action-map-...
package_schema_version: 1
analysis_id: ...
status: reviewed
measurement_question: ...
decision_use: ...

scope:
  campaign_id: ...
  date_from: ...
  date_to: ...
  release_sha: ...
  page_archetype: ...
  layout_contract: ...
  component_contract: ...
  model_version: ...
  experiment_id: ...

quality:
  eligible_views: ...
  captured_views: ...
  denominator_definition: ...
  delivery_coverage: ...
  unmapped_rate: ...
  dropped_count: ...
  minimum_sample: ...
  performance_parity: PASS | FAIL | UNKNOWN

facts: []
limitations: []
competing_explanations: []
finding:
  status: accepted | rejected | insufficient-data
  text: ...
options: []
decision:
  owner: ...
  outcome: ship | change | instrument-better | stop | pending
follow_up:
  measurement_question: ...
  due_or_stop_rule: ...
artifacts:
  page_map: ...
  component_map: ...
  representative_render: ...
  aggregate_snapshot: ...
provenance:
  campaign_manifest_sha256: ...
  schema_sha256: ...
  aggregate_or_export_sha256: ...
  generated_at: ...
  reviewed_at: ...
  reviewers: []
  package_sha256: ...
```

`facts`, limitations, quality/coverage и immutable provenance обязательны даже при `insufficient-data`. Исправление создаёт новый `evidence_id` и explicit supersession; старый package не переписывается.

Hotspot, cold area, dead/repeat candidate или другая визуальная особенность не становится автоматически finding, product problem, UI gap, component change или profile signal. Сначала фиксируются fact, competing explanations, limitations и quality state; finding требует review, decision — явного owner outcome.

## Privacy boundary

В Git не попадают:

- ПДн;
- raw feedback text или screenshots без отдельного защищённого основания;
- raw Search queries;
- session-level clickstream;
- credentials и private access links;
- production DB exports.

Допустимы агрегаты, обезличенные примеры, query/export receipts и ссылки на защищённый источник.

## Именование

```text
docs/product-model/analysis/YYYY-MM-DD-<slug>.md
```

## Связь с Product Atlas

Из reviewed/accepted records в каноническую Git-модель могут быть внесены:

- finding;
- product problem candidate;
- evidence gap;
- decision;
- metric snapshot с provenance;
- follow-up measurement question.

Никакой renderer или Penpot integration не интерпретирует raw data и не придумывает вывод. Product Atlas Penpot projection создаётся из reviewed Git entities/evidence package только явной scoped-задачей через MCP.

Для action map page/component maps могут появиться в Penpot только после:

1. reviewed immutable package;
2. accepted Git finding/decision linkage;
3. exact Product Atlas target read through MCP;
4. bounded mutation и read-back receipt.

Penpot MCP не изменяет analysis package и не создаёт finding из визуального overlay.

## Минимальный шаблон

```markdown
# <Название анализа>

- Analysis ID:
- Status:
- Date / data cutoff:
- Question / decision use:
- Linked Jobs / outcomes / capabilities:

## Sources and method

## Facts

## Interpretation

## Uncertainty and missing data

## Product implications

## Options

## Decision required / accepted decision

## Follow-up evidence
```
