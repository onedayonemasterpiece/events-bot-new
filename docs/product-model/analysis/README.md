# Product analysis records

> **Статус:** рабочий контракт для результатов целевых продуктовых анализов.  
> **Источник аналитики:** проверяемые запросы к БД/экспортам, документации и production evidence.  
> **Не является:** автоматическим dashboard, raw-data store или заменой статистического runtime.

## Назначение

Продуктовая аналитика выполняется по конкретному вопросу владельца продукта — в том числе через ChatGPT — и сохраняется как versioned Markdown record.

```text
вопрос
→ источники и data cutoff
→ запрос / выборка / evidence
→ анализ
→ findings и неопределённость
→ варианты решения
→ owner decision
→ Product Atlas projection
```

Product Atlas показывает только сохранённые и проверяемые analysis records. Ответ в чате без записи и provenance не становится product truth.

## Что хранится

Analysis record содержит:

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

В Markdown обязательны разделы:

1. вопрос и зачем он нужен;
2. источники и точный временной срез;
3. метод и определения;
4. факты;
5. интерпретация;
6. ограничения и недостающие данные;
7. влияние на Jobs/outcomes/capabilities;
8. варианты решений;
9. решение или `decision_required`;
10. ссылки на последующие проверки.

## Правила достоверности

1. Факт, интерпретация и гипотеза записываются отдельно.
2. Raw counts без denominator не используются для продуктового вывода.
3. Отсутствие данных обозначается `unknown` или `insufficient_data`, а не PASS.
4. Before/after comparison не называется причинным эффектом без соответствующего design.
5. Release, environment, population и exclusions указываются явно.
6. Если анализ основан на export, сохраняются manifest/hash/run ID, но не raw sensitive data.
7. Противоречащие findings не усредняются молча.
8. Более поздний анализ supersedes ранний только по явно указанным вопросам и с сохранением history.

## Action-map evidence package

Для зарегистрированной `action_map_diagnostic` кампании concrete analysis
record является единственной канонической точкой принятия результата. Он может
ссылаться на один immutable reviewed `ProductAnalyticsEvidencePackage`, но не
на raw stream или живой dashboard. Обязательная цепочка не сокращается:

```text
MeasurementQuestion
→ evidence
→ finding
→ decision
→ follow-up
```

Минимальный логический contract package:

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
  status: accepted | rejected | insufficient_data
  text: ...
options: []
decision:
  owner: ...
  outcome: ship | change | instrument_better | stop | pending
follow_up:
  measurement_question: ...
  due_or_stop_rule: ...

artifacts:
  page_map: ...
  component_map: ...
  representative_render: ...
  aggregate_snapshot: ...
  methodology_receipt: ...
resource_links:
  product_atlas_ids: []
  resource_graph_ids: []

provenance:
  campaign_manifest_sha256: ...
  schema_sha256: ...
  aggregate_or_export_sha256: ...
  release_archive_sha256: ...
  generated_at: ...
  reviewed_at: ...
  reviewers: []
  package_sha256: ...
```

`facts`, `limitations`, quality/coverage и immutable provenance обязательны
даже при `insufficient_data`. Package content-addressed: после review его bytes,
artifacts и source receipts не переписываются. Исправление или новый cutoff
создают новый `evidence_id`/hash и explicit `supersedes` link из нового analysis
record; старый package остаётся воспроизводимым. Артефакты содержат только
aggregate maps и санитизированный representative render, без raw actor/view
identity.

Hotspot, cold area, overlay, dead/repeat candidate или другая визуальная
особенность **не становится автоматически** finding, product problem,
`ProblemBubble`, UI gap, Component Contract change или profile signal. Сначала
фиксируются fact, competing explanations, limitations и quality state; finding
требует human review, а decision — явного owner outcome. Promotion в модель
персонализации остаётся отдельным benchmark/experiment/versioned decision.

## Privacy boundary

В Git не попадают:

- ПДн;
- raw feedback text или screenshots без отдельного защищённого основания;
- raw Search queries;
- session-level clickstream;
- bearer URLs, tokens и secrets;
- выгрузки production DB.

Допустимы агрегаты, обезличенные примеры, query/export receipts и ссылки на защищённый источник.

## Именование

```text
docs/product-model/analysis/YYYY-MM-DD-<slug>.md
```

Пример:

```text
docs/product-model/analysis/2026-09-10-search-job-completion.md
```

## Связь с Product Atlas

Из accepted/reviewed records извлекаются:

- `analysis_finding` cards;
- `product_problem` candidates;
- evidence gaps;
- decision callouts;
- metric snapshots с provenance.

Plugin не интерпретирует raw данные и не придумывает вывод. Он только визуализирует зафиксированные findings и связи.

Для action map page/component maps и package provenance могут быть
спроецированы только из reviewed package конкретного analysis record. Page 50
получает evidence; page 40 получает finding/decision лишь после их принятия.
Projection не меняет package и сохраняет deep links в Resource Graph.

## Минимальный шаблон

```markdown
# <Название анализа>

- Analysis ID:
- Status:
- Date / data cutoff:
- Question:
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
