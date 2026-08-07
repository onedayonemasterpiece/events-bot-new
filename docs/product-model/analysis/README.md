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
