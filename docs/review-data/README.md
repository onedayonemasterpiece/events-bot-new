# Review data

Этот каталог хранит **редакционные входы**, а не publication truth.

- `static_collections_review_seed_v1.json` — provisional, source-bound выборка
  для проверки ontology v2. Её нельзя импортировать в production exporter или
  переименовывать в owner gold сменой status.
- `static-collections-source-reviews-v1/` — hash-bound receipts ручной
  перепроверки известных дефектов и occurrence families по `event_source` из
  Fly SQLite.
- будущий owner-approved gold живёт отдельно в `tests/fixtures/`, создаётся
  только после независимого owner/editor review и сам по себе не разрешает
  публикацию.

`needs_source_review` не входит в supply. Raw source quote сохраняется вместе с
EventSource identity и hash-bound source-ref projection. Canonical description
и `topics` не считаются исходной цитатой.

Текущий seed не содержит PR-B scores/winning prototypes и честно помечает их
`pending_pr_b`. Кино исключено. Для фестивалей scope разделён явно:

- festival parent/aggregate rows не входят в semantic review supply;
- самостоятельный festival child event может входить только с
  occurrence-specific raw source и `festival_scope_kind=festival_child_event`;
- festival extraction и страницы остаются отдельным track и в PR A не меняются.

Поэтому 4648 удалён из `science_pop`/`family_suitable` positives; 6871 исключён
из supply и имеет receipt на реальный показ 2026-08-08; 7103 исключён, потому
что occurrence source не доказывает интенсивную практику из определения
`strong_impressions`. Receipts 7333/7344 сохраняют только известный
duplicate-family defect и не добавляют festival extraction/source/page behavior.

## Canonical evidence snapshot

Snapshot является ignored operational artifact:
`artifacts/codex/static-collections-pr-a/static-collections-evidence-snapshot-v1.json`.
В Git сохраняются его SHA-256, схема и точный serialization contract, но не
production dump. `seed.evidence_snapshot_sha256` обязан точно совпадать с
`index.source_snapshot_sha256`.

`canonical-json-v1` — это UTF-8 от:

```python
json.dumps(
    payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
    allow_nan=False,
).encode("utf-8")
```

Trailing newline отсутствует; events отсортированы по `id`, вложенные
`event_sources` — по `id`; SQLite JSON-columns сохраняются как исходный TEXT.
Payload обязан объявлять `static-collections-evidence-snapshot-v1`,
`canonical-json-v1` и `event-review-source-v1`. Команда из
`source.generator_command` проверяет canonical bytes, hash, порядок и точное
совпадение цитат с snapshot, затем воспроизводимо сериализует seed.

## Source ref и quote contract

`source_ref_sha256` — SHA-256 canonical-json-v1 projection из полей в таком
логическом составе (физический JSON key order не важен):

```text
event_id, source_id, source_type, source_url, trust_level,
source_chat_username, source_message_id, source_text_sha256,
source_text_char_count
```

Validator пересчитывает этот hash во всех seed rows и во всех receipt evidence.
Старый `source_record_sha256` сохраняет binding исходного export record; новый
самодостаточный `source_ref_sha256` устраняет зависимость от неописанного порядка
полей.

Для каждой цитаты обязательны `*_quote_kind=full|excerpt`, boolean
`*_quote_truncated`, start/end offsets, длина и число отброшенных символов до/после.
`full` обязан занимать весь source text и совпадать с `source_text_sha256`;
`excerpt` честно сообщает усечение. Полное substring-сопоставление с raw source
выполняет builder при наличии ignored snapshot.

Три repo SHA имеют разные роли: `extraction_repo_sha` — код checkout во время
production evidence export; `seed_builder_repo_sha` — версия source binder;
`integration_repo_sha` — воспроизводимая PR-A integration base. Они не
схлопываются обратно в неоднозначный `generator_repo_sha`.
