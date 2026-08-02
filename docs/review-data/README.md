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
EventSource identity и SHA-256 полного source row. Canonical description и
`topics` не считаются исходной цитатой.

Текущий seed не содержит PR-B scores/winning prototypes и честно помечает их
`pending_pr_b`. Кино- и festival events исключены из semantic review rows.
Receipts 7333/7344 сохраняют только уже известный duplicate-family defect и не
добавляют festival extraction, source или page behavior.
