# Event media: единый автоматический gate

`event_media.py` — канонический слой изображений событий. Он является частью
Smart Update, а не отдельным ручным процессом и не renderer-side фильтром.

## Инвариант

- `EventPoster` — ledger логических изображений и их alternate locations.
- Публично доступны только строки `review_status=approved`.
- `Event.photo_urls`/`photo_count` — производная совместимая проекция approved
  rows, не второй источник истины.
- Telegraph, Telegram, managed VK, promo/video recovery и static exporter читают
  approved projection. Renderer не добавляет изображения в БД и не принимает
  perceptual-решения.
- Строки и Storage никогда не удаляются автоматически: duplicate/rejected/
  unavailable остаются доказательством.

Следствие: дубль может кратковременно существовать **в ledger** как
`pending_review`, но он не попадает в публичную галерею до автоматического
решения. Это и есть prevention contract; обещание «в БД никогда не появится
вторая строка» было бы неверным и уничтожало бы provenance.

## Где поступают новые изображения

Все production paths сходятся в `_apply_posters()` Smart Update или его тонкий
adapter `ingest_event_media_urls()`:

1. VK/TG parsing и обычный Smart Update create/merge;
2. `main.upsert_event_posters` после обработки `PosterMedia`;
3. новый `EventSource`: outbox `event_media_review` при необходимости агрегирует
   media нескольких **specific** sources (общие roundup-source rows исключаются);
4. promo Telegraph recovery, video popular-review recovery и full source parsing;
5. operational Catbox → managed-storage backfill.

`tests/test_event_media_gate.py::test_production_media_writers_are_restricted_to_the_gate`
защищает от появления нового прямого конструктора `EventPoster` вне этого списка.
Festival media — отдельная модель `Festival` и не входит в этот event-media gate.

## Решение

### Детерминированно

Для скачанных байтов считаются:

- raw SHA-256;
- normalized RGB pixel SHA-256;
- repository-compatible dHash16 (`EventPoster.phash`);
- независимый DCT pHash16 (`perceptual_hash`);
- MIME/dimensions и SSIM как evidence.

Равный raw/pixel SHA — exact duplicate без LLM. Равенство/близость dHash, pHash,
OCR или SSIM само по себе никогда не удаляет и не скрывает approved media.

### Автоматический VLM review

Каждое новое второе и последующее изображение остается `pending_review`. Worker
сравнивает ровно одну пару за job/call. Один pending frontier сравнивается с
текущими approved rows; матрица всех pending пар заранее не создается.

Defaults:

- primary: `gemini-3.1-flash-lite`;
- escalation: `models/gemma-4-31b-it` только для uncertain/low-confidence primary;
- ключ: scoped `GOOGLE_API_KEY4`, без reserve overflow и без `GOOGLE_API_KEY2`;
- feature budgets: 100 primary + 25 escalation calls/UTC day;
- максимум 3 automatic attempts, затем `unresolved`; изображение остается
  непубличным, ручной очереди нет;
- exhausted primary budget не тратит escalation budget; retry переносится на
  следующий UTC day;
- cache/idempotency: `event_media_pair_review.pair_input_hash` включает event,
  fingerprints, event context и policy/prompt versions.

Результаты: duplicate → `duplicate`; distinct → `approved`; semantic conflict/
unrelated candidate → `rejected`; failure/uncertain → fail-closed quarantine.
После изменения projection обычный event fanout перестраивает Telegraph,
Telegram, managed VK и static site.

## Schema

`EventPoster` дополняется `raw_sha256`, `pixel_sha256`, `perceptual_hash`,
dimensions/MIME, `review_status`, `duplicate_of_id`, reason/time/order.
Старые rows переводятся в `approved` ровно при первом schema upgrade; повторный
`Database.init()` не перезаписывает статусы. Таблицы `event_media_pair_review` и
`event_media_review_usage` хранят решения/retry и независимые суточные бюджеты.

## Audited cleanup / rollout

Канонический audit runbook: [event-image-duplicate-audit](../../operations/event-image-duplicate-audit.md).
Baseline 2026-07-13: `266` eligible, `158` multi-image, `823` current static refs,
`79` events with visually confirmed duplicates, `136` excess refs; visual rows
exist for `158/158` multi-image events. Evidence is uncommitted under
`artifacts/codex/event-image-duplicate-audit-20260713/`.

Cleanup is soft and backup-first:

```bash
python scripts/apply_event_media_audit_cleanup.py \
  --db /data/db.sqlite \
  --audit-dir /tmp/event-image-duplicate-audit-20260713
# inspect dry-run, then add --apply

python scripts/stage_event_media_review_backfill.py \
  --db /data/db.sqlite --current-date YYYY-MM-DD
# inspect dry-run, then add --apply
```

The dated cleanup requires an unchanged audited visible prefix, creates dated
backup tables, assigns audited statuses, quarantines the unaudited >12 exporter
tail and fixes projection/count. A changed projection rearms only public
surfaces that already exist (Telegraph path, Telegram post, or hash-proven
managed VK post), so cleanup can never become first-time publication; the
global static build is rearmed once. The staging command
handles rows created/changed after the snapshot; it keeps one seed, quarantines
additional media and uses only the same automatic outbox worker. Repeated
staging preserves every non-empty automatic/audit decision reason, so a
successful VLM decision cannot be returned to quarantine by the backfill.

Never delete Storage during this rollout. Before production apply: fresh DB
snapshot + SHA-256, `PRAGMA quick_check`, dry-run/stale review. After apply:
`quick_check`, status/projection reconciliation, zero approved members from each
confirmed duplicate group, job completion and live static/Telegraph/TG/VK checks.

## Regression contracts

- `INC-2026-06-09-event-media-duplicates`
- `INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped`
- `INC-2026-05-05-event-source-media-aggregation-gap`
- `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`

The tram-photo rule is preserved: two distinct event photos are not collapsed
by a hash threshold. Publisher exact-URL uniqueness remains a last safety net,
not an independent visual policy.
