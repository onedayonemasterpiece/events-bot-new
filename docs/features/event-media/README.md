# Event media: единый автоматический gate

`event_media.py` — канонический слой изображений событий. Он является частью
Smart Update, а не отдельным ручным процессом и не renderer-side фильтром.

Планируемый release-only social derivative с фирменной mobile-site биркой для
достоверно `visual_only` изображений Telegram/VK описан отдельно в
[social publish brand tag](social-publish-brand-tag.md). Он пока не реализован и
не меняет текущий approved media projection или publishers.

## CDN invariant

В production (`EVENT_MEDIA_REQUIRE_CDN=1`) изображение не может попасть в
`approved` projection, пока Smart Update media gate не материализует его в
текущем Yandex bucket и не получит URL
`https://static.kenigevents.ru/...`. Исходный URL остаётся provenance в
`catbox_url`, а публичные consumers получают CDN URL из `supabase_url`.
Raw URL `storage.yandexcloud.net/kenigevents.ru/...` безопасно меняет host на
CDN без копирования объекта; source/Supabase/legacy-bucket URL скачиваются,
нормализуются в WebP и загружаются в deterministic `p/dh16/...`.
Одновременно materialization готовит независимые content-addressed WebP
миниатюры по длинной стороне `256` и `512` px в `p/thumb/v1/...`. Статический
сайт использует их через `srcset`/`sizes`; полноразмерный объект остаётся только
для hero и раскрытого viewer. Sprite/contact sheet не является production
контрактом: отдельные immutable derivatives лучше используют HTTP cache,
не заставляют декодировать невидимые изображения и не требуют runtime crop-map.

Ошибка оставляет `review_status=pending_review`,
`review_reason=cdn_mirror_pending` и durable retry `event_media_review`. Это
единый путь для новых событий, existing-event parser updates, source rehydrate
и TelegramMonitor; static/Telegraph/Telegram/VK только читают projection.
Static exporter дополнительно fail-close фильтрует gallery refs: принимает только
`static.kenigevents.ru`, а raw URL текущего bucket
`storage.yandexcloud.net/kenigevents.ru/...` канонизирует на CDN host. Source
CDN, Supabase и legacy bucket не могут быть renderer fallback.

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
Повтор одного и того же resolved display URL схлопывается ещё раньше, до
download/hash/VLM; связанные deferred pair rows закрываются детерминированно.
При retry CDN-materialization одинаковый raw SHA сохраняется только на уже
существующем survivor: второй ledger row не нарушает partial unique index
`(event_id, raw_sha256)`, а exact-equality остаётся в pair-review evidence.

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
- `running` pair, прерванный deploy/restart, автоматически возвращается в
  `deferred` через десять минут; evidence попытки сохраняется, а дальнейшая
  обработка следует обычным budget/retry правилам без ручного вмешательства.

Результаты: duplicate → `duplicate`; distinct → `approved`; semantic conflict/
unrelated candidate → `rejected`; failure/uncertain → fail-closed quarantine.
После изменения projection обычный event fanout перестраивает Telegraph,
Telegram, managed VK и static site.

### LLM-first semantic media role

Dedup review и смысловая роль изображения — два разных решения. После
материализации каждого distinct approved asset один небольшой VLM-запрос
`event-media-role-v1` классифицирует его в закрытый enum:

- `event_identity_poster`;
- `event_photo`;
- `attendee_information`;
- `program_or_schedule`;
- `wayfinding`;
- `sponsor_or_brand`;
- `unknown_document`;
- `unknown_visual`.

`event_identity_poster` — строгая положительная роль. Она разрешена только если
VLM явно подтверждает, что это афиша **конкретного события**, идентичность
события является главным назначением изображения, а доминирующим назначением
не являются услуги/правила/цены для посетителя, расписание, навигация или
спонсорский бренд. Требуется confidence не ниже
`EVENT_MEDIA_ROLE_POSTER_CONFIDENCE` (по умолчанию `0.88`) и все schema
guard booleans. OCR/keyword/соотношение сторон сами по себе никогда не повышают
изображение до афиши. Ошибка, quota или неполная schema fail-close оставляют
роль неизвестной; renderer не угадывает её повторно.

Static event-detail использует эту роль только для крупного poster companion.
`attendee_information` (например карточка услуг кемпинга),
`program_or_schedule` и другие документы остаются обычными gallery assets и
никогда не получают подпись/маркер «Афиша». Фото может стать desktop hero;
строгая афиша показывается целиком в отдельном блоке без серых полей и без crop.
На карточках и в related-feed `cover` разрешает только явная классифицированная
роль `event_photo`. `unknown_document`, `unknown_visual`, отсутствующая роль и
legacy `image_text_mode=visual_only` сохраняют полную ширину исходника: они не
могут горизонтально обрезать потенциальную афишу. После масштабирования до
ширины нормализованной карточки допускается только вертикальное переполнение с
центром или доверенным focal Y.

Идемпотентный operational backfill по умолчанию только показывает план:

```bash
python scripts/enqueue_static_event_media_enrichment.py --db /data/db.sqlite
# после проверки счётчиков:
python scripts/enqueue_static_event_media_enrichment.py --db /data/db.sqlite --apply
```

Backfill использует тот же верхнеуровневый eligibility contract, что и static
export: только `lifecycle_status=active`, `silent=0` и события, которые ещё не
закончились на `--from-date`. Отменённые, postponed, merged и silent rows не
получают derivative/LLM jobs и не могут быть случайно возвращены в public fanout.

Повторное выполнение не переотправляет rows с актуальной версией/input hash;
`--retry-errors` разрешён только для контролируемого повторного прогона.

## Schema

`EventPoster` дополняется `raw_sha256`, `pixel_sha256`, `perceptual_hash`,
dimensions/MIME, `review_status`, `duplicate_of_id`, reason/time/order,
semantic-role evidence/version/model/input hash/status/confidence,
`focal_x`/`focal_y`/`safe_crop_json` и метаданными WebP derivatives `256`/`512`.
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
Cleanup/backfill outbox timestamps use the same SQLite `DateTime` representation
as SQLAlchemy (space separator, UTC), otherwise same-day rows with a raw ISO
`T` would not satisfy the worker's `next_run_at <= now` query.
If the audit directly confirmed a duplicate in an existing Telegram album, the
cleanup job carries `public_repair_priority=true`. The outbox executes this
bounded repair lane ahead of ordinary announcement/backfill rows while still
enforcing the same global Telegram spacing interval; merely changing the DB
projection or having a pending review never grants this priority.
The Gemini call sends the strict standard schema through google-genai
`response_json_schema`; `response_schema` is not used because that narrower
protobuf transport rejects JSON Schema `additionalProperties` before inference.

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
