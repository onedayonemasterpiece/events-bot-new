# Event media: единый автоматический gate

`event_media.py` — канонический слой изображений событий. Он является частью
Smart Update, а не отдельным ручным процессом и не renderer-side фильтром.

## CDN invariant

В production (`EVENT_MEDIA_REQUIRE_CDN=1`) изображение не может попасть в
`approved` projection, пока Smart Update media gate не материализует его в
текущем Yandex bucket и не получит URL
`https://static.kenigevents.ru/...`. Исходный URL остаётся provenance в
`catbox_url`, а публичные consumers получают CDN URL из `supabase_url`.
Raw URL `storage.yandexcloud.net/kenigevents.ru/...` безопасно меняет host на
CDN без копирования объекта; source/Supabase/legacy-bucket URL скачиваются,
нормализуются в WebP и загружаются по exact encoded-SHA в immutable
`p/image/v2/...`; `p/dh16/...` остаётся только legacy read path.
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

### Legacy approved-versus-approved debt

The automatic frontier compares a new/pending candidate with approved media; it
does not silently reconsider every historical approved pair on each Smart
Update. Event `4671` exposed why this distinction matters: rows `7824` and
`8622` were both approved before the current pair-review gate, although `8622`
is a cropped/overlay variant of the canonical poster `7824`. Renderers only
collapse identical URLs and correctly refused to make a new semantic decision.

The narrow incident repair therefore backed up both the event and EventPoster
rows, marked `8622` as `duplicate` of `7824`, rebuilt the approved projection and
verified the Telegraph page. Prevention requires a bounded, auditable
approved-versus-approved historical reconciliation batch through the same VLM
pair schema; adding a broad pHash threshold or renderer-side crop heuristic is
explicitly forbidden. Regression contract:
`INC-2026-07-16-static-event-media-action-regressions`.

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
При любом CDN-materialization или Smart Update merge одинаковый raw SHA
сохраняется только на уже существующем survivor: второй ledger row не нарушает
partial unique index `(event_id, raw_sha256)`, а exact-equality остаётся в
pair-review evidence. `poster_hash` описывает identity исходного кандидата;
`raw_sha256` заполняется только доказанным SHA байтов managed display object и
не выводится из source hash.

При повторной source rehydrate exact identity разрешается **раньше** source
candidate hash, а URL считается weak identity только для legacy-row без
`raw_sha256`/`p/image/v2/...`. Один mutable VK/TG/source URL может со временем
вернуть другую rendition; она создаёт/находит отдельную exact row и проходит
обычный pair gate, но не перезаписывает уже классифицированный exact-v2 объект.
Если при этом source-level `poster_hash` остался прежним, новая rendition
получает стабильный производный row hash от `source hash + exact encoded SHA`.
Это сохраняет историческую exact-row, соблюдает уникальность
`(event_id, poster_hash)` и делает повтор той же rendition идемпотентным.
Identity index перестраивается после каждого merge внутри batch, поэтому
повторный reconcile с теми же кандидатами идемпотентен и не сбрасывает semantic
role/geometry, не создаёт tight-loop и не повторяет платный VLM-вызов.

Новые managed WebP пишутся в immutable path, адресованный точным SHA-256 уже
закодированных байтов: `p/image/v2/<first2>/<encoded_sha256>.webp`. Старые
`p/dh16/...` URL остаются читаемым legacy, но больше не являются целью новых
записей. Перцептивный dHash — только evidence сходства: использовать его как
object identity нельзя, поскольку разные пиксели могут иметь одинаковый dHash и
перезапись такого URL нарушает годовой immutable CDN cache contract.

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
изображение до афиши. Неполная/низкоуверенная schema fail-close оставляет роль
в terminal `error`. Временные `429`/RPM/RPD/timeout используют normal pool
`GOOGLE_API_KEY4,GOOGLE_API_KEY5`, оставляют роль неизвестной в `pending` и
ставят один durable delayed retry: RPD переносится на следующий UTC-day, а
краткие provider/transport ограничения получают bounded delay. Inline retry
burst, emergency overflow и model fallback запрещены; renderer роль не угадывает.

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

Повторное выполнение не переотправляет rows с актуальной версией/input hash.
Operational selector включает также отсутствующую, устаревшую по model/prompt и
несовпадающую по exact pixel hash geometry; `--event-id` поэтому может поставить
точечный geometry-only repair. `--retry-errors` остаётся ручным контролем только
для terminal semantic errors.

### Face boxes and viewer-value region

После role-classification тот же durable `event_media_review` worker отдельно
обогащает каждое подходящее изображение геометрией. Smart Update только ставит
job в outbox и никогда не ждёт скачивание/provider inline. Один короткий
`gemma-4-31b-it` запрос возвращает одновременно:

- `face_boxes_yxyx_json` — tight bbox до 25 крупнейших/наиболее различимых
  человеческих лиц (мелкие лица толпы не должны раздувать ответ без пользы для
  защиты crop);
- `valuable_region_yxyx_json` — минимальный связный прямоугольник с главным
  объектом/людьми, визуальной идентичностью и действительно важным текстом;
- confidence и короткий `reason_code`.

Обе координаты хранятся как нормализованные `[ymin, xmin, ymax, xmax]` в
диапазоне `0..1`. Это только reusable metadata: stage не выбирает aspect ratio,
не вычисляет конечный crop и не диктует downstream-механизмам, как обрезать
изображение. Компактный native `response_schema`, проверенный Gemma sampling
envelope, `thinking_level=MINIMAL`, отключённые thoughts и лимит `768` output
tokens не дают задаче уходить в длинное reasoning. Диапазоны/длина/порядок bbox
дополнительно валидируются приложением: JSON-Schema-only keywords hosted Gemma
endpoint не принимает. Лимит 25 лиц задаётся одновременно коротким prompt
contract и application validator: это сохраняет полезные для crop лица и не
позволяет массовой сцене оборвать structured JSON по `MAX_TOKENS`.
Provider deadline по умолчанию равен `90` секундам: hosted vision inference
иногда законно дольше прежних 45 секунд. Внутри одного item по-прежнему только
одна попытка; timeout сохраняется как item-level error, а повтор выполняет
durable worker/backfill отдельным paced запуском, без retry burst.

`event_image_geometry` — глобальный versioned cache по точному
`pixel_sha256 + model + prompt_version`; `EventPoster.image_geometry_id`
ссылается на него. Поэтому один и тот же ориентированный набор RGB-пикселей в
разных событиях оплачивается один раз, а re-encode/crop с другими пикселями не
получает потенциально неверные координаты из perceptual-hash cache.
Ссылка считается текущей только при точном равенстве
`EventPoster.pixel_sha256 == EventImageGeometry.pixel_sha256` и совпадении
model/prompt. Смена display URL/path или нормализованных пикселей атомарно
сбрасывает geometry и все image-dependent semantic/focal/safe-crop evidence,
после чего обычный durable worker анализирует новые байты. Финальный poster,
получивший `approved` после pair reconciliation, всегда получает enrichment
follow-up, даже если других pair-review rows уже нет.

Consumer `smart_update_image_geometry` использует normal pool
`GOOGLE_API_KEY4,GOOGLE_API_KEY5` с ротацией с первого reserve. Это не emergency
overflow и он не заимствует KEY1–KEY3. Отсутствующий limiter/registry member
останавливает stage fail-closed. Production defaults: `100` новых provider calls
за UTC-day и не чаще одного outbox item в `7` секунд; cache hits квоту не тратят.
Если более ранний semantic-role pass не продвинулся из-за своего отдельного
суточного бюджета, тот же durable worker turn продолжает geometry candidate:
pending semantic row не может заблокировать bbox lane.

Исторический backfill не скачивает изображения с Fly. Работать нужно на копии
production SQLite локально или в Kaggle; JSONL fsync-ится после каждого item и
затем импортируется в production маленькой fingerprint-guarded транзакцией:

```bash
# plan/canary на рабочей копии snapshot; run по умолчанию ограничен 20 items
python scripts/backfill_event_image_geometry.py \
  --mode plan --db artifacts/codex/image-geometry/db.sqlite --limit 20
python scripts/backfill_event_image_geometry.py \
  --mode run --db artifacts/codex/image-geometry/db.sqlite \
  --env-file /path/to/.env --limit 20 --min-delay 6 --jitter 1 \
  --chunk-size 100 --chunk-pause 75 \
  --output artifacts/codex/image-geometry/results.jsonl

# обязательный visual gate: red=faces, green=viewer-value
python scripts/render_event_image_geometry_contact_sheets.py \
  --input artifacts/codex/image-geometry/results.jsonl \
  --output-dir artifacts/codex/image-geometry/contact-sheets

# сначала dry-run против свежей target DB; --apply только после stale=0/inspection
python scripts/backfill_event_image_geometry.py \
  --mode import --db /data/db.sqlite \
  --input /tmp/image-geometry-results.jsonl
python scripts/backfill_event_image_geometry.py \
  --mode import --db /data/db.sqlite \
  --input /tmp/image-geometry-results.jsonl --apply \
  --backup-out /data/backups/image-geometry-import.json
```

Canary идёт ступенями `10–20 → visual review → около 100 → visual review →
remainder`, с паузой не менее `5–7` секунд, `60–90` секунд после каждых `100`
provider calls и немедленной остановкой на `429`. Дневной backfill cap по
умолчанию `400` оставляет запас относительно Gemma lane; ключи дают независимую
provider quota только если они принадлежат разным Google Cloud projects, поэтому
общий cap нельзя умножать просто на число env-переменных. Устаревшие/404 URL
фиксируются как item-level errors и не блокируют batch или повторно молотятся
при resume с тем же checkpoint.

## Schema

`EventPoster` дополняется `raw_sha256`, `pixel_sha256`, `perceptual_hash`,
dimensions/MIME, `review_status`, `duplicate_of_id`, reason/time/order,
semantic-role evidence/version/model/input hash/status/confidence,
`focal_x`/`focal_y`/`safe_crop_json`, ссылкой `image_geometry_id` и метаданными
WebP derivatives `256`/`512`. `event_image_geometry` хранит versioned face/value
boxes, исходные размеры, confidence, model/prompt и token usage.
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
