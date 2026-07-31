# Качество и «витринность» видео в Telegram Monitoring

Статус: целевой production-контракт для video-analysis stage в Kaggle
`TelegramMonitor`.

Этот документ отвечает только за оценку, отбор и версионирование анализа
вертикальных event-видео. Каноника общего мониторинга и импорта остаётся в
[`README.md`](README.md), а единый gate изображений — в
[`../event-media/README.md`](../event-media/README.md).

## Решение

Для видео недостаточно аналога «открыточности» одного кадра. Рабочий термин —
**видеовитринность** (`showcase_score`): насколько ролик технически пригоден и
достаточно выразителен во времени, чтобы представлять событие на публичной
странице. Она не равна ни красоте, ни релевантности:

- `aesthetic_score` — визуальное и временное мастерство самого ролика;
- `showcase_score` — пригодность ролика для публичной витрины события;
- `relevance_score` — соответствие конкретному событию; один и тот же ролик
  получает отдельный score для каждой связи;
- policy/risk gates — отдельные запреты, которые нельзя «перевесить» высоким
  числом.

Такое разделение следует результатам DOVER: общее впечатление о UGC-видео
смешивает техническое качество и эстетическое предпочтение, поэтому их нужно
оценивать раздельно. FineVQ, в свою очередь, выделяет цвет, шум, артефакты,
размытие и temporal-quality как самостоятельные признаки. См.
[DOVER, ICCV 2023](https://openaccess.thecvf.com/content/ICCV2023/html/Wu_Exploring_Video_Quality_Assessment_on_User_Generated_Contents_from_Aesthetic_ICCV_2023_paper.html)
и
[FineVQ, CVPR 2025](https://openaccess.thecvf.com/content/CVPR2025/html/Duan_FineVQ_Fine-Grained_User_Generated_Content_Video_Quality_Assessment_CVPR_2025_paper.html).

## Почему именно такая рубрика

Профессионально размеченный VADB оценивает видео не одним числом, а через
композицию, крупность/выбор плана, свет, визуальный тон, цвет и глубину резкости;
отдельные объективные теги описывают композицию, свет и движение камеры. В
датасете 10 490 видео, 37 профессиональных аннотаторов, и каждое видео оценено
как минимум 13 специалистами. Это более подходящая основа для «красивости», чем
эвристика по разрешению или bitrate.
[VADB, NeurIPS 2025](https://papers.nips.cc/paper_files/paper/2025/hash/81846dc80cf522fd8205d5a9a0fe1bf8-Abstract-Datasets_and_Benchmarks_Track.html).

Для короткого social-видео важны одновременно эстетическая ценность и
небанальность/творческая выразительность; это показало исследование 3 800+
шестисекундных micro-video.
[6 Seconds of Sound and Vision, CVPR 2014](https://openaccess.thecvf.com/content_cvpr_2014/html/Redi_6_Seconds_of_2014_CVPR_paper.html).
Однако engagement нельзя считать заменой качеству: публикационная история,
аудитория канала и алгоритм соцсети являются сильными confounders.

LMM нельзя оставлять единственным измерителем технического качества. В
Q-Bench-Video proprietary и open models показали неполное и неточное понимание
technical, aesthetic и temporal defects. Поэтому rollout v1 гибридный в
доступной телеметрии: actual bytes и Telegram media attributes отвечают за
измеряемые file/geometry/duration hard gates, а Gemini — за наблюдаемые дефекты,
смысл, композицию, намеренность движения и соответствие событию. Локальный
`ffprobe` blend описан ниже как следующий исследовательский этап, а не как уже
работающая acceptance-семантика.
[Q-Bench-Video, CVPR 2025](https://openaccess.thecvf.com/content/CVPR2025/html/Zhang_Q-Bench-Video_Benchmark_the_Video_Quality_Understanding_of_LMMs_CVPR_2025_paper.html).

## Quota-aware pipeline в Kaggle

Порядок обязателен. Он предотвращает загрузку мусора в CDN и повторную оплату
одного и того же файла.

1. **Event-confirmed + rights gate.** Рассматривать видео только после того, как текущий
   LLM-first extraction подтвердил хотя бы одно пригодное будущее событие.
   Пустой, non-event, completed-report или невалидный post не открывает
   video-call. До download также требуется явное присутствие source username в
   `TG_MONITORING_VIDEO_REPUBLICATION_ALLOWED_SOURCES`; monitoring source сам по
   себе не считается разрешением на копирование.
2. **Cheap file gate.** Скачать байты в Kaggle, проверить фактический размер
   строго `< TG_MONITORING_VIDEO_MAX_MB` (production default `10 MiB`), посчитать
   raw SHA-256 и прочитать доступные stream metadata (Telegram attributes;
   `ffprobe` допустим как дополнительная проверка). Ролик размером ровно
   `10 MiB` или больше получает
   `skipped:too_large`, но не низкую оценку качества.
3. **Permanent SHA cache gate.** До model reserve искать exact raw `sha256`.
   Первый валидный terminal result (`accept`, `review` или semantic `reject`)
   хранится без TTL в Fernet-encrypted sidecar; model/prompt/schema/policy
   versions остаются его
   audit metadata, но их смена не запускает автоматический повторный просмотр.
   Cache hit не открывает model request. Transport/provider failure не является
   terminal result и не записывается как вечный semantic reject. Переоценка
   возможна только отдельным явным операторским migration/backfill, не обычным
   мониторингом.
4. **Deterministic eligibility v1.** Fail closed до Gemini, если Telegram
   attributes не идентифицируют video, скачанные bytes пусты, reported
   width/height не дают portrait display-aspect, разрешение слишком мало или
   reported duration вне rollout-envelope. Полный local decode/stream/rotation
   probe относится к следующей `ffprobe`-версии и не заявлен как v1 gate.
5. **Один multimodal provider-send на unique SHA.** Передать локальные video bytes,
   исходный post text/OCR и все event candidates этого post одним запросом в
   `gemini-3.1-flash-lite`. Нельзя отправлять по запросу на dimension или event;
   provider `429` не открывает повторную отправку через второй ключ.
6. **Только через общий limiter.** Разрешён только `GoogleAIClient` с atomic
   `google_ai_reserve`/finalize, отдельным consumer/feature budget и scoped key
   lane. Direct SDK/HTTP, Antigravity, reserve fallback, emergency overflow и
   бесконтрольный inline retry запрещены. Exhausted budget означает deferred/
   skipped analysis, а не публикацию без gate.
7. **Application validation и арифметика.** Модель возвращает только базовые
   observations/scores. Все формулы, thresholds, enum-validation и решение
   `accept|review|reject` принадлежат deterministic application code.
8. **CDN после accept.** Только accepted bytes материализуются из Kaggle прямо
   в Yandex Object Storage/CDN по content-addressed key. Rejected/review video
   не расходует CDN storage. Один объект может иметь несколько event links.

Production budget intentionally small: `TG_MONITORING_VIDEO_MAX_MODEL_CALLS_PER_RUN=6`.
Код жёстко ограничивает effective значение сверху шестью, даже если env ошибочно
задаёт большее число. Это предел именно физических provider sends: у video-client
`max_retries=1`, model fallback и provider-429 key rotation отключены, current
`google.genai` получает `HttpRetryOptions.attempts=1`, а legacy SDK fallback
запрещён. Следующий SHA после исчерпания шести попыток получает
`skipped:model_budget`.
Обычный rotating pool задаётся `TG_MONITORING_VIDEO_GOOGLE_KEY_ENVS`
(rollout default `GOOGLE_API_KEY3,GOOGLE_API_KEY5`) и обязан содержать минимум
два разных key env. Каждый env обязан быть и
runtime secret, и active registry member общего Supabase limiter; отсутствие
любого участника или reserve RPC означает fail closed. Несколько ключей дают
rotation/изоляцию отказов, но их лимиты нельзя складывать, пока не доказано, что
они относятся к независимым provider projects. Поэтому общий feature cap
применяется ко всему run, а не отдельно к каждому ключу.

Если уже проанализированный SHA позднее предлагается новому событию, само видео
повторно модели не передаётся. В rollout v1 cache hit связывается только с
совпавшим сохранённым fingerprint `normalized title + date`; отсутствие такого
совпадения означает fail closed, а не широкое прикрепление ко всем событиям.
Для нескольких связанных event candidates из одного post все relation scores
получаются в исходном единственном video-call.

### Rollout-envelope

Первый production rollout намеренно высокоточный:

- raw file `< 10 MiB`;
- reported Telegram orientation portrait, `0.50 <= width / height <= 0.80`
  (от 9:16 до 4:5);
- минимум `540x960` display pixels;
- длительность `2..60` секунд;
- Telegram video/document attributes идентифицируют ролик, а скачанные bytes
  непусты (полный decode/stream probe — post-rollout этап).

9:16 и safe-zone не являются вкусовой эвристикой: TikTok официально рекомендует
9:16 и минимум 540x960, а Meta отмечает лучшие результаты у Reels 9:16 с audio
и ключевыми элементами в safe zone.
[TikTok In-Feed specs](https://ads.tiktok.com/help/article/tiktok-auction-in-feed-ads),
[Meta Reels guidance](https://www.facebook.com/business/ads/facebook-instagram-reels-ads).
Непопадание в envelope означает `ineligible` текущей версии, а не утверждение,
что исходный ролик некрасив. После появления transcoding policy диапазон можно
расширить отдельной версией.

## Базовые dimensions

Каждый model score — целое `0..100`, кратное 5. Число без короткого
source-grounded `pro` или `con` невалидно. Общие anchors:

| Диапазон | Anchor |
|---|---|
| `0..19` | сломан/непригоден; дефект доминирует |
| `20..39` | явно слабый; публичный показ ухудшает впечатление |
| `40..59` | обычный/компромиссный; заметны существенные недостатки |
| `60..74` | хороший и пригодный, но без сильного craft |
| `75..89` | сильный, намеренно сделанный, уверенно привлекательный |
| `90..100` | исключительный; почти нет существенных недостатков |

Это ordinal anchors, а не «процент красоты». В rollout v1 сохраняется
валидированный raw model score; calibrated score ещё не производится. Будущая
offline calibration обязана сохранить raw неизменным и записывать mapping/version
отдельно, без повторного model-call.

### `T` — technical quality

Разборчивость деталей и движения: focus/clarity, exposure, color fidelity,
noise, compression/banding, frame drops/flicker/stutter, A/V integrity. Grain,
motion blur, low light и handheld не являются дефектом сами по себе, если они
намеренны, последовательны и не скрывают главное.

Rollout v1 детерминированно проверяет фактический размер, display geometry,
минимальное разрешение и длительность. После этих gates `technical_score` равен
валидированному `T_perceptual` модели (целое, кратное 5); отсутствующие FPS и
codec bitrate не выдумываются.

Следующая исследовательская версия может добавить локальный `ffprobe` и blend:

```text
T_det = clamp(100 - resolution_penalty - fps_penalty - bitrate_penalty, 0, 100)
T = round5(0.40 * T_det + 0.60 * T_perceptual)
```

Кандидатные penalties для этой будущей версии:

| Probe | Penalty |
|---|---:|
| `>=720x1280` | `0` |
| `540x960 .. <720x1280` | `10` |
| display FPS `>=24` | `0` |
| FPS `20..<24` | `5` |
| FPS `15..<20` | `20` |
| video bitrate `>=516 kbps` | `0` |
| bitrate `300..515 kbps` | `10` |
| bitrate `<300 kbps` | `20` |

`516 kbps` и `540x960` взяты как platform floor, а не как доказательство
красоты. Codec efficiency и сложность сцены различаются, поэтому такой blend
нельзя включать в acceptance до накопления probe + human evidence и новой
`policy_version`. Отсутствие audio не штрафуется: модель отдельно отвечает
`muted_ok`.

### `V` — visual craft

Оценивает только наблюдаемое исполнение:

- ясный главный объект и визуальная иерархия;
- гармоничная/намеренная композиция без случайной тесноты;
- свет, цвет и тон поддерживают атмосферу, а не маскируют содержание;
- крупность, ракурс и глубина уместны;
- стиль последователен и отличим от случайного footage/template noise.

Нельзя автоматически награждать saturation, bokeh, дорогую камеру или
cinematic bars. Яркая motion-poster графика и честное концертное handheld-video
могут получить высокий `V` разными способами.

### `M` — motion and editing craft

- движение камеры и объектов намеренно и сохраняет главный объект;
- temporal continuity/smoothness без случайных рывков, freeze и flicker;
- темп и длина планов соответствуют содержанию/музыке;
- cuts/transitions помогают, а не отвлекают;
- ролик имеет понятное развитие, сильный opening и не выглядит случайно
  оборванным.

Это главное отличие видеовитринности от «открыточности» фотографии.

### `L` — information legibility

Текст/OCR читаем на телефоне, находится в safe zone, показан достаточно долго,
не обрезан интерфейсом и не противоречит event facts. `muted_ok` означает, что
основной смысл понятен без аудио. Если ролик осознанно не содержит текста и
страница события уже даёт logistics, модель возвращает
`legibility_applicable=false`; отсутствие титров не является дефектом.

### `U` — event usefulness

Ролик даёт зрителю полезное и привлекательное представление программы,
артиста, площадки или атмосферы; специфичен, а не выглядит generic stock;
дополняет афишу/описание и имеет пригодный preview moment. `U` не решает, к
какому именно событию относится видео.

### `R(event, video)` — relation relevance

Отдельная оценка на каждую связь:

- совпадают identity/artist/program/venue и иные source-grounded признаки;
- визуальная/звуковая тема согласуется с announcement;
- нет другой доминирующей рекламы или противоречащих даты/места/названия;
- umbrella/series footage разрешено нескольким связанным событиям, но это
  должно быть явно объяснено.

Название концерта рядом с generic dance footage само по себе не даёт высокий
`R`. Relation evidence хранится вместе со связью, а не глобально у asset.

### Risks — не score

`unsafe_explicit`, `graphic_violence`, `personal_data`, доказанное
`wrong_event`, запрещённый source и отсутствие разрешения на републикацию —
hard block. `third_party_watermark`, `rapid_flashing`, `possible_minor_privacy`,
`dominant_unrelated_brand`, `ocr_uncertain` требуют policy review.

Gemini не устанавливает авторское право. Явный source allowlist,
provenance URL, attribution и takedown/retention policy проверяются отдельно.
Неизвестное право не превращается в `allowed` из-за пустого model risk list.
В текущем rollout `rights_status=allowed` означает только операторскую запись
username в `TG_MONITORING_VIDEO_REPUBLICATION_ALLOWED_SOURCES`; вне списка
pipeline fail closed до скачивания.

## Derived scores и production thresholds

Приложение, не модель, вычисляет:

```text
A = round(0.55 * V + 0.45 * M)                         # aesthetic_score
S = round(0.20 * T + 0.35 * V + 0.25 * M
          + 0.10 * L + 0.10 * U)                      # showcase_score
rank(event) = round(0.75 * S + 0.25 * R(event,video)) # ranking_score
```

При `legibility_applicable=false` вес `L` исключается, а остальные веса `S`
нормализуются на `0.90`. Risks никогда не вычитаются числом: они блокируют или
маршрутизируют решение.

Начальный high-precision gate:

| Решение | Условия после file gates |
|---|---|
| `auto_accept` | `S >= 75`, `A >= 70`, `T >= 55`, `U >= 60`, для связи `R >= 85`, `relation_confidence >= 0.80`, `score_confidence >= 0.80`, rights allowed, hard/soft blocking flags отсутствуют |
| `review` | `S=60..74` или `R=75..84` или confidence ниже `0.80` или есть reviewable flag |
| `reject` | `S < 60`, `A < 50`, `T < 45`, `R < 75`, hard risk/rights block или schema contradiction |

Когда review UI отсутствует, `review` означает fail closed
`analyzed_not_uploaded`, а не скрытый auto-accept. Один accepted asset
загружается в CDN один раз; каждая accepted event relation имеет собственный
`R/rank/reason`. При показе нескольких роликов одного события `rank` задаёт
порядок, а одинаковые ролики/почти одинаковый монтаж не должны вытеснять
контентное разнообразие.

Thresholds — rollout priors, не исследовательская «истина». Они меняются только
через новую `policy_version` после будущей calibration evidence.

## Компактный structured-output contract

Model response не содержит derived scores, final decision, CDN path или rights.
Это исключает арифметические ошибки и попытку модели одобрить публикацию.

```json
{
  "v": 1,
  "description": "Кратко: что происходит и как ролик снят",
  "visible_text": ["точные короткие фрагменты"],
  "tags": ["concert", "stage", "night"],
  "scores": {
    "technical": 80,
    "visual": 85,
    "motion": 75,
    "legibility": 70,
    "usefulness": 85
  },
  "legibility_applicable": true,
  "muted_ok": true,
  "best_frame_sec": 3.0,
  "pros": ["balanced subject and light"],
  "cons": ["small final title"],
  "risk_flags": ["third_party_watermark"],
  "score_confidence": 0.86,
  "events": [
    {
      "id": "candidate-1",
      "relevance": 92,
      "confidence": 0.91,
      "reason": "artist, venue and format agree",
      "contradictions": []
    }
  ]
}
```

Contract limits: `description <= 600` chars; до 12 `visible_text` и 12 `tags`;
до 3 `pros/cons`; строки evidence/reason до 160 chars; event IDs только из
входного enum; score integer `0..100`; confidence `0..1`; `best_frame_sec`
внутри duration; все arrays присутствуют, но могут быть пустыми. Unknown risk
string, missing required field, score вне диапазона, echoed event ID не из input
или contradiction с `visible_text` делают ответ невалидным. Local JSON repair
может исправлять только syntax/whitespace; он не придумывает semantic fields.

`description`, OCR, tags и intrinsic scores кэшируются на asset. `events[]`
разворачиваются в relation records. Rollout v1 сохраняет exact `sha256`,
byte/доступную Telegram stream metadata, model + `analysis_version`,
валидированный model response, application-derived `A/S/R`, rights/source и
timestamps. Provider usage/status остаются в общем limiter ledger
`google_ai_requests`; отдельные calibrated score fields появятся только вместе
с реальной offline calibration.

## Ограничение Gemini и sampling

Gemini понимает visual и audio streams, но по умолчанию visual description
семплируется примерно по 1 FPS; быстрые cuts, мелькающий текст и короткий defect
могут быть пропущены. Официальная документация также рекомендует один video на
request и предупреждает о потере деталей быстрых сцен.
[Gemini Video Understanding](https://ai.google.dev/gemini-api/docs/video-understanding).

Следствия:

- не выдавать `score_confidence >= 0.80`, если ключевые титры видны меньше
  секунды или монтаж явно быстрее sampling;
- при добавлении локальных probes сохранять deterministic
  scene/freeze/stream evidence;
- OCR/дата из единственного быстрого кадра не могут в одиночку одобрить relation;
- `best_frame_sec` считать приблизительной секундой, затем локально проверить и
  извлечь реальный thumbnail.

## Human calibration (post-rollout plan; ещё не включена в v1)

Методика опирается на действующий ITU-T P.910: он поддерживает 5-point absolute
category rating и pair comparison, требует representative stimuli и допускает
task-specific instructions. Базовый диапазон stimulus — 4–20 секунд, при этом
ACR успешно применяется и к более длинным роликам.
[ITU-T P.910 (10/2023)](https://www.itu.int/rec/t-rec-p.910/en).

### Первый набор

1. Не менее 150 unique SHA: `@meowafisha` плюс другие источники; стратификация
   по concert/theatre/exhibition/family/market, live footage/motion poster/
   slideshow, day/night, 9:16/4:5, duration и quality bands.
2. Не менее трёх независимых оценщиков. Смотреть на реальном portrait phone:
   один проход с audio и отдельная проверка muted comprehension.
3. Сначала blind ACR 1–5 для `T/V/M/L/U`, acceptability и каждой relation;
   затем pairwise выбор лучшего ролика внутри одного события, особенно около
   threshold.
4. Дать оценщикам anchor clips и task instruction: «готовы ли вы показать этот
   ролик как медиа конкретного события», а не «нравится ли жанр/артист».
5. Вставить повторные/gold items; считать inter-rater agreement. Низкое
   согласие — `review/subjective`, не удобная ground truth.

### Acceptance calibration

- median/MOS human labels — будущий target; raw model outputs не переписываются;
- monotonic/isotonic mapping к calibrated scores учится offline на cached
  results и не требует новых Flash-Lite calls;
- главная метрика ранжирования — Spearman/Kendall по pairwise/MOS;
- главная safety-метрика — precision `auto_accept` и relation precision, а не
  общий accuracy;
- до расширения gate нужны `auto_accept precision >= 0.90` и relation precision
  `>= 0.95` на holdout, плюс ручная проверка всех первых 25 CDN uploads;
- model/prompt/schema/policy change начинает новый calibration slice. Старый
  cache остаётся audit evidence; re-analysis идёт постепенно в рамках budget.

## Production monitoring

На каждый run и UTC day считать:

- event-confirmed videos, `too_large`, file-ineligible и model-eligible;
- unique SHA, cache hit ratio, model calls/unique SHA и повторные call attempts;
- reserve blocked reason, provider/schema error, input/output tokens;
- distribution `T/V/M/L/U/A/S/R`, accept/review/reject и причины;
- accepted bytes/CDN objects, relation fan-out и exact duplicate reuse;
- audit precision по 10% accepted (не менее 20 за неделю), mismatch и rights
  incidents;
- drift по source, genre, duration, darkness и motion-poster/live-video cohort.

Alerts: `calls > unique uncached SHA`, cache hit резко упал, invalid schema
выше 2%, auto-accept rate/median score сдвинулся более чем на 20% без изменения
source mix, relation mismatch выше 5%, либо CDN upload появился без полностью
успешного limiter ledger + gate evidence.

## Failure modes и обязательные guardrails

- **Genre bias:** тёмный концерт и handheld могут быть выразительны; оценивать
  намеренность и читаемость, не «студийность».
- **Motion poster:** отсутствие live footage не дефект, если графика, timing и
  legibility сильны.
- **Fast edit/OCR miss:** 1 FPS может пропустить главный титр; uncertainty
  снижает confidence и закрывает auto-accept.
- **Score inflation/anchoring:** фиксированные anchors и кратность 5 обязательны
  сейчас; offline calibration и versioned drift monitoring обязательны до
  расширения rollout gate.
- **No audio:** не штрафовать автоматически; `muted_ok` и audio dependence
  хранить отдельно.
- **Recurring/linked events:** intrinsic score переиспользуется, `R` не
  переиспользуется без relation evidence.
- **Same content, new encoding:** raw SHA даёт безопасную idempotency, но другой
  encode может стать новым SHA. Telegram file identifiers и perceptual evidence
  допустимы как shortlist, но не как автоматическая identity без exact proof.
- **Copyright hallucination:** model silence не доказывает rights.
- **Oversize bias:** `>10 MiB` — operational skip, не `reject:ugly`; метрики не
  должны смешивать эти cohorts.
- **Cache confidentiality:** bucket может быть публичным, поэтому permanent
  sidecar содержит только Fernet ciphertext (`application/octet-stream`,
  `private,no-store`). Ключ `TG_MONITORING_VIDEO_ANALYSIS_CACHE_KEY` постоянный,
  попадает в Kaggle только через encrypted secrets dataset и не логируется.
- **Orphan cleanup:** CDN deletion разрешён только после отсутствия всех live
  event relations и минимум 24-hour production grace period; intrinsic
  analysis/provenance ledger при этом остаётся. Production env не может снизить
  окно ниже 24 часов. Нельзя удалить shared object при устаревании одного события.
