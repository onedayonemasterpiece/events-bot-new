# Возрастные ограничения событий

Статус: **структурированный data path реализован; публичный режим —
`declared_only`; BGE assessment — автоматический gated batch, shadow до
прохождения quality gates**.

## Контракт продукта

Система различает две сущности:

- **declared** — маркировка `0+ | 6+ | 12+ | 16+ | 18+`, явно указанная
  организатором, площадкой, билетным оператором, официальной страницей или
  event-scoped афишей;
- **assessed** — внутренняя рекомендация модели. Это не утверждение организатора,
  не формальная экспертиза и не юридическое заключение.

Неизвестное значение остаётся `NULL`. Значения по умолчанию `0+` нет. Возраст
участника, условия входа/сопровождения и рейтинг отдельной части программы не
становятся рейтингом всего события.

## Что было до изменения

На `origin/main@926dad8a` поля у `Event` и в static export отсутствовали.
`TheatreEvent.age_restriction` заполняли Qtickets, Pyramida, Дом искусств и
филармония, но `EventCandidate` терял его; обработчик сохранял значение только
в тексте описания. Universal Festival prompt уже запрашивал возраст, однако
вложенные Pydantic-модели UDS отбрасывали поле при `model_dump()`. Повторный
parser-source fast path обновлял билеты и медиа, но не возраст. В UI не было
структурного поля, поэтому безопасно показать возраст без повторного разбора
HTML было невозможно.

## Модель данных

Канонический declared результат находится непосредственно у `event`:

- `age_restriction`, `age_restriction_status`;
- `age_restriction_provenance`, `age_restriction_source_url`, `..._confidence`;
- компактный `age_restriction_evidence` (kind, короткая цитата/span, hashes и
  document id), `..._decision_version`, `..._input_hash`, `..._updated_at`.

Assessed хранится отдельно в `age_assessment*` с
`status/run_id/updated_at/engine/version/hash`. Большие
исходные тексты не копируются. Тексты остаются в существующих `event_source` и
poster OCR; каноническая запись хранит только короткое evidence. При будущей
потребности подробный decision log должен иметь отдельный retention, а не
раздувать строку события.

Provenance declared: `official_structured`, `organizer_text`, `ticketing_text`,
`venue_text`, `poster_ocr`, `manual_override`. Assessed: `llm_assessed`,
`bge_assessed`/`model_assessed`.

## Приоритет и конфликты

1. `manual_override` не перезаписывается автоматикой.
2. Source-native структурированное поле принимается без LLM, но только после
   узкой нормализации допустимого enum.
3. Text/OCR принимает semantic stage только с точной цитатой в исходном
   corpus и подтверждённым scope события/сеанса.
4. Разные declared значения не сворачиваются через `max()`/`min()`. До
   semantic adjudication сохраняются обе evidence-записи, canonical value
   становится `NULL`, status — `conflict`.
5. Рейтинги частей фестиваля сохраняются у элементов `activities_json`; общий
   рейтинг фестиваля из них не вычисляется.

## Smart Update и лимиты LLM

Возраст не открывает отдельный provider request:

1. source-native JSON/parser field → deterministic preservation;
2. extraction + context/scope check → `age_decision` внутри уже обязательного
   facts/create/merge JSON-вызова;
3. при отсутствии declared тот же вызов может сохранить **только внутренний
   single-pass LLM candidate** с evidence; он не публикуется по умолчанию;
4. строгая схема и grounding validator fail closed;
5. изменение structured age входит в effectful update/fingerprint и запускает
   обычные проекции/перестроение.

`SMART_UPDATE_EVENT_AGE_LLM_MODE=piggyback_only` — дефолт. `off` отключает
semantic age, сохраняя структурированные источники. Дополнительных LLM-запросов
на событие: **0**. Backfill также не вызывает LLM; `--max-llm-calls` фиксируется
в отчёте и сейчас всегда даёт `llm_calls_used=0`. Общие запросы Smart Update
остаются под существующими persisted provider budgets/retry caps.

Требование двух независимых LLM-pass + adjudicator намеренно не включено в
массовый путь: при ограниченном ресурсе оно умножает запросы. Пока отдельный
verification contract не профинансирован, single-pass LLM assessment считается
internal candidate, а безопасный публичный режим остаётся declared-only.

## CPU Kaggle / BGE-M3

Альтернатива без LLM-квоты реализована как CPU-only worker
`kaggle/EventAgeBgeAssessment/` и launcher
`kaggle/execute_event_age_bge_assessment.py`.

Переиспользованы проверенные контракты проекта:

- BGE-M3 batch/runtime/hash pattern из Region Talk (исторически находится в
  `origin/agent/region-talk/bge-m3-enrichment-test`);
- стандартный `kaggle_status.py` / `kaggle_status_client.py`, launcher polling и
  callback ledger, как в Telegram Monitoring;
- phase/progress/alive heartbeat и bounded runtime, как в Telegram Monitoring и
  CherryFlash.

### Запуск batch

После каждого Smart Update события без declared rating один глобальный
`JobOutbox` job с coalesce key `event_age_bge_assessment:prod` переносится на
25 минут после последнего обновления. Таким образом пачка набирается в quiet
window, а не создаёт Kaggle run на событие. При старте приложения такой же job
однократно seed-ится для исторических `not_scheduled` событий. Selector повторно
проверяет:

- active, non-silent, canonical event;
- нет declared rating и unresolved conflict;
- assessment отсутствует либо corpus hash устарел;
- обязательный OCR уже готов либо его двухчасовое окно завершилось явным
  `ocr_unavailable`.

Если первый batch видит `ocr_pending`, он обязательно создаёт durable recheck
через 30 минут; поэтому OCR wait не может оставить событие в вечном pending.

Batch имеет лимит, bounded runtime, unique output namespace, status ledger,
resource lease, heartbeat и partial follow-up. Длинный age job не считается
stale по общему 10-минутному порогу. Перед импортом снова проверяются declared
value, corpus hash, pinned model/encoder/head hashes. Поэтому поздний официальный
рейтинг или изменившийся текст не перезаписываются устаревшим Kaggle result.

### OCR как обязательная часть corpus

В Smart Update уже существующий facts-call получает `ocr_title + ocr_text` до
8 постеров (0 дополнительных LLM calls). BGE corpus помещает OCR **перед**
description/source text, чтобы max-length truncation не срезал знак на афише.
Берутся только approved event-scoped media roles; rejected/foreign poster OCR
не допускается. Состояния покрытия: `not_applicable`, `complete`, `pending`,
`terminal_unavailable`. Последнее означает наблюдаемую техническую
невозможность OCR, а не выдуманный возраст.

### Автокалибровка без человека

Человек исключён из approval-схемы. `scripts/build_event_age_bge_calibration.py`
строит private corpus из source-declared gold, маскируя явные age tokens до
embedding, чтобы модель не «сдавала экзамен по подсказке». Kaggle экспортирует
hash-bound event vectors. `scripts/calibrate_event_age_bge.py` делает
grouped train/calibration/official-holdout split, обучает два deterministic
bootstrap ridge-head и выводит пороги из calibration split.

Production gate создаётся автоматически и требует exact совпадения model
revision, encoder contract, prototype bank и classifier SHA-256, минимальный
support каждого класса, ноль severe under-rate, предел общего under-rate,
exact/within-one accuracy и accepted coverage. `ai_consensus_silver` разрешён
только для обучения при точном согласии Codex+Gemini; official holdout он не
заменяет. Полей `approved_by`/ручного review нет. Если support/метрики не
достигнуты, manifest остаётся `shadow`, worker выдаёт terminal
`insufficient_evidence`, а не угадывает по nearest prototype.

Активный classifier дополнительно pin-ится во Fly через
`EVENT_AGE_BGE_CLASSIFIER_SHA256`. Model revision, prototype bytes, corpus
contract и этот SHA входят в `assessment_policy_version`: смена любого из них
автоматически делает прежний terminal result stale и возвращает событие в batch.

### Инвариант «нет пропавшего рейтинга»

У каждого события должен быть наблюдаемый terminal outcome:

- source-declared `N+`;
- service-assessed `N+` (отдельно, непублично по умолчанию);
- `conflict`/scoped-only;
- `insufficient_evidence` или `ocr_unavailable`.

Это гарантирует отсутствие вечного silent/pending состояния. Инвариант **не**
означает принудительный numeric default: при недостатке данных `NULL` безопаснее
ложного `0+` или завышенного/заниженного значения.

### Реальный Kaggle CPU canary 2026-07-15

На private dataset запущены 12 актуальных событий без явного age token: 6 с
approved OCR и 6 без доступного OCR. Pinned BGE-M3 revision
`5617a9f61b028005a4858fdac845db406aefb181`, CPU-only, `12/12` обработаны,
checkpoint/result/event vectors/prototype vectors выгружены. Первый kernel run
завершился за 191.213 s и корректно дал 12 abstentions без classifier.

Глазной разбор текстов и Gemini Pro canary review обнаружили semantic collapse
абстрактных law anchors: например, выставка ошибочно получала ближайшим
`age18-drugs-cruelty`, а экосубботник — age/trap anchors. Prototype bank v3
переведён на конкретные content examples и **безвозрастные** neutral-context
anchors (они не назначают label). Повторный standard-run через общий
status/heartbeat launcher завершился `complete` (`12/12`; kernel 298.295 s,
launcher 345.026 s): выставка получила `neutral-art-exhibition=0.551`,
мастер-класс `neutral-creative-workshop=0.628`, экосубботник
`neutral-eco-volunteer=0.579`, концерты `neutral-classical-concert=0.576–0.627`.
Все 12 по-прежнему безопасно abstained: реальный canary улучшил retrieval
evidence, но не заменил official-holdout classifier gate.
Private input dataset после сохранения результатов удалён; canonical private
kernel оставлен для следующих hash-bound batch runs.

Ignored evidence:
`artifacts/codex/event-age-rating-auto-calibration-2026-07-15/event-age-bge-canary-20260715t1128z/`.

## Public projection

`STATIC_EVENT_AGE_POLICY`:

- `declared_only` (default) — экспортирует только declared;
- `assessment_internal_only` — эквивалентно безопасному публичному поведению,
  assessment остаётся внутри;
- `declared_or_assessed_labeled` — только после отдельного решения экспортирует
  `age_recommendation` и строку «Рекомендуемый возраст: N+ — оценка сервиса».

`PreviewEvent`, discovery payload и vector card snapshot получают структурные
поля. Export/UI не разбирают `description_html`.

## Нормативный rubric

Проверено 2026-07-15. Рабочая редакция 436-ФЗ на дату проверки — редакция от
29.12.2025. Закон относит классификацию к производителю/распространителю и
требует учитывать тему, жанр, содержание, оформление, особенности восприятия и
риск вреда; категории заданы статьями 6–10. Для зрелищных мероприятий знак и
правила оборота описаны статьями 11–12.

Источники:

- [436-ФЗ, текущая редакция](https://www.consultant.ru/document/cons_doc_LAW_108808/);
- [статья 6](https://www.consultant.ru/document/cons_doc_LAW_108808/6fca9c26908d9ea57af1348f34c9630d15533d16/);
- [статьи 7](https://www.consultant.ru/document/cons_doc_LAW_108808/07194a696bee4a97dd25ff31550a995809e343c6/),
  [8](https://www.consultant.ru/document/cons_doc_LAW_108808/961b749b665184f25f920d0ff4d628e92df77aa5/),
  [9](https://www.consultant.ru/document/cons_doc_LAW_108808/360c3e68c68d0e9fc19c65a9b434ae18a8db56f1/),
  [10](https://www.consultant.ru/document/cons_doc_LAW_108808/2025d6422eaf7de29d5df80a9eab6a678833b9c2/);
- [статья 11](https://www.consultant.ru/document/cons_doc_LAW_108808/b6ad93377c891ecf889a0b6262cb45ef1cd2479a/)
  и [статья 12](https://www.consultant.ru/document/cons_doc_LAW_108808/553f0edac652ab327b379960f12fa3f0cbdd65d6/);
- исходные official mirrors из постановки: [pravo.gov.ru](https://pravo.gov.ru/proxy/ips/?docbody=&nd=102144583),
  [Роскомнадзор PDF](https://rkn.gov.ru/docs/Zakon_RF_ot_29.12.2010_n_436-FZ_red._ot_28.04.2023_21122023.pdf),
  [материал 1](https://rkn.gov.ru/docs/Prilozhenie_1.pdf),
  [материал 2](https://rkn.gov.ru/docs/Prilozhenie_2.pdf),
  [рекомендации](https://34.rkn.gov.ru/directions/p5884/p16183/).

В engineering rubric перенесены: whole-product context; натуралистичность и
длительность насилия; страх/ужас; смерть, болезни, аварии; противоправное и
антиобщественное поведение; наркотики/алкоголь/табак; сексуальный контент;
лексика; осуждение/оправдание; образовательный, исторический и художественный
контекст; последствия и положительное разрешение. Это инженерная интерпретация,
не экспертная классификация по закону.

## Аудит 2026-07-15

Read-only production audit: 284 active canonical events; age columns отсутствуют.
Кандидатные токены найдены у 99 (34.9%) событий: source rows — 75 событий,
poster OCR — 47, пять имеют несколько разных значений. Candidate-only группы
(не юридическая разметка): consistent source 70, conflict 5, poster-only 23,
missing-assessable 183, missing-insufficient 2, очевидные unrelated-context
candidates 18; `projection_lost=99`. Группы false-positive/projection являются
пересекающимися контрольными срезами.

Full snapshot dry-run: 6 485 rows, 0 writes и 0 LLM calls. Candidate groups:
717 consistent, 230 conflict, 618 poster-only, 17 description-only,
3 023 missing-assessable, 1 880 insufficient. Полные обезличенные отчёты и
snapshot находятся в ignored `artifacts/codex/event-age-rating-audit-2026-07-15/`.

## Backfill и эксплуатация

```bash
python scripts/backfill_event_age_ratings.py \
  --db snapshot.sqlite --output artifacts/codex/age-plan-1.json \
  --checkpoint artifacts/codex/age-checkpoint.json --batch-size 250 \
  --max-llm-calls 0
```

Default открывает SQLite `mode=ro`. `--apply` требует hash-bound decision plan,
совпадение catalog/input hashes и мигрированную схему. Production write без
отдельного подтверждения запрещён. Для evaluation:

```bash
python scripts/evaluate_event_age_golden.py \
  --fixture tests/fixtures/event_age_rating_golden.json \
  --predictions predictions.json --output artifacts/codex/age-eval.json
```

Мониторить: breakdown status/provenance, pending age старше SLA,
`ocr_unavailable`/conflict share, declared
precision/recall, false-positive rate, BGE/LLM agreement, critical
over-permissive rate, LLM calls/event, stale input hashes, Kaggle heartbeat age
и partial runs.

## Нерешённые gate-вопросы

- накопить достаточный official source-declared holdout и пройти автоматические
  quality gates по каждому классу; до этого classifier остаётся shadow;
- юридическое/продуктовое решение о публичном показе assessment;
- отдельная модель для entry/audience/accompaniment constraints;
- независимая проверка LLM assessment, если когда-либо разрешат больше нуля
  дополнительных запросов.
