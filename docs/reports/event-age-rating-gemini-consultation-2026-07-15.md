# Gemini Pro consultation: event age rating

Дата: 2026-07-15. Консультант вызван через локальный agy wrapper `a-gemini`;
provider route сообщил Gemini 3.1 Pro (High). Полные prompt/response и stderr
сохранены в ignored artifact
`artifacts/codex/event-age-rating-audit-2026-07-15/`.

Вторая консультация после уточнения пользователя сохранена в
`artifacts/codex/event-age-rating-auto-calibration-2026-07-15/`
(`gemini-brief.md`, `gemini-pro.raw.md`, `execution-matrix.md`). Она также
выполнена через `a-gemini` и route Gemini 3.1 Pro (High).

## Входные ограничения

- Kaggle только CPU;
- BGE pipeline уже проверен в Region Talk;
- launch/status/heartbeat contracts уже проверены Telegram Monitoring и
  CherryFlash;
- LLM-запросы строго лимитированы, запросов на событие должно быть минимум;
- regex не решает семантику; конфликт не сворачивается в максимальный возраст;
- assessed отделён от declared и не публикуется по умолчанию.

## Принято

- hybrid pipeline: source-native structured fact → semantic context check →
  local vector/model shadow → bounded fallback;
- versioned hashes для input, rubric, model и prototype bank;
- abstention как штатный результат;
- batch caps, runtime guard, partial checkpoint и heartbeat;
- отдельные метрики over-permissive error и coverage.

## Уточнено/отклонено интегратором

Первый ответ предлагал broad regex semantics, nearest-prototype thresholds и
выбор максимума при конфликте. Это противоречит контракту задачи. В follow-up
консультант принял ограничения: regex только candidate retrieval, unresolved
conflict → `NULL`, CPU-only reuse проверенных project paths, one-call upper
bound. Даже после уточнения proposal оставался слишком уверенным в direct
prototype labels, поэтому в реализации nearest BGE prototype — только retrieval
evidence. Итоговый assessed candidate требует автоматически gated calibrated dual-head
artifact; иначе worker abstains.

## Итоговое решение

Дополнительные online LLM calls/event = 0: age JSON piggybacked в существующий
Smart Update call. Source-native values вообще не используют LLM. CPU Kaggle BGE
запускается отдельным bounded batch и не пишет declared value. Публичный default
— `declared_only`.

## Решение второй консультации: no-human / OCR / terminal completeness

Gemini поддержал следующую безопасную интерпретацию требований:

- на том этапе «нет отсутствующего рейтинга» было интерпретировано как terminal
  state для каждого события, но не выдуманный numeric default при недостатке
  evidence;
- OCR — readiness dependency и first-class часть corpus, а не необязательное
  дополнение после text inference;
- один durable coalesced outbox job запускает missing-only batch после quiet
  window, с follow-up для remainder;
- no-human approval заменяется hash-bound automatic evaluation manifest;
- source-declared gold с замаскированным знаком возраста — production holdout,
  а Codex+Gemini consensus может быть только training silver;
- canary, stale-hash rejection и rollback/shadow gate обязательны.

Интегратор принял эти пункты. Отдельный предложенный консультантом outbox не
создавался: существующий `JobOutbox` уже даёт durable coalescing/retry и
уменьшает число новых сущностей.

Эта интерпретация **не является ответом на более позднее буквальное уточнение
владельца о 100% numeric fill rate**. Оно отдельно проверено в итерации 4 ниже.

## Canary follow-up

После первого реального CPU run Gemini Pro получил 12 конкретных event corpus,
OCR coverage, nearest neighbors и гипотезы Codex. Полный ответ:
`event-age-bge-canary-20260715t1128z/gemini-canary-pro.raw.md` в том же ignored
artifact root.

Принято: диагноз semantic collapse абстрактных law anchors, переход к
event-like формулировкам, neutral anchors для выставок/экоакций/мастер-классов/
лекций/концертов и сокращение общих trap-текстов. Намеренно не принято:

- вставлять literal `12+`/`16+`/`18+` в prototype text — это label leakage;
- автоматически считать craft/history neutral event конкретным numeric class;
- объявлять Codex+Gemini silver «official holdout» — он остаётся training-only,
  official holdout состоит только из source-declared gold;
- gate по nearest-prototype Hit@1 — numeric решают calibrated dual heads, а
  retrieval остаётся объясняющим evidence.

Prototype v3 повторно проверен на тех же 12 событиях: ложные age18/age16 top-1
для безопасных выставки/экоакции/мастер-класса исчезли, корректные neutral
anchors стали top-1, а отсутствие approved classifier сохранило 12/12
abstentions.

## Итерация 3: acceptance после полного цикла

После dense BGE/ordinal/hybrid неудач консультант получил untouched grouped OOF
результат lexical safety cascade: coverage `51.41%`, exact `95.97%`, within-one
`99.27%`, under `1.47%`, severe-under `0`. Gemini 3.1 Pro High подтвердил
`PASS` исходного gate и дал итоговый `CONDITIONAL PASS` на внутренний rollout.
Условия: не использовать pickle/joblib, экспортировать raw vocabulary/IDF/
матрицы, а при старте сверять deterministic text→logits→decision cases. Оба
условия реализованы в calibrator/worker. Diagnostic child severe-over `7.41%`
консультант не признал blocker для internal assessed-only при неизменном
`declared_only`. Предложенную human audit queue интегратор отклонил как
противоречащую явному no-human контракту. Полный brief/ответ: ignored artifacts
`gemini-iteration-3-brief.md` и `gemini-iteration-3-review.raw.md`.

## Итерация 4: буквальный no-missing gate

После production batch стало видно, что terminal completeness нельзя выдавать
за numeric completeness: из первых 64 строк classifier принял 5, 46 завершил
абстенцией, ещё 13 были защищены поздно появившимся declared value. По прямому
уточнению владельца это неудовлетворительный результат.

Проверен forced-prediction режим на том же untouched grouped OOF seed:

- raw prediction для всех 531: coverage `100%`, exact `73.82%`, within-one
  `93.22%`, under `9.60%`, severe-under `0.188%` (1 событие);
- `+1` для всех rejected: exact `63.65%`, within-one `86.82%`, under `1.88%`,
  severe-under `0`, но child severe-over `43.52%`;
- dev-only ordinal/category guardrail sweeps не достигли одновременно
  `exact >= 72%`, `within-one >= 95%`, `under <= 10%`, `severe-under = 0` при
  100% coverage.

Gemini 3.1 Pro High дал текущему 51% numeric design буквальный вердикт `FAIL`.
Рекомендован каскад declared → estimate внутри уже существующего Smart Update
call → high-confidence BGE → conservative terminal prior, с раздельным
provenance и неизменным public `declared_only`. Первые три ступени уже есть.
Четвёртая намеренно не включена: простое forced/default значение не прошло
independent gate и либо допускает severe under-rating, либо тяжело завышает
детские события. Это quality blocker, а не основание подменить `NULL` ложной
точностью. Полный brief/ответ: ignored artifacts
`gemini-iteration-4-brief.md` и `gemini-iteration-4-review.raw.md`.

Полный production sweep после консультации обработал `291/291` current/future
events до terminal outcome: 52 declared, 18 assessed, 99
`insufficient_evidence`, 122 `ocr_unavailable`, 0 nonterminal. Literal numeric
fill составил `70/291 = 24.05%`; среди 239 событий без declared BGE принял
`18/239 = 7.53%`. Поэтому итоговый `FAIL` для no-missing подтверждён не только
OOF, но и полным live срезом; половинчатый результат не маркируется завершённым.
