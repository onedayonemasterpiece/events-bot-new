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

## Решение второй консультации: no-human / OCR / no-missing

Gemini поддержал следующую безопасную интерпретацию требований:

- «нет отсутствующего рейтинга» означает terminal state для каждого события,
  но не выдуманный numeric default при недостатке evidence;
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
