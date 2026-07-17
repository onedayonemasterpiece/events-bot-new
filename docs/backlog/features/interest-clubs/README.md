# Клубы по интересам — отдельный пострелизный релиз

## Статус

**R0 catalog audit delivered; owner/BGE gates open.** Это отдельная пострелизная продуктовая фича, а не блокер первой публичной презентации статического сайта.

Read-only аудит на production-снимке 2026-07-17 сформировал [воспроизводимый отчёт](../../../reports/interest-clubs-catalog-audit-2026-07-17.md): 52 reviewed candidate clusters, из них 20 `confirmed`, 14 `probable`, 8 `needs_evidence` и 10 `rejected`. `confirmed + probable` покрывают 203 уникальных surviving canonical event ids (198 past, 5 future), но это число относится к текущему reviewed pool, а не доказывает полный региональный каталог. Blind discovery-only freeze дал strict exact-name coverage 24/56 post-cutoff events и 0/5 future, name/source candidate coverage 36/56 и 2/5 future; это proxy на researcher labels, не owner-approved population recall. Paired BGE/Gemini quality benchmark на одном frozen corpus и owner review ещё не выполнены, поэтому production/UI GO отсутствует.

После owner feedback СИНЕМАНГО исправлен на `confirmed`. Расширенный controlled stand теперь содержит 48 real event/candidate pairs (24 positive + 24 hard negative). Финальный split-lane verifier сначала проверяет curated canonical source/name identity, без match fail-closes без LLM, а спорную organizer/program semantics отправляет в `gemma-4-31b-it`. Gemma приняла **22/24** positives, дала **0/24** unsafe false positives и 46/48 safe decisions; p50/p95/max provider latency — `1.682/2.242/5.586 s`. Два дополнительных повтора 12 hard cases были полностью стабильны. Все 55 final Gemma calls прошли через Supabase reserve/mark/finalize без limiter/model fallback. Lite controlled shadow ранее дал 4 false positives на 48 cases, поэтому не может автоматически подтверждать relation; final shadow был остановлен самим limiter при `RPD=450/450`. Это ещё не production Smart Update rollout и не заменяет owner-approved gold.

Каноническая ветка для исследования и последующей консолидации решений:

- `feature/interest-clubs-postrelease`
- [ветка на GitHub](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/interest-clubs-postrelease)

Ветка создана от `origin/main@100892d87c56f9fa465c4f10bcb712fda27fbbeb`; текущая documentation/release-plan ветка не использовалась как база.

## Продуктовая цель

На статическом сайте должен появиться раздел **«Клубы по интересам»**, который автоматически выявляет устойчивые клубные сообщества по повторяющимся публичным событиям и пополняется при появлении новых встреч.

Пользователь должен:

1. увидеть понятную подборку реально действующих клубов региона;
2. понять тематику, город, типичное место/ритм встреч и актуальность клуба;
3. перейти к будущим встречам клуба;
4. отличить клуб от одной повторной даты одной программы, фестиваля, площадки или случайной серии похожих событий.

## Рабочее определение

**Клуб по интересам** — устойчивая публичная группа или сообщество вокруг общего интереса, у которой есть несколько встреч в разные даты. Названия, темы и программа отдельных встреч могут меняться, но сохраняется идентичность сообщества и смысловая преемственность.

Не считать клубом автоматически:

- одну программу, повторённую в несколько дат/времён — это [связанные occurrences](../../../features/linked-events/README.md);
- многодневное единичное событие;
- одноразовый мастер-класс, лекцию или экскурсию;
- все события одной площадки или организатора без общей клубной идентичности;
- коммерческую серию концертов/спектаклей/показов только из-за сходства темы;
- фестиваль и его программу — это отдельная [festival identity](../../../features/festivals/README.md);
- дубли одной записи или одного временного слота;
- закрытую частную группу, если нет достаточного публичного event/source evidence.

Финальная taxonomy и граничные случаи должны быть закреплены после аудита в golden-наборе, а не широкими keyword/regex правилами.

## Research-first scope

Первый этап — только read-only исследование прошлых и будущих событий. Он должен дать:

- полный указанный временной интервал, timezone и фактический объём проанализированного каталога;
- список `confirmed`, `probable`, `rejected/false-positive` кандидатов;
- для каждого кандидата: название/нормализованная identity, интерес, число событий, отдельные прошлые и будущие количества, число уникальных дат, первая/последняя дата, ритм, города/площадки, организаторы/источники;
- проверяемые event ids, заголовки и source/public URLs, на которых основан вывод;
- объяснение, почему это клуб, а не linked occurrence, дубль, площадка, фестиваль или разовая серия;
- агрегаты по verdict, тематике, городу, числу встреч и покрытию будущих событий;
- time-split проверку: identity выводится по прошлому периоду и затем проверяется способность распознавать новые/будущие встречи без утечки будущих labels;
- реестр false merge, false split, устаревших и неоднозначных кандидатов.

Исследование не должно менять production DB, публичные страницы, Smart Update, расписания, публикации или очереди.

## BGE/Kaggle — пока гипотеза, не принятое решение

Предпочтительная исследовательская гипотеза — использовать BGE embeddings на Kaggle CPU для candidate retrieval/clustering, если это оправдано объёмом и воспроизводимостью. Это **не утверждённая архитектура**.

До product/technology GO нужно сравнить как минимум:

- простой source/organizer/name/cadence baseline;
- уже имеющиеся в проекте embedding/index возможности;
- BGE candidate lane на Kaggle CPU;
- LLM-first semantic adjudication кандидатов с исходными evidence.

Embeddings, cadence и детерминированная нормализация могут искать кандидатов, но не должны самостоятельно принимать смысловое решение «это один клуб». Финальный автоматический verdict должен быть evidence-grounded, LLM-first и fail closed при неоднозначности.

Критерии сравнения: precision/recall на golden pack, cluster purity, false merge/split, распознавание будущих встреч, устойчивость к разным заголовкам/площадкам, CPU runtime, peak memory, размер артефактов и стоимость регулярного обновления.

## Предварительный to-be contract

После отдельного owner GO предполагаются:

- публичный индекс `/kluby-po-interesam/`;
- при подтверждении пользы — стабильные страницы `/kluby-po-interesam/<club-slug>/`;
- карточка клуба: имя, краткая тематика, город/площадка при их устойчивости, ближайшие встречи, доказуемая активность/свежесть;
- связь `club identity ↔ event occurrences` без перезаписи смысла самого события;
- versioned static projection, changed-only coalesced rebuild, checked artifact, atomic promotion и last-good rollback;
- merge/rename/split policy для identity и стабильных URL;
- автоматическое исключение завершившихся/протухших клубов из актуальной выдачи при сохранении контролируемой истории;
- отсутствие персональных списков участников и недоказанных характеристик аудитории.

Для incremental relation matcher предварительно принят только shadow contract: curated canonical source/name aliases дают `source_match`/`name_match`. Без обоих match candidate сразу fail-closes; `source_match` и name-only organizer semantics идут в отдельные короткие prompts `gemma-4-31b-it` с `thinking=minimal`. Relation создаётся только по `yes` + дословной цитате из bounded packet. Timeout/provider error не переключается в широкий writer и не создаёт relation через Lite; пакет уходит в deferred retry/review.

Публичный UI, схема и URL не считаются утверждёнными до результатов исследования и отдельного product/technology решения.

## Экологичность хранения

- Не дублировать полный текст событий или сырые provider payloads в отдельной постоянной базе клубов.
- Хранить компактную current identity/projection, ссылки на canonical event ids, bounded provenance и версию модели/policy.
- Embeddings и исследовательские матрицы должны иметь явный lifecycle/retention и не раздувать Supabase; bulk history/артефакты не переносятся туда по умолчанию.
- Повторный запуск с тем же входным catalog hash/model/policy должен быть идемпотентным.

## Этапы отдельного релиза

### R0 — каталог-аудит

- [x] Создать `feature/interest-clubs-postrelease` от свежего `origin/main` и запушить её до длительной работы.
- [x] Провести read-only аудит прошлых и будущих событий по [готовому prompt](research-prompt.md).
- [x] Сохранить raw evidence только в `artifacts/codex/interest-clubs-audit-20260717/` и закоммитить компактный воспроизводимый отчёт.
- [ ] Получить owner review фактического списка клубов, counts, taxonomy и false-positive ledger.

### R1 — product/technology decision

- [ ] Утвердить минимальные recurrence/evidence/freshness критерии и публичный scope.
- [ ] Выбрать candidate retrieval/embedding strategy по measured evidence, а не заранее закрепить BGE.
- [ ] Зафиксировать ADR: ownership, identity, update cadence, storage budget, LLM policy, failure/rollback model.

### R2 — модель и shadow pipeline

- [ ] Versioned club identity и event relation без конфликта с linked occurrences/festivals.
- [ ] Идемпотентный incremental candidate → semantic verdict → projection pipeline.
- [ ] Historical replay, time-split golden evaluation и shadow monitoring без public UI.

### R3 — static-site slice

- [ ] Индекс и, если утверждены, detail pages/cards.
- [ ] Changed-only static scheduling, manifest parity, stale/last-good/rollback.
- [ ] Mobile/desktop/no-JS/a11y/SEO-GEO evidence после собственного UI freeze.

### R4 — отдельный RC/canary

- [ ] Zero confirmed false merge в публичном shortlist и все опубликованные relations имеют evidence.
- [ ] Новая встреча подтверждённого клуба автоматически появляется в рамках принятого freshness SLO.
- [ ] Удаление/перенос/merge события не оставляет dangling relation или неверную карточку.
- [ ] Canary, rollback drill и owner sign-off привязаны к exact RC SHA/catalog hash.

## Открытые крупные вопросы

1. Достаточен ли индекс клубов или сразу нужны отдельные страницы identity?
2. Какой minimum evidence делает клуб публичным: число дат, длительность наблюдения, независимые источники, наличие будущей встречи?
3. Показывать ли временно неактивные клубы в архиве и каков max stale age?
4. Кто владеет canonical club identity и merge/split решением?
5. Даёт ли BGE на Kaggle CPU измеримое преимущество над существующим индексом и простым baseline?
6. Нужны ли персональные рекомендации клубов — это отдельный последующий scope, не часть первого club release.

## Роутинг и ожидаемые артефакты

- Исполняемый prompt исследования: [research-prompt.md](research-prompt.md)
- Durable report: [catalog audit 2026-07-17](../../../reports/interest-clubs-catalog-audit-2026-07-17.md)
- Raw evidence, не коммитить: `artifacts/codex/interest-clubs-audit-20260717/`
- Общий release plan: [static personal announcements readiness](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
- Smart Update: [canonical docs](../../../features/smart-event-update/README.md)
- Kaggle status contract: [canonical docs](../../../features/kaggle-status-framework/README.md)
