# Клубы по интересам — отдельный пострелизный релиз

## Статус

**Planning / research pending.** Это отдельная пострелизная продуктовая фича, а не блокер первой публичной презентации статического сайта.

На 2026-07-17 исследование прошлых и будущих событий **не запускалось**, фактический каталог клубов и их количество ещё не определены. Нельзя утверждать, что найдено `N` клубов, пока не создан воспроизводимый evidence-backed отчёт по [исследовательскому заданию](research-prompt.md).

Каноническая ветка для исследования и последующей консолидации решений:

- `feature/interest-clubs-postrelease`
- [будущая ветка на GitHub](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/interest-clubs-postrelease)

Ветку нужно создать от свежего `origin/main`. Текущая documentation/release-plan ветка не является базой реализации.

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
- фестиваль и его программу — это отдельная [festival identity](../../../features/festivals/static-site-release.md);
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

Публичный UI, схема и URL не считаются утверждёнными до результатов исследования и отдельного product/technology решения.

## Экологичность хранения

- Не дублировать полный текст событий или сырые provider payloads в отдельной постоянной базе клубов.
- Хранить компактную current identity/projection, ссылки на canonical event ids, bounded provenance и версию модели/policy.
- Embeddings и исследовательские матрицы должны иметь явный lifecycle/retention и не раздувать Supabase; bulk history/артефакты не переносятся туда по умолчанию.
- Повторный запуск с тем же входным catalog hash/model/policy должен быть идемпотентным.

## Этапы отдельного релиза

### R0 — каталог-аудит

- [ ] Создать `feature/interest-clubs-postrelease` от свежего `origin/main` и запушить её до длительной работы.
- [ ] Провести read-only аудит прошлых и будущих событий по [готовому prompt](research-prompt.md).
- [ ] Сохранить raw evidence только в `artifacts/codex/interest-clubs-audit-20260717/` и закоммитить компактный воспроизводимый отчёт.
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
- Будущий durable report: `docs/reports/interest-clubs-catalog-audit-2026-07-17.md`
- Raw evidence, не коммитить: `artifacts/codex/interest-clubs-audit-20260717/`
- Общий release plan: [static personal announcements readiness](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
- Smart Update: [canonical docs](../../../features/smart-event-update/README.md)
- Kaggle status contract: [canonical docs](../../../features/kaggle-status-framework/README.md)
