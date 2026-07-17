# Prompt: исследование клубов по интересам и подготовка следующего этапа

Скопируй весь блок ниже в новое окно кодового агента.

---

Ты работаешь в репозитории `onedayonemasterpiece/events-bot-new` над отдельной пострелизной фичей **«Клубы по интересам»**.

## Ветка и границы работы

Работай только в ветке:

`feature/interest-clubs-postrelease`

GitHub URL после первого push:

`https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/interest-clubs-postrelease`

Если ветки ещё нет:

1. прочитай `AGENTS.md` и `docs/operations/repository-workflow.md`;
2. выполни `git fetch origin --prune`;
3. создай чистую branch/worktree от актуального `origin/main`, а не от чужой dirty или release-plan ветки;
4. запушь ветку в origin до длительного исследования;
5. зафиксируй base SHA в отчёте.

Канонический feature brief уже находится в:

`docs/backlog/features/interest-clubs/README.md`

Сначала прочитай также:

- `docs/README.md` и `docs/routes.yml`;
- `docs/features/linked-events/README.md`;
- `docs/features/festivals/static-site-release.md`;
- `docs/features/smart-event-update/README.md`;
- `docs/features/kaggle-status-framework/README.md`;
- `docs/llm/request-guide.md`;
- `docs/operations/runtime-logs.md`, только если понадобится анализ фактических scheduled/Kaggle прогонов.

## Цель текущего этапа

Провести **воспроизводимое read-only исследование** прошлых и будущих событий и ответить доказательно:

1. какие устойчивые клубы по интересам реально представлены в данных;
2. сколько клубов найдено по каждому уровню уверенности;
3. сколько прошлых и будущих событий связано с каждым клубом;
4. какие false-positive классы мешают автоматическому выявлению;
5. насколько BGE на Kaggle CPU перспективен как candidate-retrieval/clustering слой по сравнению с простыми и уже существующими baseline;
6. какие продуктовые и технологические решения нужно принять до реализации.

**На этом этапе не реализуй production pipeline, публичные страницы, scheduler, DB migrations или Smart Update integration.** Остановись после исследовательского отчёта, measured comparison и handoff следующего этапа.

## Определение объекта исследования

Рабочее определение: клуб по интересам — стабильная публичная группа/сообщество вокруг общего интереса, у которой есть несколько встреч в разные даты. Темы и названия встреч могут меняться, но сохраняется identity и смысловая преемственность сообщества.

Обязательно отделяй клуб от:

- одной программы в нескольких датах/времени (`linked occurrences`, M6);
- дублей одного события/слота;
- многодневного единичного события;
- случайной серии схожих мастер-классов/лекций/экскурсий;
- всех событий одной площадки или организатора;
- коммерческой серии концертов/спектаклей/показов;
- фестиваля и фестивальной программы;
- закрытой частной группы без достаточного public/source evidence.

Не превращай это определение в широкий keyword/regex classifier. Детерминированные признаки допустимы для нормализации и candidate recall, но смысловой verdict должен оставаться LLM-first, evidence-grounded и fail closed.

## Phase 0 — карта данных и воспроизводимость

До анализа:

1. Найди фактические production/core event tables, поля источника/организатора/площадки/дат/status, linked-event relations, embeddings/vector sidecars и существующие LLM/duplicate/related pipelines.
2. Зафиксируй exact base SHA, schema snapshot, DB snapshot/read timestamp, timezone и выбранные past/future окна.
3. Объясни, какие строки входят в scope и почему: current/future, завершённые исторические, cancelled/postponed, silent/quarantined, festival programme, duplicate/tombstone.
4. Для production SQLite используй проектный `fly-prod-db-access` workflow и только read-only snapshot/query. Не изменяй production DB.
5. Создай raw artifact root:
   `artifacts/codex/interest-clubs-audit-20260717/`
   Он не коммитится.

Если данных недостаточно для честного вывода, не выдумывай counts: опиши пробел и минимальный способ его закрыть.

## Phase 1 — полный каталог кандидатов

Сначала построй high-recall candidates из сочетания доступных сигналов:

- source/organizer/community identity и ссылки;
- устойчивые именованные сущности в title/description;
- повторяемость на разных уникальных датах;
- city/venue continuity и допустимая миграция площадки;
- semantic similarity встреч при меняющихся темах;
- существующие embeddings/related-event evidence;
- BGE candidate embeddings, если они действительно нужны для recall.

Далее проведи source-grounded LLM/human-verifiable adjudication каждого кандидата. Для каждого принятого/вероятного/отклонённого кластера сохрани достаточный evidence, чтобы другой агент мог воспроизвести verdict.

## Обязательная таблица результатов

Для каждого кандидата отчёт должен содержать:

| Поле | Требование |
|---|---|
| `candidate/normalized club name` | Человекочитаемое имя и предлагаемая identity |
| `interest/category` | Фактическая тематика без чувствительных пользовательских inference |
| `verdict` | `confirmed`, `probable`, `rejected`, `needs_evidence` |
| `confidence` | Калиброванное значение/уровень с объяснением |
| `events_total/past/future` | Отдельные точные counts |
| `distinct_dates` | Не число строк, а уникальные встречи/даты после duplicate handling |
| `first/last date`, `cadence` | Наблюдаемая история и ритм без придуманного расписания |
| `cities/venues` | Стабильные и меняющиеся locations |
| `organizers/sources` | Provenance и cross-source aliases |
| `event evidence` | Canonical event ids, titles, dates, source/public URLs |
| `club rationale` | Почему сохраняется identity между разными встречами |
| `confounders` | Linked occurrence, duplicate, venue-series, festival, one-off и др. |
| `future recognition` | Был ли кандидат корректно найден в holdout future period |

Не включай секреты, токены, персональные данные и закрытые source payloads в коммит.

## Обязательные агрегаты

Посчитай и объясни denominator для:

- `confirmed`, `probable`, `needs_evidence`, `rejected` club candidates;
- unique canonical events covered, отдельно past и future;
- candidates с `>=2`, `>=3` и выбранным исследованием числом уникальных встреч;
- распределение по интересам, городам и степени свежести;
- долю будущих событий, для которых past-derived model/policy узнала club identity;
- false merge, false split, duplicate leakage, linked-occurrence leakage, venue/festival leakage;
- source coverage и долю кандидатов, зависящих только от одного слабого evidence.

Любое утверждение «найдено N клубов» должно ссылаться на machine-readable ledger и понятные критерии.

## Phase 2 — честная time-split оценка

Не оценивай качество на тех же данных, на которых сформированы identities:

1. Используй прошлый период для discovery/identity hypotheses.
2. Заморозь candidate policy/thresholds.
3. Проверь следующий/будущий период как holdout.
4. Измерь precision/recall или, если полного gold denominator нет, явно названные proxy metrics и coverage.
5. Проведи обязательную визуальную/source проверку false merge/split и неоднозначных кластеров.

Сформируй минимальный versioned golden fixture только из допустимых, редактированных примеров. Не коммить production dump.

## Phase 3 — BGE/Kaggle CPU feasibility, не выбор архитектуры

BGE — исследовательская гипотеза, а не заранее принятое решение.

Сравни:

1. deterministic/source/name/cadence baseline;
2. уже существующий в репозитории embedding/vector baseline;
3. один обоснованный BGE candidate lane на Kaggle CPU;
4. LLM-first adjudication поверх кандидатов.

Оцени:

- candidate recall и accepted-cluster precision/purity;
- false merge/split;
- future-event recognition;
- устойчивость при изменении title/topic/venue;
- CPU wall time, peak memory, artifact/model size;
- incremental update feasibility и storage cost;
- воспроизводимость по catalog/model/policy hashes.

Если требуется Kaggle run, используй существующий project status/callback/heartbeat protocol, уникальный run id и сохрани evidence. Не запускай публикацию, mutation pipeline или произвольный production scheduler. Не хардкодь BGE family/model до аудита имеющихся зависимостей и актуальной официальной документации.

После двух похожих неудачных попыток с внешним SDK/model/runtime прекрати trial-and-error и сверь официальный contract/documentation перед следующей попыткой.

## Stop gate и deliverables

Текущий этап завершается только следующими материалами:

1. `docs/reports/interest-clubs-catalog-audit-2026-07-17.md` с методикой, точным окном, counts, полным club shortlist, rejected/confounder examples, time-split результатами, ограничениями и рекомендациями;
2. raw machine-readable ledgers/logs под `artifacts/codex/interest-clubs-audit-20260717/` — не коммитить;
3. при необходимости минимальные redacted fixtures в `tests/fixtures/`, только если они безопасны и действительно нужны для воспроизводимости;
4. обновления `docs/backlog/features/interest-clubs/README.md`, `docs/routes.yml`, `docs/README.md` и `CHANGELOG.md`, если исследование уточнило contract;
5. крупные открытые product/technology вопросы и proposal следующего этапа;
6. branch, commit SHA, push/PR URL и команды валидации.

Не переходи без нового явного решения владельца к:

- production DB/schema writes;
- публичному `/kluby-po-interesam/`;
- automatic Smart Update/Kaggle scheduling;
- персональным club recommendations;
- массовому backfill или публикации;
- выбору BGE как production architecture только потому, что prototype запустился.

После owner review сохрани research report и будущий ADR в этой же feature-ветке для единого контекста; production implementation затем разложи на небольшие current-main-based PR/slices с явными зависимостями и обратным слиянием в `main`.

---
