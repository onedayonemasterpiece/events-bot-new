# Промпт независимому внешнему консультанту: продуктовый аудит Region Talk

Ты — независимый senior reviewer по product/data pipelines. Нужен **второй, самостоятельный аудит**, а не пересказ уже полученной консультации Gemini. Проверяй код и контракты сам, различай текущий продуктовый результат, исторические ledger-строки, техническую активность и гипотезы.

## Репозиторий

- Репозиторий: https://github.com/onedayonemasterpiece/events-bot-new
- **Аудируемая ветка:** [https://github.com/onedayonemasterpiece/events-bot-new/tree/agent/region-talk/R04-live-canary](https://github.com/onedayonemasterpiece/events-bot-new/tree/agent/region-talk/R04-live-canary)
- Commit-pinned HEAD: [19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c](https://github.com/onedayonemasterpiece/events-bot-new/commit/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c)
- Важно: не анализируй `main` вместо этой ветки. Ветка на момент аудита имела 358 branch-only commits и 808 main-only commits относительно merge-base; Region Talk diff — около 64k строк в 97 файлах.

## Продуктовый вопрос

За примерно 12 дней непрерывной разработки pipeline «О Калининграде говорят» не достиг цели 20 актуальных согласованных постов, хотя в YDB внесён проверенный shortlist внешних блогеров/микроблогеров и evidence-ссылки на их публикации о Калининградской области. Нужно найти не только code bugs, но и организационные причины: неправильную objective function, allocation, stage gates, повторную обработку, ручные sinks, misleading metrics и чрезмерную сложность.

## Надёжные факты на 2026-07-15

- Source rows: 7,587–7,588; primary-unscanned pending: 6,160; terminal processed: 360; rescan/retry: 396.
- Verified external blogger cohort: 135 supported TG/VK sources; 91 scanned, 43 unscanned; 34 дали KO signal.
- Cohort funnel: 613 processed posts -> 66 dual/vector accepts -> 37 image/publication rows -> **4 current Gemini-confirmed**.
- Общая воронка: 12,075 unique processed posts; 811 real candidate-memory rows after pruning checkpoint-only phantom audit materialization; 146 image rows; 67 actual-scored; 100 publication-ledger URLs; 37 active; **4 current confirmed; 0 ready**.
- Backlogs/sinks: 44 image visual-review pending; 32 publication visual-review pending; 4 text-restore pending; 9 rejected.
- Historical counters are not current success: `sent=22`, but durable delivery ledger has 15 rows and current Gemini-confirmed only 4.
- Последние completed CandidateReport runs: `4 fetched / 0 first-seen / 4 reprocessed` (3 policy refresh); перед ним `22 fetched / 0 first-seen / 11 policy refresh / 10 runtime-deferred`; Jul-13 canary `33/33 refreshed-known / 0 first-seen`.
- Latest keyword cycle: 4 queries -> 27 raw hits / 27 link writes -> 0 new nonlocal sources.
- Текущий exact-link ledger около 991–1,001 rows, **но это не 1,000 actionable backlog**: snapshot показывает 985 terminal, 0 pending/ready, 6 unknown/integrity-blocked, 642 keyword + 347 hashtag. Это прежде всего evidence о низком yield и ledger growth; проверь, действительно ли он продолжает отнимать runtime.
- Latest run reported 0 primary scans while verified primary backlog existed; recent commit `73885b75` claims to fix it, but post-fix live acceptance отсутствует.
- `_progress_signature()` действительно включает все числовые метрики; оцени, может ли operational churn постоянно считаться прогрессом и блокировать нужный failover/reallocation.
- Новая проверка requested KPI: **525 / 12,075 = 4.35%** unique processed posts получили canonical `kaliningrad_oblast_only_scope=true` до ad/news/substance/media/Gemini-фильтров (43.5 на 1,000). При этом текущий scope/vector contract был явно применён только к 3,563 / 12,075 = 29.51% строк; среди evaluated конверсия в KO scope = 14.74%. Отдельный raw lexical lower-bound: 385 / 12,075 = 3.19%, но historical text/flag coverage mixed. Проверь, насколько низкий end-to-end yield объясняется плохим source allocation, а насколько неполным evaluation coverage.
- Историческое имя `CandidateReport` вводит в заблуждение: основной файл — acquisition/vector/YDB worker. Собственно XLSX/CSV/JSON/MD/HTML tail занимает лишь финальную часть и теперь отключается в автоматическом orchestration через `REGION_TALK_WRITE_REPORT_ARTIFACTS=0`; manual/offline export сохраняется. Проверь, достаточно ли этого или worker следует физически разделить позже.

## Обязательные файлы

- [Предыдущий системный аудит 9 июля](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/reports/region-talk-system-audit-2026-07-09.md)
- [Feature README и актуальные invariants](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/features/region-talk-channel/README.md)
- [Source discovery / verified blogger lane](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/features/region-talk-channel/source-discovery.md)
- [Orchestration to-be/current contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/features/region-talk-channel/orchestration-to-be.md)
- [Publication queue](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/features/region-talk-channel/publication-queue.md)
- [YDB schema](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/features/region-talk-channel/ydb-schema.md)
- [Image false-negative audit](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/docs/features/region-talk-channel/image-scoring-false-negative-review.md)
- [CandidateReport implementation](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py)
- [Orchestrator](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/scripts/region_talk_orchestrator.py#L2820-L2890)
- [Publication finalizer](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/scripts/region_talk_publication_finalizer.py)
- [ImageDiagnostic](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py)
- [BGE-M3 worker](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/kaggle/RegionTalkBgeM3Enrichment/region_talk_bge_m3_enrichment.py)
- [CandidateReport tests](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/tests/test_region_talk_candidate_report.py)
- [Orchestrator tests](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/tests/test_region_talk_orchestrator.py)
- [Finalizer tests](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/tests/test_region_talk_publication_finalizer.py)
- [ImageDiagnostic tests](https://github.com/onedayonemasterpiece/events-bot-new/blob/19de9abf96ac6ea040e2c3e4e378fdf4f786ec0c/tests/test_region_talk_image_diagnostic.py)

Особенно проверь историю/диффы commits: `14479495`, `88f1de2b`, `16a71612`, `19eea513`, `00a8dd23`, `88da0089`, `002aa310`, `e047ff25`, `30504619..755f4172`, `7bb5fc79`, `73885b75`, `b9097818`, `88d156ef`, `19de9abf`.

## Результат первой консультации Gemini

Отдельный GitHub-документ: [полный аудит Gemini](https://github.com/onedayonemasterpiece/events-bot-new/blob/agent/region-talk/R04-live-canary/docs/reports/region-talk-effectiveness-gemini-audit-2026-07-15.md). Полный текст также встроен ниже, чтобы handoff оставался самодостаточным.

Ниже полный ответ Gemini 3.1 Pro High. Считай его **review input, а не ground truth**. Обязательно проверь его по коду и цифрам. В частности:

1. Не является ли утверждение «100% Telegram budget тратится на refresh» чрезмерным обобщением нескольких runs?
2. Не смешивает ли таблица `811 candidate memory -> 146 image rows` несовместимые исторические/eligibility grains?
3. 44 visual-review rows — это не 44 partial albums (partial albums = 3). Проверь реальный состав sink.
4. Безопасно ли предложение заморозить downstream invalidation и «если когда-то прошёл — публиковать», или это воскресит stale/rejected candidates?
5. Действительно ли exact-link queue сейчас забирает execution budget, если почти все строки terminal и ready=0?
6. Безопасен ли partial-album bypass, и какой minimum-acquisition contract нужен вместо грубого обхода?
7. Достаточен ли предложенный набор KPI для `_progress_signature`, или cumulative totals тоже могут скрывать нулевой marginal yield?

<gemini-audit>
**Executive Verdict**
Главная причина провала — система превратилась в машину по обслуживанию собственного технического долга и поддержанию идеального внутреннего стейта (идемпотентность, text-restoration, downstream invalidations), полностью остановив продуктовую «добычу» (acquisition). Текущий пайплайн оптимизирует активность, а не результат. Фундаментальный архитектурный баг заключается в `_progress_signature()`: этот метод считает *любое* изменение метрик (например, `publics_needs_rescan_or_retry_total` или обновление policy) как «прогресс». В результате оркестратор не видит, что система работает вхолостую (churn), и не останавливает её (idle detector сброшен), сжигая бюджеты на перепроверку одних и тех же 30–40 старых постов.

### 1. Сильные стороны (Что сохранить)
* **PROVEN:** Жесткое разделение на изолированные worker-ядра (CandidateReport, BGE-M3, ImageDiagnostic), общающиеся через YDB очереди. Это спасает от лимитов Kaggle.
* **PROVEN:** Идемпотентность и canonical deduplication источников до того, как совершается сетевой запрос к Telegram/VK.
* **PROVEN:** Strict downstream state ledgers (терминальные статусы вроде `rejected_local_region_source`), которые позволяют безопасно перезапускать пайплайн.

### 2. Продуктовые провалы (Оптимизация активности вместо кандидатов)
* **PROVEN:** Функция `_progress_signature()` в `region_talk_orchestrator.py` (строка 2840). Включает в себя *каждую* скалярную метрику. Сюда попадают счетчики ретраев, отложенных запусков и восстановлений текста. Пайплайн крутится, меняя статусы с "wait" на "retry", считая это прогрессом.
* **PROVEN:** Text Restore Loop (коммиты `30504619..755f4172`). Пайплайн тратил ресурсы на восстановление полных текстов старых постов для «идеального» эмбеддинга вместо того, чтобы сканировать новые.
* **PROVEN:** Global Keyword Search. 27 exact-ссылок привели к 0 новых внешних каналов. Инструмент просто «переоткрывает» локальные/спам каналы, забивая `post_link_queue_item`.
* **INFERENCE:** Partial Album Review. 44 поста ждут `visual-review`, просто потому что пайплайн отказывается скорить альбом, если не удалось скачать 1 из 10 фотографий.

### 3. Фактические потери по стадиям (Funnel)
| Стадия / Группа | Вход (Denominator) | Выход / Ожидает | Конверсия / Потеря |
| :--- | :--- | :--- | :--- |
| **Source Allocation** | 7,588 известных источников | 6,160 unscanned pending | **~81% базы простаивает** |
| **High-probability Blogger**| 135 TG/VK confirmed | 91 отсканировано (43 ждут) | **~32% горячих лидов не тронуты** |
| **Blogger Post Discovery** | 613 processed posts | 66 dual vector accepts | **~10.8% конверсия в медиа-чеки** |
| **Vector to Media Queue** | 811 live candidate memory | 146 image ledger rows | **~82% loss** (269 ждут BGE, остальные отбиты text/source gate) |
| **Image Scoring** | 146 image ledger rows | 67 actual-scored (80 eligible) | **13 строк застряли** |
| **Human Review / Final** | 100 publication URLs (37 active) | **4 current Gemini-confirmed** | **~89% active URLs stuck/rejected** (32 visual-review, 4 text-restore, 9 rejected) |

### 4. Фактический Churn (Повторная обработка)
* **PROVEN:** В последнем запуске (latest): 4 fetch -> 0 first-seen (все 4 reprocessed). 3 ушли на policy refresh.
* **PROVEN:** В product canary (Jul-13): 33 fetch -> 33 refreshed-known, 0 first-seen.
* **PROVEN:** 10 runtime-deferred в предпоследнем запуске.
* *Вывод:* Пайплайн тратит 100% лимита Telegram API (`history_scan`) на «освежение» уже известных постов и восстановление данных, игнорируя 6,160 unscanned источников.

### 5. Почему не сработал аудит от 9 июля?
* **Неверный диагноз? Нет.** Призыв сделать exact-post queue и отработать authoritative sources был верным.
* **State bugs & Scheduler Allocation:** Очередь exact-постов была реализована, но немедленно забита мусором из глобального keyword-поиска. Лимит сканирований был перехвачен на text-restore.
* **Manual Review Sink:** Механизм `album-safe scoring` парализовал воронку. Если из VK или TG не скачалась одна картинка альбома, кандидат отправляется в `visual_review_pending` (их 44). Это «техническая завершённость», которая убила поток.

### 6. Монолитность и сложность. Что заморозить?
Пайплайн избыточно сложен. Для получения 20 постов *сейчас* необходимо жестко заморозить:
1. **Text Restore Pipeline:** Отключить полностью. Использовать те excerpt'ы/тексты, которые есть.
2. **Downstream Invalidation (`b9097818`, `88d156ef`):** Хватит перепроверять старые посты. Если он прошел — пускать в публикацию.
3. **Global Keyword Search:** Заморозить. Это генератор мусора для `post_link_queue_item`.
4. **Partial Album Guard:** Принимать частичные альбомы (если скачалось 3 фото из 5 — отдавать их ImageDiagnostic, а не людям на review).

### 7. Минимальный Product-First Operating Mode (Playbook на 24–48 часов)
* **Цель:** 20 кандидатов.
* **Квоты и Приоритеты:**
  1. Выделить **100% бюджета discovery** (Telegram history) исключительно на 43 нетронутых `confirmed_external` блогера. Запретить rescan/retry обычных backlog-строк (`REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES=0`).
  2. Игнорировать глобальный `post_link_queue_item` (или установить лимит чтения из него в 0), чтобы не тратить resolve на спам.
* **Stage SLO:** BGE-M3 должен работать сразу, как только появляется хотя бы 10 строк E5-without-BGE.
* **Stop Condition:** Если `processed_posts_first_seen_total == 0` на протяжении двух запусков подряд — пайплайн обязан падать (сброс idle detector через фикс `_progress_signature`).

### 8. 5-10 изменений с максимальным Impact

| Изменение | Evidence | Expected Effect | Risk | Acceptance Criterion |
| :--- | :--- | :--- | :--- | :--- |
| **1. Переписать `_progress_signature()`** | Сейчас возвращает все метрики. | Сработает Idle detector, система перестанет крутить retry-loop. | Low | Оркестратор остановится при 0 new posts. |
| **2. Отключить Text Restore** | 4 поста зависли в restore, 33 policy_refresh забирают квоту. | Вся квота пойдет на first-seen посты. | Low | `text-restore-pending` = 0. |
| **3. Bypass Partial Album Guard** | 44 поста застряли в `visual-review`. | 44 кандидата сразу уйдут на оценку к модели. | Med | `image_actual_scored` скакнет вверх. |
| **4. Force 100% History на Confirmed Unscanned** | 43 блогера лежат нетронутыми. | Моментальный приток высоковероятных постов. | Low | `publics_scanned` для cohort вырастет. |
| **5. Заморозить Keyword Search** | 27 writes -> 0 внешних. | Снижение FloodWait и замусоривания. | Low | Отсутствие новых keyword hit URL. |
| **6. Дропнуть BGE для Text-Restore** | 269 ждут BGE, E5 перегружен. | BGE drain заработает. | Med | `text_vector_e5_without_bge` -> 0. |

### 9. Проверка уже закоммиченных исправлений (Без веры в commit log)
* `73885b75` (keep unscanned evidence in primary lane): Проверять по дельте `publics_primary_unscanned_pending_total` (должна падать) и `processed_posts_first_seen_total` (должен быть > 0).
* `b9097818`, `88d156ef` (invalidations): Это исправления ради чистоты стейта (удаляют «фантомов»). Проверять по совпадению `publication_active_candidate_total` (37) и сумме реальных Gemini-статусов.
* `30504619..755f4172` (text restore): Если этот луп действительно исправлен, счетчик `refreshed-known` должен резко упасть до единиц, а `first-seen` восстановиться. Если снова 33/33 known — коммиты не работают.

### 10. Как переопределить KPI / `_progress_signature`
Необходимо изменить функцию в `scripts/region_talk_orchestrator.py` (строки 2840-2860).
Вместо цикла `for key in sorted(metrics):` необходимо жестко захардкодить отслеживание только продуктовых инкрементов:
```python
def _progress_signature(metrics: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    target_keys = {
        "publics_terminal_processed_total",
        "processed_posts_first_seen_total",
        "image_actual_scored_total",
        "publication_active_candidate_total",
        "publication_confirmed_total"
    }
    out: list[tuple[str, int]] = []
    for key in sorted(target_keys):
        if key in metrics:
            out.append((key, int(metrics[key])))
    return tuple(out)
```
Только тогда холостой прогон без новых постов или новых скоров не изменит сигнатуру, сработает таймер остановки, и оператор (или агент) увидит, что воронка встала, а не будет радоваться тысячам `needs_rescan_or_retry`.

</gemini-audit>

## Что требуется от тебя

1. Дай независимый executive verdict: одна главная причина и 3–5 contributing causes.
2. Построй корректную funnel-таблицу, не смешивая source/post/media/publication grain и current/historical epochs. Где знаменатель ненадёжен — так и напиши.
3. Раздели findings на:
   - **PROVEN CURRENT LOSS**;
   - **PROVEN REPROCESSING/CHURN**;
   - **RECENTLY FIXED, NOT LIVE-ACCEPTED**;
   - **CODE-LEVEL RISK / INFERENCE**;
   - **MEASUREMENT ARTIFACT**.
4. Проверь Gemini-аудит по пунктам: что подтверждаешь, что отвергаешь, что уточняешь.
5. Объясни, почему реализация рекомендаций аудита 9 июля не подняла `new current-confirmed candidates/hour`: неверная постановка, scheduler allocation, implementation/state bugs, precision/recall gates, manual review, Telegram constraints или комбинация.
6. Предложи **безопасный product-first режим на 24–48 часов** для verified cohort. Нужны точные приоритеты/квоты, что временно отключить, что нельзя отключать, stage SLO, stop/reallocation conditions и ожидаемая воронка.
7. Дай недельный simplification plan: какие контуры отделить от 17k-line CandidateReport, какие policy epochs заморозить, какие очереди/проекции сделать authoritatively single-source-of-truth.
8. Дай 5–10 highest-impact изменений с evidence, expected effect, risk и измеримым acceptance criterion.
9. Предложи scoreboard, где главный KPI — `new unique current-review-ready or confirmed candidates per wall-clock hour`, а техническая активность не считается продуктовым прогрессом.
10. Назови точные функции/диапазоны/commit diffs для human code review.

Пиши по-русски, answer-first, конкретно. Не предлагай ещё один большой redesign до того, как будет проверен минимальный operating mode. Не принимай наличие commit/test за live product acceptance.
