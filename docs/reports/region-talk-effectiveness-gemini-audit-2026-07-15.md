# Region Talk effectiveness audit — Gemini 3.1 Pro High

Date: 2026-07-15
Model/access: `Gemini 3.1 Pro (High)` via `a-gemini` / Antigravity (`agy`)
Audited branch baseline: `agent/region-talk/R04-live-canary` at `19de9abf`
Status: external consultant input, not an implementation contract. The independent-review brief below explicitly challenges unsupported or unsafe recommendations.

## Raw consultant response

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
