# Fable review: выборочное восстановление пропущенных Gemma-оценок через Gemini 3.5 Flash-Lite

**Дата:** 17 ноября 2026. **Исполнитель:** KenigEvents (codex). **Адресат:** Fable под пользователем `devester`, затем (только после вердикта Fable) кодовый агент релиза. **Статус:** отдельный кодовый кандидат, **НЕ production**, без деплоя, изменения векторов, БД, рабочих кэшей и VK.

**Код:** `chatgpt/related-lite-missing-verdicts-20261117`; предок — точный head [PR #1045](https://github.com/onedayonemasterpiece/events-bot-new/pull/1045) `42f96dd2be2f1a76f2507df64375c74528c4802f`, не `main`. Сравнивать **только с веткой #1045**, иначе тестов и изменённых строк окажется значительно больше. PR #1045 сам по себе ещё не объединён. [Скилл против переусложнения](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-simplicity/SKILL.md).

## 1. Уточнение ресурса

Предыдущее выражение «общий пул 3 000» было некорректно без указания модели. В нормальном лимитере подтверждены **шесть Google-проектов (S1–S6)**. Для каждой модели и проекта отражён отдельный суточный предел **500 запросов**. При шести проектах это **3 000 запросов для Gemini 3.1 Flash-Lite плюс 3 000 для Gemini 3.5 Flash-Lite, суммарно 6 000**, но лимиты не взаимозаменяемы. Несколько ключей одного проекта не дают дополнительных независимых лимитов. Минутные лимиты на модель/проект — 15 RPM и 250 000 TPM. Суточное окно лимитера — America/Los_Angeles.

| Замер 17 ноября | Gemini 3.1 Flash-Lite | Gemini 3.5 Flash-Lite |
| --- | ---: | ---: |
| Оставалось в 11:11 по Калининграду | **2 933** | **2 963** |
| Израсходовано 16 ноября (полные сутки) | 2 426 | 1 362 |
| Физических попыток за предшествующие 24 ч к 11:04 | 2 417 | 1 341 |
| Из них успешных | 1 929 | 887 |
| Из них HTTP 429 | 305 | 271 |
| Из них HTTP 503 | 71 | 134 |

Источник — [точный срез шести проектов](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/quota-evidence.json), [поток Gemma того же потребителя](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/gemma-collector-volume.json), скрипт [verify_quota.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/verify_quota.py). Это исторический срез, **не** гарантия доступной квоты при будущем запуске. Расходуется ресурс других потребителей Smart Update, мониторинга и OCR; первичным источником квоты остаётся общий лимитер GoogleAIClient. Не вводить отдельный бюджет, очередь, таймер или список ключей.

На 15 ноября в учёте были **2 023 попытки Gemma** этого потребителя и 905 использований Gemini 3.5. Пример «второй вызов Lite на каждую попытку Gemma» дал бы 2 928 из 3 000 для 3.5 — почти весь дневной лимит при сохранении текущих нагрузок. Поэтому массовый fallback запрещён.

## 2. Доказанная неисправность и реальный опыт

Читатель [VK 14778](https://vk.com/wall-231920894_14778) видит одну рекомендацию «Проверка материала опытных комиков», хотя векторный поиск уже передал Gemma несколько других стендапов. У кандидата **11010→10939** (Таня Виноградова) модель дважды продублировала один ID в обычном ответе; адресный запрос именно этого ID завершился HTTP **503**, поэтому проверка остаётся **19/20**. Это технически отсутствующее решение, **не семантический отказ**.

На этом **дословно том же одиночном запросе** Gemini 3.5 Flash-Lite вернула `0.82/reject=false`; в группе из пяти — `0.78/reject=false`. [Исходные запросы](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/lite-singleton-packets.json), [точный ответ](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/lite-singleton-responses.jsonl), [параметры до вызова](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/lite-singleton-design.json). Ещё три коротких реальных вызова Lite проверили в сумме 10 **заранее выбранных** пар, не весь каталог: [разметка](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/lite-design.json), [запросы](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/lite-packets.json), [ответы](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/lite-responses.jsonl), скрипт [check_lite.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/check_lite.py). Четыре ответа из четырёх; учтено 4 906 токенов (по записанным ответам).

| Пара | Ранее Gemma | Gemini 3.5 Flash-Lite | Контроль |
|---|---:|---:|---|
| 11010→10939, сольный стендап Виноградовой | не оценено: 503 | **0.82** | Полезное событие |
| 11010→10938, сборный стендап | 0.75 | 0.82 | Сохранить полезное |
| 11010→10976, Макс Кузнецов | 0.70 | **0.75** | Пограничное полезное |
| 11010→10940, «Девочки, это пипец» | 0.70 | **0.70** | Пограничное полезное, **не** спасено |
| 11010→9708, «Нехорошая квартирка» | 0.45, отказ | 0.40, отказ | Отрицательный контроль |
| 11068→10443, «Гарри Поттер» | 0.78 | 0.75 | Полезное |
| 11068→10883, «Жизнь Пи» | 0.65 | **0.82** | Полезное; VK-видимость доказана, место в финальной пятёрке не доказано |
| 11068→9794, лекция о крестовых походах | 0.35, отказ | 0.45, отказ | Отрицательный контроль |
| 11084→10186, «Янтарный бал» | 0.60 | 0.52, отказ | Не добавлять |
| 11084→9934, «Синдром самозванца» | 0.58 | 0.65 | Не добавлять |

С учётом порога 0.72 Lite совпала с исходной независимой разметкой по **9/10**, но это малая, специально выбранная выборка. Пакеты 5/3/2 и старые Gemma десятки различались составом. Числа **не** доказывают общей калибровки моделей и не оправдывают замену всех оценок Gemma.

### Что доходит до VK без публикации

При воспроизведении действующего `vk_event_relations.presentation()` на прочитанных данных и сохранённой проверке публичных ссылок (без записи) для 11010: **одна строка** с имеющимися решениями Gemma; **две** после добавления только восстановленного 10939; **три** при добавлении ещё одного измеренного решения по 10976. У Дроботенко (9950) нет VK URL, поэтому сильный балл сам по себе не даёт строку. [Точные ID, URL и результаты](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/reader-impact.json), пересчёт [check_reader.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/check_reader.py). Три строки — **контрфактический replay**, а не опубликованный пост.

## 3. Что изменено в кандидате кода

Только существующий `site/scripts/export-production-preview-data.py` и два регрессионных теста в `tests/test_static_site_pgvector_export.py`, плюс каноническая документация и `CHANGELOG.md`. Производственный diff — около **+39 строк**, новых сервисов, БД, очередей, конфигурационных флагов и лимитов нет.

Порядок остаётся: **pgvector → Gemma 4 26B → штатный адресный повтор отсутствующих ID из #1045 → (если часть этой же пачки уже оценена Gemma, а часть осталась неоценённой) один запрос Gemini 3.5 Flash-Lite только за остатком → существующий порог 0.72 → граф и VK**.

Сохраняются исходные решения Gemma. Оценка Lite кладётся в уже существующий audit-cache вместе с полем `_verified_model`; в `chain.reason_codes` она различима по `gemini35_lite_audit` (против `gemma4_26b_audit`). Не производится усреднение, «максимум из двух», дополнительный выбор лучшей пятёрки или повышение категории. Сам `related_v1`, pgvector, отбор самостоятельных программ и номера сеансов не меняются.

**Граница вызова Lite:** технический пробел **после** штатного количества попыток Gemma и **при наличии хотя бы одного корректного Gemma-решения в пачке**. Полный отказ Gemma без единого ответа не отправляет всю десятку в Lite. Если Gemma уже явно поставила 0.70, 0.40 или `reject=true`, Lite не вызывается. Если Lite вернула неполный ответ/429/503, исходный pass остаётся не полностью подтверждённым, ложного `strict_verified` не создаётся. Лимитер GoogleAIClient остаётся единственным владельцем допуска к провайдеру.

**Сравнение с механизмами:** #1045 чинит недостающие ID повтором **той же** Gemma, но не может выдать результат после её HTTP 503. Новая ветка обращается к Lite ровно на этом выходе и не пересоздаёт механизм восстановления. Структура provider JSON не меняется.

**Открытый технический риск для Fable:** унаследованные имена `gemma_audit_cache`, `gemma_verified_model` и `strict_verified` теперь могут содержать завершение пачки с Lite. В самой записи у Lite есть `_verified_model`, но ключ полного кэша остаётся основан на основной Gemma-политике. Просьба отдельно оценить обратную совместимость при rollback/чтении старым кодом. Не добавлять второй кэш и SHA-гейт без доказанной необходимости; если риск материален, предложить **одно минимальное** исправление в существующем месте. Этот кандидат ещё не принят для production.

## 4. Что сознательно не реализовано

Перепроверка всех оценок 0.65–0.71, всех карточек с одной рекомендацией или всех пар Gemma означала бы лишнюю нагрузку. В сохранённом кэше: **84** карточки с оценками, **1 380** запрошенных пар, **1 355** оценённых, **25** пропущенных у восьми карточек. Безусловная перепроверка диапазона 0.65–0.71 затронула бы **204 пары у 80 из 84 карточек**. Среди 49 полных двадцаток лишь пять имели не более одной сильной пары; на них пришлось 17 пограничных. [Срез](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/cache-slice.json), [скрипт пересчёта](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/count_candidates.py).

Временный кандидат для сравнения (НЕ код): только при явной жалобе или 0–1 **реально опубликованных** похожих строках после обычного восстановления проверять одну короткую пачку неотклонённых и реально доступных пар с Gemma-оценкой `0.65 <= score < 0.72`. Отрицательный контроль — гостиничный форум и «Янтарный бал»/«Синдром самозванца», положительный — Макс Кузнецов. Но получение публичного VK статуса внутри статического экспортёра ради этого не оправдано; пусть Fable сначала сравнит по сохранённым данным.

## 5. Что проверить Fable

Проверить именно **дополнительный diff к #1045**, без поднятия другого кодового агента:

1. Подтвердить арифметику независимых квот: 6 проектов × 500 **на модель**, а не 3 000 на две модели. Пересчитать потребность по двум типам триггеров без выдуманной постоянной дневной нагрузки.
2. Повторить расхождение между Gemma и Lite на 10 исходных парах, отдельно техническое спасение 11010→10939. Отделить реальный успех провайдера от настоящей полезной строки VK. Проверить, не объявляется ли `strict_verified` при неполной Lite-пачке.
3. Провести ревью кода/тестов: Gemma имеет приоритет; Lite только после исчерпания штатных попыток на частично отвеченной пачке; не вызывается на валидный низкий балл, явный отказ, полностью проваленную десятку; только один вызов на остаток; провайдер и policy audit trace сохранены. Проверить указанную границу кэша и rollback.
4. Самостоятельно оценить, стоит ли `0.72` считать взаимозаменяемым между Gemma и Lite. Если нет — предложить минимальную проверку на дополнительных **заранее** размеченных реальных парах, не широкий модельный турнир. Ниже порога автоматически не поднимать.
5. Выдать для владельца решение: **принимать / CHANGES / отклонять** узкий резерв для технических пропусков; отдельно — оставлять ли пограничный второй взгляд только исследовательским. Назвать конкретные ID положительных и отрицательных контролей, не «для надёжности».
6. Назвать риск для уже открытых PR #1045, #1046, #1052, но не объединять их и не деплоить. «Красный фонарь» и programme identity остаются отдельной задачей #1051.

### Как воспроизвести без аккаунта Google или доступа к production

Репозиторий публичный, чтение под Unix-пользователем `devester` возможно без GitHub write-permission. Не делать массовых download/кэшей. Из существующего клона:

```bash
git fetch origin chatgpt/related-lite-missing-verdicts-20261117
git show FETCH_HEAD:site/scripts/export-production-preview-data.py > /dev/null
git diff 42f96dd2be2f1a76f2507df64375c74528c4802f FETCH_HEAD -- site/scripts/export-production-preview-data.py tests/test_static_site_pgvector_export.py
# Для запуска тестов использовать собственный изолированный рабочий каталог/checkout при необходимости.
# Данные опыта и скрипты без внешних API доступны в ранее опубликованном immutably pinned архиве:
```

[Исходный опыт, Python-only scripts и сохранённые ответы Gemini 3.5](https://github.com/onedayonemasterpiece/events-bot-new/blob/0ac988ba5de09a12307f45530bd9e34b2cba3d11/docs/reports/fable-related-search-20261116/flash-lite-20261117/FABLE-GEMINI-LITE.md). В этом отчёте все относительные ссылки на JSON и скрипты относятся к этому же закреплённому архиву; новую копию не создавали. Секретов, токенов API и исходных полных приватных конфигураций нет.

**Доставка:** после независимого заключения Fable — только тогда root/кодовый агент принимает изменения, синхронизирует с актуальным `main`, завершает проверки и выпускает с обычной новой публикацией VK и контролем через существующий Crew. Старые VK публикации и рабочие данные не изменялись.

## Root integration after Opus review, 17 November 2026

Integrated with current main da195aa79462fb5504daad1e37e6d1c3f621531c after source PR1045 was deployed. The partial reply test now captures actual model and exact candidate IDs: Lite receives missing IDs in the first build, never an existing low score. Existing main damaged-cache tests also retain their strict coverage assertions and now include the bounded missing-only Lite attempt. The missing-catalog candidate control and incremental scope tests remain.

One outcome log_stage was added at the existing Lite boundary: anchor, pass, model, missing-ID count, status ok/partial/error. No request text, credentials, new state or telemetry subsystem.

Before adjusting obsolete test expectations, the merged candidate produced5 FAIL/44 PASS in10.16s. A distinct control in the existing Lite test fails on the original source with the updated test:1 FAIL in1.95s because the outcome log is absent. The corrected full pair of test files passed49 tests in16.36s. All runs used network-none/tmpfs RunCoveer containers without production DB/env/provider calls. Retained logs are verify-lite1057-integration-20261117/before.log,log-before.log,after.log in the root task artifacts.

This is code-level acceptance only. The eight proposed real Lite quality calls were not run; existing STOP/USED restrictions remain. No broad score calibration or new actual model AFTER is claimed. This PR remains a draft and is not deployed. Preserve the negative controls in REVIEW-3-LITE.md for later authorized quality acceptance. The older reader rollback retains the cached actual model metadata but loses the model-specific reason label in the projected chain.
