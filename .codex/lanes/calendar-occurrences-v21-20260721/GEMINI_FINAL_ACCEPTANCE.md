Привет! Я завершил строгий read-only аудит текущей ветки (`integration/calendar-occurrences-v21-final-20260721`) относительно `origin/main`, включая проверку тестов Playwright из `validate-v21.cjs`, серверных тестов `occurrence-families.test.mjs`, кода резолверов `eventOccurrences.ts` и Python-скрипта синхронизации, а также отчетов `W02/RESULTS.md` и `W03/RESULTS.md`.

Вот таблица с результатами по каждому из 7 критериев.

### Таблица приемки (Acceptance Review)

| Критерий | Статус | Подтверждение из кода и тестов |
| :--- | :---: | :--- |
| **1) Сегодня=21.07.2026, прошедшие приглушены, без ложного completed для без end** | **PASS** | `validate-v21.cjs` подтверждает: `today is 21 July Kaliningrad`. Проверки `m.datePast===0` и `m.falseEnded===0` проходят (события без явного `end` не получают ложный статус completed). Прошедшие даты получают класс `.is-date-past`, `filter: grayscale` и надпись "Прошло", оставляя opacity текста полным. В Python-скрипте векторной синхронизации события без `end_date` также считаются валидными (строка 163). |
| **2) Длинная стрелка (тот же endpoint); long card сохраняет город** | **PASS** | Тест `gallery cue keeps head endpoint` успешно проверяет, что координаты правого края совпадают до долей пикселя, а SVG удлиняет только вал (`M0 11V9h14`). Тест длинной карточки (Kant) проверяет, что summary получает модификатор `event-summary--long`, ширина 340px, clamp=3 и текст включает локацию («Светлогорск · Дом-музей Германа Брахерта»). |
| **3) Right-corner hero 11x6, нелинейный fade, random reload, бледный край, parallax отдельно** | **PASS** | Тест `hero 11x6 near-left retina geometry` проверяет матрицу тайлов 372×202 (при 390px экране) и retina 3x ресурсы. Проверен рандомный сид загрузки (seed меняется при reload). Левый край заходит не более чем на 0.06 (`leftMax <= 0.0601`), пересечение с текстом <= 0.04. Раннее исчезновение: подтверждено исчезновение прозрачности до прокрутки к header, с финальным состоянием `visibility: hidden`. Parallax изолирован на роуте `/date-2026-07-24-parallax/`. |
| **4) Popular shelf header safe space, rails не увеличены** | **PASS** | Тест `popular sticky shelf tag-safe` валидирует 5 скроллов для всех полок. Зафиксирована высота заголовков (80px) и rails (112px). Пересечения (collisions) с `brand-tag` строго равны нулю для title и icon. |
| **5) Occurrence family только mutual explicit links; date/family policies; rail aria** | **PASS** | В `eventOccurrences.ts` и `sync_event_search_vectors_to_supabase.py` (строка 187) связь создается **только** если `linked_id in public and event_id in links.get(linked_id)`. Тесты в `event-occurrences.test.mjs` подтверждают генерацию одной общей aria (`ariaLabel: '2 и 9 ноября в 19:00'`) и корректную работу стратегий коллапса (`per-date` и `per-family`). Detail-компонент теперь всегда выводит occurrence-presentation (проверка на отсутствие `mobile-event-production__other-dates`). |
| **6) Search E2E: snapshot, pre-pagination collapse, fallback share, malformed rules** | **PASS** | В `occurrence-families.test.mjs` и `index.ts` доказано, что `paginateOccurrenceFamilies` работает с окном `p_match_count: 60`, схлопывая family **до** отсечения пагинации (чтобы предотвратить всплытие младших сиблингов). Fallback-запрос делит общий seen-set (`collapseOccurrenceFamilies(fallbackRows, seenFamilies)`). Асимметричные/dangling связи (fail-closed) корректно остаются самостоятельными событиями, оставляя только highest-ranked представителя группы. |
| **7) Не было wholesale старого lab merge** | **PASS** | Дифф против `origin/main` содержит исключительно целевые файлы: Edge-функции, тесты, Python скрипт синхронизации векторов и `eventOccurrences.ts`. Мусорных CSS/HTML файлов от UI-лабораторий в production-ветке нет — прототипы W02 по-прежнему изолированы в `artifacts/codex/...` и `.codex/lanes/`. |

### Продуктовый вердикт по Parallax
Parallax был реализован в рамках изолированного экспериментального роута (фактор 0.15) и полностью отключается (возвращает `transform: none`) при наличии предпочтений `reduced-motion`.
**Риск/Блокер:** Parallax визуально конфликтует с нелинейным ранним (early fade) исчезновением тайлов при скролле (meaningful disappearance timeline).
**Вердикт:** Полностью согласен с выводом из W02: **не включать этот эффект по умолчанию в production**. Оставьте базовый вариант (hero fade-out) — он предсказуемо освобождает фокус для контента, в то время как параллакс в данном UX ломает задуманную динамику "растворения".

### Замечания для Интегратора
- **Увеличение нагрузки на нормализацию в E2E поиске:** Возврат сырых 60 записей Edge-функцией — осознанное решение для стабильной пагинации (RPC cap), но увеличивает издержки на маппинг. Ожидаемое поведение, блокировок нет.
- UI-прототипы из W02 (CSS/HTML) готовы и оттестированы (106/106 чеков в playwright), однако в рабочую кодовую базу (Astro-компоненты/стили) пока **не перенесены** — потребуется отдельная фаза интеграции.
- Инцидент для W03 оставлен со статусом OPEN, что является корректным (решен технический долг, но не полное закрытие issue).

**Итоговый статус ревью:** **PASS ✅**. Одобрено к интеграции в `main` и к ручному переносу UI-составляющей прототипа.
