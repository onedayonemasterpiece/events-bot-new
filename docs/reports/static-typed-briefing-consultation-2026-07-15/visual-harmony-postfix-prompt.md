# Post-fix visual acceptance gate — briefing hero desktop

Ты тот же внешний арт-директор / senior product and motion designer. Проведи строгий acceptance review **после минимального исправления** двух сцен. Это gate перед публикацией lab, не новый redesign. Нужны отдельные PASS/FAIL для каждой сцены на каждом разрешении и общий publish verdict.

## Что было FAIL до исправления

Предыдущий свежий review этих точных сценариев дал FAIL / FAIL / overall FAIL:
- P0: в header badge исчез утверждённый lockup «Анонсы»;
- P0: solid-тёмная публичная кнопка Next перебивала смысловой CTA;
- P1: small media 290×272 с тенью/radius выглядело оторванной UI-card и оставляло dead space;
- P1: H1 named-сцены 45.6px был слишком слабым.

## Что исправлено

- полный утверждённый header lockup снова виден;
- small media включено в общую grid-композицию, плоское: `box-shadow:none`, `border-radius:0`;
- named H1 усилен;
- weather удерживается в две строки на всех трёх review viewport;
- Next — прозрачный ghost/muted, gap с CTA увеличен;
- hero сохраняет ограничение <=50vh.

## Обязательно открой все шесть PNG

Папка:
`/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/postfix-local/`

Named:
1. `anticipated_person_named-1920x900.png`
2. `anticipated_person_named-1440x900.png`
3. `anticipated_person_named-1366x768.png`

Weather:
4. `weather_water_demo-1920x900.png`
5. `weather_water_demo-1440x900.png`
6. `weather_water_demo-1366x768.png`

Прочитай также `metrics.json` в этой папке. Ключевые факты:
- hero 1920/1440: 1180×378, 42vh; 1366: 1180×322.55, 42vh;
- named text width примерно 788–804px; media 320×322 на 1920/1440 и 314×266.6 на 1366;
- wordmark видим: 192×37.17;
- `mediaShadow:none`, `mediaRadius:0px`, `nextBackground:transparent` на всех состояниях.

## Продуктовая рамка / scope

- Это чистый first-screen главной, а не лабораторная карточка: 1–3 строки сильной типографики, короткий narrative, категории и начало ленты уже видны.
- Hero должен быть <=50vh и не выглядеть отдельным «фреймом внутри страницы».
- Small media — редкий editorial-приём, не обычная event card.
- Primary semantic CTA должен быть заметнее публичного Next; Next показывается только когда цепочка остановлена.
- Лента ниже — только контекст масштаба и **вне scope**.
- Не проси новые изображения и не предлагай полный redesign.

## Ответь строго

1. Таблица из шести строк: viewport × scenario, PASS/FAIL, одна конкретная причина.
2. Overall: `PUBLISH PASS`, `PASS WITH CONDITIONS` или `FAIL`; одна фраза.
3. Проверка исправлений по четырём пунктам: header lockup; media integration/dead space; hierarchy CTA/Next; typography/line breaks. Для каждого `fixed / partial / not fixed`.
4. Конкретные regressions/blockers, только если действительно видишь их. Ранжируй P0/P1/P2. Отдельно проверь:
   - не выглядит ли poster всё ещё карточкой;
   - удачен ли crop/aspect на 1366×768, где media 314×266;
   - достаточно ли слаб Next, но остаётся ли читаемым/кликабельным;
   - не конфликтует ли декоративная O с poster/text;
   - не съедает ли новый logo слишком много header/hero пространства.
5. Long-name risk: что случится с русским именем примерно на 30–36 символов и одним дополнительным словом до/после. Дай только минимальный guardrail, если нужен (например конкретный `font-size`, `max-width`, `line count`, `overflow-wrap`); не перестраивай всю систему.
6. Если verdict не FAIL: короткий measurable publication checklist для 1920×900, 1440×900, 1366×768. Если FAIL: минимальный blocking patch, без redesign.
7. Что всё равно проверить глазами после реальной анимации (максимум 3 пункта).

Пиши по-русски, критично и конкретно. Не давай общих советов и не оценивай ленту.
