# Жёсткий visual/product acceptance review: четыре точных desktop-состояния briefing hero

Ты — внешний арт-директор и senior product/motion designer. Нужен критический review точного опубликованного состояния, а не поддерживающий комментарий. Не наследуй никакой прежний PASS: прошлый финальный recheck касался `anticipated_person_named`/poster crop и не проверял эти четыре точных состояния как систему.

## Обязательно прочитай сами PNG

Контактный лист 1366×768:
`/home/dev/projects/events-bot-new-typed-briefing-followup-20260715-integration/artifacts/codex/static-typed-briefing-followup-20260715/contact-1366.png`

Контактный лист 1440×900:
`/home/dev/projects/events-bot-new-typed-briefing-followup-20260715-integration/artifacts/codex/static-typed-briefing-followup-20260715/contact-1440.png`

Оригиналы без уменьшения:
- `weekend_count-1366x768.png`, `weekend_count-1440x900.png`
- `weather_water_demo-1366x768.png`, `weather_water_demo-1440x900.png`
- `frequently_forwarded-1366x768.png`, `frequently_forwarded-1440x900.png`
- `festival_demo-1366x768.png`, `festival_demo-1440x900.png`
в каталоге:
`/home/dev/projects/events-bot-new-typed-briefing-followup-20260715-integration/artifacts/codex/static-typed-briefing-followup-20260715/`

Это реальные public screenshots immutable build, `pace=slow`, terminal/stopped state, `prefers-reduced-motion: reduce` только для детерминированного terminal capture. HTTP 200, ошибок ресурсов нет.

## Измеренная геометрия

1366×768:
- центрированный `.page-shell`: x=93, width=1180;
- hero/stage: x=93, y=89, width=1180, height=322.5px = 42vh;
- фон hero — `linear-gradient(...)`, заканчивается на x=1273, то есть на границе shell, а не viewport;
- categories: y=421.5..465.5; heading ленты начинается y=477; видны в первом viewport;
- типовой message y≈155..347; `frequently_forwarded` выше и тяжелее: y≈122..380;
- header / page background продолжаются на всю ширину viewport.

1440×900:
- shell x=130, width=1180;
- hero/stage x=130, y=89, width=1180, height=378px = 42vh;
- categories y=477..521; heading ленты y=532; видны;
- message y≈179..379, `frequently_forwarded` y≈144..414.

Wordmark `Анонсы` теперь присутствует и грузится; это не прежний missing-logo state.

## Продуктовая рамка и пользовательская критика

- Это должен быть чистый первый экран главной, не карточка и не lab-frame: сильная типографика 1–3 строки, короткая кинетическая коммуникация, затем быстрый вход в категории/ленту.
- Hero обязан занимать не более 50% viewport и не выглядеть вложенным «блоком внутри страницы».
- Пользователь справедливо спрашивает: разве фоновой разрыв по вертикальным границам x=93/1273 (или x=130/1310) не создаёт очевидный inner frame?
- Пользователь спрашивает, не стоит ли опустить надписи немного ниже; это не заданное решение. Дай независимый оптический вердикт: текущее вертикальное положение сильное/финальное или нет, и если нет — точное смещение/правило.
- `frequently_forwarded`: фраза «часто пересылают» сама не ссылка; CTA ведёт на общий `/populyarnoe/`. Оцени нарушенное ожидание clickability и укажи, какая именно часть/объект должен вести на конкретное событие.
- `festival_demo`: текст абстрактный «фестиваль идёт», CTA общий `/poisk/`. Оцени, допустим ли такой generic public narrative или нужен конкретный фестиваль и ссылка на него.
- `weather_water_demo`: «Допустим, на выходных ясно. Может, на воду?» — оцени как public copy в региональном контексте Калининграда; пользователь считает «Допустим» искусственным, а «на море» естественнее, чем «на воду».
- Новый будущий сценарий: при ветре/шторме предложить уютный indoor-план — лекцию, затем во 2–3 шагах показать одну или две конкретные лекции. Оцени цепочку и визуальную/CTA модель, не проектируй погодный backend.
- Не оценивай дизайн карточек ленты: они лишь контекст масштаба.

## Требуемый формат ответа

1. **Вердикт без дипломатии:** PASS/FAIL каждой из 4 сцен и overall. Отдельно ответь: «Да/нет, inner frame/background seam является publish blocker».
2. **Что реально пропустило прошлое acceptance:** объясни границы предыдущего PASS и почему его нельзя применять к этим сценам.
3. **Визуальная система:**
   - является ли фиксированный max-width 1180 правильным для самого фонового поля hero;
   - должен ли background быть full-bleed до viewport, при сохранении контента в grid/container;
   - нужен ли hero вообще видимый прямоугольный background или лучше бесшовный page-level wash;
   - точное рекомендуемое вертикальное положение текста для 1366×768 и 1440×900 (top/baseline/center rule, диапазон px; не просто «по вкусу»);
   - типографика, line count, декоративная O, wordmark, связь hero/categories/feed;
   - <=50vh: формальное и визуальное соблюдение.
4. **Affordance и links:** конкретно для `24 идеи`, weather, frequently forwarded, festival: что кликабельно, куда ведёт, как показать это без превращения H1 в «синие ссылки». Отдельно P0/P1/P2.
5. **Copy/narrative gate:** выбери конкретные лучшие русские формулировки вместо weather и festival placeholders; для forwarding укажи паттерн с конкретным событием. Предложи 2–3 шага цепочки «шторм → уют → конкретные лекции» с корректными ссылочными объектами/CTA, но без выдумывания актуальных названий — используй placeholders `{lecture_1}` / `{lecture_2}`.
6. **Одна patch-система, не варианты:** конкретные CSS/layout numbers для 1366 и 1440; что исправить прямо сейчас без редизайна ленты.
7. **Acceptance checklist:** измеримые visual/link checks для exact screenshots и DOM, включая отсутствие seam/frame, lower optical placement, categories visibility, конкретность festival/event links, graceful long copy.
8. **Что остаётся вкусовщиной после выполнения обязательных фиксов.**

Пиши по-русски. Будь строгим. Если композиция сейчас выглядит дешёвой/случайной — скажи это прямо. Не называй `PASS WITH CONDITIONS`, если есть publish blocker.
