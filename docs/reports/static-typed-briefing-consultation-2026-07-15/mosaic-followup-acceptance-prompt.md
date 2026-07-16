Ты — строгий внешний product/visual/motion acceptance reviewer. Это не просьба
подтвердить решение: ищи blocker и ставь FAIL, если exact evidence не доказывает
требования пользователя.

Display model должен быть Gemini 3.1 Pro (High) / Pro class; назови model в
первой строке, если доступна.

Пользователь отверг предыдущий вариант и потребовал:

1. Изображение должно появляться под большинством сценариев, чтобы эффект можно
   было нормально рассмотреть, а не увидеть один раз.
2. Текст вообще не должен менять постоянную позицию при появлении картинки;
   единственное текстовое изменение — paper stripe.
3. Нужна не равномерная градиентная прозрачность: каждый соседний квадрат должен
   заметно отличаться светлее/темнее, но средняя прозрачность должна расти к
   левому краю около текста. Изображение должно начинаться заметно левее.
4. Вход и быстрый выход должны быть неоднородными, ease-in-out и стабильными.

Открой exact artifacts текущей реализации:

- 1440×900, Татьяна Куртукова, fully revealed:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/desktop-1440-named-final.png`
- 1440×900, Алексей Мышкин, fully revealed:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/desktop-1440-live-final.png`
- 1366×768, Вертинский, fully revealed:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/desktop-1366-rare-final.png`
- 1440×900, partial entry:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/desktop-1440-live-entry.png`
- full slow 20.5s lifecycle of three consecutive image scenarios:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/desktop-1366-slow-three-mosaics.webm`
- mobile 320 and 390 text-only:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/mobile-320-named.png`
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/local/mobile-390-live.png`

Measured facts:

- 13 of 19 selectable scenarios use grounded existing event media in mosaic mode
  on eligible desktop viewports.
- Grid is deterministic 12×4 / 48 squares; 3px gutters, no radius/shadow/frame.
- Every horizontal and vertical neighboring final-alpha delta is at least .14.
  Column-average alpha grows left→right:
  `.235,.275,.320,.380,.453,.540,.620,.710,.762,.833,.883,.920`.
- At 1440 the image grid is x=406..1462 and is cropped by a viewport-bound
  shell at x=406..1440; at 1366 grid x=453.7..1385.4 and shell ends at 1366.
  Body width equals viewport width.
- Text anchor at 1440 is x=130 in both media and no-media state; automated
  comparison allows ≤1px delta for x/y/width. Mosaic mode changes only stripe
  styling, not message layout rules.
- Entry is deterministic 600ms ease-in-out with scattered 90..1218ms delays;
  exit is 260ms with reverse scattered 0..470ms delays. The next scene waits
  for exit completion.
- Mosaic is absent and its source is not assigned at 320, 390 or short desktop;
  mobile stays text-only.

Не оценивай черновые карточки ленты. Ответ на русском, без редактирования файлов:

A) `MOSAIC FOLLOW-UP GATE: PASS | PASS WITH CONDITIONS | FAIL`;
B) действительно ли соседние клетки заметно отличаются, но остаётся общий
   left-opacity trend, и не выглядит ли результат грязным checkerboard;
C) действительно ли текст остаётся на постоянном якоре и stripe достаточен;
D) ритм трёх последовательных картинок, входа и быстрого выхода;
E) mobile degradation;
F) blockers vs polish, максимум 7 пунктов;
G) `PUBLISH FOR USER REVIEW: YES|NO` — только lab, не production rollout.
