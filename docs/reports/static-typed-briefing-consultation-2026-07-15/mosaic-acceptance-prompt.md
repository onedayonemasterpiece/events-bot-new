Ты — строгий внешний product/visual/motion acceptance reviewer. Это НЕ просьба подтвердить решение: ищи blocker и ставь FAIL, если exact evidence не доказывает стабильную, гармоничную композицию.

Display model должен быть Gemini 3.1 Pro (High) / Pro class; назови model в первой строке, если доступна.

Задача пользователя:
- desktop hero не выше 50% viewport, текстовый нарратив 1–3 строки + cursor;
- иногда справа появляется кропнутое изображение как bento/mosaic 8×3 из квадратов без rounded corners;
- cells проявляются/исчезают неравномерно через opacity ease-in-out;
- cells ближе к тексту остаются частично и неодинаково прозрачными;
- текст защищён paper stripe;
- изображение доходит до физического правого края и как будто продолжается за ним;
- mobile пока должен быть спокойным text-only, без image request.

Открой ВСЕ exact artifacts текущей реализации CSS-grid (не оценивай предыдущие версии):
1. Fully revealed 1440×900:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/static-1440x900.png
2. Fully revealed 1366×768:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/static-1366x768.png
3. Motion phase ~250ms:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/phase-0250-1440x900.png
4. Motion phase ~800ms:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/phase-0800-1440x900.png
5. Motion phase ~1500ms:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/phase-1500-1440x900.png
6. Full slow lifecycle 12.5s, including reverse exit and next scenario:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/motion-1366x768-slow.webm
7. Mobile 320×568:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/static-320x568.png
8. Mobile 390×844:
/home/dev/projects/events-bot-new-typed-briefing-mosaic-20260715-integration/artifacts/codex/static-typed-briefing-mosaic-20260715/final-local/static-390x844.png

Measured facts from exact build:
- 1440: stage x=0..1440, h=360/900; mosaic x=572..1468, 896×336; text x=31.7, width=835; body scrollWidth=1440.
- 1366: stage x=0..1366, h=322.55/768; mosaic x=584.39..1391.2, 806.8×302.55; body scrollWidth=1366.
- 24 cells, 8×3; w/h delta <=1px; 3px gutters; radius=0; shadow=none.
- exact alpha rows: [.16,.28,.44,.62,.78,.88,.94,.97] / [.12,.24,.40,.58,.76,.90,.96,1] / [.18,.30,.48,.66,.80,.89,.93,.96].
- deterministic rank matrix; 520ms ease-in-out entry, 360ms reverse exit; no randomness/saturation.
- only one raster network request on eligible desktop; zero on 320, 390 and 1024×600; image 404 collapses to text-only.
- mobile uses data-media-mode=none, 3 text lines, no overflow; categories/feed entry visible.
- linked future event: 6112, 13 Aug 2026; UI intentionally does not invent profession.

Do not evaluate draft event cards below the hero. Do evaluate the hero/categories boundary and whether this still looks like a nested frame/banner.

Ответ на русском, без редактирования файлов:
A) `MOSAIC LAB GATE: PASS | PASS WITH CONDITIONS | FAIL`;
B) desktop visual harmony, edge/crop, stripe/readability and whether mosaic really feels like one image rather than 24 unrelated thumbnails;
C) motion: scattered/non-wipe, stable, not noisy; exact exit/next rhythm; cursor coexistence;
D) mobile degradation;
E) blockers vs polish (max 7 concrete points);
F) explicit `PUBLISH FOR USER REVIEW: YES|NO` (lab only, not production rollout).
