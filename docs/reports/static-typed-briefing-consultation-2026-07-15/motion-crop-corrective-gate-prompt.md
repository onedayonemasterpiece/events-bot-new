Ты — независимый критический acceptance reviewer уровня principal product designer + motion engineer. Не соглашайся из вежливости. Оцени результат строго по визуальным артефактам и коду. Это isolated lab, не production homepage.

Контекст ошибки: прошлый проход убрал entry-анимацию целиком, чтобы картинка не исчезала в terminal/manual state, и массово поменял crop у всех сцен, хотя проблема с лицами была точечной. Нужно проверить корректирующий патч.

Открой и визуально изучи (используй доступ к локальным файлам):
- текущие screenshots:
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/02-ivana-kupala-1920x900.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/04-writing-kaliningrad-1920x900.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/08-hay-day-1920x900.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/14-portrait-collage-1920x900.png
- motion videos:
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/entry-hold-exit-next-entry.webm
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/terminal-image-persists.webm
- representative extracted frames:
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/motion-0.4.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/motion-1.2.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/motion-5.4.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/motion-6.0.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/terminal-0.4.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/terminal-4.5.png
  - artifacts/codex/typed-briefing-motion-crop-corrective-20260717/visual/frames/terminal-6.0.png
- accepted older 12-scene contact sheet:
  - artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-3842/contact-sheet-12.jpg
- implementation:
  - site/src/pages/lab/briefing/index.astro
  - site/src/data/briefingLab.ts
  - tests/playwright/static_briefing_lab.spec.ts (особенно media-review и adaptive mosaic tests)

Обязательный продуктовый контракт:
1. Каждая новая mosaic-картинка появляется неравномерной tile entry-анимацией.
2. Exit запускается только непосредственно перед реально запланированным автоматическим successor и старый кадр остаётся до готовности следующего media.
3. После exit новая copy/media коммитятся вместе, а новая картинка снова проходит entry.
4. Если текущий кадр terminal или выбран вручную и auto-successor нет, exit не запускается, картинка остаётся зрителю сколько угодно.
5. Pause отменяет ожидающий transition и сохраняет текущий кадр.
6. Ivana Kupala и «День валяния в сене» должны вернуть достоинства старого accepted crop, а не получить новый массовый crop.
7. «Пишу из Калининграда» не должна быть маленькой оторванной маркой справа, но лица не должны обрезаться сверху; оцени также гармонию текста и изображения.
8. Три вертикальных изображения должны каждое заполнять свои contiguous 5×5 квадраты cover-crop'ом: никаких половинных/letterbox клеток, растяжения или смешивания источников в одной клетке.
9. Горизонтальный курсор остаётся во время ожидаемого продолжения; terminal курсор может несколько раз мигнуть и уйти, но media остаётся.

Нужен жёсткий ответ:
- общий verdict PASS / FAIL (не условный);
- таблица по 9 контрактам PASS/FAIL + точное доказательство;
- отдельная визуальная оценка 4 screenshots: композиция, crop лиц, конфликт текста с лицами, гармония, 1–10;
- отдельная оценка motion: entry читается или слишком слабая; виден ли exit; нет ли пустого провала; terminal persistence несёт ценность;
- если FAIL — не общие советы, а минимальный список конкретных code/data diffs с селекторами/полями/таймингами;
- прямо ответь, можно ли это публиковать пользователю на повторный просмотр или пока нельзя.
