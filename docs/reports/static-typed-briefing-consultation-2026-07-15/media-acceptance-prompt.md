Продолжение того же visual gate. Реализация переделана по твоему FAIL. Обязательно открой и сравни:

BEFORE:
/home/dev/projects/events-bot-new-typed-intro-prototype/artifacts/codex/static-typed-briefing-homepage-media/screenshots/desktop-1440-rare-wide-media.png

AFTER desktop, фаза media полностью вошла, public Next скрыт, 1440×900:
/home/dev/projects/events-bot-new-typed-intro-prototype/artifacts/codex/static-typed-briefing-media-correction/visual/desktop-1440-wide-motion-present.png

AFTER mobile clean, цепочка активна, 390×844:
/home/dev/projects/events-bot-new-typed-intro-prototype/artifacts/codex/static-typed-briefing-media-correction/visual/mobile-390-clean-running.png

AFTER mobile terminal, цепочка 3/3 остановлена и только тогда появился Next:
/home/dev/projects/events-bot-new-typed-intro-prototype/artifacts/codex/static-typed-briefing-media-correction/visual/mobile-390-chain-stopped.png

Измерения AFTER desktop: stage x=0..1440, h=378/900; media x=489.6..1440, width=950.4=66vw; text x=31.7, width=576; border-radius=0; shadow=none; body scrollWidth=1440. LAB-док находится после ленты и закрыт, поэтому не виден на этих кадрах. Цепочка автоматически проходит 3 связанных сценария с readable hold, потом останавливается.

Дай краткий acceptance review на русском:
1) итог desktop wide-media: PASS / PASS WITH CONDITIONS / FAIL;
2) устранён ли именно «frame inside frame»;
3) итог mobile clean/terminal;
4) максимум 5 конкретных остаточных замечаний, раздели blockers и polish;
5) можно ли публиковать этот lab для пользовательского просмотра (не production rollout).
Не редактируй файлы. Не оценивай черновые карточки ленты. В начале назови display model, если доступна.
