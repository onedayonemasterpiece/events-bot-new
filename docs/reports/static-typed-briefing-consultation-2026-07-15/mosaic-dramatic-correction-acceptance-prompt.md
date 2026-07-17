Ты — строгий внешний product/visual/motion reviewer Pro-класса. Предыдущий
Gemini PASS оказался ошибочным: reviewer повторил переданные ему success-метрики
и не заметил шахматную маску и растяжение лица. Поэтому **не доверяй** никаким
описаниям реализации и сначала самостоятельно посмотри exact pixels/video.

Пользователь отверг предыдущий вариант по шести причинам:

1. часть правых квадратов не стала полноценно яркой;
2. вместо случайно выглядящей неоднородности получилась шахматная доска;
3. осветление справа налево выглядело слишком плавным и предсказуемым;
4. в различиях прозрачности и motion не было динамики или драмы;
5. изображение должно занимать примерно правые 3/4 viewport и уходить дальше
   влево;
6. источник нельзя растягивать до пропорций grid — нужен умный `cover` crop с
   сохранением лиц, букв и геометрии.

Rejected baseline, который **обязан выглядеть хуже**:

- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/public/desktop-1440-named-final.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-followup-20260716-integration/artifacts/codex/static-typed-briefing-mosaic-followup-20260716/public/desktop-1440-live-final.png`

Candidate exact final states:

- portrait source at `1920×900`:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/static-typed-briefing-mosaic-dramatic-20260717/local-final/desktop-1920-named-final.png`
- the same portrait source at `1440×900`:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/static-typed-briefing-mosaic-dramatic-20260717/local-final/desktop-1440-named-final.png`
- horizontal event photo at `1440×900`:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/static-typed-briefing-mosaic-dramatic-20260717/local-final/desktop-1440-live-final.png`
- another narrative at `1366×768`:
  `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/static-typed-briefing-mosaic-dramatic-20260717/local-final/desktop-1366-unusual-final.png`

Entry sequence, inspect as a contact sequence rather than isolated decoration:

- `desktop-1440-live-entry-early.png`
- `desktop-1440-live-entry-mid.png`
- `desktop-1440-live-entry-late.png`

Exit sequence:

- `desktop-1440-live-exit-early.png`
- `desktop-1440-live-exit-mid.png`
- `desktop-1440-live-exit-late.png`

All six phase files are in:

`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/static-typed-briefing-mosaic-dramatic-20260717/local-final/`

Exact `20.5s` slow lifecycle of three image scenarios:

`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/static-typed-briefing-mosaic-dramatic-20260717/local-final/desktop-1366-dramatic-three-mosaics.webm`

Mobile text-only safety evidence:

- `mobile-320-named.png`
- `mobile-390-live.png`

Review order:

1. Сначала опиши, что действительно видишь в rejected baseline и candidate,
   без ссылок на числа, тесты или авторские утверждения.
2. Отдельно проверь: правый edge; checkerboard/parity; локальные острова и
   reversals вместо ровного gradient; drama entry/exit; покрытие правых 3/4;
   пропорции лица и букв; качество focal crop портретной и горизонтальной media.
3. Проверь, что текст сохраняет тот же якорь, а stripe помогает чтению, не
   превращая композицию в тяжёлую белую карточку.
4. Проверь WebM: три сцены должны ощущаться связанными, entry/exit —
   нерегулярными и не механическим reverse одного порядка.
5. Mobile оценивай только как text-only degradation; изображение там
   намеренно запрещено.

Ставь FAIL при любом из шести исходных blocker, даже если технические тесты
зелёные. Не оценивай черновые карточки ленты. Ответ на русском:

A) `MOSAIC DRAMATIC CORRECTION: PASS | PASS WITH CONDITIONS | FAIL`;
B) независимое сравнение baseline → candidate;
C) шесть требований: каждое `PASS|FAIL` с наблюдением;
D) motion rhythm;
E) text/stripe и mobile;
F) blockers и polish, максимум 8 пунктов;
G) `PUBLISH FOR USER REVIEW: YES|NO` — только isolated lab, не production.
