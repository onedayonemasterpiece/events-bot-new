# Post-fix acceptance: briefing hero text states, links, storm chain and cursor

Ты — тот же строгий внешний арт-директор/senior product-motion designer. Это
post-fix gate после твоего `FAIL` и corrective continuation. Нельзя наследовать
PASS от других сцен. Проверь именно новые файлы и вынеси честный publish verdict
для изолированной lab-страницы (не для production home).

## Обязательно открой оригинальные PNG

Каталог:

`/home/dev/projects/events-bot-new-typed-briefing-followup-20260715-integration/artifacts/codex/static-typed-briefing-followup-20260715/postfix-local/`

Текстовые состояния, каждое в 1366×768 и 1440×900:

- `final-weekend_count-1366x768.png`, `final-weekend_count-1440x900.png`;
- `final-weather_water_demo-1366x768.png`, `final-weather_water_demo-1440x900.png`;
- `final-frequently_forwarded-1366x768.png`, `final-frequently_forwarded-1440x900.png`;
- `final-festival_demo-1366x768.png`, `final-festival_demo-1440x900.png`;
- `final-storm_weekend_demo-1366x768.png`, `final-storm_weekend_demo-1440x900.png`.

Мобильные storm-состояния:

- `final-storm_weekend_demo-320x568.png`;
- `final-storm_weekend_demo-390x844.png`.

Курсор в гарантированно видимой фазе blink (это тот же DOM/CSS, только animation
в capture поставлена на паузу с opacity=1):

- `final-cursor-visible-storm-1366x768.png`;
- `final-cursor-visible-storm-390x844.png`.

Реальная motion-запись 390×844, без остановки animation:

- `final-storm-cursor-chain-390x844.webm`.

## Что изменено после FAIL

1. Контент остаётся в grid 1180px, но wash hero теперь ровно `100vw` и не
   заканчивается на границах shell. У stage нет прямоугольного background.
2. Desktop text-only message bottom-anchored; actions→stage-bottom измеряется
   52–78px на 1366×768 и 1440×900. Stage = 42svh с desktop cap 360px и всегда
   меньше 50vh; категории и начало ленты видны в первом viewport.
3. Good-weather copy: «Обещают ясные выходные. Махнём на море?»; «на море?» и
   CTA ведут в подборку.
4. Forwarded copy теперь называет «Планету Океан»; и фраза, и название, и CTA
   ведут прямо в один конкретный event `6466`.
5. Festival copy теперь конкретный: `Pianissimo. Максим Милославский.`; оба
   linked fragment и CTA ведут прямо в event `5294`.
6. Добавлена последовательная цепочка из трёх заходов: storm premise →
   конкретная лекция о монументальном искусстве (`3592`) → конкретная лекция о
   съёмках Калининграда (`5077`). Один semantic CTA/linked object на экран;
   public Next остаётся secondary и появляется лишь после остановки цепочки.
7. После завершения печати, пока следующий экран реально запланирован таймером,
   справа от последнего знака мигает отдельный горизонтальный underscore. При
   terminal state он делает три цикла (2340ms) и исчезает. При pause,
   static/reduced-motion бесконечного курсора нет.
8. Inline editorial links — brand-colored thin underline с hover/focus, без
   solid-button и без «синих ссылок». Wordmark `Анонсы` и исходная широкая O
   сохранены.

## Проверенный DOM/test contract

- 19 сценариев + fallback, каждый максимум 3 строки на 320/375/390/1440;
- hero <=50vh, категории и heading ленты видны в первом viewport;
- full-width wash = viewport width ±1px;
- horizontal scroll невозможен; wide-media exit clipped внутри viewport;
- concrete fragment/CTA href совпадают;
- курсор имеет width больше 20px на desktop, height меньше 10px, `position:
  absolute`, blink animation; timed next держит cursor, terminal снимает его;
- reduced/static states курсор не держат.

## Требуемый ответ

1. `PASS` или `FAIL` отдельно для пяти desktop-сцен и overall. Не используй
   `PASS WITH CONDITIONS`, если остался publish blocker.
2. Закрыт ли прежний P0 seam/inner-frame blocker: да/нет и почему по PNG.
3. Вертикальный ритм: стало ли bottom-anchor оптически цельным на обоих desktop
   viewport; не выглядит ли текст слишком высоко/низко.
4. Copy/link verdict для weather, forwarded, festival, storm chain. Отдельно:
   понятно ли, что linked fragments кликабельны, но не превращают H1 в web-link
   soup.
5. Cursor/motion verdict по двум cursor-visible PNG и WebM: достаточно ли ясно
   показывает ожидание продолжения; не конфликтует ли с underline ссылок;
   корректна ли terminal retirement model.
6. Mobile verdict 320×568 и 390×844: <=50vh, видимость категорий/ленты,
   отсутствие горизонтального обрезания значимого контента.
7. Если FAIL — только конкретные blockers с точными patch numbers. Если PASS —
   отдельно перечисли вкусовые/non-blocking вещи, которые нельзя выдавать за
   дефект.
8. Финальная строка строго одна из: `LAB PUBLISH PASS` или `LAB PUBLISH FAIL`.

Пиши по-русски и без дипломатии. Не оценивай дизайн карточек ленты: это только
контекст масштаба.
