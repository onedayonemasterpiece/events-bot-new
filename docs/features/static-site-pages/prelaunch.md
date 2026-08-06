# Публичная prelaunch-заглушка до 1 сентября 2026 года

## Продуктовая формулировка

> 1 сентября 2026 года состоится публичный запуск «Полюбить Калининград Анонсы» — персонализированного сервиса анонсов и навигатора по культурным и просветительским событиям Калининградской области. Это не просто электронная афиша: сервис будет учитывать интересы пользователей, помогать быстрее находить подходящие события и открывать новые возможности для знакомства с культурной жизнью региона. Первыми к его тестированию присоединились участники фестиваля — они сформировали фокус-группу, чьи оценки и предложения будут учтены при подготовке сервиса к запуску для широкой аудитории.

## Публичная поверхность до запуска

В production по умолчанию включён `PUBLIC_PRELAUNCH_MODE=on`.

- `/` — индексируемая заглушка с датой запуска, кратким объяснением сервиса и формой уведомления;
- все остальные HTML-страницы получают `noindex,nofollow,noarchive,nosnippet` на уровне итогового артефакта;
- из HTML-маршрутов `robots.txt` разрешает только точный корень; технические `/_astro/` и `/assets/` остаются доступны для отображения заглушки;
- `sitemap.xml` содержит только `https://kenigevents.ru/`;
- полный каталог продолжает собираться и проходить продуктовые проверки, но до запуска не продвигается в поиске.

Итоговая build-политика применяется после Astro-сборки ко всем HTML-файлам и не зависит от layout отдельной страницы.

## Визуальная концепция

Канонический implementation/review contract находится в [`prelaunch-handoff/README.md`](prelaunch-handoff/README.md). Текущее состояние и точка продолжения зафиксированы в [`prelaunch-handoff/CONTINUATION-20260805.md`](prelaunch-handoff/CONTINUATION-20260805.md).

Референсы:

- исходная desktop-композиция: [`prelaunch-handoff/reference/target-desktop.webp`](prelaunch-handoff/reference/target-desktop.webp);
- PWA-образ: [`prelaunch-handoff/reference/PWA-icon.webp`](prelaunch-handoff/reference/PWA-icon.webp);
- принятая модель света, бликов, пудры и CTA: [`prelaunch-handoff/reference/generated-lighting-desktop-v1.webp`](prelaunch-handoff/reference/generated-lighting-desktop-v1.webp);
- принятый мобильный масштаб и компоновка: [`prelaunch-handoff/reference/generated-lighting-mobile-v1.webp`](prelaunch-handoff/reference/generated-lighting-mobile-v1.webp).

Generated-reference файлы служат только для visual review и не загружаются пользователю в runtime.

Заглушка использует утверждённый PWA-образ через production asset:

`site/public/assets/pwa/announcements-brand-v2-512.png`

Изображение является одним непрерывным background layer. Над ним находятся 72 стеклянные плитки и отдельная непрозрачная маска межплиточных швов.

### Обязательные визуальные инварианты

- изображение не может быть видно в промежутках между плитками;
- изображение и glass effects не могут проявляться за скруглёнными углами плитки;
- square tile-container закрывает угловые вырезы непрозрачным seam-color, а rounded glass surface живёт внутри него;
- `.prelaunch__tile` всегда имеет opacity `1`;
- затемнение, blur, matte texture, border и bevel реализованы на внутренней glass surface;
- состояния `sealed`, `dim`, `revealed` отличаются по transmission, blur, saturation, brightness, кромке и глубине;
- над всей мозаикой существует один пространственный источник света справа сверху;
- отдельная плитка не рисует собственный radial spotlight;
- расстояние до общего источника влияет только на transmission и подсветку верхней/правой кромки;
- часть плиток на траектории света имеет более заметную подсветку границ, но остаётся частью одного светового поля;
- золотистая пудра — один статический scene-overlay, а не множество анимированных DOM-частиц;
- изменяется маленькая случайная группа, а не вся мозаика;
- при `prefers-reduced-motion: reduce` сцена полностью статична.

Использование opacity всей плитки запрещено: оно ослабляет кромку и открывает фон в шве. Анимация `backdrop-filter` на 72 плитках также запрещена из-за ненужной нагрузки на compositor.

### Адаптивность

Desktop и mobile используют одну визуальную систему стекла, света, пудры и edge accents. Mobile не является отдельным дизайном:

- grid и карта света вычисляются из фактического числа колонок;
- desktop использует 9 колонок, mobile — 6 более мелких квадратных колонок;
- на mobile PWA-подложка уменьшается примерно до 88% ширины viewport и сохраняет цельный wordmark;
- mobile меняет только геометрию и типографический ритм, а не материал и освещение;
- страница остаётся single-screen без вертикального и горизонтального scroll на принятой матрице viewport.

## Визуальное доказательство

Browser evidence должен сохранять PNG, DOM и computed-style JSON как минимум для:

- `1200×1200` — reference-square;
- `1440×900` — desktop;
- `390×844` — mobile;
- `320×568` — compact mobile;
- idle и registered состояния формы на mobile.

Gates должны доказывать:

- ровно 72 плитки;
- 8 цельных reveal-windows, адаптивно рассчитанных по grid;
- 3 усиленных edge accents вдоль верхнеправого светового луча;
- 4 spatial edge bands: `ambient`, `soft`, `warm`, `hot`;
- 72 корректные rounded-corner masks;
- единый root-level radial emitter;
- отсутствие radial-gradient и viewport-fixed gradients внутри отдельных плиток;
- наличие golden-powder overlay;
- отсутствие whole-tile opacity и blur animation;
- sparse motion `2–5` плиток;
- отсутствие horizontal/vertical overflow;
- корректные idle и registered состояния формы.

Control workflow: `.github/workflows/prelaunch-v12-control.yml` в ветке `automation/prelaunch-control-20260804`.

## Уведомление о запуске

Форма вызывает `public.register_prelaunch_notification_v1` через общий отказоустойчивый транспорт Supabase ↔ Yandex relay.

Данные сохраняются в отдельной таблице:

`personalization.prelaunch_launch_subscription`

Границы:

- прямой browser `SELECT/INSERT/UPDATE/DELETE` запрещён;
- RLS включён;
- email нормализуется и хранится один раз;
- повтор запроса или транспортный replay не создаёт дубль;
- сохраняется только email и минимальные служебные поля отправки, без IP, user agent и истории просмотров;
- назначение согласия — одно письмо о запуске 1 сентября;
- установлен срок удаления после запуска;
- новые уникальные записи ограничены 5 000 в календарный день Калининграда;
- honeypot не записывает автоматические отправки.

Миграция:

`supabase/migrations/20260803113000_prelaunch_launch_notifications_v1.sql`

Миграция должна быть применена к personalization Supabase до публикации заглушки. Наличие файла в репозитории само по себе не означает, что таблица уже создана в рабочем проекте.

### UX-состояния формы

Обязательны:

1. `idle` — email, CTA, согласие и обещание одного письма;
2. `submitting` — CTA показывает «Сохраняем…», повторное действие блокируется;
3. `error` — понятное сообщение, введённый email не теряется;
4. `success` — input и consent заменяются подтверждением «Готово, вы записаны»;
5. `registered` — при повторном открытии на том же устройстве показывается «Вы уже записаны»;
6. `reset` — действие «Другой e-mail» возвращает форму, а backend-idempotency продолжает защищать от дублей.

В idle/success/registered copy должно быть зафиксировано, что для подписавшихся приготовлен отдельный приятный сюрприз. Обещание не раскрывает механику сюрприза и не меняет правовое назначение consent: пользователь по-прежнему соглашается на одно письмо о запуске.

## Запуск 1 сентября

Публичный запуск — отдельный release transition:

1. применить все обязательные production migrations и проверить очередь уведомлений;
2. собрать чистый production artifact с `PUBLIC_PRELAUNCH_MODE=off`;
3. прогнать полный production/browser release gate;
4. опубликовать полный каталог атомарно;
5. отправить одно письмо только строкам со статусом `pending`, после успешной отправки записать `delivery_status='sent'` и `notification_sent_at`;
6. проверить, что новый `robots.txt` и полный `sitemap.xml` опубликованы вместе с сайтом.

Простое удаление заглушки без переключения индексирующего контракта не считается запуском.

## Проверка

```bash
npm --prefix site run test:static-release
PUBLIC_PRELAUNCH_MODE=on npm --prefix site run build
python3 -m http.server 4173 --directory site/dist
npm --prefix site run check:prelaunch-browser -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-browser
npm --prefix site run check:prelaunch-light-model -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-browser
npm --prefix site run check:prelaunch-experience -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-browser
npm --prefix site run build:production
```

Production checker различает `prelaunch` и `full_catalog` по полю `prelaunch_mode` в build/manifest и блокирует релиз, если до запуска индексируется более одного HTML-документа.
