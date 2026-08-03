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

Канонический implementation/review contract находится в [`prelaunch-handoff/README.md`](prelaunch-handoff/README.md). Основной референс хранится в [`prelaunch-handoff/reference/target-desktop.webp`](prelaunch-handoff/reference/target-desktop.webp).

Заглушка использует утверждённый PWA-образ через production asset:

`site/public/assets/pwa/announcements-brand-v2-512.png`

Изображение является одним непрерывным background layer. Над ним находятся 72 стеклянные плитки и отдельная непрозрачная маска межплиточных швов.

### Обязательные визуальные инварианты

- изображение не может быть видно в промежутках между плитками;
- `.prelaunch__tile` имеет прозрачный base и opacity `1`;
- затемнение, blur, matte texture, border и bevel реализованы на псевдоэлементах плитки;
- состояния `sealed`, `dim`, `revealed` отличаются не только opacity, но и blur, saturation, brightness, кромкой и глубиной;
- плитки в верхнеправой зоне отражают направленный тёплый свет, но не превращаются в резкие вырезанные окна;
- изменяется маленькая случайная группа, а не вся мозаика;
- при `prefers-reduced-motion: reduce` сцена полностью статична.

Использование opacity всей плитки запрещено: оно одновременно ослабляет кромку и открывает фон в шве, разрушая стеклянную конструкцию.

## Визуальное доказательство

Browser gate сохраняет PNG, DOM и computed-style JSON для:

- `1200×1200` — reference-square;
- `1440×900` — desktop;
- `390×844` — mobile.

В reference-square проверяются положение первых швов, H1 и PWA-проекции. Дополнительно gate доказывает:

- ровно 72 плитки;
- детерминированное исходное распределение состояний;
- три разных backdrop-filter treatment;
- непрозрачную repeating-gradient маску швов;
- отсутствие whole-tile opacity animation;
- sparse motion `2–5` плиток;
- отсутствие horizontal overflow.

Workflow `.github/workflows/prelaunch-visual-review.yml` публикует screenshots, DOM, scene JSON и reference WebP одним artifact для ручного сравнения.

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
npm --prefix site run build:production
```

Production checker различает `prelaunch` и `full_catalog` по полю `prelaunch_mode` в build/manifest и блокирует релиз, если до запуска индексируется более одного HTML-документа.
