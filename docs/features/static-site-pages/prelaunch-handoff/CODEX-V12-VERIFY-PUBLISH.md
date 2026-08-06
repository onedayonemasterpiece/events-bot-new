# Минимальная задача для Codex: проверить и опубликовать prelaunch v12

> Эта задача операторская. Не переписывать дизайн и не начинать реализацию заново.

## Контекст

Репозиторий: `onedayonemasterpiece/events-bot-new`  
PR: `#296`  
Ветка: `agent/prelaunch-landing-20260803`  
Implementation baseline: `5da758c135864eacbc26fe2b0cb4a3becaec44f5`

После baseline в ветке могут находиться только handoff/prompt-документы. Перед работой получить наиболее свежий head PR №296 и убедиться, что изменения после baseline не затрагивают runtime-код. Использовать свежий head, если это действительно docs-only.

GitHub-hosted Actions сейчас не стартуют из-за account billing/spending-limit blocker. Это не ошибка приложения. Последняя точная annotation:

```text
The job was not started because recent account payments have failed or your spending limit needs to be increased.
```

## Разрешённая работа

1. Checkout точного свежего head PR №296.
2. Ничего не коммитить и не менять в коде.
3. Выполнить проверки локально или в доверенном operator environment.
4. Сохранить screenshots/DOM/JSON evidence.
5. Если все gates зелёные, опубликовать новый immutable secret review prefix и вернуть работающую ссылку.
6. Если gate падает, не исправлять код: вернуть точную ошибку и artifact/evidence.

## Проверки

```bash
npm ci --prefix site --no-audit --no-fund
npm --prefix site run test:static-release
PUBLIC_PRELAUNCH_MODE=on npm --prefix site run build
python3 -m http.server 4173 --directory site/dist
```

В другом терминале:

```bash
npm --prefix site run check:prelaunch-browser -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-browser

node site/scripts/check-prelaunch-viewport-fit.mjs \
  --url http://127.0.0.1:4173/ \
  --artifact-dir artifacts/prelaunch-browser

npm --prefix site run check:prelaunch-light-model -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-browser

npm --prefix site run check:prelaunch-experience -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-browser
```

Скопировать в artifact reference-файлы:

```text
docs/features/static-site-pages/prelaunch-handoff/reference/target-desktop.webp
docs/features/static-site-pages/prelaunch-handoff/reference/PWA-icon.webp
docs/features/static-site-pages/prelaunch-handoff/reference/generated-lighting-desktop-v1.webp
docs/features/static-site-pages/prelaunch-handoff/reference/generated-lighting-mobile-v1.webp
```

После этого:

```bash
npm --prefix site run check:prelaunch-product-visual-policy -- \
  --artifact-dir ../artifacts/prelaunch-browser
```

Обязательные screenshots:

```text
prelaunch-reference-square-1200x1200.png
prelaunch-desktop-1440x900.png
prelaunch-mobile-390x844.png
prelaunch-mobile-small-320x568.png
prelaunch-experience-idle-390x844.png
prelaunch-experience-registered-390x844.png
```

## Что проверить глазами перед публикацией

- в скруглённых углах плиток не видно изображения или иной подложки;
- межплиточные швы полностью тёмные;
- нет отдельного radial spotlight внутри каждой плитки;
- один источник справа сверху читается как непрерывное световое поле;
- несколько плиток на траектории света имеют заметно более яркие верхнюю/правую кромки;
- золотистая пудра и редкие точки-блики присутствуют, но не перегружают сцену;
- mobile использует более мелкие плитки и уменьшенный цельный PWA-образ;
- CTA выглядит как тёплая премиальная кнопка;
- idle copy содержит обещание отдельного приятного сюрприза;
- registered state заменяет поля сообщением «Вы уже записаны»;
- страница не скроллится.

## Публикация

Публиковать только после success всех gates. Создать новый случайный 256-bit base64url token без padding. Старую review-ссылку не переиспользовать.

Использовать существующий secret-candidate/operator pipeline и его credentials:

```text
KENIGEVENTS_SITE_YC_BUCKET
KENIGEVENTS_SITE_YC_ACCESS_KEY_ID
KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY
KENIGEVENTS_SITE_YC_ENDPOINT
KENIGEVENTS_SITE_YC_REGION
KENIGEVENTS_SITE_PUBLIC_BASE_URL
PUBLIC_PERSONALIZATION_SUPABASE_URL
PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY
PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL
```

Жёсткие ограничения:

- create-only upload;
- новый prefix `/_review/<token>/`;
- production root не менять;
- stable ICS не менять;
- старые review prefixes не менять;
- PR не merge;
- main не менять;
- files changed by this task: none.

После публикации выполнить public Chromium smoke на `1440×900`, `390×844`, `320×568`, проверить HTTP 200, assets, noindex, no-referrer, отсутствие scroll, форму и hash/MIME каждого объекта.

## Финальный ответ

При успехе начать ровно так:

```text
Готово.
Новая секретная ссылка:
https://kenigevents.ru/_review/<new-token>/
```

Затем указать:

```text
PR: #296
Published SHA: <sha>
Local/operator visual gates: success
Artifact/evidence path: <path-or-id>
Public hash/MIME verification: success
Public Chromium desktop: success
Public Chromium mobile: success
Idle form state: success
Registered form state: success
Form transport configured: yes
Production root changed: no
Stable ICS changed: no
Old review links changed: no
Files changed by this task: none
```

При failure вернуть только точный SHA, команду, exit code, первую ошибку и путь к сохранённому evidence. Код не менять.
