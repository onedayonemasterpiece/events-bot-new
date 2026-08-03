# Handoff: полноэкранный prelaunch «Полюбить Калининград Анонсы»

> Основная каноническая документация поведения: [`../prelaunch.md`](../prelaunch.md).
> Этот файл — implementation/review prompt для PR №296 и не создаёт вторую landing page.

## Задача кодового агента

Переработать функциональный prelaunch baseline ветки `agent/prelaunch-landing-20260803` в полноэкранную кинематографичную композицию по `reference/target-desktop.webp`. Не потерять Supabase, RLS, RPC, consent, honeypot, resilient transport, indexing и immutable secret-candidate contracts.

## Source of truth

- визуальная геометрия и итоговый acceptance screenshot: `reference/target-desktop.webp`;
- направляющий автономный пример: `prototype/`;
- production PWA asset: `site/public/assets/pwa/announcements-brand-v2-512.png`;
- источник альтернативной приложенной иконки: `../auto-present/scenario-assets/PWA-icon.png`;
- reference-копия для сравнения: `reference/PWA-icon.webp`;
- функциональный baseline: `site/src/components/PrelaunchLanding.astro`;
- production/release contract: `../prelaunch.md`.

Reference WebP не заменяют production asset.

## Точный текст видимой страницы

- бренд: «Полюбить Калининград Анонсы»;
- H1: «Запуск 1 сентября»;
- объяснение: «Персонализированный сервис анонсов и навигатор по культурным и просветительским событиям Калининградской области»;
- заголовок формы: «Напомнить о запуске»;
- placeholder: «E-mail»;
- кнопка: «Напомнить о запуске»;
- consent и status-копирайт остаются purpose-limited: одно письмо о запуске.

## Слои и z-index

| Порядок | Слой | Ответственность |
| --- | --- | --- |
| 0 | background | тёмная основа и одно непрерывное PWA-изображение |
| 1 | mosaic | 72 крупные плитки на весь viewport |
| 2 | atmosphere | виньетка, мягкий медный halo, noise и редкие искры |
| 3 | foreground | бренд, H1, текст и рабочая форма |

Foreground не использует blend mode. Декоративные слои имеют `aria-hidden` и `pointer-events:none`.

## Геометрия и стекло

- Desktop: фиксированная квадратная tile size через `clamp`, сетка шире viewport и слегка сдвинута, чтобы края не выглядели рамкой.
- Mobile: те же 72 элемента, 6 колонок, без DOM-подмены.
- Межплиточный шов 7–13 px, скругление 10–17 px, тонкая кромка и inset-depth.
- `sealed`: почти закрытая плитка, cover около `0.94`.
- `dim`: читается слабый цвет, cover около `0.70`.
- `revealed`: PWA-изображение различимо, cover около `0.28`.

Один background `<img>` остаётся непрерывным под плитками; запрещено резать картинку на tile-файлы или превращать её в отдельную правую карточку.

## Motion contract

- меняются небольшие группы 2–5 плиток;
- интервал выбора 1,8–3,6 секунды;
- easing 4,2–8,1 секунды;
- предыдущая группа не выбирается в следующем цикле;
- анимация останавливается при hidden document и `prefers-reduced-motion: reduce`;
- нет LED-вспышек, частого мерцания и layout-анимаций.

## Responsive contract

Проверить минимум `320×900`, `390×844`, `768×1024`, `1024×768`, `1440×900`, `1920×1080`.

- Desktop: бренд сверху слева, H1 в средней части, форма у нижней границы.
- Mobile: фон и мозаика остаются сценой; H1 переносится; форма в одну колонку; страница может вертикально прокручиваться на коротком экране.
- `100svh` — минимум, а не фиксированная высота с обрезанием.
- Ни один viewport не имеет горизонтального overflow.

## Supabase — не менять контракт

Форма вызывает только:

`public.register_prelaunch_notification_v1`

через `getResilientDataClient` и общий Supabase ↔ Yandex relay. Сохраняются:

- `personalization.prelaunch_launch_subscription`;
- RLS и запрет browser table CRUD;
- publishable key, никогда не service role;
- явный checkbox consent `launch-2026-09-01-v1`;
- server-side honeypot `p_website` с одинаковым public success shape;
- нормализованный unique email;
- idempotent transport replay;
- Kaliningrad-day capacity и retention contract.

Не переносить `subscribe_site_launch_v1` или миграцию из альтернативного lab prototype.

## SEO / GEO / indexing

- один видимый H1, русский `lang`, title/description, canonical и OG/Twitter metadata;
- JSON-LD `WebSite`, `WebPage`, `Service`, Калининград и Калининградская область;
- production prelaunch: индексируется только `/`;
- secret candidate: `noindex,nofollow,noarchive,nosnippet`, `no-referrer`, base-prefixed canonical;
- `prelaunch_mode` наследуется из checked production manifest и записывается вместе с `public_surface`.

## Accessibility / performance

- skip link, label, native email semantics, keyboard focus, `aria-live` status;
- status не полагается только на цвет;
- projection имеет явные width/height, background copy декоративна;
- анимация меняет composited opacity, не геометрию;
- reduced motion статичен и тестируется браузером.

## Файлы реализации

- `site/src/components/PrelaunchLanding.astro`
- `site/src/layouts/PrelaunchLayout.astro`
- `site/src/styles/prelaunch-motion.css`
- `site/scripts/build-secret-candidate.mjs`
- `site/scripts/check-secret-candidate.mjs`
- `site/scripts/check-prelaunch-browser.mjs`
- `site/tests/prelaunch-landing.test.mjs`
- `docs/features/static-site-pages/prelaunch.md`
- `CHANGELOG.md`

## Проверки

```bash
npm --prefix site run test:static-release
PUBLIC_PRELAUNCH_MODE=on npm --prefix site run build
npm --prefix site run check:prelaunch-browser -- \
  --url http://127.0.0.1:4321/ \
  --artifact-dir ../artifacts/codex/prelaunch/browser-local
```

Если pinned Chromium уже установлен вне стандартного Playwright cache, передать его явно через `PRELAUNCH_CHROMIUM_EXECUTABLE_PATH`.

После checked production build:

```bash
npm --prefix site run build:secret-candidate
npm --prefix site run check:secret-candidate
npm --prefix site run plan:secret-candidate
```

Только после local browser gates, exact object plan и anonymous-list preflight допускается immutable publish. Bearer URL не коммитится и отправляется только в согласованное место.

## Definition of Done

- PR №296 содержит handoff, code, docs и changelog;
- PWA image непрерывен под полноэкранной сеткой;
- три состояния и slow sparse motion подтверждены;
- desktop/mobile/reduced-motion browser gate зелёный;
- RPC/RLS/consent/honeypot/replay сохранены и live RPC проверен после миграции;
- secret candidate содержит prelaunch root и прошёл hash/MIME/noindex проверки;
- immutable bearer URL открыт с desktop/mobile;
- URL отправлен reply к согласованному Telegram message;
- итоговый отчёт содержит SHA, команды, screenshots, public smoke и Telegram receipt ID без секретов.
