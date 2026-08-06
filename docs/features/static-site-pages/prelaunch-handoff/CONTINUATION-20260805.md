# Продолжение работы: prelaunch v12 — свет, маски, mobile и форма

> **Запрещено генерировать новые картинки.** Следующая работа ведётся только с кодом и уже сохранёнными reference-файлами в GitHub.

## Быстрый вход в следующем окне

```text
@GitHub events-bot-new

Прочитай полностью:
docs/features/static-site-pages/prelaunch-handoff/CONTINUATION-20260805.md

Продолжай PR №296 с наиболее свежего head ветки
agent/prelaunch-landing-20260803.

Не генерируй изображения. Работай только с кодом, существующими reference-файлами и browser evidence.
```

## Репозиторный контекст

```text
Repository: onedayonemasterpiece/events-bot-new
Feature PR: #296
Feature branch: agent/prelaunch-landing-20260803
Control PR: #329
Control branch: automation/prelaunch-control-20260804
Implementation baseline: 5da758c135864eacbc26fe2b0cb4a3becaec44f5
Operator prompt commit: ff692ab5bfe7a24dc11cf564f0fb164052bbdd41
```

После `Implementation baseline` в feature-ветке находятся handoff/operator docs. Перед продолжением всегда получить актуальный head PR №296 и проверить diff после baseline. Runtime-реализация v12 находится в baseline.

PR №296 остаётся open и пока не mergeable. Не выполнять merge без отдельного решения.

## Принятые референсы

Все референсы сохранены в GitHub и предназначены только для visual review, а не runtime:

```text
docs/features/static-site-pages/prelaunch-handoff/reference/target-desktop.webp
docs/features/static-site-pages/prelaunch-handoff/reference/PWA-icon.webp
docs/features/static-site-pages/prelaunch-handoff/reference/generated-lighting-desktop-v1.webp
docs/features/static-site-pages/prelaunch-handoff/reference/generated-lighting-mobile-v1.webp
```

Generated references:

```text
generated-lighting-desktop-v1.webp
size: 1000×1000
sha256: 30334b933cc84f48878bfe7aeea31bd2c775108b3e8538c6b94352b19c5aef32

generated-lighting-mobile-v1.webp
size: 700×1244
sha256: bfdb2461dcb9fcb1e6e94b89ea1b5da511ee17170fd146b8c8455fd63aaaf1bb
```

## Что потребовал пользователь

1. За скруглёнными углами плиток не должна быть видна подложка.
2. Между плитками изображение не должно просвечивать.
3. Не должно быть отдельной подсветки внутри каждой карточки.
4. Должен читаться один сильный позитивный источник света из правого верхнего угла.
5. Некоторые плитки на траектории общего света должны иметь заметно более яркую подсветку границ.
6. Нужны редкие акцентные блики и золотистая пудра.
7. Mobile должен использовать ту же визуальную систему, но меньшие плитки и заметно уменьшенный цельный PWA-образ.
8. Браузер не должен перегружаться.
9. CTA должен быть визуально сильнее и дороже.
10. Форма должна реально сохранять email через существующий Supabase-контур.
11. Нужны idle/submitting/error/success/registered/reset состояния.
12. Пользователю сообщается, что для подписавшихся будет отдельный приятный сюрприз.
13. Страница должна оставаться single-screen без scroll на принятой матрице viewport.

## Реализованная архитектура v12

### Entry point

`site/src/pages/index.astro` в prelaunch mode рендерит:

```astro
<PrelaunchLanding />
<PrelaunchExperience />
```

`PrelaunchLanding` сохраняет действующую форму, Supabase RPC, consent, honeypot и motion baseline. `PrelaunchExperience` добавляет новую адаптивную visual/UX систему без переписывания backend-контура.

### Новые runtime-файлы

```text
site/src/components/PrelaunchExperience.astro
site/src/scripts/prelaunchExperience.ts
site/src/styles/prelaunch-fit-v12.css
```

### Один общий источник света

`prelaunch-fit-v12.css` создаёт один root-level `::before` emitter:

- radial-gradient вне правого верхнего края;
- z-index ниже mosaic;
- непрерывное световое поле под всеми плитками;
- ни одна плитка не рисует собственный radial-gradient;
- spatial response задаётся transmission/brightness/edge shadow, а не локальным spotlight.

### Spatial edge response

`prelaunchExperience.ts` получает фактическое число CSS-grid колонок и рассчитывает расстояние каждой плитки до внешнего источника. Плиткам присваиваются:

```text
data-edge=ambient
data-edge=soft
data-edge=warm
data-edge=hot
```

Три плитки на траектории света получают `data-accent=true`. У них сильнее верхняя/правая кромка и edge bloom, но нет собственного источника света.

### Скруглённая маска

Решение v12:

- parent tile квадратный, `overflow:hidden`, `border-radius:0`;
- внутренняя glass surface имеет реальный `border-radius`;
- opaque spread shadow seam-color заполняет четыре угловых выреза;
- подложка не может проявиться за скруглением;
- межплиточный gap закрыт отдельной repeating-gradient seam mask.

Ключевой CSS-инвариант:

```css
0 0 0 calc(var(--pane-radius) + 2px) var(--prelaunch-seam)
```

### Золотистая пудра и акценты

- один статический root `::after` с 17 radial-gradient particles;
- `mix-blend-mode:screen`;
- без particle DOM и без animation loop;
- три уже существующих spark nodes используются как редкие scene-level accents;
- нижнеправый star-glint восстановлен.

### Performance

- backdrop-filter не анимируется;
- `will-change` не резервирует 72 compositor layers;
- powder статична;
- blur levels конечны и меняются только при смене CSS state;
- ResizeObserver работает на одном mosaic;
- MutationObserver следит только за `data-state` и пересчитывает coherent reveal-cluster после sparse motion.

### Mobile

- 6 маленьких квадратных колонок вместо 4 крупных;
- тот же material/light/powder contract;
- PWA-образ уменьшен до `min(88vw, 386px)`;
- coherent reveal-map вычисляется из реального grid, отдельной mobile-сцены нет;
- меняются только scale, layout и typography rhythm.

### Форма

Backend остаётся прежним:

```text
RPC: public.register_prelaunch_notification_v1
Table: personalization.prelaunch_launch_subscription
Transport: resilient Supabase ↔ Yandex relay
Migration: supabase/migrations/20260803113000_prelaunch_launch_notifications_v1.sql
```

`prelaunchExperience.ts` добавляет:

- idle promise про одно письмо и отдельный приятный сюрприз;
- rich success state «Готово, вы записаны»;
- returning state «Вы уже записаны» через UX-hint в localStorage;
- действие «Другой e-mail»;
- сохранение backend idempotency;
- accessibility status/focus handling.

## Новые и обновлённые проверки

```text
site/scripts/check-prelaunch-browser.mjs
site/scripts/check-prelaunch-light-model.mjs
site/scripts/check-prelaunch-experience.mjs
site/scripts/check-prelaunch-product-visual-policy.mjs
site/tests/prelaunch-experience.test.mjs
site/tests/prelaunch-landing.test.mjs
```

Проверяется:

- 72 panes;
- 8 coherent reveal windows;
- 3 edge accents;
- 4 spatial edge bands;
- 72 corner masks;
- единый shared emitter;
- отсутствие pane-local radial/fixed spotlights;
- golden powder;
- отсутствие backdrop-filter animation и pane will-change;
- desktop 9 columns / mobile 6 columns;
- mobile artwork ratio;
- no-scroll;
- reduced motion;
- idle и registered form states;
- наличие pleasant-surprise copy;
- обязательные screenshots и reference-файлы в artifact.

Control workflow:

```text
.github/workflows/prelaunch-v12-control.yml
.github/prelaunch-v12.request
```

## Текущий внешний blocker

GitHub-hosted jobs сейчас не стартуют. Это подтверждённый billing blocker, а не ошибка кода.

Последние runs:

```text
Run 30992749687 — failure before runner allocation
Run 30992974869 — failure before runner allocation
runner_id=0
steps=[]
```

Точная GitHub annotation:

```text
The job was not started because recent account payments have failed or your spending limit needs to be increased. Please check the 'Billing & plans' section in your settings
```

Из-за этого после v12 пока нет новых Playwright screenshots и browser secret link.

## Следующее действие

Вариант A — после восстановления GitHub Actions billing:

1. получить свежий head PR №296;
2. обновить `.github/prelaunch-v12.request` exact SHA;
3. дождаться `Prelaunch v12 visual control`;
4. скачать artifact;
5. сравнить desktop/mobile/registered screenshots с четырьмя reference-файлами;
6. при необходимости внести только доказанные CSS-калибровки;
7. после visual acceptance опубликовать новый immutable secret review prefix.

Вариант B — без ожидания Actions:

передать Codex только операторскую задачу:

[`CODEX-V12-VERIFY-PUBLISH.md`](CODEX-V12-VERIFY-PUBLISH.md)

Codex не должен менять код. Он должен локально выполнить gates, сохранить evidence и при success создать новый secret review URL.

## Команды проверки

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

Скопировать четыре reference-файла в `artifacts/prelaunch-browser/reference/`, затем:

```bash
npm --prefix site run check:prelaunch-product-visual-policy -- \
  --artifact-dir ../artifacts/prelaunch-browser
```

## Критические запреты

- не генерировать изображения;
- не создавать отдельный mobile visual design;
- не возвращать per-pane radial spotlight;
- не убирать corner mask;
- не анимировать backdrop-filter;
- не ослаблять no-scroll;
- не переписывать Supabase/RPC/migration без доказанной backend-причины;
- не публиковать production root;
- не переиспользовать старый secret URL;
- не merge PR №296 без отдельного решения.
