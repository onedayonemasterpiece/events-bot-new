# Handoff: статическая prelaunch-заглушка — новый secret candidate

> Этот файл предназначен для продолжения работы в новом окне ChatGPT без потери контекста. Начните с раздела **«Промпт для нового окна»** и выполняйте задачу самостоятельно через GitHub/GitHub Actions. Кодового агента не привлекать.

## Промпт для нового окна

```text
@GitHub https://github.com/onedayonemasterpiece/events-bot-new

Продолжи и ЗАВЕРШИ работу над облегчённой статической prelaunch-заглушкой «Полюбить Калининград Анонсы».

Критический контекст:

1. Пользователь НЕ просит ещё одну переработку тяжёлой динамической стеклянной сцены.
2. Для быстрого продуктового результата уже сгенерированы две готовые фоновые картинки со стеклянными плитками, светом и PWA-образом:
   - site/public/assets/prelaunch/prelaunch-scene-desktop.webp
   - site/public/assets/prelaunch/prelaunch-scene-mobile.webp
3. Поверх фона остаются живыми HTML/UI-элементы: бренд, заголовок, описание, форма email, consent, состояния success/error.
4. Цель: новая секретная ссылка `https://kenigevents.ru/_review/<NEW_TOKEN>/`, на которой открывается ИМЕННО облегчённая статическая версия и работает отправка email.
5. Не возвращать пользователю URL из старого publication run `31174523587`: он указывает на прежнюю тяжёлую динамическую версию.
6. Не останавливаться с промежуточным отчётом. Продолжать цикл самостоятельно до новой рабочей secret URL либо до реального внешнего блокера, который нельзя устранить без пользователя.

Текущая рабочая ветка статической версии:

- branch: `agent/prelaunch-static-background-20260807`
- head SHA до добавления этого handoff-файла: `71537bd213dbf3775a511024542dc4b3448b6ca0`
- последний зелёный визуальный run: `31184589333`
- workflow: `Prelaunch visual review`
- artifact: `prelaunch-evidence-31184589333`
- artifact digest: `sha256:44da26803bfba2de5311fe98d113c2101c617c3185406ff7a4a263b4c76b964f`

Что уже реализовано в static branch:

- `site/src/components/PrelaunchPage.astro`
  - использует `<picture>` с desktop/mobile backgrounds;
  - НЕ импортирует тяжёлый `prelaunchScene.ts`;
  - импортирует только живую форму `prelaunchForm.ts`;
  - содержит email, обязательное согласие, CTA и success-state.
- `site/src/styles/prelaunch-static.css`
  - статическая полноэкранная сцена;
  - без `backdrop-filter` на сотне тайлов;
  - без динамической мозаики и desktop lag.
- `site/src/scripts/prelaunchForm.ts`
  - строгая нормализация/валидация email;
  - direct Supabase + resilient relay route;
  - идемпотентный RPC `register_prelaunch_notification_v1`;
  - consent version `prelaunch-updates-2026-v1`;
  - состояния idle/submitting/error/success/registered.
- `site/src/layouts/PrelaunchLayout.astro`
  - viewport: `maximum-scale=1, user-scalable=no, viewport-fit=cover`;
  - SEO/GEO: title, description, canonical, hreflang, OG, Twitter, JSON-LD Organization/WebSite/WebPage/Service;
  - production root indexable, secret candidate noindex/no-referrer.
- source contracts и browser evidence прошли в run `31184589333`.

Скриншоты в artifact `prelaunch-evidence-31184589333`:

- `prelaunch-visual-wide-1728x900.png`
- `prelaunch-visual-desktop-1440x900.png`
- `prelaunch-reference-square-1200x1200.png`
- `prelaunch-mobile-390x844.png`
- `prelaunch-mobile-small-320x568.png`
- `prelaunch-mobile-landscape-844x390.png`
- viewport-fit и form-security evidence рядом.

Почему новая ссылка до сих пор не появилась:

- существующий publication workflow `.github/workflows/prelaunch-v12-publish.yml` жёстко ожидает:
  - branch `agent/prelaunch-landing-20260803`;
  - workflow `Prelaunch v12 visual control`;
  - artifact `prelaunch-v12-visual-<run>`;
  - старые dynamic-scene browser gates.
- Поэтому ранее был повторно выдан URL старого dynamic candidate, хотя static branch уже существовала. Это ошибка процесса, а не отсутствие статической версии.

Что нужно сделать сейчас:

A. Быстро осмотреть глазами PNG из run `31184589333`.
- Проверить только продуктовые блокеры: фон реально виден, desktop/mobile композиция приемлема, текст и форма читаемы, нет скролла.
- Не затевать новый дизайн и не возвращаться к динамической стеклянной архитектуре.
- Если визуал приемлем — сразу переходить к публикации.

B. Сделать совместимый publication path для static branch.
Предпочтительно минимально адаптировать control/publication workflow либо создать отдельный static publication workflow.

Обязательные изменения публикационного контура:

1. Разрешить exact target branch:
   `agent/prelaunch-static-background-20260807`
2. Разрешить exact target SHA текущего static branch.
3. Принимать зелёный run `Prelaunch visual review` и artifact `prelaunch-evidence-<run_id>` либо сначала выпустить совместимый exact-SHA receipt для static branch.
4. Не запускать старые проверки, ожидающие DOM-тайлы `[data-prelaunch-tile]`.
5. Для публичной проверки static candidate использовать актуальные проверки:
   - `npm --prefix site run check:prelaunch-scene`
   - `node site/scripts/check-prelaunch-viewport-fit.mjs`
   - `node site/scripts/check-prelaunch-form-security.mjs`
   либо эквивалентный static-specific gate.
6. Build должен использовать существующие production environment variables для Supabase, relay и Yandex Object Storage.
7. Honeypot/direct probe должен использовать актуальный consent:
   `prelaunch-updates-2026-v1`
   — не старый `launch-2026-09-01-v1`.
8. Создать новый случайный 256-bit review token.
9. Опубликовать create-only в НОВЫЙ `_review/<token>/`.
10. Production root и старые review prefixes не менять.

C. После публикации проверить уже публичный URL.

- открыть public URL через Playwright;
- снять desktop 1728×900 и mobile 390×844;
- глазами проверить, что это static-background версия, а не прежняя динамическая;
- проверить noindex/no-referrer;
- проверить форму:
  - invalid email rejected до сети;
  - consent required;
  - direct path работает;
  - relay path настроен и используется как fallback, если доступен;
  - повтор идемпотентен;
  - success-state отображается.

D. Финальный ответ пользователю должен начать с прямой ссылки:

`https://kenigevents.ru/_review/<NEW_TOKEN>/`

Затем кратко:

- static feature SHA;
- visual run ID;
- publication run ID;
- form direct/relay status;
- production root changed: no.

Не возвращать старую ссылку. Не писать «следующий шаг». Не передавать задачу кодовому агенту. Результат этого окна — рабочая новая secret URL.
```

## Зафиксированная продуктовая цель

Нужна простая и быстрая заглушка сайта:

- визуально дорогая и похожая на исходный референс;
- без тормозов на desktop;
- адаптивная на mobile;
- без скролла на основных viewport;
- с работающим сбором email;
- с надёжной передачей direct Supabase / Yandex relay;
- с корректным SEO/GEO для будущей публикации в корне;
- сейчас — только по новой секретной ссылке.

## Не повторять

- Не выдавать URL старого dynamic candidate как результат static work.
- Не считать зелёный локальный visual run опубликованной страницей.
- Не возвращаться к 100+ живым стеклянным тайлам.
- Не тратить новый цикл на формальные метрики без просмотра PNG.
- Не останавливаться после первого исправимого failure.
- Не менять production root до ручного принятия новой secret URL.

## Полезные ссылки

- Static branch:
  `https://github.com/onedayonemasterpiece/events-bot-new/tree/agent/prelaunch-static-background-20260807`
- Static visual run:
  `https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31184589333`
- Старый publication run — только как отрицательный пример, URL из него НЕ использовать:
  `https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31174523587`
