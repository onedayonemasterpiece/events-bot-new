# Event card UI A/B — 2026-06-27

## Цель

Проверить, помогает ли двухстрочная схема действий в мобильной ленте события сделать карточку менее перегруженной, вернуть `В календарь` для однодневных событий и сохранить главный продуктовый принцип: крупный лайк остаётся справа снизу, в зоне правого большого пальца.

## v26 decision

After product review and the v26 Opus fallback consultation, `split-actions` is accepted as the baseline for normal event-detail discovery feeds. `overlay-controls` remains a historical/rejected variant: it overloaded the image/card bottom area and made service actions too prominent. The under-card row must keep visible `Поделиться` text and a large like at the right-thumb end. Calendar appears for all calendar-eligible feed cards; `Не интересно` stays low-priority and should move toward an undo-safe destructive-action pattern before production.

## Варианты

### A — `overlay-controls`

Исторический контрольный вариант, no longer used on normal event pages:

- одна строка действий поверх нижней части карточки;
- слева вторичное `Не интересно`, по центру `Поделиться`, справа крупный лайк;
- календарь в feed не показывается, чтобы не перегружать строку;
- плюс: карточка компактнее, поведение ближе к текущему v17;
- риск: нижняя строка конкурирует с контентом карточки и хуже масштабируется при добавлении действий.

Preview: <https://kenigevents.ru/preview-20260627-event-pages-v19/sobytiya/pesni-sssr-svetlogorsk-5878/>

### B — `split-actions`

Принятый baseline-вариант для normal event pages:

- первая строка внутри карточки: `В календарь` только для calendar-eligible/one-day событий + вторичное `Не интересно`;
- вторая строка под карточкой: видимое текстовое `Поделиться` + лайк последним элементом справа; визуально это icon/text actions без кругляшков/pill-фона, но с 44px hit area;
- плюс: меньше случайного снятия лайка, легче вернуть календарь, share сохраняет accessible подпись, но под карточкой выглядит как иконка;
- риск: карточка становится выше и между карточками появляется больше воздуха; без подписи под карточкой share может быть менее очевиден, поэтому его нужно держать лаконичным и не возвращать share в overlay/pill.

Preview: <https://kenigevents.ru/preview-20260627-event-pages-v19/sobytiya/den-valyaniya-v-sene-romanovo-6322/>

## Продуктовая гипотеза

B принят как stronger default для мобильной ленты: лайк остаётся самым доступным действием, share получает понятную подпись под карточкой, а календарь получает место без перегруза. A не должен использоваться на normal event pages; он допустим только как исторический лабораторный референс.

## Что считать успехом перед production

- Визуально: карточка остаётся image-first; нет горизонтального rail; действия не перекрывают критичный OCR-текст афиши.
- UX: лайк справа снизу, share вызывает native share/copy fallback, calendar есть только у однодневных событий.
- Архитектура: `data-feed-card-variant="split-actions"` задаётся на уровне страницы события и применяется ко всем preloaded + JSON-hydrated карточкам одной ленты.
- SEO/GEO: карточки по-прежнему имеют реальные `<a>` на media/title; full-card click добавляется JS поверх, без nested anchor.
- Проверки: `site/scripts/check-preview.mjs` контролирует split-actions baseline, visible under-card share label, no overlay controls on normal pages, Event JSON-LD, calendar gating, absence of zero counters, media ratio rules, hero modes и static-10 + JSON hydration contract.

## Артефакты локальной визуальной проверки

Не коммитятся, но доступны в рабочей сессии:

- `artifacts/codex/static-site-ui-ab-v18/index-mobile.png`
- `artifacts/codex/static-site-ui-ab-v18/variant-a-mobile-5878-tall.png`
- `artifacts/codex/static-site-ui-ab-v18/variant-b-mobile-6322-tall.png`
- `artifacts/codex/static-site-ui-ab-v18/variant-a-desktop-5878.png`
- `artifacts/codex/static-site-ui-ab-v18/variant-b-desktop-6322.png`
