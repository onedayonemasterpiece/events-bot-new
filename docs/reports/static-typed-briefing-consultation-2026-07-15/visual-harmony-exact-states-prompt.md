# Критический visual acceptance review — две точные desktop-сцены briefing hero

Ты — внешний арт-директор и senior product/motion designer. Нужен жёсткий acceptance review, а не поддерживающий комментарий. Явно ставь **PASS** или **FAIL** каждой сцене и общий вердикт. Не предлагай «варианты ради вариантов»: назови одну рекомендуемую систему.

## Важное ограничение предыдущего ревью

Предыдущее Gemini acceptance **НЕ рассматривало эти точные состояния** `anticipated_person_named` (малое медиа) и `weather_water_demo` (крупная типографика). Оно касалось другой wide-media сцены и mobile. Поэтому не наследуй прежний PASS и оцени заново с нуля.

## Артефакты (прочитай сами PNG, не только описание)

1. `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/captures/anticipated-person-named-stable-1920x900.png`
   - реальный public build, viewport 1920×900, исходный URL сохранён;
   - hero: x=370, y=89, w=1180, h=378 = 42vh;
   - h1: x=370, y≈216, w≈802, h≈95, font 45.6px/44.7px;
   - small media: x≈1236, y≈142, w=290, h≈272; с радиусом/тенью;
   - header terracotta badge показывает только «ПОЛЮБИТЬ КАЛИНИНГРАД», графическая часть утверждённого lockup «Анонсы» исчезла.

2. `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/captures/weather-water-demo-user-next-state-1920x900.png`
   - реальная public геометрия, состояние публичной кнопки `Показать следующее` визуально восстановлено по приложенному пользователем скриншоту (только `hidden=false`, прочая DOM/CSS-геометрия не изменена);
   - hero: x=370, y=89, w=1180, h=378 = 42vh;
   - h1: x=370, y≈188, w=1180, h≈150, font 72px/70.6px;
   - декоративная широкая O справа;
   - CTA-ссылка и public Next стоят в одной строке;
   - тот же потерянный графический lockup «Анонсы» в терракотовой бирке.

3. Контрольный естественный initial state второй сцены (Next скрыта):
`/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/captures/weather-water-demo-stable-1920x900.png`

## Продуктовая рамка

- Это не лабораторная карточка, а чистый первый экран главной: 1–3 строки сильной типографики, короткая кинетическая коммуникация и быстрый вход в категории/ленту.
- Hero обязан занимать не более 50% видимого экрана и не выглядеть отдельным «фреймом внутри страницы».
- Малое медиа — редкий editorial-приём, а не обычная event card.
- Типографика и медиа должны быть одной композицией. Нельзя добавлять лишнюю оболочку/рамку.
- Публичная кнопка Next допустима только в остановленном состоянии цепочки и должна быть вторичной относительно смыслового CTA.
- Декоративная O должна оставаться настоящим брендовым ассетом; header обязан показывать узнаваемый утверждённый lockup с «Анонсы», не текстовую имитацию.

## Ответь строго по структуре

1. **Вердикт**: сцена 1 PASS/FAIL; сцена 2 PASS/FAIL; overall PASS/FAIL. Одна фраза почему.
2. **Что именно сломано**, ранжируя P0/P1/P2:
   - интеграция small media vs «оторванная карточка» и мёртвое пространство;
   - масштаб/переносы/оптический вес weather typography;
   - потеря логотипа/lockup в header badge;
   - CTA/Next и их иерархия;
   - соблюдение <=50vh как формально, так и визуально.
3. **Одна рекомендуемая desktop-система** с конкретной геометрией для 1920, 1440 и 1366 px:
   - max-width/внешние отступы hero;
   - высота hero (px/vh bounds);
   - text column width или grid fractions;
   - h1 min/preferred/max size и line-height; целевое число строк;
   - small media width/height/aspect, позиция, crop, нужны ли radius/shadow;
   - место декоративной O;
   - размеры/внутренние отступы header badge и минимальная безопасная зона для полного lockup;
   - gap/типографика/форма primary CTA и public Next.
4. **Motion/kinematics**: 5–8 точных параметров (durations, easing, порядок появления small media, момент ухода, поведение cursor, reduced-motion). Не превращать медиа в слайд-шоу/карточку.
5. **Минимальный patch brief**: что разработчику изменить сейчас, без редизайна ленты и без новых изображений.
6. **Acceptance checklist**: измеримые пункты для скриншотов 1920×900, 1440×900 и 1366×768; отдельно graceful behavior при длинном русском имени.
7. **Риск после фикса**: какие 2–4 вещи всё равно надо проверить человеком.

Пиши по-русски, конкретно, критично. Не оценивай карточки ленты — они только контекст масштаба.
