# Unified mobile discovery shell

Канонический контракт общей мобильной оболочки статической Афиши. Он устраняет
расхождение между календарным прототипом, Search и обычными Astro-страницами, не
переписывая содержимое самих страниц.

> **Implementation status, 2026-07-21.** Контракт ниже пока не означает, что в
> Astro уже существует общий `MobileHeader`: календарь v23 генерирует header
> отдельно, а Search использует drawer из `EventLayout`. Это и есть найденная
> причина визуального расхождения. До production-конвергенции нельзя называть
> совпадающие по смыслу, но разные DOM/CSS реализации «единым компонентом».

## Структура

Один shell владеет тремя независимыми зонами:

1. `MobileHeader` — бренд, drawer и контекст страницы;
2. `MobileBottomNav` **или** event-specific sticky CTA;
3. `MobileToastRegion`, привязанный к нижней границе эффективной шапки.

Страница передаёт явное состояние, а не управляет оболочкой через `body:has()`:

```text
topMode: standard | contextual | immersive
section: afisha | dates | search | personal | null
bottomMode: nav | cta | none
```

### Top modes

- `standard`: резервирует `64px + safe-top`, сохраняет принятый mobile brand
  lockup/drawer. Search, Popular, обычные listings и Personal используют его;
- `contextual`: та же высота, справа атомарно появляется дата/количество/город.
  Это режим календарной страницы, а не вторая шапка;
- `immersive`: прозрачный fixed chrome поверх hero на event detail. После hero
  он получает читаемый фон по уже принятой механике. Bottom nav здесь не
  монтируется.

Root splash остаётся явно задокументированным исключением.

## Bottom navigation

Нижняя навигация имеет четыре top-level пункта и один источник `aria-current`:

| Section | Routes |
|---|---|
| `afisha` | Popular, exhibitions, weekend/general listings, clubs/partners |
| `dates` | today и конкретные calendar dates |
| `search` | Search и materialized query collections |
| `personal` | персональная лента |

Event detail использует `section=null` и `bottomMode=cta`: nav и transactional
CTA взаимоисключаемы. Back возвращает пользователя на предыдущую поверхность с
её history/scroll state; искусственно подсвечивать `Афиша` на скрытой панели не
нужно.

Навигация не исчезает от обычного вертикального скролла: это ломает доступность
top-level переходов и создаёт лишнюю анимацию. Calendar date rail может быть
`bottomAccessory` над nav, а shell сам вычисляет `--mobile-bottom-stack-h` и
content padding.

### Cross-preview composition

Research calendar v23 and Astro Search v24 are separate noindex builds but one
mobile journey. Both dock and drawer resolve routes through the same
`mobileDiscoveryHref` table. With build-time bases configured, `Афиша`, `Даты`,
`Завтра`, `Выходные` and `Для меня` go to the accepted calendar v23 prefix;
`Поиск` goes to the current Search v24 prefix. Unrelated Astro routes such as
`Инфопартнёры` remain local. A Search preview must not silently fall back to its
legacy local `/segodnya/`, `/populyarnoe/` or missing `/dlya-menya/` pages; the
release build and generated-output gate must assert the four exact dock URLs.

Current accepted research composition:

```text
PUBLIC_MOBILE_CALENDAR_BASE_URL=https://kenigevents.ru/preview-20260721-mobile-calendar-v23
PUBLIC_MOBILE_SEARCH_BASE_URL=https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24
```

## City and auth

- город — контекст выборки, а не глобальный top-level раздел. В календаре он
  входит в contextual slot; на Search/listings активные города показываются
  компактной контентной строкой/фильтром с явным сбросом;
- auth остаётся инициируемым из Search и Personal. В header допустим компактный
  avatar/account action после входа, но нельзя убирать Yandex CTA из Search до
  появления равноценного понятного входа: иначе gate поиска становится
  невидимым.

## Toast placement and height budget

Shell публикует переменные:

```css
--mobile-header-h: 64px;
--mobile-top-chrome-bottom: calc(env(safe-area-inset-top) + var(--mobile-header-h));
--mobile-header-handle-overhang: 24px;
--mobile-nav-h: 64px;
--mobile-bottom-stack-h: calc(var(--mobile-nav-h) + env(safe-area-inset-bottom));
```

Toast — overlay без layout shift:

```text
top = --mobile-top-chrome-bottom + --mobile-header-handle-overhang + 8px
left/right = max(12px, safe-area inset)
max height = 72px (две строки и controls)
```

На `390×844` без notch стабильный chrome занимает `64 + 64 = 128px`, оставляя
`716px` viewport до overlay. Safe areas добавляются физическим устройством, но
не должны дважды входить в header/nav height. Toast начинается на 8px ниже
фактического края выступающей бренд-бирки, временно перекрывает верхние `56–72px`
content area и не сдвигает карточки. Drawer open скрывает или
приостанавливает toast, чтобы два слоя не конкурировали.

### Toast runtime policy

`EventLayout` монтирует ровно один `MobileToastRegion` сразу после header. Он
принимает `window.KenigEventsToast.show(...)` и `kenigevents:toast`, показывает
один toast, держит ограниченную FIFO-очередь и заменяет совпадающий
`dedupeKey`, перезапуская его срок. Обычный `info/success` живёт **5 секунд**;
ошибка или сообщение с действием остаётся до явного закрытия/действия. Таймер
и подчёркивание приостанавливаются при удержании, focus, потере видимости и
открытом drawer. Старый timer не может закрыть replacement.

Нижняя линия начинается полной и правым краем отступает к левому
(`transform-origin:left`, `scaleX(1→0)`), то есть визуально показывает остаток,
а не «загрузку». При `prefers-reduced-motion` движение отключено, но линия и
текст состояния остаются. Toast не получает autofocus; controls имеют минимум
`44×44px`, polite/error announcements разделены и не дублируют уже озвученный
inline status.

В общий region перенесены существующие глобальные всплывающие сообщения
keyboard actions, mobile share/copy и phone-copy. Search/auth/quota/progress,
feedback сохранения, calendar state, consent, gallery counters, like animation
и `Не интересно / Отменить` остаются inline/local: их исчезновение без
контекста ухудшило бы управление или создало двойное screen-reader сообщение.

## Acceptance

- один header/drawer DOM contract на Astro mobile surfaces;
- ровно один текущий bottom-nav item; drawer отражает тот же section;
- dock и drawer используют один route resolver; Search v24 возвращается на
  принятые calendar/Popular v23 pages, а не на старые same-prefix templates;
- nav/CTA/none взаимоисключаемы;
- Search/collection=`search`, Today/date=`dates`, Popular/listings=`afisha`,
  Personal=`personal`;
- controls минимум `44×44px`, safe areas и keyboard viewport учтены;
- toast всегда ниже эффективной шапки и не зависит от высоты нижнего стека;
- `standard/contextual/immersive` проверены в 320 и 390 CSS px и при high DPR;
- `prefers-reduced-motion` отключает декоративные перемещения, но не обратную
  связь состояния.

## Rejected alternatives

- отдельный hardcoded bottom nav только для Search;
- скрытие dock на listings или при обычном scroll;
- одновременный nav + event CTA;
- auth только в Personal, из-за чего закрытый Search не объясняет способ входа;
- локальные route guesses в каждом компоненте вместо общего section mapping;
- новый glass/blur/font layer поверх принятой cream/terracotta системы.

## Consultant decision trace, 2026-07-21

Gemini 3.1 Pro (High) подтвердил три top-mode, единый `aria-current`,
взаимоисключение nav/CTA, привязку toast к header и safe-area budget. Его советы
оставить Search hardcoded-исключением, убрать dock с listings и скрывать nav при
скролле отклонены как противоречащие самой цели унификации и постоянной
доступности top-level навигации.

## A/B/C research lab v2, 2026-07-21

Отдельный noindex-lab проверяет shell, а не новую карточку события. Builder —
`scripts/build_mobile_shell_unification_lab.py`. Calendar и Popular копируют
принятый generated v23 donor: event rail DOM, crop, gestures, medallions,
date accessory и bottom dock не пересобираются «по мотивам». Search и Personal
в lab — функциональные specimens для оценки оболочки, а не заявление об их
production-готовности.

### Motion invariant

Первая версия lab отклонена: она ошибочно фиксировала бирку и двигала отдельную
панель. Канонический behavioral donor — mobile Search v24 из commit `3f21baa9`:

```text
<details.mobile-discovery-menu>  ← единственный moving parent
  <summary>brand tag</summary>
  <div>cream plane</div>
</details>
```

`transform` применяется только к общему `details`. В closed state parent
сдвинут на `-var(--plane-h)`, поэтому видна пришитая к его нижнему краю бирка
`x=12, y=0, 120×84`. В open state весь объект возвращается в `translateY(0)`:
бирка оказывается на `y=plane-h` и плоскость полностью видна над ней. Нельзя
анимировать summary и panel отдельно, менять motion на height/max-height,
толкать `main`, вводить backdrop/body scroll lock или убирать бирку раньше plane.

Открытие/закрытие наследует donor: `320ms cubic-bezier(.22,.86,.32,1)`, повторный
tap, Escape, outside click, переход по ссылке и vertical scroll более `24px`.
При закрытии `open` снимается через `340ms`, чтобы panel не исчезла до окончания
общего transform. Reduced motion оставляет те же состояния без интерполяции.

### Общие инварианты

- один закрытый tag bbox на Search/Today/Popular/Personal;
- один 64px four-item dock и ровно один `aria-current`; date accessory остаётся
  только календарным дополнительным уровнем;
- plane — продолжение v23 cream canvas с flat typography/hairlines, без белой
  card, glass, floating islands, massive pills и нового шрифта;
- Search form остаётся в canvas; progress визуально живёт **внутри submit CTA**.
  Рядом допустим только скрытый semantic `role=progressbar`;
- unknown phase не изображает процент; determinate fill монотонен, 100% держится
  `700–800ms`; append имеет собственный control;
- high-DPR обеспечивается donor SVG и 1x/2x/3x media assets, CSS geometry остаётся
  в CSS pixels.

### Сравниваемые варианты

| Вариант | Plane IA | `--plane-h` | Footer policy | Trade-off |
|---|---|---:|---|---|
| A «Строгие строки» | сервисная строка; Сегодня/Завтра/Выходные; Выставки/Клубы/Бесплатно | `152px` | micro-footer на finite Popular/Search/Personal, без footer у Calendar | ближе всего к принятому v23 и проще для первого сравнения; требует дисциплины вторичных ссылок |
| B «Индекс» | слева город/profile/service, справа три действия текущего surface при неизменной геометрии | `160px` | одинаковый компактный `Куда дальше?` на четырёх surface | лидер продуктовой приёмки: лучше объясняет контекст, но labels надо централизовать, чтобы surfaces не расходились |
| C «Тональные зоны» | спокойная service zone и 2×2 typographic index без cards | `184px` | mobile footer отсутствует, service/legal принадлежит plane | лидер визуальной приёмки: чище завершает ленту, но plane выше и legal path менее заметен |

Full desktop `SiteFooter` остаётся на desktop/detail/institutional surfaces. Он не
должен случайно появляться только на Search mobile. Production-выбор A/B/C
определит общий tracked header component/token contract и единый route resolver
для dock, plane и любого terminal/footer.

### Lab acceptance

Playwright проверяет все `3 × 4 × 2` combinations при `320×700` и `390×844`:
closed tag `12,0,120×84`, open `tag.y == plane-h`, horizontal overflow `0`, один
current dock item и отсутствие page errors. Дополнительно проверяются five close
paths, отсутствие backdrop/body lock, неизменность accepted v23 rail selectors и
Search sequence `unknown → 12 → 38 → 64 → 86 → 100` без видимой внешней полосы.
Visual gate сравнивает screenshots с v23 donor и отклоняет card-in-card, detached
tag, pills, Search form внутри plane, внешний progress rail и новый font/palette
даже при формально правильном bbox.

## Global-navigation lab v3, 2026-07-21

Вариант B из v2 был намеренно contextual experiment: правая часть plane менялась
между Search/Today/Popular/Personal. Он не является принятой архитектурой. После
product feedback добавлен отдельный D/E/F lab, где проверяется ожидаемая роль
верхнего объекта как глобального меню. Builder —
`scripts/build_mobile_shell_global_nav_lab.py`, build id —
`preview-20260721-mobile-shell-global-nav-lab-v3`.

Разделение ответственности в новой серии:

- bottom dock — четыре primary discovery destination: Popular, Dates, Search,
  Personal;
- top plane — устойчивый город/account и вторичные глобальные destinations;
- transient filters, дата календаря и действия карточки остаются в canvas;
- D/E не меняют ни одного label при переходе между четырьмя страницами;
- F меняет только одну явно подписанную строку `На этой странице`; глобальные
  `138px` из `186px` остаются неизменными.

| Вариант | Plane IA | Высота | Context change | Footer policy |
|---|---|---:|---|---|
| D «Глобальное меню» | город/account; Рубрики/Площадки/Подборки; О проекте/Поддержка/Документы | `164px` | отсутствует | mobile footer отсутствует на всех четырёх surface; service/legal принадлежат plane |
| E «Карта афиши» | город/account; две колонки Каталог и Редакция с устойчивыми сущностями сайта | `180px` | отсутствует | одинаковый compact legal footer на всех surface |
| F «Глобальное + контекст» | глобальные `138px` как неизменная база; отдельная secondary row | `186px` | ровно два коротких действия под label `На этой странице` | одинаковый compact legal footer на всех surface |

Gemini Pro был привлечён отдельно как product analyst и mobile UI designer.
Первичные предложения не переносились автоматически: отклонены дубли dock в
top plane, возврат к quick-filter menu и horizontal scroll contextual row.
Итоговые D/E/F — синтез проектировщика с этими критическими ограничениями, а не
выбор одного необработанного consultant output.

Playwright gate покрывает `3 × 4 × 2` состояния при `320×700` и `390×844`:
closed tag `12,0,120×84`, open `tag.y == plane-h`, отсутствие horizontal
overflow/page errors, один current dock item и отсутствие внутреннего scroll у
plane. Дополнительно D/E обязаны иметь один и тот же normalized panel text на
всех четырёх страницах, а F — один invariant global subtree и ровно одну
contextual row фиксированной высоты `48px`.

## Factual-navigation lab v4, 2026-07-22

D/E/F lab не является источником информационной архитектуры: его абстрактные
`Рубрики / Площадки / Подборки` отклонены. Новый noindex-lab строится от
фактических `HEADER_NAVIGATION`, `DRAWER_NAVIGATION`, `MobileBottomNav` и от
контрактов ветки `origin/docs/static-site-release-plan-20260717`. Builder —
`scripts/build_mobile_shell_factual_nav_lab.py`, build id —
`preview-20260721-mobile-shell-factual-nav-lab-v4`.

Top plane остаётся **глобальным и одинаковым** при переходе между Главной,
календарём, Популярным, Поиском и Для меня. Выбранный пункт нижнего dock не
меняет состав верхнего меню. Сохраняется принятая механика: cream plane и
бренд-бирка являются одним moving object; плоскость полностью уезжает вверх, а
бирка остаётся её нижним краем. Calendar/Popular rails, crop, gestures и
медальоны наследуются из v23 donor, а Search progress остаётся внутри submit
CTA.

### Фактическая карта

- current: `Главная`, `Сегодня`, `Завтра`, `Выходные`, `Выставки`,
  `Популярное`, `Клубы` (feature flag), `Поиск`, `Для меня`,
  `Инфопартнёры`;
- bottom dock: `Афиша`, `Даты`, `Поиск`, `Для меня`;
- first release target: глобальные account actions и `Моё избранное`;
- post-release only: `Фестивали`. Пока `/festivali/` отсутствует, этот пункт
  допустим только как неактивный `Фестивали · позже`, без `href`;
- institutional footer: `Инфопартнёры`, `Информационное партнёрство`,
  `Правообладателям`.

В current drawer название `Все анонсы` заменено на явно запрошенное владельцем
`Главная`; URL остаётся `/`. `Городской обзор` не включён в меню: в release plan
это возможный блок Главной, а не отдельный destination. Не существующие сейчас
`Площадки`, `Организаторы` и `Журнал` не выдаются за live top-level routes.

| Вариант | Назначение | Plane | Что проверяем |
|---|---|---|---|
| G «Текущий сайт» | контроль фактической IA сегодня | только существующие current destinations | насколько достаточен минимальный drawer без roadmap-функций |
| H «Первый релиз» | основной release-кандидат | current + account/service + `Моё избранное` | универсальная глобальная оболочка без ложных обещаний |
| I «Релиз + фестивали» | проверка будущей ёмкости | H + неактивный `Фестивали · позже` | выдержит ли компоновка будущий раздел без преждевременного live-route |

Главные страницы вариантов действительно находятся в корне variant URL;
Search находится на `/poisk/`. Lab также материализует локальные страницы всех
показываемых ссылок, чтобы сравнение на телефоне не перебрасывало на старые
preview templates.

### Gate

Local Playwright покрывает `3 × 4 × 2` состояния при 320/390 CSS px и DPR 3:
whole-object bbox, отсутствие horizontal overflow и clipping, стабильный текст
plane между страницами, фактические четыре labels dock и truthfulness фестиваля.
Дополнительно проверяются 36 variant-route URL. Вариант I обязан иметь ровно
один `aria-disabled` festival item и ноль ссылок на `/festivali/`; G/H не имеют
festival item.

Запрошенная внешняя приёмка Gemini Pro была инициирована через agy для
`gemini-3.1-pro-preview` (High), но 2026-07-22 UTC остановлена provider gate:
`Your current account is not eligible for Antigravity, because it is not
currently available in your location` на redacted interactive account lane.
Evidence: `artifacts/codex/mobile-shell-factual-nav-v4-20260721/gemini-product.typescript`.
Допустимый fallback Claude `Opus` также заблокирован отсутствующей авторизацией
(`Not logged in · Please run /login`; evidence:
`artifacts/codex/mobile-shell-factual-nav-v4-20260721/opus-fallback-product.md`).
Поэтому v4 нельзя маркировать как завершивший external consultant review:
фактическая IA подтверждена кодом и release-plan branch, а внешняя критическая
приёмка остаётся pending.

## Contrast-navigation lab v5, 2026-07-22

Telegram review v4 выявил три самостоятельных дефекта: open plane имел тот же
цвет, что и canvas; все сравнения оставались вариациями одной прямоугольной
сетки; визуальная масса шапки была слишком лёгкой. Новый builder —
`scripts/build_mobile_shell_contrast_nav_lab.py`, build id —
`preview-20260722-mobile-shell-contrast-nav-lab-v5`.

Чтобы сравнивать именно композицию и вес chrome, во всех трёх вариантах
зафиксирована одна factual release IA: account/share, Главная, Моё избранное,
Сегодня/Завтра/Выходные, Выставки/Популярное/Клубы, Поиск/Для меня/
Инфопартнёры и неактивный `Фестивали · позже`. Нижний dock также неизменен:
`Афиша / Даты / Поиск / Для меня`.

| Вариант | Композиция | Plane | Visual weight |
|---|---|---|---|
| J «Типографическая плоскость» | свободные строки без внутренних границ; иерархия только кеглем, весом и интервалами | тёплый sand `#f2e3d1`, accent edge и shadow | средний, лидер безопасной эволюции |
| K «Асимметричный индекс» | один терракотовый вертикальный якорь и свободный редакционный flow справа | split `#b55a38 / #fffaf2`, без матрицы ячеек | средне-тяжёлый |
| L «Терракотовый монолит» | цельная брендовая плоскость без карточек и клеток | `#ad4926`, более тёмный самостоятельный tag `#70250f` | намеренно тяжёлый, проверка верхнего предела |

Plane во всех вариантах отличается от canvas не только цветом, но и жёсткой
нижней границей с shadow. K/L используют отличающийся оттенок tag, border и
shadow, поэтому tag остаётся самостоятельной перекрывающей деталью, а не
растворяется в терракотовом plane.

Bottom dock и micro-footer намеренно остаются светлыми. Тяжёлый низ вместе с
тяжёлой шапкой создаёт «сэндвич», конкурирует с brand tag и визуально зажимает
ленту. Полный desktop/SEO footer не переносится в мобильный canvas; остаётся
только компактная institutional строка.

Предложение Gemini добавить scrim и body lock отклонено: оно конфликтует с
принятым свободным скроллом и не требуется после явного surface contrast,
accent edge и shadow. Механика plane+tag как одного moving object, v23 rails,
media/crop/gestures и route-local links не меняются.

### Acceptance

Playwright покрывает `3 × 4 × 2` open/route состояния при 320/390 CSS px и DPR
3 плюс 36 внутренних route URL: horizontal overflow `0`, clipping `0`, все
interactive/disabled targets не ниже 44px, page errors `0`, состав plane
неизменен между pages, festival item disabled и не имеет `href`.

Gemini 3.1 Pro (High) был доступен 2026-07-22: выполнены отдельные product и
visual reviews. Первичная acceptance обнаружила слияние tag/plane в K/L; после
разведения оттенков, border и shadow повторная visual acceptance подтвердила:
`visual blockers ... не осталось`, итог **SHIP** для исследовательского lab.
Artifacts: `artifacts/codex/mobile-shell-factual-nav-v4-20260722/gemini-product-review.md`,
`gemini-designer-review.md`, `gemini-v5-acceptance.md`,
`gemini-v5-reacceptance.md`.

## Reference-4 leather-tab challenger v6, 2026-07-22

Отдельный вариант M адаптирует композиционный принцип из
`references/mobile menu reference (4).png`, но не копирует бутафорскую
навигацию референса. Builder —
`scripts/build_mobile_menu_reference4_lab.py`, build id —
`preview-20260722-mobile-menu-reference4-lab-v6`.

### Что перенесено и что сознательно отклонено

- Справа остаётся самостоятельная «кожаная» бирка. В закрытом состоянии она
  показывает канонический SVG `Анонсы` с длинной «о» и подпись `Меню`, в
  открытом — `× / Закрыть`. Бирка прикреплена к нижней правой кромке plane и
  едет вместе с ним: это сохраняет принятый whole-object motion contract.
- В верхней части plane используется тот же канонический wordmark, а не текст,
  растянутый CSS и не логотип из референса. Ссылка явно подписана `Главная`.
- Быстрые даты имеют точные labels `Сегодня / Завтра / Выходные`.
- Factual IA первого релиза: `Выставки / Популярное / Клубы`, account,
  избранное, инфопартнёры и share. `Фестивали` показаны только как
  `aria-disabled` слот `позже`; неподтверждённые `Мои билеты`, `Бесплатно` и
  `Детям` не импортированы из референса.
- `Поиск` и `Для меня` не повторяются внутри plane: они остаются глобальными
  пунктами нижнего dock. Это уменьшает дублирование и сохраняет четыре
  стабильные точки для большого пальца.
- Полноэкранное стекло/blur и body lock отклонены: они тяжелее, скрывают
  контекст ленты и нарушают принятую возможность продолжить вертикальный
  скролл. Под plane есть только лёгкий scrim. Он является настоящей кнопкой
  закрытия, поэтому визуально приглушённая карточка не остаётся случайно
  кликабельной; вертикальный жест и существующее auto-close сохраняются.

«Кожа» — намеренное research-only отклонение от текущего brand contract, где
production-бирка однотонная. Она реализована детерминированной CSS-текстурой
без растрового ассета и не считается принятой в дизайн-систему без отдельного
brand sign-off.

### Продуктовое и визуальное сравнение

| Вариант | Сильные стороны | Риски и цена | Роль |
|---|---|---|---|
| J, типографическая плоскость | минимальная когнитивная нагрузка, наиболее безопасная эволюция production shell | меньше характера и слабее дата-first считывание | production baseline |
| K, асимметричный индекс | явная композиционная ось, хорошая редакционная иерархия | левый якорь и неоднородная плотность менее нейтральны | альтернативный challenger |
| L, терракотовый монолит | максимальная брендовая заметность, быстрый крупный scan | слишком много accent color, тяжелее лента | верхняя граница visual weight |
| M, кожаная бирка справа | лучше всего считываются даты и разделы; закрытый handle сам объясняет меню; удобен правому большому пальцу | plane выше J, праворукость решения, декоративная «кожа» требует проверки бренда и производительности | сильный исследовательский challenger |

M не назначается production-победителем автоматически. Рекомендация —
сохранить J как безопасный baseline и проверить M на телефонах: открываемость
меню, ошибочные закрытия через scrim, долю переходов по датам/разделам,
left-handed reach и субъективное восприятие «кожи». В отличие от прошлых
J/K/L, M лучше воспроизводит информационную иерархию референса, но имеет
наибольшую цену брендового отклонения.

### Acceptance

Local Playwright покрывает variant M на главной, Search, Today и Popular при
320/390 CSS px и DPR 3: 8 open/closed states и 12 route checks. Horizontal
overflow, clipping, short targets и page errors — `0`; wordmark использует
`viewBox="0 0 7819 1514"`, даты точные, disabled Festivals не имеет `href`,
scrim закрывает меню без смены URL.

Gemini 3.1 Pro (High) отдельно выполнил product/design review и повторную
критическую арбитражную приёмку. Первичная визуальная приёмка ошибочно
трактовала сохранённый free-scroll contract как дефект; после сверки DOM,
геометрии перекрытия и interaction contract арбитраж признал это замечание
невалидным и дал итог **SHIP для research preview**, без production sign-off
на кожаную фактуру. Artifacts:
`artifacts/codex/mobile-menu-reference4-20260722/gemini-adaptation-review.md`,
`gemini-implementation-acceptance.md`, `gemini-arbitration-review.md`.
