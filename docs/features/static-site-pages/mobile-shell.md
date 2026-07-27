# Unified mobile discovery shell

Канонический контракт общей мобильной оболочки статической Афиши. Он устраняет
расхождение между календарным прототипом, Search и обычными Astro-страницами, не
переписывая содержимое самих страниц.

> **Implementation status, 2026-07-27.** Общая Astro-оболочка реализована:
> `EventLayout` ровно один раз монтирует `Reference4MobileMenu`,
> `MobileBottomNav`, `MobileToastRegion` и общий `StaticSiteAuthRuntime`.
> Calendar/Popular rails сохраняют принятый v23 behavioral donor, но больше не
> имеют отдельной «уменьшенной desktop» шапки. Исторические split-preview base
> URLs ниже остаются только исследовательским evidence; единый release build
> использует свой `BASE_PATH`.

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
`Партнёры` remain local. A Search preview must not silently fall back to its
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
- auth принадлежит одному origin-scoped Supabase PKCE controller, а не Search.
  Search, Personal и mobile menu читают одну session и показывают одну и ту же
  identity; вход через Yandex доступен как из Search, так и непосредственно из
  menu/Personal. После входа menu показывает compact account/identity action;
- `EventLayout` монтирует один `StaticSiteAuthRuntime`. В DOM/custom events
  нельзя публиковать access/refresh tokens. OAuth callback очищается на любой
  статической странице, а origin-scoped session сохраняется при переходах
  между immutable review prefixes.

## R14 shell/auth integration, 2026-07-27

- `Reference4MobileMenu` ведёт `Бесплатно` на материализованную
  `/podborki/besplatnye-sobytiya/`, а не заполняет Search;
- `Для меня` и menu не имеют отдельных auth clients и не отправляют пользователя
  «войти через поиск»;
- обычный Enter в Search вызывает native `form.requestSubmit()`, IME/composition
  не прерывается, а `enterkeyhint="search"` даёт мобильной клавиатуре правильное
  действие;
- Calendar/Popular/listing/event routes используют один mobile shell contract;
  локальные header/drawer forks блокируют release;
- полная browser-приёмка реального Yandex round-trip остаётся обязательной на
  замороженном immutable candidate; mocked PKCE/Edge smoke не подменяет её.

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

## R6 mobile acceptance, 2026-07-23

Последняя продуктовая приёмка уточняет event-detail и terminal behavior:

- мобильная страница события не монтирует хлебные крошки или отдельную
  `← Афиша`-строку; desktop semantic hierarchy и JSON-LD не меняются;
- event sticky CTA скрыт над hero, доступен между hero и рекомендациями и после
  прохождения границы связанных событий больше не появляется повторно у
  подвала. Единственное основное действие при этом не удаляется;
- если адаптивная панель оставляет широкую кнопку Share, она показывает
  `Поделиться`, а не только пиктограмму;
- меню остаётся одним настоящим полупрозрачным glass-слоем. Читаемость
  canonical lockup на светлом и тёмном hero обеспечивает локальный светящийся
  translucent scrim под логотипом, а не непрозрачная заливка всей панели;
- около мобильного подвала находится progressive PWA action. Он скрыт по
  умолчанию и открывается только на Android после фактического
  `beforeinstallprompt`. Событие сохраняется для одного `prompt()`, кнопка
  атомарно скрывается до ожидания системного диалога, а `appinstalled` очищает
  состояние. Никакого постоянного флага «уже устанавливал» нет: после удаления
  приложения новый browser event снова может вооружить кнопку.

Manifest использует base-aware `id`, `scope`, `start_url`, `display=standalone`
и PNG `192×192`/`512×512`; preview publisher отдельно сохраняет
`application/manifest+json`. Launcher-name намеренно короткий — `Анонсы` и в
`name`, и в `short_name`: полный вариант рядом с Android-иконкой обрезается, а
полный бренд `Полюбить Калининград / Анонсы` уже встроен в утверждённую
кожаную иконку. Из `docs/reference/PWA-icon.png` локальный deterministic
generator создаёт обычную и maskable-пару обоих размеров; versioned
manifest-link принудительно обновляет ранее закешированную иконку.
Поведение соответствует текущим контрактам
[MDN `beforeinstallprompt`](https://developer.mozilla.org/en-US/docs/Web/API/Window/beforeinstallprompt_event),
[MDN install prompt](https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps/How_to/Trigger_install_prompt)
и [installability guide](https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps/Guides/Making_PWAs_installable).

### Presentation QR install entry

Ссылка `https://kenigevents.ru/?install=presentation` является стабильным
presentation entry point. Она не обходит системное подтверждение установки, но
на Android сразу показывает фиксированную install-card:

- до получения `beforeinstallprompt` карточка объясняет, что страницу нужно
  открыть в Chrome, и даёт резервный маршрут
  `меню Chrome → Добавить на главный экран`;
- после реального browser event появляется активная кнопка
  `Установить приложение`, которая вызывает сохранённый event ровно один раз;
- после принятия/отклонения системного окна остаётся честный статус, а
  `appinstalled` показывает успешное завершение;
- обычные страницы без `install=presentation` сохраняют progressive footer
  contract и не показывают install UI до browser event.

Production acceptance для этого URL требует `200` на `/manifest.webmanifest`,
MIME `application/manifest+json`, доступные PNG-иконки `192×192` и `512×512`,
наличие manifest-link и presentation controller в `/`, Android browser smoke и
достижимость deployed SHA из `origin/main`.

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
  `Партнёры`;
- bottom dock: `Афиша`, `Даты`, `Поиск`, `Для меня`;
- first release target: глобальные account actions и `Моё избранное`;
- post-release only: `Фестивали`. Пока `/festivali/` отсутствует, этот пункт
  допустим только как неактивный `Фестивали · позже`, без `href`;
- institutional footer: `Партнёры`, `Информационное партнёрство`,
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
Партнёры и неактивный `Фестивали · позже`. Нижний dock также неизменен:
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

> **Superseded / rejected after owner review.** v6 неправильно применил
> референс к закрытому состоянию: заменил стандартную левую бренд-бирку,
> отказался от стекла и иконок и перенёс лишь общую композиционную идею. Он не
> является donor для следующей итерации.

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

## Exact expanded menu from reference 4, v7, 2026-07-22

Вариант N исправляет root cause v6: файл
`references/mobile menu reference (4).png` трактуется только как контракт
**раскрытого** меню. Builder —
`scripts/build_mobile_menu_reference4_exact_lab.py`, build id —
`preview-20260722-mobile-menu-reference4-exact-lab-v7`.

### State contract

- Закрытая `.mobile-discovery-menu__summary` целиком принадлежит существующему
  `build_mobile_shell_unification_lab.py`: слева, `120×84`, однотонная
  `#98401f`, тот же endorsement, wide-o wordmark и chevron. Новый builder не
  переопределяет её markup, размеры, положение или материал.
- После раскрытия стандартная summary только скрывается. Отдельная правая
  кнопка закрытия живёт внутри glass sheet и вызывает общий close path с
  возвратом focus на стандартную бирку.
- Leather surface — прямой lossless crop `333×376` из пользовательского
  референса, отображаемый `112×126 CSS px` на обычном телефоне. Alpha mask
  отделяет силуэт; CSS добавляет только внешние drop shadows. Source bbox и
  processing записаны в
  `site/public/assets/ui/reference4-leather-close.metadata.json`.

### Expanded surface and IA

Sheet занимает доступную высоту до неизменного нижнего dock и использует
реальный `backdrop-filter: blur(25px) saturate(.9)`, тёплый полупрозрачный
gradient, стеклянные cards и белые edges. Dock остаётся видимым и приглушается;
нажатия на него блокируются, пока menu open. На `320×700` sheet имеет один
вертикальный scroll surface, а sticky leather close остаётся на `y=0`.

Верхний home-link — канонический
`announcements-wordmark-ui.svg#announcements-wordmark-ui`, белый, без typeset и
искажения. Далее идут точные chips `Сегодня / Завтра / Выходные`. Единственный
большой list card сохраняет все пункты референса, кроме явно исключённых
`Скоро` и `Мои билеты`:

1. Бесплатно;
2. Детям;
3. Выставки;
4. Фестивали;
5. Популярное;
6. Партнёры;
7. Поиск;
8. Для меня.

Поскольку отдельные production routes Бесплатно/Детям/Фестивали ещё не
материализованы, эти строки truthfully ведут в Search с заполненным `q`, а не
на fake destinations. Utility card содержит `Войти`, `Избранное` и рабочее
действие `Поделиться`.

### Icons and asset provenance

Строки используют визуально проверенное единое семейство Dazzle Line Icons из
SVG Repo. `Бесплатно` — явная композиция Euro Circle + Slash; Share Nodes выбран
отдельно за максимально близкий к референсу трёхузловой силуэт. Durable SVG,
точные SVG Repo IDs и CC BY 4.0/CC0 attribution находятся в
`site/public/assets/icons/reference4/ATTRIBUTION.md`. Иконки выводятся mask-слоем
в одном цвете и одном оптическом размере; пути не перерисовываются.

### Acceptance

Local Playwright: `8` open/closed cases на Home/Today/Search/Popular при
320/390 CSS px и DPR 3, плюс `12` route checks. Подтверждено:

- стандартный closed bbox всегда `x=12, y=0, 120×84`, материал не изменён;
- open panel всегда `y=0`, leather close `y=0` до и после small-screen scroll;
- horizontal overflow, clipped/short targets и page errors — `0`;
- 8 точных list labels, 3 даты, `Поделиться`; `Скоро` и `Мои билеты` отсутствуют;
- live blur/gradient, dimmed inert dock, canonical wide-o symbol и direct
  `333×376` leather crop присутствуют;
- закрытие возвращает focus на standard summary.

Gemini 3.1 Pro (High) получил точный, непереинтерпретируемый owner contract,
исходный reference, 320/390 screenshots и код. Строгая product/design review
подтвердила faithful adaptation, а финальная reacceptance после уменьшения
chevrons и sticky-close дала **SHIP (research preview)** без blockers. Evidence:
`artifacts/codex/mobile-menu-reference4-v7-20260722/gemini-design-review.md` и
`gemini-final-acceptance.md`.

## Full-viewport reference 4 menu, v8, 2026-07-22

Owner review superseded v7 as a visual candidate: its open state omitted the
canonical endorsement, left Share below the initial viewport at `320×700`,
showed the lower dock, used a visually heavy Dazzle set and compounded panel +
card alpha to roughly `88%`. The v8 research builder is
`scripts/build_mobile_menu_reference4_fullglass_lab.py`; build id is
`preview-20260722-mobile-menu-reference4-fullglass-lab-v8`, variant `P`.

### State and geometry

- Closed state still comes unchanged from the shared shell: `x=12, y=0`,
  `120×84`, solid `#98401f`, the canonical endorsement/wordmark/chevron and no
  leather background. v8 overrides only the expanded plane.
- Open state is a navigation mode occupying the whole visual viewport. The
  underlying page is scroll-locked and the 64px dock becomes hidden and inert;
  it is not a second competing navigation layer behind the glass.
- At `320×700`, the brand block is `100px`, date targets are `44px`, eight
  list rows are `48px`, the account targets are `44px` and Share is `44px`.
  Share is initially visible at `y=609…653`; `scrollHeight == clientHeight ==
  700`. At `390×844`, Share is `y=663…713` and the same no-scroll invariant
  holds. Screens below `680px` keep the panel's accessibility scroll fallback
  rather than clipping enlarged text.
- The open lockup adds the exact compact text `Полюбить Калининград` above the
  canonical wide-o SVG `Анонсы`.
- The leather close is a new direct lossless crop `333×332`: `44px`/`11.7%`
  was removed from the top of the previous `333×376` source crop. It renders
  `104×104` at 320 CSS px and `112×112` at 390 CSS px; CSS adds only layout
  shadows.

### Glass and icons

The panel now has one compositor-owned `blur(30px) saturate(1.12)
brightness(1.08)` layer. Warm backgrounds stay mostly in the `.18–.30` alpha
range, while the list/utility panes use `.30–.46`; nested backdrop filters were
removed. A top halo, active-date glow and strong bottom bloom keep the supplied
reference's luminous glass hierarchy without washing out the dark labels.

The implemented set is coherent **Phosphor Thin**. Exact sources, licenses and
the visually reviewed Lucide/Solar/alternate Popular candidates are in
`site/public/assets/icons/reference4-v8/ATTRIBUTION.md`. Product decisions:

- `Бесплатно` uses typographic `0 ₽` inside a thin circle. Both local critical
  review and Gemini 3.1 Pro rejected crossed ₽ because it can mean “rubles/cash
  prohibited”, while `0 ₽` unambiguously means zero ticket price.
- `Популярное` uses Trend Up Thin, not the v7 star. Gemini initially suggested a
  flame as a generic “hot” convention, but the owner's explicit request was an
  allegory of **growth**. In the labelled event-navigation row, the rising line
  is literal and does not inherit a financial-dashboard meaning.
- Festival uses the thin architectural/gate allegory, closer to the supplied
  reference than a party horn; Share uses the reference-like three-node Share
  Network; Personal uses User Focus rather than the generic chat composite.

### Acceptance

Local Playwright covers Home and Search at 320/390 CSS px plus all 12 generated
routes. The machine gate confirms: `4` open/closed cases, `12` HTTP-200 routes,
`0` failures; full-height panels, no initial scroll, Share fully in viewport,
all targets `>=44px`, zero horizontal overflow/page errors, hidden/inert dock,
exact labels, canonical wordmark, `0 ₽`, Trend Up, Share Network and direct
`333×332` leather asset. Escape and the leather button both close through the
shared path; the visible close action restores focus to the standard summary.

Evidence lives in
`artifacts/codex/mobile-menu-reference4-v8-20260722/{report.json,failures.json,open-320.png,open-390.png,gemini-design-review.md,gemini-final-acceptance.md}`.

## Reference 4 glass-depth correction, v9, 2026-07-22

Telegram review superseded the v8 expanded-state lockup: the detached white
wordmark lost contrast over the bright page, the account and Share surfaces
read as one card, and the menu-specific Share had regressed to a link-only
implementation. The corrective research builder is
`scripts/build_mobile_menu_reference4_glass_depth_lab.py`; build id is
`preview-20260722-mobile-menu-reference4-glass-depth-lab-v9`, variant `P`.
This state was subsequently superseded by v10: holding the summary stationary
with a counter-transform made the closed tag float incorrectly above the open
menu.

### One tag, two states

v9 does not draw a second terracotta brand object. The accepted
`.mobile-discovery-menu__summary` remains the same `120×84` DOM node and the
same closed visual. While the full-height plane translates into view, an equal
opposite transform keeps that tag at `x=12, y=0`; the leather control remains
the explicit right-side Close action. This preserves the familiar brand anchor
without the low-contrast detached white lockup or competing duplicate logo.

The sheet uses a darker backdrop sample and weaker glass than v8:
`blur(22px) saturate(.96) brightness(.84)` with low-alpha warm overlays. The
main list and utility remain translucent, but sticky page headings cannot paint
above the drawer: the shell header is promoted to the drawer stacking level
while open. The utility is deliberately two-zone: a slightly dark recessed
account track above a separate light Share surface. The typographic Free sign
remains the unambiguous `0 ₽`, enlarged to `16px` (`15px` at `<=350px`) inside
its 30px circle.

### Factual service drill-down

The former direct `Партнёры` row becomes `О сервисе`. It does not expand the
eight-row card. A horizontal drill-down swaps the list plane in place and keeps
Share fixed, exposing only factual destinations already present in the site:
`Партнёры` (`/partners/`), `Информационное партнёрство`
(`/partnerstvo/`) and the canonical `Правообладателям` mail action. No invented
`О проекте` route is emitted.

### Native Share reuse

The menu action now carries the production `data-native-share` contract used by
`EventHero`, `EventCtaPanel`, `EventCard` and handled in `EventLayout.astro`.
On an event surface it inherits the reviewed event payload, including title,
text, canonical URL and image metadata. The standalone research build mirrors
the same image-first chain: fetch the page/event image, wrap it in a `File`,
guard with `navigator.canShare`, call `navigator.share` with text + URL + file,
then fall back to the branded generated image and finally link/copy. It must not
be replaced again with a menu-local link-only `navigator.share(payload)` call.

### Acceptance

Playwright at `320×700` confirms all 12 factual routes return 200, the retained
tag stays `12,0,120×84`, the panel has no initial vertical scroll, Share ends at
`y=649`, and the menu has no horizontal overflow. At `390×844`, Share ends at
`y=714`. The drill-down leaves Share at the same coordinates and updates
`aria-hidden` / `aria-expanded`. A stubbed native-share probe received title,
text, page URL and an `image/webp` File (`18,852` bytes), rather than only a
Pinterest/site link.

Gemini 3.1 Pro (High) separately inspected the source reference, both owner
screenshots, the implementation and 320/390 renders. Its first review defined
the scrim/glass/utility correction; final acceptance passed six of seven owner
checks and blocked only on the still-small 12px `0 ₽`. After its concrete
15/16px correction, the blocking item is resolved. Evidence:
`artifacts/codex/mobile-menu-reference4-v9-20260722/{gemini-visual-review.txt,gemini-acceptance.txt,open-320-final.png,open-390-fixed.png,service-320.png,route-qa.txt,share-probe.js}`.

## Moving leather brand tag, v10, 2026-07-22

The v10 research builder is
`scripts/build_mobile_menu_reference4_leather_tag_lab.py`; build id is
`preview-20260722-mobile-menu-reference4-leather-tag-lab-v10`, variant `P`.
It corrects the v9 motion model rather than hiding the symptom:

- closed, the summary remains `x=12, y=0, 120×84`;
- during opening it travels down with the same parent plane (`y=564.5` in the
  captured 320px animation frame);
- fully open it starts at `y=700` for `320×700` and `y=844` for `390×844`, so
  no part of the primary tag can float above or intercept the expanded menu;
- the expanded top-left home target is a separate flat typographic lockup on
  glass, not another leather object. It passes the point hit-test at `24,42`.

### Leather asset and retina typography

`mobile-head-skinny.png` is not distorted from its portrait tag into a wide
rectangle. The durable asset
`site/public/assets/ui/mobile-head-skinny-leather-3x.webp` uses source crop
`xyxy 110,700,1565,1718`, preserving the side stitching, lower stitched seam
and rounded foot at the target `120:84` ratio. An explicit alpha mask removes
the photographed phone/background; CSS supplies the responsive drop shadow.
The result is a transparent `360×252` 3x WebP of about `12.5 KB`. Processing
metadata lives next to it in
`mobile-head-skinny-leather-3x.metadata.json`.

The raster contains leather only. `Полюбить Калининград` remains live HTML and
the wide-o `Анонсы` remains the canonical SVG; white ink color, a small dark
text shadow and an SVG drop shadow make them read as applied to the leather
without sacrificing retina sharpness. The open lockup is dark typographic ink
with no background, border or radius, avoiding a duplicate-tag illusion.

### Playwright and consultant acceptance

Visual QA captures closed, mid-animation and fully open states at 320px, plus
the open 390px state. It verifies actual z-order/hit targets instead of only
static geometry. All 12 generated routes return 200; each 320px panel has
`scrollHeight == clientHeight == 700`, panel horizontal overflow `0`, Share
ends at `y=649`, and the leather Close remains at `y=0`. The existing
image-first Share probe still receives title, text, URL and an `image/webp`
File.

Gemini 3.1 Pro (High) inspected the new source, original expanded reference,
closed/mid/open renders and implementation. Its design review confirmed the
lower horizontal crop and required removal of the counter-transform. Final
critical acceptance marked all five owner requirements PASS and returned
`SHIP research preview`, with no blocking visual defect. Evidence:
`artifacts/codex/mobile-menu-reference4-v10-20260722/{gemini-design-review.txt,gemini-acceptance.txt,closed-320-final.png,mid-320-final.png,open-320-final.png,open-390.png,visual-motion-qa.txt,route-qa.txt,share-probe.txt}`.

## Stable viewport drawer and aligned brand, v11, 2026-07-22

Telegram device review found three defects which desktop static geometry alone
did not expose: the closed tag could interpolate vertically while Android
Chrome changed the dynamic viewport, the first swipe over the open menu could
chain to browser/document scrolling and reveal a blank lower strip, and both
brand lockups were optically misaligned. The same research builder now emits
`preview-20260722-mobile-menu-reference4-leather-tag-lab-v11`.

The fullscreen wrapper is no longer `calc(100dvh + tag)`. It is a single fixed
`inset:0` viewport plane. Closed state uses `translateY(-100%)`, while the
summary is positioned at `top:100%`; because both percentages resolve from the
same box, the visible tag remains exactly at `y=0` without animating whenever
the mobile URL bar changes viewport height. Open state is exactly one viewport
high and the summary begins at its lower edge. The panel owns the only allowed
vertical scroll surface, uses `overscroll-behavior:none`, and switches between
`touch-action:none` when content fits and `pan-y` when content genuinely
overflows. Both `html` and `body` are scroll-locked while open. The old generic
document-scroll auto-close listener is deliberately omitted for this fullscreen
variant: a deliberate panel swipe must never be interpreted as a close signal.

The closed leather tag keeps its `120×84` WebP and live Retina typography, but
removes the donor chevron. Endorsement receives a `-1px` optical compensation
against the live font's side bearing while the canonical wordmark box remains
at `x=26`. The open flat brand uses the accepted three-line
rhythm — `Полюбить`, `Калининград`, `Анонсы` — without becoming a second
leather object. The Free icon uses the unambiguous `0 ₽` label with U+200A hair
space, a 1px circle, regular-weight 14px type and reduced optical opacity so it
matches the Phosphor Thin family instead of reading as the darkest icon.

Playwright acceptance now includes interaction, not only screenshots. At
`320×700`, scrolling the closed page from `scrollY=0` to `420` leaves summary
`y=0` (`delta=0`). Open motion captures the same summary between `y=0` and
`y=700`; fully open panel bounds are exactly `0..700`. Two CDP touch drags over
the non-scrollable panel leave document `scrollY=0`, panel `scrollTop=0` and
blank bottom `0`. After resizing to `320×620`, the panel becomes internally
scrollable and a touch drag moves only its `scrollTop` to `46`; document
`scrollY` and blank bottom remain `0`. At `390×844`, panel bottom and viewport
bottom are both `844`, and re-close returns summary to `y=0`. Evidence:
`artifacts/codex/mobile-menu-reference4-v11-20260722/{gemini-design-review.txt,visual-motion-scroll-qa.txt,public-visual-motion-scroll-qa.txt,closed-320-final.png,open-320-final.png,open-390.png}`.
Gemini 3.1 Pro's first acceptance correctly rejected bbox-only brand alignment;
after the `-1px` optical side-bearing compensation it rechecked the final
render, found no regression and returned `SHIP research preview` in
`gemini-acceptance.txt`.

## Service Share and Android clip correction, v12, 2026-07-22

Device review of v11 exposed two remaining compositor defects and one product
contract regression. The same research builder now emits
`preview-20260722-mobile-menu-reference4-leather-tag-lab-v12`.

The closed drawer stacking context is `41`, above the page/group sticky layers
(`30/40`), while the wrapper remains `pointer-events:none` and only the
summary is interactive. Thus a scrolled Popular group header can no longer cut
through the leather tag. During opening the accepted whole object still moves
without clipping. After the 320ms transform has settled, the wrapper receives
`is-open-settled` and `overflow:clip`; the class is removed before close motion.
This clips the summary whose layout position is `top:100%`, preventing Android
visual-viewport bounce from briefly exposing it at the bottom. The guard uses a
fallback timer plus a class mutation observer and must not be implemented as a
window scroll handler.

Menu Share is a **service share**, not page/event share. The v11 DOM scan,
first-event-image selection and generated canvas fallback have been removed.
The menu now exposes the same `data-service-share-root`, manifest, button,
status and canonical fallback contract as `ServiceShareAction.astro`. The
builder copies the existing `site/src/lib/service-share/controller.js` without
forking its behavior, plus the versioned `site/public/service-share/` bundle.
The current manifest supplies:

- canonical URL `https://kenigevents.ru/`;
- copy `Полюбить Калининград Анонсы — события всего региона`;
- the accepted `72,414` byte WebP service card, named by the controller
  `kenigevents-service-20260715-896b8af26ac6679f.webp`.

Playwright at `320×700` confirms the closed summary remains the hit target over
scrolled Popular content (`z-index 41 > 30`), mid-opening remains unclipped,
and settled open state uses `overflow:clip`. The first and second CDP touch
drags leave the document at the same `scrollY`, keep blank bottom `0` and never
hit the summary at the lower viewport pixel. At `320×620`, only the panel moves
to `scrollTop=46`; close removes clipping before reverse motion and restores
summary `y=0`. The native-share stub receives the exact service copy, canonical
URL and one `image/webp` File of `72,414` bytes. All 12 routes remain HTTP 200,
fit at `320×700` and have zero horizontal overflow. Evidence:
`artifacts/codex/mobile-menu-reference4-v12-20260722/{gemini-design-review.txt,qa-a.txt,qa-resize-close.txt,route-qa.txt,closed-sticky-320.png,open-mid-320.png,open-settled-320.png,open-after-two-drags-320.png}`.
Gemini 3.1 Pro independently rechecked all six gates against the final code and
renders and returned `SHIP research preview` with no blocking defect; its raw
acceptance is `gemini-acceptance.txt` in the same artifact directory.

## Closed tag stacking-context correction, v13, 2026-07-22

Real-device review of v12 showed that its sticky-header gate tested the drawer
child's computed `z-index`, but missed the parent stacking context. The closed
`.mobile-discovery-menu` had `z-index:41`, yet it remained inside
`.shell-header` at `z-index:26`; an external `.group-head` at `z-index:30`
therefore painted over and intercepted the lower 20px of the `120×84` leather
tag whenever the shelf header stuck at `top:64px`.

The variant-scoped fix raises the **closed parent shell header** to `z-index:39`.
This gives the mobile stack an explicit order: shelf headers `30`, filtered-city
context `38`, closed shell/tag `39`, bottom navigation `40`, open full-screen
menu `60`. The transparent header remains `pointer-events:none`; only the
visible summary keeps `pointer-events:auto`. No rail, sticky shelf geometry,
tag geometry or drawer motion changes.

The regression gate must scan multiple page offsets at which a shelf header is
actively sticky, not a single arbitrary offset. At each geometric intersection
it must assert both the parent/peer stack order and that
`elementFromPoint(60,70)` resolves to the leather summary. The original failure
is captured at `360×700`, `scrollY=160..720`: summary `y=0..84`, group header
`y=64..144`, hit target `.group-head`. Evidence and Gemini 3.1 Pro review:
`artifacts/codex/mobile-menu-reference4-v13-20260722/{scan-before.txt,scan-after.txt,before-overlap-360.png,after-overlap-360.png,qa-a.txt,qa-resize-close.txt,route-qa.txt,gemini-fix-review.txt,gemini-acceptance.txt}`.
The post-fix scan covers 37 active geometric intersections and resolves the
overlap point to the summary in every case. Gemini 3.1 Pro passed sticky
overlap, pointer target, z-order, open/close motion, service Share and all
routes, returning `SHIP research preview`.

## Astro integration and unified Search/Calendar preview, v14, 2026-07-22

The first large-card Search preview regressed visually even though its Search
runtime was correct: `AuthorizedEventSearch` was mounted inside the legacy
mobile portion of `EventLayout`, while the accepted reference-4 v13 shell
existed only in a Python-generated lab. Navigating from Search therefore also
opened legacy same-prefix Calendar/Popular pages. This was an integration
failure, not a reason to redesign Search or the menu.

`site/src/components/Reference4MobileMenu.astro` is now the reusable Astro
owner of the accepted v13 shell. `EventLayout` mounts it once and keeps the
existing desktop header unchanged. The port preserves the accepted contracts:

- closed leather tag `x=12, y=0, 120×84` with live endorsement and canonical
  wide-o SVG wordmark;
- one full-viewport glass plane, with the tag moving as part of the same
  parent and the leather close control at the upper-right;
- complete factual menu/date/account/favorite/service-share IA and the same
  Phosphor Thin asset set;
- panel `0..visual viewport`, no horizontal overflow, internal scroll only
  below the fitted height, settled clipping and body overscroll lock;
- bottom dock hidden/inert while the plane is open;
- stack order `sticky shelves < closed shell < dock < open shell`;
- existing service-share controller and versioned service card, not a local
  page/event-image share fork;
- legacy desktop-style `SiteFooter` hidden only on mobile nav surfaces; desktop,
  detail CTA surfaces and institutional layouts keep their existing footer.

The noindex phone-review build is composed by
`scripts/assemble_mobile_search_calendar_preview.py`. It is explicitly an
assembly step, not another renderer: Astro continues to own the functional
Search page and canonical `EventCard`; accepted v23 donor HTML continues to own
calendar rails, crop, gestures, medallions and date navigation. The assembler
transplants the exact v13 shell into donor date pages under the same build
prefix so dock/menu transitions never fall back to old templates. It retains
Astro generated labs and event pages so `check:preview` remains a valid gate.

The first R7 port was rejected because it reproduced a generic compact row
instead of the accepted physical rail. The canonical implementation is the
v23 donor embedded in
`preview-20260722-mobile-search-artifact-menu-v28`, assembled from
`integration/mobile-search-unified-v14-20260722@3f5b88f9`. The tracked Astro
port must preserve its structural and geometric contract: full-viewport
`.rail-window`, `112px` physical row, `5px` track start, `296×112px`
`.event-summary`, then real-aspect `.event-media`, digest, medallions and the
separate like action. A narrow image sliver at the right edge is the affordance
for horizontal continuation; an inset card with an image inside the summary is
not an accepted substitute.

This exact rail renders at `<=720px` on Today, Tomorrow, Weekend and Popular;
their desktop timelines/boards remain the single active desktop
representation. Today/Tomorrow/Weekend are per-date. Popular collapses only
reciprocal explicit `other_date_ids` families and never infers family identity
from title/type/venue. Search, Personal, Exhibitions, Collections, Clubs and
event-detail continuation retain their separately accepted mobile surfaces.

The open reference-4 menu also follows v28 literally: the dark live wordmark
sits directly on the one glass plane. Do not add a local rounded blur, glow or
light scrim behind it; that creates a second glass card and visibly diverges
from the accepted donor. Readability is provided only by the donor text color
and restrained one-pixel drop shadow.

Acceptance for build
`preview-20260722-mobile-search-unified-shell-v26`:

- focused mobile-shell/Search tests: `16/16`;
- Astro preview build and generated-output `check:preview`: pass (`303` events);
- mocked authorized browser smoke: `2` canonical large cards, initial skeleton,
  exact/feedback/discovery order and monotonic progress runs
  `[2,55,72,96,100]`, `[2,55,72,96,100]`, `[2,28]`,
  `[2,55,72,96,100]`;
- local Playwright geometry at `320×700` and `390×844`: exact tag/panel bounds,
  Share visible, dock hidden, horizontal overflow `0`;
- Search, Today, Tomorrow, Weekend, Popular, Personal and dated continuation
  routes return HTTP `200` within one prefix.

Gemini 3.1 Pro (High) reviewed the root-cause fix rather than proposing a new
IA and returned **GO**, provided the exact v13 geometry, full IA, Search runtime,
mobile/desktop isolation and footer policy remain invariant. Review artifact:
`artifacts/codex/mobile-search-unified-shell-v26-20260722/gemini-shell-review.md`.

### v27 Search field and amber-artifact research overlay

The v26 shell and accepted v23 Calendar/Popular donor remain unchanged. v27
corrects a narrower integration miss: the large standalone Search field had
existed in the accepted mobile-shell unification v2 lab but was not transferred
into the real `AuthorizedEventSearch`. `/poisk/` now uses the visible
`Что хочется сделать?` label, a three-row multiline field and the same
full-width in-button backend progress. Embedded Search stays compact. Runtime,
auth, result cards and skeletons are not forked.

The same assembly script also emits two isolated, noindex amber-artifact
placement prototypes. They add only a sibling button to a copied v23 rail and
do not change rail height, event link, like control, crop, gesture or medallion
rendering. Product decision and exact specimens are canonical in
[`amber-artifact-easter-egg.md`](amber-artifact-easter-egg.md).

### v28 menu rollout and prototype regression guard

The accepted reference-4 v13 leather/glass menu is the single mobile menu for
all `EventLayout` pages and for every Calendar/Popular/date donor assembled into
the mobile preview. The root splash is intentionally outside this navigation
surface; non-HTML feeds and manifests do not receive a menu.

The first artifact A/B build accidentally re-rendered its donor after the
temporary accepted-shell override had been restored, so those two pages alone
received the old tonal menu. v28 derives both artifact pages from the already
assembled `date-2026-07-24/index.html` and injects only the artifact control.
Generated-output gates require `.reference4-menu` and reject the old
`.tone-service` / `.tone-grid` plane. The accepted closed leather tag, glass
plane, complete IA, service Share, dock suppression and viewport-fit behavior
are therefore identical on Search, Calendar, Popular and both artifact links.

## Integrated mobile acceptance R9, 2026-07-23

The integrated noindex prototype keeps v23/v28 as the rail and navigation
donor rather than replacing it with compact cards. The following corrections
are part of the shared Astro implementation:

- full mobile event detail uses the same
  `mobile-head-skinny-leather-3x.webp` Reference4 tag as the other mobile
  surfaces. Its live white lockup paints over an immediate `#98401f` fallback;
  the open glass plane keeps the dark wordmark without a second pale scrim;
- rail media reserves its final `112px` physical slot while loading. A bounded
  skeleton remains until the image has loaded and decoded, then either reveals
  the image or changes to a stable error surface without moving the row;
- every single explicitly classified, crop-safe `visual_only` source uses the
  donor's `140×112` landscape `5:4` cover window regardless of the source
  orientation. A separately source-reviewed no-text portrait selected from a
  mixed OCR/photo inventory may use a `90×112` vertical `4:5` cover window only
  when at least 80% of its area remains. Overriding an event-level OCR marker
  additionally requires the explicit per-asset `listing_no_ocr_review=true`
  provenance bit; generic crop review is not sufficient. `ocr_text`, `unknown`, unreviewed and
  protected-document media keep their fail-closed authored geometry. The
  selected asset's own classification controls its rail class; an OCR primary
  poster must not poison an explicitly reviewed alternate photo;
- date pages use the full `56px` horizontal date accessory above the bottom
  dock, including the current-date chip, generated weekend ranges and calendar
  sheet. All 42 cells are real links: today/tomorrow keep their named routes,
  Saturdays use the generated weekend route and the remaining dates use
  generated `/date-YYYY-MM-DD/` pages, including honest empty states;
- after the page hero leaves the viewport, a compact title is exposed in the
  existing `64px` header lane. Popular/group and Weekend day shelf headings
  remain native `position:sticky` at `top:64px`; their ancestors must not
  introduce clipping or overflow that disables sticking;
- the continuation cue is one exact `48×23` inline SVG path with a horizontal
  shaft and symmetric head. CSS border/rotation arrows are rejected because
  fractional rendering produced the crooked Android result;
- rail social proof and the terminal action both reuse the shared
  `Icon.astro` heart: hollow by default and filled only for the liked state.
  The accepted edge mechanics are executable, not decorative: a horizontal
  pull right from the physical start reveals the red negative layer and opens
  explicit confirmation at `>=86%` plus `>=140px`; a `>=120px` left overpull
  from the physical end applies Like. Mouse/pointer and touch paths share the
  same state machine, cancel settles the track, reduced motion disables edge
  actions, and both outcomes expose a 4.5-second Undo. Trusted post-drag clicks
  are intercepted in capture phase so the underlying event link cannot fire;
  canonical feedback state completion is observed through `aria-pressed`
  mutations rather than guessed with a fixed delay;
- standalone Search retains the accepted full-width in-button progress fill.
  It owns one request epoch, blocks synchronous double submits and resets on
  success, error, abort, logout and page exit. A pending pre-request session
  check is epoch-invalidated too, so its late continuation cannot restart the
  CTA after logout. This is acceptance of the visual and lifecycle contract,
  not a claim that the real authenticated Search journey has been
  owner-accepted.

The amber artifact remains an explicitly enabled **noindex research layer**.
The accepted A-tail button is a sibling after the large like, never part of the
event link or medallion set. It uses local browser state only and is excluded
from production output; its exact placement and motion contract remain
canonical in
[`amber-artifact-easter-egg.md`](amber-artifact-easter-egg.md).

## R11 rail consent and temporal truth, 2026-07-24

The red `Не интересно` edge is deliberately taught once per device:

- the first completed negative swipe opens an `alertdialog`;
- its copy says that the current event will be hidden and that later swipe
  marks will be applied without confirmation, with Undo still available;
- consent is persisted only after the canonical feedback control confirms
  `aria-pressed="true"`; Cancel and storage failures keep the fail-closed
  confirmation path;
- subsequent negative swipes call the same canonical feedback action directly
  and keep the 4.5-second Undo. Consent applies only to the swipe shortcut.

On `Сегодня`, the browser recalculates temporal state against the Kaliningrad
clock on load and once per minute. A row is visually past after an explicit
`end_at`, or `started-earlier` after a start with no trustworthy duration has
been behind the current time by at least one hour. Only the main event image is
desaturated; identity/free medallions, text, controls and row order remain
unchanged.

An immutable noindex preview may remain open after its projected
`data-mobile-listing-date` has elapsed. In that case a no-`end_at` row must not
revert to vivid merely because the listing date no longer equals today's
Kaliningrad date: the elapsed listing day is `past` and its main image stays
muted. A trustworthy explicit end remains authoritative, including an
explicitly future multi-day end. Regression coverage uses real 2026-07-26 rows
`7018`, `6956` and `7043`, a controlled browser clock, a post-midnight pass and
a desktop-isolation check.

Compact rail schedule tiles must never ellipsize the essential end date. A
same-month range is written as `8–9 августа`; cross-month ranges retain both
month names. The full schedule remains in the accessible label.
