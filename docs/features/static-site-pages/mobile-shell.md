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
