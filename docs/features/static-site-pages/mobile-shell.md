# Unified mobile discovery shell

Канонический контракт общей мобильной оболочки статической Афиши. Он устраняет
расхождение между календарным прототипом, Search и обычными Astro-страницами, не
переписывая содержимое самих страниц.

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
