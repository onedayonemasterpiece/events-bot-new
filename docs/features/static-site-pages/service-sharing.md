# F18: поделиться сервисом KenigEvents

> Статус: **accepted service footer v1**. После отдельной проверки noindex
> specimen он принят владельцем продукта и подключён как общий footer событий,
> списочных, поисковых и lab-страниц. Маршрут
> `/lab/event-desktop/examples/footer-service-v1/` сохранён как regression
> specimen, а не как отдельная ветка интерфейса.

## Назначение и граница

F18 помогает рассказать о самом сервисе «Полюбить Калининград Анонсы», а не о
конкретном событии. Канонический payload всегда содержит:

- одну централизованно подготовленную карточку сервиса;
- короткий продуктовый текст;
- ровно один canonical URL: `https://kenigevents.ru/`.

F18 не использует URL текущего события, preview URL, personal-secret URL, query,
email, профиль или session data. Карточка сервиса не становится event media и не
попадает в gallery, hero, `Event.image[]` или event-share payload. Шеринг события
остаётся отдельным контрактом в [event-sharing.md](event-sharing.md).

## Текущий продуктовый copy contract

В footer action отделён от списка навигации и социальных ссылок коротким
контекстом **«Понравились Анонсы? Поделитесь»**. Слово `Анонсы` рендерится
каноническим inline SVG-компонентом с расширенной `о`, а единый accessible label
остаётся обычным текстом. Это не второй H1 страницы и не рекламный
баннер: подложка, тень и маркетинговый вопрос удалены. Контролы остаются
компактными:

| Состояние | Видимый label |
|---|---|
| mobile `<768px` | **«Поделиться»** |
| desktop | **«Скопировать карточку»** и **«Скопировать текст и ссылку»** |

Accessible names различают намерения: **«Понравились Анонсы? Поделитесь»** для
группы и **«Поделиться сервисом KenigEvents»** для share-контрола,
**«Скопировать карточку KenigEvents»** и **«Скопировать текст и ссылку
KenigEvents»**. Минимальная зона нажатия — `44×44 px`; обязательны native button
semantics, keyboard activation, `focus-visible` и отдельный визуально скрытый
`aria-live` status, который не меняет высоту footer.

Не используются как основной label:

- «Поделиться сайтом» — описывает технический объект, а не продукт;
- «Поделиться сайтом с другом» — длиннее и необоснованно исключает группу/канал;
- «Скопировать ссылку» на mobile — не соответствует system-share transport;
- один desktop-control с несколькими clipboard representations — target app
  может выбрать не то представление и тем самым смешать два намерения.

Один allowlisted textual payload формируется из manifest, а не из текущей страницы:

```text
title: Полюбить Калининград Анонсы
text: События Калининграда и области. Найдите своё событие быстрее.
proof: {events_floor}+ актуальных событий       # только при валидной метрике
url: https://kenigevents.ru/
```

`proof` можно опустить целиком, но нельзя подставлять stale/unverified число.
Desktop text intent соединяет разрешённые строки и canonical URL; mobile
передаёт `text` и `url` отдельными Web Share fields, не дублируя
event/preview URL.

Точный success copy:

- text intent: **«Текст и ссылка скопированы»**;
- image intent: **«Карточка скопирована в буфер»**.

После Web Share нельзя писать «отправлено» или «поделились»: API не доказывает
доставку. `AbortError` означает отмену пользователем, а не поломку.

## Компонент и placements

Один `ServiceShareAction` и один browser controller обслуживают один manifest и
payload. На mobile видна одна native-share action. На desktop одновременно
видны две одинаково вторичные action с разными намерениями: image-only PNG и
plain text + canonical URL. Они намеренно не объединяются в один ambiguous
`ClipboardItem`.

Текущий preview scope:

1. **Done target:** общий footer на preview index, listing, search, event detail и
   закрытых lab-страницах;
2. **Deferred до V12:** action внутри раскрытого mobile menu под бренд-биркой;
3. **Deferred до V12:** отдельное место в desktop navigation shell, если оно
   останется нужно после product review footer.

Полный F18 по-прежнему требует две placements одного компонента: navigation shell
и footer. Footer-only preview — намеренно неполный срез, а не изменение этого
целевого инварианта.

### Accepted service footer v1

`SiteFooter.astro` является единым placement дизайн-системы и задаёт следующий
информационный порядок на всех статических поверхностях:

1. сервисный share вынесен в одну цельную компактную inline-поверхность:
   `84px` на desktop и около `116px` на mobile вместо крупного split-баннера;
   `ServiceShareAction` получил только нейтральный API `showPrompt=false`, чтобы
   не повторять вопрос внутри этой поверхности. Desktop сохраняет два intent,
   mobile — один системный share, все кнопки не ниже `44px`;
2. навигация разделена на `Афиша`, `О проекте` и `Документы`; ссылка на
   информационное партнёрство существует ровно в одном месте, исторический дубль
   `Партнёры` не переносится;
3. в `Документы` заранее зарезервированы пользовательское соглашение и политика
   обработки персональных данных. Пока тексты не прошли юридическую проверку,
   элементы имеют `role=link`, `aria-disabled=true` и не ведут на 404;
4. ссылка `Правообладателям` и контакт остаются отдельными от партнёрства;
5. mobile текстовые действия получают не менее `44px` по высоте, share/social
   контролы сохраняют `44px+`, а muted copy на графитовом фоне использует
   контрастный токен `#bfb1a6`;
6. MAX на графитовом фоне использует немодифицированный цветной знак из
   [официального брендбука](https://go.max.ru/brandbook), а не наследующий цвет
   монохромный glyph. Локальный asset сопровождается provenance metadata.

Secret-candidate упаковщик сохраняет ровно два явных lab specimen: ранее
согласованную транспортную A/B/C страницу и footer regression specimen.
Остальные lab-маршруты по-прежнему исключаются. Сам footer при этом одинаков на
всех статических маршрутах и не имеет legacy/prototype-переключателя. Footer
specimen использует замороженный desktop fixture `5658`, а не строку только из
текущего active-catalog, поэтому истечение даты события не может сорвать
production-candidate build до проверки самого подвала.

Перед production нужны реальные тексты и правовая проверка состава документов.
Политика обработки персональных данных должна быть доступна со страниц, где
сайт собирает персональные данные; отдельные согласия привязываются к конкретным
формам/целям обработки, а не добавляются в footer как абстрактная ссылка.

## Mobile transport

На mobile breakpoint controller:

1. заранее, после critical content, начинает готовить versioned WebP asset;
2. в момент клика проверяет `navigator.share`;
3. если WebP уже готов, создаёт `File` и проверяет `navigator.canShare({files})`;
4. вызывает `navigator.share()` с WebP file, коротким text и canonical URL;
5. если файл ещё не готов или file share не поддержан, немедленно использует
   system share с text+URL, не теряя transient user activation;
6. если Web Share отсутствует или завершается рабочей ошибкой, пробует clipboard
   text+URL;
7. если clipboard запрещён, показывает обычную selectable HTTPS-ссылку.

Долгий `await fetch()` до `navigator.share()` запрещён. `AbortError` записывается
как `share_cancelled` без аварийного fallback.

Планировочный контракт ссылался на проверенный Pharmastaff organization-card
share flow, но точные repository/branch/SHA в доступной истории не были найдены.
До заявления о parity implementation PR обязан приложить точную ссылку/SHA и
сопоставить preloaded-file, transient activation, `canShare`, cancel/error и
fallback checks. Отсутствие ссылки не разрешает ослабить эти проверки.

## Desktop transport

На desktop `navigator.share()` не вызывается никогда.

- **«Скопировать карточку»** создаёт ровно один `ClipboardItem` и ровно одно
  representation `image/png`; ошибка не подменяет намерение копированием текста;
- **«Скопировать текст и ссылку»** вызывает
  `navigator.clipboard.writeText(short_text + canonical_url)`; при ошибке
  остаётся selectable canonical link.

PNG является настоящим проверенным PNG. Promise representation создаётся внутри
`ClipboardItem` до произвольного `await`, совместимо с Safari. Исторический
`PUBLIC_SERVICE_SHARE_DESKTOP_MODE=d0|d1|d2` пока принимается только как
совместимый marker; UI и поведение больше не ветвятся по нему.

Подробный gate: [desktop clipboard research](service-sharing-desktop-clipboard-research.md).

## Ежедневная динамическая карточка с кубами

### Источник данных и claims

Гарантируется минимум один durable coalesced запрос на календарный день
`Europe/Kaliningrad`, когда builder включён; accepted финальный результат
появляется только после обычных build/check gates. Обычный Smart Update может
легитимно дать дополнительный accepted build в тот же день, если изменились
публичные входы; отдельный daily scheduler для этого не создаётся.
Вход — тот же accepted catalog snapshot, из которого собирается static site.
Eligible set содержит distinct canonical active current/future events;
cancelled, postponed, inactive, `silent`, review-only, duplicates и отдельные
occurrence-дубли исключаются.

Manifest фиксирует формулу, `measured_at`, local date, catalog hash и:

- `eligible_event_count`;
- консервативный `events_floor`, никогда не превышающий eligible count;
- distinct normalized `city_count` и список городов evidence-only;
- claim/copy/template versions.

Фразы «самая большая база», «крупнейшая», «быстрее других» и неподтверждённое
напоминание запрещены. При невалидной метрике pipeline использует evergreen copy
без числа либо fail-closed result, но не вчерашнее заведомо ложное число.

### События на кубах

Целевая воспроизводимая смесь — `3 popular + 2 promoted + 3 stable-random`:

- popular ранжируются текущим составным сигналом сайта;
- promoted поступают только из активного read-only promo resolver для surface
  `service_share_card`;
- stable-random выбираются дневным hash seed и меняются между днями.

Каждая строка проходит current/future, image/media-role и source-safety preflight.
Недостающие promo rows не заменяются скрыто случайными: underfill/reason отражается
в result, а финальный render принимается только по явно разрешённой политике.
Название и дата будущего события наносятся на его грань после crop/contain.

### Композиция и Kaggle

Сохраняется утверждённый cube concept: бесшовная циклорама, fixed brand/product
иерархия, связная цепь кубов и обязательный hero cube, выходящий за right+bottom
границы. Дневной deterministic seed выбирает одну из проверенных families:
`soft_s_curve`, `diagonal_ribbon`, `ascending_arc`, с ограниченным jitter.

Production-like render выполняется так:

1. Kaggle GPU debug gate (`512`, low samples) проверяет scene/typography/geometry;
2. только accepted GPU result разрешает Kaggle CPU final (`1024×1024`, high samples);
3. GPU и CPU обязаны совпадать по bundle SHA, local date, catalog hash, selection
   hash, family request/resolved family и seed;
4. kernel публикует внутренние heartbeat/progress/status events и terminal report;
5. scheduler не принимает opaque `FAILED/ERROR` без kernel log/status evidence.

R15 routes the accepted final render through the existing coalesced
StaticSiteBuilder snapshot/handoff rather than a browser or a second
unmonitored scheduler. The sole daily trigger is the existing
`static_site_calendar_rollover` cron at `00:00 Europe/Kaliningrad`, with
`startup_catchup` using the same enqueue path after process restart. Both feed
the normal StaticSiteBuilder outbox/single-flight flow; Smart Update requests
remain allowed to satisfy the same local day without creating another job.

Every enabled request carries
`service-share-daily:<timezone>:<local-date>` in the durable request watermark
and build fingerprint. The outbox transaction uses SQLite `BEGIN IMMEDIATE` to
check/update this local-day evidence atomically: a same-day daily trigger
returns `daily-already-requested`, while the next local date replaces the
marker and requeues the one coalesced row without using operator
`force_rebuild`. A disabled builder writes no daily row.

The versioned PNG/WebP and current manifest advance atomically only after their
dimensions, MIME and hashes validate. A failed render retains the previous
verified asset, records stale age and cannot publish a partial replacement.

Projection gates запрещают safe-zone intrusion, разрыв цепочки, слишком маленькие
дальние кубы и hero без требуемого screen exit. Финальный master конвертируется
централизованно, не в browser click.

### Asset/manifest contract

Один visual payload даёт:

- лёгкий WebP для mobile Web Share;
- настоящий PNG для desktop Clipboard;
- versioned JSON manifest.

Manifest хранит schema/copy/template/asset versions, local date, catalog/selection/
visual payload hashes, immutable URLs, MIME, dimensions, byte size и SHA-256 для
обоих assets, metrics, event IDs/buckets, composition family/seed и Kaggle run/status
evidence. WebP и PNG обязаны иметь один `visual_payload_hash`.

URL content-addressed, CORS-readable и immutable. Mobile WebP target budget —
`<=350 KiB`; фактические PNG size, cold fetch и clipboard-write latency измеряются
до desktop decision. Logo, CTA, домен, metric и event dates проходят visual/OCR
QA на master и thumbnail width `360px`. PNG — только clipboard representation,
не page/LCP image. Browser не создаёт карточку canvas-ом и не обращается к
per-click rendering backend.

## Preview analytics boundary

До принятия production ingest разрешён только bounded in-memory test ledger или
`CustomEvent`:

- `service_share_opened`, `service_share_invoked`;
- `service_link_copied`, `service_copy_attempted`, `service_copy_result`;
- `share_file_unsupported`, `share_cancelled`, `share_error`.

Поля: `surface=footer|lab`, `transport=system_share|clipboard`,
`mode=mobile_file|mobile_text|d0|d1|d2`, coarse platform, capability flags,
allowlisted reason, asset version и latency band. Запрещены clipboard contents,
full UA, profile/email/session, event/personal URL, `paste_completed` и
`message_sent`.

## Acceptance текущего preview slice

- [x] footer использует один общий component/controller на preview index, listing,
  search, event detail и lab;
- [x] mobile и desktop используют один canonical URL, manifest и copy source;
- [x] mobile file/text/clipboard/link fallbacks и `AbortError` проверены Playwright;
- [x] desktop image-only и text+URL intents, security и fallback contract проверены;
- [x] `/lab/service-share/` существует, закрыт от навигации и `noindex`;
- [x] daily selector/metrics/composition и GPU→CPU contract дают validated manifest;
- [x] local build/check/Playwright и public HTTPS preview проверки зелёные;
- [x] preview publish не меняет stable ICS, production/current pointer или assets;
- [x] implementation SHA, preview URL, manifest SHA и screenshots записаны;
- [x] no-JS/clipboard-denied state оставляет обычную selectable canonical link;
- [ ] native matrix честно остаётся Pending, если реальные устройства не проверены.

## Что остаётся до полного F18

- [ ] V12: разместить тот же component под mobile tag/navigation shell;
- [ ] приложить точный Pharmastaff repository/branch/SHA либо явно оставить parity gate Pending;
- [ ] проверить обе placements на всех public HTML families;
- [ ] выполнить реальные Android/iOS file-share проверки в Telegram/VK/MAX;
- [ ] выполнить Windows/macOS matrix для image-only и text+URL intents;
- [ ] подписать production copy/asset SHA и реальные target-app результаты;
- [ ] доставить код в `origin/main`, включить schedule/publisher и доказать rollback.

Пока эти пункты не закрыты, формулировка результата только **«footer-only test
implementation ready»**, не «F18 готова» и не «готово к production».
