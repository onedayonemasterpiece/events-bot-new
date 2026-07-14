# F18 desktop clipboard research: Windows and macOS

> Статус: **обязательная аналитика и cross-platform test plan; product behavior «Скопировать карточку» ещё не принято**. До завершения матрицы безопасный desktop baseline F18 остаётся «Скопировать ссылку».

## Место в единой F18

Это не отдельная desktop-фича и не третья точка входа. F18 реализуется как **один адаптивный service-sharing component** в двух общих placements — под раскрытой бренд-биркой/в навигационном shell и в footer:

- на mobile та же action вызывает системный Web Share с централизованно подготовленной карточкой, текстом и canonical URL, затем использует mobile fallbacks;
- на desktop та же action в тех же placements не вызывает native share, а записывает принятый D0/D1/D2 payload в clipboard;
- смысл действия, service URL, версия карточки, copy/claim gates, доступное имя и семейство аналитики общие; меняется только platform transport;
- mobile и desktop не получают расходящиеся компоненты, manifests или независимо редактируемый текст, и в одном breakpoint не показываются две конкурирующие кнопки «поделиться»/«скопировать».

Следовательно, эта страница является дочерним test gate основного [контракта F18](service-sharing.md), а не самостоятельным feature contract. Product acceptance проверяет комплекс целиком: mobile system share и desktop clipboard behavior одного RC SHA.

## Зачем нужен отдельный gate

На desktop нет требования вызывать native share. Возможное улучшение — по одному явному клику записать в системный clipboard одну карточку с несколькими представлениями:

- настоящий `image/png`;
- безопасный `text/html` с изображением, коротким текстом и кликабельной ссылкой;
- `text/plain` с тем же тезисом и URL.

Это **не означает**, что одно нажатие `Ctrl+V`/`⌘V` гарантированно создаст в Telegram, VK, MAX, Word, Notes или другом приложении единое сообщение «изображение + подпись + кликабельная ссылка». Clipboard хранит представления, а принимающее приложение выбирает поддерживаемый формат и может санитизировать HTML. Поэтому desktop behavior фиксируется только после фактических тестов на Windows и macOS.

## Проверенные web-platform факты

- [`navigator.clipboard.write()`](https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/write) работает только в secure context и принимает `ClipboardItem`; MDN помечает API как Baseline 2024, но старые browser/OS versions могут отличаться.
- [W3C Clipboard API](https://www.w3.org/TR/clipboard-apis/) описывает один clipboard item как набор MIME representations и относит `text/plain`, `text/html` и `image/png` к обязательным web clipboard types. Платформы могут не сохранять несколько самостоятельных clipboard items, поэтому F18 использует **один** `ClipboardItem`, а не три items.
- [WebKit](https://webkit.org/blog/10855/async-clipboard-api/) требует secure context и user gesture, поддерживает `text/plain`, `text/html`, `text/uri-list`, `image/png`, санитизирует HTML/PNG и сохраняет порядок type fidelities при чтении. Это не доказывает, что каждое macOS target app выберет первый type.
- Для Safari асинхронную загрузку PNG нужно передавать как Promise representation внутри `ClipboardItem`, не выполнять произвольный `await` до clipboard call; этот совместимый pattern отдельно описан в [web.dev Async Clipboard guidance](https://web.dev/articles/async-clipboard).
- Chromium может требовать `clipboard-write` Permissions Policy внутри iframe; production F18 работает top-level, но embedded preview должен тестироваться отдельно и не использоваться как единственное evidence.
- `ClipboardItem.supports()` полезен, когда доступен, но сам имеет более новый compatibility floor. Его отсутствие не равно отсутствию mandatory PNG/HTML/text support: implementation сначала проверяет constructor/`write`, затем при наличии `supports()` использует его как дополнительный probe.

Любое утверждение о результате вставки в конкретное desktop-приложение остаётся гипотезой до записи в матрице ниже.

## Кандидаты для исследования

### D0 — текущий безопасный baseline

- label: **«Скопировать ссылку»**;
- `navigator.clipboard.writeText()` копирует короткий текст и `https://kenigevents.ru/`;
- при ошибке показывается выделяемая обычная ссылка;
- это обязательный fallback для всех остальных кандидатов.

### D1 — rich clipboard, HTML-first

Один `ClipboardItem` с representations в порядке:

1. `text/html`;
2. `image/png`;
3. `text/plain`.

Гипотеза: rich-text targets чаще сохранят композицию с текстом и ссылкой. Риск: messenger может выбрать HTML/text и не вставить PNG как attachment.

### D2 — rich clipboard, PNG-first

Один `ClipboardItem` с representations в порядке:

1. `image/png`;
2. `text/html`;
3. `text/plain`.

Гипотеза: image-capable messenger чаще выберет карточку. Риск: подпись/кликабельная ссылка будут проигнорированы.

Порядок representations является тестовой переменной, а не универсально доказанной настройкой предпочтения. Нельзя выпускать browser-specific order без evidence и documented fallback.

## Payload contract для D1/D2

### `image/png`

- байты являются настоящим PNG (`Content-Type: image/png`, корректная PNG signature), а не переименованным WebP/JPEG;
- визуал и claims совпадают с current service-share WebP и тем же `visual_payload_hash`;
- домен и CTA уже видны внутри картинки, потому что target может вставить только PNG;
- PNG централизованно prerendered из того же SVG/template payload, а не генерируется при desktop click;
- PNG — узкое clipboard compatibility representation, не обычный page image и не исключение для LCP/CDN WebP-first policy;
- исследование измеряет byte size и time-to-write; предварительный ceiling утверждается только после Windows/macOS тестов.

### `text/plain`

Не более четырёх коротких строк:

```text
KenigEvents — события Калининградской области
Найдите своё событие быстрее
{events_floor}+ актуальных событий · {city_count} городов
https://kenigevents.ru/
```

Текст использует те же claim gates, что и изображение. Он не содержит текущий event URL, personal-secret URL, email, profile, query или session data.

### `text/html`

- минимальный semantic fragment: `<article>`, HTTPS `<img>`, heading/paragraph и обычный `<a>`;
- только escaped allowlisted copy и canonical URL;
- никакого script, inline event handler, tracking pixel, hidden content, user data или dependence on unsanitized HTML;
- HTML sanitization/удаление картинки/стилей является нормальным возможным target outcome, а не поводом ослаблять security;
- remote PNG URL должен быть immutable и доступен target app; отдельно проверить, встраивает ли target картинку или сохраняет только remote reference.

## Security, activation and fallback

- только HTTPS/top-level focused document;
- `navigator.clipboard.write()` вызывается из непосредственного click/keyboard activation;
- Safari-compatible PNG Promise создаётся внутри `ClipboardItem`, без предварительного `await`, способного потерять activation;
- CDN/same-origin fetch обязан вернуть `200 image/png`, CORS-readable body и корректные bytes; opaque `no-cors` response запрещён;
- использовать ровно один `ClipboardItem` с несколькими representations;
- `NotAllowedError`, `DataError`, unsupported constructor/type, unfocused document, asset/CORS/timeout failure и iframe policy дают честный D0 fallback;
- success copy не называется «отправлено» или «вставлено»: допустимый текст — **«Карточка скопирована. При вставке приложение выберет поддерживаемый формат»**;
- при D0 — **«Скопированы текст и ссылка»**;
- рядом/в success state должен оставаться явный **«Скопировать текст и ссылку»** fallback, если D1/D2 будет принят.

## Test harness до product decision

Нужна отдельная research branch и top-level HTTPS test page, не подключённая к production navigation. Она содержит:

- D0/D1/D2 buttons;
- один и тот же immutable test PNG/text/HTML payload;
- capability panel: secure context, focus, `navigator.clipboard.write`, `ClipboardItem`, optional `ClipboardItem.supports`, browser UA/version recorded only in the test evidence;
- redacted result/error and timing;
- controlled paste targets: plain `<textarea>`, rich `contenteditable`, image-aware paste listener;
- export одного versioned JSON ledger без содержимого чужого clipboard.

Playwright проверяет собственный payload и fallbacks, но **не заменяет native OS/application tests**:

- click/keyboard activation вызывает один `clipboard.write([oneItem])`;
- item содержит ожидаемые three MIME representations и точные bytes/copy/URL;
- HTML escaping/security negative cases;
- D1/D2 ordering;
- asset error, CORS, timeout, API absence and `NotAllowedError` → D0;
- обе F18 desktop placements используют один controller;
- success/error объявляются через `aria-live` и не требуют мыши.

Playwright WebKit на Linux нельзя выдавать за проверку реального Safari/macOS clipboard; mocked API нельзя выдавать за доказательство вставки в Telegram Desktop.

## Обязательная Windows matrix

Минимальная система: поддерживаемая Windows 11, все exact versions записаны в evidence.

Source browsers:

- Microsoft Edge stable;
- Google Chrome stable;
- Mozilla Firefox stable.

Paste targets:

1. controlled `<textarea>`;
2. controlled `contenteditable` и image paste target;
3. Windows Notepad;
4. Telegram Desktop;
5. VK web composer в каждом primary Chromium browser;
6. MAX web/desktop, если фактически доступен;
7. один установленный rich native editor (Word/Outlook либо documented substitute).

Основной cross-app прогон выполняется для Edge и Chrome; Firefox обязан пройти controlled targets, Notepad, Telegram и минимум один web composer. Для каждого target проверить D0/D1/D2 через `Ctrl+V` и записать фактический результат.

## Обязательная macOS matrix

Минимальная система: поддерживаемая macOS, все exact versions записаны в evidence.

Source browsers:

- Safari stable;
- Google Chrome stable;
- Mozilla Firefox stable.

Paste targets:

1. controlled `<textarea>`;
2. controlled `contenteditable` и image paste target;
3. TextEdit в plain-text и rich-text mode;
4. Apple Notes;
5. Telegram Desktop;
6. VK web composer в Safari и Chrome;
7. MAX web/desktop, если фактически доступен;
8. Pages/Mail либо documented installed rich editor.

Основной cross-app прогон выполняется для Safari и Chrome; Firefox проходит controlled targets, TextEdit, Telegram и минимум один web composer. Для каждого target проверить D0/D1/D2 через `⌘V`.

## Result taxonomy и evidence

Каждый matrix cell получает ровно один primary result:

- `rich_image_text_link`;
- `image_only`;
- `html_text_link_no_image`;
- `plain_text_link`;
- `empty_paste`;
- `write_blocked_permission`;
- `unsupported_type_or_api`;
- `asset_or_cors_failure`;
- `target_sanitized_or_rejected`;
- `not_tested_blocked`.

Дополнительно записываются:

- OS/browser/target app exact versions;
- source mode D0/D1/D2 and type order;
- API resolution/error and elapsed time;
- screenshot/screen recording after paste, без личных чатов/данных;
- виден ли домен/CTA;
- кликабельна ли ссылка;
- является ли изображение attachment, inline bitmap или remote HTML image;
- повторяемость минимум `2/2` на чистом test conversation/document.

Artifacts не коммитятся. В репозиторий попадает только redacted summary matrix и product recommendation.

## Research acceptance и product decision

Аналитика считается завершённой, когда:

- [ ] выполнены все controlled browser cells Windows/macOS;
- [ ] выполнены обязательные native/messenger/web-composer cells либо каждый blocker доказан;
- [ ] ни один supported browser не получает `empty_paste` после объявленного success;
- [ ] D0 всегда даёт текст+canonical URL либо честный selectable-link fallback;
- [ ] PNG-only результат сохраняет домен/CTA внутри изображения;
- [ ] нет утверждения, что API знает о фактической вставке/отправке;
- [ ] измерены PNG size/fetch/write latency и отказ при cold cache;
- [ ] D1 и D2 сравнены одной и той же матрицей;
- [ ] owner выбирает один вариант: `D0 copy-link`, `D1 rich HTML-first`, `D2 PNG-first` либо две явные desktop actions;
- [ ] выбранные label, success copy, fallback и supported-browser policy перенесены в основной F18 contract и frozen UI.

До этого gate нельзя менять релизное обещание с «Скопировать ссылку» на «Скопировать карточку» и нельзя считать attachment-анализ из одного браузера доказательством Windows/macOS compatibility.

## Production analytics boundary после возможного запуска

Допустимы только compact события:

- `service_copy_attempted`: `mode=clipboard_card|text_link`, `surface`, coarse `platform=windows|macos|other`, browser family, capability flags;
- `service_copy_result`: `api_resolved|fallback_text|denied|unsupported|asset_error|timeout`;
- агрегированная latency band и asset version.

Запрещено:

- читать clipboard обратно для аналитики;
- сохранять clipboard contents;
- утверждать или измерять `paste_completed`/`message_sent`, которых web API не сообщает;
- отправлять full UA, target application guess, personal URL или message content в production telemetry.
