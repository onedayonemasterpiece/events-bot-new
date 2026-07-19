# F18 desktop clipboard research: Windows and macOS

> Статус: **test harness contract; native matrix Pending**. До реальных Windows/
> macOS проверок безопасный desktop default — D0 «Скопировать ссылку». D1/D2
> доступны только на `/lab/service-share/` или через явный preview config.

## Место в F18

Desktop clipboard — transport той же service-sharing action, а не отдельная
фича. Текущий v11-based preview размещает action только в footer; navigation/menu
placement Deferred до V12 и поэтому полный two-placement F18 gate не закрыт.

Контракт UI:

- footer block даёт контекст сервиса;
- mobile label: **«Поделиться»**;
- desktop D0 label: **«Скопировать ссылку»**;
- D1/D2 label: **«Скопировать карточку»**;
- accessible name: **«Поделиться KenigEvents»**.

## Почему нужен native gate

Один `ClipboardItem` может содержать `image/png`, `text/html` и `text/plain`, но
target app сама выбирает representation и может санитизировать HTML. Browser API
не гарантирует единое сообщение «картинка + подпись + ссылка» и не сообщает о
фактической вставке или отправке.

## Режимы

### D0 — безопасный default

- `navigator.clipboard.writeText()`;
- payload: короткий текст + `https://kenigevents.ru/`;
- success: **«Скопированы текст и ссылка»**;
- denied/unsupported: обычная selectable canonical link.

### D1 — HTML-first

Ровно один `ClipboardItem`, порядок:

1. `text/html`;
2. `image/png`;
3. `text/plain`.

### D2 — PNG-first

Ровно один `ClipboardItem`, порядок:

1. `image/png`;
2. `text/html`;
3. `text/plain`.

Для D1/D2 success всегда:
**«Карточка скопирована. При вставке приложение выберет поддерживаемый формат»**.

## Payload и security

`image/png` имеет PNG signature, `image/png` MIME, immutable HTTPS URL, тот же
`visual_payload_hash`, что WebP, и видимые CTA/домен внутри изображения.

`text/plain` содержит allowlisted product copy и canonical URL; не содержит
event URL, preview URL, personal-secret URL, query или user data.

`text/html` — минимальный escaped fragment (`article`, image, heading/paragraph,
обычный `a`). Запрещены script, event handler, tracking pixel, hidden content,
user/session fields и несанитизированная вставка.

Обязательные preconditions/fallbacks:

- secure top-level focused document и непосредственная activation;
- проверка `navigator.clipboard.write`, `ClipboardItem`, optional `supports()`;
- Safari-compatible Promise representation внутри `ClipboardItem`, без долгого
  `await` до `clipboard.write()`;
- CORS-readable `200 image/png`, не opaque `no-cors`;
- `NotAllowedError`, `DataError`, API/type absence, unfocused page, asset/CORS/
  timeout failure → D0 → selectable link, если D0 тоже запрещён.

## `/lab/service-share/`

Закрытая от основной навигации `noindex`-страница содержит:

- preview одного current WebP;
- D0/D1/D2 controls;
- secure context, focus, Share/canShare, clipboard.write, ClipboardItem и optional
  supports capability panel;
- textarea, contenteditable и image-aware paste target;
- redacted result/reason, elapsed time, payload/asset version;
- versioned JSON export bounded ledger и кнопку очистки только этого ledger.

Страница не вызывает `navigator.clipboard.read*()`. Paste target может обработать
явный user paste для текущего DOM-сеанса, но содержимое не попадает в ledger,
storage, analytics или export. Full UA не сохраняется.

## Playwright boundary

Mocks должны доказать controller/payload/fallback contract: один ClipboardItem,
порядок MIME, точные manifest bytes/hashes, PNG signature, HTML escaping, D0
fallback, keyboard и `aria-live`. Linux Chromium/WebKit не является доказательством
native Windows, macOS или Safari paste behavior.

## Native evidence

Канонический шаблон: [manual matrix](service-sharing-desktop-clipboard-manual-matrix.md).
Для каждой ячейки нужны exact OS/browser/target versions, D0/D1/D2, результат,
ссылка/изображение/CTA, повторяемость `2/2` и redacted screenshot.

Primary result taxonomy:

- `rich_image_text_link`, `image_only`, `html_text_link_no_image`, `plain_text_link`;
- `empty_paste`, `write_blocked_permission`, `unsupported_type_or_api`;
- `asset_or_cors_failure`, `target_sanitized_or_rejected`, `not_tested_blocked`.

## Product decision gate

D1/D2 нельзя сделать production default, пока:

- не заполнены обязательные Windows/macOS controlled и real-app cells;
- нет `empty_paste` после объявленного success в supported browser;
- измерены cold PNG fetch/write size/latency;
- owner не выбрал D0, D1 или D2 и не подписал label/success/fallback/asset SHA.

Текущий статус всех native Android/iOS/Windows/macOS проверок — **Pending**.
