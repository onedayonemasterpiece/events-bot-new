# Сценарии для отладки статического сайта

Идентификаторы нужны для стабильных ссылок из багов, Playwright/Gherkin-сценариев и release evidence. Сценарии пользователя сохранены отдельно от сценариев, добавленных по результатам анализа.

> **Статус:** канонический acceptance inventory, а не журнал уже пройденных
> тестов. Наличие ID не означает, что функция реализована, автоматизирована или
> принята в production. Current release scope и gates определяет
> [release-plan.md](release-plan.md).

## Уровни доказательства

- **Automated release** — выполняется против clean main-reachable production
  candidate/public surface и выдаёт сохраняемое evidence.
- **Automated component** — unit/build/fixture contract; полезен, но один не
  закрывает release scenario.
- **Demo-only** — исполняемый stand/mocked client, не текущий Astro/public site.
- **Draft** — Gherkin/spec без полного runnable step/runtime path.
- **Manual/native** — требуется реальная ОС, browser/app/email/calendar/maps или
  визуальный review; mocks не заменяют evidence.
- **Planned** — product/implementation ещё отсутствует в `origin/main`.

## Карта текущего покрытия на 2026-07-21

| Surface | Сценарии | Уровень сейчас | Канонический test/command | Что не доказано |
|---|---|---|---|---|
| Preview build, files, routes, JSON-LD, ICS, media/static related | `ADD-BUILD-*`, `ADD-SEO-*`, часть `ADD-EVENT/MEDIA-*` | Automated component | `npm --prefix site run build:preview && npm --prefix site run check:preview` | production root, full live catalog, interaction, promotion/rollback |
| Public projection eligibility/media safety | `ADD-BUILD-03/04`, `ADD-MEDIA-*` | Automated component | `pytest -q tests/test_static_site_public_gate.py` | whole-catalog reconciliation and live HTTP |
| Static build orchestration and immutable Kaggle handoff | `ADD-BUILD-01`, `ADD-BUILD-08/13`, `ADD-OBS-01` | Automated component | `pytest -q tests/test_static_site_release.py tests/test_static_site_build_handoff.py` | real Kaggle/status-ledger and enabled Fly flags |
| Production/preview/secret artifact profiles | `ADD-BUILD-07/09/11/12` | Automated component | `npm --prefix site run test:static-release`; production/secret build+check commands from the runbook | live candidate HTTP and root promotion |
| KAUP timetable A/B/C | `ADD-TR-06..10` | Automated component + manual visual | `pytest -q tests/test_static_site_bus_boarding.py tests/test_static_site_transport_experiment.py` | consented focus-group/live outcomes and statistical decision |
| Exact KAUP reference fidelity | `ADD-V12-01` | Component + secret-candidate Playwright | exact A/B/C copy assertions; forced query URLs on the retained noindex specimen | user visual acceptance |
| CTA invariant geometry | `ADD-V12-02` | Browser geometry | `STATIC_SITE_REVIEW_BASE_URL=… npm --prefix site run check:desktop-cta-geometry` at `1536×864` | additional browser matrix |
| Venue medallion conflict ceiling | `ADD-V12-03` | Unit + generated HTML | `npm --prefix site run test:content-media`; events `5295/6796` | canonical cleanup of ambiguous past rows |
| Kaliningrad today + build single-flight | `ADD-V12-04..06` | State-machine/unit + fresh candidate | build clock boundary, no-op fingerprint, live/terminal orphan adoption, `/segodnya/` public check | production-run evidence |
| Typed no-image fallbacks | `ADD-V12-07` | Unit + public candidate | fallback is visible but absent from gallery/OG/JSON-LD/share | inventory refresh after each snapshot |
| Footer/media/continuation rollout | `ADD-V12-09..12` | Component + public Playwright | global service footer, related-card skeleton, portrait/wide CTA families, bounded desktop continuation | native share and long-session analytics |
| Desktop event keyboard navigation | `ADD-KEY-01..07` | Component + Chromium secret-candidate route | `node --test site/tests/keyboard-event-navigation-production.test.mjs`; `npm --prefix site run check:keyboard-event-navigation` | Firefox/Safari, screen reader, high contrast and root rollout |
| Generated event release journeys | `ADD-KEY-08..10`, `ADD-DISC-08..13` | Blocking Chromium production/candidate gate | `npm --prefix site run check:browser-release -- --root <generated-root>`; Smart Update/Kaggle invokes it before each archive | native Safari remains a root-rollout gate |
| Listing CSS + sticky stack | `ADD-V12-13` | Built CSS + public Playwright | four listing routes at `1366×768`, `1536×864`, `1920×1080`; header `0..57`, rail starts at `57` | cross-browser sticky behavior |
| Anonymous personalization | `USR-10..12`, `ADD-PERS-*` | Demo-only + Draft | `tests/playwright/static_personalization_contract.spec.ts` (9 mocked demo tests); `tests/e2e/features/static_site_personalization.feature` (`@draft`, no Behave steps) | current Astro/public E2E and backend persistence |
| Share/calendar/maps/email/native browser | `USR-02/03/06/07/13/15/16`, `ADD-SHARE/FAV/MAIL/TR-*` | Manual/native or Planned | per-scenario evidence | real target applications and not-yet-implemented durable flows |
| Atomic release and Telegraph cutover | `ADD-BUILD-*`, `ADD-CUTOVER-*` | Planned | future production publisher/resolver suite | весь production cutover contract |

### V12 mandatory regressions

- **ADD-V12-01 — Reference, not reinterpretation.** Arms A/B/C preserve their
  distinct last-mile and warning copy from Telegram messages `261–264`, the
  green route hierarchy, shared icons and `на Кауп` grammar. On the retained
  noindex QA route, every forced arm must be visible and overflow-free at both
  `1536×864` and `390×844`.
- **ADD-V12-02 — CTA bottom row.** At `1536×864`, event `6551` and the positive
  control `5374` each have exactly three aligned, contained bottom controls in
  calendar/share/like order with no horizontal overflow.
- **ADD-V12-03 — One venue medallion.** A short alias matches only at Unicode
  token boundaries; equally strong conflicting venue/ticket evidence renders
  zero venue marks, never two.
- **ADD-V12-04 — Local day.** `21:59 UTC` remains the same Kaliningrad day and
  `22:00 UTC` rolls to the next; `/segodnya/` must match the build receipt date.
- **ADD-V12-05 — No duplicate remote push.** A live orphan is deferred; an
  exact completed orphan is downloaded, hash/identity validated and adopted
  without `kernels_push`; only mismatched/failed remote identity releases the
  old claim for a replacement. If startup or Smart Update rearms the exact
  active error job, its remote handoff and immutable snapshot survive the
  payload merge; a non-active stale job does not leak those identities into a
  new request. A callback token committed by the runner must also be visible to
  the live aiohttp process even when its shared SQLite connection holds an old
  snapshot; callback writes retry a bounded writer lock and cannot poison the
  following callback transaction. A newer pending follow-up cannot supersede
  the older error row while that row is still the exact active recovery owner;
  recovery is ordered before follow-up generation.
- **ADD-V12-06 — Content no-op.** Operational queue churn and already elapsed
  rows do not change the public fingerprint; public fields, policy, repo SHA,
  related cache or local date do. Explicit operator force is audited separately.
- **ADD-V12-07 — Presentation-only art.** Symphonic/lecture fallback selection
  is typed and deterministic; it cannot become event media or appear in machine
  metadata/share payloads.
- **ADD-V12-08 — Expiry-safe design fixtures.** Full-catalog generation after a
  reviewed event expires must still build every design-system scenario from the
  frozen examples; current production membership is not a prerequisite for a
  lab contract.
- **ADD-V12-09 — One accepted footer.** Event, listing, search and retained lab
  pages render the same `service-v1` footer: one partnership link, compact share,
  disabled non-404 legal placeholders and the official coloured MAX asset. No
  legacy/prototype footer switch or duplicate partnership navigation remains.
- **ADD-V12-10 — Related-card loading geometry.** Desktop `Смотрите дальше`
  emits a server-rendered skeleton and `aria-busy=true`; successful and failed
  image completion both clear the state without changing the reserved frame.
- **ADD-V12-11 — CTA follows media family.** At `1536×864`, the retained Split
  fixtures `cta-phone-invariant`, `cta-registration-invariant` and
  `cta-free-calendar-invariant` keep one contained inline row, validate long
  primary-label compaction and calendar-primary semantics (including a free
  event with an official source URL), while the
  retained `footer-service-v1` Editorial fixture keeps the three-row panel with
  calendar/share/like aligned on the bottom row. Review may additionally sample
  live events. Real-data event `6667` is the range/free regression: Split OCR
  keeps Yantar Hall plus `0 ₽` inline, has no TopSlot, and exposes exactly one
  terracotta `Добавить в календарь` primary whose ICS is
  `20260708` through exclusive `20260729`. Expiry of `6876`, `4783`, `6551` or `5374` cannot remove or
  false-fail this executable geometry contract. Editorial side motion and CTA
  docking use that same state machine with one or many distinct photos; absence
  of the optional thumbnail rail cannot disable motion or change the family.
- **ADD-V12-12 — Finite desktop continuation.** Reaching `Смотрите дальше`
  produces exactly six deduplicated cards and `Все анонсы`, never load-more or
  infinite scroll. A mature profile uses `По вашим интересам`; an immature one
  is honestly labelled `Ещё события`, respects 3/category and 2/venue, and is
  not duplicated on the established mobile discovery surface. The separate
  heading is retained on desktop to mark the transition from explicit
  similarity to wider anti-bubble discovery; the latter may mix profile,
  non-rejected vector tail and diverse upcoming fallback rather than looping
  within one theatre/event type.
- **ADD-V12-13 — One styled listing shell.** Built Today, Tomorrow, Weekend and
  Popular HTML must resolve the shared listing selectors. After a real scroll,
  the global header stays sticky at `0..57px`, the discovery rail starts at
  `57px` below it, its z-index remains lower, representative card media stays
  bounded, the focused skip link remains above the header and no route creates horizontal overflow. Run with
  `STATIC_SITE_REVIEW_BASE_URL=… npm --prefix site run check:listing-desktop-geometry`.

### Desktop keyboard navigation V7

- **ADD-KEY-01 — One reviewed router.** Lab fixtures and secret-candidate event
  routes import the same V7 router originating at `d0027a53`; generated HTML and
  a second production imitation are forbidden. Flag-off/root/listing/mobile
  install no document listeners or key hints.
- **ADD-KEY-02 — Intent and ownership.** Page load never steals focus. The
  router activates after meaningful keyboard intent and handles contexts in the
  fixed order editor/IME → top dialog/gallery → service share → CTA → managed
  card → provenance-gated body recovery → native browser behavior. A fresh
  multi-image event permits only the first physical Left/Right intent to enter
  its hero; unrelated pointer/focus/blur/hidden provenance revokes that entry.
- **ADD-KEY-03 — Gallery Down latch.** A fresh `ArrowDown` closes a keyboard- or
  pointer-opened gallery through its existing close control, restores the
  logical owner and neither scrolls the covered page nor leaks held repeat into
  page scrolling. Only a physical keyup permits a later Down scroll.
- **ADD-KEY-04 — Lost-focus actions.** Body recovery for physical
  `KeyL/KeyK/KeyS/Enter` works only after CTA, related/broad-card ownership or a
  real click on inert current-event content. Header, footer, editor, unrelated
  control, blur/hidden and top dialogs disarm it until a new managed owner
  exists. Acceptance dispatches Cyrillic logical keys (`д/л/ы`) with stable
  physical codes to prove layout independence.
- **ADD-KEY-05 — Canonical two-zone cards.** `Смотрите дальше` and the finite
  broader section use the same `EventCard` DOM/controller. Arrows bridge their
  explicit zones; Enter/L/K/S and rerender focus restoration behave identically
  without erasing feedback state or rotating unrelated cards.
- **ADD-KEY-06 — Lifecycle/privacy.** `destroy()` aborts listeners, disconnects
  observers, clears timers/RAF/latches and restores every pre-existing
  title/ARIA/tabindex attribute. Slow async consent is transition-owned rather
  than dropped after a timer. Daily local facts are boolean,
  allowlisted, deduplicated and URL/event/title/key/count/time-free; no remote
  collector is enabled.
- **ADD-KEY-07 — Release boundary.** Every newly checked immutable secret
  candidate enables the router across its full event catalog and remains exact
  `noindex`/no-referrer. Root/current/stable ICS hashes do not change. A root
  rollout remains blocked until Firefox/Safari, screen-reader, high-contrast and
  zoom/reflow gates pass.
- **ADD-KEY-08 — Real cross-document handoff.** Open the current event gallery,
  reach its recommendation, activate it with Enter without intercepting the
  navigation, then press Left/Right while destination focus is still `BODY`.
  A short-lived same-origin provenance handoff restores the destination event
  surface and changes the hero. Independently, a direct fresh load/reload keeps
  `BODY` focus but lets the first Left/Right intent enter a multi-image hero;
  single-image pages and pages whose header/editor/dialog owns context remain
  native/no-op.
- **ADD-KEY-09 — Footer after pointer scroll.** Scroll to a visible footer with
  touchpad/wheel while focus is `BODY`, then repeat with focus retained on an
  off-screen managed card or current-event action surface. `P` and `S` must copy the service image/text target
  and show feedback, never silently do nothing or share the stale card.
- **ADD-KEY-10 — Footer ambiguity guard.** A visible/focused header, editor,
  dialog, on-screen event action or card retains ownership; footer visibility
  alone must not steal shortcuts from a meaningful visible target.

### Related graph and visual release regressions

- **ADD-DISC-08 — Global row optimizer owns compact crop.** Every generated
  related and broader card uses one `EventCard` layout decision from the
  globally minimum-height feasible grouping. Cards may reorder; each row has
  one visible media height and one total card height. Every image uses `cover`
  with zero unused frame; an ordinary OCR/document source fixes the natural row
  ratio, while only a very tall document may crop and actual area loss is at
  most `20%`. Declared treatment, computed `object-fit`, row/column, reserved
  ratio and decoded-pixel crop budget must agree at `1536×864`; surface CSS
  may not reinterpret the decision.
- **ADD-DISC-14 — Full rows and intrinsic compact chrome.** For every desktop
  related/personal section, all rows except the final row contain exactly three
  cards. The final row contains `1..3`; explicit grid coordinates never leave a
  middle hole. Media and total card heights are equal within each row, but body
  and action tracks are intrinsic per row rather than globally fixed to
  `184px + 58px + 56px`. The generated browser report records row cardinality
  and rejects a row whose content-owning card retains a large synthetic body
  gap. Counts `1..10` and the real event `6408` are mandatory canaries.
- **ADD-DISC-15 — Generated occurrence-family card.** When review/canonical
  input explicitly carries reciprocal `6318 ↔ 6586`, generated event `6408`
  must contain exactly one Romeo card, `data-occurrence-member-ids="6318,6586"`,
  visible text `2, 3 ноября 19:00` and full aria label
  `2 и 3 ноября в 19:00`. The same titles without reciprocal explicit links
  remain separate; venue/title inference is forbidden.
- **ADD-DISC-11 — Loaded media owns the frame.** For every canonical
  recommendation card, Playwright waits for the lazy image to settle and
  `decode()`. A successfully loaded image must hide the semantic date/type/city
  failure fallback and leave no band; a failed image must keep that fallback.
  The release artifact retains a full related-section screenshot and a
  `1536×864` viewport capture after pixels settle. Correct `object-fit` alone
  is not acceptance.
- **ADD-DISC-12 — Cold and mixed-input matrix.** On both reported routes
  `6408` and `6593`, the blocking generated-tree gate covers fresh load,
  reload, real inert-content mouse click, Russian-layout `L/K/S`, Enter,
  header/editor negatives, a single-image negative, cross-document gallery
  handoff, footer touchpad ownership and canonical related/broad-card Enter.
  Run the same command with `--browser firefox` and `--browser webkit` where
  the host provides Playwright system dependencies; Playwright WebKit is an
  approximation, not a substitute for the native Safari rollout check.
- **ADD-DISC-13 — Non-OCR hero/gallery fills the stage.** On event `6408` and
  the generated crop-canary route, each `visual_only` hero and gallery slide
  computes to `cover` even when semantic-role metadata is uncertain; OCR/text/
  unknown documents remain `contain`. The browser report and retained hero
  screenshot must show no top/bottom fields on the non-OCR slide.
- **ADD-DISC-09 — Exact-title recall without unsafe merge.** Every eligible
  exact-normalized-title pair is mutual in the related graph even when Smart
  Update has not merged it. This is a discovery invariant, not evidence that
  differing venues/dates/sources may be merged deterministically.
- **ADD-DISC-10 — Vector revision and topology barrier.** A changed
  `related_v1_hash` invalidates the related cache. Full production export fails
  before archive when the vector receipt is incomplete/stale, zero-incoming is
  at least 5%, an eligible exact-title pair is disconnected, or a chain has
  fewer than four candidates.

### Release evidence row contract

При автоматизации или ручном приёмочном прогоне к ID добавляется отдельная
evidence row/report со следующими полями: release tier, implementation status,
automation level, canonical test/command, fixture/persona, URL/page family,
viewport/device/OS/browser, production SHA, snapshot/build/manifest id, expected
result, actual result, artifact/log link и reviewer. Эти поля не подменяются
словом `passed` без привязки к конкретной сборке.

## Сценарии, добавленные пользователем

- **USR-01 — Лайк события.**
- **USR-02 — Поделиться событием на десктопе.**
- **USR-03 — Поделиться событием на мобильном устройстве.**
- **USR-04 — Поиск по жанру и времени.** Поискать события, например по названию любимого жанра и признаку времени.
- **USR-05 — Поиск уличного спектакля.**
- **USR-06 — Добавление в календарь и избранное.** Проверить, что событие после добавления видно в разделе «Моё» / «Избранное» или в окончательно выбранном разделе.
- **USR-07 — Привязка почты.** Протестировать привязку почты с использованием ссылки и/или кода подтверждения. Для теста использовать собственную `info@kenigevents.ru` и автоматически проверить поступление письма.
- **USR-08 — Индикатор новых интересных событий.** Проверить, что при появлении в категории новых событий, которые могут быть интересны пользователю, у соответствующего пункта меню появляется выделение в виде красной точки.
- **USR-09 — Визуальная целостность списочных элементов.** Проверить, что списочные элементы не разваливаются.
- **USR-10 — Накопление персонализации.**
- **USR-11 — Нахождение интересного события.** При накопленной персонализации интересное пользователю событие должно встретиться не позднее просмотра 30 карточек.
- **USR-12 — Сброс персонализации.**
- **USR-13 — Почтовый дайджест.** Проверить дайджест интересных событий на почту при развитой персонализации.
- **USR-14 — Подтверждение успешности поиска.** На mobile проверить всю границу
  результата: во время первого запроса виден structural skeleton больших
  `EventCard`, provisional vector payload не заменяет его временными карточками,
  exact cards идут под `Результаты поиска`. Пока `has_more=true`, feedback и
  fallback отсутствуют. После исчерпания exact выдачи идут
  `Нашли то, что искали?` → `Да, нашёл`/`Нет, не нашёл` (RPC values
  `matched`/`missed`), затем отдельное `Ещё можно посмотреть`. Для zero-result
  перед feedback явно показано `По вашему запросу ничего не найдено`.
- **USR-15 — Проверка транспорта.**
- **USR-16 — Маршрут «Как добраться».** Проверить загрузку маршрута до события в карты от текущей локации пользователя. Должна открыться именно найденная именованная локация события по корректным координатам, а не случайная или безымянная точка.
- **USR-17 — Визуальная проверка изображений.** Проверить, что изображения не обрезают лица в `hero talk`, hero-блоках и каруселях.
- **USR-18 — Desktop/mobile coverage.** Все применимые сценарии проверить как минимум на десктопе и мобильном viewport/device-профиле.

## Дополнительные сценарии по результатам анализа

Эти сценарии не заменяют `USR-*` и не меняют их смысл. Они закрывают найденные пробелы вокруг identity, персонализации, жизненного цикла события, статической публикации, почты, accessibility, SEO/GEO и деградаций.

### Identity, авторизация и почта

- **ADD-ID-01 — Единое состояние identity на всех страницах.** Вход, выход, добавление и забывание email должны одинаково работать на главной, листингах, поиске, странице события, похожих, персональной странице и в избранном; поиск не должен быть единственным владельцем identity UI.
- **ADD-ID-02 — Email вводится один раз.** После подтверждения email сохраняется в разрешённом device-local состоянии, переживает reload/переходы и не запрашивается повторно при каждом добавлении события в календарь.
- **ADD-ID-03 — «Забыть почту на этом устройстве».** Действие очищает локальный email/session state и обновляет интерфейс во всех открытых вкладках, но не выдаёт себя за удаление аккаунта, отзыв согласия или отписку от писем.
- **ADD-ID-04 — Яндекс login/logout и склейка профиля.** Анонимные действия до входа не теряются и не удваиваются после входа; logout не показывает данные другого пользователя, а повторный вход восстанавливает именно его данные.
- **ADD-ID-05 — Email verification edge cases.** Проверить неверный, истёкший и повторно использованный код/ссылку, лимит повторной отправки, смену email и безопасное восстановление после reload/back.
- **ADD-ID-06 — Яндекс без доступного email.** Вход остаётся успешным, но email-зависимое действие явно предлагает отдельную верификацию почты и не обещает несуществующее уведомление.
- **ADD-ID-07 — Переключение аккаунтов.** После выхода пользователя A и входа пользователя B не должны протекать избранное, email, профиль, поисковые теги или персональная выдача пользователя A.

### Персональные страницы, согласие и приватность

- **ADD-PRIV-01 — Публичная секретная ссылка.** Персональная статическая страница открывается без авторизации любому владельцу ссылки, включая другого пользователя, и не переназначается на его текущий профиль.
- **ADD-PRIV-02 — `noindex` персональной страницы.** Страница по секретной ссылке имеет корректный `noindex`, отсутствует в sitemap и внутренних публичных индексах, но остаётся доступной по самой ссылке.
- **ADD-PRIV-03 — Отсутствие персональных утечек.** В HTML, URL, метаданных, share-card, analytics и source map не появляются email, account id, скрытые scores, сырой профиль или история действий.
- **ADD-PRIV-04 — Consent denied/revoked.** При отказе или отзыве согласия удалённая персонализация/telemetry не включается; статическая выдача, поиск, ICS и базовая навигация продолжают работать.

### Персонализация и discovery

- **ADD-PERS-01 — Cold start.** Новый пользователь без истории получает полезную разнообразную статическую выдачу без пустого блока, выдуманных предпочтений и требования авторизации.
- **ADD-PERS-02 — Полная цепочка сигнала.** Один correlated сценарий доказывает: действие записалось в localStorage → принято backend/DB → изменило ожидаемый профиль → повлияло на следующую выдачу.
- **ADD-PERS-03 — Дедупликация сигналов.** Reload, back/forward, повторный клик и повторная доставка telemetry не создают несколько одинаковых действий или искусственно завышенный интерес.
- **ADD-PERS-04 — «Не интересно» и отмена.** Карточка превращается в понятное ненавигационное состояние, `Отменить` восстанавливает её, а сигнал корректно отражается в localStorage, backend и следующей выдаче.
- **ADD-PERS-05 — Межвкладочная согласованность.** Like, favorite, dislike, identity и сброс профиля синхронизируются между вкладками без гонок и возврата устаревшего состояния.
- **ADD-PERS-06 — Отсутствие жёсткого инфопузыря.** У зрелой golden-персоны выполняется целевой лимит нахождения интересного, но в пределах принятого окна остаются контролируемые новые/разнообразные события вне уже известных интересов.
- **ADD-PERS-07 — Нет релевантного предложения в каталоге.** Система не должна считать KPI проваленным или подставлять слабое событие как «точно интересное», если в актуальном inventory нет подходящего предложения.
- **ADD-PERS-08 — Сброс без восстановления из кэша.** После сброса профиль и персональный порядок не возвращаются из старого cache, service worker, history/back или другой вкладки.

### Поиск, теги, похожие и популярное

- **ADD-DISC-01 — Пустой и ошибочный поиск.** Пустой запрос, опечатка, отсутствие результатов, timeout/quota и offline дают понятные состояния и не ломают базовый статический листинг.
- **ADD-DISC-02 — Сохранение поиска как публичного тега.** Intent проходит нормализацию, проверку минимального результата и novelty; практически совпадающий поиск переиспользует существующий тег, действительно новый получает стабильный публичный URL.
- **ADD-DISC-03 — Идемпотентная генерация тега.** Повторный запуск с тем же нормализованным intent/result hash не создаёт дубль страницы или навигационного пункта; rejected/pending тег не попадает в sitemap и public navigation.
- **ADD-DISC-04 — Актуализация сохранённого тега.** После изменения каталога страница тега обновляет результаты одним штатным static rebuild и не сохраняет завершённые/отменённые события как актуальные.
- **ADD-DISC-05 — Качество «Похожих событий».** Список действительно семантически похож, не содержит исходное событие, дубль или ту же программу из блока «Другие даты», не повторяет карточки и имеет безопасный fallback при отсутствии vectors/LLM verdict.
- **ADD-DISC-06 — Автоматическое обновление похожих.** После effectful Smart Update нового/изменённого события, debounce и Kaggle-прохода новый verified related graph попадает в опубликованный static manifest; reverse-affected старые события также обновляются.
- **ADD-DISC-07 — «Популярное» из общей статистики.** Порядок использует единую консолидированную статистику источников и сайта; проверить source-only, site-only и смешанный пример, freshness/last-good и отсутствие отдельной скрытой формулы страницы. Для `multi_source` нужны две независимые внешние publisher-family: собственные `kldevents`/`kenigevents` и VK `231920894`/`231828790` схлопываются в одну owned-family и не превращают один внешний анонс в ложное подтверждение «в нескольких источниках».
- **ADD-DISC-08 — Desktop Popular: актуальность и family dedup.** На момент `build.generated_at` уже начавшееся разовое событие отсутствует, ещё идущее многодневное остаётся; normalized family встречается только один раз на пяти полках, а другие даты свернуты в `ещё N показов` без суммирования engagement.
- **ADD-DISC-09 — Desktop Popular: честные короткие полки.** Порядок полок фиксирован (`fast_growth`, `multi_source`, `discussed`, `frequently_shared`, fallback), каждая показанная evidence-полка содержит 3–5 реальных кандидатов в одной строке; нет fillers, card-level reason, чипов и load-more. Break Summer Fest `5130` присутствует в актуальной глобальной выдаче.
- **ADD-DISC-10 — Desktop Popular: warm-only 4+1.** Cold/no-consent/несовместимый профиль и два сильных сигнала не создают шестую полку; совместимый профиль с тремя сигналами получает ровно 4 affinity + 1 anti-bubble после исключения hidden/not-interested и уже показанных families; недостаточный пул скрывает полку целиком и не меняет mobile V26 order.

### Избранное, календарь и уведомления

- **ADD-FAV-01 — Идемпотентное сохранение.** Повторное добавление одного события не создаёт дубль в избранном, ICS или reminder; badge показывает число уникальных сохранённых событий только при `N > 0`.
- **ADD-FAV-02 — Полный список «Моё избранное».** Список открывается из общего меню на всех страницах, показывает все сохранённые события и явно различает upcoming, перенесённые, отменённые и уже завершившиеся.
- **ADD-FAV-03 — Удаление и отмена.** Удаление из карточки, detail и избранного приводит к одному состоянию; undo не создаёт второй объект или второй reminder.
- **ADD-FAV-04 — Корректность ICS.** Проверить title, именованную локацию, координаты/адрес, timezone, начало/конец, canonical URL, перенос и отмену в реально импортированном календаре.
- **ADD-FAV-05 — Видимое обещание D-1 письма.** После добавления интерфейс явно показывает, будет ли письмо за день, на какой masked email и что нужно сделать, если email отсутствует/не подтверждён или consent не дан.
- **ADD-FAV-06 — D-1 идемпотентность и изменение события.** Повторный scheduler run не отправляет второе письмо; reschedule пересчитывает уведомление, отмена его подавляет, слишком позднее сохранение следует принятой catch-up policy.
- **ADD-FAV-07 — Раздельность like/favorite/transport.** Like, скачанный ICS и отдельно сохранённый транспортный участок не должны ошибочно увеличивать badge сохранённых событий.

### Почтовые подборки и deliverability

- **ADD-MAIL-01 — Ровно три предложения и рабочая персональная ссылка.** Письмо содержит три актуальных неповторяющихся события, а его страница соответствует тому же snapshot/пользователю без PII в URL или HTML.
- **ADD-MAIL-02 — Suppression и bounce.** Hard bounce, complaint, unsubscribe и действующая suppression исключают следующую отправку; provider event применяется идемпотентно и не понижает репутацию другого mail stream/поддомена.
- **ADD-MAIL-03 — Ссылки и представление письма.** Проверить canonical/UTM links, plain-text fallback, изображения-disabled режим, mobile/desktop mail clients и отсутствие preview/secret leakage.
- **ADD-MAIL-04 — Устаревшее событие между enqueue и send.** Отменённое, перенесённое или завершившееся событие не уходит со старой датой; применяется revalidation или безопасная отмена отправки.

### Связанные даты и жизненный цикл события

- **ADD-EVENT-01 — Несколько дат/времени одной программы.** Карточка явно показывает наличие альтернатив, открывает полный список на detail и сохраняет именно выбранный occurrence с правильными URL/ICS/reminder.
- **ADD-EVENT-02 — Same-slot duplicate не является альтернативой.** Дубли одного времени объединяются/исключаются, а не показываются как «другая дата».
- **ADD-EVENT-03 — Перенос, отмена и завершение.** Изменение статуса согласовано на главной, листингах, поиске, тегах, похожих, избранном, detail, share и calendar/reminder surfaces.
- **ADD-EVENT-04 — Возрастная маркировка.** Подтверждённое `0+|6+|12+|16+|18+` одинаково видно на каждой event-bearing surface; неизвестное значение остаётся без выдуманного `0+`.
- **ADD-EVENT-05 — Критические факты совпадают.** Название, дата/время, timezone, город, площадка, адрес, ticket/free status и canonical link совпадают между карточкой, detail, JSON-LD, ICS, письмом и share payload.

### Изображения, медальоны и share

- **ADD-MEDIA-01 — Нет дублей изображений внутри события.** Проверить exact SHA, re-encode и визуально одинаковые crop/overlay варианты; содержательно разные постеры/программа/фото сохраняются.
- **ADD-MEDIA-02 — Умный фокус во всех форматах.** Лицо, заголовок афиши и главный объект не теряются в mobile/desktop card, hero, carousel, related, email и share-card; OCR-постер не обрезается как фотография.
- **ADD-MEDIA-03 — Битое/отсутствующее изображение.** Broken CDN URL, slow image и отсутствие media дают чистый fallback без layout shift, пустой alt, бесконечного loader или потери ссылки на событие.
- **ADD-MEDIA-04 — Медальоны на всех static-site surfaces.** Playwright снимает фактические места на mobile/desktop; artwork, ring и shadow не обрезаны и не загрязнены. Telegram-медальоны в этот сценарий не входят.
- **ADD-SHARE-01 — Adaptive share одного события.** На mobile вызывается системный share с корректным fallback; на desktop выполняется принятый clipboard-сценарий. Обе версии используют canonical URL и правильную карточку именно этого события.
- **ADD-SHARE-02 — Поделиться самим сервисом.** Одна кнопка доступна в раскрытом mobile menu и footer, на desktop работает как copy; используется заранее отрендеренная актуальная карточка, а не on-demand генерация.
- **ADD-SHARE-03 — Отмена и недоступность системного share/clipboard.** Cancel, permission denial, insecure context и unsupported API дают честное ненавязчивое состояние и рабочий fallback, а не ложное «отправлено».

### Транспорт

- **ADD-TR-01 — Eligibility транспорта.** Транспортная подсказка показывается только для поддерживаемого события вне Калининграда и не появляется с нерелевантным маршрутом для городского события.
- **ADD-TR-02 — Туда/обратно и достижимость.** Рейс действительно позволяет прибыть до начала и, если обещан обратный путь, уехать после завершения с допустимым запасом времени.
- **ADD-TR-03 — Provider stale/partial failure.** При недоступности КППК или автобусов показывается last-good с честной свежестью либо безопасное отсутствие блока; данные одного provider не затирают исправные данные другого.
- **ADD-TR-04 — Транспортная карточка в галерее.** Если опция включена, «Как добраться» является отдельной понятной карточкой, не выдаёт себя за афишу и совпадает с detail route data.
- **ADD-TR-05 — Избранный транспортный участок.** Повторное сохранение идемпотентно, относится к выбранному occurrence/направлению и не увеличивает event-favorite badge.
- **ADD-TR-06 — Три лаконичных варианта расписания.** Для одной и той же
  транспортной проекции доступны `departure_board_v1`, `route_strips_v1` и
  `next_departure_queue_v1`; вариант меняет только представление, но не рейсы,
  остановку посадки, маршруты или CTA.
- **ADD-TR-07 — Стабильное распределение A/B/C.** Один browser subject получает
  один вариант SHA-256-распределением и сохраняет его между событиями, rebuild и
  secret-prefix; в каждой сессии отрисован ровно один arm.
- **ADD-TR-08 — QA override не загрязняет эксперимент.** Query override позволяет
  визуально проверить каждый arm, но forced/QA impression и action не попадают в
  доверенную telemetry; production-root по умолчанию работает в `off`.
- **ADD-TR-09 — Квалифицированная экспозиция и исход.** Impression засчитывается
  только после не менее 50% видимости блока в течение одной секунды; outcome —
  только осмысленный клик `transfer`, `map`, `walk` или `car`, связанный с тем же
  experiment/variant/subject.
- **ADD-TR-10 — Посадка на Северном вокзале.** Если рейс проходит остановку
  «Калининград Северный», расчёт и видимая транспортная цепочка начинаются там,
  а не с конечной остановки маршрута.

### Социальные действия и обратная связь по событию

- **ADD-SOC-01 — Консолидированные просмотры, лайки и share.** Действия на сайте учитываются вместе со статистикой Telegram/VK через один общий event-level contract; retry/reload не удваивает действие, stale source не превращается в ноль, а все consumers видят совместимые totals.
- **ADD-SOC-02 — Честные публичные счётчики.** Нулевые значения скрыты по принятому UI-контракту, displayed total совпадает с принятой проекцией, а пользовательский интерфейс не называет reach-наблюдения уникальными людьми.
- **ADD-FEEDBACK-01 — Проверенные факты из комментариев.** На странице показываются только актуальные event-bound факты с допустимым source evidence; вопрос, слух, отрицание, неопределённость, ответ неавторитетного пользователя или комментарий к другому событию не становятся утверждением.
- **ADD-FEEDBACK-02 — Исправление и протухание факта.** Correction, retraction и TTL удаляют/заменяют публичный факт одним rebuild, не переписывают критические date/time/location поля и не оставляют старую формулировку в HTML/cache.
- **ADD-FEEDBACK-03 — «Активно обсуждают».** Если post-release medallion включён, он появляется только при выполнении принятого human-discussion threshold, не учитывает ботов/официальные ответы/дубли/engagement bait и не влияет на ranking без отдельного решения.

### Regression-сценарии изменений 15–17 июля

- **ADD-RECENT-01 — Interest-club projection gate.** В public manifest и sitemap
  попадают только approved/fresh club identities и accepted event relations;
  deferred/review/festival-only/устаревшие identities не протекают, а failed club
  projection не блокирует базовый event build.
- **ADD-RECENT-02 — Desktop Editorial media-role contract.** На desktop строгий
  `event_identity_poster` получает полный poster companion, photo сохраняет
  принятую Editorial composition, а service/schedule/document/OCR/unknown media
  не превращаются в crop-cover из-за размеров или наличия текста.
- **ADD-RECENT-03 — Owned social popularity без двойного счёта.** Метрики всех
  управляемых TG/VK event surfaces собираются, но repost одного события внутри
  owned audience схлопывается по max-компонентам, а независимые внешние источники
  продолжают учитываться отдельно.
- **ADD-RECENT-04 — Возрастная маркировка.** Публично показывается только
  подтверждённое declared значение `0+|6+|12+|16+|18+`; assessed-only/unknown не
  превращается в выдуманное ограничение, а видимый факт совпадает с export и
  structured data.
- **ADD-RECENT-05 — Полная матрица event-detail v11.** Preview обязан собрать
  каждый сценарий из `eventTemplateContract.json`: горизонтальное фото,
  горизонтальное фото с отдельной OCR-афишей, OCR/document contain,
  одиночные/серийные вертикальные изображения, low-resolution и
  quality-fallback. Все сценарии и реальные event pages используют один
  `DesktopEventPage`/`buildDesktopEventPresentation`, а не legacy desktop DOM.
- **ADD-RECENT-06 — Один quality-admitted набор на desktop и mobile.** Если у
  события есть технически сильные event-local фотографии, desktop hero/viewer
  и mobile hero/gallery используют только этот набор и одинаковый hidden count;
  слабые renditions сохраняются только в weak-only семье. Classified
  OCR/document media не удаляются этим техническим gate и продолжают
  показываться без crop. Контроль: событие `4783` содержит индексы
  `0,4,6,8,9,10,11` и скрывает `1,2,3,5,7` на обеих поверхностях.
- **ADD-RECENT-07 — Low-resolution portrait без разрушительного увеличения.**
  Единственная слабая вертикальная фотография не исключается, но получает
  source-size bounded `contain` на desktop и mobile и открывает тот же
  эффективный viewer; она не превращается в растянутый crop/длинный
  natural-size scroll ни в hero, ни в fullscreen. Контроль: событие `6815`.
- **ADD-RECENT-08 — Source-consistent occurrence projection.** Если точный
  structured first-party source совпадает с canonical row по date, time и
  ticket URL, но canonical type/copy загрязнены другим occurrence, статическая
  страница fail-safe показывает подтверждённые source title/type/description.
  Это не заменяет Smart Update repair и не является keyword/date-only merge
  правилом. Некорректный OCR/non-identity document не может владеть hero при
  наличии сильного event-local photo и никогда не crop-ится. После canonical
  repair горизонтальная classified event photo чуть ниже Editorial-порога
  также может уступить hero более сильной горизонтальной classified photo того
  же события; portrait/square primary этим правилом не подменяется. Контроль:
  `5756` в обоих source-consistent состояниях.
- **ADD-RECENT-09 — Sentence-safe editorial lead.** Lead использует готовое
  авторское summary, но обнаруженный ложный punctuated prefix заменяет полной
  source-backed фразой; raw character cut раскрывается знаком `…`, а не
  придуманной точкой. Контроль: `5658` заканчивает вводную словами
  «превращается в остросюжетный конфликт.»
- **ADD-RECENT-10 — Принятый transport fallback не откатывается в compact list.**
  Все три arm `departure_board_v1|route_strips_v1|next_departure_queue_v1`
  остаются принятыми A/B/C, а off/no-JS/automation/elapsed/ineligible состояние
  показывает принятый `departure_board_v1`, не старую компактную таблицу.
  Forced QA проверяет каждый arm, один рейс не выводит «Ещё 0 рейсов», а
  официальный трансфер допускает «автобус или микроавтобус». Контроль: `4671`.
- **ADD-RECENT-11 — Pixel-current bbox и fail-closed crop.** Exact-v2 row не
  сливается с другой rendition по mutable source URL и повторный source
  reconcile не сбрасывает её semantic/geometry evidence. Static export отдаёт
  bbox только при совпадении pixel/model/prompt. Production desktop contract
  допускает `cover` только с `protected_regions_fit`; неизвестный responsive
  aspect честно остаётся `contain` с reason, а не блокирует всю сборку.

### Статическая публикация, CDN и устойчивость

- **ADD-BUILD-01 — Debounce после Smart Update.** Несколько effectful обновлений в пределах окна приводят к одному build через 15 минут после последнего эффекта; no-op update не запускает лишнюю сборку.
- **ADD-BUILD-02 — Atomic promotion и rollback.** Пользователь не видит смесь HTML/assets разных releases; failed check не меняет current, а rollback полностью возвращает последний проверенный manifest.
- **ADD-BUILD-03 — Catalog parity и отсутствие устаревших страниц.** Все eligible события присутствуют, ineligible/quarantined не опубликованы, удалённые slug получают принятую redirect/410 policy и не остаются в sitemap.
- **ADD-BUILD-04 — CDN asset contract.** Canonical HTML/assets/ICS реально обслуживаются через принятый CDN path; runtime raster assets легковесные WebP, векторные — безопасные SVG, нет случайных тяжёлых PNG/JPEG или origin-only зависимостей.
- **ADD-BUILD-05 — Degraded network.** Slow 3G, offline after first load, CDN image failure и временная недоступность personalization/search API не ломают статический контент, навигацию, detail и ICS.
- **ADD-BUILD-06 — Cache/back/version transition.** После публикации новой версии reload/back/forward/service cache не возвращают несовместимые JS/data или состояние старого пользователя.
- **ADD-BUILD-07 — Preview/production/secret isolation.** Preview и secret
  candidate остаются `noindex`; production profile формирует root canonical,
  indexable robots/sitemap и не включает preview/lab/fixture routes. Ссылка
  бренда внутри fullscreen hero-gallery также ведёт в home текущего профиля, а
  не возвращает production-пользователя в `__preview`.
- **ADD-BUILD-08 — Immutable Kaggle handoff.** Kaggle получает отдельный
  read-only SQLite snapshot с `quick_check`, SHA-256, размером, max revision и
  уникальными `snapshot_id/run_id/build_id`; неверный hash/manifest останавливает
  запуск.
- **ADD-BUILD-09 — Manifest/tree/catalog parity.** Checked manifest доказывает
  равенство eligible catalog, event pages, sitemap/ICS и фактического дерева,
  отсутствие ineligible leaks, orphan references и dangling occurrence links.
- **ADD-BUILD-10 — Immutable candidate и rollback boundary.** Failed build/check
  не пишет ни одного объекта; accepted candidate загружается create-only только
  в новый secret prefix. Изменение `root`, `current` и stable `/ics/` запрещено.
  Reader-atomic root promotion/rollback остаётся отдельным blocked gate и не
  подменяется последовательным копированием файлов.
- **ADD-BUILD-11 — CDN/MIME/assets.** Все objects candidate имеют manifest hash,
  корректный MIME/cache policy, допустимый CDN asset URL и после публикации
  проверяются authenticated HEAD/GET; root/current не меняются.
- **ADD-BUILD-12 — Capacity и privacy preflight.** До upload проверяются bounded
  result/archive, свободное место/лимиты и запрет anonymous bucket listing;
  secret token не попадает в sitemap, canonical, логи или внутренние ссылки.
- **ADD-BUILD-13 — Freshness/retry/catch-up.** Feature runtime допускает до 5400
  секунд; retry bounded и классифицирован, stale/missed/deferred request после
  restart получает ровно один catch-up, а update во время running build — ровно
  один follow-up с более новым snapshot.
- **ADD-BUILD-14 — Template provenance.** Production и secret manifest фиксируют
  `static-event-detail-v11` и accepted source SHA `3b17e536…`; каждая страница
  несёт те же markers и один из разрешённых `editorial|split` результатов.
  Real-event specimens привязаны к стабильному event id, а не к изменяемому
  после Smart Update slug/title.
  Отсутствие хотя бы одного marker, lab-сценария или real-event family
  останавливает build до Kaggle artifact publication.
  Production-profile check всегда читает root-form `dist/sobytiya`, даже если
  Kaggle runtime унаследовал исторический `PREVIEW_BUILD_ID`.
- **ADD-BUILD-15 — Dual-disk и bounded artifacts.** Перед удалённым push
  проверяются writable/free-space для persistent `/data` и root scratch `/tmp`
  реальной create+fsync+remove пробой. Static snapshot/output хранится только в
  configured `/data/static_site_builder`; после durable terminal receipt
  удаляются лишь распознанные `output-production-*` сверх retention, точный
  active/recoverable handoff сохраняется, symlink/unknown path остаётся
  нетронутым. Любой runner `preview-*|production-*` build id валидируется до
  построения пути; pre-existing symlink и traversal отклоняются. Broken `/tmp`
  делает health/preflight красным, а запрос остаётся pending без расходования
  конечного retry budget.
- **ADD-BUILD-16 — Bounded video side outputs.** После отправки удаляются только
  exact non-symlink `videoannounce-publish-only-source-<id>`,
  `videoannounce-publish-only-<id>` и `videoannounce-logs-<id>` внутри
  configured temp root. Основной `videoannounce-<id>` удаляется лишь по
  persisted published terminal state без ожидающего main target; active,
  ledger-live, failed и publish-blocked сохраняются. Startup reconciliation
  повторяет только это assertion-safe удаление после прошлой ошибки cleanup.

### Related/vector barrier

- **ADD-RELATED-05 — Единая геометрия continuation.** `Смотрите дальше` и
  desktop `Ещё события` используют один three-card row contract: одинаковый
  canonical `EventCard` DOM/behavior renderer (не отдельный строковый HTML),
  shell, `visual_only` cover, OCR/document crop не более 20%, skeleton без shift.
- **ADD-RELATED-06 — Diverse mixed continuation.** Максимум шесть карточек;
  current/prior/hidden/recent-served исключены, cap `3/category`, `2/venue`;
  profile и non-rejected vector-adjacent interleave детерминированно. Raw/not-run
  vector не называется LLM-approved/`Похожие`, explicit rejected не показывается.

- **ADD-RELATED-01 — Revision barrier.** При включённом related mode manifest
  фиксирует ожидаемые `search_v3/related_v1` revisions/hashes и не публикует
  устаревшую verified projection.
- **ADD-RELATED-02 — Reverse-affected anchors.** Изменение кандидата обновляет не
  только его страницу, но и старые anchors, в related-выдачу которых он входит.
- **ADD-RELATED-03 — Last-good или честное отсутствие.** Provider/vector failure
  сохраняет совместимую last-good projection либо убирает optional block; raw
  candidates не маркируются проверенными.
- **ADD-RELATED-04 — Optional barrier не блокирует base pages.** При выключенной
  related/personalization части базовый full-catalog export/build/check остаётся
  работоспособным и явно записывает disabled revision state.

### Telegraph coexistence и D10 cutover

- **ADD-CUTOVER-01 — Dual-run создаёт оба event-detail артефакта.** В D0–D9
  eligible событие попадает в current static release и в Telegraph shadow path;
  отчёт связывает оба URL с одним event revision.
- **ADD-CUTOVER-02 — Readiness до выдачи static URL.** Resolver возвращает static
  URL только если current promoted manifest содержит нужные `event_id` и revision;
  до D10 допускается явный existing-Telegraph fallback, после D10 — ожидание/catch-up,
  но не создание Telegraph.
- **ADD-CUTOVER-03 — D10 запрещает create и recreate.** В `existing_only` и `off`
  новый event не вызывает Telegraph create, а failed edit старой страницы не
  провоцирует fallback-recreate; счётчики попыток после cutoff равны нулю.
- **ADD-CUTOVER-04 — Legacy Telegraph URLs сохраняются.** Cutover, rebuild,
  rollback и cleanup не очищают `telegraph_url/path`; старые опубликованные ссылки
  остаются доступными и не меняют canonical нового static event page.
- **ADD-CUTOVER-05 — Все outbound surfaces используют resolver.** Telegram/VK
  event posts, importer waits, digests и admin reports не зависят от успешного
  `telegraph_build`, когда static revision ready; в dual report разрешены оба URL.
- **ADD-CUTOVER-06 — Rollback обратим и ограничен.** До D10 outward mode можно
  вернуть в Telegraph/dual и восстановить last-good static pointer; после D10
  emergency `create_edit` включается только явно и выполняет bounded backfill без
  удаления legacy URL/evidence.
- **ADD-CUTOVER-07 — Aggregate scope не выключается случайно.** Event-detail D10
  не останавливает month/weekend/festival Telegraph pages; их отдельный switch
  разрешён только после static parity и собственного acceptance pack.

### Mobile Popular density comparison

- **ADD-LISTING-05 — Compact packing without scale regression.** На
  `360/390px` больше половины ranked events участвуют в парных рядах, но media
  height побайтно/геометрически совпадает с V23. Разрешены только mobile
  Popular gutter/gap corrections; OCR crop, rank reorder и masonry запрещены.
- **ADD-LISTING-06 — Pinch mirrors the visible density control.** Только
  `/populyarnoe/` запрещает browser zoom; pinch-in включает `Компактно`,
  pinch-out — `Крупно`, изменяя те же `aria-checked/hidden/inert` состояния.
  Один жест срабатывает один раз, single-touch scroll не отменяется, dock
  остаётся доступным для keyboard/screen-reader пользователей.
- **ADD-LISTING-07 — Compact pictogram truth.** Пиктограмма `Компактно`
  содержит ровно две контурные плитки и не меняет доступное текстовое имя
  кнопки.
- **ADD-LISTING-01 — Exact canonical large card.** На `/populyarnoe/` при
  ширине до `720px` режим `Крупно` рендерит общий `EventCard.astro` с
  `split-actions`, совпадающий с `Смотрите дальше`; отдельный
  `listing-proof`/копия DOM запрещены.
- **ADD-LISTING-02 — Ordered adaptive compact flow.** `Компактно` сохраняет тот
  же дедуплицированный порядок event IDs, использует существующий
  `ListingEventCard`, одинаковую media height и последовательный flex-wrap без
  masonry/order: на реальной выборке есть и парный ряд, и singleton, без
  horizontal overflow и нового crop правила для OCR.
- **ADD-LISTING-03 — Full-width accessible dock.** На `360/390/430px` нижний
  переключатель закреплён в `bottom:0`, занимает всю ширину viewport, имеет две
  цели минимум 48px и safe-area/padding compensation. Неактивное представление
  одновременно `hidden` и `inert`, выбранное состояние и ближайший event anchor
  сохраняются. При переключении в обе стороны остаётся тот же просматриваемый
  `event_id` и его верхняя координата с допуском 1px; контрольный пример —
  `4689`, `Фестиваль добровольчества #МЫВМЕСТЕ`. Щипок над карточкой обязан
  предпочесть именно затронутый `event_id`.
- **ADD-LISTING-04 — Breakpoint and desktop non-regression.** На `720px` видны
  только mobile family и dock, на `721px` — только desktop family без dock. На
  `1366/1536/1920px` геометрия десктопного Popular совпадает с V22 при одном
  snapshot; режимы телефона не влияют на фильтры и их счётчики.
- **ADD-LISTING-08 — Mobile chrome uses one navigation hierarchy.** На
  `360/390/430px` обычная `.site-nav` скрыта только для mobile Popular, а общий
  `.mobile-discovery-menu` остаётся доступен. Четыре listing routes занимают
  один ряд высотой 44px без стрелок, cities-only rail — один статический ряд
  высотой 48px без пустого второго уровня и overlap с мобильной биркой.
- **ADD-LISTING-09 — Popular group hierarchy is visible.** В обоих мобильных
  режимах заголовок каждой категории имеет минимум `24px/900`, спокойный счётчик
  и явный смысловой разрыв перед следующей группой. После ухода исходного заголовка
  он становится компактной sticky-меткой справа, а не полноширинным слоем chrome.
- **ADD-LISTING-10 — Compact evidence does not defeat scan density.** В compact
  режиме внешняя evidence-ось занимает 28px только для ненулевых social proof и
  44px при наличии медальона. Медальоны 40px непрозрачны, безопасный overlay
  остаётся внутри wide non-OCR. На `390px` paired share не хуже V24, media
  height/ratio неизменны, а полный документ не становится выше универсального
  under-photo прототипа.
- **ADD-LISTING-11 — Mobile sticky group context.** На `360/390/430px` текущий
  подзаголовок после прилипания занимает не более `min(100vw - 156px, 288px)`, прижат
  вправо, не перекрывает ручку mobile drawer и не перехватывает pointer events. Следующая
  группа нативно выталкивает предыдущую без double-label frame; переключение density
  сохраняет и event anchor, и активную категорию. Исходящий header не сбрасывает
  compact-стиль по приближённому bottom-threshold, а уходит только за нативной границей секции.
  На `721px+` sticky-контракт не применяется.

### UI, accessibility и browser matrix

- **ADD-HOME-HERO-01 — Current deterministic deck.** Главный Hero Talk при
  одном snapshot/build id повторяет event IDs и режимы после reload, не
  содержит inactive/past occurrence даже при максимальном engagement, не
  повторяет взаимную explicit family и не делает больше двух text-only или
  одной mosaic подряд.
- **ADD-HOME-HERO-02 — Automatic but respectful motion.** На `1366×768` и
  `1440×900` первая сцена запускается сама, затем появляется следующий
  уникальный event; Play/Next/Replay/pace/progress/Lab controls отсутствуют.
  Hidden tab и hero вне viewport останавливают цикл, возврат продолжает его;
  reduced motion и no-JS сохраняют полезную первую ссылку без движения.
- **ADD-DATE-HERO-01 — Exact safe linked hero.** На `/segodnya/`,
  `/zavtra/` и `/date-YYYY-MM-DD/` selector использует только active occurrence
  выбранной даты с fresh `visual_only` cover-safe geometry. OCR/unknown,
  соседняя дата или отсутствие safe photo дают обычную страницу без hero.
  `/vyhodnye/` hero не получает.
- **ADD-DATE-HERO-02 — Reversible 66 tiles.** На `320×667`, `390×844` и
  `430×932` hero содержит одну настоящую внутреннюю ссылку и 66 decorative
  tiles. Scroll вниз монотонно уменьшает alpha, scroll вверх восстанавливает те
  же значения; height-only resize и reload не меняют schedule. При `scrollY=80`
  Y-transform равен `22.4±1px`, до первой карточки hero исчезает, horizontal
  overflow отсутствует.
- **ADD-FOCUS-SCORE-01 — Page-type 24-hour continuity.** Подтверждённая
  оценка восстанавливается после перехода на другую страницу того же типа для
  того же auth user, calendar объединяет today/tomorrow/date, другой тип и
  другой user её не наследуют. До истечения 24 часов число можно заменить;
  после TTL выбор исчезает. Локальное состояние меняется только после успешного
  RPC.
- **ADD-UI-01 — Единая навигационная модель.** Mobile tag/menu и desktop header сохраняют одинаковые destinations, labels, порядок, active state, избранное и identity semantics.
- **ADD-UI-02 — Граничные viewport.** Помимо обычных mobile/desktop проверить узкий телефон, landscape, tablet и ширины непосредственно до/после breakpoint; исключить horizontal overflow и скачки layout.
- **ADD-A11Y-01 — Keyboard/focus.** Все ссылки, меню, карусель, modal/gallery, like/share/calendar, email form и undo доступны с клавиатуры; focus видим, не заперт и возвращается к trigger.
- **ADD-A11Y-02 — Screen reader semantics.** У controls корректные accessible names/state/live announcements; карточка не создаёт конфликтующих вложенных ссылок/кнопок, возраст и альтернативные даты читаются понятно.
- **ADD-A11Y-03 — Reduced motion.** `prefers-reduced-motion` отключает parallax/автодвижение без потери контента или управления; autoplay не мешает чтению.
- **ADD-A11Y-04 — No-JS.** Основные страницы, факты, ссылки, похожий static fallback, navigation и SEO-content остаются доступными без JS; enhancement не оставляет пустые обязательные блоки.
- **ADD-A11Y-05 — Gallery targets.** Hover/focus над закрытым desktop hero
  включает Left/Right; fullscreen CTA pointer/touch переходит по ссылке, а не
  dismiss. Reduced motion запрещает timed advance, inactive slide не tabbable.
- **ADD-UI-03 — Длинный и экстремальный контент.** Проверить длинные названия, площадки/адреса, несколько медальонов, отсутствие цены, пять возрастных вариантов, много дат и 1/10+ изображений.
- **ADD-UI-04 — Browser/device matrix.** Финальный RC проверить минимум в актуальных Chrome/Edge/Firefox на Windows, Safari/Chrome/Firefox на macOS и Safari iOS/Chrome Android; native share/clipboard/calendar проверять на реальных ОС, не только Playwright mocks.
- **ADD-UI-05 — CTA по фактической геометрии компонента.** Desktop action panel
  измеряет собственную inline-композицию после рендера и при изменении ширины
  или текста; если цена/основной CTA/calendar/share/like не помещаются, panel
  переходит в stacked layout до появления horizontal overflow. Решение не
  привязано только к viewport breakpoint.

### SEO/GEO, безопасность и наблюдаемость

- **ADD-SEO-01 — Canonical/robots/sitemap.** Каждая публичная page family имеет корректные status, canonical, indexability и sitemap membership; preview, secret и personal URLs не индексируются.
- **ADD-SEO-02 — Structured-visible parity.** JSON-LD не расходится с видимыми title/date/location/status/age/ticket facts и не сохраняет устаревший occurrence после rebuild.
- **ADD-SEO-03 — Share/search preview.** Open Graph и остальные preview metadata ведут на canonical страницу, используют доступное CDN-изображение и не содержат персональные/preview URL.
- **ADD-SEC-01 — XSS и URL safety.** Source title/description/address/comment facts/search input и generated tags не могут внедрить script/HTML; внешние URL используют допустимые схемы и безопасные атрибуты.
- **ADD-SEC-02 — Auth/admin boundaries.** Обычная авторизация, знание event id или `noindex` не открывают admin report/history; повторный report защищён от двойного запуска и показывает реальный статус/результат нескольких incidents.
- **ADD-OBS-01 — Ошибки видимы и коррелируются.** Build/search/share/email/transport/personalization failure оставляет run/request correlation evidence без PII; UI не сообщает успех, если backend действие не принято.
- **ADD-OBS-02 — Чистая клиентская сессия.** На каждой page family нет неожиданных console errors, unhandled rejections, failed first-party requests, hydration mismatch и повторяющегося telemetry spam.
- **ADD-OBS-03 — Static build report.** Redacted 24h report различает outcomes,
  channel pointers, page/object/byte counts и history ↔ ledger orphans без bearer.
- **ADD-SEO-04 — Past-event lifecycle.** Полезный past URL остаётся 200 с
  archive label; real move — 301/308, permanent removal — 404/410; noindex/test
  URLs отсутствуют в sitemap и удаляются только manifest-bound cleanup.

## Правило исполнения

- `USR-*` — исходные сценарии пользователя; их нельзя тихо удалить, переименовать по смыслу или закрыть только unit-тестом.
- `ADD-*` — дополнительные сценарии аудита; при переносе в Gherkin/Playwright идентификатор сохраняется в названии scenario/test и в release evidence.
- Для каждого применимого сценария фиксируются URL/page family, viewport/device/OS/browser, test identity/persona, catalog/build SHA, входное состояние, ожидаемый результат и фактическое evidence.
- Автоматизация не заменяет native/manual evidence там, где проверяется системный share, clipboard/paste, email client, calendar/maps handoff или реальное визуальное качество.
