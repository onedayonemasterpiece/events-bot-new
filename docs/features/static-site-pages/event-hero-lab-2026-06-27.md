# Event hero lab — 2026-06-27

## 2026-07-14 approved mobile brand-handle update

The drawer behavior below remains unchanged, but its visual lockup is now governed by the static-site design system:

- handle `128×96px` plus safe-area top, solid `#98401f`, `0 0 12px 12px` radius;
- shared outlined `Анонсы` wordmark at `104px` width;
- umbrella endorsement intentionally split into `ПОЛЮБИТЬ / КАЛИНИНГРАД` at a subordinate `8/9px`, weight `600`, opacity `0.9`;
- static content lockup with no periodic title sway;
- lighter contextual shadow because the handle overlays event photography;
- the native `<details>` monolithic drawer, top attachment, no chevron and plain full-width navigation rail are preserved.

This is an optical mobile adaptation, not a proportional reduction of the approved `240×88px` desktop tag. Canonical geometry and prohibited changes: [`design-system/brand-lockups.md`](design-system/brand-lockups.md). Visual QA: `/lab/design-system/` plus a real event page at `320/360/390/430px`.

## Задача

После визуального ревью v18/v19 mobile hero был признан недостаточно сильным: v19 стал технически безопаснее для изображений, но всё ещё выглядел как “картинка + текстовый блок”, а не как эмоциональная обложка события. Поэтому v20 разделил два уровня, а v21 усилил именно первый mobile экран:

1. **image policy** — как безопасно показать исходную картинку;
2. **hero composition** — как устроен первый экран события: размер картинки, связь H1 с афишей, CTA, header/breadcrumbs и возможная motion-динамика.

## Внешнее ревью и текущая позиция

- v19 accepted only as **asset-policy foundation**, not as final hero.
- OCR/unknown posters must not be cropped and must not receive H1 overlay over poster text.
- `visual_only` photos can use `object-fit: cover`.
- A slight attached/overlap card is allowed when it overlaps the safe slab/lower edge, not the meaningful poster text.
- Mobile breadcrumbs must not appear above the hero visual.
- Primary CTA must be visually dominant; calendar/share are secondary actions.
- No `100vh`, no blur/backdrop/double image fill, no runtime OCR/ML.

Historic consultant artifacts for v19 are stored under `artifacts/codex/hero-consultation-v19/`. The user-supplied v20 critique is treated as the corrective brief: build a **composition lab**, not another image-mode lab.




## v28 implemented hero gallery and flat drawer rail

- The mobile drawer panel is now a flat full-width rail: `border-radius: 0`, plain text links, no pill/button styling inside the drawer. The terracotta brand tag remains the handle and moves down with the rail.
- Tapping/clicking the hero visual opens a fixed fullscreen `hero-gallery` moved to `document.body` at open time so it is not clipped by the hero card. The first slide preserves the source image with `object-fit: contain`; the final slide is a clearly labeled CTA to a similar event (`Смотреть похожее`).
- Manual controls are supported: next/prev buttons, swipe and `Esc` close. There is still no default hero autoplay and no pull-to-expand overscroll behavior; those stay future experiments because they need multi-image fixtures, reduced-motion controls and mobile pull-to-refresh conflict testing.
- Playwright evidence for `preview-20260627-event-pages-v28` is under `artifacts/codex/event-pages-v28/`: hero 100vw bbox, fullscreen gallery open/CTA, flat drawer CSS, local-only like flow and same-browser related-card reflection.

## v28 drawer mechanics, parallax hardening and gallery policy

v28 keeps the corrected v24/v26 drawer mechanics: the terracotta service tag is the **handle of the drawer**, not an independent button above content. Closed state shows only the handle; open state slides a real 100vw navigation rail down from the top and moves the handle down by the same rail height. The handle has no chevron/up/down icon and is clamped to `y>=0` so the touch target is not clipped above the viewport. On mobile, the large after-hero logo-button header is removed; when the user is past the hero, the closed handle is hidden so it does not cover the feed/content. `body.is-past-hero` remains the state contract for sticky CTA/header/drawer chrome.

Parallax remains allowed only for `visual_only` photo-cover hero compositions. The transform uses constant scale and vertical translate only; no class threshold may change scale or switch contain/cover while scrolling. v28 uses stable `svh` sizing for mobile photo heroes to avoid dynamic browser chrome resize jumps after scroll. `prefers-reduced-motion` disables the transform.

Multi-image events are P1, but the product contract is fixed now:

- first paint uses one deterministic best hero image, not autoplay;
- tap/click on hero can open a manual fullscreen gallery with swipe/buttons;
- the last gallery slide may be an explicit CTA card, not a disguised photo;
- autoplay is not default and, if later enabled inside the gallery, requires pause/stop and reduced-motion handling;
- all carousel/gallery slides must keep a stable frame height to avoid CLS; OCR/poster slides use contain/no-crop, visual-only slides may cover only with focal/face-aware metadata.

Smart Update/image-preparation requirement: when OCR/media enrichment runs, it must emit `ocr_boxes`, `face_boxes`, `saliency_boxes`, `focal_point`, `recommended_object_position`, `recommended_hero_fit` and `safe_crop`/crop guards. Face boxes are normalized 0..1 rectangles; cover crops must not cut the top of detected faces. If safe crop is impossible, the renderer must fall back to contain/natural presentation.

External review status for v26/v27: Gemini Pro consultation was attempted only on approved models (`gemini-3.1-pro-preview`, `gemini-3-pro-preview`) and was blocked by free-tier quota `limit: 0` / HTTP `429 RESOURCE_EXHAUSTED`; Flash/Lite/Gemma were not substituted. The same brief was sent to `a-opus` as the allowed fallback consultant. Opus accepted `split-actions` as the correct feed-card baseline and flagged follow-up hardening: drawer handle must not be clipped (`y>=0`, fixed in v26), JSON-LD `validFrom` must be ISO 8601 with timezone (fixed in v26), `Не интересно` should move toward an undo pattern, and multi-image hero should prioritize tap-to-fullscreen gallery before any autoplay.

## v24 sliding discovery row correction

The v23 top sheet was rejected visually because it behaved like a floating popover/card. v24 keeps the useful brand tag handle but changes the interaction model: opening the tag reveals a **full-width additional navigation row sliding down from the top edge**. This is the intended product direction for a future richer menu with `Сегодня`, `Выходные` and later real category/collection routes.

Implementation contract:

- still no-JS-first: the handle remains a native `<details>`/`<summary>` control and links are crawlable HTML anchors;
- the revealed area is `position: fixed` + `width: 100vw` with a top-row/tray treatment, not a centered/floating card;
- motion uses only `transform`/`opacity` (`translate3d(0, -112%, 0)` → `translate3d(0, 0, 0)`) with `@starting-style`, so the row visually slides from above;
- the terracotta tag stays above the row as the visual handle; the row itself uses the site warm surface palette;
- only real generated destinations are shown for now: `Сегодня`, `Выходные`, `Все анонсы`; no dead category chips until corresponding static routes exist.

## v23 mobile discovery top sheet and motion correction

The v23 pass turns the hero brand tag into a no-JS-first mobile discovery top sheet instead of a decorative-only label. The component is a native `<details data-mobile-discovery-menu>`:

- closed state: the same terracotta service tag over the hero;
- open state: a top sheet over the hero with real generated links only: `Сегодня`, `Выходные`, `Все анонсы`;
- no dead category chips are rendered until the corresponding static routes exist;
- JS enhancement only closes on Escape, outside click and link click; navigation remains available without JS;
- preview/debug links are not mixed into this user-facing top sheet.

The v23 pass also removes two visual artifacts from the hero area:

- the decorative `brand::after` stripe is gone; the tag uses only shadow/elevation;
- `body.hero-chrome-immersive::before` is disabled on mobile to remove the 1px fixed grid/top stripe over the hero.

Parallax policy is clarified:

- OCR/unknown poster heroes do **not** get parallax, because the poster must stay fully readable and un-cropped;
- verified `visual_only` photo-cover heroes (`photo-cinematic-sheet` and `photo-parallax-sheet`) get parallax;
- dynamic zoom was removed because it produced a visible scale jump at the end of movement. The remaining scale is constant (`scale(1.14)`) only to reserve safe edges, while the visible motion is a stronger vertical offset (`±64px`).

## v22 edge-to-edge / brand-color correction

После проверки v21 отдельно зафиксировано: TASS был только референсом формы бирки, а не палитры, и hero image должен быть защищён от любых layout gutters. Поэтому v22 меняет brand tag на палитру сайта (`#793014` → `#a54821`) и делает mobile hero container/article full-viewport, а visual/image центрируется через `left: 50% + translateX(-50%)` с `width/min-width: 100vw`. Acceptance теперь проверяет именно bbox hero image `x=0,width=viewport`, а не только наличие CSS `100vw`.

## v21 corrective pass

Дополнительное ревью v20 зафиксировало: “сдвиг есть, но это ещё не hero”. В v21 внесены три обязательных корректировки:

1. **Actual 100vw poster image on mobile.** Для `poster-billboard` / `poster-attached-card` на mobile 100% ширины viewport получает не только dark visual slab, а сама hero-картинка: `width: 100vw`, `max-width: none`, без боковых gutter/padding. OCR/unknown poster по-прежнему не crop-ится.
2. **TASS-like brand tag.** Пока hero видим, обычная шапка заменена на компактную синюю “бирку” `Полюбить Калининград / Анонсы`, торчащую с верхнего края. Она оставляет hero первым визуальным объектом, но сохраняет название сервиса. После ухода hero включается полноценная fixed-шапка с навигацией.
3. **Stronger premium parallax lab.** `photo-parallax-sheet` остаётся экспериментом, но теперь использует заметный, контролируемый transform: медленный vertical offset плюс scale/zoom через `--hero-parallax-y` и `--hero-parallax-scale`; `prefers-reduced-motion` продолжает выключать motion.

Методология ревью также исправлена: `/lab/hero/review/` теперь показывает **same-event comparison**, то есть несколько композиций для одного и того же события, а не разные события под разными вариантами.

## Deterministic image policy

`EventHero.astro` chooses media mode from build/export metadata, without runtime OCR/ML:

| `image_text_mode` / данные | Hero mode | Политика |
| --- | --- | --- |
| `ocr_text` | `poster-stage` | Full poster is visible; `object-fit: contain` is allowed only inside the hero solid slab. H1/CTA stay as HTML, never overlay poster text. |
| `unknown` | `poster-stage` | Safe default: do not crop unknown images. |
| `visual_only` | `photo-cover` | `object-fit: cover` is allowed in a reserved visual area. H1 still stays in HTML sheet, not as image text. |
| no image | `fallback-art` | Branded typographic fallback with the same decision block. |

Important: `poster-stage` is a hero-only exception. Discovery cards/listings keep the OCR-safe policy from v15: `ocr_text`/`unknown` render in natural ratio without fixed contain-frame; `visual_only` uses vertical `4:5` cover.

## Hero composition variants in v20/v21

Hero composition is explicit via `data-hero-composition` and is independent from `data-hero-mode`:

| Composition | Intended use | Product idea |
| --- | --- | --- |
| `poster-billboard` | Default candidate for OCR/unknown posters | The first mobile screen starts with a 100vw hero slab. The poster is full and uncut; the HTML decision sheet is attached below with a small safe overlap. |
| `poster-attached-card` | Bolder poster experiment | Stronger attached card/stripe while still not covering meaningful poster text. Useful to compare against the safer billboard. |
| `photo-cinematic-sheet` | `visual_only` images/photos | 100vw cover image first, then a strong title/CTA sheet partly attached to the lower edge. |
| `photo-parallax-sheet` | visual-only experiment | Same as cinematic, plus subtle JS parallax after hydration; disabled by `prefers-reduced-motion`. |
| `compact-ticketing` | bad/no images, very long H1, very small screens | Stable fallback with less spectacle and more transaction clarity. |

Default event mapping in the current preview:

- `5878` «Песни СССР» → `poster-billboard`;
- `4913` «Пионеры советской археологической науки…» → `poster-attached-card`;
- `6322` «День валяния в сене» → `photo-cinematic-sheet`;
- `5370` / `4512` visual-only controls → `photo-parallax-sheet`;
- events without image → `compact-ticketing`.

## Mobile header and breadcrumbs

For event pages v28 keeps `heroChrome="immersive"` and uses the menu-enabled drawer handle:

- while the hero is visible on mobile, the full site header is replaced by the compact top brand tag; it is fixed over the image, but no longer reserves vertical space before the hero;
- after the hero leaves the viewport, JS toggles `body.is-past-hero` and reveals the full fixed header with navigation;
- breadcrumbs are rendered **after** the hero in HTML and hidden on mobile, while JSON-LD BreadcrumbList remains present for SEO/GEO;
- the event type/city/status are represented inside the hero eyebrow/facts instead of a breadcrumb row above the image.

This is a preview hypothesis. The brand tag can still be tuned for exact size/placement if it hides meaningful poster text too aggressively, but the core rule is fixed: mobile event pages start with a full-width image-led hero, not with a standard header block.

## CTA hierarchy

Mobile order inside the decision sheet:

1. status eyebrow;
2. `H1`;
3. primary CTA as a dominant full-width action;
4. calendar/share as secondary icon-text actions;
5. date/place facts;
6. summary is hidden from the first mobile decision area, because the full description remains visible below the hero.

CTA/calendar/share remain ordinary HTML links/buttons and work without personalization/Supabase. Calendar is still only exposed for eligible short events. The mobile sticky CTA is suppressed while the hero is visible, so the first screen does not duplicate the primary hero action, and is also hidden again when the discovery feed is reached.

## Hero lab and review routes

Preview routes:

- `/lab/hero/` — composition lab: all real fixture events rendered under all composition variants, with headings demoted to `h3` so the lab page has one `H1`.
- `/lab/hero/review/` — public noindex review route with live 390×844 iframe frames grouped by event. It compares several compositions on the same event: `5878 × {poster-billboard, poster-attached-card, compact-ticketing}`, `6322 × {photo-cinematic-sheet, photo-parallax-sheet, compact-ticketing}`, and `4913 × {photo-cinematic-sheet, poster-attached-card}`.
- `/lab/hero/review/<case>/` — individual full-page review cases used by those iframes.

Public v28 URLs:

- Preview index: <https://kenigevents.ru/preview-20260627-event-pages-v28/__preview/>
- Hero composition lab: <https://kenigevents.ru/preview-20260627-event-pages-v28/lab/hero/>
- Same-event viewport review: <https://kenigevents.ru/preview-20260627-event-pages-v28/lab/hero/review/>
- Poster Billboard control: <https://kenigevents.ru/preview-20260627-event-pages-v28/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Photo Parallax review case: <https://kenigevents.ru/preview-20260627-event-pages-v28/lab/hero/review/6322-photo-parallax-sheet/>

## Acceptance checks

`site/scripts/check-preview.mjs` verifies:

- `lab/hero/index.html` and `lab/hero/review/index.html` exist and are in sitemap;
- representative same-event review cases exist and are in sitemap;
- control event `5878` renders `data-hero-mode="poster-stage"`, `data-hero-composition="poster-billboard"`, and `data-hero-image-text-mode="ocr_text"`;
- event pages include the mobile discovery drawer/handle and stable `is-past-hero` state contract;
- every event page has exactly one visible `H1` and a composition marker;
- event hero is rendered before after-hero breadcrumbs in HTML;
- visual-only events render `photo-cover`; OCR/unknown events render `poster-stage`;
- no `blur(`, duplicate/backdrop poster fill, repeated `--poster-image`, `media-backdrop`, `image-backdrop`, old `3:4` ratio, or fragile `100vh` leaks into CSS/HTML;
- `poster-stage` hero has `object-fit: contain`, and `photo-cover` hero has `object-fit: cover`;
- poster billboard visual **and image itself** are full viewport width on mobile;
- visual-only cinematic/parallax heroes have a reduced-motion-aware parallax hydrator with constant scale, stronger vertical offset and no end-of-hero scale snap;
- split-actions under-card share/like remain transparent icon-style controls, not pill buttons.

Playwright smoke evidence for v28 is stored under `artifacts/codex/event-pages-v28/` and checks: mobile hero visual bbox `x=0,width=viewport`, fullscreen hero gallery open/next/CTA/close, flat no-radius drawer rail with plain text links, current-event local like count/action, same-browser related-card like reflection, no server write request during static-preview like flow, and the earlier footer/split-actions/parallax gates through `check:preview`.
