# Event hero lab — 2026-06-27

## Задача

После визуального ревью v18/v19 mobile hero был признан недостаточно сильным: v19 стал технически безопаснее для изображений, но всё ещё выглядел как “картинка + текстовый блок”, а не как эмоциональная обложка события. Поэтому v20 разделяет два уровня:

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

## Deterministic image policy

`EventHero.astro` chooses media mode from build/export metadata, without runtime OCR/ML:

| `image_text_mode` / данные | Hero mode | Политика |
| --- | --- | --- |
| `ocr_text` | `poster-stage` | Full poster is visible; `object-fit: contain` is allowed only inside the hero solid slab. H1/CTA stay as HTML, never overlay poster text. |
| `unknown` | `poster-stage` | Safe default: do not crop unknown images. |
| `visual_only` | `photo-cover` | `object-fit: cover` is allowed in a reserved visual area. H1 still stays in HTML sheet, not as image text. |
| no image | `fallback-art` | Branded typographic fallback with the same decision block. |

Important: `poster-stage` is a hero-only exception. Discovery cards/listings keep the OCR-safe policy from v15: `ocr_text`/`unknown` render in natural ratio without fixed contain-frame; `visual_only` uses vertical `4:5` cover.

## Hero composition variants in v20

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

For event pages v20 uses `heroChrome="immersive"`:

- on mobile the site header is an overlay over the top of the hero visual, so it does not consume the first screen before the image;
- breadcrumbs are rendered **after** the hero in HTML and hidden on mobile, while JSON-LD BreadcrumbList remains present for SEO/GEO;
- the event type/city/status are represented inside the hero eyebrow/facts instead of a breadcrumb row above the image.

This is a preview hypothesis. If the overlay header feels too heavy, the next alternative is “header below hero, sticky after scroll”, but that should be tested as a separate composition rather than mixed into the default.

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
- `/lab/hero/review/` — public noindex review route with live 390×844 iframe frames for the main event-page variants. This replaces private-only screenshot evidence for external reviewers.

Public v20 URLs:

- Preview index: <https://kenigevents.ru/preview-20260627-event-pages-v20/__preview/>
- Hero composition lab: <https://kenigevents.ru/preview-20260627-event-pages-v20/lab/hero/>
- Viewport review: <https://kenigevents.ru/preview-20260627-event-pages-v20/lab/hero/review/>
- Poster Billboard control: <https://kenigevents.ru/preview-20260627-event-pages-v20/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Photo Cinematic Sheet control: <https://kenigevents.ru/preview-20260627-event-pages-v20/sobytiya/den-valyaniya-v-sene-romanovo-6322/>

## Acceptance checks

`site/scripts/check-preview.mjs` verifies:

- `lab/hero/index.html` and `lab/hero/review/index.html` exist and are in sitemap;
- control event `5878` renders `data-hero-mode="poster-stage"`, `data-hero-composition="poster-billboard"`, and `data-hero-image-text-mode="ocr_text"`;
- every event page has exactly one visible `H1` and a composition marker;
- event hero is rendered before after-hero breadcrumbs in HTML;
- visual-only events render `photo-cover`; OCR/unknown events render `poster-stage`;
- no `blur(`, duplicate/backdrop poster fill, repeated `--poster-image`, `media-backdrop`, `image-backdrop`, old `3:4` ratio, or fragile `100vh` leaks into CSS/HTML;
- `poster-stage` hero has `object-fit: contain`, and `photo-cover` hero has `object-fit: cover`;
- poster billboard visual is full viewport width on mobile;
- parallax experiment is present with reduced-motion guard;
- split-actions under-card share/like remain transparent icon-style controls, not pill buttons.

Playwright smoke evidence for v20 is stored under `artifacts/codex/hero-impact-v20/` and includes 375×667, 390×844 and 430×932 checks for hero visual share of the first screen, H1/CTA visibility, no horizontal overflow, full-bleed visual width, poster `contain`, photo `cover`, parallax transform change, lab/review route integrity and no accidental H1 overlay on OCR posters.
