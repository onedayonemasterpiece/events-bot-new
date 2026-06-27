# Event hero lab — 2026-06-27

## Задача

После визуального ревью v18 mobile hero был признан слабым: страница начиналась как обычный текстовый документ, а афиша не работала как эмоциональный decision hero. При этом для событий нельзя просто делать красивый crop: часть картинок — текстовые афиши с OCR, где обрезка или overlay заголовка поверх постера ломают и UX, и доверие.

## Внешнее ревью

- Gemini Pro: `gemini-3.1-pro-preview`, артефакт локального прогона `artifacts/codex/hero-consultation-v19/gemini-3.1-pro-preview.md`.
- Opus через `a-opus`, полный артефакт `artifacts/codex/hero-consultation-v19/a-opus-full-review.md`.

Оба консультанта сошлись на MVP-упрощении: не делать `Magazine`/overlap/100vh-hero, оставить deterministic policy и проверить её на реальных 10 событиях. Gemini предлагал blur/backdrop для poster-stage, но это отклонено по пользовательскому требованию и по Opus-ревью: фон hero — только намеренная solid/tinted slab, без повторной картинки, blur, backdrop или повторяющихся краёв.

## Принятая deterministic policy

`EventHero.astro` выбирает режим только по build/export metadata, без runtime OCR/ML:

| `image_text_mode` / данные | Hero mode | Политика |
| --- | --- | --- |
| `ocr_text` | `poster-stage` | Полный постер виден целиком; `object-fit: contain` допустим **только внутри hero solid slab**, без duplicate/backdrop/blur; H1 и CTA идут ниже изображения в обычном HTML-потоке. |
| `unknown` | `poster-stage` | Safe default: не резать неизвестную картинку. |
| `visual_only` | `photo-cover` | Можно `object-fit: cover` в зарезервированной visual area; H1/CTA всё равно не накладываются на картинку. |
| нет изображения | `fallback-art` | Типографическая брендовая заглушка с тем же decision block. |

Важно: правило `poster-stage` — это исключение для **hero**. Для discovery cards/listings сохраняется прежняя OCR-safe политика: `ocr_text`/`unknown` рендерятся в натуральном соотношении без fixed contain-frame; `visual_only` использует вертикальный `4:5` cover.

## UX/information hierarchy

Mobile order in hero:

1. visual slab / cover / fallback;
2. status eyebrow;
3. `H1`;
4. primary CTA / calendar if eligible / share;
5. date/place facts;
6. short summary.

CTA поднят выше фактов, чтобы на 375–390px экран попадали H1 и primary action. Дата/место остаются рядом в том же decision hero, но не отодвигают primary action ниже первого решения.

## Split-actions under-card row

Для варианта B (`split-actions`) share/like под карточкой — это icon actions, а не «кругляшки»/pill buttons:

- визуально прозрачный фон и прозрачная граница в обычном состоянии;
- минимум 44×44px hit area сохраняется для доступности;
- иконка `Поделиться` остаётся доступной по `aria-label`, подпись визуально скрыта только в under-card row;
- лайк остаётся последним справа, в зоне правого большого пальца.

## Hero lab

Preview route: `/lab/hero/`.

В lab на одной странице рендерятся 10 реальных событий в принудительных режимах `poster-stage`, `photo-cover`, `fallback-art`. Внутри lab hero headings рендерятся как `h3`, чтобы страница сохраняла ровно один `H1`.

Public v19 URLs:

- Hero lab: <https://kenigevents.ru/preview-20260627-event-pages-v19/lab/hero/>
- Control OCR/poster hero: <https://kenigevents.ru/preview-20260627-event-pages-v19/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Visual-only/photo-cover hero + split-actions B: <https://kenigevents.ru/preview-20260627-event-pages-v19/sobytiya/den-valyaniya-v-sene-romanovo-6322/>

## Acceptance checks

`site/scripts/check-preview.mjs` now verifies:

- `lab/hero/index.html` exists and is in sitemap;
- control event `5878` renders `data-hero-mode="poster-stage"` and `data-hero-image-text-mode="ocr_text"`;
- every event page has exactly one visible `H1`;
- visual-only events render `photo-cover`; OCR/unknown events render `poster-stage`;
- no `blur(`, duplicate/backdrop poster fill, repeated `--poster-image`, `media-backdrop` or `image-backdrop` leaks into HTML/CSS;
- card OCR media still does not use `object-fit: contain` over a fixed frame;
- `poster-stage` hero has `object-fit: contain`, and `photo-cover` hero has `object-fit: cover`;
- split-actions under-card share/like are transparent icon-style controls, not pill buttons.

Playwright smoke evidence for v19 is stored locally under `artifacts/codex/hero-consultation-v19/`:

- `control-mobile-390.png`, `control-mobile-375.png`;
- `split-actions-mobile-390.png`;
- `hero-lab-desktop.png`;
- `hero-smoke-result.json` with bounding-box checks: one H1, poster/photo modes, H1 starts before 620px, primary CTA starts before 760px, split-actions controls remain transparent and at least 44px.
