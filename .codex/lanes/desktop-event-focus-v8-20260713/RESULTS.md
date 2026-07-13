# Desktop event focus v8 — lane results

## L01 media mapper — completed

- Event `5077` has two decoded `955×1280` near-identical renditions (dHash distance `2`), so a second compact-rail item would advertise no new semantic image.
- Event `6550` has three decoded `1357×1919/1920` near-identical renditions (pair distances `5/1/4`).
- Event `5761` has twelve genuinely distinct images; gallery order is `[5,0,1,2,3,4,6,7,8,9,10,11]` when source index `5` is the hero.
- Safe future identity contract: upstream `canonical_media_id` plus optional `duplicate_of_source_order`; do not perceptually merge uncertain images in the client.

## L02 design research — completed

- At the v7 FHD/125% equivalent viewport (`1536×864`), the pinned photo cropped a face/top edge, Continuous still used cover, the OCR companion poster was only `92×118`, equal-flex Split rails could stretch one item to `743px`, and the low-resolution split left about `126px` of flat background.
- `cover` was retained for ordinary visual media; OCR/document media requires full `contain` unless a source-grounded safe crop exists.
- Compared related-card directions: aggressive cover, fixed ambient contain, and adaptive aspect buckets with visual cover plus OCR ambient contain.
- Gemini 3.1 Pro (High) selected top-safe pinned media, photo rail before CTA, larger OCR companion, bounded low-resolution cover and the adaptive hybrid. It rejected blind OCR crop and flat graphite fields.

## L03 integration — implementation and local acceptance completed

- Implemented all v8 lab contracts in the shared noindex desktop component and scenarios.
- Static build: `443` pages under `preview-20260713t-desktop-focus-v8`.
- `npm run check:preview`: passed.
- Final-dist Playwright at `1536×864` and `1440×900`: zero console errors; pinned image top-safe; continuous exact ratio; Split exact half width; low-resolution remainder `0`; duplicate-only rails absent; real rail `124.78px` high with aspect buckets; OCR companion poster `192×267.83`; gallery requested/active index `3/3`; per-row related-card heights normalized.
- Photo Editorial order is rail `1` then CTA `2`; OCR Editorial order is CTA `1`, companion `2`, rail `3`.
- At `390×844`, the desktop lab root is absent and horizontal overflow is `0`.
- Public preview, public Playwright and final Gemini browser acceptance remain the deployment closure steps.

## L04 closure review — running

Checklist reviewer: `019f5bf5-9b01-7973-b957-e198dc67228d` (`Meitner`).
