# R14-VISUAL results

## Scope delivered

- **R02:** OCR/unknown `poster-stage` event heroes retain authored contain geometry on mobile; the production override no longer clips or parallax-translates those images.
- **R04:** mobile listing rails render a bounded gallery of up to four real source assets, selected asset first, with independent OCR/document crop protection and focal positions. Temporal muting now covers every media cell.
- **R07:** the Clubs route has a mobile-only sticky section shelf below the mobile header; desktop remains unchanged.
- **R09 visual lane:** added the pure 1080×1350 `eventShareImage` composer (photo, actual announcements wordmark bitmap, title, date, place, admission) and exposed media semantics/brand URLs on every authored native-share control. Composer is enabled only for `visual_only`; OCR and unknown fail closed.

## Verification

- `node --test tests/event-share-image.test.mjs tests/mobile-listing-rail-media.test.mjs tests/mobile-listing-rails.test.mjs tests/mobile-event-chrome-contract.test.mjs tests/interest-club-catalog.test.mjs` — **31 passed** (including the separate wiring suite in the final combined run).
- `npm run build` — **passed**, 429 pages built. (The subsequent shelf `top` token correction is CSS-only and covered by the static contract.)
- `node --test tests/interest-club-catalog.browser.test.mjs` against the default build — **not a valid clubs acceptance run**: that build had `PUBLIC_INTEREST_CLUBS_ENABLED` disabled and therefore contained 0 cards; the test requires a clubs-enabled build. A Playwright geometry probe exposed a 7px shelf/header offset, which was corrected to use `--ke-site-header-bar-height`.

## Required EventLayout integration hunk (not edited: lane boundary)

`site/src/layouts/EventLayout.astro` remains the owner of the actual share click handler and hydrated continuation cloning. During integration:

1. Import/bundle `composeEventShareImage` from `src/lib/eventShareImage.mjs` in the client share runtime.
2. Read `button.dataset.shareImageTextMode` and `button.dataset.shareBrandImage`. Only when the mode is `visual_only`, fetch the source image blob, call the composer with the button title/date/place/admission fields, and share the resulting PNG file. Do **not** compose/crop OCR or unknown media; retain their fail-closed/original-image behavior.
3. In the hydrated continuation share-button assignment block (currently around the `data.share_*` assignments near lines 4246–4258), also assign:
   - `shareButton.dataset.shareImageTextMode = data.image_text_mode || 'unknown'`
   - `shareButton.dataset.shareBrandImage` from the server-authored/base-aware brand value (or the same `withBase('/brand/announcements-wordmark-ui.svg')` contract).
4. Preserve the existing share-count commit/fallback behavior and call order; composition belongs immediately after a successful source-image fetch and before `navigator.share({ files })`.

## Notes / risks

- The rail gallery intentionally caps at four cells despite the helper accepting a hard maximum of six; the default UI call uses four.
- The composer depends on Canvas + `createImageBitmap`; integration must retain the existing fallback when either API or file sharing is unavailable.
