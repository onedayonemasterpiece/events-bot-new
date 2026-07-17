---
name: static-listing-visual-lab
description: Use in events-bot-new when exploring, comparing, visually explaining, or accepting KenigEvents static-site list/card concepts—Главная, Для вас, Популярное, date/category/search lists—especially when real event media, OCR posters, mixed image sets, smart crop or text overlays, agy/Gemini consensus, responsive prototypes, or Telegram review threads are involved.
---

# Static Listing Visual Lab

Build product decisions from production media evidence and inspectable real-image prototypes, not prose-only wireframes or placeholders.

## Non-negotiable workflow

1. **Freeze the decision.** State the surface, user job, ranking semantics, viewport, and success criterion. For personalized discovery keep the explicit target: a returning user reaches a genuinely relevant event within 20–30 cards.
2. **Read the existing contract.** Inspect current types/components/export code and accepted related-card evidence before inventing new crop/alignment behavior.
3. **Audit fresh production media first.** Use read-only Fly access through `fly-prod-db-access`. Count events and approved assets separately. Treat `event.photo_urls` mapped to `eventposter.review_status='approved'` as canonical; never use the raw poster table or the bounded preview fixture as the population.
4. **Build a stratified real corpus.** Include no image, low resolution, one OCR/unknown poster, one verified photo, portrait-only, landscape-only, square-only, mixed orientation, mixed OCR/photo, and all-OCR galleries. Cache media and a manifest under `artifacts/codex/<task>/`; never commit production media.
5. **Write a media decision matrix before layout.** Read [references/media-decision-matrix.md](references/media-decision-matrix.md). Separate asset selection, crop eligibility, target aspect, focal behavior, overlay eligibility, and fallback.
6. **Create competing responsive concepts on identical ranked data.** Prefer real HTML/CSS and browser screenshots over abstract boxes. Show desktop and mobile. Preserve ranking order; geometry must not reorder, skip, or stop a personalized/popular feed.
7. **Treat text-on-image as a distinct evidence-gated mode.** Prototype overlay and external-copy variants. Never infer overlay safety from `safe_crop`. Without a persisted safe region plus OCR/face/saliency clearance and contrast evidence, title/date stays outside the image.
8. **Run independent formation before synthesis.** Ask agy/Gemini Pro to create its own renderable variants, not just review Codex proposals. Use only project-approved Pro-class models. Preserve its artifacts and disagreements.
9. **Synthesize explicitly.** Compare concepts by new-user orientation, 20–30-card personalized hit rate, media selling power, scan density, rank integrity, OCR safety, harmony, responsive behavior, data readiness, and MVP cost.
10. **Run a fresh-context validation pass.** Give the validator screenshots/contact sheet, statistics, and decision matrix—not the desired verdict. Require pixel-level inspection and a fail/conditional/pass result. See [references/consultant-and-acceptance.md](references/consultant-and-acceptance.md).
11. **Publish only after authorization.** Use `telegram-human-session` for Telegram. Send the visual artifact before/with concise analysis, then read the topic back and retain a receipt. Do not claim publication from a send return alone.

## Required gates

- No layout conclusion before a fresh production inventory and real-image contact sheet.
- No “real prototype” containing placeholder or generated media.
- No smart-crop claim without role/text mode, dimensions, focal/safe-crop evidence, and pixel inspection.
- No overlay claim without a real overlay-safe region; default to external copy.
- No one-size crop: stabilize the outer media zone while adapting the inner treatment.
- No blurred/duplicated backdrop or ambient field masking to hide incompatible ratios.
- No geometry-driven reordering or dropped ranked items.
- No “consensus with agy” without independent formation, synthesis, and separate validation.
- No OpenAI image generation/editing without explicit user consent in the current turn.

## Reusable utilities

- `scripts/audit_event_media.py --db <sqlite> --output <json>`: reproduce the public-active, approved-media inventory from a local read-only snapshot.
- `scripts/build_contact_sheet.py --manifest <json> --output <png>`: produce a labeled real-image contact sheet.
- `scripts/capture_visual_board.py --url <url-or-html> --output-dir <dir>`: capture deterministic desktop/mobile screenshots when Python Playwright is installed.
- `assets/visual-board-shell.html`: copy as a lightweight starting frame; replace every sample/data slot with real task data.

Store raw queries, manifests, consultant output, screenshots, scorecards, and Telegram receipts in one task artifact directory. Never commit those artifacts.
