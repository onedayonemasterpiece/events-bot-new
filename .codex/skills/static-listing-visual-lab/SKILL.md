---
name: static-listing-visual-lab
description: Use in events-bot-new when exploring, comparing, visually explaining, or accepting KenigEvents static-site list/card concepts—Главная, Для вас, Популярное, date/category/search lists—especially when real event media, OCR posters, mixed image sets, smart crop or text overlays, agy/Gemini consensus, responsive prototypes, or Telegram review threads are involved.
---

# Static Listing Visual Lab

Build product decisions from production media evidence and inspectable real-image prototypes, not prose-only wireframes or placeholders.

## Non-negotiable workflow

1. **Freeze the decision.** State the surface, user job, ranking semantics, viewport, and success criterion. For personalized discovery, interpret “find a genuinely relevant event within 20–30 cards” as a **journey-level cumulative exposure budget across pages, blocks, and surfaces**. Never translate it into 20–30 cards, 4–5 screens, or one long feed unless the user explicitly defines a single-surface test. Measure unique qualified card exposures and success-at-or-before touch 30.
2. **Read the existing contract.** Inspect current types/components/export code and accepted related-card evidence before inventing new crop/alignment behavior.
3. **Audit fresh production media first.** Use read-only Fly access through `fly-prod-db-access`. Count events and approved assets separately. Treat `event.photo_urls` mapped to `eventposter.review_status='approved'` as canonical; never use the raw poster table or the bounded preview fixture as the population.
4. **Build a stratified real corpus.** Include no image, low resolution, one OCR/unknown poster, one verified photo, portrait-only, landscape-only, square-only, mixed orientation, mixed OCR/photo, and all-OCR galleries. Cache media and a manifest under `artifacts/codex/<task>/`; never commit production media.
5. **Write a media decision matrix before layout.** Read [references/media-decision-matrix.md](references/media-decision-matrix.md). Separate asset selection, crop eligibility, target aspect, focal behavior, overlay eligibility, and fallback.
6. **Stress-test real titles before choosing density.** Query the active public title distribution and render at least p95, p99, and the actual maximum title in every shortlisted desktop family. Record the width, font, line budget, overflow count, and the exact treatment of the canonical title. Do not validate long-title behavior with invented lorem ipsum.
7. **Create competing responsive concepts on identical ranked data.** Prefer real HTML/CSS and browser screenshots over abstract boxes. Show desktop and mobile, but do not assume that one geometry should survive both. Preserve ranking order; geometry must not reorder, skip, or stop a personalized/popular feed. Assign every option a stable short ID and name (for example `AR-1 · Aspect Rail`) and render that label visibly inside the board. Repeat the same ID in filenames, Telegram captions, comparison tables, and acceptance notes; also label the intended scroll/interaction direction.
8. **Treat text-on-image as a distinct evidence-gated mode.** Prototype overlay and external-copy variants. Never infer overlay safety from `safe_crop`. Without a persisted safe region plus OCR/face/saliency clearance and contrast evidence, title/date stays outside the image.
9. **Run independent formation before synthesis.** Ask agy/Gemini Pro to create its own renderable variants, not just review Codex proposals. Use only project-approved Pro-class models. Preserve its artifacts and disagreements.
10. **Synthesize explicitly.** Compare concepts by new-user orientation, 20–30-card personalized hit rate, media selling power, scan density, rank integrity, OCR safety, harmony, responsive behavior, data readiness, and MVP cost.
11. **Run a fresh-context validation pass.** Give the validator screenshots/contact sheet, statistics, and decision matrix—not the desired verdict. Require pixel-level inspection and a fail/conditional/pass result. See [references/consultant-and-acceptance.md](references/consultant-and-acceptance.md).
12. **Publish only after authorization.** Use `telegram-human-session` for Telegram. Send the visual artifact before/with concise analysis, then read the topic back and retain a receipt. Do not claim publication from a send return alone.
13. **Hand off final parity.** Once one exact Git SoT/Penpot/Astro tuple exists,
    stop treating the visual lab as acceptance authority and invoke canonical
    `ui-three-way-conformance` for final affected-scope comparison. The lab may
    explore variants; it cannot replace exact conformance or owner acceptance.

## Required gates

- No layout conclusion before a fresh production inventory and real-image contact sheet.
- No “real prototype” containing placeholder or generated media.
- No smart-crop claim without role/text mode, dimensions, focal/safe-crop evidence, and pixel inspection.
- No overlay claim without a real overlay-safe region; default to external copy.
- No square-corner shortlist on desktop: every accepted image surface has a visible, consistent radius that survives edge-to-edge media.
- No decorative image padding on all four sides, blurred ratio-fill, or fake ambient canvas. Use edge-to-edge media, a different accepted ratio, or an intentional text fallback.
- No arbitrary aspect-ratio continuum for verified textless photos. Normalize only crop-safe photos into a finite named token set (default candidates: `P 4:5`, `S 1:1`, `W 4:3`, `L 3:2`); each token needs a tested minimum resolution and focal contract.
- No forced normalization for OCR posters, unknown documents, or unclassified media. Preserve a natural/nearest safe full-bleed ratio until classification rather than center-cropping away evidence.
- No low-resolution upscale or padded rescue in browse cards. Prefer another approved identity asset; otherwise use an intentional standard text fallback without dropping or demoting the event.
- No shortlist without a real maximum-title render and an explicit canonical-title contract. A visible fact-preserving `browse_title` may satisfy a bounded list-card line budget, while the canonical title remains the detail-page H1, structured-data title, accessible full label, and source of truth; coverage and validation must be complete for all active overflow cases before release.
- No blurred/duplicated backdrop or ambient field masking to hide incompatible ratios.
- No geometry-driven reordering or dropped ranked items.
- No rank-aware Bento packing. Bento may be a clearly bounded curated/editorial block with stable order and deliberate rhythm, never the primary personalized or Popular ranked stream.
- No automatic desktop-to-mobile geometry inheritance. Evaluate desktop normalized rails separately from a mobile linear feed plus compact navigation blocks.
- No unlabeled visual alternative: every board and variant needs a visible stable ID/name that a reviewer can quote without describing the layout.
- No conversion of a journey-level 20–30-touch KPI into required screens or cards on one page; report cross-surface exposure paths and `success@30` instead.
- No “consensus with agy” without independent formation, synthesis, and separate validation.
- No OpenAI image generation/editing without explicit user consent in the current turn.

## Reusable utilities

- `scripts/audit_event_media.py --db <sqlite> --output <json>`: reproduce the public-active, approved-media inventory from a local read-only snapshot.
- `scripts/build_contact_sheet.py --manifest <json> --output <png>`: produce a labeled real-image contact sheet.
- `scripts/capture_visual_board.py --url <url-or-html> --output-dir <dir>`: capture deterministic desktop/mobile screenshots when Python Playwright is installed.
- `assets/visual-board-shell.html`: copy as a lightweight starting frame; replace every sample/data slot with real task data.

Store raw queries, manifests, consultant output, screenshots, scorecards, and Telegram receipts in one task artifact directory. Never commit those artifacts.
