---
name: vk-engagement-cards
description: Use when designing, rendering, reviewing, or evolving generated VK engagement/promo cards in this repo: hook cards, CTA cards, carousel cards, poster extensions, and afishaengagement-derived visual layouts.
---

# VK Engagement Cards

Contract version: `v6`
Last updated: `2026-06-12`

Use this skill whenever a task changes generated VK cards: standalone hook
cards, CTA cards, carousel first/last slides, poster overlays/extensions, or
any new VK visual surface that should stay consistent with existing
`afishaengagement` work.

## Canon

- Visual reference implementation: `afishaengagement.py`
- Promo carousel application: `promo.py` / `vk_festival_carousel`
- VK publishing contract: `docs/features/vk-publishing/README.md`
- Promo docs: `docs/features/promo-campaigns/README.md`
- Afisha Engagement docs: `docs/features/afishaengagement/README.md`

## v5 Visual Contract

Generated VK cards should reuse the existing afishaengagement visual language
unless there is an explicit product reason to diverge.

Reuse the maximum practical subset:

- Cygre fonts via `afishaengagement._load_font`;
- `CTA_EDITORIAL_PALETTES` for editorial card palettes;
- `fit_text` for Russian text fitting and line count control;
- anti-aliased edge/seam treatment via `_compose_cta_edge`;
- subtle surface texture via `_apply_cta_grain`;
- proven carousel composition ideas from `hook_swipe_cta`: readable hook card,
  clear CTA card, strong safe zones, no cramped text;
- the `листай` right-arrow cue and CTA down-arrow cue from
  `afishaengagement._render_hook_swipe_cta`.

Do not create a second unrelated visual system with ad hoc fonts, random
palettes, or untested text wrapping while afishaengagement already has a
working solution for the same class of card.

## Card Types

`hook_card`:

- one clear question or short hook;
- no links on the image;
- footer should use the compact `листай` cue with a right arrow for carousels;
- text must fit without edge collisions at 1080x1350.

`cta_card`:

- final carousel card when the post text contains links;
- message should point to the text, not duplicate long URLs;
- visual should include a down-arrow cue when the next action is below the
  carousel in post text;
- the down arrow must be large, central, and high-contrast, following
  `afishaengagement._render_hook_swipe_cta`, not a small corner icon;
- footer/rule lines must leave a central gap around the down arrow, so the
  arrow never crosses a horizontal line;
- use visual hierarchy from afishaengagement CTA cards, but do not force
  like/comment/repost mechanics unless the product surface is engagement.

`poster_extension` / `poster_overlay`:

- use afishaengagement renderers directly where possible;
- preserve source poster readability;
- do not cover meaningful poster text or faces/logos.

`carousel_event_posters`:

- for VK carousels, preserve the poster while wrapping it as a 1080x1350 card
  when a carousel cue is needed;
- do not add a full-width bottom rail by default; poster cards are poster-first;
- add the compact `листай` + right-arrow cue as a floating/control element that
  does not create an extra decorative stripe;
- do not cover meaningful poster text or faces/logos; prefer a safe corner or
  small floating control over a heavy overlay.

## Palette Policy

Generated carousel posts must not all use the same fixed palette. Reuse
`CTA_EDITORIAL_PALETTES` and select a stable palette per activity or per hook
variant from a configured `palette_ids` list, falling back to the standard
editorial set. Store the selected `palette_id` in audit details so the visual can
be reproduced.

## Product Separation

Afishaengagement is for VK-native social actions: like, comment, share/repost.
Promo carousels are for program navigation and registration intent.

Do not stack both surfaces on the same generated unit:

- `vk_festival_carousel` must not call `maybe_publish_shadow_debug_copy`;
- afishaengagement should not add a second CTA layer over a carousel hook/CTA
  card;
- if a surface already has a generated hook and CTA card, treat it as complete
  unless the user explicitly asks for engagement mechanics too.

## Copy And LLM

For broad semantic copy such as hooks, use LLM-first when no approved operator
copy exists:

- exact campaign copy in config wins;
- otherwise use the existing project LLM wrapper (`main.ask_4o`) with a compact
  JSON contract;
- validate length, placeholders, unsupported claims, and tone;
- keep deterministic fallback for outages and tests.

Visual rendering itself remains deterministic.

For celebrity/program-leader hooks, do not fill the carousel with generic event
posters. Event metadata is only a hypothesis; the image itself must prove the
claim. Use explicit `celebrity_poster_urls_by_event_id` /
`celebrity_photo_urls_by_event_id` when the campaign has been curated; otherwise
include only images where OCR/vision or another evidence source shows visible
person names/roles on the poster. Fail closed when unsure.

Celebrity/program-leader carousels may add curated FIO/role cards after the
poster cards when the poster set does not cover all important people. Keep the
full carousel at nine cards maximum for this variant: hook, poster cards,
person cards, optional final CTA. Use `covered_celebrity_names_by_event_id` or
the equivalent poster-name config to skip people already visible on selected
posters.

Celebrity/program-leader person discovery is LLM-first. The prompt must receive
the remaining card budget and event evidence, but rendering code must still
enforce the nine-card carousel cap and dedupe covered poster names after the
LLM returns. Explicit `celebrity_person_cards` is an operator override, not the
default production workflow.

## Links On VK Cards

- Do not render long URLs onto image cards.
- Put links in post text.
- In VK post text, prefer VK-shortened links (`vk.cc`) where possible.
- In Telegram surfaces, keep expanded canonical links and never reuse VK-only
  shortlinks.

## Shadow Review

For visual card changes that need operator review:

1. Publish to VK postponed posts, usually 2-3 days ahead.
2. Mark the post text clearly as debug/shadow.
3. Record an audit exposure row if the surface is promo-backed.
4. Include enough details to reproduce the visual: event ids, hook text,
   template/card kind, attachment count, target URL, scheduled timestamp.
5. Do not switch the same configuration to normal publication until the user
   approves the shadow post.

## Validation

Minimum checks:

- targeted unit test for generated attachments and text;
- `py_compile` for touched renderer modules;
- if visual framing matters, create or inspect a rendered sample under
  `artifacts/codex/...` and do not commit it.

For `vk_festival_carousel` specifically:

```bash
.venv/bin/python -m pytest tests/test_promo.py::test_vk_festival_carousel_shadow_posts_hook_posters_and_cta -q
.venv/bin/python -m pytest tests/test_promo.py::test_vk_festival_carousel_celebrity_llm_adds_person_cards_with_budget -q
.venv/bin/python -m py_compile promo.py
```

## Versioning

When card grammar, typography, palette policy, LLM copy contract, or shadow
evidence changes, bump `Contract version` and add a short changelog section:

```markdown
## v2 Changes

- Add `листай` right-arrow and CTA down-arrow cues from afishaengagement.
- Require palette diversity through stable per-activity palette selection.
- Require celebrity/program-leader carousels to use only explicitly relevant
  event posters.

## v3 Changes

- Poster cards are poster-first and must not add a full-width bottom rail by
  default.
- CTA cards require a large central down arrow.
- Celebrity/program-leader carousels require image-level evidence, not just
  event metadata.

## v4 Changes

- Celebrity/program-leader carousels can add curated FIO/role person cards.
- Celebrity carousels are capped at nine total cards.
- Person cards skip names configured as already visible on selected posters.

## v5 Changes

- Celebrity/program-leader person discovery is LLM-first by default.
- The LLM prompt receives the remaining person-card budget.
- Rendering still hard-caps and deduplicates person cards after LLM output.

## v6 Changes

- CTA footer/rule lines must leave a central gap around the down arrow so final
  action cards do not visually cross the arrow with a horizontal line.
```
