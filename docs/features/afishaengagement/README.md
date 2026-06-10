# Afisha Engagement

> **Status:** Visual-debug iteration implemented locally; production deploy and VK shadow verification pending.
> **Confirmation:** Not confirmed by user after VK visual review.
> **Canonical requirements snapshot:** [requirements.md](requirements.md).

`afishaengagement` is a promo activity with the operator-facing meaning
`Мотивация`. It enhances Smart Update VK event posts with CTA motivator images
for comments, likes, and reposts.

The feature is deliberately attached to the Smart Update VK media-preparation
path, not to the standalone promo VK runner. Promo campaign/activity rows decide
whether the enhancer is eligible; the executor runs from `sync_vk_source_post`
after a normal event post is created.

## MVP Contract

- Surface key: `afishaengagement`.
- First rollout target: VK `klgdevents` (`VK_EVENTS_GROUP_ID`).
- Telegram adaptation is out of scope for the first stage.
- Posts without illustrations are skipped before any LLM/Vision work.
- The normal Smart Update VK post remains unchanged in debug shadow mode.
- A separate debug copy is scheduled in VK postponed posts with generated
  afishaengagement media and a cleanup marker.
- Existing managed VK post edits can create shadow copies during the visual
  debug phase, so `/vk_auto_import` can produce the normal post plus a marked
  postponed CTA copy for every applicable post.
- Dedupe is stored through `promo_exposure` rows with
  `surface='afishaengagement'` and `publish_status='VK_SCHEDULED_DEBUG'`.

## Promo Activity Config

The activity is enabled manually as a `PromoActivity` row:

```json
{
  "surface": "afishaengagement",
  "enabled": true,
  "config_json": {
    "target_group": "klgdevents",
    "debug_shadow": true,
    "apply_rate": 1,
    "debug_marker": "#afishaengagement_shadow",
    "debug_cleanup_before": true,
    "debug_cap": 500,
    "debug_publish_delay_days": 3,
    "vision_enabled": true,
    "llm_plan_enabled": true,
    "apply_salt": "campaign-rollout-1"
  }
}
```

Supported target types are the existing promo campaign targets:

- `target_type='event'` with `event_id`;
- `target_type='festival'` with `festival_name`.

For bounded debug batches only, `afishaengagement` also accepts
`target_type='all'`. Use it with `debug_cap` and a temporary marker so the
debug run can collect real Smart Update samples without creating one target row
per event.

Optional config `event_type_keys` narrows a broad target after normal promo
target matching. MVP keys are the internal classifier outputs:
`lecture`, `concert`, `workshop`, `theatre`, `cinema`, `festival`, `market`,
`other`. For example, lecture-only debug uses `target_type='all'` plus
`"event_type_keys": ["lecture"]`.

`apply_rate` accepts `0..1` or percent-like values above `1` (`50` means 50%).
The decision is stable per `event_id/campaign_id/activity_id/apply_salt/media_hash`,
so retries do not randomly flip the same event in and out.
When several activities match the same event, they are evaluated in campaign
priority order. A dice miss, disabled shadow mode, duplicate, or debug cap on
one activity is logged and the next matching activity may still create the
shadow copy. This allows a 70% festival-specific CTA to fall through to the
100% all-post visual-debug activity.

Activities may provide prioritized custom CTA templates:

```json
{
  "mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0},
  "cta_templates": {
    "by_event_type": {
      "*": {
        "likes": [
          "Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."
        ]
      }
    }
  }
}
```

`{THIS_EVENT}` is resolved by event type, for example `эту лекцию`,
`этот концерт`, `этот спектакль`, or `этот кинопоказ`. Configured templates are
preferred inside their activity by default; set
`"prefer_configured_cta_templates": false` only when they should mix with the
generic pool.

## Runtime Flags

- `AFISHAENGAGEMENT_DEBUG_SHADOW_ENABLED=1` enables shadow copies globally. If
  unset, `activity.config_json.debug_shadow` is used.
- `AFISHAENGAGEMENT_DEBUG_MARKER` overrides the marker. Default:
  `#afishaengagement_shadow`.
- `AFISHAENGAGEMENT_DEBUG_CLEANUP_BEFORE=1` deletes existing postponed debug
  copies with the same marker once per process before creating a new batch.
- `AFISHAENGAGEMENT_DEBUG_CAP=20` caps debug shadow rows per activity in the
  last 24 hours.
- `AFISHAENGAGEMENT_DEBUG_PUBLISH_DELAY_DAYS=3` schedules debug copies several
  days ahead, rounded to the next 5-minute boundary.
- `AFISHAENGAGEMENT_DEBUG_SLOT_SPACING_MINUTES=5` controls the spacing between
  shadow debug copies when the base postponed timestamp is already occupied.
- `AFISHAENGAGEMENT_DEBUG_SLOT_SEARCH_LIMIT=96` caps the number of spacing slots
  scanned before falling back to the next slot after the window.
- `AFISHAENGAGEMENT_VISION_ENABLED=1` enables poster OCR/Vision summary.
- `AFISHAENGAGEMENT_LLM_PLAN_ENABLED=1` lets LLM choose mechanic/text/template/
  palette. If disabled or if the LLM output is invalid, deterministic fallback
  chooses a safe plan.

Before scheduling a debug copy, the feature reads current VK postponed posts and
recent `VK_SCHEDULED_DEBUG` exposure rows, then selects the first free
5-minute slot. The selected slot, base slot, counts of occupied sources, and
publish attempts are included in the structured `afishaengagement.decision` log.

## Rendering

The MVP renderer is deterministic PIL with Cygre fonts from
`kaggle/CherryFlash/assets/ro_znanie_fonts/`.

Implemented templates:

- `right_extension`: expands the poster canvas to the right, keeps the
  original poster at its source resolution on the left, and places CTA text in
  an added right color strip with a slight right-leaning diagonal seam. The
  poster itself is not resized or padded; only the canvas grows.

- `bottom_extension`: for horizontal posters, extends the image downward and
  places an engagementcard-style CTA block below the poster. The block may
  slightly overlap the image, but must not cover meaningful poster text or key
  visual objects. The poster itself remains at source resolution.

- `bottom_overlay`: places a solid, high-contrast CTA sticker over a safe lower
  poster area. The block is opaque, has an accent seam, and keeps the mechanic
  badge below the diagonal safe line.

- `hook_swipe_cta`: creates a two-card carousel. The first card keeps the poster
  readable in an upper band and places the hook in a separate engagementcard-
  style lower block with lowercase `листай` plus a right arrow. The second card
  is a deterministic CTA card with a rounded downward arrow.

The renderer uses engagementcard principles from guide excursion monitoring:

- large editorial typography;
- safe zones and word-boundary wrapping;
- deterministic drawing of the red heart in configured CTA copy instead of
  relying on a system emoji font;
- anti-aliased diagonal seams and dividers rendered via deterministic masks;
- contrast-first palettes based on
  `docs/backlog/features/guide-excursions-monitoring/vk_hook_card_palettes.json`;
- poster-aware palette selection: CTA colors must be compatible with dominant
  poster colors and satisfy contrast/readability constraints;
- fail-closed when Cygre is missing or text cannot fit.

Small/narrow posters fail safe to `right_extension` for bottom templates. If a
bottom template still overflows text, rendering falls back to `right_extension`
instead of publishing a clipped or visually damaged image.

CTA copy must vary beyond repeated `Лайк, если ...` phrasing. Like mechanics
should also use forms such as `Поставь лайк, если ...`, `Отметь лайком, если ...`,
and `Поддержи лайком, если ...` where they fit the event.

## LLM/Vision Roles

- Vision/OCR summarizes the poster and provides a confidence signal. Dense or
  uncertain posters are not overlaid; MVP uses right extension instead of
  covering poster content.
- LLM, when enabled, selects only plan fields: `mechanic`, `template_id`,
  `palette_id`, `cta_text`, `hook_text`.
- The final layout is never generated by LLM. PIL renders the final PNG.
- Invalid LLM JSON, unresolved placeholders, too-long CTA text, or unknown
  palette/template values fall back to deterministic selection.

## Logging

Every decision is logged as one-line JSON through logger
`afishaengagement.decision`.

Stages include:

- `eligibility`;
- `cleanup`;
- `dice`;
- `text_resolve`;
- `render`;
- `vk_schedule`;
- `error`.

Logs include campaign/activity/event ids, target VK owner, apply-rate seed and
dice value, media hash, Vision confidence/provider/reason, selected mechanic,
template, palette, CTA text length, text-fit font/line count, render time, LLM
time, debug marker, scheduled publish date, and VK debug post URL/id.

## Cleanup

Manual cleanup script:

```bash
python3 scripts/cleanup_afishaengagement_debug_vk.py \
  --group-id "$VK_EVENTS_GROUP_ID" \
  --marker "#afishaengagement_shadow"
```

Use `--dry-run` to count matching postponed posts without deleting them.

The script calls VK `wall.get filter=postponed` and deletes only posts whose
text contains the marker. It does not print VK tokens or secret values.

## Production Verification Plan

Before declaring the feature confirmed:

1. Deploy from a clean worktree to Fly app `events-bot-new-wngqia`.
2. Enable debug shadow mode and an active promo campaign/activity for a bounded
   target set.
3. Let night Telegram monitoring and Smart Update run if they are already busy;
   do not interrupt a long active import with a deploy.
4. If organic monitoring does not produce enough examples, run a small number
   of production VK auto-import actions through the production bot E2E path.
5. Verify at least five postponed VK debug copies through VK API:
   marker, scheduled date, generated photo attachment, normal post unaffected.
6. Inspect visual output manually before publish time.
7. Delete the debug copies by marker after review/debug.
