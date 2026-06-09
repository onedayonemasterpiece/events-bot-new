# Afisha Engagement

> **Status:** MVP implemented locally, production shadow verification pending.  
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
- Existing managed VK post edits do not create shadow copies; MVP shadows only
  newly created event posts to avoid repeated copies during Smart Update edits.
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
    "apply_rate": 0.5,
    "debug_marker": "#afishaengagement_shadow",
    "debug_cleanup_before": true,
    "debug_cap": 20,
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
`lecture`, `concert`, `workshop`, `theatre`, `festival`, `other`. For example,
lecture-only debug uses `target_type='all'` plus
`"event_type_keys": ["lecture"]`.

`apply_rate` accepts `0..1` or percent-like values above `1` (`50` means 50%).
The decision is stable per `event_id/campaign_id/activity_id/apply_salt/media_hash`,
so retries do not randomly flip the same event in and out.

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
- `AFISHAENGAGEMENT_VISION_ENABLED=1` enables poster OCR/Vision summary.
- `AFISHAENGAGEMENT_LLM_PLAN_ENABLED=1` lets LLM choose mechanic/text/template/
  palette. If disabled or if the LLM output is invalid, deterministic fallback
  chooses a safe plan.

## Rendering

The MVP renderer is deterministic PIL with Cygre fonts from
`kaggle/CherryFlash/assets/ro_znanie_fonts/`.

Implemented template:

- `right_extension`: expands the poster into a 1440x1080 image, keeps the
  original poster on the left, and places CTA text in a right color strip with
  a slight right-leaning diagonal seam.

The renderer uses engagementcard principles from guide excursion monitoring:

- large editorial typography;
- safe zones and word-boundary wrapping;
- no emoji on the generated image;
- contrast-first palettes based on
  `docs/backlog/features/guide-excursions-monitoring/vk_hook_card_palettes.json`;
- fail-closed when Cygre is missing or text cannot fit.

Future debt: implement the `hook_swipe_cta` carousel fallback as
`original poster + hook card + CTA card` using the same engagementcard 1080x1080
safe-zone rules. The MVP code currently prefers `right_extension` for shadow
debug output.

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
