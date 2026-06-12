# Afisha Engagement

> **Status:** Visual-debug iteration deployed; fresh VK shadow audit found and fixed CTA/type guardrails.
> **Confirmation:** Not confirmed by user after VK visual review.
> **Canonical requirements snapshot:** [requirements.md](requirements.md).

`afishaengagement` is a promo activity with the operator-facing meaning
`Мотивация`. It enhances VK event posts with CTA motivator images for comments,
likes, and reposts.

Promo campaign/activity rows decide whether the enhancer is eligible. The
executor runs after a normal event post is prepared both in the Smart Update
source-post path and in promo `vk_publication`, so additional promo VK
publications do not bypass the `Мотивация` activity.

## MVP Contract

- Surface key: `afishaengagement`.
- First rollout target: VK `klgdevents` (`VK_EVENTS_GROUP_ID`).
- Telegram adaptation is out of scope for the first stage.
- Posts without illustrations are skipped before any LLM/Vision work.
- The normal Smart Update VK post remains unchanged in debug shadow mode.
- A separate debug copy is scheduled in VK postponed posts with generated
  afishaengagement media and a cleanup marker.
- Existing managed VK post edits and promo `vk_publication` runs can create
  shadow copies during the visual debug phase, so `/vk_auto_import` and promo
  deficit publications can produce the normal post plus a marked postponed CTA
  copy for every applicable post.
- Smart Update passes the exact deduped media URL list used for the normal VK
  photo attachments into the shadow renderer. This keeps the CTA text, event
  metadata, and generated poster media bound to the same event/photo set.
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
`lecture`, `meeting`, `excursion`, `concert`, `workshop`, `theatre`, `cinema`,
`festival`, `market`, `family`, `other`. For example, lecture-only debug uses
`target_type='all'` plus `"event_type_keys": ["lecture"]`.

CTA copy selection treats the stored event `event_type` as the primary source of
truth. Title/description heuristics are only a fallback or a narrow refinement:
a generic `встреча` must not become `lecture` unless lecture/speaker/discussion
signals are present, and child/family wording such as fairy-character events is
classified as `family` for CTA text.
Creative meetings and dialogue formats use `meeting` copy so educational
programs do not accidentally get theatre/exhibition wording from venue text or
artist bios. Zoo/excursion events use `excursion` copy; stored `лекция` values
are overridden only when the event text clearly says excursion/zoo/zoologist
context, preventing `лекции` CTA copy from leaking into zoo excursions.
Zoo-specific CTA copy (`зоопарк изнутри`, `зоологи`, `животные`,
`ветеринарный уход`) is allowed only when the event text or location carries a
real zoo signal. Generic backstage excursions such as `Закулисье театра` stay
on ordinary excursion/behind-the-scenes wording and must not inherit zoo copy.
Several narrow semantic safety overrides protect the CTA surface from obviously
wrong stored event types without changing the source event row:

- holiday programs like `Празднование Дня России` use `holiday` copy even when
  the stored type says `фестиваль`;
- theatre titles/venues such as `Спектакль «Гараж»` at a drama theatre use
  `theatre` copy even when the stored type says `кинопоказ`, unless the event
  text is genuinely about a film screening;
- recycling/drop-off actions such as `Приём шин` do not inherit `market`
  (`ярмарка`) copy from a bad stored type.
- family fairs/markets with explicit child/family audience signals
  (`семейный`, `детский`, `сказочные герои`, `аниматоры`, etc.) use `family`
  copy even when the stored type says `ярмарка`, so a family fair does not get
  generic market repost text like `Поделись с подругой, которая любит такие
  ярмарки`. Their repost templates use a separate parent-aware pool with
  softer `подруга-мама`, `мама-подруга`, `родители`, and child-focused wording.

Theme extraction inside an already selected event type must use safe word/stem
matching: narrow cues such as `орган` may match real organ music forms, but must
not match unrelated words such as `организаторы`. If a theme is uncertain, the
copy falls back to the event type instead of inventing a specific music/cinema
topic.
Festival context is intentionally preserved as an umbrella, but it must be tied
to the concrete program, project, or topic. For example, `80 историй о главном`
uses history/project wording, and the educational program of `Кантата` uses
`образовательная программа фестиваля Кантата` instead of generic festival
waiting copy. Festival CTA text must not ask who is waiting for a festival when
the festival may already be in progress; it should ask what in the program or
project is closer to the viewer, or who follows the concrete program/project.
When the event text explicitly names a clear concept such as a concert on water,
a candlelight concert, or an open-air screening, deterministic CTA generation
may use an idea-based CTA such as `Поставь лайк, если нравится идея концерта на
воде`. These idea CTAs are only allowed from direct event text signals and must
not invent a concept from vague theme or venue hints.
CTA text generation is hybrid. The renderer and visual plan stay deterministic,
but `llm_text_mode=auto` runs a compact LLM CTA-writer only for risky copy:
theme-heavy comment text, forbidden phrases, or event-type conflicts such as a
cinema post getting theatre wording, plus overly generic comment questions such
as `что для вас главное` / `что цепляет вас в таких событиях`. Simple safe
ready-made CTAs can stay deterministic. Comment CTAs should ask a concrete
question about the event, poster topic, organizer, artist, work, or explicit
idea when those facts are present. Cinema club events may ask whether viewers
have already attended that club's screenings, and artist/persona events may ask
about the artist's creative work, including historical artists, but only when
the name is grounded in event text. If the LLM is unavailable or returns
invalid copy, guardrails fall back to a generic safe CTA instead of publishing
an awkward template.
CTA text must stay native to VK engagement: it may ask the viewer to comment,
like, share, or repost, but must not directly push attendance, registration,
ticket purchase, booking, saving to plans, "where to go" decisions, or
joining/participating in the event itself (`присоединяйся к празднику`).
This guardrail applies both to deterministic templates and to configured
activity templates. Repost copy should keep natural variants for both `друг`
and `подруга` where the wording names a friend directly, so batches do not
repeat a single gendered formula. Family repost copy may use softer
mom-friend variants such as `подруга-мама` / `мама-подруга` when the event is
clearly aimed at parents and children.
LLM CTA output is also post-filtered for unsupported concrete references: for
example, an LLM suggestion about a zoo is rejected when the event text does not
actually mention a zoo, zoologists, animals, veterinary care, enclosures, or a
zoo location.
If a selected visual format still overflows at render time, debug shadow
generation retries once with a short safe CTA in `right_extension`; a failed
bottom/card variant must not make the whole shadow post disappear during visual
debug.

Every scheduled debug shadow exposure stores the durable debugging context
needed to audit poster/text binding after the post appears in VK postponed
posts: campaign/activity profile, event title, stored and normalized event type,
festival name, source post URLs, first rendered source-photo URL, source photo
count, CTA text, template, palette, rendered dimensions, media hash, vision
summary, dice value, scheduled timestamp, and VK debug post URL.

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
          "Поставь лайк ❤️, если тебе близки такие события."
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
Marked debug-shadow copies must not participate in the ordinary VK postponed
slot calculation for real event posts; normal source posts choose their next
slot as if `[AFISHAENGAGEMENT DEBUG COPY ...]` posts were not present.

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
  badge below the diagonal safe line. Wide horizontal posters do not use this
  as a cramped overlay; they are promoted to `bottom_extension` so the CTA is
  added below the poster instead of widening the image to the right.
  Photo-only/collage posters with sparse OCR avoid `bottom_overlay` during plan
  selection and prefer a side extension or carousel, so the CTA does not cover a
  key visual object when there is no poster text map to rely on.

- `hook_swipe_cta`: creates a two-card carousel. The first card keeps the poster
  readable in an upper band and places the hook in a separate engagementcard-
  style lower block with lowercase `листай` plus a right arrow. The poster is
  fitted with contain-style scaling so dense source posters keep all edge
  lettering instead of being center-cropped. The second card is a deterministic
  CTA card with a rounded downward arrow. In mixed `formats` configurations,
  `hook_swipe_cta` is weighted as a rarer format by default; use
  `format_weights` / `template_weights` or a single-format list when a carousel
  batch is intentional.

The renderer uses engagementcard principles from guide excursion monitoring:

- large editorial typography;
- safe zones and word-boundary wrapping;
- deterministic drawing of the red heart in configured CTA copy instead of
  relying on a system emoji font;
- anti-aliased diagonal seams and dividers rendered via deterministic masks;
- CTA-card separation as a distinct action surface: multi-layer edge treatment
  with poster-side hairline, seam, inner accent rim, light hairline, stronger
  poster-side shadow, and deterministic low-amplitude grain only inside the CTA
  surface. Palette compatibility must not make the CTA read as part of the
  original poster.
- diagonal CTA shadows follow the actual seam normal instead of using a fixed
  horizontal or vertical offset, so side and bottom templates keep the shadow
  aligned with the slanted boundary.
- bottom CTA layouts keep mechanic badges as lower action anchors with explicit
  clearance from the diagonal seam; when the safe spacing cannot be met, the
  renderer fails over instead of publishing a cramped bottom block.
- mechanic badges are rendered as outline pills on the CTA surface, using the
  same signal color as the rim/rail, to avoid a cheap sticker look. Badge icons
  are mechanic-aware: likes use a drawn red heart, comments use a small downward
  arrow, and reposts keep a right arrow.
- contrast-first palettes based on
  `docs/backlog/features/guide-excursions-monitoring/vk_hook_card_palettes.json`;
- poster-aware CTA color separation: the scorer evaluates the local seam-side
  poster region, then picks curated editorial/noir/ivory/graphite palette
  families by readability, luma separation, poster contrast, hue distance, and
  event tone. Compatibility is a tie-breaker; a harmonious palette loses if the
  CTA block would read as part of the poster.
- curated/trend palette bank: proven `engagementcard` colors are extended with
  designer-adjacent pairs such as ivory/plum/wasabi, clay/cobalt, botanical
  green/citron, plum noir/cloud, smoky jade/terracotta, future-dusk/lime,
  transformative teal/persimmon, mocha/aqua, butter/cherry, thermal
  cobalt/tomato, ink/fuchsia/mint, sage/lilac, and oxide/citron. The renderer
  chooses among these designed pairs; it does not synthesize arbitrary
  mathematical colors.
- saturated legacy yellow/violet remains available for bright festival/pop
  cards, but receives a scoring penalty for lecture, meeting, exhibition, and
  excursion-like cards so real batches do not collapse into one loud color
  family.
- extended color roles: `surface`, `ink`, `signal`, `signal_ink`, `seam`, and
  `rim` are mapped back to legacy `background/text/accent` fields for
  compatibility.
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
dice value, media hash, event title, source URLs, first media URL, Vision
confidence/provider/reason, selected mechanic, template, palette, CTA text
length, text-fit font/line count, render time, LLM time, debug marker, scheduled
publish date, and VK debug post URL/id.

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

If old debug posts were created without a searchable marker in VK text, cleanup
must use the recorded `promo_exposure.public_targets_json` URLs instead:

```bash
python3 scripts/cleanup_afishaengagement_debug_vk.py \
  --from-db \
  --dry-run \
  --group-id "$VK_EVENTS_GROUP_ID" \
  --db-path /data/db.sqlite \
  --stale-before "2026-06-11T09:18:00+00:00"
```

Remove `--dry-run` only after reviewing the candidates. This mode deletes only
future `surface='afishaengagement'` / `publish_status='VK_SCHEDULED_DEBUG'`
posts and marks their exposure rows as `VK_DELETED_DEBUG`.

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

Fresh audit evidence from 2026-06-11 production recovery:

- Telegram Monitoring Kaggle recovery import for
  `run_id=7f4af7474db2421f9ee506d8157886be` finished successfully as
  `ops_run.id=2275` with `messages_processed=165`, `events_imported=29`,
  `events_created=16`, `events_merged=13`, and `errors_count=0`.
- The existing recovery mechanism produced 31
  `surface='afishaengagement'` / `publish_status='VK_SCHEDULED_DEBUG'` rows in
  the last 24 hours, so no manual `/vk_auto_import` batch was needed after
  recovery completed.
- Audit artifacts for this run are under
  `artifacts/codex/afishaengagement-audit-20260611/` and are intentionally not
  committed.
