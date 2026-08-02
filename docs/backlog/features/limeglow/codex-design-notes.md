# Limeglow / Guide Excursion Motion Stories

> Status: design / ready for local keyframe prototyping, not ready for full video generation or production publish.
> Scope: premium vertical motion story for guide-excursion digest announcements.
> Canonical doc: this file.

## Goal

Limeglow is a generated Telegram Stories video format that announces selected guide excursions from the guide-excursions digest. It should feel like an expensive editorial motion opener: dark canvas, cutout collage objects, pseudo-3D depth, kinetic typography, precise ease-in-out camera movement, and a route through a larger scene rather than a sequence of flat cards.

The immediate task is not full automation. The immediate task is to design the system enough to start local trial generation of key frames, then iterate on those frames before building the whole rendered story and Kaggle handoff.

## How I Understand The Product

Limeglow is a sibling product to CherryFlash and Kenigsberg Stories, not a replacement for either.

The creative unit is not a poster scene. The creative unit is a large 2D/2.5D editorial canvas where each selected excursion becomes a spatial node:

- one guide cutout;
- one or more cutout route/location/photo objects;
- a short typographic lockup with title, date, place, guide, and one hook;
- accent route geometry that helps the camera travel to the next node.

The camera travels across that canvas. A previous excursion may remain as a small background fragment while the next excursion takes focus. Each generation must first plan the route mechanics and should vary them from the previous generation: lateral pass, push-through, pull-back, diagonal climb, object-as-portal, triptych, clone/echo, route-line follow, and so on.

Important product constraint: Limeglow is a digest of independent excursion
options, not one fictional route that the viewer takes from excursion A to
excursion B. The director may use a camera path to travel between visual nodes,
but the copy and hierarchy must keep each excursion independent: its own guide,
date, hook, and object artifacts.

Do not label digest nodes as `первая прогулка`, `вторая прогулка`, or similar
serial episodes unless the source product explicitly has a ranked/ordered
program. Use the excursion title, hook type, theme tag, guide/date, or neutral
digest wording instead.

## Hard Requirements

- Output format: vertical Telegram Story, `720x1280`.
- Debug path: local keyframe generation first. Do not start with full Kaggle video.
- Full render path: reuse CherryFlash/Kenigsberg/Crumple mechanics for Kaggle handoff, polling, story encoding, and publishing.
- Audio for early prototypes: use CrumpleVideo audio `video_announce/assets/The_xx_-_Intro.mp3`, with the CrumpleVideo cue start at `1:17`.
- Input eligibility: an excursion can enter Limeglow only if it has:
  - an excursion/location/photo image suitable for a collage object;
  - a guide photo with background removed, or a documented placeholder only for early keyframe debugging.
- Photo treatment:
  - remove or simplify background when the subject can be isolated;
  - keep the strongest recognizable part of the image;
  - allow sepia, grayscale, duotone, posterization, grain, and contrast processing;
  - if a source image has several useful objects, split it into smaller collage objects.
- Building and architecture photos are a separate treatment class, not ordinary
  background removal. For facades, the primary extraction task is to find the
  skyline: the visible boundary where the roof/building contour meets sky or
  open negative space. The final cut should use large editorial newspaper-like
  lines along that skyline and a deliberately simple lower/side crop, rather
  than trying to remove every branch, wire, or window-detail hole.
- Object extraction should be LLM-first for semantic object choice. Deterministic code may crop/mask/normalize, but should not decide the semantic meaning of a photo by broad regex/keyword rules.
- Before full generation:
  - produce an overall scene route plan;
  - produce several keyframe plans;
  - render keyframe PNGs;
  - only after the keyframes are approved, render the full motion story.

## Reference Video Findings

Reference file:

`docs/backlog/features/limeglow/Screen_Recording_20260606_235658_YouTube.mp4`

Local analysis artifact:

`artifacts/codex/limeglow-reference-analysis/`

Measured facts from the local analysis:

- source recording size: `2400x1080`;
- duration: `12.034s`;
- FPS reported by OpenCV: about `27.837`;
- useful opener region: roughly `0.50s..11.50s`;
- the YouTube UI and progress controls are recording artifacts and must be ignored.

Dominant visual statistics confirm the intended dark pattern:

- very dark frames dominate most of the reference;
- dominant dark colors: near `#000000`, `#101010`, `#202020`;
- main accents: magenta around `#D02050`, electric blue around `#0060F0` / `#1060E0`;
- bright white/gray is mostly used for cutout people and typography, not as the base.

### Motion Grammar To Transfer

The reference is best understood as a camera journey over a large editorial collage canvas.

Core mechanics:

- dark void with sparse high-contrast objects;
- monochrome people and objects over strong accent rectangles;
- oversized typography cropped by frame edges;
- tag-like UI micro-labels;
- background letters and geometry that move slower than the hero layer;
- foreground tags/icons that move faster or arrive with slight delay;
- push-in transitions into people, cards, or color blocks;
- pull-back transitions to reveal the whole composition;
- lateral movement where the previous scene remains partially visible;
- triptych/card stacks with staggered appearances;
- echo/clone treatment of people during transitions;
- final lockup with several figures and title.

The important part is not copying the exact people/news branding. The important part is the hierarchy:

1. Global camera route over a big canvas.
2. Parallax by layer depth.
3. Desynchronized easing per layer.
4. Local micro-motion for tags, icons, object cutouts, and typography.

### Easing Contract

Use strong editorial easing, not linear movement.

Recommended curves:

- global camera: `cubicBezier(0.76, 0.00, 0.24, 1.00)` or `easeInOutQuart/Quint`;
- cards and photo slabs: `cubicBezier(0.65, 0.00, 0.35, 1.00)`;
- tags and small accent objects: `cubicBezier(0.34, 1.56, 0.64, 1.00)` with restrained overshoot.

Motion rhythm:

- start acceleration: `0.10..0.18s`;
- fast travel: `0.35..0.70s`;
- settle: `0.18..0.35s`;
- tags/icons may finish `40..180ms` after the hero layer.

### Layer Depth Contract

Each object in the scene plan should carry a semantic depth.

Suggested depth scale:

```json
{
  "far_background": -1.0,
  "background_typography": -0.6,
  "color_blocks": -0.25,
  "main_photo_object": 0.0,
  "guide_cutout": 0.35,
  "foreground_tags": 0.75,
  "route_particles": 0.9
}
```

Parallax factor:

```text
depth -1.0 -> about 45% of camera movement
depth  0.0 -> about 85-100%
depth  1.0 -> about 125%
```

Layer delays should be explicit in the plan. Example:

```json
{
  "background_typography_delay_ms": 140,
  "color_block_delay_ms": 80,
  "guide_cutout_delay_ms": 0,
  "foreground_tag_delay_ms": 60,
  "route_particle_delay_ms": 130
}
```

## Existing Project Solutions To Reuse

### CherryFlash

Relevant docs and files:

- `docs/backlog/features/cherryflash/README.md`
- `kaggle/CherryFlash/`
- `scripts/render_cherryflash_full.py`
- `scripts/render_mobilefeed_intro_scene1_approval.py`
- `video_announce/scenario.py`
- `video_announce/kaggle_client.py`
- `kaggle/CherryFlash/runtime_locator.py`

Reusable contracts:

- separate product/profile boundary;
- local preview and final mode split;
- `720x1280` story geometry in final mode;
- frame-by-frame renderer pattern;
- payload-driven scene manifest;
- per-run Kaggle session dataset pattern;
- fresh runtime bundle upload instead of stale baked assets;
- wait until Kaggle dataset sources are really bound;
- poller and operator status handling;
- explicit artifact directories under `artifacts/codex/`.

Do not copy CherryFlash's visual grammar as-is. Its poster/phone/ribbon language is a different product. Reuse its runtime and deployment lessons.

### CrumpleVideo

Relevant docs and files:

- `docs/features/crumple-video/README.md`
- `kaggle/CrumpleVideo/story_publish.py`
- `kaggle/CrumpleVideo/story_gesture_overlay.py`
- `video_announce/assets/The_xx_-_Intro.mp3`

Reusable contracts:

- audio cue: `The_xx_-_Intro.mp3` from `1:17`;
- shared story publish helper;
- Telegram story media preflight/publish lessons;
- story-safe encoding discipline.

Limeglow should not inherit CrumpleVideo's paper-unfold visual language.

### Kenigsberg Stories

Relevant docs and files:

- `docs/features/kenigsberg-stories/README.md`
- `scripts/render_kenigsberg_story.py`
- `kaggle/KoenigsbergStories/`

Reusable contracts:

- sibling-product model;
- local script plus Kaggle runtime split;
- `720x1280` native story target;
- manifest logging for generated output;
- strict input constraints before expensive render.

### AfishaThumb

Relevant files:

- `scripts/afishathumb/camera_plan.py`
- `scripts/afishathumb/flythrough.py`
- `kaggle/AfishaThumb/scripts/scene_llm.py`

Reusable ideas:

- explicit camera route planning before render;
- dwell vs fly-by concepts;
- LLM-assisted scene composition;
- validation of generated layout objects;
- non-dwell background objects that enrich the camera pass.

AfishaThumb is not the runtime target, but its planning vocabulary is useful for Limeglow.

## Palette System

Base reference palette:

```json
{
  "background": "#08080D",
  "surface_dark": "#101014",
  "text": "#F4F4F4",
  "muted": "#8D8D96",
  "magenta": "#D02050",
  "blue": "#0060F0"
}
```

Limeglow may also reuse and extend palettes from:

`docs/backlog/features/guide-excursions-monitoring/vk_hook_card_palettes.json`

Useful first-run families:

- `oxide_cloud` for brick architecture and forts;
- `prussian_cream` for German heritage / architecture;
- `baltic_navy_sand` for sea and city routes;
- `charcoal_apricot` for universal dark editorial scenes;
- `aubergine_lemon` for theatre / unusual walks.

Palette rule: keep the general dark pattern, but vary accent colors per generated story. Avoid turning the whole roll into one monotone hue family.

## First Debug Source Pack

VK posts requested for the first debug generation:

- `https://vk.com/wall-238875824_8`
- `https://vk.com/wall-238875824_12`

Local debug asset artifact:

`artifacts/codex/limeglow-vk-debug-assets/`

### Media Mapping From VK API

`wall-238875824_8`

- `photo 1`, `-238875824_457239023`, `853x1280`: poster for `Тепло / Лекции и экскурсии`, visually tied to the Железнодорожные ворота / festival lecture block.
- `photo 2`, `-238875824_457239024`, `960x1280`: brick residential facade. Strongest inferred match: the `Амалиенау` cluster in the same digest, because the visual subject is historic residential architecture. Confidence is medium because the digest has several repeated `Амалиенау` entries with different dates/guides.

`wall-238875824_12`

- `photo 1`, `-238875824_457239027`, `960x1280`: ice cream by the sea. Weak inferred match by media order: `По следам Э.Т.А. Гофмана`; this needs confirmation because the image itself does not strongly encode Hoffmann, Litovsky Val, or another specific route.
- `photo 2`, `-238875824_457239028`, `960x1280`: park/lake; likely `Макс Ашманн парк`.
- `photo 3`, `-238875824_457239029`, `1080x1125`: monument `Покорителям ближней вселенной`; likely `Городскими орбитами улиц`.
- `photo 4`, `-238875824_457239030`, `828x1110`: tram in green corridor; strong match: `Кенигсбергский трамвай - назад в прошлое`.
- `photo 5`, `-238875824_457239031`, `768x736`: Schiller monument; likely `Культурная революция: туда и обратно`.

### Suggested Debug Selection

Use three visual nodes for first keyframes:

1. `Амалиенау` cluster from `wall-238875824_8`, using the brick facade as the main object.
2. Ice-cream photo from `wall-238875824_12`, temporarily mapped to `По следам Э.Т.А. Гофмана` until confirmed.
3. `Кенигсбергский трамвай - назад в прошлое` from `wall-238875824_12`, using the tram photo.

This is enough to test different object treatments:

- architecture facade as a large cutout/surface;
- food/hand/sea object as foreground cutout;
- tram as a route object with strong perspective and linear motion.

For the first two-excursion visual test, use only the strongest/least ambiguous pair:

1. `Амалиенау` / brick facade.
2. `Кенигсбергский трамвай - назад в прошлое` / tram corridor.

The ice-cream node can stay out of the first test because its excursion mapping is weak. It is more useful as a third-node stress test once the core visual grammar works.

### Debug Speaker Cutout Pool

For local Limeglow prototyping, use the already prepared speaker PNGs from:

`docs/backlog/features/limeglow/speakers-test-pics/`

Current files:

- `Бойко Андрей.png`
- `Криммель Наталья.png`
- `Литвинович Ирина.png`
- `Перкусов Николай.png`
- `Селин Игорь - 2.png`
- `Удовенко Татьяна.png`

These images are debug speaker cutouts only. They are not yet tied to the real guide/excursion source graph, and the renderer must not imply that a random speaker is the actual guide for the shown excursion.

Debug assignment contract:

- choose speaker cutouts in random order for `debug-01`;
- make the random order seedable and log it in `scene_plan.json`;
- avoid repeating the same speaker inside one short prototype until the pool is exhausted;
- keep visible guide labels text-based from the excursion payload, not from the random cutout filename;
- store the assignment as `speaker_cutout_debug`, not as `guide_cutout`, so production code cannot accidentally treat it as factual metadata.

Speaker treatment rule:

- Guide/speaker cutouts are not thumbnails. At least once per selected
  excursion, the guide must become a primary hero layer.
- A strong approved treatment is `speaker_echo`: render the main guide cutout
  as a high-contrast monochrome hero over an accent field, with 1-2
  semi-transparent duplicate silhouettes behind or beside it. Echo copies must
  use lower opacity, small position offsets, and slightly different depth/phase
  values so they read as motion/transition residue rather than extra people.
- Other allowed speaker treatments for director variation: edge-cropped hero,
  full-height cutout over color field, small figure-in-depth, and final lockup
  group. The director should vary these treatments by seed while keeping guide
  readability.

## First Keyframe Prototype

Working title:

`Limeglow Debug 01 / Dark Route Collage`

Target:

- keyframe PNGs first, not video;
- `720x1280`;
- 3 excursion nodes plus final CTA lockup;
- use random debug speaker cutouts from `speakers-test-pics/`; they are visual placeholders and are not factual guide identities.

### Route Concept

The camera travels across one oversized vertical canvas:

```text
small intro node
  -> push into brick facade
  -> lateral drift through magenta title field
  -> foreground ice-cream object wipes the frame
  -> diagonal route-line fall into tram corridor
  -> pull-back final lockup
```

The first generation should avoid standard card cuts. Each transition should be spatial:

- brick facade edge becomes a vertical wipe;
- ice cream cone becomes a foreground parallax object;
- tram rails/trees become a route vector;
- final pull-back shows the three nodes as parts of one map-like editorial spread.

### Keyframes To Render

`KF-00 / cold open`

- Time: `0.0s`.
- Visual: near-black background, tiny speaker cutout, route dot, or huge cropped background word fragments.
- Purpose: establish scale and empty premium space.

`KF-01 / brick landing`

- Time: `1.2s`.
- Visual: brick facade enlarged and treated in warm monochrome/sepia; guide cutout overlaps lower-right; magenta or oxide accent slab behind.
- Text: `АМАЛИЕНАУ` plus date/guide in small tag.
- Motion into it: push-in and slight rightward drift.

`KF-02 / brick exit`

- Time: `2.6s`.
- Visual: facade edge passes foreground; previous node remains cropped at left; route geometry points toward next node.
- Text: title fragments partially occluded, not a full flat card.
- Motion: lateral pass with background typography lagging.

`KF-03 / ice-cream foreground`

- Time: `3.9s`.
- Visual: ice cream cone/hand cut out as foreground object; sea simplified into a dark cyan/blue field; random debug speaker cutout smaller in depth.
- Text: compact hook tag. The exact excursion label needs confirmation.
- Motion into it: foreground object arrives earlier than the background, with overshoot on the tag.

`KF-04 / tram corridor`

- Time: `5.9s`.
- Visual: tram photo treated as deep green/gray corridor; tram remains recognizable; blue route line and small date badge.
- Text: `КЕНИГСБЕРГСКИЙ ТРАМВАЙ`.
- Motion: diagonal camera drop into rails/trees, foreground route particles at high parallax.

`KF-05 / final pull-back`

- Time: `8.5s`.
- Visual: all three nodes visible as fragments on the same canvas; CTA/title lockup.
- Text: `Экскурсии недели` / channel-brand line to be decided.
- Motion: pull-back and settle, with tiny UI tags still drifting.

## Candidate Payload Schema

The first local renderer should consume a JSON payload rather than hardcoded assets.

```json
{
  "product": "limeglow",
  "version": "debug-01",
  "canvas": {"width": 720, "height": 1280, "fps": 30},
  "audio": {
    "path": "video_announce/assets/The_xx_-_Intro.mp3",
    "start_seconds": 77
  },
  "palette": {
    "id": "oxide_blue_debug",
    "background": "#08080D",
    "text": "#F4F4F4",
    "accent_primary": "#D02050",
    "accent_secondary": "#0060F0"
  },
  "speaker_cutout_pool": {
    "mode": "debug_random_seeded",
    "path": "docs/backlog/features/limeglow/speakers-test-pics",
    "seed": "limeglow-debug-01",
    "factual_binding": false
  },
  "excursions": [
    {
      "id": "debug-amalienau-brick",
      "title": "Амалиенау",
      "guide": "Игорь Ляшук / Валя Симкова / Таня Бурдужан",
      "date_line": "7/14/21/28 июня",
      "place": "Калининград",
      "source_post": "https://vk.com/wall-238875824_8",
      "main_photo": "wall-238875824_8 photo 2",
      "speaker_cutout_debug": "random_from_pool",
      "mapping_confidence": "medium"
    },
    {
      "id": "debug-icecream-unconfirmed",
      "title": "По следам Э.Т.А. Гофмана",
      "guide": "Катя Марти",
      "date_line": "13 июня",
      "place": "Калининград",
      "source_post": "https://vk.com/wall-238875824_12",
      "main_photo": "wall-238875824_12 photo 1",
      "speaker_cutout_debug": "random_from_pool",
      "mapping_confidence": "low"
    },
    {
      "id": "debug-tram",
      "title": "Кенигсбергский трамвай - назад в прошлое",
      "guide": "Дина Лях",
      "date_line": "27 июня, 17:30",
      "place": "Калининград",
      "source_post": "https://vk.com/wall-238875824_12",
      "main_photo": "wall-238875824_12 photo 4",
      "speaker_cutout_debug": "random_from_pool",
      "mapping_confidence": "high"
    }
  ],
  "route_plan": {
    "variant": "push-lateral-foreground-diagonal-pullback",
    "keyframes": ["KF-00", "KF-01", "KF-02", "KF-03", "KF-04", "KF-05"]
  }
}
```

## LLM Responsibilities

Limeglow should use small, explicit LLM calls rather than one giant vague creative call.

### Scene Route Planner

Input:

- selected excursions;
- available photo objects;
- guide cutout availability;
- previous route variant, if known;
- palette family.

Output:

- route variant;
- keyframes;
- per-node mechanics;
- layer list with depth, delay, and treatment;
- warnings for weak media mapping.

### Engagement Hook Composer

Do not invent a new hook-generation policy for Limeglow. Reuse the hook logic from:

`docs/backlog/features/guide-excursions-monitoring/engagementcards.md`

For Limeglow, the engagementcards contract is adapted from static VK cards to motion tags:

- analyze the prepared guide-excursion facts;
- choose the most visually/productively promising excursions, not necessarily all excursions;
- write one short hook per selected visual node;
- prefer one strong question or intrigue phrase over explanatory copy;
- no emoji;
- ideal hook length: `35..65` characters;
- hard maximum for a motion tag: `95` characters;
- each hook must be fact-grounded and must not invent route details;
- output should include the hook type: `big_question`, `contrast`, `place_intrigue`, or `collection_invite`.
- do not replace the engagementcards hook with a generic transition caption
  like "next route" or "we go further"; those may be motion labels only, never
  the excursion hook.

Static engagementcards examples transfer directly into Limeglow motion language:

- `Что скрывает немецкая вилла на тихой улице?`
- `Дом, мимо которого проходят — и почти ничего о нём не знают`
- `Калининград, который не показывают в первый день`
- `Три прогулки, после которых город становится другим`

For the two-excursion debug test, the hook text may be manually seeded from this grammar before full LLM wiring. The important thing is to test how hook tags, title lockups, and route visuals behave together in motion.

### Object Selector

Input:

- one source image;
- excursion title and digest facts;
- desired visual role: `main_object`, `foreground_wipe`, `background_texture`, `route_vector`.

Output:

- extraction mode:
  - `paper_cut_object` for food, vehicles, sculptures, people, and other foreground objects;
  - `building_skyline_cut` for houses, facades, churches, towers, gates, and roof-led architecture;
  - `texture_slab` for photos that should stay as treated rectangular/irregular editorial surfaces;
- semantic object description;
- crop/mask instructions;
- skyline instructions for architecture: likely roofline, sky/background side, useful facade area, and whether wires/branches should be ignored as texture;
- polygon or bbox candidates;
- treatment recommendations;
- confidence.

For the first prototype, bbox/polygon can be approximate and manually inspected. Later, this can be connected to a segmentation model, remove-background tool, or architecture-specific skyline detector.

### Cutout Shape Contract

Limeglow cutouts should look designed, not like raw automatic segmentation.

Preferred final shape:

- large newspaper/magazine cut lines;
- confident convex or mostly-convex silhouette;
- shallow notches are acceptable;
- strong internal concavities should usually be suppressed;
- tiny holes and lace-like alpha detail should be removed unless the object is a person/hair-specific cutout;
- paper edge, subtle off-white backing, grain, or tiny shadow can make a rough cut read as premium rather than sloppy.

Transport-specific rule:

- If a segmentation model produces a good vehicle mask, do not always simplify it into a rough polygon. For trams, buses, cars, and similar objects, a stronger premium treatment may be: keep the good alpha mask, put the vehicle over a flat solid palette field or geometric plate, and use motion/parallax/duotone to make it editorial. A forced coarse hull can make a good vehicle mask look worse.

Architecture-specific rule:

- Architecture should not use generic background removal as the primary path.
  The preferred production direction is semantic sky segmentation:
  `SegFormer/ADE sky class -> keep only the sky component connected to the top
  frame edge -> close wires/holes -> derive the skyline from the lower sky
  boundary -> simplify into a large newspaper polyline -> remove/replace sky`.
- Raw skyline pixels are not the final Limeglow contour. After segmentation,
  run a separate shape-design pass that converts the skyline into sparse,
  confident editorial anchors. This pass should remove pixel noise, cable
  wiggles, tiny roof steps, and accidental jagged marker-like lines.
- For pointed roofs, gates, towers, and gables, the skyline can often become a simple angular polyline.
- For rounded or stepped roof forms, use a denser polygon that approximates the curve in large segments.
- If the whole architecture photo creates a large concave silhouette because buildings sit on both frame edges and the main subject is in the center, do not keep the full skyline. Prefer a `central_skyline_crop`: discard side-edge buildings or walls that create the global concavity, keep the strongest central block, and build a confident paper-cut around that center.
- Side crops may be deliberately stronger than the semantic mask. If an edge
  building creates a global U-shape/concavity, crop it away even if the
  segmentation mask is accurate there.
- Do not chase every tree branch or cable crossing the roof. If branches are visually important, keep them as printed texture inside the facade fragment, not as alpha holes.
- The bottom/side of an architecture cut can be an intentional editorial crop rather than a semantic object boundary.

Recommended architecture model path for Kaggle probes:

- primary: `nvidia/segformer-b5-finetuned-ade-640-640`;
- fallback candidate: `facebook/mask2former-swin-large-ade-semantic`;
- target class: `sky`;
- keep only top-connected sky, so bright signs, windows, white stickers, and reflections are not mistaken for removable sky;
- after semantic sky extraction, the final Limeglow asset should still be a designed paper cut, not the raw segmentation contour.

### Copy Condenser

Input:

- selected excursion facts.

Output:

- short title lockup;
- hook from the Engagement Hook Composer contract;
- date/place/guide label;
- no invented facts.

This must preserve the guide-excursions LLM-first policy. Deterministic fallback may shorten typography only after LLM-produced copy exists.

## Two-Excursion Test Readiness

The first test should optimize for visual sequence and product presentation, not production completeness.

### Electronic Director Contract

The Limeglow renderer must be driven by an electronic director, not by one
hardcoded show. The director creates a seeded variant that satisfies product
constraints:

- input: selected independent excursions, guide cutouts, 1-3 owned object
  artifacts per excursion, engagementcards-style hooks, date/time/place, seed,
  palette family, and allowed motion archetypes;
- output: master shot plan, virtual canvas layout, camera path, per-layer
  depth/delay/treatment, transition directives, and final CTA lockup;
- invariants: readable hook/date/guide, one primary focus per beat, guide is a
  hero layer at least once per selected excursion, speaker treatment is chosen
  from the allowed set (`speaker_echo`, edge crop, color-field hero,
  figure-in-depth, final group), no more than one semantic hero representation
  of the same object in one beat, final lockup is the first place where all
  primary objects may be visible together.

The same input with a different seed may choose a different global route,
palette pair, scene archetypes, speaker treatments, and transition pattern,
but it must keep the product invariants above.

### Camera / Z Model

Keyframes should be sampled from one scene graph and one camera track whenever
we are testing motion logic. Avoid manually scaling individual objects per
frame. Camera zoom applies to every visible layer; depth only changes parallax,
small scale bias, drift, and delay.

Reference formula:

```text
screen = viewport_center + (layer_world - camera_world) * camera.zoom * parallax(depth)
scale  = layer.base_scale * camera.zoom * (1 + depth * 0.055)
```

Permanent diagonal/grid stripes are not part of the design language. Directional
marks should be local to the current motion beat; otherwise every scene reads
as the same diagonal movement.

Use this exact first test scope:

- two excursions: `Амалиенау` with the brick facade, and `Кенигсбергский трамвай - назад в прошлое` with the tram corridor;
- random debug speaker cutouts from `speakers-test-pics/`;
- one hook per excursion using the engagementcards hook contract;
- 4 keyframes minimum:
  - cold open;
  - brick excursion landing;
  - tram excursion landing;
  - final pull-back/CTA;
- optional 5th keyframe: transition bridge where brick geometry or route line pulls the viewer toward the tram corridor.

Generated for `debug-01`:

- `docs/backlog/features/limeglow/payload.debug-01.json` with two concrete
  excursion nodes, hooks, asset paths, camera beats, and seeded debug speaker
  selection;
- `scripts/render_limeglow_keyframes.py`, a local PIL keyframe renderer;
- `artifacts/codex/limeglow-debug-01/keyframes/*.png`;
- `artifacts/codex/limeglow-debug-01/keyframes_contact_sheet.jpg`;
- `artifacts/codex/limeglow-debug-01/scene_plan.json`;
- `artifacts/codex/limeglow-debug-01/world_canvas.png`.

The first generated layout uses:

- `Амалиенау`: brick facade plus the accepted architecture skyline cutout;
- `Кёнигсбергский трамвай`: tram corridor plus the accepted tram palette plate;
- ice cream as a foreground transition/wipe object, not as a third excursion;
- random debug speaker cutouts stored as non-factual visual placeholders.

Still missing after this test:

- approval/refinement of keyframe visual hierarchy;
- final typography/motion-safe sizing after feedback;
- production guide/photo factual binding;
- full mp4 with audio;
- Kaggle runtime;
- Telegram/VK publication.

Not blocking for this test:

- real speaker/guide binding;
- exact ice-cream mapping;
- production guide cutouts;
- full mp4 with audio;
- Kaggle runtime;
- Telegram/VK publication;
- perfect object segmentation.

## Renderer Architecture Proposal

### Local Prototype

Current prototype files:

- `scripts/render_limeglow_keyframes.py`
- `docs/backlog/features/limeglow/payload.debug-01.json`

Current output:

- `artifacts/codex/limeglow-debug-01/keyframes/*.png`
- `artifacts/codex/limeglow-debug-01/scene_plan.json`

The keyframe renderer can start with PIL/OpenCV-style compositing:

- load images;
- create masks/cutouts or use temporary rectangular cutouts;
- for architecture, prefer skyline-led paper cuts over generic foreground removal;
- apply duotone/grayscale/contrast;
- lay out objects using the scene plan;
- render typography;
- export keyframes only.

The first keyframe renderer does not need audio or mp4.

### Full Local Video

Once keyframes are approved:

- extend the renderer to frame sequences at `30fps`;
- use easing and parallax contracts from this doc;
- mux audio from `The_xx_-_Intro.mp3`;
- export a local preview mp4 under `artifacts/codex/`.

The current machine did not have `ffmpeg/ffprobe` installed during this design pass, so full local mp4 rendering will need either:

- project-local venv plus a video library that can bundle/use ffmpeg; or
- local installation/discovery of `ffmpeg`; or
- render PNG frames locally and let Kaggle encode.

### Kaggle Runtime

After local motion is approved:

- create `kaggle/Limeglow/`;
- use a CherryFlash-style session bundle;
- include payload, assets, scripts, and manifest;
- reuse `video_announce.kaggle_client` for upload/push/polling;
- reuse shared story publish helper if publishing is enabled;
- keep Limeglow profile separate from CherryFlash/CrumpleVideo.

## Open Problems

1. Production guide cutouts are currently missing for the debug posts.
   For local keyframes, use random speaker PNGs from `speakers-test-pics/`. Limeglow's production requirement still says each selected excursion needs a real guide photo with background removed and factual source binding.

2. Media-to-excursion mapping is not reliable from digest attachment order.
   The brick facade and tram are reasonably mappable. The ice-cream image is weakly mapped and needs confirmation. Production needs explicit media references per occurrence from the guide digest pipeline.

3. Cutout extraction is not yet production-ready.
   The working direction is not "LLM draws final coordinates". LLM should choose the semantic object and extraction mode; candidate masks may come from BiRefNet/RMBG, SAM/Grounded-SAM, rembg, or local CV; then a deterministic shape-design stage converts the result into a Limeglow paper-cut polygon. For buildings, this includes a dedicated skyline detector rather than generic background removal.

4. Local video tooling is incomplete.
   `ffmpeg/ffprobe` were unavailable in the local shell during this pass. PNG keyframes are unaffected; full mp4 preview needs a tooling step.

5. Typography and brand lockup need a final decision.
   The reference uses news/editorial typography. Limeglow needs its own viewer-facing lockup: likely `Экскурсии недели`, `Маршруты гидов`, or a channel-branded phrase.

6. Audio sync is only provisionally defined.
   CrumpleVideo audio from `1:17` is approved for prototyping, but the motion beats should be checked once a real mp4 preview exists.

7. Publication target and surface are not specified.
   The design assumes Telegram Stories. It is not yet decided whether debug videos should also be sent as ordinary Telegram video messages or VK previews.

## Questions To Resolve Before Full Motion

- Which exact guide/person photos should be used once Limeglow moves from debug cutouts to production source binding?
- Should the brick facade map to a specific `Амалиенау` date/guide, or to the whole repeated `Амалиенау` cluster?
- Which excursion is the ice-cream photo actually meant to represent?
- What final CTA should be used: `Экскурсии недели`, `Новые маршруты гидов`, `Смотреть дайджест`, or a branded channel line?
- Do we want Limeglow to show prices and booking contacts in-story, or only title/date/place/guide and leave details to the digest?
- Should first approval artifacts be still PNG keyframes only, or keyframes plus a rough camera-path animatic without final masks?

## Definition Of Ready For Trial Keyframes

Ready to start `debug-01` keyframe generation when:

- this doc is accepted as the working spec;
- three debug excursions are confirmed or the weak mappings are explicitly accepted as temporary;
- source photos are available locally under artifacts or a noncommitted debug asset folder;
- debug speaker cutouts are available in `docs/backlog/features/limeglow/speakers-test-pics/`;
- the keyframe renderer output path is fixed under `artifacts/codex/limeglow-debug-01/`.

## Definition Of Ready For Full Video

Ready to render a full Limeglow story when:

- keyframes are visually approved;
- object masks/cutouts are good enough for motion;
- scene route plan includes exact timings and easing;
- audio cue and start time are confirmed;
- local frame sequence render is stable;
- story-safe mp4 encoding path is available locally or on Kaggle;
- Kaggle bundle contract is implemented without modifying CherryFlash/CrumpleVideo behavior.
