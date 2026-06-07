# Limeglow Director Plan Debug-08

Рабочий проектный план Codex для следующего прототипа. Канонические
требования остаются в `requirements.md`; этот файл задаёт конкретный
режиссёрский план перед рендером.

Опорные документы:

- `requirements.md`
- `motion-audit-debug07.md`
- `geometry-grammar-library.md`
- `payload.debug-02.json`
- `../guide-excursions-monitoring/engagementcards.md`

## Verdict From Debug-07

`debug-08` нельзя начинать с Blender-сцены. Сначала нужна карта смыслов,
продукта, объектов, геометрии, типографики, движения и памяти зрителя.

Цель `debug-08`: не финальное качество, а доказать, что ролик можно собрать
как продуктовую историю о двух независимых экскурсиях, где зритель успевает
увидеть и запомнить гида, визуальный образ, хук, дату/время и CTA.

## Video Meta

- `format`: vertical story
- `resolution`: `720x1280`
- `fps`: `24`
- `duration`: `16.0s`
- `frames`: `384`
- `render_mode`: local Blender/OpenGL preview first, then frame-by-frame audit
- `audio_debug`: reuse CrumpleVideo track only as tempo bed, no audio-sync
  dependency yet

Why 16s:

- 8s was too fast for two excursions + intro + CTA;
- requirements allow intro, excursion blocks and CTA as separate stages;
- reference holds several scenes for 0.8-2.2s, so each Limeglow excursion
  needs real reading time.

## Debug-08 Pack Artifacts

Concrete packs for the next render have been collected in:

`artifacts/codex/limeglow-debug08-packs/`

Files:

- `input_pack.json`
- `product_pack.json`
- `hook_pack.json`
- `asset_treatment_plan.json`
- `grammar_selection.json`
- `object_map_seed.json`
- `manifest.json`

Pack caveats:

- speaker cutouts are debug placeholders and are not fact-bound to the named
  guides;
- local context does not contain full original excursion descriptions, so
  debug hooks must be revalidated before production;
- ice cream and the Ponart skyline/pipes asset are explicitly not selected for
  this two-excursion render to avoid semantic clutter.

## Debug-08 Fast Render Result

Generated low-cost preview:

- path: `artifacts/codex/limeglow-blender-debug-08/motion_preview_15fps_480p.mp4`
- renderer: `scripts/render_limeglow_blender_debug08.py`
- resolution: `270x480`
- fps: `15`
- duration: `16s`
- frames: `240`
- full frame sequence:
  `artifacts/codex/limeglow-blender-debug-08/frames/frame_0001.png` ...
  `frame_0240.png`
- audit pages:
  `artifacts/codex/limeglow-blender-debug-08/audit/page-01.jpg` ...
  `page-12.jpg`
- render timing: `69s` Blender frame sequence, `3s` ffmpeg post-processing,
  `72s` total.

First-pass visible issues to inspect in motion:

- some large typography can crop during camera travel, especially intro and
  excursion titles;
- hook panels are currently too heavy and can compete with faces/objects;
- tram beat has honest depth but the hook panel may cover too much of the
  visual promise;
- final CTA is stable, but the human/CTA scale balance may still be too small
  for the intended premium lockup.

## Debug-08 v2 Motion Rework

Generated second fast preview after the first critique:

- path:
  `artifacts/codex/limeglow-blender-debug-08-v2/motion_preview_15fps_480p.mp4`
- resolution: `270x480`
- fps: `15`
- duration: `16s`
- frames: `240`
- render timing: `77s` Blender frame sequence, `3s` ffmpeg post-processing,
  `80s` total.

What changed:

- camera is baked frame-by-frame through
  `cubicBezier(0.76, 0.00, 0.24, 1.00)`, instead of relying on Blender's
  default Bezier handles;
- route includes a push-in into the architecture object and a second push-in
  toward the tram visual;
- major object hard visibility windows were removed;
- speaker cutouts were enlarged and lowered so they better exit through the
  bottom frame edge.

v2 finding:

Removing hard visibility windows alone is not enough. It prevents in-frame
pop-in/pop-out, but it exposes too much of the full motion board too early:
future excursion objects become visible during intro and transitions. The next
renderer must use `occluded switch` rather than either hard visibility or
always-visible objects.

### Occluded Switch Rule

Objects may change visibility only while the camera is fully inside an object,
portrait, plate, ticket, route block or foreground wipe that covers the frame.
The viewer should experience this as a push-in transition:

1. camera accelerates into the current primary object;
2. object/plate fills the frame;
3. hidden layout switch happens while covered;
4. camera emerges into the next scene;
5. new primary accent is already present and readable.

No object may simply appear or disappear on an uncovered frame.

## Story Structure

| Scene | Time | Frames | Purpose |
| --- | ---: | ---: | --- |
| `S0 Intro` | 0.00-2.00s | 1-48 | Establish premium excursion digest, big humans/objects, no tiny cards. |
| `S1 Amalienau` | 2.00-6.40s | 49-154 | Make Amalienau memorable: guide + villa/facade visual promise. |
| `S2 Transition` | 6.40-7.20s | 155-173 | Move through shared canvas with architecture/route geometry, no semantic dead zone. |
| `S3 Tram` | 7.20-11.60s | 174-278 | Make tram excursion memorable: guide + tram/rails visual promise. |
| `S4 Digest Bridge` | 11.60-13.40s | 279-322 | Show these are independent choices in one digest. |
| `S5 Outro CTA` | 13.40-16.00s | 323-384 | Pull-back final lockup: read digest / choose excursion. |

## Global Camera Route

Route archetype: `editorial-zigzag-with-two-human-holds`

The route must feel like a camera travelling across a large motion board, not
like slide switches.

1. `Intro push-in`: starts with large partial humans/objects, not small cards.
2. `A human hold`: camera settles on a large guide.
3. `A visual reveal`: camera shifts/zooms to villa/facade object while guide
   remains spatially related.
4. `Architecture-to-route pass`: facade geometry crosses foreground and
   becomes transition.
5. `B human hold`: new guide appears large on dark field, no mandatory plate.
6. `B visual push`: rail diagonals pull camera toward tram object.
7. `Digest pull-back`: camera reveals both guide/object islands as one digest.
8. `CTA settle`: final slow hold, micro-UI still alive.

Camera easing:

- main camera moves use `editorialEase = cubicBezier(0.76, 0.00, 0.24, 1.00)`;
- each move must include visible acceleration, fast travel, and deceleration;
- no default Blender Bezier without explicit curve check;
- every camera move must have a purpose: reveal / connect / transition /
  pull-back.

## Accent Map

This map answers: what should the viewer look at, in what order, and why.
Every beat has one primary accent, one optional secondary accent, and ambient
motion that must not compete with the primary read.

| Time | Frames | Primary accent | Secondary accent | Ambient/support | Memory target |
| --- | ---: | --- | --- | --- | --- |
| 0.00-0.35 | 1-8 | Large cropped people | Dark field | Far `ЭКСКУРСИИ` begins delayed | This is human-led, not a poster grid. |
| 0.35-1.25 | 9-30 | `ЭКСКУРСИИ НЕДЕЛИ` | Human crops | Micro-ui stagger | It is an excursion digest. |
| 1.25-2.00 | 31-48 | Camera direction toward A | Facade hint | Far typography drift | We are entering the first excursion. |
| 2.00-3.25 | 49-78 | Amalienau guide | `АМАЛИЕНАУ` far word | Facade verticals | Remember the guide as person. |
| 3.25-4.35 | 79-104 | Amalienau hook | Guide still present | Window blocks | Understand the question/intrigue. |
| 4.35-5.50 | 105-132 | Facade/villa visual | Guide + fact tag | `ВИЛЛЫ` / `АМАЛИЕНАУ` repeat | Want to see this place. |
| 5.50-6.40 | 133-154 | Guide + facade overlap | Compact date/name | Roofline motion | This guide shows this place. |
| 6.40-7.20 | 155-173 | Transition geometry | `МАРШРУТ`/rails hint | Far word lag | Move to second excursion, no dead zone. |
| 7.20-8.45 | 174-203 | Tram guide | `ТРАМВАЙ` far word | Rail diagonals | Remember the second guide. |
| 8.45-9.55 | 204-229 | Tram hook | Route dots | Far `РЕЛЬСЫ` | Understand the second question. |
| 9.55-10.75 | 230-258 | Tram visual | Guide remains related | Rail depth | Want to see the tram/route. |
| 10.75-11.60 | 259-278 | Guide + tram overlap | Date tag | Route dots settle | This guide shows this route. |
| 11.60-13.40 | 279-322 | Digest bridge: two choices | Two guides | Small visual fragments | These are independent excursions in one digest. |
| 13.40-16.00 | 323-384 | CTA | Two guides | Slow micro-ui, far `ДАЙДЖЕСТ` | Read digest / choose excursion. |

Accent rules:

- primary accent must not change faster than the viewer can name it;
- date/name tags appear only after guide or visual context is established;
- ambient geometry must have lower contrast or slower motion than primary
  accents;
- if the primary accent is text, the camera must not crop it during the hold;
- if the primary accent is a face, no geometry line may cross the face;
- if the primary accent is a visual object, it must be treated as a designed
  object, not a raw rectangle.

## Semantic Layer

### S0 Intro

- Core meaning: "Это не один маршрут, а подборка экскурсий недели".
- Memory words: `ЭКСКУРСИИ`, `ПРОГУЛКИ`, `ДАЙДЖЕСТ`.
- Mood: cultural media opener, dark, confident, high-contrast.
- Must not: show tiny people as the main human read.

### S1 Amalienau

- Core meaning: "район вилл, архитектурная прогулка, тихая городская деталь".
- Memory words: `АМАЛИЕНАУ`, `ВИЛЛЫ`, `ПРОГУЛКА`.
- Visual promise: "гид покажет архитектуру/фасады/район, который хочется
  рассмотреть глазами".
- Hook direction: from engagementcards logic, not only from image. Debug hook
  may be close to `Что скрывает немецкая вилла на тихой улице?`, but it must
  be validated against real excursion copy before production.

### S3 Tram

- Core meaning: "трамвайный маршрут / старый Кёнигсберг / городские рельсы".
- Memory words: `ТРАМВАЙ`, `РЕЛЬСЫ`, `МАРШРУТ`.
- Visual promise: "гид ведёт к истории города через трамвай и маршрут".
- Hook direction: engagementcards-style question tied to actual excursion
  facts: `Куда ведут рельсы старого Кёнигсберга?`

### S5 Outro

- Core meaning: "эти маршруты собраны в дайджесте, дальше нужно читать
  подборку".
- Memory words: `ДАЙДЖЕСТ`, `ЭКСКУРСИИ`, `ВЫБРАТЬ`.
- CTA: `ПРОЧИТАТЬ ДАЙДЖЕСТ` or `ВЫБРАТЬ ЭКСКУРСИЮ`.

## Product Layer

### Debug Data

Current payload uses debug speaker photos with no factual binding. Product
names remain:

- Amalienau: guide text `Игорь Ляшук`, title `Амалиенау`, place
  `Калининград`;
- Tram: guide text `Дина Лях`, title `Кёнигсбергский трамвай`, place
  `Калининград`.

Production note: before real generation, speaker cutouts must be bound to the
actual guide source.

### Product Facts To Show

Each excursion scene must show:

- guide name;
- title/district/theme;
- date/time tag;
- hook;
- visual promise object.

### Date Policy

Debug payload currently has Amalienau date line `7 / 14 / 21 / 28 июня`.
That is visually noisy. For `debug-08`:

- do not render the full multi-date row as a long strip;
- use compact debug tag: `июнь, несколько дат` or choose one occurrence if
  available from the real digest source;
- mark this as a data-normalization requirement before production.

For tram:

- date tag: `27 июня, 17:30`.

## Object Map

All objects below must be represented in the render plan. New geometry or
typography objects should be added to `geometry-grammar-library.md` before
use.

### Shared Objects

| id | role | grammar/treatment | depth | dominance | semantic purpose |
| --- | --- | --- | --- | --- | --- |
| `bg_dark_field` | depth-mass | `D-GEN-01 Dark Negative Space Field` | far | ambient | Premium dark space and contrast. |
| `type_excursions_far` | typography | `T-GEN-01 Giant Topic Word` = `ЭКСКУРСИИ` | far | support | Product frame, depth. |
| `type_digest_far` | typography | `T-GEN-01 Giant Topic Word` = `ДАЙДЖЕСТ` | far | support | CTA context. |
| `micro_orbit_set` | micro-ui | `M-GEN-01 Orbit Icons` | mid/foreground | ambient | Reference-like living detail. |

### S0 Intro Objects

| id | role | grammar/treatment | depth | dominance | entry/exit |
| --- | --- | --- | --- | --- | --- |
| `intro_guide_a_large_crop` | speaker | `clean-cutout-on-dark` | hero | hero | already large at frame 1, exits by camera move. |
| `intro_guide_b_large_crop` | speaker | `clean-cutout-on-dark` | mid/hero | support | partial crop from opposite edge. |
| `intro_visual_facade_fragment` | visual | processed facade crop, no raw rectangle | mid | support | slides behind guide. |
| `intro_rail_diagonal_hint` | geometry | `G-TRAM-01 Rail Diagonals` | foreground | support | quick pass, hints second node. |
| `intro_title` | typography | foreground fact/tag | foreground | hero text | `ЭКСКУРСИИ НЕДЕЛИ`, readable hold. |

S0 note: people may be partially cropped by frame, but not tiny. The first
human read must happen immediately.

### S1 Amalienau Objects

| id | role | grammar/treatment | depth | dominance | semantic purpose |
| --- | --- | --- | --- | --- | --- |
| `a_guide_hero` | speaker | `clean-cutout-on-dark` + optional subtle duotone | hero | hero | Human trust and memory. |
| `a_guide_echo_fan` | speaker effect | animated `echo-fan` | hero/far | support | Reference-style clone beat, not static. |
| `a_facade_visual` | visual | cutout/duotone/paper edge, not raw rectangle | hero-adjacent | hero in visual beat | The thing viewer wants to see. |
| `a_facade_verticals` | geometry | `G-AMA-01 Facade Vertical Rhythm` | far/mid | support | Architecture depth and route. |
| `a_window_blocks` | geometry | `G-AMA-03 Window Blocks` | mid | support | Connect guide to villa/facade. |
| `a_roofline_mask` | transition/visual | `G-AMA-02 Roofline Cut` | foreground | support/transition | Magazine cut and exit bridge. |
| `a_word_far` | typography | `T-GEN-01 Giant Topic Word` = `АМАЛИЕНАУ` | far | support | District memory. |
| `a_word_stack` | typography | `T-GEN-02 Repeated Semantic Stack` = `ВИЛЛЫ` / `ПРОГУЛКА` | far/mid | support | Semantic rhythm. |
| `a_hook` | typography | large readable question | foreground | hero text | Engagementcards-style interest. |
| `a_fact_tag` | typography | `T-GEN-03 Foreground Fact Tag` | foreground | support | `Игорь Ляшук`, compact date. |

### S3 Tram Objects

| id | role | grammar/treatment | depth | dominance | semantic purpose |
| --- | --- | --- | --- | --- | --- |
| `b_guide_hero` | speaker | `side-geometry`, no mandatory color plate | hero | hero | Human trust and memory. |
| `b_tram_visual` | visual | clean alpha/duotone/paper object, no black matte | hero-adjacent | hero in visual beat | Main object viewer wants to see. |
| `b_rail_diagonals` | geometry | `G-TRAM-01 Rail Diagonals` | far/foreground | support | Movement, route, depth. |
| `b_route_dots` | micro-ui/geometry | `G-TRAM-02 Route Node Dots` | mid/foreground | support | Route/stop feeling. |
| `b_ticket_tag` | geometry/tag | `G-TRAM-03 Ticket Punch Blocks` | foreground | support | Date/info tag with transport flavor. |
| `b_word_far` | typography | `T-GEN-01 Giant Topic Word` = `ТРАМВАЙ` | far | support | Theme memory. |
| `b_word_stack` | typography | `T-GEN-02 Repeated Semantic Stack` = `РЕЛЬСЫ` | far/mid | support | Semantic rhythm. |
| `b_hook` | typography | large readable question | foreground | hero text | Engagementcards-style interest. |
| `b_fact_tag` | typography | `T-GEN-03 Foreground Fact Tag` | foreground | support | `Дина Лях`, `27 июня, 17:30`. |

### S4/S5 Digest Objects

| id | role | grammar/treatment | depth | dominance | semantic purpose |
| --- | --- | --- | --- | --- | --- |
| `final_guide_a` | speaker | cutout-on-dark, no individual card | hero | hero | Human-first CTA. |
| `final_guide_b` | speaker | cutout-on-dark, no individual card | hero | hero | Human-first CTA. |
| `final_visual_fragments` | visual | 1-2 small fragments only | mid | support | Remind choices without clutter. |
| `final_type_digest` | typography | `T-GEN-01/T-GEN-02` = `ДАЙДЖЕСТ` | far | support | CTA architecture. |
| `final_cta` | typography | large readable CTA | foreground | hero text | Action. |
| `final_micro_ui` | micro-ui | small orbit/dots | mid/foreground | ambient | Keep scene alive. |

## Motion Map

### Global Easing Tokens

- `editorial_camera`: cubicBezier(0.76, 0.00, 0.24, 1.00)
- `slide_editorial`: cubicBezier(0.65, 0.00, 0.35, 1.00)
- `pop_tag`: cubicBezier(0.34, 1.56, 0.64, 1.00)
- `slow_drift`: easeInOutSine
- `hard_none`: only for hidden cuts behind foreground masks

### Layer Delay Rules

- far typography: delay `4-8` frames, movement factor `0.35-0.50`;
- far geometry: delay `3-6` frames, movement factor `0.45-0.60`;
- mid geometry/cards: delay `1-4` frames, movement factor `0.70-0.90`;
- hero speaker/object: delay `0-2` frames, movement factor `1.00`;
- foreground tags/masks: delay `-1..2` frames, movement factor `1.10-1.30`.

### Echo Rule

Echo is a beat, not a style:

- source portrait visible first;
- clone 1 appears after `3` frames;
- clone 2 after `5` frames;
- clone 3 after `7` frames if used;
- clones fan out backward in depth and sideways;
- settle/hold at least `10-14` frames;
- exit must be by camera move or occlusion, not instant hide.

## Frame-Level Timeline

### S0 Intro: Frames 1-48

Purpose: establish digest and premium language.

Readability:

- `ЭКСКУРСИИ НЕДЕЛИ` readable frames `14-38`;
- large cropped people visible from frame `1`, not tiny.

Motion:

- frames `1-10`: dark field + huge cropped guide silhouettes already present;
- frames `8-20`: `ЭКСКУРСИИ` far word drifts in delayed behind people;
- frames `14-30`: intro title locks, micro-ui appears staggered;
- frames `30-48`: camera pushes toward Amalienau side through facade
  vertical rhythm.

No:

- no tiny speaker cards;
- no all-assets-at-once collage.

### S1 Amalienau: Frames 49-154

#### Beat A1 Guide Hold: Frames 49-78

Focus: large guide cutout, no mandatory color plate.

Readable:

- guide face/silhouette;
- `АМАЛИЕНАУ` far/mid typography appears as topic, not yet all facts.

Motion:

- camera decelerates into guide;
- far `АМАЛИЕНАУ` lags;
- facade verticals drift slowly behind;
- micro-ui minimal.

#### Beat A2 Hook Lands: Frames 79-104

Focus: question hook.

Readable:

- hook: `Что скрывает немецкая вилла?` or validated engagementcards hook;
- no full multi-date row.

Motion:

- hook slides/pops after guide hold;
- guide remains large but shifts to side;
- window blocks appear staggered as support.

#### Beat A3 Visual Promise: Frames 105-132

Focus: processed facade/villa object.

Readable:

- visual object large enough to want to inspect;
- `Игорь Ляшук` tag;
- compact date tag.

Motion:

- facade object reveals through roofline/paper cut;
- guide and facade overlap spatially;
- far typography repeats `АМАЛИЕНАУ` / `ВИЛЛЫ`.

#### Beat A4 Exit Bridge: Frames 133-154

Focus: roofline/facade geometry becomes transition.

Motion:

- `a_roofline_mask` crosses foreground;
- camera follows diagonal/vertical facade rhythm toward B;
- no semantic dead zone.

### S2 Transition: Frames 155-173

Purpose: bridge architecture to route/tram.

Objects:

- `a_roofline_mask` exits;
- `G-TRAM-01 Rail Diagonals` enters;
- far `ПРОГУЛКА` or `МАРШРУТ` word lags behind.

Motion:

- fast but short editorial pass;
- foreground mask hides any visibility switch;
- max transition-only duration: `19` frames.

No:

- no ice cream unless tied to a real excursion fact;
- no empty dark travel.

### S3 Tram: Frames 174-278

#### Beat B1 Guide Hold: Frames 174-203

Focus: large guide cutout, side geometry.

Readable:

- guide is large;
- `ТРАМВАЙ` far word starts as topic.

Motion:

- camera settles from rail transition;
- side rail geometry gives depth;
- guide has no colored portrait card.

#### Beat B2 Hook Lands: Frames 204-229

Focus: tram hook.

Readable:

- `Куда ведут рельсы старого Кёнигсберга?`

Motion:

- hook appears after guide is understood;
- route dots appear staggered like stops;
- far `РЕЛЬСЫ` typography lags.

#### Beat B3 Visual Promise: Frames 230-258

Focus: tram object, no black matte.

Readable:

- tram visual large;
- guide remains related;
- date tag `27 июня, 17:30`.

Motion:

- rail diagonals pull camera into tram;
- tram object may exceed frame edges;
- ticket/date tag pops after tram is visible.

#### Beat B4 Object/Human Overlap: Frames 259-278

Focus: "guide will show this route".

Readable:

- guide + tram + date/title together;
- no repeated duplicate tram.

Motion:

- camera begins pull-back prep;
- route dots and rail lines settle.

### S4 Digest Bridge: Frames 279-322

Purpose: make the two excursions feel like independent choices in one digest.

Objects:

- both guides visible on one dark field, large enough;
- visual fragments: one facade fragment + one tram fragment, small/supporting;
- semantic typography: `ЭКСКУРСИИ`, `ДАЙДЖЕСТ`, maybe `2 ПРОГУЛКИ`.

Motion:

- camera pulls back;
- far typography lags;
- guides do not appear via hard switch; they are revealed by camera or
  occlusion.

No:

- no cluttered object grid;
- no four guides;
- no tiny unreadable speaker cards.

### S5 Outro CTA: Frames 323-384

Purpose: action and memory.

Readable:

- CTA readable frames `330-384`;
- people visible and grounded/cropped through lower frame;
- optional line: `в дайджесте экскурсий`.

Motion:

- final camera settle;
- micro-ui slow drift only;
- no new product facts introduced after frame `350`.

CTA copy candidates:

- `ПРОЧИТАТЬ ДАЙДЖЕСТ`
- `ВЫБРАТЬ ЭКСКУРСИЮ`
- `ВСЕ МАРШРУТЫ В ДАЙДЖЕСТЕ`

## Asset Treatment Requirements

### House / Amalienau

Current raw house photo is not acceptable as-is.

Needed treatment before render:

- crop to strong architectural fragment;
- use mask/cutout or journal-card with intentional paper edge;
- duotone/contrast treatment to remove ordinary-photo feeling;
- add `G-AMA-01/G-AMA-03` geometry so it reads as designed architecture.

### Tram

Current tram palette plate is not acceptable as-is if black matte remains.

Needed treatment before render:

- clean alpha around tram;
- remove/replace black rectangle;
- if using a card, make the card intentionally designed, not accidental;
- combine with `G-TRAM-01` rails and `G-TRAM-02` route dots.

### Speakers

Needed treatment:

- at least two scenes with large guide cutouts without color plates;
- optional monochrome/duotone consistency;
- echo fan only as animated beat;
- people can crop off lower edge and side edges;
- faces must not be cropped when guide identity is the beat.

## Debug-08 Render Acceptance Checklist

Before accepting a render:

- full frame-by-frame sheet generated;
- no assessment by sparse keyframes only;
- each scene has object map entries visible;
- each excursion has guide hold, visual hold, guide+visual overlap;
- hook readable for enough frames;
- title/place anchor visible per excursion;
- CTA appears through pull-back or reveal, not hard switch;
- no raw photo rectangle;
- no black matte around tram;
- echo appears over time;
- geometry and typography objects have their own motion;
- people are large in at least two beats;
- viewer can name both excursions after one watch.

## Regular Production Process

After a visually acceptable prototype, Limeglow should not rely on manual
scene design. The regular mode should be a reproducible LLM-assisted director
pipeline with deterministic validation.

### Production Goal

For every new guide-excursion digest, produce a fresh but style-consistent
story video:

- different route and grammar choices by seed;
- hooks grounded in the excursion data and engagementcards logic;
- clear product meaning per excursion;
- controlled camera and object motion;
- frame-by-frame readability validation before publishing.

### Production Principle

LLM designs, deterministic code enforces.

The production system should not ask a renderer to "make something stylish".
It should ask several small LLM/VLM stages to produce explicit, grounded
planning artifacts, then compile those artifacts into a constrained render
plan. The renderer must reject plans that break product, motion or readability
contracts.

Non-negotiable contracts:

- source facts stay grounded in digest/excursion data;
- every scene has an accent map before render;
- every excursion has guide, visual object, hook, date/time and place/title;
- every visual object has a treatment plan before entering Blender/Remotion;
- every motion object has depth, easing, entry, hold and exit;
- every render gets frame-by-frame audit, not sparse-keyframe approval.

### LLM Director Roles

The industrial version should split "electronic director" into roles. These
roles may be separate prompts, separate agents, or one model called in small
stages, but their outputs must be persisted separately.

| Role | Main question | Output |
| --- | --- | --- |
| `Product Editor` | What should the viewer understand and remember about each excursion? | `product_pack.json` |
| `Hook Editor` | Which engagementcards-style question is grounded and catchy? | `hook_pack.json` |
| `Visual Editor` | Which image/guide asset carries the visual promise, and how should it be treated? | `asset_treatment_plan.json` |
| `Grammar Curator` | Which reusable geometry/typography grammars support this meaning? | `grammar_selection.json` |
| `Motion Director` | How does the camera travel through the board and where are the accents? | `director_plan.json` |
| `Readability Critic` | Can a viewer actually read, see and remember the story? | `audit_report.json` |

The `Motion Director` must always produce an explicit `accent_map`. Without it,
render is blocked.

### Accent Contract For Production

Every beat must declare:

- primary accent: exactly one object or text block;
- secondary accent: optional, never competing with primary;
- ambient layer: geometry/typography/micro-ui that supports depth;
- memory target: what the viewer should retain after this beat;
- minimum readable frames for text accents;
- face-safe / object-safe crop rules for visual accents.

Example beat contract:

```json
{
  "scene_id": "S1",
  "beat_id": "A2_hook",
  "frames": [79, 104],
  "primary_accent": "a_hook",
  "secondary_accent": "a_guide_hero",
  "ambient": ["a_window_blocks", "a_word_far"],
  "memory_target": "The viewer understands the Amalienau intrigue.",
  "readable_min_frames": 18,
  "forbidden": ["camera_crop_text", "geometry_crosses_face", "new_fact_after_hold"]
}
```

Renderer/compiler validation:

- primary accent exists and is visible during the declared frame range;
- text primary accent has enough on-screen frames and is not cropped;
- human primary accent has face-safe framing;
- visual primary accent is not a raw rectangle or black matte;
- ambient objects move slower or lower-contrast than the primary accent;
- accent handoff between beats is visible and motivated.

### Production Artifact Chain

A regular run should create a traceable chain of planning files:

1. `input_pack.json`: selected digest/excursion records, guide data, dates,
   source text and asset refs.
2. `product_pack.json`: factual product meaning and viewer memory targets.
3. `hook_pack.json`: engagementcards-style hooks grounded in source text.
4. `asset_treatment_plan.json`: VLM/CV decisions for speakers and visual
   objects.
5. `grammar_selection.json`: selected objects from
   `geometry-grammar-library.md`.
6. `director_plan.json`: story structure, object map, accent map, camera route,
   motion map and CTA.
7. `render_manifest.json`: deterministic compiled render scene.
8. `frame_audit_report.json`: frame-by-frame visual/readability audit.
9. `publish_manifest.json`: final file path, target platform and run metadata.

Debug artifacts can live in `artifacts/codex/limeglow-<run-id>/`. Production
metadata should be persisted in the future production storage layer, but the
same artifact chain should remain inspectable.

### Limits And Model Budgeting

All LLM/VLM calls in regular mode must go through the project limit-control
framework, not direct ad-hoc calls.

Suggested budget split:

- cheap text model: product brief, hook variants, grammar selection;
- stronger text model: final director plan and critic pass;
- VLM only where images matter: asset selection, crop/mask/treatment review;
- segmentation/CV models for masks after VLM selects the target object;
- no repeated VLM calls for unchanged assets; cache by asset hash and prompt
  version.

Each stage should declare:

- model family;
- max calls;
- max tokens;
- cache key;
- fallback behavior;
- reason why the call is needed.

The pipeline should fail gracefully when the budget is exhausted: save the last
valid planning artifact, mark the run incomplete, and avoid partial publishing.

### Pipeline Stages

#### Stage 0. Input Pack

Deterministic task:

- assemble selected digest/excursion facts;
- attach guide names, dates, titles, places and source descriptions;
- attach speaker cutouts and visual image candidates;
- attach existing engagementcards hook data when available;
- attach previous cached asset analyses by hash.

Output JSON:

```json
{
  "run_id": "...",
  "seed": 12345,
  "target_platform": "telegram_story|vk_story",
  "excursions": [
    {
      "id": "...",
      "title": "...",
      "guide_name": "...",
      "date_time": "...",
      "place": "...",
      "source_description": "...",
      "speaker_assets": ["..."],
      "visual_assets": ["..."],
      "engagementcard_hooks": ["..."]
    }
  ]
}
```

Validation:

- no missing guide/title/date/place;
- every excursion has at least one speaker asset and one visual asset;
- no fabricated facts.

#### Stage 1. Candidate Selection

Input:

- guide digest occurrences;
- guide profiles / speaker images;
- excursion photos;
- existing digest copy and facts;
- available grammar library entries.

LLM task:

- choose 1-N strongest excursions for video;
- score visual potential, hook potential, guide/photo availability, date
  clarity, product diversity;
- reject excursions without both guide cutout and usable visual image.

Output JSON:

```json
{
  "selected_excursions": [
    {
      "id": "...",
      "selection_reason": "...",
      "visual_promise": "...",
      "hook_potential": 0.0,
      "asset_risk": "low|medium|high"
    }
  ]
}
```

Deterministic validation:

- has guide cutout;
- has at least one visual image;
- has future date/time;
- no more than target count;
- asset risk not high unless debug override.

#### Stage 2. Semantic/Product Brief

LLM task:

- create a compact semantic brief per excursion;
- produce hook candidates from actual facts, not from image-only guessing;
- extract memory words for semantic typography;
- define what the viewer must remember.

Output JSON:

```json
{
  "excursion_briefs": [
    {
      "id": "...",
      "core_meaning": "...",
      "visual_promise": "...",
      "hook": "...",
      "memory_words": ["..."],
      "must_remember": {
        "guide": "...",
        "title": "...",
        "date": "...",
        "place": "..."
      }
    }
  ]
}
```

Validation:

- hook is a question or short intrigue;
- no unsupported facts;
- title/place/date present;
- memory words are short and renderable.

#### Stage 3. Asset Treatment Plan

LLM/Vision task:

- inspect candidate images;
- decide visual-object treatment: cutout, duotone, paper edge, solid plate,
  skyline/roofline, crop;
- identify dominant object and safe crop;
- propose object-mask/cutout strategy.

Output JSON:

```json
{
  "asset_treatments": [
    {
      "asset_id": "...",
      "role": "speaker|visual_object|foreground_mask",
      "treatment": "cutout|duotone|paper_card|solid_plate|roofline_cut",
      "crop_intent": "...",
      "mask_strategy": "...",
      "avoid": ["black_matte", "raw_rectangle"]
    }
  ]
}
```

Validation:

- no black matte;
- no raw rectangle unless intentionally designed;
- face-safe crop for guide;
- visual object has a clear dominant area.

#### Stage 4. Grammar Selection

LLM task:

- choose named entries from `geometry-grammar-library.md`;
- map each selected grammar to excursion meaning;
- add new grammar entry only if existing library is insufficient.

Output JSON:

```json
{
  "grammar_selection": [
    {
      "scene_id": "S1",
      "entries": ["G-AMA-01", "T-GEN-02", "M-GEN-01"],
      "reason": "architecture/facade memory and depth"
    }
  ]
}
```

Validation:

- at least 1 large geometry grammar per excursion;
- at least 1 semantic typography grammar per excursion;
- no decorative-only grammar without semantic role.

#### Stage 5. Director Plan

LLM task:

- build story structure, scene durations, accent map, object map, camera route;
- define intro, each excursion, transitions, digest bridge, CTA;
- assign primary/secondary/ambient accents per beat.

Output JSON:

```json
{
  "video_meta": {"duration_sec": 16, "fps": 24},
  "story_structure": [...],
  "accent_map": [...],
  "object_map": [...],
  "camera_route": [...],
  "motion_map": [...]
}
```

Validation:

- every beat has one primary accent;
- every excursion has guide hold, visual hold, guide+visual overlap;
- hooks readable long enough;
- CTA hold long enough;
- no semantic dead zones.
- no beat may introduce product facts without a readable hold;
- no speaker may appear tiny when identity is the primary accent;
- no scene may use only one depth plane unless it is a deliberate hard graphic
  transition under `0.5s`.

#### Stage 6. Motion Curve Audit Before Render

Algorithmic task, not LLM-only:

- check camera keyframes for acceleration/travel/deceleration;
- ensure object delays are plausible;
- ensure visibility switches are hidden by camera/occlusion/wipe;
- flag any object that appears/disappears in-frame without reason.

Output:

```json
{
  "motion_preflight": {
    "ok": true,
    "warnings": []
  }
}
```

#### Stage 7. Render And Frame Audit

Render:

- low-fps/low-cost preview first;
- generate full frame-by-frame sheet;
- compute frame diff / optical flow metrics as diagnostics;
- run OCR/readability checks where possible.

LLM/Vision review task:

- compare frame sheet against director plan;
- identify missed accents, unreadable hooks, clutter, broken depth, bad asset
  treatment.

Validation:

- frame-by-frame audit required;
- sparse keyframe approval forbidden;
- if viewer cannot name each excursion after one watch, fail.

### Variant Generation

Regular production should create a slightly different motion world for every
digest, but not by random chaos. Variation is parameterized:

- `seed`;
- route archetype;
- intro archetype;
- speaker treatment;
- image treatment;
- grammar entries;
- palette;
- typography scale;
- transition grammar;
- CTA lockup style.

The seed may change look and route, but it must not change product contracts.
For example, it may choose `echo-fan` for one guide and `side-geometry` for the
other, but it cannot remove the guide hold, hook hold, visual promise or
date/time read.

### Industrial Quality Gates

The run cannot proceed to final render unless all gates pass:

| Gate | Checks |
| --- | --- |
| `facts` | guide/title/date/place/hook grounded in input data. |
| `product` | each excursion has a clear memory target and visual promise. |
| `assets` | no raw accidental rectangles, no black matte, no face-breaking crop. |
| `grammar` | geometry/typography objects have semantic role and depth. |
| `accent` | every beat has primary accent, readable hold and non-competing support. |
| `motion` | camera route has purposeful moves, easing and no in-frame hard pops. |
| `depth` | each main scene has far/mid/hero or foreground layers. |
| `readability` | hooks/CTA/date/name readable for declared frame counts. |
| `memory` | after one watch, viewer can name the two excursions and why they matter. |

### Reproducibility

Every production run should persist:

- input digest IDs and occurrence IDs;
- selected assets and treatments;
- LLM prompts/model/version;
- grammar entries selected;
- seed;
- director plan JSON;
- render manifest;
- frame audit report;
- final video path and publication targets.

### Variation Controls

Variation should come from:

- route archetype;
- grammar selection;
- palette;
- speaker treatment;
- intro archetype;
- transition grammar;
- CTA lockup style.

Variation must not break:

- product readability;
- guide/object/date/hook presence;
- motion clarity;
- frame-by-frame audit.

## Open Questions Before Production

- Which real speaker photo maps to each guide?
- What is the exact occurrence/date policy for multi-date excursions?
- Should debug-08 be 16s, or should production target 18-22s for more than two
  excursions?
- Which engine remains best after planning: Blender, Remotion, or hybrid?

For debug-08 planning, use Blender because local rendering is already proven,
but keep the object/timeline plan engine-agnostic.

## Debug-08 v3 Asset Prep And Render

Source recovery:

- restored the two source VK photos from `vk.com/uhtykaliningrad` through the
  read-only service key, without using Telegram/Kaggle auth bundles;
- local source files:
  - `artifacts/codex/limeglow-vk-debug-assets/m238875824_8_photo2_m238875824_457239024.jpg`;
  - `artifacts/codex/limeglow-vk-debug-assets/m238875824_12_photo4_m238875824_457239030.jpg`.

Kaggle cutout probe:

- kernel path: `kaggle/LimeglowCutoutProbe/`;
- Kaggle kernel: `zigomaro/limeglow-cutout-probe`;
- output path:
  `artifacts/codex/limeglow-kaggle-cutout-probe-v3/limeglow_cutouts/`;
- output files include:
  - `preview_sheet.jpg`;
  - `cutout_report.json`;
  - `amalienau_brick_house_exact_cutout.png`;
  - `amalienau_brick_house_paper_object.png`;
  - `tram_corridor_exact_cutout.png`;
  - `tram_corridor_paper_object.png`.

Kaggle prep findings:

- tram mask is good enough for motion debugging: it preserves the real tram and
  a thin vertical pole/wire artifact;
- architecture mask still selects a window/facade fragment, not a full house or
  skyline. For debug-08 it can be used honestly as an editorial architectural
  fragment, but production needs a stronger architecture-specific mask strategy
  if the intended promise is the whole facade/roofline;
- this confirms that `asset_prep_status` must be a hard preflight gate:
  renderer must not silently replace missing masks with graphic placeholders.

Render v3/v3b:

- v3 path:
  `artifacts/codex/limeglow-blender-debug-08-v3/motion_preview_15fps_480p.mp4`;
- v3b path:
  `artifacts/codex/limeglow-blender-debug-08-v3b/motion_preview_15fps_480p.mp4`;
- v3b timing:
  - Blender render: `62s`;
  - ffmpeg post-processing: `3s`;
  - render+post total inside container: `65s`;
  - full wall-clock including container bootstrap: `118s`.

What v3b improves over v2:

- intro no longer shows the first guide already duplicated into echo copies;
- `echo` now appears as a separate beat after the guide identity is established;
- restored real VK visual objects and Kaggle cutouts are used instead of a
  graphic tram placeholder;
- Amalienau now uses a prepared paper/facade object, not the raw full photo;
- guide/date labels are split into larger blocks:
  - `ИГОРЬ ЛЯШУК` + `14 ИЮНЯ`;
  - `ДИНА ЛЯХ` + `27 ИЮНЯ 17:30`;
- far semantic words are no longer globally visible for the whole roll;
- background render works in Docker without `xvfb-run` by falling back from
  OpenGL preview to normal background Workbench rendering.

Remaining v3b defects:

- transition occluder is no longer a white blank field, but still reads as a
  service-like grey wipe. It should become a push-in into the actual
  architecture fragment, a dark typographic plate, or another designed object;
- tram foreground rail line is still too dominant and crosses the hero object.
  Next version should replace one giant diagonal with several depth-separated
  rail/route grammar objects;
- tram hero is real, but still too small during the readable date/name beat;
- the Amalienau object is a window/facade fragment, not a full architectural
  promise. This may be acceptable only if the scene is framed as detail of
  Amalienau, not as whole-house reveal;
- the scene still needs a stricter automatic audit for `primary_accent_visible`
  and `readable_text_frames`, not just manual contact sheets.

Next gate before another render:

1. Replace the grey occluded switch with a designed push-in transition.
2. Redesign rail/route geometry so it supports depth without crossing faces or
   the tram hero.
3. Increase the tram/date/name hold and scale.
4. Decide whether Amalienau should use this window fragment or needs a new
   architecture-specific mask/crop pass.
5. Persist `asset_prep_status` in the packs and fail the render if primary
   visual objects are missing or downgraded to fallback.

## Debug-08 v4 Direction From Operator Review

Operator review of `motion_preview_15fps_480p.mp4` after v3b:

- intro speakers still read as floating, because their bodies do not leave
  through the lower frame strongly enough;
- the blue/yellow stripes/rails still read like unexplained service graphics;
- intro speaker copies disappear too late during the flight to the first beat;
  if they remain during travel, they need an opacity fade;
- echo must be a separate beat: one photo first, then two echo copies split out
  with its own dynamic easing. It must not be only parallel background motion;
- geometry grammar must be farther back and less close to the hero plane;
- hard object disappearance remains visually cheap; important objects need
  fade/eased removal or must be hidden only while covered;
- the current Amalienau mask is a bad whole-house result: it is only a window
  or facade fragment. Do not spend this iteration fixing it, but do not treat
  it as a solved architectural object;
- the most important product failure is still reading time: viewer does not
  reliably retain guide + concrete date.

Third requirements addition:

- photos may also stay as full-scene background plates rather than cutout
  collage objects;
- background photo treatment: large enough to fill the full story frame before,
  during and after the push; initially far / misty / low-contrast, then brought
  forward by push-in, then pushed back into haze;
- edges should be hard to perceive, ideally blurred or pushed outside the frame;
- possible processing: stylish black-and-white, duotone, color accents.

v4 render decision:

- keep v3b artifacts as-is;
- add renderer variant `tram_background`;
- render v4 to a separate output folder;
- use the real restored tram corridor photo as a full-frame atmospheric
  background plate for the second excursion;
- remove or heavily reduce rail stripes in the background-photo variant;
- extend debug duration to `22s` at `15fps`, because the current `16s` pass
  does not leave enough space for two independent guide/date/product reads;
- lower intro speakers and fade them out during the flight to Amalienau;
- make echo a staged beat between guide identity and hook;
- split Amalienau/tram guide/date/title windows so date and guide remain
  visible for materially longer than in v3b.

## Debug-08 v5/v6 Background Tram Iteration

Rendered artifacts:

- v5: `artifacts/codex/limeglow-blender-debug-08-v5-bgtram/motion_preview_15fps_480p.mp4`;
- v6: `artifacts/codex/limeglow-blender-debug-08-v6-bgtram/motion_preview_15fps_480p.mp4`;
- v6 render timing: `DOCKER_TOTAL_SECONDS=150` for 330 frames at 270x480,
  15fps, 22 seconds.

What changed in v5/v6:

- `requirements.md` is now tracked in git so the restored user-authored
  requirements are no longer an untracked local file;
- renderer now supports explicit variants:
  - `collage_cutout`;
  - `tram_background`;
- `tram_background` keeps the tram corridor photo as a large full-scene
  background plate, initially misty/low-contrast, then stronger during the
  push-in, then fading back;
- blue/yellow foreground route stripes are removed from the background-photo
  variant;
- small geometry/window blocks are pushed farther back and reduced in opacity;
- intro speakers are lower/larger, but still need a stricter rule that the
  portrait bottom must leave through the lower frame;
- first excursion echo is separated from the hook more clearly than v3b/v4;
- hook cards now include concrete product meta:
  - `14 ИЮНЯ · ИГОРЬ ЛЯШУК`;
  - `27 ИЮНЯ 17:30 · ДИНА ЛЯХ`;
- product date/name blocks are larger and live longer.

Manual audit after v6:

- tram background treatment is directionally successful: the photo reads as
  environment/depth rather than a pasted rectangle;
- foreground service-like diagonal stripes are gone in the tram-background
  variant;
- first-excursion product meta is more visible inside the hook beat, but the
  guide portrait still competes with the hook and the architecture object;
- Amalienau visual object remains a known weak asset: it is still a window /
  facade fragment, not a solved house/skyline cutout;
- the second excursion date is readable in more frames than before, but it is
  still too close to a subtitle. A production-grade version needs a dedicated
  product hold where guide + concrete date + destination are the main accent;
- intro speakers are lower but not yet reliably "grounded" by the bottom edge.

Next implementation rule:

- every excursion gets three distinct beats:
  1. guide/human attraction;
  2. hook + visual promise;
  3. product hold with guide name, single concrete date/time, and route/title as
     the primary accent;
- echo and other speaker treatments must be a separate beat with their own
  easing, not a decoration layered on top of the hook;
- background-photo treatment should remain available as a reusable asset mode,
  not replace cutout collage mode globally.
