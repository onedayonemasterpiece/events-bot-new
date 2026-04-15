# CherryFlash / Popular Video Afisha

> **Status:** In active implementation / Not confirmed by user  
> **Scope:** daily popularity-driven video announce mode for `/v` ("Быстрый обзор") with automatic publication to `@keniggpt` in phase 1.
>
> **Naming + product boundary**
> - marketing / product name: `CherryFlash`
> - product description: `Popular Video Afisha`
> - internal mode key: `popular_review`
> - this is a separate daily video product and must not modify, replace, or destabilize the existing `CrumpleVideo` runtime;
> - canonical docs and Kaggle runtime for this product now live under `CherryFlash`, not under historical `CrumpleVideo` naming.

## Canonical routes

- Current `/v` feature: `docs/features/crumple-video/README.md`
- Current `/v` rollout/tasks: `docs/features/crumple-video/tasks/README.md`
- Popularity signal and `/popular_posts`: `docs/features/post-metrics/README.md`
- Scheduler policy: `docs/operations/cron.md`

## Operator launch contract

- `/v` must expose CherryFlash as a direct one-click operator action, not only as a generic profile hidden behind the manual selection flow.
- The dedicated CherryFlash button must call `VideoAnnounceScenario.run_popular_review_pipeline()` directly.
- The legacy `vidstart:popular_review` route must resolve to the same direct pipeline and must not create a generic manual-selection session for `popular_review`.
- `/v` may still keep a separate `⚙️ Каналы` path for CherryFlash channel configuration, but launch and configuration must be distinct actions.

## Product identity

- `CherryFlash` is the user-facing / marketing name of the daily popularity-driven video product described in this document.
- `Popular Video Afisha` is the descriptive product family label for docs and operator communication.
- `popular_review` remains the canonical internal mode / profile key in code and session metadata.
- planned canonical Kaggle runtime path for this product:
  - `kaggle/CherryFlash/`
- current notebook delivery path:
- the launcher mirrors the working Telegram/Telegraph Kaggle pattern: it creates a one-run Kaggle dataset with a unique slug, uploads the full mounted CherryFlash runtime tree (`mobilefeed_intro_still.py`, `scripts/`, `assets/`) in that single `CreateDataset` step, waits until Kaggle reports that dataset ready and the mounted runtime files are visible, then waits for kernel metadata to bind that dataset through `dataset_sources`, and the notebook runs `scripts/render_mobilefeed_intro_scene1_approval.py` from the mounted tree.
  - the runtime dataset must be populated from the real `/popular_posts` pool for that run, not from a stale baked poster pack: launcher-side prep writes a fresh `assets/cherryflash_selection.json` plus poster files under `assets/posters/`, so the mounted Kaggle render uses the same current event payload that was selected before upload.
  - the Kaggle notebook must resolve the runtime source from the newest mounted top-level `cherryflash-session-*` bundle and must not pick the first recursive `scripts/render_cherryflash_full.py` match anywhere under `/kaggle/input`, because older still-mounted session datasets may coexist with the new one during manual reruns or debugging.
  - the Kaggle notebook must reuse the same shared story-helper dependency contract as `CrumpleVideo`: if `kaggle_common/story_publish.py` is shipped in the bundle, the notebook environment must already install its runtime dependencies (`opencv-python`, `requests`, `telethon`, `cryptography`) instead of introducing a CherryFlash-only conditional bootstrap path.
  - the Kaggle notebook must also reuse the same thread-safe async bridge pattern as the working `CrumpleVideo` papermill runtime when calling story preflight/publish helpers; nested `asyncio.run()` or direct `new_event_loop().run_until_complete(...)` from inside the notebook cell are not allowed because papermill already executes under a running event loop.
- CherryFlash keeps the one-run unique dataset naming pattern, and the upload contract is now explicitly single-step: the full runtime bundle must go through one `CreateDataset` call for that unique slug; the old bootstrap `CreateDataset` + `CreateDatasetVersion` sequence is no longer canonical because it can stall on prod before any visible `kernels_push`, leaving CherryFlash with no actual Kaggle run.
  - CherryFlash runtime prefetch must fail fast on unstable poster hosts such as `files.catbox.moe`: remote poster prefetch is a best-effort acceleration step, not a mandatory gate before Kaggle upload, so dead Catbox URLs must not burn multiple 20-second retries before the run can even reach Kaggle.
  - the selection manifest must preserve `poster_candidates`, not only a derived `poster_file` basename: if launcher-side prefetch could not materialize a local poster into `assets/posters/`, the Kaggle intro runtime must still be able to resolve the same event artwork from the original remote candidate list instead of crashing on a synthetic missing local filename.
  - CherryFlash selection itself must be renderability-aware before dataset upload: an event may enter the runtime bundle only if it already has at least one non-`files.catbox.moe` poster URL or if a source-specific rehydrate recovers one (`t.me` public-page poster fallback or production `VK wall.getById` poster fetch); catbox-only events that still have no renderable poster after that rehydrate must be dropped from selection instead of being pushed into Kaggle with a guaranteed missing `assets/posters/*` dependency.
- canonical local launch path for live runs:
  - `scripts/run_cherryflash_live.py`
  - this runner must start CherryFlash through the same scheduled scenario path as production (`_run_scheduled_popular_review` -> `VideoAnnounceScenario.run_popular_review_pipeline()`), keep the event loop alive until a terminal session status, and must not bypass the scenario by pushing Kaggle notebooks manually.
  - `--no-wait` is not a valid CherryFlash live-run mode: `start_render()` hands the real Kaggle push to a background asyncio task, so exiting the runner immediately after session creation kills the render before any visible Kaggle kernel run can appear.
  - for stale local prod snapshots, the runner may reset an already stuck `RENDERING` video session only through its explicit `--force-reset-rendering` flag before the new CherryFlash start.
  - Kaggle dataset/kernel API failures on this path must surface the raw Kaggle response body in logs instead of a bare `400/403`, so auth and dataset-contract incidents are diagnosable without a second repro pass.
- Product runtime split:
  - candidate selection comes from the `/popular_posts`-style popularity pool with weekly anti-repeat;
  - CherryFlash selection is `future-start-only`: events whose `start date` is already before the current local day must not appear in the ribbon/date strip even if they still have a later `end_date`;
  - intro comes from the new `Мобильная лента` / `MobileFeed Intro` 3D block;
  - main 2D scene flow follows `kaggle/VideoAfisha/video_afisha.ipynb`;
  - the runtime selection manifest for the intro must carry raw machine-readable event fields (`date_iso`, `time`, `city`, `location_name`) in addition to viewer-facing display text, so the intro date strip never falls back to the old sample April cluster when a real CherryFlash selection exists;
  - intro poster loading must accept both mounted local poster filenames and remote fallback candidates from the same selection manifest, because real CherryFlash runs may contain a mix of successfully prefetched posters and runtime-only remote poster URLs;
  - the existing `CrumpleVideo` daily pipeline remains a separate product and must not be regressed by this work.

## Current state against the request

## Requirements confirmation gate: 2026-04-13 selection/runtime delta

This delta records the latest mismatch found after reviewing the broken CherryFlash full runs and the user reports about past dates in the intro.

### Already present

- The canon already requires the low-event expansion path:
  - when there are `<=3` base events and a selected event has a second image, CherryFlash may add one follow-up scene with `soft-move-left` and a short description in the lower text block;
  - current implementation status: code path exists, but the user has not yet confirmed it on a full live run.
- The canon now also requires a renderability gate before CherryFlash bundle upload:
  - selection may reuse existing direct poster URLs as-is;
  - if the stored event media is catbox-only, CherryFlash may only keep that event after source-specific rehydrate finds a non-catbox poster;
  - approved reuse paths are the existing public Telegram poster fallback and the production `VK wall.getById` poster fetcher, not a new CherryFlash-only downloader.

### Needs clarification

- `Future events only` is now interpreted as a start-date rule for CherryFlash selection:
  - events starting `today` are still allowed;
  - events whose start date is already before `today` must be excluded even if they are technically ongoing through `end_date`;
  - this rule applies to the CherryFlash popularity product and its intro/date-strip payload.

### Missing

- The launcher/runtime contract needed an explicit safety rule:
  - after pushing the CherryFlash kernel update, the server must wait until Kaggle metadata really shows the new `dataset_sources` bound to the kernel before treating the run as started;
  - otherwise a rerun may still execute against an older mounted `cherryflash-session-*` bundle and silently reproduce stale video defects.
- The notebook/runtime contract needed an explicit parity rule with `CrumpleVideo`:
  - CherryFlash must not invent a separate story-helper bootstrap that can fail earlier than render;
  - the same shared `story_publish.py` helper may stay mounted but the notebook environment must satisfy its imports up front, while actual story publish remains controlled only by config/preflight availability;
  - the notebook-side async bridge for story preflight/publish must stay papermill-safe and match the proven `CrumpleVideo` thread-runner pattern.

## Requirements confirmation gate: 2026-04-12 full-run delta

This delta records the latest requirement check after the user accepted moving beyond the `intro + scene1` approval artifact and requested the full daily product path.

### Already present

- The canon already fixes CherryFlash as a separate daily `popular_review` product with:
  - `/popular_posts`-driven candidate sourcing;
  - phase-1 publication to `@keniggpt`;
  - story autopublish disabled by default;
  - readiness measured by `12:30 Europe/Kaliningrad`, not by a hardcoded final public story slot yet.
- The canon already fixes the rollout boundary that Blender owns only the intro block while the main scene flow stays in the `VideoAfisha` 2D family.
- The canon already fixes the minimum/maximum editorial payload:
  - `2..6` events per release;
  - skip the run instead of publishing a fake single-event video.

### Needs clarification

- The new low-event expansion rule needs one implementation-level interpretation lock:
  - the user confirmed `soft-move-left` for the extra follow-up beat and confirmed that the text should switch from `title + date/time` to a short description;
  - the canon still needed an explicit statement that the description continues to live in the lower text block of the 2D scene grammar unless a later shot-specific approval replaces that layout.
- Scheduler wording needed one correction:
  - the old `11:10 Europe/Kaliningrad` text is no longer the active operational requirement;
  - the canon should instead require an independently switchable CherryFlash cron slot that is early enough for the publish surface to be ready by `12:30 Europe/Kaliningrad`.

### Missing

- A canonical full-run stage after `intro + scene1` acceptance:
  - `intro + scene1` is now accepted as the handoff/design baseline for expanding CherryFlash;
  - duplicate-frame cleanup is still an open defect and is **not** considered user-confirmed solved until the full-run result is checked.
- An explicit render rule for low-event follow-up scenes:
  - when there are `<=3` base events and a selected event has a second image, CherryFlash may add one extra follow-up scene for that event using the second image.
- An explicit frame-dedup rule for the 2D assembly:
  - exact consecutive duplicate frames may be removed only for accidental intro / handoff duplicates before the first approved `move-up` anchor;
  - intended 2D hold frames after the intro must stay untouched and must not be globally hash-deduped;
  - if intro-side removal happens before the first `move-up` beat anchor, audio timing must be shifted so the same strong beat still lands on the same visual `move-up` frame.
- A mandatory final brand scene after the event scenes:
  - CherryFlash full release is not complete without the final branded outro;
  - the active approved contract is no longer a static last frame;
  - CherryFlash should reuse the proven `Video Afisha` three-line slide-in outro grammar;
  - the outro palette must follow the canonical `Final.png` color family without changing the shared black video background;
  - approved current direction: black background, yellow strips, dark typography on the strips;
  - `Final.png` remains the canonical visual reference asset for the outro family, not the literal required final frame.
 - A primary-scene cadence guard after `move-up`:
  - the late 2D hold after the main `move-up` must not use a micro-drift that reads as `15 fps` / every-second-frame motion;
  - if the drift cannot stay perceptually smooth at `30 fps`, the poster should hold steady until the exit phase instead of simulating a weak continuous crawl.

## Requirements confirmation gate: 2026-04-12 delta

This delta records the latest requirement check against the current CherryFlash canon before implementation continues.

### Already present

- `CherryFlash` is already fixed as a separate daily product with internal mode key `popular_review`.
- The intro contract already captures the `Мобильная лента` ribbon geometry:
  - the second poster is the main handoff poster;
  - it is aligned to the phone-screen width first;
  - ribbon height is then derived from that poster;
  - all other posters are scaled by the shared ribbon height and stitched flush edge-to-edge;
  - the ribbon is glossy dense paper, not elastic rubber.
- The product boundary already states that the current technical milestone is `intro + first 2D scene`, before expanding to the full `2..6` daily release.
- The docs already fix phase-1 testing publication to `@keniggpt` and keep story autopublish disabled until the mode is validated.

### Needs clarification

- Scheduler time should no longer be treated as a single hardcoded start-time requirement; the canonical operational requirement is now readiness by `12:30 Europe/Kaliningrad`, while the exact start time may be back-calculated during implementation from measured runtime plus safety margin.
- The testing/publication language needs one explicit split:
  - while story publish is disabled, the validation surface is the ordinary publish target `@keniggpt`;
  - when story publish is later enabled, the production readiness criterion becomes a live story already published by `12:30 Europe/Kaliningrad`.

### Missing or conflicting with older canon

- The selection window traversal order needs to be updated from the older maturity-first reading to the newly confirmed editorial order:
  - `24 часа` first;
  - then `3 суток`;
  - then `7 суток`.
- The implementation notes should explicitly say that this mode does not contend for Gemma capacity and therefore must not be blocked by Gemma-related heavy-job guards by default.

## Requirements confirmation gate: `Мобильная лента`

This section captures the latest intro-direction request as an explicit delta to the current canon, so the next pass can distinguish confirmed requirements from still-open design choices.

### Already present

- The intro already has a confirmed technical boundary:
  - Blender owns only the opening `1.0-1.5s` intro block;
  - the main scene flow remains on legacy `kaggle/VideoAfisha/video_afisha.ipynb`.
- The selection side already has a stable product scope for the future intro payload:
  - one release contains `2..6` events;
  - events come from the `/popular_posts`-style popularity pool;
  - the final output stays a popularity-driven `popular_review` video rather than a tomorrow-only announce.
- The project already has a design backlog location and approval tooling for intro concepts in this feature folder, so the new concept can be documented without inventing a parallel spec path.
- A local confirmed phone asset now exists in the workspace for this concept:
  - `/workspaces/events-bot-new/docs/reference/iphone-16-pro-max.zip`
  - user approved `iPhone 16 Pro Max` as the asset to use for `Мобильная лента`.

### Needs clarification

- Phone asset sourcing order needs one canonical fallback chain in the docs:
  - preferred: the confirmed local `iPhone 16 Pro Max` asset already provided in the workspace;
  - otherwise: mockup-ready Blender asset with editable screen;
  - fallback: high-quality free phone mesh plus local lookdev/shading;
  - if a premium asset is unavailable, implementation must still proceed with a compatible free mesh rather than blocking the concept.
- Outer metadata policy needs one final wording lock:
  - dates and cities are allowed outside the phone in strong typography;
  - they must support watch intent without competing with the phone+ribbon hero or breaking small-preview readability.

### Missing

- A canonical intro concept definition for `Мобильная лента` / `MobileFeed Intro`.
- A strict reference lock to the provided shot:
  - `/workspaces/events-bot-new/docs/reference/09bc959616262101b9cd310629f08b84.jpg`
  - allowed deviation: slightly larger starting composition and slightly calmer phone tilt only.
- A scene contract stating that:
  - the intro uses a stylized 3D phone object;
  - a continuous left-to-right poster ribbon passes through the phone;
  - the ribbon length equals the number of posters/events in the video;
  - the second poster in the ribbon is the poster that must hand off into scene `1` of `video_afisha`.
- A transition contract for the new intro:
  - start from a readable static hero frame;
  - over `~3.2-3.4s` perform one continuous premium move that combines camera reorientation toward the phone screen with a synced push-in;
  - the handoff must happen on a continuing zoom vector or at the end of the zoom phase, never with a visible pull-back or scale reset;
  - strict sync should be anchored mainly to the legacy `0.9 -> 1.0` zoom tail, while the earlier first-half rhythm may stay freer but visually related;
  - the approval / final validation render may continue into the first 2D beats until the end of zoom / start of the upward move in `video_afisha`;
  - the strongest musical accent of the current `Pulsarium.mp3` cue should land at the end of the late `0.9 -> 1.0` tail, just as the first 2D scene begins its upward move.
- A poster-ribbon data contract:
  - posters must come from real selected events, not placeholder art;
  - the ribbon must be stitched edge-to-edge with height-aligned poster panels;
  - posters must keep their full artwork with no ribbon-side cropping;
  - the ribbon must not introduce visible borders, gutters, or per-poster framing around the artwork;
  - the second poster must be framed inside the phone the same way scene `1` arrives in `video_afisha` (center-led handoff);
  - ribbon scale must be solved from that second poster, with its width equal to the phone screen width at the zoom target state.

## Confirmed decisions

- User confirmed a separate internal mode/profile: `popular_review`.
- User confirmed that this backlog item is a separate daily product rather than a `CrumpleVideo` variant:
  - `CrumpleVideo` must continue to work independently on its own schedule and render contract;
  - this product uses popularity-based selection plus `VideoAfisha` 2D scene grammar under a separate runtime contract.
- User approved the marketing / product name:
  - product name: `CherryFlash`;
  - product description: `Popular Video Afisha`.
- User confirmed the scheduled launch time: `11:10 Europe/Kaliningrad`.
- User confirmed the rendering boundary:
  - scene generation remains on the legacy `VideoAfisha` pipeline;
  - this design work upgrades intro language, typography, and intro-to-scene handoff without switching the scene generator away from `kaggle/VideoAfisha/video_afisha.ipynb`.
- User rejected `V2 Ticket Stack` for the current approval round because the composition reads as unstable in preview and visually escapes the frame edges.
- User requested that approval mockups remove operator/debug/service copy from the visible cover frame and keep only viewer-facing text.
- User requested a cover-level CTA that explains why the viewer should open/watch the clip when the intro shows several popular events on different dates.
- User confirmed the 3D intro direction:
  - intro itself should become a short `1.0-1.5s` 3D sequence;
  - Blender should render only that intro block;
  - after the 3D intro ends, the pipeline must hand off into the standard `video_afisha.ipynb` scene flow for speed.
- User rejected pseudo-3D approval mockups as insufficiently Blender-like and asked for the intro approval pack to be rendered through a real Blender still pipeline.
- User requested a new intro concept track named `Мобильная лента`.
- User provided a canonical shot reference for this track and asked that the intro follow it strictly:
  - `/workspaces/events-bot-new/docs/reference/09bc959616262101b9cd310629f08b84.jpg`
  - only minor deviations are allowed: a slightly larger initial composition and a slightly calmer phone tilt.
- User requested the intro object grammar for this track:
  - one stylized 3D phone;
  - one seamless left-to-right ribbon made from real event posters;
  - posters are stitched into one continuous ribbon, magazine-spread style;
  - ribbon length equals the number of posters/scenes in the video announce.
- User tightened the poster-ribbon fit contract for this track:
  - the ribbon must not crop poster artwork;
  - the ribbon must not add visible borders, gutters, or outer poster frames;
  - all posters are normalized to the same height while keeping their native width ratio;
  - poster panels are glued flush edge-to-edge;
  - the global ribbon scale is derived from the second poster so its width exactly matches the phone screen width at the handoff zoom target.
- User requested that the second poster in the ribbon be the one shown inside the phone and become the handoff poster for the non-3D `video_afisha` continuation.
- User requested strong typography and multi-level CTA:
  - CTA inside the phone is required;
  - a second CTA level may exist inside or outside the phone;
  - dates/cities may be placed outside the phone if they stay large, premium, and non-disruptive.
- User narrowed the active work boundary for the current pass:
  - the task right now is only the `MobileFeed Intro` block itself;
  - the intro must end on the first full-frame 2D-ready poster state and stop there;
  - work on later `video_afisha` scenes resumes only after the intro handoff is approved.
- User later expanded the **approval artifact** boundary without changing the production one:
  - Blender still renders only the intro block;
  - the approval export may include `intro + full first 2D scene + music` to validate the real handoff;
  - this wider export scope is for approval only and does not change the intended production render split.
- User clarified that one CTA layer must carry the actual time selection signal from the real payload:
  - date, date range, period, or another structure that best matches the selected example set;
  - this should be worked through on real examples rather than on generic placeholder copy.
- User confirmed that the CTA system must be evergreen for a daily scheduled mode:
  - month-locked headlines like `КУДА ПОЙТИ / В АПРЕЛЕ` are not acceptable as the default family;
  - the main CTA should work across same-month, cross-month, and sparse far-future payloads;
  - the time signal should be its own dynamic layer driven by the real payload.
- Current default CTA routing for `MobileFeed Intro` is now:
  - external L1 promise: `ВЫБЕРИ СОБЫТИЕ`;
  - inside-phone upper CTA: count-driven product signal plus a compact real type cluster, for example `6 событий` + `кино • лекции • встречи • экскурсии`;
  - inside-phone lower CTA: compact city cluster from the real payload, for example `КАЛИНИНГРАД • ЧЕРНЯХОВСК`;
  - exact sparse dates may stay outside the phone in larger editorial typography.
- Current draft still/preview implementation keeps that split literally on the phone surface:
  - top screen label: `6 событий` + a compact type line such as `кино • лекции • встречи • экскурсии`;
  - bottom screen label: compact city cluster such as `КАЛИНИНГРАД • ЧЕРНЯХОВСК`;
  - these on-screen labels are part of the 3D phone stack, not a flat external overlay;
  - current approved routing no longer carries these labels across the cut;
  - instead they stay screen-locked on the phone surface until the final 3D beat and fade out there, so the 2D continuation begins without a second coordinate system for the same CTA.
  - optional refinement is allowed: the second line of the top screen label may switch to a different type cluster during the intro if that improves readability/product value without breaking screen lock or making the scene noisy.
  - the active screen treatment is now allowed to use a premium app-like dark UI state on the phone itself rather than keeping the whole screen bright through the end of the 3D phase.
- User requested stronger depth signaling inside the phone:
  - on-screen CTA should not live only as an external editorial overlay;
  - the phone screen should carry CTA/support typography above and/or below the handoff poster so the depth stack is visible in the 3D frame.
- Current draft-preview status:
  - low-sample `9:16` intro preview now exists on the canonical `1080 x 1920` story canvas and uses draft preview renders at the same aspect ratio;
  - the current preview now uses a matched late-tail cut into `video_afisha` instead of resetting to the small centered `scene1-start` poster;
  - on-screen labels now stay rigidly screen-locked inside the 3D shot and complete their fade before the cut, preventing the previous cross-cut screen-jitter bug;
  - the previous early-2D milky-to-black fade is no longer the preferred target;
  - the active direction is to move the tonal transition into the late 3D phase, so the handoff arrives with the phone/surround already heading into the darker state before the 2D scene fully takes over;
  - the previous `~1.9s` draft duration target is now obsolete;
  - the current clean target for the canonical `intro + scene1` validation clip is `~3.2-3.4s` for the Blender intro, with Blender owning a slower premium combined move and the validation clip continuing into the late 2D zoom and the first upward-move beat;
  - current final-sync target: measured against the current `Pulsarium.mp3` cue offset, the strongest early accent lands around `3.58s`, and the intro must arrive so that this accent matches the end of the late zoom tail / start of the upward move;
  - the latest motion-approval pass now renders a cheaper `360 x 640` preview clip for `intro + scene1 + music`, while keeping the same `9:16` geometry and the same beat-lock target as the future clean pass;
  - both preview and clean `--final` approval clips must now render every intro frame at full cadence; synthetic in-between intro frames from `Image.blend` or similar shortcuts are rejected because they create staircase motion and false smoothness;
  - the current handoff retune keeps scene `1` near local `~1.80s`, so the cut lands just after the start of `move_up`; the upward move must already be visible on the strong accent, but the intro must not swallow the whole upward phase;
  - the full CherryFlash renderer must preserve that same first-scene handoff contract in production output: scene `1` starts from the approved late-start / visible-`move_up` window instead of replaying the early `scale 0.4 -> 1.0` zoom, while scenes `2+` and follow-up beats still keep the full local `0.0 -> ...` timing contract;
  - the current clean pass also applies a targeted intro timing warp around the previously reported near-static pairs (`second 2: frames 11/12 and 26/27`, `second 3: frames 26/27`) so those beats no longer read as duplicate holds while the handoff frame and `move_up` strong-beat anchor stay unchanged;
  - the 2D upward move now uses a less delayed cubic in/out motion for the validation clip; the previous quintic curve delayed visible movement too much and made the upward shift feel late and then abrupt;
  - the 3D camera progress curve must keep a small non-zero tail velocity into the handoff; a fully eased-to-zero close-in plateau reads as a micro-stop before the 2D upward move and is a defect;
  - the white 3D push-in should use one coordinated progress path for camera location, target, up vector, and lens; if those components run on competing progress curves and create visible micro-stalls, the render is defective even if every frame is technically rendered for real.
  - the white-background camera move now uses one continuous global motion curve with a clearer premium in/out ease instead of stitched stage-local timing plateaus or a near-linear push; if a perceptible mid-clip pause appears on white, or if the push-in starts reading mechanically linear, that is a motion-curve defect rather than an acceptable preview shortcut;
  - the active fix also removes the previous mixed-location camera solve in the late white push; the phone now follows one monotonic location model instead of blending two competing location paths that caused visible flutter in the close-in phase;
  - the handoff should now stay a true cut in image space, with softness carried by tonal continuity already established in the late 3D frames rather than by whole-frame hybrid phone/poster dissolves;
  - the first 2D frames should inherit the darker state instead of being the place where the viewer first sees the white-to-black shift.
  - dense-tail rerenders for frames `96-106` must remain pinned to the original absolute intro timeline; rendering this tail as a fresh mini-animation is a defect because it can reintroduce earlier wide camera poses in the middle of the clip.
  - the previously exported `--final` artifact is not user-confirmed and should be treated as defective, because it still inherited draft-grade motion assembly and did not yet meet the clean-render smoothness bar.
  - the old short-timing draft is superseded; the active clean-render timing and beat-lock target are now user-confirmed.
- User tightened the physical-material expectation for the ribbon:
  - the ribbon must meet the phone screen and the imaginary desk surface cleanly;
  - it should behave like glossy magazine paper, with soft folds, sag, and gravity-led drape rather than a rigid strip.
- User reported a regression in the current preview pass:
  - ribbon artwork/text must never appear mirrored or flipped relative to readable poster orientation;
  - the ribbon must not sink into or clip through the phone body or the desk plane;
  - the material behavior should read as dense magazine stock that keeps width and form, bending recognizably instead of stretching like a soft ribbon.
- User requested that the intro motion feel premium and highly designed:
  - the move must be easing-driven rather than linear;
  - the transition should feel expensive and deliberate even in draft preview quality.
- User identified text-over-text collisions as a defect:
  - overlapping CTA layers are not an approved stylistic choice;
  - approval previews should treat any competing or intersecting text stacks as a bug to fix.
- User clarified that the output is intended for stories and therefore needs safe layout margins:
  - viewer-facing CTA, dates, cities, and other critical text must stay inside story-safe bounds;
  - the intro should avoid placing key information into areas commonly covered by Telegram / Instagram / similar story UI chrome.
- User approved a longer clean-render timing contract for the current final pass:
  - the previous `< 2.0s` intro cap is no longer the active target for `Мобильная лента`;
  - the clean-render intro should stretch to roughly `3.2-3.4s`;
  - its strongest musical sync point should happen when the late `0.9 -> 1.0` zoom tail completes and the first 2D scene starts moving upward;
  - the earlier part of the intro may be slower, more graceful, and more expensive-looking as long as the end-state match stays intact.
- User raised the quality bar for the current final render:
  - the previous `samples=2` approval look is explicitly rejected as too close to the draft;
  - the clean final pass must use a materially higher render-quality budget for the phone and ribbon, with cleaner light, cleaner surfaces, and a visibly less noisy 3D result.
  - any so-called final clip that still uses draft-grade sparse intro frame synthesis, obvious step-wise motion, or unreadable on-screen CTA during movement is a failed approval artifact rather than a valid clean render.
  - the clean final pass should prioritize real detail and clean typography over artificial softness; if motion blur makes phone edges or screen CTA look smeared, the correct fix is better cadence/render quality, not more blur.
- User refined the final-pass visual and motion requirements further:
  - the inside-phone CTA should read as a modern UI/screen layer rather than print-style poster typography;
  - inside-phone CTA blocks should not use drop shadows under the text slabs;
  - a dedicated modern font family from `/workspaces/events-bot-new/docs/reference/шрифт РО Знание.zip` should be used for the phone-screen typography in the next pass;
  - the large editorial outer layer may keep the stronger poster-like visual language if it helps the composition.
  - the phone CTA should sit visually on the glass/UI layer itself, not read as a floating sticker that separates from the screen and creates a shadow-like halo.
- User clarified the ribbon geometry guardrail:
  - the ribbon must still behave like glossy magazine paper, with visible stiffness, drape, and characteristic bends;
  - each poster must keep its own image proportion on the paper surface;
  - width changes are acceptable only when explained by perspective, camera angle, or paper curvature;
  - local mesh or UV deformation that makes a neighboring poster look unnaturally narrow, pinched, or wrinkled is a defect.
- User tightened the current `Мобильная лента` defect-fix target for the still/preview pass:
  - the second poster in the ribbon is the master panel for ribbon scaling and must read as the full phone-width handoff poster inside the shot, not as a narrower center card with too much neighboring artwork visible inside the phone area;
  - ribbon height must be derived from that master panel after matching its width to the phone width in the shot grammar;
  - every other poster in the ribbon must then be scaled only by that shared height, with native proportions preserved and with no per-panel stretch or squeeze;
  - phone-surface CTA should be large and reference-like on the screen itself, not miniature;
  - phone-surface CTA geometry must be solved from the imported screen in world space, not from an unscaled local bbox, otherwise label size/placement drift away from the real phone screen;
  - the top phone CTA must also respect the phone sensor/island safe area; overlap with the camera island is a defect, not an acceptable crop;
  - the phone CTA must use the supplied `Cygre` family in the wider UI-appropriate weights from the archive, not a narrower fallback look that reads unlike the approved pack;
  - for this shot family, the top phone CTA should prefer an asymmetrical left-column lockup below the sensor area instead of a centered banner competing with the island.
  - neighboring posters may bend like magazine paper, but they must not look falsely squeezed or horizontally crushed by the ribbon geometry.
  - inside the phone, the ribbon should stay sufficiently planar across the master poster, with stronger curvature beginning only at or just beyond the phone edge;
  - the exit bend should match the strict reference: broad and magazine-like, not a sharp collapse into the phone body and not a drop that kills readability of adjacent posters;
  - several neighboring posters may remain visible exactly as allowed by the strict reference, but that visibility must come from camera/framing and paper bends rather than from any per-panel stretch, squeeze, or artificial collapse;
  - the edge bends must preserve panel width distribution along the strip: broad lifts and settles are allowed, but the geometry must not remap adjacent posters into compressed shoulder bands that read as rubber.
  - in the hero frame, the ribbon sits in front of the screen content and the phone UI/copy remains visible above and below the strip, as in the strict reference;
  - the ribbon must continue beyond the camera frame on both left and right sides; visible strip endpoints are a defect.
  - the approved local Pro Max phone asset should be treated as visually sufficient; if the phone reads wrong, washed out, or low-grade in render, the defect is in the material/light/shading path rather than in the model choice itself.
  - the scene should separate a warm-milky environment from a cooler phone screen state and a darker premium phone shell with readable edge highlights and contact shadows;
  - keep the local model's native screen construction; do not fake a black border around the whole screen if the asset itself does not have one.
  - the screen may begin in a brighter cool state or directly in a near-black app-like state, but by the late 3D phase it is explicitly allowed to live in a controlled near-black `night-mode` treatment if that improves the premium handoff into the darker 2D scene.
  - when the screen uses this darker state, the phone CTA should invert into light UI typography on the glass surface rather than staying as dark print-like copy.
  - the sensor-island / camera block should still stay close to black and the upper phone contour must keep enough contrast against the environment.
  - ribbon posters should keep print density and color richness rather than collapsing into a pale or dusty look.
  - saturated poster colors, especially deep reds, must survive the render/light pipeline without collapsing into muddy near-black patches.
  - the hero still should recreate the clear phone+ribbon cast shadows from the strict reference; weak washed-out shadows are a defect because they flatten the product-shot depth.
  - the current visual priority stack for lookdev is now explicit: `1)` remove washed-out contact shadow, `2)` keep the sensor-island near-black, `3)` give the upper shell a cleaner rim/spec separation so the phone stops reading as plastic.
  - the approved reference-light decomposition is now explicit: one dominant soft warm key from upper-left, a very weak fill, a slimmer cool rim/spec edge for the phone contour, and a contact-core plus softer tail shadow falling down-right; overfilled ambient light that erases this hierarchy is a defect.
- User clarified the environment and motion guardrails for the next approval pass:
  - the scene should read as an infinite white product-shot space with shadows, not as a bounded tabletop with visible gray edges;
  - the phone path must stay continuous and premium, without a draft-like move where the phone first flies away/up and then returns;
  - before another high-quality render, the next approval artifact should first prove the corrected motion path and the beat-locked handoff into the first upward-moving 2D beat.
- User approved a new screen-transition direction for the current pass:
  - the phone CTA should read more like a real mobile app surface and less like print pasted onto glass;
  - the supplied `Cygre` pack should remain the active source, but the layout and weights should read as wide modern UI typography rather than narrow poster fallback;
  - it is acceptable to start the phone screen already dark if that gives a cleaner result than trying to keep the screen bright and then abruptly darkening it later;
  - whichever option is used, the perceived white-to-dark move should happen in the 3D phase, not be introduced for the first time by the early 2D continuation.
- Current implementation note for this defect cluster:
  - phone-screen event counters now use plural-safe Russian copy (`1 событие`, `2 события`, `4 события`, `5 событий`);
  - the current local screen-label generation now prefers wider `Cygre` weights (`Bold` / `SemiBold`) instead of the earlier narrower-looking phone treatment;
  - the current local screen-label texture canvas now also preserves the real on-phone label-plane aspect, so the correct `Cygre` glyphs are no longer horizontally squeezed into a narrow fallback-like read by the Blender screen plane;
  - all primary CherryFlash 2D scenes must keep the full local timing contract (`zoom-in -> move-up -> post-move continuation -> exit`); reusing the approved first-scene late-start shortcut for scene `2+` is defective because it hides cadence bugs by cutting away part of the scene instead of fixing active-motion math;
  - this typography fix remains `Not confirmed by user` until the next CherryFlash render is manually checked.
- User provided local downloaded phone assets and approved the Pro Max variant for implementation:
  - `/workspaces/events-bot-new/docs/reference/iphone-16-pro-max.zip`
  - `/workspaces/events-bot-new/docs/reference/iphone-16-free.zip`
  - confirmed implementation asset: `iphone-16-pro-max.zip`.
- Current local implementation path for preview stills now also uses the matching extracted `glb` asset:
  - `/workspaces/events-bot-new/docs/reference/iphone_16_pro_max.glb`
  - this `glb` is the active preview render source for `Мобильная лента`, because it imports into Blender 4 with cleaner material plumbing than the original `fbx` bundle.

### Already present

- `/v` already has a manual profile titled `Быстрый обзор` (`video_announce/assets/profiles.json`).
- `/v tomorrow` already has a scheduler path and automatic publication plumbing through `VideoAnnounceScenario.run_tomorrow_pipeline(...)`.
- Telegram stories infrastructure for `/v` already exists on the Kaggle side and can stay disabled until rollout.
- Post popularity signal already exists as a canonical feature:
  - per-source medians for TG/VK;
  - `⭐/👍` popularity marks;
  - `/popular_posts` report with windows `7 суток`, `3 суток`, `24 часа`.
- `/v` already stores publication history through `VideoAnnounceSession.published_at` and `VideoAnnounceEventHit`, so the project already has a base layer for anti-repeat logic.

### Needs clarification

- Anti-repeat semantics still need one final wording lock in implementation notes.
  - Recommended interpretation: rolling `7 x 24h`, because the request explicitly allows a return once the previous showing was more than a week ago.
- The `Мобильная лента` concept is now the active intro direction, but CTA copy and final outer-metadata density still need one last approval pass before implementation.
- Phone asset fallback should be treated as non-blocking:
  - if a premium/mockup asset is unavailable, the implementation may proceed with the best compatible free phone asset while preserving the same shot grammar.
- The concept no longer waits on external asset search:
  - use the provided local `iPhone 16 Pro Max` asset first;
  - treat all other phone assets only as fallback if this local asset fails technically.

### Missing

- A dedicated event selector for popularity-driven `/v`, built from the same candidate pool as `/popular_posts`.
- Event-level dedupe across:
  - multiple source posts that map to the same event;
  - multiple `/popular_posts` windows in the same run.
- A mode-specific anti-repeat rule for the last `7` days.
- Automatic scheduled publication of this mode to `@keniggpt`.
- Explicit phase-1 rule that story autopublish is prepared but remains disabled.
- A replacement intro/cover design direction for this mode.
- A canonized `Мобильная лента` intro spec with strict shot-matching and an explicit handoff contract into `video_afisha`.

## Requested product behavior

### Current rollout stage: full-run implementation

- The current validation path remains Kaggle-first:
  - approval and clean test renders should stay on the same Kaggle-side path that is intended to become the real working render path for this product.
  - the final mux step must stay compatible with multiple Kaggle `moviepy` API variants; CherryFlash must not depend on one specific volume-scaling method because the notebook image can expose a different audio-clip surface after `subclip/set_duration`.
  - the final mux should not needlessly degrade music quality relative to the approved source cue; for the current `Pulsarium.mp3` source (`192 kb/s` stereo), CherryFlash should export AAC audio at `192 kb/s` stereo unless a later approved delivery constraint requires a lower bitrate.
- The earlier `intro + scene1` gate is now considered passed enough to continue:
  - the handoff, ribbon alignment, and beat placement were accepted as the baseline for expanding CherryFlash into a full daily product run;
  - exact duplicate-frame cleanup is still an active defect-fix track and must be revalidated on the full run.
- The active implementation target is now:
  - render the full CherryFlash release with `3D intro + 2..6` event scenes/posters on real data, followed by the canonical animated brand outro;
  - validate scheduled generation;
  - validate phase-1 publication to `@keniggpt`;
  - keep story autopublish code path disabled in this step.

### Phase 1 rollout

- One automatic run per day.
- Use a dedicated CherryFlash schedule slot that is independently switchable from `/v tomorrow`.
- Scheduler start time may be back-calculated from observed runtime, but the operational requirement is:
  - by `12:30 Europe/Kaliningrad` the current day's CherryFlash result is already published on its phase-1 target surface.
- Publish final mp4 to `https://t.me/keniggpt`.
- Do not publish stories yet.
- This mode may include events not only for tomorrow/near future, but also clearly future events that already proved audience interest through source-post popularity.

### Output size

- Maximum `6` events in one release.
- Minimum `2` events in one release.
- A single-event release is invalid and must not be published.
- If only `0` or `1` eligible event remains after filtering, the run is skipped and the operator/superadmin receives a diagnostic message instead of a placeholder post.

## Intro data contract

- Intro rendering must support not only one date or one continuous range, but also **sparse date sets**.
- The mode must distinguish at least these topologies:
  - `single_day`
  - `continuous_range_same_month`
  - `continuous_range_cross_month`
  - `sparse_dates_same_month`
  - `sparse_dates_multi_month`
- For sparse selections the payload must not degrade to only `min_date..max_date`, because that hides the actual editorial meaning of the release.
- Recommended payload delta for implementation:
  - `intro.date_topology`
  - `intro.date_start` / `intro.date_end` for true ranges
  - `intro.date_points[]` for sparse selections
  - `intro.date_count`
  - `intro.months[]`
- Public intro behavior:
  - use a normal range when the selected events form a true continuous range;
  - show explicit anchor dates when the dates are sparse;
  - when dates are too scattered for one clean typographic range, surface month cluster + anchor dates rather than pretending it is one continuous interval.

## Selection contract

### 1. Source candidate pool

- The source pool must be derived from the same selection principle as `/popular_posts`.
- Use the same three popularity windows, but traverse them in the confirmed CherryFlash editorial order:
  - `24 часа` with `age_day=0`;
  - then `3 суток` with preferred `age_day=2` and fallback to the last available `age_day<=2`;
  - then `7 суток` with preferred `age_day=6` and fallback to the last available `age_day<=6`.
- Include only source posts that are strictly above the per-source median on `views` or `likes`.
- Keep the same upcoming-only rule as `/popular_posts`:
  - events scheduled for today or later are eligible;
  - multi-day events stay eligible while `end_date` remains in the future.

### 2. Event extraction and dedupe

- The video selector works with **events**, not with source posts.
- If several popular posts map to the same event, they collapse into one candidate event.
- If the same event appears in several popularity windows, it still collapses into one candidate event.
- Each final candidate keeps provenance for operator/debug visibility:
  - source post URL(s);
  - winning window;
  - popularity marks / normalized score.
- The same event must not appear twice inside one video.

### 3. Anti-repeat rule

- An event is excluded if the same mode already published it less than `7` days ago.
- An event becomes eligible again only when the last successful publication in this mode is older than `7` days.
- The anti-repeat rule is mode-specific:
  - regular `/v tomorrow` history must not silently block this mode forever;
  - this mode must not rely on the current global "seen in any `/v` session" behavior without an explicit time window.

### 4. Ranking

- Ranking should inherit `/popular_posts` logic first, not tomorrow-oriented proximity.
- Primary ordering uses the same normalized popularity score as `/popular_posts`.
- Tie-breakers:
  - higher raw `views`;
  - higher raw `likes`;
  - earlier confirmed CherryFlash traversal window when several candidates are otherwise comparable inside one run (`24 часа` > `3 суток` > `7 суток`).
- Near-date proximity is **not** a required tie-breaker for this mode; far-future events remain eligible if they are in the popularity candidate set.

## Scene expansion contract

### 5. Low-event follow-up scenes

- Base CherryFlash releases still select `2..6` **events**, not arbitrary scene fragments.
- If the base event count for one run is `<=3`, CherryFlash may expand the release with one extra follow-up scene per event when:
  - that event has a second non-empty image in `photo_urls`;
  - the event also has a short descriptive text suitable for the lower scene block.
- Follow-up scene rules:
  - the extra beat belongs to the same event and must immediately follow that event's primary scene;
  - it uses the second image of the event;
  - its motion swaps the normal soft `move-up` emphasis for an analogous soft `move-left`;
  - the lower text block switches from `title + date/time + location` to a compact description text in the `search_digest` family;
  - the preferred source for that description is `search_digest`, with existing event short-description fallbacks if needed;
  - the intended copy density is roughly `16-20` words, but the hard requirement is meaning-preserving short descriptive copy rather than an exact token count.
- The intro/ribbon still counts **events**, not follow-up beats:
  - intro `count`, ribbon panel count, and second-poster handoff logic continue to be derived from the base selected events only.

## Render sync contract

### 6. Exact-frame dedupe and beat lock

- CherryFlash full-run assembly must automatically detect and remove **exact consecutive duplicate frames**.
- This dedupe runs after frame generation and before final audio muxing.
- Beat-lock rule:
  - if duplicate removal changes clip duration **before** the first `move-up` beat anchor, the audio start offset must be shifted by the removed duration so the strong beat still lands on the same `move-up` frame;
  - duplicate removal after that anchor must not force a second retime of the already-approved first `move-up` beat.
- Dedupe output should be visible in artifacts/logs so remaining visual holds can be audited from the run output.
- The proven fix for the reproduced “frozen / near-frozen” 2D pairs is now in the encode stage, not in the scene math:
  - the source PNG frame sequence may already be visually smooth while the final mp4 is defective;
  - the root cause for the reproduced two-scene probe was `MoviePy.ImageSequenceClip.write_videofile()`, which introduced presentation-timestamp jitter during encode and caused certain decoded frames to map back to the previous source PNG;
  - CherryFlash must therefore encode the final mp4 through a direct `ffmpeg` image-sequence path (`image2` demuxer with explicit `-framerate <FPS>`), so each `frame_%04d.png` receives an exact `1/FPS` timestamp and no synthetic duplicate-frame pattern is introduced by the wrapper layer;
  - when validating similar incidents, always inspect both the source PNG sequence and the decoded mp4 frame mapping before changing scene math, easing, or drift-speed hypotheses;
  - viewer-facing `date_line` / `location_line` in the full CherryFlash renderer must still use the same human formatter as the approved first-scene path, so ISO dates or raw comma-heavy address strings never leak into the final 2D scenes;
  - a CherryFlash render that still contains obvious exact-neighbour duplicates after the direct `ffmpeg` image-sequence encode is defective and should be investigated as a render-math issue, not silently masked by a second global dedupe pass.
- The final publish-grade mp4 must also use a compact HEVC profile on that same direct `ffmpeg` path:
  - target budget: the complete daily release with intro, `2..6` event scenes, and branded outro should fit within about `15 MB` for Telegram/Stories-style delivery;
  - final mode should therefore prefer `libx265` / HEVC with `hvc1` tagging, while local preview mode may keep `libx264` for faster iteration;
  - this compact publish profile is a delivery optimization on top of the direct image-sequence encode, not a replacement for source-frame validation.
- Kaggle notebook bootstrap must tolerate transient third-party apt mirror incidents:
  - CherryFlash setup must not fail the whole run because a non-essential external repo such as `cloud.r-project.org` is mid-sync during `apt-get update`;
  - before installing the required runtime packages (`ffmpeg`, `libxrender1`, `libxi6`, `libxkbcommon-x11-0`), the notebook may disable that flaky CRAN source and retry `apt-get update` with explicit apt retry options;
  - such bootstrap hardening is part of the renderability contract, because a run that never reaches the renderer due to a temporary mirror mismatch is still a failed CherryFlash release.

## Delivery contract

### Profile / mode identity

- Recommended rollout shape:
  - introduce the dedicated internal mode/profile key `popular_review`;
  - keep the user-facing label compatible with the existing `/v -> Быстрый обзор` mental model if needed.
- The new mode must be visibly distinguishable in logs/traces/scheduler notifications from `/v tomorrow`.

### Publication target

- Phase 1 target channel: `@keniggpt` only.
- Recommended safe routing for phase 1:
  - configure this mode's publish destination to `@keniggpt`;
  - do not auto-fanout to additional main channels yet.

### Scheduler shape

- This is a separate scheduled mode, not a reinterpretation of the existing `/v tomorrow` slot.
- It should have its own enable flag and time routing, for example:
  - `ENABLE_V_POPULAR_REVIEW_SCHEDULED`
  - `V_POPULAR_REVIEW_TIME_LOCAL`
  - `V_POPULAR_REVIEW_TZ`
  - `V_POPULAR_REVIEW_PROFILE`
- Confirmed operational target for implementation planning:
  - the mode runs daily;
  - it does not consume Gemma and should not be blocked by Gemma-related heavy-job scheduling by default;
  - by `12:30 Europe/Kaliningrad` the daily output should already be ready on its target surface for the current rollout phase.
- Exact env names and start-time knobs may still change during implementation, but the schedule must stay independently switchable from `/v tomorrow`.

## Story publish enabled for prod

- CherryFlash reuses the same Kaggle-side `story_publish.py` helper path already proven on `CrumpleVideo`.
- `popular_review` now requests story publish by default in production mode.
- CherryFlash keeps its story routing local to the profile selection params instead of changing global story env fanout:
  - first target: `@kenigevents`, normal upload, no delay;
  - second target: `@lovekenig`, `delay_seconds=600`, `mode=repost_previous`, so the second publication is a repost of the first story instead of a second media upload.
- Story preflight/publish remains blocking for the run when enabled: if any target fails, the notebook writes `story_publish_report.json` and the whole CherryFlash release is treated as failed.
- Production readiness expectation remains:
  - daily CherryFlash should already be ready in the first half of the day;
  - the scheduled run should reuse the same story config as manual `/v -> CherryFlash`, not a separate legacy branch.

## Telegram publish surface

- Final CherryFlash render now exports a sibling `telegram_preview.jpg` built from frame `0001`, and Telegram publication goes through `send_video(..., supports_streaming=True)` while explicitly attaching that file as the channel/list thumbnail.
- Final CherryFlash render also exports a sibling `telegram_publish.mp4` in H.264/AVC specifically for Telegram channel delivery, while the main release artifact may stay on the more compact HEVC path.
- The expected feed/list preview in Telegram should therefore come from the first frame of that Telegram-compatible publish copy instead of relying on Telegram-side HEVC thumbnail inference.
## Data and observability deltas

- Session metadata should explicitly mark this mode, for example `selection_params.mode=popular_review`.
- Candidate traces should persist enough data to explain why an event was selected:
  - popularity score;
  - marks;
  - winning source post;
  - winning window;
  - cooldown skip reason if excluded.
- Anti-repeat queries must become time-aware and mode-aware.

## Acceptance checklist

- [ ] There is a dedicated scheduled mode for the popularity-driven quick review.
- [ ] One successful run publishes no more than `6` and no fewer than `2` events.
- [ ] A run with only `0` or `1` eligible event does not publish a video.
- [ ] Every selected event belongs to the same candidate universe that `/popular_posts` would surface at build time.
- [ ] No event repeats inside one release.
- [ ] No event repeats if its previous publication in this mode is less than `7` days old.
- [ ] The same event can return after the cooldown expires.
- [ ] Far-future upcoming events can be selected if they satisfy the popularity filter.
- [ ] Intro date rendering correctly distinguishes single-day, true-range, and sparse-date releases.
- [ ] The active `MobileFeed Intro` work unit stops exactly on the first full-frame handoff poster and does not redesign later `video_afisha` scenes in the same pass.
- [ ] The intro handoff animation reaches a synced zoom-handoff in `~3.2-3.4s` with premium easing, no backward step, and a clean continuation toward the start of the upward move in `video_afisha`, with the strongest early music accent landing on the end of the late zoom tail / start of the upward move.
- [ ] The full CherryFlash Kaggle artifact now renders the complete `2..6` scene daily release on real data, using the already-approved `intro + scene1` handoff as its baseline.
- [ ] Exact duplicate-frame cleanup is applied automatically, and the first strong beat still lands on the same visual `move-up` anchor after dedupe.
- [ ] When a run has `<=3` base events and an event has a second image, CherryFlash can append a follow-up `move-left` scene with description text for that event.
- [ ] The ribbon behaves like glossy paper, with plausible contact against the phone and desk surfaces plus visible soft folds/sag.
- [ ] Ribbon poster/text orientation remains readable and non-mirrored throughout the intro.
- [ ] The ribbon never clips into the phone body or the desk surface.
- [ ] The default CTA family is evergreen for daily scheduled releases and does not depend on a specific month name in the headline.
- [ ] The phone-screen CTA stack reads in depth above/below the poster without text-on-text collisions.
- [ ] Critical CTA/date/city content stays inside story-safe bounds and avoids common Telegram / Instagram story UI overlay zones.
- [ ] Phase 1 publication goes only to `@keniggpt`.
- [ ] Story autopublish stays off, while the mode remains story-ready for later rollout.
- [ ] When story autopublish is later enabled, the target operating expectation is that the story is already published by `12:30 Europe/Kaliningrad`.

## Linked design work

- Intro/cover concept work for this mode lives in `docs/backlog/features/cherryflash/design-brief.md`.
- The current approval render tooling lives in:
  - `kaggle/CherryFlash/mobilefeed_intro_still.py`
  - `kaggle/CherryFlash/assets/`
  - `kaggle/CherryFlash/cherryflash.ipynb`
  - `kaggle/execute_cherryflash_intro_scene1.py`
  - `scripts/render_mobilefeed_intro_still.py`
  - `scripts/render_mobilefeed_intro_preview.py`
  - `scripts/render_mobilefeed_intro_scene1_approval.py`
- Current implementation note:
  - CherryFlash now owns its own Kaggle notebook path under `kaggle/CherryFlash/`;
  - the intended long-term Kaggle contract is still a mounted CherryFlash render bundle under `/kaggle/input`, matching the project's production-proven notebook style;
  - current preproduction bootstrap stays input-first: `zigomaro/cherryflash` should receive a one-run Kaggle dataset with a unique slug and a mounted runtime tree under `/kaggle/input`, the launcher should wait until Kaggle marks that dataset ready and exposes the required files before pushing the kernel, and the runtime payload must avoid nested non-ASCII zip names such as the old `ro_znanie.zip` path that Kaggle rejected during dataset creation;
  - the current Kaggle preproduction entrypoint must remain compatible with Kaggle's bundled `moviepy` layout, because the approval clip assembly happens inside `scripts/render_mobilefeed_intro_scene1_approval.py` in the remote runtime;
  - CherryFlash runtime payloads may carry viewer-facing formatted dates inside the selection manifest, so intro/scene loaders must accept either ISO dates or preformatted date strings and must not crash on already-rendered copies such as `3 мая`;
  - the launcher/runtime path must tolerate mixed ORM + `raw_conn()` access on the same SQLite DB during live popularity selection; if SQLite temporarily refuses `PRAGMA journal_mode=...` with `database is locked`, CherryFlash should continue rather than abort before the session is even created;
  - canonical kernel ref for the current preproduction route: `zigomaro/cherryflash`;
  - the existing `CrumpleVideo` runtime remains separate and must not be used as the CherryFlash render/notebook home.
- Current working refinement track for approval:
  - treat previous slab-card experiments as exploratory only;
  - the active next concept for approval is `Мобильная лента` / `MobileFeed Intro`;
  - it must be documented and implemented as a phone + stitched-poster-ribbon scene that hands off into `video_afisha`;
  - the new concept should be developed from the strict provided shot reference rather than from the earlier slab-card composition.

## Confirmed next visual phase

- After intro concept approval, the implementation target is a short `1.0-1.5s` 3D typographic intro before the normal `VideoAfisha` flow.
- Confirmed boundary:
  - 3D is used only for the opening intro object / typography beat;
  - after the opening beat, the video returns to the improved legacy `VideoAfisha` scene generation path;
  - poster/event scene generation remains non-3D for speed;
  - the 3D block should be built on a fresh Blender branch, not the old `3.6`-locked intro path.
