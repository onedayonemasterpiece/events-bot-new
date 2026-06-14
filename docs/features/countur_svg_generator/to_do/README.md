# countur_svg_generator to_do

This folder tracks visual benchmark inputs and concrete follow-up tasks for the
neural-first contour SVG generator.

## Current Benchmark

- `92-11-16.jpg` — tower with foreground leaves, dome, balcony ring, arched
  windows and cylindrical brick body.
- Sample building baseline:
  `../samples/generated/sample_building_kaggle_20260614_1219_preview.png`
  from Kaggle run `contour-svg-sample-20260614-121950-8f16aa`. The run completed
  on stable kernel `zigomaro/contour-svg-generator` and selected
  `B2_structural_primitives_02`.

## Next Tasks

- Improve the sample building baseline from coarse-to-fine B2 to cleaner
  reference-like two-color graphic line art:
  - treat the acceptance target as a recognizable designer/draftsperson image,
    not a dense photo trace;
  - cluster roof/cornice lines by vanishing direction and merge duplicates;
  - remove long accidental diagonals that cross facade planes;
  - keep one strong outer shell before interior detail;
  - fit arcs/ellipses/circles where the architecture calls for rounded forms;
  - prefer semantic facade completion for the tree-occluded left edge instead
    of carrying leaf contours into the silhouette;
  - preserve the central arch, facade corner, cornices, windows, balcony and
    steps while reducing texture strokes.
- Keep the v0.3 primitive-rendered final gate mandatory. The first successful
  run showed that perspective guides and contrast contours are available;
  quality loss happens when raster/vectorized proposal candidates can win
  without semantic line groups, global shell and coarse-structure requirements.
- Use the new debug artifacts from the next run to tune the pipeline:
  `mask_bundle.json`, `masks_multistate_overlay.png`, `line_candidates.jsonl`,
  `line_groups.json`, `line_groups.pruned.json`, `line_groups_overlay.png`,
  `gemini_line_group_actions.json`, and `arch_primitives.json`.
- Use `debug/contact_sheet.png`, `leaderboard.csv`, `final.meta.json`, and
  `kaggle_status_events.jsonl` from
  `artifacts/codex/contour-svg-sample/contour-svg-sample-20260614-1219/` as the
  immediate evidence set for the next iteration.
- Run `examples/tower_92_11_16.yaml` through the same v0.3 final gate and
  record failures around leaf rejection, balcony ellipses, dome curvature,
  arched windows and cylinder contour.
- Promote stable thresholds from run artifacts into configs; do not hand-draw
  final SVG paths as a substitute for pipeline output.
