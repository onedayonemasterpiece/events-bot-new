# countur_svg_generator

Status: Hybrid v0.3 research prototype
Readiness: Kaggle run required for visual acceptance

`countur_svg_generator` converts a source photo into clean contour line-art SVG.
The main goal is to create a recognizable two-color vector graphic image of a
building or object: lines plus background. The target style is not raw Canny
tracing; the output should look like a graphic designer or architectural
draftsperson simplified the primary object into smooth contour geometry with
straight lines, rounded joins, arcs, ellipses and circles where the object calls
for them.

The generator should work from general to specific: first preserve the global
silhouette/shell and major perspective planes, then add roof/cornice axes,
openings, arches, ellipses and selected details. A candidate that has attractive
details but no coherent overall building/object form is not acceptable.

## Current Implementation

The prototype lives in the root Python package `contour_svg/` and the Kaggle
entrypoint `kaggle/ContourSvgGenerator/script.py`.

Primary pipeline:

```text
image
→ Gemini semantic plan through `google_ai.client.GoogleAIClient`
→ GroundingDINO + Florence-2 + YOLO-World primary object / occluder box evidence
→ SAM2 masks
→ multi-state masks: object_visible / occluder / background / object_unknown
→ CMP Facade SegFormer facade elements
→ silhouette / Canny / Hough / LSD / M-LSD / DeepLSD / HAWP / arc guides
→ typed EvidenceInventory from masks, facade elements, guides and semantic plan
→ BuildingShell hard gate and shell-only diagnostic SVG candidate
→ PlaneGraph hard gate and plane-scaffold diagnostic SVG candidate
→ FeatureGraph hard gate for facade elements, rows and simplified openings
→ candidate line graph + Gemini line-group pruning
→ conservative completion proposals from masks, repeated openings and line gaps
→ primitive-rendered final candidates with line budgets
→ source-preserving SD1.5 ControlNet img2img proposal candidates
  from lineart/MLSD + Depth Anything V2 controls
→ optional B3/B4 IP-Adapter style-reference proposal candidates
  using `input.style_reference_path` as visual approach only
→ SVG hard gates + CV score + Gemini contact-sheet review of final-eligible candidates
→ final.svg + top alternatives + debug artifacts
```

There is intentionally no deterministic substitute path. If Gemini, the shared
Google AI limiter, GroundingDINO, SAM2, CUDA ControlNet, SVG preview rendering,
Florence-2, YOLO-World, CMP Facade parsing, M-LSD, DeepLSD, HAWP, ControlNet,
SVG preview rendering, or Gemini ranking is unavailable, the run must fail
loudly instead of producing a surrogate result. Gemini calls must go through the shared `google_ai`
gateway/rate limiter; the feature must not instantiate a provider SDK client
directly.

The local research launcher keeps the Kaggle notebook slug stable:
`zigomaro/contour-svg-generator`. Per-run payload/secret/key datasets are
allowed to vary, but the notebook itself is versioned under that stable name.

The Kaggle entrypoint is status-aware. It loads `kaggle_run.json` via
`kaggle_status_client`, starts alive heartbeats, emits `kernel_started`,
`preflight_ok` and terminal `report_written` events, and reports domain stages
with `progress_percent` / `progress_label`: input normalization, Gemini semantic
plan, GroundingDINO primary/occluder detection, SAM2 masks, guide extraction,
multi-state masks, facade parsing, typed evidence inventory, BuildingShell
coarse object graph, PlaneGraph perspective scaffold, FeatureGraph facade
elements/rows, line graph pruning, conservative completion,
source-preserving ControlNet img2img proposal generation,
IP-Adapter style-reference proposal generation when B3/B4 are enabled,
primitive rendering/proposal vectorization,
Gemini/CV ranking, and final export.

## Usage

Kaggle script defaults to the sample building config:

```bash
python kaggle/ContourSvgGenerator/script.py
```

Local/package CLI:

```bash
python -m contour_svg run \
  --config docs/features/countur_svg_generator/examples/sample_building.yaml
```

Useful env overrides for Kaggle:

- `CONTOUR_CONFIG` — config path; default is `examples/sample_building.yaml`.
- `CONTOUR_INPUT` — input image override.
- `CONTOUR_OUTPUT_DIR` — output dir override.
- `CONTOUR_SAM2_CHECKPOINT` — attached SAM2/SAM2.1 checkpoint path.
- `CONTOUR_DEEPLSD_CHECKPOINT` — optional attached DeepLSD checkpoint path;
  otherwise the Kaggle entrypoint downloads `deeplsd_md.tar`.
- `CONTOUR_HAWP_CHECKPOINT` — optional attached HAWP checkpoint path; otherwise
  the Kaggle entrypoint downloads `hawpv3-imagenet-03a84.pth`.
- `GOOGLE_API_KEY` / `GOOGLE_API_KEY2` / `GOOGLE_API_KEY3` / `GOOGLE_API_KEY4` — registered Google
  AI key envs used by the shared limiter. `gemini.api_key_env` should point to
  one of these registered env names unless a local-only experiment is intended.
- `GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS` — optional comma-separated spare key
  env names for the shared limiter when the scoped lane is out of daily budget.
  The local Kaggle launcher derives `GOOGLE_API_KEY2,GOOGLE_API_KEY3,GOOGLE_API_KEY4` when
  those keys are present and the variable is unset.
- `SUPABASE_URL` + `SUPABASE_SERVICE_KEY` or `SUPABASE_KEY` — enable shared
  Google AI reserve/finalize RPCs inside `GoogleAIClient`.
- `CONTOUR_SKIP_PIP_INSTALL=1` — skip Kaggle dependency install.

The diffusion branch is intentionally proposal-only. It uses the source photo
as the img2img init image, neutralizes occluders with the multi-state mask,
conditions on lineart/MLSD plus Depth Anything V2 depth, and uses style-only
prompts so Stable Diffusion does not replace the building with a generic
facade. When `B3`/`B4` are enabled and `input.style_reference_path` exists, the
same source-preserving img2img setup also loads SD1.5 IP-Adapter weights and
passes the reference image as style context. The reference may influence line
economy, contrast and simplification, but it must not define the object
identity. The final postcard/sketch look is produced by primitive SVG rendering
(`style.stroke_style=sketch`), not by accepting a neural raster as final truth.

There is also a separate neural-branch probe for the research question "can a
model turn prepared masks/edge maps into a useful line-art PNG?". It is not a
replacement for the final SVG path. It prepares edge/mask/facade composites
from an existing artifact pack and optionally runs CUDA Diffusers img2img in
line-to-line mode: the prepared line map is both `image` init and ControlNet
condition. The original source photo is not used by default and is allowed only
in the explicit `photo_assisted` research mode.

```bash
python -m contour_svg neural-branch \
  --artifacts docs/features/countur_svg_generator/samples/generated/audit_1527 \
  --out artifacts/codex/contour-svg-neural-branch-local \
  --init-modes line_init \
  --variants A1,A3,C2,D1,E1 \
  --style-reference docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp
```

Real Kaggle probe:

```bash
python scripts/run_contour_svg_neural_branch_kaggle.py
```

That launcher keeps the Kaggle kernel slug stable:
`zigomaro/contour-svg-neural-branch`. Per-run payload datasets are unique.
The probe emits stage statuses for `preflight`, `neural_inputs`,
`neural_img2img`, `neural_report`, and writes `neural_branch/*.png` plus
`neural_branch_report.json`. It can also take a freshly downloaded full-pipeline
artifact directory, which is the intended way to test a new benchmark image after
mask/edge extraction:

```bash
python scripts/run_contour_svg_neural_branch_kaggle.py \
  --run-label contour-svg-tower-neural \
  --output-root artifacts/codex/contour-svg-tower-neural \
  --artifacts artifacts/codex/contour-svg-tower/<run>/contour_svg_tower_92_11_16 \
  --variants A1,A3,C2,D1,E1 \
  --init-modes line_init \
  --seeds 92
```

The current v0.2 neural experiment adds a batchable guide-only ControlNet
line-art workflow for fast visual research before SVG reconstruction. It takes
source photos directly, builds a role-separated guide bank (`G1_edge_raw`,
`G3_edge_thickened`, `G4_edge_cleaned`, `G7_silhouette_outline`,
`G10_fused_guide` plus composite `CG*` guides), then runs Lineart/Scribble
ControlNet candidates without using the source photo as default img2img init.
Every candidate writes raw, black-on-white line mask, cleaned mask, burgundy
preview, JSON metrics and contact sheets. The canonical target remains a clean
raster architectural line-art PNG similar in approach to
`samples/output/IMG_20260614_115550.webp`; SVG is still a later stage.

Batch Kaggle probe for all images in `to_do/`:

```bash
python scripts/run_contour_svg_line_art_batch_kaggle.py \
  --source-dir docs/features/countur_svg_generator/to_do \
  --branches E1_lineart_control_only,E2_lineart_line_init,E3_scribble_control_only,E4_scribble_line_init \
  --guide-ids G3_edge_thickened,G4_edge_cleaned,CG1_silhouette_plus_structure,CG3_fused_balanced,CG4_minimal_clean \
  --seeds 42
```

The stable Kaggle notebook slug is `zigomaro/contour-svg-line-art-batch`.
Per-image output folders contain `edge_mask.png`, `guides/`,
`composite_guides/`, `candidates/`, `contact_sheets/`, `reports/` and
`result.png` / `result_burgundy_preview.png` for the best line-mask candidate.

Full Kaggle runs can select a non-default feature config through the local
launcher without changing the stable notebook slug. For the tower benchmark:

```bash
python scripts/run_contour_svg_kaggle_sample.py \
  --run-label contour-svg-tower \
  --output-root artifacts/codex/contour-svg-tower \
  --config docs/features/countur_svg_generator/examples/tower_92_11_16.yaml \
  --kaggle-output-dir /kaggle/working/contour_svg_tower_92_11_16
```

## Output Contract

Each run writes:

- `final.svg`, `preview.png`, `final.meta.json`;
- `top_alternatives/`;
- `candidates/*.svg`, previews and metadata;
- `leaderboard.csv`, `ranking_report.json`;
- root-level `edge_map.png` (occluder-subtracted), `edge_mask.png`
  (primary-object Canny before occluder subtraction) and typo-compatible
  `egde_mask.png` line-mask review artifacts, plus the same mask aliases under
  `debug/`;
- `debug/input_normalized.png`, `semantic_plan.json`, detector box JSON files,
  masks, CMP facade element masks/overlay, edge maps, M-LSD guide,
  DeepLSD/HAWP line JSON/overlays, guide source counts,
  `evidence_inventory.json`, `evidence_contact_sheet.png`,
  `building_shell.json`, `building_shell_overlay.png`,
  `building_shell_score.json`, `plane_graph.json`, `plane_graph_overlay.png`,
  `plane_graph_score.json`, `feature_graph.json`, `feature_graph_overlay.png`,
  `feature_graph_score.json`, completion proposals,
  line overlays, `line_candidates.jsonl`, `line_groups.json`,
  `arch_primitives.json`, ControlNet img2img init/control images,
  optional `style_reference_adapter_image.png`,
  `debug/neural_branch_raw/neural_branch_meta.json`, contact sheet, and Gemini
  scores when available.
- `kaggle_status_events.jsonl` in Kaggle output when the standard status
  dataset is mounted; callback tokens are redacted by the shared helper.
- `final.meta.json` records `llm_gateway.google_ai_client` and
  `llm_gateway.supabase_limiter`; runs without the shared Supabase limiter must
  fail before the Gemini provider call.

`final.svg` must be transparent by default, contain no `<image>` or embedded
base64 raster data, use `fill="none"`, one stroke color, and round line caps and
joins. It must be exported from a primitive-rendered final-eligible candidate;
raster/vectorized proposal candidates may remain under `candidates/` and
`debug/`, but cannot directly win `final.svg`.

## Samples And Benchmarks

- `samples/input/image - 2026-06-14T115705.752.png` — first calibration photo.
- `samples/output/IMG_20260614_115550.webp` — expected style reference.
- `samples/generated/sample_building_kaggle_20260614_1219_preview.png` — first
  complete Kaggle neural-first sample. It is a coarse-to-fine B2 structural
  baseline: the global building shell, roofline, facade corner, side volume,
  windows and steps are present, but roof/right-facade line clustering remains
  noisier than the reference.
- `samples/generated/sample_building_kaggle_20260614_1219_final.svg` and
  `.meta.json` — matching generated SVG and metadata.
- `samples/generated/audit_1527/sprint1_shell/shell_only_preview.png` — first
  recovery-plan milestone artifact: a clean BuildingShell-only SVG/PNG
  diagnostic with roofline, main facade corner and base, before windows/details.
- `samples/generated/audit_1527/sprint2_plane/plane_scaffold_preview.png` —
  second recovery milestone: shell + facade planes/bands/corner scaffold without
  drawing the wall-plane evidence rectangle as final geometry.
- `samples/generated/audit_1527/sprint3_feature/feature_scaffold_preview.png` —
  third recovery milestone: shell + plane scaffold + simplified windows, arches,
  balcony/pilaster evidence from `FeatureGraph`; still a diagnostic candidate,
  not the final polished postcard output.
- `artifacts/codex/contour-svg-neural-branch-kaggle/contour-svg-neural-20260614-1849/`
  — completed neural-branch probe on Kaggle. It writes `result.png`,
  `result_raw.png`, `result_thresholded.png`, candidate contact sheets and a
  report from the older photo-init experiment. That run is now retained as a
  negative result: it proved photo init makes outputs too photo-like before
  thresholding and too foliage-noisy after thresholding. Current policy is
  line-to-line by default.
- `artifacts/codex/contour-svg-neural-branch-kaggle/contour-svg-neural-20260614-1923/`
  — first completed line-init neural branch run. `source_photo_init_used=false`;
  prepared `edge_*` line maps are both img2img init and ControlNet condition.
  It writes strict `result.png`, `result_transparent.png`,
  `result_burgundy_preview.png`, raw candidates, line masks and per-candidate
  gate reports. The visual result is a real line-art proposal, though still not
  final target quality.
- `to_do/92-11-16.jpg` — second benchmark for tower geometry, leaf occlusion,
  arches, dome, balcony ellipses, and cylindrical perspective.

## Application Ideas

- Half-photo / half-vector visual where part of the original photo is replaced
  by contour line art, optionally with nearby text.
- Scenario video based on vectorization and restored photo: lines are drawn
  progressively into a complete vector image, then transform into the source
  photo or a filtered photorealistic version. This can support educational
  videos about art objects and cultural heritage, with subtitles or narration.

Configs:

- `examples/sample_building.yaml`
- `examples/tower_92_11_16.yaml`

Current research notes:

- `research/external_agent_handoff_20260614.md` — branch/document/artifact
  briefing for external architecture review.
- `research/evidence_fusion_pipeline_design_20260614.md` — pipeline redesign
  after the first human visual audit: evidence layers should feed a shared
  building shell / plane / feature graph before candidate rendering.
- `research/neural_branch_img2img_decision_20260614.md` — source-preserving
  ControlNet img2img and IP-Adapter style-reference proposal strategy.
- `research/tool_docs_audit_20260614.md` — external tool documentation audit.
- `requirements/contour_svg_generator_audit_and_recovery_plan_20260614.md` —
  external implementation audit and recovery plan; the next milestone is a
  clean `BuildingShell`/shell-only SVG before adding feature detail.

## Folders

- `requirements/` — incoming requirement files and source notes.
- `research/` — experiments, findings, and implementation notes.
- `samples/input/` — source examples for generator research.
- `samples/output/` — generated or expected examples.
- `samples/generated/` — curated, committed generated baselines only. Full run
  artifacts stay under `artifacts/codex/contour-svg-sample/`.
- `to_do/` — next visual targets and benchmark tasks.
