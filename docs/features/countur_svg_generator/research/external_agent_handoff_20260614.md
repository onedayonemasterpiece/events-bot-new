# External Agent Handoff — Contour SVG Generator v0.3

Date: 2026-06-14

Branch:

- Local branch: `feature/contour-svg-generator-v03`
- GitHub branch URL after push:
  <https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/contour-svg-generator-v03>

Status:

- Research/prototype, not visually accepted yet.
- The pipeline has produced complete SVG/PNG runs, but the current best output
  is still a partial architectural interpretation, not the requested
  high-similarity final result.
- Recovery Sprints 1-3 are now implemented: typed `EvidenceInventory`,
  `BuildingShell`, `PlaneGraph`, `FeatureGraph`, hard gates, and shell/plane/
  feature diagnostic SVG/PNG candidates.
- The latest full run with useful debug artifacts is the curated `audit_1527`
  pack below.
- Later attempts hit shared Gemini RPD through the required
  `GoogleAIClient`/Supabase limiter. This must not be bypassed.

## What The User Wants

Input photo:

- [`samples/input/image - 2026-06-14T115705.752.png`](../samples/input/image%20-%202026-06-14T115705.752.png)

Desired style / approach references:

- [`samples/output/IMG_20260614_115550.webp`](../samples/output/IMG_20260614_115550.webp)
- [`to_do/92-11-16.jpg`](../to_do/92-11-16.jpg)

Expected result:

- A two-color line graphic where the source building is clearly recognizable.
- General mass first: full shell, roofline, facade corner, base and planes.
- Details second: cornices, windows, arches, balconies, stairs/base.
- Trees/fence/sky/road should not become building geometry.
- Occluded parts should be conservatively completed when supported by visible
  architecture.

## Key Documents

Start here:

- [`README.md`](../README.md) — current feature summary and output contract.
- [`requirements/requirements.md`](../requirements/requirements.md) — user-facing goal and requirements index.
- [`requirements/user_audit_1.md`](../requirements/user_audit_1.md) — human visual audit of previous debug artifacts.
- [`requirements/contour_svg_generator_audit_and_recovery_plan_20260614.md`](../requirements/contour_svg_generator_audit_and_recovery_plan_20260614.md) — external implementation audit and recovery plan; treats the next milestone as a clean building shell before rich detail.
- [`research/evidence_fusion_pipeline_design_20260614.md`](evidence_fusion_pipeline_design_20260614.md) — most important current design correction.

Core specifications:

- [`requirements/contour_svg_generator_engineering_spec_v0_3.md`](../requirements/contour_svg_generator_engineering_spec_v0_3.md)
- [`requirements/models_tools_catalog_v0_1.md`](../requirements/models_tools_catalog_v0_1.md)
- [`requirements/architecture_elements_library_v0_1.md`](../requirements/architecture_elements_library_v0_1.md)
- [`requirements/gemini_prompts_and_schemas_v0_1.md`](../requirements/gemini_prompts_and_schemas_v0_1.md)
- [`requirements/implementation_backlog_v0_1.md`](../requirements/implementation_backlog_v0_1.md)

Recent implementation/research notes:

- [`research/neural_branch_img2img_decision_20260614.md`](neural_branch_img2img_decision_20260614.md)
- [`research/tool_docs_audit_20260614.md`](tool_docs_audit_20260614.md)

Configs:

- [`examples/sample_building.yaml`](../examples/sample_building.yaml)
- [`examples/tower_92_11_16.yaml`](../examples/tower_92_11_16.yaml)

## Code Entry Points

Package:

- [`contour_svg/config.py`](../../../../contour_svg/config.py)
- [`contour_svg/pipeline.py`](../../../../contour_svg/pipeline.py)
- [`contour_svg/contracts.py`](../../../../contour_svg/contracts.py)
- [`contour_svg/semantic_gemini.py`](../../../../contour_svg/semantic_gemini.py)
- [`contour_svg/llm_gateway.py`](../../../../contour_svg/llm_gateway.py)
- [`contour_svg/detection_grounding.py`](../../../../contour_svg/detection_grounding.py)
- [`contour_svg/segment_sam2.py`](../../../../contour_svg/segment_sam2.py)
- [`contour_svg/masks.py`](../../../../contour_svg/masks.py)
- [`contour_svg/facade_parser.py`](../../../../contour_svg/facade_parser.py)
- [`contour_svg/guides.py`](../../../../contour_svg/guides.py)
- [`contour_svg/line_graph.py`](../../../../contour_svg/line_graph.py)
- [`contour_svg/completion.py`](../../../../contour_svg/completion.py)
- [`contour_svg/primitive_renderer.py`](../../../../contour_svg/primitive_renderer.py)
- [`contour_svg/diffusion_controlnet.py`](../../../../contour_svg/diffusion_controlnet.py)
- [`contour_svg/gemini_judge.py`](../../../../contour_svg/gemini_judge.py)

Kaggle:

- [`kaggle/ContourSvgGenerator/script.py`](../../../../kaggle/ContourSvgGenerator/script.py)
- [`scripts/run_contour_svg_kaggle_sample.py`](../../../../scripts/run_contour_svg_kaggle_sample.py)

Focused tests:

- [`tests/test_contour_svg_config.py`](../../../../tests/test_contour_svg_config.py)
- [`tests/test_contour_svg_llm_gateway_contract.py`](../../../../tests/test_contour_svg_llm_gateway_contract.py)
- [`tests/test_contour_svg_kaggle_launcher.py`](../../../../tests/test_contour_svg_kaggle_launcher.py)
- [`tests/test_contour_svg_ranking.py`](../../../../tests/test_contour_svg_ranking.py)
- [`tests/test_contour_svg_svg_contract.py`](../../../../tests/test_contour_svg_svg_contract.py)
- [`tests/test_contour_svg_v03_contract.py`](../../../../tests/test_contour_svg_v03_contract.py)

## Current Pipeline As Implemented

```text
input image
→ Gemini semantic plan through shared GoogleAIClient limiter
→ GroundingDINO / Florence-2 / YOLO-World primary and occluder box evidence
→ SAM2 primary and occluder masks
→ multi-state masks: object_visible / occluder / background / object_unknown
→ CMP Facade SegFormer element parsing
→ Canny / Hough / LSD / M-LSD / DeepLSD / HAWP guide extraction
→ EvidenceInventory
→ BuildingShell hard gate and shell-only diagnostic candidate
→ PlaneGraph hard gate and plane-scaffold diagnostic candidate
→ FeatureGraph hard gate and feature-scaffold diagnostic candidate
→ line candidates and line groups
→ Gemini line-group editor
→ conservative completion proposals
→ primitive renderer candidate families
→ ControlNet img2img proposal rasters
→ optional IP-Adapter style-reference B3/B4 proposal rasters
→ SVG hard gates, CV scoring, Gemini contact-sheet ranking
→ final.svg / preview.png / metadata / alternatives
```

The important remaining flaw: the scene graph is now present only through
`BuildingShell → PlaneGraph → FeatureGraph`. The next design steps should be
`OccluderAwareCompletionGraph → Gemini/neural fusion editor → PrimitiveScene`,
so candidate families become different renderings of one repaired fused scene
rather than separate raw-source interpretations.

## Role Of The Architecture Elements Library

The architecture library is not meant to be decorative documentation. Its role
should be:

1. define the grammar for simplifying detected facade elements;
2. define how windows, arches, cornices, rooflines, balconies, pilasters and
   stairs become editable SVG primitives;
3. define line budgets and visual hierarchy;
4. constrain conservative completion under occlusion;
5. prevent raw edge/texture/fence/tree lines from masquerading as architecture.

Current implementation now uses the idea at three levels: `BuildingShell`
establishes the mass, `PlaneGraph` assigns facade planes/bands/vertical edges,
and `FeatureGraph` assigns facade elements to planes and opening rows before
rendering a diagnostic feature scaffold. The remaining gap is completion/repair:
there is still no explicit occluder-aware completion graph, Gemini/neural fusion
editor over graph nodes, or final polished `PrimitiveScene` renderer.

## Neural Renderer Status

There is no accepted neural-final renderer result yet.

What exists:

- Earlier B1/B2 ControlNet PNG proposals were generated and are included in the
  curated `audit_1527` pack. They are useful evidence, but not a valid final
  artifact because they can drift from the source building and are rasters.
- The code now includes a source-preserving ControlNet img2img setup and B3/B4
  IP-Adapter style-reference branches. These were added after the observed
  neural drift.
- The B3/B4 style-reference branch has not yet completed a full Kaggle sample
  run because subsequent attempts hit shared Gemini RPD before reaching
  diffusion.

Suggested direction:

- keep neural output as compositor/repair/style evidence;
- use Gemini or another vision model to compare source, evidence overlays,
  neural line-art proposal and primitive preview;
- extract repair instructions into the graph;
- final SVG should still be editable primitive output unless the project later
  defines a separate raster-only deliverable.

## Curated Result Pack In Git

Curated pack from the useful 2026-06-14 15:27 Kaggle run:

- [`samples/generated/audit_1527/preview.png`](../samples/generated/audit_1527/preview.png) — final preview from that run; useful but not accepted.
- [`samples/generated/audit_1527/final.svg`](../samples/generated/audit_1527/final.svg) — final SVG from that run.
- [`samples/generated/audit_1527/contact_sheet.png`](../samples/generated/audit_1527/contact_sheet.png) — top primitive-rendered candidates side by side.
- [`samples/generated/audit_1527/leaderboard.csv`](../samples/generated/audit_1527/leaderboard.csv) — candidate ranking table.
- [`samples/generated/audit_1527/ranking_report.json`](../samples/generated/audit_1527/ranking_report.json) — full ranking report.
- [`samples/generated/audit_1527/final.meta.json`](../samples/generated/audit_1527/final.meta.json) — metadata for final candidate.

Evidence layers praised in the human audit:

- [`samples/generated/audit_1527/edge_map.png`](../samples/generated/audit_1527/edge_map.png) — highly accurate but too dense; should confirm lines, not be traced directly.
- [`samples/generated/audit_1527/mlsd_guide.png`](../samples/generated/audit_1527/mlsd_guide.png) — strong large-geometry guide.
- [`samples/generated/audit_1527/deeplsd_lines_overlay.png`](../samples/generated/audit_1527/deeplsd_lines_overlay.png) — good structural straight lines.
- [`samples/generated/audit_1527/elements_overlay.png`](../samples/generated/audit_1527/elements_overlay.png) — facade parser windows/doors/balconies/pilasters overlay.
- [`samples/generated/audit_1527/wall_plane.png`](../samples/generated/audit_1527/wall_plane.png) — facade wall-plane mask.
- [`samples/generated/audit_1527/masks_multistate_overlay.png`](../samples/generated/audit_1527/masks_multistate_overlay.png) — object/occluder/background/unknown overlay.
- [`samples/generated/audit_1527/mask_occluder.png`](../samples/generated/audit_1527/mask_occluder.png) — occluder mask used for interpolation policy.

Neural proposal rasters from that run:

- [`samples/generated/audit_1527/B1_controlnet_condition_lineart.png`](../samples/generated/audit_1527/B1_controlnet_condition_lineart.png)
- [`samples/generated/audit_1527/B1_lineart_controlnet_seed42.png`](../samples/generated/audit_1527/B1_lineart_controlnet_seed42.png)
- [`samples/generated/audit_1527/B1_lineart_controlnet_seed43.png`](../samples/generated/audit_1527/B1_lineart_controlnet_seed43.png)
- [`samples/generated/audit_1527/B2_controlnet_condition_mlsd.png`](../samples/generated/audit_1527/B2_controlnet_condition_mlsd.png)
- [`samples/generated/audit_1527/B2_mlsd_controlnet_seed42.png`](../samples/generated/audit_1527/B2_mlsd_controlnet_seed42.png)
- [`samples/generated/audit_1527/B2_mlsd_controlnet_seed43.png`](../samples/generated/audit_1527/B2_mlsd_controlnet_seed43.png)

Structured debug JSON:

- [`samples/generated/audit_1527/semantic_plan.json`](../samples/generated/audit_1527/semantic_plan.json)
- [`samples/generated/audit_1527/facade_elements.json`](../samples/generated/audit_1527/facade_elements.json)
- [`samples/generated/audit_1527/completion_proposals.json`](../samples/generated/audit_1527/completion_proposals.json)

Recovery Sprint 1 shell artifacts:

- [`samples/generated/audit_1527/sprint1_shell/shell_only_preview.png`](../samples/generated/audit_1527/sprint1_shell/shell_only_preview.png) — clean shell-only diagnostic preview; not final postcard output.
- [`samples/generated/audit_1527/sprint1_shell/shell_only.svg`](../samples/generated/audit_1527/sprint1_shell/shell_only.svg)
- [`samples/generated/audit_1527/sprint1_shell/debug/evidence_inventory.json`](../samples/generated/audit_1527/sprint1_shell/debug/evidence_inventory.json)
- [`samples/generated/audit_1527/sprint1_shell/debug/evidence_contact_sheet.png`](../samples/generated/audit_1527/sprint1_shell/debug/evidence_contact_sheet.png)
- [`samples/generated/audit_1527/sprint1_shell/debug/building_shell.json`](../samples/generated/audit_1527/sprint1_shell/debug/building_shell.json)
- [`samples/generated/audit_1527/sprint1_shell/debug/building_shell_overlay.png`](../samples/generated/audit_1527/sprint1_shell/debug/building_shell_overlay.png)
- [`samples/generated/audit_1527/sprint1_shell/debug/building_shell_score.json`](../samples/generated/audit_1527/sprint1_shell/debug/building_shell_score.json)

Recovery Sprint 2 plane artifacts:

- [`samples/generated/audit_1527/sprint2_plane/plane_scaffold_preview.png`](../samples/generated/audit_1527/sprint2_plane/plane_scaffold_preview.png) — clean shell + perspective/facade band scaffold; diagnostic, not final output.
- [`samples/generated/audit_1527/sprint2_plane/plane_scaffold.svg`](../samples/generated/audit_1527/sprint2_plane/plane_scaffold.svg)
- [`samples/generated/audit_1527/sprint2_plane/debug/plane_graph.json`](../samples/generated/audit_1527/sprint2_plane/debug/plane_graph.json)
- [`samples/generated/audit_1527/sprint2_plane/debug/plane_graph_overlay.png`](../samples/generated/audit_1527/sprint2_plane/debug/plane_graph_overlay.png)
- [`samples/generated/audit_1527/sprint2_plane/debug/plane_graph_score.json`](../samples/generated/audit_1527/sprint2_plane/debug/plane_graph_score.json)

Recovery Sprint 3 feature artifacts:

- [`samples/generated/audit_1527/sprint3_feature/feature_scaffold_preview.png`](../samples/generated/audit_1527/sprint3_feature/feature_scaffold_preview.png) — shell + plane scaffold + simplified windows/arches/balcony/pilaster evidence from FeatureGraph; diagnostic, not final polished postcard output.
- [`samples/generated/audit_1527/sprint3_feature/feature_scaffold.svg`](../samples/generated/audit_1527/sprint3_feature/feature_scaffold.svg)
- [`samples/generated/audit_1527/sprint3_feature/debug/feature_graph.json`](../samples/generated/audit_1527/sprint3_feature/debug/feature_graph.json)
- [`samples/generated/audit_1527/sprint3_feature/debug/feature_graph_overlay.png`](../samples/generated/audit_1527/sprint3_feature/debug/feature_graph_overlay.png)
- [`samples/generated/audit_1527/sprint3_feature/debug/feature_graph_score.json`](../samples/generated/audit_1527/sprint3_feature/debug/feature_graph_score.json)

Earlier curated baseline:

- [`samples/generated/sample_building_kaggle_20260614_1219_preview.png`](../samples/generated/sample_building_kaggle_20260614_1219_preview.png)
- [`samples/generated/sample_building_kaggle_20260614_1219_final.svg`](../samples/generated/sample_building_kaggle_20260614_1219_final.svg)
- [`samples/generated/sample_building_kaggle_20260614_1219_final.meta.json`](../samples/generated/sample_building_kaggle_20260614_1219_final.meta.json)

Full local artifacts, not intended for git:

- `artifacts/codex/contour-svg-sample/contour-svg-sample-20260614-1527/`
- `artifacts/codex/contour-svg-sample/contour-svg-sample-20260614-1603/`

The `1603` run failed early at `semantic_plan` with shared Gemini RPD. That is
expected fail-loud behavior under the project limiter contract.

## Suggested Global Redesign

The next implementation should add explicit graph objects and debug artifacts:

```text
EvidenceInventory
→ BuildingShell
→ PlaneGraph
→ FeatureGraph
→ Occluder-aware CompletionGraph
→ Neural/Gemini Fusion Editor
→ PrimitiveScene
→ Candidate Families
```

New artifacts to require:

- `debug/evidence_inventory.json`
- `debug/evidence_contact_sheet.png`
- `debug/building_shell.json`
- `debug/building_shell_overlay.png`
- `debug/plane_graph.json`
- `debug/plane_graph_overlay.png`
- `debug/feature_graph.json`
- `debug/fusion_editor_actions.json`
- `debug/neural_repair_contact_sheet.png`
- `debug/fusion_acceptance_report.json`

The critical acceptance question for each run:

> Did the pipeline understand the whole building before drawing details?

If not, the run should fail or rank low, even if some windows or roof fragments
look good.

## Questions For The External Agent

1. Is the proposed `BuildingShell → PlaneGraph → FeatureGraph` split the right
   abstraction, or should facade parsing produce another intermediate object?
2. How should the architecture elements library be converted from grammar notes
   into enforceable primitives and completion rules?
3. Which model should act as the fusion editor: Gemini over contact sheets,
   a vision-language model inside Kaggle, or a diffusion/control branch followed
   by structured extraction?
4. Can neural PNG proposals be used to propose missing primitives without
   causing identity drift?
5. What hard gates best measure "recognizable same building" before aesthetic
   ranking?
6. What should be the minimal next implementation slice that visibly improves
   the sample without overfitting?
