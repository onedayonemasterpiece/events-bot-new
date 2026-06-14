Neural Branch Reinstatement Plan v0.1

Project: Contour SVG Generator
Context: Alternative branch for neural simplification / controlled line-art generation
Status: Proposed branch to be implemented in parallel to the deterministic graph-first pipeline
Priority: High — must be run and shown in every real pipeline run, even if not selected as final

1. Why this branch must exist

The current project has a strong deterministic / graph-based recovery direction:

EvidenceInventory → BuildingShell → PlaneGraph → RhythmGraph → FeatureGraph → CompletionGraph → PrimitiveScene → SVG

This is the correct core architecture for control, traceability, SVG validity, and geometric consistency.

However, the project also has a second valid and important opportunity:

edge_map.png already captures the building very well as an image of lines;
it preserves the silhouette, perspective, corner, roof, windows, arches, and much of the recognizable architectural identity;
it is much closer to the target graphic style than the raw photo;
it is a strong candidate for neural simplification / stylization / completion.

Therefore the neural branch must be restored and formalized as a parallel alternative generation path, not removed.

Key principle

The neural branch is not a replacement for the deterministic graph pipeline.

The neural branch is a:

simplification branch,
completion proposal branch,
style translation branch,
comparison baseline branch.

Its job is to produce one or more line-based raster candidates that:

preserve the building identity,
simplify and enlarge lines,
reduce noise,
optionally continue lines across occluders,
stay visually close to the intended postcard / contour aesthetic.

The final SVG is still expected to be built by deterministic rendering or at least deterministic post-processing. But the neural branch may provide:

alternative canonical line-images,
strong comparison candidates,
merge evidence for the fusion editor,
optional guides for primitive extraction.
2. Mandatory product requirement

This branch must be executed in real runs and its outputs must be visible in artifacts, even if later rejected.

Hard requirement

For every full pipeline run, produce and save:

neural_branch/
  N0_inputs_contact_sheet.png
  N1_edge_only_seedXX.png
  N2_edge_plus_shell_seedXX.png
  N3_edge_plus_occluder_seedXX.png
  N4_edge_plus_features_seedXX.png
  N5_edge_plus_style_ref_seedXX.png
  contact_sheet.png
  neural_branch_report.json

At least one branch output must always be included in the run report and contact sheet.

Even if Gemini or CV ranking rejects all neural candidates, they must still be visible in outputs.

3. Conceptual role of the neural branch
3.1 What it should do

Input is already a graphic or semi-graphic representation:

edge_map / shell map / feature overlay / cleaned mask composite

Neural branch should transform this into:

cleaner, bolder, more coherent architectural line-art
3.2 What it must NOT do

It must not:

replace the building with another style or another structure;
invent a new building;
add decorative motifs unsupported by evidence;
turn the output into a filled illustration;
produce painterly / shaded / photorealistic results;
become the only final decision source.
3.3 Expected visual properties

All neural branch outputs must remain:

line-based,
mostly monochrome / contour-based,
readable at small size,
architecturally recognizable,
cleaner and more simplified than the raw edge map.

Allowed forms:

white / light contour on dark solid background;
dark contour on light solid background;
contour lines on transparent background.

Preferred for downstream work:

transparent or near-binary line output;
minimal fill;
strong silhouette.
4. Main hypothesis of this branch

The best neural inputs are not raw photos.

The best neural inputs are prepared structural images.

Strongest current base

Primary base image:

edge_map.png

Reason:

it already encodes the building as a line-image;
it has strong perspective and recognizable identity;
it is close to the intended final language;
neural model can focus on simplification rather than full scene understanding.
5. Variants of neural inputs to test

The implementation must support several input-preparation variants.

These variants are not optional. They are part of the debug and exploration space.

Variant family A — direct edge-based
A1. edge_only

Input: edge_map.png

Goal:

simplest baseline;
pure simplification / boldening / cleanup.

Use case:

check whether the neural model can already convert the existing line image into a stronger line image without extra structure.
A2. edge_binarized

Input: edge_map.png after thresholding and optional skeleton cleanup.

Goal:

reduce low-value grayscale noise;
give model a stricter signal.
A3. edge_thickened

Input: edge_map.png after slight dilation / thickening.

Goal:

encourage larger, more poster-like lines;
help the model read large structure instead of micro-detail.
Variant family B — shell-guided edge inputs
B1. edge_plus_shell

Input composition:

base: edge_map.png
plus masked_background or shell outline
optional highlight of outer silhouette

Goal:

encourage stronger outer shape;
prevent the model from losing building mass.
B2. edge_plus_wall_plane

Input composition:

base: edge_map.png
plus wall_plane / facade plane cues

Goal:

help preserve front vs side plane relationship;
help simplification keep perspective logic.
Variant family C — occluder-aware inputs
C1. edge_plus_occluder_mask

Input composition:

base: edge_map.png
plus occluder_mask

Goal:

explicitly tell the model where vegetation / tree / fence interference exists;
encourage continuation through those areas.

Important:

The prompt must state that occluded areas may be interpolated conservatively, but no new volumes may be invented outside the visible shell.

C2. edge_minus_occluders

Input composition:

base: cleaned edge map where occluder-heavy regions are suppressed or masked;
optional shell continuation hints.

Goal:

prevent the model from tracing foliage as architecture.
Variant family D — semantic feature-guided inputs
D1. edge_plus_features

Input composition:

base: edge_map.png
plus elements_overlay simplified to feature hints

Goal:

reinforce windows, arches, balcony, pilasters;
help the model preserve identifiable features.
D2. edge_plus_planes_plus_features

Input composition:

base: edge_map.png
plus shell / plane scaffold
plus simplified feature hints

Goal:

strongest structured input without using the raw photo.
Variant family E — style-guided inputs
E1. edge_plus_style_reference

Input composition:

primary structural input: edge_map.png
optional style reference image(s) showing desired thick clean line-art

Goal:

translate from rough edge language into target postcard style.

Note:

This is a style-guided branch. It is valuable for comparison but must be checked carefully for geometry drift.

E2. edge_plus_target_palette

Input composition:

same as above,
plus explicit palette control or post-process palette normalization.

Goal:

keep the visual output closer to brand / reference style.
Variant family F — photo-assisted but edge-led
F1. edge_with_photo_low_weight

Input composition:

edge map as primary control;
raw photo as secondary weak context.

Goal:

allow the model to recover certain structural truths from the photo;
still keep line map as the main guide.

Warning:

Use only as an optional comparison variant. Risk of style drift is higher.

6. Recommended prompt families

All prompts should insist on the following:

contour drawing,
architectural line-art,
simplified lines,
preserved perspective,
preserved identity,
no texture / no shading,
no foliage lines,
no fence,
no road texture,
no extra objects,
conservative continuation behind occluders.
Prompt family P1 — simplification
Convert the provided architectural line image into a cleaner, bolder, simplified contour drawing.
Preserve the building identity, silhouette, perspective, main corner, roofline, windows, arches, pilasters and base.
Reduce line clutter, merge nearby redundant lines, and produce larger, more confident strokes.
Keep the result strictly as line art with no shading, no textures, no foliage contours, no fence, and no background details.
Prompt family P2 — postcard line-art
Create a clean postcard-like architectural contour illustration from the provided structural line image.
Keep the building recognizable and preserve its perspective and key architectural features.
Use fewer, larger, elegant lines with simplified geometry.
Remove tiny noisy details and keep only the most important architectural contours.
The result must remain a line-based image, suitable for later vectorization.
Prompt family P3 — conservative completion
Using the provided building line image and occluder guidance, simplify and clean the contour drawing.
Where the building is partially hidden by trees or other occluders, continue only the most likely architectural lines conservatively.
Do not invent new building volumes or decorative details outside the visible structure.
Preserve the silhouette and perspective of the real building.
Prompt family P4 — style translation
Translate the provided rough architectural line image into a refined, clean, geometric architectural line-art style.
Keep the same building identity and perspective, but simplify the line system into fewer, more legible contour lines.
Make the result feel like a strong graphic illustration with clean, confident contours.
7. Model and tool options

This document does not force a single model, but requires one or more neural image-to-image branches.

7.1 Recommended families
Option N-A — Stable Diffusion + ControlNet

Recommended for:

edge-led img2img;
structure-preserving stylization;
canny / lineart / scribble conditioning;
multiple candidates with seeds.

Possible controls:

lineart
canny
scribble
MLSD-like structural guides

Best use here:

generate 4–12 controlled candidates from edge_map-based inputs.
Option N-B — IP-Adapter / reference-guided img2img

Recommended for:

style-guided branch;
preserving a reference look while keeping structure.

Best use here:

combine structural input with one or more target-style line-art references.
Option N-C — direct image-to-image line simplification model

If a lightweight or task-specific model is available for:

sketch simplification,
line-art cleanup,
contour extraction simplification,

it may be tested as a sub-branch.

Option N-D — multimodal LLM or VLM as critic, not renderer

Gemini may be used to:

compare neural candidates,
judge identity preservation,
identify drift,
recommend which candidate should be merged into the main pipeline.

Gemini should not be responsible for pixel-perfect geometry.

8. Deterministic normalizer: optional or not?

The author explicitly raised the question: should there be a deterministic normalizer in the middle or can we try without it?

Answer

Both modes should exist.

Mode M1 — no intermediate deterministic normalizer
prepared structural input
→ neural img2img / ControlNet
→ raster output candidate
→ post-filter / compare / show

This mode must exist because:

it is simple;
it tests the raw power of the neural branch;
it may already give surprisingly strong results;
it is useful as a clean comparison branch.
Required artifact name
neural_raw_mode/
Mode M2 — with intermediate deterministic normalizer
prepared structural inputs
→ deterministic cleanup / compositing / binary control prep
→ neural img2img / ControlNet
→ raster output candidate
→ deterministic post-clean / vector-aware filtering

This mode must also exist because:

it gives the neural model a better, cleaner signal;
it may reduce drift;
it may help keep the output line-based.
Required artifact name
neural_normalized_mode/
Important implementation note

The first implementation may start with M1 and then add M2.

But the repository and Kaggle notebook must be structured so that both can be run.

9. Expected outputs of the neural branch

Every neural branch candidate must stay in lines.

Hard output rules

The generated result must be:

line-based;
recognizable as the same building;
free of foliage/fence/background clutter;
simpler and bolder than the source edge map;
saveable as a raster candidate for later vectorization or comparison.
Output formats

For each candidate save:

candidate.png
candidate_thresholded.png
candidate_overlay_vs_edge.png
candidate_report.json

Optional:

candidate_vector_preview.svg
candidate_vector_preview.png
10. How neural branch integrates with the main fusion architecture
10.1 Position in the pipeline
photo
→ masks / guides / facade parser / shell hints / evidence prep
→ [A] deterministic graph core
→ [B] neural branch
→ fusion / comparison / ranking
→ final SVG decision
Neural branch receives from upstream
edge_map.png
mlsd_guide.png
deeplsd_lines_overlay.png
masked_background.png
wall_plane.png
elements_overlay.png
occluder_mask.png
optional style references
Neural branch returns
raster line-art candidates;
optional simplified binary candidates;
optional suggested guide maps.
10.2 What neural outputs are allowed to become

Allowed roles:

proposal
comparison_candidate
merge_evidence
primitive_extraction_guide

Not automatically allowed:

direct final SVG without validation.

If a neural candidate is extremely good, it may still be used as a near-final raster template, but must go through:

structural validation,
geometric sanity check,
optional vector cleanup,
report visibility.
11. Merge strategy after neural generation

The neural branch should not be a dead-end. Its results must be mergeable back into the system.

Merge levels
Merge level L1 — comparison only
neural output shown side by side with deterministic candidates;
Gemini / CV ranking decides whether it is useful.
Merge level L2 — evidence support
neural output is analyzed to confirm or deny:
roof lines,
silhouette,
windows,
arches,
balcony,
pilaster arrangement.
Merge level L3 — primitive extraction guide
extract simplified contours or junction hints from the neural raster;
compare them with current PrimitiveScene;
allow them to support merge decisions.
Merge level L4 — raster template for vector drafting
a top neural candidate is converted into a guide layer for deterministic vector drafting.
12. Ranking and rejection policy

The neural branch may be rejected from final selection, but only after full reporting.

Ranking dimensions

Each candidate should be scored for:

identity preservation;
silhouette strength;
line simplicity;
perspective consistency;
architectural recognizability;
occluder cleanup;
style closeness to desired contour aesthetic;
vectorization readiness.
Mandatory report fields
{
  "branch_name": "N2_edge_plus_shell",
  "mode": "neural_raw_mode",
  "model": "...",
  "seed": 42,
  "identity_score": 0.0,
  "postcardness_score": 0.0,
  "structure_score": 0.0,
  "line_simplicity_score": 0.0,
  "vectorization_readiness": 0.0,
  "accepted_for_fusion": true,
  "accepted_as_final": false,
  "rejection_reason": "..."
}

Even rejected candidates must remain visible in contact_sheet.png.

13. Proposed immediate experiment matrix

Start with a small but meaningful matrix.

Inputs
A1 edge_only
B1 edge_plus_shell
C1 edge_plus_occluder_mask
D1 edge_plus_features
D2 edge_plus_planes_plus_features
E1 edge_plus_style_reference
Modes
M1 neural_raw_mode
M2 neural_normalized_mode
Seeds
3 seeds minimum for each main branch, or 1 seed if runtime is very tight.
Minimal matrix
A1 / M1
B1 / M1
C1 / M1
D1 / M1
D2 / M1
E1 / M1
Preferred matrix
A1 / M1
A1 / M2
B1 / M1
B1 / M2
C1 / M1
D1 / M1
D2 / M1
E1 / M1
14. Debug assets that must be saved
neural_branch/
  input_maps/
    edge_map.png
    edge_binarized.png
    edge_thickened.png
    edge_plus_shell.png
    edge_plus_occluder.png
    edge_plus_features.png
    edge_plus_planes_plus_features.png
  raw_candidates/
  normalized_candidates/
  contact_sheet.png
  top3_contact_sheet.png
  neural_branch_report.json
  neural_vs_deterministic_comparison.png

This is required so the branch can be visually audited later.

15. Acceptance criteria for first implementation

The branch is considered implemented when:

It runs on a real sample using edge_map.png as the main base.
It produces at least 4 neural candidates.
At least one candidate is visibly line-based and simpler / bolder than the input edge map.
The run saves all outputs and reports.
The branch result is included in the global contact sheet.
The branch result is visible even if it loses in ranking.
The notebook / status messages explicitly mention the neural branch phase.
16. Recommended implementation order
Phase N0 — restore the branch
create explicit module / folder for neural_branch;
wire the branch into the run pipeline;
guarantee artifacts are saved.
Phase N1 — edge-only baseline
use edge_map.png as base;
run 1–2 core neural variants;
produce visible line-art outputs.
Phase N2 — structured input composites
add edge_plus_shell;
add edge_plus_occluder_mask;
add edge_plus_features.
Phase N3 — style-guided branch
add edge_plus_style_reference.
Phase N4 — merge back
compare with deterministic graph output;
allow neural result to influence fusion decisions.
17. Instructions to the implementing agent
1. Reinstate the neural branch.
2. Do not position it as the sole final renderer.
3. Use edge_map.png as the primary base input.
4. Implement at least the following prepared inputs:
   - edge_only
   - edge_plus_shell
   - edge_plus_occluder_mask
   - edge_plus_features
5. Support two modes:
   - neural_raw_mode
   - neural_normalized_mode
6. Save visible outputs for every neural candidate.
7. Ensure all neural outputs remain line-based.
8. Include neural outputs in run reports and contact sheets even if rejected.
9. Let Gemini rank / critique them, but never hide them.
10. The first real run after implementation must show the branch outputs explicitly.
18. Final architectural stance

This branch is not a detour.

It is a valid parallel path because the project already has a strong intermediate artifact (edge_map.png) that is visually meaningful and close to the desired outcome.

Final stance
Deterministic graph core = truth / geometry / SVG control.
Neural branch = simplification / completion / style proposal.
Fusion = combine both and decide.

The system should not choose one ideology only.

It should exploit both:

deterministic structure,
neural visual abstraction.

That is the most realistic path toward a strong final contour-SVG result.

19. Optional future extensions
neural branch fed by multi-panel structured input contact sheet;
neural branch conditioned on silhouette + feature hints;
primitive extraction from top neural candidates;
iterative loop:
deterministic scene draft → neural stylization → deterministic cleanup;
specialized architecture line-art LoRA if dataset becomes available.
20. Required visible outcome after next real run

After the next real run, the author should be able to inspect:

the deterministic branch result,
the neural branch result(s),
the comparison sheet,
the rejection/acceptance report.

Specifically, the run must visibly include at least one line-based neural output derived from edge_map.png.

This is mandatory.