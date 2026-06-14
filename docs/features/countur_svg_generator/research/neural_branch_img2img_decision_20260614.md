# Neural Branch Img2Img Decision — 2026-06-14

This is a working implementation note, not a replacement for the canonical
requirements. It records the decision made during prototype implementation after
reviewing Diffusers ControlNet docs, inspecting the bad ControlNet output, and
asking the project Opus agent for a focused prompt/model critique.

## Problem Observed

The first SD1.5 ControlNet proposal branch ran technically, but it generated a
different generic classical building. The branch used text-to-image ControlNet:
the latent started from noise, and the control image plus prompt were not enough
to preserve the source building identity.

This does not prove the neural branch is useless. It proves the current recipe
was wrong for identity preservation.

## Working Decision

Keep the neural branch, but constrain it to proposal/evidence roles:

- never directly write `final.svg`;
- never become the final winning family without primitive rendering;
- use the source photo as img2img init image;
- neutralize occluders before diffusion;
- condition on lineart or M-LSD plus depth;
- use style-only positive prompts and identity-defense negative prompts;
- keep all neural artifacts in `candidates/` and `debug/neural_branch_raw/`.

## Implementation Recipe

```text
source photo
→ crop by primary object mask + margin
→ neutralize occluders using multi-state masks
→ lineart / M-LSD guide with occluder regions erased
→ Depth Anything V2 depth guide from neutralized source image
→ SD1.5 StableDiffusionControlNetImg2ImgPipeline
   with ControlNet(lineart or MLSD) + ControlNet(depth)
→ optional IP-Adapter image prompt from the style reference for B3/B4
→ proposal-only raster artifacts
→ optional vectorization / line-evidence experiments
```

Current prototype defaults:

```text
pipeline: StableDiffusionControlNetImg2ImgPipeline
base: runwayml/stable-diffusion-v1-5
lineart: lllyasviel/control_v11p_sd15_lineart
mlsd: lllyasviel/control_v11p_sd15_mlsd
depth: lllyasviel/control_v11f1p_sd15_depth
depth model: depth-anything/Depth-Anything-V2-Small-hf
strength: 0.40
guidance_scale: 5.0
controlnet_conditioning_scale: [1.15, 0.80]
control_guidance_end: 0.85
style reference adapter: h94/IP-Adapter / models / ip-adapter_sd15.bin
style_reference_adapter_scale: 0.35
style_reference_strength: 0.34
```

Positive prompt is style-only:

```text
clean minimal monoline contour drawing, white ink on dark background, even line weight, transparent-style line art, vector poster aesthetic, geometric perspective lines, calm composition
```

Negative prompt defends identity:

```text
different building, alternate facade, modified massing, added wings, extra windows, hallucinated columns, generic classical building, white marble, stock illustration, photo, realistic, color fill, shading, gradient, sketch noise, scribbles, crosshatching, texture, bricks, trees, foliage, fence, people, cars, street, pavement, watermark, text, blur, dense lines, tiny details
```

## Renderer Decision

The postcard/sketch look should not be delegated to diffusion. It is a primitive
SVG rendering concern. The prototype adds `style.stroke_style=sketch`, which
renders accepted primitive lines as multiple slightly jittered stroke passes
while preserving the SVG contract: no raster images, no base64, stroke paths
only.

## Remaining Work

- Add a real identity gate before neural evidence affects ranking:
  - SAM2/SAM2-like mask IoU between source and neural raster;
  - Gemini same-building check through the shared limiter;
  - roof/top-profile and window-count proxy metrics.
- Add `debug/neural_branch_eval.json`.
- Decide whether neural lines may only up-vote source-photo lines or can propose
  missing silhouette segments in object-unknown regions.
- Benchmark on the sample building and `to_do/92-11-16.jpg`.

## Alternative Experiment: Style Reference Conditioning

The user hypothesis remains valid for research: a neural model may eventually
produce the best final-looking line-art image, especially when given an example
of the desired style. The current prototype should not collapse that branch
after one bad ControlNet run.

Implemented candidate experiment:

```text
source photo + object/occluder masks
reference style image (`samples/output/...` or `to_do/92-11-16.jpg`)
→ source-preserving img2img / IP-Adapter reference-conditioned branch
→ identity gate
→ style similarity / two-color line-art gate
→ primitive extraction or SVG reconstruction
```

Important constraints:

- The reference image may define style only: two-color line drawing, line
  economy, stroke rhythm, architectural simplification, background/foreground
  contrast.
- It must not define object identity. The output must preserve the source
  building massing, roof/corner/facade/window rhythm and occlusion policy.
- The result can be shown as PNG during research, but it cannot become
  `final.svg` unless converted into editable primitive SVG and passed through
  the normal hard gates.
- Useful future tools to test beyond the current IP-Adapter branch:
  ControlNet Reference-style workflows, InstantStyle-style layer scaling, or a
  two-stage Gemini/vision judge that scores "same building" and "same visual
  approach" separately.

Current implementation:

- `input.style_reference_path` may point to an example line-art image.
- Gemini candidate ranking receives it as style context only.
- The prompt explicitly says the source image defines object identity and the
  style reference must not be copied as object geometry.
- `B3_ref_lineart_depth` and `B4_ref_mlsd_depth` load SD1.5 IP-Adapter weights
  through Diffusers, pass the style reference via `ip_adapter_image`, and keep a
  lower img2img strength than the base branch to reduce identity drift.

## Separate Neural Branch Probe — 2026-06-14 18:16 UTC / 18:49 UTC

To avoid running the whole SVG pipeline while testing the user's mask/edge
neural hypothesis, a separate Kaggle probe was added:

- kernel: `zigomaro/contour-svg-neural-branch`
- local launcher: `scripts/run_contour_svg_neural_branch_kaggle.py`
- package helper/CLI: `contour_svg.neural_branch` and
  `python -m contour_svg neural-branch ...`
- downloaded output:
  `artifacts/codex/contour-svg-neural-branch-kaggle/contour-svg-neural-20260614-1816/`
- latest completed output:
  `artifacts/codex/contour-svg-neural-branch-kaggle/contour-svg-neural-20260614-1849/`

The probe used the existing `audit_1527` evidence pack and produced:

```text
neural_branch/
  N0_inputs_contact_sheet.png
  contact_sheet.png
  top3_contact_sheet.png
  input_maps/
  raw_candidates/
  normalized_candidates/
  neural_branch_report.json
```

Status evidence from the completed 18:49 run:

- `preflight_ok` saw a Tesla T4 and CUDA-enabled torch.
- `neural_inputs_started` prepared edge/mask/feature composites.
- `neural_img2img_started` generated 5 real ControlNet lineart candidates.
- `alive` heartbeat events were emitted during the neural step.
- `neural_report_written` and `report_written` completed with
  `candidate_count=5`.

Visual result:

- `result.png` now exists and is a concrete black-line-on-white output selected
  from the thresholded neural candidates.
- `result_raw.png` shows the underlying neural image before thresholding.
- The branch now uses the original source photo as img2img init and uses
  `edge_only`, `edge_thickened`, `edge_minus_occluders`, `edge_plus_features`,
  and `photo_plus_edge_style_reference` as ControlNet inputs.
- `photo_plus_edge_style_reference` now loads SD1.5 IP-Adapter style-reference
  weights. The first attempt failed on Diffusers `SlicedAttnProcessor` after
  attention slicing; the completed run fixed this by not enabling attention
  slicing before IP-Adapter loading.
- The completed output is still visually below target: the raw image remains too
  close to photo mode, and thresholding preserves too much tree/foliage noise
  while losing some facade clarity. It is a useful concrete probe, not an
  accepted target-quality result.

Conclusion:

- The neural branch is viable as a visible proposal/comparison path.
- The latest probe did produce the requested concrete PNG artifact path, but not
  the requested bold two-color postcard quality.
- Next neural run should increase style-rewrite strength, shorten prompts to fit
  CLIP, suppress photo/shading/color mode harder, and test mask/inpaint or
  reference-conditioning variants that remove foliage as content rather than
  merely thresholding it afterward.
