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

## Override — 2026-06-14 late iteration

The earlier source-photo img2img decision is now explicitly overridden for the
separate neural branch. The 18:49 Kaggle run proved the failure mode: using the
source photo as the init image anchors Stable Diffusion in photo/render mode,
and thresholding that output produces noisy foliage-heavy edge maps rather than
designer line art.

Current policy:

- default neural branch is **line-to-line**;
- prepared `edge_map`/cleaned line maps are both img2img `image` init and
  ControlNet condition;
- the source photo is not mentioned in positive prompts;
- source-photo init is allowed only in explicit `photo_assisted` research mode;
- style reference `samples/output/IMG_20260614_115550.webp` may be used only as
  style/reference conditioning, not geometry or identity.

## Working Decision

Keep the neural branch, but constrain it to proposal/evidence roles:

- never directly write `final.svg`;
- never become the final winning family without primitive rendering;
- use prepared line maps as img2img init image by default;
- use the same prepared line maps as ControlNet lineart condition;
- use style-only positive prompts that do not mention photos;
- keep `photo_assisted` as explicit research mode only, never default;
- keep all neural artifacts in `candidates/` and `debug/neural_branch_raw/`.

## Implementation Recipe

```text
edge_map.png / cleaned line map
→ optional occluder erasure and facade feature hints
→ prepared black-line-on-white structural line map
→ SD1.5 StableDiffusionControlNetImg2ImgPipeline
   image=prepared line map
   control_image=prepared line map
   ControlNet(lineart)
→ optional IP-Adapter image prompt from the style reference for E1
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
strength: 0.60
guidance_scale: 9.0
controlnet_conditioning_scale: 0.75
style reference adapter: h94/IP-Adapter / models / ip-adapter_sd15.bin
style_reference_adapter_scale: 0.55
style_reference_strength: 0.65
```

Positive prompt is style-only:

```text
clean black-ink architectural contour line art on pure white background, bold confident strokes, simplified geometry, recognizable building silhouette, roofline, corners, cornices, windows, arches, base, no shading, no fill, no texture
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
- corrected line-init output:
  `artifacts/codex/contour-svg-neural-branch-kaggle/contour-svg-neural-20260614-1923/`

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

- `result.png` from the 18:49 run exists, but it is now classified as a
  negative photo-init probe rather than the desired neural branch output.
- `result.png` from the 19:23 run is the first corrected line-init output:
  `source_photo_init_used=false`, all candidates use `init_mode=line_init`, and
  the output pack includes raw candidates, strict black-on-white line masks,
  transparent line PNGs, burgundy previews and per-candidate line-only gates.
- The branch now switches default policy to line-to-line: `edge_only`,
  `edge_thickened`, `edge_minus_occluders`, `edge_plus_features`, and
  `edge_plus_style_reference` become prepared line-init/control maps.
- `edge_plus_style_reference` loads SD1.5 IP-Adapter style-reference weights.
  The reference defines line economy and target look only; it must not define
  geometry or identity.
- A failed attempt exposed a Diffusers `SlicedAttnProcessor` conflict after
  attention slicing; the completed code avoids enabling attention slicing before
  IP-Adapter loading.
- The previous completed output is still visually below target because the raw
  image remained too close to photo mode. That is the reason for the line-init
  override.
- The corrected line-init result is materially closer to the intended family
  because it no longer returns color/photo renderings. It is still a proposal:
  lines remain noisy and the geometry/detail balance needs further model/input
  cleanup before it can be called target-quality.

Conclusion:

- The neural branch is viable as a visible proposal/comparison path.
- The latest photo-init probe did produce a concrete PNG artifact path, but not
  the requested bold two-color postcard quality.
- Next neural run should validate the corrected line-init default, reject
  photo-like/color candidates through hard gates, and use the target reference
  only as style guidance.
