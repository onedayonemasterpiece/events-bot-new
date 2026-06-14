# Contour SVG Generator Tool Documentation Audit — 2026-06-14

Purpose: make the external-tool research explicit before continuing v0.3
implementation/debugging. Earlier implementation moved too far on partial
knowledge. This document records which tool docs were checked, what they imply
for the pipeline, what is implemented now, and what remains a gap.

Canonical requirement reminder:

- final path: `multi-state masks -> candidate line graph -> semantic pruning -> primitive rendering -> final.svg`;
- no surrogate/fallback final path;
- Gemini only through the shared `GoogleAIClient` / `contour_svg.llm_gateway`
  limiter path;
- Kaggle run must expose stage-level status with throttled heartbeat and useful
  stage labels.

## Summary

Current implementation is closer to the intended v0.3 stack than the first
baseline, but it is still not a complete "fully researched, fully implemented"
pipeline. The most important remaining gap is not line extraction alone; it is
the semantic architecture loop after extraction:

1. every detector/parser must emit evidence into a shared typed graph;
2. line groups must be linked to facade elements and vanishing-plane structure;
3. conservative completion must reconstruct missing silhouette/roof/facade
   pieces from semantic evidence, not just fill local gaps;
4. ControlNet/diffusion branches must generate a visible two-color proposal
   image, but cannot become `final.svg` without primitive extraction/rendering;
5. Gemini must judge/edit bounded groups and candidates through the shared
   limiter, not act as a coordinate SVG generator.

## P0 / P1 Tool Status

| Area | Docs checked | What docs imply | Current implementation | Gap / action |
|---|---|---|---|---|
| Kaggle runtime | Kaggle Notebooks, Models, Datasets docs; Kaggle kernel metadata docs; project Kaggle status framework and secrets docs | Stable notebook slug/title is required for versioned kernels; datasets/models carry weights/assets; private dataset secret handoff; visible progress events | Stable slug `zigomaro/contour-svg-generator`; local launcher creates per-run payload/secrets; status-aware script emits staged progress | Need runtime lock captured in artifact: Python, torch, CUDA, diffusers, transformers, model refs, checkpoints, git SHA. Prefer Kaggle Models/Datasets for heavy weights once tools stabilize. |
| Gemini structured output | Gemini quickstart, libraries, structured output docs; project LLM gateway docs | Official SDK examples instantiate SDK clients, but this project must wrap all calls through `GoogleAIClient` and shared limiter | Feature code uses `contour_svg.llm_gateway`; direct SDK use is forbidden by tests/contracts | Need audit all prompt/schema docs for leftover direct `genai.Client` examples and replace with gateway-only snippets. |
| GroundingDINO | HF Transformers GroundingDINO docs; Grounded SAM / Grounded SAM2 references | Open-vocabulary detection with text labels; class prompts should be period-separated and postprocessed with grounded detection API | HF route is implemented for primary and occluder boxes; debug JSON is written | Need prompt sets and thresholds moved into explicit config/evidence table. Detector ensemble should report per-tool failures and confidences without silent substitution. |
| SAM2 | Official SAM2 repo and HF Transformers SAM2 docs | SAM2 inference/checkpoints need careful runtime handling; box prompts from detectors are the intended route | SAM2 object/occluder masks are mandatory; debug masks/overlays are emitted | Need checkpoint/version/runtime metadata in output. Need stronger mask QA before downstream stages. |
| Florence-2 | HF Transformers Florence-2 docs | Prompt-based vision model can provide detection/caption-like evidence; useful as secondary object/scene evidence | Florence-2 route exists, but sample run produced no useful boxes | Keep as evidence branch only until prompt/output parsing is proven. Do not count it as satisfying primary detection unless it emits boxes. |
| YOLO-World | Ultralytics YOLO-World docs and YOLO-World repo | Open-vocabulary detector with `set_classes`; practical route via Ultralytics, but it pulls CLIP/model assets and needs cache control | YOLO-World route exists and produced occluder evidence in sample attempts | Need cache/path preflight so Kaggle output is not polluted by CLIP/model downloads. Need model/license/version pinning. |
| CMP Facade / SegFormer | CMP Facade database, HF dataset, SegFormer CMP model | CMP has 606 annotated rectified facade images and classes: facade, molding, cornice, pillar, window, door, sill, blind, balcony, shop, deco, background | CMP SegFormer parser is implemented; emits per-class masks, elements JSON, overlay | Need connect facade elements into line graph and primitive budgets more deeply. Current use is still too shallow for roof/cornice/window reconstruction. |
| 3DOM Semantic Facade | 3DOM repo | Historic facade segmentation classes include wall/facade, window, door, sky, obstacle; useful for occlusion/background separation | Not implemented | P1 research branch. Useful for tower/historic facade samples, especially obstacle/vegetation rejection. |
| DeepFacade / RTFP / SAAF | DeepFacade repo/paper, RTFP repo, SAAF paper | Useful research for symmetry, line-aware facade parsing, wall/window segmentation and completion; not simple drop-in runtime components | Not implemented as models | Extract ideas into primitive grammar/completion rules. Do not add as mandatory runtime until install/model availability is verified. |
| M-LSD | M-LSD repo and `controlnet_aux` usage | Architecture-friendly line detector; useful as a line graph source and ControlNet guide | M-LSD guide is mandatory and written to debug | Need source-quality metrics and line grouping by vanishing direction / facade plane. |
| DeepLSD | DeepLSD repo | Quickstart inference can run without final line refinement; full refinement depends on native `pytlsd`/`line_refinement` and older tested CUDA/Python stack | Runtime clone/checkpoint path implemented; current Kaggle adaptation uses quickstart-style import stubs to avoid native build failure on Python 3.12 | This needs an explicit documented decision: either pin an older compatible runtime / prebuilt dataset, or accept "DeepLSD inference without final refinement" as a named mode. It must not be hidden as if full DeepLSD refinement is active. |
| HAWP | HAWP repo | Wireframe parser can emit vertices/edges; valuable for junction graph, not just loose line segments | CLI JSON route implemented using `hawp.ssl.predict` | Need junction graph preservation, not only conversion to independent lines. HAWP failures should include stderr and version/runtime evidence. |
| Hough/LSD/OpenCV | OpenCV Hough docs/tutorials | Deterministic line sources are acceptable as evidence sources, not as final fallback | Implemented as line source | Keep as evidence source only; final must still pass primitive-renderer gate. |
| Shapely / NetworkX / RDP / scikit-image | Official docs | Geometry ops, graph ops, simplification and morphology are the right base for candidate graph and primitives | Partially used; graph/primitive abstraction exists but is still modest | Serious work remains: true graph topology, junction merging, plane grouping, arc/ellipse fitting, repeated-opening constraints. |
| Diffusers ControlNet + IP-Adapter | Diffusers ControlNet guide, IP-Adapter guide and SD1.5 ControlNet model cards | ControlNet can condition generation on lineart/MLSD/depth; source identity requires img2img init, not text-to-image from noise. IP-Adapter adds image-based guidance and exposes scale control, so a sample output can guide style without being copied as object identity. | B1/B2 are source-preserving `StableDiffusionControlNetImg2ImgPipeline` proposal branches with source-photo init, occluder-neutralized controls, lineart/MLSD + Depth Anything V2 and style-only prompts. B3/B4 additionally load `h94/IP-Adapter` and pass `input.style_reference_path` through `ip_adapter_image`. | Need identity metrics/gate before neural evidence affects final scoring by default. It still cannot directly become final. |
| Inpainting / LaMa | Diffusers inpaint docs, LaMa repo | Useful for occluder removal proposal, high hallucination risk | Not implemented as mandatory v0.3 path | P1/P2 proposal branch only. Need mask QA and conservative completion before enabling. |
| Depth Anything / MiDaS | HF/GitHub docs | Depth can help preserve planes/background and reduce ControlNet identity drift | Depth Anything V2 Small HF is used as the second ControlNet condition for source-preserving neural proposals | Need compare depth branch on the sample/tower benchmarks and persist identity metrics. |
| VTracer / Potrace | Official docs | Raster-to-vector tools trace bitmap shapes; VTracer handles color/high-res, Potrace traces bitmaps | Proposal/vector-trace branches exist as non-final candidates | Keep non-final only. They do not satisfy editable architectural stroke primitive output. |
| svgwrite / SVGO | Official docs | Programmatic SVG rendering and optimization | `svgwrite` renderer and SVG gates exist | Need stronger final SVG contract tests around primitive metadata and line hierarchy. |
| Aesthetic/preference models | LAION aesthetic predictor, improved aesthetic predictor, PickScore, ImageReward, HPS docs | Useful for visual ranking, but less important than semantic/geometry gates | Not implemented | Do not prioritize before sample acceptance; Gemini/CV/hard gates are enough for now. |

## Immediate Implementation Corrections

1. Treat this audit as a preflight checklist before further tool-level retries.
2. Record runtime/tool decisions in `final.meta.json`, especially DeepLSD mode,
   HAWP mode, YOLO cache paths and exact model identifiers.
3. Add an artifact-level evidence file such as `debug/tool_evidence.json` with:
   enabled tools, loaded model path/ref, success/failure, output counts and
   debug artifact paths.
4. Strengthen line graph semantics:
   - preserve HAWP junctions;
   - group DeepLSD/M-LSD/Hough/LSD lines by vanishing direction and facade plane;
   - attach CMP facade elements to nearest line groups;
   - require silhouette/roof/facade groups before any final candidate.
5. Strengthen conservative completion:
   - complete global roof/silhouette/facade shell first;
   - then complete repeated windows/cornices/porticos;
   - mark every completion as visible/interpolated/ambiguous with evidence.
6. Ensure the neural two-color proposal image exists as a debug/proposal output,
   but route it through primitive extraction before it can influence final SVG.

## Status Discipline

Kaggle progress must expose the current domain stage without message storms. The
status stream should show coarse stages such as:

- `preflight`
- `semantic_plan`
- `detectors`
- `sam2_masks`
- `facade_parser`
- `line_sources`
- `line_graph`
- `completion`
- `neural_proposals`
- `primitive_candidates`
- `ranking`
- `export`

Every long stage should include `progress_percent`, `progress_label`, and enough
counts to diagnose stalls: detector counts, mask area, facade element count,
line counts by source, completion proposal count, candidate count, and ranking
winner.

## Sources Opened

- Kaggle: https://www.kaggle.com/docs/notebooks, https://www.kaggle.com/docs/models, https://www.kaggle.com/docs/datasets, https://github.com/Kaggle/kaggle-cli/blob/main/docs/kernels_metadata.md
- Gemini: https://ai.google.dev/gemini-api/docs/quickstart, https://ai.google.dev/gemini-api/docs/libraries, https://ai.google.dev/gemini-api/docs/structured-output
- Detection/segmentation: https://huggingface.co/docs/transformers/en/model_doc/grounding-dino, https://github.com/IDEA-Research/Grounded-SAM-2, https://github.com/facebookresearch/sam2, https://huggingface.co/docs/transformers/en/model_doc/sam2, https://huggingface.co/docs/transformers/en/model_doc/florence2, https://docs.ultralytics.com/models/yolo-world/
- Facade parsing: https://cmp.felk.cvut.cz/~tylecr1/facade/, https://huggingface.co/datasets/Xpitfire/cmp_facade, https://huggingface.co/Xpitfire/segformer-finetuned-segments-cmp-facade, https://github.com/3DOM-FBK/3DOM-Semantic-Facade, https://github.com/liuhantang/DeepFacade, https://www.ijcai.org/proceedings/2017/0320.pdf, https://github.com/wbw520/RTFP, https://arxiv.org/abs/2506.09071, https://www.mdpi.com/2075-5309/14/9/2602
- Line/geometry: https://github.com/navervision/mlsd, https://github.com/cvg/DeepLSD, https://github.com/cherubicXN/hawp, https://shapely.readthedocs.io/, https://networkx.org/documentation/stable/tutorial.html, https://scikit-image.org/, https://rdp.readthedocs.io/
- Diffusion/proposals: https://huggingface.co/docs/diffusers/en/using-diffusers/controlnet, https://huggingface.co/docs/diffusers/en/using-diffusers/inpaint, https://github.com/advimman/lama, https://github.com/DepthAnything/Depth-Anything-V2, https://huggingface.co/docs/transformers/en/model_doc/depth_anything_v2, https://github.com/isl-org/MiDaS
- SVG/vector/ranking: https://github.com/visioncortex/vtracer, https://potrace.sourceforge.net/, https://potrace.sourceforge.net/potrace.1.html, https://svgwrite.readthedocs.io/, https://svgo.dev/, https://github.com/LAION-AI/aesthetic-predictor, https://github.com/christophschuhmann/improved-aesthetic-predictor, https://huggingface.co/yuvalkirstain/PickScore_v1, https://github.com/zai-org/ImageReward, https://huggingface.co/zai-org/ImageReward, https://github.com/tgxs002/HPSv2
