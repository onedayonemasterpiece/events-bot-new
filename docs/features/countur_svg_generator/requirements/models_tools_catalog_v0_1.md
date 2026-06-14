# Models and Tools Catalog v0.1

**Purpose:** catalogue of models, libraries, tutorials and docs for the Contour SVG Generator. The coding agent should use this as a tool-selection and troubleshooting map.

Legend:

```text
P0 = recommended for MVP / high priority
P1 = useful after MVP
P2 = research branch / optional
```

Decision contract:

| Stage | Default tool | Enabled when | Required artifact evidence | Failure mode |
|---|---|---|---|---|
| Gemini semantic plan | `GoogleAIClient` via `contour_svg.llm_gateway` | every production/research run | `debug/semantic_plan.json`, limiter status in `final.meta.json` | fail before provider call if shared limiter is unavailable |
| Primary/occluder detection | GroundingDINO HF route first | every run | bbox metadata, mask prompts, status event | fail if required primary object cannot be detected |
| Segmentation | SAM2/SAM2.1 | every run | `mask_object_visible.png`, `mask_occluder.png`, `mask_object_unknown.png`, overlay | fail if required SAM2 checkpoint/API is unavailable |
| Line graph | OpenCV LSD/Hough + M-LSD, then DeepLSD/HAWP after preflight | every run | `line_candidates.jsonl`, `line_groups.json`, `line_groups_overlay.png` | fail if no silhouette/structure groups are produced |
| Gemini line editor | `GoogleAIClient` structured JSON, chunks of <=25 groups | every run before primitive final rendering | `gemini_line_group_actions.json` | fail if IDs are missing/unknown or JSON invalid after retry policy |
| Primitive renderer | custom stroke SVG renderer | every final candidate | `arch_primitives.json`, primitive-rendered `candidates/*.svg` | fail if no final-eligible primitive candidate passes gates |
| ControlNet / raster proposals | Diffusers SD1.5 Lineart/MLSD | proposal branch only | `debug/neural_branch_raw/*` or candidate metadata | may fail the run if configured mandatory, but cannot directly win `final.svg` |

---

## 1. Kaggle / runtime

| Priority | Tool | Role | Link | Notes |
|---|---|---|---|---|
| P0 | Kaggle Notebooks | development/runtime | https://www.kaggle.com/docs/notebooks | Check GPU session limits and working storage. |
| P0 | Kaggle Models | store model weights | https://www.kaggle.com/docs/models | Use for pre-downloaded weights if notebook internet is disabled. |
| P0 | Kaggle Datasets | store assets, weights, benchmark images | https://www.kaggle.com/docs/datasets | Alternative to Kaggle Models for static files. |

---

## 2. Gemini / multimodal semantic judge

| Priority | Tool | Role | Link | Notes |
|---|---|---|---|---|
| P0 | Gemini API quickstart | API setup | https://ai.google.dev/gemini-api/docs/quickstart | Use `google-genai`, API key via env/secret. |
| P0 | Google GenAI SDK | Python SDK | https://ai.google.dev/gemini-api/docs/libraries | Official SDK family. |
| P0 | Structured outputs | JSON schemas for judge loop | https://ai.google.dev/gemini-api/docs/structured-output | Use for primary object report, line critique, ranking. |
| P0 | Python GenAI SDK docs | SDK reference | https://googleapis.github.io/python-genai/ | Useful for implementation details. |

Recommended Gemini roles:

```text
1. primary object selector;
2. occluder detector/critic;
3. architecture feature summarizer;
4. line-group editor;
5. postcardness judge;
6. repair-plan generator.
```

Do not use Gemini as the module that emits final SVG coordinates.

---

## 3. Open-vocabulary object detection / primary object selection

| Priority | Model/tool | Link | Best use | Kaggle caveats |
|---|---|---|---|---|
| P0 | GroundingDINO | https://github.com/IDEA-Research/GroundingDINO | text-prompt detection: building, tree, fence, etc. | install can be fragile; cache weights. |
| P0 | Grounding DINO Transformers docs | https://huggingface.co/docs/transformers/en/model_doc/grounding-dino | HF-native implementation route | easier integration if available. |
| P0 | Grounded Segment Anything | https://github.com/IDEA-Research/Grounded-Segment-Anything | GroundingDINO + SAM recipe | good implementation reference. |
| P0 | Grounded SAM 2 | https://github.com/IDEA-Research/Grounded-SAM-2 | GroundingDINO/Florence + SAM2 pipeline | high-value examples for segmentation. |
| P1 | Florence-2 | https://huggingface.co/docs/transformers/en/model_doc/florence2 | prompt-based caption/detect/segment | alternate experiment; not a surrogate final path. |
| P1 | YOLO-World | https://github.com/AILab-CVC/YOLO-World | real-time open-vocabulary detection | license/check dependencies. |
| P1 | Ultralytics YOLO-World docs | https://docs.ultralytics.com/models/yolo-world/ | practical YOLO-World use | easiest if using ultralytics. |
| P1 | OWL-ViT | https://huggingface.co/docs/transformers/en/model_doc/owlvit | zero-shot detection | often weaker but simple. |
| P1 | OWLv2 | https://huggingface.co/docs/transformers/en/model_doc/owlv2 | zero-shot detection | alternate experiment only. |

Suggested prompt set:

```text
building . house . facade . roof . window . door . balcony . column . arch . tree . foliage . fence . sky . road . pavement . wire . pole . person . car
```

---

## 4. Segmentation / masks

| Priority | Model/tool | Link | Best use | Notes |
|---|---|---|---|---|
| P0 | SAM2 GitHub | https://github.com/facebookresearch/sam2 | promptable segmentation refinement | Use boxes from detector. |
| P0 | SAM2 Transformers docs | https://huggingface.co/docs/transformers/en/model_doc/sam2 | HF integration | Can use automatic mask generation. |
| P1 | SAM1 GitHub | https://github.com/facebookresearch/segment-anything | compatibility experiment | Older but widely used; not a substitute for required SAM2 runs. |
| P0 | MMSegmentation | https://github.com/open-mmlab/mmsegmentation | semantic segmentation training/inference | Useful for facade parser experiments. |
| P0 | MMSeg docs | https://mmsegmentation.readthedocs.io/en/main/overview.html | model zoo/tutorials | Use for custom facade datasets. |
| P1 | ADE20K / Mask2Former models | https://huggingface.co/models?search=mask2former%20ade20k | generic semantic labels | building/sky/tree/wall/window-like labels. |
| P1 | Cityscapes SegFormer models | https://huggingface.co/models?search=segformer%20cityscapes | building/vegetation/road/fence/pole | Helps occluder/background separation. |

Mask tasks:

```text
- object_visible;
- tree/foliage;
- fence;
- sky;
- ground/pavement;
- wires/poles if detectable;
- unknown object under occlusion.
```

---

## 5. Architecture/facade-specific datasets and models

| Priority | Resource | Link | Use | Notes |
|---|---|---|---|---|
| P0 | CMP Facade Database | https://cmp.felk.cvut.cz/~tylecr1/facade/ | facade element classes | 606 rectified facades; manually annotated. |
| P0 | CMP Facade HF dataset | https://huggingface.co/datasets/Xpitfire/cmp_facade | quick loading via HF | includes 12 facade classes. |
| P0 | SegFormer CMP Facade | https://huggingface.co/Xpitfire/segformer-finetuned-segments-cmp-facade | ready facade segmentation baseline | classes: facade, molding, cornice, pillar, window, door, sill, blind, balcony, shop, deco, background. |
| P1 | 3DOM Semantic Facade | https://github.com/3DOM-FBK/3DOM-Semantic-Facade | historic facade semantic segmentation dataset | useful for older buildings. |
| P1 | DeepFacade GitHub | https://github.com/liuhantang/DeepFacade | facade parsing reference | old PyTorch; research/reference. |
| P1 | DeepFacade paper | https://www.ijcai.org/proceedings/2017/0320.pdf | symmetry-aware facade parsing ideas | good for grammar/completion. |
| P1 | RTFP | https://github.com/wbw520/RTFP | facade parsing with transformers and line integration | promising for line-aware parsing. |
| P2 | SAAF paper | https://arxiv.org/abs/2506.09071 | multimodal semantic guidance for facade walls/windows | check code availability. |
| P2 | Irregular Facades dataset | https://www.mdpi.com/2075-5309/14/9/2602 | irregular facade segmentation research | dataset/licensing must be checked. |
| P2 | ZAHA | https://github.com/oloocki/zaha | large-scale point cloud facade segmentation | mostly 3D; conceptual for facade classes. |
| P2 | TUM-Facade | https://github.com/oloocki/tum-facade | facade point cloud labels | 3D; optional. |
| P1 | FacMagNet paper | https://eprints.whiterose.ac.uk/id/eprint/173692/1/paper_facade_semantic_segmentation__revision_clean_copy.pdf | residential facade segmentation, urban furniture/flora | useful for occlusion-heavy facades. |

Recommended architecture branch order:

```text
1. Try SegFormer CMP Facade on cropped/rectified facade if possible.
2. Use ADE/Cityscapes segmentation as weak labels for building/tree/fence/road/sky.
3. Use line graph + architecture grammar as main source of final SVG.
4. Treat DeepFacade/RTFP/SAAF as research branches; do not block MVP on them.
```

---

## 6. Line detection / wireframe parsing

| Priority | Tool/model | Link | Role | Notes |
|---|---|---|---|---|
| P0 | OpenCV HoughLinesP | https://learnopencv.com/hough-transform-with-opencv-c-python/ | baseline line detection | quick, tunable. |
| P0 | OpenCV Hough tutorial mirror | https://vovkos.github.io/doxyrest-showcase/opencv/sphinxdoc/page_tutorial_py_houghlines.html | reference | includes cv2.HoughLines/HoughLinesP. |
| P0 | M-LSD | https://github.com/navervision/mlsd | light real-time line segment detector | good for architecture straight lines. |
| P0 | DeepLSD | https://github.com/cvg/DeepLSD | robust line segment detection/refinement | high priority for line graph. |
| P0 | HAWP | https://github.com/cherubicXN/hawp | wireframe parsing: lines + junctions | good for building structure. |
| P1 | ScaleLSD | https://ant-research.github.io/scalelsd/ | high-coverage line segment detection | research option; may produce too many lines. |

Suggested ensemble:

```text
Fast pass: OpenCV LSD/Hough + Canny.
Main pass: DeepLSD + M-LSD.
Structure pass: HAWP if install/runtime ok.
Then merge/suppress candidates in line graph.
```

---

## 7. Perspective / geometry utilities

| Priority | Tool | Link | Role |
|---|---|---|---|
| P0 | Shapely | https://shapely.readthedocs.io/ | geometry operations, intersection/overlap, buffers. |
| P0 | NetworkX | https://networkx.org/documentation/stable/tutorial.html | line/junction graph. |
| P0 | RDP | https://rdp.readthedocs.io/ | polyline simplification. |
| P0 | scikit-image | https://scikit-image.org/ | skeletonization, morphology, contours. |
| P1 | Depth Anything V2 | https://github.com/DepthAnything/Depth-Anything-V2 | optional depth cue for object planes. |
| P1 | Depth Anything V2 docs | https://huggingface.co/docs/transformers/en/model_doc/depth_anything_v2 | HF integration. |
| P1 | MiDaS | https://github.com/isl-org/MiDaS | alternate monocular depth experiment. |

Depth is optional; useful for separating foreground occluders from background/building and for crop/plane hints, not for final geometry by itself.

---

## 8. Inpainting / occluder proposal generation

| Priority | Tool/model | Link | Role | Caveat |
|---|---|---|---|---|
| P1 | LaMa | https://github.com/advimman/lama | remove occluders to generate line proposals | proposal only, not truth. |
| P1 | LaMa project page | https://advimman.github.io/lama-project/ | paper/examples | good for large masks/periodic structures. |
| P1 | Diffusers inpainting | https://huggingface.co/docs/diffusers/en/using-diffusers/inpaint | text-guided inpaint proposals | high hallucination risk. |
| P2 | OpenCV inpainting | https://pyimagesearch.com/2020/05/18/image-inpainting-with-opencv-and-python/ | simple baseline | not semantic; small gaps only. |

Use inpainting only to propose line continuations. Final acceptance requires geometry constraints.

---

## 9. Diffusion / neural line-art

| Priority | Model/tool | Link | Role | Notes |
|---|---|---|---|---|
| P0 | Diffusers ControlNet guide | https://huggingface.co/docs/diffusers/en/using-diffusers/controlnet | implementation docs | main diffusion library route. |
| P0 | ControlNet paper | https://arxiv.org/abs/2302.05543 | conceptual reference | explains conditional controls. |
| P0 | SD1.5 ControlNet Lineart | https://huggingface.co/lllyasviel/control_v11p_sd15_lineart | line-art generation candidate | start here. |
| P0 | SD1.5 ControlNet MLSD | https://huggingface.co/lllyasviel/control_v11p_sd15_mlsd | architecture straight-line candidate | useful for buildings. |
| P0 | SD1.5 ControlNet Canny | https://huggingface.co/lllyasviel/control_v11p_sd15_canny | edge-constrained candidate | risk: too much detail. |
| P1 | SD1.5 ControlNet SoftEdge | https://huggingface.co/lllyasviel/control_v11p_sd15_softedge | smoother edge condition | may help non-architecture. |
| P1 | SD1.5 ControlNet Inpaint | https://huggingface.co/lllyasviel/control_v11p_sd15_inpaint | masked redraw branch | not final truth. |
| P1 | SDXL ControlNet Canny | https://huggingface.co/diffusers/controlnet-canny-sdxl-1.0 | higher quality if GPU allows | heavier. |
| P2 | Architecture SD workflows | https://cadman.dk/stable-diffusion-controlnet-in-architecture/ | workflow ideas | external tutorial; not API reference. |

Recommended MVP: SD1.5 + Lineart/MLSD only after geometry branch is working.

---

## 10. Raster-to-vector / SVG writing

| Priority | Tool | Link | Role | Caveat |
|---|---|---|---|---|
| P0 | svgwrite | https://svgwrite.readthedocs.io/ | programmatic SVG writing | maintenance mode but simple. |
| P0 | svgwrite PyPI | https://pypi.org/project/svgwrite/ | install info | pure Python. |
| P0 | SVGO | https://svgo.dev/ | optimize final SVG | run after validation. |
| P0 | SVGO GitHub | https://github.com/svg/svgo | CLI/docs | Node dependency. |
| P1 | VTracer | https://github.com/visioncortex/vtracer | vectorize raster line-art branch | do not use as final universal path. |
| P1 | Potrace | https://potrace.sourceforge.net/ | bitmap to vector | binary input; useful for masks/logos. |
| P1 | Potrace man page | https://potrace.sourceforge.net/potrace.1.html | CLI options | reference. |
| P1 | svgpathtools | https://github.com/mathandy/svgpathtools | path manipulation | optional. |

Preferred final route:

```text
primitive graph → svgwrite/custom SVG writer → SVGO
```

Proposal-only route:

```text
raster candidate → skeleton/path extraction → primitive cleanup → SVG
```

Avoid:

```text
raw neural/canny raster → vector tracer → accepted final candidate
```

---

## 11. Local aesthetic / preference scoring

| Priority | Tool/model | Link | Role | Caveat |
|---|---|---|---|---|
| P1 | LAION aesthetic predictor | https://github.com/LAION-AI/aesthetic-predictor | local aesthetic weak signal | not specialized for SVG line-art. |
| P1 | Improved aesthetic predictor | https://github.com/christophschuhmann/improved-aesthetic-predictor | CLIP+MLP aesthetic | weak auxiliary score. |
| P1 | PickScore | https://huggingface.co/yuvalkirstain/PickScore_v1 | image/prompt preference ranking | use on rendered preview PNG. |
| P1 | Pick-a-Pic paper | https://huggingface.co/papers/2305.01569 | background | text-to-image preference. |
| P1 | ImageReward | https://github.com/zai-org/ImageReward | human preference reward | heavyweight; preview PNG. |
| P2 | HPSv2 | https://github.com/tgxs002/HPSv2 | human preference score | optional. |
| P2 | HPSv3 | https://huggingface.co/MizzenAI/HPSv3 | newer human preference model | may be too heavy for Kaggle. |

Use local scores as auxiliary. Gemini judge remains primary for postcardness because the task is highly specific.

---

## 12. Candidate optimization

| Priority | Tool | Link | Use |
|---|---|---|---|
| P1 | Optuna | https://optuna.readthedocs.io/ | optimize thresholds and weights. |
| P1 | Optuna ask-and-tell | https://optuna.readthedocs.io/en/stable/tutorial/20_recipes/009_ask_and_tell.html | expensive black-box evaluations. |
| P1 | Optuna multi-objective | https://optuna.readthedocs.io/en/stable/tutorial/20_recipes/002_multi_objective.html | maximize postcardness/recognizability, minimize noise. |

Suggested optimization variables:

```text
max_strokes, min_len_px, duplicate_distance_px, occluder_overlap_threshold,
line_density_penalty, completion_gap_px, crop_margin, stroke_weight_primary,
stroke_weight_secondary, Gemini shortlist size.
```

---

## 13. Recommended first implementation stack

```yaml
MVP_stack:
  object_detection:
    - GroundingDINO or Florence-2
  segmentation:
    - SAM2
    - ADE/Cityscapes segmentation optional
  line_detection:
    - OpenCV Hough/LSD
    - M-LSD
    - DeepLSD
  geometry:
    - Shapely
    - NetworkX
    - scikit-image
    - RDP
  rendering:
    - custom SVG writer or svgwrite
    - SVGO
  semantic_judge:
    - Gemini structured outputs
  neural_branch:
    - ControlNet Lineart/MLSD after M1-M5
```
