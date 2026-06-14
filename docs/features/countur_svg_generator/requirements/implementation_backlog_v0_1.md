# Implementation Backlog v0.1

**Goal:** give a coding agent a practical implementation plan for the semantic/vector Contour SVG Generator.

---

## 1. Immediate priority

The next milestone is not another Canny threshold experiment. The next milestone is:

```text
multi-state masks + line graph + semantic pruning + primitive SVG renderer
```

This is the minimum architecture that can remove parasite lines from foliage/fence/texture.

---

## 2. Phase 0 — Project scaffolding

### Tasks

- Create Python package structure.
- Add config loader.
- Add CLI entrypoint.
- Add debug output directory conventions.
- Add baseline notebook for one image.

### Deliverables

```text
src/contour_svg/cli.py
configs/default.yaml
notebooks/01_debug_single_image.ipynb
README.md
```

### Definition of Done

```bash
python -m contour_svg.cli generate --image demo.png --out /tmp/out
```

creates at least:

```text
/tmp/out/debug/input_normalized.png
/tmp/out/debug/config_resolved.yaml
```

---

## 3. Phase 1 — Primary object and masks

### Tasks

1. Implement `primary_object.py`:
   - detector interface;
   - Gemini primary report interface;
   - primary score.

2. Implement `masks.py`:
   - SAM2 mask refinement;
   - occluder masks;
   - background masks;
   - object_unknown estimation;
   - mask visualization.

3. Add mask QA:
   - connected component cleanup;
   - fill holes only for object mask, not for occluders;
   - tree/fence leakage flags.

### Target APIs

```python
def select_primary_object(image, hint, config) -> SemanticScene: ...
def build_mask_bundle(image, scene, config) -> MaskBundle: ...
def visualize_masks(image, mask_bundle, out_path): ...
```

### Tests

```text
- tree is not included in object_visible;
- foreground fence is separate occluder;
- sky and pavement do not become allowed line region;
- object_unknown exists only near occluder/object boundary.
```

---

## 4. Phase 2 — Line candidate graph

### Tasks

1. Implement detector wrappers:
   - `opencv_lsd_detector`;
   - `hough_detector`;
   - `mlsd_detector`;
   - `deeplsd_detector`;
   - optional `hawp_detector`.

2. Normalize outputs to `LineCandidate`.

3. Compute features:
   - length;
   - angle;
   - mask overlaps;
   - detector agreement;
   - local density;
   - background/occluder leakage;
   - simple texture likelihood.

4. Cluster angles and approximate VP groups.

5. Merge duplicates.

### APIs

```python
def detect_line_candidates(image, masks, config) -> list[LineCandidate]: ...
def group_line_candidates(lines, masks, config) -> list[LineGroup]: ...
def score_line_group(group, masks, scene, config) -> float: ...
```

### Debug outputs

```text
debug/all_line_candidates.png
debug/line_candidates_by_source.png
debug/line_groups_overlay.png
debug/line_candidates.jsonl
debug/line_groups.json
```

### DoD

- Foliage-like short/jagged groups receive low scores.
- Long roof/cornice/facade lines receive high scores.
- Duplicate roof/cornice groups are suppressed/merged.

---

## 5. Phase 3 — Architecture grammar v1

### Tasks

1. Implement primitive classes:
   - silhouette;
   - roof;
   - eaves/cornice;
   - facade corner;
   - rectangular window;
   - arched window;
   - door/entrance;
   - pilaster;
   - stairs/base;
   - balcony simplified.

2. Implement proposal mapping:
   - line groups → primitive candidates;
   - semantic masks → primitive candidates;
   - Gemini features → primitive priorities.

3. Implement line budgets.

4. Implement renderer.

### APIs

```python
class ArchitectureGrammar:
    def propose(self, line_groups, masks, semantic_scene) -> list[PrimitiveCandidate]: ...
    def select_for_family(self, primitives, family_config) -> list[PrimitiveCandidate]: ...

class SVGRenderer:
    def render(self, primitives, layout, style) -> str: ...
```

### DoD

- `postcard_minimal` candidate has ≤ 70 strokes.
- `balanced_architectural` candidate has ≤ 170 strokes.
- No foreground fence/foliage primitives.
- Arched windows render as arcs, not jagged contours.

---

## 6. Phase 4 — Conservative completion

### Tasks

1. Implement line gap continuation.
2. Implement cornice/band continuation.
3. Implement vertical pilaster continuation.
4. Implement repeated window placeholder only when strong evidence exists.
5. Mark all completed lines in debug.

### APIs

```python
def propose_completions(line_groups, primitives, masks, config) -> list[PrimitiveCandidate]: ...
def validate_completion(candidate, masks, scene, config) -> CompletionDecision: ...
```

### Hard rules

```text
- no completion outside object hull;
- no new decorative elements in hidden areas;
- no roof extension beyond observed/projected roof envelope;
- no window completion unless repeated pattern exists.
```

### DoD

- Tree-covered gaps in cornices can be bridged.
- Tree outline is not drawn.
- Completed elements are visually conservative.

---

## 7. Phase 5 — Candidate generation

### Tasks

- Implement candidate families.
- Generate layout/crop variants.
- Generate stroke budget variants.
- Generate feature emphasis variants.
- Save every candidate with metadata.

### Candidate metadata

```json
{
  "candidate_id": "candidate_014",
  "family": "balanced_architectural",
  "stroke_count": 132,
  "crop_variant": "heroic_08",
  "completion_mode": "conservative",
  "primitive_counts": {"roof": 8, "window": 32, "cornice": 12},
  "source_branches": ["geometry", "architecture_grammar"]
}
```

### DoD

Generate 32+ candidates for one image in batch mode.

---

## 8. Phase 6 — Ranking

### Tasks

1. Implement hard gates.
2. Implement local metrics.
3. Implement contact sheet generation.
4. Implement Gemini candidate ranking.
5. Implement pairwise tournament for near-tie final-eligible candidates.
6. Save `ranking_report.json`.

### Local metrics APIs

```python
def compute_svg_metrics(svg_path, masks, scene, config) -> dict: ...
def render_thumbnail(svg_path, size=128) -> Image: ...
def compute_postcard_proxy(metrics) -> float: ...
```

### Hard gate examples

```python
if metrics["embedded_raster_count"] > 0: reject("embedded raster")
if metrics["foreground_color_count"] > 1: reject("not two-color")
if metrics["occluder_overlap_ratio"] > cfg.reject_occluder_overlap_gt: reject("occluder leakage")
if metrics["short_fragment_ratio"] > cfg.max_short_fragment_ratio: reject("too fragmented")
```

### DoD

- Saves final.svg and top_alternatives.
- Ranking report includes reasons and rejected candidate problems.
- Top candidate is not dominated by foliage/fence.

---

## 9. Phase 7 — Neural line-art branch

### Tasks

1. Implement ControlNet Lineart/MLSD branch.
2. Use object/occluder masks before generating guide image.
3. Generate multiple seeds with strict prompts.
4. Convert neural raster output into path candidates, not final SVG.
5. Run same cleanup/ranking pipeline.

### DoD

- Neural branch adds useful alternatives.
- Neural outputs with hallucinated or noisy geometry are rejected.
- Geometry branch remains available as a proposal source and must pass through line graph, semantic pruning and primitive rendering before final export.

---

## 10. Phase 8 — Architecture-specific model experiments

### Tasks

- Try `Xpitfire/segformer-finetuned-segments-cmp-facade` on cropped facade.
- Compare with ADE20K/Cityscapes models.
- Add weak evidence from facade parser to primitive scoring.
- Research DeepFacade/RTFP/SAAF integration.

### DoD

- At least one facade parser provides useful window/cornice/pillar priors.
- Pipeline does not fail if facade parser is wrong/unavailable.

---

## 11. Recommended class skeletons

```python
@dataclass
class MaskBundle:
    object_visible: np.ndarray
    occluders: dict[str, np.ndarray]
    background: dict[str, np.ndarray]
    object_unknown: np.ndarray
    allowed_line_region: np.ndarray

@dataclass
class LineCandidate:
    id: str
    points: list[tuple[float, float]]
    source: list[str]
    length: float
    angle_deg: float
    object_visible_overlap: float
    occluder_overlap: float
    background_overlap: float
    vp_group: str | None
    score: float

@dataclass
class LineGroup:
    id: str
    member_ids: list[str]
    geometry: dict
    semantic_label: str | None
    score: float
    decision: str
    reason: str

@dataclass
class PrimitiveCandidate:
    id: str
    primitive_class: str
    geometry: dict
    confidence: float
    importance: str
    evidence: list[str]
    source_group_ids: list[str]
    completion_status: str

@dataclass
class CandidateSVG:
    id: str
    family: str
    svg_path: str
    preview_path: str
    metadata: dict
    metrics: dict
```

---

## 12. Development benchmark set

Create a small local benchmark:

```text
benchmark/
  001_building_clear_front/
  002_building_tree_occlusion/
  003_building_fence_foreground/
  004_building_low_contrast/
  005_building_perspective_3_4/
  006_non_architecture_vehicle/
  007_non_architecture_statue/
```

For each:

```text
input.jpg
manual_primary_mask.png optional
manual_occluder_notes.md
expected_features.json
human_notes.md
```

Expected features example:

```json
{
  "must_keep": ["roofline", "facade_corner", "arched_window", "main_entrance"],
  "must_drop": ["tree", "foreground_fence", "sky", "pavement"],
  "allowed_completion": ["cornice through foliage", "roofline through small gap"],
  "forbidden_completion": ["new windows under tree", "tree-shaped wall contour"]
}
```

---

## 13. Common failure modes and debugging

### Failure: too many parasite lines

Check:

```text
- occluder masks too weak;
- line budget too high;
- duplicate suppression too weak;
- short fragment threshold too low;
- no local density penalty;
- vectorizing raw raster instead of primitive rendering.
```

### Failure: building not recognizable

Check:

```text
- silhouette dropped;
- roof/facade corner missing;
- key windows/entrance not preserved;
- crop too loose/tight;
- line budget too low;
- completion too conservative around major occlusion.
```

### Failure: beautiful but inaccurate

Check:

```text
- neural branch hallucinated;
- Gemini over-weighted aesthetics;
- hallucination_risk not vetoed;
- completion rules too permissive.
```

### Failure: SVG too heavy

Check:

```text
- too many path elements;
- unsimplified polylines;
- no SVGO pass;
- VTracer/Potrace generated filled paths instead of stroke primitives.
```

---

## 14. Final Definition of Done for v1

A v1 implementation is acceptable if:

```text
- runs in Kaggle notebook on a single image;
- produces transparent final.svg + top_alternatives SVGs;
- separates tree/fence from building in debug masks;
- creates candidate line graph and line group overlay;
- uses architecture primitive renderer for at least 8 classes;
- generates ≥24 candidates;
- ranks candidates with hard gates + Gemini judge;
- stores ranking_report.json;
- no final result is raw raster tracing without semantic pruning;
- the result is visually closer to postcard line art than to Canny edge map.
```
