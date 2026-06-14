# Contour SVG Generator v0.3 — Implementation Audit and Recovery Plan

Дата: 2026-06-14  
Репозиторий/ветка: `onedayonemasterpiece/events-bot-new`, `feature/contour-svg-generator-v03`  
Цель документа: зафиксировать, что уже реализовано, почему текущий результат остаётся некачественным, и какой следующий инженерный пайплайн должен быть собран без резких смен направления.

---

## 1. Executive summary

Текущая реализация уже содержит большую часть **технических компонентов**: primary object detection, occluder masks, facade parsing, edge/MLSD/DeepLSD/HAWP guides, line groups, Gemini line review, completion proposals, primitive renderer, ControlNet candidates, ranking and SVG export.

Проблема не в отсутствии отдельных моделей. Проблема в отсутствии **центрального слоя слияния доказательств**:

```text
сейчас:
many evidence layers → many candidates → ranking

нужно:
many evidence layers → one fused architectural scene graph → controlled candidate families
```

Иными словами, алгоритм научился производить много полезных промежуточных файлов, но ещё не научился собирать из них **одно осмысленное представление здания** до этапа рисования. Поэтому финал всё ещё выглядит как смесь полезной геометрии, edge tracing и паразитных линий.

Главный следующий шаг — не “выбросить AI ветку” и не “заменить всё диффузией”, а добавить промежуточные графы:

```text
EvidenceInventory
→ BuildingShell
→ PlaneGraph
→ FeatureGraph
→ OccluderAwareCompletionGraph
→ Neural/Gemini Fusion Editor
→ PrimitiveScene
→ Candidate Families
```

Финальный SVG должен рендериться из `PrimitiveScene`, а не напрямую из `edge_map`, `mlsd_guide`, ControlNet PNG или сырого набора линий.

---

## 2. What is already implemented

### 2.1 Package-level modules exist

В репозитории уже есть модульный пакет `contour_svg` со следующими значимыми файлами:

- `pipeline.py`
- `config.py`
- `contracts.py`
- `detection_grounding.py`
- `segment_sam2.py`
- `masks.py`
- `facade_parser.py`
- `guides.py`
- `line_graph.py`
- `completion.py`
- `primitive_renderer.py`
- `diffusion_controlnet.py`
- `gemini_line_editor.py`
- `gemini_judge.py`
- `scoring.py`
- `ranking.py`
- `vectorize.py`
- `svg_export.py`

Это подтверждает, что агент реализовал не один скрипт, а набор стадий. Но эти стадии пока соединены скорее “последовательно”, чем “семантически”.

### 2.2 Current implemented pipeline

Handoff описывает текущий pipeline так:

```text
input image
→ Gemini semantic plan through shared GoogleAIClient limiter
→ GroundingDINO / Florence-2 / YOLO-World primary and occluder box evidence
→ SAM2 primary and occluder masks
→ multi-state masks: object_visible / occluder / background / object_unknown
→ CMP Facade SegFormer element parsing
→ Canny / Hough / LSD / M-LSD / DeepLSD / HAWP guide extraction
→ line candidates and line groups
→ Gemini line-group editor
→ conservative completion proposals
→ primitive renderer candidate families
→ ControlNet img2img proposal rasters
→ optional IP-Adapter style-reference B3/B4 proposal rasters
→ SVG hard gates, CV scoring, Gemini contact-sheet ranking
→ final.svg / preview.png / metadata / alternatives
```

Это хорошая техническая база, но она всё ещё не гарантирует, что “здание понято как объект”.

### 2.3 Useful artifacts already produced

По human audit, уже есть полезные evidence layers:

- `edge_map.png` — точная, но слишком плотная карта контуров.
- `mlsd_guide.png` — сильная крупная геометрия.
- `deeplsd_lines_overlay.png` — хорошие прямые конструктивные линии.
- `elements_overlay.png` — facade parser неплохо находит окна/двери/балконы/пилястры.
- `wall_plane.png` — полезная маска плоскости фасада.
- `masks_multistate_overlay.png` — useful state map: object / occluder / background / unknown.
- `mask_occluder.png` — важен для политики восстановления через листву.
- B1/B2 ControlNet rasters — полезны как style/repair evidence, но не как финальный SVG.

Эти файлы нужно не “выбирать один лучший”, а научиться **собирать** в сцену.

---

## 3. Key diagnosis

### 3.1 The pipeline draws before it understands

Сейчас система извлекает линии, группирует их и сразу делает candidate families. Но отсутствует стадия:

```text
What is the building shell?
What are the main facade planes?
Where is the roof?
Which detected elements belong to which plane?
Which lines are structural, repeated, ornamental, or noise?
```

Без этого финальный рендер неизбежно получает:

- пучки параллельных линий на крыше;
- шум от окна/балкона/переплётов;
- часть контуров листвы и occluder boundaries;
- неверное усиление edge-based деталей;
- отсутствие уверенного “общего силуэта” до деталей.

### 3.2 Evidence is concatenated, not fused

`edge_map`, `MLSD`, `DeepLSD`, `facade_elements`, mask contours and ControlNet rasters сейчас попадают в общий поток как источники кандидатов. Это не одно и то же, что слияние.

Нужно различать:

```text
edge_map       → high-resolution evidence; confirms local details, but should not be traced directly
MLSD/DeepLSD   → structural evidence; candidates for shell/planes
facade parser  → semantic evidence; windows/doors/pilasters/cornices
masks          → ownership evidence; object vs occluder vs background
neural rasters → style/repair evidence; not final geometry
Gemini         → critic/editor; not coordinate generator
```

### 3.3 There is a primitive renderer, but no primitive scene

`primitive_renderer.py` уже создаёт primitive candidates from groups/elements/completion and family budgets. Но эти primitives — всё ещё локальные линии и bbox-элементы. Они не являются полноценной сценой:

- no BuildingShell;
- no PlaneGraph;
- no FeatureGraph;
- no row/column layout;
- no feature-to-plane ownership;
- no explicit facade coordinate frame;
- no shell confidence gate before details.

### 3.4 Neural branch is mispositioned

ControlNet/IP-Adapter не должен быть “финальным художником”, особенно если он меняет стиль или уводит здание. Но его не надо выбрасывать.

Правильная роль:

```text
Neural branch = proposal / repair / style evidence
Final = editable SVG primitives from PrimitiveScene
```

То есть neural output должен помогать находить missing lines, стиль упрощения и repair hints, но финальный SVG должен оставаться управляемым и проверяемым.

---

## 4. Implemented vs missing

| Area | Status | Comment |
|---|---:|---|
| Primary object detection | Implemented | Есть Grounding/open-vocab + semantic plan. |
| SAM2 primary/occluder masks | Implemented | Полезно, но object/occluder mask ещё не используется достаточно строго как graph constraint. |
| Multi-state masks | Implemented | Есть `object_visible`, `occluder`, `background`, `object_unknown`. |
| Facade parser | Implemented | CMP Facade SegFormer extracts windows/doors/balconies/pilasters/cornices/wall plane. |
| Edge map | Implemented | Очень полезна как local evidence, но опасна как direct vector source. |
| MLSD / DeepLSD / HAWP lines | Implemented/partially gated | Отличные structural candidates, но не превращены в canonical shell/planes. |
| Line graph | Implemented shallowly | Есть line candidates/groups, but semantic guess is heuristic role/angle/y-threshold, not scene-aware. |
| Gemini line review | Implemented | Полезен, но работает на shallow line groups, не на fused scene graph. |
| Completion proposals | Implemented shallowly | Bridges gaps and proposes repeated windows, but lacks plane/row/occluder-aware constraints. |
| Primitive renderer | Implemented shallowly | Generates families, but from local primitives, not from scene graph. |
| SVG hard gates | Implemented | Проверяет SVG/primitive renderer/global structure, but not architectural identity. |
| Ranking | Implemented | Uses Gemini/CV/path-count/simplicity; missing shell/plane/recognizability gates. |
| Neural branch | Implemented as proposal rasters | No accepted neural-final; should stay as evidence/fusion input. |
| EvidenceInventory | Missing | Needed to merge sources with confidence and roles. |
| BuildingShell | Missing | The most important gap. |
| PlaneGraph | Missing | No real facade plane / vanishing point ownership. |
| FeatureGraph | Missing | No windows/cornices/pilasters attached to planes and rows. |
| PrimitiveScene | Missing | Need final canonical scene before candidate family rendering. |
| Fusion Editor | Missing/incomplete | Gemini currently edits line groups, not evidence→scene fusion. |

---

## 5. Why the current result has parasite lines

### 5.1 Raw guide/vector candidates still enter the pool

The pipeline creates primitive candidates, but it also vectorizes raster proposal outputs and guide rasters. Specifically, it adds:

- `raster_to_candidate` for ControlNet rasters;
- `guide_lines_to_candidate(guides.lines)`;
- `raster_to_candidate` for `mlsd_guide` and `edge_map`.

Even if these are proposal-only, they influence contact sheets, debugging, ranking perception and agent behavior. More importantly, this coding pattern suggests the wrong mental model: “vectorize all useful-looking evidence”, whereas the desired model is “use evidence to form a scene”.

### 5.2 Line grouping is too local

The line graph groups by semantic guess, pseudo-VP group, angle bucket and rho bucket. This can reduce duplicates, but it does not know:

- which face/plane a line belongs to;
- whether it is a wall edge, cornice, window edge, roof edge, or accidental texture;
- whether two short segments should form one canonical line;
- whether a line crosses facade planes;
- whether a line only exists because of leaf boundaries.

### 5.3 The facade parser is useful but not structurally integrated

`elements_overlay` looks promising because it provides semantic anchors. But current use mostly maps each detected element bbox into a rect/arch primitive. There is no step:

```text
window candidates → align to facade plane → group into rows/columns → snap to perspective → choose representative windows → render simplified grammar
```

Without that, bbox elements can create noisy or inconsistent windows.

### 5.4 Completion is too primitive

Current completion bridges line gaps and creates repeated window placeholders. It does not yet implement robust interpolation rules:

- only bridge across occluder, not arbitrary allowed/unknown region;
- require plane ownership;
- require row/column support;
- forbid extrapolation beyond plane/shell;
- distinguish “hidden by foliage” from “not visible because absent”.

### 5.5 Scoring rewards SVG cleanliness more than “same building”

Current ranking/scoring has useful gates, but recognizability and shell correctness are still under-specified. A candidate can have a reasonable path count and global_structure_score yet still not be a good drawing of the target building.

---

## 6. User hypothesis: stitching useful evidence layers

The user’s hypothesis is directionally correct: many intermediate files are useful and need to be stitched together. But the stitching should not be free-form image generation.

### Bad version

```text
send all debug images to neural model → ask it to draw final image → vectorize raster
```

Risks:

- style drift;
- hallucinated elements;
- non-editable geometry;
- raster-to-vector noise;
- weak reproducibility.

### Good version

```text
all evidence layers
→ EvidenceInventory
→ Gemini/vision model reviews evidence contact sheet
→ model returns structured keep/drop/merge/snap/complete instructions
→ Python executes geometry operations
→ PrimitiveScene
→ controlled SVG render
```

Gemini can be the semantic fusion editor, but it should not directly author SVG coordinates.

---

## 7. Proposed revised pipeline

### 7.1 Stage A — EvidenceInventory

Inputs:

- original image;
- normalized image;
- primary mask;
- occluder mask;
- object_visible/background/object_unknown masks;
- masked_background / object silhouette;
- `edge_map`;
- `mlsd_guide`;
- `deeplsd_lines`;
- `hawp_lines`;
- facade parser elements;
- wall_plane;
- neural rasters B1/B2/B3/B4;
- semantic plan.

Output:

```text
debug/evidence_inventory.json
debug/evidence_contact_sheet.png
```

Data model:

```python
@dataclass
class EvidenceItem:
    id: str
    kind: Literal[
        "mask_region",
        "line_segment",
        "polyline",
        "facade_element",
        "neural_line",
        "wall_plane",
        "occluder_region"
    ]
    source: str
    geometry: ...
    bbox_xyxy: tuple[float, float, float, float]
    confidence: float
    object_visible_overlap: float
    occluder_overlap: float
    background_overlap: float
    role_hint: str | None
    semantic_hint: str | None
    debug_image: str | None
```

Rules:

- no final drawing happens here;
- every source is normalized into a comparable evidence object;
- evidence keeps provenance.

### 7.2 Stage B — BuildingShell

Purpose: construct the main object before details.

Inputs:

- primary mask contour;
- masked_background silhouette;
- top MLSD/DeepLSD long structural lines;
- wall_plane;
- original image;
- occluder mask.

Output:

```text
debug/building_shell.json
debug/building_shell_overlay.png
```

Data model:

```python
@dataclass
class BuildingShell:
    hull_polygon: list[Point]
    visible_hull_segments: list[ShellSegment]
    completed_hull_segments: list[ShellSegment]
    roof_segments: list[ShellSegment]
    base_segments: list[ShellSegment]
    facade_corner_segments: list[ShellSegment]
    bbox_xyxy: Box
    shell_confidence: float
    occlusion_zones: list[Box | Polygon]
```

Hard gate:

```text
Do not render windows/details until BuildingShell passes:
- shell_confidence >= threshold
- has roofline
- has base or lower mass
- has at least one facade corner / dominant vertical
- occluder leakage below threshold
```

For current sample, the first visible target is not the final rich drawing. The first milestone should be:

```text
a clean 20–35 stroke SVG containing:
- outer silhouette
- roofline
- major facade corner
- lower base
- approximate side volume
- no windows
- no trees/fence
```

### 7.3 Stage C — PlaneGraph

Purpose: assign geometry to facade/roof planes.

Inputs:

- BuildingShell;
- MLSD/DeepLSD/HAWP structural lines;
- wall_plane;
- facade parser elements;
- vanishing direction clustering.

Output:

```text
debug/plane_graph.json
debug/plane_graph_overlay.png
```

Data model:

```python
@dataclass
class Plane:
    id: str
    plane_type: Literal["front_facade", "side_facade", "roof", "base", "unknown"]
    polygon: list[Point]
    vanishing_group_x: str | None
    vanishing_group_y: str | None
    confidence: float
    evidence_ids: list[str]
```

Implementation notes:

- cluster lines into vertical, left_vp, right_vp, horizontal/shallow roof;
- use RANSAC or robust clustering for vanishing directions;
- attach lines and facade elements to planes;
- reject lines that cross incompatible planes.

### 7.4 Stage D — FeatureGraph

Purpose: turn semantic elements into architectural features.

Inputs:

- PlaneGraph;
- facade parser elements;
- edge_map details;
- line candidates;
- architecture element library;
- Gemini review if enabled.

Output:

```text
debug/feature_graph.json
debug/feature_graph_overlay.png
```

Data model:

```python
@dataclass
class FeatureNode:
    id: str
    feature_type: Literal[
        "window_rect",
        "window_arch",
        "door",
        "cornice_band",
        "pilaster",
        "balcony",
        "stairs",
        "roof_pediment",
        "decorative_accent"
    ]
    plane_id: str
    bbox_xyxy: Box
    simplified_geometry: list[PrimitiveSpec]
    evidence_ids: list[str]
    row_id: str | None
    col_id: str | None
    confidence: float
    visible_status: Literal["visible", "partial", "occluded", "inferred"]
```

Rules:

- windows/doors must belong to a plane;
- features should snap to local plane axes;
- repeated features form rows/columns;
- do not render every detected feature; choose representative features by family budget;
- facade parser bboxes are hints, not final drawing.

### 7.5 Stage E — OccluderAwareCompletionGraph

Purpose: conservative interpolation only.

Inputs:

- BuildingShell;
- PlaneGraph;
- FeatureGraph;
- occluder mask;
- object_unknown;
- original image.

Output:

```text
debug/completion_graph.json
debug/completion_graph_overlay.png
```

Allowed completions:

1. Continue a shell/roof/cornice line through an occluder if confirmed on both sides.
2. Continue a facade band if it is plane-consistent and bounded by shell.
3. Infer a missing repeated window only inside a confirmed row/column and plane.
4. Complete partial feature geometry if its visible bbox and nearby repeated features support it.

Forbidden completions:

1. inventing decorative elements inside occluder without repeated pattern;
2. extrapolating outside BuildingShell/PlaneGraph;
3. drawing contours of trees as architecture;
4. adding windows on a wall plane with no row/column evidence.

### 7.6 Stage F — Neural/Gemini Fusion Editor

Purpose: use AI as editor, not uncontrolled final renderer.

Inputs:

- original image;
- evidence contact sheet;
- building shell overlay;
- plane graph overlay;
- feature graph overlay;
- neural rasters;
- current primitive preview.

Output:

```text
debug/fusion_editor_actions.json
debug/fusion_acceptance_report.json
```

Actions schema:

```json
{
  "shell_actions": [
    {
      "target_id": "shell.roof.03",
      "action": "keep|drop|merge|extend|shorten|snap",
      "reason": "...",
      "confidence": 0.0
    }
  ],
  "feature_actions": [
    {
      "target_id": "feature.window.row_02.col_03",
      "action": "keep|drop|simplify|snap_to_row|lower_priority",
      "reason": "...",
      "confidence": 0.0
    }
  ],
  "completion_actions": [
    {
      "target_id": "completion.gap_05",
      "action": "accept|reject|shorten",
      "reason": "...",
      "confidence": 0.0
    }
  ],
  "global_assessment": {
    "same_building_score": 0.0,
    "postcardness_score": 0.0,
    "noise_risk": 0.0,
    "main_failure": "..."
  }
}
```

Important:

- Gemini may choose/criticize/merge/simplify;
- Python owns geometry;
- diffusion may propose missing strokes, but final acceptance is graph-constrained.

### 7.7 Stage G — PrimitiveScene

Purpose: one canonical editable scene.

Output:

```text
debug/primitive_scene.json
debug/primitive_scene_overlay.png
```

Data model:

```python
@dataclass
class PrimitiveScene:
    source_image_size: tuple[int, int]
    shell: BuildingShell
    planes: list[Plane]
    features: list[FeatureNode]
    completions: list[CompletionNode]
    style_policy: StylePolicy
```

Candidate families should render the same scene differently:

```text
POSTCARD_MINIMAL      = fewer features, stronger shell
BALANCED_ARCHITECTURAL = shell + key features + selected rhythm
CONSERVATIVE_COMPLETION = balanced + accepted occluder completions
FEATURE_EMPHASIS_OPENINGS = more windows/arches
DETAILED_EDITORIAL    = richer, but still scene-constrained
```

They should not be separate raw-source candidates.

---

## 8. Architectural elements library — enforceable subset

The existing architecture library should become code rules.

### 8.1 Core primitives

#### Building shell

```yaml
outer_silhouette:
  priority: 100
  source_priority:
    - primary_mask_hull
    - masked_background
    - long_mlsd_deeplsd
    - gemini_shell_review
  render:
    stroke_width: main
    linecap: round
    linejoin: round
  max_segments:
    minimal: 12
    balanced: 18
    detailed: 28
```

#### Roofline

```yaml
roofline:
  priority: 95
  constraints:
    - above upper 55% of building bbox
    - aligns with left/right VP or shallow roof angle
    - attached to shell or roof plane
  render:
    stroke_width: main
  max_segments:
    minimal: 6
    balanced: 12
    detailed: 20
```

#### Facade corner / vertical mass

```yaml
facade_corner:
  priority: 90
  constraints:
    - near plane boundary
    - vertical or perspective-consistent
    - long enough to define building mass
  max_segments:
    minimal: 2
    balanced: 6
    detailed: 10
```

#### Cornice band

```yaml
cornice_band:
  priority: 75
  constraints:
    - horizontal or VP-aligned
    - belongs to facade plane
    - should be merged if parallel duplicates exist
  render:
    stroke_width: secondary
  max_parallel_lines_per_band:
    minimal: 1
    balanced: 2
    detailed: 3
```

#### Window rectangle

```yaml
window_rect:
  priority: 55
  constraints:
    - must be inside facade plane
    - aspect ratio plausible
    - belongs to row/column or high semantic confidence
  render:
    geometry: rectangle_outline
    optional_inner_line: true
  budget:
    minimal: representative_only
    balanced: key_rows
    detailed: more_rows
```

#### Arched window

```yaml
window_arch:
  priority: 70
  constraints:
    - tall aspect or parser/Gemini says arch
    - should preserve if visually distinctive
  render:
    geometry: two_verticals_plus_semicircular_or_elliptic_arc
```

#### Door / entrance

```yaml
door:
  priority: 65
  constraints:
    - lower facade
    - vertical opening
    - not fence/occluder
```

#### Balcony

```yaml
balcony:
  priority: 45
  constraints:
    - attached to facade plane
    - not foreground fence
  render:
    simplified:
      - platform_line
      - outer_rect_or_rail
      - limited vertical posts
```

#### Stairs/base

```yaml
stairs_base:
  priority: 60
  constraints:
    - lower part of shell
    - near entrance/base plane
  render:
    max_lines:
      minimal: 3
      balanced: 6
      detailed: 10
```

### 8.2 Line budget policy

Initial target for this building:

```yaml
postcard_minimal:
  total_strokes: 45-75
  shell_roof_base: 55%
  openings: 25%
  decorative: 10%
  completion: 10%

balanced:
  total_strokes: 80-130
  shell_roof_base: 40%
  openings: 35%
  cornices_pilasters: 15%
  completion: 10%

detailed_editorial:
  total_strokes: 130-210
  shell_roof_base: 30%
  openings: 40%
  cornices_pilasters: 20%
  completion: 10%
```

---

## 9. Revised ranking/gating

### 9.1 Hard gates before aesthetic ranking

Reject/fail-low if:

```text
- no BuildingShell
- shell_confidence below threshold
- no roofline
- no main vertical/facade corner
- too much line length overlaps occluder/background
- top candidate is raster-derived
- primitive scene not available
- details exist before shell passes
- edge_map-derived candidate beats scene-derived candidate
```

### 9.2 New scores

Add to `Candidate.parameters`:

```python
shell_score
plane_score
feature_consistency_score
occluder_leakage_score
same_building_score
thumbnail_readability_score
postcardness_score
source_fidelity_score
hallucination_risk
```

### 9.3 Ranking should prefer scene-derived candidates

Suggested formula:

```text
final_score =
  0.22 * same_building_score
+ 0.18 * shell_score
+ 0.14 * plane_score
+ 0.12 * feature_consistency_score
+ 0.14 * postcardness_score
+ 0.08 * thumbnail_readability_score
+ 0.06 * line_economy_score
+ 0.06 * svg_validity_score
- 0.22 * occluder_leakage
- 0.18 * hallucination_risk
- 0.12 * raw_edge_trace_risk
```

Do not hard-code the ideal path count around one number such as 92. Use family-specific path ranges instead.

---

## 10. Concrete next implementation plan

### Sprint 1 — freeze models, build shell

Do not change model stack. Do not touch ControlNet. Do not tune Canny.

Implement:

```text
EvidenceInventory
BuildingShell
shell_score
shell_overlay
```

Use existing generated artifacts from `audit_1527` as regression fixture.

Acceptance:

- `building_shell_overlay.png` contains a clean large building mass.
- 20–35 strokes can describe the building mass without windows.
- no foliage/fence lines in shell.
- roof and base are visible.
- if shell fails, pipeline stops before primitive details.

### Sprint 2 — PlaneGraph

Implement:

```text
vanishing direction clustering
plane polygons
line-to-plane assignment
feature-to-plane assignment
```

Acceptance:

- top roof lines belong to roof plane;
- front/side facades are distinguishable enough;
- wall_plane is integrated, not just overlaid.

### Sprint 3 — FeatureGraph

Implement:

```text
facade element snapping
window row/column grouping
cornice band merging
representative opening selection
architecture grammar rendering
```

Acceptance:

- windows are aligned and simplified;
- no every-pixel window tracing;
- arched windows survive if distinctive.

### Sprint 4 — Occluder-aware completion

Implement:

```text
completion only through occluder zones
two-sided line gap continuation
row/column inferred openings
completion debug report
```

Acceptance:

- leafy occlusion can be crossed by roof/cornice lines;
- no invented decorations outside confirmed planes;
- repeated windows only if row evidence exists.

### Sprint 5 — Neural/Gemini Fusion Editor

Implement:

```text
fusion contact sheet
shell/plane/feature/completion prompts
structured actions
graph-constrained repair
```

Acceptance:

- B1/B2/B3/B4 never directly become final SVG;
- neural candidates can propose missing lines;
- Gemini can say “drop tree contour”, “merge roof duplicates”, “keep main arch”, etc.;
- Python executes geometry.

---

## 11. Rules for the code agent

1. **Do not remove AI branch because one diffusion output looked wrong.** Change its role to evidence/repair/style proposal.
2. **Do not vectorize raw edge_map into final.** Edge map is confirmation evidence.
3. **Do not add more models before BuildingShell exists.** The bottleneck is fusion, not detection.
4. **Do not tune thresholds blindly.** Every tuning change must improve a named artifact: shell, plane, feature, completion, ranking.
5. **Do not rank details before shell.** The shell is the primary acceptance gate.
6. **Do not make candidate families from unrelated raw sources.** Candidate families should be renderings of the same PrimitiveScene.
7. **Do not let Gemini output coordinates.** Let Gemini output structured decisions; geometry stays local.
8. **Every stage must write a visual overlay and JSON.**
9. **Every run must answer:** did the pipeline understand the whole building before drawing details?
10. **Use `audit_1527` as fixed regression input** until there is one visibly good result.

---

## 12. Required debug artifact contract

```text
debug/evidence_inventory.json
debug/evidence_contact_sheet.png

debug/building_shell.json
debug/building_shell_overlay.png
debug/building_shell_score.json

debug/plane_graph.json
debug/plane_graph_overlay.png

debug/feature_graph.json
debug/feature_graph_overlay.png

debug/completion_graph.json
debug/completion_graph_overlay.png

debug/fusion_editor_actions.json
debug/fusion_acceptance_report.json

debug/primitive_scene.json
debug/primitive_scene_overlay.png

candidates/postcard_minimal.svg
candidates/balanced_architectural.svg
candidates/conservative_completion.svg
candidates/feature_emphasis_openings.svg

leaderboard.csv
ranking_report.json
final.svg
preview.png
```

---

## 13. Suggested files/classes to add

```text
contour_svg/evidence_inventory.py
contour_svg/building_shell.py
contour_svg/plane_graph.py
contour_svg/feature_graph.py
contour_svg/completion_graph.py
contour_svg/fusion_editor.py
contour_svg/primitive_scene.py
contour_svg/architecture_grammar.py
contour_svg/scene_ranking.py
```

Suggested contracts:

```python
EvidenceItem
EvidenceInventory
BuildingShell
ShellSegment
VanishingDirection
Plane
PlaneGraph
FeatureNode
FeatureGraph
CompletionNode
CompletionGraph
PrimitiveScene
SceneCandidate
FusionAction
FusionReport
```

---

## 14. Minimal refactor of current pipeline

Replace this section:

```text
guides
→ line_graph
→ gemini_line_editor
→ completion
→ primitive_renderer
→ ControlNet rasters
→ raster/guide vectorization
→ ranking
```

with:

```text
guides + masks + facade_elements + neural_rasters
→ EvidenceInventory
→ BuildingShell
→ PlaneGraph
→ FeatureGraph
→ CompletionGraph
→ Fusion Editor
→ PrimitiveScene
→ candidate family rendering
→ ranking
```

Important: ControlNet can still run, but its output should feed `EvidenceInventory` or `FusionEditor`, not `final_pool`.

---

## 15. Acceptance criteria for the current sample

A result is acceptable only if all are true:

1. At 128×128 preview, the object reads as the same large neoclassical building.
2. Roofline and main facade corner are clear.
3. Trees/fence/sky/road are not drawn as building geometry.
4. Left occlusion does not become a black/white foliage contour.
5. There is a plausible continued shell behind foliage, but no invented external extension.
6. Windows/arches are simplified, not copied as noisy edge maps.
7. SVG has transparent background and one stroke color.
8. SVG is editable: primitives are lines/polyline/path arcs, no embedded raster.
9. Top candidate is scene-derived, not raster-derived.
10. Final has a report explaining which evidence layers contributed to shell, planes, features and completions.

---

## 16. Repository references for the agent

Main handoff:

- `docs/features/countur_svg_generator/research/external_agent_handoff_20260614.md`

Current code entry points:

- `contour_svg/pipeline.py`
- `contour_svg/guides.py`
- `contour_svg/line_graph.py`
- `contour_svg/facade_parser.py`
- `contour_svg/completion.py`
- `contour_svg/primitive_renderer.py`
- `contour_svg/vectorize.py`
- `contour_svg/scoring.py`
- `contour_svg/ranking.py`
- `contour_svg/gemini_line_editor.py`

Useful current sample artifacts:

- `docs/features/countur_svg_generator/samples/generated/audit_1527/preview.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/contact_sheet.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/edge_map.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/mlsd_guide.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/deeplsd_lines_overlay.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/elements_overlay.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/wall_plane.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/masks_multistate_overlay.png`
- `docs/features/countur_svg_generator/samples/generated/audit_1527/mask_occluder.png`

---

## 17. Short instruction to the next agent

Do not redesign the whole system again.  
Do not discard existing artifacts.  
Do not jump to another model.

Implement the missing middle:

```text
EvidenceInventory → BuildingShell → PlaneGraph → FeatureGraph → PrimitiveScene
```

Use the current debug files as evidence. The goal of the next milestone is not the final postcard. The goal is:

```text
a clean, correct building shell with no parasite objects
```

Only after that, add windows, cornices, arches and completion.
