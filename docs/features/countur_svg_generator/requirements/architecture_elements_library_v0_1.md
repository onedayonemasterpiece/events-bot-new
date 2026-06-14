# Architecture Elements Library v0.1

**Цель документа:** дать кодовому агенту библиотеку архитектурных элементов и графических правил, чтобы финальный SVG строился не как bitmap tracing, а как осмысленный набор line-art primitives.

Эта библиотека не должна быть единственным источником истины. Она работает как renderer grammar и semantic prior: если элемент найден CV/ML/Gemini-ветками, библиотека задаёт, как его упростить, сколько линий дать, как восстановить скрытые части и когда отказаться.

---

## 1. Общие правила графики

### 1.1. Общий стиль

```yaml
style:
  background: transparent
  foreground: one_color
  fill: none
  stroke_linecap: round
  stroke_linejoin: round
  stroke_hierarchy: [primary, secondary, tertiary]
  geometry: clean, mostly straight, rounded only where semantically justified
  no_texture: true
  no_hatching: true
  no_photo_noise: true
```

### 1.2. Иерархия линий

```text
Primary strokes:
  - внешний силуэт;
  - главные линии крыши;
  - главные вертикальные ребра объёма;
  - основание/постамент;
  - один-два ключевых архитектурных акцента.

Secondary strokes:
  - окна;
  - двери;
  - карнизы;
  - пилястры/колонны;
  - балконная плита;
  - лестница.

Tertiary strokes:
  - редкие декоративные панели;
  - 2–6 балясин/перил, если это часть здания;
  - мелкие оконные разделители, если нужны для узнаваемости.
```

### 1.3. Line budget по стилям

```yaml
postcard_minimal:
  total_strokes: 35-70
  details: only iconic features

balanced_architectural:
  total_strokes: 90-170
  details: key facade rhythm

detailed_editorial:
  total_strokes: 160-280
  details: acceptable for poster, risky for postcard
```

---

## 2. Общие архитектурные evidence types

Каждый primitive должен иметь evidence:

```text
visible_line_evidence    — линия найдена на видимой части объекта;
semantic_mask_evidence   — facade parser / segmentation подтверждает класс;
repetition_evidence      — элемент повторяется в ряду/сетке;
perspective_evidence     — линия согласована с VP group;
gemini_evidence          — Gemini назвал элемент значимым;
completion_evidence      — элемент восстановлен по строгому правилу.
```

Confidence levels:

```text
0.90–1.00: видимый крупный элемент, подтверждён ≥2 источниками;
0.70–0.89: видимый элемент с одним сильным источником;
0.50–0.69: слабый элемент, рисовать только в detailed/balanced;
0.35–0.49: completion proposal, рисовать только если повышает читаемость;
<0.35: не рисовать.
```

---

## 3. Библиотека элементов

## 3.1. Outer silhouette / внешний силуэт

**Назначение:** узнаваемость primary object при thumbnail size.

**Источники:** object_visible mask, object hull, line detectors, roof/facade semantic masks.

**Render:** длинные polyline/path segments, primary stroke.

**Budget:** 8–24 strokes.

**Keep:**

- roof outline;
- side wall outline;
- base outline;
- main protrusions: balcony block, entrance canopy, tower/dormer, если видимы.

**Drop:**

- jagged tree-shaped contours;
- foliage holes;
- fence boundary;
- random mask serration.

**Simplification:**

- Douglas-Peucker/RDP;
- angle snapping to VP groups;
- remove dents below 8–20 px depending on output size;
- preserve large concave/convex turns.

**Completion:** allowed only across occluder if silhouette is visible before and after occluder or strongly implied by roof/facade plane.

---

## 3.2. Roof ridge / конёк крыши

**Cues:** long top diagonal/horizontal line; separates roof from sky; strong VP alignment.

**Render:** 1 primary line/polyline. Optional second parallel line for roof thickness.

**Budget:** 1–4 strokes.

**Rules:**

```text
- Prefer one confident line over many close duplicates.
- If 3+ parallel roof candidates exist, merge/suppress to 1–2.
- Do not preserve small broken fragments caused by leaves.
```

**Completion:** can bridge small occluder gaps if line exists on both sides.

---

## 3.3. Roof slopes / скаты крыши

**Cues:** diagonal lines converging to VP; connected to ridge/eaves.

**Render:** primary/secondary lines. For mansard/hip roof use simplified trapezoids.

**Budget:** 3–12 strokes.

**Pattern examples:**

```svg
<path d="M 250 240 L 520 90 L 790 230" />
<path d="M 270 260 L 520 120 L 770 250" />
```

**Drop:** decorative roof texture, tile edges, noisy short lines.

---

## 3.4. Eaves / карниз крыши

**Cues:** long horizontal/diagonal bands below roof, often multiple parallel lines.

**Render:** 1–3 long secondary strokes. For postcard minimal use only 1.

**Budget:** 2–10 strokes.

**Duplicate suppression:**

```text
If many parallel lines within 3–12 px, keep:
- longest;
- line with best VP alignment;
- optionally second line if it creates clear cornice thickness.
```

---

## 3.5. Cornice / molding / belt course

**Cues:** horizontal facade bands between floors, under windows, near roof, base.

**Render:** simplified long strokes following facade perspective.

**Budget:**

```text
minimal: 0–4
balanced: 4–14
detailed: 10–28
```

**Rules:**

- keep bands that explain floor levels;
- merge dense double/triple bands;
- do not trace wall material boundaries or shadows;
- continuation through occluder allowed if facade plane and band position are stable.

---

## 3.6. Facade corner / главное ребро объёма

**Cues:** strong vertical line separating planes; often central in 3/4 view.

**Render:** primary vertical line, maybe doubled if pilaster/corner column.

**Budget:** 1–4 strokes.

**Importance:** always high. If this line missing, 3D silhouette collapses.

**Rules:**

- snap to vertical group;
- extend only between roof/eaves and base;
- do not let it run into sky/ground without reason.

---

## 3.7. Wall plane boundary / плоскость фасада

**Cues:** exterior wall edges, plane changes.

**Render:** primary/secondary lines.

**Budget:** 2–8 strokes.

**Rules:**

- preserve major left/right facade plane boundaries;
- avoid internal texture boundaries;
- use depth/VP cues if available.

---

## 3.8. Plinth / base / цоколь

**Cues:** lower wall zone, material/color change, horizontal base lines.

**Render:** 2–6 secondary strokes; can be simplified into long stepped bands.

**Budget:** 2–10 strokes.

**Rules:**

- keep if it stabilizes building on canvas;
- drop detailed brick/block grid unless necessary;
- for postcard minimal draw only top and bottom base lines.

---

## 3.9. Stairs / лестница

**Cues:** repeated horizontal lines near entrance, decreasing length by perspective.

**Render:** 3–7 short horizontal/perspective strokes.

**Budget:**

```text
minimal: 0–3
balanced: 3–7
detailed: 5–12
```

**Pattern:**

```svg
<path d="M 420 780 L 640 780" />
<path d="M 390 820 L 670 820" />
<path d="M 360 860 L 700 860" />
```

**Drop:** pavement joints and road texture.

---

## 3.10. Rectangular window / прямоугольное окно

**Cues:** repeated rectangles, semantic window mask, vertical/horizontal line group.

**Render options:**

```text
simple: outer rectangle only;
medium: outer rectangle + one inner vertical line;
detailed: outer rectangle + inner mullion + sill.
```

**Budget per window:** 2–5 strokes.

**Rules:**

- prefer representative windows over all windows;
- preserve rhythm: several evenly spaced simplified windows better than one over-detailed window;
- snap verticals and horizontals to facade plane;
- if windows too small in preview, draw simple mark/slot.

**Do not:** trace curtains, glass highlights, frame shadows.

---

## 3.11. Arched window / арочное окно

**Cues:** arch edge, central high window, neoclassical/historic facade.

**Render:** two verticals + arc. Optional inner arc.

**Budget per arched window:** 3–6 strokes.

**SVG template:**

```svg
<path d="M {x0} {y1} L {x0} {yc} A {r} {r} 0 0 1 {x1} {yc} L {x1} {y1}" />
```

**Importance:** high for historic/neoclassical buildings; often key recognizability feature.

**Completion:** allowed only if visible arch is partially covered and geometry is obvious, or if repeated arch row exists.

---

## 3.12. Door / entrance

**Cues:** ground-level rectangular/arched opening, stairs, canopy/pediment.

**Render:** rectangle/arch plus 1–3 surrounding strokes.

**Budget:** 3–10 strokes.

**Rules:**

- preserve main entrance even in minimal variants;
- do not trace door texture/panels unless iconic;
- if entrance hidden by tree, draw only confirmed frame/steps, not invented door.

---

## 3.13. Pediment / фронтон

**Cues:** triangular element above entrance/window/roof line.

**Render:** triangle or trapezoid, secondary/primary depending on scale.

**Budget:** 2–5 strokes.

**Pattern:**

```svg
<path d="M 420 360 L 470 320 L 520 360" />
<path d="M 405 370 L 535 370" />
```

**Rules:**

- highly valuable for neoclassical style;
- suppress duplicate internal triangles unless visible and useful;
- never infer a pediment only from generic style if no visual/semantic cue.

---

## 3.14. Pilaster / column / vertical order

**Cues:** strong vertical facade strips, lighter wall zones, repeated supports.

**Render:** pair of vertical lines + optional cap/base.

**Budget per element:** 2–5 strokes.

**Rules:**

- preserve major pilasters because they explain architecture;
- avoid drawing every shading boundary;
- snap to vertical group;
- if pilaster intersects occluder, continuation allowed between confirmed top/bottom.

---

## 3.15. Balcony / loggia

**Cues:** projecting slab, railing, repeated vertical balusters, shadow under slab.

**Render:** slab outline + a limited number of railing strokes.

**Budget:**

```text
minimal: slab only, 2–4 strokes
balanced: slab + 3–8 balusters
detailed: slab + 8–16 railing details
```

**Rules:**

- distinguish building balcony from foreground fence;
- keep balcony if attached to facade and part of object;
- foreground fence almost always drop.

---

## 3.16. Railing / перила

**Use only for:** balcony/porch/entrance railings attached to primary object.

**Suppress:** street fence in front of building, unless user explicitly wants it.

**Render:** 3–8 evenly spaced verticals, not every real bar.

**Rules:**

```text
if railing is foreground fence:
  reject
if railing is balcony and supports composition:
  simplify to rhythm marks
```

---

## 3.17. Dormer / attic window / mansard detail

**Cues:** small roof windows, protruding boxes on roof.

**Render:** tiny simplified rectangle/trapezoid.

**Budget:** 1–4 strokes per dormer, max 2–4 dormers.

**Rules:**

- draw only if visible and improves roof recognizability;
- drop if thumbnail becomes noisy.

---

## 3.18. Decorative panel / framed wall panel

**Cues:** rectangular panels on walls, common in neoclassical facades.

**Render:** simple U-shape or rectangle missing one side if occluded.

**Budget:** 0–8 strokes total.

**Rules:**

- optional; use in balanced/detailed;
- not important enough to preserve if line budget is tight.

---

## 3.19. Arch / arcade

**Cues:** repeated arches, open loggia/arcade.

**Render:** repeated simplified arch paths.

**Budget:** 3–8 strokes per major arch; for repeated row, use 2–5 arches max.

**Rules:**

- repeated arches can become strong postcard rhythm;
- suppress inner texture and shadows.

---

## 3.20. Chimney / tower / cupola

**Cues:** vertical protrusion above roof.

**Render:** small silhouette block.

**Budget:** 1–4 strokes.

**Rules:**

- keep if iconic;
- do not invent if hidden.

---

## 3.21. Ground contact / floating base

For postcards, the building can either stand on a simplified base or float as an icon.

**Options:**

```yaml
grounded:
  draw: base + 2-5 stair/ground strokes
floating:
  draw: no road/pavement, maybe short base line only
```

Do not trace pavement, cobblestones, road edges or grass texture.

---

## 4. Suppression library: what to not draw

## 4.1. Trees and foliage

**Symptoms:** jagged contour, many short irregular edges, green/leaf semantic mask, high local density, no VP alignment.

**Action:** drop.

**Exception:** if tree is intentional primary object in future non-architecture mode.

## 4.2. Foreground fence

**Symptoms:** repeated thin verticals in foreground; not attached to facade; often crosses lower building.

**Action:** drop. Do not confuse with balcony railing.

## 4.3. Wires

**Symptoms:** very long thin diagonal/curved lines across sky/facade.

**Action:** drop unless user requests environmental context.

## 4.4. Pavement / road / brick texture

**Action:** drop. Keep only stairs/base if semantically part of object.

## 4.5. Material texture / shadows

**Action:** drop. Use semantic wall boundaries, not shadows.

---

## 5. Primitive rendering templates

### 5.1. Straight line

```json
{
  "template": "line",
  "params": ["x1", "y1", "x2", "y2"],
  "svg": "<path d=\"M {x1} {y1} L {x2} {y2}\" />"
}
```

### 5.2. Polyline with rounded joins

```json
{
  "template": "polyline",
  "svg": "<path d=\"M x0 y0 L x1 y1 L x2 y2 ...\" />"
}
```

### 5.3. Rectangular window

```svg
<path d="M {x0} {y0} L {x1} {y0} L {x1} {y1} L {x0} {y1} L {x0} {y0}" />
```

### 5.4. Minimal window slot

```svg
<path d="M {x0} {y0} L {x1} {y0} L {x1} {y1}" />
```

### 5.5. Arched window

```svg
<path d="M {x0} {y1} L {x0} {yc} A {r} {r} 0 0 1 {x1} {yc} L {x1} {y1}" />
```

### 5.6. Pediment

```svg
<path d="M {x0} {y1} L {xc} {y0} L {x1} {y1}" />
<path d="M {x0b} {y1b} L {x1b} {y1b}" />
```

### 5.7. Cornice band

```svg
<path d="M {x0} {y0} L {x1} {y1}" />
<path d="M {x0} {y0+offset} L {x1} {y1+offset}" />
```

### 5.8. Stairs

```svg
<path d="M {x0_1} {y1} L {x1_1} {y1}" />
<path d="M {x0_2} {y2} L {x1_2} {y2}" />
<path d="M {x0_3} {y3} L {x1_3} {y3}" />
```

---

## 6. Element scoring matrix

| Element | Minimal | Balanced | Detailed | Completion allowed | Notes |
|---|---:|---:|---:|---|---|
| Outer silhouette | required | required | required | yes, strict | Top priority |
| Roof ridge/slopes | required | required | required | yes | Merge duplicates |
| Facade corner | required | required | required | yes | 3D readability |
| Cornice/eaves | 1–4 | 4–14 | 10–28 | yes | Avoid dense bands |
| Pilasters/columns | 0–4 | 4–12 | 8–20 | yes | Neoclassical feature |
| Windows | 2–8 | 8–24 | 20–50 | limited | Rhythm matters |
| Arched windows | keep if visible | keep | keep | limited | High recognizability |
| Door/entrance | 1 key | 1–2 | 1–3 | limited | Strong anchor |
| Pediment | keep if visible | keep | keep | limited | Style marker |
| Balcony | optional | keep if attached | keep | no/limited | Not foreground fence |
| Stairs/base | optional | keep | keep | yes | Do not trace pavement |
| Decorative panels | no | optional | optional | no | Low priority |
| Fence | reject | reject | reject | no | Unless primary object |
| Foliage | reject | reject | reject | no | Occluder only |

---

## 7. Neoclassical / historic building preset

Useful for the provided example building.

```yaml
preset_neoclassical_postcard:
  must_keep:
    - roof_ridge
    - roof_slopes
    - eaves
    - facade_corner
    - central_arched_window
    - vertical_pilasters
    - entrance_or_door
    - plinth_or_base
  should_keep:
    - side_windows_rhythm
    - balcony_if_visible
    - stairs_if_visible
    - pediment_if_visible
  should_drop:
    - trees
    - foreground_fence
    - sky
    - pavement
    - utility_wires
    - wall_texture
  stroke_budget:
    minimal: 55
    balanced: 125
    detailed: 210
  completion:
    allow_roof_and_cornice_gaps: true
    allow_pilaster_vertical_gaps: true
    allow_window_repetition: conservative
    forbid_new_decor: true
```

---

## 8. How to use this library in code

Suggested classes:

```python
class PrimitiveCandidate:
    id: str
    primitive_class: str
    geometry: dict
    evidence: list[str]
    confidence: float
    importance: str
    completion_status: str

class ArchitectureGrammar:
    def propose_primitives(line_groups, masks, semantic_scene): ...
    def score_primitive(primitive, style_profile): ...
    def render_primitive(primitive, svg_context): ...
    def suppress_non_architecture(line_groups, masks): ...
```

Renderer selection:

```python
if candidate_family == "postcard_minimal":
    keep_classes = ["silhouette", "roof", "facade_corner", "arched_window", "entrance", "key_windows"]
elif candidate_family == "balanced_architectural":
    keep_classes = ["silhouette", "roof", "cornice", "pilaster", "window", "arched_window", "entrance", "stairs", "balcony"]
elif candidate_family == "detailed_editorial":
    keep_classes = ["*"]
```

---

## 9. Debug requirements

For every primitive in final SVG save:

```json
{
  "svg_element_id": "svg_021",
  "primitive_id": "P_window_arch_01",
  "class": "arched_window",
  "source_line_groups": ["G_arch_01", "G_vertical_10"],
  "evidence": ["visible_line", "gemini_feature", "architecture_grammar"],
  "confidence": 0.82,
  "line_weight": "secondary",
  "completion_status": "visible_only"
}
```

For rejected lines save reason:

```json
{
  "group_id": "G_noise_017",
  "decision": "drop",
  "reason": "high foliage overlap and no VP alignment",
  "scores": {"occluder_overlap": 0.77, "vp_alignment": 0.12, "length": 18}
}
```
