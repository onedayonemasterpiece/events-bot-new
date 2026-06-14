# Contour SVG Generator v0.3

**Назначение:** генератор векторных двухцветных контурных изображений по фотографии primary object. Текущий приоритет — архитектура; ядро должно быть расширяемым на другие типы объектов.

**Целевой результат:** чистый SVG на прозрачном фоне: набор осмысленных контурных линий, а не трассировка всех контрастных границ фотографии. Preview PNG с цветным фоном допустим только как вспомогательный артефакт.

**Ключевое инженерное решение:** не строить финальную картинку по схеме `edge map → vtracer/potrace → SVG`. Такая схема переносит листву, забор, текстуру, шум, дублирующие линии и случайные границы. Нужен промежуточный слой: `candidate line graph → semantic pruning → primitive rendering → SVG`.

---

## 1. Контекст проблемы

Промежуточные результаты показывают, что алгоритм уже находит много реальных контрастных границ. Но для открыткообразного контурного изображения нужны не все физические границы, а только линии, которые зритель воспринимает как графический смысл объекта.

Типовые дефекты текущего результата:

1. **Паразитные линии от листвы и деревьев.** Они попадают в маску и становятся частью графики.
2. **Паразитные линии от забора, фонарей, брусчатки, текстур стены и оконных переплётов.** Они увеличивают плотность штрихов и создают «паутину».
3. **Дублирующие параллельные линии.** Детектор находит несколько почти одинаковых карнизов/скатов/кромок, renderer переносит все.
4. **Отсутствие различия между видимым объектом и закрытой областью.** Листва должна быть не «дырой» и не «контуром», а occluder-областью, где разрешена только осторожная интерполяция подтверждённых архитектурных линий.
5. **Недостаточная графическая иерархия.** В целевом стиле главные линии длинные, уверенные, геометричные; вторичные детали редуцированы.

---

## 2. Принципы итоговой архитектуры

### 2.1. Не пиксельный, а объектно-графовый пайплайн

Рабочий объект после ранних CV-стадий — не растровый edge map, а набор кандидатов:

```text
LineCandidate → LineGroup → ArchPrimitive/ObjectPrimitive → SVGStroke
```

Каждая линия должна иметь источник, геометрию, принадлежность к объекту/occluder, перспективную группу, семантический guess и оценку полезности.

### 2.2. Tri-state / multi-state masks вместо одной бинарной маски

Минимальные маски:

```text
object_visible    — видимая часть primary object;
occluder          — дерево, листва, забор, фонарь, авто, человек, провод, временный объект;
background        — небо, дорога, земля, соседние здания, окружение;
object_unknown    — зона, где объект вероятно продолжается под occluder, но не наблюдается напрямую.
```

Линии внутри `occluder` запрещены, если они не являются геометрически подтверждённым продолжением архитектурной линии.

### 2.3. Восстановление = интерполяция, не фантазия

Разрешается:

- продолжить прямую линию через occluder gap, если сегменты видны с двух сторон;
- продолжить карниз/фасадный band внутри той же плоскости;
- достроить повторяющийся элемент, если есть ритм и 2+ подтверждённых соседних элемента.

Запрещается:

- дорисовывать новые окна/декор в закрытой области без паттерна;
- продолжать крышу или фасад за пределы object hull;
- использовать форму дерева как форму здания;
- принимать inpainting-output как источник истины без геометрических constraints.

### 2.4. Много альтернатив, затем отбор

Один пайплайн не будет стабильно лучшим для всех фото. Нужно генерировать семейство кандидатов:

- strict visible;
- conservative completion;
- postcard minimal;
- balanced architectural;
- detailed editorial;
- heroic crop;
- floating icon;
- feature-emphasis variants;
- neural line-art variants.

Затем проводится автоматический отбор по hard gates, CV-метрикам и Gemini judge.

---

## 3. Целевая схема пайплайна

```text
Input photo
  ↓
Image normalization / crop candidates / EXIF fix
  ↓
Primary object selection
  ├─ Gemini semantic report
  ├─ GroundingDINO / Florence-2 / YOLO-World boxes
  └─ optional manual hint
  ↓
Multi-state segmentation
  ├─ object_visible mask
  ├─ occluder masks: tree, fence, people, cars, poles, wires
  ├─ background masks: sky, ground, road
  └─ object_unknown estimation
  ↓
Parallel proposal branches
  ├─ B1: geometric line graph branch
  │     Canny/LSD/M-LSD/DeepLSD/HAWP → line candidates → VP grouping
  ├─ B2: architecture semantic branch
  │     facade parser / ADE-Cityscapes / CMP facade → element candidates
  ├─ B3: neural line-art branch
  │     ControlNet/SD img2img → simplified line-art proposal → path candidates
  ├─ B4: conservative completion branch
  │     line gaps + facade rhythm + symmetry constraints
  └─ B5: Gemini-assisted pruning branch
        line-group overlay → keep/drop/merge/extend suggestions
  ↓
Candidate primitive assembly
  ├─ SVG primitive renderer
  ├─ stroke hierarchy
  ├─ crop/layout variants
  └─ line budget variants
  ↓
Candidate ranking
  ├─ hard gates
  ├─ CV/vector metrics
  ├─ Gemini postcard judge
  ├─ optional local aesthetic/preference models
  └─ diversity-aware shortlist
  ↓
Outputs
  ├─ final.svg
  ├─ preview.png
  ├─ final.meta.json
  ├─ top_alternatives/*.svg
  ├─ candidates/*.svg
  ├─ leaderboard.csv
  ├─ ranking_report.json
  └─ debug/*.png, *.json
```

---

## 4. Выходные артефакты

Обязательные:

```text
/output/{job_id}/final.svg
/output/{job_id}/preview.png
/output/{job_id}/final.meta.json
/output/{job_id}/leaderboard.csv
/output/{job_id}/ranking_report.json
/output/{job_id}/top_alternatives/rank_01_*.svg
/output/{job_id}/top_alternatives/rank_02_*.svg
/output/{job_id}/top_alternatives/rank_03_*.svg
/output/{job_id}/candidates/*.svg
/output/{job_id}/debug/masks.png
/output/{job_id}/debug/line_groups_overlay.png
/output/{job_id}/debug/dropped_lines_overlay.png
/output/{job_id}/debug/candidate_contact_sheet.png
```

Опциональные:

```text
/output/{job_id}/debug/semantic_scene.json
/output/{job_id}/debug/line_candidates.jsonl
/output/{job_id}/debug/line_groups.json
/output/{job_id}/debug/arch_primitives.json
/output/{job_id}/debug/gemini_primary_object_report.json
/output/{job_id}/debug/gemini_ranking_report.json
/output/{job_id}/debug/neural_branch_raw/*.png
/output/{job_id}/debug/vector_sanitized/*.svg
```

SVG должен быть:

- прозрачный фон;
- без embedded raster image;
- по умолчанию только strokes, без filled shapes;
- `stroke-linecap="round"`, `stroke-linejoin="round"`;
- единая палитра: foreground color + transparent;
- допустимы stroke weights 1–3 уровней, но все одного цвета;
- viewBox нормализован, например `0 0 1024 1024`.

---

## 5. Данные и JSON-схемы внутренних объектов

### 5.1. InputJob

```json
{
  "job_id": "demo_001",
  "image_path": "/kaggle/input/building/photo.png",
  "primary_object_hint": "main yellow neoclassical building",
  "priority_domain": "architecture",
  "output_size": 1024,
  "transparent_svg": true,
  "max_candidates": 48,
  "gemini_enabled": true,
  "manual_masks": null,
  "style_profile": "postcard_monoline"
}
```

### 5.2. SemanticScene

```json
{
  "primary_object": {
    "label": "building",
    "confidence": 0.91,
    "bbox_xyxy": [120, 20, 1130, 720],
    "description": "large neoclassical building with pitched roof, arched windows, pilasters and balcony"
  },
  "occluders": [
    {"label": "tree foliage", "bbox_xyxy": [0, 0, 500, 620], "severity": "high"},
    {"label": "fence", "bbox_xyxy": [0, 470, 1200, 710], "severity": "medium"}
  ],
  "salient_architectural_features": [
    "roof ridge",
    "front pediment",
    "arched central window",
    "vertical pilasters",
    "balcony on right facade",
    "stairs and base"
  ],
  "do_not_draw": ["trees", "fence", "sky", "pavement", "utility wires", "random texture"]
}
```

### 5.3. LineCandidate

```json
{
  "id": "L_0042",
  "geometry": {"type": "line", "points": [[302.5, 188.0], [742.0, 73.2]]},
  "length": 454.2,
  "angle_deg": -14.6,
  "source": ["DeepLSD", "Canny"],
  "vp_group": "roof_left_vp",
  "object_visible_overlap": 0.86,
  "occluder_overlap": 0.02,
  "background_overlap": 0.05,
  "junction_support": 0.71,
  "semantic_guess": "roof_edge",
  "raw_score": 0.78,
  "keep_probability": 0.66
}
```

### 5.4. LineGroup

```json
{
  "id": "G_roof_03",
  "members": ["L_0042", "L_0043", "L_0047"],
  "merged_geometry": {"type": "polyline", "points": [[302,188],[520,130],[742,73]]},
  "semantic_label": "main_roof_edge",
  "importance": "primary",
  "duplication_group": "roof_parallel_band_1",
  "decision": "candidate",
  "reason": "long aligned roof line with low occluder overlap"
}
```

### 5.5. ArchPrimitive / ObjectPrimitive

```json
{
  "id": "P_window_arch_01",
  "class": "arched_window",
  "source_groups": ["G_arch_left_01", "G_vertical_12", "G_vertical_13"],
  "confidence": 0.72,
  "bbox_xyxy": [392, 275, 478, 430],
  "render_template": "arched_window_simple",
  "style_weight": "secondary",
  "completion_status": "visible_only"
}
```

---

## 6. Детализация стадий

## 6.1. Stage A — Preprocess

Цели:

- привести изображение к RGB;
- исправить EXIF orientation;
- создать несколько crop candidates;
- сохранить исходные координаты для обратного маппинга;
- уменьшить слишком большие изображения для inference, но сохранить full-res для финальной геометрии.

Рекомендуемые операции:

```text
- resize max side to 1280/1536 for CV inference;
- keep original for SVG mapping;
- contrast-limited adaptive histogram equalization only for line detection branch;
- no aggressive sharpening before segmentation;
- optional sky/ground rough segmentation for crop scoring.
```

Crop candidates:

```text
C0: full image;
C1: primary object bbox + 12% margin;
C2: primary object bbox + 25% margin;
C3: heroic crop: bbox + 5–8% margin;
C4: square social crop centered at optical center;
C5: floating icon crop with extra negative space.
```

---

## 6.2. Stage B — Primary object selection

Primary object выбирается через ensemble:

1. Пользовательский hint, если он есть.
2. Gemini scene description и object priority.
3. Open-vocabulary detector: GroundingDINO / Florence-2 / YOLO-World / OWL-ViT.
4. Маска от SAM/SAM2 по выбранному box/points.
5. Scoring по площади, центральности, архитектурному приоритету и saliency.

В архитектурном режиме запросы detector-а:

```text
building . house . facade . historical building . neoclassical building . roof . window . door . balcony . tree . foliage . fence . sky . road . pavement . lamp post . person . car . wire . pole
```

Primary score:

```text
primary_score =
  1.4 * detector_confidence
+ 1.2 * gemini_priority_score
+ 0.8 * area_reasonableness
+ 0.7 * centrality_score
+ 0.5 * architecture_keywords_score
- 1.2 * occluder_or_background_likelihood
```

---

## 6.3. Stage C — Multi-state segmentation

Результат — не одна маска, а `MaskBundle`:

```text
mask_object_visible.png
mask_occluder_tree.png
mask_occluder_fence.png
mask_occluder_poles_wires.png
mask_background_sky.png
mask_background_ground.png
mask_object_unknown.png
mask_allowed_line_region.png
```

`mask_allowed_line_region` формируется так:

```text
allowed = object_visible
allowed += conservative_completion_regions
allowed -= occluders
allowed -= background
```

Для архитектуры важна отдельная карта occluder severity:

```text
0 = no occlusion
1 = soft occlusion / thin branches / wires
2 = medium occlusion / tree trunk / fence
3 = hard occlusion / dense foliage / vehicle / large object
```

Линии с overlap по severity 2–3 почти всегда отбрасываются, если нет подтверждения continuation rule.

---

## 6.4. Stage D — Geometric line graph branch

Источники кандидатов:

- Canny + contour extraction;
- OpenCV LSD / HoughLinesP;
- M-LSD;
- DeepLSD;
- HAWP / wireframe parsing;
- optional ControlNet preprocessors: MLSD, lineart, softedge.

Нормализация:

1. Перевести всё в общие координаты.
2. Удалить линии короче `min_len_px`.
3. Привязать линии к маскам: object/occluder/background overlap.
4. Сгруппировать по углам и vanishing directions.
5. Схлопнуть дублирующиеся близкие параллельные линии.
6. Построить junction graph.

Scoring line candidate:

```text
line_score =
  1.40 * vp_alignment
+ 1.25 * semantic_importance
+ 1.00 * length_score
+ 0.85 * object_visible_overlap
+ 0.75 * facade_rhythm_score
+ 0.65 * junction_support
+ 0.45 * detector_agreement
- 1.80 * occluder_overlap
- 1.40 * short_noise_penalty
- 1.30 * duplicate_parallel_penalty
- 1.20 * local_density_penalty
- 1.00 * texture_likelihood
- 0.80 * background_overlap
```

Группировка по vanishing points:

```text
vertical_group: angle near 90° ± tolerance
left_vp_group: dominant negative/positive diagonal cluster
right_vp_group: opposite diagonal cluster
horizontal_band_group: near-horizontal facade bands
other: candidate for rejection unless semantic source is strong
```

---

## 6.5. Stage E — Architecture semantic branch

Архитектурный branch работает с элементами, а не только с линиями.

Источники:

- generic semantic segmentation: ADE20K / Cityscapes models;
- facade-specific segmentation: CMP Facade / 3DOM Semantic Facade / DeepFacade / RTFP / SAAF-like approach;
- detector for windows/doors/balconies;
- Gemini feature list;
- repeated pattern mining.

Классы, которые нужно попытаться получить:

```text
facade/wall
roof
window
door
arched_window
balcony/loggia
pilaster/column
cornice/molding/sill
pediment
stairs/base/plinth
background
vegetation
fence
sky
ground
```

Даже если facade parser слабый, его можно использовать как weak signal. Например, если `window` heatmap подтверждает группу линий, повышать score; если линия попадает в `vegetation`, понижать.

---

## 6.6. Stage F — Conservative completion

Completion module не должен генерировать художественную фантазию. Он может только создавать новые линии с `completion_status=interpolated` или `completion_status=repeated_pattern`, если есть evidence.

### Правило прямого продолжения

Разрешить line extension через occluder, если:

```text
- есть два сегмента A и B по разные стороны occluder;
- |angle(A)-angle(B)| < angle_tol;
- оба принадлежат одному vp_group;
- расстояние между продолжениями < snap_tol;
- gap лежит внутри occluder или object_unknown;
- extension не выходит за object_hull;
- длина gap < max_gap_ratio * min(len(A), len(B)) или < max_gap_px;
- Gemini не пометил область как impossible/ambiguous.
```

### Правило фасадного band-а

Разрешить восстановить горизонтальный/перспективный карниз, если:

```text
- видны 2+ параллельных bands выше/ниже;
- band пересекает фасадную плоскость;
- occluder перекрывает локальный участок;
- band не пересекает реальные видимые окна/двери странным образом;
- итоговая линия помогает перспективной читаемости.
```

### Правило повторяющихся окон

Разрешить восстановить часть оконного ритма, если:

```text
- есть минимум 2 подтверждённых окна в одном ряду;
- расстояние/размеры образуют регулярный паттерн;
- скрытая позиция попадает в тот же facade plane;
- рисуется упрощённый placeholder, а не детальный декор;
- confidence ниже, чем у видимых элементов, и line weight secondary/tertiary.
```

---

## 6.7. Stage G — Neural line-art branch

Neural branch нужен не как единственный источник финального SVG, а как генератор альтернатив и источник hints.

Рекомендуемые варианты:

```text
N1: SD 1.5 + ControlNet Lineart, strong object mask, low detail prompt
N2: SD 1.5 + ControlNet MLSD, architecture prompt, strict negative prompt
N3: SDXL + ControlNet Canny/Depth, если Kaggle GPU позволяет
N4: inpaint occluders → lineart ControlNet → strict vector cleanup
N5: Gemini image-to-image/style judge only; не использовать Gemini для точных SVG coordinates
```

Neural branch output обязательно проходит через:

```text
raster line-art
→ binarization/skeletonization
→ path extraction
→ line/curve grouping
→ occluder/background veto
→ primitive renderer
→ SVG validation
```

Prompt baseline:

```text
clean minimal architectural contour line art, monoline vector postcard, transparent background, white strokes only, recognizable building silhouette, straight geometric perspective lines, simplified roof, arched windows, pilasters, elegant sparse lines, no texture, no shadows, no trees, no fence, no sky, no pavement, no sketch noise
```

Negative prompt:

```text
photo, realistic, texture, shading, gradient, color fill, messy sketch, scribbles, trees, foliage, fence, people, cars, street, pavement, bricks, tiny details, dense lines, crosshatching, watermark, text, logo, blur, random geometry, hallucinated windows
```

---

## 6.8. Stage H — Gemini-assisted pruning and ranking

Gemini используется в трёх ролях:

1. **Semantic analyst:** primary object, occluders, notable features.
2. **Line editor:** keep/drop/merge/extend по line-group overlay с ID.
3. **Postcard judge:** выбор лучшего из contact sheet.

Не использовать Gemini как основной генератор точных SVG coordinates. Координатную работу должен выполнять Python.

### Line-group overlay protocol

1. Сгенерировать PNG с линиями, где группы подписаны ID.
2. Приложить исходное фото + object mask + occluder mask + overlay.
3. Запросить JSON:

```json
{
  "groups": [
    {"group_id": "G_roof_03", "action": "keep", "reason": "main roof edge"},
    {"group_id": "G_tree_01", "action": "drop", "reason": "foliage contour"},
    {"group_id": "G_cornice_04", "action": "merge_with", "target_group_id": "G_cornice_05", "reason": "duplicate parallel cornice"}
  ]
}
```

### Candidate ranking protocol

- Давать Gemini contact sheet из 6–12 вариантов.
- Лучше pairwise/tournament, чем один абсолютный скор.
- Оценивать только после hard gates.
- Требовать reasons и repair suggestions.

---

## 6.9. Stage I — Primitive SVG renderer

Renderer должен создавать SVG из primitives, а не трассировать растровую картинку.

Базовые primitive types:

```text
line
polyline
quadratic_bezier
cubic_bezier
arc_path
rect_window
arched_window
roof_shape
cornice_band
pilaster
stairs
balcony_simplified
silhouette_segment
```

Пример SVG path для арочного окна:

```svg
<path d="M 100 220 L 100 160 A 40 40 0 0 1 180 160 L 180 220" />
<path d="M 110 220 L 110 165 A 30 30 0 0 1 170 165 L 170 220" />
```

Глобальные атрибуты:

```svg
<g fill="none" stroke="#FFFFFF" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke">
```

Stroke hierarchy:

```yaml
stroke_weights:
  primary: 4.0      # silhouette, roof, facade corner
  secondary: 2.6    # windows, doors, cornices
  tertiary: 1.6     # optional details, balcony rails
```

Для строгого двухцветного решения все strokes одного цвета, но разная толщина допустима. Если требуется абсолютно один stroke weight, параметр `single_weight_mode=true`.

---

## 7. Генерация альтернатив

### 7.1. Candidate families

```yaml
families:
  strict_visible:
    max_strokes: [60, 90, 120]
    completion: none
    occluder_policy: hard_reject
    detail: low_to_medium

  conservative_completion:
    max_strokes: [80, 120, 160]
    completion: line_gap_and_rhythm_only
    occluder_policy: allow_confirmed_continuation
    detail: medium

  postcard_minimal:
    max_strokes: [35, 50, 70]
    completion: conservative
    keep_groups: [silhouette, roof, facade_corner, key_windows, entrance]
    drop_groups: [texture, small_rails, fence, foliage]

  balanced_architectural:
    max_strokes: [90, 130, 170]
    completion: conservative
    keep_groups: [silhouette, roof, cornice, pilasters, major_windows, entrance, stairs]

  detailed_editorial:
    max_strokes: [160, 220, 280]
    completion: conservative
    allow_tertiary_details: true
    warning: lower_postcardness_if_thumbnail_unreadable

  heroic_crop:
    crop_margin: [0.04, 0.08]
    scale_object: large
    max_strokes: [80, 130]

  floating_icon:
    crop_margin: [0.20, 0.32]
    scale_object: medium
    ground_context: minimal
    max_strokes: [50, 90]

  feature_emphasis:
    variants: [roof_emphasis, arch_emphasis, entrance_emphasis, rhythm_emphasis, silhouette_emphasis]
```

### 7.2. Candidate diversity

Перед Gemini-оценкой нужно удалить почти одинаковые варианты.

Diversity metrics:

```text
- stroke count difference;
- crop bbox IoU;
- line group Jaccard distance;
- thumbnail perceptual hash distance;
- primitive class distribution distance.
```

Shortlist strategy:

```text
1. Сгенерировать 32–96 candidates.
2. Отфильтровать hard gates.
3. Сгруппировать похожие candidates.
4. Взять top 1–2 из каждой группы.
5. Передать Gemini 8–12 вариантов.
6. Финально сохранить top 3.
```

---

## 8. Hard gates и scoring

### 8.1. Hard gates

Candidate reject, если:

```text
- SVG invalid;
- есть embedded raster/base64;
- foreground не один цвет;
- фон не прозрачный;
- stroke count > max_allowed_for_family;
- больше X% длины линий лежит в occluder/background;
- thumbnail 128 px нечитабелен;
- object occupancy слишком маленький/большой;
- слишком много коротких stroke fragments;
- слишком высокая local line density;
- main silhouette отсутствует;
- доминирует fence/tree leakage;
- линии выходят далеко за object hull без причины.
```

### 8.2. CV/vector metrics

```text
recognizability_proxy:
  silhouette coverage
  key feature coverage
  primary object occupancy
  line alignment with object hull

postcardness_proxy:
  line economy
  negative space balance
  optical center balance
  thumbnail readability
  stroke hierarchy clarity
  rhythm/repetition quality

noise_proxy:
  short fragment ratio
  occluder overlap
  background overlap
  local density peaks
  duplicate parallel lines

geometry_proxy:
  VP alignment
  junction consistency
  vertical/horizontal/diagonal group clarity
  facade plane consistency
```

### 8.3. Suggested scoring formula

```text
score_total =
  0.26 * gemini_postcard_score
+ 0.20 * recognizability_score
+ 0.16 * silhouette_strength
+ 0.12 * line_economy_score
+ 0.10 * geometry_confidence
+ 0.08 * thumbnail_readability
+ 0.05 * local_aesthetic_score
+ 0.03 * diversity_bonus
- 0.24 * occluder_leakage
- 0.20 * hallucination_risk
- 0.14 * clutter_score
- 0.10 * crop_badness
```

Важно: `hallucination_risk` и `occluder_leakage` должны иметь veto-power. Красивый, но выдуманный вариант не должен побеждать.

---

## 9. Параметры конфигурации

```yaml
project:
  priority_domain: architecture
  output_viewbox: [0, 0, 1024, 1024]
  foreground: "#FFFFFF"
  transparent_background: true

kaggle:
  device: cuda
  precision: fp16
  model_cache_dir: /kaggle/working/models
  allow_internet: false

segmentation:
  primary_detector_order: [groundingdino, florence2, yolo_world, gemini]
  mask_refiner: sam2
  occluder_classes: [tree, foliage, fence, pole, wire, person, car, lamp_post]
  background_classes: [sky, ground, road, pavement, neighboring_building]
  object_unknown_enabled: true

line_detection:
  detectors: [deeplsd, mlsd, opencv_lsd, canny]
  min_len_px: 20
  angle_cluster_tol_deg: 7
  duplicate_distance_px: 6
  max_short_fragment_ratio: 0.28

architecture:
  enabled: true
  facade_parser_order: [segformer_cmp_facade, ade20k_mask2former, cityscapes_segformer]
  use_grammar_renderer: true
  completion_mode: conservative
  max_completion_gap_px: 180
  max_completion_gap_ratio: 1.8

renderer:
  stroke_linecap: round
  stroke_linejoin: round
  vector_effect: non_scaling_stroke
  stroke_weights:
    primary: 4.0
    secondary: 2.6
    tertiary: 1.6
  max_paths_absolute: 320

candidates:
  generate_families:
    - strict_visible
    - conservative_completion
    - postcard_minimal
    - balanced_architectural
    - heroic_crop
    - floating_icon
    - feature_emphasis
    - neural_lineart
  max_candidates_total: 64
  shortlist_for_gemini: 12

gemini:
  enabled: true
  model: gemini-2.5-flash-lite
  structured_outputs: true
  pairwise_ranking: true
  max_retries: 3

ranking:
  reject_occluder_overlap_gt: 0.12
  reject_background_overlap_gt: 0.10
  reject_stroke_count_gt: 320
  thumbnail_sizes: [64, 128, 256]
```

---

## 10. Kaggle implementation notes

### 10.1. Environment

Рекомендуемый подход:

1. В отдельном Kaggle notebook с internet on скачать модели.
2. Сохранить веса как Kaggle Dataset или Kaggle Models.
3. В production/debug notebooks подключать веса из `/kaggle/input/...`.
4. Gemini API key хранить через Kaggle Secrets, не коммитить в notebook.

Базовые зависимости:

```bash
pip install -q \
  torch torchvision torchaudio \
  diffusers transformers accelerate safetensors controlnet_aux \
  opencv-python scikit-image scipy scikit-learn pillow numpy pandas \
  shapely networkx svgwrite svgpathtools rdp \
  google-genai pydantic rich tqdm matplotlib
```

Proposal/debug only:

```bash
pip install -q supervision ultralytics segment-anything segment-anything-2
npm install -g svgo
```

`vtracer`/`potrace` can be installed only for isolated diagnostics. They are
not part of the final `final.svg` path.

Некоторые репозитории могут требовать отдельной установки из GitHub:

```bash
# пример: точные команды проверять по актуальному README конкретного repo
pip install git+https://github.com/IDEA-Research/GroundingDINO.git
pip install git+https://github.com/cvg/DeepLSD.git
```

### 10.2. Repo layout

```text
contour_svg_generator/
  README.md
  configs/
    default.yaml
    kaggle_architecture.yaml
    fast_debug.yaml
  src/
    contour_svg/
      __init__.py
      cli.py
      io.py
      preprocess.py
      primary_object.py
      masks.py
      line_detection.py
      line_graph.py
      perspective.py
      architecture/
        __init__.py
        facade_parser.py
        grammar.py
        primitives.py
        completion.py
      neural_lineart.py
      gemini_client.py
      gemini_prompts.py
      renderer_svg.py
      vector_cleanup.py
      metrics.py
      ranking.py
      debug_viz.py
      schemas.py
  notebooks/
    00_download_models.ipynb
    01_debug_single_image.ipynb
    02_batch_candidates.ipynb
  tests/
    test_svg_validity.py
    test_masks.py
    test_line_grouping.py
    test_completion_rules.py
    test_ranking.py
```

### 10.3. CLI

```bash
python -m contour_svg.cli generate \
  --image /kaggle/input/demo/photo.png \
  --config configs/kaggle_architecture.yaml \
  --primary-object "main building" \
  --out /kaggle/working/output/demo_001
```

Debug:

```bash
python -m contour_svg.cli debug-lines \
  --image /kaggle/input/demo/photo.png \
  --out /kaggle/working/debug_lines

python -m contour_svg.cli rank \
  --candidate-dir /kaggle/working/output/demo_001/candidates \
  --photo /kaggle/input/demo/photo.png \
  --out /kaggle/working/output/demo_001/ranking_report.json
```

---

## 11. Milestones

### M0 — Baseline reproducibility

- Зафиксировать текущий pipeline и сохранить debug outputs.
- Создать 5–10 benchmark images, включая фото с листвой/забором.
- Сохранить ручную оценку: good/bad и причины.

### M1 — Multi-state masks

- Подключить GroundingDINO/Florence/YOLO-World + SAM2.
- Разделить building/tree/fence/sky/ground.
- Сделать `mask_allowed_line_region`.
- Проверка: дерево не должно создавать финальные линии.

### M2 — Line graph вместо raster trace

- Подключить 2–4 line detectors.
- Сохранить `line_candidates.jsonl`.
- Реализовать overlap с масками, length/angle/source fields.
- Реализовать duplicate suppression и local density penalty.

### M3 — Perspective grouping + architecture grammar v1

- Найти основные angle clusters / vanishing groups.
- Создать primitive renderer для silhouette, roof, cornice, windows, arched windows, entrance, stairs.
- Подключить библиотеку архитектурных элементов из companion doc.

### M4 — Conservative completion

- Реализовать line gap continuation.
- Реализовать facade band continuation.
- Реализовать repeated-window placeholder только при достаточном evidence.
- Все completed strokes помечать в debug JSON.

### M5 — Candidate generation and ranking

- Создать 6–8 candidate families.
- Реализовать hard gates.
- Реализовать CV/vector scoring.
- Реализовать Gemini contact sheet judge.
- Сохранять top 3 + ranking report.

### M6 — Neural branch

- Добавить SD 1.5 + ControlNet Lineart/MLSD.
- Добавить строгую vector cleanup после neural output.
- Сравнить с geometry branch на benchmark set.

### M7 — Architecture-specific model experiments

- Протестировать SegFormer CMP facade model.
- Протестировать ADE/Cityscapes segmentation as weak labels.
- Оценить DeepFacade/RTFP/SAAF-like approaches как research branch.

---

## 12. Acceptance criteria

Для архитектурных фото с частичным закрытием:

```text
- final.svg валиден и открывается в браузере/Inkscape/Figma;
- нет растровых embed-ов;
- фон прозрачный;
- foreground один цвет;
- здание узнаваемо в 128 px preview;
- дерево/забор не являются доминирующими линиями;
- stroke count для postcard_minimal ≤ 70, balanced ≤ 170;
- основные roofline/facade corner/entrance или key windows сохранены;
- completed линии не выходят за object hull;
- final.svg создан только из primitive-rendered final-eligible candidate;
- raster/ControlNet/vector-trace candidates могут попасть только в candidates/debug, не в final.svg.
- ranking_report объясняет, почему best выбран;
- debug overlays позволяют понять, какие линии отброшены и почему.
```

---

## 13. Anti-patterns

Не делать:

1. Не тюнить бесконечно Canny threshold как основной путь. Это локально улучшит результат, но не даст semantic pruning.
2. Не использовать `mask → architecture elements` как единственный путь. Маска бедна внутренней информацией.
3. Не принимать diffusion/inpainting output как truth. Использовать только как proposal.
4. Не просить Gemini генерировать точные SVG path coordinates.
5. Не векторизовать весь raster line-art без semantic cleanup.
6. Не пытаться сразу сделать универсальный pipeline для всех объектов. Сделать core + architecture plugin.

---

## 14. Расширение на другие объекты

Общее ядро:

```text
primary object selection
multi-state masks
occluder suppression
candidate contour graph
primitive rendering
candidate ranking
Gemini semantic judge
SVG export
```

Плагины:

```text
architecture_plugin:
  vanishing points, facade primitives, windows, roof, cornices, completion rules

vehicle_plugin:
  wheels, windows, silhouette, lights, grille, side profile

statue_plugin:
  silhouette, face/body planes, pedestal, key contours

product_plugin:
  silhouette, brand-safe simplification, major edges, symmetric cleanup

animal_plugin:
  body silhouette, head, eyes, ears, limbs, fur suppression
```

---

## 15. Companion documents

1. `architecture_elements_library_v0_1.md` — библиотека архитектурных элементов и правил отрисовки.
2. `models_tools_catalog_v0_1.md` — инструменты, модели, ссылки, роль в пайплайне.
3. `gemini_prompts_and_schemas_v0_1.md` — промпты и JSON schemas для Gemini semantic/review loop.
4. `implementation_backlog_v0_1.md` — задачи для кодового агента, roadmap, Definition of Done.

---

## 16. Reference links

### Kaggle

- Kaggle Notebooks: https://www.kaggle.com/docs/notebooks
- Kaggle Models: https://www.kaggle.com/docs/models

### Gemini

- Gemini API quickstart: https://ai.google.dev/gemini-api/docs/quickstart
- Google GenAI SDK libraries: https://ai.google.dev/gemini-api/docs/libraries
- Structured outputs: https://ai.google.dev/gemini-api/docs/structured-output
- Python SDK: https://googleapis.github.io/python-genai/

### Detection / segmentation

- GroundingDINO GitHub: https://github.com/IDEA-Research/GroundingDINO
- Grounding DINO Transformers docs: https://huggingface.co/docs/transformers/en/model_doc/grounding-dino
- Grounded Segment Anything: https://github.com/IDEA-Research/Grounded-Segment-Anything
- Grounded SAM 2: https://github.com/IDEA-Research/Grounded-SAM-2
- SAM2 GitHub: https://github.com/facebookresearch/sam2
- SAM2 Transformers docs: https://huggingface.co/docs/transformers/en/model_doc/sam2
- Florence-2 docs: https://huggingface.co/docs/transformers/en/model_doc/florence2
- YOLO-World GitHub: https://github.com/AILab-CVC/YOLO-World
- Ultralytics YOLO-World docs: https://docs.ultralytics.com/models/yolo-world/
- OWL-ViT docs: https://huggingface.co/docs/transformers/en/model_doc/owlvit
- OWLv2 docs: https://huggingface.co/docs/transformers/en/model_doc/owlv2

### Architecture / facade

- CMP Facade Database: https://cmp.felk.cvut.cz/~tylecr1/facade/
- CMP Facade HF dataset: https://huggingface.co/datasets/Xpitfire/cmp_facade
- SegFormer CMP Facade model: https://huggingface.co/Xpitfire/segformer-finetuned-segments-cmp-facade
- 3DOM Semantic Facade: https://github.com/3DOM-FBK/3DOM-Semantic-Facade
- DeepFacade GitHub: https://github.com/liuhantang/DeepFacade
- DeepFacade paper: https://www.ijcai.org/proceedings/2017/0320.pdf
- RTFP facade parsing repo: https://github.com/wbw520/RTFP
- SAAF paper: https://arxiv.org/abs/2506.09071
- Irregular Facades dataset paper: https://www.mdpi.com/2075-5309/14/9/2602
- ZAHA facade dataset repo: https://github.com/oloocki/zaha
- TUM-Facade: https://github.com/oloocki/tum-facade
- MMSegmentation: https://github.com/open-mmlab/mmsegmentation

### Line detection / wireframe

- M-LSD GitHub: https://github.com/navervision/mlsd
- DeepLSD GitHub: https://github.com/cvg/DeepLSD
- HAWP GitHub: https://github.com/cherubicXN/hawp
- Hough Transform tutorial: https://learnopencv.com/hough-transform-with-opencv-c-python/
- OpenCV Hough lines tutorial mirror: https://vovkos.github.io/doxyrest-showcase/opencv/sphinxdoc/page_tutorial_py_houghlines.html

### Inpainting / depth

- LaMa GitHub: https://github.com/advimman/lama
- LaMa project: https://advimman.github.io/lama-project/
- Diffusers inpainting: https://huggingface.co/docs/diffusers/en/using-diffusers/inpaint
- OpenCV inpainting tutorial: https://pyimagesearch.com/2020/05/18/image-inpainting-with-opencv-and-python/
- Depth Anything V2 GitHub: https://github.com/DepthAnything/Depth-Anything-V2
- Depth Anything V2 Transformers docs: https://huggingface.co/docs/transformers/en/model_doc/depth_anything_v2
- MiDaS GitHub: https://github.com/isl-org/MiDaS

### Diffusion / ControlNet

- Diffusers ControlNet guide: https://huggingface.co/docs/diffusers/en/using-diffusers/controlnet
- ControlNet paper: https://arxiv.org/abs/2302.05543
- ControlNet Lineart SD 1.5: https://huggingface.co/lllyasviel/control_v11p_sd15_lineart
- ControlNet MLSD SD 1.5: https://huggingface.co/lllyasviel/control_v11p_sd15_mlsd
- ControlNet Canny SD 1.5: https://huggingface.co/lllyasviel/control_v11p_sd15_canny
- ControlNet SoftEdge SD 1.5: https://huggingface.co/lllyasviel/control_v11p_sd15_softedge
- ControlNet Inpaint SD 1.5: https://huggingface.co/lllyasviel/control_v11p_sd15_inpaint

### SVG / vector

- VTracer: https://github.com/visioncortex/vtracer
- Potrace: https://potrace.sourceforge.net/
- Potrace man page: https://potrace.sourceforge.net/potrace.1.html
- svgwrite docs: https://svgwrite.readthedocs.io/
- svgwrite PyPI: https://pypi.org/project/svgwrite/
- SVGO: https://svgo.dev/
- SVGO GitHub: https://github.com/svg/svgo
- Shapely docs: https://shapely.readthedocs.io/
- NetworkX docs: https://networkx.org/documentation/stable/tutorial.html
- scikit-image: https://scikit-image.org/
- RDP package docs: https://rdp.readthedocs.io/

### Aesthetic / preference scoring

- LAION aesthetic predictor: https://github.com/LAION-AI/aesthetic-predictor
- Improved aesthetic predictor: https://github.com/christophschuhmann/improved-aesthetic-predictor
- PickScore model: https://huggingface.co/yuvalkirstain/PickScore_v1
- Pick-a-Pic paper: https://huggingface.co/papers/2305.01569
- ImageReward GitHub: https://github.com/zai-org/ImageReward
- ImageReward HF: https://huggingface.co/zai-org/ImageReward
- HPSv2: https://github.com/tgxs002/HPSv2
- HPSv3: https://huggingface.co/MizzenAI/HPSv3
