# Addendum: `RhythmGraph` — архитектурный ритм как калибратор геометрии SVG-пайплайна

Дата: 2026-06-14
Контекст: `contour_svg_generator`, ветка `feature/contour-svg-generator-v03`
Статус: проектное дополнение к recovery plan / engineering spec v0.3

---

## 0. Короткое решение

Архитектурный ритм **обязательно стоит внедрять**. Это не декоративная эвристика, а один из главных недостающих механизмов, который может превратить набор найденных линий в цельную архитектурную графику.

Но внедрять его нужно не как отдельный генератор и не как “рисуй повторяющиеся окна где красиво”, а как **геометрический регуляризатор** между `PlaneGraph` и финальным `FeatureGraph`.

Правильная роль ритма:

```text
перспектива задаёт систему координат фасадной плоскости;
ритм задаёт регулярную или полурегулярную сетку внутри этой плоскости;
линии, окна, карнизы, пилястры и арки снапятся к этой сетке;
пропуски под листвой достраиваются только там, где ритм подтверждён видимыми элементами.
```

В текущей спецификации ритм уже упоминался как “repetition / rhythm score”, но этого недостаточно. Его нужно поднять до полноценной структуры данных: `RhythmGraph`.

---

## 1. Почему это реально поможет

Текущий пайплайн страдает не от отсутствия линий, а от отсутствия калибровки линий. `edge_map`, `DeepLSD`, `MLSD`, `elements_overlay` и фасадный парсер уже дают полезные свидетельства, но они не согласованы между собой.

Архитектура, особенно фасадная, часто организована ритмами:

- повтор окон по горизонтали;
- повтор этажей по вертикали;
- карнизные горизонтальные пояса;
- пилястры / колонны / простенки;
- арки в одном ряду;
- повтор фронтонов / слуховых окон / декоративных вставок;
- лестничные ступени;
- балюстрады и перила, если они принадлежат зданию, а не забору.

Этот ритм позволяет:

1. **Сдвигать линии на правильное место.**
   Если линия Canny или DeepLSD чуть шумная, но рядом есть сильная фасадная сетка, её можно привязать к ближайшей оси.

2. **Схлопывать паразитные пучки.**
   Если 7 почти параллельных линий на крыше/карнизе претендуют на одну роль, ритм и перспектива помогают оставить 1–3 канонические линии.

3. **Достраивать закрытые фрагменты без галлюцинации.**
   Если окно или карниз прерывается деревом, но виден до и после окклюзии, его можно продолжить. Если виден ряд из 4 окон и одно место закрыто листвой, можно создать `missing_slot` и осторожно дорисовать.

4. **Отличать архитектурный повтор от шума.**
   Листва тоже создаёт много линий, но они не ложатся в фасадную регулярность, плохо совпадают с перспективными группами и находятся в `occluder_mask`.

5. **Повысить “открыточность”.**
   Люди лучше воспринимают архитектурную открытку, когда в ней есть ясный силуэт, несколько доминантных направлений и ритмичная повторяемость деталей, а не случайная паутина контуров.

---

## 2. Что сейчас учтено недостаточно

В текущей логике уже есть:

```text
EvidenceInventory-like layers:
- edge_map;
- MLSD / DeepLSD;
- facade wallplane;
- elements_overlay;
- object / occluder masks;
- ControlNet candidates;
- primitive renderer families;
- Gemini judge/editor.
```

Но нет центрального слоя:

```text
PlaneGraph → RhythmGraph → RefinedFeatureGraph
```

Из-за этого:

- `elements_overlay` хорошо находит окна, но окна не выстраиваются в ряды/колонки;
- `deeplsd_lines_overlay` даёт хорошие линии, но пучки не схлопываются до канонических осей;
- `edge_map` очень точный, но слишком детальный и не подчинён архитектурной сетке;
- `occluder_mask` показывает листву, но не говорит, какие архитектурные линии должны пройти через закрытые зоны;
- Gemini редактирует line groups, но не видит формализованного ритма фасада.

---

## 3. Важное уточнение: перспектива — не просто коэффициент

Интуиция автора верная: перспектива даёт поправку, ритм даёт калибратор. Но в реализации лучше думать не об одном коэффициенте, а о **проективной системе координат каждой фасадной плоскости**.

Для каждой плоскости здания:

```text
PlaneGraph.plane_id = front_facade | side_facade | roof_plane | base_plane | ...
H_plane      = homography from rectified plane coordinates to image pixels
H_plane_inv  = inverse homography from image pixels to rectified plane coordinates
```

Точка изображения `(x, y)` переводится в локальные координаты плоскости:

```text
[u, v, w] = H_plane_inv · [x, y, 1]
u = u / w
v = v / w
```

В этих координатах ритм часто становится почти равномерным:

```text
u_k ≈ u0 + k * Δu       # колонки / bays / окна по фасаду
v_m ≈ v0 + m * Δv       # этажи / горизонтальные пояса
```

А затем канонические линии обратно проецируются в изображение:

```text
[x, y, w] = H_plane · [u_k, v_m, 1]
x = x / w
y = y / w
```

Это автоматически учитывает перспективное сжатие: в пикселях расстояния могут уменьшаться к точке схода, но в rectified plane они остаются регулярными.

### MVP-упрощение

Если полноценная homography для фасада пока неустойчива, допустимо начать с более простого режима:

```text
- vertical axis = вертикали в изображении;
- horizontal/diagonal axis = линии из VP-групп;
- rhythm coordinate = параметр вдоль семейства параллельных/сходящихся линий;
- snap производится не в реальной плоскости, а в псевдо-плоскости.
```

Но конечная цель — именно `plane-local coordinates`.

---

## 4. Место `RhythmGraph` в общем пайплайне

Текущий целевой пайплайн нужно изменить так:

```text
Photo
→ EvidenceInventory
→ BuildingShell
→ PlaneGraph
→ PreliminaryFeatureGraph
→ RhythmGraph
→ RefinedFeatureGraph
→ OccluderAwareCompletionGraph
→ PrimitiveScene
→ SVG Candidate Families
→ Gemini / CV Ranking
→ Top SVG outputs
```

Где:

- `PreliminaryFeatureGraph` — сырые окна, двери, карнизы, пилястры, линии, найденные из CV/ML evidence;
- `RhythmGraph` — регулярности и фасадные сетки, оцененные по preliminary features и line evidence;
- `RefinedFeatureGraph` — те же элементы, но после snap/merge/drop/complete;
- `PrimitiveScene` — единственный источник финального SVG.

Нельзя строить финальный SVG напрямую из `edge_map`, `mlsd_guide` или `ControlNet PNG`. Эти слои должны быть evidence, а не final source.

---

## 5. Структуры данных

### 5.1. `RhythmGraph`

```python
@dataclass
class RhythmGraph:
    image_id: str
    plane_rhythms: list[PlaneRhythm]
    global_score: float
    warnings: list[str]
```

### 5.2. `PlaneRhythm`

```python
@dataclass
class PlaneRhythm:
    plane_id: str
    plane_type: str  # front_facade | side_facade | roof | base | unknown
    homography_available: bool
    H_image_from_plane: list[list[float]] | None
    H_plane_from_image: list[list[float]] | None
    vertical_sequences: list[RhythmSequence]
    horizontal_sequences: list[RhythmSequence]
    opening_sequences: list[RhythmSequence]
    cornice_sequences: list[RhythmSequence]
    confidence: float
```

### 5.3. `RhythmSequence`

```python
@dataclass
class RhythmSequence:
    id: str
    plane_id: str
    type: str
    # examples:
    # window_columns, window_rows, floor_bands, cornice_stack,
    # pilaster_bays, arch_row, stairs_steps, balcony_posts
    axis: str  # u | v | diagonal | mixed
    model: str  # uniform | alternating | grouped | irregular_supported
    origin: float
    step: float
    slots: list[RhythmSlot]
    support_feature_ids: list[str]
    support_line_group_ids: list[str]
    visible_support_ratio: float
    occluded_support_ratio: float
    residual_mean: float
    residual_p95: float
    confidence: float
```

### 5.4. `RhythmSlot`

```python
@dataclass
class RhythmSlot:
    id: str
    sequence_id: str
    index: int
    plane_coord: tuple[float, float] | None
    image_geometry: dict
    status: str
    # observed | inferred_occluded | inferred_low_confidence | rejected
    assigned_feature_ids: list[str]
    assigned_line_group_ids: list[str]
    confidence: float
    completion_allowed: bool
    completion_reason: str | None
```

### 5.5. Расширение `FacadeElement`

В `FeatureGraph` / `FacadeElement` добавить поля:

```python
rhythm_sequence_id: str | None
rhythm_slot_id: str | None
plane_id: str | None
pre_snap_geometry: Geometry
snapped_geometry: Geometry | None
snap_residual_px: float | None
snap_confidence: float
is_rhythm_outlier: bool
```

### 5.6. Расширение `LineGroup`

```python
rhythm_role: str | None
# main_axis | duplicate_candidate | cornice_band | floor_band | opening_edge | noise
rhythm_sequence_id: str | None
canonical_line_id: str | None
merge_target_id: str | None
snap_residual_px: float | None
```

---

## 6. Архитектурные типы ритма

### 6.1. `floor_bands`

Горизонтальные уровни этажей, карнизы, молдинги, цокольные линии.

Используются для:

- выравнивания горизонтальных фасадных линий;
- удаления дублирующих edge-линий;
- построения этажной сетки;
- достраивания линии через дерево.

SVG-render policy:

```text
minimal: 1 линия на главный этажный уровень
balanced: 1–2 линии на карниз / пояс
editorial: до 3 линий, если они ясно различимы
```

### 6.2. `window_rows`

Ряды окон на одном этаже.

Используются для:

- snap оконных прямоугольников к общей высоте;
- объединения найденных окон в ряды;
- восстановления пропущенных окон только внутри подтверждённого ряда.

SVG-render policy:

```text
window_rect: внешний прямоугольник + максимум 1–2 внутренних штриха
window_arch: две вертикали + дуга
small window: только внешний контур
```

### 6.3. `window_columns` / `bay_rhythm`

Вертикальные ряды окон, простенки, bays, пилястры.

Используются для:

- расстановки вертикалей;
- восстановления фасадного ритма под листвой;
- выявления главных осей композиции.

SVG-render policy:

```text
minimal: сохранить только 2–4 главных bay-оси
balanced: сохранить bay-оси + основные окна
editorial: добавить больше оконных деталей
```

### 6.4. `pilaster_bays`

Ритм пилястр, колонн, вертикальных простенков.

Особенно важен для неоклассической архитектуры.

Используется для:

- усиления узнаваемости;
- стабилизации вертикальных линий;
- отделения архитектурных вертикалей от стволов деревьев / фонарей.

### 6.5. `cornice_stack`

Набор близких параллельных линий карниза.

Задача — не сохранить все найденные линии, а выбрать канонический стек.

Правило:

```text
если найдено 5–12 близких параллельных линий,
renderer оставляет 1–3 линии в зависимости от candidate family.
```

### 6.6. `roof_rhythm`

Ритм крыши, фронтонов, слуховых окон, наклонных рёбер.

Используется для:

- схлопывания паразитных диагональных пучков;
- сохранения главного roofline;
- различения roof edge и случайных линий листвы/веток.

### 6.7. `arch_row`

Ряд арочных окон или арочных ниш.

Используется для:

- сохранения характерного архитектурного признака;
- упрощения арки до чистой дуги;
- достраивания арки только при сильном подтверждении.

### 6.8. `stairs_steps`

Ритм ступеней.

Используется осторожно: ступени могут легко перегрузить открытку.

Правило:

```text
minimal: 0–3 ступени
balanced: 3–5 ступеней
editorial: 5–8 ступеней, если они помогают опоре здания
```

### 6.9. `negative_rhythm`: забор, листва, брусчатка

Очень важно: не всякий повтор полезен.

Забор, деревья и брусчатка тоже создают сильный ритм, но это **отрицательный ритм**. Он должен помогать удалять паразитные линии, а не добавлять их.

```text
если line sequence ритмична, но:
- лежит вне object_visible;
- совпадает с fence/tree/ground mask;
- не принадлежит PlaneGraph;
то sequence получает role = negative_rhythm и запрещается для SVG.
```

---

## 7. Алгоритм построения `RhythmGraph`

### Шаг 1. Получить `PlaneGraph`

Минимально нужны:

```text
- front facade polygon;
- side facade polygon;
- roof / base polygons, если доступны;
- vanishing direction groups;
- major verticals;
- shell contour.
```

Если `PlaneGraph` низкого качества, `RhythmGraph` должен работать в weak mode или отключаться.

### Шаг 2. Собрать preliminary features

Источники:

```text
- facade parser elements: window, door, balcony, pilaster, cornice, molding, wall_plane;
- line groups from MLSD / DeepLSD / Hough / edge_map;
- shell contour;
- neural proposals, если они прошли sanity checks;
- occluder mask / object_visible mask.
```

Каждый feature должен иметь:

```text
- bbox / polygon / line geometry;
- confidence;
- source;
- overlap with object_visible;
- overlap with occluder;
- tentative plane_id.
```

### Шаг 3. Перевести features в plane-local coordinates

Для каждой плоскости:

```python
for feature in features_on_plane:
    feature.local_geometry = project_image_to_plane(feature.geometry, H_plane_inv)
```

Если homography нет:

```text
использовать pseudo-plane coordinates:
- u = projection along horizontal/VP axis;
- v = projection along vertical axis;
- confidence lower.
```

### Шаг 4. Найти 1D-пики ритма

Для каждой плоскости строятся проекционные профили:

```text
vertical_profile(u): density of vertical edges, window centers, pilasters
horizontal_profile(v): density of horizontal edges, cornices, sill lines, floor bands
```

Источники плотности:

```text
- центры окон;
- стороны окон;
- длинные фасадные линии;
- карнизы / молдинги;
- пилястры;
- shell edges;
- edge energy из edge_map внутри object_visible, но не внутри occluder.
```

### Шаг 5. Кластеризовать пики

Использовать:

```text
DBSCAN / HDBSCAN-like clustering для близких координат;
weighted median для центра оси;
robust confidence по источникам.
```

Примеры:

```python
u_clusters = cluster_1d(weighted_u_peaks, eps=eps_u)
v_clusters = cluster_1d(weighted_v_peaks, eps=eps_v)
```

### Шаг 6. Подогнать регулярную сетку

Для кандидатов ритма подбирается модель:

```text
slot_position(k) = origin + k * step
```

Оценка:

```text
loss = Σ weighted_distance(observed_peak, nearest_slot)
       + missing_penalty
       + irregularity_penalty
       + occluder_context_adjustment
```

Рекомендуемые методы:

```text
- RANSAC по парам/тройкам пиков;
- dynamic programming для assignment observed peaks → slots;
- grid search по step/origin с robust loss;
- Hungarian assignment для сопоставления observed features и slots;
- Theil-Sen / median-based estimates для устойчивости.
```

### Шаг 7. Назначить features слотам

```python
for feature in preliminary_features:
    slot = nearest_slot(feature.local_center, allowed_distance)
    if slot and semantic_compatible(feature, slot):
        assign(feature, slot)
```

Пример совместимости:

```text
window feature → window_row/window_column slot
cornice line → floor_band/cornice_stack slot
pilaster line → bay_rhythm slot
roof edge → roof_rhythm slot
fence line → rejected, even if rhythmic
```

### Шаг 8. Снапнуть геометрию

Снап выполняется не бинарно, а с силой:

```text
snap_strength = rhythm_confidence * feature_confidence * plane_confidence
```

Правила:

```text
- сильный snap для оконных рядов и карнизов;
- умеренный snap для roof edges;
- слабый snap для декоративных деталей;
- no snap для low-confidence / occluder-heavy lines.
```

Пример:

```python
snapped_x = lerp(original_x, rhythm_x, snap_strength)
```

Для финального SVG часто лучше не частично двигать исходную шумную линию, а заменить её каноническим примитивом:

```text
window bbox → normalized window primitive
cornice line group → canonical cornice stroke(s)
arch contour → canonical cubic Bézier arc
```

### Шаг 9. Схлопнуть дубликаты

Если несколько line groups попали в один rhythm slot:

```text
- выбрать canonical line по source confidence + length + semantic role;
- остальные пометить merge_target_id;
- renderer рисует только canonical line или small stack.
```

Это должно устранить пучки на крыше и карнизах.

### Шаг 10. Создать completion slots

Completion разрешается только если:

```text
- rhythm sequence confidence >= threshold;
- есть минимум N наблюдаемых слотов;
- missing slot пересекает occluder_mask;
- slot находится внутри BuildingShell / PlaneGraph polygon;
- нет противоречия с видимым background;
- Gemini или CV gate не пометили область как unsafe.
```

Запрещено:

```text
- добавлять новые ряды за пределами наблюдаемой области;
- продолжать ритм наружу за shell;
- строить окна в неизвестных местах без повторяемости;
- восстанавливать детали по одному единственному примеру.
```

---

## 8. Как это применить к текущему зданию из `audit_1527`

По текущим артефактам автор заметил:

```text
facade wallplane      — полезен;
deeplsd_lines_overlay — неплох;
edge_map              — точен, но требует чистки;
elements_overlay      — неплохо определил окна;
masked_background     — хорошо даёт контур здания;
mlsd_guide            — хорош для крупной геометрии;
occluder_mask         — полезен для деревьев и интерполяции.
```

`RhythmGraph` должен использовать это так:

### 8.1. `masked_background` + masks → `BuildingShell`

Сначала строится чистый внешний корпус здания:

```text
- крыша;
- главный фасад;
- правый объём;
- цоколь / основание;
- исключение деревьев и забора.
```

### 8.2. `MLSD` / `DeepLSD` → major axes

Использовать только длинные линии:

```text
- roofline;
- карнизы;
- вертикальные углы фасада;
- основные горизонтали цоколя;
- перспективные направляющие правого фасада.
```

Мелкие линии не входят в SVG напрямую.

### 8.3. `elements_overlay` → preliminary window/door features

Окна не рисуются как найденные bbox один-в-один. Они распределяются по рядам и колонкам:

```text
front facade:
  row_1, row_2, row_3
  bay_1, bay_2, bay_3

side facade:
  row_1, row_2
  bay_1..N
```

После этого renderer решает, сколько окон реально рисовать.

### 8.4. `edge_map` → detail support, not direct vector

`edge_map` используется для:

```text
- подтверждения арок;
- уточнения roofline;
- проверки, есть ли видимая линия на ожидаемом rhythm slot;
- поддержки оконной рамки;
- локальной формы арочного окна.
```

Но `edge_map` не должен становиться самостоятельным SVG candidate.

### 8.5. `occluder_mask` → missing slots

Деревья слева должны стать `occluded area`.

Допустимое восстановление:

```text
- продолжить карниз через крону, если линия видна слева и справа;
- продолжить фасадный пояс, если он лежит в том же floor_band;
- восстановить окно только если ряд окон убедительно продолжается под деревом.
```

Недопустимое:

```text
- переносить контур кроны как часть здания;
- создавать декоративные детали, которых не подтверждает ритм;
- добавлять окна за пределами фасадной плоскости.
```

---

## 9. Роль Gemini в ритме

Gemini не должен вычислять координаты ритма. Его сильная роль — semantic judge / critic.

### 9.1. Что Gemini должен получать

Contact sheet / overlay:

```text
1. original photo;
2. building shell overlay;
3. plane graph overlay;
4. preliminary features overlay;
5. rhythm candidate overlay with sequence IDs;
6. occluder mask overlay;
7. current SVG preview.
```

### 9.2. Что Gemini должен возвращать

Структурированный JSON:

```json
{
  "global_assessment": {
    "rhythm_useful": true,
    "main_issue": "roof and cornice lines are duplicated; window rows should be aligned",
    "risk": "left side is occluded by trees; do not infer new decorative details"
  },
  "rhythm_sequences": [
    {
      "sequence_id": "front_floor_bands_v2",
      "decision": "accept",
      "reason": "matches visible cornice levels and perspective",
      "strength": "strong"
    },
    {
      "sequence_id": "tree_leaf_edges_left",
      "decision": "reject",
      "reason": "this is foliage contour, not architectural rhythm",
      "strength": "none"
    }
  ],
  "repair_actions": [
    {
      "type": "merge_duplicate_lines",
      "target": "roof_cornice_stack_right",
      "max_output_lines": 3
    },
    {
      "type": "snap_features_to_row",
      "target": "front_window_row_2",
      "snap_strength": 0.75
    },
    {
      "type": "allow_completion",
      "target": "front_floor_band_1_gap_tree",
      "condition": "only across occluder and inside shell"
    }
  ]
}
```

### 9.3. Gemini prompt sketch

```text
You are reviewing architectural line-art construction evidence.
Your job is not to draw SVG coordinates.
Your job is to decide which detected rhythms are real architectural rhythms,
which are occluder/background rhythms, and which should be used to snap,
merge, or complete vector primitives.

Prefer conservative decisions.
Reject tree/fence/pavement rhythms even if visually repetitive.
Allow completion only inside the building shell and only through occluder regions.
```

---

## 10. Scoring и gates для ритма

### 10.1. `rhythm_confidence`

```text
rhythm_confidence =
  + visible_support_ratio
  + source_diversity_score
  + plane_alignment_score
  + low_residual_score
  + semantic_consistency_score
  - occluder_leakage_penalty
  - background_overlap_penalty
  - irregularity_penalty
```

### 10.2. `source_diversity_score`

Ритм сильнее, если его подтверждают разные источники:

```text
+ facade parser
+ DeepLSD / MLSD
+ edge_map energy
+ shell/plane geometry
+ Gemini accept
```

Слабее, если его нашёл только один источник.

### 10.3. Hard gates

Отклонить sequence, если:

```text
- больше 40% support лежит в occluder/background;
- sequence не принадлежит ни одной plane;
- observed slot count < min_observed_slots;
- residual слишком большой;
- ритм совпадает с fence/tree/ground class;
- sequence находится вне BuildingShell;
- completion требует экстраполяции наружу, а не интерполяции.
```

### 10.4. Completion gates

Разрешить missing slot, если:

```text
- slot расположен между observed slots, а не за пределами ряда;
- область slot пересекает occluder mask;
- local plane confidence высокая;
- рядом есть минимум 2–3 похожих observed features;
- completion не увеличивает SVG complexity слишком сильно.
```

---

## 11. Влияние на candidate families

`RhythmGraph` должен влиять на все render families.

### 11.1. `strict_visible`

```text
- без inferred slots;
- snap разрешён;
- merge duplicates разрешён;
- completion запрещён.
```

### 11.2. `postcard_minimal`

```text
- только главные rhythm sequences;
- line budget 40–80;
- окна рисуются выборочно;
- карнизные стеки схлопываются до 1–2 линий;
- высокий negative space.
```

### 11.3. `balanced_architectural`

```text
- главные + вторичные rhythm sequences;
- line budget 80–150;
- часть оконных рядов;
- консервативные completion slots разрешены.
```

### 11.4. `detailed_editorial`

```text
- больше повторов;
- line budget 150–250;
- но отрицательные ритмы всё равно запрещены.
```

### 11.5. `feature_emphasis`

```text
- arch_emphasis: сохранять arch_row;
- roof_emphasis: сохранять roof_rhythm;
- entrance_emphasis: сохранять центральные оси и door rhythm;
- facade_rhythm_emphasis: усилить floor_bands + bay_rhythm.
```

---

## 12. Конфигурация

Добавить в config:

```yaml
rhythm_graph:
  enabled: true
  mode: "medium" # off | weak | medium | strong

  min_plane_confidence: 0.45
  min_sequence_confidence: 0.55
  min_observed_slots:
    floor_bands: 2
    window_rows: 3
    window_columns: 2
    cornice_stack: 2
    pilaster_bays: 2
    stairs_steps: 3

  snap:
    enabled: true
    max_snap_px: 18
    default_strength: 0.65
    strong_sequence_strength: 0.85
    weak_sequence_strength: 0.35

  duplicate_merge:
    enabled: true
    angle_eps_deg: 4.0
    distance_eps_px: 12
    keep_max_per_slot:
      minimal: 1
      balanced: 2
      detailed: 3

  completion:
    enabled: true
    allow_only_inside_shell: true
    require_occluder_intersection: true
    max_missing_slots_per_sequence: 2
    max_completion_length_ratio: 0.35
    min_neighbor_support: 2

  negative_rhythm:
    reject_tree: true
    reject_fence: true
    reject_ground_pavement: true
    reject_sky: true

  gemini_review:
    enabled: true
    review_top_sequences: 20
    require_accept_for_low_confidence_completion: true
```

---

## 13. Новые debug artifacts

Агент должен выводить:

```text
debug/rhythm_graph.json
```

Полная структура ритмов, слотов, confidence, assigned features.

```text
debug/rhythm_overlay.png
```

Overlay на исходнике:

- принятые ритмы зелёным/синим;
- rejected negative rhythms красным;
- inferred occluded slots пунктиром;
- slot IDs.

```text
debug/plane_rectified_front.png
debug/plane_rectified_side.png
```

Rectified views фасадных плоскостей, если homography доступна.

```text
debug/rhythm_profiles_front.json
```

1D-профили по `u` и `v`, пики, кластеры, fitted grid.

```text
debug/snap_before_after_overlay.png
```

До/после snap для окон, карнизов и rooflines.

```text
debug/rhythm_completion_overlay.png
```

Разрешённые и запрещённые completion proposals.

---

## 14. Acceptance criteria для следующего спринта

### 14.1. Минимальный milestone

На `audit_1527` должно появиться:

```text
- debug/rhythm_graph.json;
- debug/rhythm_overlay.png;
- хотя бы 1 front facade rhythm;
- хотя бы 1 horizontal floor/cornice rhythm;
- хотя бы 1 window row или bay rhythm, если elements_overlay это поддерживает;
- rejected negative rhythm для дерева/забора/земли, если они попадают в кандидаты;
- shell-only и rhythm-only SVG previews.
```

### 14.2. Визуальные критерии

В rhythm-enhanced candidates:

```text
- крыша и карнизы имеют меньше дублирующих параллельных линий;
- окна/двери выровнены лучше, чем в сыром elements_overlay;
- линии дерева не попадают в архитектурные ритмы;
- пропуски под листвой достраиваются только как продолжение существующих фасадных линий;
- общий SVG выглядит менее “паутинным”.
```

### 14.3. Числовые критерии

```text
- duplicate_parallel_groups_count reduced by >= 30%;
- accepted_negative_rhythm_count = 0;
- avg_snap_residual_px <= configured threshold;
- inferred completion slots <= max_missing_slots_per_sequence;
- line count не растёт после внедрения ритма, а обычно уменьшается;
- Gemini postcard score не ухудшается при равной узнаваемости.
```

---

## 15. Implementation plan

### Task 1. Добавить `rhythm_graph.py`

Функции:

```python
def build_rhythm_graph(
    shell: BuildingShell,
    planes: PlaneGraph,
    preliminary_features: FeatureGraph,
    line_groups: list[LineGroup],
    masks: MultiStateMasks,
    config: RhythmGraphConfig,
) -> RhythmGraph:
    ...
```

Внутренние функции:

```python
project_features_to_plane()
build_axis_profiles()
cluster_axis_peaks()
fit_uniform_rhythm_sequences()
assign_features_to_slots()
classify_negative_rhythms()
score_rhythm_sequences()
```

### Task 2. Добавить `rhythm_snap.py`

```python
def refine_feature_graph_with_rhythm(
    feature_graph: FeatureGraph,
    rhythm_graph: RhythmGraph,
    config: RhythmSnapConfig,
) -> FeatureGraph:
    ...
```

Функции:

```python
snap_window_boxes_to_rows()
snap_cornice_lines_to_bands()
snap_pilasters_to_bays()
merge_duplicate_line_groups_by_slot()
```

### Task 3. Добавить `rhythm_completion.py`

```python
def propose_rhythm_completions(
    rhythm_graph: RhythmGraph,
    shell: BuildingShell,
    planes: PlaneGraph,
    masks: MultiStateMasks,
    config: RhythmCompletionConfig,
) -> list[CompletionProposal]:
    ...
```

### Task 4. Интегрировать Gemini review

Добавить новый prompt:

```text
review_rhythm_graph_prompt.md
```

Новая схема:

```text
gemini_rhythm_review.schema.json
```

Gemini должен возвращать:

```text
accept / reject / weaken / strengthen / allow_completion / forbid_completion
```

### Task 5. Обновить `PrimitiveScene`

`PrimitiveScene` должен принимать уже refined features:

```text
PrimitiveScene = shell + planes + rhythm-refined features + accepted completions
```

И не рисовать raw line candidates, если они:

```text
- не принадлежат plane;
- не имеют semantic role;
- rejected by rhythm;
- являются duplicate within rhythm slot.
```

### Task 6. Обновить scoring

Добавить в `scoring.py`:

```python
rhythm_coherence_score
negative_rhythm_leakage_penalty
snap_residual_penalty
completion_rhythm_support_score
```

---

## 16. Псевдокод

```python
def run_pipeline(image):
    evidence = collect_evidence(image)

    shell = build_building_shell(evidence)
    if shell.score < cfg.shell.min_score:
        return fail_low("No reliable building shell")

    planes = build_plane_graph(shell, evidence)

    prelim_features = build_preliminary_feature_graph(
        facade_elements=evidence.facade_elements,
        line_groups=evidence.line_groups,
        masks=evidence.masks,
        planes=planes,
    )

    rhythm = build_rhythm_graph(
        shell=shell,
        planes=planes,
        preliminary_features=prelim_features,
        line_groups=evidence.line_groups,
        masks=evidence.masks,
        config=cfg.rhythm_graph,
    )

    if cfg.gemini.rhythm_review:
        rhythm_review = gemini_review_rhythm(
            image=image,
            shell=shell,
            planes=planes,
            rhythm=rhythm,
            debug_overlay=render_rhythm_overlay(rhythm),
        )
        rhythm = apply_rhythm_review(rhythm, rhythm_review)

    refined_features = refine_feature_graph_with_rhythm(
        prelim_features,
        rhythm,
        cfg.rhythm_snap,
    )

    completions = propose_rhythm_completions(
        rhythm,
        shell,
        planes,
        evidence.masks,
        cfg.rhythm_completion,
    )

    scene = build_primitive_scene(
        shell=shell,
        planes=planes,
        features=refined_features,
        completions=completions,
    )

    candidates = render_svg_families(scene, cfg.render_families)
    ranked = rank_candidates(candidates, evidence, rhythm)
    return ranked
```

---

## 17. Риски

### 17.1. Ритм может усилить галлюцинации

Если слишком агрессивно достраивать повтор, система начнёт рисовать окна там, где их нет.

Митигация:

```text
- require observed slots;
- completion only inside shell;
- completion only across occluder;
- no extrapolation;
- Gemini review for low-confidence completions;
- family-specific budgets.
```

### 17.2. Нерегулярная архитектура

Не все здания имеют регулярный ритм.

Митигация:

```text
- rhythm mode can be weak/off;
- allow irregular_supported model;
- use rhythm only for merge/drop, not for completion;
- fallback to shell + major line graph.
```

### 17.3. Забор может выглядеть как сильный ритм

Забор — частый ложный позитив.

Митигация:

```text
- fence mask must override rhythm;
- anything below facade/base plane or outside object_visible is negative_rhythm;
- high repetition alone is not enough.
```

### 17.4. Ошибки фасадного парсера

Если окна найдены неверно, rhythm fitting может снапнуть мусор.

Митигация:

```text
- source diversity required;
- line evidence + segmentation evidence + mask overlap;
- robust fitting with outlier rejection;
- debug overlays mandatory.
```

---

## 18. Связь с архитектурной библиотекой элементов

`RhythmGraph` не заменяет библиотеку архитектурных элементов. Он делает её полезной.

```text
FacadeElement detection говорит: “тут похоже окно”.
RhythmGraph говорит: “это окно является частью второго ряда и третьего bay”.
Architecture grammar говорит: “рисуем его как чистый SVG-примитив с такими правилами”.
```

Пример:

```text
raw detected window bbox:
  x=413, y=287, w=31, h=74, confidence=0.62

rhythm slot:
  plane=front_facade, row=2, bay=3, expected bbox after projection

renderer:
  draw window_arch or window_rect depending on semantic type and style family
```

Без ритма библиотека элементов будет просто набором красивых правил. С ритмом она становится инструментом восстановления фасадной структуры.

---

## 19. Где ритм помогает, а где нет

### Помогает сильно

```text
- фасады с рядами окон;
- неоклассическая архитектура;
- здания с карнизами и пилястрами;
- частичная окклюзия деревьями;
- шумные edge/DeepLSD линии;
- выбор между minimal/balanced/detailed SVG.
```

### Помогает умеренно

```text
- современные нерегулярные фасады;
- скульптуры и памятники;
- здания в очень сильной перспективе;
- объекты с малым количеством повторов.
```

### Может вредить

```text
- если BuildingShell ошибочный;
- если PlaneGraph неверно назначил фасадную плоскость;
- если забор/дерево приняты за объект;
- если режим completion слишком агрессивный.
```

Поэтому порядок внедрения важен:

```text
сначала BuildingShell и PlaneGraph,
затем RhythmGraph,
только потом completion и финальный SVG.
```

---

## 20. Полезные инструменты и ссылки для реализации

### Геометрия и перспектива

- OpenCV geometric transformations / `warpPerspective`:
  https://opencv24-python-tutorials.readthedocs.io/en/latest/py_tutorials/py_imgproc/py_geometric_transformations/py_geometric_transformations.html

- OpenCV Hough Line Transform:
  https://opencv24-python-tutorials.readthedocs.io/en/latest/py_tutorials/py_imgproc/py_houghlines/py_houghlines.html

### Robust fitting / clustering

- scikit-learn `RANSACRegressor`:
  https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.RANSACRegressor.html

- scikit-learn `DBSCAN`:
  https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html

- SciPy Hungarian assignment / `linear_sum_assignment`:
  https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.linear_sum_assignment.html

### Geometry operations

- Shapely manual:
  https://shapely.readthedocs.io/en/2.1.1/manual.html

- Shapely `simplify`:
  https://shapely.readthedocs.io/en/2.1.2/reference/shapely.simplify.html

### SVG rendering / optimization

- svgwrite:
  https://svgwrite.readthedocs.io/

- SVGO:
  https://github.com/svg/svgo

---

## 21. Практическая рекомендация агенту

Не начинать с полной автоматической реконструкции. Сделать маленький, проверяемый спринт:

```text
Sprint: RhythmGraph MVP for audit_1527
```

Scope:

```text
1. взять уже существующие artifacts: shell/masks/elements/MLSD/DeepLSD;
2. выбрать одну основную фасадную плоскость;
3. построить horizontal floor/cornice rhythm;
4. построить window row rhythm, если хватает detected elements;
5. сделать snap/merge для линий карниза и окон;
6. вывести rhythm_overlay + rhythm-only SVG;
7. сравнить с текущим preview по line clutter и recognizability.
```

Не делать в этом спринте:

```text
- новую diffusion модель;
- новый фасадный датасет;
- полное восстановление всех окон;
- агрессивный inpainting;
- прямую векторизацию edge_map в финал.
```

Критерий успеха:

```text
даже если финальная открытка ещё не идеальна,
после RhythmGraph линии должны стать более выровненными,
дубликаты должны уменьшиться,
а скрытые деревьями участки должны обрабатываться как gaps,
а не как форма здания.
```

---

## 22. Итог

Идея архитектурного ритма не просто полезна — она закрывает ключевой разрыв между “много хороших intermediate maps” и “один качественный SVG”.

Ритм следует внедрить как `RhythmGraph`:

```text
PlaneGraph задаёт перспективную систему координат.
RhythmGraph находит регулярности внутри фасадных плоскостей.
FeatureGraph снапится к этим регулярностям.
CompletionGraph использует ритм для интерполяции через occluders.
PrimitiveScene рендерит чистую открытку, а не edge-map.
```

Это должно стать обязательным этапом recovery plan после стабилизации `BuildingShell` и `PlaneGraph`.
