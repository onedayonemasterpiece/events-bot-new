Task spec для агента: multi-guide ControlNet line-art workflow
0. Контекст задачи

Нужно доработать countur_svg_generator / contour_svg_generator в проекте events-bot-new, чтобы из одной исходной фотографии архитектурного объекта получать несколько качественных raster line-art кандидатов.

Целевой стиль проекта: узнаваемый архитектурный postcard / brand line-art — крупные чистые линии, обобщённая геометрия, без листвы, текстур, теней, брусчатки, случайного edge-noise; preview может быть на бордовом фоне с тёплыми белыми линиями, но основной технический output лучше держать как black lines on white background. Такой целевой стиль уже описан в проектном контексте: результат должен быть не фотореализмом и не прямым edge-map, а узнаваемым графическим представлением архитектуры.

Сейчас нужно не финализировать SVG, а добиться хорошей чистой line-art картинки, из которой потом уже проще делать SVG.

1. Главная идея PR

Сделать новый экспериментальный workflow:

source photo
→ object crop / object mask / optional occluder mask
→ guide bank: несколько edge/line/mask вариантов
→ role separation: silhouette / structure / detail / texture-noise
→ composite guides
→ ControlNet generation matrix
→ line-only postprocess
→ contact sheet + report

Не должно быть одного единственного edge_map, который сразу идёт в один ControlNet-прогон.

Нужно получить набор кандидатов и визуально сравнить:

какой guide + какая model branch → лучший clean line-art
2. Основной концептуальный фикс
2.1 Не использовать photo-derived init в новой experimental branch

В новой ветке генерация не должна работать так:

source photo crop → img2img init
edge / mask → control image

Нужный режим:

prepared guide image → generation input
prepared guide image → control image

или:

prepared guide image → control-only ControlNet

То есть если используется StableDiffusionControlNetImg2ImgPipeline, то:

image = prepared_line_map
control_image = prepared_line_map

а не:

image = source_photo_crop
control_image = prepared_line_map

Это важно, потому что текущая проблема neural branch описана именно как переход в photo-like output: ветка должна быть не source photo + ControlNet edge condition → photo-like output, а edge_map / cleaned line map → line-to-line neural simplification → strict line-only output.

3. Что именно нужно реализовать
3.1 Новый режим запуска

Добавить experimental режим:

generation_mode: guide_only_experiments

или аналогичный флаг:

line_art_experiments:
  enabled: true
  use_source_photo_as_init: false

Default для этой ветки:

use_source_photo_as_init: false

Фото можно оставить только как отдельную diagnostic branch:

F1_photo_assisted_low_weight

но не как default.

4. Guide bank: набор масок / guide-изображений
4.1 Нужно строить несколько guide variants

Из одной фотографии нужно автоматически получать банк guide-изображений.

Минимальный набор:

G1_edge_raw
G2_edge_binarized
G3_edge_thickened
G4_edge_cleaned
G5_edge_inside_object
G6_edge_minus_occluders
G7_silhouette_outline
G8_detail_edge
G9_structure_edge
G10_fused_guide

Если всё сразу долго, то минимальный practical scope:

G1_edge_raw
G3_edge_thickened
G4_edge_cleaned
G5_edge_inside_object
G7_silhouette_outline
G10_fused_guide
4.2 Назначение каждого guide
G1_edge_raw

Сырой edge-map.

Нужен как baseline, чтобы видеть, что вообще извлекается из изображения.

G2_edge_binarized

Нормализованный бинарный вариант.

Требования:

black lines on white background
или
white lines on black background в debug,
но canonical для генерации лучше black-on-white
G3_edge_thickened

Слегка утолщённые линии.

Цель: сделать guide более читаемым для ControlNet.
Для тонких и рваных линий это часто критично.

Параметры:

dilation: 1–3 px
optional close: 1–2 px
G4_edge_cleaned

Версия после удаления мелкого мусора.

Минимальная очистка:

remove tiny connected components
remove isolated short fragments
optional morphology open/close
G5_edge_inside_object

Оставить edge-линии только внутри object_mask.

Цель: убрать фон, деревья, небо, землю и мусор за пределами объекта.

G6_edge_minus_occluders

Если есть occluder_mask, удалить линии внутри окклюдеров.

Цель: убрать дерево / листву / помехи, если они выделены.

G7_silhouette_outline

Только внешний контур объекта.

Это очень важный guide, потому что он фиксирует узнаваемую общую форму.

Для башни сюда должны попасть:

купол
верхняя башенка
внешний цилиндр
галерея / балкон
нижний контур объекта
G8_detail_edge

Guide для деталей.

Сюда должны попасть:

окна
арки
перила
проёмы
декоративные крупные элементы

Но не должна попадать мелкая кирпичная / каменная фактура.

G9_structure_edge

Guide для крупной структуры.

Сюда должны попасть:

горизонтальные пояса
крупные дуги
балкон / галерея
границы ярусов
крупные вертикали
основные контуры купола
G10_fused_guide

Главный сбалансированный guide.

Должен быть не просто OR всех масок, а осмысленная сборка:

silhouette: high priority
structure: medium/high priority
detail: selective low/medium priority
texture-noise: suppress
5. Важное правило fusion

Не делать тупо:

fused = G1 OR G2 OR G3 OR G4 OR G8

Это почти гарантированно вернёт весь шум.

Нужно сделать role-based fusion:

CG = silhouette
   + major structure
   + selected details
   - tiny components
   - texture-like dense regions
   - occluder regions

То есть объединение должно учитывать роль линии.

6. Composite guides

Кроме raw G*, нужно построить 3–4 composite-guide.

6.1 Обязательные composite guides
CG1_silhouette_plus_structure
CG2_structure_plus_details
CG3_fused_balanced
CG4_minimal_clean
6.2 Что означает каждый composite
CG1_silhouette_plus_structure
G7_silhouette_outline + G9_structure_edge

Цель: максимально сохранить объект и крупную архитектуру, без мусора.

CG2_structure_plus_details
G9_structure_edge + filtered(G8_detail_edge)

Цель: дать модели окна / арки / детали, но не перегрузить её шумом.

CG3_fused_balanced

Главный balanced input.

silhouette strong
structure medium/high
detail selective
noise suppressed
CG4_minimal_clean

Минималистичный input.

только силуэт + самые крупные архитектурные линии

Цель: проверить, может ли ControlNet сам сделать красивую clean line-art картинку без перегруза деталями.

7. Специально для башни

На башне видно, что основная проблема — не только контуры, а texture-noise на цилиндрическом стволе.

Нужно явно добавить tower-specific логику:

7.1 Сохранять
outer silhouette
dome outline
lantern room / верхняя башенка
gallery / balcony outline
large arches
major horizontal rings
large window openings
7.2 Подавлять
мелкую каменную / кирпичную фактуру
шум на цилиндре
случайные короткие линии
плотные texture regions
7.3 Не делать MLSD основным для башни

Для круглой башни:

main: Lineart
second: Scribble
optional: Canny
low priority: MLSD

MLSD можно оставить как эксперимент, но не как главный путь. Он полезнее для прямоугольных фасадов, чем для куполов, цилиндров и округлых галерей.

8. Generation branches

Нужно запускать не одну ветку, а матрицу.

8.1 Минимальный набор веток
E1_lineart_control_only
E2_lineart_line_init
E3_scribble_control_only
E4_scribble_line_init

Опционально:

E5_canny_control_only
E6_mlsd_control_only
E7_photo_assisted_low_weight
8.2 control_only

Использовать StableDiffusionControlNetPipeline.

Схема:

control_image = prepared guide
no source photo init

Цель: проверить, может ли модель построить line-art строго из guide.

8.3 line_init

Использовать StableDiffusionControlNetImg2ImgPipeline.

Но важно:

image = prepared_guide_image
control_image = prepared_guide_image

Не использовать source photo как image.

8.4 photo_assisted_low_weight

Опционально, только diagnostic.

source photo may be used only in clearly labelled experimental branch

Название кандидата должно явно показывать риск:

F1_photo_assisted_low_weight

Такие кандидаты не должны быть final-eligible.

9. Какие guide прогонять через generation branches

Минимальный набор для прогона:

G3_edge_thickened
G4_edge_cleaned
CG1_silhouette_plus_structure
CG3_fused_balanced
CG4_minimal_clean

Если позволяет время / GPU:

все G* и CG*
10. Prompt policy
10.1 Основной prompt

Использовать prompt не “сделай красивую картинку из фото”, а “преврати structural line map в clean line-art”.

You are given a structural architectural line map, not a photo.
Transform it into a cleaner, bolder, simplified architectural contour drawing.
Preserve the same object identity, silhouette, perspective, dome, roofline, gallery, arches, windows, major horizontal rings and base.
Use fewer and larger confident strokes.
Merge redundant close parallel lines.
Remove foliage, trees, background, wall texture, brick texture, shadows and random edge noise.
Output must be strictly black line art on a plain white background.
No color, no shading, no realistic rendering, no filled surfaces, no paper texture, no sketch hatching.

Для обычных зданий можно добавлять:

preserve main corner, facade planes, cornices, pediments, pilasters, balcony, base and stairs
10.2 Negative prompt
photo, photorealistic, realistic render, colored facade, blue sky, trees, foliage, fence, road, pavement, people, cars, shadows, gradients, wall texture, brick texture, dense tiny details, scribble, crosshatching, blurry, watermark, text, different building, extra floors, extra wings, distorted architecture, decorative sparkles
10.3 Не просить бордовый фон как основной output

Основной output:

black lines on pure white background

Бордовый preview делать потом программно:

line_mask → warm white strokes on solid burgundy background

Такой подход уже соответствует ранее сформулированному требованию: основной режим лучше держать как black-on-white line mask, а затем отдельным deterministic palette layer делать transparent strokes или burgundy preview.

11. Параметры grid search

Не делать огромную сетку сразу. Нужна разумная матрица.

11.1 Для line_init
strength: [0.45, 0.60, 0.75]
control_scale: [0.65, 0.85, 1.05]
guidance_scale: [7.0, 9.0]
steps: [28, 36]
seeds: [42]
11.2 Для control_only
control_scale: [0.75, 0.95, 1.10]
guidance_scale: [7.0, 9.0]
steps: [28, 36]
seeds: [42]
11.3 Правила интерпретации

Если результат слишком повторяет шум:

уменьшить control_scale
использовать более clean/minimal guide
использовать Scribble вместо Lineart

Если результат теряет объект:

увеличить control_scale
дать CG1_silhouette_plus_structure
дать G7_silhouette_outline в fusion

Если модель выдумывает архитектуру:

увеличить control_scale
уменьшить guidance_scale
использовать line_init вместо control_only
12. Postprocess каждого результата

Для каждого candidate нужно сохранять не только raw output.

12.1 Обязательные output artifacts
candidate_raw.png
candidate_line_mask.png
candidate_thresholded.png
candidate_cleaned.png
candidate_burgundy_preview.png
candidate_report.json

SVG пока можно не делать как обязательный артефакт. Если уже есть лёгкий trace — можно сохранять как diagnostic:

candidate_vector_preview.svg

но не считать это финальным SVG.

12.2 Line-only normalization

Для каждого raw output:

convert to grayscale
detect/invert if needed
threshold
remove small connected components
remove huge filled areas if any
normalize to black lines on white background
save candidate_line_mask.png
13. Hard gates / метрики

Нужно добавить отчётные метрики, даже если они пока простые.

candidate_report.json:

{
  "candidate_id": "CG3_fused_balanced__E1_lineart_control_only__seed42",
  "guide_id": "CG3_fused_balanced",
  "branch": "E1_lineart_control_only",
  "model": "control_v11p_sd15_lineart",
  "seed": 42,
  "source_photo_used_as_init": false,
  "guide_used_as_init": true,
  "control_only": true,
  "raw_colorfulness": 0.0,
  "line_density": 0.0,
  "small_component_count": 0,
  "small_component_ratio": 0.0,
  "large_filled_region_ratio": 0.0,
  "object_line_overlap": 0.0,
  "occluder_line_overlap": 0.0,
  "line_only_gate_passed": true,
  "rejection_reason": null
}
13.1 Reject candidate если
output цветной / photo-like
фон грязный или градиентный
есть большие залитые области
слишком много tiny components
слишком высокая line density
много линий в occluder/background zone
объект потерял силуэт
видна текстурная каша вместо архитектурных линий
модель добавила text/watermark/sparkle/people/cars
14. Contact sheet

Нужен обязательный contact sheet.

14.1 Формат

Для каждой строки:

guide input | branch | raw output | line_mask | cleaned | burgundy preview

Подпись:

CG3_fused_balanced + E1_lineart_control_only + seed42
14.2 Отдельные sheets

Сохранить:

contact_sheet_all.png
contact_sheet_passed.png
contact_sheet_rejected.png

Не скрывать плохие результаты. Они нужны для аудита.

15. Summary report

Сохранить:

line_art_experiment_report.md
line_art_experiment_report.json

В отчёте:

1. какие guide variants были построены;
2. какие composite guides были построены;
3. какие generation branches были запущены;
4. какие candidates прошли gates;
5. какие candidates rejected и почему;
6. какая комбинация guide + branch визуально лучшая;
7. что стоит попробовать дальше.
16. Output directory structure

Предлагаемая структура:

outputs/<run_id>/
  source/
    source_crop.png
    object_mask.png
    occluder_mask.png

  guides/
    G1_edge_raw.png
    G2_edge_binarized.png
    G3_edge_thickened.png
    G4_edge_cleaned.png
    G5_edge_inside_object.png
    G6_edge_minus_occluders.png
    G7_silhouette_outline.png
    G8_detail_edge.png
    G9_structure_edge.png
    G10_fused_guide.png

  composite_guides/
    CG1_silhouette_plus_structure.png
    CG2_structure_plus_details.png
    CG3_fused_balanced.png
    CG4_minimal_clean.png

  candidates/
    <candidate_id>/
      input_guide.png
      candidate_raw.png
      candidate_line_mask.png
      candidate_thresholded.png
      candidate_cleaned.png
      candidate_burgundy_preview.png
      candidate_report.json

  contact_sheets/
    contact_sheet_all.png
    contact_sheet_passed.png
    contact_sheet_rejected.png

  reports/
    line_art_experiment_report.md
    line_art_experiment_report.json
17. Что НЕ делать в этом PR

Не делать сейчас:

full PrimitiveScene
RhythmGraph production implementation
финальную интеллектуальную SVG reconstruction
ручное восстановление окон/арок/куполов
сложный deterministic renderer как основной путь

Это можно вернуть позже.

Сейчас scope:

single photo
→ multi-guide bank
→ composite guides
→ ControlNet line-art candidates
→ cleanup
→ audit contact sheet
18. Но не ломать существующую архитектуру

Не удалять neural branch.

Не делать вывод, что neural branch бесполезна.

Старый плохой результат не доказывает, что neural branch плохая. Он показывает, что режим был неправильный: вместо photo → photo-like ControlNet img2img нужно перевести её в edge_map / cleaned line map → line-to-line neural simplification → strict line-only normalization. Это требование уже зафиксировано в проектном handoff.

19. Model priority

Для этого PR использовать:

Main
ControlNet Lineart
Secondary
ControlNet Scribble
Optional
ControlNet Canny
ControlNet MLSD

Для башни:

Lineart > Scribble > Canny > MLSD

Для прямоугольных фасадов:

Lineart > Scribble ≈ MLSD > Canny
20. Acceptance criteria

PR считается успешным, если:

AC1. Multi-guide bank

За один запуск из одной фотографии создаются минимум:

G1_edge_raw
G3_edge_thickened
G4_edge_cleaned
G7_silhouette_outline
G10_fused_guide
CG1_silhouette_plus_structure
CG3_fused_balanced
AC2. No photo-init in default experimental branch

Для всех default candidates:

"source_photo_used_as_init": false

Допустимо только для явно маркированной ветки:

F1_photo_assisted_low_weight
AC3. ControlNet branches

Запускаются минимум:

E1_lineart_control_only
E2_lineart_line_init
E3_scribble_control_only
E4_scribble_line_init
AC4. Candidate artifacts

Для каждого кандидата есть:

input_guide.png
candidate_raw.png
candidate_line_mask.png
candidate_cleaned.png
candidate_burgundy_preview.png
candidate_report.json
AC5. Contact sheet

Есть:

contact_sheet_all.png

Желательно также:

contact_sheet_passed.png
contact_sheet_rejected.png
AC6. Main output is line-based

Основной output нормализуется в:

black lines on white background

Бордовый фон — только preview layer.

AC7. Rejected candidates visible

Плохие кандидаты не скрываются. Они должны попадать в contact_sheet_all.png и иметь rejection_reason.

AC8. Tower-specific sanity

На башне хотя бы один candidate должен:

сохранять общий силуэт башни
сохранять купол / верхнюю башенку
сохранять галерею / балкон
сохранять крупные арки
подавлять часть текстурного шума на цилиндре
AC9. No final SVG claim

PR не должен утверждать, что решает финальную SVG-реконструкцию.

Цель PR:

получить хороший raster line-art candidate

SVG — следующий этап.

21. Минимальный план реализации
Step 1

Добавить config:

line_art_experiments:
  enabled: true
  mode: guide_only_experiments
  use_source_photo_as_init: false
  output_size: [1024, 1024]
  seeds: [42]
  guide_variants:
    - edge_raw
    - edge_thickened
    - edge_cleaned
    - silhouette_outline
    - fused_guide
  composite_guides:
    - silhouette_plus_structure
    - fused_balanced
  branches:
    - lineart_control_only
    - lineart_line_init
    - scribble_control_only
    - scribble_line_init
Step 2

Реализовать модуль:

contour_svg/guide_bank.py

Функции:

build_guide_bank(...)
build_composite_guides(...)
normalize_guide(...)
save_guide_debug_artifacts(...)
Step 3

Реализовать модуль:

contour_svg/line_art_experiments.py

Функции:

run_line_art_experiment_matrix(...)
run_controlnet_control_only(...)
run_controlnet_line_init(...)
Step 4

Реализовать postprocess:

contour_svg/line_art_postprocess.py

Функции:

extract_line_mask(...)
clean_line_mask(...)
render_burgundy_preview(...)
measure_line_candidate(...)
Step 5

Реализовать отчётность:

contour_svg/line_art_report.py

Функции:

build_contact_sheet(...)
write_experiment_report(...)
22. Короткая формулировка задачи для commit / PR
Add guide-only multi-mask ControlNet line-art experiment pipeline.

This PR adds a new experimental workflow that builds multiple guide masks from a source photo, fuses them into role-aware composite guides, runs Lineart/Scribble ControlNet branches without using source photo as default img2img init, normalizes outputs into black-on-white line masks, and saves contact sheets/reports for visual audit.

The goal is not final SVG generation yet, but reliable raster line-art candidates suitable for later vectorization.
23. Самый важный practical смысл

Нужно проверить гипотезу:

не одна маска → один результат,
а несколько guide roles → несколько генеративных кандидатов → лучший line-art выбирается по аудиту.

Особенно для башни:

силуэт и крупные дуги должны быть якорем,
детали должны добавляться дозированно,
текстура камня должна подавляться.

Это и есть главный workflow, который надо реализовать сейчас.