# Typed briefing: face bbox contract

Этот документ задаёт будущий контракт между Smart Update, static exporter и
desktop mosaic renderer. Текущий lab уже принимает
`image_assets[].face_boxes: ImageBox[]`; producer/storage ещё не считаются
реализованными.

## Ownership and identity

Face metadata принадлежит конкретному исходному asset, а не событию и не
коллажу целиком. До promotion/reordering она привязывается к стабильному
`source_order` и content-addressed `asset_key`.

Рекомендуемый producer envelope:

```json
{
  "version": "face-bboxes.v1",
  "status": "complete",
  "asset_key": "sha256:…",
  "source_width": 2560,
  "source_height": 1541,
  "coordinate_space": "normalized_display_upright",
  "boxes": [
    { "x": 0.28, "y": 0.07, "w": 0.19, "h": 0.40, "confidence": 0.98 }
  ],
  "detector": { "name": "…", "version": "…" },
  "analyzed_at": "2026-07-17T00:00:00Z"
}
```

`status: complete` с `boxes: []` означает «анализ завершён, лиц нет».
Отсутствующий envelope и статусы `error`/`stale` означают unknown; для них
допустим `reason_code`, но renderer не делает browser-side face inference.
Metadata принимается только при совпадении asset key и upright dimensions.
Thumbnail/derivative может переиспользовать bbox исходника лишь если явно
помечен geometry-preserving: тот же upright aspect ratio, без crop. Иначе
derivative анализируется отдельно либо bbox игнорируется.

Нельзя хранить identity, embeddings, возраст, пол, эмоции или другие
демографические выводы. Нужны только геометрия, confidence и provenance
детектора.

## Coordinates and renderer mapping

Все `x/y/w/h` нормализованы в `[0,1]` относительно upright source raster.
Для source `W×H` внутри panel viewport `Pw×Ph`:

```text
cover scale   s = max(Pw/W, Ph/H)
contain scale s = min(Pw/W, Ph/H)
Rw = W*s, Rh = H*s
ox = (Pw-Rw) * focusX
oy = (Ph-Rh) * focusY
```

Renderer добавляет к bbox примерно `12%` его ширины/высоты, ограничивая
padding диапазоном `0.012…0.04` normalized units, затем переводит координаты в
viewport своей source panel. Клетка получает `faceShield`, если площадь
пересечения больше одного CSS pixel. Face rule применяется последним:

```text
final tile opacity >= 0.50
```

Для collage bbox никогда не объединяются: каждая панель рассчитывает свой
scale/offset/focus и защищает только собственные клетки.

## Crop safety is a separate gate

Opacity floor не доказывает хороший crop. После projection renderer/exporter
отдельно проверяет, что padded bbox целиком находится в panel viewport. При
небезопасном crop меняется focal point/panel allocation или asset исключается;
`contain` не включается автоматически для всех portrait изображений.

Lab hooks:

- root: `data-face-metadata`, `data-face-box-count`, `data-face-crop-safe`;
- tile: `data-face-shield`, `data-face-shield-source-index`.

## Fallback and acceptance

- `complete + []`: известное отсутствие лиц, обычная mosaic topology;
- absent/error/stale/mismatch: unknown, проверенный текущий crop сохраняется,
  но renderer ничего не «угадывает»;
- copy + face intersection: face floor выигрывает, текст защищает лёгкий paper
  stripe;
- reduced motion: тот же final face floor без stagger;
- delayed accent cell никогда не выбирается из face/text protected cells;
- mobile не получает и не запрашивает desktop narrative raster.

Обязательные тесты producer rollout: empty-complete vs missing, hash/dimension
mismatch, boundary bbox, copy+face priority, независимые collage sources,
portrait/panorama cover mapping, geometry-preserving derivative, crop-safe
diagnostic и отсутствие metadata fallback.
