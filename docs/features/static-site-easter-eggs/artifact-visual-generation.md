# Визуальный контракт и промпт для новых артефактов

> **Статус:** канонический production-intent контракт для подготовки следующих
> изображений; сам референс «Янтарный космонавт» остаётся noindex prototype и не
> разрешён к production до подтверждения provenance/derivative rights.
> **Дата консолидации:** 2026-07-27.

## 1. Найденный отработанный артефакт

До составления промпта найден и проверен существующий specimen
**«Янтарный космонавт»**:

- runtime component:
  [`site/src/components/listings/AmberRailArtifact.astro`](../../../site/src/components/listings/AmberRailArtifact.astro);
- collection rendering:
  [`site/src/components/artifacts/ArtifactCollectionProgress.astro`](../../../site/src/components/artifacts/ArtifactCollectionProgress.astro);
- transparent runtime derivatives:
  `site/public/assets/gamification/amber-cosmonaut-{1x,2x,3x}.webp`;
- историческая product/motion acceptance:
  [amber-artifact-easter-egg.md](../static-site-pages/amber-artifact-easter-egg.md);
- source references found in the working repository:
  `amber-cosmonavt (2).png`, SHA-256
  `aa9f4fa9064a58b7c85a54b76e2481c0e41d2852565bbb356b8a5093080431eb`,
  and `amber-cosmonavt (3).png`, SHA-256
  `17821f70d1e9dc3cdf5acacc0f44b9a1bd023b72ba868e71aa560cd5a438dede`.

The source PNGs are untracked and have no committed license/provenance record.
The derived WebPs are therefore evidence of the visual direction, not
production-safe source material.

## 2. Что именно переносим в серию

The accepted object is not a round event medallion, card or banner. It is a
small, portrait-oriented **faceted regional token**:

- irregular shield/cabochon silhouette, approximately `223:288` (`0.774`);
- complete object inside the canvas, never cropped or stretched;
- translucent honey/golden material, deep warm rim and bevelled facets;
- one immediately recognizable central subject, rendered as relief/inclusion
  rather than a pasted photo;
- strong readability at the actual `74×96 CSS px` size;
- transparent background with clean alpha.

The visual system deliberately has separate layers:

1. **generated base object:** transparent token only;
2. **CSS staging:** halo, lower ring and, if explicitly allowed by the state
   contract, finite rays/glint;
3. **UI state:** focus, `Найден`, progress and dialog.

Do not bake halo, rays, floor ellipse, shadow, text or UI into the raster. That
would make the family inconsistent, prevent accessible/reduced-motion variants
and create rectangular glow artefacts.

## 3. Input contract

For each new artifact attach `1–4` source images and fill:

```text
ARTIFACT_NAME:
COLLECTION:
REGIONAL_CONNECTION:
PRIMARY_SOURCE_IMAGE:
SECONDARY_SOURCE_IMAGES:
MUST_PRESERVE:
MUST_NOT_INVENT:
ACCENT_PALETTE:
RIGHTS/PROVENANCE:
FACT_SOURCES:
```

- `PRIMARY_SOURCE_IMAGE` defines identity and silhouette.
- Secondary images may clarify another angle, material or a small factual
  detail. They are not permission to make a collage.
- `MUST_PRESERVE` lists identity-bearing geometry: outline, proportions,
  characteristic openings, ornament, pose or landmark profile.
- `MUST_NOT_INVENT` lists unknown/unsafe details, logos, inscriptions and
  reconstructions.
- No generation begins while rights/provenance or the exact subject is
  ambiguous. A model must ask for clarification instead of choosing a
  plausible but false regional symbol.

## 4. Master-output contract

- one isolated object, portrait canvas, minimum `1024 px` on the long edge;
- PNG with genuine alpha; white-background output is accepted only as an
  explicit fallback followed by deterministic, reviewed cutout;
- normalized final crop at `223:288`; at 3x leave a consistent `4 px` safe
  transparent inset around non-transparent pixels;
- centered visual mass and stable baseline across the collection;
- same apparent volume, camera distance, upper-left key light, warm inner
  transmission and edge depth;
- no lettering, numerals, logo, watermark, QR, frame, platform UI or
  photorealistic scene;
- no human face or anatomy invented from an unclear source;
- no checkerboard baked into the pixels, white fringe, clipped glow or
  semi-transparent rectangle.

The master is processed reproducibly into:

| Runtime file | Pixels | CSS display |
|---|---:|---:|
| `*-1x.webp` | `74×96` | `74×96` |
| `*-2x.webp` | `149×192` | `74×96` |
| `*-3x.webp` | `223×288` | `74×96` |

All three outputs use the same crop and `object-fit: contain`; no anisotropic
scaling is permitted.

## 5. Готовый промпт для image LLM

Use the following prompt only after the input block and source images are
attached.

```text
Ты создаёшь ОДИН унифицированный коллекционный артефакт для интерфейса
KenigEvents. Вложенные изображения — фактические визуальные источники, а не
свободный moodboard.

Данные артефакта:
- публичное название: {{ARTIFACT_NAME}}
- коллекция: {{COLLECTION}}
- связь с Калининградской областью: {{REGIONAL_CONNECTION}}
- главное исходное изображение: {{PRIMARY_SOURCE_IMAGE}}
- дополнительные исходные изображения: {{SECONDARY_SOURCE_IMAGES}}
- обязательно сохранить: {{MUST_PRESERVE}}
- нельзя выдумывать: {{MUST_NOT_INVENT}}
- акцентная палитра: {{ACCENT_PALETTE}}

Задача:
Преобразуй предмет с исходных изображений в один вертикальный коллекционный
токен той же визуальной семьи, что референс «Янтарный космонавт»: компактный
иррегулярный огранённый талисман/кабошон примерно в пропорции 223:288,
полупрозрачный тёплый материал, глубокий многослойный кант и фаски, мягкое
внутреннее свечение, крупные читаемые блики и один узнаваемый центральный
предмет как рельеф или включение внутри материала.

Сохрани фактический силуэт, пропорции и отличительные признаки предмета по
главному источнику. Дополнительные изображения используй только для уточнения
достоверных деталей. Не делай коллаж, не объединяй разные ракурсы в новый
объект и не дорисовывай неизвестные элементы. Если источники противоречат друг
другу или предмет нельзя уверенно опознать, остановись и перечисли, что нужно
уточнить, вместо генерации.

Композиция:
- ровно один целый объект, по центру, без обрезания;
- почти фронтальный предмет с очень небольшим объёмным поворотом;
- устойчивый нижний край и одинаковый визуальный масштаб с другими токенами;
- распознаваемость центрального образа при уменьшении до 74×96 px;
- чистый прозрачный фон PNG.

Важно: сгенерируй ТОЛЬКО базовый объект. Не добавляй фон, сцену, пьедестал,
овальное кольцо снизу, внешний halo, лучи, искры, частицы, анимационный след,
тень от интерфейса, карточку, рамку, текст, подпись, цифры, логотип, watermark
или UI. Световой halo, кольцо, glint и состояние «Найден» добавляются отдельно
в CSS.

Материал и свет:
- тёплая медово-золотая прозрачность с допустимым акцентом
  {{ACCENT_PALETTE}};
- тёмная янтарно-коричневая глубина по краям, не чёрная пластиковая рамка;
- ясные крупные фаски без мелкого визуального шума;
- мягкий ключевой свет сверху-слева и внутренний свет, без пересвета;
- предмет не должен выглядеть фотографией, плоской наклейкой, монетой,
  эмодзи, пластиковым бейджем или логотипом события.

Технический результат:
- portrait master, длинная сторона не менее 1024 px;
- настоящий alpha channel, прозрачные углы и чистый край без белой каймы;
- внешний силуэт полностью помещается в canvas с небольшим равномерным
  прозрачным запасом;
- без baked checkerboard и без полупрозрачного прямоугольника.

Перед финалом проверь и кратко сообщи:
1) какие признаки источника сохранены;
2) какие детали сознательно не были выдуманы;
3) что фон действительно прозрачен;
4) что объект остаётся узнаваемым в thumbnail 74×96 px.
```

### Negative prompt / запреты

```text
multiple objects, collage, landscape scene, room, pedestal, card, banner,
round event badge, coin, sticker, emoji, logo, text, letters, numbers, QR,
watermark, UI, baked halo, floor ring, rays, particles, sparkle field,
rectangular glow, checkerboard background, white fringe, cropped object,
extreme perspective, fisheye, photorealistic product shot, black plastic frame,
tiny noisy facets, invented architecture, invented inscription, invented face
```

## 6. Acceptance checklist

An artifact is accepted only when all checks pass:

1. **Identity:** a reviewer can match it to the primary source without relying
   on the name.
2. **Regional truth:** story and imagery are backed by approved sources; no
   generated landmark, inscription or anatomy is treated as fact.
3. **Family:** silhouette, material, camera, light and scale read as the same
   series as the worked token without copying its astronaut subject.
4. **Thumbnail:** the subject is recognizable at `74×96`, not only in the
   master.
5. **Alpha:** transparent corners, no white fringe/rectangle; object is not
   clipped.
6. **Layering:** raster contains no halo/ring/rays/text/UI.
7. **Derivatives:** `1x/2x/3x` share the exact crop, aspect and color intent.
8. **Accessibility:** public name and factual alt text are stored as metadata,
   not drawn into the image. Before discovery, neither visible nor accessible
   copy reveals the name.
9. **Rights:** source, generator/model/version, prompt, date, operator,
   license/consent and derivative hashes are recorded.
10. **Runtime:** no layout shift; reduced-motion is fully useful; low-end
    Android scroll/tap/FPS is manually accepted.

## 7. Motion boundary

This image prompt does not create motion. Production follows
[measurement-and-state-contract.md](measurement-and-state-contract.md):
unfound is static; a hint may use one finite halo sequence; find has one short
confirmation; found echo is static. The historical infinite float/glint in the
isolated amber prototype is evidence to evaluate, not the production default.
