# Limeglow Motion Audit Debug-07

Рабочий документ Codex. Канонические требования остаются в
`requirements.md`; этот файл фиксирует только аудит текущего Blender-прогона
`debug-07` и выводы из покадрового сравнения с референсом.

## Sources

- Requirements:
  `docs/backlog/features/limeglow/requirements.md`
- Reference video:
  `docs/backlog/features/limeglow/Screen_Recording_20260606_235658_YouTube.mp4`
- Debug render:
  `artifacts/codex/limeglow-blender-debug-07/motion_preview_12fps.mp4`
- Frame-by-frame audit:
  `artifacts/codex/limeglow-blender-debug-07/frame-by-frame-audit/`
- Dense motion metrics:
  `artifacts/codex/limeglow-blender-debug-07/audit-dense/audit_meta.json`
- Reusable grammar library:
  `docs/backlog/features/limeglow/geometry-grammar-library.md`
- Next concrete director plan:
  `docs/backlog/features/limeglow/director-plan-debug08.md`

## Debug-08 v6/v9 Pre-Render Self-Audit

Этот блок фиксирует ошибки текущего подхода до следующего рендера. Он
основан не на пересказе замечаний, а на покадровом просмотре:

- v6 full preview:
  `artifacts/codex/limeglow-blender-debug-08-v6-bgtram/frames/`
- v6 audit sheets:
  `artifacts/codex/limeglow-blender-debug-08-v6-bgtram/audit/`
- v9 smoke:
  `artifacts/codex/limeglow-blender-debug-08-v9-workbench-smoke/`

### D8-1. v9 smoke is invalid as a self-check

`v9-workbench-smoke` на 4 секунды нельзя считать честной проверкой первых
четырёх секунд ролика.

Причина: renderer использует hardcoded beat-окна и camera route до `320+`
кадров, но при smoke `frame_end=60`. Последний camera key принудительно
ставится на `frame_end`, поэтому камера начинает тянуться к финалу внутри
первых секунд. Из-за этого в `page-03` финальный CTA-остров появляется в
раннем smoke, хотя по сценарию он должен жить только в конце.

Вывод: короткий smoke должен работать как "render first N frames of the full
timeline", а не как пересборка всей сцены с `frame_end=N`.

### D8-2. Workbench image fade bug

`v9` показал технический баг: на `frame_0001` виден текст intro, но не видны
оба гида.

Причина в текущем fade-механизме: `fade_image()` вызывает
`key_object_opacity()`, который меняет `mat.diffuse_color.alpha`, но не
keyframe'ит сам `diffuse_color`. В Workbench `color_type=TEXTURE` этот
статический alpha влияет на image-plane во всех кадрах. Если последний fade
ставит alpha в `0`, объект становится невидимым с первого кадра.

Вывод: для Workbench нельзя использовать такой material-alpha fade как
универсальный способ исчезновения. Нужны либо:

- Eevee/renderer path, где node alpha корректно анимируется;
- geometry-based exit: объект уходит за foreground occluder / за край /
  внутрь push-in;
- отдельный проверенный image-plane fade, который не меняет статический
  `diffuse_color` после постановки ключей.

### D8-3. v6 has hard 1-frame flicker

Покадрово найден конкретный грязный участок:

- `frame_0086`: hook-плашка Амалиенау видна;
- `frame_0087`: hook-плашка полностью пропадает;
- `frame_0088`: hook-плашка снова видна.

Это не вопрос вкуса и не "плавность". Это технически грязный один кадр,
который нельзя пропускать в preview.

Вероятная причина: конфликт старых широких visibility windows и точечных
окон/слоёв. Даже после удаления broad `a_*` / `b_*` окон следующий renderer
должен иметь frame-by-frame assertion: если объект должен быть виден в beat,
он не может иметь одиночный выключенный кадр внутри окна.

### D8-4. Echo is not a beat

В v6 echo у первого гида не читается как отдельный эффект. На `page-03` /
`page-04` зритель одновременно получает:

- крупный портрет;
- hook;
- дату/имя;
- оконные блоки;
- полупрозрачные копии.

Эффект echo спрятан под смысловой плашкой и не имеет своего clear beat. В
референсе echo работает иначе: сначала один cutout-человек, затем быстрый
агрессивный split копий, короткий hold, потом следующий смысловой переход.

Вывод: echo должен быть отдельным beat между "гид представлен" и "hook
показан". На этом beat hook/text не должны конкурировать с clone-split.

### D8-5. Amalienau semantic/title layer is confused

`frame_0116` показывает ошибку смыслового слоя:

- кусок гида остаётся слева как хвост предыдущего beat;
- белый объект дома стоит в центре;
- белое `АМАЛИЕНАУ` лежит прямо поверх белого объекта;
- серое `ПРОГУЛКА` висит в фоне;
- имя гида есть, но дата в этот момент не читается.

Итог: `АМАЛИЕНАУ` не считывается как название/район/экскурсия, а выглядит как
оторванная фоновая надпись поверх коллажного объекта.

Вывод: у каждого product beat должен быть явно назначенный тип текста:

- `background semantic word`: дальний план, низкий контраст, не обязан читаться
  как главный текст;
- `scene title`: главный читабельный слой, на тёмной/контрастной подложке;
- `fact tag`: гид/date/time, отдельный readable beat;
- `hook`: вопрос, отдельный readable beat.

Нельзя класть scene title поверх белого визуального объекта без контрастной
системы.

### D8-6. Product read time is too short and overloaded

По v6 невозможно честно запомнить первую экскурсию:

- hook появляется до того, как echo оформлен;
- visual object появляется до того, как hook отработал;
- имя/дата оказываются внизу и частично конкурируют с объектом;
- один beat одновременно пытается быть hook, title, guide, date и visual
  promise.

Для второй экскурсии ошибка похожая:

- фоновая фотография трамвая становится шумным главным полем;
- hook и дата лежат поверх активного фото;
- guide/date слишком мелкие и короткие;
- title/product layer растворяется в фоне.

Вывод: для каждой экскурсии нужны минимум четыре последовательных product
акцента:

1. `guide identity`: крупный гид, без hook-плашки;
2. `hook question`: вопрос с гидом как вторичным визуальным якорем;
3. `visual promise`: объект/фото экскурсии как главный образ;
4. `when/who lock`: имя гида + конкретная дата/время на чистом контрастном
   слое.

### D8-7. Background tram mode needs hierarchy

Вариант с фоновым фото трамвая допустим, но в v6 он не иерархизирован:

- фото слишком активно и контрастно для дальнего плана;
- поверх него лежат hook/date/title, из-за чего текст плохо читается;
- tram image ведёт себя как прямоугольная картинка, а не как дальний
  environment layer.

Вывод: если фото используется как фон, оно должно быть обработано как
`background environment`:

- затемнение/дуотон/чёрно-белая обработка;
- сильная маска/veil;
- текст только на отдельной контрастной зоне;
- foreground guide и mid geometry должны быть главнее фото.

### D8-8. Speakers still need grounding

В v6 стартовые гиды видны крупнее, чем раньше, но они всё ещё не выглядят
уверенно "вышедшими из нижней границы". Нижний край cutout читается как
обрезанный объект, поставленный на холст.

Вывод: для людей нужно не только scale-up, а правило композиции:

- bottom of body must be below frame bottom during human-led beats;
- crop by camera frame is allowed and desirable;
- no floating full cutout unless it is intentionally a card/clone effect.

### D8-9. CTA lockup is not clean

На `page-15` / `page-16` CTA конкурирует с остатками tram-background и не
становится главным финальным акцентом. Гиды собраны в финале, но плашка CTA
зажимает их, а не объединяет дорогой lockup.

Вывод: финал должен быть отдельным clean CTA beat:

- предыдущая экскурсия уже растворена/уведена/закрыта push-in;
- CTA главный;
- два гида крупные и grounded;
- визуальные фрагменты только поддерживают, не спорят;
- фоновые слова `ЭКСКУРСИИ` / `ДАЙДЖЕСТ` могут жить далеко и тихо.

### Required Fixes Before Next Full Render

1. Исправить self-check: короткий smoke не должен менять смысл `frame_end` и
   ломать полный timeline.
2. Запретить текущий Workbench `fade_image()` как механизм исчезновения до
   проверки renderer path.
3. Сделать visibility/opacity audit по всем обязательным объектам: внутри
   visible-window не должно быть одиночных пропаж.
4. Развести echo в отдельный beat без hook/text конкуренции.
5. Развести product beats каждой экскурсии: guide → hook → visual → when/who.
6. Для Amalienau создать контрастный title/fact слой, не писать белое
   `АМАЛИЕНАУ` поверх белого объекта.
7. Для tram background сделать фото дальним обработанным environment, а не
   активным прямоугольником под текстом.
8. Опустить/масштабировать людей так, чтобы в human-led beats они уходили за
   нижний край.
9. Финал собрать как отдельный CTA lockup после очищенного перехода.

## Method Correction

Предыдущий анализ по `0.25s` и по ключевым кадрам был методически неверен.
Для motion-ролика такой sampling теряет появление объектов, easing,
stagger, echo и реальную читаемость продуктовых beat'ов.

Правильный текущий минимум:

- смотреть все кадры текущего рендера;
- смотреть все кадры референса или хотя бы покадрово каждый переходный
  участок;
- отдельно фиксировать, какой смысл считывается в каждом beat'е;
- отдельно фиксировать, где объект входит/выходит и каким способом;
- считать keyframes только контрольными стопами, не доказательством движения.

Сделанная раскладка:

- render: `96` кадров, `12 fps`, `8.0s`, `720x1280`;
- reference: `335` кадров, `27.837 fps`, `12.034s`, `2400x1080`;
- render pages:
  `frame-by-frame-audit/render/page-01.jpg` ... `page-04.jpg`;
- reference pages:
  `frame-by-frame-audit/reference/page-01.jpg` ... `page-14.jpg`.

## Requirements Check

Текущий render соответствует только технической части:

- вертикальный формат `720x1280`;
- локальная отладка до Kaggle;
- Blender может построить перспективную сцену с depth-плоскостями;
- есть два экскурсионных узла и финальный CTA;
- есть debug-гиды и debug-объекты.

Но render не соответствует главным художественным и продуктовым требованиям:

- нет дорогого motion language;
- нет спроектированного маршрута камеры как истории;
- нет убедительного ease-in-out с быстрым разгоном, пролётом и торможением;
- нет рассинхронизированного параллакса как осознанной системы;
- нет достаточно разных обработок картинок/спикеров;
- хук/гид/дата/образ не держатся в читаемом фокусе достаточно долго;
- продуктовая история двух экскурсий не успевает считаться;
- сцены выглядят как техническое перемещение по доске, а не как
  editorial/broadcast opener.

## Reference Cross-Cutting Observations

Референс не является набором равномерных сцен. Он устроен как серия
разных motion-механик на общем тёмном холсте.

## Reference Speaker Treatment

Важное уточнение после дополнительного просмотра полноразмерных кадров:
цветная плашка в референсе не является обязательной подложкой под спикера.
Чаще спикер работает как самостоятельный вырезанный alpha-объект на тёмном
поле, а цветная геометрия существует отдельно.

Наблюдения:

- `f170`: крупный ч/б человек без цветной плашки под ним; есть только тёмное
  поле, большой crop тела и маленький цветной акцент справа снизу;
- `f193`: крупный ч/б портрет, маджента-треугольник находится позади/сбоку,
  но человек не "сидит на карточке";
- `f209`: один крупный человек на переднем плане, второй маленький человек на
  дальнем плане, синяя геометрия формирует глубину и маршрут сцены;
- `f234`: человек крупно, foreground-микрофоны перекрывают нижний край, сзади
  крупная типографика и маджента-поле;
- `f263`: финальный lockup держит несколько людей без индивидуальных цветных
  плашек; людей объединяет тёмное поле и большая белая типографика.

Выводы для Limeglow:

- нельзя превращать каждого гида в "портрет на цветной плашке";
- цветная плашка является одним из приёмов, а не базовым контейнером спикера;
- гид должен часто быть самостоятельным крупным cutout-объектом;
- гид может пересекаться с типографикой, объектом экскурсии, геометрией,
  foreground-тегами;
- подложка допустима, когда она несёт роль mid-plane или композиционной массы,
  но она не должна превращать сцену в набор карточек.

Практические варианты speaker treatment для следующего прохода:

1. `clean-cutout-on-dark`: крупный гид без плашки, только тёмный фон, far
   typography и мелкие tags;
2. `side-geometry`: гид на тёмном поле, сбоку/позади крупный треугольник или
   прямоугольник;
3. `echo-fan`: гид без плашки, clone-копии появляются с задержкой и уходят
   назад по depth;
4. `foreground-overlap`: гид частично перекрыт объектом экскурсии или
   foreground-tag'ом снизу, но не закрыт декоративной карточкой;
5. `final-group`: два гида вместе на общем тёмном поле, без индивидуальных
   карточек, CTA и типографика собирают их в один дайджест.

## Reference Geometry And Ornament Layers

В референсе геометрия не является "полосками для украшения". Это отдельная
система планов с собственной морфологией и движением.

Типы геометрии:

- крупные цветные поля: маджента-прямоугольники, большие треугольники,
  диагональные массы;
- средние формы: синие треугольники, ступенчатые прямоугольники, плашки;
- мелкая UI-геометрия: circles, plus, star, outline-pill, маленькие label tags;
- фоновые типографические массивы: огромные белые слова, обрезанные краями;
- thin-line ornaments: контуры, диагонали, route-like линии.

Как это работает:

- крупные формы дают композиционную массу и направление камеры;
- средние формы помогают прочитать глубину между человеком и фоном;
- мелкие формы имеют собственные задержки/дрейф и делают сцену живой;
- формы часто сдвигаются не синхронно с камерой;
- геометрия может быть предметной метафорой: маршрут, район, архитектурная
  сетка, рельсы, карта, оконная геометрия.

Выводы для Limeglow:

- каждый excursion node должен иметь geometry grammar, а не одну цветную
  плашку;
- геометрия должна работать как один из depth-планов;
- крупные формы могут выходить за края кадра;
- мелкие формы должны иметь controlled micro-motion;
- геометрия не должна конкурировать с гидом и визуальным образом экскурсии;
- для трамвая геометрия может быть рельсами/маршрутными линиями/синими
  диагоналями;
- для Амалиенау геометрия может быть окнами, фасадными вертикалями,
  roofline-линиями, квартальной сеткой.

## Product Visual Priority

Для Limeglow есть два равноправных визуальных магнита:

1. `ГИД`: человек, которому зритель должен поверить и которого нужно запомнить;
2. `ВИЗУАЛЬНЫЙ ОБРАЗ ЭКСКУРСИИ`: объект/место/фрагмент маршрута, который
   хочется увидеть глазами.

Хук и дата важны, но они не могут заменить визуальный интерес. Экскурсия
продаётся не только текстом, а обещанием увидеть что-то конкретное вместе с
конкретным гидом.

Контракт для каждого экскурсионного узла:

- минимум один beat, где гид занимает доминирующее визуальное место;
- минимум один beat, где visual object занимает доминирующее визуальное место;
- минимум один overlap beat, где гид и visual object находятся в одной сцене
  и понятно, что гид "покажет это";
- hook должен усиливать этот visual promise, а не быть случайным вопросом по
  картинке;
- дата/время должны появляться в момент, когда экскурсия уже визуально
  распознана.

Debug-07 нарушает этот контракт:

- первый гид виден, но Амалиенау как экскурсия не имеет сильного
  самостоятельного visual promise;
- дом остаётся обычной фотографией, а не желанным объектом;
- второй гид и трамвай чаще живут рядом, но трамвай перетягивает сцену как
  прямоугольная карточка;
- мороженое занимает отдельный beat, но не усиливает ни гида, ни экскурсию.

## Reference Timeline Observations

### R1. Intro Reveal

Frames примерно `f015-f024` (`0.50-0.83s`):

- маленький герой/объект сначала стоит в большом пустом пространстве;
- маджента-плашка появляется за ним;
- плашка не просто включается, а становится новой опорой композиции;
- вокруг есть маленькие акценты, которые живут на другом темпе.

Вывод для Limeglow: старт с маленьких гидов допустим только если это
осознанный establishing shot на 0.3-0.5s, после которого быстро появляется
крупная человеческая сцена. Нельзя оставлять мелких людей как главный способ
представить гидов.

### R2. Long Collage Hold

Frames примерно `f041-f088` (`1.44-3.13s`):

- одна сложная сцена держится около `1.7s`;
- внутри кадра меняются маленькие теги/плашки/акценты;
- композиция не дергается каждые 0.3s, зритель успевает понять сцену;
- глубина читается за счёт foreground-объектов, людей, плашки и тегов.

Вывод: дорогой темп не равен частой смене смысловых объектов. В каждом
экскурсионном узле нужен hold-beat, где продуктовая связка читается минимум
около `1.2-1.6s` на preview-скорости.

### R3. Card Beat

Frames примерно `f089-f116` (`3.16-4.13s`):

- переход к яркой карточке не выглядит как исчезновение предыдущей сцены;
- крупная розовая плашка, герой и типографика собираются как новый остров;
- человек обрезан кадром и не выглядит "наклеенным на пустоту";
- вокруг есть мелкие графические акценты, но они не спорят с героем.

Вывод: карточные сцены Limeglow должны быть самостоятельными beat'ами, а не
промежуточными позициями камеры.

### R4. Object/Foreground Beat

Frames примерно `f117-f143` (`4.17-5.10s`):

- новый герой с крупным foreground-объектом;
- объект в руках работает как фокус, а не как случайная перебивка;
- маленькие иконки/теги плавают с отдельной микродинамикой;
- розовая плашка позади держит силуэт.

Вывод: foreground-объект должен быть связан со смыслом сцены. Если предмет
не помогает понять экскурсию, его нельзя оставлять только как wipe.

### R5. Triptych/Card Stack

Frames примерно `f144-f169` (`5.14-6.04s`):

- несколько карточек появляются как ступенчатый stack;
- карточки и большая типографика живут на разных слоях;
- это не каша, потому что у каждого элемента есть ясная роль и порядок входа.

Вывод: для двух экскурсий можно использовать короткий card-stack, но только
как организованную систему: например, гид + объект + дата-tag, а не несколько
равноправных случайных картинок.

### R6. Echo/Clone Beat

Frames примерно `f170-f192` (`6.07-6.86s`):

- echo занимает около `0.8s`;
- копии не просто видны сразу, а постепенно становятся веером;
- эффект имеет собственный easing и небольшой hold;
- после echo происходит переход к другому крупному портрету.

Вывод: echo в Limeglow должен быть отдельной анимацией:
`source portrait -> delayed clones fan-out -> settle -> camera move`.
Статичные полупрозрачные копии в первом кадре сцены не считаются echo.

### R7. Wide Geometry Scene

Frames примерно `f209-f233` (`7.47-8.33s`):

- широкий кадр с foreground-человеком, background-человеком и синей
  геометрией;
- теги и микроиконки находятся на отдельных планах;
- это один из самых явных примеров глубины в референсе.

Вывод: каждый Limeglow-узел должен иметь хотя бы один кадр, где глубина
читается без объяснений: foreground, hero, mid geometry, far typography.

### R8. Foreground Microphones Scene

Frames примерно `f234-f262` (`8.37-9.38s`):

- герой крупно, foreground-объекты перекрывают его снизу;
- задняя типографика обрезана и работает как архитектура;
- маленькие теги/иконки удерживают тему;
- сцена идёт перед финальным pull-back, то есть не всё завершается сразу
  после предыдущего объекта.

Вывод: перед финальным CTA нужен кульминационный human/object beat, а не
резкий переход от трамвая к финалу.

### R9. Final Pull-Back Lockup

Frames примерно `f263-f326` (`9.41-11.68s`):

- камера отъезжает в общий lockup;
- люди и заголовок держатся стабильно;
- маленькие цветные элементы продолжают жить;
- финал длится больше двух секунд и поэтому успевает закрепиться.

Вывод: CTA Limeglow должен быть не просто последним экраном, а результатом
накопления предыдущих сцен. Финал должен держаться дольше и иметь меньше
случайных объектов.

## Debug-07 Frame Findings

### D1. Intro Too Small And Not Paid Off

Render frames `f001-f008` (`0.00-0.58s`):

- стартует с очень мелких спикеров;
- далее первый крупный guide-beat появляется не как payoff, а как резкая
  подмена композиции;
- крупность гидов в начале не соответствует требованию сделать людей
  узнаваемыми и не "бросать" их как маленькие стикеры.

Вывод: intro должен либо длиться очень коротко как establishing shot, либо
сразу строиться вокруг крупного человека/людей.

### D2. First Hook Is Not Given Time

Render frames `f009-f024` (`0.67-1.92s`):

- хук появляется уже в готовой сцене;
- на `f019-f024` верхняя часть хука начинает уходить/обрезаться;
- глаз в этот же момент конкурирует между лицом, домом, датой и большой
  плашкой;
- хук не становится режиссёрским центром.

Вывод: hook-beat должен иметь отдельную фазу: вход/hold/переход. Для
вопросного хука нужно минимум 12-18 кадров при 12 fps или эквивалент при
нормальном fps, где текст не уезжает с главного места.

### D3. First Excursion Has No Clear Name/Place

Render frames `f009-f034`:

- есть хук, гид, дата и картинка дома;
- нет явного названия `Амалиенау` как продуктового ориентира;
- нет `Калининград`/формата/куда идти;
- дом показывает архитектуру, но не объясняет экскурсию.

Вывод: overlap-beat должен включать не только hook/date/guide/object, но и
короткий title/place anchor.

### D4. Raw House Photo Breaks Collage Contract

Render frames `f009-f040`:

- дом вставлен как необработанная прямоугольная фотография;
- это противоречит требованию превращать фото в коллаж-объект;
- референс почти не использует "обычную фотографию в прямоугольнике" без
  стилизации.

Вывод: для следующего прохода дом должен быть обработан как graphic object:
mask/cutout, duotone, paper edge, crop через skyline/roofline или сильная
журнальная рамка.

### D5. Ice Cream Is A Semantic Dead Zone

Render frames `f035-f049` (`2.83-4.00s`):

- мороженое занимает примерно `1.17s`;
- сцена не сообщает новую экскурсию, дату, гида или CTA;
- это слишком долго для transition-пропа;
- нет остановки смысла, только перемещение камеры.

Вывод: foreground-wipe должен быть короче и мотивирован. Если мороженое не
связано с выбранной экскурсией, его лучше убрать.

### D6. Second Node Appears By Hard Cut Logic

Render frames `f050-f066` (`4.08-5.42s`):

- второй узел появляется резко после пустого/мороженого участка;
- нет отдельного входа гида или объекта;
- текст/дата/гид уже собраны в одной карточке, но без staged reveal;
- дата повторялась в ранней версии; в текущей версии это исправлено, но сама
  карточная логика остаётся плоской.

Вывод: второй узел должен начинаться не включением готовой карточки, а
последовательностью: route/object hint -> guide enters -> hook lands ->
date/title lock.

### D7. Tram Object Looks Like A Rectangle, Not A Blender Object

Render frames `f067-f081` (`5.50-6.67s`):

- трамвай читается как растровая карточка с чёрным прямоугольником;
- белая обводка и растровые края выглядят как Pillow-precomp, а не как
  дорогой Blender-layer;
- объект слишком долго живёт отдельно от гида и продуктовой информации.

Вывод: трамвай должен быть либо полноценным alpha-plane без чёрного
прямоугольника, либо intentionally posterized/duotone с чистой графической
рамкой, а не промежуточный PNG-карточный артефакт.

### D8. CTA Is A Montage Switch, Not A Pull-Back

Render frames `f082-f096` (`6.75-7.92s`):

- финал включается почти сразу после трамвая;
- нет накопления через wide geometry / human-object beat;
- CTA держится примерно `1.25s`, но при этом не выглядит кульминацией;
- people-lockup уже статичен и не развивается.

Вывод: финал должен появляться через pull-back: камера показывает, что две
экскурсии были частью одного дайджеста. CTA должен быть результатом
пространственного раскрытия, а не новым экраном.

## Motion Metrics

Из `audit_meta.json`:

- render frame diff mean: `23.67`;
- reference frame diff mean: `6.60`;
- render optical flow mean magnitude: `4.42`;
- reference optical flow mean magnitude: `0.68`;
- render fps: `12`;
- reference fps: `27.837`.

Эти числа нельзя читать как прямую меру качества, но они подтверждают
наблюдение глазами: render меняется грубо и быстро, при более низком fps.
Референс имеет много удержаний и микродвижений, поэтому среднее изменение
кадра ниже, хотя motion воспринимается дороже.

Вывод: следующий preview нельзя оценивать только по "есть движение". Нужно
проверять speed curve: где разгон, где пролёт, где торможение, сколько кадров
смысл удерживается.

## Failed Assumptions In Debug-07

1. **Blender depth сам сделает дорого.** Неверно. Depth-плоскости только дают
   инструмент; дорогой motion появляется из маршрута, timing, stagger, hold и
   смысловых переходов.
2. **Visibility windows решат мусор.** Частично. Они убрали протекание старых
   объектов, но создали ощущение hard cut, если нет мотивированного входа/выхода.
3. **Echo можно сделать полупрозрачными копиями.** Неверно. Echo должен быть
   разворачивающейся анимацией.
4. **Foreground object может быть нейтральным wipe.** Неверно. Если объект
   занимает больше нескольких кадров, он обязан работать продуктово.
5. **Question hook можно придумать по картинке.** Неверно. Hook должен идти из
   engagementcards/данных экскурсии, а картинка только усиливает его.
6. **Keyframes достаточно для оценки.** Неверно. Для этой задачи keyframes
   допустимы только после покадрового motion audit.

## Reference Techniques Checklist

| Technique | Reference | Debug-07 | Verdict |
| --- | --- | --- | --- |
| Big shared canvas | yes | partial | Camera moves over a board, but board is not story-designed. |
| Establishing small-to-large payoff | yes | weak | Starts small but payoff is abrupt and not human-first enough. |
| Long readable collage hold | yes | no | Product beats change too quickly. |
| Hook/title as scene architecture | yes | weak | Hook appears but does not control the beat. |
| Human cutout as hero | yes | partial | People are large later, but not consistently staged. |
| People grounded/cropped by frame | yes | partial | Improved, but intro still uses tiny people. |
| Speaker without mandatory color plate | yes | no/weak | Debug-07 overuses colored portrait plates and card logic. |
| Speaker/object overlap promise | yes | weak | Guide and excursion object coexist, but not as a clear "guide shows this" beat. |
| Echo as animated beat | yes | no | Static/early echo, no fan-out drama. |
| Triptych/card stack | yes | no | Not implemented as a staged mechanism. |
| Foreground object with meaning | yes | no | Ice cream is semantically empty. |
| Object/person/typography interaction | yes | weak | Mostly stacked layers, not interactions. |
| 3 depth plans visible | yes | partial | Technical layers exist, visual depth is inconsistent. |
| Geometry/ornament as active depth plan | yes | weak | Far labels exist, but no designed large/small geometry grammar. |
| Desynchronized parallax | yes | weak/no | Camera moves layers, but no intentional lag system. |
| Editorial ease-in-out | yes | weak/no | Blender Bezier is not enough; curves are not designed. |
| Pull-back final lockup | yes | weak | CTA appears as switch, not reveal. |
| Controlled micro-UI/geometry | yes | weak | Far labels exist, but not enough designed micro-motion. |
| Image treatment consistency | yes | weak | House/tram assets still look like raw/precomp rectangles. |

## Product Story Verdict

Debug-07 does not yet tell a clear product story about two excursions.

What is present:

- two debug guides;
- two visual objects;
- two hooks;
- dates;
- CTA.

What is missing:

- title/place anchor per excursion;
- enough time to read each excursion;
- credible connection "this guide shows this object/place";
- hook derived from actual excursion content instead of image-only association;
- final CTA as conclusion of the digest, not as a separate card.

## Motion Object Map Requirement

Следующий director pass не должен начинаться с расстановки картинок. Он должен
начинаться с карты всех motion-объектов сцены: кто они, зачем они нужны,
на каком depth-плане живут, как входят, как движутся, как исчезают и какой
смысл поддерживают.

В референсе такими объектами являются не только люди. Там есть несколько
семейств объектов:

- `speaker objects`: крупные cutout-люди без фона, часто без цветной плашки;
- `visual anchors`: предметы/образы сцены, которые зритель хочет рассмотреть;
- `typography objects`: смысловые слова, повторяющиеся на разных планах;
- `geometry objects`: треугольники, прямоугольники, диагонали, line-ornaments;
- `micro-ui objects`: маленькие теги, outline-icons, pills, crosses, circles;
- `foreground blockers`: объекты перед камерой, которые помогают переходам;
- `background masses`: большие тёмные/цветные формы, задающие направление.

Для Limeglow аналогичная карта должна строиться до рендера. Пример типов:

- `speaker`: гид, главный человеческий магнит;
- `excursion_visual`: дом/трамвай/деталь маршрута, главный визуальный магнит;
- `semantic_typography`: `ЭКСКУРСИЯ`, `ПРОГУЛКА`, `АМАЛИЕНАУ`, `ТРАМВАЙ`,
  `КАЛИНИНГРАД`, `ДАЙДЖЕСТ`;
- `supporting_typography`: повтор названия района/темы на дальнем плане,
  крупно и частично обрезанно;
- `geometry_large`: большие диагонали/прямоугольники/треугольники, которые
  дают массу и направление;
- `geometry_small`: мелкие линии, точки, маршрутные маркеры, иконки;
- `info_tag`: дата, имя гида, формат, район, CTA;
- `transition_mask`: объект или форма, через которую камера переходит дальше.

Обязательные поля карты объекта:

- `id`;
- `role`: speaker / visual / typography / geometry / micro-ui / foreground /
  background;
- `semantic_purpose`: что объект помогает понять или запомнить;
- `scene_beat`: в каком beat'е он нужен;
- `depth_plane`: far / mid / hero / foreground;
- `dominance`: hero / support / ambient;
- `entry`: from edge / scale-in / wipe / reveal behind object / clone fan-out /
  already-present;
- `exit`: occluded / camera leaves / wipe / scale-out / remains as fragment;
- `motion_curve`: camera-linked / own ease-in-out / spring-pop / slow drift;
- `delay_frames`: задержка относительно камеры или главного объекта;
- `readability_frames`: сколько кадров объект должен быть читаемым;
- `may_crop`: можно ли резать краем кадра;
- `must_not_crop`: лицо, имя, дата, ключевой hook и CTA.

Важно: один и тот же смысловой типографический объект может повторяться на
разных планах. Например:

- `АМАЛИЕНАУ_far`: большое слово на дальнем плане, частично обрезанное,
  медленно отстаёт от камеры;
- `АМАЛИЕНАУ_mid`: районный label рядом с домом;
- `АМАЛИЕНАУ_foreground`: короткий tag, который быстро входит и фиксирует
  продуктовый смысл.

Такой повтор не считается дублем, если у каждого экземпляра разная роль,
глубина и motion. Это ближе к референсу, где слово/тема может жить как
фоновой типографический объект, как tag и как часть финального lockup.

## Semantic Typography Contract

Типографика в Limeglow должна быть не только текстом для чтения, но и
motion-объектом. В референсе смысловые слова вроде `Религия` появляются
разными способами: как tag, как поддерживающая подпись, как часть
типографического фона. Это помогает зрителю держать тему даже во время
быстрого движения.

Для Limeglow нужно использовать такой же принцип:

- `ЭКСКУРСИЯ` / `ЭКСКУРСИИ`: общий product-frame и фон первого/финального
  beat'а;
- `ПРОГУЛКА`: альтернативное слово, если источник/дайджест так называет
  формат;
- `АМАЛИЕНАУ`: районный visual-anchor для первого узла;
- `ТРАМВАЙ`: объектно-тематический anchor для второго узла;
- `ДАЙДЖЕСТ`: финальный CTA-context;
- `КАЛИНИНГРАД`: общий city-anchor, если нужен для продуктовой ясности.

У каждого typography-object должны быть:

- собственная глубина;
- собственный entry/exit;
- собственный easing;
- собственное отношение к камере: отставать, опережать, дрейфовать или
  фиксировать сцену;
- явная функция: фон, тема, label, CTA, rhythm accent.

Запрет: нельзя использовать одну большую надпись как статичный фон на весь
ролик и считать это типографической системой. Типографика должна помогать
восприятию конкретной экскурсии и глубины конкретной сцены.

## Memorability And Reading Contract

Цель ролика не в том, чтобы "показать все ассеты". Цель: человек должен
увидеть и запомнить конкретную экскурсию.

Для каждой экскурсии зритель должен успеть запомнить:

- кто ведёт;
- что он/она покажет;
- где/о чём экскурсия;
- когда можно пойти;
- почему это интересно.

Минимальный visual memory contract для одного экскурсионного узла:

- `guide-face-hold`: лицо/силуэт гида крупно и читаемо минимум один отдельный
  beat;
- `visual-object-hold`: образ экскурсии крупно и читаемо минимум один
  отдельный beat;
- `guide-object-overlap`: гид и образ вместе в одном кадре минимум один
  отдельный beat;
- `fact-lock`: дата/имя/район появляются после того, как зритель уже понял
  визуальный контекст;
- `no-rush`: нельзя менять главный смысловой объект быстрее, чем зритель
  успевает его назвать.

Практически это означает, что для двух экскурсий 8 секунд почти наверняка
слишком мало, если внутри есть intro, два узла, transition, culmination и CTA.
Либо ролик должен быть длиннее, либо нужно сокращать количество beat'ов и
делать каждый beat гораздо чище.

## Human Scale Requirement

Референс часто использует людей очень крупно:

- лицо/верх тела занимает значительную часть кадра;
- человек выходит за края;
- нижняя часть часто перекрывается foreground-объектами или уходит за край;
- человек без фона не обязан иметь плашку, потому что сам является сильной
  графической массой.

Debug-07 этого почти не сделал: люди чаще были средними карточными фигурами,
а не доминирующими героями. Следующий проход должен заложить хотя бы два
кадра/beat'а, где гид крупнее и сильнее текущего `debug-07`, без обязательной
цветной подложки.

## Next Iteration Blocking Contract

Before another full render, the director must produce a frame-level plan with
these fields for every beat:

- frame range;
- camera intent: hold / push-in / pull-back / lateral / vertical / orbit-lite;
- expected focus object;
- text that must be readable;
- foreground/mid/hero/far objects;
- how each object enters and exits;
- easing curve name and duration;
- layer delay/stagger in frames;
- product fact being communicated.

Before another full render, the director must also produce an object map:

- all speakers;
- all excursion visual objects;
- all semantic typography objects;
- all geometry objects;
- all micro-ui/info tags;
- all transition masks;
- depth, motion, easing, delay and semantic purpose for each object.

The object map should reuse named entries from
`geometry-grammar-library.md` where possible, or add new entries there before
using a new geometry/typography grammar in a render.

Minimum required beat structure for two excursions:

1. `Intro / digest premise`  
   Max 0.5s if people are small. Must quickly pay off into large humans.
2. `Excursion A hook`  
   Hook readable with title/place anchor. No competing object dominance.
3. `Excursion A guide + object overlap`  
   Guide, object, date, and title together. At least one clear depth composition.
   This beat must make the visual promise visible: this guide will show this
   place/object.
4. `Transition A -> B`  
   No semantic dead zone longer than 0.25-0.35s. Transition object must be
   motivated or removed.
5. `Excursion B hook`  
   Hook readable with title/place anchor.
6. `Excursion B guide + object overlap`  
   Guide, object, date, and title together. Object must not be a black rectangle.
   This beat must make the visual promise visible: this guide will show this
   place/object.
7. `Human/object culmination`  
   One beat where people and visual objects coexist, like reference foreground
   microphone / wide geometry scenes.
8. `Final pull-back CTA`  
   CTA appears by camera reveal. People large enough, grounded, no random extra
   objects.

Hard bans for next prototype:

- no evaluating motion by sparse keyframes;
- no static echo in the first frame of a scene;
- no treating colored plates as mandatory containers for speakers;
- no guide-only or object-only excursion node without at least one clear
  guide+visual overlap beat;
- no object appearing/disappearing in-frame without wipe/camera/occlusion reason;
- no raw rectangular photos unless the rectangle is an intentional designed
  card with treatment;
- no black matte around cutout PNGs;
- no filler foreground object that does not communicate product meaning;
- no hook invented only from image content;
- no full-scene hard visibility switch unless it is hidden by a designed
  transition.

Mandatory design choices for next prototype:

- define at least two speaker treatments that do not use a color plate behind
  the person;
- define one animated echo beat with frame-level fan-out timing;
- define a geometry grammar per excursion node:
  - Amalienau: facade/windows/roofline/quartal-grid inspired forms;
  - Tram: rails/route-line/diagonal blue geometry inspired forms;
- define which object is foreground, hero, mid, and far in each beat;
- define visual-object treatment before animation: cutout/duotone/paper-card/
  skyline/solid-background, with no accidental black matte.

## Render Time Notes

The final `debug-07` preview render took approximately `2m 23s` wall-clock,
including Docker setup for Xvfb/GL/fonts. Keyframe-only Blender passes took
roughly `40-45s` each. Frame-by-frame audit sheet generation took about `24s`
for both videos.



#Дополнения требований 1

Я смотрю на референс и вижу что там есть и геометрическая грамматика (а объекты эти имеют и глубину и свою трансформацию), и такие же объекты типографики есть (например в референсе есть слово религия несколько раз) и они могут быть поддерживающими в смысловом плане объекты, в нашем случае это "экскурсия", "прогулка", название района "Амалиенау" и он может даже повторяться на разных слоях разной дальности и кадый объект имеет свою анимацию и ease там и отдельно на самой анимации и целиком на камере.

Продолжай наполнять свой аудит документ и и дополни его что тебе надо составить карту всех этих объектов и продумать их наличие движение присутствие, смысл и глубину в сцене. Наличие таких объектов лучше даёт понимание глубины и смыслов.
И люди в референсе часто крупные (у тебя ни разу такого не было ещё)

И ещё, цель чтобы человек видел и запоминал, сцена про конкретную экскурсию строится так чтобы человек успел понять и запонить информацию о ней
