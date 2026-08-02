# Limeglow Geometry Grammar Library

Рабочая накопительная библиотека Codex. Канонические требования остаются в
`requirements.md`; этот файл хранит переиспользуемые геометрические,
типографические и motion-объекты для будущих генераций Limeglow.

Цель библиотеки: не придумывать геометрию заново в каждой итерации, а выбирать
подходящие grammar objects под конкретную экскурсию, гида, визуальный образ и
маршрут камеры.

Первый конкретный план, который обязан выбирать объекты из этой библиотеки:
`director-plan-debug08.md`.

## Entry Schema

Каждый объект или паттерн в библиотеке должен описываться так:

- `name`: стабильное название;
- `family`: geometry / typography / micro-ui / transition / depth-mass;
- `semantic_fit`: для каких экскурсий, районов, объектов или настроений подходит;
- `visual_role`: что делает в кадре: масса, маршрут, фон, ритм, фокус, переход;
- `depth_defaults`: far / mid / hero-adjacent / foreground;
- `morphology`: форма и варианты;
- `motion`: собственное движение объекта;
- `camera_relation`: как реагирует на движение камеры;
- `easing`: рекомендуемые easing-профили;
- `typography_pairing`: какие слова/теги хорошо с ним работают;
- `do`: что усиливает;
- `avoid`: где начинает дешевить или мешать;
- `debug_notes`: заметки после прогонов.

## Families

### Geometry

Крупные и средние формы, которые задают глубину, направление и композиционную
массу: прямоугольники, треугольники, диагонали, фасадные линии, рельсы, сетки,
оконные ритмы.

### Typography

Слова как motion-объекты, а не просто подписи. Могут повторяться на разных
планах: большой дальний фон, средний тематический label, ближний tag.

### Micro-UI

Маленькие круги, кресты, outline-icons, pills, точки маршрута, календарные
метки. Дают живой слой, но не должны спорить с гидом и визуальным образом.

### Transition

Объекты или формы, через которые камера переходит дальше: foreground mask,
wipe-plane, diagonal pass, typographic tunnel, object-edge push.

### Depth Mass

Большие тёмные или цветные массы, которые помогают читать объём и путь камеры,
но не являются самостоятельным продуктовым объектом.

## Global Motion Principles

- Каждый объект имеет собственное движение, даже если оно маленькое.
- Движение объекта и движение камеры не должны быть одним и тем же.
- Большие far-объекты чаще отстают от камеры.
- Foreground-объекты могут чуть опережать камеру или пересекать кадр быстрее.
- Micro-UI может иметь stagger и delayed settle.
- Typography-object может быть одновременно смыслом и глубиной.
- Повтор одного слова на разных планах разрешён, если роли различаются.

## Object Entries

### G-AMA-01 Facade Vertical Rhythm

- `family`: geometry
- `semantic_fit`: Амалиенау, немецкие виллы, архитектура, фасады, окна.
- `visual_role`: поддерживает визуальный образ дома, даёт mid/far depth,
  связывает гида с архитектурой.
- `depth_defaults`: far + mid.
- `morphology`: набор вертикальных линий и узких прямоугольников разной высоты,
  вдохновлённых фасадными простенками и оконными осями; часть линий может
  уходить за верхний/нижний край.
- `motion`: медленный lateral drift против камеры, отдельные линии входят
  stagger'ом на 2-4 кадра.
- `camera_relation`: far слой двигается 35-50% от camera move, mid слой
  65-85%.
- `easing`: `easeInOutQuart` для группового сдвига, `easeOutBack` очень
  слабо для отдельных вертикалей.
- `typography_pairing`: `АМАЛИЕНАУ`, `ВИЛЛЫ`, `ПРОГУЛКА`.
- `do`: использовать за гидом или за вырезанным домом, чтобы дом не был просто
  прямоугольной фотографией.
- `avoid`: не делать равномерную сетку как офисный паттерн; линии должны быть
  архитектурными, не табличными.

### G-AMA-02 Roofline Cut

- `family`: geometry / transition
- `semantic_fit`: архитектурные маршруты, дома с заметной линией крыши,
  skyline/roofline work.
- `visual_role`: создаёт журнальный край выреза и может быть переходным
  foreground/mid mask.
- `depth_defaults`: hero-adjacent + foreground.
- `morphology`: крупная ломано-полигональная линия крыши без мелкой дрожи;
  допускаются сильные выпуклые формы, вогнутые участки упрощаются.
- `motion`: линия может "срезать" кадр как mask-wipe или медленно раскрывать
  дом снизу вверх.
- `camera_relation`: foreground version опережает камеру; hero-adjacent
  version следует за visual object.
- `easing`: `easeInOutExpo` для wipe, `easeOutCubic` для reveal.
- `typography_pairing`: `АМАЛИЕНАУ`, `РАЙОН`, `ВИЛЛА`.
- `do`: использовать как чистую дизайнерскую замену плохой raw-photo рамке.
- `avoid`: не превращать в маркерную ломаную с десятками точек; нужна
  журнальная уверенность.

### G-AMA-03 Window Blocks

- `family`: geometry
- `semantic_fit`: виллы, модерн, фасады, прогулки по району.
- `visual_role`: создаёт mid-layer рядом с гидом и объектом; связывает текст
  с архитектурой.
- `depth_defaults`: mid.
- `morphology`: 3-7 прямоугольников с разной толщиной рамки, как абстрактные
  окна; часть может быть solid, часть outline.
- `motion`: scale-in с маленьким stagger; отдельные окна могут слегка
  сдвигаться по вертикали.
- `camera_relation`: реагирует на камеру умеренно, 70-90%.
- `easing`: `easeOutBack` для появления, `easeInOutSine` для drift.
- `typography_pairing`: короткие tags: `дом`, `район`, `архитектура`.
- `do`: использовать для object/guide overlap, чтобы визуально объяснять
  "гид покажет фасады/дома".
- `avoid`: не закрывать лицо и ключевые даты.

### G-TRAM-01 Rail Diagonals

- `family`: geometry
- `semantic_fit`: трамвай, транспортные маршруты, городские линии,
  движение по городу.
- `visual_role`: направление камеры, ощущение рельсов, depth и speed.
- `depth_defaults`: far + foreground.
- `morphology`: две или четыре диагональные линии/полосы, сходящиеся в
  перспективе; могут быть синими, белыми или тёмно-синими.
- `motion`: foreground-рельсы проходят быстрее камеры, far-рельсы отстают;
  допускается лёгкое "схождение" линий при push-in.
- `camera_relation`: foreground 110-130%, far 40-55%.
- `easing`: `easeInOutExpo` для camera-linked pass, `linear-drift` только как
  вторичный слой.
- `typography_pairing`: `ТРАМВАЙ`, `РЕЛЬСЫ`, `МАРШРУТ`.
- `do`: использовать как переход к трамвайному объекту или как дальний план за
  гидом.
- `avoid`: не проводить линии через лицо; не делать одну случайную полоску.

### G-TRAM-02 Route Node Dots

- `family`: micro-ui / geometry
- `semantic_fit`: прогулки с маршрутом, транспорт, районные экскурсии.
- `visual_role`: маленький живой слой, который объясняет маршрутность.
- `depth_defaults`: mid + foreground.
- `morphology`: 3-5 точек на линии, маленькие labels или rings.
- `motion`: точки появляются stagger'ом, как остановки; активная точка может
  scale-pop.
- `camera_relation`: mid layer 80%, foreground labels 110%.
- `easing`: `easeOutBack` для active dot, `easeInOutCubic` для линии.
- `typography_pairing`: дата-tag, `старт`, `маршрут`, `17:30`.
- `do`: добавлять в моменты, когда нужно связать гида и трамвай как экскурсию,
  а не просто транспортную картинку.
- `avoid`: не превращать в настоящую карту с мелким текстом.

### G-TRAM-03 Ticket Punch Blocks

- `family`: geometry / transition
- `semantic_fit`: транспорт, ретро-маршруты, билеты, архивность.
- `visual_role`: foreground transition или mid-card под дату.
- `depth_defaults`: mid + foreground.
- `morphology`: прямоугольник с 1-3 круглыми/полукруглыми вырезами по краям,
  напоминающий билет.
- `motion`: короткий slide-in, затем punch-hole mask может раскрыть следующий
  слой.
- `camera_relation`: как foreground blocker на переходах.
- `easing`: `easeInOutQuart` для slide, `easeOutBack` для tag pop.
- `typography_pairing`: дата, цена/формат, `ТРАМВАЙ`.
- `do`: использовать вместо случайного foreground-предмета.
- `avoid`: не делать билет главным объектом, если главным должен быть трамвай.

### T-GEN-01 Giant Topic Word

- `family`: typography
- `semantic_fit`: любой excursion node.
- `visual_role`: дальний смысловой фон и depth anchor.
- `depth_defaults`: far.
- `morphology`: очень крупное слово, обрезанное краями кадра; может повторяться
  2-3 раза с разной прозрачностью.
- `motion`: медленный drift с задержкой относительно камеры; допускается
  slight scale.
- `camera_relation`: 30-50% camera move, delay 3-8 frames.
- `easing`: `easeInOutSine` или custom slow editorial drift.
- `typography_pairing`: само является typography object:
  `ЭКСКУРСИЯ`, `ПРОГУЛКА`, `АМАЛИЕНАУ`, `ТРАМВАЙ`, `ДАЙДЖЕСТ`.
- `do`: использовать как архитектуру кадра, не как заголовок для чтения.
- `avoid`: не ставить поверх лица и не считать единственным текстовым слоем.

### T-GEN-02 Repeated Semantic Stack

- `family`: typography
- `semantic_fit`: район, тема, формат; работает как аналог повторяющихся
  смысловых слов в референсе.
- `visual_role`: смысловой ритм и depth.
- `depth_defaults`: far + mid.
- `morphology`: одно слово повторяется 2-4 раза вертикально или диагонально,
  часть строк обрезана.
- `motion`: строки имеют разные задержки; far-строка отстаёт, mid-строка
  догоняет, одна строка может slide-in.
- `camera_relation`: 45-80% depending on depth.
- `easing`: `easeInOutQuart` для основной строки, `easeInOutSine` для far.
- `typography_pairing`: `АМАЛИЕНАУ`, `ТРАМВАЙ`, `ПРОГУЛКА`.
- `do`: помогает зрителю запомнить район/тему во время движения.
- `avoid`: не использовать длинные фразы; только 1-2 слова.

### T-GEN-03 Foreground Fact Tag

- `family`: typography / micro-ui
- `semantic_fit`: дата, время, имя гида, формат.
- `visual_role`: закрепляет факт после visual recognition.
- `depth_defaults`: foreground.
- `morphology`: pill, small rectangle, outlined label.
- `motion`: короткий pop/slide после появления visual object.
- `camera_relation`: 105-125%, может чуть опережать camera settle.
- `easing`: `easeOutBack` для появления, `easeInOutCubic` для ухода.
- `typography_pairing`: дата, имя, район.
- `do`: показывать после того, как зритель уже увидел гида/объект.
- `avoid`: не выводить все факты сразу в начале сцены.

### M-GEN-01 Orbit Icons

- `family`: micro-ui
- `semantic_fit`: все сцены, где нужен живой мелкий слой.
- `visual_role`: поддерживает дорогой broadcast/editorial feel.
- `depth_defaults`: mid + foreground.
- `morphology`: outline circles, plus, star, small cross, arrow dot.
- `motion`: slow orbit/drift, stagger fade, occasional scale-pop.
- `camera_relation`: 80-120%, delay varies.
- `easing`: `easeInOutSine` для drift, `easeOutBack` для pop.
- `typography_pairing`: не несёт текст; работает рядом с tags.
- `do`: использовать малым количеством вокруг активной сцены.
- `avoid`: не разбрасывать равномерно по всему кадру.

### D-GEN-01 Dark Negative Space Field

- `family`: depth-mass
- `semantic_fit`: все сцены.
- `visual_role`: даёт дорогую паузу, воздух, контраст для людей и объектов.
- `depth_defaults`: far.
- `morphology`: почти чёрное поле с едва заметным градиентом/шумом.
- `motion`: почти неподвижно; может иметь очень медленный drift.
- `camera_relation`: 20-35%.
- `easing`: none / very slow ease.
- `typography_pairing`: giant topic words.
- `do`: оставлять воздух вокруг крупного лица или объекта.
- `avoid`: не заполнять всё декоративными деталями.

### X-GEN-01 Diagonal Color Pass

- `family`: transition / geometry
- `semantic_fit`: переход между экскурсионными узлами.
- `visual_role`: скрывает монтаж, задаёт направление камеры.
- `depth_defaults`: foreground + mid.
- `morphology`: крупная диагональная цветная масса, может быть прямоугольником
  или треугольником, выходящим за края.
- `motion`: быстрый pass через кадр, за которым открывается новый node.
- `camera_relation`: foreground 120-140%, mid 80-100%.
- `easing`: `easeInOutExpo`, 6-12 кадров на preview.
- `typography_pairing`: короткий far word может отстать за pass.
- `do`: использовать вместо hard visibility switch.
- `avoid`: не использовать как бессмысленную декоративную полоску без
  transition-работы.

## Selection Rules

Для каждого excursion node режиссёр должен выбрать:

- 1 speaker treatment;
- 1 visual-object treatment;
- 1 large geometry grammar;
- 1 semantic typography grammar;
- 1-2 micro-ui grammars;
- 0-1 transition grammar.

Подбор должен зависеть от фактической экскурсии:

- архитектура -> facade/window/roofline grammars;
- транспорт -> rails/route/ticket grammars;
- музей/экспонат -> vitrine/frame/object-spotlight grammars;
- природа/море -> contour/wave/horizon grammars;
- городская прогулка -> map-grid/route-dot/street-corner grammars.

## Debug-07 Lessons Applied To Library

- A color plate behind a speaker is not a default grammar.
- A raw photo rectangle is not a visual-object treatment.
- A semantic word repeated at different depths is allowed and desirable.
- Geometry must have meaning, depth and motion; otherwise it becomes clutter.
- Foreground transition objects must either hide a cut or sell the excursion.
- People and excursion visuals must both get memory holds.
