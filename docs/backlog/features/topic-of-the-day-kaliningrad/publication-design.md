# Редакционный формат, Telegram и VK

## 1. Редакционная позиция

Дайджест — самостоятельная короткая сводка по нескольким источникам, а не копия «лучшего поста». Текст должен быть выразительным и живым, но креативность строится на композиции, точных глаголах и контрасте вкладов авторов, а не на выдуманных деталях, сенсационности или рекламных превосходных степенях.

Обязательные правила:

- нейтральное название темы без кликбейта;
- каждое конкретное утверждение имеет `evidence_post_id`;
- факт одного источника явно атрибутируется;
- конфликтующие сведения не склеиваются в выдуманный консенсус;
- «лучше/больше/эмоциональнее» означает только сравнение внутри просмотренного дневного корпуса;
- ссылки ведут на конкретный оригинальный пост, а не только на главную страницу паблика;
- один автор не повторяется в нескольких почти одинаковых фразах;
- остальные авторы перечисляются в конце без повторения уже названных;
- большая часть чужого текста не цитируется и не пересказывается слишком близко;
- персональное имя используется только при надёжном `author_identity`; иначе указывается название паблика/канала.

## 2. Telegram text contract

Зафиксированный Telegram-формат — **один `sendPhoto` с Bento-карточкой и компактным HTML caption**. Он даёт один platform message id и остаётся единственным Telegram publication mode. Album и отдельные video/photo attachments не входят в roadmap без нового решения владельца. Сильное исходное видео может получить роль и ссылку в тексте/Bento, но само видео в Telegram-дайджест не прикрепляется.

Структура:

```text
<b>Тема дня: [6–12 слов]</b>

[2–3 предложения сводки.]

<a href="post_url">Источник/автор</a> подробнее других собрал факты,
а <a href="post_url">источник/автор</a> показал самые сильные кадры.
[Опционально ещё одна действительно различимая роль.]

Также писали: <a href="...">...</a>, <a href="...">...</a>.
```

Ограничения до send:

- caption проходит реальную Bot API length/entity validation;
- целевой бюджет — до ~900 видимых символов, чтобы оставался запас;
- 2–4 категорийных упоминания, остальные — в финальном списке;
- ссылки дедуплицированы по canonical URL/source;
- HTML entities построены из offsets/escaped text, а не string concat без проверки;
- итоговый `payload_hash` фиксируется до publication claim.

Если текст не помещается, writer сокращает сводку и число ролей; отправка второго сообщения, album или source video не являются fallback, потому что нарушают Bento-only contract.

### Закрытый Telegram admin preview

До публикации в открытый канал bot отправляет в allowlisted admin-чат:

- финальную Bento-карточку;
- точный Telegram caption и VK text;
- source day, planned slot `10:00 Europe/Kaliningrad`;
- coverage, выбранную тему и ближайшую альтернативу;
- список авторов/ролей, original links и finalist metrics;
- предупреждения anti-repeat, sensitive-scope и media checks;
- короткий immutable `payload_hash` fingerprint.

Inline-кнопок ровно две:

- `✅ Одобрить к публикации` — создаёт payload-bound approval; до 10:00 он ставит выпуск в разрешённое ожидание слота, но не вызывает public API прямо из callback;
- `❌ Отклонить выпуск` — фиксирует решение и запрещает публикацию этого payload.

Кнопок пересборки текста, смены фото и выбора другой темы нет. Решение принимает один configured administrator, одного нажатия достаточно. Повторное нажатие идемпотентно. После принятого решения кнопки заменяются статусом, а изменение текста, links или assets требует нового preview и нового approve. Если approve приходит после 10:00, publisher запускается сразу.

## 3. Media selection

Для каждого finalist post рассматриваются все media items в ограниченном альбоме, а не только первая картинка.

Каскад:

```text
download metadata / rights gate
  → resolution, blur, exposure, aspect, pHash dedupe
  → screenshot/banner/text-overlay/watermark/face/safety checks
  → aesthetic + technical scorer
  → CLIP/SigLIP relevance prompts
  → VLM только для top media
  → best-of-album + diversity selection
```

В `media-manifest` сохраняются model/version, конкретный album index, технические scores, short explanation, rights policy, source URL и reasons pass/fail. Существующий Region Talk threshold `0.70` нельзя считать готовым: его нужно откалибровать на локальных карточках, репортажных фото, афишах и видео thumbnails.

### Возможные media roles

- `hero_photo` — лучший безопасный кадр темы;
- `fact_photo` — документирует ключевой факт/место;
- `emotional_photo` — передаёт реакцию, но не заменяет факты;
- `best_video` — ролик после отдельной content/technical проверки;
- `source_avatar` — медальон автора/паблика;
- `fallback_graphic` — deterministic card без чужого media.

## 4. Права и атрибуция

Зафиксированная продуктовая policy:

- `public_source_attributed` — любое фото из доступного публичного source post можно использовать в карточке после safety/identity checks; обязательны прямая ссылка на original post на самом изображении и кликабельная ссылка в тексте;
- `private_or_unavailable` — private, deleted, access-restricted или не имеющий проверяемого original URL asset не используется;
- `blocked_safety` — опасный, не относящийся к теме, ошибочно привязанный или иной safety-blocked asset не используется.

Решение «используем любое публичное фото со ссылкой» является продуктовой установкой владельца, а не отдельным юридическим заключением. Если позднее legal/platform review потребует ограничений, policy version меняется явно и проходит replay. Если original post недоступен или ссылку нельзя надёжно связать с media, Bento использует deterministic typographic fallback без этой фотографии.

Attribution хранится и на asset level, и в тексте. Нельзя создавать впечатление, что автор одобрил/спонсировал канал.

### Обязательная ссылка прямо на фотографии

Решение владельца продукта: каждое source photo, попавшее в Telegram Bento или VK slide, получает видимую прямую ссылку на **конкретный оригинальный пост**, из которого взято изображение.

Правила overlay:

- показывать canonical path без протокола, например `t.me/channel/123` или `vk.com/wall-123_456`;
- не заменять оригинальный URL сторонним shortener;
- размещать ссылку на контрастной компактной подложке в safe zone, не закрывая смысловой объект;
- сохранять читаемость на mobile preview 360–430 px;
- если в Bento несколько фотографий из разных постов, маркировать каждый photo tile его собственной ссылкой;
- если несколько фото происходят из одного поста, на них допустима одна и та же ссылка;
- для video frame/thumbnail указывать ссылку на исходный видеопост;
- renderer валидирует соответствие `media_id → source_post_url → rendered label` до публикации;
- прямая ссылка также остаётся кликабельной в caption/wall text.

В рамках текущего решения публичность original post + обязательная ссылка являются достаточным продуктовым условием использования фото; safety, identity и доступность original URL остаются отдельными hard gates.

## 5. Telegram Bento + медальоны

Стартовый размер — `1080×1350`, deterministic SVG/HTML/canvas renderer.

Композиция:

1. сильный заголовок темы;
2. один крупный hero-сектор;
3. 2–4 небольших contribution blocks;
4. круглые медальоны автора/источника;
5. короткие labels: «факты», «видео», «контекст», «обсуждение»;
6. прямые ссылки на source photos;
7. дата и бренд канала без конкуренции с темой.

Правила медальонов:

- avatar привязан к stable source/author id и provenance;
- при сомнительной идентичности — инициалы/логотип паблика;
- не больше 4–5 медальонов на обложке;
- один и тот же медальон не повторяется;
- photo crop не отрезает лицо/логотип;
- роль на карточке совпадает с role assignment и ссылкой в caption;
- размер текста читается на мобильном превью;
- значимость автора не кодируется только цветом.

## 6. VK ordered multi-photo post

Под «каруселью» понимается ordered multi-photo wall post; разные VK clients могут показывать его сеткой или горизонтальным просмотром, поэтому одинаковый carousel UI не обещается.

Базовый набор 4–7 слайдов `1080×1350`:

1. **Cover** — тема и hero photo/typographic fallback.
2. **Сводка** — 3–5 ключевых фактов.
3. **Лучший вклад** — «X подробнее других собрал факты».
4. **Медиа/видео** — сильный кадр и корректная атрибуция.
5. **Эмоция/конструктив** — только если роль уверенно определена.
6. **Облако источников** — тема + все авторы/паблики.
7. **Источники** — короткий список/CTA перейти к оригиналам, если он не перегружает wall text.

Не все слайды обязательны. Слабая роль пропускается; пустой placeholder не создаётся.

Все изображения сначала успешно загружаются, затем выполняется единственный `wall.post`. Partial media upload блокирует publish. Wall text содержит обычные VK links/упоминания и не полагается на текст внутри картинок.

## 7. Облако источников

Главная тема набирается сильной типографикой. Визуальный размер source tag вычисляется из capped influence score:

```text
tag_influence =
  0.30 * contribution_quality
+ 0.25 * normalized_engagement
+ 0.20 * unique_fact_value
+ 0.15 * discussion_signal
+ 0.10 * media_value
```

Это только стартовая визуальная формула. Ограничения:

- minimum font size остаётся читаемым;
- максимум/минимум отличаются умеренно, без унижения малых источников;
- raw subscriber count не используется;
- missing comments/reposts не считаются нулём;
- source, названный внутри текста, всё равно может быть в cloud, но финальный текстовый список его не повторяет;
- облако не является графиком точных величин; exact metrics остаются в audit report.

## 8. Writer output contract

```json
{
  "title": "...",
  "summary_sentences": ["..."],
  "inline_contributions": [
    {
      "source_id": "...",
      "author_label": "...",
      "post_url": "...",
      "role_id": "most_factual",
      "sentence": "...",
      "evidence_ids": ["..."]
    }
  ],
  "also_covered_by": [
    {"source_id": "...", "label": "...", "post_url": "..."}
  ],
  "telegram_html": "...",
  "vk_text": "...",
  "claims": [
    {"text": "...", "evidence_post_ids": ["..."]}
  ],
  "risk_flags": [],
  "writer_confidence": 0.0
}
```

Validator проверяет:

- все URLs принадлежат verified cluster;
- все inline authors уникальны либо повтор оправдан;
- `also_covered_by` не пересекается с inline list;
- каждый superiority label имеет role evidence и margin;
- claims покрыты evidence;
- запрещённые/неподтверждённые персональные имена отсутствуют;
- Telegram/VK тексты семантически согласованы;
- topic не заблокирован anti-repeat policy.

## 9. Brand/render strategy

На docs/MVP стадиях не нужна платная generative image pipeline. Сначала используются:

- deterministic SVG/HTML/canvas templates;
- разрешённые source photos;
- source avatars/initials with provenance;
- существующие локальные icon assets с проверенной лицензией;
- автоматические contrast/safe-zone/mobile-preview checks.

AI image generation не является частью базового продукта. Если позднее она понадобится, это отдельное решение по стоимости, правам и визуальной правдивости.

## 10. Обязательный visual QA

Перед publish:

- размер, формат, file size и uploadability;
- mobile screenshot 360–430 px;
- нет обрезанного заголовка, URL или лица;
- contrast/legibility;
- avatar/source/role identity совпадает;
- hero media соответствует теме, а не соседнему посту;
- на каждом source photo есть читаемая прямая ссылка именно на его original post;
- rights policy разрешает каждый asset;
- VK slides имеют стабильный order;
- Telegram caption entities и links валидны;
- отсутствуют дубли авторов и повтор прошлой темы.
