# VK Dynamic Cover

Каноника MVP для автоматической обложки VK-сообщества.

## Цель

- Временно подсвечивать важные фестивали/промо в шапке VK-сообщества, не теряя связь с визуалом `Полюбить Калининград`.
- Генерировать wide cover `1920×768` для desktop VK и mobile slide assets `1080×1920` для будущих живых обложек.
- Давать суперадмину ручной контроль через `/cover`, включая выключение автоматики поверх промо.
- До отдельного approval-flow не менять VK-обложку автоматически: генерация отправляется
  в Telegram админу как proposal без публикации.

## MVP Поведение

- `/cover status` показывает включена ли автоматика, срок активной временной обложки и размер истории.
- `/cover preview` генерирует wide PNG и первые mobile PNG и отправляет их в Telegram как документы без пережатия.
- `/cover request` выбирает до трёх актуальных фестивалей из `Festival`, генерирует wide/mobile pack и отправляет proposal в Telegram админу без VK upload.
- `/cover apply` в текущем MVP намеренно оставлен безопасным alias к `/cover request`: он генерирует proposal, но не публикует VK cover. Прямой upload должен появиться только через отдельный approval-flow с явным подтверждением.
- `/cover save_default` скачивает текущую VK-обложку сообщества и сохраняет её на сервере как дефолт для восстановления.
- `/cover restore` восстанавливает сохранённую дефолтную обложку. Если дефолт ещё не сохранён, команда не подменяет его шаблоном и просит сначала сохранить текущую обложку.
- `/cover on` / `/cover off` включает или выключает автоматику; при `off` промо/cron не должны менять обложку.
- `/cover history` показывает последние смены.
- В сообщении `/cover status` есть inline-кнопки для preview/request/save default/restore/on-off/history/status.
- Scheduler `vk_dynamic_cover_expiry` раз в час проверяет `vk_dynamic_cover_active_until`; после истечения срока возвращает сохранённую дефолтную обложку.

## Дизайн

- Wide cover использует левый editorial brand-block с референс-логотипом из `docs/backlog/features/vk-dynamic-cover/photo_2025-02-02_11-08-25.jpg`.
- Фестивали раскладываются в диагональные панели с badge-периодом, крупным названием, короткой подписью и тонкими акцентными разделителями.
- Палитра единая для всего pack: глубокий editorial dark/navy/green, тёплый акцент только для дат, правил и небольших маркеров. Это снижает визуальный шум и держит обложку профессиональной.
- Mobile assets сохраняют примерно 20% brand-связи через логотип/подпись; первый slide работает как общий editorial cover недели, следующие slides — по одному фестивалю с асимметричной композицией.

## VK Upload

- Wide cover публикуется через `photos.getOwnerCoverPhotoUploadServer` -> upload `file` -> `photos.saveOwnerCoverPhoto`.
- Для upload нужен user token (`VK_USER_TOKEN`, локальный fallback `VK_ACCESS_TOKEN4`), потому что VK photo/upload методы часто недоступны group token.
- Target group: `VK_DYNAMIC_COVER_GROUP_ID`, fallback `VK_EVENTS_GROUP_ID`, затем `VK_AFISHA_GROUP_ID`.
- Сохранение дефолта читает текущую обложку через `groups.getById(fields=cover)`, выбирает самое крупное изображение, нормализует его в `1920×768` JPEG и кладёт в `VK_DYNAMIC_COVER_STORAGE_DIR` (`/data/vk_dynamic_cover` на production, локально `artifacts/codex/vk-dynamic-cover`).
- В текущем proposal-only MVP upload path используется только для `/cover restore`
  сохранённого дефолта и для будущего approval-flow. `/cover request` и `/cover
  apply` VK API не вызывают.

## Festival Data MVP

- Первый целевой набор: `80 историй о главном` и `Кантата`.
- Production-аудит 2026-06-07 показал split между событиями и справочником:
  обе сущности есть в `event.festival` и `festival_queue`, у `80 историй о
  главном` также есть активная `promo_campaign`, но строк в таблице `Festival`
  для `80 историй о главном` и `Кантата` нет. Поэтому `/fest` их не показывает,
  а текущий renderer, который читает `Festival`, не может использовать их как
  полноценные фестивали.
- Причина: `/fest` строится только по таблице `Festival`; `ensure_initial_80_stories_campaign`
  создаёт промо-таргет по future events, но не материализует `Festival`. Авторан
  `festival_queue` на production выключен по умолчанию, поэтому pending rows не
  превращаются в справочник.
- Для MVP нужен targeted light-monitor: брать whitelisted festival labels из
  событий/очереди, материализовать или обновлять `Festival`, подтягивать сайт,
  VK/social links, logo, базовую палитру и период. Запуск должен быть
  ограничен whitelist и `--limit`, без полного прохода по старой очереди.
- Варианты названия `Кантата` (`VI Международный фестиваль классической музыки
  «Кантата»`, `Кантаты`, `Кантата.Россия`) нельзя сливать регулярками вслепую:
  normalizer должен давать confidence и писать evidence, какие события/источники
  привязаны к каноническому фестивалю.

## Kaggle Light Monitor Guardrails

- Существующий Universal Festival Parser сейчас нельзя запускать как основу MVP
  без аудита: код и документация всё ещё помечены как parser version `1.0.0` и
  описывают Gemma 3-27B, тогда как актуальные лимиты/модели GoogleAI уже другие.
- В `kaggle/UniversalFestivalParser/src/rate_limit.py` есть token-bucket limiter,
  но для нового light-flow этого недостаточно: нужны hard `--limit`, `--timeout`,
  dry-run, whitelist фестивалей, сохранение `rate_usage.json` и fail-fast при
  quota/rate-limit errors без бесконечного retry.
- Light-monitor не должен расходовать массовую LLM-квоту, пока для 80/Kantata
  достаточно deterministic enrichment: VK `groups.getById` для логотипа, Pillow
  quantize для палитры, базовые факты из event/festival_queue/source links.


## Daily mobile cover video (design spec)

Status: planned / render-first. This is a new sub-surface of VK Dynamic Cover:
a lightweight daily `1080×1920` video for the mobile live-cover slot of the
VK community. It must reuse CherryFlash infrastructure patterns for Kaggle
handoff/status, but it must not mutate or destabilize the existing CherryFlash
`popular_review` daily video job.

### Product contract

- Generate one mobile cover video per day for `vk.com/kenigeventsofficial`.
- The video is a short editorial recommendation story, not a generic slideshow.
- MVP content: three event recommendations with real posters and fact overlays:
  1. “Сегодня порекомендую вам сходить на органный концерт …” → organ event
     poster enters with a smooth ease-in and a fact card.
  2. “Но если вы любите классику, послушайте Шуберта в Филармонии …” →
     classical event poster + date/time/place.
  3. “А если хочется размять мозги, сходите на квиз …” → quiz poster +
     date/time/place.
- The exact event categories are examples, not hardcoded regex labels. The
  daily story framing must be produced by an LLM-first editorial pass from
  source-grounded event facts; deterministic code may only validate required
  fields, poster availability, timing, and safety constraints.
- If fewer than three strong poster events are available, render a two-event or
  one-event variant. If no safe event pack exists, skip the daily cover instead
  of publishing filler.
- Publication remains render/proposal-only until a live-cover upload path is
  verified against VK UI/API. The existing static wide-cover restore/upload path
  is not proof that mobile live-video cover upload works automatically.

### VK format research snapshot (2026-07-03)

The project has an implemented wide-cover API path for static desktop covers:
`photos.getOwnerCoverPhotoUploadServer` → multipart upload →
`photos.saveOwnerCoverPhoto` per official VK API docs. Public official VK API
pages found during this research document static owner-cover photo upload, not a
confirmed mobile live-video cover upload method.

Consistent public VK live-cover guidance from SMM/design references says mobile
live covers are vertical `1080×1920`, can be a series of up to five images/videos,
and video files should be no longer than 30 seconds, no larger than 20 MB, H.264
video with AAC audio, and roughly 15–60 fps. Because these are not all official
API docs, implementation must keep a stricter internal delivery gate and verify
upload in a real VK community before enabling automation.

Internal delivery gate for generated video:

- canvas: `1080×1920` (`9:16`), no upscale from lower-resolution final renders;
- container: MP4;
- video codec: H.264, `yuv420p`, web/mobile-compatible profile;
- audio: none or AAC low bitrate; the story must be understandable muted;
- frame rate: `30 fps` target (`24 fps` acceptable for lower weight);
- duration: `18–24 s` target, `30 s` hard maximum;
- file size: `<=12 MB` target, `<=18 MB` warning/fail-closed for automation,
  `20 MB` hard maximum;
- `ffmpeg -movflags +faststart` so the file starts quickly after upload;
- safe text zone: keep critical copy in the central column (`x≈120..960`) and
  above the lower VK profile overlay (`y≈220..1420`); bottom content is branding
  or decorative only until mobile-device QA refines the mask.

Sources used for the research snapshot:

- Official VK static cover upload docs:
  `https://dev.vk.com/ru/method/photos.getOwnerCoverPhotoUploadServer`,
  `https://dev.vk.com/ru/method/photos.saveOwnerCoverPhoto`.
- Live-cover format references:
  `https://skillbox.ru/media/marketing/zhivye-oblozhki-vo-vkontakte-instruktsiya-po-primeneniyu-i-22-primera-dlya-vdokhnoveniya/`,
  `https://targbox.ru/blog/vkontakte/razmer-life-oblozhki-dlya-soobschestva-vkontakte/`,
  `https://www.canva.com/ru_ru/sozdat/zhivaya-oblozhka-vkontakte/`.
- Typography reference supplied by the user: `https://vk.ru/wall868977531_1`.
  Browser inspection without login showed a black vertical video thumbnail with
  centered stacked typography: small date, very large condensed numeral, then a
  short stacked title. Local evidence from the inspected page is saved as an
  ignored research artifact at
  `artifacts/codex/vk-mobile-cover-reference/vk_ref_mobile.png`. Use the
  hierarchy, restraint, and central alignment as inspiration, not as a pixel
  copy.

### Storyboard v1

Target runtime: ~20 seconds.

1. `0.0–1.2` — brand/opening: dark editorial background, soft halo bloom, small
   `Полюбить Калининград`/date lockup.
2. `1.2–4.8` — recommendation 1 text appears in two strong typographic beats:
   `Сегодня` → `органный концерт`. Poster enters from scale `0.92` to `1.0`
   with `easeOutCubic`, masked by a warm halo wipe. Fact strip: date, time,
   venue.
3. `4.8–5.8` — poster dissolves through the halo; background gradient rotates.
4. `5.8–10.0` — recommendation 2: `Любите классику?` → `Шуберт в Филармонии`.
   Poster/fact card uses a cooler palette and slightly different motion so the
   story feels curated, not templated.
5. `10.0–14.2` — recommendation 3: `Размять мозги?` → `Квиз сегодня/на неделе`.
   Motion is more energetic, but still readable.
6. `14.2–18.5` — recap: three compact poster chips with dates/venues and one
   CTA line: `Выбирайте событие дня` / `Больше — в Полюбить Калининград`.
7. `18.5–20.0` — quiet branded hold so VK looping does not cut on a noisy frame.

### Visual system

- Background: dark base plus slow moving halo fields, aurora/perlin gradients,
  and local glow wipes behind poster entrances/exits.
- Typography: condensed bold for big nouns/numbers, neutral readable sans for
  fact strips. Existing project references: Cygre/engagement-card palettes from
  `afishaengagement.py` and the current VK Dynamic Cover editorial palette.
- Posters: large enough to be recognizable, never full-screen unreadable noise;
  add a subtle dark scrim or glass plate behind fact text when poster contrast is
  high.
- Motion: use deterministic easing curves (`easeInOutCubic`, `easeOutBack` only
  for small accents), avoid jitter, avoid frame duplication, and render from a
  stable image sequence before final encode.
- Accessibility/readability: no long sentences on a single frame; each text beat
  should be readable in under 1.5 seconds and remain legible on a phone preview.

### Selection and narration

Selection input should reuse event-quality constraints already used by
CherryFlash and VK publishing:

- future/current event, not cancelled, not sold out when ticket status is known;
- title, date/time and venue are present and source-grounded;
- poster is renderable from a stable cached or source URL;
- no duplicate event/source/poster in the same cover;
- prefer a balanced editorial pack (music/classical/intellectual/family/etc.)
  over three visually identical events.

Narration should be a separate LLM-first writer step with a compact schema:

```json
{
  "date_label": "сегодня | завтра | на выходных | 5 июля",
  "beats": [
    {
      "event_id": 123,
      "hook": "Сегодня — органный концерт",
      "bridge": "для тихого вечера в соборе",
      "fact_line": "19:00 · Кафедральный собор"
    }
  ],
  "cta": "Больше идей — в Полюбить Калининград"
}
```

The renderer must treat LLM text as display copy only after validating that every
fact line matches canonical event fields. If validation fails, regenerate copy or
fall back to deterministic fact labels.

### Existing VK Dynamic Cover placement mechanics

The current static cover implementation is useful as placement prior art, but
not as the final video renderer:

- `vk_dynamic_cover.py` defines the exact existing canvases: wide `1920×768`,
  mobile `1080×1920`, and up to `5` mobile cover slides.
- `render_wide_cover()` reserves a fixed `460 px` left brand block, then divides
  the remaining area into up to three diagonal festival/event cells. This is the
  current “main VK image” composition pattern: stable brand identity on the left,
  timely content in angled panels on the right.
- `_draw_brand_block()` keeps the default community visual anchor by compositing
  `docs/backlog/features/vk-dynamic-cover/photo_2025-02-02_11-08-25.jpg`, then
  draws the `ПОЛЮБИТЬ / КАЛИНИНГРАД` lockup and subtitle. The mobile cover video
  should preserve this as a short opening/closing lockup instead of occupying
  20% of every frame.
- `render_mobile_covers()` already proves safe vertical layout: first slide is a
  weekly editorial summary, following slides are one item each, with large title
  blocks, top badges, bottom brand line and a strong central reading area. The
  video should animate this hierarchy rather than invent a new microtypographic
  system.
- `_fit_wrapped_text()` and `_wrap_text()` are reusable constraints: every text
  beat in the video renderer should be pre-fit into a known box and should fail
  closed when the title/fact line cannot be made readable.
- `PALETTES`/`MASTER_PALETTE` provide the current dark editorial colors. The
  video halo/aurora background should derive from these colors and from poster
  accent extraction, not from unrelated random gradients.
- Current `/cover request` and `/cover apply` are proposal-only; that safety
  model should be copied for video (`video_request`/`video_preview`) until the
  live-cover video upload path has separate approval and restore evidence.

Implication for the mobile video: start from the existing vertical slide grammar
(brand summary + one item per beat), add time-based easing/halo transitions, and
keep posters/fact strips within the same central safe zones. Do not reuse the
wide diagonal-panel layout directly for `1080×1920` video frames.

### Architecture proposal

Reuse CherryFlash patterns, not CherryFlash global state:

- Server side:
  - add a dedicated scheduled job kind such as `vk_mobile_cover_video`;
  - create a new profile key such as `vk_mobile_cover_video` instead of adding
    more behavior to `popular_review`;
  - build a per-run Kaggle dataset with `payload.json`, poster assets,
    `cover_video_story.json`, status callback config, and renderer scripts;
  - do not include Telegram story auth or lease `TELEGRAM_AUTH_BUNDLE_S22` for a
    VK-only cover render.
- Kaggle side:
  - use a dedicated kernel/runtime path (`kaggle/VKMobileCoverVideo/`) or a
    dedicated CherryFlash-derived kernel, so concurrent cover renders cannot
    detach the active `zigomaro/cherryflash` dataset sources;
  - emit `vk_mobile_cover.mp4`, `cover_publish_report.json`, `ffprobe.json`, and
    a small JPEG preview frame/contact sheet;
  - use the same status heartbeat/final-report pattern as CherryFlash.
- VK publication:
  - phase 1: render and send proposal to Telegram admin; no automatic live-cover
    upload;
  - phase 2: after VK upload mechanics are verified, add an explicit transport
    such as `vk_mobile_live_cover`, separate from `vk_wall`, `vk_wall_story`, and
    static `photos.saveOwnerCoverPhoto`;
  - store previous/default live-cover state before publishing and provide manual
    restore in `/cover` history.

### Scheduling

Planned env knobs:

- `ENABLE_VK_MOBILE_COVER_VIDEO_SCHEDULED=0|1`
- `VK_MOBILE_COVER_VIDEO_TIME_LOCAL=09:30`
- `VK_MOBILE_COVER_VIDEO_TZ=Europe/Kaliningrad`
- `VK_MOBILE_COVER_VIDEO_PROFILE=vk_mobile_cover_video`
- `VK_MOBILE_COVER_VIDEO_PUBLISH_MODE=proposal|live`

Daily closure rule: the job is successful only when the current-day artifact is
rendered and either proposal delivery or verified live-cover publication is
recorded. A Kaggle handoff alone is not enough.

### Reuse and gaps found in the current project

Reusable now:

- `vk_dynamic_cover.py` already generates wide `1920×768` and mobile
  `1080×1920` static proposals, keeps default-cover state, and exposes `/cover`.
- `handlers/vk_cover_cmd.py` already gives the manual admin surface for status,
  preview, restore and history.
- `scheduling.py` already has `vk_dynamic_cover_expiry` and CherryFlash daily
  scheduler patterns.
- CherryFlash (`video_announce/scenario.py`, `video_announce/story_publish.py`,
  `kaggle/CherryFlash/`, `scripts/render_cherryflash_full.py`) already proves
  the per-run Kaggle dataset, status, video render and VK token-secret pattern.
- `kaggle/CrumpleVideo/story_publish.py` already has VK `video.save`, `wall.post`
  and story upload helpers for social surfaces.

Gaps before implementation:

- No code path currently uploads or manages VK mobile live-cover video slots.
- Existing `/cover apply` is intentionally proposal-only for wide/static covers;
  it must not become silent live-video publishing without an approval state.
- CherryFlash uses a shared Kaggle kernel/dataset-source mutation path; a cover
  job needs a separate kernel or strict serialization to avoid corrupting the
  daily CherryFlash run.
- The VK live-cover video API/upload mechanics must be proven with a real
  community before `PUBLISH_MODE=live` is allowed.

### Implementation lanes

1. **Research/prototype gate** — confirm VK live-cover upload mechanics and
   whether it is API-accessible or UI-only; document exact request contract.
2. **Renderer MVP** — deterministic 1080×1920 video renderer with three poster
   beats, halo transitions, ffprobe/file-size gate and local sample artifacts.
3. **Selector/writer** — event pack selection plus LLM-first narration schema and
   fact-validation fallback.
4. **Kaggle runtime** — dedicated dataset/kernel path with status heartbeat,
   artifact download and no Telegram session lease.
5. **Admin/proposal flow** — `/cover video_preview` or `/cover video_request`
   sends mp4 + preview stills to Telegram without publishing.
6. **Live publish flow** — only after approval: explicit `vk_mobile_live_cover`
   transport, saved default live-cover state, history, restore and tests.

### Verification checklist

- Unit selection tests: no sold-out/past/missing-fact events; no duplicate
  posters; balanced fallback when fewer than three events exist.
- Prompt tests: LLM copy cannot introduce ungrounded venue/date/time; rejected
  copy falls back to canonical fact labels.
- Renderer smoke: `1080×1920`, target duration, H.264/yuv420p, AAC-or-muted,
  `+faststart`, file size under target, readable safe-zone frame samples.
- Kaggle smoke: one-run dataset contains current payload/posters, status
  callback reaches terminal state, output artifacts are downloadable.
- VK publish mock tests: new cover transport does not call `vk_wall`,
  `vk_wall_story`, Telethon, or static wide-cover upload by mistake.
- Live preflight before enabling automation: save current/default cover state,
  upload a test live cover manually or through the verified API, inspect mobile
  display, then restore default and record evidence in artifacts.

## State

MVP не добавляет таблицы и хранит состояние в `setting`:

- `vk_dynamic_cover_enabled`
- `vk_dynamic_cover_active_until`
- `vk_dynamic_cover_last_state`
- `vk_dynamic_cover_history`
- `vk_dynamic_cover_default_state`

## Ограничения

- Mobile live covers в MVP только генерируются и отправляются в preview. Их автоматическая публикация в VK намеренно не включена до отдельной проверки стабильного API/прав доступа для live cover slots.
- Извлечение логотипов фестивалей из VK/сайтов и видео/футажей остаётся следующим этапом. Сейчас используются текстовые фестивальные панели и имеющиеся `Festival` metadata.
- Wide cover proposal не считается согласованной или опубликованной обложкой,
  пока нет отдельной кнопки approve и записи approval-state.

## Проверки

- Unit: `tests/test_vk_dynamic_cover.py`.
- Live smoke: `/cover save_default`, `/cover preview`, `/cover request`,
  визуально проверить wide/mobile PNG в Telegram. До реализации approval-flow
  `/cover apply` должен вести себя так же, как request, и не менять VK cover.
  `/cover restore` проверяется отдельно как аварийное восстановление сохранённого
  дефолта.
