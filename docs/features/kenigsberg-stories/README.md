# Мост в Кёнигсберг / Story Generator

> **Status:** MVP manual Kaggle render ready / Scheduled production not enabled
> **Scope:** Kaggle-based Telegram Story generator for the `Мост в Кёнигсберг` history channel family. The implementation must reuse the CherryFlash runtime, Kaggle upload, native story encoding, and story publish helper wherever possible.

## Как я понял задачу

Нужно быстро запустить MVP генератора коротких исторических сторис: вертикальный ролик `720x1280`, примерно `15-20` секунд основного монтажа плюс outro, без звука из исходных видео, с музыкой из отдельного Kaggle dataset и публикацией в Telegram Stories.

Первые тестовые публикации идут в `https://t.me/keniggpt`. После отдельного сигнала о production-переходе автопубликация должна перейти на сторис канала `https://t.me/mostvkenig`.

Каждый выпуск:

- выбирает один исторический период / dataset;
- берет случайный набор видео из этого периода в случайном порядке;
- выбирает одну мудрую мысль без повторов, пока пул не исчерпан;
- пересказывает ее в сильный, но лаконичный hook;
- раскладывает мысль на `4-6` сцен, при необходимости чуть больше, если музыка и текст требуют более мелкой нарезки;
- подбирает музыкальный участок без слов;
- режет видео по сильным долям / длине ритма;
- делает плавные переходы на `2-3` кадра;
- поверх основного ролика показывает нижний centered watermark `Мост в Кёнигсберг`;
- в конце показывает два outro-экрана:
  - `Мост в Кёнигсберг`
  - `Знай прошлое — строй будущее`

Текстовая анимация должна быть в BBC-подобной типографике: stripe-блоки выровнены по левому краю, stripes появляются слева направо, затем текст выезжает снизу внутри каждого stripe вверх; исчезновение идет в том же порядке, быстрее, через ease.

## Canonical assets

### Video datasets

- `zigomaro/koenigsberg19191940` — период `1919-1940`.
- `zigomaro/koenigsberg-winter` — отдельный зимний dataset; если выбран он, все сцены берутся только из него.

Новые datasets могут добавляться позже. Генератор не должен иметь hardcoded списка файлов: при каждом запуске он читает актуальные mounted dataset files и включает новые видео в выборку.

Dataset period selection is weighted by the current number of video files inside each mounted dataset. On 2026-05-13 the latest Kaggle dataset counts were `koenigsberg19191940=20` and `koenigsberg-winter=19`, so the effective probabilities were about `51.3%` / `48.7%`; the distribution will automatically shift as more footage is added.

### Music dataset

- `zigomaro/koenigsberg-music`

Начальный whitelist участков без слов:

| Track | Allowed instrumental ranges |
| --- | --- |
| `The promise.flac` | `3:44-4:26`, `6:42-end` |
| `Wyatt Earth` | `0:00-1:48` |
| `Save Me` | `0:00-0:38` |
| `Manuela` | `3:03-3:37` |
| `One Truth` | `1:36-1:50` |
| `Terminal` | `3:40-4:26` |
| `Elegy` | `6:18-6:46` |

Музыка выбирается случайно, но с низкой вероятностью повтора прошлого выпуска. История использования треков должна деприоритизировать recent picks на `2-4` недели, но не банить навсегда.

Allowed instrumental ranges are hard constraints for the full encoded story, including outro. A track without a configured matching range, or a range shorter than the final video duration, must be skipped/fail closed instead of falling back to the whole track.

Если в `zigomaro/koenigsberg-music` добавляются новые треки, они попадут в ротацию только вместе с разрешенными безголосыми диапазонами. Диапазоны можно держать в dataset-файле `music_ranges.json` или `kenigsberg_music_ranges.json`:

```json
{"tracks":{"Fresh Track.flac":[["0:10","0:50"],{"start":"1:20","end":"1:55"}]}}
```

Это сознательная граница: неизвестный трек без whitelist-диапазона не должен случайно попасть в историю целиком или с голосом.

### Thoughts source

Канонический список исходных мыслей: `docs/features/kenigsberg-stories/thoughts.md`.

Пул должен работать как shuffle-bag: мысль не повторяется, пока все мысли не были использованы. После полного цикла bag сбрасывается и начинается новый круг. Мысль считается использованной только после успешной регистрации `kenigsberg_issue_manifest.json`; failed Kaggle launch/render не должен вынимать мысль из пула.

## Reuse from CherryFlash

MVP должен быть sibling product к CherryFlash, а не новым независимым механизмом.

Переиспользовать напрямую:

- Kaggle session dataset pattern из CherryFlash:
  - per-run dataset `kenigsberg-session-*`;
  - `payload.json`;
  - `bundle_manifest.json`;
  - runtime scripts/assets копируются в dataset, а не подтягиваются из внешнего repo в notebook.
- Kernel handoff / polling / error diagnostics из `video_announce.scenario`.
- `telegram_story_native_hevc_720p_v1` encoding contract:
  - `720x1280`;
  - H.265 / HEVC in MP4;
  - AAC audio `48kHz`;
  - story-safe size under Bot API limit.
- Shared Kaggle story helper:
  - `kaggle/CrumpleVideo/story_publish.py` mounted as `kaggle_common/story_publish.py`.
- Business Stories publish path from `docs/features/telegram-business-stories/README.md`.
- CherryFlash lessons:
  - fail early if dataset assets are missing;
  - inspect Kaggle `SaveKernel` response fields, not only absence of exception;
  - CPU fallback is acceptable after GPU debugging;
  - runtime logs must expose enough non-secret evidence to diagnose story publish.

Variation points for this product:

- video source is historical footage dataset, not event posters;
- text source is the curated final wording in `thoughts.md`, not `/popular_posts` and not an LLM rewrite;
- scene cadence is beat-driven, not fixed CherryFlash scene timing;
- target test channel is `@keniggpt`;
- production Business/story target is `@mostvkenig` after explicit user signal;
- outro text and stripe animation are product-specific.

## MVP pipeline

1. Operator runs `/kenigsberg`.
2. Bot creates a `kenigsberg_story` generation session.
3. Server selects only run metadata that must live outside Kaggle:
   - one thought from the shuffle-bag;
   - deterministic seed / issue id.
4. Server takes the selected `thoughts.md` entry as final copy and asks Gemini lite to split it into readable `scene_lines[]` without rewording, deleting facts, dropping the tail, or changing punctuation. This step retries only the configured Gemini-lite model (`KENIGSBERG_STORIES_TEXT_SPLIT_ATTEMPTS`, `KENIGSBERG_STORIES_TEXT_SPLIT_RETRY_DELAYS_SEC`) and must not use the global `GOOGLE_AI_FALLBACK_MODELS` Gemma fallback. If Gemini lite does not return a validated split, the server makes one explicit fallback call to `KENIGSBERG_STORIES_TEXT_SPLIT_FALLBACK_4O_MODEL` (`gpt-4o` by default) through the existing `ask_4o` path. If both models fail validation, generation fails before Kaggle.
5. Server creates a per-run Kaggle dataset:
   - payload;
   - selected thought metadata;
   - story publish config/secrets if story publish is enabled;
   - runtime scripts;
   - no raw secrets in logs or docs.
6. Kaggle runtime:
   - discovers mounted video datasets and randomly selects one available period inside Kaggle;
   - discovers actual videos in that dataset;
   - probes durations and dimensions;
   - applies bottom crop to remove the `VEO` mark without scaling;
   - chooses a usable music subrange;
   - detects beats / downbeats;
   - builds single-beat and double-beat scene slots;
   - chooses source video subclips that avoid banned source ranges;
   - renders text stripes, watermark, transitions, and two-screen outro;
   - encodes final story video.
7. Story helper publishes the generated result.
8. Server persists generation history:
   - issue number (`kenigsberg #N`);
   - selected dataset / source file / source start-end per scene;
   - generated-video timeline start-end per scene;
   - selected thought id;
   - music track/range/start-end;
   - story publish result.

## Operator commands

`/kenigsberg` is the explicit command surface for this product. It is superadmin-only.

Initial command set:

- `/kenigsberg` — start a manual production Kaggle generation; production story publishing is enabled in code, not by a feature ENV flag.
- `/kenigsberg status` — show next issue number, thought-pool status, known issues and ban count.
- `/kenigsberg bans` — list source-video bans.
- `/kenigsberg unlock` — mark the latest stuck Kenigsberg `local:*` pre-handoff session as `FAILED` so a new manual run can start.
- `/kenigsberg ban #15 1-3, 7, 16-17` — map seconds from generated issue `#15` back to source-video coordinates and ban them for future cuts.
- `/kenigsberg bans reset` — clear bans for testing.

`/a` is the natural-language control layer over the same command surface, not a separate state machine. The assistant must route requests such as:

- `в выпуске kenigsberg #15 бан 1-3, 7, 16-17`
- `покажи баны kenigsberg`
- `статус kenigsberg`

into concrete commands and ask for confirmation before execution:

- `/kenigsberg ban #15 1-3, 7, 16-17`
- `/kenigsberg bans`
- `/kenigsberg status`

This keeps the risky operation auditable: the user sees the exact command before `/a` executes it.

Manual launch UX contract:

- `/kenigsberg` must answer immediately before admin/DB preflight, before checking active video sessions, preparing text, creating a Kaggle dataset, or calling Kaggle.
- The long preflight/Kaggle handoff runs in a background task and reports follow-up statuses as separate messages.
- The active-session gate is scoped only to `profile_key=kenigsberg_story`; CherryFlash, CrumpleVideo, and stale default video sessions must not block a manual Kenigsberg launch.
- If another Kenigsberg launch is already in the preflight/handoff section, the second command gets an explicit message instead of going silent.

## Beat and cut contract

The preferred MVP implementation on Kaggle:

- Use `librosa` for beat tracking and onset strength:
  - load only the allowed instrumental ranges;
  - compute tempo and beat frames;
  - score candidate windows by stable tempo, stronger onset envelope, and enough beats for `15-20` seconds.
- If downbeat detection is unreliable, treat the strongest beat in each local measure as the visual cut anchor rather than overfitting a fragile music-theory model.
- Build a rhythm grid:
  - the selected audio range may start before a strong beat, so the first visual slot may be partial from audio start to the first detected strong beat;
  - after the first strong beat, every scene cut must land on a detected strong-beat anchor;
  - later slots randomly use only `1x` and `2x` strong-beat spans;
  - target approximately four double-span equivalents before outro;
  - if the strong-beat grid reaches a reasonable story length, the main montage ends on the detected strong beat before outro;
  - if detected strong beats stop too early, preserve the established target story length with a logged target-duration fallback instead of publishing a sharply shortened clip.
- Render logs and manifests must include both raw detected `beat_times`, selected `strong_beat_times`, `rhythm_end_mode`, and any `fallback_reason`, so a generated issue can be audited against the real music segment.
- Transitions are short crossfades / eased opacity blends of `2-3` frames, centered on beat anchors where possible.
- Video source clips may start at a random offset if there is enough usable duration until the chosen cut end.

If beat detection fails or does not produce enough strong-beat anchors, the renderer may use an approximate timing grid to avoid blocking publication, but this must be explicit in the render metadata via `rhythm_end_mode=approximate_fallback` and `fallback_reason`. The fallback is a resilience path, not the preferred rhythm contract.

## Video cut selection strategy

The user preference is for a neural model to choose the most successful source-video fragment after the videos are selected. The implementation should support that as a later strategy, but it is not the fastest safe MVP path.

### MVP: `heuristic_v1`

For the first working generation, source cuts are chosen with deterministic, logged heuristics:

- probe every candidate video with `ffprobe`;
- crop out bottom `VEO` area before composing;
- reject fragments that overlap stored source bans;
- reject fragments with black frames, very low sharpness, or too little motion;
- prefer fragments with enough duration for `1x` or `2x` beat slots;
- apply a recent-use penalty for source files and source ranges used in the last `2-4` weeks;
- write every chosen segment into the issue manifest with:
  - dataset;
  - source file;
  - source start/end;
  - generated timeline start/end;
  - selection score and reject reasons for near misses.

This can be implemented quickly inside Kaggle with `ffmpeg`/`ffprobe`, optional `opencv-python`, and simple scoring. It also gives `/a` bans the exact mapping they need.

Current MVP implementation:

- bot command: `handlers/kenigsberg_stories_cmd.py`;
- Kaggle kernel: `kaggle/KoenigsbergStories/`;
- renderer: `scripts/render_kenigsberg_story.py`;
- test publication target: `@keniggpt` through the existing video poller;
- output manifest: `kenigsberg_issue_manifest.json`, imported by `video_announce.poller` into `setting.kenigsberg_stories_state`.
- before Kaggle launch the bot sends an immediate operator ack, then calls the text LLM only for semantic screen boundaries; `text_source=thoughts_md_llm_split` means the file is treated as final editorial copy and the LLM is allowed to split only, not rewrite; Gemini lite is primary, and `gpt-4o` is the explicit fallback for this small text-boundary task;
- LLM split validation requires that joining `scene_lines` with spaces exactly recreates the selected `thoughts.md` entry after whitespace normalization; overlong lines, missing tails, extra words, changed punctuation, or invalid JSON fail the generation before Kaggle;
- recent source segments from registered issue manifests are passed into the next run as soft temporary exclusions, so the renderer avoids the same recently published source time range when possible but still produces a story if a small dataset has no fully fresh segment; operator `/a` bans remain hard exclusions;
- within one generated story the renderer keeps a strict per-run source-range exclusion map, so if the same source video must be reused it should use a different interval; overlap inside the current issue is allowed only as an emergency fallback when no non-overlapping source interval exists;
- the per-run bundle includes the canonical `thoughts.md` for auditability, while the selected thought text is also copied into `payload.json`;
- Kaggle output keeps the final MP4, `kenigsberg_issue_manifest.json`, detailed `kenigsberg_render_log.json`, runtime bundle and three preview frames only; the full rendered frame sequence is deleted after encoding so output download is deterministic and small.
- period/dataset selection happens inside the Kaggle renderer from actually mounted inputs; the server must not preselect a period or add env switches for dataset choice.
- music selection happens inside the Kaggle renderer from the configured instrumental whitelist; the selected `music_start` and `music_end` must both stay inside the allowed range and be written to the manifest/render log.
- recent music windows from registered issue manifests are passed into the next Kaggle run and used as tiered selection constraints, not only a small score nudge: the renderer first searches for a non-overlapping, non-recent-track, low-voice-risk candidate; only if no such candidate exists does it step down through fresh-track, non-overlap, low-voice, and finally emergency pools. Same-track fatigue grows with recent issue proximity and recent use count so a track like `The Promise` cannot keep winning simply because its raw `voice_risk` score is slightly lower.
- whitelisted instrumental ranges remain hard bounds, but the renderer also computes a best-effort `voice_risk` score for candidate windows and prefers lower-risk fragments when a range contains non-lexical voice/vocalization. Voice analysis is a strong selector input plus logged diagnostic (`music_selection_tier`, `same_track_issue_gap`, `track_fatigue`, `tracks_with_allowed_ranges`); if it cannot run, the renderer still stays inside the whitelist and logs the fallback.
- rhythm slots are seeded from the per-run payload seed and recorded into `kenigsberg_issue_manifest.json` / `kenigsberg_render_log.json` so repeated cadence bugs can be audited from Kaggle output.
- Kaggle rendering still requires explicit validated `scene_lines` in `payload.json`; it must not fall back to slicing raw text on Kaggle. Visual stripe wrapping may use up to 7 rows on the vertical canvas when needed, but it must never silently truncate the tail of a line.
- source scene clips are decoded through ffmpeg into constant `30fps` `720x1280` frame sequences before overlays/transitions are applied; the renderer must not fill normal source-fps gaps by repeating the last OpenCV frame at the end of a scene.
- status updates and Kenigsberg state writes retry short SQLite locks so heavy VK imports do not leave a completed Kaggle run stuck in local `RENDERING`.
- Kaggle handoff writes (`RENDERING`, `kaggle_dataset`, `kaggle_kernel_ref`, and fail-close updates) retry transient SQLite locks; stale `local:KoenigsbergStories` sessions older than the handoff grace window are auto-failed on the next `/kenigsberg`, and can also be manually cleared with `/kenigsberg unlock`.
- Kenigsberg active render locking is profile-scoped: a running CrumpleVideo/default render must not block `/kenigsberg`, but a second Kenigsberg render is still rejected until the first Kenigsberg session finishes or is unlocked.

Notebook guardrail:

- `tests/test_kenigsberg_notebook.py` must pass before deploy: it parses `kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`, rejects literal escaped newlines in code-cell source, and compiles every code cell. This protects the `INC-2026-05-12-kenigsberg-notebook-escaped-newlines` regression where Kaggle failed in cell 1 before rendering.
- The shared `kaggle_common/story_publish.py` helper is bundled for the future production-story path, but the notebook must import/preflight it only when `story_publish.json` is present. Normal manual `@keniggpt` test renders do not need Telethon or story secrets and must not fail because story publishing is disabled.

### Later: `gemma4_clip_judge_v2`

Gemma 4 can be added as a judge after `heuristic_v1` produces a small candidate set. The proposed contract:

1. Deterministic sampler extracts thumbnails / short contact sheets for candidate windows.
2. Gemma 4 receives only a small set of candidate previews plus the thought/hook and returns structured scores:
   - visual appeal;
   - historical mood fit;
   - text-safe negative space;
   - no obvious watermark/defect;
   - reason.
3. The allocator still enforces hard constraints:
   - no source-ban overlap;
   - no recent-window repeat;
   - enough duration;
   - crop-safe composition.

Do not make Gemma 4 scan all raw videos in the MVP. That would require video sampling, thumbnail packaging, quota budgeting, latency control, and fallback logic before the first story can be tested. The fast path is to ship `heuristic_v1`, preserve manifests, and add Gemma as a reranker once real generated issues show where the heuristic is weak.

## Typography contract

Main scene text:

- left-aligned stripe group;
- all stripes share the same left edge;
- each stripe appears left-to-right;
- text moves from the bottom edge of its stripe upward into position;
- disappearance repeats the same order, faster;
- use ease functions with visible inertia, not linear motion: stripe reveal uses an eased/back motion, text reveal starts only after the stripe is substantially visible, and exit uses a faster eased reverse motion;
- keep lines short and avoid text spilling outside the stripe at `720x1280`.
- prefer `3-4` readable stripe rows per text screen; allow up to `7` rows for long curated phrases when that preserves the exact thought. Cutting text with `lines[:N]` or any other silent truncation is forbidden.
- text timing is independent from source-video segment timing. Video cuts follow the music grid; text cues follow comfortable reading durations across the main `15-20` second story.

Outro:

- no bottom watermark during final phrases;
- two sequential screens:
  - screen 1: `Мост в Кёнигсберг`;
  - screen 2: `Знай прошлое — строй будущее`;
- animation reuses the CherryFlash `brand_outro` mechanics: black background, yellow strip blocks, large condensed uppercase type, alternating side slide-in, exponential ease and short fade-in. Kenigsberg adapts the copy into two screens:
  - `МОСТ` / `В КЁНИГСБЕРГ`;
  - `ЗНАЙ ПРОШЛОЕ` / `СТРОЙ БУДУЩЕЕ`.

## Watermark and crop

- During all non-outro scenes show `Мост в Кёнигсберг` at the bottom center.
- The watermark is a copy-protection mark, not a subtitle; it must stay readable but secondary.
- Source videos are prepared for story canvas at `720p`.
- Bottom `VEO` text is removed by cropping height, not scaling the video. The render should compose from the cropped source into the story canvas.
- Some source clips also carry a thin grey bottom strip with light text near the lower black/grey edge. After composition, the renderer masks the bottom `KENIGSBERG_STORIES_BOTTOM_MASK_PX` pixels (default `34`) with a dark/sampled edge color before watermark/text export so this strip is not visible in the story.

Open implementation detail: exact bottom crop in pixels should be measured from the actual dataset files during the first Kaggle smoke and then stored as config, because the datasets may not all carry the mark at identical height.

## Bans and generation history

Command `/a` must support banning bad source fragments after a generated issue is reviewed by routing the request to `/kenigsberg ban ...`.

Examples:

- `в выпуске kenigsberg #15 бан 1-3, 7, 16-17`
- `покажи баны`
- `сбрось баны` for testing

The user provides seconds in the generated video timeline. The system must:

1. find issue `#15`;
2. map each generated-video second range to the scene segments active at those seconds;
3. choose one dominant scene segment per requested range by maximum overlap, treating tiny edge overlaps from whole-second input as operator imprecision;
4. convert that selected generated range to source video coordinates using the persisted source file and source start;
5. store banned source ranges in DB;
6. exclude future random source cuts that overlap banned ranges.

Allowed behavior after a ban:

- use the same source video before or after the banned source range;
- use other videos from the same dataset normally.

The ban model must store at least:

- dataset slug / period;
- source file path inside dataset;
- source start/end seconds;
- reason / issue id;
- created_at;
- optional reset/test flag.

Current implementation state:

- ban parsing and generated-timeline-to-source mapping are implemented in `kenigsberg_stories.state`;
- state is stored in `setting.kenigsberg_stories_state` as JSON for MVP speed;
- a dedicated SQL table is still the expected production hardening step once the generation manifest shape stabilizes.

## Scheduling

Manual `/kenigsberg` came first during MVP testing.

Scheduled production is enabled after the 2026-05-13 approval. The daily slot is `20:10 Europe/Kaliningrad` (`18:10 UTC`): the job is `kenigsberg_story_daily`, publishes a production story to `@mostvkenig`, and is intentionally not behind `KENIGSBERG_STORIES_*_ENABLED` feature flags. Startup catch-up runs the same production launch if the app restarts after today's slot and no scheduled production Kenigsberg handoff exists for the local day.

The time slot should avoid other heavy operations:

- avoid CherryFlash scheduled render window;
- avoid Telegram Monitoring / guide monitoring Kaggle windows;
- avoid daily imports or Smart Update heavy windows;
- prefer a slot after observing actual GPU and CPU runtime in test mode.

Debug phase uses Kaggle GPU. After the rendering path is stable, production should switch to CPU unless measured CPU runtime makes the slot unsafe.

## Telegram publish contract

Testing:

- historical MVP tests published generated MP4s to `@keniggpt`;
- current manual `/kenigsberg` uses the same production story publishing path as the scheduler and sends operational copies/logs only to the operator/superadmin chat.

Production:

- publish stories for `@mostvkenig`;
- Kenigsberg reuses the same shared Kaggle story helper as CherryFlash:
  - `kaggle/CrumpleVideo/story_publish.py` is bundled as `kaggle_common/story_publish.py`;
  - `story_publish.json`, `story_publish.enc`, and `story_publish.key` are written into the same `kenigsberg-session-*` dataset;
  - Kaggle preflights story targets before render and publishes `kenigsberg_story_final.mp4` after render.
- required production story target is the configured Telethon story account itself (`peer=me` in `story_publish.json`);
- `@mostvkenig` fanout is attempted as best-effort `repost_previous`: Telegram may reject channel story publishing with `BOOSTS_REQUIRED` even when the user account is an admin, so it must not block render/publish;
- Kenigsberg must not use the shared Business story allowlist from video announcements. This is a separate history-channel product, not a partner video-announcement fanout; `story_business_targets` is forced to an empty list in code.
- keep Business connection secrets in encrypted cache / per-run encrypted Kaggle story secrets;
- never log raw `business_connection_id`, Telegram user id, bot token, auth bundle, or personal account handles.

Readiness check on 2026-05-12:

- production webhook allowed updates include `business_connection`, `business_message`, and `edited_business_message`;
- `VIDEO_ANNOUNCE_STORY_ENABLED=1` is present in production and Kenigsberg story publishing is active by code path;
- `@mostvkenig` is not required in the local `channel` table for the default story override because the story helper resolves the explicit peer string. If a future path uses `main_chat_id` DB resolution instead, add the channel row first.
- 2026-05-13 production catch-up showed direct `@mostvkenig` Telethon story preflight returning `BOOSTS_REQUIRED`. The next fix keeps the story on the configured Telethon account as the required target and treats `@mostvkenig` repost as best-effort until channel-native story rights are available.

Production reset on approval:

- before the first production compensation run, preserve `source_bans` and issue history, but set `recent_usage_reset_at` and clear `recent_music` / `recent_sources`. This keeps explicit `/a` bans hard while dropping MVP-test recency pressure from source and music selection.

Important boundary:

- Business Stories use Bot API `postStory` and encrypted `business_connection_id`.
- Do not repurpose `TELEGRAM_AUTH_BUNDLE_E2E` or `TELEGRAM_AUTH_BUNDLE_S22` for Business story publishing.

## Open questions

1. What exact Telegram surface should receive test stories: ordinary channel post to `@keniggpt`, Telegram story on a Business-connected account associated with `@keniggpt`, or both?
2. For production `@mostvkenig`, is the required Business connection already cached with `can_manage_stories=True`, or should the implementation include an operator preflight/check command before launch?
3. Should `/kenigsberg` be admin-only and visible in the same admin command family as `/v`, or available to a narrower allowlist?
4. How many thoughts should the initial `thoughts.md` contain before production: the provided 18 are enough for MVP, but scheduled daily use needs a larger pool.
5. Should future editorial changes keep all story copy fully manual in `thoughts.md`, or should an optional LLM suggestion mode exist outside the publication path?
6. What is the desired cadence after production approval: daily, weekdays only, several times per week, or manually curated?
7. What is the acceptable max render time on CPU before the schedule is considered unsafe?
8. Should winter dataset be a normal random period or seasonal-weighted only around winter dates?
9. What exact bottom crop removes `VEO` across the first two datasets? This needs measurement on real samples.
10. Do we need a visible issue number on the final video or only in logs/admin messages?

## Pitfalls

- Beat detection can overfit weak or rubato music. MVP should prefer stable, logged heuristics over fragile downbeat perfection.
- `15-20` seconds plus two outro screens may exceed the emotional pacing if the thought is too long; final `thoughts.md` entries should be edited to fit the format because the publication path no longer rewrites them.
- Kaggle dataset mounts can lag or keep stale sources; reuse CherryFlash dataset readiness and kernel response validation.
- Story publish can fail after successful render due to missing Business rights; required targets should preflight before expensive render when production mode is on.
- New videos/music added to Kaggle datasets can change file order. Selection must be manifest-based and logged, not dependent on notebook filesystem order.
- Video bans require precise timeline mapping. Without persisted per-scene source coordinates, `/a бан ...` cannot be implemented correctly later.
- Cropping out `VEO` by height changes source composition; text/watermark safe zones need visual smoke screenshots.
- Source clips can be lower-fps or variable-fps. If the renderer reads them sequentially into a fixed `30fps` output, scene tails may freeze; keep CFR extraction as the source-frame contract.
