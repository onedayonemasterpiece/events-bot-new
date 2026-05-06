# Changelog

## [Unreleased]

- **LLM / Smart Update G4 Full-Surface Sandbox Benchmark**: added `scripts/inspect/benchmark_smart_update_g4_full_surfaces.py`, which copies a production SQLite snapshot into `artifacts/codex/`, recreates selected events through the real `smart_event_update()` create path on `gemma-4-31b-it`, and compares persisted event fields / infoblock, `event_source_fact`, `facts_text_clean`, Telegraph body, `short_description`, and `search_digest`. Current three-fixture evidence (`smart_update_g4_full_surface_benchmark_20260506T094705Z`) has `0` field/infoblock diffs, but only `1/3` accepted fact coverage and unacceptable latency/provider stability.
- **LLM / Smart Update And Event Topics Gemma 4 Defaults**: `SMART_UPDATE_MODEL` and `EVENT_TOPICS_MODEL` now default to `gemma-4-31b-it` instead of the unavailable Gemma 3 model. Event topics no longer inherit `TG_MONITORING_TEXT_MODEL`, keeping the Smart Update persistence benchmark on the same Gemma 4 runtime path instead of disabling topic classification.
- **LLM / Smart Update G4 Benchmark Coverage Surfaces**: `scripts/inspect/benchmark_smart_update_g4_stages.py` now reports candidate logistics / infoblock facts separately from `facts_text_clean`, carrying logistics-like raw facts plus canonical date/time/venue/address/city/free/ticket/price/Pushkin-card fields into the fact-coverage payload. Reviewer provider failures are now surfaced as `review_error` instead of being normalized into a false `accepted` verdict.
- **LLM / Smart Update G4 Prod-DB Benchmark**: `scripts/inspect/benchmark_smart_update_g4_stages.py` now runs multi-fixture staged comparisons from a local production SQLite snapshot via `--prod-db --event-ids`, using frozen Gemma 3-era production `description` / `short_description` / `search_digest` / `event_source_fact` as baseline and source evidence only for the Gemma 4 candidate. Reports now fence generated markdown descriptions, include baseline/candidate Telegraph previews with infoblock/search/description, checkpoint after each completed fixture, and run the final writer as primary `Gemma 4` (`writer.final_g4_primary`, `four_o_calls_observed=0`) while keeping the old 4o-primary branch commented for rollback.
- **LLM Gateway / Google GenAI SDK Migration**: `GoogleAIClient` now prefers the current `google.genai` SDK for provider calls and keeps deprecated `google.generativeai` only as a lazy compatibility fallback. The Smart Update G4 benchmark routes Gemma JSON stages through this shared gateway instead of direct legacy SDK calls, and benchmark imports no longer emit the `google.generativeai` deprecation warning.
- **LLM / Smart Update G4 Frozen Baseline Benchmark**: `scripts/inspect/benchmark_smart_update_g4_stages.py` now supports `--reuse-baseline-artifact`, loading preserved Gemma 3 baseline facts/text/timings from `artifacts/codex/lollipop_g4_benchmark_20260501T212029Z.json` or a previous single-fixture staged artifact instead of trying to rerun the now-unavailable Gemma 3 baseline. The generated staged report records `baseline_path=frozen_current_smart_update_baseline` and `baseline_source_artifact=...`; candidate generation remains baseline-free.
- **LLM / Smart Update G4 Single-Fixture Recovery**: tightened the G4 create-bundle prompt so object/craft exhibition facts preserve source-local qualities (technique, secret production details, visual signature, diversity/uniqueness, freedom of execution, typical plots/forms) and derived-field prompts avoid CTA/promo register. The staged benchmark now reuses valid bundle `short_description` / `search_digest` before spending extra Gemma calls, and falls back to a compact structured Gemma 4 writer payload when final `4o` is unavailable. Latest `RED-COSMOS` evidence (`artifacts/codex/smart_update_g4_stage_benchmark_20260502T145722Z.{md,json}`) reaches accepted gates for facts (`12/12` baseline covered, `0` lost), final description (`747` chars, `4` semantic headings, `7` bullets, no writer validation errors), and derived fields; latency remains a warning (`131.30s` vs `36.98s`, ~`3.55x`) and 4o still returns `429 insufficient_quota`.
- **LLM / Smart Update G4 Stage Benchmark**: added `scripts/inspect/benchmark_smart_update_g4_stages.py`, a single-fixture staged benchmark for the real Smart Update create path. It compares current Smart Update `Gemma 3` baseline with `Smart Update G4 variant 2 + lollipop-light` across source evidence, create bundle fields, raw facts, `facts_text_clean`, lollipop bucket/weight/lead/layout/writer_pack, final writer, derived `short_description`/`search_digest`, fact coverage, and per-stage timings. First evidence (`artifacts/codex/smart_update_g4_stage_benchmark_20260502T072137Z.{md,json}` on `RED-COSMOS-7902`) shows: raw facts `12 vs 12`, grounded baseline coverage `10/12` with two minor losses, lollipop-light writer_pack restored three semantic headings, final Gemma 4 writer fallback produced a non-empty structured text after `4o` returned `429 insufficient_quota`, but derived fields were empty and latency regressed hard (`36.98s` baseline vs `245.23s` candidate; biggest blockers `create_bundle_g4`, `search_digest_g4`, `short_description_g4`). Status: diagnostic only, not accepted for rollout.
- **LLM / Lollipop G4 Benchmark Diagnostics**: made `smart_update_lollipop_lab.full_cascade.run_full_cascade_variant` fail open when the final `writer.final_4o` call errors (for example OpenAI `429`). The benchmark now keeps upstream Gemma/lollipop stage payloads, writer pack, timings, and a visible `writer.final_4o.error:*` validation error instead of aborting before writing an artifact. The local `_ask_4o_json` helper now records structured OpenAI HTTP error details (`type`, `code`, message, `Retry-After`, rate-limit headers) and uses bounded jittered retry for retryable 429s, while avoiding blind retry for `insufficient_quota`. Layout cleanup now also drops invalid one-letter/generic headings before writer_pack so a model typo like `s` does not become an exact heading contract.
- **Docs / Smart Update G4 Variant 2 Benchmark Plan**: reframed the Smart Update Gemma 4 migration docs around the real production Smart Update path, not an abstract writer swap. The canonical target now states that the candidate path must remove live Gemma 3 calls, while the final public writer may be either a Gemma 4 writer lane or an explicit final 4o lane chosen by measured output quality. Documented the selected next path as variant 2 (`G4 parity Smart Update + lollipop-light description`) and pinned the risk that lollipop layout/writer_pack must not lose baseline writer strengths (epigraphs, semantic headings, lists, style, logistics separation). Added a professional stage-by-stage benchmark protocol: source evidence parity, field/merge safety, fact extraction coverage, Smart Update buckets, lollipop-light layout/writer_pack, final description, derived public fields, latency/cost, single-fixture gates before five-fixture expansion, and mandatory writer-lane reporting.
- **LLM / Lollipop Legacy Full-Source Fact Benchmark**: corrected the five static `lollipop_legacy.v14` benchmark fixtures to use full Telegram post snapshots instead of shortened source excerpts (`AUDIO-WALK`, `SACRED-LECTURE`, `WORLD-HOBBIES`, `RED-COSMOS`; `PETER-FLEET` was already corrected in the previous pass), and made Telegram public-page extraction target the exact `data-post` via `?embed=1&mode=tme` instead of the first message on `t.me/s/...`. The markdown report now renders source excerpts with a larger limit for manual audit. Gemma 4 extraction prompt was tightened for dense audio-route / lecture / object-exhibition posts, preserving participant-control details, source/methodology, craft/object attributes, and qualitative source-local facts without deterministic semantic repair. The fact-coverage reviewer timeout is now larger and has a benchmark-only 4o fallback for reviewer timeouts; generation payloads remain baseline-free. Latest full-source evidence (`artifacts/codex/lollipop_g4_benchmark_20260501T212029Z.{md,json}`): `55/56` grounded Gemma 3 baseline facts covered, `0` critical/major losses, `3/5` accepted (`PETER`, `SACRED`, `WORLD`), `2/5` partial (`AUDIO` due suspicious/fragmentary G4 facts despite 14/14 baseline coverage; `RED` due one minor craft-quality fact loss). Latency remains not accepted (`8.7x..11.4x` vs baseline), so this is a fact-layer recovery artifact, not rollout evidence.
- **LLM / Lollipop Legacy v14 Fact-Coverage Reviewer**: bumped `LEGACY_CONTRACT_VERSION` to `lollipop_legacy.v14` and added an LLM-first fact-coverage reviewer to the benchmark. After Gemma 4 extracts public/logistics facts and the writer runs, a separate Gemma 4 reviewer reads the source excerpt, the Gemma 3 baseline facts, and the Gemma 4 facts, and judges per-fact grounding, baseline coverage, useful additions, and suspicious facts; output is a structured `coverage_summary` (public/logistics/named_entity/format_topic_program/overall_verdict) plus `lost_baseline_facts[]` / `added_g4_facts[]` / `suspicious_g4_facts[]`. The reviewer is benchmark-only — baseline facts are allowed in its payload, but the extractor and writer payloads still receive zero baseline text/facts (regression-pinned by `test_lollipop_legacy_reviewer_payload_uses_baseline_facts_but_generation_does_not`). A deterministic verdict floor cross-checks the LLM judgement: a single critical loss of a grounded baseline fact, or three+ ungrounded G4 facts, force `rejected`. Markdown reports now expose every fact surface needed for manual audit: raw Gemma 3 `per_source_facts`, filtered baseline `facts_text_clean`, metadata anchors, and exact Gemma 4 public/logistics facts. The `PETER-FLEET-LECTURE-5600` fixture was corrected from a shortened manual excerpt to the full post snapshot; the earlier `T095915Z` fact-coverage artifact is therefore superseded for that fixture. Latest evidence (`artifacts/codex/lollipop_g4_benchmark_20260501T105522Z.{md,json}`): 4/5 fixtures `accepted`, `PETER-FLEET` `partial` because Gemma 4 missed the grounded baseline fact that the доклад is based on service documents and personal-origin sources. No writer prompt was tuned in this pass.
- **LLM / Lollipop Legacy v13 Simplification**: rewrote the `lollipop_legacy` lab variant from the multi-stage `v7..v12` pipeline (per-source extract + enrichment + plan + paragraph-bound writer + repair) to a tight baseline-equivalent Gemma 4 path with a 4o final-writer fallback. `smart_update_lollipop_lab/legacy_writer_family.py` now exposes only `build_extraction_system_prompt`, `extraction_response_schema`, `normalize_extraction_payload`, `merge_extraction_facts`, `build_writer_system_prompt`, `writer_response_schema`, `build_writer_payload`, `apply_writer_output`, `validate_writer_output` (read-only), and `compare_to_baseline` (read-only). All `enhancement` / `enrich_v8` / `plan_v8` / `writer_v7` / `source_writer` / `source_fact` stages and helpers (`_baseline_fact_list`, `_legacy_event_fact_floor`, `_legacy_required_fact_floor`) are removed. `scripts/inspect/benchmark_lollipop_g4.py::_run_lollipop_legacy_variant` is now: per-source Gemma 4 extraction → single Gemma 4 writer → 4o final-writer fallback when Gemma 4 writer times out / errors / returns empty. No baseline text or facts enter any generation payload (Gemma 4 or 4o), no repair pass, no source-draft fallback, no regex post-processing, and `--legacy-g4-extract` is kept only as a deprecated CLI no-op. `_ask_4o_json` now reads `FOUR_4O_TOKEN` (with `FOUR_O_TOKEN` legacy fallback) and converts Gemma uppercase JSON-Schema types to OpenAI lowercase before calling structured-outputs. Latest evidence (`artifacts/codex/lollipop_g4_benchmark_20260430T201038Z.{md,json}`): `5/5` non-empty legacy outputs, `0` critical validation errors, `0` baseline leakage; `SACRED-LECTURE`, `WORLD-HOBBIES`, `RED-COSMOS` accepted on Gemma 4 alone (speed `0.97x..2.57x`); `AUDIO-WALK` and `PETER-FLEET` succeeded on the 4o fallback after Gemma 4 writer timeouts (speed `3.10x..3.20x`, recorded as warnings, not errors). Status: **partial accepted** — non-empty stability and zero baseline leakage achieved; further work needed on 4o-fallback register and Gemma 4 writer reliability for sparse sources.
- **LLM / Baseline G4 Translation Benchmark**: added a `baseline_g4` benchmark variant for a 100% LLM-first Gemma 4 baseline translation attempt: Gemma 4 native-schema fact extraction, Gemma 4 writer, Gemma 4 reviewer, no Gemma 3 generation fallback, and no deterministic semantic text repair. The harness now renders a Baseline vs Baseline G4 comparison table with chars, quality, validation, speed ratio, and Gemma calls. Current evidence is **not accepted as complete**: `AUDIO-WALK-QUARTER-971` can pass with `errors=0` and reviewer `accepted`, but `RED-COSMOS-7902` remains a blocker due Gemma 4 writer timeout/repetition/dry-copy instability on thin object-list sources.
- **LLM / Lollipop Legacy v7 Gemma 4-only Contract**: replaced the failed `v3` source-draft/repair experiments with a stricter `lollipop_legacy.v7` path: Gemma 4 `facts_v7` extracts source-derived public/logistics facts, Gemma 4 `writer_v7` now returns paragraph records bound to `public_fact_indexes` / `logistics_fact_indexes`, and `description_md` is assembled deterministically without rewriting. Baseline text/facts, source draft, text repair, and fallback are all absent from generation. Added source-span fact checks, source-fidelity comparison against the Gemma 3 baseline, filler/adjacent-stem guards, and `length.below_public_min` for empty public copy. Regression tests cover the two-call Gemma 4-only route, source-fidelity quality delta, and validation behavior. Latest five-fixture evidence (`lollipop_g4_benchmark_20260430T153849Z`) is boundary-correct but **not accepted** as a quality replacement: `5/5` pass latency and `0` repair/fallback, but the stricter contract over-compresses source facts; `AUDIO-WALK-QUARTER-971`, `SACRED-LECTURE-ZYGMONT-3170`, and `RED-COSMOS-7902` still have validation errors, while the remaining rows are too thin for public-copy rollout.
- **LLM / Lollipop Legacy v3 Audit Fix**: corrected `lollipop_legacy` from the baseline-assisted `v2` prototype to a real Gemma 4-only lab candidate. The benchmark generation path now records `generation_uses_baseline=false`, `uses_baseline_fact_floor=false`, `includes_baseline_stage=false`, and `writer_fallback_to_baseline=false`; baseline is comparison-only and never enters source-facts, writer, repair, or fallback payloads. Added Gemma 4 `source_facts.v3`, source-draft timeout fallback, provider `500 Internal` retry handling, and regression tests that fail if baseline facts/description are sent into legacy generation. Current five-fixture evidence (`lollipop_g4_benchmark_20260430T141614Z`) is honest but **not accepted**: `3/5` improved, `2/5` regressed, and `3/5` exceed the `<=3x` latency gate, so `v3` is a corrected boundary plus failing quality baseline, not a pinned release.
- **Docs / Lollipop Legacy Version Pin**: historically pinned `lollipop_legacy.v2` in the canonical funnel doc with git tag `lollipop-legacy-v2` and acceptance artifact `lollipop_g4_benchmark_20260430T115455Z`; after the v3 audit this entry is treated as the baseline-assisted prototype record, not as the Gemma 4-only accepted contract.
- **LLM / Lollipop Legacy Prompt + Quality Tuning**: added a narrator-frame lead ban (`Погружение в ...`, `Знакомство с ...`, `Путешествие в мир ...`, `Прогулка по миру ...`, `Окунитесь в ...`, `Откройте для себя ...`, `Добро пожаловать в ...`, `Приготовьтесь ...`) plus concrete positive lead exemplars to the `lollipop_legacy.v2` writer prompt, so the editor opens with a grounded object/actor/quote/event-action instead of a stock copywriter narrator pose. The objective `compare_to_baseline` guard now treats a narrator-frame opening as a regression when the baseline did not have one, rewards narrator-frame avoidance, and emits soft warnings for `quality.lost_baseline_headings` (≥2 baseline `### ` blocks collapsed to none) and `quality.lost_baseline_epigraph` (baseline `>` opener dropped). Validator now also rejects the recurring Gemma 4 `,, ` double-comma artifact as `text.double_comma`. After the prompt + guard tightening on the same five-fixture pack, every fixture remains `quality_delta_status=improved`, narrator-frame leads disappear in `peter_fleet_lecture` and `red_cosmos`, the dropped `### ` structure is restored on `audio_walk` (1 heading) / `peter_fleet_lecture` (3) / `sacred_lecture` (2), the writer repair pass reliably fixes one observed double-comma artifact in `peter_fleet_lecture`, and all five outputs stay inside the `1.0 < speed_ratio <= 3.0` gate (max 2.84). Captured in `artifacts/codex/lollipop_g4_benchmark_20260430T115455Z.{md,json}`.
- **LLM / Lollipop Legacy Lab**: added and tightened the experimental `lollipop_legacy.v2` benchmark variant for Smart Update copy. It runs the current Gemma 3 baseline first and counts that stage in timing, but now treats non-logistics baseline facts as the mandatory public floor while keeping date/time/ticket/address facts as compact logistics context instead of forcing them into narrative prose. Gemma 4 31b runs a bounded baseline-editor pass with an objective no-worse quality gate, optional repair pass for validation/quality regressions, and baseline fallback only after repair failure or timeout. The benchmark harness supports `--variants baseline,lollipop_legacy`, ships five real-post fixtures, records the stricter `1.0 < speed_ratio <= 3.0` gate, validates style/promo/report leakage plus duplicate word/tail artifacts, and reports `quality_delta_vs_baseline`.
- **Reference / Location Audit**: added checked venue references and aliases for `Бар Ельцин`, `Частная школа Дирижабль`, `Стендап клуб Локация`, `Бар Чилинтано`, `ДК Машиностроитель`, `Виштынецкий эколого-исторический музей`, and Зеленоградский ГЦКИ after the 2026-04-28 fresh-event location audit found missing addresses and prose-fragment venue drift in Telegram imports.
- **Guide Excursions / Source Catalog**: added `@murnikovaT` to the canonical guide monitoring source seed as a personal Kaliningrad guide channel.
- **Incident / VK Smart Update False Skips**: opened `INC-2026-04-28-vk-smart-update-false-skips` after two normal VK events were rejected as `skipped_festival_post` and `skipped_non_event:online_event`; Smart Update no longer treats `онлайн-регистрация` as online-only, festival routing rescues a single concrete event inside a cycle/program from `festival_post` even when the post has bullet-listed materials/conditions, and the event-parse prompt now distinguishes whole-festival posts from `event_with_festival` single events.
- **Incident / CherryFlash Missing Rehydrated Posters**: opened `INC-2026-04-27-cherryflash-missing-photo-urls` after the scheduled CherryFlash loop repeatedly failed before Kaggle because selected events had in-memory rehydrated posters but empty persisted `photo_urls`; selection now writes rehydrated Telegram/VK poster URLs back to the event row before session/render payload creation.
- **CherryFlash / Rehydrated Poster SQLite Lock Guard**: rehydrated poster persistence now retries transient SQLite lock failures and, if the repair still cannot be made durable, skips only that candidate instead of crashing the entire popularity selection or selecting an event that will reload without photos.
- **CherryFlash / Business Story Secret Cohesion**: CherryFlash now co-locates encrypted story secrets with the per-run `cherryflash-session-*` dataset and marks configured Telegram Business targets as blocking/required, so a missing Business connection secret fails preflight before render instead of silently producing an incomplete personal-account fanout.
- **CherryFlash / Stale Kaggle Bundle Guard**: CherryFlash kernel deploys now prune older `cherryflash-session-*` dataset sources, require Kaggle metadata to confirm the fresh session dataset before persisting handoff, and log story runtime config/secrets matching inside Kaggle so stale mounted bundles cannot masquerade as a successful daily run.
- **CherryFlash / Scheduled Runner Visibility**: scheduled `popular_review` now fails `ops_run(kind='video_popular_review')` if the pipeline returns without creating a session, instead of recording a misleading success for a no-op catch-up.
- **Telegram Business Stories / Profile Page Visibility**: Business story posts from CherryFlash and `/check_business` now send Bot API `post_to_chat_page=true`, so accepted stories are intended for the account page/profile story list, not only the active story ring.
- **Incident / Prod Unresponsive During CherryFlash Recovery**: opened `INC-2026-04-27-prod-unresponsive-during-cherryflash-recovery` and made production health/webhook readiness a required closure check for long CherryFlash catch-up validation.
- **Incident / Telegram Monitoring Sticky Skipped Post**: opened `INC-2026-04-27-tg-monitoring-sticky-skipped-post` after `@kraftmarket39/193` was extracted by Kaggle but stored as `telegram_scanned_message.status=skipped` with `events_imported=0`, keeping a valid May 15 lecture out of `/daily` and video-announcement inventory. Telegram Monitoring now reprocesses legacy incomplete skipped/partial/error scan rows when they have no diagnostic reason and no attached `event_source`, while new incomplete skips persist a compact `skip_breakdown` so intentional skips remain terminal. Telegram import also treats a zero extracted ticket price as free even when the LLM returned `is_free=false`, covering `80 историй о главном` posts where Telegram custom emoji `🆓` can be stripped before extraction.
- **Telegram Monitoring / Remote Session Guard**: Kaggle `CANCEL_ACKNOWLEDGED` and `CANCELED` states are now treated as terminal by the shared remote Telegram session guard, so a manually cancelled monitoring kernel no longer blocks the next catch-up as `remote_telegram_session_busy`.
- **Telegram Sources / `@ecoklgd`**: added `@ecoklgd` to the canonical Telegram Monitoring source catalog and fixed `normalize_tg_username()` so public `t.me/...` and `tg://resolve?domain=...` links normalize to usernames instead of being rejected by double-escaped URL patterns.
- **Telegram Business Stories / `/check_business` Tester**: added admin-only `/check_business` that lists every cached Business connection with `is_enabled=True` and `can_manage_stories=True`, labels rows with `@username` (fallback to `hash:<8>` when username is missing), and on click puts the operator into a 10-minute photo session — the next image (photo or `image/*` document, `/cancel` aborts) is uploaded via Bot API `postStory` (`active_period=21600`) on behalf of the chosen partner so the operator can verify a connection end-to-end without waiting for a CherryFlash slot.
- **Telegram Business Stories / Cache Write Hotfix**: `cache_business_connection` no longer crashes on the live aiogram update because `BusinessConnection.date` is now coerced from `datetime` to a unix timestamp before JSON-encoding, so the Fernet-encrypted store is actually written on every webhook event. `load_cached_business_connections` skips entries that fail to decrypt with a warning instead of taking the whole story-secrets build down. The superadmin now receives a DM from the production bot whenever a Business connection is newly cached or its `is_enabled`/`can_manage_stories` state changes (both via the dedicated `business_connection` event and via the `business_message` recovery path), so connection health is visible without inspecting `/data`.
- **Incident / CrumpleVideo Required Story Fanout**: opened `INC-2026-04-26-crumple-story-required-channel-fanout` after the scheduled CrumpleVideo story published only to `@lovekenig` while `@kenigevents` returned `BOOSTS_REQUIRED`; story targets now distinguish the self-account render gate from required channel fanout, so `me` can keep the render alive while missing required channels make final story publish fail instead of ending green.
- **Telegram Business Stories / Existing Connection Recovery**: webhook updates now include `business_message` / `edited_business_message`, and those updates recover and encrypted-cache `business_connection_id` through `getBusinessConnection`, so already-connected Business accounts can be captured without reconnecting the chatbot.
- **CherryFlash / Business Story Target Storage**: personal Telegram Business story targets now come from runtime DB setting `video_announce_story_business_targets` instead of repo env/code, while `story_publish.json` still carries only hash labels and encrypted story secrets carry the Bot API connection data.
- **CherryFlash / Telegram Business Stories Fanout**: scheduled CherryFlash story publish now appends encrypted Telegram Business targets after the existing Telethon channel-story chain, keeps `600s` spacing between all targets, posts Business stories through Bot API `postStory`, and stores only hash labels in config/docs while `business_connection_id` stays inside encrypted story secrets.
- **Codex / Telegram Business Stories Skill**: added the project-local `.codex/skills/telegram-business-stories` playbook for safe Business Bot API story publishing, webhook/capture recovery, encrypted connection-cache handling, and prod verification.
- **Telegram Business Stories / Webhook Contract**: production webhook now includes `business_connection`, startup uses a canonical allowed-updates list, and incoming Business connections are cached only as Fernet-encrypted payloads with hash-only logs so story publishing no longer requires a manual capture window after connection changes.
- **Incident / Prod Slow During VK Daily Catch-Up**: opened `INC-2026-04-26-prod-slow-during-vk-daily-catchup` after a manual full-bot VK daily catch-up attempt on the serving Fly machine slowed `/healthz` and produced `/webhook` proxy errors; recovery now records the safer compensation path and adds a follow-up for a lightweight VK daily catch-up tool/runbook.
- **Incident / VK Daily Message Limit**: opened `INC-2026-04-26-vk-daily-message-limit` after the scheduled VK daily announcement generated a single oversized `wall.post` payload and VK rejected it with `message_character_limit`; VK daily sections now split into bounded chunks via `VK_DAILY_POST_MAX_CHARS` and only mark the slot sent after every chunk returns a VK post URL.
- **VK Auto Queue / Gemma 4 Draft Extraction**: scoped scheduled/manual VK auto-import draft parsing to `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL`, defaulting to `models/gemma-4-31b-it`. The override flows through `vk_intake` into the Gemma event parser only for `vk_auto_queue`; global `/parse`, generic `event_parse`, and Smart Update routing remain unchanged.
- **Codex / Gemma 4 Migration Skill**: updated `.codex/skills/gemma-4-migration-playbook/SKILL.md` with the completed Telegram Monitoring rollout, including `GOOGLE_API_KEY3` isolation, Gemma 4 smell checks, production/import closure evidence, the location-fragment incident lessons, and the stricter closure bar for future Gemma 3 -> Gemma 4 migrations.
- **Telegram Monitoring / Gemma 4 Venue Prompt Contract**: tightened the Kaggle producer `location_name` schema/prompt so Gemma 4 must return a real venue/place name rather than copying prose, speaker bios, schedule commentary, film metadata, ticket instructions, or event descriptions into `location_name`; schedule rescue now passes full-message shared venue context to each day-block prompt so trailing lines like `📍Остров Канта` are LLM-visible for all extracted schedule rows.
- **Incident / Daily Location Fragments**: opened `INC-2026-04-26-daily-location-fragments` after the 08:00 daily announcement exposed Gemma 4 Telegram extraction regressions where prose fragments were saved as `location_name`; Telegram import now drops prose-like venue strings and recovers from `default_location`, known venue aliases/addresses, source text, or OCR, the location reference gained missing venues/aliases, known venue normalization now stores structured `location_name`/`location_address`/`city` instead of the full reference line, daily city hashtags no longer disappear just because a venue name contains an adjectival form like `Светлогорского`/`Калининградского`, and `/daily` splitting now keeps each event card whole in one Telegram post.
- **Incident / Prod Bot Unresponsive After Telegram Monitoring Smoke**: opened `INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke` after the production bot stopped responding to `/start` and `/healthz` during Telegram Monitoring Gemma 4 post-deploy validation; the incident contract now requires immediate escalation on `/healthz` or `/webhook` unresponsiveness, production-safe smoke isolation, runtime log mirror checks before fallbacks, and post-mitigation `/start`/webhook/`ops_run` evidence.
- **Operations / Fly Health Check Config**: production `fly.toml` now uses the active `[[services.http_checks]]` syntax for `GET /healthz` with `interval=15s`, `timeout=5s`, and `grace_period=60s`; the previous `[[services.checks]]` block looked valid locally but was absent from `flyctl config show`, leaving the deployed app without a service-level health check.
- **Operations / Incident Escalation Trigger**: project and incident-management instructions now treat production `/healthz` timeouts, Fly proxy `/webhook` errors, bot non-response to `/start`, and critical scheduled-slot hangs as automatic incident workflow triggers even when the user has not supplied an `INC-*` id yet.
- **Operations / Runtime Log Investigation Workflow**: project instructions now require agents to check the production `/data/runtime_logs` file mirror, active and rotated log files, and runtime log env before claiming logs are unavailable; if the mirror is disabled or retention has expired, agents must say so explicitly and continue with Fly logs, Kaggle output/logs, `ops_run` rows, and `artifacts/codex/` evidence.
- **CrumpleVideo / Story Channel Boosts Hotfix**: production `/v tomorrow` stories now publish first to the authenticated Premium self-account (`me`) and treat `@kenigevents` / `@lovekenig` as `repost_previous` best-effort fanout targets, so Telegram `BOOSTS_REQUIRED` on channel stories no longer prevents the daily video render. Added `INC-2026-04-24-crumple-story-channel-boosts-required` as the regression contract.
- **CherryFlash / Scheduled Handoff And Catch-Up Guard**: scheduled `popular_review` now waits until the CherryFlash run persists a real Kaggle dataset and non-local kernel ref before marking `ops_run` success. Failed local-only handoffs for today's slot now trigger same-day startup/watchdog catch-up, while existing remote handoffs suppress duplicate reruns even if local status later drifts.
- **Guide Excursions / Digest Completeness Follow-Up**: guide digest candidate selection now compares fresh candidates against already published future occurrences via the existing LLM-first dedup stage, so reposted aggregator/source updates do not crowd out truly new cards. Digest candidates also require ISO dates to avoid recurring text like `every Thursday` leaking through SQLite string comparisons. Multi-block Kaggle rescue now passes shared post context into the block Gemma prompt for common booking/contact/price facts, and public identity, dedup, and digest-writer polish are timeout-bounded so they cannot stall digest publication.
- **Guide Excursions / OCR And Multi-Block Rescue Hardening**: fixed the guide Kaggle OCR media path missing its top-level `hashlib` import, which made image OCR fail open with `NameError` on poster/photo posts. Multi-date `announce_multi` extraction now also lets block-level Gemma rescue continue when the full-post Gemma call times out, and caps that broad full-post attempt with `GUIDE_MONITORING_ANNOUNCE_MULTI_FULL_TIMEOUT_SEC` and no same-prompt timeout retry, so one slow broad extract cannot erase or stall all individually scheduled blocks from the same post.
- **Guide Excursions / OCR Observability And Eligibility Guardrail**: guide OCR success/empty/error logs now include source username, source post, media message id, image index, short media hash, text length and OCR signal flags, and Gemma retry logs can carry the same per-call context. Digest eligibility normalization now treats LLM-provided disqualifying reasons such as `tentative_or_free_date` as authoritative false flags even if the boolean field is inconsistent.
- **Telegram Monitoring / Bridge-Lifting Events**: `@klgdcity` bridge lifting notices (`развод/разводка мостов`, `развести/разведут мосты`) are now treated as official city events by the Gemma 4 Telegram producer, with a narrow Gemma rescue-pass plus grounded `@klgdcity` structural fallback for empty or malformed bridge model output; the existing `/daily` bridge notice hook stays behind successful `event_id` creation.
- **Guide Excursions / Emoji-Date Schedule Extraction**: fixed `announce_multi` guide posts whose schedule lines use Telegram keycap emoji digits (`3️⃣ мая`, `1️⃣3️⃣ мая`) instead of plain numerals. The Kaggle runner now normalizes keycap digits only for prefilter/block-splitting schedule anchors, strips decorative separator-only lines from block text, passes normalized `schedule_blocks` / `schedule_anchor_text` to Gemma as reading aids, runs full-post extraction before block rescue, excludes explicitly tentative/free-date blocks from digest readiness, and fails open per block/enrichment call when one Gemma call times out so already extracted schedule lines are not lost. Occurrence materialization remains LLM-first.
- **Guide Excursions / Gemma 4 Digest-Loss Prompt Tuning**: tightened the guide `trail_scout.screen`, `trail_scout.announce_extract_tier1`, block rescue, and `route_weaver.enrich` prompts after `INC-2026-04-23-guide-digest-extraction-loss`, so multi-date schedules preserve each available dated excursion as digest-ready, sold-out lines stay excluded, no-date/on-demand offers no longer default to digest-ready, volunteer cleanup/subbotnik posts stay out of excursion scope, and `title_normalized` stays a stable route identity core. Added prompt-contract regression coverage and a canonical incident record; live Gemma 4 eval artifacts for the production cases are stored under `artifacts/codex/guide-gemma4-incident-eval/`.
- **Telegram Monitoring / Gemma 4 OCR Title Regression Guard**: after forced A/B smoke on the same 16 Telegram posts (`g3cmp095e0785bc` legacy Gemma 3 vs `abfull095ef8e2d2` current Gemma 4), tightened the LLM-first extraction prompt so poster OCR service headings like `НАЧАЛО В ...`, `БИЛЕТЫ`, `РЕГИСТРАЦИЯ`, dates/times, prices, age limits, or venue labels do not replace a real named event from the message caption as `title`; OCR remains authoritative for date/time/venue/ticket details. Targeted smokes `sig10512b518272c` / `sig10512r6cb27e5` showed prompt-only guidance and full-event repair were still ignored on `@signalkld/10512`, so a narrow compact LLM title-review stage now asks Gemma 4 for replacement title/event_type/search_digest only, then applies the LLM-selected title while preserving event count/order and leaving semantic title choice LLM-owned. Targeted validation `sig10512u8402a5b` fixed the regression (`НАЧАЛО В 19:00` -> `Второй Большой киноквиз`, `event_type=квиз`); full forced regression `abfinal095edeb15` on the same 16 posts extracted `14` events with `0` checked leak/ghost/empty-date/bad-date/English-city/English-event_type/unknown/service-heading smells, compared with legacy Gemma 3 `10` events plus `empty_date=1` and `english_event_type=4`.
- **Telegram Monitoring / Gemma 4 Date-Anchor Prompt Contract**: after controlled Kaggle smoke with local `GOOGLE_API_KEY2` mapped into the `GOOGLE_API_KEY3` env path (`tg_g4_key2_as_key3_forced_eval_70b4fc14`, focused extraction-only `tg_g4_key2_as_key3_focused_eval_90e527f5`), tightened the Gemma 4 extraction prompt so non-exhibition single events such as lectures/talks/excursions must not use `message_date` as a fallback event date when text/OCR has no explicit date or relative anchor. `message_date` remains context for resolving explicit `сегодня`/`завтра`/`послезавтра` anchors and for museum/exhibition as-of merge cases; a narrow post-LLM enforcement guard drops non-exhibition rows that still lack a supported date or borrow unanchored `message_date` without rewriting any event semantics.
- **Telegram Monitoring / Gemma 4 LLM-First Prompt Tuning (Local-Only Eval)**: continued the Gemma 4 prompt-quality pass against the curated `TG-G4-EVAL-01..10` pack using local-only `GOOGLE_API_KEY2` (**not production-equivalent**; production Telegram Monitoring still requires `GOOGLE_API_KEY3`). Removed the earlier semantic fallback direction and kept the extraction LLM-first by adding narrow staged Gemma prompts for single invited lectures, named ongoing exhibitions, museum spotlight cards, and schedule chunks. Local full-path checks now produce clean rows for `TG-G4-EVAL-02` (`Космос красного`, no `unknown`), `-03` (Amber Museum lecture with OCR date/time), `-04` (museum spotlight as exhibition card), `-07` (single lecture, no duplicate/venue-only row), and `-10` (positive-control exhibition remains one row). `TG-G4-EVAL-08` now avoids the previous garbage placeholder row and extracts real zoo schedule rows through chunked schedule prompts, but provider `500`/timeout on individual chunks can still create partial recall; production-equivalent smoke is still required before calling quality parity done.
- **Telegram Monitoring / Gemma 4 Prompt-Quality Iter2 Safety-Net (Local-Only Eval)**: kept the deterministic post-LLM safety-net strictly syntax-level: `_sanitize_extracted_events` strips HTML-like tags, trailing `own title/id/type/event/field` meta tails, markdown/comment leaks, placeholder literals such as `unknown`/`n/a`/`none`/`title`, and drops rows that do not have a real title after cleanup. No semantic regex extraction or English→Russian `event_type` normalizer is present.
- **Telegram Monitoring / Gemma 4 Prompt-Quality Hardening Wave (Fine-Tuning)**: audited Kaggle producer prompts against real Gemma 4 output in `run_id=48fa98294333486d94dd0e14785d774f` (84 events across 69 messages) and shipped targeted fixes: `EVENT_ARRAY_SCHEMA` gained `description` guidance on `title`/`city`/`event_type`/`date`/`time`/`location_*`; `extract_events`, exhibition fallback, and all three JSON-fix retry prompts now forbid inline `//`/`#` commentary, meta-commentary, and markdown markers inside JSON values (regression from `@barn_kaliningrad/971` where `title` contained a leaked `(// a single event with multiple dates..."` tail and `city` leaked the Korean token `여러`), forbid placeholder events with empty `title` and empty `date` (regression from 26/84 ghost rows in the full run), forbid the literal string `"unknown"`, require lowercase Russian `event_type` tokens instead of the `exhibition`/`meetup`/`party`/`stand-up` drift we observed, skip fundraiser / video-content / book-review posts without a forward invite, and constrain `city` to the venue city (fixes `Музей Янтаря … Калининград → city="Saint Petersburg"` from a parenthetical museum-origin note). Added a deterministic LLM-output safety-net `_sanitize_extracted_events` that trims leaked `//`/`#` tails, strips stray `**`/`__`/``` ``` ``` markdown markers, normalizes `"unknown"`/`"n/a"`/`"none"` to `""`, and drops ghost rows with no title and no date, so known Gemma 4 failure modes cannot reach Smart Update. Also closed a latent post-migration regex regression: `extract_events`'s `open_call_re` and `anchor_re` guards were shipped as double-escaped raw strings (`r"\\b..."`) in the first Gemma 4 migration commit, so they never matched real text — silently broke open-call filtering and, worse, dropped valid events whose only anchor was a word like `сегодня`/`23 апреля`. Regression contracts extended in `tests/test_tg_monitor_gemma4_contract.py`.
- **Telegram Monitoring / Gemma 4 Eval Pack**: added `tests/fixtures/telegram_monitor_gemma4_eval_pack_2026_04_23.json`, a curated 10-case fixture carved out of full run `48fa98294333486d94dd0e14785d774f`, plus a contract test that pins the real failure families we still care about: thought leak, ghost rows, `unknown` placeholders, city drift, English `event_type`, retrospective/non-event false positives, and one positive-control extraction that future prompt tuning must not regress.
- **Telegram Monitoring / Gemma 4 Runtime Isolation + Timeout Hardening**: Kaggle secrets now ship only the selected Telegram monitoring key (`GOOGLE_API_KEY3`) plus a legacy `GOOGLE_API_KEY` alias pointing to the same value, instead of leaking unrelated Google key pools into the notebook. Fixed empty scoped-key cache handling so missing `GOOGLE_API_KEY3` registry rows stay on the process-local limiter across repeated calls, and added `GOOGLE_AI_PROVIDER_TIMEOUT_SEC` / `TG_MONITORING_LLM_TIMEOUT_SECONDS` with a `45s` Telegram default to cap slow provider calls before Gemma 4 `500/504` stalls can consume a full Kaggle window.
- **Telegram Monitoring / Gemma 4 Full Scheduled Evidence**: scheduled full run `48fa98294333486d94dd0e14785d774f` produced Kaggle output on `models/gemma-4-31b-it` for 45 sources (`messages_scanned=177`, `messages_with_events=69`, `events_extracted=84`) with `GOOGLE_API_KEY3`, no `GOOGLE_API_KEY2`, and no Gemma 3 fallback; recovery import `ops_run id=803` finished `success`, `errors_count=0`, `events_imported=14`. Follow-up post-`45s` smoke `tg_g4_45s_smoke_20260423a` finished through the primary `ops_run id=807` as `success` (`sources_scanned=3`, `messages_processed=3`, `messages_with_events=2`, `errors_count=0`) and confirmed fast fail-open `45s` timeouts without recovery.
- **LLM Gateway / Scoped Key Isolation Fallback**: when a `GoogleAIClient` runtime is scoped to a non-default key env such as Telegram Monitoring's `GOOGLE_API_KEY3`, a missing Supabase `google_ai_api_keys` row no longer silently widens reservation to the shared key pool; the client now falls back to the process-local limiter while still using the scoped env key. Telegram Monitoring's Kaggle key candidate resolver now applies the same rule and logs `action=local_primary_limiter`.
- **Telegram Monitoring / Gemma 4 Live Canary Evidence**: live Kaggle subset run `tg_g4_live_smoke_subset_20260422g` produced `telegram_results.json` on `models/gemma-4-31b-it` (`sources_total=3`, `messages_scanned=2`, `messages_with_events=1`, `events_extracted=4`) and server import/recovery finished successfully (`ops_run` `797` and import-only repeat `798`, `errors_count=0`).
- **Telegram Monitoring / Gemma 4 Prompt Hardening**: tightened the Kaggle `Gemma 4` source-metadata prompt/sanitizer so `suggested_website_url` no longer accepts social/profile links (`Telegram`, `Telegra.ph`, `Instagram`, `VK`, `YouTube`, `Linktree`, `Taplink`, `Boosty`, `Patreon`), and strengthened the structured Telegram extract prompt to merge OCR venue/date/time evidence into the event object, avoid whitespace-only strings, and avoid inventing `end_date` for single-date events. Added regression contracts in `tests/test_tg_monitor_gemma4_contract.py`.
- **Telegram Monitoring / Gemma 4 Kaggle Notebook Entrypoint**: generated Kaggle notebooks now strip the script-only `asyncio.run(main())` / already-running-loop guard and invoke `main()` from a dedicated `nest_asyncio` notebook cell, fixing the live Papermill failure `telegram_monitor.py should not be imported while an event loop is already running`.
- **Telegram Monitoring / Gemma 4 Kaggle Bootstrap Fix**: the generated Kaggle notebook now embeds `google_ai` sources directly into the `.ipynb` bootstrap and the `telegram_monitor` runner still checks kernel root, `/kaggle/working`, and `/kaggle/input` before its first import, fixing the live `ModuleNotFoundError: No module named 'google_ai'` failure on Gemma 4 kernels. Added regression contracts in `tests/test_tg_monitor_gemma4_contract.py`.
- **Telegram Monitoring / Gemma 4 Kaggle Runtime Migration**: Telegram Monitoring Kaggle producer now has a canonical `kaggle/TelegramMonitor/telegram_monitor.py` source synced into `telegram_monitor.ipynb` before push, stages local `google_ai` into the kernel bundle, defaults text/vision to `models/gemma-4-31b-it`, routes structured text/OCR/source-metadata stages through shared `GoogleAIClient` with native `response_schema`, and isolates the producer to `GOOGLE_API_KEY3` / `GOOGLE_API_LOCALNAME3` with explicit fallback only to the legacy bot key/account envs. Local migration contracts are covered by `tests/test_google_ai_client.py` and `tests/test_tg_monitor_gemma4_contract.py`; live Kaggle validation still requires real secrets in the operator environment.
- **Guide Excursions / OCR Through Shared Gemma Gateway**: guide Kaggle monitoring now runs candidate poster/image OCR through the shared `GoogleAIClient` multimodal path (`guide_scout_ocr`) instead of a text-only extraction boundary, threads `ocr_chunks` into `trail_scout.screen.v1`, `trail_scout.*extract*`, and `route_weaver.enrich.v1`, and upgrades the gateway prompt-estimator so multimodal `inline_data` blobs reserve sane TPM without stringifying raw bytes.
- **Docs / Gemma 4 Repo-Wide Status Inventory**: added `docs/reports/gemma-4-migration-status-2026-04-22.md`, a canonical snapshot of all Gemma surfaces actually found in the repo, which migration docs already exist, which paths are already on Gemma 4 (`guide-excursions`), which still default to Gemma 3 (`smart_update`, `event_parse`, `event_topics`, `TelegramMonitor`, `UniversalFestivalParser`, `admin_assist`, `geo_region`, `video_announce`), and where direct `google.generativeai` paths still bypass the shared gateway.
- **Codex / Gemma 4 Migration Skill**: added the project-local skill `.codex/skills/gemma-4-migration-playbook/SKILL.md`, wrapping the successful `guide-excursions` Gemma 4 rollout into a reusable playbook with canonical doc links, proven canary evidence, and regression-check guidance for future migrations.
- **Ops / Prod Tooling Self-Bootstrap Rule**: project instructions now explicitly require agents handling prod-bound tasks to self-bootstrap deploy tooling instead of treating a missing CLI in the current `PATH` as a blocker; release checks must look for user-level installs like `~/.fly/bin/flyctl`, use absolute paths or export `PATH`, and only call the task blocked after a real bootstrap/install attempt.
- **CherryFlash / Published-Only Anti-Repeat Guard**: CherryFlash `popular_review` no longer backfills cooldowned events via `repeat_fill`; the 7-day guard now stays a hard exclusion for already published CherryFlash outputs, and `PUBLISHED_TEST` sessions now stamp `published_at` so cooldown queries and future story-success evidence do not silently miss successful runs.
- **CherryFlash / Recovery Handoff + Admin-Only Service Routing**: restart recovery now gives fresh `local:CherryFlash` handoffs a bounded grace window before failing them as pre-Kaggle orphans, while still failing truly stale local refs closed; normal `start_render()` now persists `notify_chat_id`, and recovery/service diagnostics no longer fall back to publish channels like `test_chat_id`, routing only to explicit operator/admin targets or the resolved superadmin DM.

- **Guide Excursions / Scheduled Digest Partial Gate**: scheduled `full` guide scans now treat Kaggle post-level `partial` results from isolated LLM timeouts/provider errors as non-blocking warnings instead of `result.errors`, so `ENABLE_GUIDE_DIGEST_SCHEDULED=1` can still publish fresh eligible `new_occurrences`; `/guide_report` and completion messages keep the warnings visible, `/guide_runs` now includes `llm_err`, and the Kaggle Gemma wrapper adds bounded retries for `asyncio.TimeoutError` and provider `5xx` without retrying schema/config errors. Local Gemma 4 digest-preview stages (`enrich`, `dedup`, `digest_writer`) now also have bounded per-call timeouts and fall back to existing deterministic content instead of hanging the scheduled digest/catch-up.
- **Video Announce / About-Fill Emergency Bypass**: scheduled `/v tomorrow` compensating reruns can now temporarily set `VIDEO_ANNOUNCE_DISABLE_ABOUT_FILL=1` to fail open past the LLM `about_fill` step on the pre-Kaggle critical path, so an LLM-side stall no longer has to block today’s render recovery.
- **Video Announce / Local-Kernel Recovery Guard**: startup recovery now refuses to resume `RENDERING` sessions that still carry repo-local refs like `local:CrumpleVideo` / `local:CherryFlash`; those sessions are failed explicitly as interrupted-before-Kaggle-handoff instead of getting stuck behind impossible Kaggle `kernels_status` polling, and `/v tomorrow` no longer tells operators that Kaggle has started before render prep actually reaches that handoff.
- **Guide Excursions / Gemma 4 Canonical Eval Pack + Screen Model Swap + Live Kaggle Canary (`2026-04-20`)**: добавлен канонический live-eval pack на 7 реальных постах из Калининградской области (`artifacts/gemma4-migration-2026-04-20/eval_fixture.json`) и harness реальных Gemma-вызовов через production `screen_post` + extract (`run_eval.py`, `verdict.py`). Eval выявил, что `models/gemma-4-26b-a4b-it` non-deterministically зависает ≥120s на длинных русскоязычных reportage-постах под `response_schema`, тогда как `models/gemma-4-31b-it` проходит те же кейсы за 4-5s при одинаковых квотах. Канонический `trail_scout.screen.v1` model routing переведён с `gemma-4-26b-a4b-it` на `gemma-4-31b-it` ([kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py:138](/workspaces/events-bot-new/kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py#L138)). Тот же prompt дополнительно ужесточён двумя `LLM-first` правилами — "trailing-CTA в reportage — это announce/status_update" и "multi-region round-up не материализуется как announce даже если один пункт внутри base_region" — без regex/keyword shortcuts. Итоговое сравнение на канонических кейсах: `Gemma 3 baseline 4/7 pass / 3.53s mean` → `Gemma 4 final 6/7 pass / 4.26s mean / 0 timeouts`, строго `no-worse` с двумя улучшениями (`GE-EVAL-01` announce; `GE-EVAL-05` multi-region round-up больше не материализует out-of-region фестивали). После этого проведена четырёхступенчатая live Kaggle canary (`artifacts/gemma4-migration-2026-04-20/canary_push.py` + `canary_runs/`) через `zigomaro/guide-excursions-monitor` на `TELEGRAM_AUTH_BUNDLE_S22` и `GOOGLE_API_KEY2`: Stage-1 `@tanja_from_koenigsberg`, Stage-2 `@ruin_keepers + @twometerguide`, Stage-3 `+@excursions_profitour`, Stage-4 full `light` smoke (5 sources, `GUIDE_DAYS_BACK_LIGHT=3`). На всех стадиях — `0 llm_error`, `0 schema/provider reject`, `0 Gemma 3 fallback` (screen/extract закреплены на `models/gemma-4-31b-it`), `0 out-of-region materializations`, mean screen latency ~5-9s. Stage-3 подтвердил корректные live-материализации `GE-EVAL-02` (ruin_keepers/5209 announce/inside) и `GE-EVAL-03` (excursions_profitour/917 announce/inside). Изолированный `llm_deferred_timeout` на `twometerguide/2908` воспроизводится только в Kaggle (direct-run 5/5 OK @ ~5s, decision=`announce`); трактуется как Kaggle-runtime transient, не prompt/model regression, и задокументирован как fail-closed false-negative со следующей задачей на runtime hardening Kaggle-gateway. Canonical docs (`docs/reports/gemma-4-migration-research-2026-04-19.md`, `docs/features/guide-excursions-monitoring/README.md`) обновлены с результатом canary и известным исключением; миграция считается production-ready.
- **Guide Excursions / LLM-Owned `base_region_fit` + Gemma 4 Prompt Tightening**: removed the deterministic `_region_fit_label` keyword fallback from the Kaggle guide runner so `base_region_fit` is now fully Gemma-owned; `_clean_occurrence_payload` chains `occurrence → screen → unknown` and only rejects on an LLM-declared `outside`. `trail_scout.screen.v1`, `trail_scout.announce_extract_tier1.v1`, block-level rescue, and `route_weaver.enrich.v1` prompts now explicitly require per-stage ownership of `base_region_fit` against `source.base_region`, matching the README's `LLM-first` policy lock. Canonical docs (`docs/features/guide-excursions-monitoring/README.md`, `docs/reports/gemma-4-migration-research-2026-04-19.md`) updated with the policy fix.
- **Guide Excursions / Empty Digest Bot-Only Ack**: scheduled and manual guide digest publication no longer sends service-only `Новых экскурсионных находок пока нет.` / `Сигналов ... пока нет.` posts into target channels; empty issues are marked as `empty`, channel publish is skipped, and the operator gets a bot-only acknowledgement with the `issue_id` so automation still proves it ran.

### Added
- **CherryFlash / `/v` One-Click Launch**: `/v` now exposes CherryFlash as a dedicated direct-launch button plus a separate channel-settings button, and both the direct callback and `vidstart:popular_review` route now dispatch straight into `run_popular_review_pipeline()` instead of falling back to the generic manual-selection session flow.
- **Media Storage / Yandex Object Storage Posters**: server-side image uploads (`upload_images`, Telegraph cover mirrors, festival/source-page cover uploads) now prefer Yandex Object Storage when `YC_SA_BOT_STORAGE[_KEY]` is configured, Telegram Monitoring passes Yandex credentials into Kaggle, and managed-storage URL detection/cleanup now understand `storage.yandexcloud.net` while keeping legacy `supabase_url/supabase_path` field names for backward compatibility.
- **Agent Workflow / Reuse Audit + Product-Line Guardrails**: `AGENTS.md` now requires a mandatory `reuse audit` before adding new helpers, modes, fallbacks, or parallel pipelines, prioritizes extension of `production-proven` paths, formalizes `common stable capability` vs `variation points` for sibling products, and explicitly forbids both duplicate helper creation and over-generic “super-abstractions” when product lines have different contracts.
- **CherryFlash / CrumpleVideo Bootstrap Parity**: the CherryFlash Kaggle notebook now installs the same shared story-helper runtime dependencies used by `CrumpleVideo` (`opencv-python`, `requests`, `telethon`, `cryptography`) before importing the common `story_publish.py`, preventing notebook bootstrap failures like `ModuleNotFoundError: telethon` while keeping actual story publish gated by config/preflight.
- **CherryFlash / Single-Step Kaggle Dataset Upload**: CherryFlash now uploads each unique `cherryflash-session-*` runtime bundle through one `CreateDataset` call instead of the old bootstrap `CreateDataset` + `CreateDatasetVersion` sequence, because the version-upload stage could stall on prod before any visible `zigomaro/cherryflash` kernel run appeared.
- **CherryFlash / Fast-Fail Catbox Poster Prefetch**: CherryFlash scene/test-poster prefetch now treats `files.catbox.moe` as a best-effort unstable source and uses a much shorter timeout/retry budget before continuing to Kaggle, so dead Catbox poster URLs no longer stall a live run for minutes before upload.
- **CherryFlash / Papermill-Safe Story Preflight**: CherryFlash notebook story preflight/publish now uses the same thread-safe async bridge pattern as `CrumpleVideo`, preventing papermill failures like `asyncio.run() cannot be called from a running event loop` when the shared story helper is mounted.
- **CherryFlash / Future-Only Selection + Kernel Bind Guard**: CherryFlash now excludes past-start events from `popular_review` even when they are still technically ongoing via `end_date`, the intro bundle carries raw `date_iso/time/city/location_name` fields for real date-strip generation, and the video launcher now waits for Kaggle `dataset_sources` metadata to bind the freshly pushed `cherryflash-session-*` dataset before treating the kernel run as started.
- **CherryFlash / Live Runner**: added `scripts/run_cherryflash_live.py` as the canonical live launcher, so real CherryFlash runs can be started through the same scheduled scenario path as production and kept alive until a terminal session status instead of stopping at a manual notebook trigger; for stale local snapshots it can also explicitly reset a stuck `RENDERING` session via `--force-reset-rendering` before launch.
- **Kaggle Client / Response Diagnostics**: Kaggle dataset create/version/delete failures now include the raw Kaggle API response body in the raised runtime error, so incidents like invalid tokens or dataset contract violations stop collapsing into a blind `400/403`.
- **CherryFlash / Common Story Publish Helper Path**: CherryFlash now ships the same Kaggle-side `story_publish.py` helper used by `CrumpleVideo`, writes story config/secret datasets through the shared server-side builder when the profile flag is enabled, and the CherryFlash notebook now runs the common story preflight/publish hooks while keeping the path disabled by default for `popular_review`.
- **CrumpleVideo / Story Gesture Onboarding**: implemented a lightweight 3-step Telegram-stories CTA inside the first folded-paper interstitials, rendered in `Cygre` and composited under the paper mask so the hint stays secondary and disappears naturally during unfold without changing video duration or shot order.
- **CrumpleVideo / Poster Preflight Checklist**: the Kaggle notebook now logs an explicit `Poster preflight` checklist with `✅/❌` lines for remote poster sources and scene readiness plus a final render-readiness summary, so one broken poster no longer reads like a total media loss during `/v` runs.
- **Operations / Repository Workflow**: added a canonical branch/worktree workflow for parallel feature work, prod-safe hotfix isolation, and recovery, and linked it from `docs/README.md`, `docs/routes.yml`, and `AGENTS.md` so dirty local development no longer becomes an excuse for abandoned or unsafe production rollouts.
- **CherryFlash / Full Popular Review Runtime**: added a dedicated `popular_review` runtime path in `video_announce`, including a separate CherryFlash scheduler slot (`ENABLE_V_POPULAR_REVIEW_SCHEDULED` / `V_POPULAR_REVIEW_*`), default publish routing to `@keniggpt`, a CherryFlash-specific Kaggle dataset bundle with mounted runtime scripts/assets, and a full-render notebook path that now assembles the complete daily `2..6` scene release instead of only `intro + scene1`.

### Changed
- **LLM Gateway / Requested Model Head-Of-Chain**: `GoogleAIClient` больше не переставляет все Gemma-текстовые запросы на `gemma-3-27b` как первый hop model-chain; теперь gateway сохраняет `requested_model` первой попыткой, а fallback-модели использует только как хвост, что разблокирует реальный rollout `Gemma 4` для guide monitoring.
- **Guide Excursions / Gemma 4 First-Stage Migration**: guide monitoring now defaults to a Gemma 4 split tuned from the `lollipop g4` research track: `trail_scout.screen` runs on `gemma-4-26b-a4b-it`, while guide extract / enrich / dedup / digest-writer stages use `gemma-4-31b`, keeping the guide-only runtime on `GOOGLE_API_KEY2`.
- **LLM Gateway / Gemma 4 Structured Output Contract**: `GoogleAIClient` now preserves native `response_mime_type=application/json` and `response_schema` for Gemma 4 callers while still stripping legacy JSON knobs for older Gemma models, and it filters `parts[].thought=true` before parsing response text so Gemma 4 hidden reasoning does not leak into JSON pipelines.
- **Guide Excursions / LLM-First Gemma 4 Policy Lock**: canon docs now explicitly lock guide `screen/extract` migration to `LLM-first`: semantic decisions must stay inside Gemma stages, the current `Gemma 4` risk is documented as a legacy prompt-contract mismatch rather than a model-quality downgrade, and regex/keyword shortcuts are recorded as a non-canonical fix path for this surface.
- **CherryFlash / Story Repost Fanout Extension**: CherryFlash `popular_review` keeps the story upload on `@kenigevents`, reposts it to `@lovekenig` after `600` seconds, and now also reposts the already-published story to `@loving_guide39` after another `600` seconds, preserving the shared `repost_previous` publish contract instead of uploading the media a second or third time.
- **CherryFlash / Kaggle SaveKernel Response Validation + CPU Fallback**: CherryFlash no longer treats `api.kernels_push()` as successful just because the Python call returned. The launcher now inspects `ApiSaveKernelResponse` directly, fails on non-empty `error`, retries once on CPU when Kaggle rejects the push with the weekly GPU quota error, and treats `invalidDatasetSources` for the fresh `cherryflash-session-*` bundle as a retryable bind-lag instead of silently logging a stale-kernel “success”.
- **CherryFlash / Non-Fatal Kernel Metadata Bind Check**: the live `#163` incident showed that Kaggle can successfully save and start `zigomaro/cherryflash` while `GetKernel` still exposes only the static story-secret datasets in `dataset_sources`. CherryFlash now treats this post-push metadata bind check as telemetry/warning and continues into normal Kaggle status polling after a successful `kernels_push`, matching the production-proven launch model used by the other working Kaggle paths.
- **CherryFlash / Kaggle Dataset File Pagination**: the `#162` prod incident showed that CherryFlash session bundles can have more than `20` files, so a first-page-only `dataset_list_files` readiness check never saw `payload.json` even though Kaggle had already uploaded the full bundle. Kaggle dataset file inspection now follows pagination and readiness no longer aborts just because `payload.json` landed beyond the first page.
- **CherryFlash / Kaggle Session Dataset Readiness**: the live `#161` incident showed that CherryFlash could `CreateDatasetVersion` and `kernels_push` successfully while Kaggle still had not made the fresh `cherryflash-session-*` dataset bindable, so the kernel metadata never picked up the new session bundle and the run failed as `kaggle push failed`. The launcher now waits for real Kaggle dataset readiness (`dataset_status + dataset_list_files`, requiring `payload.json`) instead of a blind `sleep(15)`, and it persists the actual deployed kernel slug (for example `zigomaro/cherryflash`) immediately after `kernels_push` so recovery/poller paths never fall back to querying the non-Kaggle pseudo-ref `local:CherryFlash`.
- **CherryFlash / Prod Story Autopublish + Repost Fanout**: `popular_review` now enables the shared Kaggle-side story publish path by default, but keeps its target routing local to CherryFlash selection params: first publish goes to `@kenigevents`, then after `600` seconds `@lovekenig` receives a `repost_previous` story instead of a second media upload. The shared story target config/parser now supports that repost mode as an explicit variation point.
- **CherryFlash / Single-File HEVC Telegram Publish**: CherryFlash channel/test publish no longer relies on a separate Telegram-only H.264 sidecar. The final artifact stays a single HEVC mp4 and now uses direct `ffmpeg image2` muxing with `libx265`, `hvc1`, `+faststart`, fixed one-second GOP, repeated headers, and a closed GOP so Telegram can use the same compact release file for normal playback and feed preview.
- **CherryFlash / Story Config Hard-Fail**: when CherryFlash requests story publish, dataset creation now fails immediately if `story_publish.json` was not generated, instead of silently uploading a Kaggle bundle that logs `story publish disabled for this run`.
- **CherryFlash / Kaggle Story Runtime Hard-Fail**: the CherryFlash notebook now also fails closed if the mounted bundle says story publish was requested but the shared story helper or `story_publish.json` is missing inside Kaggle input, preventing another silent mp4-only run after upload/deploy drift.
- **CherryFlash / Story Selection-Param Merge Fix**: the last missing story-publish bug was traced to `_create_dataset()` preferring payload-level `selection_params`, even though `payload_as_json()` intentionally strips them down to viewer/runtime metadata. CherryFlash dataset assembly now merges `session.selection_params` first, so `story_publish_enabled`, `story_publish_mode`, and `story_targets_override` survive into Kaggle bundle creation instead of being silently dropped before `story_publish.json` is written.
- **CherryFlash / One-Pass Telegram-Native Story Encode**: CherryFlash final mode now renders the exact story upload artifact in one pass as `720x1280` `H.265/AAC` (`libx265`, `hvc1`, `30fps`, one-second GOP, `yuv420p`, `+faststart`, `128k` stereo `48kHz` audio) with a compact fixed bitrate budget tuned for the current `~53s` max run shape instead of relying on a second lossy upload-side transcode.
- **CherryFlash / Validator-Only Story Upload Path**: when CherryFlash requests story publish, the shared Kaggle `story_publish.py` helper now validates and reports the final Telegram-native mp4 instead of re-encoding it by default, so production story upload preserves the render’s audio/video quality while still failing closed on non-native media.
- **CherryFlash / Prod Daily Scheduler Enablement**: production `fly.toml` now explicitly enables the independent CherryFlash daily cron slot via `ENABLE_V_POPULAR_REVIEW_SCHEDULED=1` with `V_POPULAR_REVIEW_TZ=Europe/Kaliningrad` and `V_POPULAR_REVIEW_TIME_LOCAL=10:15`.
- **CherryFlash / Direct ffmpeg Image-Sequence Encode + Compact HEVC Final Profile**: CherryFlash no longer relies on `MoviePy.ImageSequenceClip.write_videofile()` for the final mp4. The reproduced “frozen / near-frozen” 2D pairs were traced to MoviePy frame-timing jitter in output PTS, which caused certain decoded frames to map back to the previous source PNG. The final renderer now calls `ffmpeg` directly with `-framerate <FPS> -i frame_%04d.png`, and final publish mode uses a compact HEVC (`libx265` + `hvc1`) profile on that same path so a full intro + `2..6` scene + outro release can target the `<=15 MB` delivery budget without reverting to the duplicate-frame artefact. This remains `Not confirmed by user` until the next manual prod validation run.
- **CherryFlash / Viewer-Facing Date/Location Formatting**: the final CherryFlash renderer now reuses the approved first-scene viewer formatter for `date_line` / `location_line`, so ISO dates and raw address strings no longer leak into the final scenes.
- **CherryFlash / Kaggle Apt Bootstrap Resilience**: the CherryFlash notebook setup now disables the flaky `cloud.r-project.org` jammy CRAN apt source and retries `apt-get update` with explicit apt retry options before installing runtime packages, so transient mirror-sync mismatches no longer fail the run before the renderer starts.
- **CherryFlash / First Scene Beat-Locked Handoff Restore**: the full CherryFlash renderer now reuses the already-approved `SCENE1_START_LOCAL = 1.80` handoff timing from the `intro + scene1` approval path for the first primary 2D scene only, so the cut from 3D lands directly in the visible `move_up` window on the strong beat again; scenes `2+` and follow-up beats still keep the full local timing contract.
- **Operations / Runtime Log Retention**: production can now mirror the root runtime logger into hourly rotated files under `/data/runtime_logs`, with retention capped at about 24 hours through `ENABLE_RUNTIME_FILE_LOGGING`, `RUNTIME_LOG_DIR`, `RUNTIME_LOG_BASENAME`, and `RUNTIME_LOG_RETENTION_HOURS`.
- **CrumpleVideo / MobileFeed Intro Scene1 Final Export**: the `Мобильная лента` `intro + scene1` export path now produces a clean final validation clip under `artifacts/codex/mobilefeed_intro_scene1_final/`, stretched to the new music-locked duration, with a stronger render-quality budget for the Blender intro and the same legacy `Pulsarium.mp3` cue path used by `video_afisha`.
- **CrumpleVideo / MobileFeed Intro Scene1 Approval Export**: added `scripts/render_mobilefeed_intro_scene1_approval.py`, which renders the `Мобильная лента` Blender intro in full `1080x1920`, continues into the first legacy-style `video_afisha` scene, and muxes the same `Pulsarium.mp3` cue path used by the original pipeline so handoff approval can happen on a real `intro + scene1 + music` clip instead of on a draft-only preview.
- **CrumpleVideo / MobileFeed Intro Draft Handoff Preview**: added a canonical `9:16` low-sample preview path for `Мобильная лента` via `scripts/render_mobilefeed_intro_preview.py`, now producing a `~1.9s` Blender intro draft with a matched late-tail cut into `video_afisha`, together with storyboard, frame exports, and late-zoom 2D reference frames for visual verification.
- **LLM / Lollipop G4 Full-Cascade Retune**: the experimental `lollipop g4` lab now keeps all three `KALMANIA` sources in scope for Gemma 4, runs native structured output on extract and merge families, chunk-splits `facts.dedup` with a final reconcile pass, restricts `literal_items` to true program lists, and tightens `writer.final_4o` so rich named casts are less likely to collapse into `и другие` while keeping the architecture `Gemma 4 upstream + final 4o`.
- **CrumpleVideo / MobileFeed Intro Still Pipeline**: added a dedicated Blender still path for the new `Мобильная лента` concept via `kaggle/CrumpleVideo/mobilefeed_intro_still.py` and `scripts/render_mobilefeed_intro_still.py`, using the local `iphone_16_pro_max.glb`, a real stitched poster atlas from the April 6 popularity payload, and a desk-level product-shot camera with post-composited external CTA over the 3D phone/ribbon render.
- **Docs / CrumpleVideo Popular Quick Review**: added a canonical backlog spec and separate design brief for a popularity-driven `/v -> Быстрый обзор` mode with a `2..6` event output cap, `/popular_posts`-based candidate pool, 7-day anti-repeat rule, phase-1 publication to `@keniggpt`, story-ready-but-disabled rollout, and an intro/cover concept pack for approval.
- **CrumpleVideo / Popular Review Blender Still Approval Pack**: added a real Blender-based static intro approval pipeline for the upcoming `popular_review` mode via `kaggle/CrumpleVideo/popular_review_intro_stills.py` plus `scripts/render_popular_review_blender_stills.py`, using the fresh April 6, 2026 prod snapshot payloads and the repo typography stack (`DrukCyr`, `Akrobat`, `BebasNeue`) instead of pseudo-3D paintovers.
- **CrumpleVideo / Popular Review Single-Scene 3D Refinement Pack**: added a second approval track for one-scene low-sample Blender stills via `kaggle/CrumpleVideo/popular_review_single_scene.py` plus `scripts/render_popular_review_single_scene.py`, keeping the approved poster composition as unlit face artwork on real 3D slab cards so intro exploration can focus on premium material/light/shadow before animating the `1.0-1.5s` handoff.
- **LLM / Gemma 4 Benchmark Harness**: added `scripts/inspect/benchmark_lollipop_g4.py` for live `baseline / lollipop / lollipop g4` comparisons with the canonical `Gemma 4 upstream + final 4o` architecture, including native-`system_instruction` attempts with automatic inline-system fallback when the Google Gemma transport rejects developer instructions on older models.
- **CrumpleVideo / Kaggle Story Publish**: `/v` can now publish Telegram stories directly from inside the `CrumpleVideo` Kaggle notebook via Telethon, using encrypted split-datasets for auth delivery, `story_publish.json` for target config, a JSON `story_publish_report.json` output, and a dedicated `kaggle/execute_crumple_story_smoke.py` image-only smoke runner for fast channel checks before a full video render.
- **LLM Gateway / Gemma 4 limiter caps**: added canonical `google_ai_model_limits` seed coverage for `gemma-4-31b` and `gemma-4-26b-a4b`, matching the Google AI Studio project quotas verified on April 6, 2026; Gemma 4 `Unlimited TPM` is now represented in the limiter as the integer sentinel `2147483647` so reserve cannot silently under-cap these models.
- **Docs / Release Governance**: added canonical production release policy in `docs/operations/release-governance.md`, routed it from `docs/README.md` and `docs/routes.yml`, locked the GitHub Fly workflow to checkout and verify `main`, and documented clean-worktree / back-merge rules for manual `flyctl deploy`.
- **Agent Workflow / Requirements Governance + Push Policy**: `AGENTS.md` now requires re-reading canonical requirements docs before implementation, presenting requirement changes as explicit document deltas, stopping for user approval on requirement conflicts, and keeping `origin` regularly synchronized with explicit stage/commit/push hygiene for durable task-related files only; the repo contract now also includes a visible `Requirements Confirmation Gate` (`Confirmed` / `Conflict`, plus `Already present` / `Needs clarification` / `Missing` for requirement-diff tasks), and `docs/tools/codex-cli.md` plus `CLAUDE.md` point to the same workflow contract.
- **Docs / Scheduler Routing**: added explicit scheduler routing to `docs/README.md`, `docs/routes.yml`, and `docs/operations/cron.md`, so schedule changes now have a canonical path: policy in `docs/operations/cron.md`, APScheduler defaults in `scheduling.py`, production overrides in `fly.toml`, and local env examples in `.env.example`.
- **CrumpleVideo / Scheduled Tomorrow Test**: added an optional scheduler job for fully automatic `🧪 /v - Тест завтра`, running the existing `VideoAnnounceScenario.run_tomorrow_pipeline(... test_mode=True)` flow at a configurable local-time slot and sending the finished video to the configured test channel or back to the operator/superadmin chat when no test channel is configured.
- **Guide Excursions / Scheduled Auto-Publish**: scheduled guide monitoring can now auto-publish the `new_occurrences` digest immediately after a successful `full` scan/import when `ENABLE_GUIDE_DIGEST_SCHEDULED=1`, avoiding a separate digest cron slot and keeping publish tied to fresh Kaggle facts.
- **Docs / Guide Kaggle Session Incident**: added a canonical postmortem for the March 16, 2026 Telegram session-boundary incident, documenting why `TELEGRAM_AUTH_BUNDLE_S22` and `TELEGRAM_AUTH_BUNDLE_E2E` are not interchangeable and what runtime/process guardrails now prevent a repeat.
- **Guide Excursions / Source Seed Expansion**: added `@art_from_the_Baltic` to the canonical guide monitoring seed and casebook as a provisional `guide_project` source pending a fuller deep-scan review.
- **Guide Excursions / Guide Profile Enrichment**: added a separate Gemma-only `guide_profile` enrichment pass that materializes grounded public guide name/line, credentials, and expertise tags from `guide_source.about_text` plus sample excursion context into `guide_profile.summary_short` / `facts_rollup_json`.
- **Guide Excursions / Hook + Region-Fit Enrichment**: added two server-side Gemma-only enrich passes for guide occurrences: `main_hook` and `audience_region_fit (locals|tourists|mixed)`, materialized into `fact_pack_json` and `GuideFactClaim` so digest copy can lead with a grounded hook while admin facts remain inspectable.

### Fixed
- **CrumpleVideo / CherryFlash Story Fanout Blocking Rule**: the shared Kaggle `story_publish.py` helper now treats only the first ordered story target as the blocking gate for render/publish success; downstream repost/fanout targets are best-effort by default, so `BOOSTS_REQUIRED` or another target-local failure on `@lovekenig`/later channels no longer aborts video generation or prevents attempts on subsequent targets that can still accept the repost.
- **CherryFlash / Telegram Story Media Profile**: the shared Kaggle story helper now prepares the exact `SendStoryRequest` video as a stricter Telegram-safe `720x1280 H.264/AAC` file (`b:v=900k`, `maxrate=1200k`, `bufsize=2400k`, `bf=0`, `avc1`, `+faststart`, `AAC 128k`) and writes media diagnostics into `story_publish_report.json`, after the `v16-story-native-720p` runs showed that a render can pass preflight but still fail with `MEDIA_FILE_INVALID` on media upload.
- **Operations / Prod Disk Pressure Recovery**: production Fly volume `/data` was exhausted by accumulated sqlite backups plus runtime file logs, which made the bot fail during startup logging with `OSError: [Errno 28] No space left on device`. Old backup artifacts were cleaned from the volume, the bot was restarted successfully, and production `ENABLE_RUNTIME_FILE_LOGGING` is now disabled by default again to avoid another silent disk-pressure outage on the current volume budget.
- **Telegram Monitoring / Gate Venue Grounding**: candidate build now cross-checks extracted gate venues against message text and poster OCR, replacing unsupported extractor guesses with the explicitly grounded gate venue from the same Telegram post when available.
- **Smart Event Update / Sensitive Fact Grounding**: fact-first canonical facts now reject ungrounded linked-source claims about age limits, group-size limits, durations, and concert/music details before they enter `event_source_fact`, `description`, `search_digest`, or `short_description`.
- **Locations / Kaliningrad Gate Spaces**: `docs/reference/locations.md` now explicitly includes `Закхаймские ворота`, `Фридландские ворота`, and `Железнодорожные ворота`, and Smart Update no longer treats bare `Ворота` as a safe alias for Zakheim.
- **Operations / Prod DB Sync CLI Parity + Fast-Fail Diagnostics**: `scripts/sync_prod_db.sh` now uses the current Fly CLI file-transfer path (`fly sftp get`) instead of legacy `fly ssh sftp`, enforces an explicit timeout for both remote snapshot creation and download, and points operators to concrete Fly/WireGuard troubleshooting steps instead of hanging indefinitely on `Connecting to fdaa...`.
- **CherryFlash / Renderable Poster Selection Gate**: `popular_review` selection now rejects catbox-only events unless source-specific rehydrate recovers at least one direct renderable poster URL (`t.me` public-page poster fallback or production `VK wall.getById`), so CherryFlash no longer uploads guaranteed-broken runtime bundles that crash later on missing `assets/posters/*` files.
- **CherryFlash / Intro Poster Candidate Materialization**: CherryFlash selection manifests now keep the full `poster_candidates` list alongside `poster_file`, and the intro runtime can materialize remote poster candidates on Kaggle when local `assets/posters/*` prefetch did not produce a mounted file, preventing `FileNotFoundError` crashes like missing `assets/posters/7fp9nh.jpg`.
- **CherryFlash / Phone Screen Cygre + Count Copy**: the CherryFlash phone-screen UI labels now use plural-safe Russian event counters (`4 события` instead of `4 событий`) and wider `Cygre` weights for both the top count cluster and the bottom city cluster, removing the previous narrow fallback-like look on the phone surface.
- **CrumpleVideo / Story Gesture Timing Alignment**: the CTA onboarding layer now starts on the tail of the preceding paper fold instead of only on `hold_ball + unfold`, so the instruction appears slightly earlier and reads as part of the folded-paper interstitial rather than lagging behind the shot transition.
- **CherryFlash / SQLite Mixed-Access Lock Guard**: `db.raw_conn()` now tolerates a transient `database is locked` failure when reapplying `PRAGMA journal_mode=...`, so the live `popular_review` path can survive the `ensure_access/has_rendering -> /popular_posts raw_conn()` sequence instead of aborting before the CherryFlash session is even created.
- **CherryFlash / Kaggle Audio Mux Compatibility**: the full CherryFlash renderer and the `intro + scene1` approval renderer no longer assume one specific `moviepy` volume-scaling API; both now fall back across `with_volume_scaled`, `volumex`, effect-based scaling, and finally the original audio level, preventing Kaggle notebooks from failing after all scenes have already rendered.
- **CherryFlash / Full Runtime Duration + Outro Guard**: the full CherryFlash assembler now keeps hash-based frame dedupe limited to the intro segment, preserving intended hold frames in later 2D scenes instead of silently shortening and roughening the video, and it now carries the canonical final-brand reference asset through the mounted dataset so the release no longer ends before the branded finish stage.
- **CherryFlash / Viewer-Date Payload Compatibility**: the intro approval loader now accepts either ISO dates or already formatted viewer-facing date strings from the CherryFlash selection manifest, so the Kaggle runtime no longer crashes on real payload dates like `3 мая`.
- **CherryFlash / Low-Event Follow-up + Exact Frame Dedupe**: `payload_as_json()` now expands low-count `popular_review` runs with a follow-up `move-left` scene when an event has a second image, and the full CherryFlash render path now removes exact consecutive duplicate frames before muxing audio, shifting the audio offset only when duplicate removal happens before the first approved `move_up` beat anchor.
- **CherryFlash / Animated Outro + Palette Match**: the full CherryFlash renderer now reuses the proven `Video Afisha` three-line slide-in outro grammar instead of ending on a static card, while keeping the shared black video background and inverting only the strip colors to the `Final.png` family (`yellow strips + dark typography`).
- **CherryFlash / 2D Primary Cadence**: the full CherryFlash renderer now keeps the original gentle post-`move-up` drift for primary 2D scenes, but renders it through the subpixel frame compositor instead of the integer-snapping MoviePy composition path, removing the “effective 15 fps / every-second-frame” cadence without flattening the motion into a static hold.
- **CherryFlash / Phone Label Aspect**: the mobile-feed phone-surface label textures now match the real on-phone plane aspect so `Cygre` copy is no longer horizontally squeezed into a narrow fallback-like look.
- **CherryFlash / Audio Encode Bitrate**: the full CherryFlash export and the `intro + scene1` approval mux now encode AAC audio at `192k` instead of `96k`, avoiding the previous audible quality drop relative to the `Pulsarium.mp3` source.
- **CherryFlash / Kaggle Session Bundle Resolution**: the CherryFlash notebook bootstrap no longer recursively grabs the first `scripts/render_cherryflash_full.py` under `/kaggle/input`; it now resolves the newest mounted top-level `cherryflash-session-*` bundle deterministically, preventing a fresh rerun from silently rendering an older attached dataset like `session-142` instead of the newly uploaded runtime.
- **CherryFlash / Live Popular Payload + Bottom City Label**: `kaggle/execute_cherryflash_intro_scene1.py` now builds the runtime dataset from the real `/popular_posts` selection flow (`24h -> 3d -> 7d`) using the latest prod snapshot, downloads fresh poster assets into `assets/posters/`, writes `assets/cherryflash_selection.json` for the mounted render bundle, and can locally reconstruct the final mp4 from Kaggle-exported frames if Kaggle omits the container file; the phone-screen bottom geography label also switched from `Cygre Book` to denser `Cygre Medium` so city copy no longer collapses into an overly narrow read.
- **CherryFlash / Requirements Governance + Live Payload Contract**: aligned the canonical CherryFlash docs with the current requirement set (`24h -> 3d -> 7d` popularity traversal, readiness by `12:30 Europe/Kaliningrad`, testing publish to `@keniggpt`, no default Gemma-heavy-job blocking) and imported the stronger requirement-governance policy into `AGENTS.md`, so feature work now has an explicit `Already present / Needs clarification / Missing` gate before implementation.
- **CherryFlash / MobileFeed Intro Motion Cadence**: restored the `CherryFlash` Kaggle bundle in the main workspace, switched the Kaggle runner to the same one-run dataset pattern used by the working Telegram/Telegraph pipelines (`unique slug + dataset-ready polling + dataset_sources bind + mounted runtime tree`), removed the nested `ro_znanie.zip` payload that Kaggle rejected because of invalid inner names by shipping pre-extracted ASCII `Cygre-*.ttf` files instead, and added a targeted intro timing warp so the previously reported near-duplicate frame pairs (`second 2: 11/12 and 26/27`, `second 3: 26/27`) move distinctly without shifting the handoff frame or the `move_up` strong-beat sync.
- **Scheduler / Guide + VK Incident Guards**: scheduled guide excursions now go through the shared heavy-job guard at the scheduler layer and materialize `ops_run(kind='guide_monitoring', status='skipped')` instead of hanging invisibly behind a stuck heavy job, while `vk_auto_import` now enforces `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC` per inbox row so one bad post cannot block the whole scheduled queue indefinitely.
- **Scheduler / 3D Preview Guard Scope**: scheduled `/3di` no longer participates in the shared heavy-job gate, so unrelated long internal jobs such as month-page sync no longer skip the slot; `/3di` still serializes against itself via the preview-specific lock, and production restores the `17:15` Europe/Kaliningrad fallback slot.
- **Telegram Monitoring / Kaggle Recovery Grace Window**: `tg_monitoring` no longer drops recovery jobs immediately when Kaggle briefly reports `error/failed/cancelled` or when the local poll loop times out; recovery now keeps rechecking the kernel for a configurable grace window (`TG_MONITORING_RECOVERY_TERMINAL_GRACE_MINUTES`, default `360`) so late-written `telegram_results.json` can still be imported after the server already marked the scheduled run as failed.
- **VK Auto Queue / Historical Non-Event Prefilter**: long retrospective VK posts with rolled-forward historical dates in the text no longer bypass the conservative non-event prefilter just because `event_ts_hint` looks future-like; strong historical/info posts now skip before the LLM stage instead of repeatedly consuming queue slots and hitting `drafts_rate_limited`.
- **Post Metrics / Popular Posts Windows**: `/popular_posts` now adds a `7 суток` block, uses the latest available snapshot up to each target bucket (`age_day<=6` / `age_day<=2`) instead of requiring exact mature rows, and explains when `POST_POPULARITY_MAX_AGE_DAY` or `TG_MONITORING_DAYS_BACK` are too low to accumulate full 7-day Telegram data.
- **Post Metrics / Popular Posts Active Events**: `/popular_posts` now keeps only posts linked to current or future events, hides posts whose matched events are already finished, and shows explicit `skip(past_event_only)` diagnostics alongside the median filters.
- **CrumpleVideo / Notebook Helper Embedding**: `kaggle/CrumpleVideo/build_notebook.py` no longer leaves a trailing extra quote when syncing embedded helpers into `crumple_video.ipynb`; CI now validates that the embedded `poster_overlay.py` and `story_publish.py` blocks parse cleanly and match the canonical repo helper files, preventing scheduled Kaggle runs from failing before render with a `SyntaxError`.
- **CrumpleVideo / Short Telegram Cache Filenames**: notebook-side Telegram poster cache paths now use short hash-based filenames instead of raw remote basenames, preventing another `File name too long` failure when a CDN or Telegram URL tail is extremely long.
- **CrumpleVideo / Scheduled Tomorrow Live-Run Guarantee**: `video_tomorrow` now gets a dedicated misfire grace window plus an independent same-day watchdog task, so a live process that silently misses the `16:45` slot can still dispatch the exact scheduled `/v tomorrow` path the same day instead of waiting for a restart; `/healthz` now also fails when APScheduler or the registered `video_tomorrow` job disappears, allowing Fly to recycle a “HTTP alive, cron dead” runtime.
- **CrumpleVideo / Story Fanout + Story-Safe Canvas**: production `/v tomorrow` runs now keep Telegram stories enabled in Fly config, exact scheduled reruns no longer silently lose `story_publish.json`, and `CrumpleVideo` story upload wraps the canonical `1080x1572` render into a padded `1080x1920` story-safe mp4 so Telegram stories stop zooming/cropping the video.
- **CrumpleVideo / Story Publish TTL + Pinned Surface**: production CrumpleVideo stories now derive their Telegram lifetime from the selected event-date span (`12h` for tomorrow-only, `24h` for two or more dates), request `pinned=true` so they land in the channel’s published-stories surface, and keep smoke/image-only story runs out of that pinned list while preserving the existing story-safe `1080x1920` upload path.
- **CrumpleVideo / Kaggle GPU Metadata Guard**: the local-kernel deploy path now force-keeps `enable_gpu=true` for `CrumpleVideo` pushes, so even a stale metadata drift cannot silently send tomorrow-runs back to `Accelerator None`.
- **CrumpleVideo / Render Contract + Schedule**: `CrumpleVideo` now keeps its own production contract without leaking `VideoAfisha` defaults: the dataset/notebook use only `The_xx_-_Intro.mp3` at `1:17`, the Blender + ffmpeg path render and encode at `1080x1572`, Cycles auto-selects GPU when Kaggle exposes one instead of forcing CPU, and the scheduled `/v tomorrow` slot moves to `16:45 Europe/Kaliningrad` to target a publish closer to `19:00`.
- **CrumpleVideo / MobileFeed Intro Motion Sync + Blender Resolution**: the current `Мобильная лента` preview stack now resolves Blender binary discovery dynamically, renders on the canonical `1080 x 1920` story geometry (with same-aspect draft previews), fixes the broken auto-clamped easing helper, removes top-of-frame world leakage by separating camera-visible world color from HDRI lighting, and cuts into the real `video_afisha` late zoom tail instead of snapping back to the tiny centered start state.
- **CrumpleVideo / Scheduled Tomorrow Startup Catch-Up**: `video_tomorrow` now records scheduled dispatches in `ops_run` and performs a same-day startup catch-up after a missed `16:00 Europe/Kaliningrad` slot, so Fly restarts no longer silently drop the automatic `/v tomorrow` run until the next day.
- **CrumpleVideo / Popular Review Backdrop Lighting Artifact**: the single-scene Blender intro refinement now treats the yellow poster backdrop as an unlit image surface instead of a lit paper wall, preventing stray triangular light wedges at the top of the frame while keeping lighting reserved for the 3D card slabs.
- **CrumpleVideo / Popular Review Single-Scene Depth Read**: the Blender single-scene intro refinement now uses restrained perspective framing, explicit shadow planes, overscanned backdrop coverage, richer dark/orange inset materials, and front-most text overlays above inset bars, fixing the previous flat-looking pass where depth barely read and date/title lockups were physically swallowed by the 3D bars.
- **Guide Excursions / Media Album Downlinks**: after publishing guide digest text parts, the bot now edits the first media-album caption and adds readable short links like `Подробнее: Описание` or `Подробнее: Часть 1 · Часть 2` to related digest posts below, so forwarded albums stay connected to the excursion descriptions without tiny one-character tap targets.
- **LLM / Gemma 4 Eval Fixture Hygiene**: corrected the `KALMANIA` benchmark Telegram packet in the Gemma 4 eval flow after a source-poisoning bug pulled a `Лукоморье` post into the first harness run, and documented the corrected rerun plus revised ranking in the canonical Gemma 4 eval log.
- **CrumpleVideo / Story Preflight**: Kaggle story publish now checks story capability before the expensive video render and preserves target-specific Telegram API errors in `story_publish_report.json` and poller notifications, so non-premium or otherwise ineligible sessions fail fast instead of burning render time first.
- **CrumpleVideo / Story Report Serialization**: successful Kaggle story publishes no longer crash while writing `story_publish_report.json` when Telethon includes `datetime` values in the API result payload.
- **CrumpleVideo / Story Video Cover**: video stories now explicitly request `video_timestamp=0`, so the first frame of the rendered CrumpleVideo is used as the preview/cover frame.

### Fixed
- **Smart Event Update / Copy-Post Duplicate Guard**: Smart Update now converges same-day reposts with near-identical source text even when ticket URLs differ, and it bridges `doors/start` variants like `сбор гостей 19:00, начало 20:00` so one show no longer spawns parallel `19:00` and `20:00` cards across Telegram/VK copy posts.
- **Video Afisha 2D / MoviePy 2.x Resize Compat**: `video_announce.video_afisha_2d.create_advanced_scene()` now uses `clip.resized(...)` when MoviePy 2.x exposes the renamed API and falls back to `clip.resize(...)` on older builds, and the selection regression suite now imports `SelectionContext` from `video_announce.custom_types` instead of the removed `video_announce.types` alias.
- **CrumpleVideo / Story Target Ordering**: story fanout can now be pinned with explicit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, so production order no longer depends on the profile `main` channel and can be fixed to `@kenigevents` first, then `@lovekenig` with a `600s` delay.
- **Guide Excursions / Template Detail Surface**: added `/guide_template <id>` and template detail inline actions so operators can inspect accumulated `GuideTemplate` route facts, hook rollups, locals/tourists/mixed votes, and linked occurrences instead of only seeing flat template titles.
- **Guide Excursions / Lollipop Trails Digest Writer**: added a guide-specific `Lollipop Trails` batch writer for digest cards, where Gemma writes only grounded `title + digest_blurb` from materialized fact packs while card shell fields stay deterministic from stored facts.
- **Guide Excursions / Admin Run Reports**: added Smart Update-style admin observability for guide monitoring via `/guide_runs [hours]`, `/guide_report [ops_run_id]`, and `/guide_log <occurrence_id>`, with `ops_run`-backed source/post/occurrence drilldown and occurrence-level claim provenance.
- **Guide Excursions / Inventory Admin Commands**: added `/guide_events [page]`, `/guide_templates [page]`, and `/guide_recent_changes [hours]` so operators can inspect future guide occurrences, template inventory, and created-vs-updated excursions directly in Telegram, with inline delete actions for occurrences and templates.
- **Guide Excursions / Fact-First Audit**: added a canonical audit + migration report for moving guide excursions from the old regex-heavy/local MVP to a `Kaggle -> fact-first import -> digest` architecture, including the current gap list and phased rollout status.
- **Guide Excursions / Kaggle Runtime + Fact Inspection**: added a guide-specific Kaggle monitor (`kaggle/GuideExcursionsMonitor`) plus `/guide_facts <occurrence_id>` so operators can inspect the materialized fact pack and stored `GuideFactClaim` rows for a concrete excursion before publication.
- **Tools / Claude Code**: added shared Claude Code project configuration with `opus`-only model policy, default `high` effort, project `CLAUDE.md`, and the `Opus` subagent alias for consultation and substantial rework workflows; built-in Claude delegations are denied in shared settings so project-side delegation stays on `Opus`, while especially complex consultations may temporarily raise effort to `max`.
- **Guide Excursions Monitoring**: added a new guide-only monitoring track with seed casebook Telegram sources, separate `guide_*` SQLite tables, `/guide_excursions` operator UI (`scan / preview / publish / sources`), guide-specific digest preview/publication to `@keniggpt`, scheduler hooks, `/general_stats` visibility, and live E2E coverage via `tests/e2e/features/guide_excursions.feature`.
- **Guide Excursions / Dedup Audit**: added `scripts/inspect/audit_guide_excursion_duplicates.py` plus canonical docs for `Route Matchmaker v1` and LLM prompt contracts, based on live duplicate findings like `ruin_keepers/5054` vs `ruin_keepers/5055`.
- **LLM Gateway / Kaggle Gemma Key Probe**: added a private Kaggle notebook `kaggle/GemmaKey2Probe/gemma_key2_probe.ipynb` plus launcher `kaggle/execute_gemma_key2_probe.py` for smoke-testing `GOOGLE_API_KEY2` via the same encrypted split-datasets flow used by Telegram Monitoring and `/3di`; the launcher can load an extra env file, push the kernel, download `output.json`, and avoid printing the secret.
- **Docs / Backlog Event Static Pages**: added a canonical backlog analysis for migrating public event pages from Telegraph to static HTML on a project-owned domain (Yandex S3/Object Storage direction), including research TODO for real event/source data, SEO/GEO requirements, and a linked excursions future-domain/bucket track.
- **Ops / Location Repair**: added `scripts/fix_molodezhny_locations.py` for the targeted prod backfill of youth-center events that were stored with `city='МОЛОДЕЖНЫЙ'`, with optional Telegraph rebuild after the DB fix.
- **Location Reference / Aliases**: added `docs/reference/location-aliases.md` for data-driven venue aliases and typo handling on top of `docs/reference/locations.md`.

### Fixed (continued)
- **CrumpleVideo / Exact Scheduled Same-Day Override**: production now has a date-scoped startup override for the exact `_run_scheduled_video_tomorrow` path via `V_TOMORROW_FORCE_RUN_LOCAL_DATE` + `V_TOMORROW_FORCE_RUN_TOKEN`; with `V_TOMORROW_FORCE_RESET_RENDERING=1` it can first clear a wrong blocking `RENDERING` session, so same-day validation no longer depends on waiting until tomorrow’s slot.
- **CrumpleVideo / Scheduled Tomorrow Canonical Env Guard**: when `ENABLE_V_TOMORROW_SCHEDULED=1` is enabled, the scheduler now resolves timing/timezone/profile only from `V_TOMORROW_*`; legacy `V_TEST_TOMORROW_*` remain backward-compatible only for older env sets that still use the legacy enable flag, so a stale test slot can no longer silently drag production `/v tomorrow` back to the old time.
- **CrumpleVideo / Kaggle Timeout**: raised the default `/v` Kaggle timeout from `150` to `225` minutes, so long renders have enough headroom to finish the mp4 and complete the output-download path instead of timing out just before the final fetch.
- **LLM Gateway / Default Key Scoping**: `GoogleAIClient` now scopes `google_ai_reserve` to the client’s `default_env_var_name` when the caller does not provide explicit `candidate_key_ids`, so generic bot flows like `smart_update`, `event_parse`, and VK auto-import cannot silently reserve guide-only `GOOGLE_API_KEY2` just because its metadata row exists in `google_ai_api_keys`.
- **Guide Excursions / Kaggle Session Boundary Guard**: guide Kaggle monitoring now fails closed unless it uses `TELEGRAM_AUTH_BUNDLE_S22`; silent fallback or manual override to `TELEGRAM_AUTH_BUNDLE_E2E` is blocked unless `GUIDE_MONITORING_ALLOW_NON_S22_AUTH=1` is explicitly set for an аварийный override.
- **Guide Excursions / Fixed KEY2 Server-Side Gateway**: server-side guide Gemma consumers (`enrich`, `dedup`, `digest_writer`) now resolve `candidate_key_ids` from `google_ai_api_keys.env_var_name`, prefer `GOOGLE_API_KEY2` / `GOOGLE_API_KEY_2` as the fixed guide key, and pass that fixed-key choice into `google_ai_reserve` instead of silently reserving the generic `GOOGLE_API_KEY` pool on the bot side.
- **Guide Excursions / Digest Publish TPM Budgeting**: guide digest publication now splits `Lollipop Trails` batches more aggressively and is willing to wait through several explicit provider `retry after ... ms` hints within a bounded window, so live preview/publish is less likely to die right after a heavy scan while still keeping the full fact-first semantic surface.
- **Guide Excursions / Prompt Family Split**: guide Kaggle extraction no longer relies on one oversized universal extraction prompt for announce/status/template posts; the notebook now routes posts through narrower `trail_scout.announce_extract_tier1.v1`, `trail_scout.status_claim_extract.v1`, `trail_scout.template_extract.v1`, and `route_weaver.enrich.v1` passes so TPM mitigation comes from stage decomposition rather than silently dropping semantics.
- **Guide Excursions / Public Line Authoring**: guide digest cards now keep the channel line separate from `Гид`, prefer Gemma-derived `guide_profile.guide_line`, let Gemma normalize human-facing `price/route/audience-region` lines from materialized facts, and stop leaking raw shell text like `500/300 руб взрослые/дети,пенсионеры` into the public digest.
- **Guide Excursions / Booking + Organizer Shells**: guide digest cards now prefer one best booking endpoint with mobile-phone priority for `tel:` contacts, can render `🏢 Организатор` instead of falsely labeling an operator as `Гид`, and the future inventory view shows a separately clickable source-post URL.
- **Guide Excursions / Organizer Brand + Phone Links**: when an operator-like `guide_line` is demoted, digest cards now prefer the grounded organizer brand (`marketing_name` / source title) instead of a verbose operator bio, and lone phone-only booking facts are rendered as clickable `tel:` links even if upstream did not materialize `booking_url`.
- **Guide Excursions / Telegram Phone UX**: phone-only booking lines now render as plain compact numbers like `+79217101161` instead of HTML `tel:` anchors, because Telegram makes the compact number tappable more reliably in digest messages.
- **Guide Excursions / Raw Price Copy Guardrail**: public guide cards now hide slashy source-price copy when Gemma did not produce a clean `price_line`, instead of republishing raw shell text that only looks LLM-authored on narrow fixtures.
- **Guide Excursions / Digest Window Coverage**: the default `new_occurrences` digest horizon is now `45` days instead of `30`, so near-future excursions slightly beyond the monthly cutoff do not disappear from operator preview/publication by default.
- **Guide Excursions / Digest Card Clarity**: guide cards now render explicit `Гид` and `Локация` lines when facts are available, expose regional fit from materialized locals/tourists/mixed classification, suppress junk placeholders like `одна дата`, and normalize bare `t.me/...` booking targets to clean clickable links.
- **Guide Excursions / Public Region-Fit Line**: the regional-audience line is now rendered as a standalone house-line (`🏠 ...`) instead of the awkward `Кому больше:` label, while Gemma is prompted to describe local-vs-tourist fit without duplicating the separate `👥 Кому подойдёт` audience row.
- **Guide Excursions / Public Location Aliases**: guide digest cards now render `🏙 Локация` instead of `🏙 Город` and pass place names through a guide-specific alias table (`docs/reference/guide-place-aliases.md`), so historical names like `Роминта` can surface as a clearer modern public label.
- **Guide Excursions / Card Separation**: guide digest messages now insert a blank line plus a horizontal divider between cards, making long Telegram digests easier to scan.
- **Guide Excursions / Media Diversification**: when several digest cards come from one multi-announce Telegram post, guide media selection now rotates through distinct `media_refs` for that post instead of repeating the same first image across multiple cards.
- **Guide Excursions / Album Media Backfill**: album selection now keeps scanning later digest rows until it fills the available media pack, instead of stopping early when some of the first cards have no usable media.
- **Guide Excursions / Single Source-Post Tap Target**: guide cards now keep the source-post link only on the title and no longer render a duplicate `🔗 Анонс: исходный пост` line in the body.
- **Guide Excursions / Co-Guide Public Identity Resolution**: preview/publish now perform a lightweight public Telegram username resolution for co-guides mentioned in the source prose, allowing plural `👥 Гиды: ...` lines and correcting partial names like `Анна Туз` to the public profile name `Анастасия Туз` when occurrence facts already point to the same person.
- **Guide Excursions / Guide Alias Collapse**: guide preview now also collapses `marketing_name` / username aliases to the canonical public profile name when they refer to the same person (`Amber Fringilla` -> `Юлия Гришанова`), so one guide does not appear as a fake plural pair.
- **Guide Excursions / Lead Emoji From Gemma**: guide digest writer can now return a grounded thematic title emoji (`🐦`, `🏛️`, `🌲`, etc.) so title accents come from Gemma rather than only from deterministic popularity markers.
- **Guide Excursions / Terminology Policy**: guide digest writer now explicitly preserves grounded `прогулка` vs `экскурсия` wording from source titles/facts and prefers neutral nouns like `маршрут` or `выход` when the format is ambiguous, instead of mixing both terms inside one card.
- **Guide Excursions / Audience-Fit Anti-Repeat**: digest writer now rejects blurbs that repeat the separate local-vs-tourist fit line inside `🧭`, so region-fit stays in the dedicated `🏠` row instead of echoing twice in one card.
- **Guide Excursions / Main-Hook Term Guardrail**: occurrence enrichment now asks Gemma to preserve the dominant `прогулка` vs `экскурсия` term already at `main_hook` stage and prefers noun-phrase hooks over generic sentence shells like `Экскурсия раскрывает...`.
- **Guide Excursions / Region-Fit Score Materialization**: server-side region-fit enrichment now normalizes Gemma's occasional compact `0..10` audience scores into percent-style `0..100`, so `locals/tourists/mixed` facts remain visible in admin surfaces and digest shells instead of degrading into near-zero confidence.
- **Guide Excursions / Enrich TPM Retry**: guide occurrence enrichment now keeps smaller default Gemma batches and retries once on explicit provider `retry after ... ms` hints, so the second enrich pass (`audience_region_fit`) is less likely to disappear after a successful `main_hook` call on the same live run.
- **Guide Excursions / Digest Writer TPM Retry**: `Lollipop Trails` digest batch writing now also retries once on explicit provider `retry after ... ms` hints and uses smaller default batches, reducing live preview/publish fallbacks to stale seed blurbs right after a heavy scan.
- **Guide Excursions / Kaggle Source Reconnects**: guide Kaggle monitoring now reconnects the Telethon client between sources when needed and retries Gemma calls on explicit `retry after ... ms`, reducing clean-run partials where later sources disappeared with `Cannot send requests while disconnected` or short TPM spikes.
- **Guide Excursions / Template Rollup Preservation**: rebuilding `GuideTemplate.facts_rollup_json` no longer discards existing template-hint facts like route anchors; occurrence-derived cities/routes/hooks are now merged into the rollup instead of overwriting it.
- **Guide Excursions / Digest Eligibility Guardrail**: guide import/runtime now fail-closed on undated, cancelled, and private occurrences, but fact-rich `limited` announcements with a real future date can be promoted to `digest_eligible=true` so public digests do not silently drop grounded upcoming excursions like `@excursions_profitour/863`.
- **Guide Excursions / LLM Boundary Enforcement**: the emergency local guide parser no longer heuristically materializes semantic rich fields like `duration_text`, `route_summary`, or `group_format`; those facts remain Gemma-owned on the canonical Kaggle extraction path, while deterministic code is limited to structural split, operational extraction, and safety guardrails.
- **Guide Excursions / Fresh Mass Rerun Validation**: a clean 16 March 2026 Kaggle rerun on a fresh guide DB (`run_id=8a01ff760d1e`) validated the current fact-first path across `10` sources with `36` scanned posts, `21` successful Gemma extractions, `10` created occurrences, and a published `7`-card digest after runtime dedup, confirming the flow is not limited to one hardcoded guide/sample.
- **Guide Excursions / Prompt Boundary Cleanup**: guide Kaggle screening now classifies the whole post without seeing pre-split `occurrence_blocks`, `base_region_fit` is requested from Gemma at screen time instead of being left to downstream heuristics, and notebook import no longer silently substitutes `digest_blurb` from `summary_one_liner`.
- **Guide Excursions / Multi-Occurrence Fact Shell**: guide Kaggle extraction now passes explicit `occurrence_blocks` into Gemma, rescues uncovered multi-announce blocks one-by-one, normalizes list-like payload fields to avoid broken outputs like `Н, е, о`, rejects generic out-of-region travel calendars such as `twometerguide/2761`, and materializes richer fact-pack fields (`duration_text`, `route_summary`, `group_format`) that the digest now renders as clean `Кому подойдёт / Формат / Что в маршруте / Продолжительность / Место сбора` lines without placeholder noise.
- **Guide Excursions / Kaggle Recovery Parity**: guide monitoring now registers canonical kernel runs in the shared `kaggle_registry`, recovers completed `guide_monitoring` kernels through the common `kaggle_recovery` scheduler after bot restarts, and re-imports `guide_excursions_results.json` into a fresh `ops_run` instead of silently losing finished Kaggle work.
- **Guide Excursions / Kaggle Polling Resilience**: guide polling now matches Telegram Monitoring more closely by tolerating transient Kaggle API SSL/network errors during status checks and still validating `run_id` on downloaded output before import, reducing false failures and stale-result imports on the canonical kernel.
- **Guide Excursions / Kaggle LLM Gateway Enforcement**: guide Kaggle extraction no longer calls Gemma directly via the raw SDK; the notebook now ships shared `google_ai` code inside the pushed kernel like production Telegram Monitoring, keeps secrets split into the canonical `cipher + key` datasets, uses `GoogleAIClient` with Supabase-backed reserve/finalize flow, routes guide traffic through `GOOGLE_API_KEY2` + `GOOGLE_API_LOCALNAME2`, logs the active gateway/consumer path in Kaggle output, and marks rate-limit/timeout deferrals explicitly instead of silently hanging.
- **Guide Excursions / Kaggle Live Logs**: `GuideExcursionsMonitor` now ships as a notebook kernel entrypoint that delegates to the same Python fact-first runner, aligning guide executions with Telegram Monitoring so Kaggle UI log streaming is easier to observe during live runs.
- **Guide Excursions / Canonical Kaggle Notebook Guard**: guide monitoring now verifies after `kernels_push` that the canonical Kaggle kernel still resolves as a notebook (`kernel_type=notebook`, notebook `code_file`) and fails fast with an actionable error if the slug regresses back to a script kernel, instead of looping on stale output from a non-notebook runtime.
- **Guide Excursions / Embedded Google AI In Notebook**: the generated `GuideExcursionsMonitor` notebook now embeds the shared `google_ai` package into notebook cells instead of relying on sibling kernel files that Kaggle notebook runtimes do not expose, eliminating repeated 120-second repo-bundle waits and the Telethon session corruption they caused during live guide scans.
- **Guide Excursions / Live E2E Window**: Behave manual E2E now caps guide full-scan defaults to a 7-day / 25-post horizon unless explicitly overridden, so live verification stays fast and operator-readable instead of replaying the full 21-day production window.
- **Guide Excursions / Notebook Build Source Of Truth**: guide Kaggle pushes now materialize the notebook directly from `guide_excursions_monitor.py`, so the notebook runtime no longer fails trying to find a sibling script file that Kaggle does not expose at execution time.
- **Guide Excursions / E2E No Hidden Local Fallback**: live behave runs now set `GUIDE_EXCURSIONS_LOCAL_FALLBACK_ENABLED=0`, so a broken Kaggle/Gemma path fails loudly instead of being masked by `transport=local_fallback`.
- **Guide Excursions / LLM Visibility**: guide Kaggle output and admin run reports now surface `llm_ok / llm_deferred / llm_error` counts, so operators can verify how many posts actually reached Gemma and whether limiter deferrals happened during the run.
- **Guide Excursions / Isolated Kaggle Smoke Slug**: manual guide-monitoring runs can now override the pushed kernel slug via `GUIDE_MONITORING_KERNEL_SLUG`, which helps live E2E/smoke runs avoid collisions with an already-running shared guide kernel without changing the canonical production path.
- **Guide Excursions / Semantic Updated-At**: marking guide occurrences as published in a digest no longer mutates `guide_occurrence.updated_at`, so “recently updated” admin/report views reflect source/import changes instead of digest bookkeeping.
- **Guide Excursions / Digest Coverage + Compact Caption**: `new_occurrences` now marks as published only the cards that actually reached the digest and their dedup-cluster siblings, so editorial-suppressed light/full findings can still surface after later enrichment instead of disappearing prematurely; when a media digest fits into a single safe caption (`<=1000` chars), publish/backfill now sends it as one album message without a separate text post.
- **Smart Update / Telegraph Text Escapes**: public event descriptions and Telegraph rendering now unescape leaked JSON-style control sequences (`\n`, `\r`, `\t`, `\"`), so pages no longer show literal backslashes/newline markers inside the narrative body.
- **Smart Update / Implicit-Year Dates**: missing-year dates are now resolved around the source publish date with a recent-past window, preventing bogus next-year imports like March 2027 pages from posts published in March 2026 while still rolling genuinely far-past mentions into the next season.
- **Guide Excursions / Kaggle Gemma JSON Mode**: the guide Kaggle monitor no longer requests unsupported JSON mode from `models/gemma-3-27b-it` in the legacy `google.generativeai` SDK, fixing live `InvalidArgument: JSON mode is not enabled` failures that previously turned guide runs into `partial=true` with zero imported occurrences.
- **Guide Excursions / Live Claim Shapes**: server-side guide import now normalizes live Gemma `fact_claims` payloads that use `fact_type` and `claim_text`, so occurrence-level `GuideFactClaim` rows survive import and `/guide_facts` shows real per-occurrence claims instead of only template/profile hints.
- **Guide Excursions / Gemma Extraction Contract**: guide Kaggle Tier-1 extraction now accepts valid bare-array Gemma JSON for `occurrences`, keeps the prompt on explicit object-wrapper output, and normalizes bare textual fact claims (`fact`) into stored `GuideFactClaim` rows instead of silently dropping them during import.
- **Guide Excursions / Kaggle Prompt Budget**: guide announce extraction now uses a more compact `occurrence_blocks`-first payload and lower output-token budgets, which removed live `llm_deferred_rate_limit:tpm` noise on the full 10-source E2E run while keeping the fact-first Gemma path intact.
- **Guide Excursions / Past Occurrence Guard**: guide imports now skip already finished occurrences in the MVP path instead of storing stale rows that should never become digest candidates.
- **Bot Startup / Recent Imports**: Removed a duplicate `recent_imports_router` registration in `create_app()`, which crashed the production webhook process during startup and made the bot stop responding to commands.
- **Runtime Health / Fly Auto-Recovery**: `/healthz` now validates startup readiness, runtime heartbeat freshness, required background tasks, bot-session openness, and a live SQLite ping instead of returning a blind static `ok`, so Fly can mark “HTTP alive but Telegram/scheduler broken” instances unhealthy and recycle them automatically.
- **Smart Update / Address-Based Venue Rescue**: deterministic shortlist/match now keeps exact-title cross-source candidates together when venue names differ only as short alias vs official long form but the normalized `location_address` matches, preventing live duplicates like the same Gusev museum show imported from Telegram and VK into separate events.
- **VK Auto Queue / Stale Legacy Statuses**: stale `vk_inbox.status='importing'` rows are now requeued to `pending` together with stale `locked` rows, so legacy interrupted imports do not disappear from the modern auto-import scheduler.
- **Telegram Monitoring / Multi-event Poster Fallback**: when a multi-event Telegram post arrives without `posters[]` from upstream, server-side public-page fallback now scrapes the post images from `t.me/s/...`, reuses image-only photos across all split events, and still applies OCR-based filtering when the scraped posters contain readable event text.
- **Add Event Watcher / Stall Guard**: `_watch_add_event_worker()` now updates `_ADD_EVENT_LAST_DEQUEUE_TS` through the intended module-level state instead of crashing with `UnboundLocalError`, so the queue watcher can keep restarting a stalled add-event worker instead of failing its own health check.

### Changed
- **Docs / MobileFeed Final Timing + Quality Contract**: the canonical `Мобильная лента` requirement docs now replace the old `~1.7-1.9s` / `<2.0s` intro target with a clean-render `~3.2-3.4s` target, lock the strongest early music accent to the end of the late `0.9 -> 1.0` zoom tail / start of the upward move, and explicitly reject draft-level phone/ribbon noise for the final pass.
- **CrumpleVideo / MobileFeed Clean Render Path**: the current `intro + scene1` renderer now uses a longer late-tail handoff, higher-quality Cycles settings, denoising/adaptive sampling hooks, stronger contact shadowing, and a dedicated final artifact directory instead of the old short low-budget approval timing.
- **CrumpleVideo / MobileFeed Screen CTA Handoff Contract**: the current `Мобильная лента` preview/approval path no longer carries phone CTA planes through the 3D-to-2D cut; the labels now stay pinned to the phone screen while visible and fade there before the cut, removing the previous cross-cut repositioning jitter.
- **Docs / Popular Review MobileFeed Requirement Gate**: the canonical `popular_review` backlog spec and design brief now record `Мобильная лента` / `MobileFeed Intro` as the active next intro concept, with a strict local shot reference, phone+ribbon scene contract, center-led handoff into `video_afisha`, multi-level CTA architecture, and an explicit requirements-confirmation gate separating confirmed requirements from remaining open decisions.
- **Docs / Popular Review MobileFeed CTA Timing Rule**: the `Мобильная лента` brief now requires at least one CTA layer to carry the real temporal signal of the selected payload (`date / range / period / month-cluster` as appropriate), and phone-asset sourcing is explicitly non-blocking: if a premium mockup is unavailable, the concept proceeds with the best compatible free fallback while preserving the same shot grammar.
- **Docs / Popular Review MobileFeed Confirmed Phone Asset**: the `Мобильная лента` canon now records the user-provided local `iPhone 16 Pro Max` archive (`docs/reference/iphone-16-pro-max.zip`) as the primary implementation asset, so the next pass starts from a confirmed FBX+textures source instead of continuing the open-ended external asset search.
- **CrumpleVideo / MobileFeed On-Screen CTA Routing**: the current `Мобильная лента` draft now treats the phone surface as a real CTA layer, keeping `ПОПУЛЯРНОЕ` above the poster and moving the lower on-screen support to a payload-driven temporal line (`6 • 7 • 8 • 9 • 10 • 12` for the current sparse-April example) instead of a generic count-only footer.
- **CrumpleVideo / MobileFeed Draft Handoff Cleanup**: the low-sample intro preview now pushes the on-screen phone labels off the surface before the final beat, strengthens ribbon clearance above the phone shell/desk by extruding thickness away from collision surfaces, uses a matched hard cut on the late zoom tail instead of a muddy dissolve, and keeps the approval preview running through the first 2D continuation beats.
- **CrumpleVideo / MobileFeed Screen-Locked CTA + Smooth Tonal Tail**: the current intro preview now keeps the on-screen CTA planes locked to the phone screen, delays the upper-label fade so it stays readable deeper into the zoom, and smooths the early 2D white-to-black background transition across the tail instead of stepping abruptly into black.
- **CrumpleVideo / MobileFeed Payload CTA Routing + Cross-Cut Dissolve**: the phone UI layer now uses a payload-shaped CTA stack (`N популярных событий + period` above the poster, city cluster below it), removes the separate 3D-only label exit, and carries those labels through the matched cut so they dissolve during the early 2D tonal handoff instead of disappearing as an unrelated animation.
- **CrumpleVideo / Scheduled Tomorrow Slot**: the canonical automatic `/v` slot is now production-first (`ENABLE_V_TOMORROW_SCHEDULED`) with a default `16:00 Europe/Kaliningrad` start and legacy `V_TEST_TOMORROW_*` compatibility, so a worst-case `225` minute render plus story fanout still fits before the evening audience window and before the `20:10` guide full scan.
- **Scheduler / Morning Heavy Jobs**: moved the default scheduled morning `/3di` slot to `07:15 Europe/Kaliningrad` (`THREEDI_TIMES_LOCAL=07:15,15:15,17:15`) and aligned Fly production cron from the legacy `SOURCE_PARSING_TIME_LOCAL=02:15` / `THREEDI_TIMES_LOCAL=03:15,15:15` to `04:30` / `07:15,15:15`, so nightly `/parse` gets a larger head start and scheduled `/3di` is less likely to skip on the shared heavy-job guard.
- **Guide Excursions Monitoring / Live E2E Coverage**: the canonical `guide_excursions` live scenario now walks the full operator path from `/start` and Kaggle scan to `/guide_report`, `/guide_runs`, `/guide_recent_changes`, `/guide_events`, and `/guide_facts`/`/guide_log` on multiple control excursions before publishing the digest to `@keniggpt`.
- **Guide Excursions Monitoring / E2E LLM Counters**: the live `guide_excursions` scenario now also asserts completion/report LLM counters (`LLM ok/deferred/error`, `llm_ok`, `llm_deferred`) so the Telegram UI run proves actual Gemma activity, not just occurrence import side effects.
- **Guide Excursions Monitoring / Full-Run E2E Timeout**: guide live E2E now treats `Мониторинг экскурсий завершён` as a dedicated long-running Kaggle operation with its own generous timeout envs (`E2E_GUIDE_MONITOR_TIMEOUT_SEC`, `E2E_GUIDE_MONITOR_POLL_SEC`) instead of the generic 5-minute fallback.
- **Guide Excursions Monitoring / Success-Only E2E Completion**: the live guide scenario now waits specifically for `✅ Мониторинг экскурсий завершён` and requires `/guide_report ... status=success`, so `⚠️ ... завершён с ошибками` no longer passes as a false-positive completion.
- **Guide Excursions Monitoring / Publish Order E2E**: the live guide scenario now validates the real Telegram publish order by checking the new media album before the new digest text message, avoiding false negatives when `@keniggpt` receives the album first and the text summary second.
- **Guide Excursions Monitoring / Short Daily Horizon**: guide defaults are now tuned for real daily monitoring (`GUIDE_DAYS_BACK_FULL=5`, `GUIDE_DAYS_BACK_LIGHT=3`), and live E2E uses the same short horizon instead of replaying a week-plus tail by default.
- **Guide Excursions / Bootstrap Horizon**: the first full guide scan on an empty DB now automatically widens post lookback to `GUIDE_DAYS_BACK_BOOTSTRAP=14`, while warmed-up daily/full runs still use the short `GUIDE_DAYS_BACK_FULL=5` window so bootstrap digests do not miss still-relevant older announcement posts.
- **Guide Excursions / Operator Identity Guardrail**: digest rendering now demotes operator-like `guide_line` values back to `🏢 Организатор` for excursion operators and aggregators instead of publishing organization copy under the personal `Гид` label.
- **Guide Excursions Monitoring / Operator UX**: manual `/guide_excursions` scans now surface the detailed run report directly in Telegram after completion, expose quick actions for the latest run/recent runs, and point preview operators to both `/guide_facts <id>` and `/guide_log <id>` for fact-first verification before publish.
- **General Stats / Guide Excursions**: `/general_stats` now shows guide `occurrences_updated`, current `occurrences_future_now`, and `templates_total` in addition to sources/posts/digest counters and guide-monitoring runs.
- **Guide Excursions Monitoring / Runtime Boundary**: guide monitoring now prefers a guide-specific Kaggle/Gemma extraction path, imports fact packs into `GuideProfile / GuideTemplate / GuideOccurrence / GuideFactClaim`, grounds digest/editorial on those fact packs, and keeps the old local Telethon scan only as an explicit emergency fallback.
- **Guide Excursions Monitoring / Live UI & E2E**: manual `/guide_excursions` scans now expose the transport path in Telegram UI (`transport=kaggle` on the canonical path) and previews point operators to `/guide_facts <id>` for fact-level verification before publish.
- **Qtickets / Source Parsing**: parser-backed `/parse` events now send structured site facts (`date`, `time`, `venue`, `ticket status`, `prices`, `url`) into the LLM draft builder before Smart Update, preventing mass downstream failures on sparse Qtickets descriptions while keeping the Qtickets Playwright parser itself unchanged.
- **Guide Excursions Monitoring / Runtime Path**: current MVP runtime now uses a pragmatic local Telethon scan/import path with Bot API publication, keeps guide data out of the regular `event` surfaces, and falls back to Telethon media download when Bot API cannot forward media from public source channels into the digest bundle.
- **Guide Excursions Monitoring / Manual UI & E2E**: manual scan completion now assumes a follow-up action menu for `preview/publish`, while live guide-monitoring E2E explicitly requires that the local bot be the only `getUpdates` consumer on the token; otherwise command replies may come from an external runtime with stale DB state.
- **Guide Excursions Monitoring / Digest Links**: guide digest cards now keep the excursion title linked to the concrete source post, leave the channel line as plain text to avoid adjacent tap-targets, and still render booking links as clickable anchors.
- **Guide Excursions Monitoring / Digest Dedup**: live guide digests now run `Route Matchmaker v1` before render/publish, suppress same-occurrence teaser/update duplicates (including live cases like `ruin_keepers` same-day teaser chains and `alev701` schedule-vs-departure `Восток-2`), and mark suppressed member rows as covered so they do not reappear in the next “new excursions” digest.
- **Docs / Guide Excursions Monitoring**: guide-monitoring backlog docs now explicitly fix the execution boundary as `Kaggle notebook -> server import -> digest publish`, require maximum reuse of the existing Telegram Monitoring stack (`TelegramMonitor` notebook, service/handlers/split-secrets flow), integrate the live `Opus` audit into the canonical design, split the LLM layer into Kaggle `Tier 1` extraction plus server-side `status_bind/enrich/digest_batch`, formalize `title_normalized` / `rescheduled_from_id` / partial transport contracts, tighten digest split rules and media-bridge cleanup, extend the frozen eval pack with harder real-world guide cases, and lock the MVP rule that past occurrences are not stored while deferring booking-click tracking to a later dedicated layer.
- **Telegraph Month Pages / Exhibitions**: months with more than `10` ongoing exhibitions (`MONTH_EXHIBITIONS_PAGE_THRESHOLD`) now publish a dedicated Telegraph page for `Постоянные выставки <месяца>` and link to it from the footer of the main month page; public exhibition lists also display-dedupe existing long-running duplicate rows instead of repeating them verbatim.
- **CrumpleVideo / `/v`**: quick tomorrow flows now stop on a manual preflight instead of auto-starting Kaggle, the test cap is raised to `12` posters, the selection UI shows the active render limit and blocks over-limit launches, and the CrumpleVideo notebook holds each poster slightly longer before transitioning.
- **Preview 3D / Current Month Alignment**: `/3di` missing-only runs for the current month now use the same future-only window as the public month page (`date >= today`), so batch renders stop burning Kaggle time on already finished dates that can no longer appear on the current month Telegraph page.
- **Preview 3D / Kaggle Preflight**: before pushing `payload.json` to Kaggle, `/3di` now probes source `photo_urls`, prunes explicitly dead `4xx` images, and skips events whose surviving image set no longer satisfies the selected mode, instead of discovering those dead inputs only after a full Blender run.
- **Post Metrics / Popular Posts**: `/popular_posts` now hides posts that are linked only to already finished events; the report keeps only events scheduled for today or later, while multi-day events remain visible through `end_date`.
- **VK Auto Queue / Scheduler Cadence**: default scheduled VK auto-import now runs in smaller, more frequent batches (`06:15,10:15,12:00,18:30` Europe/Kaliningrad with `VK_AUTO_IMPORT_LIMIT=15`) so one skipped heavy-job window is less likely to leave a full-day backlog while staying away from the `08:00` daily announcement and late-evening monitoring windows.
- **Smart Update / Telegram Group Authors**: Telegram `group/supergroup` event posts authored by a user account now fall back to the post author as the contact link (`https://t.me/<username>` or `tg://user?id=<id>`) when no explicit ticket/registration link is found and the post does not expose phone/email booking contacts; the same release also restores the intended narrow Stage 04 deterministic rescues for same-post long-run updates, ticket/source-text bridges, `Bar/Бар` venue normalization, and city-noise repost matches.
- **Preview 3D / Month Picker**: `/3di` month selection now queues only events in the chosen month that are still missing `preview_3d_url`; the menu text explicitly marks month mode as missing-only, while full current-month regeneration remains a separate explicit action.
- **Preview 3D / Month Batch Size**: `/3di` month mode now opens a second step with batch-size options (`25`, `50`, `100`, `all`) before starting Kaggle, so heavy months can be rendered in smaller missing-only chunks instead of one oversized run.
- **Post Metrics / Popular Posts**: `/popular_posts` now includes posts that are strictly above the per-source median on `views` or `likes` (not only on both), improving sparse-result windows while keeping the same diagnostic breakdown for `views/likes/оба`.
- **Post Metrics / Popular Posts 7-Day Window**: `/popular_posts` now adds a `7 суток` block, prefers mature `age_day=6` snapshots with fallback to the latest available snapshot up to the target bucket (same fallback model now also used for the `3 суток` block), and explicitly reports when `POST_POPULARITY_MAX_AGE_DAY` / `TG_MONITORING_DAYS_BACK` are too low to accumulate true 7-day Telegram data.
- **Admin Reports / Recent Imports UI**: `/recent_imports` rows now start with `id`, then show a compact status icon (`✅` created, `🔄` updated), and only then the Telegraph-linked title, reducing visual noise in long source reports.
- **General Stats / Telegram Source Shares**: `/general_stats` now shows Telegram `events_created/events_updated` from `tg_monitoring` run logs and adds source-share coverage blocks for `vk` / `telegram` / `/parse` both for events touched in the report window and for the current active future inventory; shares are event-level coverage and may overlap for multisource events.
- **Smart Update / Fair Dates**: Default fallback `end_date = date + 1 month` now applies only to exhibitions; fairs without an explicit closing period stay single-day, preventing false ongoing fairs in `/v` and other date-based selections.
- **CrumpleVideo / `/v`**: Tomorrow-mode selection no longer treats fairs with inferred fallback `end_date` as ongoing multi-day events, and intro date range for confirmed long-running items now clamps to the selected target window instead of leaking the historical start date.

### Fixed
- **VK Auto Queue / Crash Recovery + RAM Guard**: startup recovery now increments attempts only for `auto:*` locked rows and translates them to terminal `failed` after `VK_AUTO_IMPORT_RECOVERY_MAX_ATTEMPTS` instead of recycling the same crash-interrupted post forever; the queue also defaults to conservative row-by-row processing (`VK_AUTO_IMPORT_PREFETCH=0`) and caps live VK photo fetches via `VK_AUTO_IMPORT_MAX_PHOTOS=4` to reduce OOM pressure.
- **Media / Upload Diagnostics**: managed poster uploads no longer log successful Yandex/Supabase writes under misleading `CATBOX ...` summary labels; runtime logs now use `poster_upload ...` and `storage_msg=...`, so `storage_primary` is clearly visible as managed storage success instead of a Catbox incident.
- **CherryFlash / Live Runner No-Wait Guard**: `scripts/run_cherryflash_live.py` now rejects `--no-wait` because CherryFlash render startup continues in a background asyncio task after session creation; exiting the runner early created misleading local `RENDERING` sessions without any real visible Kaggle kernel run.
- **Scheduler / VK + Telegram Run Visibility**: scheduled `vk_auto_import` and `tg_monitoring` now create a bootstrap `ops_run` before resolving superadmin or entering the inner runner, reuse that same row inside the real run, and close it as `skipped/error` on bootstrap failures so false APScheduler fires no longer vanish without diagnostics.
- **Telegram Monitoring / Kaggle Recovery Grace Window**: `tg_monitoring` no longer drops recovery jobs immediately when Kaggle briefly reports `error/failed/cancelled` or when the local poll loop times out; recovery now keeps rechecking the kernel for a configurable grace window (`TG_MONITORING_RECOVERY_TERMINAL_GRACE_MINUTES`, default `360`) so late-written `telegram_results.json` can still be imported after the server already marked the scheduled run as failed.
- **VK Auto Queue / Historical Non-Event Prefilter**: long retrospective VK posts with rolled-forward historical dates in the text no longer bypass the conservative non-event prefilter just because `event_ts_hint` looks future-like; strong historical/info posts now skip before the LLM stage instead of repeatedly consuming queue slots and hitting `drafts_rate_limited`.
- **Smart Update / Tretyakov Venue Alias**: shared location-reference aliases now normalize `Третьяковская галерея` to `Филиал Третьяковской галереи, Парадная наб. 3, Калининград`, so VK/TG reposts can merge into existing Tretyakov parser events instead of creating duplicate rows under a shorter venue name.
- **Smart Update / Location Reference**: Smart Update now applies the canonical `docs/reference/locations.md` matcher to incoming `location_name/location_address` after the narrow hardcoded aliases and treats the matched venue city as authoritative, so noisy extractor outputs like `city='МОЛОДЕЖНЫЙ'` collapse back to the real Kaliningrad venue instead of creating a phantom city; the reference list now also includes `Клуб Спутник, Карташева 6, Калининград` and `М/К Сфера, Энгельса 9, Калининград`.
- **Qtickets / Source Parsing**: `source_parsing/qtickets.py` now accepts the current Kaggle `qtickets_events.json` contract (`date_raw`, `parsed_date`, `parsed_time`, `photos`, `ticket_price_min/max`, `ticket_status`) while remaining backward-compatible with the older legacy fields, preventing mass `missing_date` failures during `/parse`.
- **Telegram Monitoring / Location Canonicalization**: Telegram candidate building now normalizes `location_name/location_address/city` through the shared reference layer before Smart Update, so `/daily` no longer stores mixed venue spellings like `Bar Sovetov` vs `Бар Советов`; canonical aliases now also cover `Бар Советов` and typo variants of `Суспирия`.
- **Smart Update / Non-Event Guards**: VK/TG report/recap posts about already finished events now skip as `skipped_non_event:completed_event_report` when the text is in past tense (`приняли участие`, `встреча прошла`, `выразили благодарность`) and lacks concrete invite/registration/ticket signals, preventing Telegraph cards from being created for post-event reports; explicit continuation announcements such as `следующий показ будет ...`, `в следующий раз встречаемся ...`, and `вас вновь ждёт ...` are now exempt from this deterministic guard so hybrid `recap + future event` posts stay in the normal event pipeline.
- **Tools / Event Dedup Cleanup**: `scripts/inspect/dedup_event_duplicates.py` now repoints `vk_inbox.imported_event_id` and rewrites `event.linked_event_ids` during merges, so safe cleanup of legacy duplicate rows does not leave stale internal references after the dropped event is deleted.
- **Telegraph Pages / `/pages_rebuild`**: fixed a runtime `NameError` in the month/weekend rebuild helper path so production rebuilds can execute from the bot/runtime shell without crashing before weekend mapping is prepared.
- **CrumpleVideo / Poster Overlay**: Notebook overlay placement now scans the full poster for low-text zones, keeps fact-only overlays compact, and avoids styling the first line as a large title when the overlay only adds date/location facts.
- **General Stats / Parse Runs**: `/general_stats` now restores `/parse` `events_created/events_updated` from per-source `ops_run.details_json.sources` when older parse runs logged zero metrics, shows `updated=` in `/parse breakdown`, and `/parse` itself now writes run metrics from `stats_by_source` instead of chat-only added/updated lists.
- **Admin Reports**: Added `/recent_imports [hours]` superadmin report for events created or updated from `Telegram`, `VK`, and `/parse` over the last `N` hours (default `24`), with Telegraph-linked titles, dedupe by `event_id`, multi-message pagination, and `/a` routing for natural-language requests about recent source imports.
- **Smart Update / Matching**: Added a narrow `copy_post_ticket_same_day` deterministic rescue plus `city_noise_copy_post_shortlist` expansion for multi-event repost families, so same-date child events can still converge on existing rows when one source injects noisy `city/location_name` but the repost keeps the same ticket link and near-identical source text.
- Added `Smart Update ice-cream` `v2.16.1 iter3` dry-run duel artifacts with stricter anti-expansion guardrails, updated stage profiling, and new consultation synthesis for the `iter3` prompt-contract round.
### Fixed
- **Telegram Monitoring / Exhibition Duplicates**: Telegram `default_location` no longer blindly overwrites an explicitly grounded off-site venue from the post text, and Smart Update now has an earlier deterministic `longrun exhibition exact-title` rescue for same-range exhibition reposts / multi-slot posts, preventing the live duplicate patterns seen on the March 2026 prod snapshot.
- **Telegraph Month Pages**: `/pages_rebuild` month splitting now also splits oversized `Постоянные выставки` tails across continuation pages (instead of forcing every exhibition onto the final page), and the splitter can fall back all the way to `no ICS + no details` before publishing.
- **Scheduler / Superadmin Chat Resolution**: scheduled and recovery admin reports now resolve the target chat from the registered superadmin in SQLite first, with `ADMIN_CHAT_ID` kept only as a legacy fallback; this removes the hidden dependency on a separate secret for `vk_auto_import`, `tg_monitoring`, `/parse`, `/3di` recovery, `general_stats`, and related scheduler notifications.
- **General Stats / VK Auto Queue Scheduler Visibility**: scheduled `vk_auto_import` attempts that are skipped before entering the main run (shared heavy-job guard, unresolved superadmin chat, missing bot) now write `ops_run.status='skipped'` with a reason, and `/general_stats` prints `trigger=...` plus skip diagnostics so manual and scheduled VK runs are no longer indistinguishable in the report.
- **Preview 3D / Raw Image Fallbacks**: month/weekend/festival pages in `show_3d_only` contexts no longer substitute `photo_urls[0]` for missing `preview_3d_url`, so `/3di` gaps stay visibly empty instead of silently degrading into ordinary event pictures.
- **Preview 3D / Ops Run Status**: `/3di` `ops_run.status` now reflects partial failures (`partial_success` / `error`) instead of marking every completed Kaggle session as plain `success` even when previews rendered only partially or not at all.
- **Add Event Worker / Runtime Health**: `_watch_add_event_worker` now updates the shared dequeue timestamp via the module-level global during stall recovery, preventing `UnboundLocalError` crashes that could leave `/healthz` red even though the worker restart path was supposed to self-heal.
- **Preview 3D / Kaggle Supabase Runtime Env**: `/3di` now ships both `SUPABASE_KEY` and `SUPABASE_SERVICE_KEY` plus `SUPABASE_BUCKET` and `SUPABASE_MEDIA_BUCKET` into the Kaggle runtime datasets, so preview uploads stay enabled across old/new notebook variants instead of rendering for minutes and then failing with Supabase env mismatch.
- **Runtime Health / Fly Auto-Recovery**: `/healthz` now validates startup readiness, runtime heartbeat freshness, required background tasks, bot-session openness, and a live SQLite ping instead of returning a blind static `ok`, so Fly can mark “HTTP alive but Telegram/scheduler broken” instances unhealthy and recycle them automatically.
- **Smart Update / Address-Based Venue Rescue**: deterministic shortlist/match now keeps exact-title cross-source candidates together when venue names differ only as short alias vs official long form but the normalized `location_address` matches, preventing live duplicates like the same Gusev museum show imported from Telegram and VK into separate events.
- **VK Auto Queue / Stale Legacy Statuses**: stale `vk_inbox.status='importing'` rows are now requeued to `pending` together with stale `locked` rows, so legacy interrupted imports do not disappear from the modern auto-import scheduler.
- **Telegram Monitoring / Multi-event Poster Fallback**: when a multi-event Telegram post arrives without `posters[]` from upstream, server-side public-page fallback now scrapes the post images from `t.me/s/...`, reuses image-only photos across all split events, and still applies OCR-based filtering when the scraped posters contain readable event text.
### Added
- **Smart Update / Gemma Event Copy V2.15.10 Screening Grounding Retune**: Added a focused screening-only harness (`artifacts/codex/experimental_pattern_screening_grounding_retune_v2_15_10_2026_03_08.py`) that splits `screening_card` downstream into sentence-level lead/plot/support steps, adds a Gemma grounding-audit gate for unsupported claims, publishes a canonical report for the two screening cases, and preserves a manual micro-contract follow-up showing that the remaining `2659` support facts can be recovered without world-knowledge drift.
- **Smart Update / Gemma Event Copy V2.15.9 Downstream Assembly Retune**: Added a downstream-only retune harness (`artifacts/codex/experimental_pattern_downstream_assembly_retune_v2_15_9_2026_03_08.py`) that reuses winning extraction outputs from `v2.15.8`, removes `plan_lead` as an LLM step, switches to deterministic routing plus strict `assemble_lead / assemble_body`, incorporates external `Opus` and `Gemini` guidance, and publishes a 6-event report showing where contract-based assembly improves lead discipline and where it still leaks unsupported world knowledge.
- **Smart Update / Gemma Event Copy V2.15.8 Atomic Shape Batch**: Added a new 6-event atomic batch harness (`artifacts/codex/experimental_pattern_atomic_shape_batch_v2_15_8_2026_03_08.py`) over `screening_card`, `party_theme_program`, and `exhibition_context_collection`, saved full per-step prompt/output traces, ran strict `Opus` and `Gemini` consultations for shape/downstream design, and published a canonical batch report that isolates the next bottleneck in `plan_lead -> generate_lead -> generate_body` after extraction starts transferring across multiple shapes.
- **Smart Update / Gemma Event Copy V2.15.7 Atomic Step Tuning**: Added a third atomic single-event tuning harness (`artifacts/codex/experimental_pattern_atomic_step_tuning_v2_15_7_2026_03_08.py`) for contrastive `program_rich` event `2734`, expanded the cycle to pre-generation preparation (`normalize_concept -> normalize_setlist -> normalize_performer -> normalize_stage -> plan_lead -> generate_lead -> generate_body -> repair`), added grounded rescue for dropped setlist/performer/stage facts, and published a canonical report showing local wins across all key steps with full `6/6` semantic slot coverage.
- **Smart Update / Gemma Event Copy V2.15.6 Atomic Step Tuning**: Added a second atomic single-event tuning harness (`artifacts/codex/experimental_pattern_atomic_step_tuning_v2_15_6_2026_03_08.py`) for contrastive event `2687` (`lecture_person`) that splits the Gemma flow into `normalize_cluster -> normalize_theme -> normalize_profiles -> plan_lead -> generate_lead -> generate_body -> repair`, evaluates each request with shape-specific fact-slot metrics, saves per-step prompt/output traces, and publishes a canonical transfer report showing that the atomic approach carries to a different event shape while keeping full slot coverage and removing forbidden lecture boilerplate.
- **Smart Update / Gemma Event Copy V2.15.5 Atomic Step Tuning**: Added an atomic single-event tuning harness (`artifacts/codex/experimental_pattern_atomic_step_tuning_v2_15_5_2026_03_08.py`) for event `2673` that splits the Gemma flow into `normalize_subject -> expand_agenda -> normalize_program -> plan_lead -> generate_lead -> generate_body -> repair`, evaluates each request with local event-core metrics, saves per-step prompt/output traces, and publishes a canonical report showing local wins on the pre-repair steps before any new batch dry-run.
- **Smart Update / Gemma Event Copy V2.15.3 Step Profiling**: Added a single-event step profiler harness (`artifacts/codex/experimental_pattern_step_profile_v2_15_3_2026_03_08.py`) that saves prompt, schema, input state, output, and diagnostics after each LLM call and major deterministic stage; ran it on event `2673` and added both the raw step-profile report and a canonical review localizing the current root cause to the `normalize -> planner -> generation` chain.
- **Smart Update / Gemma Event Copy V2.15.3 Dry-Run**: Added a thirteenth `2.15.x` experimental harness (`artifacts/codex/experimental_pattern_dryrun_v2_15_3_2026_03_08.py`), a docs-only `v2.15.3` prompt-context pack, comparative `baseline / v2.13 / v2.14 / v2.15.2 / v2.15.3` outputs, a compact `baseline vs v2.15.3` text-only comparison report, and a grounded `v2.15.3` review with explicit verdicts against baseline and `v2.15.2`.
- **Smart Update / Gemini V2.15.3 Text Consultation**: Added a self-contained `gemini-3.1-pro-preview` consultation over the real `baseline vs v2.15.3` texts, prompt context, and dry-run diagnostics, preserved the raw external report, and added a canonical review that accepts the strongest prose-quality signals while rejecting overly broad baseline verdicts and hard-ban overreach.
- **Smart Update / Gemma Event Copy V2.15.3 Design Brief**: Added a canonical `2.15.3` brief that narrows the next round to quote/epigraph discipline, deterministic format enforcement, project-case prose cleanup, and stronger planner semantics without changing the `LLM-first` core architecture.
- **Smart Update / Gemma Deep Research Impact**: Added a canonical analysis of the new Gemma deep-research materials, incorporated the later-saved Gemini research as an additional source, extracted only the practically relevant Gemma-specific carries, and folded the useful ones into the `2.15.2` design brief.
- **Smart Update / Opus V2.15.3 Prompt Pack**: Added a strict one-shot `claude-opus-4-6` consultation that produced a full Gemma prompt pack for `normalize_floor`, `shape_and_format_plan`, `generate_description`, and `targeted_repair`; preserved the raw JSON, extracted the prompt pack into a readable report, and added a canonical review that accepts the prompt-repack direction while rejecting literal over-acceptance of inconsistent fields and overly rigid style rules.
- **Smart Update / Gemma Event Copy V2.15.2 Dry-Run**: Added a sixteenth experimental Gemma event-copy harness (`artifacts/codex/experimental_pattern_dryrun_v2_15_2_2026_03_08.py`), a docs-only `v2.15.2` prompt-context pack, comparative `baseline / v2.13 / v2.14 / v2.15.2` outputs, and a grounded `v2.15.2` review that explicitly compares the new round against baseline, `v2.13`, and `v2.14`.
- **Smart Update / Gemini V2.15.2 Text Consultation**: Added a self-contained `gemini-3.1-pro-preview` consultation over the real `v2.15.2` texts, sources, facts, format plans, and prompt context, preserved the raw external report, and added a canonical review that accepts the quote/format blockers while rejecting overly broad deterministic or semantic rewrites.
- **Smart Update / Event Copy Recommendations Master Retrospective**: Added a single canonical knowledge-base document that consolidates the main `Opus` and `Gemini` recommendations across the whole Gemma event-copy cycle, phase by phase, with explicit notes on what later confirmed in dry-runs and what did not.
- **Smart Update / Event Copy V2.15.2 Opus Prompt Profiling**: Added a strict one-shot `claude-opus-4-6` prompt-profiling consultation for the `2.15.2` Gemma architecture, preserved the raw JSON and markdown report, and added a canonical review that accepts dynamic prompt assembly, quote-aware normalization, facts-backed repair, and a smaller structural planner while rejecting a blind removal of the planning layer.
- **Smart Update / Event Copy External Consultation Retrospective**: Added a canonical synthesis of the strongest recurring `Opus` and `Gemini` recommendations across the event-copy cycle, separating stable carries from repeated weak ideas and making them reusable for future `2.15.x` rounds.
- **Smart Update / Gemma Event Copy V2.15.2 Design Brief**: Added a canonical `2.15.2` brief that turns `v2.15` into an atomic Gemma-friendly multi-step prompt architecture (`normalize -> pattern/format plan -> generate -> validate -> narrow repair`) and explicitly separates scalable core steps from optional boosters.
- **Smart Update / Gemini V2.15 Text Consultation**: Added a real `gemini-3.1-pro-preview` consultation over the canonical `v2.15` design brief, preserved the raw external report in `artifacts/codex/reports/`, and added a canonical review that separates useful text-quality guidance from overly rule-heavy architecture advice.
- **Smart Update / Gemma Event Copy V2.15 Design Brief**: Added a canonical `v2.15` design brief that restores a scalable pattern-driven generation layer on top of `full-floor normalization`, keeps baseline wins like epigraphs and structured lists as safe modules, and separates scalable core architecture from optional quality boosters.
- **Smart Update / Gemma Event Copy Retrospective**: Added a canonical retrospective for `baseline -> v2.14` that fixes the stable requirements for event-copy quality, traces the anti-cliche / anti-template work across the whole cycle, and summarizes each architecture by text quality, failure modes, LLM-vs-deterministic balance, and scalability.
- **Smart Update / Gemma Event Copy V2.14 Dry-Run**: Added a fifteenth experimental Gemma event-copy harness (`artifacts/codex/experimental_pattern_dryrun_v2_14_2026_03_08.py`), a docs-only `v2.14` prompt-context pack, a grounded `v2.14` review with explicit verdicts against baseline and `v2.13`, and a full post-run competitive consultation cycle (`Opus -> Gemini`) with synthesis for the next `v2.15` subtraction release.
- **Smart Update / Gemma Event Copy V2.13 Dry-Run**: Added a fourteenth experimental Gemma event-copy harness (`artifacts/codex/experimental_pattern_dryrun_v2_13_2026_03_08.py`), a docs-only `v2.13` prompt-context pack, a grounded `v2.13` review with explicit verdicts against baseline and `v2.12`, and a full post-run competitive consultation cycle (`Opus -> Gemini`) with a new synthesis for the next `v2.14` iteration.
- **Smart Update / Gemma Event Copy V2.12 Dry-Run**: Added a thirteenth experimental Gemma event-copy harness (`artifacts/codex/experimental_pattern_dryrun_v2_12_2026_03_08.py`), a docs-only prompt-context pack for the new `full-floor normalization` architecture, a grounded `v2.12` review with explicit baseline-relative verdict, and a full post-run competitive consultation cycle (`Opus -> Gemini`) with synthesis for the next `v2.13` iteration.
- **Tools / Opus Wait Policy**: Clarified that in strict `Opus` consultations long runtime and silent `claude -p` execution are normal for heavy reasoning/context loads; fallback or retry now requires a preselected timeout policy and explicit process/JSON failure evidence.
- **Tools / Opus Anti-Duplicate Launch**: Clarified the Opus consultation workflow to require one staged brief, one canonical launch, explicit JSON validation before any retry, and self-contained context instead of relying on file-path lists in strict `--tools ""` mode.
- **Tools / Strict Opus Only**: Clarified the default Opus consultation mode for Claude CLI: use full model id `claude-opus-4-6`, disable tools via `--tools ""`, avoid `plan`, and verify that raw JSON `modelUsage` contains only `claude-opus-4-6`.
- **Tools / Opus Prompt Review**: Clarified that for Opus consultations the agent may optionally ask for prompt-quality review and concrete prompt revisions when the task is materially about LLM behavior or text quality, especially for Gemma.
- **Tools / Opus Alias**: Clarified in agent instructions and tool runbooks that `Opus` in user requests is the canonical shorthand for `Claude CLI` with `--model opus`.
- **Tools / Complex Consultation**: Added a canonical `docs/tools/complex-consultation.md` workflow for trigger phrase `комплексная консультация`: first `Opus`, then `Gemini 3.1 Pro` with the Opus report included, followed by agent-side verification and synthesis.
- **Tools / Claude CLI**: Added a canonical `docs/tools/claude-cli.md` runbook for local Claude consultations, `--model opus` headless usage, and verifying the resolved model via JSON `modelUsage`.
- **Tools / Gemini CLI**: Added a canonical `docs/tools/gemini-cli.md` runbook for local Gemini consultations, explicit model selection, and verifying the real responding model via Gemini session logs.
- **Smart Update / Identity Longrun**: Added an offline quality-first identity-resolution benchmark and report for duplicate-vs-merge analysis on a fresh production snapshot, including expanded gold cases, mined control pairs, pairwise Gemma triage, and rollout guidance.
- **Smart Update / Opus Consultation Toolkit**: Added a reproducible dry-run runbook (`docs/operations/smart-update-opus-dryrun.md`) and a bundling helper (`scripts/inspect/prepare_opus_consultation_bundle.py`) to extract source texts from prod snapshots + `telegram_results.json`, package case materials, and hand over verifiable artifacts for external LLM review.
- **Smart Update / Opus Sample Refresh**: Expanded the external-consultation casebook with 10 more production-snapshot cases covering same-source triple duplicates, recurring repertory pages, same-day double shows, generic-ticket false friends, and same-slot false-merge controls; refreshed the ready-to-send casepack/bundle and prompt package around stable `*_latest` artifacts.
- **Smart Update / Stage 04 Consensus Dry-Run**: Added a reproducible Stage 04 consensus dry-run script (`artifacts/codex/stage_04_consensus_dryrun.py`), latest artifacts, and Stage 04 Opus handoff materials to narrow the deterministic preprod candidate with three evidence-based local runs on top of the Stage 03 baseline.
- **Smart Update / Gemma Event Copy V2 Dry-Run**: Added a second experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_2026_03_07.py`) plus comparative `baseline / v1 / v2` outputs and review for prompt-quality calibration on real events.
- **Smart Update / Gemma Event Copy V2.1 Dry-Run**: Added a third experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_1_2026_03_07.py`), comparative `baseline / v1 / v2 / v2.1` outputs, a grounded failure review, and a new ready-to-send Opus consultation package for the `v2.1` regressions.
- **Smart Update / Gemma Event Copy V2.2 Dry-Run**: Added a subtractive `v2.2` experimental harness (`artifacts/codex/experimental_pattern_dryrun_v2_2_2026_03_07.py`), comparative `baseline / v1 / v2 / v2.1 / v2.2` outputs, a grounded review, and a new ready-to-send Opus consultation package focused on real text-quality evidence.
- **Smart Update / Gemma Event Copy V2.3 Dry-Run**: Added a fourth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_3_2026_03_07.py`), comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3` outputs with source texts and intermediate fact layers, a grounded `v2.3` review, and a Gemini-ready consultation package explicitly requesting concrete prompt-level revisions for Gemma.
- **Smart Update / Gemma Event Copy V2.4 Dry-Run**: Added a fifth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_4_2026_03_07.py`), comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4` outputs, a critical review of the Gemini `v2.3` consultation response, and a grounded `v2.4` review showing where the patch pack improved structure but regressed overall text quality.
- **Smart Update / Gemma Event Copy V2.5 Dry-Run**: Added a sixth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_5_2026_03_07.py`), comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5` outputs, a grounded `v2.5` review, and a full Gemini consultation package/response/review over the real dry-run evidence set.
- **Smart Update / Gemma Event Copy V2.6 Dry-Run**: Added a seventh experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_6_2026_03_07.py`), a pre-run Gemini hypothesis consultation/review, comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5 / v2.6` outputs, a grounded `v2.6` review, and a full Gemini post-run consultation package/response/review focused on real text-quality evidence.
- **Smart Update / Gemma Event Copy V2.7 Dry-Run**: Added an eighth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_7_2026_03_07.py`), a pre-run Gemini hypothesis consultation/review around safe-positive transformations, comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5 / v2.6 / v2.7` outputs, a grounded failure review, and a full Gemini post-run failure-consultation package/response/review for the regression round.
- **Smart Update / Gemma Event Copy V2.8 Dry-Run**: Added a ninth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_8_2026_03_07.py`), a pre-run Gemini hypothesis consultation/review, comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5 / v2.6 / v2.7 / v2.8` outputs, a grounded `v2.8` review, a docs-only prompt context dump for external review, a full Gemini post-run consultation package/response/review, and a narrow Gemini sanitizer follow-up that identified the prompt-facing `Тема:` rewrite as part of the current runtime root cause map.
- **Smart Update / Gemma Event Copy V2.9 Dry-Run**: Added a tenth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_9_2026_03_08.py`), a pre-run Gemini hypothesis consultation/review, a docs-only `v2.9` prompt-context pack, comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5 / v2.6 / v2.7 / v2.8 / v2.9` outputs, a grounded `v2.9` review, and a full Gemini post-run consultation package/response/review focused on sanitizer bypass, dense fact shaping, and next-step prompt changes for Gemma.
- **Smart Update / Gemma Event Copy V2.10 Dry-Run**: Added an eleventh experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_10_2026_03_08.py`), a docs-only `v2.10` prompt-context pack, comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5 / v2.6 / v2.7 / v2.8 / v2.9 / v2.10` outputs, a grounded `v2.10` review, and a full Gemini post-run consultation package/response/review focused on `list consolidation`, action-oriented hints, presentation-case nominalization, and anti-quote control.
- **Smart Update / Event Copy V2.11 Consultation + Dry-Run**: Added a cross-model `Opus -> Gemini` consultation review for the `v2.11` patch pack, a twelfth experimental Gemma event-copy dry-run harness (`artifacts/codex/experimental_pattern_dryrun_v2_11_2026_03_08.py`), comparative `baseline / v1 / v2 / v2.1 / v2.2 / v2.3 / v2.4 / v2.5 / v2.6 / v2.7 / v2.8 / v2.9 / v2.10 / v2.11` outputs, and a grounded `v2.11` review covering post-merge semantic dedup, anti-quote generation rules, scoped list consolidation, and clause-style nominalization.
- **Smart Update / Event Copy V2.12 Consultation Direction**: Added a new results-focused consultation brief, a strict `claude-opus-4-6` architecture consultation, a Gemini second opinion, and a synthesis report that reframes `v2.12` around shape-routed full-floor LLM normalization, deterministic cleanup, and baseline-relative success criteria instead of another single-pass extraction patch pack.

### Changed
- **Docs / MobileFeed Evergreen CTA Routing**: promoted the daily-mode CTA decision into canon so `popular_review` no longer defaults to month-locked outer headlines; the default stack is now an evergreen external promise (`ВЫБЕРИ СОБЫТИЕ`), inside-phone upper CTA (`ПОПУЛЯРНОЕ`), inside-phone lower count-driven CTA (`N АНОНСОВ`), and a separate payload-driven temporal layer.
- **Docs / MobileFeed Ribbon Mechanics Guardrails**: after preview regression feedback, the canonical `Мобильная лента` brief now explicitly forbids mirrored poster/text orientation, forbids ribbon penetration into the phone or desk, and reframes the material target as dense coated magazine stock with developable bending and limited twist instead of an elastic soft strip.
- **Docs / MobileFeed Intro Handoff + Story-Safe Contract**: tightened the canonical `Мобильная лента` requirements so the current implementation pass stops strictly at the first 2D-ready handoff poster, treats overlapping viewer-facing text as a defect, requires story-safe layout margins for CTA/date/city content, formalizes internal on-screen CTA layering above/below the poster, and upgrades the intro motion target to an easing-driven premium `~1.5s` push-in with credible glossy-paper ribbon behavior against the phone and desk surfaces.
- **Docs / MobileFeed Ribbon Fit Contract**: tightened the `Мобильная лента` canon so the poster ribbon now explicitly forbids poster cropping and visible per-poster borders/gutters, requires equal poster height with native-width preservation and flush edge-to-edge joins, and locks the ribbon scale to the second poster whose width must match the phone screen width at the `video_afisha` handoff zoom target.
- **Docs / Popular Review 3D Intro Direction**: upgraded the `popular_review` intro plan from a tentative next-step note to a confirmed requirement: a short `1.0-1.5s` 3D intro on a fresh Blender branch, rendering only the opening beat and then handing off to the improved legacy `video_afisha.ipynb` scene flow; the design brief now also records the current official Blender baseline (`v4.4.3` latest stable, `4.5 LTS` active LTS on blender.org).
- **Docs / Popular Review CTA Semantics**: refined the intro brief so CTA now targets the viewer's actual decision (`куда пойти / что посмотреть`) instead of “finding a date”, forbids mixed sparse-date notations that can fake a range inside one frame, and recommends comparing CTA copy on the same layout/data before picking the final poster language.
- **Docs / Popular Review CTA + Clean Frame**: refined the `popular_review` intro brief after approval feedback: `V2 Ticket Stack` is deprioritized for the next round, covers should hide service/debug copy and keep only viewer-facing text, CTA is now a first-class design rule (`НАЙДИ СВОЮ ДАТУ` / `N СОБЫТИЙ ВНУТРИ` / `СМОТРИ ПОДБОРКУ`), and the next motion experiment is documented as a short `~1s` 3D typographic pre-roll before the improved legacy `VideoAfisha` scene flow.
- **Docs / Popular Review Intro Boundary**: clarified in the canonical `popular_review` spec/design brief that scene generation remains on legacy `VideoAfisha`, while the new work upgrades intro typography, cover language, and the intro-to-scene handoff; approval mockups are now expected to use fresh prod-snapshot candidate sets from the `/popular_posts` pool instead of stale local snapshots.
- **Guide Excursions / Publish Target Channel**: guide digest manual/scheduled publication is now documented and configured via `GUIDE_DIGEST_TARGET_CHAT`, with project env defaults pointed at `@wheretogo39` instead of hardcoded references to `@keniggpt` in the current runtime path.
- **Reports / Smart Update Duplicate Casebook**: Extended the canonical casebook with a fresh `2026-03-09` production-snapshot investigation of far-future events, including the full `> 9 months` list, the separate `> 6 months` confirmed VK false-create, and the grounded root-cause split between Telegram OCR year-rollover and far-future body-vs-poster date conflicts.
- **Smart Update / Opus + Gemini Master Retrospective**: Extended the master recommendations retrospective with the late `2.15.3` carries from the strict `Opus` prompt-pack round, the Gemini post-run text consultation, and the new step-level profiling findings so the shared knowledge base keeps accumulating recent evidence instead of freezing at `2.15.2`.
- **Smart Update / Opus V2.15.3 Prompt Pack Review**: Clarified explicitly how much of the Gemma research carry `Opus` actually respected, what it implemented well in the prompt pack, and which remaining schema/style inconsistencies still had to be corrected locally before the real `2.15.3` run.
- **Smart Update / Gemma Event Copy V2.15.2 Design Brief**: Clarified the separation between richer editorial pattern vocabulary and the smaller execution pattern set for Gemma, so runtime `2.15.2` can preserve prose variability without overloading the model with an overly abstract pattern taxonomy.
- **Smart Update / Gemma Event Copy V2.15.2 Design Brief**: Added an explicit link to the new master retrospective of `Opus` and `Gemini` recommendations so `2.15.2` implementation can rely on one canonical knowledge base instead of scattered consultation reviews.
- **Smart Update / Gemma Event Copy V2.15.2 Design Brief**: Refined the `2.15.2` brief after strict `Opus` prompt profiling so the pipeline now explicitly targets a `2-3` LLM-call happy path, uses a deterministic / tiny-hybrid `shape_and_format_plan`, adds quote metadata to normalization, shifts toward dynamic generation-prompt assembly, and makes repair facts-backed instead of vibe-based.
- **Smart Update / Event Copy External Consultation Retrospective**: Extended the retrospective with late-stage `2.15.x` prompt-profiling carries, including dynamic prompt assembly, stop-phrase de-duplication, register-over-persona guidance, facts-backed repair, and the distinction between a smaller execution pattern set and a richer editorial pattern vocabulary.
- **Smart Update / Gemma Event Copy V2.15 Design Brief**: Added an explicit top-level text-quality checklist to the canonical `v2.15` brief so future `2.15.x` iterations can validate prose goals against one compact reference list instead of reading them only across expanded subsections.
- **Smart Update / Gemma Event Copy V2.15 Design Brief**: Expanded the canonical `v2.15` brief with a fixed future iteration numbering policy (`2.15 -> 2.15.2 -> 2.15.3`, then `2.16` on major redesign) and a detailed target text-quality profile covering naturalness, professionalism, anti-cliche hygiene, formatting expectations, and scalability constraints.
- **Smart Update / Gemma Event Copy Retrospective**: Expanded the canonical `baseline -> v2.14` retrospective with attributed `Opus` and `Gemini` evaluation signals from existing consultation rounds, explicitly separating external-model judgments from the final local synthesis.

### Fixed
- **Guide Excursions / Materialized Digest Media**: guide Kaggle scan now exports per-post media files, server import persists them under `GUIDE_MEDIA_STORE_ROOT`/`guide_monitor_post.media_assets_json`, digest publish sends albums only from those materialized assets, and production Fly runtime keeps `GUIDE_EXCURSIONS_LOCAL_FALLBACK_ENABLED=0`; publish-time `forward -> file_id` / Telethon / web fallbacks are no longer used, and missing media now abort publication instead of silently degrading to text-only.
- **Telegram Monitoring / Recovery + Statuses**: `tg_monitoring` now registers running Kaggle kernels in the shared `kaggle_registry`, `kaggle_recovery` can resume completed kernels after bot restarts and re-import their `telegram_results.json`, and interrupted runs no longer fall through to false `success` with zero metrics; completed empty reports are now marked `empty` instead.
- **3D Preview / Kaggle Runtime Attachments**: Preview3D now follows the same Kaggle split-secrets flow as Telegram Monitoring: separate cipher/key datasets, longer dataset propagation wait, and shared `KaggleClient.push_kernel(...)` handling for `dataset_sources`, so manual/prod `/3di` runs no longer fail at notebook startup when Kaggle attaches runtime datasets slowly.
- **3D Preview / Scheduled `/3di`**: Preview3D Kaggle runs now receive Supabase runtime config and secrets through encrypted split datasets (`config.json` + `secrets.enc`/`fernet.key`) before render/upload, so scheduled night runs no longer finish with `previews_rendered=0` only because `SUPABASE_URL/SUPABASE_KEY` were missing inside Kaggle.
- **VK Auto Import / Multi-Post Duplicates**: Exact duplicate child drafts from one VK multi-poster schedule post now collapse before persistence when `date + explicit time + venue + normalized title` match, and VK persistence derives `post_id/group_id` from `wall-...` URLs so same-post idempotency can converge earlier on retries/near-duplicates.
- **Smart Update / Far-Future Create Guard**: Events more than `9` months ahead now require a minimum grounding score on create (`explicit year`, canonical parser source, specific non-generic ticket flow); Smart Update logs `grounding_score` and `strong_doubt_score`, and contradictory poster OCR or recap/congrats context rejects the create instead of silently allowing a weak far-future card through.
- **General Stats / Telegram Monitoring**: `/general_stats` now shows how many events Telegram monitoring created and updated during the report window, including per-run `events_created/events_updated` in the TG runs block (`events_merged` is rendered as `events_updated`).
- **General Stats / Source Coverage**: `/general_stats` now shows source-share coverage for `vk` / `telegram` / `/parse` both for events touched in the report window and for the current active future inventory; shares are event-level coverage and may overlap for multisource events.
- **Event Parse / Gemma Robustness**: Gemma parse now uses stricter JSON-only instructions (including explicit `[]` for image-only schedule wrappers like “листайте афиши”), and falls back to 4o after an invalid-JSON repair failure instead of stopping immediately with `bad gemma parse response`.
- **Smart Update / Title Grounding**: Create-bundle prompts now explicitly ban invented editorial/ideological titles and interpretive `short_description` framing, while deterministic title grounding requires broader source overlap so partial-token matches no longer approve fabricated titles.
- **VK Parse / Giveaways**: Prize-only giveaway posts (e.g. “разыгрываем билеты на матч/концерт”) no longer create pseudo-events: VK parse now gets an explicit LLM hint to return `[]` for prize-only contests, and Smart Update ignores date/time facts that appear only inside giveaway-mechanics/prize sentences.
- **Smart Update / Merge Quality**: Added a narrower Stage 04 deterministic identity layer before LLM (`same_post_exact_title`, `same_post_longrun_exact_title`, `broken_extraction_address_title`, `specific_ticket_same_slot`, `doors_start_ticket_bridge`, `cross_source_exact_match`) plus blockers for `generic_ticket_false_friend` and multi-event same-source programs; raw ticket-link equality no longer acts as a broad auto-merge shortcut.
- **Smart Update / Matching**: Fixed two live duplicate-quality gaps on top of the Stage 04 layer: exact-title city-noise duplicates can now re-enter the shortlist via a narrow `date + venue + exact title` fallback, and wrong same-family sibling selections are redirected before merge when another shortlisted sibling has the matching title anchors.
- **Daily Announcements (Telegram)**: Prevented repeated `/daily` sends for the same channel/day by adding an in-process scheduler dedup guard (inflight + sent-today cache); persist `last_daily` after partial successful sends; and mark today as handled on `forbidden/chat not found` so the scheduler does not backfill old batches later when channel rights are restored.
- **Daily Announcements**: Location lines now suppress duplicated address/city fragments when `location_name` already embeds them, preventing `/daily` outputs like `..., Мира 9, Калининград, Мира 9, #Калининград`.
- **Location Canonicalization**: Known venues and Telegram `default_location` are now unpacked into structured `location_name/location_address/city` fields instead of storing the full `venue, address, city` string in `location_name`, reducing duplicate-location artefacts at the source.
- **Telegram Monitor / Kaggle Polling**: Reworked dynamic timeout to be source-count driven from production baseline (`~3.64 min/source`) with default `+30%` safety via `TG_MONITORING_TIMEOUT_SAFETY_MULTIPLIER=1.3`, so `/tg` scales with channel count and avoids under-sized limits on long runs.
- **Telegraph Event Pages**: Removed synthetic blank `&#8203;` spacer right before `<ul>/<ol>` list blocks on event/source pages, while preserving other existing paragraph spacings.
- **Deploy**: Excluded local backups, `__pycache__`, `.pytest_cache`, and temp directories from Docker build context to avoid oversized Fly deploy uploads.
- **VK Auto Queue**: Prevented `/vk_auto_import` from being killed by OOM on small machines by making N+1 prefetch lightweight by default; full media/OCR/LLM prefetch is now opt-in via `VK_AUTO_IMPORT_PREFETCH_DRAFTS=1`.
- **Ops Run / VK Inbox**: On app startup, orphaned `ops_run(status=running)` are marked as `crashed` and VK inbox locks are released, so queues recover automatically after restarts/OOM.
- **Media**: Added a guardrail for WEBP/AVIF→JPEG conversion (`ENSURE_JPEG_MAX_PIXELS`, default `20000000`) to skip oversized images instead of risking OOM.
- **Smart Update / Tickets**: Prevented donor compensation amounts (e.g. “компенсация 1063 руб.”) from being treated as ticket prices; blood donation actions are auto-marked free so Telegraph/VK summaries don’t show them as paid.
- **Smart Update / Matching**: Normalized Russian `ё`→`е` in title matching to prevent duplicate events that differ only by that letter.
- **Smart Update / Giveaways**: Ticket-giveaway promos with only deadline/result dates (e.g. “итоги 14.03”) are now skipped as `skipped_giveaway` (`giveaway_no_event`) to avoid pseudo-events.
- **Telegraph / Formatting**: Reformatted viewer-review bullets like `Имя: ...` in review sections into a quote + attribution style (`> «…»` + `> — Имя`) for better readability on Telegraph.
- **Smart Update / Past Events**: Automated imports now skip events that already ended before today (local date) as `skipped_past_event` (`past_event`) to avoid useless event/Telegraph/ICS load.
- **Smart Update / Far-Future Dates**: When creating events more than `SMART_UPDATE_FAR_FUTURE_REVIEW_MONTHS` months ahead, Smart Update checks poster OCR for an explicit conflicting `DD/MM` (or `DD <month>`) date and auto-sets `event.silent=1` on mismatch to prevent public-facing wrong far-future dates.
- **Admin / Delete Event**: Event deletion no longer attempts to delete source VK wall posts; only bot-managed VK posts (`event.vk_source_hash` present) are deleted.
- **VK Auto Queue / Event Parse**: Added a conservative prefilter for obvious long historical/admin non-event VK posts before full `event_parse`; ambiguous or event-like posts still go through the normal LLM parse unchanged, reducing wasted TPM on repeated non-events.
- **General Stats / Festivals Queue**: `/general_stats` now shows current festival queue snapshot (`total/pending/running/done/error`), active backlog (`pending+running`), and active breakdown by source (`vk/tg/url`) in addition to daily inflow.

## [1.12.0] - 2026-03-04
### Highlights
- **Smart Update (feature bundle)**: unified create/merge pipeline for VK/TG/`/parse` with source logs, Telegraph consistency, festival-context routing, and out-of-region filtering safeguards.
- **LLM Rate-Limit Control Framework (feature bundle)**: Supabase limiter + resilient fallback/retry/defer behavior across Smart Update, event parsing, Telegram monitoring, and VK auto queue.
- **Gemma Migration (feature bundle)**: transition to Gemma-first processing for event parsing and Smart Update (4o remains fallback/override where explicitly configured).
- **Telegram Monitoring (feature bundle)**: `/tg` operational flow (source management, trust/festival settings, Kaggle processing, Smart Update import, scheduled runs, and operator-facing reports).
- **VK Auto Queue Import (feature bundle)**: automatic VK inbox processing (`/vk_auto_import` + scheduler), including progress/reporting, prefetch pipeline, cancellation/defer handling, and Smart Update integration.

### Added
- **Locations**: Added `Центр культуры и досуга, Калининградской ш. 4А, Гурьевск` to the canonical venue list in `docs/reference/locations.md`.
- **General Stats**: Added `/general_stats` superadmin command with a 24-hour rolling report window (`[start_utc, end_utc)`) and explicit Kaliningrad period boundaries in the message.
- **General Stats**: Added scheduled daily report job (`ENABLE_GENERAL_STATS`, `GENERAL_STATS_TIME_LOCAL`, `GENERAL_STATS_TZ`) with delivery to both `OPERATOR_CHAT_ID` and `ADMIN_CHAT_ID` when configured.
- **Post Metrics**: Added `/popular_posts` superadmin command to find TG/VK posts that created events and performed above per-channel medians (3-day and 24-hour windows), including source + Telegraph links and `⭐/👍` markers.
- **Admin Action Assistant**: Added `/assist` (alias: `/a`) and menu button `🧠 Описать действие` to map an admin’s natural-language request to an existing bot command via Gemma, show a preview + confirmation, and execute only after approval.
- **Supabase Storage**: Added bucket usage guard helper (`supabase_storage.check_bucket_usage_limit*`) to keep media buckets below a safe limit (default `490MB`, configurable via ENV wrapper).
- **Ops Run Log**: Introduced unified `ops_run` SQLite table (`kind/trigger/chat_id/operator_id/started_at/finished_at/status/metrics_json/details_json`) with indexes for operational run tracking.
- **Festivals Queue**: Added unified `festival_queue` pipeline (`/fest_queue`) with manual processing, status mode (`-i/--info`), source filters (`--source=vk|tg|url`), and scheduler hooks.
- **Ticket Sites Queue**: Added recurring `ticket_site_queue` pipeline (`/ticket_sites_queue`) that auto-enqueues ticket links discovered during Smart Event Update (any source: Telegram/VK/manual) (pyramida.info / домискусств.рф / qtickets) and enriches events via Kaggle parsing + Smart Update.
- **Festivals Metadata**: Added `festival_source/festival_series` support for Telegram and VK sources (including Alembic migration + seed of known festival Telegram channels).
- **Holidays**: Added псевдо‑фестивали «Масленица» (подвижный, `movable:maslenitsa`) и «8 Марта» в `docs/reference/holidays.md`.
- **Geo Region Filter**: Added deterministic Kaliningrad-only guard (allowlist + SQLite cache + Wikidata check + Gemma fallback) for Smart Update imports.
- **Testing**: Added unit tests for `collect_general_stats` on temporary SQLite (`tests/test_general_stats.py`) including half-open window boundary checks.
- **E2E/Smoke**: Added release smoke scenarios (`release_smoke_smart_update`, `release_multisource_control`, `festival_queue`, `smoke_vk_access`) and expanded live-E2E checks.
- **E2E/Smoke**: Added automated smoke check that `/start` → «➕ Добавить событие» creates/updates events via Smart Update (bot source-log + poster fact).
- **Ops Scripts**: Added `scripts/preflight_release_smoke.py`, `scripts/run_bot_dev.sh`, and `scripts/seed_dev_superadmin.py` for local/live validation workflows.
- **Ops Scripts**: Added `scripts/inspect/dedup_event_duplicates.py` to find and merge high-confidence duplicate events in SQLite (creates a backup under `artifacts/db/` before applying).
- **Ops Scripts**: Added `scripts/inspect/audit_media_dedup.py` to audit Supabase media deduplication over the last N hours (DB scan + optional Storage HEAD checks).
- **Telegram Monitor**: `/tg` now shows live Kaggle kernel status (polling) and sends a detailed per-event report with Telegram + Telegraph links.
- **Telegram Monitor**: Source list now includes per-channel stats (last scan/message, counts) and supports delete.
- **Telegram Monitor**: `/tg` → source list now shows per-channel median `views/likes` and days covered within the `POST_POPULARITY_HORIZON_DAYS` window (operator baseline for ⭐/👍).
- **Telegram Sources**: Added canonical Telegram monitoring source catalog (`docs/features/telegram-monitoring/sources.yml`) with trust/defaults/filters + idempotent seeding (`scripts/seed_telegram_sources.py` and `/tg` → «🧩 Синхронизировать источники»).
- **E2E Telegram Sources**: Added UI scenario verifying canonical Telegram sources + pagination (`tests/e2e/features/telegram_sources_seed.feature`).
- **Telegram Monitor (Kaggle)**: Supabase poster uploads use short object keys (configurable prefix `TG_MONITORING_POSTERS_PREFIX`, default `p`) to minimize public URL length.
- **Telegram Monitor (Kaggle)**: Added message-level video export/upload to Supabase (`messages[].videos[]` + `video_status`) with max file size `TG_MONITORING_VIDEO_MAX_MB=10` and a stricter video bucket safe-guard `TG_MONITORING_VIDEO_BUCKET_SAFE_MB=430`.
- **Telegram Monitor (Kaggle)**: Added source-level metadata export in `telegram_results.json` (`schema_version=2`, top-level `sources_meta[]` with `title/about/about_links/meta_hash/fetched_at`) plus best-effort LLM suggestions for `festival_series`/`website_url`.
- **Telegram Monitor UI**: Added `/tg` debug button `♻️ Импорт из JSON` to rerun server import from one of the recent local `telegram_results.json` outputs (default: last 4) without waiting for a new Kaggle run.
- **Telegram Monitor UI (DEV)**: Added `/tg` mode `DEV: Recreate + Reimport` (only when `DEV_MODE=1`) with confirm preview; it deterministically deletes matching telegram events (`event_source.source_url`), clears `joboutbox` for affected `event_id`, clears `telegram_scanned_message` marks for `(source,message_id)` pairs from the selected JSON, then reruns import.

### Changed
- **Telegram Monitor**: Nightly monitoring now performs a bounded recent-rescan pass only for already known Telegram posts that previously yielded events within `TG_MONITORING_DAYS_BACK`, so channels without new messages still refresh `views/likes` for the last few days without re-running OCR/media/LLM extraction on old posts; added explicit `TG_MONITORING_RECENT_RESCAN_ENABLED` and `TG_MONITORING_RECENT_RESCAN_LIMIT` env knobs for that backfill.
- **Post Metrics / Popular Posts**: `/popular_posts` now treats the 3-day window as “posts published in the last ~3 days” and picks the latest available per-post snapshot (`age_day<=2`, preferring `age_day=2`) instead of requiring an exact `age_day=2` row, so quiet Telegram channels no longer produce systematically empty 3-day reports.
- **Smart Update / Gemma Event Copy V2.15.3 Brief**: Expanded the `2.15.3` brief from a narrow delta-only patch note into a full carry-forward document that explicitly preserves the base text-quality goals, architectural invariants, pattern-driven philosophy, and the rule that narrow metrics cannot replace editorial quality.
- **Smart Update / Gemma Event Copy V2.15.3 Brief**: Reframed `2.15.3` from a narrow bugfix round into a full prompt-pack repack for Gemma within the same `LLM-first` architecture, explicitly requiring self-contained/sectioned prompts across normalization, planning, generation, and repair instead of treating the new Gemma research as a few local carry tweaks.
- **Event Parse / Locations**: Unknown venues are no longer canonicalized to a known entry from `docs/reference/locations.md` by generic tokens like `школа`; when the post contains an explicit conflicting address, raw `location_name/location_address/city` are preserved instead of building a hybrid location line.
- **LLM Gateway / Gemma**: Prompt TPM reservation is now more conservative for long Cyrillic/OCR-heavy inputs, reducing cases where Supabase `reserve` passed but Google AI still returned provider-side `429 tpm`.
- **VK Auto Queue**: Removed redundant Telegraph wait from the hot path when inline Telegraph jobs are enabled, and added automatic slow-row stage timing logs for long VK imports.
- **LLM Gateway / Gemma**: Provider-side `429` is now fail-fast in `GoogleAIClient` (no internal multi-minute retry loop); waiting/defer logic remains in higher-level workflows like event parse and VK auto queue.
- **LLM Gateway / Supabase**: `google_ai_mark_sent` and `google_ai_finalize` now retry transient RPC/network failures before giving up, reducing accumulation of stuck `google_ai_requests.status='reserved'`.
- **LLM Gateway / Supabase**: Added stale-reservation recovery SQL (`migrations/003_google_ai_sweep_stale.sql`) and inspect helper `scripts/inspect/sweep_google_ai_stale.py` to safely sweep old `reserved` rows with `sent_at is null`.
- **Run Instrumentation**: Added `ops_run` logging for `vk_auto_import`, `/parse` (manual + scheduled + if-changed skip), `/3di` (manual + scheduler), `festival_queue` (manual + scheduler), and `tg_monitoring` runs, including successful zero-result runs.
- **/3di (Preview3D)**: Preview images are now uploaded to Supabase Storage (`SUPABASE_MEDIA_BUCKET`, prefix `p3d/event/<event_id>.webp`) instead of Catbox; added manual cleanup scripts for test Storage.
- **General Stats**: Daily `/general_stats` now includes per-run durations (`took=...`) for ops-run instrumented jobs.
- **General Stats**: `/general_stats` now shows VK queue state (`vk_queue_added_period`, `vk_queue_parsed_period`, `vk_queue_unresolved_now`) so operators can see daily inflow, processed volume, and current unresolved backlog.
- **Scheduler**: Added optional heavy-job serialization (`SCHED_SERIALIZE_HEAVY_JOBS=1`) to avoid overlapping multi-hour jobs (Telegram monitoring, VK auto-import, `/parse`, `/3di`, etc.).
- **Schema**: Added `festival.updated_at` (with migration/backfill and update trigger) for daily updated-festival metrics.
- **Schema**: Added immutable `geo_city_region_cache.created_at` (with migration/backfill) for first-seen city reporting in general stats.
- **Schema**: Added `telegram_post_metric` and `vk_post_metric` tables to store daily `views/likes` snapshots per post (for popularity analytics).
- **Geo Cache Upsert**: Updated city-region cache writes to preserve `created_at` on conflict updates.
- **Help**: Added `/general_stats` to `HELP_COMMANDS` with `superadmin` access.
- **Event Parse Backend**: `parse_event_via_llm` now defaults to Gemma for VK/TG draft extraction (4o remains optional fallback/override).
- **VK Auto Queue**: Added N/N+1 prefetch pipeline (`VK_AUTO_IMPORT_PREFETCH`) and optional inline post-import jobs (`VK_AUTO_IMPORT_INLINE_JOBS`, `VK_AUTO_IMPORT_INLINE_INCLUDE_ICS`) to speed up and stabilize operator runs.
- **VK Auto Queue**: Unified Smart Update report now can include `views/likes` + multi-level popularity markers (e.g. `⭐⭐` / `👍👍`) for the source post when metrics are available (baseline is per-group and excludes non-event posts).
- **VK Auto Queue**: Auto-import queue picking now runs in strict chronological order (oldest→newest globally, including cross-community rows) to avoid newer event facts being overwritten by older posts processed later.
- **E2E/Smoke**: Release smoke scenario set now includes псевдо‑фестиваль «Масленица» из holidays (VK → Smart Update → фестивальная страница/индекс).
- **Telegram Sources UI**: `/tg` source management now supports `festival_series` editing from inline controls (`🎪 Фестиваль`).
- **Telegram Sources UI**: `/tg` list supports pagination and normalizes added usernames (lowercase, `@`/URL/post-id safe) to avoid duplicates.
- **Telegram Sources UI**: `/tg` list now shows suggested festival series/site from source metadata and provides `✅ Принять подсказку` (applies only when manual `festival_series` is empty).
- **Telegram Monitor**: Import stage now streams per-post progress in `/tg` (Telegram post `X/Y`, Smart Update counters, `event_ids`, illustrations delta, `took_sec`) and sends per-post `Smart Update (детали событий)` right after each post is processed.
- **Telegram Monitor**: Server import now persists message-level videos into `event_media_asset` when a post maps to exactly one imported event; multi-event posts skip video attachment (`skipped:multi_event_message`) and `/tg` shows explicit video media status (`supabase`/`skipped:*`).
- **Smart Update (UI)**: Operator reports now show when items were enqueued into `festival_queue` / `ticket_site_queue` (so missing lines are a quick signal that URL/context extraction needs fixing).
- **Ticket Sites Queue / Smart Update**: Enqueue is now fully centralized in Smart Update for any caller (VK/TG/manual/`/parse`); all detected ticket-site URLs from `source_text`/`source_url`/`ticket_link`/`links_payload` are queued with the current `event_id`, while Telegram Monitoring only forwards hidden links via `candidate.links_payload`.
- **Smart Event Update Text Quality**: Added normalization for jammed inline bullets and minimal heading injection (`### ...`) for long multi-paragraph descriptions.
- **Smart Event Update Text Quality**: Added optional light emoji accents in `event.description` for readability (enabled by default; controlled via `SMART_UPDATE_DESCRIPTION_EMOJI_MODE`).
- **Fact-first Smart Update (Research)**: Refined Gemma-only dryrun pipeline (sources → facts → text) with strict coverage/forbidden checks, plus derived `short_description` and `search_digest`; documented the contract in `docs/features/smart-event-update/fact-first.md`.
- **Smart Event Update**: Public `description` is now generated fact-first (sources → facts → text, variant C+D) strictly from extracted facts, controlled by `SMART_UPDATE_FACT_FIRST` (default: enabled; rollback: set to `0`).
- **Docs (Smoke)**: Release smoke checklist now includes `/general_stats` verification on both test and prod (manual + scheduled delivery checks).
- **Smart Event Update**: Incoming hashtags are stripped from titles/descriptions/source text before saving and Telegraph rendering.
- **Linked Events / Telegraph**: Event pages now show `🗓 Другие даты` in the infoblock for linked occurrences; linked-event groups are recomputed symmetrically across Smart Update, source parsing, and manual edits, and linked pages are refreshed via `telegraph_build`.
- **Telegram Monitor (Kaggle)**: Extraction/ OCR now use message date context and infer missing event years relative to the message date.
- **Docs (Backlog)**: Added design spec for multi-provider movie metadata + cinema showtimes feature, including Kinopoisk Unofficial API notes (`docs/backlog/features/movie-showtimes/README.md`).
- **Smart Update (UI)**: Unified per-event reports are now more compact and actionable: the Telegraph link is embedded into the event title (no duplicated `Telegraph:` line when URL exists), `ICS` is shown as a short link label, the report includes the full source history list, and it shows video counts alongside illustrations.
- **Post Metrics / Popular Posts**: `/popular_posts` now renders created event IDs alongside Telegraph links and always shows explicit sample sizes (posts/sources; metrics vs imported events) plus an “above median” breakdown (views/likes/both) to make sparse-result windows easier to audit.
- **Ops / Rebuild**: `/rebuild_event` now supports `--regen-desc` to regenerate fact-first descriptions from stored facts before enqueuing rebuild jobs.

### Fixed
- **Admin Assistant / Coverage**: `/a` allowlist is now synced with the registered slash-command surface (including `/rebuild_event`, `/telegraph_cache_stats`, `/telegraph_cache_sanitize`, `/ik_poster`, `/start`, `/register`, `/assist_cancel`, and stateful `/cancel`), and explicit command-like inputs such as `rebuild_event 123` or `/recent_imports 48` now route deterministically before Gemma fallback.
- **Admin Assistant / Recent Imports Routing**: `/a` now recognizes source-origin list requests like “события из телеграм и вк за сутки” as `/recent_imports` instead of drifting into the calendar-day `/events` date picker; `/help` also exposes `/recent_imports` and `/popular_posts` for better report discoverability.
- **Kaggle Polling / `/tg` + `/v`**: Kaggle-driven runs now confirm notebook metadata is bound to the expected temporary dataset(s) before polling and re-check the binding before consuming terminal output, preventing stale/foreign notebook runs from being mistaken for the current Telegram monitor or video session.
- **Telegraph Pages**: Removed the extra visual blank line between description subheadings (`h3/h4`) and the first text block on event/source Telegraph pages, while keeping normal paragraph spacing intact.
- **Smart Update (fact-first)**: Improved facts-extraction prompt so short program lists (e.g., film lineups) are returned as individual facts and can appear in the generated `description`.
- **VK Auto Queue / SQLite**: Queue state writes (`imported/failed/rejected/pending/skipped`) now retry transient `database is locked` commit failures, so local VK auto-import is less likely to fail after a successful Smart Update.
- **Event Parse Prompt**: `parse_event_via_llm` now loads only the parser-specific `MASTER-PROMPT` block from `docs/llm/prompts.md` instead of unrelated digest/classifier/metadata prompt sections, substantially reducing Gemma TPM usage for VK/TG draft extraction.
- **VK Auto Queue / Event Parse**: Long VK posts now apply an OCR prompt budget before `event_parse`: poster OCR is skipped for already-detailed posts and capped for short posts, reducing oversized Gemma requests that could hit provider-side `429 TPM` in auto queue.
- **VK Auto Queue / Event Parse**: Festival normalisation context is now passed only for sources marked `vk_source.festival_source=1`, avoiding unnecessary prompt bloat for regular VK posts while preserving festival-specific parsing routes.
- **Exhibitions / Telegraph**: Synthetic `end_date = start + 1 month` remains available for listings and merge logic, but Telegraph event infoblocks now hide it until a source confirms the real closing date; inferred fallback also no longer overwrites an already confirmed `end_date`.
- **Telegraph (Infoblock)**: Event pages now show `✅ Пушкинская карта` in the quick-facts infoblock when `pushkin_card=true` (line omitted otherwise).
- **Smart Update (Exhibitions)**: Prevented catastrophic merges of unrelated exhibitions: long-running events are no longer auto-matched solely by venue + date-range overlap when titles are unrelated; Smart Update falls back to LLM match/create (or creates a new event when LLM is disabled).
- **Smart Update (Matching)**: Prevented VK/TG duplicates when `match_or_create` returns `create` due to a weak `candidate.title`: Smart Update now does a deterministic rescue-match via grounded `bundle.title` against the shortlist before creating a new event.
- **Smart Update (Title)**: Generic fallback titles like "`<event_type> — <venue>`" can now be replaced by a grounded proper event name from source text/OCR (prevents meaningless titles like "Концерт — Янтарь холл" when "ЕвроДэнс'90" is present).
- **Post Metrics / Popular Posts**: Fixed `/popular_posts` report rendering in Telegram HTML mode by escaping the debug counter label `skip(&lt;=median)` (prevented `can't parse entities: Unclosed start tag` failures when a section has no items).
- **Telegram Monitoring (UI report links)**: `Smart Update (детали событий)` now renders Telegram source-post links with preview-friendly `?single` in `href` (display text and canonical stored `source_url` remain unchanged).
- **Telegram Monitoring (media fallback)**: Public `t.me/s/...` fallback for posters/full text in single-event posts is no longer gated by `bot is not None`; importer now logs fallback results (`tg_monitor.poster_fallback ... posters=N`) and debug-errors for failed fallback attempts.
- **Telegram Monitoring (media fallback)**: When poster fallback can extract image URLs from public `t.me/s/...` HTML but media upload (`process_media`) fails, importer now keeps direct Telegram CDN image URLs as a hard fallback so events are not imported without illustrations.
- **Telegram Monitoring (linked sources)**: `linked_source_urls` now enrich event media during import: posters are pulled from linked Telegram posts (payload-first, then `t.me/s/...` fallback) and merged into the single-event candidate before Smart Update.
- **Telegram Monitoring (linked sources)**: Linked Telegram posts are now best-effort scanned for text and processed via an extra Smart Update pass so their facts appear in the source log (instead of “без извлечённых фактов”).
- **Smart Update (Fact-first)**: Legacy events with only an old `description` are now backfilled into source facts so fact-first description regeneration preserves pre-existing details.
- **Smart Update (Fact-first)**: Fact-first descriptions now allow Yandex Music playlist links (`music.yandex.ru/users/.../playlists/...`) and keep participant chat mentions while stripping chat URLs; plus scaled description/coverage budgets to reduce truncation and missing facts in Telegraph pages.
- **VK Intake (Locations)**: Fixed venue resolution when a known-but-wrong library is selected while the post contains an explicit address; address now disambiguates to a single known venue, and source logs no longer duplicate address/city when `location_name` is a full canonical line.
- **General Stats**: `/general_stats` now reports `gemma_requests_count` from Supabase `google_ai_requests` (with `google_ai_usage_counters` fallback, and `token_usage` fallback when Google AI tables are unavailable).
- **Scheduler**: Scheduled heavy jobs now skip (with admin-chat notification) when another heavy operation is already running, preventing parallel long imports/monitoring (`/tg`, `/vk_auto_import`, `/parse`, `/3di`). Config: `SCHED_HEAVY_GUARD_MODE`, `SCHED_HEAVY_TRY_TIMEOUT_SEC`.
- **JobOutbox / Telegram Monitor**: Job handlers now have a hard max runtime, preventing a stuck outbox task from freezing `/tg` imports; inline drain is time-bounded via `TG_MONITORING_INLINE_DRAIN_TIMEOUT_SEC`.
- **Smart Update (Posters)**: Poster ordering now prefers images whose OCR matches the event `title/date/time`, preventing unrelated posters from becoming the first Telegraph cover.
- **Telegraph Rendering**: Overlong paragraph-like headings (e.g. `### ...` on one line) are now demoted to normal paragraphs during HTML build, preventing “giant heading” event descriptions.
- **Telegraph Rendering / Smart Update**: Inline/bold `Facts:`/`Факты:` markers are stripped from public descriptions; short `###` headings are preserved more reliably.
- **Telegraph Rendering / Markdown**: Fixed HTML tag balancing so Markdown lists (`- ...`) render as proper `<ul>/<li>` blocks (instead of broken paragraphs with `&#8203;` spacers between items).
- **Smart Event Update**: Orphan `###` headings without a body (e.g. empty `### Подробности` followed by another same-level heading) are now dropped during description normalization, preventing empty sections on Telegraph pages.
- **VK Auto Queue / Smart Update**: Venue status-update posts (closure/eviction/lease deadlines, petitions/fundraising) are now skipped as `skipped_non_event:venue_status_update` to prevent false events.
- **Smart Event Update**: Work-schedule notices (режим/график работы учреждений, включая «праздничные дни» и «расширенный график») are now skipped more reliably as `skipped_non_event:work_schedule` for VK/TG auto-ingest, preventing pseudo-events from timetable updates.
- **Smart Event Update**: Service/package promos (e.g. “Выпускные 2026 … бронирование открыто”) are now skipped as `skipped_non_event:service_promo` when the post has no concrete date/time, preventing pseudo-events from venue advertising.
- **Locations**: Added `Дизайн-резиденция Gumbinnen, Ленина 29, Гусев` to `docs/reference/locations.md` to prevent venue normalization to the wrong city (e.g. “…, Калининград”).
- **Event Parse (Gemma/4o prompt)**: Added an explicit non-event rule for institution working-hours notices (`график/режим/часы работы`, `праздничные/выходные дни`, `санитарный день`, `расширенный график`) so such posts return no events unless they contain an explicit attendable event announcement.
- **VK Intake**: Drafts with a parsed date but no date/time signals in the source are rejected early (prevents “today” hallucinations on non-event VK posts).
- **Topics**: Topic classification cache key no longer relies on `short_description`, improving reuse for multi-day expansions.
- **Telegram Monitor / Smart Update**: For Telegram sources with `default_location`, conflicting extracted cities are now disambiguated via a short LLM check before region filtering (prevents false rejects on context mentions like “(г. Москва)” while still rejecting truly out-of-region posts).
- **Telegram Monitor (Kaggle)**: Poster uploads now default to Supabase (`TG_MONITORING_POSTERS_SUPABASE_MODE=always`) and skip Catbox when Supabase Storage is available, improving Telegraph/Telegram preview stability.
- **Telegram Monitor (Kaggle)**: Poster uploads to Supabase now convert to WebP (no JPEG objects in Storage) to reduce bucket usage and enable cross-env deduplication.
- **Telegram Monitor (Kaggle)**: Video uploads now default to Supabase (`TG_MONITORING_VIDEOS_SUPABASE_MODE=always`), enabling automatic attachment when videos are under size/bucket guards.
- **Telegram Monitor (Server)**: Message videos are now attached even when Smart Update ends with `skipped_nochange` (single-event posts), and `telegraph_build` is re-queued after new video attachment to ensure Telegraph pages reflect video embeds/links.
- **Telegraph Event Pages**: Poster render selection now prefers Supabase URLs by default when both Supabase and Catbox variants exist (override via `TELEGRAPH_PREFER_SUPABASE=0`).
- **Telegraph Event Pages**: Attached Supabase videos (`event_media_asset`) are now rendered on the event page as embedded video previews for direct files (`.mp4/.webm/.mov`), with a link fallback for other URLs.
- **Telegraph Event Pages**: Non-selected poster variants are excluded from tail image rendering, reducing “foreign poster” leaks in multi-poster albums.
- **Telegraph Event Pages**: Image URL reachability probes no longer strip the last remaining illustration on transient failures (prevents “no cover” pages).
- **VK Intake / Smart Update**: `vk_source.default_time` is now treated as a low-priority time fallback: it is saved as `event.time` with `event.time_is_default=1` and does not act as a strict matching anchor, so explicit time from other sources can override it (prevents phantom showtimes and duplicate theatre events).
- **Smart Event Update**: Matching now converges more reliably on existing events during reprocessing (including flows with `check_source_url=False`) by using safe source anchors (`event_source.source_url` / `(source_message_id,source_url)`) and deterministic exact-title anchor matching before LLM, reducing duplicate creation.
- **Fact-first Smart Update**: Tightened `facts_text_clean` filtering to keep anchors/logistics out of narratives (including free-form date/time and venue-marker phrases like `зал/этаж/аудитория`); generation loop is more resilient and includes a last‑mile cleanup for forbidden `посвящ…` phrasing.
- **Fact-first Smart Update**: Description generation now prefers 2–3 informative `###` headings and avoids “micro-sections” (single-sentence blocks), grouping same-type topics into lists when appropriate.
- **Fact-first Smart Update**: Description generation now runs a single coverage-check pass and at most 2 revise passes (bounded to 2–4 Gemma calls) and allows larger `description_budget_chars` for rich fact sets.
- **Fact-first Smart Update**: `facts_before` now includes `duplicate` facts to keep canonical fact sets stable across re-processing the same source URL.
- **VK Auto Queue**: Cancellation notice detection no longer false-positives on phrases like “перенесут вас…”, and cancellation source logging now reuses existing `(event_id, source_url)` rows to avoid unique-constraint failures.
- **Smart Event Update**: Dedup matching now uses a soft city filter (`candidate.city` also matches legacy rows with empty `event.city`) to prevent duplicate creation when the first import missed city and the next source resolves it; LLM match prompts were also tightened for placeholder time (`00:00`/`time_is_default`) and “same event, different title phrasing” cases.
- **Event Parse (Gemma/4o prompt)**: Draft extraction now avoids producing multiple events with identical anchors (same date/time/location) by merging program-item lists (speakers/bands/topics) into a single umbrella event.
- **Database**: `Database.init()` now backfills missing `event_source` rows from legacy `event.source_post_url` / `event.source_vk_post_url` to improve Smart Update idempotency (opt-out: `DB_INIT_SKIP_EVENT_SOURCE_BACKFILL=1`).
- **Source Parsing**: Theatre site upserts now treat empty `time` as a placeholder and can update it when a canonical schedule provides an exact start time.
- **Source Parsing**: `/parse` safeguard enqueue now keeps deferred semantics for navigation rebuilds: fallback `month_pages` and `weekend_pages` jobs are queued with `next_run_at=now+15m` (no immediate Telegraph month/weekend rebuild after batch import).
- **Admin Action Assistant**: Requests about “общая статистика / авторазбор VK+TG / Gemma” now prefer `/general_stats` (instead of `/stats`).
- **Telegram Monitor**: `database is locked` during long SQLite imports now triggers automatic import retries (with operator-visible progress notice) instead of immediate run failure; SQLite PRAGMAs are also applied on every ORM connection to reduce lock contention (`DB_TIMEOUT_SEC` default increased to `30s`).
- **Telegram Monitor**: Poster-bridge (forwarded poster in the next message) is now stricter: it is gated by a short caption + small time delta and attaches posters only when OCR matches the target event (`title/date/time`), preventing unrelated posters from leaking into video-only posts.
- **Telegram Monitor**: When `message.text` looks truncated (ends with `…`/`...`), import can fetch the full post body from public `t.me/s/...` HTML and use it as Smart Update `source_text` to avoid missing performer/support lines.
- **Telegram Monitor**: Single-event posts can now correct extracted date/time from a single clear poster OCR date/time pair when the extracted date is clearly too far in the past vs the message date.
- **Telegram Monitor**: More import-side SQLite commits now retry on `database is locked` (source meta/title updates, force-message cleanup, linked sources, video assets) to reduce flakiness on long runs.
- **Telegram Monitor**: auth bundle selection is now explicit and safe by default (`TELEGRAM_AUTH_BUNDLE_S22` for Kaggle, optional override via `TG_MONITORING_AUTH_BUNDLE_ENV`), and `/tg` now returns an explicit operator hint when Kaggle fails with `AuthKeyDuplicatedError`.
- **E2E**: `behave` now skips malformed `db_prod_snapshot.sqlite` when auto-picking a default `DB_PATH`, falling back to the newest healthy `db_prod_snapshot_*.sqlite` snapshot.
- **E2E**: Live runs now isolate any `*snapshot*.sqlite` DB into a per-run copy (not only `db_prod_snapshot*.sqlite`), keeping E2E repeatable even when using snapshots under `artifacts/`.
- **E2E**: `.env` loader now replaces obviously invalid/placeholder Supabase keys (very short `SUPABASE_KEY`/`SUPABASE_SERVICE_KEY`) with values from repo `.env`, preventing Storage RLS failures in media-related E2E checks.
- **Festivals (Telegraph)**: Festival pages and festivals index now self-heal `PAGE_ACCESS_DENIED` by creating new pages under the current `TELEGRAPH_TOKEN` (useful for DEV/E2E snapshots).
- **Festivals (Telegraph)**: Festival gallery enrichment no longer skips when `festival.photo_url` already exists; it can still populate `festival.photo_urls` from program/event pages to satisfy “cover + images”.
- **Festivals (Telegraph)**: Festival pages now fall back to showing all festival events when there are no upcoming ones (prevents empty festival programs for ongoing/past-only festivals in snapshots).
- **E2E (Telegram Sources)**: Destructive step “очистить список источников Telegram через UI” now requires explicit opt-in and a DB-isolated run; festival queue E2E no longer clears Telegram sources by default.
- **Festivals Queue (UI)**: `/fest_queue` now streams operator-visible progress (including TG queue items via Telegram Monitoring progress messages).
- **Festivals Queue**: Festival queue runner now auto-recovers stale `festival_queue.status=running` items back to `pending` after a timeout (`FESTIVAL_QUEUE_STALE_RUNNING_MINUTES`, default `60`).
- **Festivals Queue (TG)**: When forced Telegram monitoring fails (e.g. transient Kaggle kernel error), queue processing now falls back to `ensure_festival` from queue item metadata (`festival_name`/`source_text`) and still builds festival/index pages instead of failing with `festival page not created`.
- **Smart Update / Festival & Daily snippets**: `short_description` is now generated/refreshed via LLM as one complete 12–16 word sentence (no `...`/`…` tails); list rendering no longer degrades valid snippets into visually broken ellipsis fragments.
- **Media Uploads**: `upload_images()` now uses Supabase Storage when Catbox uploads are disabled (and falls back to Supabase when Catbox uploads fail), improving reliability of event posters/covers.
- **Media Uploads**: `upload_images()` now defaults to `UPLOAD_IMAGES_SUPABASE_MODE=prefer` (Supabase first, Catbox fallback); Telegraph upload fallback was removed (`telegra.ph/upload` deprecated/unavailable).
- **Media Uploads**: Supabase public URLs returned by `upload_images()` now strip the trailing `?` for stable poster dedupe/rendering.
- **Media Uploads**: Supabase poster uploads are now stored as WebP and keyed by a perceptual hash (dHash16) to deduplicate visually identical images across sources and between PROD/TEST (even when resolutions differ).
- **Smart Update Inputs**: `/addevent`, VK auto import and source parsers now map Supabase-hosted poster URLs into `PosterCandidate.supabase_url` (instead of leaking them into `catbox_url` only), improving downstream rendering/cleanup consistency.
- **Telegram Monitor (Kaggle)**: Poster uploads now target `SUPABASE_MEDIA_BUCKET` explicitly (instead of legacy `SUPABASE_BUCKET`) to keep media separated from ICS storage.
- **Cleanup**: Supabase Storage deletions are now persisted in `supabase_delete_queue` and retried on the next cleanup run, preventing orphaned objects when Supabase is temporarily unavailable.
- **Cleanup**: `cleanup_old_events` now avoids deleting Supabase media objects still referenced by other events (dedup-safe) and supports disabling media deletes in shared buckets via `SUPABASE_MEDIA_DELETE_ENABLED=0`.
- **Smart Update**: Manual bot ingests (`source_type=bot`) no longer get blocked by the deterministic region filter when `city` is missing, and keep operator-provided `title` (no LLM renaming).
- **Smart Update**: Matching is now more robust for time/location anchors: `00:00` is treated as a placeholder (unknown) during matching, placeholder times can be filled from matched TG/VK sources, and location matching tolerates punctuation/dash variants (e.g. `Янтарь-холл` vs `Янтарь холл, Ленина 11`), reducing duplicate events.
- **Daily Announcements**: Daily posts now render the short one-liner from `Event.search_digest` (with a safe fallback) instead of the full `Event.description`, preventing long LLM Markdown blocks from flooding `/daily` announcements.
- **Festivals (Telegraph)**: Festival pages now render event summaries from `Event.search_digest` (1 sentence, short digest) instead of full `Event.description`, keeping festival programs readable and consistent with `/daily`.
- **Festivals (Telegraph)**: Festival/event digest rendering now hard-limits visible snippets to `<=16` words (same guardrail for festival cards and `/daily`) to prevent long paragraphs in list-style UIs.
- **Festivals (Telegraph)**: Festival/month/weekend event cards now show date/time and location as explicit `📅`/`📍` lines (same template across pages).
- **Festivals (Telegraph)**: Service markers for the “near festivals” block no longer render as visible `<!-- ... -->` text; pages are updated idempotently using invisible anchor markers.
- **Festivals (Telegraph)**: Fallback rendering in `event_to_nodes` now preserves clickable service links (`подробнее`, `добавить в календарь`) instead of degrading them to plain text when strict `html_to_nodes` parsing fails.
- **Festivals (Telegraph)**: Festival page media now prefers preview-friendly images and excludes Catbox URLs from page cover/gallery; when needed, a safe fallback cover is used so Telegram cached preview remains available.
- **Festivals (Telegraph)**: When a festival has only legacy Catbox illustrations in DB snapshots, the page now keeps a preview-friendly cover fallback and still renders the legacy gallery (instead of becoming cover-only).
- **Festivals (Telegraph)**: Public festival pages no longer render `пост-источник` links; Telegram channel links that duplicate the source-post channel are hidden unless explicitly confirmed in `telegram_source` as a festival source for that series.
- **Festivals (Telegraph)**: Festival pages now render a source counter (`📚 Источников: N`) based on unique festival/event source links used for the assembled page.
- **Festivals Queue**: For `День <...>` sources, queue processing now re-grounds festival name against explicit source wording (quotes/`День рождения ...`) when parser-provided name is not present in source text, reducing false merges between unrelated festival series.
- **Festivals (Telegraph)**: `telegra.ph/file`/`graph.org/file` URLs are no longer treated as preview-friendly targets for festival/index covers (upload endpoint is deprecated); safe fallback now relies on Supabase/public HTTPS sources.
- **Festivals Index (Telegraph)**: When no preview-friendly cover is available, the festivals index auto-generates and hosts a safe cover (Supabase) so Telegram can cache/preview the page reliably.
- **Festivals Index (Telegraph)**: `sync_festivals_index_page` now builds from the same “current + near-upcoming” set as rebuild flow (window `FESTIVALS_UPCOMING_HORIZON_DAYS`, default `120`), so the page no longer fills with distant/archive-like festivals.
- **Festivals Index (Telegraph)**: Supabase public cover URLs are normalized (trailing `?` stripped) for more stable Telegram preview caching behavior.
- **Festivals Index (Telegraph)**: Unsafe stored index covers are now auto-replaced with preview-friendly covers (generated/uploaded fallback or explicit `FESTIVALS_INDEX_FALLBACK_COVER_URL`), and Catbox images are excluded from index-card image rendering to prevent broken Telegram cache/preview.
- **Festivals Index (Telegraph)**: Upcoming ordering now anchors to the nearest not-ended festival event (not stale historic `start_date`), and card images are best-effort rehosted to preview-friendly URLs for missing illustrations.
- **E2E (Festival Queue)**: Fixed Telegraph assertions for `og:image` and festival digest parsing (`<br>`/whitespace regexes), eliminating false failures in live checks.
- **E2E (Festival Queue)**: Festival index link assertion now accepts both absolute (`https://telegra.ph/...`) and relative (`/path`) Telegraph URLs.
- **E2E (Festival Queue Prefilled)**: TG scenario now explicitly re-arms canonical sources to `pending` and runs with `--limit=2` to stay deterministic when extra TG items exist in `festival_queue`.
- **Daily Announcements**: Daily post titles now link to the event Telegraph page (`telegraph_url`/`telegraph_path`) instead of Telegram/VK source links.
- **Telegraph**: Fixed occasional `telegraph_build` failures (`'b' tag closed instead of 'i'`) by balancing mis-nested inline tags produced by the lightweight Markdown renderer before `html_to_nodes`.
- **Telegraph (Telegram preview)**: Added optional Telegram preview warm-up after event page publish (`TELEGRAPH_PREVIEW_WARMUP=1`) to trigger `cached_page`/Instant View generation without bloating operator reports.
- **Telegraph (Telegram preview)**: Added Telegraph cache sanitizer (Kaggle/Telethon): new `/telegraph_cache_stats` and `/telegraph_cache_sanitize` commands + scheduler job to probe/warm `cached_page`/preview `photo`, store results in SQLite (`telegraph_preview_probe`), and enqueue rebuilds for persistently failing pages (skips past pages: ended events / past weekends / past months). Manual `/telegraph_cache_sanitize` now shows a Kaggle kernel status message that updates while polling (like `/tg`).
- **Telegraph (Telegram preview)**: Cache sanitizer event target selection now rotates by last probe time (and prioritizes persistent failures) instead of always probing the earliest upcoming events, so pages outside the first LIMIT window get covered over multiple runs.
- **Smart Update**: Added deterministic merge by `date + explicit time + location + related title` before LLM, reducing duplicates from variant titles like `Гегель` vs `Гегель: философия истории` during parser/VK/TG re-imports.
- **Telegraph (Telegram preview)**: Cache sanitizer now treats `webpage.cached_page` (Instant View) as the primary OK signal; missing `webpage.photo` is reported as a warning and no longer triggers regeneration by itself.
- **Telegraph (Telegram preview)**: Kaggle probe now waits for `cached_page` after preview attachment and best-effort refreshes the WebPage via `messages.getWebPage` before reporting `no_cached_page` (reduces false negatives on slow previews).
- **Telegraph (Telegram preview)**: When a Telegraph page cover is only available as WEBP, the builder best-effort creates a JPEG mirror in Supabase Storage (prefix `SUPABASE_TELEGRAPH_COVER_PREFIX`, default `tgcover`) so Telegram Instant View caching is more reliable for event/month/weekend pages.
- **Telegraph Event Pages**: Tiny avatar-like illustrations are now dropped when a real poster-sized image exists (prevents channel avatars/icons from showing up on event pages).
- **Telegram Monitor**: Multi-day posters (several explicit date/time pairs in OCR for one title) are now expanded into separate events when the extractor collapsed them into one date/time.
- **Telegram Monitor**: Poster-only forwarded follow-ups (text post + next-message poster) are now bridged to attach the poster to the previous imported event (best-effort, tight time/id window).
- **Telegram Monitor**: Missing event times are now additionally inferred from poster OCR when the post text lacks explicit time (reduces “date-only” events with missing ICS).
- **Telegram Monitor**: Message-date year rollover correction now applies only to “~1 year drift” cases to avoid rolling genuine near-past dates into the next year.
- **Telegram Monitor**: Multi-event poster matching now prefers returning no posters (instead of wrong posters) when OCR/title/date/time signals are inconclusive.
- **Telegram Monitor**: Public `t.me/s/...` poster fallback no longer captures channel avatars or neighboring-post images; it extracts only photo media from the target post.
- **Telegram Monitor**: For sources with `default_location`, extracted `city` is now derived from the default location (prevents false `rejected_out_of_region` when the post mentions other cities as context, e.g. “(г. Москва)” in speaker bios).
- **Telegram Monitor**: Per-post `/tg` details now wait briefly for `telegraph_build` to materialize the Telegraph link before rendering the operator report (reduces `Telegraph: ⏳ running` cases).
- **Telegram Monitor**: Per-post `/tg` details now also try a direct Telegraph build fallback when the outbox drain doesn’t produce a link quickly (reduces `Telegraph: ⏳ pending` cases).
- **Telegram Monitor (Kaggle)**: Video uploads now use content-addressed keys (`v/sha256/<first2>/<sha256>.<ext>`) with a Supabase Storage existence check to avoid re-uploading duplicates; a legacy fast-path can reuse an existing `v/tg/<document.id>.<ext>` object without re-downloading.
- **Telegram Monitor**: Import now enforces chronological post processing (`message_date` ascending, old→new) before Smart Update to prevent stale posts from overwriting fresher event data in the same run.
- **Telegram Monitor**: Per-post Smart Update reporting now (best-effort) drains JobOutbox tasks (`ics_publish` + `telegraph_build`) for the touched `event_id` before sending details, so Telegraph/ICS links are up-to-date immediately (and DEV snapshots can recreate Telegraph pages on `PAGE_ACCESS_DENIED`).
- **Telegram Monitor**: Import now ignores new Kaggle messages with `events=[]` (unless forced/previously scanned) to reduce noise and avoid polluting popularity baselines; baselines are computed only from posts known to contain events.
- **Locations**: Address display/normalization now strips `ул.`/`улица` and collapses comma-separated fragments (e.g. `ул. Тельмана, 28` → `Тельмана 28`) for a more compact canonical format.
- **Smart Update**: "Фото дня" rubric posts from VK/TG are now treated as non-event content unless strong event signals are present (time/period/invite/tickets/registration).
- **Telegram Monitor**: Popularity markers ⭐/👍 now compare against per-channel medians by default (works even with fresh metrics: one monitoring run is enough); markers also support multi-level outliers (e.g. `⭐⭐⭐`) and baseline lookups are no longer cached in a way that freezes the sample at zero during the run.
- **Smart Event Update**: Псевдо‑фестивали из `docs/reference/holidays.md` теперь применяются внутри Smart Update (не зависит от источника вызова: VK/TG/ручной ввод).
- **Festivals Queue**: Smart Update now recognizes TelegramMonitor outputs with boolean `festival: true` (or `event_type: festival`) and enqueues them into `festival_queue` using the extracted `title` as the festival name when missing.
- **Telegram Monitor (Kaggle)**: Auth now supports `TELEGRAM_AUTH_BUNDLE_S22` (bundle with device params); `TG_SESSION` remains a fallback.
- **Telegram Monitor (Kaggle)**: Ticket giveaways are no longer auto-skipped; text is kept intact and LLM is instructed to ignore giveaway mechanics while preserving event facts when present.
- **Telegram Monitor (Kaggle)**: Fixed hidden link extraction for text-url entities/buttons: links are now exported at message-level (`messages[].links`) and per-event ticket-link mapping no longer breaks due to offset drift; links are no longer accidentally stored inside `posters[].links`.
- **Telegram Monitor (Kaggle)**: Poster/media download no longer relies on date/time heuristics and also supports rich preview images (`webpage.photo`), reducing “Telegraph page without photos” cases; schedule fast-path extraction is conservative (requires 2+ matched lines) to avoid false titles like `", четверг"`. Default `TG_MONITORING_MEDIA_MAX_PER_SOURCE` increased to `12`.
- **Telegram Monitor (Kaggle)**: Open calls / «конкурсный отбор» / «приём заявок» no longer produce events, and event dates are no longer defaulted to the message publish date unless there is an explicit date anchor or an explicit `end_date` for exhibition/fair context (prevents “no date in post → event created for today”).
- **Telegraph**: Event pages force `preview_3d_url` to be the cover image when available.
- **Telegraph**: Event pages render merged `event.description` (not a single legacy `source_text`) so newly merged facts show up on the page.
- **Smart Event Update**: New-text detection prefers full source text (not only excerpts) and records a more “new-facts” snippet in source logs.
- **Smart Event Update**: Clears `preview_3d_url` when the illustration set changes so `/3di` scheduled runs can regenerate 3D previews.
- **Telegraph**: Event page rebuild commits before Telegraph API edit/create calls to avoid holding long SQLite write locks during network operations (reduces `database is locked` contention under concurrent imports/workers).
- **Smart Event Update**: Telegram short-source descriptions are now kept close to the total source volume (post text + poster OCR): create-bundle prompts include a strict `description_budget_chars`, and Smart Update runs an LLM-only `shrink_desc` pass when the model over-expands short sources; also strips neural clichés like «это создаёт ...».
- **Smart Event Update**: Prevented leaking internal headings like `Facts/Added Facts` / `Факты` into public Telegraph descriptions (also strips paragraph-leading `Facts:` and drops internal `Факты для лога источников` blocks); inline quotes in «...» are now promoted to blockquotes for better Telegraph formatting.
- **Smart Event Update**: Added deterministic skip for open-call/application posts (VK/TG) to avoid creating pseudo-events even if upstream extractors misclassify them.
- **Smart Event Update**: Disabled deterministic promo/giveaway/channel-promo/schedule stripping (LLM-only policy) and stopped filtering posters by OCR relevance (keep all posters; OCR is used for ordering/priority only) to avoid events ending up without images.
- **VK Auto Queue**: Auto-import now skips VK posts that are deleted/unavailable at import time (marks inbox rows as `rejected`) and does not create events from stale `vk_inbox.text` when `wall.getById` fails (marks as `failed`, opt-in fallback via `VK_AUTO_IMPORT_ALLOW_STALE_INBOX_TEXT_ON_FETCH_FAIL=1`).
- **VK Intake**: For standup/comedy sources, VK intake now injects an explicit hint into the LLM parse input to make the format visible in the title (e.g. `Стендап: …`) without deterministic post-parse renames.
- **VK Intake**: Fixed a common VK parse bug where a date token like `21.02` (DD.MM) was misread as a time `21:02` (HH:MM); such false-positive times are now stripped when the source contains no real time.
- **VK Intake**: Program/schedule posts (single umbrella event with a time-based program) no longer create multiple duplicate events for each time slot; the importer collapses them into one event with a `time` range.
- **Smart Update**: Sanitizer now demotes overlong Markdown headings back to normal paragraphs and strips inline `Facts:`/`Факты:` prefixes that leaked into public Telegraph pages.
- **Telegram Monitor / Smart Update**: Prevented duplicate events created from a single Telegram post by (1) de-duplicating extracted `events[]` within the message payload and (2) forcing Smart Update match by the source-post anchor when the same message is imported twice in one run (e.g. linked-post enrichment).
- **Telegram Monitor (Kaggle)**: Export now includes message-level `links` extracted from entities and URL-buttons (e.g. “More info”, “билеты”), and best-effort maps them into per-event `ticket_link` when the URL is hidden behind text.
- **Smart Event Update**: Municipal notices (utility outages / road closures) are now deterministically skipped as `skipped_non_event:utility_outage` / `skipped_non_event:road_closure` (not “events to attend”).
- **Telegraph**: Hardened event page rendering against malformed inline HTML (auto-balances common tags and falls back to plain-text body if `html_to_nodes` fails), preventing Telegraph job failures like `'p' tag closed instead of 'i'`.
- **Smart Event Update**: Added additional deterministic skips for auto-ingest sources: `skipped_non_event:book_review`, `skipped_non_event:too_soon`, `skipped_non_event:online_event`.
- **Telegram Monitor**: Import now keeps posters for multi-event posts when poster OCR is empty so extracted events don't end up without illustrations.
- **Telegram Monitor**: Import skips poster-only “events” without any date/time signals on the poster itself (avoids creating events from artwork titles).
- **Telegram Monitor**: Ticket contact patterns like “Билеты у @username” are now converted into `ticket_link=https://t.me/username` and rendered on Telegraph pages.
- **Telegram Monitor**: Ticket links without scheme (e.g. `clck.ru/...`) are now coerced to `https://...` so they render as clickable links.
- **Telegram Monitor**: Per-post progress UI now marks video-only posts (`Медиа: 🎬 видео (фото=0)`) to explain missing illustrations.
- **Telegram Monitor (Kaggle)**: Fallback to local rate limiting when Supabase RPC is missing (PGRST202), so extraction keeps working in dev/test.
- **Telegram Monitor**: Import now infers missing `location_name`/`ticket_link`/bad titles from the message text and (when available) Kaggle-exported message links (best-effort) and the report breaks down skipped/invalid/rejected/nochange counts to explain “extracted vs created” gaps; operator also receives a per-post list of skipped/partial imports with links and reasons.
- **JobOutbox**: Error statuses now record a non-empty `last_error` even for exceptions with empty `str(exc)` (e.g. timeouts); Telegram monitor reports also show proper JobStatus values (so `error` is not displayed as `jobstatus.error`).
- **Telegram Monitor**: Re-scanned posts (same `message_id`) are no longer reprocessed through Smart Update; they update `views/likes` snapshots only and show up as `Посты только для метрик` (with optional popularity markers ⭐/👍).
- **Telegram Monitor**: Interactive `/tg` imports no longer repeat the full “created/updated events” list in the final report (per-post Smart Update details remain; final report keeps popular/skipped blocks). Override with `TG_MONITORING_FINAL_EVENT_LIST=1`.
- **Telegram Monitor (Kaggle)**: Kaggle status polling now tolerates transient network/SSL errors (e.g. `UNEXPECTED_EOF_WHILE_READING`) and keeps the run alive until `COMPLETE/FAILED` or timeout; UI shows a temporary “network error” phase instead of aborting the run.
- **LLM Gateway**: `google_ai_reserve` now retries transient network/SSL failures (`timeout`/`disconnect`/`EOF`) before switching to local limiter fallback, reducing noisy `reserve_rpc_error_fallback` incidents during long imports.
- **Telegram Monitor**: Kaggle polling timeout now scales with the number of configured Telegram sources to avoid false `timeout` failures on large source lists.
- **DB Maintenance**: Added retention cleanup for `telegram_post_metric`/`vk_post_metric` (default 90 days) to bound DB growth.
- **Telegram Sources UI**: Stores and displays Telegram channel title (from Kaggle/Telethon results) next to `@username` to make source lists and reports more readable.
- **Telegram Monitor**: Import now accepts both `schema_version=1` and `schema_version=2`, prefers `sources_meta[].title`, updates `telegram_source.title` when it changes, and persists source metadata/suggestions (`about/links/hash/fetched_at` + `suggested_*`) without auto-overwriting manual `festival_series`.
- **Telegram Monitor**: Added cross-process global lock for monitoring/import (including DEV `Recreate + Reimport`) to prevent duplicate UI progress spam and reduce SQLite `database is locked` incidents when multiple bot instances run concurrently; progress upserts now tolerate no-op edits and de-duplicate duplicate `done` updates.
- **Smart Update**: Prevented erroneous poster pruning when `poster_scope_hashes` is provided but poster selection is empty (common when OCR matching fails); Telegram monitoring now keeps at least one poster for single-event posts more often, so events don’t lose illustrations unexpectedly.
- **Telegraph**: Infoblock-logistics stripping no longer removes sentence-ending punctuation, fixing “text without dots” on Telegraph event pages after Smart Update.
- **Smart Update / Telegraph**: Removed regex-based infoblock-logistics cutting; when duplicates are detected, cleanup is now done via an LLM editor pass (best-effort), otherwise the original text is preserved.
- **Telegram Monitor**: Single-event posts now keep all attached photos (dedupe by sha256); OCR is used for ordering/prioritization, not for dropping posters.
- **E2E Telegram Monitoring**: Added dedicated feature file and more robust step handling for async bot updates.
- **Docs (Locations)**: Restored canonical `docs/reference/locations.md` (fixes redirect-loop) and updated runtime lookup to use it.
- **Event Parse (Gemma)**: Added retry/wait handling on provider rate limits during draft extraction.
- **Telegraph**: Prevented month navigation footer anchoring from truncating event body text when the description contains Markdown/HTML `<hr>` dividers (now treated as internal body dividers).
- **Telegraph**: Fixed BODY_DIVIDER marker detection in footer anchoring (regression: internal dividers were treated as footer `<hr>`, truncating the main event text on some pages).
- **Telegram Monitor**: “🔥 Популярные посты” block now includes Telegraph links when the post is already associated with one or more events.
- **/log**: Source log now lists extra `event_source` rows that have no extracted facts yet (e.g. linked “more info” Telegram posts), so all attached sources are visible to the operator.
- **Telegraph**: Event pages now render `search_digest` even for short bodies and avoid extra blank spacer paragraphs around internal body dividers (`---`/`<hr>`), so dividers don’t create “пустые строки”.
- **Telegraph**: Event page rebuild now filters/replaces broken Catbox/Supabase image URLs before publishing; this prevents Telegram web preview from missing `cached_page` (Instant View) due to `<img>` 404s.
- **Smart Event Update**: Normalized Telegram bullet markers (`·`/`•` → Markdown list) and added a safety-net that re-attaches short source lists when the rewrite drops them, so Telegraph pages don’t lose factual пункты.
- **Smart Event Update**: Prevented create-time over-compression when sources are rich (including poster OCR): if the initial description is too short, Smart Update performs a second-pass full rewrite so Telegraph pages keep a meaningful main text (VK/TG imports).
- **Smart Event Update**: Create-time fallback no longer publishes full `source_text` verbatim when LLM rewrite/bundle is unavailable; it uses `raw_excerpt`/short digest instead and enforces a stricter non-verbatim guard.
- **Smart Event Update**: Legacy pre-Smart description snapshots no longer enter `facts_before` (prevents service phrases leaking into Telegraph text) but are still preserved as a legacy source baseline (`event.source_texts` / `event_source`).
- **Smart Event Update**: Removed “facts for source log” wording from description prompts and explicitly forbids “facts sections” inside `description`, preventing service-like “Факты …” blocks from leaking into public Telegraph pages.
- **Smart Event Update / Telegraph**: Unescaped backslash-escaped quotes (e.g. `\\\"...\\\"`) in event page body rendering to avoid broken-looking quotation marks on Telegraph pages.
- **Smart Event Update**: Create-time LLM bundle can now return an improved `title` using poster OCR headings (`poster_titles`), preventing generic or overlong titles and preserving key semantic markers (e.g. “Масленица”).
- **/log**: Source log no longer labels the source URL as “Telegraph” when the event Telegraph page has not been built yet (shows Telegraph only when available).
- **Telegraph**: Added a hard safety cap for overlong/broken `event.title` when creating/editing Telegraph pages to prevent stuck `TITLE_TOO_LONG` joboutbox failures.
- **Smart Event Update**: One-day “Акция” posts (“билет действует только на указанную дату/только сегодня/завтра”) no longer get a default 1‑month `end_date` even if misclassified as an exhibition.
- **VK intake**: Stopped defaulting missing draft date/time to “today/00:00” (prevents pseudo-events when the parser returns a non-event notice).
- **Event Parse**: Clarified prompt rules to ignore deadline-based notices (“до <date>”) and skip government-service/courses promo posts as non-events.
- **Cleanup**: Supabase Storage deletion now groups objects per bucket by parsing stored public URLs (supports split buckets and avoids leaking media objects when buckets diverge).
- **VK Auto Queue**: Rate-limited rows are now safely deferred back to `pending` (instead of hard-failing), with explicit `inbox_deferred` accounting.
- **VK Intake**: Improved title grounding (fallback for hallucinated/garbled tokens), recap/year-rollover handling, and rejection of stale past-only recap items.
- **Classification**: Normalized board-game meetup misclassification (`мастер-класс` → `встреча` where appropriate).
- **E2E Runtime**: Fail-fast on bot UI messages like `Результат: ошибка ...`; improved manual-tag handling and DB isolation defaults for live runs.
- **Source Log**: Avoids showing duplicate poster URL facts when `Афиша в источнике` equals `Добавлена афиша`.
- **VK**: VK source-post updates track their own hash (`vk_source_hash`) instead of reusing Telegraph `content_hash`, preventing redundant repost/edit churn.
- **Bot (local/dev)**: Running `python main.py` without `WEBHOOK_URL` now defaults to polling (prevents “bot is silent” runs); added `FORCE_POLLING=1` override.

## [1.11.1] - 2026-01-27
### Changed
- **CrumpleVideo**: Updated test mode configuration to use multiple scenes (Kaliningrad, Chernyakhovsk) and extended date range (2 days) for better verification.
- **Testing**: Improvements in `execute_crumple_test.py` and `crumple_video.ipynb` to support multi-city intro testing.

## [1.11.0] - 2026-01-25
### Added
- **Telegram Monitor**: Full release of the Intelligent Monitoring System.
  - **Standard Pipeline**: Events from Kaggle are now processed via the standard `/addevent` pipeline (GPT-4o + deduplication).
  - **Secure Sessions**: Implemented Fernet-based session splitting (Key/Cipher) for Kaggle isolation.
  - **Inline UI**: New `/tg` command with interactive buttons.
  - **Docs**: Comprehensive walkthrough and setup guide.

## [1.10.6] - 2026-01-25
### Changed
- **Video Announce**: Improved poster overlay text cleaning by stripping emojis from `ocr_text` and `description` to prevent font rendering issues.

## [1.10.5] - 2026-01-25
### Added
- **Video Announce**: Implemented `cross_month` layout for "Compact" intro pattern, allowing distinct date placement when events span across month boundaries.

## [1.10.4] - 2026-01-25
### Changed
- **Video Announce**: Refactored `_filter_events_by_poster_ocr` in selection logic to improve code organization and testability.

## [1.10.3] - 2026-01-24
### Fixed
- **3D Preview**: Fixed logic in automatic generation to reliably detect and process events with missing previews ("gaps"), scanning the last 14 days.
- **3D Preview**: Added an extra scheduled run at 17:15 to ensure previews are ready for the 18:00 pinned button update.

## [1.10.2] - 2026-01-24
### Fixed
- Filtered out past events during parsing to prevent them from being announced.

## [1.10.1] - 2026-01-24
### Fixed
- Fixed `TypeError` in parsing results summary when using date objects (Philharmonia parser).

## [1.10.0] - 2026-01-24
### Added
- **Source Parsing**: Added full support for **Kaliningrad Regional Philharmonia** (`filarmonia39.ru`).
  - Implemented Kaggle parser (`ParsePhilharmonia`) that scans proper 6-month window using direct URL navigation.
  - Integration in `/parse` command and scheduled jobs.
  - Supports automatic ticket status updates (`available` / `unavailable`) and price extraction.
  - Proper date normalization to avoid parsing errors.

### Fixed
- **Source Parsing**: Updated `requirements.txt` to include `beautifulsoup4` and `lxml` for local parsing utilities if needed.

## [1.9.13] - 2026-01-24
### Changed
- **CrumpleVideo**: Minor metadata updates in notebook.


## [1.9.12] - 2026-01-24
### Added
- **Video Announce**: Implemented "Poster Overlays" feature. Uses Google Gemma to check if posters are missing Title/Date/Time/Location. Adds an overlay badge to the video if critical info is missing.
- **Dependencies**: Added `google-generativeai`.

## [1.9.11] - 2026-01-24
### Fixed
- **Scheduler**: Fixed `_job_wrapper` to accept `**kwargs`, resolving `ValueError` when registering jobs with keyword arguments (like `3di_scheduler` with `chat_id`).

## [1.9.10] - 2026-01-24
### Fixed
- **Scheduler**: Added `_register_job` wrapper to prevent scheduler startup crashes if a single job fails to register.
- **Scheduler**: Added explicit "SCHED skipping" logs when optional jobs (source parsing, 3di) are disabled via env.

## [1.9.9] - 2026-01-24
### Fixed
- **CrumpleVideo**: Improved FFmpeg robustness with file existence checks, audio merge validation, and mpeg4 fallback.
- **Video Announce**: Enhanced polling reliability with retry logic (3 attempts) and recursive file search in output directory.

## [1.9.8] - 2026-01-23
### Changed
- **CrumpleVideo**: Updated test mode to use 5 scenes (was 1), samples=15, render_pct=84.
- **Video Announce**: Increased Kaggle timeout from 40 to 150 minutes to handle queue delays.
- **Kaggle Assets**: Fixed dataset slug format to `video-afisha-session-{id}` for compatibility.

## [1.9.7] - 2026-01-21
### Changed
- **CrumpleVideo**: Adjusted audio start timestamp to 1:17 (was 1:10) for better intro sync.
- **CrumpleVideo**: Increased `is_test_mode` render quality: samples raised to 18, percentage to 70% for clearer previews.


## [1.9.6] - 2026-01-21
### Fixed
- **Video Announce**: Improved random_order fallback and added notebook logging.
- **Tests**: Fixed import error in `test_video_announce_selection.py`.


## [1.9.5] - 2026-01-20
### Added
- **Preview 3D**: Автоматическая генерация 3D-превью (`/3di`) по расписанию (`ENABLE_3DI_SCHEDULED=1`, `THREEDI_TIMES_LOCAL`).
- **Source Parsing**: Поддержка дневного автозапуска (`ENABLE_SOURCE_PARSING_DAY`, `SOURCE_PARSING_DAY_TIME_LOCAL`).
- **Source Parsing**: Защита от холостого парсинга — если сигнатуры страниц театров не изменились, повторный парсинг пропускается.

### Changed
- **Config**: Часовые пояса для шедулеров теперь настраиваются явно (`SOURCE_PARSING_TZ`, `SOURCE_PARSING_DAY_TZ`, `THREEDI_TZ`).
### Added
- **Source Parsing**: Автозапуск парсинга по расписанию (по умолчанию 02:15 KGD). Настройка через `ENABLE_SOURCE_PARSING=1` и `SOURCE_PARSING_TIME_LOCAL`.
- **Source Parsing**: Таймауты для OCR (60 сек) и скачивания изображений.
- **Source Parsing**: Детальная диагностика событий (через `SOURCE_PARSING_DIAG_TITLE`).
- **Source Parsing**: Логи теперь сохраняются в Persistent Volume `/data/parse_debug`.

### Fixed
- **Source Parsing**: Улучшена обработка ошибок в боте и на сервере, предотвращены "молчаливые" падения.
- **Source Parsing**: OCR отключен по умолчанию для источника `tretyakov` (`SOURCE_PARSING_DISABLE_OCR_SOURCES`) для стабильности.
- **CrumpleVideo**: Test "Tomorrow" renders now use lower samples and resolution to speed up single-scene previews.
- **CrumpleVideo**: Test-mode intro previews now default to `STICKER_YELLOW` when no explicit pattern is provided.
- **Intro Visuals**: Restored the dark default palette and added a yellow theme via `_YELLOW` patterns.

## [1.9.3] - 2026-01-20
### Fixed
- **Source Parsing**: Исправлена нормализация локаций Третьяковки — теперь сохраняется информация о сцене (`Кинозал`/`Атриум`), что позволяет различать события в одном месте в одно время. Ранее события в разных залах ошибочно определялись как дубликаты.
- **Source Parsing**: Добавлен label `🎨 Третьяковка` в отчёты `/parse`.
- **Kaggle Assets**: Preserve existing Kaggle kernel dataset sources while appending new ones, and restore `generate_intro_image` in the CrumpleVideo notebook.
- **CrumpleVideo**: Move `_resolve_image_path` to module scope so the main pipeline can call it safely.
- **CrumpleVideo**: Define `is_last` before building the intro segment to avoid `UnboundLocalError` in production.

## [1.9.2] - 2026-01-20
### Fixed
- **Kaggle Assets**: Fixed `ModuleNotFoundError` by moving assets and `pattern_preview.py` to a dedicated Kaggle Dataset (`video-announce-assets`) and mounting it in the kernel.
### Fixed
- CrumpleVideo Kaggle kernel now loads `pattern_preview` via the `video-announce-assets` dataset instead of local files.

## [1.9.1] - 2026-01-20
### Fixed
- **Kaggle Kernel ID**: Fixed a bug where `kaggle_client.py` was forcing the legacy `video-afisha` kernel ID, preventing `CrumpleVideo` updates.
- **Intro Visuals**: Integrated verified `pattern_preview` logic into the `CrumpleVideo` kernel to ensure correct fonts and alignment in production.
- **Outro Animation**: Disabled physics simulation (crumpling) for the Outro scene, ensuring it remains static/readable.

## [1.9.0] - 2026-01-20

### Added
- **Video Announce**: Automated "Tomorrow" pipeline (`/v` -> `🚀 Запуск Завтра`).
- **Video Announce**: Test mode (`/v` -> `🧪 Тест Завтра`) for single-scene verification.
- **Video Announce**: Randomize event order selection (prioritizing OCR candidates).
- **Video Announce**: Visual improvements for City/Date intro layout.

## [1.8.2] - 2026-01-07

### Fixed
- **Channel Navigation Buttons**: Buttons ("Today", "Tomorrow" etc.) are now ONLY added if the post contains `#анонс`, `#анонсКалининград` or `#анонскалининград` hashtags. Fixes EVE-13 where buttons appeared in all channel posts.

## [1.8.1] - 2026-01-05

### Fixed
- **Channel Navigation Buttons**: Исправлено получение постов канала — добавлен `channel_post` в `allowed_updates` webhook.
- **Channel Navigation Buttons**: Исправлен доступ к `db` и `bot` в хэндлере — теперь берутся из модуля `main`.
- **Channel Navigation Buttons**: Исправлен фильтр команд — проверка `/` вынесена внутрь хэндлера.

## [1.8.0] - 2026-01-05

### Added
- **Channel Navigation Buttons**: Добавлены inline-кнопки навигации для постов в канале:
  - «📅 Сегодня» — ссылка на текущий месяц
  - «📅 Завтра» — ссылка на специальную страницу завтрашнего дня (33% шанс)
  - «📅 Выходные» — ссылка на ближайшие выходные (33% шанс)
  - «📅 [Месяц]» — ссылка на следующий месяц (33% шанс)
  - Алгоритм `random.choice` для равномерного распределения
  - Модель `TomorrowPage` для кэширования страниц «завтра»
  - Фильтрация рубрик и автоматических постов (не добавляем кнопки)
- **3D Preview on Split Pages**: На страницах месяца с большим количеством событий (>30) теперь отображаются 3D-превью, если они есть (обычные фото скрываются).

### Changed
- Навигация в подвале спец-страниц (`/special`, Tomorrow, Weekend): текущий месяц теперь кликабельный.

## [1.7.8] - 2026-01-04

### Added
- **🏛 Dom Iskusstv Parsing**: Полноценная интеграция парсера Дома искусств с Kaggle:
  - Кнопка "🏛 Дом искусств" в главном меню для ввода ссылки на спецпроект
  - Кнопка "🏛 Извлечь из Дом искусств" в VK review для автоматического парсинга
  - Kaggle notebook `ParseDomIskusstv` для скрейпинга событий с сайта
  - Автоматическое создание Telegraph страниц с билетами, фото и полным описанием
  - BDD E2E тесты для всех сценариев парсинга
- **E2E Testing**: Добавлен фреймворк для E2E BDD тестов (`tests/e2e/`):
  - `HumanUserClient` — обёртка Telethon с имитацией человеческого поведения
  - BDD сценарии на Gherkin с русским синтаксисом
  - Верификация контента Telegraph страниц (проверка наличия 🎟, Билеты, руб.)

### Fixed
- **Telegraph PAGE_ACCESS_DENIED Fallback**: При ошибке редактирования Telegraph страницы (PAGE_ACCESS_DENIED) теперь автоматически создаётся новая страница вместо сбоя.
- **Telegraph Rebuild on Event Update**: Вызов `update_event_ticket_status` теперь триггерит перестройку Telegraph страницы, гарантируя актуальность данных о билетах.
- **Dom Iskusstv Updated Events Links**: Ссылки на Telegraph отображаются для обновлённых событий (добавлено отслеживание `updated_event_ids`).
- **Events List Message Length**: Fallback на компактный формат при превышении лимита Telegram (4096 символов).
- **Фестивали**: 3D превью над заголовком события на странице фестиваля.

### Changed
- **Фестивали**: `/festivals_fix_nav` пропускает архивные фестивали без будущих событий.

## [1.7.7] - 2026-01-02

### Added
- **3D Preview**: Добавлена кнопка "🌐 All missing" в меню `/3di` для генерации превью всех будущих событий без preview_3d_url одним нажатием.
- **Фестивали**: Добавлена кнопка "🔄 Обновить события" в меню редактирования фестиваля (`/fest edit`) для обновления списка событий на Telegraph-странице фестиваля.

## [1.7.5] - 2026-01-02

### Changed
- Increased event limit for 3D previews on month pages from 10 to 30.

## [1.7.6] - 2026-01-02

### Fixed
- **3D Preview**:
  - **Notebook Cleanup**: Kaggle notebook now performs aggressive cleanup (`rm -rf`) of Blender binary and image directories before completion. This prevents the bot from downloading massive amount of data (hundreds of MBs) and ensures only the result JSON is retrieved, fixing "Result not applied" errors.

## [1.7.4] - 2026-01-02

### Added
- **Telegraph**: Для событий с длинным описанием (>500 символов) теперь отображается краткое описание (`search_digest`) над полным текстом, разделённое горизонтальной линией. Улучшает читаемость страниц событий.

### Fixed
- **Tretyakov Parser**: 
  - Исправлена навигация по календарю — теперь парсер корректно находит все даты через стрелку `.week-calendar-next`.
  - Исправлено извлечение времени — парсер теперь прокручивает календарь к нужной дате перед кликом, устраняя ошибки `00:00` для дат на других страницах календаря.
  - Добавлена полная поддержка min/max цен из всех секторов.
  - Добавлена дедупликация событий с объединением фото (исполнитель приоритет над фестивалем).

## [1.7.3] - 2026-01-02

### Added
- **3D Preview**: Added "Only New" button to `/3di` command. Allows generating missing previews for new events without reprocessing existing ones.
- **Pyramida**: Fixed price parsing from ticket widget. Now extracts specific prices (e.g. "500 ₽") and price ranges ("500 - 1000 ₽"), ensuring correct `ticket_status` ("available" instead of "unknown").

## [1.7.2] - 2026-01-02

### Changed
- **3D Preview Aesthetics**:
    - **Soft Shadows**: Increased light source angle to 10° for softer, more realistic shadows.
    - **Cinematic Rotation**: The first card in the stack is now slightly rotated (-3°) for a more dynamic look.

## [1.7.1] - 2026-01-02

### Fixed
- **3D Preview**: Fixed argument parsing in `/3di` command to support running from image captions and avoid errors when `message.text` is None (aiogram v3 compatibility).

## [1.7.0] - 2026-01-02

### Added
- **3D Preview**: Added `/3di multy` command mode. Generates previews only for events with 2 or more images, filtering out single-image events.
- **3D Preview**: Improved lighting with a new "Shadow Lift" fill light. This makes cards 2, 3, and 4 readable by softening the hard shadows while maintaining the dramatic texture.

## [1.6.11] - 2026-01-02

### Changed
- **Configuration**: Increased Kaggle polling timeout from 30 minutes to 4 hours to accommodate CPU fallback scenarios.

## [1.6.10] - 2026-01-01

### Fixed
- **Source Parsing**: Исправлено формирование `short_description` для событий из `/parse`. Усилен промпт LLM — добавлены подробные правила генерации `short_description` (REQUIRED поле, one-sentence summary с примерами). Убран fallback на `full_description` (многострочный текст), fallback на title используется только в крайнем случае с логированием warning.
- **Special Pages**: Added support for 3D generated previews (`preview_3d_url`) in special pages. If available, the 3D preview is used as the main event image, prioritizing it over regular photos.

## [1.6.9] - 2026-01-01

### Changed
- **3D Preview**: Changed the Blender background color from dark gray to pure Black (#000000) for better integration with both light and dark Telegraph themes.

## [1.6.8] - 2026-01-01

### Refinements
- **3D Preview**:
  - **Cover Logic**: 3D preview is now used as the Telegraph page cover ONLY if the event has 2 or more source photos. If there is only 1 photo, the original is preserved.
  - **Transparency**: Added a dark background to the Blender scene to fix transparency rendering issues in Telegraph.
  - **Composition**: Improved layout for single images (< 3 photos) to use a centered single plane instead of the carousel.

### Refined
- **3D Preview**: Use the preview image as the leading Telegraph photo, add a dark scene background, and simplify layout when fewer than three images are available.

## [1.6.7] - 2026-01-01

### Fixed
- **3D Preview**: Fixed critical bug where database session variable shadowed the user session dictionary, causing "AsyncSession object does not support item assignment" error.

## [1.6.6] - 2026-01-01

### Performance
- **3D Preview**:
  - Notebook now cleans up Blender binaries and input files before completion, leaving only `output.json`. This dramatically speeds up the result download (from minutes to seconds) and prevents timeouts.
  - Handler now actively cleans up temporary download directories in `/tmp` to save disk space.

## [1.6.5] - 2026-01-01

### Fixed
- **3D Preview**:
  - Increased output download retry limit to 10 attempts (50s total timeout).
  - Implemented automatic Month Page rebuild triggering after 3D preview application.
  - Added detailed final report in Telegram with links to the updated month page and events.

## [1.6.4] - 2026-01-01

### Fixed
- **3D Preview**: Added 3 retry attempts for downloading output.json from Kaggle (handles API race conditions).

## [1.6.3] - 2026-01-01

### Fixed
- **3D Preview**: Added 15s delay after dataset creation in handler (syncing pattern with video_announce) to ensure dataset availability before kernel start.

## [1.6.2] - 2026-01-01

### Fixed
- **3D Preview**: Added 60s retry loop for payload detection in Kaggle notebook to handle dataset mounting latency.

## [1.6.1] - 2026-01-01

### Fixed
- **3D Preview**:
  - Fixed payload path detection in Kaggle notebook (now uses `rglob`).
  - Added "fail fast" logic in notebook if payload is missing.
  - Implemented live status updates in Telegram message during polling.
  - Added `asyncio.Lock` to serialize concurrent generation requests.
  - Fixed output directory collisions by using per-session paths.

## [1.6.0] – 2026-01-01

### Added
- **3D Preview Feature**:
  - Added `preview_3d_url` to `Event` model.
  - Created `/3di` command for generating 3D previews using Kaggle.
  - Implemented Kaggle orchestration pipeline (dataset -> kernel -> polling -> db update).
  - Added support for GPU rendering on Kaggle.
  - Integrated 3D previews into Telegraph month pages (displayed as main image).

## [1.5.3] – 2026-01-01
- **Performance**: Оптимизация LLM-вызовов в `/parse` — унифицирована логика `find_existing_event` с `upsert_event`. Теперь существующие события распознаются до вызова LLM, что значительно снижает расход токенов и время обработки.

## [1.5.2] – 2025-12-31
- **Logging**: логируются выбор kernel, путь локального kernel и состав файлов при push в Kaggle.

## [1.5.1] – 2025-12-31
- **Fix**: В импортировании payload и перезапуске последней сессии добавлен шаг выбора kernel перед рендером, чтобы избежать 403.
## [1.5.0] – 2025-12-31
- **Fix**: Исправлена маска MoviePy в Kaggle-ноутбуке `video_afisha.ipynb` — маска остается 2D для корректного blit.
- **Feature**: Добавлена кнопка "📥 Импортировать payload" для запуска рендера видео-анонса из сохранённого `payload.json` без этапа подбора событий.

## [1.4.6] – 2025-12-31
- **Fix**: Исправлена ошибка фильтрации в `/special`: события больше не скрываются, если у него ошибочно указан `end_date` в прошлом (проверяется `max(date, end_date)`).
- **Refinement**: Очистка описаний Музтеатра и Кафедрального собора на продакшене.
- **Fix**: Исправлена дата и метаданные события в Веселовке.
- **Infrastructure**: Введено правило изоляции временных скриптов в папке `scripts/`.

## [1.4.5] – 2025-12-31

### Fixed
- **Muzteatr Parser**: Fixed empty descriptions by extracting text from `og:description` meta tags (site structure changed).

## [1.4.4] - 2025-12-31

### Fixed
- **Dramteatr Parser**: Fixed DOM traversal issue where date block was missed because it is a sibling of the link wrapper.

## [1.4.3] - 2025-12-31

### Fixed
- **Dramteatr Parser**: Fixed date extraction (incomplete dates like "31 ДЕКАБР") using CSS selectors.
- **Parsing**: Improved duplicate detection with fuzzy title matching (Codex).
- **Video Announce**: Filter out "sold_out" events from video digests by default.
- **UI**: Minor adjustment to ticket icon order in summaries.

## [1.4.2] - 2025-12-31

### Changed
- **Source Parsing**: Улучшен алгоритм сопоставления событий (parser.py) — добавлено извлечение стартового времени для более точного поиска дубликатов.
- **Source Parsing**: Добавлено детальное логирование (per-event logging) с метриками (LLM usage, duration).

## [1.4.1] - 2025-12-31

### Fixed
- **Source Parsing**: Раскомментированы блоки Драмтеатра и Музтеатра в ноутбуке `ParseTheatres`.

## [1.4.0] - 2025-12-31

### Added
- **Special Pages**: Новая команда `/special` для генерации праздничных Telegraph-страниц. Поддержка произвольного периода (1–14 дней), дедупликация событий с одинаковыми названиями (объединение в блок с несколькими временами), загрузка обложки, автоматическое сокращение периода при превышении лимита Telegraph.
- **Special Pages**: Нормализация названий локаций при генерации страницы (удаление дублей адресов).
- **Special Pages**: Улучшили навигацию — добавлена навигация по месяцам в футере страницы.
- **Source Parsing**: Улучшен Kaggle-ноутбук `ParsePyramida` для более надежного парсинга.

### Fixed
- **System**: Исправлен конфликт `sys.modules` при запуске бота, вызывавший ошибку доступа к базе данных (`get_db() -> None`) в динамически загружаемых модулях.
- **Month/Weekend Pages**: Исправлено отсутствие дат и времени на страницах месяцев и выходных в Telegraph. Теперь дата и время отображаются корректно в формате "_31 декабря 19:00, Место, Город_".

### Fixed

## [1.3.7] - 2025-12-31

### Added
- **Telegraph**: Телефонные номера на страницах событий теперь кликабельные (ссылки `tel:`). Поддерживаются форматы: +7, 8, локальные номера.
- **Performance**: Отложенные перестройки страниц (Deferred Rebuilds) — задачи `month_pages` и `weekend_pages` откладываются на 15 минут для оптимизации при массовом добавлении событий.
- **Conditional Images**: На месячных и выходных страницах Telegraph отображаются изображения событий, если на странице менее 10 событий.
- **EVENT_UPDATE_SYNC**: Добавлена поддержка синхронного режима для тестирования отложенных задач.

### Changed
- **/parse limit**: Лимит одновременно добавляемых событий снижен с 10 до 5 для стабильности.
- **/parse rebuild**: Убрана автоматическая пересборка Telegraph страниц после `/parse` — теперь используется стандартная очередь отложенных задач.

### Fixed
- **/parse month_pages**: При добавлении событий через `/parse` теперь гарантированно создаются задачи `month_pages` для всех затронутых месяцев for deferred rebuild.
- **Deferred Rebuilds**: Исправлен обход отложенности — `_drain_nav_tasks` больше не создаёт немедленные follow-up задачи если уже есть отложенная задача для event_id. Это предотвращает преждевременную пересборку страниц Telegraph.
- **VK Inbox**: Исправлено отсутствие ссылки на Telegraph страницу в отчёте оператору ("✅ Telegraph — "). Теперь бот ожидает создания страницы перед отправкой ответа (до 10 секунд).
- **Deferred Rebuilds**: Убран синхронный вызов `refresh_month_nav` при обнаружении нового месяца, вызывавший немедленную пересборку всех страниц. Теперь новые месяцы обрабатываются через отложенную очередь.
- **Deferred Rebuilds**: `schedule_event_update_tasks` по умолчанию теперь использует `drain_nav=False`, гарантируя соблюдение 15-минутной задержки перед сборкой.
- **Deferred Rebuilds TTL**: Исправлено преждевременное истечение (expiration) отложенных задач — TTL теперь считается от момента запланированного выполнения (`next_run_at`), а не от момента создания (`updated_at`). Ранее задачи с 15-минутной отложенностью истекали через 10 минут (TTL=600с).
- **Rebuild Notifications**: При автоматической пересборке страниц теперь суперадминам приходит уведомление с перечнем обновлённых месяцев.
- **Navigation Update**: При добавлении события на новый месяц (например, Апрель) теперь обновляются футеры навигации на всех существующих страницах (Январь, Февраль и т.д.).
- **Year Suffix**: Исправлено отображение года в навигации — "2026" добавляется только к Январю или при смене года, а не ко всем месяцам.
- **Spam Removal**: Удалены отладочные сообщения `NAV_WATCHDOG`, которые отправлялись в чат оператора при каждой отложенной задаче.
- **Retry Logic**: При ошибке `CONTENT_TOO_BIG` флаг `show_images` теперь корректно прокидывается в ретрай.
- **Test Stability**: `main_part2.py` теперь безопаснее импортировать напрямую (fallback для `LOCAL_TZ`, `format_day_pretty`).
- **Photo URL Validation**: Добавлена проверка схемы `http` для `photo_urls`.

## [1.3.5] - 2025-12-29

### Fixed
- **Pyramida**: Исправлен парсинг дат в формате `DD.MM.YYYY HH:MM` (например `21.03.2026 18:00`). Ранее такие даты не распознавались и события не добавлялись.

## [1.3.4] - 2025-12-29

### Fixed
- **Pyramida**: Исправлена ошибка ("missing FSInputFile"), из-за которой не отправлялся JSON файл с результатом парсинга.
- **Pyramida**: Включено OCR для событий, добавляемых через кнопку в VK Review (ранее работало только для `/parse`).

## [1.3.3] - 2025-12-29

### Fixed
- **Pyramida**: Добавлено отображение статуса работы Kaggle (Running/Poling) в чате. Теперь пользователь видит прогресс выполнения ноутбука.

## [1.3.2] - 2025-12-29

### Added
- **Source Parsing**: Добавлена поддержка OCR (распознавание текста) для событий из Pyramida и /parse. Теперь афиши скачиваются, распознаются и текст используется для улучшения описания события.

## [1.3.1] - 2025-12-29

### Fixed
- **Pyramida**: Исправлен парсинг описания событий (корректный селектор для Playwright/BS4)
- **Pyramida**: Добавлена отправка JSON файла с результатами парсинга в чат
- **Docs**: Уточнено, что OCR для Pyramida не выполняется

## [1.3.0] - 2025-12-29

### Added
- **Pyramida extraction**: Новая кнопка "🔮 Извлечь из Pyramida" в VK review flow для автоматического парсинга событий с pyramida.info. Извлекает ссылки из поста, запускает Kaggle notebook, добавляет события в базу. См. [docs/PYRAMIDA.md](docs/PYRAMIDA.md)
- **Pyramida manual input**: Кнопка "🔮 Pyramida" в меню /start (для супер-админов) для ручного ввода

## [1.2.17] - 2025-12-29



### Added
- **source_parsing**: Новый Kaggle-ноутбук `ParseTheatres` с полем `description`
- **docs**: Документация `/parse` в `docs/pipelines/source-parsing.md`

### Fixed
- **source_parsing**: События из `/parse` теперь корректно появляются в ежедневном анонсе — исправлен подсчёт новых vs обновлённых событий
- **source_parsing**: Отчёт теперь показывает 🔄 Обновлено для существующих событий (ранее не отображалось)
- **source_parsing**: Добавлено debug-логирование в `find_existing_event` для диагностики
- **source_parsing**: Прогресс теперь редактирует одно сообщение вместо множества
- **source_parsing**: Поле `description` корректно передаётся в БД из парсера

## [1.2.15] - 2025-12-28

### Fixed
- **source_parsing**: Исправлено добавление событий — теперь используется `persist_event_and_pages` вместо несуществующего `persist_event_draft`
- **source_parsing**: Добавлена отправка JSON файлов из Kaggle в ответ на `/parse`
- **source_parsing**: Улучшено логирование создания событий

## [1.2.14] - 2025-12-28

### Added
- Улучшено логирование модуля `source_parsing` для отладки команды `/parse`:
  - Логирование при получении команды и проверке прав
  - Логирование старта и завершения Kaggle-ноутбука
  - Логирование количества полученных событий

## [1.2.13] - 2025-12-28

### Fixed
- Улучшен промпт `about_fill_prompt` для видеоанонсов: теперь LLM явно включает title в about когда ocr_title пуст.
- Синхронизированы правила about в `selection_prompt` и `about_fill_prompt`.

## [1.2.1] - 2025-12-27

### Fixed
- Исправлено дублирование заголовков выходных дней ("суббота/воскресенье") на месячных Telegraph-страницах при инкрем ентальном обновлении

## [1.2.0] - 2025-12-27
### Fixed
- Fixed critical `TypeError` in video announce generation caused by mismatched arguments in `about` text normalization calls across `scenario.py`, `selection.py`, and `finalize.py`.

## [1.1.1] - 2025-12-27
### Fixed
- Fixed bug where `search_digest` was not saved to database during event creation via text import.
- Updated `about_fill_prompt` to preserve proper nouns (e.g. "ОДИН ДОМА") in about text.
- Removed anchor prepending logic in `about.py`, making LLM fully responsible for about text generation.
- Updated agent instructions to require explicit user command for production deployment.

<!-- Новые изменения добавляй сюда -->
- Исправлен сбор логов с Kaggle: теперь `poller.py` корректно скачивает логи из вложенных директорий и пакует их в zip-архив, если файлов больше 10.

---

## [1.1.0] – 2025-12-27

### Added

- **Развернуть всех кандидатов**: в UI выбора событий появилась кнопка «+ Все кандидаты», разворачивающая полный список событий в 5-колоночный формат для ручного добавления.
- **Экран сортировки**: кнопка «🔀 Сортировка» открывает экран с выбранными событиями и кнопками ⬆️/⬇️ для изменения порядка показа в видео.
- **Текущие выходные**: если сегодня суббота или воскресенье, в периодах появляются две кнопки — «Эти выходные (дата)» и «Выходные (следующая дата)».

---

## [1.0.0] – 2025-12-27

> **Первый мажорный релиз** — введено семантическое версионирование (SemVer).

### Added

- Исправили падение при запуске: добавили импорт `dataclass` для работы альбомов в видео-анонсах и других обработчиках.
- Видео-анонсы перестали искажать LLM-описания: строки `about` теперь лишь очищаются от лишних пробелов/эмодзи, а усечение и дедупликация слов остаются только для резервного текста.
- Перевели справочник сезонных праздников на локализованный формат дат `DD.MM` и текстовые диапазоны, сохранили столбец `tolerance_days` и обновили парсер импорта под новый формат.
- `/vk_misses` superadmins review fresh Supabase samples: the bot pulls post text, up to ten images, filter reasons, and matched keywords from `vk_misses_sample`, adds «Отклонено верно»/«На доработку» buttons, and records revision notes for the latter in `VK_MISS_REVIEW_FILE` (defaults to `/data/vk_miss_review.md`).
- Добавили `/ik_poster`, вынесли логику в новый модуль `imagekit_poster.py`, подключили зависимости ImageKit и пересылаем результаты в операторский чат.
- Фестивальные редакторы загружают кастомные обложки через кнопку «Добавить иллюстрацию»: фича опирается на Telegram-поток `festimgadd` в `main.py`, бот пересылает туда файлы, автоматически разворачивает обложку в альбомную ориентацию и сохраняет обновления.
- Исправили кросс-базовую совместимость `festival.activities_json`: SQLite снова работает и не падает при чтении поля, закрывая регрессию с крэшем.
- `GROUNDED_ANSWER_MODEL_ID` теперь фиксирован на `gemini-2.5-flash-lite`, чтобы grounded-ответы consistently шли через новую модель.
- Добавили пайплайн видео-анонсов: новые сущности `videoannounce_session` / `videoannounce_item` / `videoannounce_eventhit` хранят статусы, ошибки и историю включений, а watchdog переводит застрявшие рендеры в `FAILED`.
- В `/events` появилась кнопка `🎬` с циклическим счётчиком 0→5: доступна неблокированным модераторам/суперадминам (партнёры правят только свои события) и резервирует включения в ролик; после публикации основного ролика счётчик уменьшается.
- `/v` открывает суперадминское меню профилей с запуском новой сессии, показом пяти последних и перезапуском последней упавшей; пока Kaggle-рендер в статусе `RENDERING`, UI блокируется.
- Kaggle-интеграция для видео: сбор JSON-пейлоада, публикация датасета и kernel, трекинг статусов и `run_kernel_poller`, проверка учётки через `/kaggletest`.
- Готовый ролик и логи уходят в выбранный для профиля тестовый канал (если не задан, используется операторский чат); при наличии выбранного основного канала видео публикуется туда и фиксируется имя файла. События из ролика помечаются `PUBLISHED_MAIN` и тратят одну единицу включения.
- Исправили SQLite-инициализацию видео-анонсов: таблица `videoannounce_session` теперь создаётся с колонками `profile_key`, `test_chat_id` и `main_chat_id`, чтобы меню `/v` не падало на старых базах.
- Расширили SQLite-инициализацию видео-анонсов: добавляем `published_at`, `kaggle_dataset` и `kaggle_kernel_ref`, чтобы селекты `/v` не ломались на старых базах.
- Видео-анонсы показывают интро от LLM после подбора с кнопками для правки, просмотра JSON-файла и запуска Kaggle; пользовательские правки сохраняются в `selection_params`, а JSON отправляется как файл при предпросмотре и перед стартом рендеринга.
- Видео-анонс ранжирования собирает единый JSON-запрос с промптом, инструкциями, кандидатами, `response_format` и `meta` и отправляет его и результат в 4o и операторский чат даже без пользовательской инструкции.

### Video Announce Intro Patterns (2025-12-27)

- **Визуальные паттерны интро**: добавлены три паттерна для интро-экрана видео — `STICKER`, `RISING`, `COMPACT`. Пользователь выбирает паттерн в UI с кнопками и превью перед рендерингом.
- **Генератор превью паттернов**: новый модуль `video_announce/pattern_preview.py` генерирует PNG-превью паттернов на сервере без Kaggle.
- **Города и даты в интро**: `payload_as_json()` извлекает города и диапазон дат из событий и передаёт их в ноутбук для отображения на интро.
- **Шрифт Bebas Neue**: заменён шрифт Oswald на Bebas Neue Bold для Better Cyrillic rendering и соответствия референсному дизайну.
- **Fly-out анимация Sticker**: все текстовые стикеры вылетают с overshoot-эффектом в направлении наклона с задержкой между элементами.
- **Пропорциональные отступы**: вертикальные отступы между элементами интро теперь составляют 15% от высоты контента (как в превью), вместо фиксированных пикселей.
- **Исправлено отображение городов**: notebook теперь копирует `cities`, `date`, `pattern` из payload в `intro_data`, города отображаются в интро.
- **Дайджест в статусе импорта**: VK-импорт показывает поле `search_digest` события в сообщении об успехе.

## v0.3.17 – 2025-10-07

- VK crawler telemetry now exports group metadata, crawl snapshots, and sampled misses to Supabase (`vk_groups`, `vk_crawl_snapshots`, `vk_misses_sample`) with `SUPABASE_EXPORT_ENABLED`, `SUPABASE_RETENTION_DAYS` (default 60 days), and `VK_MISSES_SAMPLE_RATE` governing exports, sampling, and automatic cleanup.
- VK stories now ask whether to collect extra editor instructions and forward the answer plus any guidance to the 4o prompts.
- Добавлен справочник сезонных праздников (`docs/reference/holidays.md`), промпт 4o теперь перечисляет их с алиасами и описаниями, а импорт событий автоматически создаёт и переиспользует соответствующие фестивали.
- Log OpenAI token usage through Supabase inserts (guarded by `BOT_CODE`) and ship the `/usage_test` admin self-test so operators can verify the inserts and share usage snapshots during release comms.
- `/stats` подтягивает сводку токенов напрямую из Supabase (`token_usage_daily`/`token_usage`) и только при ошибке падает обратно на локальный снапшот, чтобы в релизных отчётах отображались свежие значения.

## v0.3.16 – 2025-10-05
- Telegraph event source pages now include a “Быстрые факты” block with date/time, location, and ticket/free status, hiding each line when the underlying data is missing so operators know it’s conditional.
- Системный промпт автоклассификации запрещает выбирать темы `FAMILY` и `KIDS_SCHOOL`, когда у события задан возрастной ценз; см. обновления в `main.py` (`EVENT_TOPIC_SYSTEM_PROMPT`) и документации `docs/llm_topics.md`.
- Уведомления в админ-чат для партнёров теперь включают первую фотографию события и ссылки на Telegraph и исходный VK-пост, чтобы операторы могли оперативно проверить публикацию.
- Fixed VK weekday-based date inference so it anchors on the post’s publish date and skips phone-number fragments like `474-30-04`, preventing false matches in review notes.
- Сокращённые VK-рерайты теперь дополняются тематическими хэштегами из ключевых тем события (например, `#стендап`, `#openair`, `#детям`, `#семье`).
- Тематический классификатор теперь доверяет 4o выбор темы `KRAEVEDENIE_KALININGRAD_OBLAST`, локальные эвристики отключены, а постобработка распределения тем удалена.
- Ограничение на количество тем увеличено до пяти, промпты 4o обновлены под новый лимит и требования к краеведению.
- Восстановили независимую тему `URBANISM`, чтобы классификатор снова различал городские трансформации и не смешивал их с инфраструктурой.
- Month-page splitter final fallback now removes both «Добавить в календарь» and «Подробнее» links, keeping oversized months deployable despite Telegraph size limits and closing the recent operator request.

- Запустили научпоп-дайджест: в `/digest` появилась отдельная кнопка, а кандидаты отбираются по тематике `SCIENCE_POP`, чтобы операторы могли быстро собрать подборку.
- В `/digest` добавили кнопку краеведения, чтобы операторы могли собирать подборки по теме `KRAEVEDENIE_KALININGRAD_OBLAST` без ручной фильтрации.

- Expanded THEATRE_CLASSIC and THEATRE_MODERN criteria to include canonical playwrights and contemporary production formats.
- `/digest` для встреч и клубов теперь подсказывает тону интро по `_MEETUPS_TONE_KEYWORDS`, использует запасной вариант «простота+любопытство» и отдельно просит сделать акцент на живом общении и нетворкинге, если в подборке нет клубов; описание обновлено в `docs/digests.md`.
- Заголовки митапов в `/digest` нормализуются постпроцессингом: если событие открывает выставку, к названию добавляется пояснение «— творческая встреча и открытие выставки», как задокументировано в `docs/digests.md`.

## v0.3.15 – 2025-10-04
- Clarified the 4o parsing prompt and docs for same-day theatre showtimes: posters with one date and multiple start times now yield separate theatre events instead of a single merged entry.
- Added admin digests for нетворкинг, развлечения, маркеты, классический/современный театр, встречи и клубы и кинопоказы; обновлён список синонимов тем и меню /digest.
- Library events without explicit prices now default to free, so operators can spot the change in billing behavior.

## v0.3.14 – 2025-09-23
- Нормализованы HTML-заголовки и абзацы историй перед публикацией.
- Равномерно распределяем inline-изображения в исторических текстах.
- Убрали символное усечение ответов `ask_4o`.
- Очищаем заголовки историй от VK-разметки.

## v0.3.13 – 2025-09-22
- `/exhibitions` теперь выводит будущие выставки без `end_date`, чтобы операторы видели их и могли удалить вручную при необходимости.
- Починили обработку фестивальных обложек: теперь `photo_urls = NULL` не приводит к ошибкам импорта.
- Исправлена обработка обложек фестивалей: отсутствие `photo_urls` больше не приводит к ошибкам.
- Добавили поддержку загрузки обложки лендинга фестивалей через `/weekendimg`: после загрузки страница пересобирается автоматически.
- `/addevent`, форварды и VK-очередь теперь распознают афиши (один проход Catbox+OCR), подмешивают тексты в LLM и показывают расход/остаток токенов.
- Результаты распознавания кешируются и уважают дневной лимит в 10 млн токенов.
- Added `/ocrtest` diagnostic command, чтобы сравнить распознавание афиш между `gpt-4o-mini` и `gpt-4o` с показом использования токенов.
- Clarified the 4o parsing prompt to warn about possible OCR mistakes in poster snippets.
- `/events` автоматически сокращает ссылки на билеты через vk.cc и добавляет строку `Статистика VK`, чтобы операторы могли открыть счётчик переходов.
- VK Intake помещает посты с одной фотографией и пустым текстом в очередь и отмечает их статусом «Ожидает OCR».
- В VK-очереди появились кнопки «Добавить (+ фестиваль)»/«📝🎉 …», а импорт теперь создаёт или обновляет фестиваль даже при отсутствии событий.
- На стартовом экране появилась кнопка «+ Добавить фестиваль»: оператор жмёт её, чтобы открыть ручное создание фестиваля, а при отсутствии распознанного фестиваля LLM-поток останавливает импорт с явным предупреждением.
- Уточнены правила очереди: URGENT с горизонтом 48 ч, окна SOON/LONG завершаются на 14 / 30 дней, FAR использует веса 3 / 2 / 6, джиттер задаётся по источнику, а стрик-брейкер FAR срабатывает после K=5 не-FAR выборов.
- Истории из VK перед публикацией прогоняются через редакторский промпт 4o: бот чинит опечатки, разбивает текст на абзацы и добавляет понятные подзаголовки.
- Month pages retry publishing without «Добавить в календарь» links when Telegraph rejects the split, preventing `/pages_rebuild` from failing on oversized months.
- На исторических страницах автоматически очищаем сторонние VK-ссылки, оставляя только ссылку на исходный пост.
- Импорт списка встреч из VK-очереди создаёт отдельные события для каждой встречи, а не только для первой.
- Список сообществ ВК показывает статусы `Pending | Skipped | Imported | Rejected` и поддерживает пагинацию.
- Ежедневный Telegram-анонс теперь ссылается на Telegraph-страницу для событий из VK-очереди (кроме партнёрских авторов).
- «✂️ Сокращённый рерайт» сохраняет разбивку на абзацы вместо склеивания всего текста в один блок.
- VK source settings now store default ticket-link button text and prompt; ingestion applies the saved link only when a post lacks its own ticket or registration URL, keeping operator-provided links untouched.
- Запустили психологический дайджест: в `/digest` появилась отдельная кнопка, подбор идёт по тематике и автоматически создаётся интро.

- Introduced automatic topic classification with a closed topic list, editor display, and `/backfill_topics` command.
- Classifier/digest topic list now includes the `PSYCHOLOGY`, `THEATRE_CLASSIC`, and `THEATRE_MODERN` categories.
- Refreshed related documentation and tests so deploy notes match the current feature set.

- Fixed VK review queue issue where `vk_review.pick_next` recalculates `event_ts_hint` and auto-rejects posts whose event date
  disappeared or fell into the past (e.g., a 7 September announcement shown on 19 September).
- Карточки отзывов VK теперь показывают совпавшие события Telegraph для распознанной даты и времени.

## v0.3.10 – 2025-09-21
This release ships the updates that were previously listed under “Unreleased.”

- Компактные строки «Добавили в анонс» теперь начинаются с даты в формате `dd.mm`.
- `/events` теперь содержит кнопку быстрого VK-рерайта с индикаторами `✂️`/`✅`, чтобы операторы видели, опубликован ли шортпост.

## v0.3.12 – 2025-09-21
### Added
- Добавили JSON-колонку `aliases` у фестивалей и пробрасываем пары алиасов в промпт 4o, чтобы нормализовать повторяющиеся названия.
- В интерфейсе редактирования фестиваля появилась кнопка «🧩 Склеить с…», запускающая мастер объединения дублей с переносом событий и алиасов.

### Changed
- После объединения фестивалей описание пересобирается на основе актуальных событий и синхронизируется со страницами/постами.
- Промпт 4o для фестивальных описаний требует один абзац до 350 знаков без эмодзи, чтобы операторы придерживались нового стандарта.

## v0.3.11 – 2025-09-20
### Added
- Ввели ручной блок «🌍 Туристам» в Telegram и VK с кнопками «Интересно туристам» и «Не интересно туристам».
- Добавили меню причин и поддержку комментариев.
- Добавили экспорт `/tourist_export` в `.jsonl`.

### Changed
- Обновили справочник факторов: `🎯 Нацелен на туристов`, `🧭 Уникально для региона`, `🎪 Фестиваль / масштаб`, `🌊 Природа / море / лендмарк / замок`, `📸 Фотогенично / есть что постить`, `🍲 Местный колорит / кухня / крафт`, `🚆 Просто добраться`.
- Обновили документацию про TTL: 15 минут для причин и 10 минут для комментария.

### Security
- Доступ к кнопкам туристической метки и `/tourist_export` оставили только неблокированным модераторам и администраторам; авторазметка запрещена до окончания исследования.

## v0.1.0 – Deploy + US-02 + /tz
- Initial Fly.io deployment config.
- Moderator registration queue with approve/reject.
- Global timezone setting via `/tz`.

## v0.1.1 – Logging and 4o request updates
- Added detailed logging for startup and 4o requests.
- Switched default 4o endpoint to OpenAI chat completions.
- Documentation now lists `FOUR_O_URL` secret.

## v0.2.0 – Event listing
- `/events` command lists events by day with inline delete buttons.

## v0.2.1 – Fix 4o date parsing
- Include the current date in LLM requests so events default to the correct year.

## v0.2.2 – Telegraph token helper
- Automatically create a Telegraph account if `TELEGRAPH_TOKEN` is not set and
  save the token to `/data/telegraph_token.txt`.
## v0.3.0 - Edit events and ticket info
- Added ticket price fields and purchase link
- Inline edit via /events
- Duplicate detection improved with 4o

## v0.3.1 - Forwarded posts
- Forwarded messages from moderators trigger event creation
- Events keep `source_post_url` linking to the original announcement

## v0.3.2 - Channel registration
- `/setchannel` registers a forwarded channel for source links
- `/channels` lists admin channels with removal buttons
- Bot tracks admin status via `my_chat_member` updates

## v0.3.3 - Free events and telegraph updates
- Added `is_free` field with inline toggle in the edit menu.
- 4o parsing detects free events; if unclear a button appears to mark the event as free.
- Telegraph pages keep original links and append new text when events are updated.

## v0.3.4 - Calendar files
- Events can upload an ICS file to Supabase during editing.
- Added `ics_url` column and buttons to create or delete the file.
- Use `SUPABASE_BUCKET` to configure the storage bucket (defaults to `events-ics`).
- Calendar files include a link back to the event and are saved as `Event-<id>-dd-mm-yyyy.ics`.
- Telegraph pages show a calendar link under the main image when an ICS file exists.
- Startup no longer fails when setting the webhook times out.

## v0.3.5 - Calendar asset channel
- `/setchannel` lets you mark a channel as the calendar asset source.
- `/channels` shows the asset channel with a disable button.
- Calendar files are posted to this channel and linked from month and weekend pages.
- Forwarded posts from the asset channel show a calendar button.

## v0.3.6 - Telegraph stats

- `/stats` shows view counts for the past month and weekend pages, plus all current and upcoming ones.

- `/stats events` lists stats for event source pages sorted by views.

## v0.3.7 - Large month pages

- Month pages are split in two when the content exceeds ~64&nbsp;kB. The first
  half ends with a link to the continuation page.

## v0.3.8 - Daily announcement tweak

- Daily announcements no longer append a "подробнее" link to the event's
  Telegraph page.

## v0.3.9 - VK daily announcements

- Daily announcements can be posted to a VK group. Set the group with `/vkgroup` and adjust
  times via `/vktime`. Use the `VK_TOKEN` secret for API access.

## v0.3.10 - Festival stats filter and daily management updates

- `/stats` now lists festival statistics only for upcoming festivals or those
  that ended within the last week.
- `/regdailychannels` and `/daily` now show the VK group alongside Telegram channels.
  VK posting times can be changed there and test posts sent.
- Daily announcements include new hashtag lines for Telegram and VK posts.

## v0.3.11 - VK monitoring MVP and formatting tweaks

- Added `/vk` command for manual monitoring of VK communities: add/list/delete groups and review posts from the last three days.
- New `VK_API_VERSION` environment variable to override VK API version.
- VK daily posts show a calendar icon before "АНОНС" and include more spacing between events.
- Date, time and location are italicized if supported.
- Prices include `руб.` and ticket links move to the next line.
- The "подробнее" line now ends with a colon and calendar links appear on their own line as
  "📆 Добавить в календарь: <link>".

## v0.3.12 - VK announcement fixes

- Remove unsupported italic tags and calendar line from VK posts.
- Event titles appear in uppercase and the "подробнее" link follows the
  description.
- A visible separator line now divides events to improve readability.

## v0.3.13 - VK formatting updates

- VK posts use two blank separator lines built with the blank braille symbol.
- Ticket links show a ticket emoji before the URL.
- Date lines start with a calendar emoji and the location line with a location pin.

## v0.3.14 - VK link cleanup

- Removed the "Мероприятия на" prefix from month and weekend links in VK daily posts.

## v0.3.15 - Channel name context

- Forwarded messages include the Telegram channel title in 4o requests so the
  model can infer the venue.
- `parse_event_via_llm` also accepts the legacy `channel_title` argument for
  compatibility.

## v0.3.16 - Festival pages

- Added a `Festival` model and `/fest` command for listing festivals.
- Daily announcements now show festival links.
- Logged festival-related actions including page creation and edits.
- Festival pages automatically include an LLM-generated description and can be
  edited or deleted via `/fest`.

## v0.3.17 - Festival description update

- Festival blurbs use the full text of event announcements and are generated in
  two or three paragraphs via 4o.

## v0.3.18 - Festival contacts

- Festival entries store website, VK and Telegram links.
- `/fest` shows these links and accepts `site:`, `vk:` and `tg:` edits.
- **Edit** now opens a menu to update description or contact links individually.

## v0.3.19 - Festival range fix

- LLM instructions clarified: when festival dates span multiple days but only
  some performances are listed, only those performances become events. The bot
  no longer adds extra dates unless every day is described.

## v0.3.20 - Festival full name

- Festivals now store both short and full names. Telegraph pages and VK posts
  use the full name while events and lists keep the short version.
- `/fest` gained edit options for these fields. Existing records are updated
  automatically with the short name as the default full one.

## v0.3.21 - Partner activity reminder

- Partners receive a weekly reminder at 9 AM if they haven't added events in
  the past seven days.
- The superadmin gets a list of partners who were reminded.

## v0.3.22 - Partner reminder frequency fix

- Partners who haven't added events no longer receive daily reminders; each
  partner is notified at most once a week.

## v0.3.23 - Weekend VK posts

- Creating a weekend Telegraph page now also publishes a simplified weekend
  post to VK and links existing weekend VK posts in chronological order.

## v0.3.24 - Weekend VK source filter

- Weekend VK posts include only events with existing VK source posts and no
  longer attempt to create source posts automatically.

## v0.3.25 - Daily VK title links

- Event titles in VK daily announcements link to their VK posts when available.

## v0.3.26 - Festival day creation

- Announcements describing a festival without individual events now create a
  festival page and offer a button to generate day-by-day events later.
- Existing databases automatically add location fields for festivals.

## v0.3.27 - Festival source text

- Festival descriptions are generated from the full original post text.
- Festival records store the original announcement in a new `source_text` field.

## v0.3.28 - VK user token

- VK posting now uses a user token. Set `VK_USER_TOKEN` with `wall,groups,offline` scopes.
- The group token `VK_TOKEN` is optional and used only as a fallback.

## v0.3.29 - Film screenings

- Added support for `кинопоказ` event type and automatic detection of film screenings.

## v0.3.30 - Festival ticket links

- Festival records support a `ticket_url` and VK/Telegraph festival posts show a ticket icon and link below the location.

## v0.3.31 - Unified publish progress

- Event publication statuses now appear in one updating message with inline status icons.

## v0.3.32 - Festival program links

- Festival records support a `program_url`. Telegraph festival pages now include a "ПРОГРАММА" section with program and site links when provided, and the admin menu allows editing the program link.

## v0.3.33 - Lecture digest improvements

- Caption length for lecture digests now uses visible HTML text to fit up to 9 lines.
- Removed URL shortener functionality and related configuration.
- 4o title normalization returns lecturer names in nominative form with `Имя Фамилия: Название` layout.

## v0.3.34 - VK Intake & Review v1.1

- Added database tables and helpers for VK crawling and review queue.
- Introduced `vk_intake` module with keyword and date detection utilities.

## v0.3.35 - VK repost link storage

- Event records now include an optional `vk_repost_url` to track reposts in the VK afisha.

## v0.3.36 - VK crawl utility

- Introduced `vk_intake.crawl_once` for cursor-based crawling and enqueueing of
  matching posts.
- Dropped the unused VK publish queue in favor of operator-triggered reposts;
  documentation updated.

## v0.3.37 - VK inbox review

- The review flow now reads candidates from the persistent `vk_inbox` table.
- Operators can choose to repost accepted events to the Afisha VK group.
- Removed remaining references to the deprecated publish queue from docs.

## v0.3.38 - VK queue summary

- `/vk_queue` displays current inbox counts and offers a button to start the
  review flow.

## v0.3.39 - VK review UI polish

- Review flow now presents media cards with action buttons and logs rebuilds
  per month.
- Accepted events immediately send Telegraph and ICS links to the admin chat.
- The "🧹 Завершить…" button rebuilds affected months sequentially.
- Operators can repost events to the Afisha VK group via a dedicated button
  storing the final post link.

## v0.3.40 - VK intake improvements

- Incremental crawling with pagination, overlap and optional 14‑day backfill.
- Randomised group order and schedule jitter to reduce API load.
- Keyword detector switched to regex stems with optional `pymorphy3` lemma
  matching via `VK_USE_PYMORPHY`.
- Date and time parser recognises more Russian variants and returns precise
  timestamps for scheduling.

## v0.3.41 - VK group context for 4o

- VK event imports now send the group title to 4o so venues can be inferred from
  `docs/reference/locations.md` when posts omit them.

## v0.3.42 - VK review media
- VK review: поддержаны фото из репостов (copy_history), link-preview, doc-preview; для video берём только превью-картинки, видео не загружаем

## v0.3.43 - Festival landing stats

- `/stats` now shows view counts for the festivals landing page.

## v0.3.44 - VK short posts

- VK review reposts now use safe `wall.post` with photo IDs.
- Added "✂️ Сокращённый рерайт" button that publishes LLM‑compressed text.

## v0.3.45 - VK shortpost preview

- "✂️ Сокращённый рерайт" отправляет черновик в админ-чат с кнопками
  публикации и правки.
- Посты больше не прикрепляют фотографии, только ссылку с превью.

## v0.3.46 - Video announce ranking context

- Ранжирование видеоподбора отправляет в LLM полный текст со страницы
  Telegraph и сохраняет в экспортируемом JSON полный промпт запроса.
