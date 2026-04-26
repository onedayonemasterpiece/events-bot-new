---
name: gemma-4-migration-playbook
description: Use when migrating an existing repo pipeline or feature from Gemma 3 to Gemma 4, or when auditing a Gemma 4 rollout against the proven guide-excursions and Telegram Monitoring migrations in this repository.
---

# Gemma 4 Migration Playbook

Use this skill for repo-internal Gemma 4 migrations. It is based on the successful `guide-excursions` rollout and the completed Telegram Monitoring migration/hardening.

Do not use this skill for a generic model comparison or for replacing the final public writer in `Smart Update`.

## Start

Read only the canon that matches the surface you are touching:

- `docs/reports/gemma-4-migration-research-2026-04-19.md` for key topology, rollout order, and known risks.
- `docs/features/llm-gateway/README.md` sections `2.5`, `2.6`, and `4.2` for the provider/runtime contract.
- `docs/llm/smart-update-lollipop-gemma-4-migration.md` and `docs/llm/smart-update-lollipop-gemma-4-eval.md` for `Smart Update` / `lollipop g4`.
- `docs/features/guide-excursions-monitoring/README.md` for the only production-proven Gemma 4 migration in the repo.
- `docs/features/telegram-monitoring/README.md` for the completed Telegram Monitoring Gemma 4 producer/import rollout.
- `docs/reports/incidents/INC-2026-04-21-guide-gemma4-partial-monitoring.md` when the change touches scheduled runs, `partial` semantics, or digest publication.
- `docs/reports/incidents/INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke.md` when production validation, `/healthz`, `/webhook`, or serving/runtime health is involved.
- `docs/reports/incidents/INC-2026-04-26-daily-location-fragments.md` when Telegram extraction touches `location_name`, venue grounding, reference locations, or `/daily`.

## Proven Reference

Treat `guide-excursions` and Telegram Monitoring as the successful migration templates.

Guide working model split:
  - Kaggle `screen/extract`: `models/gemma-4-31b-it`
  - server `enrich/dedup/digest_writer`: `gemma-4-31b`
Guide proven outcomes:
  - canonical eval pack `GE-EVAL-01..07`: `Gemma 3 = 4/7 pass`, `Gemma 4 = 6/7 pass`, `0` timeouts;
  - live canary `2026-04-20`: `0 llm_error`, `0 schema/provider reject`, `0 Gemma 3 fallback`, `0 out-of-region materializations`.

Telegram Monitoring working model split:
- Kaggle text/OCR/source-metadata producer: `models/gemma-4-31b-it` through shared `GoogleAIClient` with native `response_schema`.
- Server import remains Smart Update/fact-first; Gemma 4 producer output must still pass candidate grounding, reference-location normalization, and Smart Update merge.
- Key isolation: `GOOGLE_API_KEY3` / `GOOGLE_API_LOCALNAME3`; `GOOGLE_API_KEY` inside the notebook is only a legacy alias to that same selected monitoring key.

Telegram Monitoring proven outcomes:
- full scheduled Gemma 4 run `48fa98294333486d94dd0e14785d774f`: 45 sources, `messages_scanned=177`, `messages_with_events=69`, `events_extracted=84`, `GOOGLE_API_KEY3`, `gemma-3=0`, recovery import `ops_run id=803`, `errors_count=0`;
- post-timeout smoke `tg_g4_45s_smoke_20260423a`: primary `ops_run id=807`, `errors_count=0`, bounded 45s provider fail-open behavior;
- forced A/B gate on the same 16 posts: Gemma 3 baseline `10` events with `empty_date=1`, `english_event_type=4`; final Gemma 4 gate `abfinal095edeb15` produced `14` events with `0` checked leak/ghost/empty-date/bad-date/English-city/English-event_type/unknown/service-heading-title smells;
- production postdeploy `prod_g4_postdeploy_20260425b`: 45 sources, `messages_scanned=118`, `messages_with_events=39`, `events_extracted=61`, producer log `models/gemma-4-31b-it`, `gemma-3=0`, `Traceback=0`, `ERROR=0`, recovery import `ops_run id=858`, `events_imported=36`, `errors_count=0`;
- location regression incident `INC-2026-04-26-daily-location-fragments` is closed: deployed hotfixes reached `origin/main`, 36 targeted tests passed, 20 affected rows repaired/rebuilt, corrected daily catch-up sent, and local Gemma 4 producer eval returned `bad_hits=[]` for the incident location-fragment cases.

Use these rollouts for concrete patterns, not as a reason to copy prompts blindly.

## Hard Rules

- Migrate one bounded feature surface at a time.
- Keep migrations stage-oriented. `Gemma 4` is not a drop-in replacement for legacy `Gemma 3` prompts.
- Split `system` policy from `user` payload.
- For structured stages on `Gemma 4`, prefer native `response_schema` plus `response_mime_type=application/json`.
- Keep schemas in the provider-compatible subset. Avoid `anyOf`, `additionalProperties`, and nullable schema tricks unless they are live-proven on the target path.
- Filter `parts[].thought = true` before JSON parsing, persistence, or operator-facing output.
- Keep the requested model first in the gateway fallback chain.
- Preserve key isolation:
  - bot/runtime -> `GOOGLE_API_KEY`
  - guide -> `GOOGLE_API_KEY2`
  - Telegram monitoring Kaggle -> `GOOGLE_API_KEY3` once migrated
- Stay `LLM-first`: do not patch semantic regressions with regex or keyword shortcuts.
- For text-quality regressions, first tighten the stage prompt/schema and, if needed, add a narrow staged Gemma review/rescue pass. Deterministic code may enforce syntax, grounding, venue-shape, reference normalization, and safe drops, but must not invent event semantics.
- Never call a migration complete from local-only eval or a partial smoke. Closure needs production-like or production evidence for the actual key/runtime path plus import/catch-up evidence if a scheduled surface was affected.

## Known Anti-Patterns

- `models/gemma-4-26b-a4b-it` was not stable for long Russian reportage posts under structured output on the guide screen stage.
- Legacy prompt-only JSON contracts are not enough for the canonical structured `Gemma 4` path.
- Run-level `partial` must not automatically block publish if fresh eligible material was imported successfully.
- For `Smart Update`, the canon is still `Gemma 4 upstream + final 4o`; do not treat final-writer swap tests as rollout evidence.
- Telegram extraction can look clean on schema/output-shape checks while still leaking prose into `location_name`; treat venue grounding as its own smell class, not as covered by "value appears in source text".
- `message_date` is context for explicit relative anchors only. It is not a fallback event date for non-exhibition single events.
- OCR headings such as `НАЧАЛО В`, `БИЛЕТЫ`, dates, prices, age limits, and venue labels are facts, not event titles.
- Production smoke must not be launched in a way that starves the serving bot. If `/healthz`, `/webhook`, or `/start` stops responding, switch to incident workflow immediately.

## Migration Sequence

1. Fix the gateway/runtime contract first if the path cannot carry native `Gemma 4` settings safely.
2. Move one feature to a staged canary, not the whole repo.
3. Prefer `31b` for quality-critical structured stages unless a smaller model is already proven on the same task.
4. Build a real eval pack from live posts or sources, not only synthetic fixtures.
5. Run local and production-like canaries on the same evidence where possible; use A/B against Gemma 3 only as a regression detector, not as an abstract benchmark.
6. Run source-level smell checks on produced JSON: hidden Gemma 3 fallback, schema/provider rejects, thought/comment/markdown leaks, ghost rows, empty title/date, bad date shape, English city/event_type drift, `unknown` literals, service-heading titles, unsupported `message_date` fallback, and prose-like `location_name`.
7. Harden timeout, provider `5xx`, scheduled `partial`, Kaggle recovery, and serving-runtime behavior before calling the rollout complete.
8. Add regression tests plus canonical doc and `CHANGELOG` updates.
9. If production data/users were affected, finish with repair/rebuild/catch-up evidence, not just a code deploy.

## Telegram Monitoring Closure Bar

Before saying the Telegram Monitoring Gemma 4 migration is closed, verify:

- producer defaults and logs show `models/gemma-4-31b-it` and `gemma-3=0`;
- Kaggle uses shared `google_ai` / `GoogleAIClient`, native `response_schema`, bundled `google_ai` sources, and notebook-safe `main()` execution;
- secrets are scoped to `GOOGLE_API_KEY3` / `GOOGLE_API_LOCALNAME3`, with no unrelated key-pool leakage;
- provider calls are bounded by `TG_MONITORING_LLM_TIMEOUT_SECONDS` / `GOOGLE_AI_PROVIDER_TIMEOUT_SEC`;
- `telegram_results.json` passes the smell checks above on real posts, including location-fragment checks;
- server import/recovery reaches terminal `ops_run` status with `errors_count=0` or documented nonfatal skips;
- `/daily` or other public surfaces affected by the migration have been previewed or rerun when needed;
- any incident spawned by validation is recorded, compensated, and either closed or explicitly left in `monitoring` with evidence.

## Validation

Reuse the existing regression shape where possible:

- `tests/test_google_ai_client.py`
- `tests/test_tg_monitor_gemma4_contract.py`
- `tests/test_tg_candidate_location_grounding.py`
- `tests/test_daily_format.py`
- `tests/test_guide_kaggle_schema_contract.py`
- `tests/test_guide_local_llm_timeout_contract.py`
- `tests/test_scheduling_guide_digest.py`

Verify all of the following:

- no schema/provider rejects on the live provider;
- no hidden `Gemma 3` fallback when `Gemma 4` is intended;
- source/post-level diagnostics remain visible;
- local preview/digest Gemma calls are timeout-bounded where the pipeline needs fail-open behavior;
- scheduled publication is still driven by occurrence-level freshness, not a blanket run-level `partial`.
- Telegram Monitoring imports do not surface prose/schedule fragments as locations in `/daily`, Telegraph, or Smart Update facts.
