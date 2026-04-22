---
name: gemma-4-migration-playbook
description: Use when migrating an existing repo pipeline or feature from Gemma 3 to Gemma 4, or when auditing a Gemma 4 rollout against the proven guide-excursions migration in this repository.
---

# Gemma 4 Migration Playbook

Use this skill for repo-internal Gemma 4 migrations. It is based on the successful `guide-excursions` rollout and its follow-up hardening.

Do not use this skill for a generic model comparison or for replacing the final public writer in `Smart Update`.

## Start

Read only the canon that matches the surface you are touching:

- `docs/reports/gemma-4-migration-research-2026-04-19.md` for key topology, rollout order, and known risks.
- `docs/features/llm-gateway/README.md` sections `2.5`, `2.6`, and `4.2` for the provider/runtime contract.
- `docs/llm/smart-update-lollipop-gemma-4-migration.md` and `docs/llm/smart-update-lollipop-gemma-4-eval.md` for `Smart Update` / `lollipop g4`.
- `docs/features/guide-excursions-monitoring/README.md` for the only production-proven Gemma 4 migration in the repo.
- `docs/reports/incidents/INC-2026-04-21-guide-gemma4-partial-monitoring.md` when the change touches scheduled runs, `partial` semantics, or digest publication.

## Proven Reference

Treat `guide-excursions` as the successful migration template.

- Working model split:
  - Kaggle `screen/extract`: `models/gemma-4-31b-it`
  - server `enrich/dedup/digest_writer`: `gemma-4-31b`
- Proven outcomes:
  - canonical eval pack `GE-EVAL-01..07`: `Gemma 3 = 4/7 pass`, `Gemma 4 = 6/7 pass`, `0` timeouts;
  - live canary `2026-04-20`: `0 llm_error`, `0 schema/provider reject`, `0 Gemma 3 fallback`, `0 out-of-region materializations`.

Use the guide rollout for concrete patterns, not as a reason to copy prompts blindly.

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

## Known Anti-Patterns

- `models/gemma-4-26b-a4b-it` was not stable for long Russian reportage posts under structured output on the guide screen stage.
- Legacy prompt-only JSON contracts are not enough for the canonical structured `Gemma 4` path.
- Run-level `partial` must not automatically block publish if fresh eligible material was imported successfully.
- For `Smart Update`, the canon is still `Gemma 4 upstream + final 4o`; do not treat final-writer swap tests as rollout evidence.

## Migration Sequence

1. Fix the gateway/runtime contract first if the path cannot carry native `Gemma 4` settings safely.
2. Move one feature to a staged canary, not the whole repo.
3. Prefer `31b` for quality-critical structured stages unless a smaller model is already proven on the same task.
4. Build a real eval pack from live posts or sources, not only synthetic fixtures.
5. Run a production-like canary with source-level diagnostics.
6. Harden timeout, provider `5xx`, and scheduled `partial` behavior before calling the rollout complete.
7. Add regression tests plus canonical doc and `CHANGELOG` updates.

## Validation

Reuse the existing regression shape where possible:

- `tests/test_google_ai_client.py`
- `tests/test_guide_kaggle_schema_contract.py`
- `tests/test_guide_local_llm_timeout_contract.py`
- `tests/test_scheduling_guide_digest.py`

Verify all of the following:

- no schema/provider rejects on the live provider;
- no hidden `Gemma 3` fallback when `Gemma 4` is intended;
- source/post-level diagnostics remain visible;
- local preview/digest Gemma calls are timeout-bounded where the pipeline needs fail-open behavior;
- scheduled publication is still driven by occurrence-level freshness, not a blanket run-level `partial`.
