# CLAUDE

## Session Defaults
- Use Claude Opus only in this repository.
- Keep effort at `high` for every session and delegated task.
- For difficult consultations, architecture review, deep debugging, or major redesign, you may temporarily raise effort to `max`.
- Keep extended thinking enabled by default.
- Do not switch to Sonnet or Haiku unless the user explicitly changes project policy.
- Built-in non-project delegations are blocked in shared settings; use the project `Opus` alias for delegation.

## Opus Alias
- The project provides a dedicated subagent alias: `Opus`.
- Use `Opus` for consultation, architecture review, prompt critique, and substantial rework.
- When the user asks for "Opus", "consultation", "second opinion", or "доработай через Opus", delegate to the `Opus` subagent instead of changing the main session model ad hoc.
- For LLM-quality tasks, prefer asking `Opus` for concrete prompt-family edits, schema tightening, and `lollipop`-style stage decomposition rather than broad high-level architecture commentary.
- For external consultant reviews, follow `AGENTS.md`: Gemini review must use `gemini-3-pro-preview` or `gemini-3.1-pro-preview`; Flash/Lite outputs are supplementary probes only. If Gemini Pro is unavailable, use `a-opus`/Antigravity or this project `Opus` alias when access is active, otherwise record a blocker.

## Working Rules
- Start with the canonical project docs: `AGENTS.md`, `docs/README.md`, and `docs/routes.yml`.
- For behavior changes, keep canonical docs in `docs/` updated and add a concise entry to `CHANGELOG.md` under `[Unreleased]`.
- Treat `AGENTS.md` as the repository-wide routing and workflow contract.
- For festival monitoring, Festival Queue, `/start` add-event publication drift, or VK festival aggregate posts, also open `docs/backlog/features/festival-monitoring-debt/README.md` and `docs/reports/incidents/INC-2026-06-08-festival-vk-aggregate-regression.md` before proposing changes.
- For production Telegram UI E2E, target `@events_love39_bot` explicitly, use local `.env` only for the human Telethon session/API id/hash, check `/data/db.sqlite` table `user` for the E2E user's `is_superadmin=1`, and inspect `/data/runtime_logs/events-bot.log` immediately when the bot is silent.
- Respect Telegram session boundaries exactly as written in `AGENTS.md`: `TELEGRAM_AUTH_BUNDLE_S22` is for Kaggle/remote monitoring only, while `TELEGRAM_AUTH_BUNDLE_E2E` (or `TELEGRAM_SESSION`) is for local live E2E only.
- Do not substitute one auth bundle for another without explicit user permission, even as a temporary debugging shortcut.

## Requirements Discipline
- **Treat the canonical feature doc (e.g. `docs/features/<name>/README.md`) as the live requirements ledger.** When the user refines, clarifies, or contradicts an earlier requirement (including the original `docs/backlog/<...>/requirements.md`), record the change in the canonical doc the same turn it lands. Do not let refinements live only in chat — they will be lost.
- **Flag contradictions immediately.** Before silently following a new instruction that overrides an earlier one (either the spec doc or an earlier user statement), surface the contradiction:
  - quote the prior requirement,
  - quote the new requirement,
  - state which interpretation you intend to take and why,
  - ask the user to confirm before continuing.
  Do not just pick a side and proceed — drift caught after multiple rounds is much more expensive to fix than a 30-second check-in.
- **Each requirement change gets a CHANGELOG line.** Under `[Unreleased]`, record the override with both versions (old / new), the reason, and the date. This is the audit trail for why the implementation diverges from the original spec.
- **Re-derive open questions on every feedback round.** When the user pushes back on the result, list (a) what they're correcting, (b) which earlier requirement that contradicts or refines, (c) what else might be implicitly affected. Confirm with the user before broad rework.
- This applies even when the user explicitly tells you to "just continue" — autonomy on execution does not mean autonomy on changing requirements without documenting them.
