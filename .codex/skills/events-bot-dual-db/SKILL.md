---
name: events-bot-dual-db
description: Use for events-bot-new database, Supabase, PostgreSQL, static-site personalization, analytics, storage, or production DB tasks where Codex must route between the existing Fly SQLite core database and the separate Supabase/Postgres personalization database. Triggers include Supabase access, PERSONALIZATION_SUPABASE_* env vars, PERSONALIZATION_DIRECT_CONNECTION_STRING, pg_database_size, anonymous personalization, static-site dynamic feeds, RLS, Data API, Fly /data/db.sqlite health, or questions about which database should store a feature.
---

# Events Bot Dual DB

## Boundary

This repository now has **two different databases for different jobs**:

1. **Core production DB** — Fly.io SQLite at `/data/db.sqlite`.
   - Owns bot state, canonical events, source imports, Telegram/VK publication state, guide tables, job queues, pages/Telegraph metadata, and operational scheduler state.
   - Use the `fly-prod-db-access` skill and read-only Fly SQL probes for production checks.

2. **Personalization DB** — separate Supabase/Postgres project.
   - Owns static-site personalization, anonymous/browser telemetry, user/session/profile snapshots, recommendation cache/API/RPC, and future static-site dynamic feed state.
   - Use `PERSONALIZATION_SUPABASE_*` env vars and `PERSONALIZATION_DIRECT_CONNECTION_STRING` from `.env`.
   - Do **not** reuse the legacy/current project `SUPABASE_URL` / `SUPABASE_KEY` for personalization; those belong to existing token/storage/VK telemetry integrations.

3. **YDB sidecars / analytics** — service-only high-volume history/analytics and explicitly isolated feature sidecars such as event-comment-feedback.
   - YDB is not the user identity/profile/favorites/subscription/email-outbox system of record.
   - YDB may receive de-identified/HMAC analytics projections asynchronously.
   - A YDB outage must not block a user action, profile update or send-control transaction.

Never silently move core bot data into the personalization DB, personalization state into Fly SQLite, or create a competing YDB user profile/email control plane. Canonical ADR: `docs/architecture/personalization-data-ownership.md`.

## Required startup checks

1. Open `docs/README.md` and `docs/routes.yml` per repo rules.
2. For Supabase/Postgres work, use the installed Supabase skill/docs when available, or official Supabase docs via web if tools are absent.
3. Check `.env` key presence without printing secrets:
   - personalization: `PERSONALIZATION_SUPABASE_URL`, `PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`, `PERSONALIZATION_SUPABASE_SECRET_KEY`, `PERSONALIZATION_DIRECT_CONNECTION_STRING`.
   - legacy project: `SUPABASE_URL`, `SUPABASE_KEY`.
4. If querying prod SQLite, keep artifacts under `artifacts/codex/` or `artifacts/db/` and do not dump sensitive rows unless needed.

## Supabase personalization access

- Frontend/static site may use only `PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`.
- Backend/Fly/migrations may use `PERSONALIZATION_SUPABASE_SECRET_KEY` or direct Postgres connection string.
- Treat `sb_secret_...` as backend-only; never put it in browser-exposed variables.
- Prefer Supabase MCP `execute_sql` if available. If not available, use `PERSONALIZATION_DIRECT_CONNECTION_STRING` with a Postgres client.
- Direct Supabase DB host (`db.<project>.supabase.co:5432`) can be IPv6-only. In this repo environment, use Supavisor pooler strings when direct IPv6 fails:
  - session pooler: port `5432`, good for migrations/SQL sessions;
  - transaction pooler: port `6543`, fine for simple SQL checks and many serverless-style calls, but avoid prepared-statement-dependent workflows.

## RLS and Data API policy

For the personalization project:

- Keep Data API enabled when frontend will call Supabase directly.
- Keep automatic RLS enabled for safety.
- Disable “Automatically expose new tables” when possible; explicitly grant Data API access only for the intended roles.
- Enable RLS on every exposed table.
- Prefer views/RPCs for public recommendation reads instead of exposing raw profile tables.
- Use insert-only policies for anonymous telemetry tables.
- Use `TO anon` / `TO authenticated` clauses in policies; avoid deprecated `auth.role()` checks.
- Do not create public `SECURITY DEFINER` functions without revoking default `PUBLIC` execute and adding explicit grants.

## Routing rules

Store in **core SQLite**:

- canonical events, venues, sources, source facts, media/poster rows tied to public event pages;
- Telegram/VK/Telegraph publication state;
- scheduler/ops/joboutbox state;
- guide monitoring canonical rows and digest state;
- static page generation metadata if it is tied to event lifecycle (`event_id`, `url`, `hash`, `generated_at`).

Store in **personalization Supabase/Postgres**:

- anonymous visitor IDs and sessions;
- page impressions, detail views, dwell/skip/hide/ticket-click events;
- short/mid/long-term interest profiles;
- recommendation cache and ranking inputs/outputs;
- browser-facing RPCs/views for dynamic event feed personalization;
- E2E personalization personas and debug snapshots.
- Yandex/verified-email identity-linked profile state, consent evidence and merge audit;
- durable favorites/event follows/calendar-save state;
- recommendation/transactional subscriptions, suppressions, outbox/send guards and delivery control evidence;
- personal recommendation issue/card rows and personal-page token metadata.

Store in **YDB sidecars/analytics**:

- de-identified raw/aggregate product and delivery analytics with explicit TTL/retention;
- independent event-comment-feedback crawl/comment/embedding/verifier state;
- never plaintext email, bearer tokens, current user profile, send eligibility or a second outbox.

Use object storage/CDN, not either DB, for static HTML/JSON/media blobs whenever possible.

## Helper script

Run this when you need a redacted personalization DB health/size check:

```bash
python3 .codex/skills/events-bot-dual-db/scripts/check_personalization_db.py
```

If no Postgres driver is installed, install one into an ignored artifact directory and rerun:

```bash
python3 .codex/skills/events-bot-dual-db/scripts/check_personalization_db.py --install-driver
python3 .codex/skills/events-bot-dual-db/scripts/check_personalization_db.py
```

The script reads `.env`, never prints passwords/keys, and reports `pg_database_size(current_database())`, schema table counts, extensions, and top relation sizes.

## Release hygiene

- Durable schema/code changes must update the canonical docs in `docs/` and `CHANGELOG.md` when they affect application behavior.
- Do not commit `.env`, direct DB URLs, generated DB snapshots, or `artifacts/`.
- Stage only files directly related to the current task.

## External consultant policy

For personalization/static-site architecture reviews, canary gates, Supabase/Postgres write-path reviews, or other external-consultant decisions:

- Valid Gemini consultant models are only `gemini-3-pro-preview` and `gemini-3.1-pro-preview`.
- Do not present Gemini Flash, Flash-Lite, Lite, Gemma, embeddings, OpenAI, or any lower-tier smoke/probe output as a completed external consultant review.
- If both Gemini Pro models fail with quota/capacity/billing/provider errors, record blocker evidence and do not substitute a lower model.
- Acceptable external consultant fallback is Opus via `a-opus`/Antigravity or the project Claude `Opus` alias when available.
