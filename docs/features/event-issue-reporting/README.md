# Static event issue reporting → ArtKodex incident workflow

Admin-only static event pages can hand off event-quality defects to ArtKodex.

## Product contract

- The report entry point is intentionally small: an admin-only link `Исправить ошибку` on `/sobytiya/*` pages opens a textarea and submit action.
- The block is visually separate from the public event UI (blue/dashed admin panel) and is hidden by default.
- Browser visibility is UX-only. The authoritative admin check happens in the `event-issue-report` Supabase Edge Function against server-side env allowlists.
- The current admin is the only real Yandex-authenticated Supabase user: `art.koder@yandex.ru` / Supabase Auth user id configured in `EVENT_ISSUE_REPORT_ADMIN_USER_ID`.

## Data flow

```text
Admin on static event page
  -> Supabase Auth session from the same kenigevents.ru origin
  -> GET /functions/v1/event-issue-report checks admin status
  -> POST /functions/v1/event-issue-report stores event_issue_reports row
  -> ArtKodex polls event_issue_reports with a server-side secret key
  -> ArtKodex creates an events-bot incident task and forum topic
  -> Codex investigates/fixes root cause and repairs canonical event content
  -> next static-site generation edits/removes the event page
```

The table lives in the personalization Supabase/Postgres database because the write originates from authenticated static-site UI. Canonical event repair remains in the Fly SQLite events-bot database.

## LLM-first incident expectations

ArtKodex prompts static-page reports as event-quality incidents. The task must:

1. open incident-management docs and prior event-quality `INC-*` contracts;
2. verify original sources, production DB rows, media/OCR, Telegraph, Telegram and VK surfaces;
3. judge event meaning/date/title/source support LLM-first, using deterministic checks only as evidence collectors/guardrails;
4. fix the code/config root cause before or alongside data repair;
5. repair/delete canonical event content so the next static generation reflects the fix.

## Operational env

Supabase Edge Function secrets:

- `EVENT_ISSUE_REPORT_ADMIN_USER_ID` or comma-separated `EVENT_ISSUE_REPORT_ADMIN_USER_IDS`;
- optional `EVENT_ISSUE_REPORT_ADMIN_EMAIL(S)`;
- existing personalization Supabase URL, publishable key, and secret/service key.

ArtKodex env:

- `ARTKODEX_EVENT_ISSUE_REPORTS_SUPABASE_URL`;
- `ARTKODEX_EVENT_ISSUE_REPORTS_SUPABASE_KEY` (server-side secret/service key);
- `ARTKODEX_EVENT_ISSUE_REPORTS_TABLE=event_issue_reports`;
- `ARTKODEX_EVENT_ISSUE_REPORTS_POLL_INTERVAL_SEC=15`;
- optional `ARTKODEX_EVENT_ISSUE_REPORTS_OPERATOR_CHAT_ID` / `USER_ID` (defaults to first numeric allowlisted operator).

## Verification

- Build/check static preview with public personalization envs.
- Edge Function smoke:
  - no bearer token → `401`;
  - non-admin authenticated user → `403`;
  - admin `GET` → `{ admin: true }`;
  - admin `POST` with valid event snapshot → `event_issue_reports` row with `status='submitted'`.
- ArtKodex poller should move the row to `queued`, set `artkodex_task_id`, create a forum topic when configured, and launch the task.
- Final closure must include DB/content repair evidence and branch/SHA.
