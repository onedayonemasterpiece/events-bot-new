# Static event issue reporting to ArtKodex

> Status: **prototype in a stale mixed branch; clean port required**. No production-complete idempotent ArtKodex repair loop is claimed.

## Product contract

An authenticated allowlisted administrator sees `Исправить ошибку` on an event page, submits a problem description, and can follow all repair attempts for that event:

- event id and immutable event snapshot accompany the report;
- browser visibility is UX only; server performs authoritative admin authorization;
- duplicate concurrent launch is prevented;
- ArtKodex claims one task atomically;
- status, error and structured repair result are persisted;
- after a terminal result, the admin may submit a new linked report;
- history shows multiple attempts without exposing the control to public users.

## Data boundary

The report originates from authenticated static-site UI, so Supabase/Postgres owns report/control-plane rows and RLS/admin authorization. ArtKodex uses a server-side credential. Canonical event repair remains in Fly SQLite/Smart Update/publication workflows; a report row never edits event facts directly.

## Incident expectations

Every task must follow incident management and event-quality regression contracts:

1. inspect canonical sources and public surfaces;
2. identify/fix root cause before or with row/public repair;
3. run relevant replay/tests;
4. repair canonical DB and affected Telegram/VK/Telegraph/static/ICS surfaces;
5. return structured result, incident/task links and rebuilt-page evidence.

## Required idempotency

- client idempotency key;
- unique active-report constraint or equivalent DB guard;
- atomic `submitted → processing` claim;
- retry/crash cannot create a second ArtKodex task;
- terminal report permits a new linked report;
- re-reporting never overwrites history.

## Branch governance

Do not merge/rebase `feature/event-issue-report-artkodex-20260703` wholesale: it is mixed with Smart Update incident fixes, medallions, assets and UX labs. Create a fresh branch from current `origin/main`; port only issue-report doc/component/Edge/migration/history changes, then add missing DB idempotency, poller and E2E.

## Release acceptance

- anonymous `401`, authenticated non-admin `403`, admin GET/POST success;
- no admin control/data leak in public HTML;
- concurrent submit and poller tests;
- crash/retry without duplicate task;
- real ArtKodex task/topic and terminal repair result;
- static rebuild and history/result visible after repair.
