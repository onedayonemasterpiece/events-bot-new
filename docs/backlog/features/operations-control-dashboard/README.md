# Operations control dashboard

> Status: **important separate post-release release**. A small non-visual readiness scorecard is still mandatory before the first public presentation; the protected web dashboard itself may follow after launch.

## Goal

Give the administrator one truthful place to see whether critical product pipelines completed their expected **delivery**, not merely whether a scheduler or Kaggle process started.

Existing primitives are fragmented across [`/general_stats`](../../../features/general-stats/README.md), `ops_run`, the [Kaggle status ledger](../../../features/kaggle-status-framework/README.md), [`/healthz` and scheduler state](../../../operations/cron.md), job outbox rows, video/promo tables, transport snapshots, static release manifests and image-audit evidence. No unified dashboard currently exists.

## Boundary with the first public release

Before presentation, implement a compact superadmin-only `ops_status`/readiness projection, surfaced through Telegram `/general_stats` or an equivalent protected operator command. It must cover release-critical static publication, transport refresh, image-dedup audit freshness and promo fulfilment, alert on state transitions and produce a SHA/time-bound RC evidence snapshot.

The separate post-release release adds the durable protected web UI, trends and drill-down. The absence of the polished dashboard does not waive first-release monitoring, alerts or evidence gates.

## Required coverage

| Domain | Green requires |
|---|---|
| Event ingestion | expected Telegram/VK/site/ticket/festival source slots completed, coverage/backlog/errors known and accepted delivery visible |
| Video announcements | run → session → Kaggle terminal artifact → every required public target reconciled |
| Promo campaigns | due eligible activities reconciled to actual exposures/publications; scheduler presence alone is insufficient |
| Transport | KPPK and bus provider refreshes validated, combined last-good snapshot promoted, freshness within SLA and downstream static rebuild reconciled |
| Static site | Smart Update/coalesced job → Kaggle checked artifact → release manifest → current pointer → CDN smoke all name the same accepted release |
| Image dedup | audit covers the latest eligible-media inventory and has zero confirmed duplicates and zero unreviewed candidate clusters |
| Event quality | current audit cadence, incident count/rate, reopened root causes and 14-day release window are explicit |
| Email | worker/outbox lag, delivery/suppression/DLQ and transition alerts are PII-free and within approved thresholds |
| Runtime/capacity | `/healthz`, scheduler/watchdogs, DB/disk/log budgets, Supabase budget and critical Kaggle resources are healthy |

## Status contract

Maintain a versioned registry per check:

- stable check id, system, owner and criticality;
- expected cadence/slot/timezone and freshness/SLO;
- last expected slot, attempt, success and **successful delivery**;
- `ok | warn | critical | unknown`, stable reason code and observed time;
- authoritative run/snapshot/release ids and redacted evidence/incident links;
- catch-up state and current acknowledgement, if any.

Missing or incomparable evidence is `unknown`/`critical`, never green or zero. A fresh heartbeat cannot substitute for a required publication; an acknowledgement cannot turn a failed check green. LLM analysis may summarize anomalies, but only deterministic source evidence may establish green.

## Architecture and storage

- Fly SQLite/`ops_run` remains the canonical core operations state. Provider adapters read Kaggle ledger, job outbox, publication/session/exposure tables, transport manifests, Supabase health summaries and Object Storage/CDN release evidence without creating a second source of truth.
- Store one compact current row per check and optional transition-only history with explicit TTL. Do not copy raw logs, provider payloads or all source history into a dashboard database.
- The dashboard is a dynamic protected admin surface, not a public static/noindex/bearer page. Ordinary Yandex/email site identity does not grant admin access.
- Browser code never receives SQLite access or service keys. Redact email/chat/user identifiers, tokens, private dataset references, raw payloads and stack traces.
- MVP is read-only. Retry/catch-up/kill-switch controls are a later phase with confirmation, idempotency, authorization and a separate immutable audit log.

## Delivery stages

1. **Pre-release scorecard:** normalize the critical status registry; instrument missing static promotion, transport snapshot, image-audit freshness and promo fulfilment facts; add deduplicated Telegram transition/recovery alerts.
2. **Post-release data reconciliation:** align `/general_stats`, `ops_run`, Kaggle and delivery-specific evidence; set SLOs/owners/retention and test missed-slot/partial-delivery cases.
3. **Protected read-only dashboard:** traffic-light summary, freshness/expected slot, trends, filters and drill-down to redacted evidence/incidents.
4. **Optional controlled actions:** separately approve idempotent catch-up/retry/kill-switch operations; never couple them to dashboard read correctness.

## Acceptance examples

- a missed scheduled slot cannot be green;
- a successful Kaggle render without all required video targets is red;
- a promo runner is green only when due targets are fulfilled or explicitly and validly skipped;
- static publication is green only when snapshot, checked artifact, release pointer and CDN match;
- KPPK/bus refresh retains provider last-good on partial failure, surfaces stale age and reconciles exactly one combined rebuild;
- image dedup is green only for the current media inventory with zero confirmed and zero unreviewed;
- a simulated unknown/missing adapter never renders as `0 problems`;
- unauthorized users cannot read the dashboard or its API, and no PII/secrets appear in UI, logs or evidence links.

## Related documentation

- [General stats](../../../features/general-stats/README.md)
- [Kaggle status framework](../../../features/kaggle-status-framework/README.md)
- [Runtime/scheduler health](../../../operations/cron.md)
- [Runtime logs](../../../operations/runtime-logs.md)
- [Static builder](../../../operations/kaggle-static-site-builder.md)
- [Event image duplicate audit](../../../operations/event-image-duplicate-audit.md)
- [Event quality release monitoring](../../../operations/event-quality-release-monitoring.md)
- [Public release readiness](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
