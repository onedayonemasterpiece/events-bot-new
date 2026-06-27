# Bots and Automation Contract for Anonymous Personalization

> **Status:** MVP-0 guardrail contract for `event_detail_related` and future static-site feeds.
>
> Goal: search/preview/AI crawlers must see useful static event pages, but must not receive or train anonymous personalization.

## Core rule

Search, preview and AI crawlers receive the same public static event page and static related fallback that a no-JS/no-consent user receives. They do not receive personalized ordering and do not influence anonymous personalization profiles, popularity baselines or future ranker training.

## Actor classes

| Actor class | Examples | Page content | Personalization | Telemetry handling |
| --- | --- | --- | --- | --- |
| `crawler_verified` | verified search/AI crawler by UA + reverse DNS/IP allowlist where available | static HTML, JSON-LD, static related fallback | disabled | no trusted telemetry; optional aggregate access logs only |
| `preview_bot` | Telegram/VK/WhatsApp/social link preview | static HTML/OG image/meta, static related fallback | disabled | no trusted telemetry |
| `monitor` | uptime, Rich Results, URL Inspection, internal smoke checks | static or explicit test-mode HTML | disabled unless test explicitly says otherwise | tagged diagnostic only, excluded from ranker data |
| `bot_likely` | high-rate/suspicious UA/IP/fingerprint | static fallback | disabled | drop or quarantine |
| `automation_suspected` | headless/browser automation without test token, impossible timing, repeated list summaries | static fallback or no-op telemetry | disabled for profile updates | quarantine, rate-limit evidence kept compactly |
| `human_likely` | consented browser with plausible timing and compatible profile | static HTML + local rerank after consent | enabled locally; server writes summaries only | accepted compact summaries/actions |
| `unknown` | insufficient evidence | static fallback until consent/profile is compatible | local-only if consented, conservative | accepted only after validation/rate limits; weak signals sampled/off by default |

## When personalization is disabled

Personalization is disabled when any of these is true:

- no consent;
- crawler/preview/monitor actor class;
- incompatible `profile_version`, `feature_schema_version`, `taxonomy_version` or legacy fields such as `negative_tags`;
- localStorage unavailable or corrupted;
- manifest schema mismatch;
- the selected trusted telemetry write path (`same_origin_endpoint_v1` or `supabase_rpc_ingest_v1`) is unavailable for trusted server mutation;
- request exceeds rate/shape limits or lands in quarantine.

For crawler/preview/monitor/bot/no-consent/schema-mismatch cases, disabled means: keep the static related order, do not mutate profile, do not send trusted telemetry, and do not block CTA/navigation. If only the selected write path is unavailable, trusted telemetry/server writes are disabled, but a consented compatible localStorage profile may still run local rerank as a local fallback; CTA/navigation must remain usable. Reference/demo code may record local debug-only events, but they must be marked as not trusted remote telemetry and must not reach Supabase/profile updates.

## Accepted telemetry payloads

MVP-0 accepts compact, append-only payloads only:

- `served_list_summary` — one row per meaningful list exposure, with `served_list_id`, `served_list_hash`, `surface`, `viewport_class`, `layout_mode`, `algorithm_id`, current event id and compact shown ids/scores/reason masks; full reason JSON is debug/sample-only;
- strong actions: `related_card_click`, `ticket_click`, `hide_event`, `not_interested`, `share`, `copy_link`;
- `session_summary` — periodic compact rollup, not raw scroll firehose.

Weak raw signals (`impression`, `dwell`, `quick_skip`, hover/focus diagnostics) are off or sampled with short retention until storage budgets and anti-bot gates are proven.

## Quarantine rules

Quarantine or drop payloads when:

- no consent or invalid consent/profile schema;
- actor is `crawler_verified`, `preview_bot`, `monitor`, `bot_likely` or `automation_suspected`;
- unknown `surface`, `algorithm_id`, `feature_schema_version` or event id not present in current manifest;
- `served_list_summary` repeats too often for the same `served_list_hash`;
- payload is too large, has too many shown ids, or includes raw descriptions/source text;
- timing is impossible: click before served list, excessive summaries per minute, many sessions per anon id;
- user agent/IP/session crosses rate limits.

Quarantined payloads must not update profiles, training sets, popularity or editorial quality metrics. Keep only compact evidence needed for abuse analysis.

## Rate limits and dedupe

Initial budgets:

- browser dedupe `served_list_summary` by `served_list_hash` for 10–30 minutes;
- write-path rate limit/quota by anon id + session id + IP prefix when available + actor class;
- cap one visible `event_detail_related` served summary per render list hash;
- cap strong actions per session/event;
- reject payloads above the documented schema size; write-path hard cap `8-16 KB`, accepted served-list target `<2 KB`, session summary target `<1-3 KB`.

## Write-path policy

Use a same-origin endpoint first by default:

```text
POST /api/personalization/summary
  read body with 8-16 KB hard cap
  validate schema/version/taxonomy/UUID ids/consent
  classify actor/trust; crawler_verified requires UA + reverse DNS/IP allowlist where available
  rate-limit/dedupe with bounded maps
  map client payload to compact DB row; never insert browser JSON directly
  write accepted compact row or tiny quarantine row with short DB timeout
  return 204/202 for accepted or normal dropped telemetry
```

Allowed Supabase RPC ingest mode:

```text
supabase.rpc('ingest_personalization_summary_v1', typed_compact_payload)
  validate UUID/schema/taxonomy/consent/enums/cardinality
  ignore client actor_class/trust_state/training flags
  dedupe/quota/storage-cap before accepted insert
  insert only compact arrays/bitmasks or tiny quarantine evidence
  return void/minimal success, no profiles/debug details
```

The browser must not use the Supabase secret key. Direct browser table writes are forbidden in production. Browser -> Supabase RPC is allowed only through a dedicated append-only ingest function (`supabase_rpc_ingest_v1`) that validates/compacts/dedupes/quotas before insert, keeps table grants closed to `anon`, revokes broad function execute rights, and never persists raw JSON as an accepted row.

Write-path test matrix before canary:

| Scenario | Required behavior |
| --- | --- |
| preview/social UA | drop with `204`, no accepted DB row |
| Googlebot-like UA without verification | never mark `crawler_verified` from UA alone |
| no consent or missing taxonomy/profile version | drop/quarantine, no profile update |
| oversized payload | reject/drop before accepted DB insert |
| repeated `served_list_hash` | dedupe before accepted DB insert |
| click before served list | quarantine/drop |
| many summaries/minute | rate-limit/quota with bounded memory or bounded SQL buckets |

## SEO and preview safety

- Static fallback must be genuine user-facing content, not a crawler-only fake.
- Do not cloak: crawlers and no-consent users get the same useful public event page and fallback related block.
- Link preview bots get OG/meta/image quickly; no client-side profile code should be required for previews.
- JSON-LD/Event schema and sitemap are generated from Fly SQLite canonical events, not from personalization telemetry.

## Abuse runbook

If telemetry spikes or bot quarantine grows:

1. freeze profile updates from suspicious actor classes;
2. keep static pages/CTA available;
3. lower accepted weak-signal sampling to zero;
4. inspect compact quarantine aggregates by actor class, endpoint, surface, anon/session/IP prefix;
5. rotate endpoint keys/rules if needed;
6. resume accepted telemetry only after bot rate falls and served-list/action ratios look plausible.
