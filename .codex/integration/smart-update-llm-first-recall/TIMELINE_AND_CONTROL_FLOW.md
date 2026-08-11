# P0/SEV-1 LLM-first recall — timeline and control-flow freeze

This is the required first artifact, frozen before implementation changes.
Production evidence was collected strictly read-only on 2026-08-11.

## Evidence identity

- Exact PR #494 head inspected: `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`.
- User-provided verified head: the same SHA.
- Current production release at observation: Fly v1969, image digest
  `sha256:32cc9ba7b6bb84af7eed0cd6d8f800451342e07d85395120d754c8d650ad6563`.
- `/app` has no `.git`; seven deployed runtime files matched Git commit
  `f66330f8af81d4b898d137d83356e77914dce90a` byte-for-byte.
- Full read-only first artifact:
  `artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/p0-llm-first-audit/TIMELINE_FIRST_ARTIFACT.md`,
  SHA-256
  `7361713debce0431f967757ccec43bc8ac4e7b827f1782dcdbb1015e519185c3`.
- Production mode: SQLite `mode=ro`, `query_only=1`, `quick_check=ok`; no
  production recovery, cursor mutation, LLM replay, deploy or write occurred.

## Proven chronology

| Date | Commit/release evidence | Proven change | Interpretation |
| --- | --- | --- | --- |
| 2025-09 | `b1e46df44`, `8ccffe71e`, `9c742ddc2` | `event_ts_hint`-based terminal pruning and cursor behavior | chronic pre-LLM loss predates the acute incident |
| 2025-10 | `94d50ca63`, `b4bed36ed` | `no_keywords`, `no_date`, `past_event`, `too_far` miss branches | chronic deterministic recall loss |
| 2026-04-11 | `bce860abd` | current `_vk_parse_preclassify`, prefilter flag/caller, cancellation bypass and `reject_reason` already present | proves presence by Apr 11; original deployment date may be earlier |
| 2026-04-14 | `841665558` | automatic queue retry/crash/rate-limit caps | technical failure could become terminal after three attempts |
| 2026-04-26 | `69ab6e8c9` | VK parse explicitly routed to Gemma 4 | explicit model override bypasses generic large-post/defender route |
| 2026-06-12/14 | `a1ec8e0`, `8aba058` | scheduler catchups/daytime slot | scheduled auto-import was not introduced on Aug 5 |
| 2026-07-02 | `deb3cb9a7` | long-caption OCR logistics-only budget | complete OCR could be omitted before the primary LLM |
| 2026-07-31 17:47–20:14Z | `f0394f2`, `1d2681c`, `de575df`, `7f4af3b`, `e888de5`, `7eb8b2d`, `8cf53f5`, `e04574a`; first retained later Fly v1824 22:05Z | shared limiter cutover, 14.5k request fit, prompt compaction, bounded sends/completion, thinking disable, key rotation | acute quota/capacity behavior changed before Aug 1; old image→commit identity is not provable from missing historical OCI labels |
| 2026-08-01 | PR #178/#180 merges | 10-photo/full-card expansion only behind schedule/cards regex | other carriers still receive 4 photos and 3/1200 OCR budget |
| 2026-08-04 16:26Z | `5082a502b`; Fly v1907/v1908 at 16:30/16:32Z | identity/source review and enforce merge paths | strongly correlated precursor to acute terminal loss; historical image SHA is not asserted |
| 2026-08-05 09:18Z | `86c4a62ac`; Fly v1910+ from 09:52Z | caller outcome boundary fails closed on unknown/review | plausible acute route converting identity uncertainty into lost downstream work |
| 2026-08-10 | PR #494 core/caller/recovery commits | typed Smart Update outcomes and durable retry/candidate state | correct foundation retained, but upstream semantic loss remained |

### Mandatory pickaxe results

All requested `git log -S <needle> --` queries were executed. Key first
reachable hits are:

| Needle | First/important hits |
| --- | --- |
| `prefilter_obvious_non_events` | `bce860abd`, `cdbb70130` |
| `_vk_parse_preclassify` | `bce860abd` |
| `VK_AUTO_IMPORT_PREFILTER_OBVIOUS_NON_EVENTS` | `bce860abd` |
| `_looks_like_cancellation_notice` | `bce860abd`, `cb4739892`, `bba67b5aa` |
| `reject_reason` | `bce860abd`, `cdbb70130`, `d2d8e57f2`, `d727cc4e2`, `cae9fe594` |
| `event_ts_hint IS NULL` | Sep-2025 commits plus `bce860abd` |
| `no_keywords`, `no_date`, `past_event`, `too_far` | Oct-2025 commits plus later guard changes |
| `reserve >12k TPM` | `bce860abd` |
| `EVENT_PARSE_GEMMA_TPM_RESERVATION_TARGET` | `1d2681cff`, `de575df56` |
| `enforce caller outcome boundary` | no content `-S` hit; exact subject commit `86c4a62ac` |

## Effective production configuration

- VK auto-import enabled with local slots
  `06:15,10:15,12:00,15:30,18:30`, limit 15.
- VK/Event parse model: `gemma-4-31b-it`; prefetch disabled.
- Normal/schedule photo caps: 4/10.
- `VK_AUTO_IMPORT_PREFILTER_OBVIOUS_NON_EVENTS` is unset, but deployed code
  defaults it to `1`; the prefilter is therefore effectively enabled.
- Both Smart Update identity gates are `enforce`.
- Six configured key envs map in the application limiter to six redacted quota
  scopes. This is not independent proof of six Google projects.
- Application registry value for Gemma 4 per scope: 15 RPM / 15,000 TPM /
  14,000 RPD / 1,000 extra reserve. Billing tier, spend cap and provider-side
  active limits are unavailable without authoritative AI Studio/project access.

## Acute discovery execution cliff

The aggregate Supabase snapshot is exact for retained snapshots; sampled misses
were not multiplied:

| Local day | Sources represented | Posts scanned | Matched |
| --- | ---: | ---: | ---: |
| Aug 1 | 121 | 84 | 35 |
| Aug 2 | 121 | 86 | 39 |
| Aug 3 | 121 | 15 | 5 |
| Aug 4 | 121 | 116 | 44 |
| Aug 5 | 33 | 7 | 6 |
| Aug 6 | 2 | 0 | 0 |
| Aug 7–9 | 0 snapshots | unknown | unknown |
| Aug 10 | 121 | 115 | 45 |

This proves a discovery-execution coverage collapse starting Aug 5 in addition
to older semantic false negatives. It is not an event-occurrence count.

## AS-IS graph A — VK discovery/monitoring

```text
configured vk_source
  -> vk_wall_since pagination / horizon / overlap / safety cap
  -> fetched post exists only in memory
  -> blank one-photo special-case -> vk_inbox pending
  -> history/keyword/date/event_ts_hint detectors
       -> past/too_far -> sampled miss telemetry + continue
       -> no_date/no_keywords -> sampled miss telemetry + continue
       -> narrow history rescue -> vk_inbox pending
  -> vk_inbox INSERT UNIQUE(group_id, post_id)
       -> no attachment manifest, payload/revision hash or typed reason
  -> cursor advances to max fetched post
```

Loss properties:

- The startup path bulk-rejects pending/skipped rows whose hint is NULL/old.
- `no_keywords`, `no_date`, `past_event`, and `too_far` execute before LLM and
  before durable SQLite carrier storage.
- A per-row persistence exception can still be crossed by cursor advancement.
- A page/safety cap has no durable continuation job.
- Edited revisions collide with `(group_id, post_id)` and cannot be proven as a
  new semantic revision.

## AS-IS graph B — VK auto-import

```text
vk_inbox claim
  -> recompute event_ts_hint
       -> NULL/old -> rejected before LLM
  -> fresh refetch (4 photos, or 10 via schedule regex)
       -> not_found -> rejected
       -> technical fetch error -> failed
  -> regex cancellation bypass
       -> direct lifecycle mutation + imported
       -> no match -> rejected
  -> attachment download/OCR (individual failures omitted silently)
  -> prefilter_obvious_non_events=True
       -> synthetic EventDraft(reject_reason), no primary LLM
  -> lossy OCR budget (ordinary 3 blocks / 1200 chars; long text logistics-only)
  -> primary Gemma parse with request fit/reservation
       -> 429 inline wait/defer; third defer -> failed
       -> timeout/schema/provider -> failed
       -> valid [] -> rejected without typed complete-evidence no-event
  -> deterministic post-LLM shadow classifier
       -> weak title/date/past/recap -> reject_reason
       -> suspicious venue -> child removed
       -> schedule/program collapse -> child loss
  -> surviving children -> Smart Update
       -> accepted typed outcome -> downstream effects
       -> retry -> deferred
       -> product exclusion -> rejected/partial
```

## Semantic-filter inventory and TO-BE contract

| Filter/path | Present by | Stage | Old action | Required contract |
| --- | --- | --- | --- | --- |
| `no_keywords` / `no_date` | Oct 2025 | pre-durable, pre-LLM | omit carrier, advance cursor | durable carrier + neutral hints + LLM/retry |
| `past_event` / `too_far` hint | Oct 2025 | pre-durable, pre-LLM | omit/reject carrier | priority/contradiction hint only |
| startup/claim `event_ts_hint` NULL/old | Sep 2025/Apr 2026 | pre-LLM | rejected | due/fair queue; never semantic eligibility |
| `_vk_parse_preclassify` | by Apr 2026 | pre-LLM | synthetic rejected draft | remove production API/path; diagnostic hints only |
| cancellation regex bypass | by Apr 2026 | pre-LLM | direct mutate or reject no-match | typed LLM lifecycle action; no-match durable retry; events independent |
| photo/card regex caps | Jul/Aug 2026 | pre-LLM evidence | omit attachments/OCR | complete evidence manifest; omission blocks negative terminal |
| long-text logistics OCR selector | Jul 2026 | pre-LLM evidence | omit non-logistics OCR | include every available block; reduce static prompt first |
| valid `[]` | by Apr 2026 | post-LLM | reject carrier | only typed, structured, complete-evidence `CONFIRMED_NO_EVENT` |
| weak title / missing regex date / past conflict / recap | by Apr 2026, later expanded | post-LLM | `reject_reason`, child loss | warning/grounded fallback/conditional verification |
| suspicious venue | by Apr 2026 | post-LLM | remove child | clear field or conditional verification |
| programme/session collapse | by Apr 2026 | post-LLM | remove siblings | preserve explicit occurrence children; exact replay only collapses exact identity |
| Smart Update deterministic eventness family | pre-PR #494 | post-positive upstream LLM | product skip/reject | objective schema validation or conditional LLM verification; technical uncertainty retry |
| rate-limit/timeout/provider/OCR/persist caps | by Apr 2026 | technical | terminal `failed` | release lease + durable typed retry/backoff |

## TO-BE graph

```text
fetch configured in-horizon post
  -> atomically persist raw carrier revision + attachments/hints
  -> advance cursor only after all fetched revisions are durable
  -> complete download/OCR evidence manifest
  -> exact successful revision replay? -> EXACT_REPLAY
  -> durable quota admission / fair due selection
  -> one primary typed complete-evidence LLM parse
       -> technical/schema/truncation/incomplete negative -> RETRY_SCHEDULED
       -> contradiction only -> conditional verifier on same configured route
       -> events/actions/no-event typed decision
  -> every event child -> Smart Update typed child outcome
  -> every lifecycle action -> typed resolver outcome
  -> balanced carrier terminal or RETRY_SCHEDULED
```

Regex/date/history signals remain evidence, priority hints, or conditional
verification triggers. They are never product verdicts.

