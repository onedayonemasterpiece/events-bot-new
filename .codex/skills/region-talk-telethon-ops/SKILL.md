---
name: region-talk-telethon-ops
description: Use for Events Bot / Region Talk Telegram Telethon discovery, source fast-check, keyword/hashtag search, exact post-link fetch, FloodWait/cooldown incidents, entity/access_hash cache handling, session-boundary decisions, or changes to CandidateReport/ImageDiagnostic/orchestrator Telegram behavior. Triggers include Telethon, FloodWait, ResolveUsernameRequest, get_entity, get_messages, iter_messages, access_hash, entity cache, TELEGRAM_AUTH_BUNDLE_DISCOVERY1/2/E2E/S22, Region Talk fast-check-KO, public web fallback, Telegram session safety, or questions about why Telegram metrics are not moving.
---

# Region Talk Telethon Ops

## Non-negotiables

- Do not print, commit, copy, summarize, or expose Telegram sessions, auth bundles, API hashes, tokens, `channel_id_private`, or `access_hash_private`.
- Do not use `TELEGRAM_AUTH_BUNDLE_E2E` in Kaggle kernels. It is only for local live E2E/human-client probes.
- Do not use `TELEGRAM_AUTH_BUNDLE_S22` for Region Talk unless the user explicitly overrides that exact run; S22 is reserved for production/remote monitoring.
- CandidateReport uses `TELEGRAM_AUTH_BUNDLE_DISCOVERY1`; ImageDiagnostic uses `TELEGRAM_AUTH_BUNDLE_DISCOVERY2`. Never run the same bundle concurrently in local and Kaggle contexts.
- Public `t.me/s` web fetch is not a production substitute for Telethon. In debugging, prefer disabling it so Telethon blockers are visible.
- Treat `FloodWaitError.seconds` as an instruction to stop until the cooldown expires, not as a retryable error.

## Mental model

Telethon cannot reliably operate on a public username alone. Many operations need an entity:

- human URL/username: `https://t.me/foo`, `foo`;
- private MTProto target: `InputPeerChannel(channel_id, access_hash)`.

`client.get_entity("foo")` usually performs `ResolveUsernameRequest`, which is scarce and FloodWait-prone. If `channel_id/access_hash` is already cached, use `InputPeerChannel` and avoid resolve.

## Required incident workflow

When a Region Talk Telegram run stalls, metrics do not move, or logs mention Telethon/FloodWait:

1. Inspect Kaggle/output artifacts and `region_talk_run_events_live.jsonl` before guessing.
2. Search for these events/strings: `ydb_candidate_links_loaded`, `telethon_exact_*`, `ResolveUsernameRequest`, `FloodWait`, `telegram_fetch_deferred_by_cooldown`, `fast_check_ko_*`, `keyword_discovery_*`, `telegram_public_web_*`.
3. Check `artifacts/region-talk/logs/telegram-request-ledger.jsonl` for method, decision, sleep seconds, cache hits, and cooldown.
4. Distinguish:
   - resolve failure: `ResolveUsernameRequest`, `get_entity`, no cached entity;
   - message fetch failure: `get_messages` after entity exists;
   - source-local search failure: `iter_messages(entity, search=...)`;
   - global search failure: `iter_messages(None, search=...)`.
5. Report exact method, exact FloodWait seconds, `cooldown_until`, bundle role, and whether public web fallback ran.

## Safe probe policy

Use local probes only when the user explicitly asks to test a human/E2E session. Keep probes tiny:

- 1 exact post or 1 global search query max, unless the user asks for more after seeing results.
- Sleep before calls; use large random pauses.
- Stop immediately on any FloodWait and record `seconds`/cooldown.
- Store probe scripts and outputs under `artifacts/codex/...`; never commit them.
- Redact secrets; account id/username is acceptable if needed for evidence.

Do not launch a Kaggle CandidateReport with E2E to “compare sessions”. Use local E2E only.

## Entity cache rules

When changing CandidateReport or related Telegram code:

- Save entity metadata privately whenever Telethon already gives a chat/channel object:
  - successful `resolve_entity`;
  - Telegram similar-channel result;
  - global keyword/hashtag search `msg.get_chat()`;
  - successful exact post-link fetch resolve.
- Store private fields only in state/YDB private payloads: `channel_id_private`, `access_hash_private`, `private_state_key`.
- Public XLSX/sheets may include `private_state_key`, but must never include raw `channel_id`/`access_hash`.
- Exact post-link fetch and fast-check must try cached `InputPeerChannel` before any `get_entity(username)`.
- If cache is missing, username resolve must be separately budgeted and cooldown-aware.

## Ordering rules for Region Talk pipeline

Do not let exact post-link fetch block all progress:

1. Prefer cached exact-link fetch.
2. Run fast-check on cached/resolved sources first.
3. Run history scans on cached/resolved sources.
4. Do global keyword/hashtag discovery conservatively; save entity cache from results.
5. Resolve unknown backlog in a separate scarce lane with strict limits.

If a FloodWait occurs, stop later Telegram phases in that run and persist cooldown. Continue only non-Telegram work such as BGE/vector/image/finalizer where safe.

## Code/config check list

For CandidateReport/orchestrator changes, verify both layers:

- local orchestrator action env contains intended values;
- `kaggle/execute_region_talk_candidate_report.py` serializes those values into `region_talk_run_config.json`;
- Kaggle log confirms the values had effect.

Important knobs:

- `REGION_TALK_TG_PUBLIC_WEB_FALLBACK=0` for Telethon debugging;
- `REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT`;
- exact post delays: `REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS/MAX_SECONDS`;
- `REGION_TALK_FAST_CHECK_KO_*`;
- YDB timeout/retry/circuit breaker keys.

## Verification

Run focused tests after changes:

```bash
python3 -m py_compile kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py \
  kaggle/execute_region_talk_candidate_report.py scripts/region_talk_orchestrator.py
python3 -m unittest \
  tests.test_region_talk_candidate_report.RegionTalkCandidateReportTests.test_telegram_governor_remember_entity_builds_cached_peer \
  tests.test_region_talk_candidate_report.RegionTalkCandidateReportTests.test_telegram_governor_floodwait_blocks_followup_calls \
  tests.test_region_talk_candidate_report.RegionTalkCandidateReportTests.test_runner_config_serializes_orchestrator_telethon_limits \
  tests.test_region_talk_orchestrator.RegionTalkOrchestratorTests.test_decision_prioritizes_notifier_finalizer_bge_image
```

If full tests fail due unrelated local dependency/test-env issues, report that separately and do not hide focused test status.
