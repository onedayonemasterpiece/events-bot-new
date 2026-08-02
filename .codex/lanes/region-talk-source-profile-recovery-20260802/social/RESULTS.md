# R02 social-capture results

## Lane identity

- Lane: `R02 / social-capture`
- Branch: `agent/region-talk-source-profile-recovery/social-capture`
- Base SHA: `ba8ab078ba9894ccd5810045b1b8787ecb29d743`
- Implementation head SHA: `4c66898e177dd11913689a40f4f295323bc46ce5`
- Scope: Telegram/VK acquisition adapters, deterministic capture contract,
  stable YDB capture persistence, and owned tests only.

## Requirement results

| Requirement | Result | Evidence |
|---|---|---|
| R02-01: default 50, bounded 30–80 recent posts and minimum 20 authored | Done | `capture_settings`, env defaults, bounds/readiness tests |
| R02-02: retain public description and pinned evidence | Done | Telegram `GetFullChannelRequest` adapter; VK `groups.getById` enrichment; metadata tests |
| R02-03: classify `authored/repost/service/ad_like`; exclude non-authored evidence | Done | deterministic classifier and exact 40/4/2/4 P0 fixture |
| R02-04: choose 8–16 diverse authored representatives | Done | recency/diversity selector; capture fails closed below three topic groups |
| R02-05: stable normalized fingerprint | Done | reversed-order/whitespace-equivalent 50-post P0 replay test |
| R02-06: unchanged capture makes zero profile LLM calls | Done | `capture_change_decision` and YDB unchanged-noop test; capture code makes no provider call |
| R02-07: reuse CandidateReport role-scoped Telegram session and limiter discipline | Done | adapter accepts existing `client` and `TelegramRequestGovernor`; no new `TelegramClient`/bundle; offline Google path audit clean |
| R02-08: no acknowledgement, reactions, or media downloads | Done | adapter uses only full-channel metadata, `iter_messages`, and exact pinned read; fake client asserts zero media calls |
| R02-09: separate stable-keyed YDB `source_profile_capture_item` with current read | Done | PK `source_profile_capture_item:<canonical_source_key>`; exact `SnapshotReadOnly` read before idempotent write |
| R02-10: never publish or promote | Done | capture records `autopublish_allowed=false`, `publication_effect=none`; no publication permission field/API |

## Capture contract

- Version: `region_talk_source_profile_capture.v1`
- Fingerprint version: `region_talk_source_profile_capture_fingerprint.v1`
- Stable identities: `telegram:<lowercase_handle>` / `vk:<screen_name>`
- Statuses: `ready`, `insufficient_authored_posts`,
  `insufficient_scanned_posts`, `insufficient_representative_posts`,
  `insufficient_representative_diversity`; adapters may project
  `capture_error` without writing/promoting anything.
- Durable text is limited to description, pinned evidence, and 8–16 selected
  excerpts. The 30–80-row manifest retains IDs, URLs, timestamps,
  classifications, and hashes, not every full post body.

## Commands and tests

1. P0 red run before implementation:

   ```text
   python3 -m unittest tests.test_region_talk_source_profile_capture -v
   Ran 6 tests; FAILED (errors=6: missing capture API/script)
   ```

2. Focused acceptance run in the repository Region Talk virtualenv:

   ```text
   /home/dev/.venvs/events-bot-region-talk/bin/pytest -q \
     tests/test_region_talk_source_profile_capture.py \
     tests/test_region_talk_candidate_report.py::RegionTalkCandidateReportTests::test_vk_wall_retries_resolved_signed_owner_after_domain_error \
     tests/test_region_talk_candidate_report.py::RegionTalkCandidateReportTests::test_runner_secret_names_only_include_selected_auth_bundle \
     tests/test_region_talk_candidate_report.py::RegionTalkCandidateReportTests::test_telegram_governor_humanlike_pacing_is_logged_and_observable \
     tests/test_region_talk_candidate_report.py::RegionTalkCandidateReportTests::test_telegram_governor_floodwait_blocks_followup_calls \
     tests/test_region_talk_candidate_report.py::RegionTalkCandidateReportTests::test_confirmed_external_blogger_evidence_admits_telegram_and_personal_vk \
     tests/test_region_talk_candidate_report.py::RegionTalkCandidateReportTests::test_confirmed_blogger_history_slots_are_balanced_between_telegram_and_vk
   ```

   Result: `13 passed in 1.76s`.

3. Syntax and whitespace gates:

   ```text
   python3 -m py_compile scripts/region_talk_source_profile_capture.py \
     kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py \
     tests/test_region_talk_source_profile_capture.py \
     tests/test_region_talk_candidate_report.py
   git diff --check
   ```

   Result: passed.

4. Google provider-path audit required for CandidateReport consumers:

   ```text
   python3 scripts/inspect/audit_google_ai_provider_paths.py
   ```

   Result: `PASS`, `unapproved=0`, `allowlisted_debt=0`.

5. Offline CLI smoke: 30 authored multi-topic rows produced
   `ready 30 30 12 telegram:demo`.

The system-Python full CandidateReport unittest attempt was not used as clean
acceptance evidence: that interpreter lacks `openpyxl` and its environment also
fails two pre-existing mocked LLM wrapper expectations. The changed VK adapter
regression from that attempt was isolated, updated for the intentional metadata
read, and passes in the focused virtualenv run above.

## Risks and blockers

- No live Telegram/VK/YDB execution was performed. This avoids competing for a
  role-scoped session and avoids production writes during a code lane. Live
  capture remains an integration/release task.
- VK profile capture adds one paced `groups.getById` read when `wall.get`
  metadata lacks a description. Telegram adds bounded metadata/history reads
  to the existing governor and caps source-profile captures per run.
- No LLM call is made by this lane. The downstream profile consumer must compare
  its last successful capture fingerprint and use the persisted change signal;
  it must not treat mere capture readiness as publication permission.
- Canonical docs and `CHANGELOG.md` were explicitly forbidden in this lane and
  remain the integrator's responsibility.

## Changed files

- `.env.example`
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `scripts/region_talk_source_profile_capture.py`
- `tests/test_region_talk_candidate_report.py`
- `tests/test_region_talk_source_profile_capture.py`
- `.codex/lanes/region-talk-source-profile-recovery-20260802/social/RESULTS.md`
