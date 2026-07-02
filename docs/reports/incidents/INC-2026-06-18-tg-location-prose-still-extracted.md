# INC-2026-06-18 Telegram Monitoring Still Extracts Prose As Location

Status: open
Severity: sev2
Service: Telegram Monitoring extraction / server import guard / public event inventory
Opened: 2026-06-18
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-16-tg-location-prose-cityjazz-recurrence`, `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-04-26-daily-location-fragments`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/llm/prompts.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-18, a fresh production audit found that the old `prose in location_name` failure family is still being produced by Telegram Monitoring. The current server-side deterministic guards prevented the sampled prose fragments from becoming active public event rows, but they did so by dropping the location and skipping otherwise event-like candidates as `invalid:missing_location`.

This is production-relevant because both forms are now confirmed:

- some candidates are silently lost after extraction produced a garbage venue;
- some fresh `@kldevents` publications did expose garbage / wrong location and media attachment errors.

The incident therefore remains a regression of the LLM-first venue extraction contract, the deterministic location-recovery contract, and the event-local media assignment contract.

## User / Business Impact

- Event-like Telegram posts can be lost from the public inventory when the extractor returns prose as a venue and no grounded fallback venue is available.
- Public `@kldevents` Telegram publication mostly avoided fresh prose-location leakage in the sampled window, but only because deterministic fail-closed guards rejected those candidates.
- Public `@kldevents` Telegram publication did not avoid all leakage: `https://t.me/kldevents/821` and `https://t.me/kldevents/811` exposed prose fragments in the location line.
- `https://t.me/kldevents/835` exposed a wrong venue binding: source poster says `Советский проспект 12, 809 студия`, but the event was normalized to `ИЦАЭ (в КГТУ)`.
- `https://t.me/kldevents/814` published unrelated posters from the same roundup/media context.
- Follow-up audit found `https://t.me/kldevents/698` / VK `https://vk.com/wall-231920894_3592` with `location_name='🤗Завтра'`; this was missed by the first investigation because the public Telegram/VK window around older same-day posts was not inspected.
- The system still spends runtime on repeated guardrail clean-up instead of producing source-grounded venue fields in the LLM-owned extraction stage.

2026-06-20 recurrence evidence: new public posts `https://t.me/kldevents/913` (`event.id=6162`) and `https://t.me/kldevents/914` (`event.id=6163`) again exposed short source-grounded prose/list fragments as public location lines: `📩 Зоосад с первого года (напомним` and the split list topic `о концертах` / `организованных в честь 80-летия Калининградской области;`. These posts prove that the previous guards still missed non-location emoji/list bullets and discussion-topic fragments unless the LLM venue-review stage is explicitly triggered for those shapes.

## Detection

- Operator request on 2026-06-18 to pull all `prose in location` incidents and audit current active events.
- Production runtime log mirror on Fly was available: `ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs/events-bot.log*`, 24 hourly files, retention `24` hours.
- Grep over the last ~24 hours found several Telegram import warnings with `dropping prose/person location_name after recovery`.
- Production DB check of the same source/message IDs showed `telegram_scanned_message.status='skipped'`, `events_extracted=1`, `events_imported=0`, mostly `invalid:missing_location`.

## Timeline

- 2026-06-17 06:10 UTC — `@kulturnaya_chaika/7856` posted; processed on 2026-06-18 05:18 UTC, one candidate extracted, skipped as past event after a prose-like `location_address` warning.
- 2026-06-17 07:49 UTC — `@open_fest/628` posted; processed on 2026-06-18 05:21 UTC, extracted location prose was dropped and the candidate was skipped as `invalid:missing_location`.
- 2026-06-17 09:03 UTC — `@terkatalk/5010` posted; processed on 2026-06-18 05:22 UTC, extracted location prose was dropped and the candidate was skipped as `invalid:missing_location`.
- 2026-06-17 15:15 UTC — `@kaliningradlibrary/2298` posted; processed on 2026-06-18 05:52 UTC, extracted location prose was dropped and the candidate was skipped as `invalid:missing_location`.
- 2026-06-17 15:45 UTC — `@kulturnaya_chaika/7865` posted; processed on 2026-06-18 05:52 UTC, extracted location prose was dropped and the candidate was skipped as `invalid:missing_location`.
- 2026-06-17 18:07 UTC — `@meowafisha/7677` posted; processed on 2026-06-18 05:53 UTC, extracted location prose was dropped and the candidate was skipped as `invalid:missing_location`.
- 2026-06-17 19:31 UTC — `@open_fest/631` posted; processed on 2026-06-18 06:02 UTC, extracted location prose was dropped and the candidate was skipped as `invalid:missing_location`.
- 2026-06-18 05:28-05:30 UTC — `@kldzoo/7534` produced multiple event candidates. Runtime logs show the server deterministic recovery replacing extracted/default `Калининградский зоопарк` with free-text grounded value `пожалуй, с приглашения на концерт🎻🙃`. Event `6133` was created and later published as `https://t.me/kldevents/811` with prose in `location_name`.
- 2026-06-18 05:32-05:40 UTC — `@kulturnaya_chaika/7860` produced event `6136`; later `telegraph.source_media` rehydrated 3 extra posters from the source/media context, including posters for unrelated events. It was published as `https://t.me/kldevents/814` with unrelated media.
- 2026-06-18 05:51 UTC — `@sobor39/6000` produced event `6138`. Runtime logs show deterministic replacement of extracted `Кафедральный собор` with free-text grounded value `как это чувствуется 🎹`. It was published as `https://t.me/kldevents/821`.
- 2026-06-18 05:55-05:57 UTC — `@meowafisha/7683` / linked `@sofit_models/126` produced event `6145`. Poster/source address is `Советский проспект 12, 809 студия`, but reference normalization bound it to `ИЦАЭ (в КГТУ), Советский пр-т 12`. It was published as `https://t.me/kldevents/835`.
- 2026-06-17 00:24 UTC — `@kaliningradartmuseum/8011` produced event `6089` with `location_name='🤗Завтра'`. Runtime logs for the import/publish window had already rotated out by the 2026-06-18 follow-up check; DB/source/public Telegram+VK evidence remained available. It was published as `https://t.me/kldevents/698` and live VK `https://vk.com/wall-231920894_3592`.
- 2026-06-18 UTC — current active/future event audit checked 402 active future rows. The first pass incorrectly underweighted user-visible `@kldevents` rows; follow-up inspection confirmed fresh public defects above.
- 2026-06-18 UTC — follow-up public-surface audit inspected the visible `@kldevents` window around posts 680–860 and the latest `klgdevents` VK wall items via API; it found the missed `/698` / VK `_3592` decorated temporal-location leak, plus adjacent non-location public-surface issues for separate follow-up (`Ленинскийский проспект 30`, duplicate `Янтарь холл, Ленина 11, Светлогорск, Ленина 11`, and a VK debug-copy marker).
- 2026-06-19 05:48 UTC — `event.id=6162` was published as `https://t.me/kldevents/913` with `location_name=📩 Зоосад с первого года (напомним` from source `@kaliningradartmuseum/8017`; Telegraph and managed VK `wall-231920894_3786` consumed the same bad location.
- 2026-06-19 05:58 UTC — `event.id=6163` was published as `https://t.me/kldevents/914` with `location_name=о концертах`, `location_address=организованных в честь 80-летия Калининградской области;` from source `@minkultturism_39/4826`; Telegraph and managed VK `wall-231920894_3787` consumed the same bad location.
- 2026-06-20 UTC — recurrence investigation confirmed runtime file mirror is enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs/events-bot.log*`), but the 2026-06-18 23:40 import window had rotated out; production DB/source rows and Telethon reads of `/913` and `/914` are the primary evidence.

## Root Cause

1. The LLM-owned Telegram Monitoring extraction still sometimes fills `location_name` / `location_address` with descriptive prose rather than a grounded venue or an empty value.
2. The server import path contains multiple deterministic recovery and guard layers (`_infer_location_from_text`, `_looks_like_location_prose_fragment`, person-name gate, reference normalization, known-venue grounding gate). These prevent many public leaks but can also turn an extraction miss into `invalid:missing_location` and lost recall.
3. A deterministic replacement branch can actively make the row worse: when `grounded_loc` from `_infer_location_from_text` differs from the extractor output and is considered grounded in text, it can replace a valid source/default venue with prose. This is confirmed for `@sobor39/6000` and `@kldzoo/7534`.
4. A wrong venue/address pair can survive when the LLM/extractor supplies a known venue name but source/OCR supplies a conflicting explicit address; this is confirmed for `@kldevents/835`: `ИЦАЭ (в КГТУ)` survived with `Советский проспект 12` although the curated ИЦАЭ reference is `Советский 1` and the poster says `809 студия`.
5. Event-local media assignment/rehydration can attach unrelated posters from a multi-event roundup/source context. This is confirmed for `@kldevents/814`.
6. The temporal-location guard was too literal: it matched `Завтра`, but not emoji/bullet-decorated forms such as `🤗Завтра`, so a temporal word copied by the LLM survived both Telegram import and Smart Update prose checks.
7. The historical closure criteria were too focused on preventing public garbage rows. They did not require evidence that the LLM extraction stopped producing prose-location candidates, nor that fail-closed skips and public `@kldevents`/`klgdevents` posts were audited as potentially lost or degraded events.
8. The LLM venue-review trigger still did not cover short non-location emoji/list bullets (`📩 ...`) or discussion-topic fragments (`о концертах`) when they were literally grounded in the source text. The server fail-closed gate had the same blind spot, so deterministic publication safety was again the last line of defense and it missed these short forms.

## Contributing Factors

- `_infer_location_from_text` is intentionally permissive and regex-driven; prior incidents showed it can re-select prose from free text after the LLM output was dropped.
- `_infer_location_from_text` is currently not only a fallback; in some branches it can overrule a valid extracted/default known venue.
- `normalise_event_location_from_reference` treats address matches as authoritative even when the source address contains a room/studio/sub-location that should not collapse to the known venue sharing the street address.
- Source-media rehydration is too broad for multi-event/roundup posts when poster assignment is not event-local.
- The final-stage prose gate is deterministic and syntactic; it cannot recover a valid venue when the source lacks an explicit venue or when source-level defaults/reference hints are missing.
- Observability logs warnings, but there is no production quality counter/alert that distinguishes "guard prevented bad public row" from "valid event was lost because location extraction failed".

## Confirmed User-Visible Examples From `@kldevents`

| Public post | Event | Defect | Evidence |
|---|---:|---|---|
| `https://t.me/kldevents/698` / `https://vk.com/wall-231920894_3592` | `6089` | Prose/temporal location: `location_name='🤗Завтра'`; expected source-owned default venue `Музей Изобразительных искусств, Ленинский проспект 83`. | Production DB row; `telegram_source.default_location`; public Telegram HTML and VK API showed the bad location line. |
| `https://t.me/kldevents/835` | `6145` | Wrong venue binding. Source/poster says `Советский проспект 12, 809 студия`; DB/public event says `ИЦАЭ (в КГТУ), Советский пр-т 12`. | Production DB event/source/poster rows; poster OCR contains `809 студия`. |
| `https://t.me/kldevents/814` | `6136` | Unrelated posters attached. Only the Letov/Chernyakov poster matches; other published source-media posters are for other events (`Моя Африка`, Pianissimo / Егор Кадников). | Runtime `telegraph.source_media: rehydrated event_id=6136 added_posters=3`; production `eventposter` rows. |
| `https://t.me/kldevents/728` | `5041` | Location complaint checked; current official sources say the 2026 staging is in the internal theatre courtyard at `пр-т Мира 4`, not Башня Врангеля. Not confirmed as location bug, but old `bashnya_vrangelya` media remain attached from parser/source media and should be treated as stale media risk. | `parser:dramteatr` source text and VK source `wall-132625599_19027` both say new площадка/internal courtyard, `Мира 4`; older image URLs include `bashnya_vrangelya`. |
| `https://t.me/kldevents/821` | `6138` | Prose in location: `location_name='как это чувствуется 🎹'`, `location_address='Остров Канта'`; expected canonical source/default venue `Кафедральный собор`. | Runtime line: `replaced unsupported extracted location source=sobor39 message_id=6000 ... extracted='Кафедральный собор' grounded='как это чувствуется 🎹'`. |
| `https://t.me/kldevents/811` | `6133` | Prose in location: `location_name='пожалуй, с приглашения на концерт🎻🙃'`, `location_address='Калининградский зоопарк'`; expected source/default `Калининградский зоопарк, пр-т Мира 26`. | Runtime lines: `replaced unsupported extracted location source=kldzoo message_id=7534 ... grounded='пожалуй, с приглашения на концерт🎻🙃'`; public DB row. |
| `https://t.me/kldevents/913` / `https://vk.com/wall-231920894_3786` | `6162` | Non-location emoji/list paragraph fragment in location: `location_name='📩 Зоосад с первого года (напомним'`; expected source-owned default `Музей Изобразительных искусств, Ленинский проспект 83`. | Telethon read of `/913`; production DB event/source rows; source `@kaliningradartmuseum/8017`; `telegram_source.default_location`. |
| `https://t.me/kldevents/914` / `https://vk.com/wall-231920894_3787` | `6163` | Discussion-topic bullet split across `location_name='о концертах'` and `location_address='организованных в честь 80-летия Калининградской области;'`; online-only livestream should use explicit online platform/page or fail closed, never prose. | Telethon read of `/914`; production DB event/source/poster OCR rows; source `@minkultturism_39/4826`. |

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring prompts/schema, `kaggle/TelegramMonitor/telegram_monitor.py`, server import candidate building, venue/default-location recovery, source default-location configuration, location reference normalization, or post-import quality gates.
- Auditing or changing any surface that can produce, sanitize, infer, normalize, drop, or publish `location_name`, `location_address`, or `city`.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompt/schema and OCR assignment.
- `source_parsing/telegram/handlers.py::_build_candidate`, `_infer_location_from_text`, `_infer_location_from_poster_payloads`, `_looks_like_location_prose_fragment`, `_looks_like_location_person_name_fragment`, final-stage prose gate, known-venue grounding gate.
- `docs/llm/prompts.md` and `docs/llm/request-guide.md` LLM-first venue semantics.
- Production runtime logs and `telegram_scanned_message` skip diagnostics.
- Public event inventory, `@kldevents` Telegram event publications, and managed VK `klgdevents` wall/postponed publications.

### Mandatory checks before closure or deploy

- Replay the June 18 source posts through the production Telegram import boundary and Smart Update on a prod snapshot/shadow DB.
- Positive controls: each sampled prose fragment must not persist into `event.location_name` or `event.location_address`.
- Recall controls: for event-like samples with sufficient source-grounded venue/default evidence, the candidate must import or merge instead of ending as `invalid:missing_location`.
- Negative controls: source posts that genuinely lack an offline venue must still fail closed and must not invent a default venue.
- Runtime evidence: last-24h logs after the fix must show no repeated `dropping prose/person location_name after recovery` for newly scanned posts, or must show explicit operator-visible quality reports for every fail-closed skip.
- Current/future active DB audit must show no descriptive-prose, temporal, or person-name `location_name` rows, and must specifically inspect recent `@kldevents` Telegram posts plus `klgdevents` VK wall posts for the last 24–48h, not only broad active-event heuristics.
- For source-owned venue channels such as `sobor39` and `kldzoo`, a deterministic free-text inference candidate must never overrule the source default or an extracted known venue unless an LLM-owned venue-review stage confirms the change with source-grounded evidence.
- Exact-address reference normalization must not bind to a canonical venue when the source address includes a room/studio/sub-location and the source does not name that canonical venue.
- Multi-event roundup media must be event-local: unrelated posters from the same source post must not be published with a different event.
- Short non-location emoji/list bullets such as `📩 Зоосад...`, discussion-topic fragments such as `о концертах`, and prose split between `location_name`/`location_address` must trigger LLM venue review and must not survive the server import fail-closed gate.

### Required evidence

- Minimal replay fixture with raw source text/OCR for the June 18 samples.
- Prod snapshot/shadow DB query showing event rows and `telegram_scanned_message` outcomes before and after.
- Runtime log excerpt from `/data/runtime_logs/events-bot.log*` or documented fallback evidence.
- Test/replay command output.
- If code changes are deployed, deployed SHA reachable from `origin/main` and post-deploy verification.

## Immediate Mitigation

Initial investigation-only pass did not mutate production. The repair pass is now in progress: confirmed public rows are being corrected/reposted after the root code change.

## Corrective Actions

- 2026-06-18: server import root fix prepared. `_infer_location_from_text` no longer turns `City, prose` into an override venue; deterministic `grounded_loc` replacements are restricted to strong event-local venue/address evidence; `address, studio` OCR/source lines can recover the studio instead of preserving a wrong known venue; VK hashtag-search pseudo-links are ignored as ticket links.
- 2026-06-18: Telegraph source-media rehydration prepared to skip source URLs that are shared by multiple event rows, preventing broad media reuse from multi-event/roundup posts.
- 2026-06-18: regression tests added for `kldzoo/7534`, `meowafisha/7683`, VK hashtag ticket leakage, and shared-source media rehydration.
- 2026-06-18: VK repair propagation follow-up prepared: `vk_source_hash` now includes date/time, location, ticket link, and photo URLs, because the previous hash only covered title/body text and could skip VK edits after DB location/media repair.
- 2026-06-18: follow-up LLM-first root fix prepared for emoji/bullet-decorated temporal fragments: Telegram Monitoring producer schema and the Gemma venue-review prompt now explicitly reject decorated temporal `location_name` values such as `🤗Завтра`, and the review trigger strips leading emoji/bullets before temporal checks. Telegram import and Smart Update keep the same normalization as fail-closed backup, not as the primary fix.
- 2026-06-18: public repair completed for the missed adjacent surface defects found after the weak first audit: `event 6091` / `https://t.me/kldevents/768` / `https://vk.com/wall-231920894_3593` now says `Кинотеатр «КАРО 7», Ленинский проспект 30`; `event 2759` / `https://t.me/kldevents/339` / `https://vk.com/wall-231920894_3746` now says `Янтарь холл, Ленина 11`; VK debug shadow copy `https://vk.com/wall-231920894_3747` was deleted after owner/debug-marker verification. Production backups: `codex_backup_20260618_public_surface_event_20260618_141429`, `codex_backup_20260618_public_surface_event_source_20260618_141429`, `codex_backup_20260618_public_surface_eventposter_20260618_141429`, `codex_backup_20260618_public_surface_joboutbox_20260618_141429`.
- 2026-06-18: LLM-first producer fix deployed from `96abe913` to Fly image `events-bot-new-wngqia:deployment-01KVDHKJ79KSNN06ENCA5TQC2S`; `/healthz` returned `ok=true`, `ready=true`, machine `683961db016e28` checks `1/1` passing.
- 2026-06-20: recurrence prevention added for `/913` and `/914`: Telegram Monitoring venue-review now routes non-location emoji/list bullets and discussion-topic fragments to the LLM-owned location repair path; the prompt forbids splitting one prose/list sentence across `location_name`/`location_address` and clarifies online-only livestream venue handling. Server import got the same fail-closed shape guard so these short forms cannot reach public Telegram/VK/Telegraph location fields if the remote LLM output is stale or fails.
- 2026-06-20: server import now preserves event-grounded `address, studio` evidence over ungrounded reference normalization, keeping the existing `/835` regression contract green while touching the location guard.

## Follow-up Actions

- [ ] Build a replay pack for `@open_fest/628`, `@terkatalk/5010`, `@kaliningradlibrary/2298`, `@kulturnaya_chaika/7865`, `@meowafisha/7677`, `@open_fest/631`, and `@kaliningradartmuseum/8011`.
- [x] Repair missed public-surface defects not explicitly pointed to in the previous operator message: `/768` typo, `/339` duplicated venue/address line, and `klgdevents` debug shadow post `_3747`.
- [x] Add replay controls for public defects: `@sobor39/6000` → `https://t.me/kldevents/821`, `@kldzoo/7534` → `https://t.me/kldevents/811`, `@meowafisha/7683` / `@sofit_models/126` → `https://t.me/kldevents/835`, `@kulturnaya_chaika/7860` → `https://t.me/kldevents/814`.
- [x] Tighten the Telegram Monitoring LLM extraction prompt/schema so prose/temporal-location outputs, including emoji-prefixed date words, non-location emoji/list bullets, and discussion-topic fragments, become empty/reviewed venue fields rather than garbage strings.
- [x] Replace deterministic venue overrule (`grounded_loc` over extracted/default known venue) with fail-closed or LLM-owned venue-review; deterministic inference may supply hints, not override venue semantics.
- [x] Tighten explicit `address, studio` handling so conflicting known-venue names do not survive when source/OCR names a sub-location.
- [x] Make source media rehydration fail closed for source URLs shared by multiple event rows before publication to `@kldevents`; full event-local OCR assignment remains follow-up.
- [ ] Add a production quality report/counter for `location_prose_dropped` split by `recovered_to_known_venue`, `recovered_to_default`, and `invalid_missing_location`.
- [ ] Audit source defaults for recurring venue-owned Telegram channels that still lack a grounded default location.
- [ ] Decide whether fail-closed missing-location candidates should enter operator review instead of being silently recorded as skipped.

## Release And Closure Evidence

- deployed SHA: `cc92afb3` (`fix(tg): stop prose location overrides`), pushed to `origin/main` and deployed to Fly app `events-bot-new-wngqia` on 2026-06-18.
- deploy path: clean linked worktree from `origin/main`, `flyctl deploy -a events-bot-new-wngqia --remote-only`; container verification confirmed `/app/source_parsing/telegram/handlers.py` contains `_location_override_candidate_ok` and `/app/main.py` contains the shared-source media rehydrate skip.
- regression checks:
  - `python3 -m py_compile source_parsing/telegram/handlers.py main.py tests/test_tg_candidate_location_grounding.py tests/test_telegram_link_inference.py tests/test_telegraph_side_effects.py` passed locally; full pytest was not run in the local worktree because dependencies/pytest were not installed in that environment.
  - Gemini 3 Pro review flagged expected recall tradeoffs: stricter regex fallback can drop novel uncued venues, and shared-source media rehydrate can drop shared lineup images. This is accepted for the emergency fix because the incident class is public wrong venue/media; follow-up should add LLM/event-local media review rather than reopen broad deterministic inference.
  - Production repair log showed `telegraph.source_media: skip shared multi-event source rehydrate` for `kldzoo/7534`, `kldzoo/7454`, and `kulturnaya_chaika/7860`.
- public repair evidence:
  - `https://t.me/kldevents/811` edited in place: DB now has `Калининградский зоопарк`, `пр-т Мира 26`, `ticket_link=NULL`.
  - `https://t.me/kldevents/821` edited in place: DB now has `Кафедральный собор`, `Остров Канта`, `ticket_link=NULL`.
  - `https://t.me/kldevents/835` edited in place: DB now has `809 студия`, `Советский проспект 12`; the curated reference still has ИЦАЭ at `Советский 1`.
  - `https://t.me/kldevents/814` was a 3-item bad album; messages `814`, `815`, `816` were deleted and replacement `https://t.me/kldevents/844` was published with only the matching Letov/Chernyakov poster.
  - `https://t.me/kldevents/728` was checked against current Dramteatr source text; the 2026 staging is in the internal theatre courtyard at `пр-т Мира 4`, so no location repair was applied in this pass.
- post-deploy DB verification: production event rows `6133`, `6136`, `6138`, `6145` have corrected locations; event `6136` has `photo_count=1` and only poster `11683` attached.
- follow-up repair for event `6089`: production DB now has `Музей Изобразительных искусств`, `Ленинский проспект 83`; bad `@kldevents/698` is no longer visible, replacement `https://t.me/kldevents/854` shows the corrected location with one matching poster; interim replacement `/852` and original `/698` are no longer visible, and VK live post `https://vk.com/wall-231920894_3592` was edited to the corrected location with one photo.

- 2026-06-20 recurrence prevention release:
  - commit `7c8a693f` (`fix(tg): review short prose location fragments`) pushed to `origin/main`;
  - deploy command: `flyctl deploy --remote-only --app events-bot-new-wngqia`;
  - deployed image `events-bot-new-wngqia:deployment-01KVK24ARHXNKDYN9NK3AEP3ZD`; Fly machine `683961db016e28`, version `1469`, checks `1 total, 1 passing`;
  - `/healthz` after deploy and repair: `ok=true`, `ready=true`, `db=ok`, `issues=[]`;
  - regression checks: `pytest -q tests/test_tg_candidate_location_grounding.py tests/test_tg_monitor_gemma4_contract.py` → `65 passed`; `py_compile source_parsing/telegram/handlers.py kaggle/TelegramMonitor/telegram_monitor.py`; `git diff --check`.
- 2026-06-20 public repair for new recurrence examples:
  - production backup tables: `codex_backup_event_location_repair_20260620_6162_6163`, `codex_backup_event_source_fact_location_repair_20260620_6162_6163`, `codex_backup_joboutbox_location_repair_20260620_6162_6163`;
  - `event 6162` now has `Музей Изобразительных искусств`, `Ленинский проспект 83`, `Калининград`; existing Telegram post `https://t.me/kldevents/913` was edited in place and Telethon verified the corrected `📍` line; Telegraph `https://telegra.ph/Revushchij-lev-poyushchij-los-06-18` contains the corrected location and no old location fragment; managed VK row now points to postponed `https://vk.com/wall-231920894_4023` with corrected `📍` line, while old `_3786` was absent from `wall.getById`;
  - `event 6163` now has `Онлайн`, empty address, `Калининград`; existing Telegram post `https://t.me/kldevents/914` was edited in place and Telethon verified `📍 Онлайн, #Калининград`; Telegraph location block is corrected; managed VK row now points to postponed `https://vk.com/wall-231920894_4024` with corrected `📍` line, while old `_3787` was absent from `wall.getById`.

## Prevention

Closure requires moving the primary fix back to the LLM-owned extraction contract and treating deterministic location gates as narrow guardrails/reporting, not as the main quality mechanism. Public-row absence alone is not sufficient closure evidence; recall impact from fail-closed skips must be measured.
