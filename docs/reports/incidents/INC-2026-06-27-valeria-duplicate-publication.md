# INC-2026-06-27-valeria-duplicate-publication Deterministic title guard overruled a correct LLM duplicate match for Валерия

Status: closed
Severity: sev2
Service: Smart Update dedup / managed Telegram and VK event publication
Opened: 2026-06-27
Closed: 2026-06-27
Owners: Smart Update / publication pipeline owner
Related incidents: `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge`, `INC-2026-05-05-event-quality-regression`
Related docs: `docs/features/smart-event-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-06-27 production published two managed VK posts, and started publishing Telegram event-flow posts, for the same real event: Валерия's 2026-07-01 concert at Янтарь холл. Smart Update's LLM matching correctly selected existing event `5152` for the new Telegram source `https://t.me/yantarholl/4727` with confidence `1.00`, but a deterministic title-only guard overruled the match as `unrelated_titles` because it did not treat `Валерия` and `Концерт Валерии` as related Russian title forms. A new active event `6460` was created and both rows entered managed publication queues.

## User / Business Impact

- Readers saw duplicate posts about the same concert in `klgdevents` VK on the morning of 2026-06-27.
- The Telegram event-flow queue had two active rows for the same event; one row published to `@kldevents`, and the second was still pending when the incident was investigated.
- The product contract “one real event → one canonical public card/post” was violated, making the channel look spammy and reducing trust in event freshness.

## Detection

- Operator reported seeing two Telegram/VK morning publications for Валерия.
- Production DB and runtime evidence confirmed duplicate active rows `5152` and `6460`.
- Runtime logs showed the smoking gun:
  - `smart_update.match type=llm match_id=5152 confidence=1.00 reason=Полное совпадение артиста, даты и площадки...`
  - immediately followed by `smart_update.match_overruled reason=unrelated_titles ... candidate_title=Валерия existing_id=5152 existing_title=Концерт Валерии`.

## Timeline

- 2026-05-20 00:10 UTC — Telegram source `https://t.me/yantarholl/4584` created event `5152` (`Концерт Валерии`) from the postponement notice for 2026-07-01.
- 2026-06-26 23:51 UTC — Telegram source `https://t.me/yantarholl/4727` entered Smart Update as `Валерия`, 2026-07-01 19:00, Янтарь холл.
- 2026-06-26 23:51 UTC — LLM matched it to event `5152` with confidence `1.00`; deterministic `unrelated_titles` veto cancelled the merge.
- 2026-06-26 23:52 UTC — Smart Update created duplicate event `6460` and enqueued `ics_publish`, `telegraph_build`, `tg_ics_post`, `vk_sync`, and `tg_event_publish`.
- 2026-06-27 06:00 UTC — VK managed post for event `6460` was published as live wall id `4681` (DB initially stored postponed id `4658`).
- 2026-06-27 07:45 UTC — VK managed post for event `5152` was published as live wall id `4691` (DB initially stored postponed id `4671`).
- 2026-06-27 09:08 UTC — Telegram event-flow post for `5152` published to `https://t.me/c/3954607218/1431`; event `6460` remained pending.
- 2026-06-27 — prevention patch prepared: high-confidence LLM matches with non-conflicting hard anchors are no longer vetoed by title-only guards, and Russian one-character inflection title matching covers `Валерия`/`Валерии`.

## Root Cause

1. The LLM-first duplicate decision was correct but was treated as advisory: a deterministic title-token guard retained final veto power.
2. `_titles_look_related()` lacked a narrow Russian inflection tolerance, so `Валерия` and `Концерт Валерии` shared no exact non-stopword token.
3. The single-candidate sanity guard and the later `match_overruled` block both allowed title-only unrelatedness to override a high-confidence LLM match even when date, venue and time had no factual conflict.
4. Publication fanout works per active `event` row; once the duplicate row existed, Telegram/VK jobs legitimately tried to publish both.

## Contributing Factors

- Event `5152` came from a postponement notice and had weaker data (empty time, generic ticket URL), while event `6460` came from a richer current announcement. This made a merge/update the intended behavior, but made the duplicate posts visibly different.
- Managed VK postponed ids can differ from final live wall ids; verification had to use authenticated `wall.get` rather than only DB URLs.

## Automation Contract

### Treat as regression guard when

- changing `_titles_look_related`, `_single_candidate_auto_match_ok`, `_llm_match_event`, `_llm_match_or_create_bundle`, the post-LLM `unrelated_titles` overrule, or Smart Update shortlist construction;
- changing managed `tg_event_publish` / `vk_sync` fanout for imported events;
- repairing or auditing duplicate active future event rows.

### Affected surfaces

- `smart_event_update.py` LLM duplicate matching and deterministic guard rails;
- production SQLite rows `event.id IN (5152, 6460)`, related `event_source`, `eventposter`, `joboutbox`;
- Telegraph pages `https://telegra.ph/Koncert-Valerii-05-20` and `https://telegra.ph/Valeriya-06-26-93`;
- VK managed posts `wall-231920894_4681` and `wall-231920894_4691`;
- Telegram event-flow post `https://t.me/c/3954607218/1431` and pending `tg_event_publish:6460`.

### Mandatory checks before closure or deploy

- Unit coverage that `Валерия` and `Концерт Валерии` are related title forms.
- Smart Update regression proving a high-confidence LLM match with same date/venue and no explicit time conflict is merged, not created, even when the title-wrapper guard would otherwise reject it.
- `py_compile smart_event_update.py`.
- Production repair must leave only one active canonical event for Валерия on 2026-07-01, no pending duplicate Telegram publication, and no duplicate managed VK post.
- Release-governance checks: clean hotfix worktree from current `origin/main`, commit pushed, deployed SHA recorded, and fix returned to `origin/main`.

### Required evidence

- Test output for the targeted Smart Update regression.
- Runtime log excerpt with the original overrule.
- Before/after production DB row evidence for events `5152` and `6460`.
- VK API evidence after duplicate cleanup.
- Telegram/Telegraph verification after duplicate cleanup.
- Deployed SHA reachable from `origin/main`.

## Immediate Mitigation

Completed on 2026-06-27: duplicate event `6460` was archived as `lifecycle_status=duplicate`, its pending `tg_event_publish` job was stopped, richer source facts/time/ticket/link were merged into canonical event `5152`, stale May 29 linked rows `4241`/`5048` were marked postponed, and the lower-quality managed VK duplicate was deleted.

## Corrective Actions

- `_titles_look_related()` now handles narrow Russian inflection differences for meaningful title tokens, covering `Валерия` vs `Концерт Валерии` without broad semantic regex matching.
- High-confidence LLM matches (`confidence >= 0.95`) with matching/non-conflicting hard anchors (date, venue, time) are protected from title-only deterministic vetoes in both the single-candidate sanity path and the later `unrelated_titles` overrule.
- Added targeted regression tests for the Валерия failure class.
- Production data repair left one active canonical row (`5152`) with `time=19:00`, direct Янтарь-холл ticket URL, `source_vk_post_url=https://vk.com/wall-231920894_4681`, and `tg_event_post_url=https://t.me/c/3954607218/1431`; duplicate row `6460` is non-active and has no pending TG fanout.

## Follow-up Actions

- [ ] Smart Update owner / no due date / add a publication-time duplicate cluster gate so active rows that still slip through cannot both publish to TG/VK without operator review.
- [ ] Smart Update owner / no due date / add structured metrics for `match_overruled` by reason and confidence so title-only high-confidence vetoes alert immediately.

## Release And Closure Evidence

- deployed SHA: `5ad5b4417cf0e32d33ddce248e1501c1d68373de` (`incident/2026-06-27-valeria-dedup`, deployed image `events-bot-new-wngqia:deployment-01KW47CPNPWD1N847Q81S07SSJ`; docs-only closure evidence committed afterward).
- deploy path: `flyctl deploy --remote-only --app events-bot-new-wngqia` from a clean linked worktree created from `origin/main`.
- regression checks:
  - `.venv/bin/python -m pytest tests/test_smart_event_update_duplicate_guards.py::test_title_related_allows_russian_inflection_artist_title tests/test_smart_event_update_duplicate_guards.py::test_high_confidence_llm_match_is_not_vetoed_by_title_wrapper_drift -q` -> `2 passed`.
  - `.venv/bin/python -m py_compile smart_event_update.py`.
  - Release-governance branch audit: current branch from `origin/main`; stale `release/*`/`hotfix/*` branches with ahead counts were recorded, none blocked this incident deploy.
- production repair evidence:
  - backup tables: `codex_backup_event_20260627_valeria_dedup`, `codex_backup_event_source_20260627_valeria_dedup`, `codex_backup_eventposter_20260627_valeria_dedup`, `codex_backup_joboutbox_20260627_valeria_dedup`, plus stale-date backup `codex_backup_event_20260627_valeria_stale_dates`.
  - DB verification: only event `5152` remains active for Валерия on 2026-07-01; event `6460` is `duplicate`, `tg_event_publish:6460` is stopped with `duplicate_archived_to_event_5152`, and stale May 29 rows `4241`/`5048` are `postponed`.
  - VK API verification: before cleanup both live managed posts existed (`4681`, `4691`); `wall.delete owner_id=-231920894 post_id=4691` returned `1`; after cleanup only `4681` remained.
  - Telegram verification: Telethon read of `https://t.me/c/3954607218/1431` shows edited message `2026-06-27T09:38:59+00:00` with `🎤 Валерия`, `1 июля 19:00`, Янтарь холл, and rich current-source description.
  - Telegraph verification: `https://telegra.ph/Koncert-Valerii-05-20` shows `Валерия`, `1 июля в 19:00`, Янтарь холл, direct ticket block, and no stale `Другие даты: 29 мая`.
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`; Fly machine `683961db016e28`, version `1502`, checks `1 passing`.

## Prevention

This incident record is a mandatory regression contract for any future change that lets deterministic title/anchor checks override LLM duplicate decisions. Deterministic code may veto hard factual conflicts, but must not be the semantic owner for harmless title wording or Russian inflection drift.
