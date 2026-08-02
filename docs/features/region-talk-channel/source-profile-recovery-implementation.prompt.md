# Задание кодовому агенту: Region Talk source-profile recovery и перегенерация

Работай в репозитории `onedayonemasterpiece/events-bot-new`.

## Перед началом

1. Обнови `main`.
2. Прочитай:
   - `AGENTS.md`;
   - `docs/features/region-talk-channel/AGENTS.md`;
   - `docs/features/region-talk-channel/source-profile-recovery-plan.md`;
   - `docs/features/region-talk-channel/source-onboarding-profile.md`;
   - `docs/features/region-talk-channel/external-publication-research.prompt.txt`;
   - `docs/features/region-talk-channel/publisher-profile-research.prompt.txt`;
   - `docs/features/region-talk-channel/publisher-profile-enrichment.schema.json`;
   - `docs/features/region-talk-channel/publisher-profile-enrichment-results.md`;
   - все `region-talk-publisher-profile-enrichment-*-2026-08-02.json`;
   - current Region Talk publication queue, YDB schema and external intake docs.
3. Не меняй исторические `region-talk-external-research-result-*.json`.
4. Не импортируй publisher sidecars существующим candidate importer.
5. Не включай autopublish и не повышай `manual_review_required`.
6. Любой Google/Gemini вызов — только через общий atomic limiter.
7. Не используй Telegram auth role конкурентно.

## Цель

Восстановить реальный source onboarding до Writer:

- social sources: description + pinned + bounded 30–80 posts, default 50;
- publisher/journal: durable profile from official source evidence;
- paragraph 1: content hook first, compact source onboarding second;
- paragraph 2: material-specific details;
- deterministic source-aware CTA;
- guarded profile import to YDB;
- backfill only current unpublished candidates;
- re-adjudicate the local-correspondent RG candidate before regeneration.

## P0 — сначала напиши regression tests

Зафиксируй текущую ошибку тестами:

1. one current social post is insufficient for a ready reusable profile;
2. 50-post capture yields a stable fingerprint and diverse representative excerpts;
3. description + pinned evidence are retained;
4. repost/service/ad rows do not count as authored evidence;
5. unchanged capture causes zero profile LLM calls;
6. article Writer cannot start without required publisher profile dimensions;
7. hook must cite content evidence, not source-profile evidence;
8. URL inside either paragraph fails;
9. truncated sentence before footer fails;
10. federal brand + `reg-szfo` + local byline cannot pass externality;
11. profile sidecar replay is idempotent;
12. candidate correction cannot mutate live candidate without a strong re-read and explicit review path.

## P1 — social source capture

Implement a bounded acquisition path using the existing role-scoped Telegram/VK readers.

Required behavior:

- new env defaults:
  - `REGION_TALK_SOURCE_PROFILE_SCAN_POSTS=50`;
  - min 30, max 80;
  - `REGION_TALK_SOURCE_PROFILE_MIN_AUTHORED_POSTS=20`;
- fetch public description;
- fetch pinned post when available;
- scan latest posts without read acknowledgement/reactions/media download;
- classify deterministic `authored|repost|service|ad_like`;
- normalize/deduplicate;
- preserve source IDs/URLs/timestamps;
- build deterministic topic/format/entity digest;
- select 8–16 representative excerpts by diversity and recency;
- store capture fingerprint/version/status;
- one profile LLM call only when fingerprint changes.

Do not run LLM per post. Do not add a vector database for MVP.

## P2 — change readiness/order

In `scripts/region_talk_publication_finalizer.py` and backfill:

- replace the current one-excerpt sufficiency rule;
- do not cap the upstream scan to the current eight memory fragments;
- keep selected excerpts bounded but derive them from a larger capture;
- prepare/reuse source profile before public-copy Writer;
- use separate profile budget accounting;
- make `needs_source_profile` explicit;
- preserve accepted candidate verdict monotonically, but do not produce generic public copy from a missing profile.

## P3 — publisher profile importer

Add a dedicated script, for example:

`scripts/region_talk_publisher_profile_import.py`

Contract:

- validate `publisher-profile-enrichment.schema.json`;
- accept only `region_talk_publisher_profile_enrichment.v1`;
- canonicalize source keys/domains;
- compute exact input/profile/evidence hashes;
- dry-run by default;
- explicit `--execute`;
- strong live YDB re-read;
- idempotent replay;
- conflict writes nothing;
- upsert reusable publisher profile rows separately from `external_publication_intake_item`;
- candidate_corrections enter an explicit review queue, not an automatic decision write;
- sanitized report/receipt;
- no publication permission.

Add a guarded GitHub Action for files matching only:

`docs/features/region-talk-channel/region-talk-publisher-profile-enrichment-*.json`

Do not broaden the existing external-candidate auto-import glob.

The Action must:

1. check exact trusted `main`;
2. validate before Yandex authentication;
3. use OIDC/WIF;
4. import sequentially;
5. fail on conflict/incomplete read;
6. upload a short-lived sanitized receipt;
7. never call publisher/notifier/autopublish.

## P4 — merge publisher evidence from future research

Extend the normal external research importer/finalizer so the richer `publisher.*` evidence requested by `external-publication-research.prompt.txt` can update the reusable publisher profile.

Rules:

- article candidate identity and publisher profile identity are separate;
- same domain/profile across articles is merged by evidence fingerprint;
- primary article cannot overwrite a richer source profile;
- conflicting scope/locality fails closed;
- existing candidate replay may still enrich the publisher profile;
- source profile update alone does not reopen/publish a candidate.

## P5 — Writer vNext

Change Strategy/Writer/Critic contract.

Paragraph 1:

- first sentence = 45–110-char grounded hook from current article/post;
- second sentence = compact source value from reusable profile.

Paragraph 2:

- 1–2 concrete details;
- third-person ownership;
- no URL;
- no CTA/metatext;
- no exhaustive summary.

Deterministic CTA:

- social author/channel/blog;
- article on outlet;
- journal article;
- safe generic fallback.

Ban:

- `Источник публикации` as the only public CTA;
- `материал представляет ценность`;
- `публикация позволяет`;
- `оригинал доступен`;
- unsupported `известный|ведущий|главный|крупнейший|обязательный`;
- incomplete final sentence.

Revalidate the rendered caption after every repair/normalization.

Rev all relevant fingerprints/versions so old drafts cannot replay.

## P6 — apply supplied profiles

Use the three supplied sidecars from 2026-08-02.

Expected:

- Archi.ru profile imported/reused;
- «Крестьяноведение» profile imported/reused;
- RG brand profile stored only as mixed/candidate-specific;
- exact RG article queued for externality re-adjudication.

For the RG article:

`https://rg.ru/2025/09/16/reg-szfo/kak-segodnia-vosstanavlivaiut-istoricheskie-doma-i-pamiatniki-v-rossijskom-eksklave.html`

The live decision must fail closed because:

- URL section is `reg-szfo`;
- byline says `Денис Гонтарь (Калининградская область)`;
- canonical product policy treats local editions/correspondents as regional.

Do not regenerate it as an external candidate unless fresh evidence reverses that conclusion and the review path records it explicitly.

## P7 — backfill and operator delivery

After merge/deploy and only through existing locks/role-scoped credentials:

1. dry-run publisher profile import;
2. execute guarded import;
3. read back exact profile rows;
4. build social profiles for current unpublished confirmed candidates;
5. re-adjudicate candidate corrections;
6. exclude published/stale/conflicting rows;
7. regenerate copy with new profile/hook contract;
8. deliver new revisions to the existing operator chat;
9. archive old reaction projection according to the existing migration contract;
10. require fresh reactions;
11. do not publish to target channels.

## P8 — evidence and tests

Required tests:

- focused unit tests for capture/profile/import/Writer/renderer;
- full `pytest -q tests/test_region_talk*.py`;
- JSON Schema validation of all supplied sidecars;
- exact replay test;
- exact conflict test;
- zero-provider unchanged-profile test;
- 20-message copy-quality audit:
  - >=18 concrete hooks;
  - 20 grounded source sentences;
  - 0 paragraph URLs;
  - 0 incomplete sentences;
  - 0 unsupported prestige claims;
  - one source-aware CTA each;
  - RG case blocked/reviewed, not external clean.

Record:

- branch, commit, PR;
- test counts;
- dry-run receipt;
- executed profile import receipt;
- YDB readback;
- regeneration counts/statuses;
- operator message IDs;
- zero autopublish proof;
- docs and CHANGELOG update.

## Files likely to change

At minimum inspect/update:

- `scripts/region_talk_publication_finalizer.py`
- `scripts/region_talk_publication_draft_backfill.py`
- `scripts/region_talk_goal_notify.py`
- `scripts/region_talk_external_publication_import.py`
- role-scoped Telegram/VK source readers
- new publisher profile importer
- new guarded Action
- Region Talk YDB/docs/contracts
- `tests/test_region_talk*.py`
- `CHANGELOG.md`

## Stop conditions

Fail closed and report instead of guessing when:

- description/archive cannot be read;
- authored sample < required minimum;
- profile evidence conflicts;
- local edition/byline is ambiguous;
- YDB strong read incomplete;
- profile replay maps to another source key;
- provider limiter unavailable;
- Telegram session may be active elsewhere;
- candidate is already published.

## Handoff

Open a separate PR. The PR description must distinguish:

- implemented code;
- validated data;
- live import;
- live regeneration;
- operator delivery;
- production publication effect (`none` expected).

Do not call the task complete at code merge alone: complete the bounded profile import and unpublished candidate backfill, or document the exact external blocker with evidence.
