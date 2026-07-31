# L03-db results

## Scope

- Lane: `L03-db`
- Requirement coverage: operational persistence supporting R03/R05 integration
- Base SHA: `cf76303d97d665ece2df1cc8afa69121c952f26b`
- Implementation head SHA: `6e3fea981b06ef828d5d52b71f5abbcd275dc8fd`
- Production access: none

## Delivered

- Added provider-neutral `festival_web_research_run` persistence with edition
  candidates, a unique immutable input fingerprint, candidate/quality/artifact
  payloads, review and lease lifecycle fields.
- Added `festival_web_research_lane_run` attempts with separate
  `provider_state` and `semantic_state`, unique request/attempt identities,
  provider interaction references, usage/validation/candidate payloads and
  error/lifecycle fields.
- Added grouped queue membership in `festival_web_research_item`, unique per
  parent run and queue item.
- Added source snapshot/evidence metadata in `festival_web_research_source`,
  unique per lane attempt and stable source ID.
- Added indexes for run/review scheduling, provider and semantic recovery,
  queue disposition and source lookup.
- Kept SQLModel `create_all`, Alembic and additive SQLite bootstrap definitions
  aligned. No Festival/Event/public graph or mutation was added.

## Changed files

- `models.py`
- `db.py`
- `alembic/versions/20260731_festival_web_research.py`
- `tests/test_festival_web_research_models.py`
- `.codex/lanes/L03-db/RESULTS.md`

## Verification evidence

Shared interpreter:
`/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python`.

```text
PYTHONPATH=/home/dev/projects/events-bot-new-antigravity-runtime \
  .../.venv/bin/python -m pytest -q tests/test_festival_web_research_models.py
..                                                                       [100%]
2 passed in 0.64s
```

The two tests independently verify SQLModel `create_all` and `Database.init`
bootstrap tables, exact columns, named indexes/unique constraints, database
defaults, JSON defaults and duplicate input-fingerprint rejection on temporary
SQLite databases.

```text
PYTHONPATH=/home/dev/projects/events-bot-new-antigravity-runtime \
  .../.venv/bin/python -m pytest -q tests/test_db.py
......                                                                   [100%]
6 passed in 1.92s
```

Additional commands completed successfully:

```text
.../.venv/bin/python -m py_compile models.py db.py \
  alembic/versions/20260731_festival_web_research.py \
  tests/test_festival_web_research_models.py
git diff --check
```

A local AST chain check confirmed:
`20260731_festival_web_research -> 20260726_festival_calendar_items` and no
duplicate revision identifier. The repository already contains three older,
unrelated legacy Alembic heads; this migration extends the latest festival
calendar/main chain rather than changing those heads.

## Risks and integration notes

- The shared test interpreter does not contain the Alembic runtime package, so
  the migration module was syntax/chain checked rather than executed by
  Alembic. Both schema-producing runtime paths (`create_all` and SQLite
  bootstrap) were executed and compared in tests. Integration should run the
  migration in its normal Alembic-enabled environment.
- `updated_at` is database-defaulted on creation but intentionally has no
  database trigger; repository mutations must set it explicitly, matching the
  surrounding application pattern.
- Schema values are forward-compatible strings. Initial `collect_only`
  enforcement remains a service/coordinator responsibility and no public apply
  behavior exists in this lane.
- `request_uid` identifies an idempotent lane attempt; provider-owned
  accounting remains authoritative for each individual remote interaction.
- `black` and `ruff` are not installed in the shared interpreter. Syntax,
  whitespace and targeted regression checks passed.
