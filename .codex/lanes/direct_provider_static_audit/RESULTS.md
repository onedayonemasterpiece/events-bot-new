# R03 — Direct Google Provider Static Audit

## Outcome

Implemented a deterministic, offline repository audit that inventories Google AI
REST endpoint and provider SDK boundaries. It distinguishes the two approved
central gateway files, serialized central-gateway notebook snapshots, and
dependency-only SDK probes from narrowly allowlisted migration debt. Any other
finding, any debt line-shape change, or any occurrence above a debt rule's fixed
maximum fails the audit. Findings expose only detector tokens and locations, not
source lines, environment values, or API key values.

## Git evidence

- Base SHA: `86a0a8382f0dd9cbb644cd02540bf503e012332c`
- Validated implementation head SHA: `5aa384fc57dcbc4e415307fe7b199202591c0b51`
- Branch: `lane/direct-provider-static-audit`
- Worktree was clean before editing and was created directly from the assigned
  base SHA.
- No push was performed.

## Changed files

- `scripts/inspect/audit_google_ai_provider_paths.py`
- `tests/test_audit_google_ai_provider_paths.py`
- `.codex/lanes/direct_provider_static_audit/RESULTS.md`

## Commands and evidence

```text
python3 -m py_compile scripts/inspect/audit_google_ai_provider_paths.py \
  tests/test_audit_google_ai_provider_paths.py
```

Result: passed.

```text
python3 scripts/inspect/audit_google_ai_provider_paths.py
```

Result on the assigned base: `PASS`; `797` source files scanned;
`approved_gateway=7`, `approved_embedded_gateway=10`,
`approved_dependency_probe=3`, `allowlisted_debt=33`, `unapproved=0`,
`unreadable_files=0`.

```text
/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_audit_google_ai_provider_paths.py
```

Result: `5 passed in 0.17s`.

```text
python3 scripts/inspect/audit_google_ai_provider_paths.py --json
```

Result: valid deterministic JSON with `status=pass` and `unapproved=0`.

```text
git diff --cached --check
```

Result: passed before the implementation commit.

## Known allowlisted debt

The audit keeps these production-capable direct-provider paths visible as debt;
each allowance is constrained by exact path, detector, source-line regex, and a
maximum occurrence count:

- `event_identity.py` — opt-in direct REST embedding helper.
- `kaggle/GemmaKey2Probe/gemma_key2_probe.ipynb` — direct REST key probe.
- `kaggle/AfishaThumb/scripts/{camera,poster,scene,tour}_llm.py` — direct
  `google.genai` client construction and generation.
- `kaggle/UniversalFestivalParser/src/{enrich,reason}.py` — direct legacy
  `google.generativeai` configuration/model construction.
- `scripts/inspect/benchmark_lollipop_g4.py` — direct legacy SDK benchmark.
- `scripts/smoke_authorized_event_search_rpc.py` — direct REST embedding call.
- `supabase/functions/event-search/index.ts` — direct REST embedding and text
  generation calls.

Approved architecture is limited to `google_ai/client.py` and
`google_ai/interactions.py`; new files under `google_ai/` are not automatically
approved. Three import-only SDK availability probes are separately constrained
and do not construct provider clients.

## Risks / limitations

- This is a static signature audit. Deliberately obfuscated imports or provider
  hosts assembled from unrelated string fragments may evade a text signature;
  normal Python/TypeScript/Shell SDK imports, constructors, calls, and Google AI
  endpoint hosts are covered.
- Existing debt remains callable until migrated. The audit prevents silent debt
  widening but does not replace those paths with the central gateway.
- Serialized notebook gateway snapshots are approved only on the known
  assignment line prefixes. Provider references in other notebook cells fail.
- No network or provider calls were made during implementation or validation.
