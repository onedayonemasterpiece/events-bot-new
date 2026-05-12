# INC-2026-05-11-lecturer-name-and-title-dropped-from-description Lecturer name and title silently dropped from public description

Status: closed
Severity: sev3
Service: Smart Update G4 split-create / `rich_facts_extract` / public Telegraph event card
Opened: 2026-05-11
Closed: 2026-05-11
Owners: Smart Update / LLM prompts owner
Related incidents: `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (same prompt family / fact extraction stage)
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/smart-update-lollipop-gemma-4-eval.md`, `CHANGELOG.md`

## Summary

Production event `4759` («Влияние планировочных решений на качество жизни на примере старого и нового Калининграда», 2026-07-09 18:30, Историко-художественный музей) was published with a public Telegraph description that does not mention the lecturer’s name or job title, even though the source Telegram post (`t.me/kraftmarket39/219`) explicitly contains both: the post title `Лекция главного архитектора Калининграда:` and a dedicated `О спикере` section `Андрей Анисимов — главный архитектор Калининграда, работающий с вопросами архитектурного регулирования…`.

`event_source_fact` for event 4759 has no fact carrying either `Андрей Анисимов` or `главный архитектор`. The closest fact (id 56280) reads `Профессиональная позиция спикера находится на пересечении градостроительной политики, проектной практики и городской идентичности.` — an impersonal paraphrase that deletes both the name and the job title. The `split_description_writer` stage therefore could not include the lecturer because the upstream `rich_facts_extract` stage already discarded that information.

## User / Business Impact

- Public-facing Telegraph card of a flagship лекция-day event is missing the single most attendance-driving fact: that the lecture is read by the city’s chief architect.
- Reader-facing trust: a lecture at a serious municipal museum about urban planning loses its core authority signal in the bot’s output even though the source post structured the speaker prominently.
- Pattern risk: any lecture/discussion event whose source post mentions a notable speaker with a credential or professional title can lose that signal silently through the same path.

## Detection

- 2026-05-11 operator review of upcoming lecture cards flagged the missing speaker on event 4759.
- No alert fired — `rich_facts_extract` succeeded structurally (a non-empty `people_org_facts` array was produced), so the path was treated as healthy.

## Timeline

- 2026-05-09 22:49 Europe/Kaliningrad: event 4759 imported from `t.me/kraftmarket39/219` through the Telegram monitoring → Smart Update G4 split-create path. `rich_facts_extract` produced 16 facts; none preserved the speaker.
- 2026-05-09 22:49+: `split_description_writer` produced a description without the lecturer (impossible to include — facts already lost).
- 2026-05-11: operator review reported the regression.
- 2026-05-11: code-side prompt fix landed, regression tests added, incident recorded.

## Root Cause

1. The `rich_facts_extract` prompt at [smart_event_update.py:6973](smart_event_update.py#L6973) described the `people_org_facts` section only as `организаторы, институции, авторы, ведущие, исполнители, спикеры.` — generic enumeration without an explicit named-entity preservation requirement for the speaker’s job title or credentials.
2. With no rule against impersonal paraphrase, Gemma 4 chose to compress the entire `О спикере` section into `Профессиональная позиция спикера находится на пересечении…`, which is grounded prose but strips both the name (`Андрей Анисимов`) and the job title (`главный архитектор Калининграда`).
3. The writer (`split_description_writer`) faithfully refused to invent facts that are not in `facts_text_clean`, so the loss propagated to the public description.

## Contributing Factors

- The dedicated `О спикере` heading in the source post was not treated as a structural cue by the prompt; the model had no instruction to use a separate prominent section as a guarantee of a named fact.
- Downstream stages (split writer, infoblock) explicitly forbid hallucination, which is correct, but means any upstream fact loss is terminal.
- No regression fixture for "lecturer with explicit job title in a dedicated section" existed before this incident, so prompt tightening had no enforcement.

## Automation Contract

### Treat as regression guard when

- changing the prompt text of `rich_facts_extract` (smart_event_update.py around line 6967–6987);
- changing the prompt text of `split_description_writer` (smart_event_update.py around line 2332);
- changing the `people_org_facts` schema or the `_flatten_g4_rich_facts_payload` flatten order;
- routing of Smart Update `facts_extract` / `rich_facts_extract` to a different model.

### Affected surfaces

- code: `smart_event_update.py` (`rich_facts_extract` prompt; `_flatten_g4_rich_facts_payload`).
- prompt: the `people_org_facts` section bullet of the `rich_facts_extract` prompt.
- tests: `tests/test_smart_update_native_schema.py` (two new regression tests pinned to this incident).
- data: production event 4759 still needs a Smart Update re-run against the corrected prompt to regenerate its `event_source_fact` ledger and public description.

### Mandatory checks before closure or deploy

- `.venv/bin/pytest tests/test_smart_update_native_schema.py -q` → all green.
- Static check that the `rich_facts_extract` prompt still mentions `ИМЯ`, `ДОЛЖНОСТЬ`, `главный архитектор`, `О спикере`, and forbids `профессиональная позиция спикера` (covered by `test_g4_split_create_rich_facts_prompt_requires_named_speaker_with_title`).
- Behavioural check that a properly-named `people_org_facts` entry survives `_flatten_g4_rich_facts_payload` (covered by `test_g4_rich_facts_flatten_preserves_named_speaker_fact`).

### Required evidence

- deployed SHA: <to be filled after the fix is back-merged into `main` and deployed>.
- regression checks: `tests/test_smart_update_native_schema.py` 20 tests pass locally on `.venv/bin/pytest` 8.1.1.
- production data evidence: `event_source_fact` for re-imported 4759 must include an explicit `Лектор: Андрей Анисимов, главный архитектор Калининграда` (or equivalent named fact) and the new public Telegraph card must mention both name and title.

## Immediate Mitigation

- Prompt fix landed on this branch; new regression tests added.
- Production event 4759 stays on its old description until a manual re-run through Smart Update.

## Corrective Actions

- Extended the `people_org_facts` bullet of the `rich_facts_extract` prompt to:
  - require keeping ИМЯ together with ДОЛЖНОСТЬ / РЕГАЛИИ in one named fact for any explicit speaker / lecturer / host / guest / author;
  - explicitly forbid collapsing `<Имя>, <должность>` into impersonal `профессиональная позиция спикера…` / `спикер представит позицию…`;
  - treat dedicated `О спикере` / `Лектор:` / `Спикер:` / `Ведущий:` / `Автор:` sections as load-bearing, returning a named fact whenever they contain at least a name or a title.

## Follow-up Actions

- [ ] Owner: operator / no due date / re-import event 4759 via the standard Smart Update path to regenerate `event_source_fact` and `event.description` against the corrected prompt.
- [ ] Owner: Smart Update / next release / consider extending the same named-speaker rule into the legacy `facts_extract` prompt branch (used when `SMART_UPDATE_G4_SPLIT_CREATE` is off).

## Release And Closure Evidence

- deployed SHA: <to be filled at deploy time>.
- deploy path: regular `main` → fly deploy.
- regression checks: `tests/test_smart_update_native_schema.py` pass (`20 passed`).
- post-deploy verification: re-imported event 4759 must show the lecturer name and job title in the new public description.

## Prevention

- New regression tests pin the exact prompt rule against the 4759 source excerpt and the flatten path that carries `people_org_facts` into `facts_text_clean`.
- CHANGELOG entry under `[Unreleased]` documents the rule, the failing real-world source, and the required follow-up re-import.
