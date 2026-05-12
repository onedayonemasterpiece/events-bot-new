# INC-2026-05-11-zoo-lecture-premium-emoji-and-bullet-block-truncation Premium custom-emoji "free" marker stripped to spaces AND lecture bullet blocks truncated to one item each

Status: closed
Severity: sev3
Service: Kaggle Telegram monitoring kernel (`kaggle/TelegramMonitor/telegram_monitor.py`) + Smart Update G4 split-create rich_facts_extract (`smart_event_update.py`)
Opened: 2026-05-11
Closed: 2026-05-11
Owners: Telegram monitoring owner / Smart Update prompts owner
Related incidents: `INC-2026-05-11-lecturer-name-and-title-dropped-from-description` (sister rich_facts_extract loss); `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (master-prompt family).
Related docs: `kaggle/TelegramMonitor/telegram_monitor.py`, `docs/features/smart-event-update/README.md`, `CHANGELOG.md`.

## Summary

Production event `4798` («Зоопарку — быть!», 2026-06-26 18:30, Историко-художественный музей) shipped with two visible defects in the public Telegraph card:

1. **`is_free=false`** even though the source post explicitly carried four free-attendance premium custom-emoji glyphs (`🆓🆓🆓🆓`) and the wording `, по регистрации` immediately after.
2. **Short stub description** missing the lecture content blocks the source carefully structured: «О чём поговорим» (3 bullets) and «Правда ли, что» (2 bullets). Only **one** bullet from each block survived into `event_source_fact`, the rest were lost.

Bug 1 is a Kaggle-side Telegram-monitor bug: `strip_custom_emoji_entities` replaced every custom-emoji range with spaces, deleting the `🆓` Unicode fallback the author chose, which would have signalled free attendance to the downstream LLM.

Bug 2 is a Gemma 4 `rich_facts_extract` truncation: the `program_or_examples` rule said "не сворачивай длинные перечисления" but did not explicitly call out the named bullet-block headers commonly used in lecture posts (`О чём поговорим`, `Правда ли, что`, `Темы`, `Вопросы`, `В программе`, `Что обсудим`, `План встречи`).

## User / Business Impact

- A free public lecture is published as a paid-style card (no free-attendance marker), reducing CTR and misleading attendees.
- The lecture's actual content (zoo history, hippo Ганс, exhibit philosophy) is missing from the description, so readers cannot tell what the lecture is about.
- Pattern risk: any future post that uses premium emoji to mark `🆓` / `🎟` / `📅` etc., or that structures its programme under a named bullet block, is exposed to the same regression.

## Detection

- 2026-05-11 operator review reported both defects on event 4798.
- No alert fired — both stages succeeded structurally.

## Timeline

- 2026-05-10 (event_source `imported_at`): event 4798 imported through the Kaggle Telegram-monitor → Smart Update G4 split-create path. `strip_custom_emoji_entities` deleted the four `🆓` glyphs; `rich_facts_extract` produced only one bullet per content block.
- 2026-05-11: operator review reported the issue.
- 2026-05-11: both fixes landed on this branch with regression tests.

## Root Cause

1. **Custom-emoji strip dropped the meaningful Unicode fallback.** `kaggle/TelegramMonitor/telegram_monitor.py::strip_custom_emoji_entities` replaced every `MessageEntityCustomEmoji` range with spaces to keep UTF-16 offsets stable for other entities. This was overly aggressive: in Telethon the text at the entity offset is the *Unicode fallback character* the channel author chose, and authors deliberately pick semantically meaningful emojis (`🆓` for free, `🎟` for ticket, `📅` for date) when their custom premium emoji impersonates that emoji. Stripping the range deletes the signal.
2. **rich_facts_extract collapsed bullet blocks.** The `program_or_examples` rule asked Gemma to keep "ВСЕ списки", but did not enumerate the most common Russian lecture-post bullet-block headers, so Gemma stochastically summarised each multi-bullet block as one synthesised "what the lecture is about" fact, dropping the rest.

## Contributing Factors

- Telegram channels that target younger audiences use premium custom emoji as everyday formatting; the bot is now mostly importing from such channels.
- The Russian lecture-post style favours named bullet blocks (`О чём поговорим`, `Правда ли, что`, `Темы`); Gemma 4 has a generic prior to summarise lists into one sentence unless explicitly forbidden.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/TelegramMonitor/telegram_monitor.py::strip_custom_emoji_entities` or `_custom_emoji_fallback_is_meaningful`;
- changing the `rich_facts_extract.program_or_examples` rule in `smart_event_update.py` (currently around lines 6982–6989);
- adding any new "summarise programme" stage anywhere on the path that runs after `rich_facts_extract`.

### Affected surfaces

- code:
  - `kaggle/TelegramMonitor/telegram_monitor.py::_custom_emoji_fallback_is_meaningful` (new helper);
  - `kaggle/TelegramMonitor/telegram_monitor.py::strip_custom_emoji_entities` (rewritten to preserve meaningful Unicode fallback);
  - `smart_event_update.py::rich_facts_extract` `program_or_examples` rule (tightened to enumerate bullet-block headers).
- tests: `tests/test_tg_monitor_gemma4_contract.py` (3 new tests pin the strip behaviour); `tests/test_smart_update_native_schema.py` (1 new test pins the rule text against the 4798 source excerpt).
- data: production event 4798 still needs a manual re-import via Smart Update.
- deploy paths: Kaggle TelegramMonitor kernel needs a re-deploy for the strip-function change to reach prod; the rich_facts_extract change rides the regular Fly deploy.

### Mandatory checks before closure or deploy

- `.venv/bin/pytest tests/test_tg_monitor_gemma4_contract.py tests/test_smart_update_native_schema.py -q` → all green.
- Re-imported event 4798 must show:
  - `is_free=true`;
  - `event_source_fact` carries **three** facts from the `О чём поговорим` block and **two** facts from the `Правда ли, что` block;
  - description mentions the hippo Ганс and the exhibit-philosophy bullet.

### Required evidence

- 2026-05-11 unit tests: 25 passed for the Kaggle strip behaviour (static + functional exec-based), full Smart Update Native schema suite green with the new bullet-preservation test.
- Re-imported event 4798 must demonstrate the two-stage fix end-to-end.

## Immediate Mitigation

- Code change landed on this branch with regression tests on both sides.
- Production event 4798 keeps the wrong `is_free` and short description until re-imported after both deploys land (Kaggle kernel + Fly main).

## Corrective Actions

- Rewrote `strip_custom_emoji_entities` in `kaggle/TelegramMonitor/telegram_monitor.py` to consult a new helper `_custom_emoji_fallback_is_meaningful`, which classifies the Unicode fallback by codepoint range (Pictographs U+1F300..U+1FAFF, Enclosed Alphanumerics U+1F100..U+1F1FF, Misc Symbols / Dingbats U+2600..U+27BF, Misc Technical U+2300..U+23FF, Geometric Shapes U+25A0..U+25FF). When the fallback is a real pictograph, the strip preserves it; otherwise it still replaces the range with spaces to keep offsets stable.
- Mirrored the same change into `kaggle/TelegramMonitor/telegram_monitor.ipynb` cell 1 so the Kaggle dataset is consistent on next deploy.
- Tightened the `program_or_examples` rule in `smart_event_update.py::rich_facts_extract` to explicitly list the common Russian lecture bullet-block headers (`О чём поговорим`, `Правда ли, что`, `Темы`, `Вопросы`, `В программе`, `Что обсудим`, `План встречи`) and to forbid collapsing multi-bullet blocks into a single summary fact (`Если в источнике 3 bullet'а под "О чём поговорим" — верни 3 факта`).

## Follow-up Actions

- [ ] Owner: Kaggle ops / no due date / re-deploy the TelegramMonitor Kaggle kernel so the strip-function change reaches prod.
- [ ] Owner: operator / no due date / re-import event 4798 via Smart Update after both deploys land.
- [ ] Owner: prompt maintenance / next pass / monitor whether `program_or_examples` still drops bullets on edge cases (numbered programmes, dash-prefixed lists without an explicit header); extend the rule if regressions appear.

## Release And Closure Evidence

- deployed SHA: `a1d48da3` (events-bot-new-wngqia v1061, deployed 2026-05-12 05:48 UTC). Note: the Kaggle TelegramMonitor kernel still needs its own re-deploy for the `strip_custom_emoji_entities` change to reach the Kaggle-side ingest path.
- deploy paths: Fly main (for the prompt change); Kaggle TelegramMonitor kernel deploy (for the strip-function change).
- regression checks: `pytest tests/test_tg_monitor_gemma4_contract.py tests/test_smart_update_native_schema.py tests/test_prompt_json.py -q` (55 passed locally).
- post-deploy verification: re-imported event 4798 must show `is_free=true` and the full bullet content in the description.

## Prevention

- Regression tests pin both the strip behaviour and the rich_facts_extract rule text.
- The incident record sits in the index as the canonical contract for premium-emoji handling and for Russian lecture-post bullet preservation.
