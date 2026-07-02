# INC-2026-06-29 Qtickets Structured Facts Lost To Poster OCR

Status: closed
Severity: sev2
Service: source parsing / Smart Update / public event fanout
Opened: 2026-06-29
Closed: 2026-06-29
Owners: events-bot operators
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-06-18-vk-title-shortlink-public-regression`, `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-06-16-vk-quality-duplicates-non-events`
Related docs: `docs/features/source-parsing/sources/qtickets/README.md`, `docs/llm/request-guide.md`, `docs/operations/incident-management.md`

## Summary

Qtickets event `https://kaliningrad.qtickets.events/241277-flava-intensive-valera-lera-voynits` was imported as event `6114` with the poster fragment title `VALERA`, missing venue address, missing source `end_date`, and unsupported generic master-class copy.

The source page itself exposes structured JSON-LD and visible HTML with canonical facts:

- title: `FLAVA INTENSIVE (VALERA & LERA VOYNITS)`;
- start: `2026-07-04T14:10:00+02:00`;
- end: `2026-07-07T21:30:00+02:00`;
- venue: `КОНЦЕПТ`;
- address: `ул. Ленинский проспект, 42Б, Калининград, Россия`;
- ticket price: `1800 RUB`.

Production DB kept only `source_text="Возраст: 0+"`, while the poster OCR said `VALERA ... LERA VOYNITS`. Smart Update/writer therefore had the wrong grounding priority and generated a public `VALERA` event.

## User / Business Impact

- Public `@kldevents` post `1410` and `klgdevents` VK post `4975` displayed the wrong title and omitted the address.
- Telegraph page `https://telegra.ph/Master-klass--KONCEPT-06-17` had the wrong title and generated unsupported prose.
- The canonical ticket source still had the correct facts, so the incident is repairable without creating a replacement event.

## Detection

Reported by operator on 2026-06-29 after comparing the public event with the Qtickets page.

## Timeline

- 2026-06-17 13:25 UTC — production event `6114` created from `parser:qtickets`; DB saved `source_text="Возраст: 0+"`.
- 2026-06-25 — poster OCR row contains `ocr_title="VALERA"` and OCR text with `VALERA` / `LERA VOYNITS` schedule fragments.
- 2026-06-27 06:01 UTC — public Telegram post `@kldevents/1410` contains title `VALERA`.
- 2026-06-28 20:40 UTC — managed VK post `https://vk.com/wall-231920894_4975` contains title `VALERA`.
- 2026-06-29 — operator reports wrong import; incident opened.

## Root Cause

1. `kaggle/ParseQtickets/parse_qtickets.py` did not carry JSON-LD `location.address` or `endDate` into `qtickets_events.json`.
2. `source_parsing/qtickets.py` and `TheatreEvent` had no fields for `location_address` / parser-provided `end_date`.
3. `source_parsing.handlers.add_new_event_via_queue()` built a structured LLM input with `Название/Дата/Площадка`, but then discarded that structured text when constructing `EventCandidate`: `source_text` became only `full_description`, which for this page was `Возраст: 0+`.
4. The downstream LLM writer/Smart Update saw high-trust source text with no canonical title or address and poster OCR with `VALERA`, so it chose the OCR fragment as the public title and invented generic master-class copy.

## Contributing Factors

- Qtickets descriptions can be empty even when JSON-LD contains strong facts.
- Poster OCR is useful evidence but can contain multiple schedule blocks; it is not the canonical title source when a ticket page has a structured `name`.
- The production import log for the original 2026-06-17 run is outside runtime retention; DB rows and source-page artifacts are the surviving canonical evidence.

## Automation Contract

### Treat as regression guard when

- changing Qtickets parser output, `TheatreEvent`, source parsing handlers, poster OCR handoff, Smart Update `EventCandidate` construction, or ticket-site public fanout.

### Affected surfaces

- `kaggle/ParseQtickets/parse_qtickets.py` JSON-LD extraction;
- `source_parsing/qtickets.py` output parser;
- `source_parsing/parser.py::TheatreEvent`;
- `source_parsing/handlers.py::_build_parser_source_text` and `add_new_event_via_queue`;
- public Telegram/VK/Telegraph event posts for parser-backed events.

### Mandatory checks before closure or deploy

- Unit/replay test proving the Qtickets fixture keeps canonical title, address, `end_date`, price, and URL in the parser boundary.
- LLM-first contract check: source text given to Smart Update includes ticket-page structured facts and a narrow Qtickets instruction that poster OCR is secondary when canonical page title exists.
- Negative control: a Qtickets event without address/end_date remains valid and is not forced into fabricated fields.
- Public repair smoke for event `6114`: Telegram, VK, Telegraph show `FLAVA INTENSIVE (VALERA & LERA VOYNITS)`, `КОНЦЕПТ`, address or source-grounded city/address line, direct ticket link, and no unsupported generic `VALERA` copy.
- `/healthz` production OK after deploy/repair.

### Required evidence

- `tests/replays/INC-2026-06-29-qtickets-structured-facts-lost/qtickets_events.json` committed.
- pytest output for Qtickets replay/unit tests.
- production DB diff / backup evidence for event `6114`.
- post-repair public Telegram/VK/Telegraph inspection output.
- deployed SHA and confirmation that fix is reachable from `origin/main` if production code is changed.

## Immediate Mitigation

- Production DB event `6114` was repaired in place from the Qtickets source facts.
- Telegraph page was rebuilt through `update_telegraph_event_page(6114, ...)`.
- Existing managed VK post `https://vk.com/wall-231920894_4975` was edited through the VK source-post sync helper with `append_text=False` so the old `VALERA` body was replaced, not appended.
- Existing Telegram post `@kldevents/1410` was edited in place; the standard event renderer and explicit premium emoji editor were used/verified after the repair.

## Corrective Actions

- Parse and preserve Qtickets JSON-LD `location.address` and `endDate` as structured source facts.
- Preserve structured parser source text as `EventCandidate.source_text` instead of replacing it with sparse descriptions.
- Add a narrow LLM-first Qtickets source contract to the parser prompt text: page title/venue/address/dates are canonical; poster OCR is secondary evidence and must not replace the page title when the page title exists.

## Follow-up Actions

- [ ] Audit recent `parser:qtickets` active/future events where `source_text` is very short and poster OCR title differs materially from page/title evidence.

## Release And Closure Evidence

- deployed SHA: `201e40a30119f4f4676f73bb86f400757a933d2a`
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --detach`
  - image: `registry.fly.io/events-bot-new-wngqia:deployment-01KWAS744XNB1K34N5HS5GDEBX`
  - machine version: `1543`
- regression checks:
  - `uv run --with pytest --with-requirements requirements.txt pytest -q tests/test_qtickets_structured_facts.py tests/test_source_parsing.py tests/test_ticket_sites_queue.py` → `35 passed`
  - replay fixture: `tests/replays/INC-2026-06-29-qtickets-structured-facts-lost/qtickets_events.json`
  - Smart Update shadow replay test: `test_qtickets_replay_keeps_structured_text_through_smart_update`
- post-deploy verification:
  - `/healthz` OK after deploy and after repair.
  - Production DB event `6114` now has title `FLAVA INTENSIVE (VALERA & LERA VOYNITS)`, `end_date=2026-07-07`, venue `КОНЦЕПТ`, address `ул. Ленинский проспект, 42Б`, source URL and ticket URL equal to the Qtickets page.
  - Telegram `@kldevents/1410` edited in place: full title, `4–7 июля`, `КОНЦЕПТ, Ленинский проспект 42Б`, direct Qtickets ticket link entity, Telegraph/Max/VK footer, and premium emoji editor result `edited=True`, `replacements=2`.
  - VK `https://vk.com/wall-231920894_4975` edited in place: full title, corrected venue/address, direct ticket shortlink `vk.cc/cYRjrS`, old `VALERA`-only body removed.
  - Telegraph `https://telegra.ph/Master-klass--KONCEPT-06-17` rebuilt: full title, corrected address, source count/footer present, no generated “работа с материалами” master-class boilerplate.

## Prevention

This incident is prevented by data-flow/prompt grounding, not by deterministic title rewriting. Deterministic code may extract source fields and preserve them, but it must not use broad regex/keyword rules to semantically rename events.
