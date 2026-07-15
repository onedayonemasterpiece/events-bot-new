# Integration report — typed briefing artist arrivals and unusual events

## Integration identity

- Base: `a9b829d6a865bcf08bc267aa5360103298e461fc`.
- Integration branch: `integration/typed-briefing-artist-unusual-20260715`.
- Data worker head: `5bb0a05d4f1633a96021e69b64cecee0720bab6f`.
- Public UI source: `f7d99384fc7ce308399b5e047ca1ca3ce51737ce`.
- Evidence head before closure bookkeeping: `32664fcd64226beedefe198e65f765c960c01587`.

## Lane integration

| Lane | Requirements | Result |
|---|---|---|
| data-product | R01, R02, R03, R06 | Worker commits integrated as `ad768d56 -> a4b0c52e` and `5bb0a05d -> e4b5dd2a`; patch IDs and selected trees match. |
| visual-consultant | R04 | Three read-only exact-state Gemini Pro gates recorded: initial fail, conditional pass, final crop recheck `PUBLISH PASS`. |
| ui-integrator | R05 | UI correction in `f7d99384`; immutable public build and durable evidence in `32664fcd`; Telegram evidence `77–82`. |
| merge-reviewer | R01–R06 | Independent audit in `.codex/lanes/merge-reviewer/RESULTS.md`; implementation/public-lab recommendation `PASS`. |

## Requirement closure

| ID | Status | Boundary |
|---|---|---|
| R01 | Done | XLSX converted reproducibly into a provenance-preserving canonical JSON snapshot. |
| R02 | Done | Safe matching/locality contract exists; it is not yet a live classifier and all seed locality values remain `unknown`. |
| R03 | Done | Automatic visiting-artist digest is specified as a future function; runtime generation/publication is not implemented. |
| R04 | Done | The exact rejected visual states received a fresh critical Pro-class review and post-fix acceptance. |
| R05 | Done | Corrected isolated lab is tested, published, and delivered to the Telegram review topic. |
| R06 | Done | LLM-first unusual-event detection contract exists; detector, baseline corpus, and shadow evaluation remain future work. |

## Verification evidence

- Artist converter unit tests: `4/4` pass; workbook SHA verified; committed JSON reproduced byte-for-byte with `--check`.
- Isolated build/check: pass; strict allowlist contains six expected files.
- Briefing Playwright suite: `11/11` pass.
- Public build `preview-20260715t2005-briefing-lab-f7d99384`: page/wordmark/wide-O HTTP 200 and `noindex,nofollow,noarchive`.
- Final agy crop gate: blocker closed, no P0/P1 findings, `PUBLISH PASS`.
- Telegram topic `6`: messages `77–82` verified; final reread found no newer incoming comments.

## Release boundary

This closure approves only the immutable research lab and its documentation. It does not approve a production-homepage rollout, a production non-local artist classifier, an automatic visiting-artist publisher, or automatic unusual-event publication.
