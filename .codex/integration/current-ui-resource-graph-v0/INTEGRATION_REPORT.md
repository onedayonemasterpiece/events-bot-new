# Current UI Resource Graph v0 integration report

## Scope and boundary

This integration reconstructs the current Astro UI as evidence. It does not
modify `site/src`, CSS, runtime UI, Kaggle publishing, Penpot, tokens, variants,
patterns, component contracts, or normalization/defragmentation decisions.
Every candidate in the graph remains `NOT_MERGED` / `unresolved`.

## Exact evidence planes

- Latest checked immutable Kaggle candidate source:
  `ef7aa62e45c60f7a12da6160f490719c0721ec03`.
- Candidate build:
  `production-secret-20260808T144842-5472a382`.
- Candidate run:
  `static-site:production-secret-20260808T144842-5472a382:acbd40ef5203`.
- Candidate snapshot: `snapshot-20260808T124842-4786ac53bc`.
- Candidate manifest SHA-256:
  `d615f6e447dc8c6ae3b876bf4a99123d1c85afee55276c26645f020b26074322`.
- Candidate Astro / Node: `6.4.8` / `22.12.0`.
- Separate current public-root prelaunch source:
  `5a9d804438377f65fe4b26bd7019e73626529864`.
- Public-root release: `prelaunch-main-31263560430-1`; exact observed root
  HTML SHA-256:
  `1c31504d10d9ec66c7fa84ad52c94e6019a741f0ee01826f219578963e0ea21e`.

The public root is not described as the full production corpus. Its runtime and
source evidence remain a separate plane from the checked Kaggle candidate.

## Local full-run acceptance

The ignored local snapshot
`artifacts/current-ui-resource-graph/snapshot-20260808T124842-4786ac53bc-final-local-v3`
completed with manifest SHA-256
`8d4861f17433d235bd44afad4790e55e4e6ababca9a4920e791d276d36d80724`.

- runtime observations: 1,267 (1,266 candidate HTML routes + public root);
- source records: 492 (244 candidate + 248 public-root source plane);
- page families: 24;
- observed/candidate UI families: 20 / 20;
- source and computed style observations: 34,985;
- fragmentation/candidate graph rows: 20 / 20;
- screenshot index rows: 46 (40 captured, 6 explicit uncaptured);
- screenshots: exact `390x844` and `1728x900`, indexed SHA-256 verified;
- all manifest output hashes verified;
- candidate bearer URL byte-scan: absent from every artifact file;
- receipt: `complete`, output bytes: 59,290,055.

Coverage reconciliation reports 14 `FOUND`, 3 `MISSING`, 5 `DISCOVERED`
and no `AMBIGUOUS` hypotheses. In particular, Exhibitions, For Me/personal
feed, Interest Clubs and marker-backed Hero-talk are found. Editorial
Collections, Legal documents and Hero-talk page-end remain explicitly missing;
no synthetic archetype was created. Artifacts, closed-poster, focus-group,
labs/preview-special and unusual pages were discovered outside the supplied
hypothesis list.

## Validation

```text
node --check scripts/current_ui_resource_graph/decode.mjs        PASS
node --check scripts/current_ui_resource_graph/graph-lib.mjs     PASS
workflow YAML parse                                              PASS
git diff --check                                                 PASS
pytest -q tests/test_current_ui_resource_graph.py                17 passed
full exact source/runtime/browser scan                            PASS
manifest shard hash/byte verification                            PASS
screenshot physical dimension/SHA verification                   PASS (40)
secret redaction byte scan                                       PASS
manual screenshot review: Home mobile/desktop, Exhibitions       PASS
  mobile, Weekend desktop
```

## Delivery gate

The remaining delivery step is to land the decoder workflow on the repository
default branch and run it there, because the acceptance contract requires an
actual GitHub Actions artifact. Record the Actions run and uploaded artifact in
the task handoff; the committed graph generator, not the ignored local corpus,
is the durable source.
