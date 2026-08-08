# Current UI Resource Graph — event-presentation formats

## Outcome

Completed and delivered. The exact candidate graph now fixes the two desktop
Event Detail presentations, their different CTA placements, portrait/no-image
states, and large-versus-small media preview treatments as unresolved resource
evidence.

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Preserve horizontal/editorial and portrait/split desktop formats | Done | `event-presentation-formats.jsonl`; 96 editorial and 1,000 split routes; original-size human review. |
| R02 | Preserve distinct CTA blocks and media preview sizes | Done | Stacked editorial versus inline split CTA; primary frame, poster companion and small-preview records; poster-companion specimen reviewed. |
| R03 | Explain component/conflict extraction honestly | Done | 213 plane-qualified component rows / 107 logical component paths; 16 fragmentation candidates; all decisions `NOT_MERGED / unresolved`; heuristic taxonomy is explicitly partial. |
| R04 | Preserve color and typography evidence for later normalization | Done | `style-observations.jsonl` retains raw PostCSS/computed color and typography observations; 809 divergence candidates; no token or conflict resolution asserted. |
| R05 | Inspect visual evidence by eye | Done | All 46 Actions captures reviewed through contact sheets; five Event Detail pairs and the lab conflict opened at original size. |

## Delivery

- Feature PR: <https://github.com/onedayonemasterpiece/events-bot-new/pull/400>
- Budget correction PR: <https://github.com/onedayonemasterpiece/events-bot-new/pull/402>
- Successful Actions run: <https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31278123911>
- Artifact ID: `9027776108`
- Artifact digest:
  `sha256:2ebb59d1a14ec4134c192c86aad8a8295f77577990398543cfef38f570f75b6c`
- Receipt: `complete`; manifest SHA-256:
  `8f7de76208e2505aae39ebcfa258d55ef2a59bc76fbb7bf584ae99a05cec383c`
- Evidence size: 83,577,401 receipt bytes under a 90 MiB limit.

## Validation

```text
pytest tests/test_current_ui_resource_graph.py       24 passed
Node syntax checks                                  PASS
workflow YAML parse                                 PASS
git diff --check                                    PASS
Actions exact source/runtime/browser scan           PASS
15 required canonical files                         PASS
manifest output hashes and byte counts              PASS
46 screenshot physical dimensions                   PASS
candidate bearer/authorization scan                 PASS
human review of all 46 captures                      PASS WITH KNOWN LAB OBSERVATION
```

No `site/src`, Astro, CSS, UI, Penpot, token, component-contract,
defragmentation or normalization change was made.
