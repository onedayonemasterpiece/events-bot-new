# Telegram document sending through private Events MCP

Prepared: 2026-08-11

Repository: `onedayonemasterpiece/events-bot-new`

Status: **implemented and merge-ready; archived handoff evidence**.

Production activation and the real ChatGPT APK -> Telegram Saved Messages
acceptance are still pending. This archive status means the code and offline
contracts are merge-ready; it does not claim deployment or live closure.

The owner-supplied handoff has been unpacked here so the coding agent can read the contracts and executable prototype directly. The checked-in Python prototype and tests are byte-identical to the supplied package.

The implementation agent must begin from a fresh `origin/main`, record the actual base SHA, and re-check every cited code location because production and public `main` may move independently.

## Canonical current documentation

Use [`docs/operations/private-events-mcp.md`](../../../operations/private-events-mcp.md)
for the implemented contract, configuration, exact-main rollout, smoke and live
acceptance. The files below remain immutable design/handoff evidence and are not
the active implementation TODO. Do not delete them when rollout completes.

## Archived execution handoff

Use [`CODE_AGENT_EXECUTION_PROMPT.md`](CODE_AGENT_EXECUTION_PROMPT.md) for the complete one-pass task through implementation, review, merge, exact-main deployment, production activation, connector refresh, and real ChatGPT-file-to-Telegram acceptance.

[`CODE_AGENT_TASK.md`](CODE_AGENT_TASK.md) is the detailed implementation contract from the original design package. Its former exclusion of production activation is superseded by the execution prompt above.

## Materials

- `architecture.md` — consolidated design and exact implementation map.
- `acceptance-matrix.md` — required positive, negative, security, regression, and live checks.
- `CODE_AGENT_TASK.md` — detailed code/contract implementation task.
- `CODE_AGENT_EXECUTION_PROMPT.md` — canonical end-to-end execution prompt requested by the owner.
- `proposed/private_events_mcp/document_policy.py` — executable document-validation policy prototype.
- `proposed/tests/test_document_policy.py` — prototype tests.
- `prototype-test-results.txt` — prototype test receipt.

## Scope selected

- Telegram only;
- `send_message` only;
- one document plus optional caption/rich entities;
- ChatGPT `fileParams` ingress only;
- structurally validated APK, PDF, ZIP, UTF-8 text/JSON and Office files;
- 48 MiB default document limit, 64 MiB hard cap;
- independent fail-closed feature switch, source-disabled by default;
- no VK documents, multiple/mixed media, arbitrary URL/path/base64 ingress, raw provider methods, or antivirus claims.

## Prototype status

The standalone policy prototype was re-run with:

```bash
PYTHONPATH=proposed python -m pytest -q proposed/tests/test_document_policy.py
```

Result:

```text
10 passed
```

## Remaining production gate

The merge-ready implementation remains source-disabled by default. Production
closure requires an exact merged-main deploy, scoped configuration/preflight,
refresh of the existing `eventsBot` connection in place, an actual ChatGPT
`fileParams` APK send/read-back, a negative probe and the narrow file-send
off/on rollback probe. Record that evidence in the canonical runbook/incident;
do not rewrite or delete this source handoff. The separate active-backlog index
entry must be removed by the owner of `docs/backlog/README.md` during integration.
