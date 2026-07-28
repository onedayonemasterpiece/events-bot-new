# Lane result: relay_control

## Scope

- Requirements owned: **R04** control UI and status surface, **R05** minimal relay protocol.
- Implementation commit: `2f3bee1b` (`feat(autopresenter): add local relay and control UI`).
- No M0, site, agent, canonical docs, or changelog files were changed.

## Delivered

- One `aiohttp` process, default `127.0.0.1:8787`.
- In-memory, single-session relay with one live-agent lease; no database and no Socket.IO.
- Monotonic command sequence, generated or caller-provided command ID, 30 s delivery TTL, and idempotent command replay.
- Cooperative HTTP long poll and explicit agent status/command acknowledgments.
- Responsive, keyboard-accessible `/control/` UI with:
  - `Запустить «Завтра»`
  - `Стоп`
  - `Сброс`
  - machine statuses `disconnected`, `idle`, `running`, `stopping`, `completed`, `error`.

## HTTP contract

- `GET /healthz`
- `GET /control/`
- `GET /api/state`
- `POST /api/commands` — `{ "action": "run|stop|reset", "command_id": "optional-id" }`
- `GET /api/commands/next?agent_id=...&after_seq=...&wait_ms=...`
- `POST /api/commands/{command_id}/ack` — `{ "agent_id": "...", "sequence": 1, "status": "...", "detail": "..." }`
- `POST /api/state/agent` — `{ "agent_id": "...", "status": "...", "detail": "..." }`

Command response fields are `id`, `sequence`, `action`, `issued_at`, `expires_at`, and remaining `ttl_ms`. Long polls are capped at 25 seconds. A second live agent gets HTTP 409; it may take over after the first lease times out.

## Verification

Commands run from the lane worktree:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python \
  -m unittest discover -s tools/autopresenter/relay/tests -v
/home/dev/.codex/venvs/events-bot-new/bin/python \
  -m py_compile tools/autopresenter/relay/server.py \
  tools/autopresenter/relay/tests/test_server.py
git diff --check
```

Result: **6/6 tests passed**, Python compilation passed, diff check passed.

Manual process smoke:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python \
  tools/autopresenter/relay/server.py --port 18787
curl http://127.0.0.1:18787/healthz
curl http://127.0.0.1:18787/control/
curl -H 'Content-Type: application/json' \
  -d '{"action":"run","command_id":"manual-smoke"}' \
  http://127.0.0.1:18787/api/commands
```

Result: health JSON OK, control HTTP 200 with required labels, run command accepted with sequence `1` and a 30 s TTL.

## Integration / risks

- The integrator should launch this with a Python environment containing repo requirement `aiohttp>=3.9.5`.
- The agent must poll continuously, POST `idle` at startup, send the exact command `sequence` in acknowledgments, and acknowledge stop as `idle` (optionally with a stopped detail).
- This relay intentionally has no persistence, authentication, rooms, or multi-agent arbitration. Binding remains loopback by default; exposing it beyond localhost is out of scope.
- TTL controls queue delivery, not scenario runtime: acknowledgments remain valid after the delivery TTL once an agent has received the command.
