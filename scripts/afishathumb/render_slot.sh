#!/usr/bin/env bash
# Full per-slot pipeline: prepare → render in Blender → generate trace
# overlay. Removes the old "I forgot to regenerate slot_trace" footgun
# from the round-7 feedback.
set -euo pipefail

EVENT_ID="${1:?usage: render_slot.sh <event_id> [--no-date-sticker]}"
shift || true

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PY="${REPO_ROOT}/.venv/bin/python"

"${PY}" "${REPO_ROOT}/scripts/afishathumb/prepare_slot.py" --event-id "${EVENT_ID}" "$@"

AFISHATHUMB_SLOT_EVENT_ID="${EVENT_ID}" \
    bash "${REPO_ROOT}/scripts/afishathumb/blender_run.sh" \
    "${REPO_ROOT}/scripts/afishathumb/render_slot_blender.py"

"${PY}" "${REPO_ROOT}/scripts/afishathumb/slot_trace.py" "${EVENT_ID}"
