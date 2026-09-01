from __future__ import annotations

import atexit
import json
import os
import threading
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable


RUN_FILENAME = "kaggle_run.json"
EVENTS_FILENAME = "kaggle_status_events.jsonl"
TERMINAL_EVENTS = {"render_done", "report_written"}
TERMINAL_STATUSES = {"complete", "done", "failed", "error", "cancelled", "canceled"}
PHASE_PROGRESS_PERCENT = {
    "bootstrap": 0,
    "created": 0,
    "prepare": 5,
    "preflight": 5,
    "pushed": 10,
    "kernel_shape_wait": 15,
    "poll": 20,
    "run": 50,
    "parse": 55,
    "download": 45,
    "distill": 65,
    "reason": 80,
    "render": 60,
    "publish": 85,
    "fresh_output_wait": 95,
    "report": 95,
    "write_report": 95,
    "cleanup": 98,
}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _find_file(filename: str, roots: list[Path] | None = None) -> Path | None:
    roots = roots or [Path("/kaggle/input"), Path("/kaggle/working"), Path.cwd()]
    for root in roots:
        candidate = root / filename
        if candidate.exists():
            return candidate
        if not root.exists():
            continue
        try:
            matches = sorted(root.rglob(filename))
        except Exception:
            matches = []
        if matches:
            return matches[0]
    return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _bounded_percent(value: Any) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    return max(0, min(100, int(round(number))))


def _progress_percent(progress: dict[str, Any], *, event: str, status: str, phase: str) -> int | None:
    for key in ("progress_percent", "percent", "completion_percent", "pct"):
        percent = _bounded_percent(progress.get(key))
        if percent is not None:
            return percent
    status_l = str(status or "").casefold()
    event_l = str(event or "").casefold()
    phase_l = str(phase or progress.get("phase") or "").casefold()
    if event_l in TERMINAL_EVENTS and status_l in TERMINAL_STATUSES:
        return 100
    if status_l in {"complete", "done"}:
        return 100
    for done_key, total_key in (
        ("cell_index", "cell_total"),
        ("url_index", "urls_total"),
        ("source_index", "sources_total"),
        ("sources_done", "sources_total"),
        ("post_index", "posts_total"),
        ("posts_done", "posts_total"),
        ("event_index", "events_total"),
        ("events_done", "events_total"),
        ("scene_index", "scenes_total"),
        ("scenes_done", "scenes_total"),
        ("frame_index", "frames_total"),
        ("frames_done", "frames_total"),
        ("month_index", "months_total"),
        ("item_index", "items_total"),
        ("items_done", "items_total"),
        ("processed", "total"),
    ):
        done = _as_float(progress.get(done_key))
        total = _as_float(progress.get(total_key))
        if done is None or total is None or total <= 0:
            continue
        percent = _bounded_percent((done / total) * 100.0)
        if percent is None:
            continue
        if status_l in {"running", "alive", "queued"} and percent >= 100:
            return 95
        return percent
    if phase_l in PHASE_PROGRESS_PERCENT:
        return PHASE_PROGRESS_PERCENT[phase_l]
    return None


class KaggleStatusClient:
    def __init__(
        self,
        config: dict[str, Any] | None,
        *,
        output_dir: str | Path | None = None,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config or {}
        self.output_dir = Path(output_dir or "/kaggle/working")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.output_dir / EVENTS_FILENAME
        self.log = log or (lambda message: print(message, flush=True))
        self._alive_stop = threading.Event()
        self._alive_thread: threading.Thread | None = None
        self._alive_progress_provider: Callable[[], dict[str, Any]] | None = None
        self._started_at = time.monotonic()
        self._event_seq = 0
        self._lock = threading.Lock()
        atexit.register(self.stop_alive)

    @classmethod
    def discover(
        cls,
        *,
        output_dir: str | Path | None = None,
        log: Callable[[str], None] | None = None,
    ) -> "KaggleStatusClient":
        path = _find_file(RUN_FILENAME)
        config: dict[str, Any] | None = None
        if path:
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    config = loaded
            except Exception:
                if log:
                    log(f"[kaggle_status] failed to load {path}: {traceback.format_exc(limit=1).strip()}")
        client = cls(config, output_dir=output_dir, log=log)
        if path:
            client.log(f"[kaggle_status] loaded {path}")
        else:
            client.log("[kaggle_status] kaggle_run.json not found; callbacks disabled")
        return client

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("callback_url") and self.config.get("run_id") and self.config.get("token"))

    def _append_local(self, payload: dict[str, Any], response: dict[str, Any] | None, error: str | None) -> None:
        safe_payload = dict(payload)
        if safe_payload.get("token"):
            safe_payload["token"] = "<redacted>"
        row = {"ts": _now_iso(), "payload": safe_payload, "response": response, "error": error}
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    def event(
        self,
        event: str,
        *,
        event_uid: str | None = None,
        phase: str | None = None,
        status: str | None = None,
        progress: dict[str, Any] | None = None,
        message: str | None = None,
        resource: dict[str, Any] | None = None,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        with self._lock:
            self._event_seq += 1
            seq = self._event_seq
        progress_payload = dict(progress or {})
        event_phase = phase or event
        event_status = status or ("alive" if event == "alive" else "running")
        percent = _progress_percent(
            progress_payload,
            event=event,
            status=event_status,
            phase=event_phase,
        )
        if percent is not None:
            progress_payload["progress_percent"] = percent
        payload = {
            "run_id": self.config.get("run_id"),
            "session_id": self.config.get("session_id"),
            "kind": self.config.get("kind"),
            "notebook": self.config.get("notebook"),
            "token": self.config.get("token"),
            "event": event,
            "event_uid": event_uid or f"{event}:{seq}",
            "phase": event_phase,
            "status": event_status,
            "progress": progress_payload,
        }
        if message:
            payload["message"] = str(message)
        if resource:
            payload["resource"] = resource
        response: dict[str, Any] | None = None
        error: str | None = None
        if self.enabled:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            request = urllib.request.Request(
                str(self.config["callback_url"]),
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    response = json.loads(raw) if raw else {}
            except urllib.error.HTTPError as exc:
                error = f"HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')[:500]}"
            except Exception as exc:
                error = f"{exc.__class__.__name__}: {exc}"
        else:
            error = "callbacks disabled"
        self._append_local(payload, response, error)
        if error:
            self.log(f"[kaggle_status] {event} callback failed: {error}")
        else:
            self.log(f"[kaggle_status] {event} callback ok: {response}")
        return response or {"ok": False, "error": error}

    def acquire_resource(self, key: str, *, ttl_seconds: int = 7200) -> bool:
        # Resource acquisition is idempotent for the same run on the host.  A
        # callback can therefore time out after the host has committed the
        # lease (for example while SQLite has another writer) even though the
        # notebook received no response.  Retry with one stable event UID so
        # the notebook does not turn that successful lease into a false
        # ``resource busy`` failure and leak it until TTL expiry.
        raw_attempts = os.getenv("KAGGLE_STATUS_RESOURCE_ACQUIRE_ATTEMPTS", "3")
        try:
            attempts = max(1, min(5, int(raw_attempts)))
        except ValueError:
            attempts = 3
        event_uid = f"resource_acquire:{key}"
        result: dict[str, Any] = {"ok": False}
        for attempt in range(1, attempts + 1):
            result = self.event(
                "resource_acquire",
                event_uid=event_uid,
                phase="preflight",
                status="running",
                resource={"key": key, "action": "acquire", "ttl_seconds": ttl_seconds},
            )
            action = result.get("resource_action")
            if action == "acquired":
                return True
            # A real holder response is authoritative; retry only ambiguous
            # transport/callback failures where no resource decision arrived.
            if action == "blocked" or result.get("ok") is True:
                return False
            if attempt < attempts:
                self.log(
                    f"[kaggle_status] resource_acquire ambiguous response; "
                    f"retrying attempt={attempt + 1}/{attempts}"
                )
                time.sleep(min(2, attempt))
        return False

    def release_resource(self, key: str) -> None:
        self.event(
            "resource_release",
            phase="cleanup",
            status="done",
            resource={"key": key, "action": "release"},
        )

    def start_alive(
        self,
        *,
        interval_seconds: int = 60,
        progress_provider: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        if self._alive_thread is not None:
            return
        self._alive_progress_provider = progress_provider

        def _loop() -> None:
            while not self._alive_stop.wait(max(5, int(interval_seconds))):
                progress = {
                    "elapsed_seconds": int(time.monotonic() - self._started_at),
                    "working_dir": os.getcwd(),
                }
                if self._alive_progress_provider:
                    try:
                        progress.update(self._alive_progress_provider() or {})
                    except Exception as exc:
                        progress["progress_provider_error"] = f"{exc.__class__.__name__}: {exc}"
                self.event("alive", phase=str(progress.get("phase") or "running"), status="alive", progress=progress, timeout=5.0)

        self._alive_thread = threading.Thread(target=_loop, name="kaggle-status-alive", daemon=True)
        self._alive_thread.start()

    def stop_alive(self) -> None:
        self._alive_stop.set()
        thread = self._alive_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)
        self._alive_thread = None


def load_status_client(*, output_dir: str | Path | None = None, log: Callable[[str], None] | None = None) -> KaggleStatusClient:
    return KaggleStatusClient.discover(output_dir=output_dir, log=log)
