#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_static_site_builder_kaggle import (  # noqa: E402
    copy_tree,
    create_input_dataset,
    wait_dataset_ready,
    wait_kernel_dataset_sources,
    write_dataset_metadata,
)

KERNELS = {
    "kppk": (ROOT / "kaggle" / "TransportKppkRefresh", "kenigevents-transport-kppk-refresh"),
    "bus": (ROOT / "kaggle" / "TransportBusRefresh", "kenigevents-transport-bus-refresh"),
}


def _publish_output(args: argparse.Namespace, output: Path) -> dict | None:
    if not args.state_root:
        return None
    from transport_refresh.store import TransportManifestStore

    manifest_path = output / f"transport-{args.provider}-manifest.json"
    candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
    enqueue = None
    if args.publish_db:
        from db import Database
        from main import enqueue_job
        from models import JobTask

        database = Database(args.publish_db)

        def enqueue(key: str, payload: dict) -> None:
            asyncio.run(enqueue_job(
                database, 0, JobTask.static_site_build, payload=payload,
                coalesce_key=key, requeue_done=True,
            ))
    report = TransportManifestStore(args.state_root).publish(args.provider, candidate, enqueue=enqueue)
    (output / "transport_publish_result.json").write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8",
    )
    return report


def _callback_url(explicit: str) -> str | None:
    if explicit.strip():
        return explicit.strip()
    if os.getenv("KAGGLE_STATUS_CALLBACK_URL", "").strip():
        return os.environ["KAGGLE_STATUS_CALLBACK_URL"].strip()
    webhook = os.getenv("WEBHOOK_URL", "").strip()
    return webhook.rstrip("/") + "/internal/kaggle/run-event" if webhook else None


def _status_dataset(args: argparse.Namespace, client, username: str, run_id: str, kernel_ref: str, dataset_ref: str) -> str | None:
    callback = _callback_url(args.status_callback_url)
    if not args.status_db or not callback:
        print("[transport-kaggle] status dataset skipped (status DB/callback not both configured)", flush=True)
        return None
    from db import Database
    from kaggle_status import create_kaggle_run_config, create_kaggle_status_dataset

    config = asyncio.run(create_kaggle_run_config(
        Database(args.status_db), run_id=f"transport-{args.provider}:{run_id}", session_id=None,
        kind=f"transport_schedule_{args.provider}", notebook=KERNELS[args.provider][0].name,
        kernel_ref=kernel_ref, dataset_ref=dataset_ref, callback_url=callback,
        resource_leases=[f"transport_schedule:{args.provider}:refresh"],
    ))
    if not config:
        return None
    ref = create_kaggle_status_dataset(client, username=username, slug_prefix=f"status-transport-{args.provider}", run_id=run_id, config=config)
    if ref:
        wait_dataset_ready(client, ref, expected_files=["kaggle_run.json", "kaggle_status_client.py"])
    return ref


def run(args: argparse.Namespace) -> int:
    from video_announce.kaggle_client import KaggleClient

    username = os.getenv("KAGGLE_USERNAME", "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required")
    source_url = args.source_url.strip()
    if not source_url.startswith("https://"):
        raise ValueError("--source-url must be HTTPS")
    client = KaggleClient()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    kernel_src, kernel_slug = KERNELS[args.provider]
    with tempfile.TemporaryDirectory(prefix=f"transport-{args.provider}-") as temp:
        root = Path(temp)
        staging, dataset = root / "kernel", root / "dataset"
        copy_tree(kernel_src, staging)
        copy_tree(ROOT / "transport_refresh", staging / "transport_refresh")
        dataset.mkdir(parents=True)
        config = {
            "provider": args.provider,
            "source_url": source_url,
            "timeout_seconds": args.source_timeout_seconds,
            "queued_at": datetime.now(timezone.utc).isoformat(),
        }
        expected = ["transport_refresh_config.json"]
        if args.source_payload:
            source = Path(args.source_payload).resolve()
            name = f"{args.provider}_source_payload.json"
            shutil.copy2(source, dataset / name)
            config["source_payload_filename"] = name
            expected.append(name)
        (dataset / "transport_refresh_config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        dataset_ref = f"{username}/transport-{args.provider}-input-{run_id.lower().replace(':', '-')}"
        write_dataset_metadata(dataset, dataset_ref, f"transport {args.provider} input {run_id}")
        create_input_dataset(client, dataset, dataset_ref)
        wait_dataset_ready(client, dataset_ref, expected_files=expected)
        kernel_ref = f"{username}/{kernel_slug}"
        sources = [dataset_ref]
        status_ref = _status_dataset(args, client, username, run_id, kernel_ref, dataset_ref)
        if status_ref:
            sources.append(status_ref)
        client.push_kernel(kernel_path=staging, dataset_sources=sources)
        wait_kernel_dataset_sources(client, kernel_ref, sources)
        print(f"[transport-kaggle] pushed provider={args.provider} run_id={run_id} kernel={kernel_ref}", flush=True)
        if args.no_wait:
            return 0
        deadline = time.monotonic() + args.timeout_minutes * 60
        last = None
        while time.monotonic() < deadline:
            time.sleep(max(5, args.poll_interval))
            last = client.get_kernel_status(kernel_ref)
            status = str(last.get("status") or "").upper()
            print(f"[transport-kaggle] status={status}", flush=True)
            if status == "COMPLETE":
                output = Path(args.output_dir) / f"{args.provider}-{run_id}"
                output.mkdir(parents=True, exist_ok=True)
                client.download_kernel_output(kernel_ref, path=output, force=True)
                print(f"[transport-kaggle] output={output}", flush=True)
                publish = _publish_output(args, output)
                if publish is not None:
                    print(f"[transport-kaggle] publish={json.dumps(publish, ensure_ascii=False, sort_keys=True)}", flush=True)
                return 0
            if status in {"ERROR", "FAILED", "CANCELLED"}:
                raise RuntimeError(f"transport Kaggle job failed: {last}")
        raise TimeoutError(f"transport Kaggle job timed out: {last}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Run one independent transport provider refresh on Kaggle CPU")
    result.add_argument("--provider", choices=sorted(KERNELS), required=True)
    result.add_argument("--source-url", required=True)
    result.add_argument("--source-payload", help="Controlled-canary JSON payload; provider kernel still validates it")
    result.add_argument("--source-timeout-seconds", type=int, default=30)
    result.add_argument("--run-id")
    result.add_argument("--status-db", default=os.getenv("TRANSPORT_STATUS_DB", ""))
    result.add_argument("--status-callback-url", default="")
    result.add_argument("--output-dir", default=str(ROOT / "artifacts" / "codex" / "transport-refresh"))
    result.add_argument("--state-root", default=os.getenv("TRANSPORT_MANIFEST_ROOT", ""), help="Server-side immutable/current manifest root")
    result.add_argument("--publish-db", default=os.getenv("TRANSPORT_PUBLISH_DB", ""), help="Fly SQLite DB for changed-only static_site_build enqueue")
    result.add_argument("--timeout-minutes", type=int, default=30)
    result.add_argument("--poll-interval", type=int, default=20)
    result.add_argument("--no-wait", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    return run(parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
