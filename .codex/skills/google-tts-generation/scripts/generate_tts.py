#!/usr/bin/env python3
"""Generate Gemini TTS only through the fail-closed shared limiter."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from google_ai.secrets import SecretsProvider  # noqa: E402
from google_ai.tts import (  # noqa: E402
    DEFAULT_TTS_MODEL,
    DEFAULT_TTS_VOICE,
    GoogleTTSClient,
    write_wav,
)


def load_env(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"env file not found: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail-closed Gemini TTS using the shared Google AI limiter"
    )
    parser.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    parser.add_argument(
        "--key-envs",
        help="Comma-separated env-variable names; defaults to GOOGLE_TTS_KEY_ENVS",
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--text-file")
    parser.add_argument("--output")
    parser.add_argument("--model", default=DEFAULT_TTS_MODEL)
    parser.add_argument("--voice", default=DEFAULT_TTS_VOICE)
    parser.add_argument("--language", default="Russian")
    parser.add_argument(
        "--style",
        default="Warm, friendly and kind, with a gentle smile.",
    )
    return parser.parse_args()


def build_supabase_client():
    try:
        from supabase import ClientOptions, create_client
    except ImportError as exc:
        raise SystemExit(
            "supabase-py is required; install the repository requirements"
        ) from exc
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (
        (os.getenv("SUPABASE_SERVICE_KEY") or "").strip()
        or (os.getenv("SUPABASE_KEY") or "").strip()
    )
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_KEY/SUPABASE_KEY are required")
    schema = (os.getenv("SUPABASE_SCHEMA") or "public").strip() or "public"
    return create_client(
        url.rstrip("/"),
        key,
        options=ClientOptions(schema=schema),
    )


def output_path(raw: str) -> Path:
    destination = Path(raw)
    if not destination.is_absolute():
        destination = REPO_ROOT / destination
    destination = destination.resolve()
    artifacts = (REPO_ROOT / "artifacts").resolve()
    if artifacts not in destination.parents:
        raise SystemExit("TTS output must be inside the repository artifacts/ directory")
    return destination


def transcript_path(raw: str) -> Path:
    source = Path(raw)
    if not source.is_absolute():
        source = REPO_ROOT / source
    source = source.resolve(strict=True)
    allowed = (REPO_ROOT / "artifacts" / "codex" / "google-tts").resolve()
    if source != allowed and allowed not in source.parents:
        raise SystemExit(
            "TTS transcript must be inside artifacts/codex/google-tts/"
        )
    if not source.is_file():
        raise SystemExit(f"TTS transcript is not a regular file: {source}")
    return source


def key_env_names(raw: str | None) -> list[str]:
    value = raw or os.getenv("GOOGLE_TTS_KEY_ENVS") or ""
    names = [item.strip() for item in value.split(",") if item.strip()]
    if not names:
        raise SystemExit("Set --key-envs or GOOGLE_TTS_KEY_ENVS")
    invalid = [
        name
        for name in names
        if not name.startswith("GOOGLE_API_KEY")
        or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for char in name)
    ]
    if invalid:
        raise SystemExit("Invalid Google key env-variable name(s): " + ",".join(invalid))
    return names


async def run() -> int:
    args = parse_args()
    load_env(Path(args.env_file).expanduser().resolve())
    client = GoogleTTSClient(
        supabase_client=build_supabase_client(),
        secrets_provider=SecretsProvider(),
        key_envs=key_env_names(args.key_envs),
        consumer="codex_google_tts",
        account_name="codex-google-tts",
    )
    check = client.preflight(model=args.model)
    if args.check:
        print(json.dumps(check, ensure_ascii=False, indent=2))
        return 0

    if not args.text_file or not args.output:
        raise SystemExit("Live generation requires --text-file and --output")
    text_path = transcript_path(args.text_file)
    transcript = text_path.read_text(encoding="utf-8").strip()
    destination = output_path(args.output)
    speech = await client.generate_speech_async(
        text=transcript,
        model=args.model,
        voice=args.voice,
        language=args.language,
        style=args.style,
    )
    write_wav(destination, speech)
    receipt = {
        "ok": True,
        "file": str(destination),
        "model": speech.model,
        "voice": speech.voice,
        "mime_type": speech.mime_type,
        "duration_seconds": round(speech.duration_seconds, 3),
        "request_uid": speech.request_uid,
        "api_key_id": speech.api_key_id,
        "key_alias": speech.key_alias,
        "quota_scope": speech.quota_scope,
        "provider_attempts": 1,
    }
    print(json.dumps(receipt, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
