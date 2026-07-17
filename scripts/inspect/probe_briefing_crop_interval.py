#!/usr/bin/env python3
"""Bounded Gemma 4 probe for briefing-image semantic crop intervals.

The model never chooses CSS object-position. It only describes the smallest
vertical interval containing the crop-critical subjects; deterministic geometry
then either fits that interval or rejects cover.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import mimetypes
import os
import time
import urllib.request
from io import BytesIO
from pathlib import Path
from typing import Any

PROMPT_VERSION = "briefing-crop-interval-v1"
DEFAULT_MODEL = "models/gemma-4-31b-it"


def build_prompt(width: int, height: int) -> str:
    return (
        f"Image is {width}x{height}. Classify it as portrait or scene. "
        "Return its crop-critical vertical interval: for portrait, all faces and complete heads; "
        "for scene, all principal subjects. Original-image pixels. JSON only."
    )


def response_schema(height: int) -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "kind": {"type": "STRING", "enum": ["portrait", "scene"]},
            "top_px": {"type": "INTEGER", "minimum": 0, "maximum": height},
            "bottom_px": {"type": "INTEGER", "minimum": 0, "maximum": height},
        },
        "required": ["kind", "top_px", "bottom_px"],
    }


def validate_interval(value: Any, height: int) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("kind") not in {"portrait", "scene"}:
        raise ValueError("invalid kind")
    top = int(value.get("top_px", -1)); bottom = int(value.get("bottom_px", -1))
    if not 0 <= top < bottom <= height:
        raise ValueError("invalid crop-critical interval")
    return {"kind": value["kind"], "top_px": top, "bottom_px": bottom}


def solve_vertical_crop(*, height: int, crop_height: int, top_px: int, bottom_px: int, margin_px: int) -> dict[str, Any]:
    crop_height = max(1, min(int(crop_height), int(height)))
    protected_top = max(0, int(top_px) - max(0, int(margin_px)))
    protected_bottom = min(int(height), int(bottom_px) + max(0, int(margin_px)))
    if protected_bottom - protected_top > crop_height:
        return {"usable": False, "reason": "critical_interval_does_not_fit"}
    preferred = round((protected_top + protected_bottom - crop_height) / 2)
    crop_top = min(protected_top, max(protected_bottom - crop_height, preferred))
    crop_top = max(0, min(height - crop_height, crop_top))
    if crop_top > protected_top or crop_top + crop_height < protected_bottom:
        return {"usable": False, "reason": "geometry_clamp_lost_interval"}
    overflow = height - crop_height
    focus_y = 50.0 if overflow <= 0 else crop_top / overflow * 100
    return {"usable": True, "crop_top_px": crop_top, "crop_bottom_px": crop_top + crop_height, "focus_y": round(focus_y, 3)}


def read_image(source: str) -> tuple[bytes, str, int, int]:
    if source.startswith(("https://", "http://")):
        with urllib.request.urlopen(source, timeout=30) as response:
            data = response.read(); mime = response.headers.get_content_type()
    else:
        path = Path(source); data = path.read_bytes(); mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    from PIL import Image
    with Image.open(BytesIO(data)) as image:
        width, height = image.size
        if not mime.startswith("image/"):
            mime = Image.MIME.get(image.format, "image/jpeg")
    return data, mime, width, height


async def run(args: argparse.Namespace) -> dict[str, Any]:
    data, mime, width, height = read_image(args.image)
    prompt = build_prompt(width, height)
    if args.dry_run:
        return {"prompt_version": PROMPT_VERSION, "model": args.model, "width": width, "height": height, "prompt": prompt, "schema": response_schema(height)}
    from google_ai import GoogleAIClient, SecretsProvider
    from main import get_supabase_client
    client = GoogleAIClient(
        supabase_client=get_supabase_client(), secrets_provider=SecretsProvider(),
        consumer="briefing_crop_probe", account_name="briefing-crop-probe",
        default_env_var_name=args.key_env, reserve_overflow_key_envs=[],
    )
    client.fallback_models = []; client.max_retries = 1
    started = time.monotonic()
    raw, usage = await client.generate_content_async(
        model=args.model,
        prompt=[{"inline_data": {"mime_type": mime, "data": base64.b64encode(data).decode("ascii")}}, prompt],
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_schema": response_schema(height),
            "thinking_config": {"include_thoughts": False, "thinking_level": "MINIMAL"},
        },
        max_output_tokens=72,
    )
    interval = validate_interval(json.loads(raw), height)
    result = {
        "prompt_version": PROMPT_VERSION, "model": args.model, "width": width, "height": height,
        "interval": interval, "latency_ms": round((time.monotonic() - started) * 1000),
        "usage": {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens},
    }
    if args.crop_height:
        result["solution"] = solve_vertical_crop(
            height=height, crop_height=args.crop_height, top_px=interval["top_px"],
            bottom_px=interval["bottom_px"], margin_px=args.margin_px,
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, help="Local path or HTTP(S) image URL")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--key-env", default="GOOGLE_API_KEY2")
    parser.add_argument("--crop-height", type=int)
    parser.add_argument("--margin-px", type=int, default=40)
    parser.add_argument("--output")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(run(args)); text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        path = Path(args.output); path.parent.mkdir(parents=True, exist_ok=True); path.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
