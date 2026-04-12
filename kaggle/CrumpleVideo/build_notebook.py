from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
NOTEBOOK_PATH = ROOT / "crumple_video.ipynb"
POSTER_MODULE_PATH = ROOT / "poster_overlay.py"
STORY_MODULE_PATH = ROOT / "story_publish.py"
GESTURE_MODULE_PATH = ROOT / "story_gesture_overlay.py"


def _replace_embedded_module(
    source: str,
    *,
    anchor: str,
    module_source: str,
) -> str:
    anchor_index = source.find(anchor)
    if anchor_index < 0:
        raise RuntimeError(f"Could not locate {anchor} in notebook")
    start = source.find("        code = ", anchor_index)
    end_marker = "\n        target.write_text(code, encoding='utf-8')"
    end = source.find(end_marker, start)
    if start < 0 or end < 0:
        raise RuntimeError(f"Could not locate embedded module block for {anchor}")
    replacement = f"        code = {module_source!r}"
    return source[:start] + replacement + source[end:]


def _replace_block(
    source: str,
    *,
    start_marker: str,
    end_marker: str,
    replacement: str,
    label: str,
) -> str:
    start = source.find(start_marker)
    if start < 0:
        raise RuntimeError(f"Could not locate {label} start marker in notebook")
    end = source.find(end_marker, start)
    if end < 0:
        raise RuntimeError(f"Could not locate {label} end marker in notebook")
    return source[:start] + replacement + source[end:]


def main() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    poster_module_source = POSTER_MODULE_PATH.read_text(encoding="utf-8").rstrip("\n")
    story_module_source = STORY_MODULE_PATH.read_text(encoding="utf-8").rstrip("\n")
    gesture_module_source = GESTURE_MODULE_PATH.read_text(encoding="utf-8").rstrip("\n")

    overlay_call_old = (
        "        overlay = scene.get(\"poster_overlay\")\n"
        "        if isinstance(overlay, dict):\n"
        "            overlay_text = overlay.get(\"text\")\n"
        "        else:\n"
        "            overlay_text = None\n"
        "        if found and isinstance(overlay_text, str) and overlay_text.strip():\n"
        "            try:\n"
        "                found = apply_poster_overlay(found, text=overlay_text, out_dir=posters_dir, search_roots=OVERLAY_FONT_ROOTS)\n"
        "            except Exception as e:\n"
        "                log(f\"⚠️ Overlay failed for scene {i}: {e}\")\n"
    )
    overlay_call_new = (
        "        overlay = scene.get(\"poster_overlay\")\n"
        "        overlay_text = None\n"
        "        highlight_title = None\n"
        "        if isinstance(overlay, dict):\n"
        "            overlay_text = overlay.get(\"text\")\n"
        "            missing = overlay.get(\"missing\")\n"
        "            if isinstance(missing, list):\n"
        "                highlight_title = \"title\" in {str(part).strip().casefold() for part in missing if isinstance(part, str)}\n"
        "        if found and isinstance(overlay_text, str) and overlay_text.strip():\n"
        "            try:\n"
        "                found = apply_poster_overlay(\n"
        "                    found,\n"
        "                    text=overlay_text,\n"
        "                    out_dir=posters_dir,\n"
        "                    search_roots=OVERLAY_FONT_ROOTS,\n"
        "                    highlight_title=highlight_title,\n"
        "                )\n"
        "            except Exception as e:\n"
        "                log(f\"⚠️ Overlay failed for scene {i}: {e}\")\n"
    )
    helper_insert_old = (
        "_ensure_story_publish_module()\n\n"
        "# Import from working dir (preferred)\n"
        "sys.path.insert(0, '/kaggle/working')\n"
        "from poster_overlay import apply_poster_overlay\n"
        "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle\n"
    )
    helper_insert_new = (
        "_ensure_story_publish_module()\n\n"
        "def _ensure_story_gesture_overlay_module():\n"
        "    target = Path('/kaggle/working/story_gesture_overlay.py')\n"
        "    if target.exists():\n"
        "        return\n"
        "    try:\n"
        f"        code = {gesture_module_source!r}\n"
        "        target.write_text(code, encoding='utf-8')\n"
        "        print(f'✅ Wrote story_gesture_overlay.py to {target}')\n"
        "    except Exception as e:\n"
        "        print(f'⚠️ Failed to write story_gesture_overlay.py: {e}')\n\n"
        "_ensure_story_gesture_overlay_module()\n\n"
        "# Import from working dir (preferred)\n"
        "sys.path.insert(0, '/kaggle/working')\n"
        "from poster_overlay import apply_poster_overlay\n"
        "from story_publish import load_story_publish_runtime, preflight_story_publish_from_kaggle, publish_story_from_kaggle\n"
        "from story_gesture_overlay import GESTURE_STEP_COUNT, apply_story_gesture_frame\n"
    )
    telegram_runtime_old = (
        "TELEGRAM_READY = False\n"
        "TELEGRAM_ERROR = None\n"
        "try:\n"
        "    from telethon import TelegramClient\n"
        "    from telethon.sessions import StringSession\n"
        "    from kaggle_secrets import UserSecretsClient\n"
        "    TELEGRAM_READY = True\n"
        "except Exception as e:\n"
        "    TELEGRAM_ERROR = e\n"
    )
    telegram_runtime_new = (
        "TELEGRAM_READY = False\n"
        "TELEGRAM_ERROR = None\n"
        "KAGGLE_SECRETS_READY = False\n"
        "KAGGLE_SECRETS_ERROR = None\n"
        "try:\n"
        "    from telethon import TelegramClient\n"
        "    from telethon.sessions import StringSession\n"
        "    TELEGRAM_READY = True\n"
        "except Exception as e:\n"
        "    TELEGRAM_ERROR = e\n"
        "\n"
        "try:\n"
        "    from kaggle_secrets import UserSecretsClient\n"
        "    KAGGLE_SECRETS_READY = True\n"
        "except Exception as e:\n"
        "    UserSecretsClient = None\n"
        "    KAGGLE_SECRETS_ERROR = e\n"
    )
    telegram_download_old = (
        "async def download_via_telegram(filenames_map):\n"
        "    if not TELEGRAM_READY or not filenames_map:\n"
        "        return\n"
        "    log(f\"\\n--- 🔵 Telegram: cache lookup ({len(filenames_map)} files) ---\")\n"
        "    try:\n"
        "        secrets = UserSecretsClient()\n"
        "        api_id = int(secrets.get_secret(\"TELEGRAM_API_ID\"))\n"
        "        api_hash = secrets.get_secret(\"TELEGRAM_API_HASH\")\n"
        "        session_str = secrets.get_secret(\"TELEGRAM_SESSION\")\n"
        "        channel_id = None\n"
        "        try:\n"
        "            cid = secrets.get_secret(\"SOURCE_CHANNEL_ID\")\n"
        "            if cid:\n"
        "                channel_id = int(cid)\n"
        "        except Exception:\n"
        "            pass\n"
        "    except Exception as e:\n"
        "        log(f\"[SKIP] Telegram secrets missing: {e}\")\n"
        "        return\n"
        "\n"
        "    try:\n"
        "        client = TelegramClient(StringSession(session_str), api_id, api_hash)\n"
        "        await client.connect()\n"
        "        if not await client.is_user_authorized():\n"
        "            log(\"[ERROR] Telegram session invalid, skipping cache.\")\n"
        "            await client.disconnect()\n"
        "            return\n"
        "        target = channel_id if channel_id else 'me'\n"
        "        target_name = 'CHANNEL' if channel_id else 'SAVED MESSAGES'\n"
        "        log(f\"   > Connected. Searching in: {target_name}\")\n"
        "        for fname, local_path in filenames_map.items():\n"
        "            if os.path.exists(local_path):\n"
        "                continue\n"
        "            log(f\"   > Search: {fname} ...\")\n"
        "            found_msg = None\n"
        "            try:\n"
        "                async for message in client.iter_messages(target, search=fname, limit=100):\n"
        "                    if message.media:\n"
        "                        found_msg = message\n"
        "                        break\n"
        "            except Exception as e:\n"
        "                log(f\"     [ERR] {e}\")\n"
        "            if found_msg:\n"
        "                log('     [FOUND] Downloading...')\n"
        "                await found_msg.download_media(file=local_path)\n"
        "                log('     [DONE]')\n"
        "            else:\n"
        "                log('     [NOT FOUND]')\n"
        "        await client.disconnect()\n"
        "    except Exception as e:\n"
        "        log(f\"[TELEGRAM ERROR] {e}\")\n"
    )
    telegram_download_new = (
        "def _telegram_runtime_search_roots():\n"
        "    roots = []\n"
        "    for raw in (Path('/kaggle/working'), SOURCE_FOLDER, Path(KAGGLE_INPUT_ROOT), Path('/kaggle/input')):\n"
        "        try:\n"
        "            root = Path(raw)\n"
        "        except Exception:\n"
        "            continue\n"
        "        if root not in roots:\n"
        "            roots.append(root)\n"
        "    return roots\n"
        "\n"
        "\n"
        "def _load_telegram_auth(log):\n"
        "    runtime_error = None\n"
        "    try:\n"
        "        runtime = load_story_publish_runtime(search_roots=_telegram_runtime_search_roots(), log=lambda _msg: None)\n"
        "    except Exception as e:\n"
        "        runtime = None\n"
        "        runtime_error = e\n"
        "    if isinstance(runtime, dict):\n"
        "        auth = runtime.get('auth') or {}\n"
        "        session_str = str(auth.get('session') or '').strip()\n"
        "        api_id = auth.get('api_id')\n"
        "        api_hash = str(auth.get('api_hash') or '').strip()\n"
        "        if session_str and api_id and api_hash:\n"
        "            channel_id = auth.get('source_channel_id')\n"
        "            try:\n"
        "                channel_id = int(channel_id) if channel_id is not None else None\n"
        "            except Exception:\n"
        "                channel_id = None\n"
        "            log('   > Telegram auth source: story runtime')\n"
        "            return {\n"
        "                'api_id': int(api_id),\n"
        "                'api_hash': api_hash,\n"
        "                'session': session_str,\n"
        "                'channel_id': channel_id,\n"
        "            }\n"
        "\n"
        "    kaggle_error = None\n"
        "    if KAGGLE_SECRETS_READY and UserSecretsClient is not None:\n"
        "        try:\n"
        "            secrets = UserSecretsClient()\n"
        "            api_id = int(secrets.get_secret('TELEGRAM_API_ID'))\n"
        "            api_hash = secrets.get_secret('TELEGRAM_API_HASH')\n"
        "            session_str = secrets.get_secret('TELEGRAM_SESSION')\n"
        "            channel_id = None\n"
        "            try:\n"
        "                cid = secrets.get_secret('SOURCE_CHANNEL_ID')\n"
        "                if cid:\n"
        "                    channel_id = int(cid)\n"
        "            except Exception:\n"
        "                pass\n"
        "            if session_str and api_hash:\n"
        "                log('   > Telegram auth source: kaggle secrets')\n"
        "                return {\n"
        "                    'api_id': api_id,\n"
        "                    'api_hash': api_hash,\n"
        "                    'session': session_str,\n"
        "                    'channel_id': channel_id,\n"
        "                }\n"
        "        except Exception as e:\n"
        "            kaggle_error = e\n"
        "\n"
        "    error = runtime_error or kaggle_error or KAGGLE_SECRETS_ERROR or TELEGRAM_ERROR or 'no auth source available'\n"
        "    log(f\"[SKIP] Telegram auth missing: {error}\")\n"
        "    return None\n"
        "\n"
        "\n"
        "async def download_via_telegram(filenames_map):\n"
        "    if not TELEGRAM_READY or not filenames_map:\n"
        "        return\n"
        "    log(f\"\\n--- 🔵 Telegram: cache lookup ({len(filenames_map)} files) ---\")\n"
        "    auth = _load_telegram_auth(log)\n"
        "    if not auth:\n"
        "        return\n"
        "\n"
        "    try:\n"
        "        client = TelegramClient(StringSession(auth['session']), auth['api_id'], auth['api_hash'])\n"
        "        await client.connect()\n"
        "        if not await client.is_user_authorized():\n"
        "            log(\"[ERROR] Telegram session invalid, skipping cache.\")\n"
        "            await client.disconnect()\n"
        "            return\n"
        "        channel_id = auth.get('channel_id')\n"
        "        target = channel_id if channel_id else 'me'\n"
        "        target_name = 'CHANNEL' if channel_id else 'SAVED MESSAGES'\n"
        "        log(f\"   > Connected. Searching in: {target_name}\")\n"
        "        for fname, local_path in filenames_map.items():\n"
        "            if os.path.exists(local_path):\n"
        "                continue\n"
        "            log(f\"   > Search: {fname} ...\")\n"
        "            found_msg = None\n"
        "            try:\n"
        "                async for message in client.iter_messages(target, search=fname, limit=100):\n"
        "                    if message.media:\n"
        "                        found_msg = message\n"
        "                        break\n"
        "            except Exception as e:\n"
        "                log(f\"     [ERR] {e}\")\n"
        "            if found_msg:\n"
        "                log('     [FOUND] Downloading...')\n"
        "                await found_msg.download_media(file=local_path)\n"
        "                log('     [DONE]')\n"
        "            else:\n"
        "                log('     [NOT FOUND]')\n"
        "        await client.disconnect()\n"
        "    except Exception as e:\n"
        "        log(f\"[TELEGRAM ERROR] {e}\")\n"
    )
    sequence_init_old = (
        "    sequence = []\n"
        "    \n"
        "    for seg in segments:\n"
    )
    sequence_init_new = (
        "    sequence = []\n"
        "    gesture_step_index = 0\n"
        "    pending_gesture_step = None\n"
        "    pending_gesture_total = 0\n"
        "    pending_gesture_offset = 0\n"
        "    \n"
        "    for seg_index, seg in enumerate(segments):\n"
    )
    unfold_old = (
        "        # Unfold (Ball -> Flat)\n"
        "        if seg.unfold_len > 0:\n"
        "            unfold = render_motion(frames, seg.unfold_len, seg.easing_unfold, False, True)\n"
        "            sequence.extend(unfold)\n"
    )
    unfold_new = (
        "        # Unfold (Ball -> Flat)\n"
        "        if seg.unfold_len > 0:\n"
        "            unfold = render_motion(frames, seg.unfold_len, seg.easing_unfold, False, True)\n"
        "            if pending_gesture_step is not None and pending_gesture_total > 0:\n"
        "                unfold = [\n"
        "                    apply_story_gesture_frame(\n"
        "                        frame,\n"
        "                        step_index=pending_gesture_step,\n"
        "                        frame_index=pending_gesture_offset + idx,\n"
        "                        total_frames=pending_gesture_total,\n"
        "                        search_roots=OVERLAY_FONT_ROOTS,\n"
        "                    )\n"
        "                    for idx, frame in enumerate(unfold)\n"
        "                ]\n"
        "                pending_gesture_step = None\n"
        "                pending_gesture_total = 0\n"
        "                pending_gesture_offset = 0\n"
        "            sequence.extend(unfold)\n"
    )
    hold_ball_old = (
        "        # Hold Ball\n"
        "        for _ in range(seg.hold_ball):\n"
        "            sequence.append(ball)\n"
    )
    hold_ball_new = (
        "        # Hold Ball\n"
        "        ball_frames = [ball for _ in range(seg.hold_ball)]\n"
        "        next_unfold_len = 0\n"
        "        if seg_index + 1 < len(segments):\n"
        "            next_unfold_len = int(max(0, segments[seg_index + 1].unfold_len))\n"
        "        if gesture_step_index < GESTURE_STEP_COUNT and next_unfold_len > 0 and ball_frames:\n"
        "            interstitial_total = len(ball_frames) + next_unfold_len\n"
        "            ball_frames = [\n"
        "                apply_story_gesture_frame(\n"
        "                    frame,\n"
        "                    step_index=gesture_step_index,\n"
        "                    frame_index=idx,\n"
        "                    total_frames=interstitial_total,\n"
        "                    search_roots=OVERLAY_FONT_ROOTS,\n"
        "                )\n"
        "                for idx, frame in enumerate(ball_frames)\n"
        "            ]\n"
        "            pending_gesture_step = gesture_step_index\n"
        "            pending_gesture_total = interstitial_total\n"
        "            pending_gesture_offset = len(ball_frames)\n"
        "            gesture_step_index += 1\n"
        "        sequence.extend(ball_frames)\n"
    )
    telegram_cache_helper_old = (
        "def _prepare_telegram_cache(urls, posters_dir):\n"
        "    if not TELEGRAM_READY or not urls:\n"
        "        return {}\n"
        "    filenames_map = {}\n"
        "    url_map = {}\n"
        "    for idx, url in enumerate(urls):\n"
        "        if not isinstance(url, str):\n"
        "            continue\n"
        "        fname = url.split('/')[-1].split('?')[0]\n"
        "        if not fname:\n"
        "            continue\n"
        "        local_path = posters_dir / f\"tg_{idx}_{fname}\"\n"
        "        filenames_map[fname] = str(local_path)\n"
        "        url_map[url] = local_path\n"
        "    if filenames_map:\n"
        "        _run_async(download_via_telegram(filenames_map))\n"
        "    cache = {}\n"
        "    for url, path in url_map.items():\n"
        "        if path.exists():\n"
        "            cache[url] = path\n"
        "    if cache:\n"
        "        log(f\"✅ Telegram cache hits: {len(cache)}\")\n"
        "    return cache\n"
    )
    telegram_cache_helper_new = (
        "def _safe_telegram_cache_path(url: str, idx: int, posters_dir: Path) -> Path:\n"
        "    fname = str(url).split('/')[-1].split('?')[0]\n"
        "    suffix = Path(fname).suffix.lower()\n"
        "    if suffix not in {'.jpg', '.jpeg', '.png', '.webp'}:\n"
        "        suffix = '.jpg'\n"
        "    digest = hashlib.sha1(str(url).encode('utf-8')).hexdigest()[:16]\n"
        "    return posters_dir / f\"tg_{idx}_{digest}{suffix}\"\n"
        "\n"
        "\n"
        "def _prepare_telegram_cache(urls, posters_dir):\n"
        "    if not TELEGRAM_READY or not urls:\n"
        "        return {}\n"
        "    filenames_map = {}\n"
        "    url_map = {}\n"
        "    for idx, url in enumerate(urls):\n"
        "        if not isinstance(url, str):\n"
        "            continue\n"
        "        fname = url.split('/')[-1].split('?')[0]\n"
        "        if not fname:\n"
        "            continue\n"
        "        local_path = _safe_telegram_cache_path(url, idx, posters_dir)\n"
        "        filenames_map[fname] = str(local_path)\n"
        "        url_map[url] = local_path\n"
        "    if filenames_map:\n"
        "        _run_async(download_via_telegram(filenames_map))\n"
        "    cache = {}\n"
        "    for url, path in url_map.items():\n"
        "        try:\n"
        "            if path.exists():\n"
        "                cache[url] = path\n"
        "        except OSError:\n"
        "            continue\n"
        "    if cache:\n"
        "        log(f\"✅ Telegram cache hits: {len(cache)}\")\n"
        "    return cache\n"
    )
    telegram_cache_block_old = (
        "    if urls_to_cache:\n"
        "        filenames_map = {}\n"
        "        for idx, url in enumerate(urls_to_cache):\n"
        "            fname = url.split('/')[-1].split('?')[0]\n"
        "            if not fname:\n"
        "                continue\n"
        "            local_path = posters_dir / f\"tg_{idx}_{fname}\"\n"
        "            filenames_map[fname] = str(local_path)\n"
        "            url_map[url] = local_path\n"
        "        if TELEGRAM_READY and filenames_map:\n"
        "            _run_async(download_via_telegram(filenames_map))\n"
        "        elif not TELEGRAM_READY:\n"
        "            log(f\"[SKIP] Telegram cache disabled: {TELEGRAM_ERROR}\")\n"
        "        telegram_cache = {url: path for url, path in url_map.items() if path.exists()}\n"
        "        if telegram_cache:\n"
        "            log(f\"✅ Telegram cache hits: {len(telegram_cache)}\")\n"
    )
    telegram_cache_block_new = (
        "    if urls_to_cache:\n"
        "        telegram_cache = _prepare_telegram_cache(urls_to_cache, posters_dir)\n"
        "        if telegram_cache:\n"
        "            url_map.update(telegram_cache)\n"
    )

    replaced = False
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        raw_source = cell.get("source", "")
        source_was_list = isinstance(raw_source, list)
        source = "".join(raw_source) if source_was_list else str(raw_source)
        if "def _ensure_poster_overlay_module():" not in source:
            continue
        source = _replace_embedded_module(
            source,
            anchor="def _ensure_poster_overlay_module():",
            module_source=poster_module_source,
        )
        source = _replace_embedded_module(
            source,
            anchor="def _ensure_story_publish_module():",
            module_source=story_module_source,
        )
        if "def _ensure_story_gesture_overlay_module():" in source:
            source = _replace_embedded_module(
                source,
                anchor="def _ensure_story_gesture_overlay_module():",
                module_source=gesture_module_source,
            )
        elif helper_insert_old in source:
            source = source.replace(helper_insert_old, helper_insert_new, 1)
        else:
            raise RuntimeError("Could not locate story gesture helper insertion point in notebook")
        if (
            "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle\n"
            in source
            and "from story_publish import load_story_publish_runtime, preflight_story_publish_from_kaggle, publish_story_from_kaggle\n"
            not in source
        ):
            source = source.replace(
                "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle\n",
                "from story_publish import load_story_publish_runtime, preflight_story_publish_from_kaggle, publish_story_from_kaggle\n",
                1,
            )
        if overlay_call_old in source:
            source = source.replace(overlay_call_old, overlay_call_new, 1)
        elif overlay_call_new not in source:
            raise RuntimeError("Could not locate overlay call block in notebook")
        if telegram_runtime_new not in source:
            source = _replace_block(
                source,
                start_marker="TELEGRAM_READY = False\n",
                end_marker="\n\nasync def download_via_telegram(filenames_map):\n",
                replacement=telegram_runtime_new,
                label="Telegram runtime",
            )
        if "_load_telegram_auth(log):" not in source:
            source = _replace_block(
                source,
                start_marker="async def download_via_telegram(filenames_map):\n",
                end_marker="\n\ndef _run_async(coro):\n",
                replacement=telegram_download_new,
                label="Telegram download",
            )
        if sequence_init_old in source:
            source = source.replace(sequence_init_old, sequence_init_new, 1)
        elif sequence_init_new not in source:
            raise RuntimeError("Could not locate sequence init block in notebook")
        if unfold_old in source:
            source = source.replace(unfold_old, unfold_new, 1)
        elif unfold_new not in source:
            raise RuntimeError("Could not locate unfold block in notebook")
        if hold_ball_old in source:
            source = source.replace(hold_ball_old, hold_ball_new, 1)
        elif hold_ball_new not in source:
            raise RuntimeError("Could not locate hold-ball block in notebook")
        if telegram_cache_helper_old in source:
            source = source.replace(telegram_cache_helper_old, telegram_cache_helper_new, 1)
        elif telegram_cache_helper_new not in source:
            raise RuntimeError("Could not locate telegram cache helper block in notebook")
        if telegram_cache_block_old in source:
            source = source.replace(telegram_cache_block_old, telegram_cache_block_new, 1)
        elif telegram_cache_block_new not in source:
            raise RuntimeError("Could not locate telegram cache block in notebook")
        cell["source"] = source.splitlines(keepends=True) if source_was_list else source
        replaced = True
        break

    if not replaced:
        raise RuntimeError("Could not find CrumpleVideo pipeline cell in notebook")

    NOTEBOOK_PATH.write_text(
        json.dumps(notebook, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
