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
        "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle\n"
        "from story_gesture_overlay import GESTURE_STEP_COUNT, apply_story_gesture_frame\n"
    )
    sequence_init_old = (
        "    sequence = []\n"
        "    \n"
        "    for seg in segments:\n"
    )
    sequence_init_prev = (
        "    sequence = []\n"
        "    gesture_step_index = 0\n"
        "    pending_gesture_step = None\n"
        "    pending_gesture_total = 0\n"
        "    pending_gesture_offset = 0\n"
        "    \n"
        "    for seg_index, seg in enumerate(segments):\n"
    )
    sequence_init_new = (
        "    sequence = []\n"
        "    gesture_step_index = 0\n"
        "    pending_gesture_step = None\n"
        "    pending_gesture_total = 0\n"
        "    pending_gesture_offset = 0\n"
        "    gesture_fold_lead_frames = 8\n"
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
    hold_ball_prev = (
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
    hold_ball_new = (
        "        next_unfold_len = 0\n"
        "        if seg_index + 1 < len(segments):\n"
        "            next_unfold_len = int(max(0, segments[seg_index + 1].unfold_len))\n"
        "\n"
        "        if gesture_step_index < GESTURE_STEP_COUNT and next_unfold_len > 0 and seg.fold_len > 0 and sequence:\n"
        "            fold_lead = min(gesture_fold_lead_frames, seg.fold_len)\n"
        "            if fold_lead > 0:\n"
        "                interstitial_total = fold_lead + seg.hold_ball + next_unfold_len\n"
        "                fold_start = len(sequence) - fold_lead\n"
        "                sequence[fold_start:] = [\n"
        "                    apply_story_gesture_frame(\n"
        "                        frame,\n"
        "                        step_index=gesture_step_index,\n"
        "                        frame_index=idx,\n"
        "                        total_frames=interstitial_total,\n"
        "                        search_roots=OVERLAY_FONT_ROOTS,\n"
        "                    )\n"
        "                    for idx, frame in enumerate(sequence[fold_start:])\n"
        "                ]\n"
        "                pending_gesture_step = gesture_step_index\n"
        "                pending_gesture_total = interstitial_total\n"
        "                pending_gesture_offset = fold_lead + seg.hold_ball\n"
        "                active_gesture_step = gesture_step_index\n"
        "                active_interstitial_total = interstitial_total\n"
        "                gesture_step_index += 1\n"
        "            else:\n"
        "                active_gesture_step = None\n"
        "                active_interstitial_total = 0\n"
        "        else:\n"
        "            active_gesture_step = None\n"
        "            active_interstitial_total = 0\n"
        "\n"
        "        # Hold Ball\n"
        "        ball_frames = [ball for _ in range(seg.hold_ball)]\n"
        "        next_unfold_len = 0\n"
        "        if seg_index + 1 < len(segments):\n"
        "            next_unfold_len = int(max(0, segments[seg_index + 1].unfold_len))\n"
        "        if active_gesture_step is not None and ball_frames:\n"
        "            ball_frames = [\n"
        "                apply_story_gesture_frame(\n"
        "                    frame,\n"
        "                    step_index=active_gesture_step,\n"
        "                    frame_index=(pending_gesture_offset - len(ball_frames)) + idx,\n"
        "                    total_frames=active_interstitial_total,\n"
        "                    search_roots=OVERLAY_FONT_ROOTS,\n"
        "                )\n"
        "                for idx, frame in enumerate(ball_frames)\n"
        "            ]\n"
        "        sequence.extend(ball_frames)\n"
    )
    preflight_helpers_old = (
        "def _guess_extension(url: str, content_type: str | None) -> str:\n"
        "    ext = Path(url.split(\"?\", 1)[0]).suffix.lower()\n"
        "    if ext in (\".jpg\", \".jpeg\", \".png\", \".webp\"):\n"
        "        return ext\n"
        "    if content_type:\n"
        "        ext_guess = mimetypes.guess_extension(content_type.split(\";\", 1)[0].strip())\n"
        "        if ext_guess in (\".jpg\", \".jpeg\", \".png\", \".webp\"):\n"
        "            return ext_guess\n"
        "    return \".jpg\"\n\n\n"
    )
    preflight_helpers_new = (
        "def _guess_extension(url: str, content_type: str | None) -> str:\n"
        "    ext = Path(url.split(\"?\", 1)[0]).suffix.lower()\n"
        "    if ext in (\".jpg\", \".jpeg\", \".png\", \".webp\"):\n"
        "        return ext\n"
        "    if content_type:\n"
        "        ext_guess = mimetypes.guess_extension(content_type.split(\";\", 1)[0].strip())\n"
        "        if ext_guess in (\".jpg\", \".jpeg\", \".png\", \".webp\"):\n"
        "            return ext_guess\n"
        "    return \".jpg\"\n\n\n"
        "def _poster_label(value: str | Path | None) -> str:\n"
        "    raw = str(value or \"\").strip()\n"
        "    if not raw:\n"
        "        return \"unknown\"\n"
        "    raw = raw.split(\"?\", 1)[0].rstrip(\"/\")\n"
        "    name = Path(raw).name or raw\n"
        "    if len(name) <= 72:\n"
        "        return name\n"
        "    return name[:32] + \"...\" + name[-24:]\n\n\n"
        "def _telegram_cache_path(posters_dir: Path, url: str, idx: int) -> Path | None:\n"
        "    if not isinstance(url, str) or not url:\n"
        "        return None\n"
        "    ext = Path(url.split(\"?\", 1)[0]).suffix.lower()\n"
        "    if ext not in {\".jpg\", \".jpeg\", \".png\", \".webp\"}:\n"
        "        ext = \".jpg\"\n"
        "    digest = hashlib.md5(url.encode(\"utf-8\")).hexdigest()[:16]\n"
        "    return posters_dir / f\"tg_{idx:02d}_{digest}{ext}\"\n\n\n"
        "def _render_readiness_label(ready_scenes: int, total_scenes: int) -> str:\n"
        "    if total_scenes <= 0:\n"
        "        return \"NO_SCENES\"\n"
        "    if ready_scenes <= 0:\n"
        "        return \"FAIL\"\n"
        "    if ready_scenes == total_scenes:\n"
        "        return \"FULL\"\n"
        "    if ready_scenes * 2 > total_scenes:\n"
        "        return \"GO (degraded)\"\n"
        "    if ready_scenes * 2 == total_scenes:\n"
        "        return \"BORDERLINE\"\n"
        "    return \"HIGH_RISK\"\n\n\n"
        "def _log_poster_preflight(source_reports, scene_reports):\n"
        "    if source_reports:\n"
        "        log(\"\\n--- 🟩 Poster preflight: source availability ---\")\n"
        "        for report in source_reports:\n"
        "            status = \"✅\" if report.get(\"ok\") else \"❌\"\n"
        "            label = report.get(\"label\") or \"unknown\"\n"
        "            source = report.get(\"source\") or \"unknown\"\n"
        "            detail = str(report.get(\"detail\") or \"\").strip()\n"
        "            tail = f\" · {detail}\" if detail else \"\"\n"
        "            log(f\"{status} {label} · {source}{tail}\")\n"
        "    if scene_reports:\n"
        "        log(\"\\n--- 🟩 Poster preflight: scene readiness ---\")\n"
        "        for report in scene_reports:\n"
        "            status = \"✅\" if report.get(\"ok\") else \"❌\"\n"
        "            scene_no = int(report.get(\"scene\", -1)) + 1\n"
        "            detail = str(report.get(\"detail\") or \"poster missing\").strip()\n"
        "            log(f\"{status} Scene {scene_no} · {detail}\")\n"
        "    ready_sources = sum(1 for report in source_reports if report.get(\"ok\"))\n"
        "    total_sources = len(source_reports)\n"
        "    ready_scenes = sum(1 for report in scene_reports if report.get(\"ok\"))\n"
        "    total_scenes = len(scene_reports)\n"
        "    missing = [str(int(report.get(\"scene\", -1)) + 1) for report in scene_reports if not report.get(\"ok\")]\n"
        "    readiness = _render_readiness_label(ready_scenes, total_scenes)\n"
        "    missing_tail = f\"; missing scenes: {', '.join(missing)}\" if missing else \"\"\n"
        "    log(\n"
        "        f\"Poster preflight summary: sources {ready_sources}/{total_sources} ready; \"\n"
        "        f\"scenes {ready_scenes}/{total_scenes} ready; render readiness: {readiness}{missing_tail}\"\n"
        "    )\n\n\n"
    )
    prepare_cache_old = (
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
        "    return cache\n\n\n"
    )
    prepare_cache_new = (
        "def _prepare_telegram_cache(urls, posters_dir):\n"
        "    if not TELEGRAM_READY or not urls:\n"
        "        return {}\n"
        "    filenames_map = {}\n"
        "    url_map = {}\n"
        "    for idx, url in enumerate(urls, start=1):\n"
        "        if not isinstance(url, str):\n"
        "            continue\n"
        "        fname = url.split('/')[-1].split('?')[0]\n"
        "        if not fname:\n"
        "            continue\n"
        "        local_path = _telegram_cache_path(posters_dir, url, idx)\n"
        "        if local_path is None:\n"
        "            continue\n"
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
        "    return cache\n\n\n"
    )
    poster_init_old = (
        "    posters_dir = WORKING_DIR / \"posters\"\n"
        "    posters_dir.mkdir(parents=True, exist_ok=True)\n"
        "    download_cache: dict[str, Path] = {}\n"
        "    telegram_cache = {}\n"
        "    url_map: dict[str, Path] = {}\n"
        "    urls_to_cache = []\n"
        "    seen_urls = set()\n"
        "    for scene in scenes[:12]:\n"
        "        imgs = scene.get('images')\n"
        "        if isinstance(imgs, str):\n"
        "            imgs = [imgs]\n"
        "        for u in (imgs or []):\n"
        "            if isinstance(u, str) and u.startswith('http') and u not in seen_urls:\n"
        "                seen_urls.add(u)\n"
        "                urls_to_cache.append(u)\n"
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
        "            log(f\"✅ Telegram cache hits: {len(telegram_cache)}\")\n\n\n"
    )
    poster_init_new = (
        "    posters_dir = WORKING_DIR / \"posters\"\n"
        "    posters_dir.mkdir(parents=True, exist_ok=True)\n"
        "    download_cache: dict[str, Path] = {}\n"
        "    download_status: dict[str, dict] = {}\n"
        "    poster_source_reports = []\n"
        "    scene_reports = []\n"
        "    telegram_cache = {}\n"
        "    url_map: dict[str, Path] = {}\n"
        "    urls_to_cache = []\n"
        "    seen_urls = set()\n"
        "    for scene in scenes[:12]:\n"
        "        imgs = scene.get('images')\n"
        "        if isinstance(imgs, str):\n"
        "            imgs = [imgs]\n"
        "        for u in (imgs or []):\n"
        "            if isinstance(u, str) and u.startswith('http') and u not in seen_urls:\n"
        "                seen_urls.add(u)\n"
        "                urls_to_cache.append(u)\n"
        "    if urls_to_cache:\n"
        "        filenames_map = {}\n"
        "        for idx, url in enumerate(urls_to_cache, start=1):\n"
        "            fname = url.split('/')[-1].split('?')[0]\n"
        "            if not fname:\n"
        "                continue\n"
        "            local_path = _telegram_cache_path(posters_dir, url, idx)\n"
        "            if local_path is None:\n"
        "                continue\n"
        "            filenames_map[fname] = str(local_path)\n"
        "            url_map[url] = local_path\n"
        "        if TELEGRAM_READY and filenames_map:\n"
        "            _run_async(download_via_telegram(filenames_map))\n"
        "        elif not TELEGRAM_READY:\n"
        "            log(f\"[SKIP] Telegram cache disabled: {TELEGRAM_ERROR}\")\n"
        "        telegram_cache = {url: path for url, path in url_map.items() if path.exists()}\n"
        "        if telegram_cache:\n"
        "            log(f\"✅ Telegram cache hits: {len(telegram_cache)}\")\n\n\n"
    )
    download_poster_old = (
        "    def _download_poster(url: str, idx: int):\n"
        "        if not url:\n"
        "            return None\n"
        "        if url in download_cache:\n"
        "            return download_cache[url]\n"
        "        cached = telegram_cache.get(url)\n"
        "        if cached is not None and cached.exists():\n"
        "            download_cache[url] = cached\n"
        "            return cached\n"
        "        last_err = None\n"
        "        for attempt in range(3):\n"
        "            try:\n"
        "                resp = SESSION.get(url, timeout=(5, 30), allow_redirects=True)\n"
        "                resp.raise_for_status()\n"
        "                content_type = resp.headers.get(\"content-type\") or \"\"\n"
        "                data = resp.content\n"
        "                if not data:\n"
        "                    raise RuntimeError(\"empty response\")\n"
        "                if content_type and not content_type.startswith(\"image/\"):\n"
        "                    raise RuntimeError(f\"unexpected content-type: {content_type}\")\n"
        "                if len(data) < 2048:\n"
        "                    raise RuntimeError(f\"response too small: {len(data)} bytes\")\n"
        "                ext = _guess_extension(url, content_type)\n"
        "                out_path = url_map.get(url)\n"
        "                if out_path is None:\n"
        "                    digest = hashlib.md5(url.encode(\"utf-8\")).hexdigest()[:8]\n"
        "                    out_path = posters_dir / f\"poster_{idx}_{digest}{ext}\"\n"
        "                elif out_path.suffix.lower() not in {\".jpg\", \".jpeg\", \".png\", \".webp\"}:\n"
        "                    out_path = out_path.with_suffix(ext)\n"
        "                    url_map[url] = out_path\n"
        "                out_path.write_bytes(data)\n"
        "                download_cache[url] = out_path\n"
        "                return out_path\n"
        "            except Exception as e:\n"
        "                last_err = e\n"
        "                log(\n"
        "                    f\"⚠️ Failed to download poster (attempt {attempt + 1}/3): {url} ({e})\"\n"
        "                )\n"
        "                time.sleep(1.5 * (attempt + 1))\n"
        "        if last_err is not None:\n"
        "            log(f\"⚠️ Poster download failed after retries: {url} ({last_err})\")\n"
        "        return None\n\n"
    )
    download_poster_new = (
        "    def _download_poster(url: str, idx: int):\n"
        "        if not url:\n"
        "            return None\n"
        "        if url in download_cache:\n"
        "            cached_path = download_cache[url]\n"
        "            if url not in download_status:\n"
        "                download_status[url] = {\n"
        "                    \"ok\": True,\n"
        "                    \"source\": \"download-cache\",\n"
        "                    \"detail\": f\"reused {cached_path.name}\",\n"
        "                }\n"
        "            return cached_path\n"
        "        cached = telegram_cache.get(url)\n"
        "        if cached is not None and cached.exists():\n"
        "            download_cache[url] = cached\n"
        "            download_status[url] = {\n"
        "                \"ok\": True,\n"
        "                \"source\": \"telegram-cache\",\n"
        "                \"detail\": f\"cached {cached.name}\",\n"
        "            }\n"
        "            return cached\n"
        "        last_err = None\n"
        "        for attempt in range(3):\n"
        "            try:\n"
        "                resp = SESSION.get(url, timeout=(5, 30), allow_redirects=True)\n"
        "                resp.raise_for_status()\n"
        "                content_type = resp.headers.get(\"content-type\") or \"\"\n"
        "                data = resp.content\n"
        "                if not data:\n"
        "                    raise RuntimeError(\"empty response\")\n"
        "                if content_type and not content_type.startswith(\"image/\"):\n"
        "                    raise RuntimeError(f\"unexpected content-type: {content_type}\")\n"
        "                if len(data) < 2048:\n"
        "                    raise RuntimeError(f\"response too small: {len(data)} bytes\")\n"
        "                ext = _guess_extension(url, content_type)\n"
        "                out_path = url_map.get(url)\n"
        "                if out_path is None:\n"
        "                    digest = hashlib.md5(url.encode(\"utf-8\")).hexdigest()[:8]\n"
        "                    out_path = posters_dir / f\"poster_{idx}_{digest}{ext}\"\n"
        "                elif out_path.suffix.lower() not in {\".jpg\", \".jpeg\", \".png\", \".webp\"}:\n"
        "                    out_path = out_path.with_suffix(ext)\n"
        "                    url_map[url] = out_path\n"
        "                out_path.write_bytes(data)\n"
        "                download_cache[url] = out_path\n"
        "                download_status[url] = {\n"
        "                    \"ok\": True,\n"
        "                    \"source\": \"http\",\n"
        "                    \"detail\": f\"downloaded {out_path.name}\",\n"
        "                }\n"
        "                return out_path\n"
        "            except Exception as e:\n"
        "                last_err = e\n"
        "                log(\n"
        "                    f\"⚠️ Failed to download poster (attempt {attempt + 1}/3): {url} ({e})\"\n"
        "                )\n"
        "                time.sleep(1.5 * (attempt + 1))\n"
        "        if last_err is not None:\n"
        "            download_status[url] = {\n"
        "                \"ok\": False,\n"
        "                \"source\": \"http\",\n"
        "                \"detail\": f\"{type(last_err).__name__}: {last_err}\",\n"
        "            }\n"
        "            log(f\"⚠️ Poster download failed after retries: {url} ({last_err})\")\n"
        "        return None\n\n"
    )
    preflight_insert_old = "    # Collect scene posters\n"
    preflight_insert_new = (
        "    for idx, url in enumerate(urls_to_cache, start=1):\n"
        "        resolved = _download_poster(url, idx)\n"
        "        status = download_status.get(url) or {}\n"
        "        poster_source_reports.append(\n"
        "            {\n"
        "                \"ok\": bool(resolved),\n"
        "                \"label\": _poster_label(url),\n"
        "                \"source\": str(status.get(\"source\") or (\"missing\" if not resolved else \"http\")),\n"
        "                \"detail\": str(status.get(\"detail\") or (\"ready\" if resolved else \"not downloaded\")),\n"
        "            }\n"
        "        )\n\n"
        "    # Collect scene posters\n"
    )
    scene_collect_old = (
        "    # Collect scene posters\n"
        "    for i, scene in enumerate(scenes[:12]):  # Max 12 as per user spec\n"
        "        img_name = scene.get('image')\n"
        "        images = scene.get(\"images\")\n"
        "        if isinstance(images, str):\n"
        "            images = [images]\n"
        "        found = None\n"
        "        if img_name:\n"
        "            found = _resolve_image_path(img_name)\n\n"
        "        if not found and images:\n"
        "            for candidate in images:\n"
        "                if not candidate:\n"
        "                    continue\n"
        "                if isinstance(candidate, str) and candidate.startswith(\"http\"):\n"
        "                    found = _download_poster(candidate, i + 1)\n"
        "                elif isinstance(candidate, str):\n"
        "                    found = _resolve_image_path(candidate)\n"
        "                if found:\n"
        "                    break\n\n"
        "        if not found:\n"
        "            # Fallback to posterN naming\n"
        "            for ext in [\".jpg\", \".png\", \".jpeg\"]:\n"
        "                local = SOURCE_FOLDER / f\"poster{i+1}{ext}\"\n"
        "                if local.exists():\n"
        "                    found = local\n"
        "                    break\n\n"
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
        "                log(f\"⚠️ Overlay failed for scene {i}: {e}\")\n\n"
        "        if found:\n"
        "            posters.append(found)\n"
        "        else:\n"
        "            log(f\"⚠️ Poster not found for scene {i}\")\n\n"
    )
    scene_collect_new = (
        "    # Collect scene posters\n"
        "    for i, scene in enumerate(scenes[:12]):  # Max 12 as per user spec\n"
        "        img_name = scene.get('image')\n"
        "        images = scene.get(\"images\")\n"
        "        if isinstance(images, str):\n"
        "            images = [images]\n"
        "        found = None\n"
        "        selected_label = None\n"
        "        selected_source = None\n"
        "        if img_name:\n"
        "            found = _resolve_image_path(img_name)\n"
        "            if found:\n"
        "                selected_label = _poster_label(img_name)\n"
        "                selected_source = \"local-image\"\n\n"
        "        if not found and images:\n"
        "            for candidate in images:\n"
        "                if not candidate:\n"
        "                    continue\n"
        "                if isinstance(candidate, str) and candidate.startswith(\"http\"):\n"
        "                    found = _download_poster(candidate, i + 1)\n"
        "                    status = download_status.get(candidate) or {}\n"
        "                    if found:\n"
        "                        selected_label = _poster_label(candidate)\n"
        "                        selected_source = str(status.get(\"source\") or \"http\")\n"
        "                elif isinstance(candidate, str):\n"
        "                    found = _resolve_image_path(candidate)\n"
        "                    if found:\n"
        "                        selected_label = _poster_label(candidate)\n"
        "                        selected_source = \"local-image\"\n"
        "                if found:\n"
        "                    break\n\n"
        "        if not found:\n"
        "            # Fallback to posterN naming\n"
        "            for ext in [\".jpg\", \".png\", \".jpeg\"]:\n"
        "                local = SOURCE_FOLDER / f\"poster{i+1}{ext}\"\n"
        "                if local.exists():\n"
        "                    found = local\n"
        "                    selected_label = local.name\n"
        "                    selected_source = \"fallback-local\"\n"
        "                    break\n\n"
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
        "                log(f\"⚠️ Overlay failed for scene {i}: {e}\")\n\n"
        "        if found:\n"
        "            posters.append(found)\n"
        "            scene_reports.append(\n"
        "                {\n"
        "                    \"scene\": i,\n"
        "                    \"ok\": True,\n"
        "                    \"detail\": f\"{selected_label or Path(found).name} via {selected_source or 'local'}\",\n"
        "                }\n"
        "            )\n"
        "        else:\n"
        "            scene_reports.append(\n"
        "                {\n"
        "                    \"scene\": i,\n"
        "                    \"ok\": False,\n"
        "                    \"detail\": \"poster missing\",\n"
        "                }\n"
        "            )\n"
        "            log(f\"⚠️ Poster not found for scene {i}\")\n\n"
    )
    preflight_log_old = "    # Check for Final scene\n"
    preflight_log_new = (
        "    _log_poster_preflight(poster_source_reports, scene_reports)\n\n"
        "    # Check for Final scene\n"
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
        if overlay_call_old in source:
            source = source.replace(overlay_call_old, overlay_call_new, 1)
        elif overlay_call_new not in source:
            raise RuntimeError("Could not locate overlay call block in notebook")
        if sequence_init_old in source:
            source = source.replace(sequence_init_old, sequence_init_new, 1)
        elif sequence_init_prev in source:
            source = source.replace(sequence_init_prev, sequence_init_new, 1)
        elif sequence_init_new not in source:
            raise RuntimeError("Could not locate sequence init block in notebook")
        if unfold_old in source:
            source = source.replace(unfold_old, unfold_new, 1)
        elif unfold_new not in source:
            raise RuntimeError("Could not locate unfold block in notebook")
        if hold_ball_old in source:
            source = source.replace(hold_ball_old, hold_ball_new, 1)
        elif hold_ball_prev in source:
            source = source.replace(hold_ball_prev, hold_ball_new, 1)
        elif hold_ball_new not in source:
            raise RuntimeError("Could not locate hold-ball block in notebook")
        if preflight_helpers_old in source:
            source = source.replace(preflight_helpers_old, preflight_helpers_new, 1)
        elif preflight_helpers_new not in source:
            raise RuntimeError("Could not locate preflight helper block in notebook")
        if prepare_cache_old in source:
            source = source.replace(prepare_cache_old, prepare_cache_new, 1)
        elif prepare_cache_new not in source:
            raise RuntimeError("Could not locate telegram cache helper block in notebook")
        if poster_init_old in source:
            source = source.replace(poster_init_old, poster_init_new, 1)
        elif poster_init_new not in source:
            raise RuntimeError("Could not locate poster init block in notebook")
        if download_poster_old in source:
            source = source.replace(download_poster_old, download_poster_new, 1)
        elif download_poster_new not in source:
            raise RuntimeError("Could not locate download poster block in notebook")
        if preflight_insert_new not in source:
            if preflight_insert_old in source:
                source = source.replace(preflight_insert_old, preflight_insert_new, 1)
            else:
                raise RuntimeError("Could not locate poster preflight insertion point in notebook")
        if scene_collect_old in source:
            source = source.replace(scene_collect_old, scene_collect_new, 1)
        elif scene_collect_new not in source:
            raise RuntimeError("Could not locate scene collect block in notebook")
        if preflight_log_new not in source:
            if preflight_log_old in source:
                source = source.replace(preflight_log_old, preflight_log_new, 1)
            else:
                raise RuntimeError("Could not locate poster preflight log block in notebook")
        # Older runs of this builder inserted preflight helpers repeatedly.
        # Keep one `_poster_label`/`_telegram_cache_path`/`_log_poster_preflight`
        # block before `main_pipeline` so the builder is idempotent.
        poster_helper_marker = "def _poster_label("
        main_pipeline_marker = "def main_pipeline():"
        poster_helper_start = source.find(poster_helper_marker)
        main_pipeline_start = source.find(main_pipeline_marker)
        if poster_helper_start >= 0 and main_pipeline_start > poster_helper_start:
            helper_region = source[poster_helper_start:main_pipeline_start]
            helper_parts = helper_region.split("\n" + poster_helper_marker)
            if len(helper_parts) > 1:
                source = (
                    source[:poster_helper_start]
                    + helper_parts[0].rstrip()
                    + "\n\n\n"
                    + source[main_pipeline_start:]
                )
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
