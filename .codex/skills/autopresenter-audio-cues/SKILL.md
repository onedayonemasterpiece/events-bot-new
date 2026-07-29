---
name: autopresenter-audio-cues
description: Source, license-check, normalize, upload, and validate short audio cues for the KenigEvents Autopresenter. Use for narration, stings, error cues, and other scene-synchronized audio stored on static.kenigevents.ru.
---

# Autopresenter Audio Cues

Use the existing Yandex Object Storage bucket/CDN. Do not create infrastructure and do not commit third-party audio files.

## Workflow

1. Confirm the exact scene, timing, and whether the source is user-provided or third-party.
2. For user-provided Telegram media, use only the approved local E2E human-session bundle; never reuse the Kaggle/monitoring bundle.
3. For third-party media, record the author, canonical source URL, exact license URL, and download URL. Accept only a license compatible with public playback; prefer CC0. Do not imitate or redistribute proprietary OS sounds when a licensed substitute works.
4. Inspect the file with `file`/`ffprobe`, compute SHA-256, and keep the original when browser-compatible. Transcode only when needed, without clipping or padding the cue.
5. Upload content-addressed to the existing prefix:
   `assets/autopresenter/scenario-YYYYMMDD/<slug>-<sha256>.<ext>`.
6. Verify the public CDN response, content type, byte count, and downloaded SHA-256. Then validate playback in the headed browser with the exact scene timing.
7. Put provenance and acceptance evidence in the canonical Autopresenter docs. Never place credentials, auth bundles, private Telegram URLs, or binary media in this skill.

## Scene integration

- Preload only short cues needed in the current presentation.
- Start narration at the same phase as its matching typed line.
- Pause and rewind every cue when switching or stopping a scene.
- Use `--autoplay-policy=no-user-gesture-required` only in the headed presentation agent; still expose a visible failure state when playback is blocked.
- Keep `prefers-reduced-motion` independent from audio unless the product requirement explicitly links them.

See [references/provenance-example.md](references/provenance-example.md) for the accepted 2026-07-30 cue records.
