---
name: google-tts-generation
description: Generate Russian or multilingual voice-overs with Google Gemini TTS through the events-bot shared Supabase quota ledger. Use for Google/Gemini TTS, requests to озвучить text, friendly female narration, multi-key TTS capacity, TTS quota checks, WAV generation, or sending a generated voice-over to Telegram Saved Messages.
---

# Google TTS Generation

Use only the bundled CLI and the repository's strict `GoogleTTSClient`.

## Hard rules

- Never call `google.genai.Client`, Gemini REST, `curl`, or a raw Google key directly.
- Never accept or print a raw key. Configure only env-variable names through `GOOGLE_TTS_KEY_ENVS`.
- Always run `--check` first. It makes no reservation and no provider request.
- Make one provider request per explicit generation. Do not retry, switch models, or try another key after a sent failure.
- Treat multi-key mode as pre-call selection: the atomic shared reserve chooses one eligible registered key before the only provider call.
- Fail closed when Supabase, a model row, the RPC, the registry, a secret, or quota is unavailable. Never use process-local/direct-key fallback.
- Use `gemini-2.5-flash-preview-tts` by default. Use `gemini-3.1-flash-tts-preview` only when explicitly requested; never as fallback.
- Save audio under `artifacts/codex/google-tts/`; never commit generated audio or receipts.
- A sent failed provider request remains spent and audited. Retry only after a new explicit user request.
- Multiple keys from one Google AI project may share provider quota. Pool only keys whose quota ownership is understood.

## Workflow

1. Create a UTF-8 transcript file under `artifacts/codex/google-tts/`.
2. Check readiness without consuming quota:

```bash
python .codex/skills/google-tts-generation/scripts/generate_tts.py \
  --env-file .env \
  --key-envs GOOGLE_API_KEY,GOOGLE_API_KEY2,GOOGLE_API_KEY3,GOOGLE_API_KEY5 \
  --check
```

3. Confirm the selected model, daily usage, remaining capacity, and exactly one provider attempt.
4. Generate only after the user explicitly requests it in the current turn:

```bash
python .codex/skills/google-tts-generation/scripts/generate_tts.py \
  --env-file .env \
  --key-envs GOOGLE_API_KEY,GOOGLE_API_KEY2,GOOGLE_API_KEY3,GOOGLE_API_KEY5 \
  --text-file artifacts/codex/google-tts/transcript.txt \
  --output artifacts/codex/google-tts/output.wav \
  --voice Aoede \
  --language Russian \
  --style "Warm, friendly and kind, with a gentle smile."
```

5. Inspect the redacted receipt. Do not repeat a failed live command automatically.
6. If the user asks for Telegram delivery, invoke the separate `telegram-human-session` skill after the WAV exists and verify Saved Messages.

The shared `google-tts` scope caps both supported model aliases together at 10 requests/day per registered quota lane.

