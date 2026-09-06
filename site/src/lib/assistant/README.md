# Conversational Search browser modules

Canonical implementation and limitations live in
[voice solution](../../../../docs/features/static-site-pages/smart-vector-search/voice-search-solution-v1.md)
and the [existing integration handoff](../../../../docs/features/static-site-pages/smart-vector-search/20260906-voice-prototype-codex.md).

This directory is no longer only the original 49-test pure kernel checkpoint:
its inline adapter uses existing Search/Auth/transport/cards, owner-scoped
IndexedDB recovery and foreground AudioWorklet capture. The shared domain
kernel is re-exported from `supabase/functions/event-search/assistant-*`.
No separate shell, personalization opt-in, provider client or telemetry sink
is owned here. Source and synthetic browser tests are not deployed ASR or
physical-phone acceptance; use the canonical documents for those gates.
