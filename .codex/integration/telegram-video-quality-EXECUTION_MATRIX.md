# Telegram video quality/CDN execution matrix

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| R01 | Analyze only posts with a confirmed event and actual video bytes strictly `<10 MiB` | Kaggle | extraction | high | video-producer | download/model/CDN remain closed before both gates |
| R02 | Use only the shared strict limiter, at least two declared keys, quota headroom and one hard small run cap | Kaggle/LLM | R01 | high | video-producer | no direct/local/overflow/model fallback; hard ceiling six |
| R03 | Never send an exact raw SHA to the model after a terminal accepted/review/rejected result | Kaggle/cache | R01,R02 | high | video-producer | permanent encrypted SHA sidecar precedes reserve |
| R04 | Score technical/visual/motion/legibility/usefulness/relevance and persist description/search evidence | research + producer + core | R01-R03 | high | video-research, video-producer, video-persistence | calibrated rubric, strict schema, application-owned scores |
| R05 | Upload accepted, rights-allowlisted videos only, directly from Kaggle to content-addressed Yandex CDN paths | Kaggle/CDN | R01-R04 | high | video-producer | rejected/review/unauthorized bytes are never uploaded |
| R06 | Support idempotent updating M:N links: many videos per event and one video for many events | core SQLite/import | producer relation payload | high | video-persistence | unique global SHA + updating relation upsert |
| R07 | Delete only a last-reference binary after grace/recheck while retaining the encrypted analysis cache | lifecycle cleanup | R06 | high | video-persistence | relink cancels orphan/queue; sidecar never queued |
| R08 | Export zero-or-more ranked vertical videos for a future static player without implementing UI | static builder/export | R04,R06 | medium | video-export | backward-compatible `video_assets[]` JSON/types/tests |
