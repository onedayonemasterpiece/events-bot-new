# INC-2026-07-27 City Jazz title/OCR conflict replay

Production Telegram source: `https://t.me/meowafisha/8017`.

The original caption explicitly names «Калининград Сити Джаз», while the visual/OCR
lane produced the unrelated phrase «КАЛИНИНГРАД СИТИ ДА БИСТРО ЯНТАРЬ». The first
Gemma extraction selected the OCR phrase and `event_type=выставка`; Smart Update's
cross-event guard then correctly refused the later grounded title proposal because
it was unrelated to the already-persisted candidate title.

`source.json` preserves the raw caption and the minimal OCR conflict needed for the
Telegram Monitoring title-review replay. The public row affected in production is
`event.id=7036`.
