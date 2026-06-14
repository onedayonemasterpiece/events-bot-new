from __future__ import annotations

from pathlib import Path

from .contracts import Candidate
from .dependencies import require_module


def make_contact_sheet(candidates: list[Candidate], out_path: str | Path, *, thumb: int = 256) -> Path:
    Image = require_module("PIL.Image", "Pillow")
    ImageDraw = require_module("PIL.ImageDraw", "Pillow")
    previews = [c for c in candidates if c.preview_path and Path(c.preview_path).exists()]
    if not previews:
        raise RuntimeError("Contact sheet requires rendered candidate previews")
    cols = min(4, len(previews))
    rows = (len(previews) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * thumb, rows * (thumb + 34)), (28, 28, 30))
    draw = ImageDraw.Draw(sheet)
    for idx, candidate in enumerate(previews):
        img = Image.open(candidate.preview_path).convert("RGB")
        img.thumbnail((thumb, thumb))
        x = (idx % cols) * thumb
        y = (idx // cols) * (thumb + 34)
        sheet.paste(img, (x + (thumb - img.width) // 2, y + (thumb - img.height) // 2))
        draw.text((x + 8, y + thumb + 8), f"{candidate.candidate_id} {candidate.cv_score:.2f}", fill=(235, 235, 235))
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(out)
    return out
